"""Strict OTLP JSON entity-event ingestion and producer snapshot reconciliation."""

from __future__ import annotations

import base64
import hashlib
import json
from collections.abc import Iterable, Iterator, Mapping, Sequence
from functools import cache
from typing import Literal, cast

from google.protobuf.json_format import ParseDict, ParseError  # type: ignore[import-untyped]
from opentelemetry.proto.collector.logs.v1.logs_service_pb2 import ExportLogsServiceRequest
from opentelemetry.proto.common.v1.common_pb2 import AnyValue, KeyValue
from opentelemetry.proto.logs.v1.logs_pb2 import LogRecord
from pydantic import ValidationError

from extended_otel_semconv.edges import edge_id
from extended_otel_semconv.entities import SemanticEntity, entity_from_attributes
from extended_otel_semconv.errors import SemanticModelError
from extended_otel_semconv.relationships import RelationshipDefinition, service_graph_relationships
from otel_servicegraph_diff.engine.elements import (
    FrozenModel,
    GraphContribution,
    GraphContributionRetraction,
    GraphEdge,
    GraphElement,
    GraphElementMutation,
    GraphNode,
    NonEmptyString,
    UnixNano,
)
from otel_servicegraph_diff.engine.relationships import relationship_allows
from otel_servicegraph_diff.ingest.metrics import IngestRejection, JsonDocument, JsonValue, ingest_rejection

ENTITY_STATE_EVENT = "entity.state"
ENTITY_DELETE_EVENT = "entity.delete"
ENTITY_OBSERVER_ID = "otel.entity.observer.id"
type EntityEventName = Literal["entity.state", "entity.delete"]
type EventAttribute = JsonValue


class EntityStateObservation(FrozenModel):
    kind: Literal["state"] = "state"
    source_key: NonEmptyString
    contributor_id: NonEmptyString
    node_element_id: NonEmptyString
    observed_at_unix_nano: UnixNano
    payload_hash: NonEmptyString
    contributions: tuple[GraphContribution, ...]


class EntityDeleteObservation(FrozenModel):
    kind: Literal["delete"] = "delete"
    source_key: NonEmptyString
    contributor_id: NonEmptyString
    node_element_id: NonEmptyString
    observed_at_unix_nano: UnixNano
    payload_hash: NonEmptyString


type EntityEventObservation = EntityStateObservation | EntityDeleteObservation


class EntitySourceState(FrozenModel):
    ordering_key: tuple[int, int, str]
    element_ids: tuple[str, ...]


class EntityReconciliationResult(FrozenModel):
    state: EntitySourceState
    mutations: tuple[GraphElementMutation, ...] = ()


def iter_otlp_json_entity_events(
    payload: str,
    *,
    report_interval_grace_seconds: int,
) -> Iterator[EntityEventObservation | IngestRejection]:
    if report_interval_grace_seconds < 0:
        raise ValueError("entity report interval grace must not be negative")
    try:
        document = JsonDocument.model_validate_json(payload).root
        request = ParseDict(
            cast(dict[str, object], document),
            ExportLogsServiceRequest(),
            ignore_unknown_fields=True,
        )
    except (ParseError, TypeError, ValidationError, ValueError) as exc:
        yield ingest_rejection("invalid_otlp_entity_event_json", exc)
        return

    for resource_logs in request.resource_logs:
        resource_attributes = _key_values(resource_logs.resource.attributes)
        for scope_logs in resource_logs.scope_logs:
            scope_identity = {
                "name": scope_logs.scope.name,
                "version": scope_logs.scope.version,
                "schema_url": scope_logs.schema_url,
            }
            for record in scope_logs.log_records:
                try:
                    observation = _entity_event(
                        record,
                        resource_attributes,
                        scope_identity,
                        report_interval_grace_seconds,
                        service_graph_relationships(),
                    )
                except (SemanticModelError, TypeError, ValidationError, ValueError) as exc:
                    yield ingest_rejection("invalid_otel_entity_event", exc)
                else:
                    if observation is not None:
                        yield observation


def reconcile_entity_event(
    previous: EntitySourceState | None,
    observation: EntityEventObservation,
) -> EntityReconciliationResult:
    precedence = 1 if isinstance(observation, EntityDeleteObservation) else 0
    ordering_key = (observation.observed_at_unix_nano, precedence, observation.payload_hash)
    if previous is not None and ordering_key <= previous.ordering_key:
        return EntityReconciliationResult(state=previous)

    previous_ids = set(previous.element_ids if previous is not None else ())
    if isinstance(observation, EntityStateObservation):
        current_ids = {item.element.id for item in observation.contributions}
        retractions = _retractions(
            previous_ids - current_ids,
            observation.contributor_id,
            observation.observed_at_unix_nano,
        )
        mutations: tuple[GraphElementMutation, ...] = (*retractions, *observation.contributions)
        state = EntitySourceState(ordering_key=ordering_key, element_ids=tuple(sorted(current_ids)))
        return EntityReconciliationResult(state=state, mutations=mutations)

    retracted_ids = previous_ids or {observation.node_element_id}
    state = EntitySourceState(ordering_key=ordering_key, element_ids=())
    return EntityReconciliationResult(
        state=state,
        mutations=_retractions(
            retracted_ids,
            observation.contributor_id,
            observation.observed_at_unix_nano,
        ),
    )


def _entity_event(
    record: LogRecord,
    resource_attributes: Mapping[str, EventAttribute],
    scope_identity: Mapping[str, str],
    report_interval_grace_seconds: int,
    relationships: Sequence[RelationshipDefinition],
) -> EntityEventObservation | None:
    attributes = _key_values(record.attributes)
    event_name = _event_name(record.event_name, attributes)
    if event_name is None:
        return None
    if record.time_unix_nano <= 0:
        raise ValueError(f"{event_name} requires a positive LogRecord timestamp")

    entity_type = _required_string(attributes, "entity.type")
    _validate_graph_entity_type(entity_type)
    identity = _required_string_map(attributes, "entity.id")
    description = _optional_map(attributes, "entity.description")
    entity = entity_from_attributes(entity_type, {**identity, **description})
    node = GraphNode(
        id=entity.entity_id,
        type=entity.entity_type,
        attributes={**identity, **description},
    )
    observer = attributes.get(ENTITY_OBSERVER_ID)
    if observer is not None and (not isinstance(observer, str) or not observer):
        raise ValueError(f"{ENTITY_OBSERVER_ID} must be a nonempty string")
    observer_identity: object = observer or {
        "resource": resource_attributes,
        "scope": scope_identity,
    }
    contributor_id = _digest(
        {
            "source": "otel.entity.event",
            "observer": observer_identity,
            "entity_type": entity.entity_type,
            "entity_id": entity.entity_id,
        }
    )
    source_key = contributor_id
    event_hash = _digest(
        {
            "event_name": event_name,
            "timestamp": record.time_unix_nano,
            "attributes": attributes,
        }
    )
    if event_name == ENTITY_DELETE_EVENT:
        return EntityDeleteObservation(
            source_key=source_key,
            contributor_id=contributor_id,
            node_element_id=node.id,
            observed_at_unix_nano=record.time_unix_nano,
            payload_hash=event_hash,
        )

    report_interval = _report_interval(attributes)
    ttl_seconds = report_interval + report_interval_grace_seconds if report_interval > 0 else None
    elements: list[GraphElement] = [node]
    elements.extend(_relationship_edges(entity, attributes, relationships))
    contributions = tuple(
        GraphContribution(
            contributor_id=contributor_id,
            observed_at_unix_nano=record.time_unix_nano,
            element=element,
            ttl_seconds=ttl_seconds,
        )
        for element in sorted(elements, key=lambda item: item.id)
    )
    return EntityStateObservation(
        source_key=source_key,
        contributor_id=contributor_id,
        node_element_id=node.id,
        observed_at_unix_nano=record.time_unix_nano,
        payload_hash=event_hash,
        contributions=contributions,
    )


def _relationship_edges(
    source: SemanticEntity,
    attributes: Mapping[str, EventAttribute],
    relationships: Sequence[RelationshipDefinition],
) -> list[GraphEdge]:
    raw_relationships = attributes.get("entity.relationships", [])
    if not isinstance(raw_relationships, list):
        raise ValueError("entity.relationships must be an array")
    edges: list[GraphEdge] = []
    for index, item in enumerate(raw_relationships):
        if not isinstance(item, dict):
            raise ValueError(f"entity.relationships[{index}] must be a map")
        relationship_type = _required_string(item, "relationship.type")
        target_type = _required_string(item, "entity.type")
        target_identity = _required_string_map(item, "entity.id")
        target = entity_from_attributes(target_type, target_identity)
        if not relationship_allows(
            relationships,
            source.entity_type,
            target.entity_type,
            relationship_type,
        ):
            raise ValueError(
                "semantic registry does not allow relationship "
                f"{source.entity_type!r} -[{relationship_type!r}]-> {target.entity_type!r}"
            )
        edges.append(
            GraphEdge(
                id=edge_id(source.entity_id, relationship_type, target.entity_id),
                type=relationship_type,
                source_id=source.entity_id,
                target_id=target.entity_id,
            )
        )
    return edges


def _validate_graph_entity_type(
    entity_type: str,
) -> None:
    if entity_type not in _service_graph_entity_types():
        raise ValueError(
            f"semantic entity type {entity_type!r} is not part of the generated service graph topology"
        )


@cache
def _service_graph_entity_types() -> frozenset[str]:
    return frozenset(
        item
        for relationship in service_graph_relationships()
        for item in (relationship.source_entity, relationship.target_entity)
    )


def _event_name(raw_event_name: str, attributes: Mapping[str, EventAttribute]) -> EntityEventName | None:
    candidate = raw_event_name
    if not candidate:
        legacy = attributes.get("otel.entity.event.type")
        if legacy in {"entity_state", "entity.state"}:
            candidate = ENTITY_STATE_EVENT
        elif legacy in {"entity_delete", "entity_deleted", "entity.delete"}:
            candidate = ENTITY_DELETE_EVENT
    match candidate:
        case "entity.state":
            return ENTITY_STATE_EVENT
        case "entity.delete":
            return ENTITY_DELETE_EVENT
        case _:
            return None


def _report_interval(attributes: Mapping[str, EventAttribute]) -> int:
    value = attributes.get("entity.report.interval", 0)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError("entity.report.interval must be a nonnegative integer")
    return value


def _required_string(attributes: Mapping[str, EventAttribute], key: str) -> str:
    value = attributes.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"{key} must be a nonempty string")
    return value


def _required_string_map(attributes: Mapping[str, EventAttribute], key: str) -> dict[str, str]:
    value = attributes.get(key)
    if not isinstance(value, dict) or not value:
        raise ValueError(f"{key} must be a nonempty map")
    result: dict[str, str] = {}
    for name, item in value.items():
        if not isinstance(item, str):
            raise ValueError(f"{key} keys and values must be strings")
        result[name] = item
    return result


def _optional_map(attributes: Mapping[str, EventAttribute], key: str) -> dict[str, EventAttribute]:
    value = attributes.get(key, {})
    if not isinstance(value, dict):
        raise ValueError(f"{key} must be a map")
    return cast(dict[str, EventAttribute], value)


def _key_values(items: Iterable[KeyValue]) -> dict[str, EventAttribute]:
    values: dict[str, EventAttribute] = {}
    for item in items:
        if item.key in values:
            raise ValueError(f"duplicate OTLP attribute {item.key!r}")
        values[item.key] = _any_value(item.value)
    return values


def _any_value(value: AnyValue) -> EventAttribute:
    match value.WhichOneof("value"):
        case "string_value":
            return value.string_value
        case "bool_value":
            return value.bool_value
        case "int_value":
            return value.int_value
        case "double_value":
            return value.double_value
        case "bytes_value":
            return base64.b64encode(value.bytes_value).decode("ascii")
        case "array_value":
            return [_any_value(item) for item in value.array_value.values]
        case "kvlist_value":
            return _key_values(value.kvlist_value.values)
        case _:
            return None


def _retractions(
    element_ids: Iterable[str],
    contributor_id: str,
    observed_at_unix_nano: int,
) -> tuple[GraphContributionRetraction, ...]:
    return tuple(
        GraphContributionRetraction(
            contributor_id=contributor_id,
            element_id=element_id,
            observed_at_unix_nano=observed_at_unix_nano,
        )
        for element_id in sorted(element_ids)
    )


def _digest(value: object) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()
