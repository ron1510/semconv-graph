"""Extract graph-element contributions from Collector service-graph metrics."""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Iterator, Mapping, Sequence
from typing import Final, Literal, cast

from pydantic import ValidationError

from extended_otel_semconv import AppEndpoint, entities_from_attributes
from extended_otel_semconv.entities import SemanticEntity, quoted_entity_id
from extended_otel_semconv.relationships import RelationshipDefinition, service_graph_relationships
from otel_servicegraph_diff.engine.elements import (
    GRAPH_REQUEST_FAILED_TOTAL,
    GRAPH_REQUEST_TOTAL,
    GraphContribution,
    GraphEdge,
    GraphElement,
    GraphNode,
    edge_id,
)
from otel_servicegraph_diff.engine.relationships import relationship_allows, relationship_edges
from otel_servicegraph_diff.ingest.metrics import (
    ROOT_SPAN_DISCOVERY_CALLS,
    IngestRejection,
    MetricPoint,
    MetricValue,
    ServiceGraphMetricName,
    ingest_rejection,
    iter_otlp_json_metric_points,
)

type TelemetryScalar = str | bool | int | float
type DimensionMap = dict[str, TelemetryScalar]
type ServiceGraphSide = Literal["client", "server"]
type DependencyEdgeType = Literal["calls", "publishes_to", "queries"]
type IngestResult = GraphContribution | IngestRejection
type ElementContributionData = tuple[GraphElement, dict[str, MetricValue]]

_GRAPH_ENTITY_TYPES: Final = frozenset(
    entity_type
    for relationship in service_graph_relationships()
    for entity_type in (relationship.source_entity, relationship.target_entity)
)


def iter_otlp_json_contributions(payload: str) -> Iterator[IngestResult]:
    for parsed in iter_otlp_json_metric_points(payload):
        match parsed:
            case IngestRejection():
                yield parsed
            case MetricPoint(value=0):
                continue
            case MetricPoint(name="semconv.graph.discovery.calls"):
                try:
                    yield from contributions_from_discovery_datapoint(
                        attributes=parsed.attributes,
                        value=parsed.value,
                        observed_at_unix_nano=parsed.observed_at_unix_nano,
                    )
                except (TypeError, ValidationError, ValueError) as exc:
                    yield ingest_rejection("invalid_discovery_datapoint", exc)
            case MetricPoint():
                try:
                    yield from contributions_from_servicegraph_datapoint(
                        metric_name=cast(ServiceGraphMetricName, parsed.name),
                        attributes=parsed.attributes,
                        value=parsed.value,
                        observed_at_unix_nano=parsed.observed_at_unix_nano,
                    )
                except (TypeError, ValidationError, ValueError) as exc:
                    yield ingest_rejection("invalid_servicegraph_datapoint", exc)


def contributions_from_servicegraph_datapoint(
    metric_name: ServiceGraphMetricName,
    attributes: Mapping[str, TelemetryScalar],
    value: MetricValue,
    observed_at_unix_nano: int,
) -> tuple[GraphContribution, ...]:
    _validate_positive_metric_value(value)
    client_name = _required_string_attribute(attributes, "client")
    server_name = _required_string_attribute(attributes, "server")
    dimensions: DimensionMap = {
        key: attribute_value
        for key, attribute_value in attributes.items()
        if key not in {"client", "server", "connection_type"}
    }
    connection_type = _dependency_edge_type(attributes)
    contributor_id = _contributor_id(client_name, server_name, connection_type, dimensions)
    client_entities = _entities_from_side(attributes, "client", client_name)
    server_entities = _entities_from_side(attributes, "server", server_name)
    elements = _elements_from_datapoint(
        metric_name,
        value,
        client_name,
        server_name,
        connection_type,
        client_entities,
        server_entities,
        service_graph_relationships(),
    )
    return tuple(
        GraphContribution(
            contributor_id=contributor_id,
            observed_at_unix_nano=observed_at_unix_nano,
            element=element,
            metric_deltas=metric_deltas,
        )
        for _, (element, metric_deltas) in sorted(elements.items())
    )


def contributions_from_discovery_datapoint(
    attributes: Mapping[str, TelemetryScalar],
    value: MetricValue,
    observed_at_unix_nano: int,
) -> tuple[GraphContribution, ...]:
    _validate_positive_metric_value(value)
    entities = entities_from_attributes(attributes)
    if attributes.get("span.kind") != "SPAN_KIND_SERVER":
        entities = [entity for entity in entities if not isinstance(entity, AppEndpoint)]
    return tuple(
        _node_contribution(entity, observed_at_unix_nano)
        for entity in sorted(entities, key=lambda item: item.entity_id)
        if entity.entity_type in _GRAPH_ENTITY_TYPES
    )


def _entities_from_side(
    attributes: Mapping[str, TelemetryScalar],
    side: ServiceGraphSide,
    service_name: str,
) -> tuple[SemanticEntity, ...]:
    prefix = f"{side}_"
    side_attributes: dict[str, object] = {
        key.removeprefix(prefix): value
        for key, value in attributes.items()
        if key.startswith(prefix)
    }
    side_attributes.setdefault("service.name", service_name)
    entities = entities_from_attributes(side_attributes)
    if side == "server":
        return tuple(entities)
    return tuple(entity for entity in entities if not isinstance(entity, AppEndpoint))


def _elements_from_datapoint(
    metric_name: ServiceGraphMetricName,
    metric_value: MetricValue,
    client_name: str,
    server_name: str,
    dependency_type: DependencyEdgeType,
    client_entities: Sequence[SemanticEntity],
    server_entities: Sequence[SemanticEntity],
    relationships: Sequence[RelationshipDefinition],
) -> dict[str, ElementContributionData]:
    elements: dict[str, ElementContributionData] = {}
    all_entities = (*client_entities, *server_entities)
    for entity in all_entities:
        _add_element(
            elements,
            GraphNode(
                id=entity.entity_id,
                type=entity.entity_type,
                attributes=entity.semantic_attributes(),
            ),
        )
    for entities in (client_entities, server_entities):
        for relationship in relationship_edges(entities, relationships):
            _add_element(
                elements,
                _graph_edge(relationship.source, relationship.target, relationship.type),
            )

    source = quoted_entity_id("service", client_name)
    target = quoted_entity_id("service", server_name)
    if source != target and relationship_allows(
        relationships,
        "service",
        "service",
        dependency_type,
    ):
        dependency = _graph_edge(source, target, dependency_type)
        _add_element(elements, dependency, _dependency_metrics(metric_name, metric_value))
    return elements


def _add_element(
    elements: dict[str, ElementContributionData],
    element: GraphElement,
    metric_deltas: dict[str, MetricValue] | None = None,
) -> None:
    existing = elements.get(element.id)
    if existing is None:
        elements[element.id] = element, metric_deltas or {}
        return
    previous, previous_metrics = existing
    if _element_identity(previous) != _element_identity(element):
        raise ValueError(f"conflicting graph elements share ID {element.id!r}")
    attributes = _merge_attributes(previous.attributes, element.attributes)
    match previous:
        case GraphNode():
            merged: GraphElement = previous.model_copy(update={"attributes": attributes})
        case GraphEdge():
            merged = previous.model_copy(update={"attributes": attributes})
    elements[element.id] = merged, {**previous_metrics, **(metric_deltas or {})}


def _element_identity(element: GraphElement) -> tuple[str, ...]:
    match element:
        case GraphNode():
            return element.kind, element.type, element.id
        case GraphEdge():
            return element.kind, element.type, element.source_id, element.target_id


def _merge_attributes(left: Mapping[str, object], right: Mapping[str, object]) -> dict[str, object]:
    merged = dict(left)
    for name, value in right.items():
        current = merged.get(name)
        if current is None or current == value:
            merged[name] = value
        else:
            merged[name] = min((current, value), key=_canonical_json)
    return merged


def _graph_edge(source: str, target: str, relationship_type: str) -> GraphEdge:
    return GraphEdge(
        id=edge_id(source, relationship_type, target),
        type=relationship_type,
        source_id=source,
        target_id=target,
    )


def _dependency_edge_type(attributes: Mapping[str, TelemetryScalar]) -> DependencyEdgeType:
    match attributes.get("connection_type"):
        case "messaging_system":
            return "publishes_to"
        case "database":
            return "queries"
        case _:
            return "calls"


def _dependency_metrics(metric_name: ServiceGraphMetricName, value: MetricValue) -> dict[str, MetricValue]:
    match metric_name:
        case "traces_service_graph_request_total":
            return {GRAPH_REQUEST_TOTAL: value}
        case "traces_service_graph_request_failed_total":
            return {GRAPH_REQUEST_FAILED_TOTAL: value}


def _contributor_id(
    client: str,
    server: str,
    connection_type: DependencyEdgeType,
    dimensions: Mapping[str, TelemetryScalar],
) -> str:
    return hashlib.sha256(
        _canonical_json(
            {
                "client": client,
                "server": server,
                "connection_type": connection_type,
                "dimensions": dict(dimensions),
            }
        ).encode("utf-8")
    ).hexdigest()


def _canonical_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _validate_positive_metric_value(value: MetricValue) -> None:
    if isinstance(value, bool):
        raise TypeError("graph metric value must be an integer or float")
    if not math.isfinite(value) or value <= 0:
        raise ValueError("graph metric value must be finite and greater than zero")


def _required_string_attribute(attributes: Mapping[str, TelemetryScalar], key: str) -> str:
    value = attributes.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"servicegraph datapoint is missing nonempty {key!r}")
    return value


def _node_contribution(entity: SemanticEntity, observed_at_unix_nano: int) -> GraphContribution:
    node = GraphNode(
        id=entity.entity_id,
        type=entity.entity_type,
        attributes=entity.semantic_attributes(),
    )
    serialized_attributes = cast(dict[str, object], node.model_dump(mode="json")["attributes"])
    fingerprint = _canonical_json(
        {
            "source": ROOT_SPAN_DISCOVERY_CALLS,
            "element_id": node.id,
            "attributes": serialized_attributes,
        }
    )
    return GraphContribution(
        contributor_id=f"spanmetrics:{hashlib.sha256(fingerprint.encode('utf-8')).hexdigest()}",
        observed_at_unix_nano=observed_at_unix_nano,
        element=node,
    )
