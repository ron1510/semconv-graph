from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from typing import cast
from unittest.mock import Mock

from kafka.structs import TopicPartition

from otel_servicegraph_diff.engine.elements import (
    GraphContribution,
    GraphElementEvent,
    GraphElementState,
    apply_contribution,
    retract_contribution,
)
from otel_servicegraph_diff.ingest.entity_events import (
    EntityDeleteObservation,
    EntityEventObservation,
    EntitySourceState,
    EntityStateObservation,
    iter_otlp_json_entity_events,
    reconcile_entity_event,
)
from servicegraph_indexer.indexer import (
    ConsumerBoundary,
    KafkaRecord,
    WriterBoundary,
    element_key,
    event_to_document,
    project_poll,
)
from servicegraph_indexer.schema import load_graph_schema

DEFAULT_TTL_SECONDS = 300
PROCESSING_TIME_MS = 10_000


class _Record:
    def __init__(self, offset: int, value: bytes) -> None:
        self.offset = offset
        self.value = value


class _Writer:
    def __init__(self) -> None:
        self.replacements: list[tuple[str, Sequence[Mapping[str, object]]]] = []
        self.deletions: list[tuple[str, Sequence[str]]] = []

    def replace_many(self, collection: str, documents: Sequence[Mapping[str, object]]) -> None:
        self.replacements.append((collection, documents))

    def delete_many(self, collection: str, keys: Sequence[str]) -> None:
        self.deletions.append((collection, keys))


def test_otlp_entity_state_update_duplicate_and_delete_reach_indexer_contract() -> None:
    source_state: EntitySourceState | None = None
    element_states: dict[str, GraphElementState] = {}

    initial = _state(
        timestamp=1_000_000_000,
        version="1.0.0",
        report_interval=60,
        relationships=[_calls("payments")],
    )
    source_state, initial_events = _reduce(source_state, element_states, initial)

    assert len(initial_events) == 2
    assert all(event.schema_version == "2.0" for event in initial_events)
    assert all(event.operation == "upsert" for event in initial_events)
    assert all(contribution.ttl_seconds == 75 for contribution in initial.contributions)
    assert all(
        snapshot.event_expires_at_unix_nano == 76_000_000_000
        for state in element_states.values()
        for snapshot in state.contributors.values()
    )

    documents = {
        collection: document
        for event in initial_events
        for collection, document in [_index_document(event)]
    }
    assert set(documents) == {"service", "calls"}
    assert documents["service"]["service_name"] == "checkout"
    assert documents["service"]["service_version"] == "1.0.0"
    assert documents["calls"]["_from"] == f"service/{element_key('service:checkout')}"
    assert documents["calls"]["_to"] == f"service/{element_key('service:payments')}"

    update = _state(
        timestamp=2_000_000_000,
        version="2.0.0",
        report_interval=60,
        relationships=[_calls("payments")],
    )
    source_state, update_events = _reduce(source_state, element_states, update)

    assert len(update_events) == 1
    update_collection, update_document = _index_document(update_events[0])
    assert update_collection == "service"
    assert update_document["service_version"] == "2.0.0"

    source_state_after_duplicate, duplicate_events = _reduce(source_state, element_states, update)
    assert source_state_after_duplicate is source_state
    assert duplicate_events == ()

    delete = _delete(timestamp=3_000_000_000)
    _, delete_events = _reduce(source_state, element_states, delete)

    assert element_states == {}
    assert len(delete_events) == 2
    assert all(event.schema_version == "2.0" for event in delete_events)
    assert all(event.operation == "delete" for event in delete_events)
    _assert_indexer_routes_deletes(delete_events)


def test_entity_report_interval_adds_configured_grace_to_contribution_ttl() -> None:
    positive = _state(report_interval=30)

    assert {item.ttl_seconds for item in positive.contributions} == {45}


def test_parser_accepts_standard_and_legacy_entity_event_names() -> None:
    standard = _parse(_payload(event_name="entity.state", report_interval=1))
    legacy_state = _parse(
        _payload(event_name="", legacy_event_type="entity.state", report_interval=1)
    )
    legacy_delete = _parse(_payload(event_name="", legacy_event_type="entity.delete"))

    assert isinstance(standard, EntityStateObservation)
    assert isinstance(legacy_state, EntityStateObservation)
    assert isinstance(legacy_delete, EntityDeleteObservation)


def _reduce(
    source_state: EntitySourceState | None,
    element_states: dict[str, GraphElementState],
    observation: EntityEventObservation,
) -> tuple[EntitySourceState, tuple[GraphElementEvent, ...]]:
    reconciliation = reconcile_entity_event(source_state, observation)
    events: list[GraphElementEvent] = []
    for mutation in reconciliation.mutations:
        if isinstance(mutation, GraphContribution):
            element_id = mutation.element.id
            result = apply_contribution(
                element_states.get(element_id),
                mutation,
                ttl_seconds=DEFAULT_TTL_SECONDS,
                processing_time_unix_ms=PROCESSING_TIME_MS,
                emitted_at_unix_ms=PROCESSING_TIME_MS,
            )
        else:
            element_id = mutation.element_id
            result = retract_contribution(
                element_states.get(element_id),
                mutation,
                emitted_at_unix_ms=PROCESSING_TIME_MS,
            )
        if result.state is None:
            element_states.pop(element_id, None)
        else:
            element_states[element_id] = result.state
        if result.event is not None:
            events.append(result.event)
    return reconciliation.state, tuple(events)


def _index_document(event: GraphElementEvent) -> tuple[str, dict[str, object]]:
    document = cast(Mapping[str, object], event.model_dump(mode="json"))
    return event_to_document(document, load_graph_schema())


def _assert_indexer_routes_deletes(events: tuple[GraphElementEvent, ...]) -> None:
    partition = TopicPartition("graph.elements.events", 0)
    records = {
        partition: [
            _Record(index, event.model_dump_json().encode())
            for index, event in enumerate(events)
        ]
    }
    consumer = Mock(spec=ConsumerBoundary)
    writer = _Writer()

    processed = project_poll(
        cast(Mapping[TopicPartition, Sequence[KafkaRecord]], records),
        consumer,
        cast(WriterBoundary, writer),
        load_graph_schema(),
    )

    assert processed == 2
    assert ("service", [element_key("service:checkout")]) in writer.deletions
    edge_deletes = [item for item in writer.deletions if item[0] != "service"]
    assert len(edge_deletes) == len(load_graph_schema().edge_collections)
    consumer.commit.assert_called_once()


def _state(
    *,
    timestamp: int = 1_000_000_000,
    version: str | None = None,
    report_interval: int | None = None,
    relationships: list[dict[str, object]] | None = None,
) -> EntityStateObservation:
    item = _parse(
        _payload(
            event_name="entity.state",
            timestamp=timestamp,
            version=version,
            report_interval=report_interval,
            relationships=relationships,
        )
    )
    assert isinstance(item, EntityStateObservation)
    return item


def _delete(*, timestamp: int) -> EntityDeleteObservation:
    item = _parse(_payload(event_name="entity.delete", timestamp=timestamp))
    assert isinstance(item, EntityDeleteObservation)
    return item


def _parse(payload: str) -> EntityEventObservation:
    parsed = tuple(iter_otlp_json_entity_events(payload, report_interval_grace_seconds=15))
    assert len(parsed) == 1
    item = parsed[0]
    assert isinstance(item, EntityStateObservation | EntityDeleteObservation)
    return item


def _payload(
    *,
    event_name: str,
    timestamp: int = 1_000_000_000,
    legacy_event_type: str | None = None,
    version: str | None = None,
    report_interval: int | None = None,
    relationships: list[dict[str, object]] | None = None,
) -> str:
    attributes = [
        _attribute("entity.type", "service"),
        _attribute("entity.id", {"service.name": "checkout"}),
    ]
    if legacy_event_type is not None:
        attributes.append(_attribute("otel.entity.event.type", legacy_event_type))
    if version is not None:
        attributes.append(_attribute("entity.description", {"service.version": version}))
    if report_interval is not None:
        attributes.append(_attribute("entity.report.interval", report_interval))
    if relationships is not None:
        attributes.append(_attribute("entity.relationships", relationships))
    return json.dumps(
        {
            "resourceLogs": [
                {
                    "resource": {"attributes": [_attribute("service.instance.id", "producer-1")]},
                    "scopeLogs": [
                        {
                            "scope": {"name": "entity-integration-test", "version": "1"},
                            "logRecords": [
                                {
                                    "timeUnixNano": str(timestamp),
                                    "eventName": event_name,
                                    "attributes": attributes,
                                }
                            ],
                        }
                    ],
                }
            ]
        }
    )


def _calls(target_service: str) -> dict[str, object]:
    return {
        "relationship.type": "calls",
        "entity.type": "service",
        "entity.id": {"service.name": target_service},
    }


def _attribute(key: str, value: object) -> dict[str, object]:
    return {"key": key, "value": _any_value(value)}


def _any_value(value: object) -> dict[str, object]:
    match value:
        case str():
            return {"stringValue": value}
        case int():
            return {"intValue": str(value)}
        case list():
            sequence = cast(list[object], value)
            return {"arrayValue": {"values": [_any_value(item) for item in sequence]}}
        case dict():
            mapping = cast(dict[object, object], value)
            return {
                "kvlistValue": {
                    "values": [_attribute(str(name), item) for name, item in mapping.items()]
                }
            }
        case _:
            raise TypeError(f"unsupported test value {value!r}")
