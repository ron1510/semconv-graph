from __future__ import annotations

import json
from typing import cast

from otel_servicegraph_diff.engine.elements import GraphContribution, GraphContributionRetraction, GraphEdge
from otel_servicegraph_diff.ingest.entity_events import (
    EntityDeleteObservation,
    EntityStateObservation,
    iter_otlp_json_entity_events,
    reconcile_entity_event,
)
from otel_servicegraph_diff.ingest.metrics import IngestRejection


def test_entity_state_becomes_node_and_registry_relationship_contributions() -> None:
    parsed = tuple(
        iter_otlp_json_entity_events(
            _payload(
                "entity.state",
                description={"service.version": "2.4.0"},
                relationships=[
                    {
                        "relationship.type": "calls",
                        "entity.type": "service",
                        "entity.id": {"service.name": "payments"},
                    }
                ],
                report_interval=60,
            ),
            report_interval_grace_seconds=15,
        )
    )

    assert len(parsed) == 1
    observation = parsed[0]
    assert isinstance(observation, EntityStateObservation)
    assert {item.element.id for item in observation.contributions} == {
        "service:checkout",
        next(
            item.element.id
            for item in observation.contributions
            if isinstance(item.element, GraphEdge)
        ),
    }
    node = next(item for item in observation.contributions if item.element.id == "service:checkout")
    assert node.element.attributes == {
        "service.name": "checkout",
        "service.version": "2.4.0",
    }
    assert all(item.ttl_seconds == 75 for item in observation.contributions)


def test_complete_state_retracts_omitted_relationship_before_upserts() -> None:
    first = _state_observation(
        _payload(
            "entity.state",
            relationships=[
                {
                    "relationship.type": "calls",
                    "entity.type": "service",
                    "entity.id": {"service.name": "payments"},
                }
            ],
        )
    )
    initial = reconcile_entity_event(None, first)
    second = _state_observation(_payload("entity.state", timestamp=2_000_000_000))

    updated = reconcile_entity_event(initial.state, second)

    assert isinstance(updated.mutations[0], GraphContributionRetraction)
    assert updated.mutations[0].element_id.startswith("edge:")
    assert isinstance(updated.mutations[-1], GraphContribution)
    assert updated.mutations[-1].element.id == "service:checkout"


def test_delete_retracts_every_contribution_and_duplicate_is_ignored() -> None:
    state_event = _state_observation(
        _payload(
            "entity.state",
            relationships=[
                {
                    "relationship.type": "calls",
                    "entity.type": "service",
                    "entity.id": {"service.name": "payments"},
                }
            ],
        )
    )
    initial = reconcile_entity_event(None, state_event)
    delete = _delete_observation(_payload("entity.delete", timestamp=2_000_000_000))

    deleted = reconcile_entity_event(initial.state, delete)
    replayed = reconcile_entity_event(deleted.state, delete)

    assert len(deleted.mutations) == 2
    assert all(isinstance(item, GraphContributionRetraction) for item in deleted.mutations)
    assert replayed.mutations == ()


def test_multiple_observers_get_independent_contributor_ids() -> None:
    first = _state_observation(_payload("entity.state", observer_id="collector-a"))
    second = _state_observation(_payload("entity.state", observer_id="collector-b"))

    assert first.node_element_id == second.node_element_id
    assert first.contributor_id != second.contributor_id


def test_invalid_supported_event_is_rejected_but_unrelated_log_is_ignored() -> None:
    invalid = json.loads(_payload("entity.state"))
    invalid["resourceLogs"][0]["scopeLogs"][0]["logRecords"][0]["attributes"] = []
    rejected = tuple(
        iter_otlp_json_entity_events(json.dumps(invalid), report_interval_grace_seconds=0)
    )
    ignored = tuple(
        iter_otlp_json_entity_events(_payload("other.event"), report_interval_grace_seconds=0)
    )

    assert len(rejected) == 1
    assert isinstance(rejected[0], IngestRejection)
    assert ignored == ()


def test_unknown_relationship_is_rejected_as_complete_snapshot() -> None:
    parsed = tuple(
        iter_otlp_json_entity_events(
            _payload(
                "entity.state",
                relationships=[
                    {
                        "relationship.type": "depends_on",
                        "entity.type": "service",
                        "entity.id": {"service.name": "payments"},
                    }
                ],
            ),
            report_interval_grace_seconds=0,
        )
    )

    assert len(parsed) == 1
    assert isinstance(parsed[0], IngestRejection)
    assert "does not allow relationship" in (parsed[0].detail or "")


def test_known_otel_entity_outside_generated_graph_topology_is_rejected() -> None:
    document = cast(dict[str, object], json.loads(_payload("entity.state")))
    resource_logs = cast(list[dict[str, object]], document["resourceLogs"])
    scope_logs = cast(list[dict[str, object]], resource_logs[0]["scopeLogs"])
    log_records = cast(list[dict[str, object]], scope_logs[0]["logRecords"])
    attributes = cast(list[dict[str, object]], log_records[0]["attributes"])
    attributes[0] = _attribute("entity.type", "host")
    attributes[1] = _attribute("entity.id", {"host.id": "host-1"})

    parsed = tuple(
        iter_otlp_json_entity_events(
            json.dumps(document),
            report_interval_grace_seconds=0,
        )
    )

    assert len(parsed) == 1
    assert isinstance(parsed[0], IngestRejection)
    assert "not part of the generated service graph topology" in (parsed[0].detail or "")


def _state_observation(payload: str) -> EntityStateObservation:
    item = next(iter_otlp_json_entity_events(payload, report_interval_grace_seconds=5))
    assert isinstance(item, EntityStateObservation)
    return item


def _delete_observation(payload: str) -> EntityDeleteObservation:
    item = next(iter_otlp_json_entity_events(payload, report_interval_grace_seconds=5))
    assert isinstance(item, EntityDeleteObservation)
    return item


def _payload(
    event_name: str,
    *,
    timestamp: int = 1_000_000_000,
    observer_id: str | None = "collector-a",
    description: dict[str, object] | None = None,
    relationships: list[dict[str, object]] | None = None,
    report_interval: int | None = None,
) -> str:
    attributes = [
        _attribute("entity.type", "service"),
        _attribute("entity.id", {"service.name": "checkout"}),
    ]
    if observer_id is not None:
        attributes.append(_attribute("otel.entity.observer.id", observer_id))
    if description is not None:
        attributes.append(_attribute("entity.description", description))
    if relationships is not None:
        attributes.append(_attribute("entity.relationships", relationships))
    if report_interval is not None:
        attributes.append(_attribute("entity.report.interval", report_interval))
    return json.dumps(
        {
            "resourceLogs": [
                {
                    "resource": {
                        "attributes": [_attribute("service.instance.id", "producer-1")],
                    },
                    "scopeLogs": [
                        {
                            "scope": {"name": "entity-test", "version": "1"},
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


def _attribute(key: str, value: object) -> dict[str, object]:
    return {"key": key, "value": _any_value(value)}


def _any_value(value: object) -> dict[str, object]:
    match value:
        case str():
            return {"stringValue": value}
        case bool():
            return {"boolValue": value}
        case int():
            return {"intValue": str(value)}
        case float():
            return {"doubleValue": value}
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
