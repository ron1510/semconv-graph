from __future__ import annotations

import json
from typing import cast

import pytest

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
        next(item.element.id for item in observation.contributions if isinstance(item.element, GraphEdge)),
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


def test_transport_resource_and_scope_do_not_change_explicit_source_identity() -> None:
    first = _state_observation(_payload("entity.state", producer="producer-1", scope="scope-1"))
    second = _state_observation(_payload("entity.state", producer="producer-2", scope="scope-2"))

    assert first.node_element_id == second.node_element_id
    assert first.contributor_id == second.contributor_id


def test_description_cannot_override_identity() -> None:
    parsed = tuple(
        iter_otlp_json_entity_events(
            _payload(
                "entity.state",
                description={"service.name": "payments"},
                report_interval=10,
            ),
            report_interval_grace_seconds=5,
        )
    )

    assert len(parsed) == 1
    assert isinstance(parsed[0], IngestRejection)
    assert "must not redefine entity.id fields: service.name" in (parsed[0].detail or "")


@pytest.mark.parametrize(
    "identity, expected_detail",
    [
        ({}, "missing: service.name; extra: none"),
        (
            {"service.name": "checkout", "service.namespace": "production"},
            "missing: none; extra: service.namespace",
        ),
    ],
)
def test_identity_must_exactly_match_registered_shape(
    identity: dict[str, object],
    expected_detail: str,
) -> None:
    parsed = tuple(
        iter_otlp_json_entity_events(
            _payload("entity.state", identity=identity, report_interval=10),
            report_interval_grace_seconds=5,
        )
    )

    assert len(parsed) == 1
    assert isinstance(parsed[0], IngestRejection)
    assert "must exactly match the registered semantic identity shape" in (parsed[0].detail or "")
    assert expected_detail in (parsed[0].detail or "")


def test_string_identity_is_converted_to_registered_strict_scalar_type() -> None:
    observation = _state_observation(
        _payload(
            "entity.state",
            entity_type="process",
            identity={"process.pid": "123", "process.creation.time": "2026-08-23T10:00:00Z"},
            report_interval=10,
        )
    )

    node = observation.contributions[0].element
    assert node.type == "process"
    assert node.attributes["process.pid"] == 123
    assert type(node.attributes["process.pid"]) is int


def test_noncanonical_scalar_identity_is_rejected_instead_of_collapsing_ids() -> None:
    parsed = tuple(
        iter_otlp_json_entity_events(
            _payload(
                "entity.state",
                entity_type="process",
                identity={"process.pid": "00123", "process.creation.time": "2026-08-23T10:00:00Z"},
                report_interval=10,
            ),
            report_interval_grace_seconds=5,
        )
    )

    assert len(parsed) == 1
    assert isinstance(parsed[0], IngestRejection)
    assert "canonical integer string" in (parsed[0].detail or "")


@pytest.mark.parametrize("report_interval", [None, 0])
def test_absent_and_zero_report_intervals_do_not_expire(report_interval: int | None) -> None:
    observation = _state_observation(_payload("entity.state", report_interval=report_interval))

    assert all(contribution.ttl_seconds == 0 for contribution in observation.contributions)


def test_unseen_older_state_recreates_after_delete_without_replaying_duplicates() -> None:
    delete = _delete_observation(_payload("entity.delete", timestamp=2_000_000_000))
    deleted = reconcile_entity_event(None, delete)
    older_state = _state_observation(_payload("entity.state", timestamp=1_000_000_000, report_interval=10))

    restored = reconcile_entity_event(deleted.state, older_state)
    replayed_state = reconcile_entity_event(restored.state, older_state)
    replayed_delete = reconcile_entity_event(restored.state, delete)

    assert len(restored.mutations) == 1
    assert isinstance(restored.mutations[0], GraphContribution)
    assert replayed_state.mutations == ()
    assert replayed_delete.mutations == ()


def test_older_state_recreates_after_delete_even_when_newer_state_was_seen_before_delete() -> None:
    initial = reconcile_entity_event(
        None,
        _state_observation(_payload("entity.state", timestamp=1_500_000_000, report_interval=10)),
    )
    deleted = reconcile_entity_event(
        initial.state,
        _delete_observation(_payload("entity.delete", timestamp=2_000_000_000)),
    )
    late_state = _state_observation(_payload("entity.state", timestamp=1_000_000_000, report_interval=10))

    restored = reconcile_entity_event(deleted.state, late_state)

    assert len(restored.mutations) == 1
    assert isinstance(restored.mutations[0], GraphContribution)


def test_invalid_supported_event_is_rejected_but_unrelated_log_is_ignored() -> None:
    invalid = json.loads(_payload("entity.state"))
    invalid["resourceLogs"][0]["scopeLogs"][0]["logRecords"][0]["attributes"] = []
    rejected = tuple(iter_otlp_json_entity_events(json.dumps(invalid), report_interval_grace_seconds=0))
    ignored = tuple(iter_otlp_json_entity_events(_payload("other.event"), report_interval_grace_seconds=0))

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
    assert parsed[0].reason == "unsupported_otel_entity_event_graph_contract"
    assert "accepts only registered service_graph relationships" in (parsed[0].detail or "")
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
    assert parsed[0].reason == "unsupported_otel_entity_event_graph_contract"
    assert "accepts only registered service_graph topology entity types" in (parsed[0].detail or "")


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
    producer: str = "producer-1",
    scope: str = "entity-test",
    entity_type: str = "service",
    identity: dict[str, object] | None = None,
    description: dict[str, object] | None = None,
    relationships: list[dict[str, object]] | None = None,
    report_interval: int | None = None,
) -> str:
    attributes = [
        _attribute("entity.type", entity_type),
        _attribute("entity.id", identity if identity is not None else {"service.name": "checkout"}),
    ]
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
                        "attributes": [_attribute("service.instance.id", producer)],
                    },
                    "scopeLogs": [
                        {
                            "scope": {"name": scope, "version": "1"},
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
            return {"kvlistValue": {"values": [_attribute(str(name), item) for name, item in mapping.items()]}}
        case _:
            raise TypeError(f"unsupported test value {value!r}")
