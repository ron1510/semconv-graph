# pyright: reportArgumentType=false, reportUnknownArgumentType=false, reportUnknownMemberType=false, reportUnknownVariableType=false

from __future__ import annotations

import time

import pytest
from gremlin_python.driver.protocol import GremlinServerError

from extended_otel_semconv import Service, ServiceCallsServiceEdge
from extended_otel_semconv.edges import edge_id as semantic_edge_id
from extended_otel_semconv.gremlin import UnsupportedSemanticTraversalError
from tools.local_demo.environment import DemoEnvironment, wait_for


@pytest.mark.e2e
def test_schema2_events_are_projected_and_traversable(e2e_environment: DemoEnvironment) -> None:
    observed_at = time.time_ns()
    storefront_id = "service:storefront"
    checkout_id = "service:checkout-api"
    edge_id = semantic_edge_id(storefront_id, "calls", checkout_id)
    events = (
        _upsert(storefront_id, _service(storefront_id, "storefront", "1.0"), observed_at, "storefront-v1"),
        _upsert(checkout_id, _service(checkout_id, "checkout-api", "2.4"), observed_at, "checkout-v1"),
        _upsert(
            edge_id,
            {
                "id": edge_id,
                "kind": "edge",
                "type": "calls",
                "source_id": storefront_id,
                "target_id": checkout_id,
                "attributes": {},
                "metrics": {
                    "service_graph.request.total": 12.0,
                    "service_graph.request.failed.total": 1.0,
                },
            },
            observed_at,
            "edge-v1",
        ),
    )
    e2e_environment.produce_events(events)

    assert wait_for("two service vertices", 60, lambda: _vertex_count(e2e_environment, "service") == 2)
    assert wait_for("calls edge", 60, lambda: _edge_count(e2e_environment, "calls") == 1)
    with e2e_environment.graph() as graph:
        checkout_versions = (
            graph.V().has_label("service").has("service_name", "checkout-api").values("service_version").to_list()
        )
        assert checkout_versions == ["2.4"]
        assert graph.V().has("service_name", "storefront").out("calls").values("service_name").to_list() == [
            "checkout-api"
        ]
        assert graph.V().has("service_name", "checkout-api").in_("calls").values("service_name").to_list() == [
            "storefront"
        ]
        assert graph.E().has_label("calls").values("service_graph_request_total").to_list() == [12.0]

    with e2e_environment.semantic_client() as client:
        services = client.query(lambda g: g.V().has_label("service").order().by("service_name"))
        calls = client.query(lambda g: g.E().has_label("calls"))
        dependencies = client.query(lambda g: g.V().has("service_name", "storefront").out("calls"))

        assert all(isinstance(service, Service) for service in services)
        assert [service.service_name for service in services if isinstance(service, Service)] == [
            "checkout-api",
            "storefront",
        ]
        assert len(calls) == 1
        assert isinstance(calls[0], ServiceCallsServiceEdge)
        assert calls[0].metrics["service_graph.request.total"] == 12.0
        assert len(dependencies) == 1
        assert isinstance(dependencies[0], Service)
        assert dependencies[0].service_name == "checkout-api"

        with pytest.raises(UnsupportedSemanticTraversalError, match="values"):
            client.query(lambda g: g.V().values("service_name"))
        with pytest.raises(UnsupportedSemanticTraversalError, match="count"):
            client.query(lambda g: g.V().count())
        with pytest.raises(UnsupportedSemanticTraversalError, match="project"):
            client.query(lambda g: g.V().project("name").by("service_name"))

    replacement = _upsert(checkout_id, _service(checkout_id, "checkout-api", "2.5"), observed_at + 1, "checkout-v2")
    e2e_environment.produce_events((replacement, replacement))
    assert wait_for(
        "idempotent vertex replacement",
        60,
        lambda: _service_versions(e2e_environment, "checkout-api") == ["2.5"],
    )
    assert wait_for("committed offsets", 30, lambda: e2e_environment.committed_offset() >= 5)

    e2e_environment.restart_projection()
    assert _service_versions(e2e_environment, "checkout-api") == ["2.5"]
    assert _edge_count(e2e_environment, "calls") == 1

    with e2e_environment.graph() as graph, pytest.raises(GremlinServerError):
        graph.add_v("service").property("service_name", "forbidden").iterate()

    e2e_environment.produce_events(
        (
            _delete(edge_id, observed_at + 2, "edge-delete"),
            _delete(checkout_id, observed_at + 2, "checkout-delete"),
            _delete(storefront_id, observed_at + 2, "storefront-delete"),
        )
    )
    assert wait_for("stale lifecycle deletes", 60, lambda: _all_counts(e2e_environment) == (0, 0))
    assert wait_for("delete offsets", 30, lambda: e2e_environment.committed_offset() >= 8)


def _service(element_id: str, name: str, version: str) -> dict[str, object]:
    return {
        "id": element_id,
        "kind": "node",
        "type": "service",
        "attributes": {"service.name": name, "service.version": version},
    }


def _upsert(
    element_id: str,
    element: dict[str, object],
    observed_at_unix_nano: int,
    event_id: str,
) -> dict[str, object]:
    return {
        "schema_version": "2.0",
        "event_id": event_id,
        "event_type": "graph_element_state_changed",
        "operation": "upsert",
        "element_id": element_id,
        "payload_hash": f"hash-{event_id}",
        "observed_at_unix_nano": observed_at_unix_nano,
        "emitted_at_unix_ms": observed_at_unix_nano // 1_000_000,
        "element": element,
    }


def _delete(element_id: str, observed_at_unix_nano: int, event_id: str) -> dict[str, object]:
    return {
        "schema_version": "2.0",
        "event_id": event_id,
        "event_type": "graph_element_state_changed",
        "operation": "delete",
        "element_id": element_id,
        "payload_hash": None,
        "observed_at_unix_nano": observed_at_unix_nano,
        "emitted_at_unix_ms": observed_at_unix_nano // 1_000_000,
        "element": None,
    }


def _vertex_count(environment: DemoEnvironment, label: str) -> int:
    with environment.graph() as graph:
        return int(graph.V().has_label(label).count().next())


def _edge_count(environment: DemoEnvironment, label: str) -> int:
    with environment.graph() as graph:
        return int(graph.E().has_label(label).count().next())


def _service_versions(environment: DemoEnvironment, name: str) -> list[str]:
    with environment.graph() as graph:
        return [str(value) for value in graph.V().has("service_name", name).values("service_version").to_list()]


def _all_counts(environment: DemoEnvironment) -> tuple[int, int]:
    with environment.graph() as graph:
        return int(graph.V().count().next()), int(graph.E().count().next())
