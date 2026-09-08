from __future__ import annotations

import random
from unittest.mock import Mock

import pytest
from opentelemetry.proto.trace.v1.trace_pb2 import Span

import servicegraph_demo.main as demo_module
from servicegraph_demo.main import (
    EDGES,
    Topology,
    build_request,
    demo_config_from_env,
    service_resource_attributes,
)


def test_demo_settings_are_cached_and_main_uses_the_factory(monkeypatch: pytest.MonkeyPatch) -> None:
    demo_config_from_env.cache_clear()
    try:
        monkeypatch.setenv("DEMO_RANDOM_SEED", "7")
        first = demo_config_from_env()
        monkeypatch.setenv("DEMO_RANDOM_SEED", "11")

        assert demo_config_from_env() is first
        assert first.random_seed == 7

        demo_config_from_env.cache_clear()
        replacement = demo_config_from_env()
        runner = Mock()
        monkeypatch.setattr(demo_module, "run", runner)
        assert demo_module.main() == 0
        runner.assert_called_once_with(replacement)
    finally:
        demo_config_from_env.cache_clear()


def test_topology_grows_then_rotates_and_prioritizes_new_edges() -> None:
    topology = Topology(EDGES, initial_edges=2, max_active_edges=3, rng=random.Random(7))

    assert set(topology.sample(2)) == set(topology.active)
    retired, introduced = topology.advance()
    assert retired is None
    assert len(topology.active) == 3
    assert topology.sample(1) == (introduced,)

    retired, introduced = topology.advance()
    assert retired is not None
    assert retired not in topology.active
    assert introduced in topology.active
    assert len(topology.active) == 3

    _, next_introduced = topology.advance()
    assert next_introduced != retired


def test_request_contains_colocated_client_server_span_pairs() -> None:
    request = build_request(
        (EDGES[0], EDGES[1]),
        namespace="shop",
        instance_id="test",
        error_rate=0,
        rng=random.Random(11),
    )

    assert len(request.resource_spans) == 4
    for index in range(0, len(request.resource_spans), 2):
        client = request.resource_spans[index].scope_spans[0].spans[0]
        server = request.resource_spans[index + 1].scope_spans[0].spans[0]
        assert client.kind == Span.SPAN_KIND_CLIENT
        assert server.kind == Span.SPAN_KIND_SERVER
        assert client.trace_id == server.trace_id
        assert server.parent_span_id == client.span_id


def test_resource_profiles_are_rich_stable_and_service_specific() -> None:
    first = dict(service_resource_attributes("checkout-api", "shop", "test"))
    repeated = dict(service_resource_attributes("checkout-api", "shop", "test"))
    other = dict(service_resource_attributes("payments-api", "shop", "test"))

    assert first == repeated
    assert len(first) >= 35
    assert first["service.name"] == "checkout-api"
    assert first["service.namespace"] == "shop"
    assert first["k8s.namespace.name"] == "commerce"
    assert first["k8s.cluster.uid"] == other["k8s.cluster.uid"]
    assert first["k8s.pod.uid"] != other["k8s.pod.uid"]
    assert first["vcs.repository.url.full"] != other["vcs.repository.url.full"]
    assert isinstance(first["process.pid"], int)


def test_request_encodes_rich_resource_attribute_types() -> None:
    request = build_request(
        (EDGES[0],),
        namespace="shop",
        instance_id="test",
        error_rate=0,
        rng=random.Random(13),
    )

    attributes = {attribute.key: attribute.value for attribute in request.resource_spans[0].resource.attributes}
    assert attributes["service.version"].string_value
    assert attributes["k8s.cluster.uid"].string_value == "cluster-demo-production"
    assert attributes["process.pid"].WhichOneof("value") == "int_value"
    assert attributes["telemetry.sdk.language"].string_value
    assert attributes["vcs.ref.head.revision"].string_value


def test_etl_demo_keeps_pipeline_on_resource_and_execution_on_client_span() -> None:
    etl_edge = next(edge for edge in EDGES if edge.etl is not None)
    request = build_request(
        (etl_edge,),
        namespace="shop",
        instance_id="test",
        error_rate=0,
        rng=random.Random(17),
    )

    client_resource = {
        attribute.key: attribute.value.string_value
        for attribute in request.resource_spans[0].resource.attributes
    }
    server_resource = {
        attribute.key: attribute.value.string_value
        for attribute in request.resource_spans[1].resource.attributes
    }
    client_span = request.resource_spans[0].scope_spans[0].spans[0]
    server_span = request.resource_spans[1].scope_spans[0].spans[0]
    client_attributes = {attribute.key: attribute.value for attribute in client_span.attributes}
    server_attributes = {attribute.key: attribute.value.string_value for attribute in server_span.attributes}

    assert client_resource["etl.pipeline.id"] == "catalog-refresh"
    assert client_resource["etl.pipeline.name"] == "Refresh Catalog"
    assert "etl.pipeline.id" not in server_resource
    expected_execution_attributes = {
        "etl.run.id": "demo-run-001",
        "etl.run.name": "Scheduled catalog refresh",
        "etl.part.run.id": "load-products",
        "etl.part.name": "Load products",
    }
    assert {
        name: client_attributes[name].string_value
        for name in expected_execution_attributes
    } == expected_execution_attributes
    assert client_attributes["semconv.graph.discovery"].bool_value is True
    assert not set(client_attributes) & set(server_attributes) & {
        "etl.run.id",
        "etl.run.name",
        "etl.part.run.id",
        "etl.part.name",
    }
