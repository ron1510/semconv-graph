# pyright: reportArgumentType=false, reportUnknownArgumentType=false, reportUnknownMemberType=false, reportUnknownVariableType=false

from __future__ import annotations

import json
import time

import pytest
from gremlin_python.driver.protocol import GremlinServerError
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest
from opentelemetry.proto.common.v1.common_pb2 import AnyValue, InstrumentationScope, KeyValue
from opentelemetry.proto.resource.v1.resource_pb2 import Resource
from opentelemetry.proto.trace.v1.trace_pb2 import ResourceSpans, ScopeSpans, Span

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


@pytest.mark.e2e
def test_spanmetrics_root_discovery_normalizes_etl_nodes_then_expires(
    e2e_environment: DemoEnvironment,
) -> None:
    children = ExportTraceServiceRequest(
        resource_spans=(
            *(
                _root_resource(
                    "etl-worker",
                    token=f"child-{index}",
                    parent_span_id=b"parent01",
                )
                for index in range(32)
            ),
        )
    )
    e2e_environment.send_otlp_traces(children.SerializeToString())
    time.sleep(7)
    assert _etl_counts(e2e_environment) == (0, 0, 0)

    roots = ExportTraceServiceRequest(
        resource_spans=(
            *(
                _root_resource("etl-worker", token=f"extract-{index}", part_run_id="extract")
                for index in range(8)
            ),
            _root_resource("etl-worker", token="load", part_run_id="load"),
        )
    )
    e2e_environment.send_otlp_traces(roots.SerializeToString())

    assert wait_for(
        "normalized discovery metric",
        45,
        lambda: sorted(_discovery_counts(e2e_environment)) == [1, 8],
    )
    assert wait_for(
        "ETL roots projected through Flink",
        90,
        lambda: _etl_counts(e2e_environment) == (1, 1, 2),
    )
    with e2e_environment.graph() as graph:
        assert (
            graph.V()
            .has_label("etl_pipeline")
            .has("etl_pipeline_id", "customers")
            .both_e()
            .count()
            .next()
            == 0
        )

    submitter = e2e_environment.kubectl(
        "get",
        "job",
        "processing-servicegraph-flink-submitter",
        "--output=jsonpath={.status.succeeded}",
    )
    assert submitter.stdout.strip() == "1"
    assert wait_for(
        "shared metrics source checkpoint",
        30,
        lambda: e2e_environment.committed_offset("graph-element-engine") >= 1,
    )
    assert wait_for(
        "root-span contributor expiry",
        90,
        lambda: _etl_counts(e2e_environment) == (0, 0, 0),
    )


@pytest.mark.e2e
def test_incremental_checkpoints_restore_granular_lifecycle_state(
    e2e_environment: DemoEnvironment,
) -> None:
    service_name = "checkpoint-stress"
    checkpoint_baseline = e2e_environment.completed_checkpoints()
    e2e_environment.produce_metrics((_discovery_metric(service_name, time.time_ns()),))

    assert wait_for(
        "high-cardinality service projection",
        90,
        lambda: _service_count(e2e_environment, service_name) == 1,
    )
    assert wait_for(
        "three incremental checkpoints",
        60,
        lambda: e2e_environment.completed_checkpoints() >= checkpoint_baseline + 3,
    )

    before_refresh = e2e_environment.completed_checkpoints()
    e2e_environment.produce_metrics((_discovery_metric(service_name, time.time_ns()),))
    assert wait_for(
        "post-refresh checkpoint",
        45,
        lambda: e2e_environment.completed_checkpoints() > before_refresh,
    )

    e2e_environment.restart_taskmanager()
    assert wait_for(
        "Flink recovery",
        90,
        lambda: _flink_job_running(e2e_environment),
    )
    assert _service_count(e2e_environment, service_name) == 1
    after_recovery = e2e_environment.completed_checkpoints()
    assert wait_for(
        "checkpoint after TaskManager recovery",
        45,
        lambda: e2e_environment.completed_checkpoints() > after_recovery,
    )

    e2e_environment.produce_metrics((_discovery_metric(service_name, time.time_ns()),))
    assert wait_for(
        "post-recovery refresh consumption",
        30,
        lambda: e2e_environment.committed_offset("graph-element-engine") >= 3,
    )
    after_refresh = e2e_environment.completed_checkpoints()
    assert wait_for(
        "post-recovery refresh checkpoint",
        45,
        lambda: e2e_environment.completed_checkpoints() > after_refresh,
    )
    assert wait_for(
        "high-cardinality contributor expiry",
        120,
        lambda: _service_count(e2e_environment, service_name) == 0,
    )
    after_expiry = e2e_environment.completed_checkpoints()
    assert wait_for(
        "checkpoint after lifecycle deletion",
        45,
        lambda: e2e_environment.completed_checkpoints() > after_expiry,
    )


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


def _root_resource(
    service_name: str,
    *,
    token: str,
    parent_span_id: bytes = b"",
    part_run_id: str = "extract",
) -> ResourceSpans:
    span_attributes = [
        _attribute("etl.run.id", "run-42"),
        _attribute("etl.run.name", "Nightly customer load"),
        _attribute("etl.part.run.id", part_run_id),
        _attribute("etl.part.name", part_run_id.title()),
    ]
    now = time.time_ns()
    span = Span(
        trace_id=token.encode().ljust(16, b"0")[:16],
        span_id=token.encode().ljust(8, b"0")[:8],
        parent_span_id=parent_span_id,
        name="customers",
        kind=Span.SPAN_KIND_INTERNAL,
        start_time_unix_nano=now - 1_000_000,
        end_time_unix_nano=now,
        attributes=span_attributes,
    )
    return ResourceSpans(
        resource=Resource(
            attributes=(
                _attribute("service.name", service_name),
                _attribute("etl.pipeline.id", "customers"),
                _attribute("etl.pipeline.name", "Extract Customers"),
            )
        ),
        scope_spans=(
            ScopeSpans(
                scope=InstrumentationScope(name="root-span-discovery-e2e"),
                spans=(span,),
            ),
        ),
    )


def _discovery_metric(service_name: str, observed_at_unix_nano: int) -> dict[str, object]:
    return {
        "resourceMetrics": [
            {
                "scopeMetrics": [
                    {
                        "scope": {"name": "checkpoint-state-e2e"},
                        "metrics": [
                            {
                                "name": "semconv.graph.discovery.calls",
                                "sum": {
                                    "aggregationTemporality": 1,
                                    "isMonotonic": True,
                                    "dataPoints": [
                                        {
                                            "attributes": [
                                                {
                                                    "key": "service.name",
                                                    "value": {"stringValue": service_name},
                                                },
                                                {
                                                    "key": "service.version",
                                                    "value": {"stringValue": f"version-{index}"},
                                                },
                                                {
                                                    "key": "span.kind",
                                                    "value": {"stringValue": "SPAN_KIND_INTERNAL"},
                                                }
                                            ],
                                            "asInt": "1",
                                            "timeUnixNano": str(observed_at_unix_nano + index),
                                        }
                                    ],
                                },
                            }
                        ],
                    }
                ],
            }
            for index in range(256)
        ]
    }


def _attribute(key: str, value: str | bool) -> KeyValue:
    match value:
        case bool():
            encoded = AnyValue(bool_value=value)
        case str():
            encoded = AnyValue(string_value=value)
    return KeyValue(key=key, value=encoded)


def _vertex_count(environment: DemoEnvironment, label: str) -> int:
    with environment.graph() as graph:
        return int(graph.V().has_label(label).count().next())


def _edge_count(environment: DemoEnvironment, label: str) -> int:
    with environment.graph() as graph:
        return int(graph.E().has_label(label).count().next())


def _service_versions(environment: DemoEnvironment, name: str) -> list[str]:
    with environment.graph() as graph:
        return [str(value) for value in graph.V().has("service_name", name).values("service_version").to_list()]


def _service_count(environment: DemoEnvironment, name: str) -> int:
    with environment.graph() as graph:
        return int(graph.V().has_label("service").has("service_name", name).count().next())


def _flink_job_running(environment: DemoEnvironment) -> bool:
    overview = environment.flink_job_overview()
    jobs = overview.get("jobs", [])
    return isinstance(jobs, list) and len(jobs) == 1 and jobs[0].get("state") == "RUNNING"


def _all_counts(environment: DemoEnvironment) -> tuple[int, int]:
    with environment.graph() as graph:
        return int(graph.V().count().next()), int(graph.E().count().next())


def _etl_counts(environment: DemoEnvironment) -> tuple[int, int, int]:
    with environment.graph() as graph:
        return (
            int(graph.V().has_label("etl_pipeline").has("etl_pipeline_id", "customers").count().next()),
            int(graph.V().has_label("etl_run").has("etl_run_id", "run-42").count().next()),
            int(graph.V().has_label("etl_part_run").has("etl_run_id", "run-42").count().next()),
        )


def _discovery_counts(environment: DemoEnvironment) -> list[int]:
    counts: list[int] = []
    for value in environment.topic_values("otel.servicegraph.metrics"):
        document = json.loads(value)
        for resource_metrics in document.get("resourceMetrics", []):
            for scope_metrics in resource_metrics.get("scopeMetrics", []):
                for metric in scope_metrics.get("metrics", []):
                    if metric.get("name") != "semconv.graph.discovery.calls":
                        continue
                    for point in metric.get("sum", {}).get("dataPoints", []):
                        count = point.get("asInt")
                        if count is not None:
                            counts.append(int(count))
    return counts
