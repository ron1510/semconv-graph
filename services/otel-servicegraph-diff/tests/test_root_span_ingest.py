from __future__ import annotations

from collections.abc import Iterable, Mapping

import pytest
from google.protobuf.json_format import MessageToJson
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest
from opentelemetry.proto.common.v1.common_pb2 import AnyValue, ArrayValue, InstrumentationScope, KeyValue, KeyValueList
from opentelemetry.proto.resource.v1.resource_pb2 import Resource
from opentelemetry.proto.trace.v1.trace_pb2 import ResourceSpans, ScopeSpans, Span

from otel_servicegraph_diff.engine.elements import (
    GraphContribution,
    GraphNode,
    apply_contribution,
    expire_contributors,
)
from otel_servicegraph_diff.ingest.attributes import scalar_attributes
from otel_servicegraph_diff.ingest.contributions import contributions_from_servicegraph_datapoint
from otel_servicegraph_diff.ingest.metrics import SERVICE_GRAPH_REQUEST_TOTAL, IngestRejection
from otel_servicegraph_diff.ingest.root_spans import (
    contributions_from_root_span,
    iter_otlp_json_root_span_contributions,
)


def test_scalar_attributes_retain_only_supported_otlp_values() -> None:
    attributes = scalar_attributes(
        (
            _attribute("string", "checkout"),
            _attribute("boolean", True),
            _attribute("integer", 3),
            _attribute("double", 1.5),
            KeyValue(key="bytes", value=AnyValue(bytes_value=b"ignored")),
            KeyValue(
                key="array",
                value=AnyValue(array_value=ArrayValue(values=(AnyValue(string_value="ignored"),))),
            ),
            KeyValue(
                key="mapping",
                value=AnyValue(
                    kvlist_value=KeyValueList(values=(_attribute("name", "ignored"),)),
                ),
            ),
            KeyValue(key="empty", value=AnyValue()),
        )
    )

    assert attributes == {
        "string": "checkout",
        "boolean": True,
        "integer": 3,
        "double": 1.5,
    }


def test_marked_etl_root_extracts_pipeline_run_and_part_nodes_only() -> None:
    span = _span(
        kind=Span.SPAN_KIND_INTERNAL,
        attributes={
            "semconv.graph.discovery": True,
            "etl.run.id": "run-42",
            "etl.run.name": "Nightly customer load",
            "etl.part.run.id": "batch-7",
            "etl.part.name": "Load customers",
        },
    )
    payload = _payload(
        _resource_spans(
            span,
            {
                "service.name": "etl-worker",
                "etl.pipeline.id": "customers",
                "etl.pipeline.name": "Extract Customers",
            },
        )
    )

    contributions = _contributions(iter_otlp_json_root_span_contributions(payload))
    etl_nodes = {
        contribution.element.type: contribution.element.id
        for contribution in contributions
        if contribution.element.type.startswith("etl.")
    }

    assert etl_nodes == {
        "etl.pipeline": "etl.pipeline:customers",
        "etl.run": "etl.run:customers:run-42",
        "etl.part.run": "etl.part.run:customers:run-42:batch-7",
    }
    assert all(isinstance(contribution.element, GraphNode) for contribution in contributions)
    assert all(contribution.metric_deltas == {} for contribution in contributions)
    assert all("semconv.graph.discovery" not in contribution.element.attributes for contribution in contributions)


def test_non_root_spans_are_ignored_without_rejection() -> None:
    child = _span(parent_span_id=b"parent01")

    assert tuple(iter_otlp_json_root_span_contributions(_payload(_resource_spans(child)))) == ()


def test_valid_root_without_identifiable_entities_is_ignored() -> None:
    root = _span(attributes={"semconv.graph.discovery": True})

    assert tuple(iter_otlp_json_root_span_contributions(_payload(_resource_spans(root)))) == ()


def test_server_root_can_create_app_endpoint_but_client_root_cannot() -> None:
    attributes = {
        "semconv.graph.discovery": True,
        "http.request.method": "POST",
        "http.route": "/imports/{id}",
    }
    resource = {
        "service.name": "etl-api",
        "service.namespace": "data",
    }
    server = _contributions(
        iter_otlp_json_root_span_contributions(
            _payload(_resource_spans(_span(kind=Span.SPAN_KIND_SERVER, attributes=attributes), resource))
        )
    )
    client = _contributions(
        iter_otlp_json_root_span_contributions(
            _payload(_resource_spans(_span(kind=Span.SPAN_KIND_CLIENT, attributes=attributes), resource))
        )
    )

    assert [item.element.type for item in server].count("app.endpoint") == 1
    assert all(item.element.type != "app.endpoint" for item in client)


def test_modeled_resource_span_conflict_rejects_only_that_root() -> None:
    conflicting = _resource_spans(
        _span(attributes={"service.name": "span-service"}),
        {"service.name": "resource-service"},
    )
    valid = _resource_spans(_span(end_time=20), {"service.name": "valid-service"})

    results = tuple(iter_otlp_json_root_span_contributions(_payload(conflicting, valid)))

    assert [item.reason for item in results if isinstance(item, IngestRejection)] == ["invalid_root_span"]
    assert {item.element.id for item in _contributions(results)} == {"service:valid-service"}


def test_unknown_resource_span_conflict_does_not_reject_semantic_nodes() -> None:
    root = _span(attributes={"custom.source": "span"})
    results = tuple(
        iter_otlp_json_root_span_contributions(
            _payload(_resource_spans(root, {"service.name": "checkout", "custom.source": "resource"}))
        )
    )

    assert not any(isinstance(item, IngestRejection) for item in results)
    assert {item.element.id for item in _contributions(results)} == {"service:checkout"}


def test_missing_end_timestamp_is_rejected_and_payload_processing_continues() -> None:
    invalid = _resource_spans(_span(end_time=0), {"service.name": "invalid"})
    valid = _resource_spans(_span(end_time=30), {"service.name": "valid"})

    results = tuple(iter_otlp_json_root_span_contributions(_payload(invalid, valid)))

    assert [item.reason for item in results if isinstance(item, IngestRejection)] == ["invalid_root_span"]
    assert {item.element.id for item in _contributions(results)} == {"service:valid"}


def test_malformed_otlp_json_is_rejected_without_retaining_input() -> None:
    results = tuple(iter_otlp_json_root_span_contributions("{not-json"))

    assert len(results) == 1
    rejection = results[0]
    assert isinstance(rejection, IngestRejection)
    assert rejection.reason == "invalid_otlp_root_spans_json"
    assert "{not-json" not in (rejection.detail or "")


def test_contributor_identity_ignores_trace_span_ids_and_observation_time() -> None:
    resource = {"service.name": "checkout", "service.version": "1.0"}
    first = contributions_from_root_span(
        resource,
        _span(trace_id=b"a" * 16, span_id=b"a" * 8, end_time=10),
    )
    repeated = contributions_from_root_span(
        resource,
        _span(trace_id=b"b" * 16, span_id=b"b" * 8, end_time=20),
    )
    changed = contributions_from_root_span(
        {"service.name": "checkout", "service.version": "2.0"},
        _span(trace_id=b"c" * 16, span_id=b"c" * 8, end_time=30),
    )

    assert first[0].contributor_id == repeated[0].contributor_id
    assert first[0].observed_at_unix_nano != repeated[0].observed_at_unix_nano
    assert first[0].contributor_id != changed[0].contributor_id


def test_root_span_and_servicegraph_contributors_merge_and_expire_independently() -> None:
    servicegraph = next(
        contribution
        for contribution in contributions_from_servicegraph_datapoint(
            SERVICE_GRAPH_REQUEST_TOTAL,
            {"client": "checkout", "server": "payments"},
            1,
            10,
        )
        if contribution.element.id == "service:checkout"
    )
    root_span = contributions_from_root_span(
        {"service.name": "checkout", "service.version": "2.0"},
        _span(end_time=20),
    )[0].model_copy(update={"ttl_seconds": 5})

    servicegraph_result = apply_contribution(
        None,
        servicegraph,
        ttl_seconds=100,
        processing_time_unix_ms=1,
        emitted_at_unix_ms=1,
    )
    assert servicegraph_result.state is not None
    merged = apply_contribution(
        servicegraph_result.state,
        root_span,
        ttl_seconds=100,
        processing_time_unix_ms=2,
        emitted_at_unix_ms=2,
    )

    assert merged.state is not None
    assert len(merged.state.contributors) == 2
    assert merged.event is not None
    assert merged.event.element is not None
    assert merged.event.element.attributes["service.version"] == "2.0"

    root_expired = expire_contributors(
        merged.state,
        clock="event_time",
        timestamp=5_000_000_020,
        emitted_at_unix_ms=3,
    )
    assert root_expired.state is not None
    assert len(root_expired.state.contributors) == 1
    assert root_expired.event is not None
    assert root_expired.event.element is not None
    assert "service.version" not in root_expired.event.element.attributes


def test_semantic_extraction_runs_once_per_root_span(monkeypatch: pytest.MonkeyPatch) -> None:
    from otel_servicegraph_diff.ingest import root_spans

    original = root_spans.entities_from_attributes
    calls: list[dict[str, object]] = []

    def recording_extraction(attributes: dict[str, object]):
        calls.append(attributes)
        return original(attributes)

    monkeypatch.setattr(root_spans, "entities_from_attributes", recording_extraction)

    contributions_from_root_span(
        {"service.name": "checkout"},
        _span(attributes={"etl.pipeline.id": "orders"}),
    )

    assert len(calls) == 1


def _span(
    *,
    kind: int = Span.SPAN_KIND_INTERNAL,
    attributes: Mapping[str, str | bool | int | float] | None = None,
    trace_id: bytes = b"t" * 16,
    span_id: bytes = b"s" * 8,
    parent_span_id: bytes = b"",
    end_time: int = 10,
) -> Span:
    return Span(
        trace_id=trace_id,
        span_id=span_id,
        parent_span_id=parent_span_id,
        name="etl",
        kind=kind,  # pyright: ignore[reportArgumentType] - protobuf enum stubs reject their runtime integers.
        start_time_unix_nano=1,
        end_time_unix_nano=end_time,
        attributes=tuple(_attribute(name, value) for name, value in (attributes or {}).items()),
    )


def _resource_spans(
    span: Span,
    attributes: Mapping[str, str | bool | int | float] | None = None,
) -> ResourceSpans:
    return ResourceSpans(
        resource=Resource(
            attributes=tuple(_attribute(name, value) for name, value in (attributes or {}).items()),
        ),
        scope_spans=(
            ScopeSpans(
                scope=InstrumentationScope(name="root-span-test"),
                spans=(span,),
            ),
        ),
    )


def _payload(*resource_spans: ResourceSpans) -> str:
    return MessageToJson(
        ExportTraceServiceRequest(resource_spans=resource_spans),
        preserving_proto_field_name=False,
    )


def _attribute(name: str, value: str | bool | int | float) -> KeyValue:
    match value:
        case bool():
            encoded = AnyValue(bool_value=value)
        case int():
            encoded = AnyValue(int_value=value)
        case float():
            encoded = AnyValue(double_value=value)
        case str():
            encoded = AnyValue(string_value=value)
    return KeyValue(key=name, value=encoded)


def _contributions(
    results: Iterable[GraphContribution | IngestRejection],
) -> tuple[GraphContribution, ...]:
    return tuple(item for item in results if isinstance(item, GraphContribution))
