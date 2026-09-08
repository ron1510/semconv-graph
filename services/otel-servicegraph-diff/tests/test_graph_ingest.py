from __future__ import annotations

import json
from collections.abc import Iterable
from pathlib import Path
from typing import Any, cast

import pytest
from pydantic import ValidationError

from otel_servicegraph_diff.engine.elements import (
    GRAPH_REQUEST_FAILED_TOTAL,
    GRAPH_REQUEST_TOTAL,
    GraphContribution,
    GraphEdge,
    GraphNode,
)
from otel_servicegraph_diff.ingest.contributions import (
    contributions_from_servicegraph_datapoint,
    iter_otlp_json_contributions,
)
from otel_servicegraph_diff.ingest.metrics import (
    SERVICE_GRAPH_REQUEST_FAILED_TOTAL,
    SERVICE_GRAPH_REQUEST_TOTAL,
    IngestRejection,
    MetricPoint,
    iter_otlp_json_metric_points,
)
from tools.semconv_codegen.registry.model import AttributeDefinition, EnumAttributeType
from tools.semconv_codegen.registry.validation import load_model_registry

CODEGEN_ROOT = Path(__file__).resolve().parents[3] / "tools" / "semconv_codegen"


def test_metric_parser_retains_only_scalar_attributes() -> None:
    point = _point(
        attributes={
            "string": "checkout",
            "boolean": True,
            "integer": 3,
            "double": 1.5,
            "array": ["ignored"],
        }
    )
    attributes = cast(list[dict[str, object]], point["attributes"])
    attributes.extend(
        [
            {"key": "bytes", "value": {"bytesValue": "b3RlbA=="}},
            {
                "key": "mapping",
                "value": {
                    "kvlistValue": {
                        "values": [{"key": "name", "value": {"stringValue": "ignored"}}],
                    }
                },
            },
            {"key": "empty", "value": {}},
        ]
    )

    results = tuple(
        iter_otlp_json_metric_points(
            _payload(_sum_metric(data_points=[point])),
        )
    )

    assert len(results) == 1
    assert isinstance(results[0], MetricPoint)
    assert results[0].attributes == {
        "string": "checkout",
        "boolean": True,
        "integer": 3,
        "double": 1.5,
    }


def test_otlp_json_parser_emits_direct_contributions_and_ignores_non_scalars() -> None:
    payload = _payload(
        _sum_metric(
            data_points=[
                _point(
                    value=3,
                    attributes={
                        "client": "frontend",
                        "server": "checkout",
                        "server_http.route": "/checkout/{id}",
                        "server_custom.array": ["ignored"],
                    },
                )
            ]
        )
    )

    contributions = _valid_contributions(iter_otlp_json_contributions(payload))

    assert len(contributions) == 3
    assert len({item.element.id for item in contributions}) == len(contributions)
    dependency = _dependency(contributions)
    assert dependency.metric_deltas == {GRAPH_REQUEST_TOTAL: 3}
    assert all("custom.array" not in item.element.attributes for item in contributions)


def test_unrelated_metrics_and_zero_deltas_are_ignored() -> None:
    payload = _payload(
        _sum_metric(name="traces_service_graph_request_duration_seconds"),
        _sum_metric(data_points=[_point(value=0)]),
    )

    assert tuple(iter_otlp_json_contributions(payload)) == ()


@pytest.mark.parametrize("temporality", [None, 2])
def test_non_delta_temporality_is_rejected(temporality: int | None) -> None:
    results = tuple(iter_otlp_json_contributions(_payload(_sum_metric(temporality=temporality))))

    assert _rejection_reasons(results) == ["invalid_servicegraph_temporality"]


def test_supported_non_sum_metric_is_rejected() -> None:
    metric: dict[str, object] = {
        "name": SERVICE_GRAPH_REQUEST_TOTAL,
        "gauge": {"dataPoints": [_point(value=1)]},
    }

    results = tuple(iter_otlp_json_contributions(_payload(metric)))

    assert _rejection_reasons(results) == ["invalid_servicegraph_metric_type"]


@pytest.mark.parametrize(
    ("include_value", "value", "value_field"),
    [
        (False, 1, "asInt"),
        (True, -1, "asInt"),
        (True, "NaN", "asDouble"),
        (True, "Infinity", "asDouble"),
    ],
)
def test_invalid_numeric_values_are_rejected(include_value: bool, value: object, value_field: str) -> None:
    point = _point(include_value=include_value, value=value, value_field=value_field)
    results = tuple(iter_otlp_json_contributions(_payload(_sum_metric(data_points=[point]))))

    assert _rejection_reasons(results) == ["invalid_servicegraph_datapoint"]


@pytest.mark.parametrize(
    ("attributes", "timestamp"),
    [
        ({"server": "checkout"}, 1_234_567_890),
        ({"client": "frontend"}, 1_234_567_890),
        (None, None),
    ],
)
def test_missing_identity_or_timestamp_is_rejected(
    attributes: dict[str, object] | None,
    timestamp: int | None,
) -> None:
    point = _point(attributes=attributes, timestamp=timestamp)
    results = tuple(iter_otlp_json_contributions(_payload(_sum_metric(data_points=[point]))))

    assert _rejection_reasons(results) == ["invalid_servicegraph_datapoint"]


def test_mixed_payload_emits_every_valid_contribution_and_rejection() -> None:
    payload = _payload(
        _sum_metric(
            data_points=[
                _point(value=1),
                _point(value=-1),
                _point(value=2, timestamp=2_234_567_890),
            ]
        )
    )

    results = tuple(iter_otlp_json_contributions(payload))
    contributions = _valid_contributions(results)

    assert [item.metric_deltas[GRAPH_REQUEST_TOTAL] for item in contributions if item.metric_deltas] == [1, 2]
    assert _rejection_reasons(results) == ["invalid_servicegraph_datapoint"]


def test_malformed_json_is_rejected_without_retaining_payload() -> None:
    results = tuple(iter_otlp_json_contributions("{not-json"))

    assert len(results) == 1
    rejection = results[0]
    assert isinstance(rejection, IngestRejection)
    assert rejection.reason == "invalid_otlp_json"
    assert "{not-json" not in (rejection.detail or "")
    assert "payload" not in IngestRejection.model_fields


def test_datapoint_extracts_entities_and_relationships_once_per_side(monkeypatch: pytest.MonkeyPatch) -> None:
    from otel_servicegraph_diff.ingest import contributions as extraction

    calls: list[dict[str, object]] = []
    original = extraction.entities_from_attributes

    def recording_extraction(attributes: dict[str, object]):
        calls.append(attributes)
        return original(attributes)

    monkeypatch.setattr(extraction, "entities_from_attributes", recording_extraction)

    contributions = _contributions(
        attributes={
            "client": "frontend",
            "server": "checkout-api",
            "client_service.namespace": "web",
            "server_service.namespace": "payments",
            "server_service.instance.id": "checkout/demo",
            "server_k8s.pod.uid": "checkout-pod-demo",
            "server_http.request.method": "POST",
            "server_http.route": "/checkout/{cart_id}",
        }
    )

    assert len(calls) == 2
    nodes = {item.element.id for item in contributions if isinstance(item.element, GraphNode)}
    edges = {
        (item.element.source_id, item.element.target_id, item.element.type)
        for item in contributions
        if isinstance(item.element, GraphEdge)
    }
    assert "service:frontend" in nodes
    assert "service:checkout-api" in nodes
    assert "app.endpoint:checkout-api:payments:POST:%2Fcheckout%2F%7Bcart_id%7D" in nodes
    assert ("service:frontend", "service:checkout-api", "calls") in edges
    assert ("k8s.pod:checkout-pod-demo", "service:checkout-api", "runs") in edges


def test_app_endpoint_is_extracted_for_server_only() -> None:
    contributions = _contributions(
        attributes={
            "client": "frontend",
            "server": "checkout",
            "client_service.namespace": "frontend",
            "server_service.namespace": "checkout",
            "client_http.request.method": "GET",
            "client_http.route": "/client/{id}",
            "server_http.request.method": "POST",
            "server_http.route": "/server/{id}",
        }
    )

    endpoints = [
        item.element.id
        for item in contributions
        if isinstance(item.element, GraphNode) and item.element.type == "app.endpoint"
    ]
    assert endpoints == ["app.endpoint:checkout:checkout:POST:%2Fserver%2F%7Bid%7D"]


@pytest.mark.parametrize(
    ("etl_attributes", "expected_types", "expected_edges"),
    [
        (
            {"etl.pipeline.id": "extract-customers"},
            {"etl.pipeline"},
            set[tuple[str, str, str]](),
        ),
        (
            {
                "etl.pipeline.id": "extract-customers",
                "etl.run.id": "run-001",
            },
            {"etl.pipeline", "etl.run"},
            {
                (
                    "etl.pipeline:extract-customers",
                    "etl.run:extract-customers:run-001",
                    "contains",
                )
            },
        ),
        (
            {
                "etl.pipeline.id": "extract-customers",
                "etl.run.id": "run-001",
                "etl.part.run.id": "extract-source-001",
            },
            {"etl.pipeline", "etl.run", "etl.part.run"},
            {
                (
                    "etl.pipeline:extract-customers",
                    "etl.run:extract-customers:run-001",
                    "contains",
                ),
                (
                    "etl.run:extract-customers:run-001",
                    "etl.part.run:extract-customers:run-001:extract-source-001",
                    "contains",
                ),
            },
        ),
    ],
)
def test_etl_hierarchy_is_extracted_from_each_valid_attribute_prefix(
    etl_attributes: dict[str, str],
    expected_types: set[str],
    expected_edges: set[tuple[str, str, str]],
) -> None:
    contributions = _contributions(
        attributes={
            "client": "etl-worker",
            "server": "customer-source",
            **{f"client_{key}": value for key, value in etl_attributes.items()},
        }
    )

    etl_nodes = {
        item.element.type
        for item in contributions
        if isinstance(item.element, GraphNode) and item.element.type.startswith("etl.")
    }
    etl_edges = {
        (item.element.source_id, item.element.target_id, item.element.type)
        for item in contributions
        if isinstance(item.element, GraphEdge) and item.element.source_id.startswith("etl.")
    }

    assert etl_nodes == expected_types
    assert etl_edges == expected_edges


def test_incomplete_etl_part_identity_keeps_valid_pipeline_contribution() -> None:
    contributions = _contributions(
        attributes={
            "client": "etl-worker",
            "server": "customer-source",
            "client_etl.pipeline.id": "extract-customers",
            "client_etl.part.run.id": "orphan-part",
            "client_etl.part.name": "Orphan part",
        }
    )

    etl_nodes = {
        item.element.id
        for item in contributions
        if isinstance(item.element, GraphNode) and item.element.type.startswith("etl.")
    }

    assert etl_nodes == {"etl.pipeline:extract-customers"}


def test_etl_retries_keep_identity_and_later_runs_create_new_entities() -> None:
    def etl_ids(run_id: str, part_name: str) -> set[str]:
        contributions = _contributions(
            attributes={
                "client": "etl-worker",
                "server": "customer-source",
                "client_etl.pipeline.id": "extract-customers",
                "client_etl.pipeline.name": "Extract Customers",
                "client_etl.run.id": run_id,
                "client_etl.run.name": "Customer import",
                "client_etl.part.run.id": "extract-source",
                "client_etl.part.name": part_name,
            }
        )
        return {
            item.element.id
            for item in contributions
            if isinstance(item.element, GraphNode) and item.element.type.startswith("etl.")
        }

    first_attempt = etl_ids("run-001", "Extract source attempt one")
    retry = etl_ids("run-001", "Extract source attempt two")
    next_run = etl_ids("run-002", "Extract source")

    assert first_attempt == retry
    assert "etl.pipeline:extract-customers" in first_attempt & next_run
    assert "etl.run:extract-customers:run-001" in first_attempt
    assert "etl.run:extract-customers:run-002" in next_run
    assert first_attempt != next_run


def test_datapoint_can_extract_all_generated_server_entities() -> None:
    raw_attributes = _maximal_entity_attributes(service_name="max-server")
    contributions = _contributions(
        attributes={
            "client": "max-client",
            "server": "max-server",
            **{f"client_{key}": value for key, value in _maximal_entity_attributes("max-client").items()},
            **{f"server_{key}": value for key, value in raw_attributes.items()},
        }
    )

    entity_types = {
        item.element.type for item in contributions if isinstance(item.element, GraphNode)
    }
    edges = {
        (item.element.source_id, item.element.target_id, item.element.type)
        for item in contributions
        if isinstance(item.element, GraphEdge)
    }
    assert _expected_identifiable_entity_types() <= entity_types
    assert ("service:max-client", "service:max-server", "calls") in edges
    assert ("service:max-server", "service.instance:max-server%2Finstance", "contains") in edges
    assert ("k8s.pod:max-server-pod-uid", "service.instance:max-server%2Finstance", "runs") in edges


@pytest.mark.parametrize(
    ("connection_type", "edge_type"),
    [("http", "calls"), ("messaging_system", "publishes_to"), ("database", "queries")],
)
def test_connection_types_map_to_dependency_edges(connection_type: str, edge_type: str) -> None:
    dependency = _dependency(
        _contributions(
            attributes={"client": "producer", "server": "target", "connection_type": connection_type}
        )
    )

    assert isinstance(dependency.element, GraphEdge)
    assert dependency.element.type == edge_type
    assert dependency.metric_deltas == {GRAPH_REQUEST_TOTAL: 1}


def test_failed_metric_is_mapped_explicitly() -> None:
    dependency = _dependency(_contributions(metric_name=SERVICE_GRAPH_REQUEST_FAILED_TOTAL, value=2))

    assert dependency.metric_deltas == {GRAPH_REQUEST_FAILED_TOTAL: 2}


def test_contributor_identity_is_deterministic_and_covers_dimensions() -> None:
    left = _contributions(attributes={"client": "a", "server": "b", "server_http.route": "/x"})
    reordered = _contributions(attributes={"server_http.route": "/x", "server": "b", "client": "a"})
    changed = _contributions(attributes={"client": "a", "server": "b", "server_http.route": "/y"})

    assert {item.contributor_id for item in left} == {item.contributor_id for item in reordered}
    assert left[0].contributor_id != changed[0].contributor_id


def test_self_call_deduplicates_shared_elements() -> None:
    contributions = _contributions(
        attributes={
            "client": "checkout",
            "server": "checkout",
            "client_service.namespace": "payments",
            "server_service.namespace": "payments",
            "client_service.version": "1.0",
            "server_service.version": "1.0",
        }
    )

    assert len({item.element.id for item in contributions}) == len(contributions)
    service = next(item.element for item in contributions if item.element.id == "service:checkout")
    assert service.attributes == {
        "service.name": "checkout",
        "service.version": "1.0",
    }
    assert not any(item.metric_deltas for item in contributions)


def test_direct_extraction_rejects_zero_and_wrong_runtime_types() -> None:
    with pytest.raises(ValueError, match="greater than zero"):
        _contributions(value=0)
    with pytest.raises(TypeError, match="integer or float"):
        _contributions(value=True)  # type: ignore[arg-type]
    with pytest.raises(ValidationError):
        contributions_from_servicegraph_datapoint(
            SERVICE_GRAPH_REQUEST_TOTAL,
            {"client": "frontend", "server": "checkout"},
            1,
            0,
        )


def _contributions(
    *,
    metric_name: Any = SERVICE_GRAPH_REQUEST_TOTAL,
    value: Any = 1,
    attributes: dict[str, Any] | None = None,
) -> tuple[GraphContribution, ...]:
    return contributions_from_servicegraph_datapoint(
        metric_name,
        attributes or {"client": "frontend", "server": "checkout"},
        value,
        1_784_215_260_000_000_000,
    )


def _dependency(contributions: tuple[GraphContribution, ...]) -> GraphContribution:
    return next(item for item in contributions if item.metric_deltas)


def _valid_contributions(
    results: Iterable[GraphContribution | IngestRejection],
) -> tuple[GraphContribution, ...]:
    return tuple(item for item in results if isinstance(item, GraphContribution))


def _payload(*metrics: dict[str, object]) -> str:
    return json.dumps(
        {
            "resourceMetrics": [
                {
                    "scopeMetrics": [
                        {
                            "metrics": list(metrics),
                        }
                    ]
                }
            ]
        }
    )


def _sum_metric(
    *,
    name: str = SERVICE_GRAPH_REQUEST_TOTAL,
    temporality: int | None = 1,
    data_points: list[dict[str, object]] | None = None,
) -> dict[str, object]:
    metric_sum: dict[str, object] = {"dataPoints": data_points or [_point()]}
    if temporality is not None:
        metric_sum["aggregationTemporality"] = temporality
    return {"name": name, "sum": metric_sum}


def _point(
    *,
    value: object = 1,
    value_field: str = "asInt",
    include_value: bool = True,
    timestamp: int | None = 1_234_567_890,
    attributes: dict[str, object] | None = None,
) -> dict[str, object]:
    point: dict[str, object] = {
        "startTimeUnixNano": "1",
        "attributes": [_json_attribute(key, item) for key, item in (attributes or _base_attributes()).items()],
    }
    if include_value:
        point[value_field] = str(value) if value_field == "asInt" else value
    if timestamp is not None:
        point["timeUnixNano"] = str(timestamp)
    return point


def _json_attribute(key: str, value: object) -> dict[str, object]:
    if isinstance(value, list):
        items = cast(list[object], value)
        return {
            "key": key,
            "value": {"arrayValue": {"values": [{"stringValue": str(item)} for item in items]}},
        }
    match value:
        case str():
            encoded: dict[str, object] = {"stringValue": value}
        case bool():
            encoded = {"boolValue": value}
        case int():
            encoded = {"intValue": str(value)}
        case float():
            encoded = {"doubleValue": value}
        case _:
            raise TypeError(value)
    return {"key": key, "value": encoded}


def _base_attributes() -> dict[str, object]:
    return {"client": "frontend", "server": "checkout"}


def _rejection_reasons(results: tuple[object, ...]) -> list[str]:
    return [result.reason for result in results if isinstance(result, IngestRejection)]


def _maximal_entity_attributes(service_name: str) -> dict[str, object]:
    registry = _merged_registry()
    attributes: dict[str, object] = {}
    for attribute_id, attribute in registry.attributes_by_id.items():
        value = _example_attribute_value(attribute)
        match value:
            case str() | bool() | int() | float():
                attributes[attribute_id] = value
            case _:
                continue
    return attributes | {
        "service.name": service_name,
        "service.namespace": f"{service_name}-namespace",
        "http.request.method": "POST",
        "http.route": f"/{service_name}/{{id}}",
        "k8s.namespace.name": f"{service_name}-namespace",
        "k8s.pod.uid": f"{service_name}-pod-uid",
        "service.instance.id": f"{service_name}/instance",
    }


def _expected_identifiable_entity_types() -> set[str]:
    registry = _merged_registry()
    return {
        entity.name
        for entity in registry.entities_by_name.values()
        if any(getattr(ref, "role", None) == "identifying" for ref in entity.attributes)
    }


def _merged_registry():
    upstream = load_model_registry(CODEGEN_ROOT / "upstream" / "otel-semconv" / "v1.43.0" / "model")
    extension = load_model_registry(CODEGEN_ROOT / "model" / "extensions")
    return upstream.model_copy(update={"groups": (*upstream.groups, *extension.groups)})


def _example_attribute_value(attribute: AttributeDefinition) -> object:
    attribute_type = attribute.type
    if attribute_type == "int":
        return 1
    if attribute_type == "boolean":
        return True
    if isinstance(attribute_type, str) and attribute_type.endswith("[]"):
        item_type = attribute_type.removesuffix("[]")
        return [1 if item_type == "int" else True if item_type == "boolean" else f"{attribute.id}-value"]
    if isinstance(attribute_type, EnumAttributeType):
        return attribute_type.members[0].value
    return f"{attribute.id}-value"
