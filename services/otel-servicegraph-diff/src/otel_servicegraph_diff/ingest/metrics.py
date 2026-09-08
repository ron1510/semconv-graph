"""OTLP JSON parsing for Collector service-graph metrics."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Annotated, Literal, cast

from google.protobuf.json_format import ParseDict, ParseError  # type: ignore[import-untyped]
from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2 import ExportMetricsServiceRequest
from opentelemetry.proto.metrics.v1.metrics_pb2 import AggregationTemporality, Metric, NumberDataPoint
from pydantic import BaseModel, ConfigDict, Field, RootModel, ValidationError

from otel_servicegraph_diff.engine.elements import FrozenModel
from otel_servicegraph_diff.ingest.attributes import TelemetryScalar, scalar_attributes

SERVICE_GRAPH_REQUEST_TOTAL = "traces_service_graph_request_total"
SERVICE_GRAPH_REQUEST_FAILED_TOTAL = "traces_service_graph_request_failed_total"
SUPPORTED_SERVICE_GRAPH_METRICS = frozenset(
    {SERVICE_GRAPH_REQUEST_TOTAL, SERVICE_GRAPH_REQUEST_FAILED_TOTAL}
)
type SupportedMetricName = Literal[
    "traces_service_graph_request_total",
    "traces_service_graph_request_failed_total",
]
type NonNegativeStrictInt = Annotated[int, Field(strict=True, ge=0)]
type NonNegativeFiniteFloat = Annotated[
    float,
    Field(strict=True, ge=0, allow_inf_nan=False),
]
type MetricValue = NonNegativeStrictInt | NonNegativeFiniteFloat
type JsonValue = None | str | bool | int | float | list[JsonValue] | dict[str, JsonValue]


class MetricPoint(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    name: SupportedMetricName
    attributes: dict[str, TelemetryScalar] = Field(default_factory=dict)
    value: MetricValue
    observed_at_unix_nano: int = Field(gt=0)


class JsonDocument(RootModel[dict[str, JsonValue]]):
    pass


class IngestRejection(FrozenModel):
    reason: Annotated[str, Field(min_length=1)]
    detail: str | None = None


def iter_otlp_json_metric_points(payload: str) -> Iterator[MetricPoint | IngestRejection]:
    try:
        document = JsonDocument.model_validate_json(payload).root
        request = ParseDict(
            cast(dict[str, object], document),
            ExportMetricsServiceRequest(),
            ignore_unknown_fields=True,
        )
    except (ParseError, TypeError, ValidationError, ValueError) as exc:
        yield ingest_rejection("invalid_otlp_json", exc)
        return

    yield from _metric_inputs(request)


def _metric_inputs(request: ExportMetricsServiceRequest) -> Iterator[MetricPoint | IngestRejection]:
    for resource_metrics in request.resource_metrics:
        for scope_metrics in resource_metrics.scope_metrics:
            for metric in scope_metrics.metrics:
                yield from _metric_points(metric)


def _metric_points(metric: Metric) -> Iterator[MetricPoint | IngestRejection]:
    if metric.name not in SUPPORTED_SERVICE_GRAPH_METRICS:
        return
    if metric.WhichOneof("data") != "sum":
        yield IngestRejection(
            reason="invalid_servicegraph_metric_type",
            detail=f"{metric.name!r} must be an OTLP Sum",
        )
        return
    if metric.sum.aggregation_temporality != AggregationTemporality.AGGREGATION_TEMPORALITY_DELTA:
        yield IngestRejection(
            reason="invalid_servicegraph_temporality",
            detail=f"{metric.name!r} must use delta temporality",
        )
        return

    metric_name = cast(SupportedMetricName, metric.name)
    for point in metric.sum.data_points:
        try:
            yield MetricPoint(
                name=metric_name,
                attributes=scalar_attributes(point.attributes),
                value=_number_value(point),
                observed_at_unix_nano=point.time_unix_nano,
            )
        except (TypeError, ValidationError, ValueError) as exc:
            yield ingest_rejection("invalid_servicegraph_datapoint", exc)

def _number_value(point: NumberDataPoint) -> int | float:
    match point.WhichOneof("value"):
        case "as_int":
            return point.as_int
        case "as_double":
            return point.as_double
        case _:
            raise ValueError("servicegraph number datapoint has no numeric value")


def ingest_rejection(reason: str, error: Exception) -> IngestRejection:
    match error:
        case ValidationError():
            details = [
                f"{'.'.join(str(part) for part in item['loc'])}: {item['type']}"
                for item in error.errors(include_input=False, include_url=False)
            ]
            return IngestRejection(reason=reason, detail="; ".join(details) or "ValidationError")
        case ValueError() if type(error) is ValueError:
            return IngestRejection(reason=reason, detail=str(error) or "ValueError")
        case _:
            return IngestRejection(reason=reason, detail=type(error).__name__)
