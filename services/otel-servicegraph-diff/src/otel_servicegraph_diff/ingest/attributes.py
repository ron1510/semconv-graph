"""Scalar OTLP attribute decoding shared by metric and trace ingestion."""

from __future__ import annotations

from collections.abc import Iterable

from opentelemetry.proto.common.v1.common_pb2 import AnyValue, KeyValue

type TelemetryScalar = str | bool | int | float


def scalar_attributes(items: Iterable[KeyValue]) -> dict[str, TelemetryScalar]:
    attributes: dict[str, TelemetryScalar] = {}
    for item in items:
        value = scalar_value(item.value)
        if value is not None:
            attributes[item.key] = value
    return attributes


def scalar_value(value: AnyValue) -> TelemetryScalar | None:
    match value.WhichOneof("value"):
        case "string_value":
            return value.string_value
        case "bool_value":
            return value.bool_value
        case "int_value":
            return value.int_value
        case "double_value":
            return value.double_value
        case _:
            return None
