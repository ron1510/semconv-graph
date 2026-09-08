"""Extract semantic node contributions from selected OTLP root spans."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterator, Mapping
from typing import Final, cast

from google.protobuf.json_format import ParseDict, ParseError  # type: ignore[import-untyped]
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest
from opentelemetry.proto.trace.v1.trace_pb2 import Span
from pydantic import ValidationError

from extended_otel_semconv import ENTITY_MODELS, AppEndpoint, entities_from_attributes
from extended_otel_semconv.entities import SemanticEntity
from otel_servicegraph_diff.engine.elements import GraphContribution, GraphNode
from otel_servicegraph_diff.ingest.attributes import TelemetryScalar, scalar_attributes
from otel_servicegraph_diff.ingest.metrics import IngestRejection, JsonDocument, ingest_rejection

type RootSpanIngestResult = GraphContribution | IngestRejection

_FIXED_MODELED_ATTRIBUTES: Final = frozenset(
    field.alias or field_name
    for model in ENTITY_MODELS.values()
    for field_name, field in model.model_fields.items()
    if (field.alias or field_name) not in model.template_fields
)
_MODELED_ATTRIBUTE_PREFIXES: Final = tuple(
    f"{attribute}."
    for attribute in sorted(
        {
            attribute
            for model in ENTITY_MODELS.values()
            for attribute in model.template_fields
        }
    )
)


def iter_otlp_json_root_span_contributions(payload: str) -> Iterator[RootSpanIngestResult]:
    try:
        document = JsonDocument.model_validate_json(payload).root
        request = ParseDict(
            cast(dict[str, object], document),
            ExportTraceServiceRequest(),
            ignore_unknown_fields=True,
        )
    except (ParseError, TypeError, ValidationError, ValueError) as exc:
        yield ingest_rejection("invalid_otlp_root_spans_json", exc)
        return

    for resource_spans in request.resource_spans:
        resource_attributes = scalar_attributes(resource_spans.resource.attributes)
        for scope_spans in resource_spans.scope_spans:
            for span in scope_spans.spans:
                if span.parent_span_id:
                    continue
                try:
                    yield from contributions_from_root_span(resource_attributes, span)
                except (TypeError, ValueError) as exc:
                    yield ingest_rejection("invalid_root_span", exc)


def contributions_from_root_span(
    resource_attributes: Mapping[str, TelemetryScalar],
    span: Span,
) -> tuple[GraphContribution, ...]:
    if span.parent_span_id:
        raise ValueError("root-span contribution cannot have a parent span ID")
    if span.end_time_unix_nano <= 0:
        raise ValueError("root span must have a positive end timestamp")

    attributes = _merge_attributes(resource_attributes, scalar_attributes(span.attributes))
    entities = entities_from_attributes(attributes)
    if span.kind != Span.SPAN_KIND_SERVER:
        entities = [entity for entity in entities if not isinstance(entity, AppEndpoint)]

    return tuple(
        _node_contribution(entity, span.end_time_unix_nano)
        for entity in sorted(entities, key=lambda item: item.entity_id)
    )


def _merge_attributes(
    resource_attributes: Mapping[str, TelemetryScalar],
    span_attributes: Mapping[str, TelemetryScalar],
) -> dict[str, TelemetryScalar]:
    merged = dict(resource_attributes)
    for name, value in span_attributes.items():
        resource_value = merged.get(name)
        if resource_value is not None and resource_value != value and _is_modeled_attribute(name):
            raise ValueError(f"modeled attribute {name!r} conflicts between resource and root span")
        merged[name] = value
    return merged


def _is_modeled_attribute(name: str) -> bool:
    return name in _FIXED_MODELED_ATTRIBUTES or name.startswith(_MODELED_ATTRIBUTE_PREFIXES)


def _node_contribution(entity: SemanticEntity, observed_at_unix_nano: int) -> GraphContribution:
    node = GraphNode(
        id=entity.entity_id,
        type=entity.entity_type,
        attributes=entity.semantic_attributes(),
    )
    serialized_attributes = cast(dict[str, object], node.model_dump(mode="json")["attributes"])
    fingerprint = _canonical_json(
        {
            "source": "root_span",
            "element_id": node.id,
            "attributes": serialized_attributes,
        }
    )
    return GraphContribution(
        contributor_id=f"root-span:{hashlib.sha256(fingerprint.encode('utf-8')).hexdigest()}",
        observed_at_unix_nano=observed_at_unix_nano,
        element=node,
    )


def _canonical_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
