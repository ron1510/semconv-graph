from __future__ import annotations

from pathlib import Path

from tools.semconv_codegen.dimensions import (
    SPAN_METRICS_BUILTIN_DIMENSIONS,
    root_span_discovery_dimensions,
    root_span_discovery_routing_attributes,
    root_span_modeled_attributes,
    service_graph_dimensions,
    service_graph_entity_names,
)
from tools.semconv_codegen.registry.validation import load_model_registry

ROOT = Path(__file__).resolve().parents[1]


def test_servicegraph_dimensions_come_from_participating_entity_refs() -> None:
    upstream = load_model_registry(ROOT / "upstream" / "otel-semconv" / "v1.43.0" / "model")
    extension = load_model_registry(ROOT / "model" / "extensions")
    registry = upstream.model_copy(update={"groups": (*upstream.groups, *extension.groups)})

    dimensions = service_graph_dimensions(registry)

    assert "service.name" in dimensions
    assert "service.namespace" in dimensions
    assert "k8s.pod.uid" in dimensions
    assert "http.route" in dimensions
    assert {
        "etl.pipeline.id",
        "etl.pipeline.name",
        "etl.run.id",
        "etl.run.name",
        "etl.part.run.id",
        "etl.part.name",
    } <= set(dimensions)
    assert "k8s.pod.label" not in dimensions
    assert "k8s.pod.annotation" not in dimensions

    participating_entity_refs = {
        attribute.ref
        for entity_name in service_graph_entity_names(registry)
        if (entity := registry.entities_by_name.get(entity_name)) is not None
        for attribute in entity.attributes
    }
    all_entity_refs = {
        attribute.ref
        for entity in registry.entities_by_name.values()
        for attribute in entity.attributes
    }

    assert set(dimensions) <= participating_entity_refs
    assert set(dimensions) <= all_entity_refs


def test_root_span_discovery_metadata_covers_scalar_entity_fields() -> None:
    upstream = load_model_registry(ROOT / "upstream" / "otel-semconv" / "v1.43.0" / "model")
    extension = load_model_registry(ROOT / "model" / "extensions")
    registry = upstream.model_copy(update={"groups": (*upstream.groups, *extension.groups)})

    dimensions = root_span_discovery_dimensions(registry)
    routing = root_span_discovery_routing_attributes(registry)
    modeled = root_span_modeled_attributes(registry)

    assert set(dimensions) == set(service_graph_dimensions(registry)) - SPAN_METRICS_BUILTIN_DIMENSIONS
    assert "service.name" not in dimensions
    assert "service.name" in routing
    assert "service.namespace" in routing
    assert "span.kind" in routing
    assert "etl.pipeline.id" in dimensions
    assert "etl.run.id" in dimensions
    assert "etl.part.run.id" in dimensions
    assert "etl.pipeline.id" in routing
    assert "etl.pipeline.name" not in routing
    assert "service.name" in modeled
    assert "etl.pipeline.name" in modeled
    assert not any(name.endswith((".label", ".annotation", ".selector")) for name in (*dimensions, *modeled))
    assert tuple(sorted(dimensions)) == dimensions
    assert tuple(sorted(routing)) == routing
    assert tuple(sorted(modeled)) == modeled
