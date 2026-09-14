"""Registry-driven service graph dimension selection."""

from __future__ import annotations

from tools.semconv_codegen.registry.model import (
    AttributeDefinition,
    EntityDefinition,
    EnumAttributeType,
    RegistryDocument,
)

TEMPLATE_SUFFIXES = (".label", ".annotation", ".selector")
SPAN_METRICS_BUILTIN_DIMENSIONS = frozenset(
    {
        "collector.instance.id",
        "service.name",
        "span.kind",
        "span.name",
        "status.code",
    }
)


def service_graph_dimensions(registry: RegistryDocument) -> tuple[str, ...]:
    """Return servicegraph dimensions from modeled servicegraph entity fields."""

    entity_names = service_graph_entity_names(registry)
    dimensions: set[str] = set()
    for entity_name in entity_names:
        entity = registry.entities_by_name.get(entity_name)
        if entity is None:
            continue
        dimensions.update(
            attribute_ref.ref
            for attribute_ref in entity.attributes
            if include_dimension_ref(attribute_ref.ref, registry.attributes_by_id.get(attribute_ref.ref))
        )
    return tuple(sorted(dimensions))


def service_graph_entity_names(registry: RegistryDocument) -> set[str]:
    entity_names: set[str] = set()
    for relationship in registry.relationships_by_id.values():
        if "service_graph" not in relationship.source_signals:
            continue
        entity_names.add(relationship.source_entity)
        entity_names.add(relationship.target_entity)
    return entity_names


def include_dimension_ref(attribute_ref: str, attribute: AttributeDefinition | None = None) -> bool:
    if any(attribute_ref.endswith(suffix) for suffix in TEMPLATE_SUFFIXES):
        return False
    if attribute is None:
        return True
    return is_scalar_attribute(attribute)


def entity_dimensions(entity: EntityDefinition) -> tuple[str, ...]:
    return tuple(sorted(ref.ref for ref in entity.attributes if include_dimension_ref(ref.ref)))


def root_span_discovery_dimensions(registry: RegistryDocument) -> tuple[str, ...]:
    """Return dimensions needed to reconstruct every graph-supported entity."""

    dimensions = _scalar_entity_attributes(registry)
    return tuple(sorted(dimensions - SPAN_METRICS_BUILTIN_DIMENSIONS))


def root_span_discovery_routing_attributes(registry: RegistryDocument) -> tuple[str, ...]:
    """Return stable attributes that shard complete semantic identities to one writer."""

    attributes = registry.attributes_by_id
    entity_names = service_graph_entity_names(registry)
    routing = {
        ref.ref
        for entity_name, entity in registry.entities_by_name.items()
        if entity_name in entity_names
        for ref in entity.attributes
        if ref.role == "identifying"
        and include_dimension_ref(ref.ref, attributes.get(ref.ref))
    }
    routing.update({"service.name", "service.namespace", "span.kind"})
    return tuple(sorted(routing))


def root_span_modeled_attributes(registry: RegistryDocument) -> tuple[str, ...]:
    """Return graph-supported fields whose resource/span conflicts are ambiguous."""

    return tuple(sorted(_scalar_entity_attributes(registry)))


def _scalar_entity_attributes(registry: RegistryDocument) -> set[str]:
    attributes = registry.attributes_by_id
    entity_names = service_graph_entity_names(registry)
    return {
        ref.ref
        for entity_name, entity in registry.entities_by_name.items()
        if entity_name in entity_names
        for ref in entity.attributes
        if include_dimension_ref(ref.ref, attributes.get(ref.ref))
    }


def is_scalar_attribute(attribute: AttributeDefinition) -> bool:
    attribute_type = attribute.type
    if isinstance(attribute_type, EnumAttributeType):
        return True
    return attribute_type in {"boolean", "double", "int", "string"}
