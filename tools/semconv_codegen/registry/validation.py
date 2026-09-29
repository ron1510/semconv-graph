"""Validation for extension registries layered over a pinned upstream OTel model."""

from __future__ import annotations

from pathlib import Path

from tools.semconv_codegen.registry.model import (
    AttributeDefinition,
    AttributeGroup,
    EntityDefinition,
    RegistryDocument,
    RelationshipDefinition,
    load_registry_documents,
    model_yaml_files,
)


def load_model_registry(model_dir: Path) -> RegistryDocument:
    return load_registry_documents(model_yaml_files(model_dir))


def validate_extension_model(upstream_model_dir: Path, extension_model_dir: Path) -> None:
    upstream = load_model_registry(upstream_model_dir)
    extension = load_model_registry(extension_model_dir)

    upstream_attributes = _attributes_by_id(upstream)
    upstream_entities = _entities_by_name(upstream)
    extension_attributes = _attributes_by_id(extension)
    extension_entities = _entities_by_name(extension)
    extension_relationships = _relationships_by_id(extension)

    _assert_no_duplicate_attributes(extension)
    _assert_no_duplicate_entities(extension)
    _assert_no_duplicate_relationships(extension)

    for attribute_id in extension_attributes:
        if attribute_id in upstream_attributes:
            raise AssertionError(f"{extension_model_dir}: extension redefines upstream attribute {attribute_id}")

    for entity_name in extension_entities:
        if entity_name in upstream_entities:
            raise AssertionError(f"{extension_model_dir}: extension redefines upstream entity {entity_name}")

    available_attributes = set(upstream_attributes) | set(extension_attributes)
    for entity in extension_entities.values():
        for attribute_ref in entity.attributes:
            if attribute_ref.ref not in available_attributes:
                raise AssertionError(
                    f"{extension_model_dir}: {entity.name} references unknown attribute {attribute_ref.ref}"
                )

    available_entities = set(upstream_entities) | set(extension_entities)
    for relationship in extension_relationships.values():
        if relationship.source_entity not in available_entities:
            raise AssertionError(
                f"{extension_model_dir}: {relationship.id} references unknown source entity "
                f"{relationship.source_entity}"
            )
        if relationship.target_entity not in available_entities:
            raise AssertionError(
                f"{extension_model_dir}: {relationship.id} references unknown target entity "
                f"{relationship.target_entity}"
            )
        for source_signal in relationship.source_signals:
            if source_signal not in {"trace", "service_graph", "span_metrics"}:
                raise AssertionError(
                    f"{extension_model_dir}: {relationship.id} uses unknown source signal {source_signal}"
                )
        graph_signals = {"service_graph", "span_metrics"}.intersection(relationship.source_signals)
        if bool(graph_signals) != (relationship.evidence_scope is not None):
            raise AssertionError(
                f"{extension_model_dir}: {relationship.id} must define evidence_scope "
                "exactly when a graph evidence source is declared"
            )
        is_interaction = relationship.evidence_scope == "interaction"
        if is_interaction and graph_signals != {"service_graph"}:
            raise AssertionError(
                f"{extension_model_dir}: {relationship.id} interaction evidence "
                "must come only from service_graph"
            )
        has_connection_type = relationship.connection_type is not None
        if is_interaction != has_connection_type:
            raise AssertionError(
                f"{extension_model_dir}: {relationship.id} must define connection_type "
                "exactly when evidence_scope is interaction"
            )
        if relationship.connection_type is not None and not relationship.connection_type.strip():
            raise AssertionError(
                f"{extension_model_dir}: {relationship.id} connection_type must be nonempty"
            )


def _attributes_by_id(registry: RegistryDocument) -> dict[str, AttributeDefinition]:
    return registry.attributes_by_id


def _entities_by_name(registry: RegistryDocument) -> dict[str, EntityDefinition]:
    return registry.entities_by_name


def _relationships_by_id(registry: RegistryDocument) -> dict[str, RelationshipDefinition]:
    return registry.relationships_by_id


def _assert_no_duplicate_attributes(registry: RegistryDocument) -> None:
    attribute_ids: set[str] = set()
    for group in registry.groups:
        if isinstance(group, AttributeGroup):
            for attribute in group.attributes:
                if attribute.id in attribute_ids:
                    raise AssertionError(f"duplicate extension attribute {attribute.id}")
                attribute_ids.add(attribute.id)


def _assert_no_duplicate_entities(registry: RegistryDocument) -> None:
    entity_ids: set[str] = set()
    entity_names: set[str] = set()
    for group in registry.groups:
        if isinstance(group, EntityDefinition):
            if group.id in entity_ids:
                raise AssertionError(f"duplicate extension entity id {group.id}")
            if group.name in entity_names:
                raise AssertionError(f"duplicate extension entity name {group.name}")
            entity_ids.add(group.id)
            entity_names.add(group.name)


def _assert_no_duplicate_relationships(registry: RegistryDocument) -> None:
    relationship_ids: set[str] = set()
    relationship_triples: set[tuple[str, str, str]] = set()
    for group in registry.groups:
        if isinstance(group, RelationshipDefinition):
            if group.id in relationship_ids:
                raise AssertionError(f"duplicate extension relationship id {group.id}")
            triple = (group.source_entity, group.name, group.target_entity)
            if triple in relationship_triples:
                raise AssertionError(
                    "duplicate extension relationship triple "
                    f"{group.source_entity} -[{group.name}]-> {group.target_entity}"
                )
            relationship_ids.add(group.id)
            relationship_triples.add(triple)
