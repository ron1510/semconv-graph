"""Generate entity models and Collector dimensions from OTel registries."""

from __future__ import annotations

import argparse
import difflib
import hashlib
import json
import re
from collections.abc import Sequence
from pathlib import Path
from typing import NamedTuple

import yaml

from tools.semconv_codegen.dimensions import service_graph_dimensions, service_graph_entity_names
from tools.semconv_codegen.registry.model import (
    EntityAttributeRef,
    EntityDefinition,
    RegistryDocument,
    RelationshipDefinition,
)
from tools.semconv_codegen.registry.validation import load_model_registry, validate_extension_model
from tools.semconv_codegen.semantic_schema import (
    SemanticModel,
    build_semantic_models,
    python_class_name,
    render_semantic_schema,
)
from tools.semconv_codegen.static_models import render_static_models

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
GRAPH_REQUEST_TOTAL = "service_graph.request.total"
GRAPH_REQUEST_FAILED_TOTAL = "service_graph.request.failed.total"
ARANGODB_SCHEMA_VERSION = "1"
ARANGODB_RESERVED_PROPERTIES = frozenset(
    {
        "_key",
        "_id",
        "_rev",
        "_from",
        "_to",
        "attributes",
        "metrics",
        "element_id",
        "semantic_type",
        "schema_version",
        "event_id",
        "payload_hash",
        "observed_at_unix_nano",
        "emitted_at_unix_ms",
    }
)


class GenerationPaths(NamedTuple):
    upstream_model: Path
    extension_model: Path
    upstream_lock: Path
    generated_dir: Path
    package_lock: Path
    semantic_schema: Path
    relationship_metadata: Path
    collector_dimensions: Path
    arangodb_schema: Path
    gremlin_schema: Path


class GeneratedRelationship(NamedTuple):
    relationship: RelationshipDefinition
    class_name: str


def default_generation_paths(root: Path = REPOSITORY_ROOT) -> GenerationPaths:
    codegen_root = root / "tools" / "semconv_codegen"
    semantic_package = root / "packages" / "extended-opentelemetry-semconv" / "src" / "extended_otel_semconv"
    return GenerationPaths(
        upstream_model=codegen_root / "upstream" / "otel-semconv" / "v1.43.0" / "model",
        extension_model=codegen_root / "model" / "extensions",
        upstream_lock=codegen_root / "upstream" / "otel-semconv.lock.json",
        generated_dir=semantic_package / "generated",
        package_lock=semantic_package / "metadata" / "otel-semconv.lock.json",
        semantic_schema=semantic_package / "metadata" / "semantic-entities.schema.json",
        relationship_metadata=semantic_package / "metadata" / "service-graph-relationships.json",
        collector_dimensions=root / "deploy" / "helm" / "servicegraph-collector" / "files" / "dimensions.yaml",
        arangodb_schema=(
            root
            / "services"
            / "servicegraph-indexer"
            / "src"
            / "servicegraph_indexer"
            / "metadata"
            / "arangodb-graph-schema.json"
        ),
        gremlin_schema=root / "deploy" / "helm" / "servicegraph-gremlin" / "files" / "arangodb-graph-schema.json",
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate models and Collector dimensions from the merged registry.")
    parser.add_argument("--check", action="store_true", help="Fail if generated files are not up to date.")
    args = parser.parse_args(argv)

    paths = default_generation_paths()
    files = generate_files(paths)
    if args.check:
        return check_files(files, paths.generated_dir)
    write_files(files, paths.generated_dir)
    return 0


def generate_files(paths: GenerationPaths) -> dict[Path, str]:
    validate_extension_model(paths.upstream_model, paths.extension_model)
    upstream = load_model_registry(paths.upstream_model)
    extension = load_model_registry(paths.extension_model)
    registry = _merged_registry(upstream, extension)
    attributes = registry.attributes_by_id
    relationships = registry.relationships_by_id

    semantic_models = build_semantic_models(
        tuple(registry.entities_by_name.values()),
        attributes,
    )
    semantic_schema = render_semantic_schema(semantic_models)
    generated_relationships = tuple(
        _generated_relationship(relationship)
        for relationship in sorted(relationships.values(), key=lambda item: item.id)
    )
    _validate_relationship_class_names(generated_relationships)

    files: dict[Path, str] = {
        paths.generated_dir / "__init__.py": _render_package_init(
            semantic_models,
            generated_relationships,
        ),
        paths.generated_dir / "_models.py": render_static_models(semantic_schema, semantic_models),
        paths.generated_dir / "entities.py": _render_entity_module(semantic_models),
        paths.generated_dir / "edges.py": _render_edge_module(generated_relationships),
        paths.package_lock: paths.upstream_lock.read_text(encoding="utf-8"),
        paths.semantic_schema: semantic_schema,
        paths.relationship_metadata: _render_relationship_metadata(relationships),
        paths.collector_dimensions: _render_collector_dimensions(registry),
        paths.arangodb_schema: _render_arangodb_schema(registry, paths.upstream_lock),
    }
    files[paths.gremlin_schema] = files[paths.arangodb_schema]
    return files


def write_files(files: dict[Path, str], generated_dir: Path) -> None:
    generated_dir.mkdir(parents=True, exist_ok=True)
    for stale_file in generated_dir.glob("*.py"):
        if stale_file not in files:
            stale_file.unlink()
    for path, content in files.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")


def check_files(files: dict[Path, str], generated_dir: Path) -> int:
    failed = False
    expected_generated = {path for path in files if path.parent == generated_dir and path.suffix == ".py"}
    for stale_file in sorted(set(generated_dir.glob("*.py")) - expected_generated):
        failed = True
        print(f"{stale_file} is stale")
    for path, expected in files.items():
        actual = path.read_text(encoding="utf-8") if path.exists() else ""
        if actual == expected:
            continue
        failed = True
        print(f"{path} is not up to date")
        for line in difflib.unified_diff(
            actual.splitlines(),
            expected.splitlines(),
            fromfile=f"{path} (actual)",
            tofile=f"{path} (expected)",
            lineterm="",
        ):
            print(line)
    return 1 if failed else 0


def _merged_registry(upstream: RegistryDocument, extension: RegistryDocument) -> RegistryDocument:
    return upstream.model_copy(update={"groups": (*upstream.groups, *extension.groups)})


def _generated_relationship(relationship: RelationshipDefinition) -> GeneratedRelationship:
    return GeneratedRelationship(
        relationship=relationship,
        class_name=(
            f"{_class_name(relationship.source_entity)}"
            f"{_class_name(relationship.name)}"
            f"{_class_name(relationship.target_entity)}Edge"
        ),
    )


def _validate_relationship_class_names(relationships: tuple[GeneratedRelationship, ...]) -> None:
    owners: dict[str, str] = {}
    for generated in relationships:
        previous = owners.get(generated.class_name)
        if previous is not None:
            raise ValueError(
                f"relationship class collision: {previous!r} and {generated.relationship.id!r} "
                f"both map to {generated.class_name!r}"
            )
        owners[generated.class_name] = generated.relationship.id


def _identifying_refs(entity: EntityDefinition) -> tuple[EntityAttributeRef, ...]:
    return tuple(ref for ref in entity.attributes if ref.role == "identifying")


def _render_entity_module(models: tuple[SemanticModel, ...]) -> str:
    lines = [
        '"""Generated public semantic entity classes."""',
        "",
        "from types import MappingProxyType",
        "from typing import ClassVar",
        "",
        "from extended_otel_semconv.entities import RawAttributes, SemanticEntity",
    ]
    for model in models:
        lines.append(
            "from extended_otel_semconv.generated._models import "
            f"{model.fields_class_name} as _{model.fields_class_name}"
        )
    lines.append("")
    for model in models:
        lines.extend(
            [
                f"class {model.class_name}(_{model.fields_class_name}):",
                f'    entity_type: ClassVar[str] = "{model.semantic_type}"',
                f"    identity_fields: ClassVar[tuple[str, ...]] = {model.identity_fields!r}",
                f"    template_fields: ClassVar[tuple[str, ...]] = {model.template_fields!r}",
                "",
                "",
            ]
        )
    lines.append("ENTITY_MODELS = MappingProxyType({")
    for model in models:
        lines.append(f'    "{model.semantic_type}": {model.class_name},')
    lines.extend(
        [
            "})",
            "",
            "",
            "def entities_from_attributes(attributes: RawAttributes) -> list[SemanticEntity]:",
            "    entities: list[SemanticEntity] = []",
            "    for entity_class in ENTITY_MODELS.values():",
            "        entity = entity_class.from_attributes(attributes)",
            "        if entity is not None:",
            "            entities.append(entity)",
            "    return entities",
        ]
    )
    return _finalize(lines)


def _render_edge_module(relationships: tuple[GeneratedRelationship, ...]) -> str:
    lines = [
        '\"\"\"Generated semantic edge interfaces.\"\"\"',
        "",
        "from typing import ClassVar",
        "",
        "from extended_otel_semconv.edges import SemanticEdge",
        "",
    ]
    for generated in relationships:
        relationship = generated.relationship
        lines.extend(
            [
                f"class {generated.class_name}(SemanticEdge):",
                f'    relationship_id: ClassVar[str] = "{relationship.id}"',
                f'    relationship_type: ClassVar[str] = "{relationship.name}"',
                f'    source_entity_type: ClassVar[str] = "{relationship.source_entity}"',
                f'    target_entity_type: ClassVar[str] = "{relationship.target_entity}"',
                "",
                "",
            ]
        )
    lines.insert(2, "from types import MappingProxyType")
    lines.append("EDGE_MODELS = MappingProxyType({")
    for generated in relationships:
        relationship = generated.relationship
        lines.append(
            f'    ("{relationship.source_entity}", "{relationship.name}", '
            f'"{relationship.target_entity}"): {generated.class_name},'
        )
    lines.extend(["})", "", "", "__all__ = ["])
    for generated in relationships:
        lines.append(f'    "{generated.class_name}",')
    lines.extend(['    "SemanticEdge",', "]"])
    return _finalize(lines)


def _render_package_init(
    entities: tuple[SemanticModel, ...],
    relationships: tuple[GeneratedRelationship, ...],
) -> str:
    lines = [
        '\"\"\"Generated semantic entity and edge interfaces.\"\"\"',
        "",
        "from extended_otel_semconv.edges import SemanticEdge, semantic_edge_from_data",
        "from extended_otel_semconv.entities import SemanticEntity, entity_from_attributes",
    ]
    names = ", ".join(entity.class_name for entity in entities)
    lines.append(f"from extended_otel_semconv.generated.entities import {names}")
    lines.append("from extended_otel_semconv.generated.entities import ENTITY_MODELS, entities_from_attributes")
    edge_names = ", ".join(relationship.class_name for relationship in relationships)
    lines.append(f"from extended_otel_semconv.generated.edges import {edge_names}")
    lines.append("from extended_otel_semconv.generated.edges import EDGE_MODELS")
    lines.extend(["", "__all__ = ["])
    for entity in entities:
        lines.append(f'    "{entity.class_name}",')
    for relationship in relationships:
        lines.append(f'    "{relationship.class_name}",')
    lines.extend(
        [
            '    "EDGE_MODELS",',
            '    "ENTITY_MODELS",',
            '    "SemanticEdge",',
            '    "SemanticEntity",',
            '    "entities_from_attributes",',
            '    "entity_from_attributes",',
            '    "semantic_edge_from_data",',
            "]",
        ]
    )
    return _finalize(lines)


def _render_relationship_metadata(relationships: dict[str, RelationshipDefinition]) -> str:
    rendered = [
        relationship.model_dump(mode="json")
        for relationship in sorted(relationships.values(), key=lambda item: item.id)
        if "service_graph" in relationship.source_signals
    ]
    return json.dumps(rendered, indent=2, sort_keys=True) + "\n"


def _render_collector_dimensions(registry: RegistryDocument) -> str:
    return yaml.safe_dump(
        {"dimensions": service_graph_dimensions(registry)},
        sort_keys=False,
        default_flow_style=False,
    )


def _render_arangodb_schema(registry: RegistryDocument, upstream_lock: Path) -> str:
    entity_names = sorted(service_graph_entity_names(registry))
    collections = _unique_sanitized_names(entity_names, "vertex collection")
    relationships = [
        relationship
        for relationship in registry.relationships_by_id.values()
        if "service_graph" in relationship.source_signals
    ]
    relationship_names = sorted({relationship.name for relationship in relationships})
    edge_collections = _unique_sanitized_names(relationship_names, "edge collection")

    canonical_properties = (*service_graph_dimensions(registry), GRAPH_REQUEST_FAILED_TOTAL, GRAPH_REQUEST_TOTAL)
    aliases = _property_aliases(canonical_properties)
    vertices: list[dict[str, object]] = []
    for entity_name in entity_names:
        entity = registry.entities_by_name[entity_name]
        identifying = [
            {"attribute": ref.ref, "property": aliases[ref.ref]}
            for ref in _identifying_refs(entity)
            if ref.ref in aliases
        ]
        vertices.append(
            {
                "semantic_type": entity_name,
                "collection": collections[entity_name],
                "identifying_properties": identifying,
            }
        )

    edges: list[dict[str, object]] = []
    for name in relationship_names:
        matching = [relationship for relationship in relationships if relationship.name == name]
        edges.append(
            {
                "semantic_type": name,
                "collection": edge_collections[name],
                "from": sorted({collections[item.source_entity] for item in matching}),
                "to": sorted({collections[item.target_entity] for item in matching}),
            }
        )

    schema: dict[str, object] = {
        "_meta": {
            "schema_version": ARANGODB_SCHEMA_VERSION,
            "upstream_registry_lock_sha256": _json_document_sha256(upstream_lock),
        },
        "graph_name": "servicegraph",
        "vertex_collections": vertices,
        "edge_collections": edges,
        "property_aliases": {
            "attributes": {name: aliases[name] for name in service_graph_dimensions(registry)},
            "metrics": {
                GRAPH_REQUEST_FAILED_TOTAL: aliases[GRAPH_REQUEST_FAILED_TOTAL],
                GRAPH_REQUEST_TOTAL: aliases[GRAPH_REQUEST_TOTAL],
            },
        },
        "reserved_properties": sorted(ARANGODB_RESERVED_PROPERTIES),
    }
    metadata = schema["_meta"]
    assert isinstance(metadata, dict)
    metadata["schema_hash"] = _schema_hash(schema)
    return json.dumps(schema, indent=2, sort_keys=True) + "\n"


def _safe_arangodb_name(value: str) -> str:
    name = re.sub(r"[^0-9a-zA-Z]+", "_", value).strip("_").lower()
    if not name or name[0].isdigit():
        raise ValueError(f"{value!r} cannot be converted to a valid ArangoDB name")
    return name


def _unique_sanitized_names(values: Sequence[str], kind: str) -> dict[str, str]:
    names: dict[str, str] = {}
    owners: dict[str, str] = {}
    for value in values:
        name = _safe_arangodb_name(value)
        previous = owners.get(name)
        if previous is not None and previous != value:
            raise ValueError(f"{kind} collision: {previous!r} and {value!r} both map to {name!r}")
        owners[name] = value
        names[value] = name
    return names


def _property_aliases(values: Sequence[str]) -> dict[str, str]:
    aliases = _unique_sanitized_names(values, "property alias")
    for canonical, alias in aliases.items():
        if alias in ARANGODB_RESERVED_PROPERTIES:
            raise ValueError(f"property alias collision: {canonical!r} maps to reserved field {alias!r}")
    return aliases


def _schema_hash(schema: dict[str, object]) -> str:
    canonical = json.dumps(schema, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(canonical).hexdigest()


def _json_document_sha256(path: Path) -> str:
    document = json.loads(path.read_text(encoding="utf-8"))
    canonical = json.dumps(document, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(canonical).hexdigest()


def _class_name(entity_name: str) -> str:
    return python_class_name(entity_name)


def _finalize(lines: list[str]) -> str:
    return "\n".join(lines).rstrip() + "\n"


if __name__ == "__main__":
    raise SystemExit(main())
