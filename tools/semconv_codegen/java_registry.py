"""Render Java runtime semantic metadata from the same IR as the Python SDK."""

from __future__ import annotations

import json

from tools.semconv_codegen.dimensions import GRAPH_EVIDENCE_SIGNALS
from tools.semconv_codegen.registry.model import RelationshipDefinition
from tools.semconv_codegen.semantic_schema import SemanticModel


def render_java_registry(
    models: tuple[SemanticModel, ...],
    relationships: tuple[RelationshipDefinition, ...],
) -> str:
    document = {
        "entities": [
            {
                "type": model.semantic_type,
                "identity_fields": list(model.identity_fields),
                "fields": [
                    {
                        "name": field.canonical_name,
                        "kind": field.kind.value,
                        "required": field.required,
                        "enum_values": list(field.enum_values),
                    }
                    for field in model.fields
                ],
            }
            for model in models
        ],
        "relationships": [
            {
                "type": relationship.name,
                "source": relationship.source_entity,
                "target": relationship.target_entity,
                "evidence_sources": sorted(
                    GRAPH_EVIDENCE_SIGNALS.intersection(relationship.source_signals)
                ),
                "evidence_scope": relationship.evidence_scope,
                "connection_type": relationship.connection_type,
            }
            for relationship in sorted(relationships, key=lambda item: item.id)
            if GRAPH_EVIDENCE_SIGNALS.intersection(relationship.source_signals)
        ],
    }
    return json.dumps(document, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
