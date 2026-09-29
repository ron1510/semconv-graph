from __future__ import annotations

from pathlib import Path

import pytest

from tools.semconv_codegen.registry.validation import load_model_registry, validate_extension_model

ROOT = Path(__file__).resolve().parents[1]
UPSTREAM_MODEL = ROOT / "upstream" / "otel-semconv" / "v1.43.0" / "model"
EXTENSION_MODEL = ROOT / "model" / "extensions"


def test_upstream_snapshot_exposes_known_entities_and_attributes() -> None:
    registry = load_model_registry(UPSTREAM_MODEL)

    assert "service.name" in registry.attributes_by_id
    assert "http.route" in registry.attributes_by_id
    assert "service" in registry.entities_by_name
    assert "service.instance" in registry.entities_by_name
    assert "service.namespace" in registry.entities_by_name
    assert "k8s.pod" in registry.entities_by_name


def test_extension_model_can_reference_upstream_attributes() -> None:
    validate_extension_model(UPSTREAM_MODEL, EXTENSION_MODEL)

    registry = load_model_registry(EXTENSION_MODEL)

    assert "app.endpoint" in registry.entities_by_name
    assert "relationship.service_exposes_app_endpoint" in registry.relationships_by_id


def test_extension_cannot_redefine_upstream_entity(tmp_path: Path) -> None:
    extension_model = tmp_path / "extensions"
    app_model = extension_model / "app"
    app_model.mkdir(parents=True)
    (app_model / "entities.yaml").write_text(
        """
groups:
  - id: entity.custom.service
    type: entity
    name: service
    stability: development
    brief: Invalid duplicate service entity.
    attributes:
      - ref: service.name
""",
        encoding="utf-8",
    )

    with pytest.raises(AssertionError, match="redefines upstream entity service"):
        validate_extension_model(UPSTREAM_MODEL, extension_model)


def test_extension_cannot_redefine_upstream_attribute(tmp_path: Path) -> None:
    extension_model = tmp_path / "extensions"
    app_model = extension_model / "app"
    app_model.mkdir(parents=True)
    (app_model / "registry.yaml").write_text(
        """
groups:
  - id: registry.custom.service
    type: attribute_group
    attributes:
      - id: service.name
        type: string
        stability: development
        brief: Invalid duplicate service name.
""",
        encoding="utf-8",
    )

    with pytest.raises(AssertionError, match="redefines upstream attribute service.name"):
        validate_extension_model(UPSTREAM_MODEL, extension_model)


def test_extension_relationship_must_reference_known_entities(tmp_path: Path) -> None:
    extension_model = tmp_path / "extensions"
    graph_model = extension_model / "graph"
    graph_model.mkdir(parents=True)
    (graph_model / "relationships.yaml").write_text(
        """
groups:
  - id: relationship.invalid
    type: relationship
    name: owns
    source_entity: missing.source
    target_entity: service
    source_signals: [trace]
""",
        encoding="utf-8",
    )

    with pytest.raises(AssertionError, match="references unknown source entity missing.source"):
        validate_extension_model(UPSTREAM_MODEL, extension_model)


def test_extension_entity_must_reference_known_attribute(tmp_path: Path) -> None:
    extension_model = tmp_path / "extensions"
    extension_model.mkdir()
    (extension_model / "entities.yaml").write_text(
        """
groups:
  - id: entity.custom
    type: entity
    name: custom
    attributes:
      - ref: missing.attribute
        role: identifying
""".lstrip(),
        encoding="utf-8",
    )

    with pytest.raises(AssertionError, match="custom references unknown attribute missing.attribute"):
        validate_extension_model(UPSTREAM_MODEL, extension_model)


@pytest.mark.parametrize(
    ("target", "signals", "message"),
    [
        ("missing.target", "[trace]", "references unknown target entity missing.target"),
        ("service", "[logs]", "uses unknown source signal logs"),
    ],
)
def test_extension_relationship_rejects_unknown_target_or_signal(
    tmp_path: Path,
    target: str,
    signals: str,
    message: str,
) -> None:
    extension_model = tmp_path / "extensions"
    extension_model.mkdir()
    (extension_model / "relationships.yaml").write_text(
        f"""
groups:
  - id: relationship.invalid
    type: relationship
    name: calls
    source_entity: service
    target_entity: {target}
    source_signals: {signals}
""".lstrip(),
        encoding="utf-8",
    )

    with pytest.raises(AssertionError, match=message):
        validate_extension_model(UPSTREAM_MODEL, extension_model)


@pytest.mark.parametrize(
    ("contract", "message"),
    [
        ("", "must define evidence_scope exactly when a graph evidence source is declared"),
        (
            "    evidence_scope: observation\n    connection_type: unset\n",
            "must define connection_type exactly when evidence_scope is interaction",
        ),
        (
            "    evidence_scope: interaction\n",
            "must define connection_type exactly when evidence_scope is interaction",
        ),
        (
            '    evidence_scope: interaction\n    connection_type: ""\n',
            "connection_type must be nonempty",
        ),
    ],
)
def test_servicegraph_relationship_contract_is_scope_driven(
    tmp_path: Path, contract: str, message: str
) -> None:
    extension_model = tmp_path / "extensions"
    extension_model.mkdir()
    (extension_model / "relationships.yaml").write_text(
        f"""
groups:
  - id: relationship.invalid
    type: relationship
    name: calls
    source_entity: service
    target_entity: service
    source_signals: [service_graph]
{contract}""".lstrip(),
        encoding="utf-8",
    )

    with pytest.raises(AssertionError, match=message):
        validate_extension_model(UPSTREAM_MODEL, extension_model)


def test_connection_type_can_select_distinct_endpoint_definitions(tmp_path: Path) -> None:
    extension_model = tmp_path / "extensions"
    extension_model.mkdir()
    (extension_model / "relationships.yaml").write_text(
        """
groups:
  - id: relationship.service_calls_service
    type: relationship
    name: calls
    source_entity: service
    target_entity: service
    source_signals: [service_graph]
    evidence_scope: interaction
    connection_type: unset
  - id: relationship.service_calls_instance
    type: relationship
    name: calls
    source_entity: service
    target_entity: service.instance
    source_signals: [service_graph]
    evidence_scope: interaction
    connection_type: unset
""".lstrip(),
        encoding="utf-8",
    )

    validate_extension_model(UPSTREAM_MODEL, extension_model)


def test_spanmetrics_allows_observations_and_rejects_interactions(tmp_path: Path) -> None:
    extension_model = tmp_path / "extensions"
    extension_model.mkdir()
    relationship_file = extension_model / "relationships.yaml"
    relationship_file.write_text(
        """
groups:
  - id: relationship.service_contains_instance
    type: relationship
    name: contains
    source_entity: service
    target_entity: service.instance
    source_signals: [span_metrics]
    evidence_scope: observation
""".lstrip(),
        encoding="utf-8",
    )
    validate_extension_model(UPSTREAM_MODEL, extension_model)

    relationship_file.write_text(
        """
groups:
  - id: relationship.service_calls_service
    type: relationship
    name: calls
    source_entity: service
    target_entity: service
    source_signals: [span_metrics]
    evidence_scope: interaction
    connection_type: unset
""".lstrip(),
        encoding="utf-8",
    )
    with pytest.raises(AssertionError, match="interaction evidence must come only from service_graph"):
        validate_extension_model(UPSTREAM_MODEL, extension_model)


def test_duplicate_relationship_triple_is_rejected(tmp_path: Path) -> None:
    extension_model = tmp_path / "extensions"
    extension_model.mkdir()
    (extension_model / "relationships.yaml").write_text(
        """
groups:
  - id: relationship.calls_one
    type: relationship
    name: calls
    source_entity: service
    target_entity: service
    source_signals: [service_graph]
    evidence_scope: interaction
    connection_type: unset
  - id: relationship.calls_two
    type: relationship
    name: calls
    source_entity: service
    target_entity: service
    source_signals: [service_graph]
    evidence_scope: interaction
    connection_type: database
""".lstrip(),
        encoding="utf-8",
    )

    with pytest.raises(AssertionError, match="duplicate extension relationship triple"):
        validate_extension_model(UPSTREAM_MODEL, extension_model)
