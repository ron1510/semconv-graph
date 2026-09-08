from __future__ import annotations

import pytest
from pydantic import ValidationError

from extended_otel_semconv import K8sPod, Process, Service, ServiceCallsServiceEdge
from extended_otel_semconv.edges import edge_id, semantic_edge_from_data
from extended_otel_semconv.entities import entity_from_attributes
from extended_otel_semconv.errors import (
    SemanticIdentityMismatchError,
    SemanticModelValidationError,
    UnknownSemanticTypeError,
)
from extended_otel_semconv.generated import EDGE_MODELS, ENTITY_MODELS
from extended_otel_semconv.relationships import service_graph_relationships


def test_relationship_metadata_is_loaded_once_per_process() -> None:
    first = service_graph_relationships()

    assert service_graph_relationships() is first


def test_generated_registries_cover_entities_and_relationships() -> None:
    assert "service" in ENTITY_MODELS
    assert len(EDGE_MODELS) == 35
    assert EDGE_MODELS[("service", "calls", "service")] is ServiceCallsServiceEdge

    with pytest.raises(TypeError):
        ENTITY_MODELS["invalid"] = Service  # type: ignore[index]
    with pytest.raises(TypeError):
        EDGE_MODELS[("service", "invalid", "service")] = ServiceCallsServiceEdge  # type: ignore[index]


def test_strict_entity_reconstruction_preserves_optional_fields_and_identity() -> None:
    entity = entity_from_attributes(
        "service",
        {"service.name": "checkout", "service.version": "1.4.0"},
        expected_id="service:checkout",
    )

    assert isinstance(entity, Service)
    assert entity.service_name == "checkout"
    assert entity.service_version == "1.4.0"


def test_strict_entity_reconstruction_rejects_unknown_missing_and_mismatched_data() -> None:
    with pytest.raises(UnknownSemanticTypeError, match="unknown"):
        entity_from_attributes("unknown", {})
    with pytest.raises(SemanticModelValidationError, match="identifying fields"):
        entity_from_attributes("service", {})
    with pytest.raises(SemanticIdentityMismatchError, match="does not match"):
        entity_from_attributes("service", {"service.name": "checkout"}, expected_id="service:other")


@pytest.mark.parametrize("invalid_name", ["", True, 42, b"checkout"])
def test_entity_identity_is_nonempty_and_strict(invalid_name: object) -> None:
    with pytest.raises(SemanticModelValidationError, match="invalid Service attributes"):
        Service.from_attributes({"service.name": invalid_name})


def test_arrays_templates_and_enums_are_typed_and_immutable() -> None:
    process = Process.from_attributes(
        {
            "process.pid": 42,
            "process.creation.time": "2026-08-12T10:00:00Z",
            "process.command_args": ["python", "-m", "worker"],
        }
    )
    pod = K8sPod.from_attributes(
        {
            "k8s.pod.uid": "pod-1",
            "k8s.pod.label.app": "checkout",
            "k8s.pod.annotation.owners": "sre",
        }
    )

    assert process is not None
    assert process.process_command_args == ("python", "-m", "worker")
    assert pod is not None
    assert pod.semantic_attributes() == {
        "k8s.pod.annotation.owners": "sre",
        "k8s.pod.label.app": "checkout",
        "k8s.pod.uid": "pod-1",
    }
    with pytest.raises(TypeError):
        pod.k8s_pod_label["team"] = "platform"  # type: ignore[index]
    with pytest.raises(SemanticModelValidationError, match="process.command_args"):
        Process.from_attributes(
            {
                "process.pid": 42,
                "process.creation.time": "2026-08-12T10:00:00Z",
                "process.command_args": "python",
            }
        )
    with pytest.raises(SemanticModelValidationError, match="service.criticality"):
        Service.from_attributes({"service.name": "checkout", "service.criticality": "urgent"})


def test_concrete_edge_reconstruction_preserves_metrics_and_identity() -> None:
    expected_id = edge_id("service:storefront", "calls", "service:checkout")
    edge = semantic_edge_from_data(
        "calls",
        "service:storefront",
        "service:checkout",
        metrics={"service_graph.request.total": 12.0},
        expected_id=expected_id,
    )

    assert isinstance(edge, ServiceCallsServiceEdge)
    assert edge.metrics == {"service_graph.request.total": 12.0}
    assert edge.edge_id == expected_id
    with pytest.raises(TypeError):
        edge.metrics["service_graph.request.total"] = 13.0  # type: ignore[index]


@pytest.mark.parametrize("invalid_metric", [True, float("nan"), float("inf")])
def test_edge_metrics_are_finite_numbers_not_booleans(invalid_metric: object) -> None:
    with pytest.raises(ValidationError):
        ServiceCallsServiceEdge(
            source_id="service:storefront",
            target_id="service:checkout",
            metrics={"service_graph.request.total": invalid_metric},  # type: ignore[dict-item]
        )


def test_edge_reconstruction_rejects_endpoints_relationships_and_identity_mismatches() -> None:
    with pytest.raises(UnknownSemanticTypeError, match="no generated semantic edge"):
        semantic_edge_from_data("calls", "k8s.pod:one", "service:checkout")
    with pytest.raises(SemanticModelValidationError, match="invalid semantic entity ID"):
        semantic_edge_from_data("calls", "invalid", "service:checkout")
    with pytest.raises(SemanticIdentityMismatchError, match="does not match"):
        semantic_edge_from_data(
            "calls",
            "service:storefront",
            "service:checkout",
            expected_id="edge:not-the-real-id",
        )
    with pytest.raises(ValueError, match="source_id must identify"):
        ServiceCallsServiceEdge(source_id="k8s.pod:one", target_id="service:checkout")
