from __future__ import annotations

import pytest
from pydantic import ValidationError

from extended_otel_semconv import (
    K8sPod,
    Process,
    Service,
    ServiceCallsServiceEdge,
    ServiceExecutesTransactionEdge,
)
from extended_otel_semconv.edges import semantic_edge_from_data
from extended_otel_semconv.entities import semantic_entity_from_data
from extended_otel_semconv.errors import SemanticModelValidationError, UnknownSemanticTypeError
from extended_otel_semconv.generated import EDGE_MODELS, ENTITY_MODELS
from extended_otel_semconv.relationships import graph_relationships


def test_relationship_metadata_is_loaded_once_per_process() -> None:
    first = graph_relationships()

    assert graph_relationships() is first
    executes = next(item for item in first if item.name == "executes")
    calls = next(item for item in first if item.name == "calls")
    assert executes.source_signals == ("span_metrics",)
    assert calls.source_signals == ("service_graph",)


def test_generated_registries_cover_entities_and_relationships() -> None:
    assert "service" in ENTITY_MODELS
    assert len(EDGE_MODELS) == 36
    assert EDGE_MODELS[("service", "calls", "service")] is ServiceCallsServiceEdge
    assert EDGE_MODELS[("service", "executes", "transaction")] is ServiceExecutesTransactionEdge

    with pytest.raises(TypeError):
        ENTITY_MODELS["invalid"] = Service  # type: ignore[index]
    with pytest.raises(TypeError):
        EDGE_MODELS[("service", "invalid", "service")] = ServiceCallsServiceEdge  # type: ignore[index]


def test_entity_reconstruction_preserves_the_stored_identity() -> None:
    entity = semantic_entity_from_data(
        "service",
        "service:identity-owned-by-java",
        {"service.name": "checkout", "service.version": "1.4.0"},
    )

    assert isinstance(entity, Service)
    assert entity.element_id == "service:identity-owned-by-java"
    assert entity.entity_id == entity.element_id
    assert entity.service_name == "checkout"
    assert entity.service_version == "1.4.0"
    assert entity.model_dump(by_alias=True) == {
        "element_id": "service:identity-owned-by-java",
        "service.criticality": None,
        "service.name": "checkout",
        "service.version": "1.4.0",
    }


def test_entity_reconstruction_rejects_unknown_missing_and_invalid_schema_data() -> None:
    with pytest.raises(UnknownSemanticTypeError, match="unknown"):
        semantic_entity_from_data("unknown", "opaque", {})
    with pytest.raises(SemanticModelValidationError, match="invalid Service attributes"):
        semantic_entity_from_data("service", "service:stored", {})
    with pytest.raises(SemanticModelValidationError, match="element_id"):
        semantic_entity_from_data("service", "", {"service.name": "checkout"})


@pytest.mark.parametrize("invalid_name", ["", True, 42, b"checkout"])
def test_entity_attributes_remain_nonempty_and_strict(invalid_name: object) -> None:
    with pytest.raises(SemanticModelValidationError, match="invalid Service attributes"):
        semantic_entity_from_data("service", "service:stored", {"service.name": invalid_name})


def test_arrays_templates_and_enums_are_typed_and_immutable() -> None:
    process = semantic_entity_from_data(
        "process",
        "process:stored",
        {
            "process.pid": 42,
            "process.creation.time": "2026-08-12T10:00:00Z",
            "process.command_args": ["python", "-m", "worker"],
        },
    )
    pod = semantic_entity_from_data(
        "k8s.pod",
        "k8s.pod:stored",
        {
            "k8s.pod.uid": "pod-1",
            "k8s.pod.label.app": "checkout",
            "k8s.pod.annotation.owners": "sre",
        },
    )

    assert isinstance(process, Process)
    assert process.process_command_args == ("python", "-m", "worker")
    assert isinstance(pod, K8sPod)
    assert pod.semantic_attributes() == {
        "k8s.pod.annotation.owners": "sre",
        "k8s.pod.label.app": "checkout",
        "k8s.pod.uid": "pod-1",
    }
    with pytest.raises(TypeError):
        pod.k8s_pod_label["team"] = "platform"  # type: ignore[index]
    with pytest.raises(SemanticModelValidationError, match="process.command_args"):
        semantic_entity_from_data(
            "process",
            "process:stored",
            {
                "process.pid": 42,
                "process.creation.time": "2026-08-12T10:00:00Z",
                "process.command_args": "python",
            },
        )
    with pytest.raises(SemanticModelValidationError, match="service.criticality"):
        semantic_entity_from_data(
            "service",
            "service:stored",
            {"service.name": "checkout", "service.criticality": "urgent"},
        )


def test_edge_reconstruction_preserves_the_stored_identity() -> None:
    edge = semantic_edge_from_data(
        "calls",
        "edge:opaque-java-value",
        "service:storefront",
        "service:checkout",
        attributes={"transport": "http"},
    )

    assert isinstance(edge, ServiceCallsServiceEdge)
    assert edge.element_id == "edge:opaque-java-value"
    assert edge.edge_id == edge.element_id
    assert edge.attributes == {"transport": "http"}
    assert edge.model_dump() == {
        "element_id": "edge:opaque-java-value",
        "source_id": "service:storefront",
        "target_id": "service:checkout",
        "attributes": {"transport": "http"},
    }
    with pytest.raises(TypeError):
        edge.attributes["transport"] = "grpc"  # type: ignore[index]


def test_edge_metrics_are_rejected_as_removed_contract() -> None:
    with pytest.raises(ValidationError):
        ServiceCallsServiceEdge(
            element_id="edge:stored",
            source_id="service:storefront",
            target_id="service:checkout",
            metrics={"service_graph.request.total": 1},  # type: ignore[call-arg]
        )


def test_edge_reconstruction_rejects_invalid_schema_but_not_arbitrary_identity() -> None:
    with pytest.raises(UnknownSemanticTypeError, match="no generated semantic edge"):
        semantic_edge_from_data("calls", "edge:any", "k8s.pod:one", "service:checkout")
    with pytest.raises(SemanticModelValidationError, match="invalid semantic entity ID"):
        semantic_edge_from_data("calls", "edge:any", "invalid", "service:checkout")
    with pytest.raises(ValueError, match="source_id must identify"):
        ServiceCallsServiceEdge(
            element_id="edge:any",
            source_id="k8s.pod:one",
            target_id="service:checkout",
        )
