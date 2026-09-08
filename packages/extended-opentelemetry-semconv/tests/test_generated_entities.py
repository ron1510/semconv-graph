from __future__ import annotations

from extended_otel_semconv import AppEndpoint, EtlPartRun, EtlPipeline, EtlRun, K8sPod, Service
from extended_otel_semconv.generated import __all__ as generated_exports


def test_generated_public_api_includes_upstream_and_extension_entities() -> None:
    assert Service.entity_type == "service"
    assert K8sPod.entity_type == "k8s.pod"
    assert AppEndpoint.entity_type == "app.endpoint"


def test_generated_etl_models_use_hierarchical_identity() -> None:
    pipeline = EtlPipeline.model_validate(
        {
            "etl.pipeline.id": "extract-customers",
            "etl.pipeline.name": "Extract Customers",
        }
    )
    run = EtlRun.model_validate(
        {
            "etl.pipeline.id": "extract-customers",
            "etl.run.id": "run-001",
            "etl.run.name": "September import",
        }
    )
    part_run = EtlPartRun.model_validate(
        {
            "etl.pipeline.id": "extract-customers",
            "etl.run.id": "run-001",
            "etl.part.run.id": "extract-source-001",
            "etl.part.name": "Extract source",
        }
    )

    assert pipeline.entity_id == "etl.pipeline:extract-customers"
    assert run.entity_id == "etl.run:extract-customers:run-001"
    assert part_run.entity_id == "etl.part.run:extract-customers:run-001:extract-source-001"


def test_etl_display_names_do_not_change_identity() -> None:
    original = EtlRun.model_validate(
        {
            "etl.pipeline.id": "extract-customers",
            "etl.run.id": "run-001",
            "etl.run.name": "Initial name",
        }
    )
    renamed = EtlRun.model_validate(
        {
            "etl.pipeline.id": "extract-customers",
            "etl.run.id": "run-001",
            "etl.run.name": "Renamed run",
        }
    )

    assert original.entity_id == renamed.entity_id


def test_entities_without_identifying_refs_are_not_generated() -> None:
    assert "Browser" not in generated_exports
    assert "Cloud" not in generated_exports
