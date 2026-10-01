from __future__ import annotations

from extended_otel_semconv import AppEndpoint, EtlPartRun, EtlPipeline, EtlRun, K8sPod, Service, Transaction
from extended_otel_semconv.entities import semantic_entity_from_data
from extended_otel_semconv.generated import __all__ as generated_exports


def test_generated_public_api_includes_upstream_and_extension_entities() -> None:
    assert Service.entity_type == "service"
    assert K8sPod.entity_type == "k8s.pod"
    assert AppEndpoint.entity_type == "app.endpoint"
    assert Transaction.entity_type == "transaction"


def test_transaction_identity_includes_native_span_name_and_kind() -> None:
    internal = semantic_entity_from_data(
        "transaction",
        "stored:transaction:internal",
        {
            "service.name": "worker",
            "span.name": "consume",
            "span.kind": "SPAN_KIND_INTERNAL",
        },
    )
    consumer = semantic_entity_from_data(
        "transaction",
        "stored:transaction:consumer",
        {
            "service.name": "worker",
            "span.name": "consume",
            "span.kind": "SPAN_KIND_CONSUMER",
        },
    )

    assert isinstance(internal, Transaction)
    assert isinstance(consumer, Transaction)
    assert Transaction.identity_fields == ("service.name", "span.name", "span.kind")
    assert internal.entity_id == "stored:transaction:internal"
    assert consumer.entity_id == "stored:transaction:consumer"


def test_generated_etl_models_use_hierarchical_identity() -> None:
    pipeline = semantic_entity_from_data(
        "etl.pipeline",
        "stored:pipeline",
        {
            "etl.pipeline.id": "extract-customers",
            "etl.pipeline.name": "Extract Customers",
        },
    )
    run = semantic_entity_from_data(
        "etl.run",
        "stored:run",
        {
            "etl.pipeline.id": "extract-customers",
            "etl.run.id": "run-001",
            "etl.run.name": "September import",
        },
    )
    part_run = semantic_entity_from_data(
        "etl.part.run",
        "stored:part-run",
        {
            "etl.pipeline.id": "extract-customers",
            "etl.run.id": "run-001",
            "etl.part.run.id": "extract-source-001",
            "etl.part.name": "Extract source",
        },
    )

    assert isinstance(pipeline, EtlPipeline)
    assert isinstance(run, EtlRun)
    assert isinstance(part_run, EtlPartRun)
    assert pipeline.entity_id == "stored:pipeline"
    assert run.entity_id == "stored:run"
    assert part_run.entity_id == "stored:part-run"


def test_etl_display_names_do_not_change_identity() -> None:
    original = semantic_entity_from_data(
        "etl.run",
        "stored:run",
        {
            "etl.pipeline.id": "extract-customers",
            "etl.run.id": "run-001",
            "etl.run.name": "Initial name",
        },
    )
    renamed = semantic_entity_from_data(
        "etl.run",
        "stored:run",
        {
            "etl.pipeline.id": "extract-customers",
            "etl.run.id": "run-001",
            "etl.run.name": "Renamed run",
        },
    )

    assert original.entity_id == renamed.entity_id


def test_entities_without_identifying_refs_are_not_generated() -> None:
    assert "Browser" not in generated_exports
    assert "Cloud" not in generated_exports
