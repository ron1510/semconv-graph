from __future__ import annotations

import httpx
import pytest
from etl_context import configure_etl_instrumentation, etl_part, etl_run
from opentelemetry import trace
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter


def test_etl_context_enriches_instrumented_spans_without_propagating_baggage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = TracerProvider(
        resource=Resource.create(
            {
                "service.name": "etl-worker",
                "etl.pipeline.id": "extract-customers",
                "etl.pipeline.name": "Extract Customers",
            }
        )
    )
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    monkeypatch.setattr(trace, "get_tracer_provider", lambda: provider)
    configure_etl_instrumentation()
    observed_headers: list[httpx.Headers] = []

    def respond(request: httpx.Request) -> httpx.Response:
        observed_headers.append(request.headers)
        return httpx.Response(200, request=request)

    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        HTTPXClientInstrumentor.instrument_client(client, tracer_provider=provider)
        try:
            with (
                etl_run(run_id="run-001", name="Customer import"),
                etl_part(part_run_id="load-customers", name="Load customers"),
            ):
                client.get("http://customer-source.test/customers").raise_for_status()
            client.get("http://customer-source.test/health").raise_for_status()
        finally:
            HTTPXClientInstrumentor.uninstrument_client(client)
            provider.shutdown()

    spans = exporter.get_finished_spans()
    assert len(spans) == 2
    enriched = next(span for span in spans if "etl.run.id" in (span.attributes or {}))
    assert enriched.resource.attributes["etl.pipeline.id"] == "extract-customers"
    assert enriched.attributes is not None
    assert enriched.attributes["etl.run.id"] == "run-001"
    assert enriched.attributes["etl.run.name"] == "Customer import"
    assert enriched.attributes["etl.part.run.id"] == "load-customers"
    assert enriched.attributes["etl.part.name"] == "Load customers"

    plain = next(span for span in spans if "etl.run.id" not in (span.attributes or {}))
    assert "etl.part.run.id" not in (plain.attributes or {})
    assert all("baggage" not in headers for headers in observed_headers)
