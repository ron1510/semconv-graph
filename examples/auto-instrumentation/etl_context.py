from __future__ import annotations

from collections.abc import Generator, Mapping
from contextlib import contextmanager
from typing import cast

from opentelemetry import context, trace
from opentelemetry.context import Context
from opentelemetry.sdk.trace import ReadableSpan, Span, SpanProcessor, TracerProvider

_ETL_CONTEXT_KEY = "semconv_graph.etl.attributes"


class EtlContextSpanProcessor(SpanProcessor):
    def on_start(self, span: Span, parent_context: Context | None = None) -> None:
        for name, value in _string_attributes(context.get_value(_ETL_CONTEXT_KEY, parent_context)).items():
            span.set_attribute(name, value)

    def on_end(self, span: ReadableSpan) -> None:
        del span


def configure_etl_instrumentation() -> None:
    provider = trace.get_tracer_provider()
    if not isinstance(provider, TracerProvider):
        raise RuntimeError("OpenTelemetry SDK tracer provider is not configured")
    provider.add_span_processor(EtlContextSpanProcessor())


@contextmanager
def etl_run(*, run_id: str, name: str | None = None) -> Generator[None]:
    attributes = {"etl.run.id": run_id}
    if name is not None:
        attributes["etl.run.name"] = name
    with _etl_context(attributes):
        yield


@contextmanager
def etl_part(*, part_run_id: str, name: str | None = None) -> Generator[None]:
    attributes = {"etl.part.run.id": part_run_id}
    if name is not None:
        attributes["etl.part.name"] = name
    with _etl_context(attributes):
        yield


@contextmanager
def _etl_context(attributes: Mapping[str, str]) -> Generator[None]:
    inherited = _string_attributes(context.get_value(_ETL_CONTEXT_KEY))
    attached = context.set_value(_ETL_CONTEXT_KEY, {**inherited, **attributes})
    token = context.attach(attached)
    try:
        yield
    finally:
        context.detach(token)


def _string_attributes(value: object) -> dict[str, str]:
    if not isinstance(value, Mapping):
        return {}
    mapping = cast(Mapping[object, object], value)
    return {
        name: item
        for name, item in mapping.items()
        if isinstance(name, str) and isinstance(item, str)
    }
