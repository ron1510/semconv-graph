from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
COLLECTOR = ROOT / "deploy" / "helm" / "servicegraph-collector"
FLINK = ROOT / "deploy" / "helm" / "servicegraph-flink"
HELM = shutil.which("helm")
pytestmark = pytest.mark.skipif(HELM is None, reason="Helm CLI is required for contract renders")


def _render(chart: Path, *settings: str) -> str:
    assert HELM is not None
    command = [HELM, "template", "contract", str(chart)]
    for setting in settings:
        command.extend(("--set", setting))
    result = subprocess.run(command, check=False, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    return result.stdout


def test_collector_renders_only_protobuf_evidence_exporters() -> None:
    manifests = _render(COLLECTOR, "rootSpanDiscovery.enabled=true")
    assert manifests.count("encoding: otlp_proto") == 2
    assert 'metric.name != "traces_service_graph_request_total"' in manifests
    assert 'metric.name != "semconv.graph.discovery.calls"' in manifests
    assert "not IsRootSpan()" in manifests
    assert "span.kind == SPAN_KIND_CLIENT" in manifests
    assert "span.kind == SPAN_KIND_SERVER" in manifests
    assert "span_metrics/root_span_discovery" in manifests
    for removed in (
        "otlp_json",
        "request_failed_total",
        "entity_events",
        "entityEvents",
        "otel.entity.events",
        "logs/entity",
    ):
        assert removed not in manifests


def test_flink_render_has_one_evidence_input_lane() -> None:
    manifests = _render(FLINK)
    assert "INTERACTION_DIFF_INPUT_TOPIC" in manifests
    assert "otel.servicegraph.metrics" in manifests
    for removed in ("ENTITY_EVENTS", "entityEvents", "otel.entity.events"):
        assert removed not in manifests
