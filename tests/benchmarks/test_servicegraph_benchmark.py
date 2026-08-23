from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import cast

import pytest

from benchmarks.cli import main
from benchmarks.servicegraph import BenchmarkConfig, DatasetConfig, generate_otlp_json, run_benchmark


def test_generated_payload_is_deterministic_for_seed() -> None:
    config = DatasetConfig(entities=5, contributors=3, seed=42)

    first = generate_otlp_json(config)
    second = generate_otlp_json(config)

    assert first == second
    assert hashlib.sha256(first.encode()).hexdigest() == (
        "b95dc227c68e02a72fbc978efd056a4a83415cc04767bc448e91426df3aa2367"
    )
    assert generate_otlp_json(DatasetConfig(entities=5, contributors=3, seed=43)) != first


def test_generated_payload_has_configured_delta_datapoints() -> None:
    payload = _mapping(json.loads(generate_otlp_json(DatasetConfig(entities=4, contributors=2, seed=7))))
    resource_metrics = _mapping(_sequence(payload["resourceMetrics"])[0])
    scope_metrics = _mapping(_sequence(resource_metrics["scopeMetrics"])[0])
    metric = _mapping(_sequence(scope_metrics["metrics"])[0])
    metric_sum = _mapping(metric["sum"])

    assert metric["name"] == "traces_service_graph_request_total"
    assert metric_sum["aggregationTemporality"] == 1
    assert len(_sequence(metric_sum["dataPoints"])) == 8


def test_configuration_rejects_invalid_cardinality() -> None:
    with pytest.raises(ValueError, match="entities"):
        DatasetConfig(entities=0)
    with pytest.raises(ValueError, match="contributors"):
        DatasetConfig(contributors=0)
    with pytest.raises(ValueError, match="iterations"):
        BenchmarkConfig(iterations=0)
    with pytest.raises(ValueError, match="warmup_iterations"):
        BenchmarkConfig(warmup_iterations=-1)


def test_report_has_sane_stable_schema_and_cardinality() -> None:
    report = run_benchmark(
        BenchmarkConfig(
            dataset=DatasetConfig(entities=3, contributors=2, seed=11),
            iterations=2,
            warmup_iterations=1,
        )
    )

    assert report["schema_version"] == "1.0"
    assert report["scope"] == "in_process_python"
    assert report["config"] == {
        "iterations": 2,
        "warmup_iterations": 1,
        "entities": 3,
        "contributors": 2,
        "seed": 11,
    }
    assert report["dataset"]["datapoints"] == 6
    assert len(report["dataset"]["payload_sha256"]) == 64
    assert report["platform"]["python_implementation"] == "CPython"

    ingest = report["results"]["ingest"]
    lifecycle = report["results"]["lifecycle"]
    assert ingest["unit"] == "datapoint"
    assert ingest["total_operations"] == 12
    assert ingest["contributions_per_iteration"] == 18
    assert ingest["operations_per_second"] > 0
    assert lifecycle["unit"] == "contribution"
    assert lifecycle["total_operations"] == 36
    assert lifecycle["operations_per_second"] > 0
    assert lifecycle["state_cardinality"] == {
        "active_elements": 6,
        "node_elements": 3,
        "edge_elements": 3,
        "contributor_snapshots": 18,
    }
    latency = lifecycle["batch_latency_ms"]
    assert min(
        latency["min"],
        latency["p50"],
        latency["p95"],
        latency["p99"],
        latency["max"],
        latency["mean"],
    ) >= 0
    json.dumps(report, allow_nan=False)


def test_cli_writes_json_only_when_requested(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    arguments = ["--entities", "2", "--contributors", "1", "--iterations", "1", "--warmup", "0"]

    assert main(arguments) == 0
    assert "In-process service-graph benchmark" in capsys.readouterr().out
    assert list(tmp_path.iterdir()) == []

    output = tmp_path / "nested" / "result.json"
    assert main([*arguments, "--json", str(output)]) == 0
    report = json.loads(output.read_text(encoding="utf-8"))
    report_object = _mapping(report)
    assert report_object["scope"] == "in_process_python"
    assert "does not" in " ".join(str(item) for item in _sequence(report_object["limitations"])).lower()


def test_cli_can_emit_explicit_json_to_stdout(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(
        [
            "--entities",
            "2",
            "--contributors",
            "1",
            "--iterations",
            "1",
            "--warmup",
            "0",
            "--json",
            "-",
        ]
    ) == 0

    report = _mapping(json.loads(capsys.readouterr().out))
    results = _mapping(report["results"])
    lifecycle = _mapping(results["lifecycle"])
    cardinality = _mapping(lifecycle["state_cardinality"])
    assert report["schema_version"] == "1.0"
    assert cardinality["active_elements"] == 4


def _mapping(value: object) -> Mapping[str, object]:
    if not isinstance(value, dict):
        raise TypeError("expected a JSON object")
    return cast(Mapping[str, object], value)


def _sequence(value: object) -> Sequence[object]:
    if not isinstance(value, list):
        raise TypeError("expected a JSON array")
    return cast(Sequence[object], value)
