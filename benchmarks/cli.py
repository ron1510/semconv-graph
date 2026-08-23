"""Command-line entrypoint for the in-process benchmark harness."""

from __future__ import annotations

import argparse
import json
import platform
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import cast

from benchmarks.servicegraph import BenchmarkConfig, BenchmarkReport, DatasetConfig, run_benchmark


def main(arguments: Sequence[str] | None = None) -> int:
    parser = _parser()
    namespace = parser.parse_args(arguments)
    if sys.version_info[:2] != (3, 12) or platform.python_implementation() != "CPython":
        parser.error("the benchmark harness requires CPython 3.12")

    config = BenchmarkConfig(
        dataset=DatasetConfig(
            entities=cast(int, namespace.entities),
            contributors=cast(int, namespace.contributors),
            seed=cast(int, namespace.seed),
        ),
        iterations=cast(int, namespace.iterations),
        warmup_iterations=cast(int, namespace.warmup),
    )
    report = run_benchmark(config)
    encoded = json.dumps(report, indent=2, sort_keys=True) + "\n"

    json_output = cast(str | None, namespace.json_output)
    if json_output == "-":
        sys.stdout.write(encoded)
    elif json_output is not None:
        path = Path(json_output)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(encoded, encoding="utf-8", newline="\n")
        _print_human_summary(report)
        print(f"JSON report: {path}")
    else:
        _print_human_summary(report)
    return 0


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m benchmarks",
        description="Benchmark in-process service-graph extraction and lifecycle logic.",
    )
    parser.add_argument("--iterations", type=_positive_int, default=10)
    parser.add_argument("--warmup", type=_nonnegative_int, default=2, help="unmeasured warmup iterations")
    parser.add_argument("--entities", type=_positive_int, default=100, help="number of service entities")
    parser.add_argument("--contributors", type=_positive_int, default=3, help="contributors per dependency")
    parser.add_argument("--seed", type=int, default=20260823, help="stable deterministic dataset seed")
    parser.add_argument(
        "--json",
        dest="json_output",
        metavar="PATH",
        help="explicitly write machine-readable JSON; use '-' for stdout",
    )
    return parser


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return parsed


def _nonnegative_int(value: str) -> int:
    parsed = int(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be zero or greater")
    return parsed


def _print_human_summary(report: BenchmarkReport) -> None:
    config = report["config"]
    dataset = report["dataset"]
    ingest = report["results"]["ingest"]
    lifecycle = report["results"]["lifecycle"]
    cardinality = lifecycle["state_cardinality"]
    print("In-process service-graph benchmark")
    print("Scope: CPython parsing/extraction and lifecycle functions only")
    print(
        "Dataset: "
        f"{dataset['datapoints']} datapoints, {config['entities']} services, "
        f"{config['contributors']} contributors/dependency"
    )
    print(
        "Ingest: "
        f"{ingest['operations_per_second']:,.0f} datapoints/s, "
        f"p50 batch {ingest['batch_latency_ms']['p50']:.3f} ms"
    )
    print(
        "Lifecycle: "
        f"{lifecycle['operations_per_second']:,.0f} contributions/s, "
        f"p50 batch {lifecycle['batch_latency_ms']['p50']:.3f} ms"
    )
    print(
        "Final state: "
        f"{cardinality['active_elements']} elements, "
        f"{cardinality['contributor_snapshots']} contributor snapshots"
    )
    print("Does not measure or claim Flink, Kafka, network, checkpoint, or ArangoDB scale.")
