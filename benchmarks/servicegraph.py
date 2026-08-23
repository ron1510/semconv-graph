"""Deterministic datasets and measurements for project-owned pure Python logic."""

from __future__ import annotations

import hashlib
import json
import math
import os
import platform
import sys
from dataclasses import dataclass
from time import get_clock_info, perf_counter_ns
from typing import Final, Literal, TypedDict

from otel_servicegraph_diff.engine.elements import (
    GraphContribution,
    GraphElementState,
    GraphNode,
    apply_contribution,
)
from otel_servicegraph_diff.ingest import IngestRejection, iter_otlp_json_contributions

SCHEMA_VERSION: Final = "1.0"
BASE_TIMESTAMP_UNIX_NANO: Final = 1_800_000_000_000_000_000
TTL_SECONDS: Final = 3_600
LIMITATIONS: Final = (
    "Runs project-owned parsing, extraction, and lifecycle functions in one CPython process.",
    "Does not start or measure Flink, Kafka, networking, checkpoints, ArangoDB, Gremlin, or Kubernetes.",
    "Does not establish distributed throughput, latency, capacity, availability, or infrastructure cost.",
)


@dataclass(frozen=True, slots=True)
class DatasetConfig:
    entities: int = 100
    contributors: int = 3
    seed: int = 20_260_823

    def __post_init__(self) -> None:
        if self.entities <= 0:
            raise ValueError("entities must be greater than zero")
        if self.contributors <= 0:
            raise ValueError("contributors must be greater than zero")


@dataclass(frozen=True, slots=True)
class BenchmarkConfig:
    dataset: DatasetConfig = DatasetConfig()
    iterations: int = 10
    warmup_iterations: int = 2

    def __post_init__(self) -> None:
        if self.iterations <= 0:
            raise ValueError("iterations must be greater than zero")
        if self.warmup_iterations < 0:
            raise ValueError("warmup_iterations must be zero or greater")


class BenchmarkContractError(RuntimeError):
    """Raised when the generated workload no longer satisfies ingest contracts."""


class LatencySummary(TypedDict):
    min: float
    p50: float
    p95: float
    p99: float
    max: float
    mean: float


class TimingResult(TypedDict):
    unit: str
    iterations: int
    operations_per_iteration: int
    total_operations: int
    total_seconds: float
    operations_per_second: float
    batch_latency_ms: LatencySummary
    amortized_operation_latency_us: LatencySummary


class IngestResult(TimingResult):
    contributions_per_iteration: int


class StateCardinality(TypedDict):
    active_elements: int
    node_elements: int
    edge_elements: int
    contributor_snapshots: int


class LifecycleResult(TimingResult):
    events_emitted: int
    state_cardinality: StateCardinality


class ReportConfig(TypedDict):
    iterations: int
    warmup_iterations: int
    entities: int
    contributors: int
    seed: int


class PlatformMetadata(TypedDict):
    python_version: str
    python_implementation: str
    system: str
    release: str
    machine: str
    processor: str
    logical_cpu_count: int | None
    perf_counter_resolution_seconds: float
    hash_randomization_enabled: bool


class DatasetMetadata(TypedDict):
    format: Literal["otlp_json_metrics"]
    metric: Literal["traces_service_graph_request_total"]
    temporality: Literal["delta"]
    datapoints: int
    payload_bytes: int
    payload_sha256: str


class BenchmarkResults(TypedDict):
    ingest: IngestResult
    lifecycle: LifecycleResult


class BenchmarkReport(TypedDict):
    schema_version: Literal["1.0"]
    scope: Literal["in_process_python"]
    limitations: list[str]
    config: ReportConfig
    platform: PlatformMetadata
    dataset: DatasetMetadata
    results: BenchmarkResults


def generate_otlp_json(config: DatasetConfig) -> str:
    """Return byte-stable Collector servicegraph OTLP JSON for ``config``."""
    services = [f"service-{index:06d}" for index in range(config.entities)]
    ordered_services = sorted(services, key=lambda name: _digest(f"{config.seed}:{name}"))
    points: list[dict[str, object]] = []
    for source_index, client in enumerate(ordered_services):
        server = ordered_services[(source_index + 1) % len(ordered_services)]
        for contributor_index in range(config.contributors):
            token = f"{config.seed}:{client}:{server}:{contributor_index}"
            delta = int(_digest(token)[:8], 16) % 10 + 1
            observed_at = BASE_TIMESTAMP_UNIX_NANO + len(points) * 1_000_000 + abs(config.seed) % 1_000_000
            attributes = {
                "client": client,
                "client_service.version": f"version-{contributor_index:04d}",
                "server": server,
                "server_service.version": f"version-{contributor_index:04d}",
            }
            points.append(
                {
                    "asInt": str(delta),
                    "attributes": [_otlp_attribute(name, value) for name, value in sorted(attributes.items())],
                    "startTimeUnixNano": str(observed_at - 1_000_000),
                    "timeUnixNano": str(observed_at),
                }
            )

    document = {
        "resourceMetrics": [
            {
                "scopeMetrics": [
                    {
                        "metrics": [
                            {
                                "name": "traces_service_graph_request_total",
                                "sum": {
                                    "aggregationTemporality": 1,
                                    "dataPoints": points,
                                    "isMonotonic": True,
                                },
                            }
                        ],
                        "scope": {"name": "servicegraph-benchmark", "version": SCHEMA_VERSION},
                    }
                ]
            }
        ]
    }
    return json.dumps(document, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def run_benchmark(config: BenchmarkConfig) -> BenchmarkReport:
    """Run the two in-process stages and return a JSON-serializable report."""
    payload = generate_otlp_json(config.dataset)
    datapoints = config.dataset.entities * config.dataset.contributors
    ingest, contributions = _benchmark_ingest(payload, datapoints, config)
    lifecycle = _benchmark_lifecycle(contributions, config)
    return {
        "schema_version": SCHEMA_VERSION,
        "scope": "in_process_python",
        "limitations": list(LIMITATIONS),
        "config": {
            "iterations": config.iterations,
            "warmup_iterations": config.warmup_iterations,
            "entities": config.dataset.entities,
            "contributors": config.dataset.contributors,
            "seed": config.dataset.seed,
        },
        "platform": _platform_metadata(),
        "dataset": {
            "format": "otlp_json_metrics",
            "metric": "traces_service_graph_request_total",
            "temporality": "delta",
            "datapoints": datapoints,
            "payload_bytes": len(payload.encode("utf-8")),
            "payload_sha256": _digest(payload),
        },
        "results": {"ingest": ingest, "lifecycle": lifecycle},
    }


def _benchmark_ingest(
    payload: str,
    datapoints: int,
    config: BenchmarkConfig,
) -> tuple[IngestResult, tuple[GraphContribution, ...]]:
    for _ in range(config.warmup_iterations):
        _extract_contributions(payload)

    samples_ns: list[int] = []
    contributions: tuple[GraphContribution, ...] = ()
    expected_count: int | None = None
    for _ in range(config.iterations):
        started = perf_counter_ns()
        contributions = _extract_contributions(payload)
        samples_ns.append(perf_counter_ns() - started)
        if expected_count is None:
            expected_count = len(contributions)
        elif len(contributions) != expected_count:
            raise BenchmarkContractError("ingest contribution count changed between iterations")

    timing = _timing_result(samples_ns, datapoints, "datapoint")
    return IngestResult(**timing, contributions_per_iteration=len(contributions)), contributions


def _benchmark_lifecycle(
    contributions: tuple[GraphContribution, ...],
    config: BenchmarkConfig,
) -> LifecycleResult:
    warmup_states: dict[str, GraphElementState] = {}
    for iteration in range(config.warmup_iterations):
        warmup_states, _ = _apply_lifecycle_batch(warmup_states, contributions, iteration)

    states: dict[str, GraphElementState] = {}
    samples_ns: list[int] = []
    events_emitted = 0
    for iteration in range(config.iterations):
        started = perf_counter_ns()
        states, batch_events = _apply_lifecycle_batch(states, contributions, iteration)
        samples_ns.append(perf_counter_ns() - started)
        events_emitted += batch_events

    node_elements = sum(
        isinstance(next(iter(state.contributors.values())).element, GraphNode)
        for state in states.values()
    )
    timing = _timing_result(samples_ns, len(contributions), "contribution")
    return LifecycleResult(
        **timing,
        events_emitted=events_emitted,
        state_cardinality={
            "active_elements": len(states),
            "node_elements": node_elements,
            "edge_elements": len(states) - node_elements,
            "contributor_snapshots": sum(len(state.contributors) for state in states.values()),
        },
    )


def _extract_contributions(payload: str) -> tuple[GraphContribution, ...]:
    contributions: list[GraphContribution] = []
    rejections: list[IngestRejection] = []
    for result in iter_otlp_json_contributions(payload):
        match result:
            case GraphContribution():
                contributions.append(result)
            case IngestRejection():
                rejections.append(result)
    if rejections:
        reasons = ", ".join(sorted({rejection.reason for rejection in rejections}))
        raise BenchmarkContractError(f"generated dataset was rejected by ingest: {reasons}")
    if not contributions:
        raise BenchmarkContractError("generated dataset produced no graph contributions")
    return tuple(contributions)


def _apply_lifecycle_batch(
    previous: dict[str, GraphElementState],
    contributions: tuple[GraphContribution, ...],
    iteration: int,
) -> tuple[dict[str, GraphElementState], int]:
    states = dict(previous)
    events = 0
    processing_time = 1_800_000_000_000 + iteration
    for contribution in contributions:
        result = apply_contribution(
            states.get(contribution.element.id),
            contribution,
            ttl_seconds=TTL_SECONDS,
            event_expiry_base_unix_nano=contribution.observed_at_unix_nano,
            processing_time_unix_ms=processing_time,
            emitted_at_unix_ms=processing_time,
        )
        if result.state is None:
            raise BenchmarkContractError("applying a contribution unexpectedly removed graph state")
        states[contribution.element.id] = result.state
        events += result.event is not None
    return states, events


def _timing_result(samples_ns: list[int], operations_per_iteration: int, unit: str) -> TimingResult:
    if not samples_ns or operations_per_iteration <= 0:
        raise BenchmarkContractError("benchmark timing requires samples and operations")
    total_ns = sum(samples_ns)
    total_operations = operations_per_iteration * len(samples_ns)
    amortized_us = [sample / operations_per_iteration / 1_000 for sample in samples_ns]
    return {
        "unit": unit,
        "iterations": len(samples_ns),
        "operations_per_iteration": operations_per_iteration,
        "total_operations": total_operations,
        "total_seconds": total_ns / 1_000_000_000,
        "operations_per_second": total_operations / (total_ns / 1_000_000_000),
        "batch_latency_ms": _latency_summary([sample / 1_000_000 for sample in samples_ns]),
        "amortized_operation_latency_us": _latency_summary(amortized_us),
    }


def _latency_summary(samples: list[float]) -> LatencySummary:
    ordered = sorted(samples)
    return {
        "min": ordered[0],
        "p50": _percentile(ordered, 0.50),
        "p95": _percentile(ordered, 0.95),
        "p99": _percentile(ordered, 0.99),
        "max": ordered[-1],
        "mean": sum(ordered) / len(ordered),
    }


def _percentile(ordered: list[float], percentile: float) -> float:
    index = max(0, math.ceil(percentile * len(ordered)) - 1)
    return ordered[index]


def _platform_metadata() -> PlatformMetadata:
    clock = get_clock_info("perf_counter")
    return {
        "python_version": platform.python_version(),
        "python_implementation": platform.python_implementation(),
        "system": platform.system(),
        "release": platform.release(),
        "machine": platform.machine(),
        "processor": platform.processor(),
        "logical_cpu_count": os.cpu_count(),
        "perf_counter_resolution_seconds": clock.resolution,
        "hash_randomization_enabled": bool(sys.flags.hash_randomization),
    }


def _otlp_attribute(name: str, value: str) -> dict[str, object]:
    return {"key": name, "value": {"stringValue": value}}


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()
