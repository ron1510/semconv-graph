"""Reproducible in-process service-graph benchmarks."""

from benchmarks.servicegraph import (
    BenchmarkConfig,
    DatasetConfig,
    generate_otlp_json,
    run_benchmark,
)

__all__ = [
    "BenchmarkConfig",
    "DatasetConfig",
    "generate_otlp_json",
    "run_benchmark",
]
