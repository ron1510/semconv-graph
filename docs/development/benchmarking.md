# Benchmarking

The repository includes a deterministic in-process benchmark for the two
framework-independent hot paths owned by this project:

1. parsing Collector service-graph OTLP JSON and extracting contributions;
2. applying contributions to graph-element lifecycle state.

Run it from the Python 3.12 development environment:

```powershell
.\.venv\Scripts\python.exe -m benchmarks `
  --warmup 5 `
  --iterations 30 `
  --entities 1000 `
  --contributors 4 `
  --seed 20260823
```

Add `--json .tmp\benchmark.json` for a machine-readable report. The report
records the deterministic payload digest, runtime and platform metadata,
throughput, latency percentiles, emitted events, graph elements, and contributor
snapshots. Compare runs only when the workload and host conditions match.

This is a regression and profiling tool, not a distributed capacity claim. It
does not run Flink, Kafka, Collector networking, checkpoints, ArangoDB, Gremlin,
or Kubernetes. See [`benchmarks/README.md`](https://github.com/ron1510/extended-opentelemetry-semconv/blob/main/benchmarks/README.md)
for the measurement boundaries and the required shape of a future distributed
benchmark.
