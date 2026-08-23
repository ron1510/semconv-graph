# In-Process Service-Graph Benchmarks

This directory contains a reproducible Python 3.12 benchmark for two
project-owned, framework-independent stages:

1. Parsing deterministic Collector servicegraph OTLP JSON and extracting graph
   contributions.
2. Applying those contributions to the contributor-aware graph lifecycle
   function.

The generated workload is byte-stable for a given seed. It creates a
deterministic ring of service dependencies, with a configurable number of
service entities and contributors per dependency. The harness performs
unmeasured warmup iterations before collecting latency samples.

The payload generator is also available directly when a fixture is needed:

```python
from pathlib import Path

from benchmarks import DatasetConfig, generate_otlp_json

payload = generate_otlp_json(DatasetConfig(entities=100, contributors=3, seed=20260823))
Path("servicegraph-payload.json").write_text(payload, encoding="utf-8")
```

## Run

Use the repository's Python 3.12 development environment, with the semantic SDK
and Flink service installed as they are for normal development:

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\pip.exe install -e ".\packages\extended-opentelemetry-semconv"
.\.venv\Scripts\pip.exe install -e ".\services\otel-servicegraph-diff"
.\.venv\Scripts\python.exe -m benchmarks
```

Configure the workload explicitly when comparing runs:

```powershell
.\.venv\Scripts\python.exe -m benchmarks `
  --warmup 5 `
  --iterations 30 `
  --entities 1000 `
  --contributors 4 `
  --seed 20260823
```

By default, the command prints a short human-readable summary and writes
nothing. Machine-readable output is created only when `--json` is supplied:

```powershell
.\.venv\Scripts\python.exe -m benchmarks --json .tmp\benchmark.json
.\.venv\Scripts\python.exe -m benchmarks --json -
```

The JSON report records the workload configuration, payload digest, platform
metadata, batch and amortized operation latency, throughput, emitted event
count, active graph elements, and contributor-snapshot cardinality. Compare
results only when configuration, payload digest, Python version, platform, and
available CPU resources are equivalent.

Run the focused deterministic and schema checks with:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\benchmarks
.\.venv\Scripts\python.exe -m ruff check benchmarks tests\benchmarks
.\.venv\Scripts\python.exe -m pyright benchmarks tests\benchmarks
```

## What This Proves

- A deterministic OTLP JSON workload is accepted by the current ingest code.
- Pure Python ingest contribution extraction can be measured independently.
- Pure Python lifecycle application can be measured independently.
- State cardinality can be observed as entity and contributor counts grow.
- Changes to these functions can be compared on the same machine and workload.

## What This Does Not Prove

This is a microbenchmark. It does **not** run or measure:

- Flink scheduling, serialization, checkpoints, recovery, backpressure, or HA.
- Kafka production, consumption, partitions, replication, or consumer lag.
- Collector processing or network transport.
- ArangoDB writes, graph storage, indexes, or Gremlin queries.
- Kubernetes resource use, horizontal scalability, availability, or cost.

Do not publish these results as distributed system throughput or capacity. They
measure one CPython process and are useful for regression detection and local
profiling only.

## Future Distributed Benchmarks

Distributed evidence should be a separate harness and report:

1. Provision an isolated, version-pinned environment with declared CPU, memory,
   storage, network, Kafka partition count, Flink parallelism, checkpoint
   interval, and ArangoDB topology.
2. Send deterministic telemetry through the real Collector endpoint rather
   than calling Python functions directly.
3. Measure accepted input rate, end-to-end visibility latency, Kafka lag, Flink
   busy/backpressured time, checkpoint duration and size, keyed-state growth,
   ArangoDB write latency, and rejected or replayed records.
4. Include steady-state, burst, restart, and recovery phases. Verify resulting
   graph cardinality and lifecycle deletions, not only process throughput.
5. Repeat runs, retain raw machine-readable observations, publish percentile
   distributions, and distinguish measured facts from extrapolations.
6. Scale one dimension at a time: input rate, Kafka partitions, Flink
   parallelism, entity cardinality, contributor cardinality, and database
   capacity.

Those tests should live separately from this microbenchmark so local function
performance is never confused with production-system behavior.
