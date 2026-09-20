# Semconv Graph

[![CI](https://github.com/ron1510/semconv-graph/actions/workflows/ci.yml/badge.svg)](https://github.com/ron1510/semconv-graph/actions/workflows/ci.yml)
[![Python 3.12](https://img.shields.io/badge/python-3.12-3776AB.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

**Turn OpenTelemetry traces into a live typed entity graph, with a zero-change
servicegraph path and opt-in discovery for non-interacting executions.**

Semconv Graph is the working product name for this repository. The published
Python SDK keeps the name `extended-opentelemetry-semconv`; no package or import
names are being renamed.

Most entity graph systems begin after a producer already emits inventory or
entity events. Semconv Graph starts with telemetry many teams already have. It
uses the OpenTelemetry Collector's service-graph metrics, a generated semantic
registry, and stateful lifecycle processing to infer typed nodes and edges.

```text
Existing OTLP traces
  -> trace-affine OpenTelemetry Collectors
  -> positive service-graph evidence
  -> Kafka OTLP Protobuf
  -> Flink contributor and lifecycle state
  -> graph-element upsert/delete events
  -> ArangoDB current-state graph
  -> read-only GraphBinary Gremlin
```

## What exists today

- Automatic extraction from existing service-graph telemetry; applications do
  not need to emit a new signal.
- Registry-generated Pydantic entity and relationship models.
- Organization-specific entity extensions and Collector dimensions.
- Optional ETL Pipeline, Run, and Part Run discovery from completed non-client/non-server root spans through spanmetrics
  carrying the generated model fields.
- Contributor-aware attribute merging and expiry in Flink.
- Equal lifecycle treatment for semantic nodes and edges.
- Deterministic, compactable Kafka upsert/delete events.
- Idempotent projection into ArangoDB and typed read-only Gremlin access.
- Helm deployments built from standard Kubernetes resources without CRDs.

Root-span discovery is available as an opt-in input. It filters for completed roots whose kind is neither client nor server, aggregates them with the Collector spanmetrics connector, and sends node-only evidence rather than raw spans. Both evidence lanes converge on one Java lifecycle engine. Numeric request counts and entity-event logs are intentionally outside the focused graph pipeline.

## Why the inputs matter

| Source | Role | Status |
| --- | --- | --- |
| Existing traces through the Collector `servicegraph` connector | Bootstrap nodes and relationships without application changes | Implemented |
| Aggregated root spans | Discover execution nodes absent from service interactions | Implemented, opt-in |

## Try the focused environment

The repository includes a persistent local demo that starts Redpanda, ArangoDB,
the Kafka indexer, and Gremlin Server, then seeds representative schema-3 graph
events for typed traversal. It also includes an opt-in Kind fixture that verifies
projection, deletion, restart persistence, and spanmetrics root discovery
through the complete Collector-to-Gremlin path.
Docker, Kind, `kubectl`, Helm 3, and Python 3.12 are required.

```powershell
python -m pip install -e ".[dev]"
python -m pip install -e "packages/extended-opentelemetry-semconv[gremlin]"
python -m pip install -e services/servicegraph-indexer
python -m tools.local_demo up
python -m tools.local_demo status
python -m tools.local_demo query
```

Provisioning can take longer than five minutes on a cold Docker cache. The demo
persists until `python -m tools.local_demo down`. See the
[local demo guide](docs/getting-started/quickstart.md) for requirements and
the exact follow-up commands.

## Evidence and limits

The focused E2E proves the Kafka lifecycle contract through ArangoDB and typed
Gremlin, plus the spanmetrics discovery path from Collector through Flink. Unit
tests cover semantic generation, service-graph ingestion,
contributor lifecycle behavior, Flink wiring, and indexer decisions. Helm and
MkDocs have deterministic validation commands.

The repository does **not** currently provide:

- an automated paired-servicegraph Collector-to-Flink Kind test;
- distributed throughput, state-size, or infrastructure-cost benchmarks;
- historical or bi-temporal graph queries;
- standard OTel output events;
- implicit deletion of incoming relationships when a target entity is deleted;
- arbitrary OTel identity shapes beyond the exact generated local semantic
  identity;
- a safe public query API (Gremlin is a trusted internal interface);
- production Kafka or ArangoDB operations.

See [Product direction](docs/product.md) before evaluating production fit.

## Repository map

| Path | Responsibility |
| --- | --- |
| `packages/extended-opentelemetry-semconv` | Generated semantic SDK and optional typed Gremlin client |
| `tools/semconv_codegen` | Registry validation and deterministic generation |
| `services/otel-servicegraph-diff` | Native Java Flink ingestion/lifecycle engine |
| `services/servicegraph-indexer` | ArangoDB initializer and Kafka projection |
| `services/servicegraph-gremlin` | Pinned read-only TinkerPop/ArangoDB runtime |
| `services/servicegraph-demo` | Optional synthetic OTLP traffic |
| `examples/auto-instrumentation` | Plain endpoint extraction and explicit ETL context examples |
| `deploy/helm` | Collector, Flink, demo, ArangoDB, indexer, and Gremlin charts |

Kafka, topic creation, and production ArangoDB remain platform concerns.

## Documentation

- [Product direction](docs/product.md)
- [Local demo](docs/getting-started/quickstart.md)
- [Auto-instrumented endpoint](docs/getting-started/auto-instrumentation.md)
- [Runtime architecture](docs/architecture.md)
- [Community and launch guide](docs/community.md)
- [Custom entity tutorial](docs/getting-started/custom-entity.md)
- [Kubernetes deployment](docs/deployment-and-operations.md)

Serve the documentation locally:

```powershell
python -m pip install -e ".[docs]"
python -m mkdocs serve
```

## License

Licensed under the [Apache License 2.0](LICENSE).

## Validation

```powershell
python -m tools.semconv_codegen --check
python -m mkdocs build --strict
python -m ruff check .
python -m pyright
python -m pytest -m "not e2e"
helm lint deploy/helm/servicegraph-collector
helm lint deploy/helm/servicegraph-demo
helm lint deploy/helm/servicegraph-flink
helm lint deploy/helm/servicegraph-arangodb
helm lint deploy/helm/servicegraph-indexer
helm lint deploy/helm/servicegraph-gremlin
```
