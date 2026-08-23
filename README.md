# Semconv Graph

[![CI](https://github.com/ron1510/extended-opentelemetry-semconv/actions/workflows/ci.yml/badge.svg)](https://github.com/ron1510/extended-opentelemetry-semconv/actions/workflows/ci.yml)
[![Python 3.12](https://img.shields.io/badge/python-3.12-3776AB.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

**Turn existing OpenTelemetry traces into a live typed entity graph without
changing app instrumentation.**

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
  -> service-graph delta metrics
  -> Kafka
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
- Contributor-aware attribute merging and expiry in Flink.
- Equal lifecycle treatment for semantic nodes and edges.
- Deterministic, compactable Kafka upsert/delete events.
- Idempotent projection into ArangoDB and typed read-only Gremlin access.
- Helm deployments built from standard Kubernetes resources without CRDs.

OpenTelemetry Entity Events are also supported as an opt-in second input. The
[OpenTelemetry Entity Data Model](https://opentelemetry.io/docs/specs/otel/entities/data-model/)
and [Entity Events](https://opentelemetry.io/docs/specs/otel/entities/entity-events/)
specifications are both in development. When enabled, filtered `entity.state`
and `entity.delete` OTLP logs enter the same contributor lifecycle engine as
the inferred service-graph source.

## Why the two sources matter

| Source | Role | Status |
| --- | --- | --- |
| Existing traces through the Collector `servicegraph` connector | Bootstrap a useful graph without changing application instrumentation | Implemented |
| Standard OTel entity events | Add explicit inventory, complete descriptions, relationships, and deletion from conforming producers | Implemented, opt-in |

Both inputs converge on one lifecycle engine. Explicit events and inferred
telemetry retain independent contributor IDs, so one source cannot retract a
fact still supported by another.

## Try the focused environment

The repository includes a persistent local demo that starts Redpanda, ArangoDB,
the Kafka indexer, and Gremlin Server, then seeds representative Flink schema-2
events for typed traversal. It also includes an opt-in Kind fixture that verifies
projection, deletion, and restart persistence.
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
Gremlin. Unit tests cover semantic generation, service-graph ingestion,
contributor lifecycle behavior, Flink wiring, and indexer decisions. Helm and
MkDocs have deterministic validation commands.

The repository does **not** currently provide:

- an automated full Collector-to-Flink Kind test;
- distributed throughput, state-size, or infrastructure-cost benchmarks;
- historical or bi-temporal graph queries;
- standard OTel output events;
- implicit deletion of incoming relationships when a target entity is deleted;
- arbitrary OTel identity shapes beyond the exact generated local semantic
  identity;
- a safe public query API (Gremlin is a trusted internal interface);
- production Kafka or ArangoDB operations.

See [Proof and limitations](docs/product.md#proof-and-limitations) and the
[OTel entity conformance matrix](docs/reference/otel-entity-conformance.md)
before evaluating production fit.

## Repository map

| Path | Responsibility |
| --- | --- |
| `packages/extended-opentelemetry-semconv` | Generated semantic SDK and optional typed Gremlin client |
| `tools/semconv_codegen` | Registry validation and deterministic generation |
| `services/otel-servicegraph-diff` | Service-graph ingestion, lifecycle engine, and PyFlink wiring |
| `services/servicegraph-indexer` | ArangoDB initializer and Kafka projection |
| `services/servicegraph-gremlin` | Pinned read-only TinkerPop/ArangoDB runtime |
| `services/servicegraph-demo` | Optional synthetic OTLP traffic |
| `deploy/helm` | Collector, Flink, demo, ArangoDB, indexer, and Gremlin charts |

Kafka, topic creation, and production ArangoDB remain platform concerns.

## Documentation

- [Product direction](docs/product.md)
- [Local demo](docs/getting-started/quickstart.md)
- [Runtime architecture](docs/architecture.md)
- [OTel entity conformance](docs/reference/otel-entity-conformance.md)
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
