# Semconv Graph

**Turn existing OpenTelemetry traces into a live typed entity graph without
changing app instrumentation.**

Semconv Graph is the working product name. The Python SDK remains
`extended-opentelemetry-semconv`, and existing package, image, chart, and import
names remain unchanged.

## The product boundary

Semconv Graph is a lifecycle engine and graph projection for semantic entities
derived from telemetry. It is not a tracing backend and it does not ask
applications to emit a proprietary inventory format.

Today it converts Collector service-graph delta metrics into semantic graph
contributions. Flink merges those contributions, owns staleness, and emits
complete node and edge lifecycle events. ArangoDB holds the current-state graph,
and trusted clients traverse it through read-only Gremlin.

```text
Existing OTLP traces
  -> Collector service-graph metrics
  -> Kafka
  -> Flink lifecycle engine
  -> graph element events
  -> ArangoDB
  -> Gremlin
```

## The adoption argument

Standard entity events are valuable when producers can explicitly describe
inventory and relationships. Existing estates often do not have those
producers yet. Inference from traces provides a lower-friction starting point:
deploy infrastructure around the telemetry pipeline, then obtain a useful graph
without changing every instrumented application.

Standard OTel entity events are an opt-in second source. Explicit entities and
inferred entities enter the same contributor lifecycle without replacing the
existing trace-derived path. Read the [product direction](product.md) and the
[conformance matrix](reference/otel-entity-conformance.md) for the exact current
boundary.

## Runtime guarantees

- Trace-affine routing keeps both sides of a trace on one service-graph backend.
- Flink owns contributor-aware merging, expiry, and graph lifecycle state.
- Complete `upsert` and `delete` events are keyed by deterministic element IDs.
- Nodes and edges follow the same lifecycle rules.
- Downstream projections do not invent their own TTL policy.
- ArangoDB projection is idempotent under Kafka replay.

Delivery is at least once. Deterministic event IDs and graph identifiers let
consumers apply events idempotently.

## Start here

- [Run the focused local environment](getting-started/quickstart.md)
- [Understand the product and roadmap](product.md)
- [Read the runtime architecture](architecture.md)
- [Check OTel entity-event conformance](reference/otel-entity-conformance.md)
- [Add a custom entity](getting-started/custom-entity.md)
- [Deploy to Kubernetes](deployment-and-operations.md)
- [Find a contribution area](community.md)

## Project status

The semantic SDK, inferred service-graph source, Flink lifecycle path, Kafka
contract, ArangoDB projection, Gremlin runtime, and Helm charts are implemented.
Opt-in standard entity-event ingestion is implemented. Historical queries,
standard OTel output, incoming-relationship implicit deletion, published scale
benchmarks, and a full automated Collector-to-Flink E2E are not yet implemented.
