# Semconv Graph

**Turn existing OpenTelemetry traces into a live typed entity graph without changing application instrumentation.**

```text
OTLP traces → Collector evidence → Kafka OTLP Protobuf → Java Flink lifecycle
            → schema-3 graph events → ArangoDB → typed Gremlin
```

The generated semantic registry keeps entity identity, relationship topology, Collector dimensions, Java extraction, Python models, and Arango collections aligned.

## Runtime guarantees

- Trace-affine routing keeps both sides of an interaction on one service-graph backend.
- Positive request and discovery sums are freshness evidence; their magnitudes are discarded.
- Flink merges independent contributors and owns 24-hour freshness by default.
- Unchanged evidence refreshes expiry without repeated graph upserts.
- Nodes and relationships use deterministic IDs and identical lifecycle rules.
- Schema-3 events are complete replacements or deletes keyed by element ID.
- The indexer commits Kafka offsets only after successful Arango writes.
- Typed Gremlin reconstructs generated node and edge models without counter metadata.

## Start here

- [Run the focused local environment](getting-started/quickstart.md)
- [Read the detailed system flow](concepts/system-flow.md)
- [Understand the product boundary](product.md)
- [Add a custom entity](getting-started/custom-entity.md)
- [Deploy to Kubernetes](deployment-and-operations.md)
- [Plan a clean upgrade](operations/upgrades.md)
