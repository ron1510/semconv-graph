# Product direction

## Identity

Semconv Graph turns existing OpenTelemetry traces into a live typed entity graph. The Python distribution remains `extended-opentelemetry-semconv`.

Semantic conventions act as executable graph schema. One generated registry controls identities, fields, relationships, Collector dimensions, Flink extraction, storage topology, and typed reconstruction.

## Product boundary

The system derives topology from activity telemetry. The Collector pairs interactions and optionally aggregates non-interacting roots. Kafka carries OTLP Protobuf evidence to Java Flink. Flink derives the same nodes and relationships, merges contributors, expires stale evidence, and publishes schema-3 current-state events. ArangoDB stores the graph and read-only Gremlin exposes it to trusted typed clients.

Request totals are deliberately evidence rather than graph counters. A positive datapoint refreshes the contributor. Numeric magnitude, failure totals, event logs, and historical interaction counts are outside this focused pipeline.

## Architecture ownership

| Component | Ownership |
| --- | --- |
| Collector | Trace affinity, pairing, root aggregation, positive evidence export |
| Generated registry | Identity, fields, relationships, dimensions, storage topology |
| Flink | Semantic derivation, contributor merging, timers, lifecycle events |
| Kafka | Durable Protobuf input and compacted schema-3 output |
| Indexer | Contract validation, idempotent projection, offset commits |
| ArangoDB | Native vertex and edge current state |
| Gremlin and SDK | Read-only traversal and generated typed reconstruction |

## What validation proves

Unit and integration tests cover Protobuf parsing, unchanged extraction, deterministic IDs, multi-contributor winners, refresh suppression, partial/final expiry, RocksDB checkpoints and recovery, schema-3 indexing, replay, deletion, code generation, and typed traversal. Helm validation checks Protobuf exporters and the absence of an entity-log lane. The disposable E2E covers Collector through Gremlin and records parser, event, checkpoint, and throughput measurements.

## Limits

The graph stores current state rather than bi-temporal history. Inferred attributes are limited to generated scalar dimensions. Deleting a node does not implicitly delete an incoming relationship owned by another surviving element. Gremlin is a trusted internal interface. Production Kafka, HA ArangoDB, backups, and disaster recovery remain platform responsibilities.
