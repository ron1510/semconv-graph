# Product Direction

## Identity

**Semconv Graph** is the working product name for the complete runtime in this
repository. It can change without disrupting the existing
`extended-opentelemetry-semconv` Python distribution or its
`extended_otel_semconv` import namespace.

Its promise is specific:

> Turn existing OpenTelemetry traces into a live typed entity graph without
> changing app instrumentation.

The project uses semantic conventions as executable graph schema. The registry
defines entity identity, attributes, and relationships; generation keeps the
SDK, Collector dimensions, Flink extraction, ArangoDB topology, and typed
Gremlin reconstruction aligned.

## The problem

OpenTelemetry traces describe activity well, but operators still need a
current answer to two different questions:

1. What semantic entities are participating in that activity?
2. How are those entities connected, and when should they disappear?

The [OpenTelemetry Entity Data Model](https://opentelemetry.io/docs/specs/otel/entities/data-model/)
defines typed entities with immutable identifying attributes and mutable
descriptions. The [Entity Events specification](https://opentelemetry.io/docs/specs/otel/entities/entity-events/)
defines structured `entity.state` and `entity.delete` log events, embedded
relationships, and reporting intervals. Both documents currently have
Development status.

Semconv Graph addresses estates that already emit traces and environments with
standard entity-event producers. It can infer a graph from service-graph
telemetry, consume explicit entity state, or merge both sources centrally.

## Differentiation

Generic entity-event consumers and Semconv Graph solve adjacent parts of the
same problem:

| Capability | Generic OTel entity-event consumer | Semconv Graph today |
| --- | --- | --- |
| Primary input | Explicit structured entity events | Existing traces by default; explicit entity events as an opt-in second source |
| Producer requirement | A producer must emit entity events | Existing trace instrumentation is reused |
| Entity descriptions | Can carry complete and complex descriptions | Scalar dimensions from inferred telemetry; complete descriptions from explicit events |
| Relationships | Explicitly supplied by the producer | Inferred relationships plus registry-validated explicit relationships |
| Lifecycle | Explicit state/delete plus report-interval expiry | Contributor-aware inactivity expiry and observer-scoped explicit reconciliation |
| Current graph | Consumer-dependent | Kafka lifecycle stream plus ArangoDB projection |

The project does not need to displace entity-event consumers. Its useful role is
to lower the cost of reaching an initial graph while accepting standard entity
events wherever producers already emit them.

## Two-source runtime

```text
OTLP traces                                  OTel entity.state/entity.delete
    |                                                   |
Collector servicegraph metrics              entity-event ingest adapter
    |                                                   |
    +---------------- graph contributions -------------+
                              |
                  Flink contributor lifecycle
                              |
                 graph.elements.events schema 2.0
                              |
                    ArangoDB current graph
```

The optional second source translates standard entity events into the same
internal contribution model:

- `entity.state` supplies an entity snapshot and embedded relationship
  contributions;
- `entity.delete` retracts the matching explicit contributor;
- `entity.report.interval` informs source-specific expiry;
- only entity types participating in generated `service_graph` relationships,
  and only relationship combinations allowed by that topology, are accepted;
- explicitly reported facts and inferred facts retain separate provenance.

Observer identity prefers the explicit `otel.entity.observer.id` extension. If
it is absent, Flink hashes the OTLP Resource attributes and instrumentation
Scope. A positive `entity.report.interval` sets the contribution TTL to the
interval plus configured grace; absent or zero uses the global contributor TTL.

This is not full conformance. Deterministic IDs still use the generated local
semantic identity, `schema_url` is not a merge boundary, output remains project
schema `2.0`, and deleting an entity does not yet retract incoming relationships
owned by other source entities.

## Architecture

The current runtime deliberately gives each stateful concern one owner:

| Component | Ownership |
| --- | --- |
| Collector routers and backends | Trace affinity and service-graph delta metrics |
| Generated semantic registry | Entity identity, fields, relationships, and graph topology |
| Flink | Contribution merging, edge metric accumulation, timers, and lifecycle events |
| Kafka | Durable input/output transport and replay boundary |
| Indexer | Idempotent current-state projection and offset commits |
| ArangoDB | Native vertex and edge storage |
| Gremlin Server | Trusted, read-only graph traversal |
| Typed SDK client | Reconstruction of element-producing traversals as Pydantic models |

See [Runtime architecture](architecture.md) for the data path and state model.

## Proof and limitations

### What repository validation proves

- Unit tests exercise deterministic model generation and registry validation.
- Flink tests exercise strict OTLP JSON parsing, semantic extraction,
  entity-event reconciliation, contributor merging, timers, lifecycle
  serialization, and job wiring.
- Indexer tests exercise topology validation, idempotent replacement/deletion,
  and commit-after-write behavior.
- The focused Kind E2E injects representative schema-2 events and verifies
  Kafka to indexer to ArangoDB to typed Gremlin, lifecycle deletion, and restart
  persistence.
- Helm lint/render commands and strict MkDocs builds are documented and
  repeatable.

### What it does not prove

- The focused E2E does not run the Collector and Flink stages.
- The in-process benchmark measures project-owned parsing and lifecycle logic;
  it does not establish distributed throughput, state growth, horizontal
  scaling, or operating cost.
- The project stores current graph state, not a bi-temporal entity history.
- The inferred source only sees scalar dimensions deliberately carried through
  service-graph metrics.
- Additional OTel identification-context keys are preserved as attributes but
  do not participate in deterministic IDs.
- Explicit deletion retracts the observer's node and outgoing relationships;
  implicit deletion of incoming relationships is not implemented.
- Output uses the project graph-element schema, not standard OTel entity events.
- Gremlin is a trusted internal interface, not a bounded public API.
- The local ArangoDB chart is not a production HA deployment.
- Kafka, production ArangoDB operations, backups, and disaster recovery remain
  external responsibilities.

These are boundaries to measure or implement, not gaps to hide behind broad
architecture claims.
