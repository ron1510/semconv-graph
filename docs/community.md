# Community and Launch

Semconv Graph should be introduced through a reproducible problem and result,
not through its dependency list.

## One-sentence description

> Semconv Graph turns existing OpenTelemetry traces into a live typed entity
> graph without changing application instrumentation.

The supporting sentence is:

> It combines inferred semantic entities with opt-in standard OpenTelemetry
> entity events in one contributor-aware lifecycle engine.

## Demonstration story

A useful public demo should take one observable interaction from input to
query:

1. Show paired client/server spans from applications with ordinary OTel trace
   instrumentation.
2. Show the Collector producing service-graph delta metrics.
3. Show Flink extracting shared nodes and edges and merging complementary
   contributor fields.
4. Query the current graph through typed Gremlin.
5. Stop one contributor and show shared elements remain.
6. Stop the final contributor and show explicit graph-element deletion.
7. Enable explicit entity events and show that the explicit source can complete
   the same graph element without replacing inferred provenance.

Use concrete services, IDs, attributes, and traversals. The repository's
in-process benchmark is reproducible, but avoid distributed throughput or cost
claims until those paths are measured.

## Presentation outline

### 1. Problem

Traces describe requests, but many teams lack a continuously maintained,
semantically typed inventory and dependency graph.

### 2. Adoption constraint

Requiring every application or infrastructure observer to emit a new entity
signal delays value. Existing trace telemetry can provide a useful initial
graph.

### 3. Working implementation

Explain trace-affine Collector routing, generated semantic extraction, Flink
contributor state, the compacted lifecycle stream, ArangoDB projection, and
read-only Gremlin.

### 4. Live proof

Run the focused environment or a prepared complete deployment. Show upsert,
shared-contributor survival, final expiry, replay, and typed query results.

### 5. OpenTelemetry alignment

Reference the official [Entity Data Model](https://opentelemetry.io/docs/specs/otel/entities/data-model/)
and [Entity Events](https://opentelemetry.io/docs/specs/otel/entities/entity-events/)
specifications. Present inferred telemetry and explicit entity events as two
sources for one graph, and show the [conformance matrix](reference/otel-entity-conformance.md).

### 6. Honest boundaries

State that standard output, incoming-relationship implicit deletion, historical
queries, full-path automated E2E, and public scale benchmarks are not complete.

### 7. Contribution request

Ask for help on a small number of bounded problems rather than asking people to
"improve the project."

## Contribution tracks

Good independent contribution areas are:

- conformance fixtures for standard `entity.state` and `entity.delete` events;
- incoming-relationship cleanup, `schema_url`, and identification-context work;
- additional semantic entity and relationship extensions;
- benchmark scenarios and measurement tooling;
- Collector deployment examples for existing environments;
- operational signals for Flink state, Kafka lag, and ArangoDB projection;
- documentation for Java, Go, or raw Gremlin clients;
- validation on Kubernetes distributions and restricted security policies.

Changes to lifecycle state, deterministic IDs, or the Kafka schema need a design
discussion because they affect recovery and downstream consumers.

## Evidence needed before scale claims

Publish a distributed benchmark before describing the complete system as
cheaper or more scalable than alternatives. At minimum record:

- source datapoints and resulting contributions per second;
- end-to-end upsert and delete latency;
- Flink keyed-state size by active contributor and graph-element count;
- Kafka partition count, parallelism, and replay time;
- indexer throughput and ArangoDB write latency;
- CPU and memory per component;
- dataset, topology, configuration, and exact image versions.

Architecture provides a scaling mechanism; these measurements establish its
actual operating envelope.

## Launch checklist

- Keep the product statement and limitations visible in the README.
- Record a short demo using the same commands documented in the quickstart.
- Publish immutable wheels, images, and chart versions when release automation
  exists.
- Open bounded issues for the contribution tracks above.
- Label introductory issues only when a maintainer can explain and review them.
- Bring the conformance matrix and a running demo to the OpenTelemetry Entities
  SIG for technical feedback.
- Turn accepted design decisions into tests and documentation before broadening
  compatibility claims.

The launch target is not maximum attention. It is enough clarity that an
engineer can reproduce the graph, identify an honest gap, and contribute to it
without first reverse-engineering the repository.
