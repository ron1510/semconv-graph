# Architecture

The runtime is an evidence-only graph projection:

```text
OTLP traces
  → Collector interaction/root discovery aggregation
  → Kafka OTLP Protobuf
  → Java Flink semantic extraction and lifecycle
  → Kafka schema-3 JSON
  → ArangoDB replacement projection
  → typed Gremlin
```

## Collector boundary

The Collector owns span pairing and root aggregation. It exports only positive delta evidence: `traces_service_graph_request_total` for interactions and spanmetrics `semconv.graph.discovery.calls` for optional roots whose kind is neither client nor server. Counts do not cross the graph boundary as public data; a positive datapoint means that the contributor observed the topology at its timestamp.

## Semantic extraction

The generated registry defines entities, identifying attributes, evidence-source eligibility, observation and interaction relationship topology, interaction connection selectors, Collector dimensions, Java registry data, Python types, and the Arango schema. Servicegraph evidence expands two `service_graph` observations plus their selected interaction; root discovery expands one `span_metrics` observation. `MetricParser` contains no concrete entity or relationship rules.

## Lifecycle processing

Each graph element is keyed independently. Contributor snapshots merge by newest observation time and canonical contributor-ID tie breaking. `AttributeWinners` maintains the winning attribute owners for the common refresh path. Every element has at most one event-time and one processing-time timer, regardless of contributor cardinality; callbacks remove all contributors whose deadlines have passed and schedule the next minimum.

An unchanged refresh updates state and deadlines without another public event. A changed aggregate emits a complete upsert. Final expiry emits a delete. State uses directly framed CBOR v3 and has no legacy reader.

## Projection and access

Schema-3 graph events contain complete node or edge state and no edge metrics. The indexer validates the contract, replaces documents by deterministic key, deletes idempotently, and commits Kafka offsets only after successful writes. Generated Arango schema version 3 contains attribute aliases and topology only. The Gremlin service exposes read-only traversals, while the Python client reconstructs generated entity and edge types.
