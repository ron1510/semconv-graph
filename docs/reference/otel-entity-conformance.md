# OpenTelemetry Entity Conformance

Semconv Graph implements an opt-in compatibility path for the official
OpenTelemetry [Entity Data Model](https://opentelemetry.io/docs/specs/otel/entities/data-model/)
and [Entity Events](https://opentelemetry.io/docs/specs/otel/entities/entity-events/)
documents. Both specifications currently have Development status. This page
separates implemented behavior from known differences; it is not a claim of
complete OpenTelemetry Entity Events conformance.

## Matrix

| OTel concept | Status | Exact behavior or limitation |
| --- | --- | --- |
| Opt-in transport | Implemented | With `entityEvents.enabled=true`, Collector routers filter entity-event OTLP logs into `streamContract.topics.entityEvents`; Flink consumes that topic with an independent group. Disabled is the default. |
| `entity.state` | Implemented | Parses the complete identity, description, outgoing relationship list, report interval, timestamp, Resource, and Scope from OTLP JSON. State becomes graph contributions for one observer. |
| `entity.delete` | Implemented | Retracts the node and all outgoing relationship contributions previously recorded for that observer. A duplicate or older event produces no mutation. |
| Entity type | Restricted | The type must participate in a generated `service_graph` relationship and therefore have a generated ArangoDB vertex collection. A known upstream SDK model outside the configured graph topology is rejected rather than emitted into an unprojectable collection. |
| Entity identity | Partial | Required local semantic identity fields are validated through the generated model and produce the deterministic element ID. Additional OTel identification-context keys remain in node attributes but do not participate in that ID. |
| Descriptive attributes | Implemented | The complete `entity.description` map is decoded and preserved in the node's canonical attributes. Known generated fields still undergo semantic model validation. |
| `schema_url` identity boundary | Not implemented | Scope `schema_url` participates in the fallback observer fingerprint, but entity schemas are not converted and `schema_url` is not part of entity merge compatibility. |
| `entity.report.interval` | Implemented | A positive interval in seconds sets that observer contribution's TTL to `interval + reportIntervalGraceSeconds`. An absent or zero interval uses `job.interactionTtlSeconds`. Negative or non-integer values are rejected. |
| Embedded relationships | Implemented with registry restriction | Each descriptor becomes a directed edge from the state-event entity to its target only when the generated `service_graph` registry allows the exact source type, relationship type, and target type. Unknown or disallowed combinations reject the event. |
| Complete outgoing relationship state | Implemented | A newer `entity.state` is complete for that observer. Any outgoing relationship it previously contributed but now omits is retracted immediately before current contributions are applied. |
| Relationship removal on `entity.delete` | Partial | Outgoing relationships previously supplied by the deleted observer are retracted. Incoming relationships owned by other source entities are not implicitly deleted yet. |
| Multiple observers | Implemented | Each observer has independent source state and contributor ownership. `otel.entity.observer.id` is preferred; otherwise a deterministic Resource-plus-Scope fingerprint is used. Contributions merge in the shared element-keyed lifecycle stage. |
| Explicit plus inferred sources | Implemented | Service-graph metrics and explicit entity events use separate Kafka sources and independent contributor IDs, then union before graph-element lifecycle processing. One source cannot retract another source's contribution. |
| Out-of-order and duplicate events | Implemented locally | Per observer and entity, timestamp, delete precedence, and payload hash form a deterministic ordering key. Older or duplicate observations are ignored. This is project reconciliation behavior, not a general event history. |
| Complex OTLP values | Implemented for canonical storage | Strings, booleans, integers, doubles, bytes, arrays, maps, and null are decoded. Generated semantic fields retain their declared type constraints. |
| Standard OTel output | Not implemented | Output remains the project `graph.elements.events` schema `2.0`, not `entity.state` or `entity.delete` OTLP logs. |
| Current-state graph | Implemented | The schema-2 stream is projected idempotently into generated ArangoDB vertex and edge collections. |
| Historical or bi-temporal queries | Not implemented | Kafka retention is externally configured and ArangoDB is a current-state projection. No history query model is exposed. |

## Observer and contributor identity

The optional `otel.entity.observer.id` attribute is a Semconv Graph extension,
not a standard OTel Entity Events field. Producers should set it to a stable,
nonempty observer identity when the same Resource and Scope can represent more
than one independent observer.

When the extension is absent, Flink hashes:

- every OTLP Resource attribute on the `ResourceLogs` record;
- instrumentation Scope name and version;
- Scope schema URL;
- the generated entity type and deterministic entity ID.

The result is both the source-state key and contributor ID. Two observers with
different explicit IDs or different fallback fingerprints can independently
support the same graph element. Attribute conflicts use the existing graph
lifecycle precedence rules; removing one contributor does not remove fields or
elements still supplied by another.

## Complete-state reconciliation

Flink stores the element IDs last supplied by each observer/entity source. For
a newer `entity.state` event it:

1. reconstructs the source node and all registry-approved outgoing edges;
2. retracts element IDs present in the previous snapshot but omitted now;
3. applies the complete current contribution set;
4. records the new source snapshot.

`entity.delete` retracts that source's recorded node and outgoing edges. It does
not scan state belonging to other source entities, so an incoming edge whose
source has not emitted a newer complete state can remain. Implementing that
implicit incoming-edge deletion requires a separate reverse-reference design.

## Identity and topology boundary

OpenTelemetry entity IDs can include additional identification context. The
current runtime preserves every `entity.id` key in the canonical node
attributes, but deterministic IDs use only the identifying fields declared by
the generated local semantic model. Two events that differ only by additional
identification-context keys can therefore merge into one graph node.

The SDK contains more upstream entity models than the deployed graph supports.
Runtime acceptance is intentionally narrower: an entity type must participate
in the generated `service_graph` relationship set so code generation also
creates its ArangoDB collection and Gremlin topology. Rejecting a known but
unprojectable type prevents Flink from emitting an element the indexer cannot
store.

## Non-claims

The input adapter provides useful interoperability with the developing OTel
contract. It does not change the project output contract, provide historical
state, apply `schema_url` as an identity boundary, include arbitrary OTel
identification context in deterministic IDs, or implement implicit deletion of
incoming relationships.
