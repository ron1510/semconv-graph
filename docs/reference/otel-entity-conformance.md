# OpenTelemetry Entity Conformance

Semconv Graph implements an opt-in, registered-topology subset of the official
OpenTelemetry [Entity Data Model](https://opentelemetry.io/docs/specs/otel/entities/data-model/)
and [Entity Events](https://opentelemetry.io/docs/specs/otel/entities/entity-events/)
documents. Both specifications currently have Development status. This is not
a claim of complete OpenTelemetry Entity Events conformance.

## Matrix

| OTel concept | Status | Exact behavior or limitation |
| --- | --- | --- |
| Opt-in transport | Implemented | Collector routers can filter entity-event OTLP logs into a separate Kafka topic; Flink consumes it with an independent group. Disabled is the default. |
| `entity.state` | Implemented for registered types | A complete identity, description, outgoing relationship list, report interval, and timestamp become graph contributions for the explicit source. |
| `entity.delete` | Partial | Retracts the explicit source's node and its recorded outgoing relationships. Duplicate deletes are ignored. Incoming relationships owned by other source entities are not implicitly deleted. |
| Entity type | Restricted | The type must participate in generated `service_graph` topology and therefore have an ArangoDB vertex collection. |
| Entity identity | Restricted | `entity.id` must contain exactly the identifying keys declared by the generated semantic model. Extra or missing keys are rejected, so distinct OTel identities are never silently merged. String-encoded scalar IDs are converted to the model's strict type. |
| Descriptive attributes | Implemented | The complete `entity.description` map is preserved. It cannot redefine an identifying field. Generated fields retain their declared type constraints. |
| `schema_url` identity boundary | Not implemented | Entity schemas are not converted and Scope schema URL is not part of deterministic identity. Deployments needing multiple schema versions must normalize before this adapter. |
| `entity.report.interval` | Implemented | Positive values use `interval + reportIntervalGraceSeconds`. Absent or zero values do not expire from inactivity. Negative or non-integer values are rejected. |
| Embedded relationships | Registry restricted | A directed edge is accepted only when generated topology allows the exact source type, relationship type, and target type. Relationship types are not treated as an open enumeration here. |
| Complete outgoing relationship state | Implemented | A newer state retracts outgoing relationships omitted from the complete snapshot before applying current contributions. |
| Multiple explicit observers | Not represented independently | Explicit events for the same registered entity ID share one authoritative source snapshot. Resource and Scope are transport metadata. The inferred metrics source remains an independent contributor. |
| Explicit plus inferred sources | Implemented | Separate Kafka sources union before graph-element lifecycle processing. Explicit retraction cannot remove an inferred contribution. |
| Out-of-order and duplicate events | Partial | State and delete duplicates are suppressed deterministically. A state event received after a delete is applied even when its timestamp is older, as required by the OTel delivery guidance. This is current-state reconciliation, not event history. |
| Complex OTLP values | Implemented for canonical storage | Strings, booleans, integers, doubles, bytes, arrays, maps, and null are decoded. Generated fields retain their model constraints. |
| Standard OTel output | Not implemented | Output remains `graph.elements.events` schema `2.0`, not OTLP entity events. |
| Historical queries | Not implemented | ArangoDB is a current-state projection; Kafka retention is externally configured. |

## Complete-state reconciliation

Flink stores the element IDs last supplied by the explicit source for each
registered semantic entity. For an accepted `entity.state` it:

1. reconstructs the source node and all registry-approved outgoing edges;
2. retracts element IDs present in the previous snapshot but omitted now;
3. applies the complete current contribution set;
4. records the new source snapshot.

`entity.delete` retracts that node and its recorded outgoing edges. It does not
scan snapshots owned by other source entities, so incoming relationships can
remain until their owners report a newer state. Full implicit relationship
deletion requires a reverse-reference design that is not present today.

## Identity boundary

OpenTelemetry permits identity maps and schema boundaries broader than this
project's generated local models. Accepting extra identity keys while deriving
an ID from only a subset would incorrectly merge distinct entities. The adapter
therefore rejects any identity shape that is not an exact match for the
registered model.

This favors correctness over broad intake. Extend the semantic registry and
regenerate the SDK, Collector dimensions, ArangoDB schema, and Gremlin topology
when a new identity shape belongs in the product graph.

## Non-claims

The adapter does not provide open-enumeration relationships, observer-specific
explicit ownership, schema conversion, implicit incoming-edge deletion,
historical state, or standard OTel output. Its supported subset is intended to
converge explicit registered facts with the graph inferred from existing
service-graph telemetry.
