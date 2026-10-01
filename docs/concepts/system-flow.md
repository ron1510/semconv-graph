# System flow: class and function reference

This page follows one observation from an application span to a typed Gremlin result. It describes the evidence-only schema-3 runtime.

## 1. Complete flow

```text
application spans
  → Collector router
      → service_graph connector
      → optional root filter + span_metrics connector
  → positive delta Sum evidence
  → Kafka otel.servicegraph.metrics (OTLP Protobuf)
  → ServiceGraphJob
  → MetricParser
  → SemanticRegistry
  → Contribution(contributorId, observedAtUnixNano, element)
  → keyBy(elementKey)
  → ElementLifecycleFunction
  → Event schema 3.0
  → Kafka graph.elements.events (deterministic JSON, key=element_id)
  → servicegraph-indexer
  → generated Arango collections
  → read-only Gremlin Server
  → SemanticGremlinClient
```

Counts end at `MetricParser`. A positive request total says an interaction was observed. Its magnitude is neither aggregated nor emitted.

## 2. Generated semantic contract

`tools.semconv_codegen.generator.generate_files()` loads the locked upstream registry and repository extensions, validates them, and renders all cross-language artifacts in one pass.

- `_render_collector_dimensions()` writes service-graph dimensions.
- `_render_root_span_discovery()` writes discovery dimensions and routing attributes.
- `render_java_registry()` writes `semantic-registry.json` for the Java job.
- `_render_entity_module()` and `_render_edge_module()` write the Python SDK types.
- `_render_arangodb_schema()` writes Arango graph schema version 3 for the indexer and Gremlin chart.

`python -m tools.semconv_codegen --check` proves that checked-in artifacts equal a fresh render. Generated Arango `property_aliases` contains only `attributes`; `metrics` is neither a property nor a reserved alias.

Entity IDs are deterministic semantic identities such as `service:checkout`. `SemanticRegistry.edgeId(source, type, target)` hashes the ordered endpoints and relationship type into an `edge:` ID. Java exclusively owns both identity algorithms; downstream components preserve `element_id` as data.

## 3. Collector processing

### 3.1 Router

The router ConfigMap exposes one OTLP receiver. The traces pipeline sends interaction traffic to the service-graph backend. In horizontal mode `load_balancing` uses `traceID`, keeping both halves of a trace on one backend. Single-writer mode sends everything to one connector.

When root discovery is enabled, `filter/root_span_discovery` keeps only roots whose span kind is neither client nor server and rejects resource/span conflicts for generated modeled attributes. Client and server spans remain the responsibility of the service-graph connector. `load_balancing/root_span_discovery` routes on generated semantic identity fields so the same logical root series reaches the same backend.

The router has no logs pipeline, Kafka exporter, or entity-event credentials.

### 3.2 Interaction backend

The `service_graph` connector pairs client and server spans. Generated dimensions carry entity identity and descriptive attributes into its output. `cumulativetodelta/servicegraph` converts connector-local cumulative sums before `filter/servicegraph_output` keeps only:

```text
traces_service_graph_request_total
```

The filter also removes integer-zero datapoints. Failed requests already increment request total, so `traces_service_graph_request_failed_total` is not exported.

### 3.3 Discovery backend

`span_metrics/root_span_discovery` aggregates completed roots with delta temporality. Histograms, exemplars, and events are disabled. `filter/root_span_discovery_output` keeps only positive, non-overflow `semconv.graph.discovery.calls` datapoints.

Both backends batch and publish gzip-compressed `otlp_proto` records to `otel.servicegraph.metrics`. Kafka topic names, acknowledgements, queues, retries, and SASL/TLS behavior remain unchanged.

## 4. Java job construction

`ServiceGraphJob.main()` builds `EngineConfig` from environment variables, configures checkpointing and restart behavior, calls `configure()`, and executes the graph.

`EngineConfig` validates:

- Kafka brokers, topics, group ID, and security properties;
- positive default and per-type TTLs;
- checkpoint and restart settings;
- supported semantic types in `GRAPH_ELEMENT_TTL_SECONDS`.

`ServiceGraphJob.configure()` creates a `KafkaSource<byte[]>`. Its deserializer returns the record bytes unchanged. The source has stable UID `graph-kafka-source`.

The source stream calls `flatMap(new MetricParser())`, declares `ContributionTypeInformation`, assigns timestamps/watermarks, then keys by `Contribution.elementKey()`. `ElementLifecycleFunction` owns all state and timers after that key. `GraphEventKafkaSerializer` writes deterministic JSON with `element_id` as the Kafka key.

There is one input lane. No union, reconciliation source, retraction stream, or entity-source state exists.

## 5. Protobuf evidence parsing

`MetricParser.flatMap(byte[], Collector<Contribution>)` calls `parse()` and emits accepted contributions. Rejections increment `rejected_inputs`, a reason-specific counter, and a bounded/rate-limited log entry.

`MetricParser.parse()` performs these steps:

1. `ExportMetricsServiceRequest.parseFrom(payload)` decodes OTLP Protobuf.
2. It walks resource metrics, scope metrics, and metrics.
3. It ignores names other than request-total and discovery calls.
4. It requires OTLP `Sum` with delta temporality.
5. It accepts integer or double datapoints only when the value is positive and finite and the timestamp is positive.
6. `scalarAttributes()` decodes scalar string, boolean, integer, and double attributes.
7. It dispatches to `servicegraph()` or `discovery()` and discards the numeric value.

Malformed Protobuf, wrong types, wrong temporality, zero, negative, nonfinite, or invalid semantic dimensions become observable rejections. Payload bodies are never logged.

### 5.1 Servicegraph extraction

`servicegraph(attributes, observed)` requires `client` and `server`. `observation()` independently strips the `client_` and `server_` prefixes, supplies `service.name`, and asks `SemanticRegistry.extractValid()` for every complete entity eligible for the generated `service_graph` source. `materializeObservation()` adds every matching `observation` relationship whose endpoints exist in that observation.

Missing, empty, and Collector-provided `unset` connection types normalize to `unset`. The registry selects `interaction` relationships by their generated `connection_type`, then expands them from client entities to server entities. Unknown selectors produce an observable rejection while preserving valid client and server observation elements. Java contains no entity names, relationship names, endpoint combinations, or selector mappings.

`add()` detects identity conflicts and merges duplicate elements deterministically. Registry expansion universally skips an edge when its source and target IDs are equal; same-type entities with different IDs remain eligible. Every resulting node and edge becomes a separate `Contribution` with the same contributor and timestamp.

### 5.2 Discovery extraction

`discovery(attributes, observed)` treats the datapoint attributes as one observation and selects only entities and relationships generated for the `span_metrics` source. It creates one contribution per node or edge. The contributor ID hashes the discovery metric name, element ID, and modeled attributes.

Eligible roots retain native `span.name` and `span.kind`. Together with `service.name`, these fields identify a generated `transaction`; the observation rule emits `service -[executes]→ transaction`. Unsupported span kinds or incomplete transaction identity skip only the transaction while other complete entities remain valid.

### 5.3 SemanticRegistry

`SemanticRegistry` loads `semantic-registry.json` once. Its main operations are:

- `extract(attributes)` validates identifying and descriptive fields and creates nodes;
- `supportsEntity(evidenceSource, type)` limits entity extraction to the current lane;
- `edges(sources, targets, evidenceSource, scope[, connectionType])` expands only generated relationship rules over observed entities;
- `quotedId()` creates the same percent-encoded IDs as the SDK;
- `edgeId()` creates deterministic relationship IDs.

The only universal topology rule in Java is the prohibition on equal endpoint IDs.

## 6. Domain model

`Element` is the top-level sealed node/edge interface in the `model` package. `Node` contains ID, type, and immutable attributes. `Edge` adds source and target IDs. Neither type contains metrics.

`Contribution` is one immutable observation record:

```text
contributorId
observedAtUnixNano
element
```

It has no operation flag, retraction, metric delta, or TTL override.

`Snapshot` stores the observation timestamp, event-time deadline, processing-time deadline, and element. `State` is the pure reference form used by lifecycle functions. `Aggregate` is the compact persisted element ID and last payload hash.

`Event` has `Upsert` and `Delete` implementations. `toMap()` always writes schema 3.0. `fromMap()` rejects other schemas and rejects elements containing a removed `metrics` field.

## 7. Lifecycle policy and state

`LifecyclePolicy.ttlSeconds(element)` starts from the default, applies a type override, and for edges also considers source and target type overrides. The minimum protects an endpoint whose policy is shorter than the relationship policy.

`snapshot(contribution, eventFloor, processingNow)` computes both deadlines from the selected TTL. Event time uses the later of the observation and watermark floor. Processing time provides expiry when the source stays idle and watermarks stop.

`ElementLifecycleFunction.open()` creates:

| Descriptor | Value |
| --- | --- |
| `graph-element-contributors` | contributor ID → CBOR-v4 `Snapshot` |
| `graph-element-aggregate` | CBOR-v4 `Aggregate` |
| `graph-element-attribute-winners` | CBOR-v4 `AttributeWinners` |
| event timer state | next registered event-time millisecond |
| processing timer state | next registered processing-time millisecond |

These descriptive state and operator names replace the old version-suffixed names at the clean CBOR-v4 boundary. Serializer snapshot version 4 deliberately rejects all older state.

## 8. Common observation path

`processElement()` first calls `observe()`.

`observe()`:

1. point-reads the current contributor snapshot;
2. ignores an older observation from that contributor;
3. loads or rebuilds `AttributeWinners`;
4. falls back to the pure merge path if replacing this contribution can remove a winning attribute;
5. computes the new snapshot and updates the winner index;
6. hashes the complete winning element only when it changed;
7. writes the contributor, winner index, and compact aggregate;
8. advances either timer only when the new deadline is earlier than the registered wake-up;
9. emits an upsert only when the complete element hash changed.

An identical request every minute therefore refreshes that contributor's 24-hour deadline without producing one event per request and without scanning thousands of contributors.

`AttributeWinners.observe()` updates attribute owners using observation time, then canonical contributor ID as the tie breaker. It point-reads another contributor only when ownership changes. `losesAttribute()` detects cases that require rebuilding from all retained snapshots.

## 9. Fallback merge and expiry

`GraphLifecycle.apply()` is the pure reference implementation. It rejects identity conflicts, replaces the contributor snapshot, calls `merge()`, hashes the complete element, and suppresses unchanged events.

`GraphLifecycle.merge()` selects a winner independently for every attribute. Node/edge identity and endpoints remain fixed for the key.

`onTimer()` clears the firing timer state and calls `GraphLifecycle.expire()` with the appropriate clock. `expire()` removes every contributor whose deadline is at or before the callback timestamp.

- If no contribution expired, the operator simply reschedules.
- If some expired and others remain, it rebuilds winners and emits only if the aggregate changed.
- If the final contributor expired, it clears state and emits a delete.

`scheduleTimers()` scans contributors only after a callback or fallback mutation to discover the next minimum. `replaceTimer()` maintains at most one event-time and one processing-time timer per element. A conservative old callback is safe: refreshed snapshots survive and the next minimum is registered.

## 10. CBOR-v4 state and checkpoints

`StateSerializer` writes exactly:

```text
format byte = 4
frame length
CBOR map
```

There is no legacy string sentinel or multi-version reader. `StateSerializer.Snapshot` accepts only snapshot version 4 with the same `Kind`. Contribution and event stream serializers use the same typed format through `ContributionTypeInformation` and `EventTypeInformation`.

`DeploymentCommands.CURRENT_RUNTIME` is `java-cbor-v4`. `validateRuntime()` accepts only that marker for restore/savepoint operations. An unmarked state directory or any older marker requires the documented clean reset.

Incremental RocksDB checkpoints capture contributors, winner indexes, aggregates, and registered timers. Recovery tests prove an idle restored timer can publish final deletion without new input.

## 11. Public event serialization

`GraphLifecycle.payloadHash()` hashes only the complete element. The top-level
model records recursively order semantic maps, so default Jackson serialization is stable
without a separate deterministic JSON implementation. Evidence magnitude cannot
affect hashes or event IDs.

`GraphEventKafkaSerializer.serialize()` uses deterministic Jackson JSON and sets the Kafka key to `element_id`. Flink checkpoint state uses a separate CBOR mapper. The compacted topic can reconstruct the latest graph from complete replacements and deletions.

Schema-3 edges contain:

```text
kind, id, type, source_id, target_id, attributes
```

Nodes contain `kind, id, type, attributes`.

## 12. Indexer projection

`run_indexer()` initializes ArangoDB, loads generated schema version 3, constructs a manual-commit Kafka consumer, and repeatedly calls `project_poll()`.

`_decode_event()` requires a JSON object, schema 3.0, and event type `graph_element_state_changed`. `event_to_document()` rejects a `metrics` field, validates kind and semantic type, verifies edge endpoint types, computes the stable Arango key, and copies canonical attributes plus generated attribute aliases.

`project_poll()` coalesces the newest record per element within the poll, groups replacements and deletions by collection, writes them, and commits the next offsets only after every database operation succeeds. Replays are idempotent because replacements use stable keys and deletes tolerate missing documents.

Node deletes route by the semantic prefix in `element_id`. Edge deletes are attempted against every generated edge collection because edge IDs deliberately hide relationship type.

## 13. ArangoDB and typed Gremlin

The generated Arango schema defines vertex collections, edge collections, endpoint collection constraints, identifying attribute aliases, graph name, schema hash, and schema version 3. It defines no metric field or alias.

Gremlin Server exposes the generated graph through a read-only traversal source. `SemanticGremlinClient.query()` validates that a callback returns an unexecuted traversal whose final traversers are vertices or edges, appends `elementMap()`, and reconstructs results.

`_semantic_element_from_map()` passes the stored `element_id` into
`semantic_entity_from_data()` or `semantic_edge_from_data()`. Semantic type and
endpoint types select the generated model. The classes validate schema fields
and preserve Java's identity without recalculating it.

## 14. Deployment order

Deploy generated schema/indexer and Gremlin artifacts first so the transaction
and executes collections exist before their events arrive. Deploy Flink next,
then the Collector. Alpha revisions provide one current generated contract and
no compatibility aliases or multi-version readers. Rebuild disposable state
when moving between incompatible alpha revisions.

## 15. Verification map

- `MetricParserTest`: Protobuf interaction/discovery evidence, malformed bytes, type, temporality, value validation, ignored failure metric, stable IDs and attributes.
- `LifecycleGoldenTest`: refresh suppression, deterministic winners, partial expiry, final delete, identity validation.
- `ElementLifecycleFunctionTest`: conservative callbacks, checkpoint/recovery, idle deletion, policy timers.
- `GranularLifecycleStateTest`: point refresh behavior with thousands of contributors.
- `StateSerializerTest`: direct CBOR-v4 frames and old-format rejection.
- indexer tests: schema-3 replacement, replay coalescing, deletion, commit-after-success, schema-2 and metrics rejection.
- SDK tests: metric-free edge construction and typed Gremlin reconstruction.
- codegen tests: Arango schema version 3, transaction topology, connection selectors, and absence of metric fields or aliases.
- Helm renders: Protobuf exporters and absence of the entity-log lane.
- disposable E2E: Collector evidence, Flink checkpoints/recovery, schema-3 projection, typed traversal, expiry, and controlled throughput measurements.
