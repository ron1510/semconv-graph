# Upgrades and recovery

## Clean cutover to Protobuf evidence and schema 3

This release changes the Kafka input encoding, public event schema, Arango schema, and Flink state shape together. It cannot restore older checkpoints or consume mixed-version topic history.

1. Stop Collector ingestion.
2. Stop Flink and the indexer.
3. Record the existing partition counts and topic configuration.
4. Recreate `otel.servicegraph.metrics` with the same configuration for Protobuf-only records.
5. Recreate compacted `graph.elements.events` for schema-3-only records.
6. Clear Flink checkpoints, savepoints, HA metadata, upgrade handoff files, and the runtime marker.
7. Reset the `graph-element-engine` source-group offsets.
8. Reset the indexer consumer group.
9. Remove only the generated `servicegraph` graph definition and its generated vertex/edge documents from ArangoDB. Preserve the server, databases unrelated to this graph, credentials, and Secrets.
10. Deploy generated Arango schema version 2, the indexer, and Gremlin.
11. Deploy Flink and verify the `java-cbor-v2` marker and new checkpoints.
12. Deploy the Collector and resume telemetry.
13. Verify that the graph reconstructs naturally.

Old markers including `java`, `java-cbor-v1`, Python markers, and unmarked state are rejected. `allowNonRestoredState` does not migrate serializers and is not a substitute for this reset.

## Routine recovery after cutover

Once every checkpoint was created by `java-cbor-v2`, normal TaskManager and JobManager recovery restores contributor snapshots, attribute winners, aggregates, and two timers per element. A recovered processing-time timer can expire idle state without new Kafka input.

Before an ordinary deployment, check checkpoint age, failed-checkpoint count, Kafka lag, RocksDB disk capacity, and JobManager HA metadata. Do not prune state while a healthy deployment may still reference it.

## Schema and registry changes

A registry change must regenerate Java, Python, Collector, indexer, and Gremlin artifacts together. Bump the public event or storage schema when the stored shape changes. Consumers must be deployed before producers for incompatible contracts.

## Rollback

Rollback within the CBOR-v2/schema-3 generation can use its checkpoints and topic history if the code remains serializer-compatible. Rolling back to schema 2, CBOR v1, JSON input, or the removed entity-event/counter model requires the same clean reset and topic recreation as a forward cutover.
