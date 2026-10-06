# Flink configuration

The processing chart runs one native Java job. Kafka input is OTLP Protobuf evidence from `otel.servicegraph.metrics`; output is canonical schema-3 JSON in the compacted `graph.elements.events` topic.

## Lifecycle settings

```yaml
job:
  groupId: graph-element-engine
  interactionTtlSeconds: 86400
  elementTtlSeconds: {}
  contributionCoalescingIntervalSeconds: 300
  allowedLatenessSeconds: 60
  checkpointIntervalMs: 30000
  checkpointTimeoutMs: 600000
  checkpointMinPauseMs: 5000
  tolerableFailedCheckpoints: 1
  restartAttempts: 3
  restartDelaySeconds: 10
```

`interactionTtlSeconds` is the default contributor freshness period. `elementTtlSeconds` can shorten or lengthen it by semantic type. For an edge, `LifecyclePolicy.ttlSeconds()` uses the shortest applicable edge or endpoint policy. Every contribution expires; there are no persistent or per-message TTL overrides.

`contributionCoalescingIntervalSeconds` limits unchanged evidence for one
element/contributor pair before it reaches the element-keyed lifecycle stage.
Its production default is five minutes. It must be `0` or shorter than the
default TTL and every per-type TTL. `0` makes the coalescer a stateless
pass-through.

New contributors and attribute or topology changes pass immediately. Repeated
unchanged evidence retains the newest observation and periodically refreshes
event-time and processing-time lifecycle deadlines without publishing another
upsert. The last contributor expiry publishes a delete.

## Kafka contract

```yaml
streamContract:
  kafka:
    brokers: [kafka.internal.example:9092]
    security:
      protocol: SASL_SSL
      saslMechanism: SCRAM-SHA-256
      existingSecret: servicegraph-kafka-auth
      usernameKey: username
      passwordKey: password
  topics:
    servicegraphMetrics: otel.servicegraph.metrics
    interactionEvents: graph.elements.events
```

The input topic must contain only serialized `ExportMetricsServiceRequest` messages. The job rejects malformed Protobuf, wrong metric types, non-delta temporality, and zero, negative, or nonfinite evidence through bounded rejection telemetry. It ignores unrelated metric names, including the removed failure metric.

The output topic contains schema-3 JSON and uses `element_id` as its Kafka key. Use log compaction and retain partition counts during cutover.

## State and recovery

The coalescer stores one latest CBOR-v4 contribution and one processing timer
per active `(element, contributor)` pair. Lifecycle stores one CBOR-v4 snapshot
per pair, a compact aggregate, an attribute-winner index, and two coalesced
timer deadlines per element. RocksDB incremental checkpoints are the production
default. Point refreshes read and write the affected contributor and winner
entries without scanning all contributors.

`java-cbor-v4` is a clean state boundary. Runtime checks reject missing markers, earlier `java-cbor` markers, Python state, and unknown markers when an upgrade could restore old state. Clear checkpoints, savepoints, HA metadata, the runtime marker, and source-group offsets before deploying this version.

## Environment mapping

| Environment variable | Helm value |
| --- | --- |
| `KAFKA_BOOTSTRAP_SERVERS` | `streamContract.kafka.brokers` |
| `INTERACTION_DIFF_INPUT_TOPIC` | `streamContract.topics.servicegraphMetrics` |
| `INTERACTION_DIFF_OUTPUT_TOPIC` | `streamContract.topics.interactionEvents` |
| `INTERACTION_DIFF_GROUP_ID` | `job.groupId` |
| `INTERACTION_DIFF_TTL_SECONDS` | `job.interactionTtlSeconds` |
| `GRAPH_ELEMENT_TTL_SECONDS` | JSON form of `job.elementTtlSeconds` |
| `CONTRIBUTION_COALESCING_INTERVAL_SECONDS` | `job.contributionCoalescingIntervalSeconds` |
| `INTERACTION_DIFF_ALLOWED_LATENESS_SECONDS` | `job.allowedLatenessSeconds` |
| `FLINK_CHECKPOINT_INTERVAL_MS` | `job.checkpointIntervalMs` |
| `FLINK_PARALLELISM` | `application.parallelism` |

The deleted entity-event environment variables are intentionally unsupported.
