# Helm Values

This page summarizes user-facing values. The chart `values.yaml` files remain
the executable defaults.

## Collector

| Path | Default | Purpose |
| --- | --- | --- |
| `image.repository` | `otel/opentelemetry-collector-contrib` | Collector image |
| `image.tag` | `0.156.0` | Collector tag |
| `image.digest` | empty | Optional immutable digest |
| `router.replicaCount` | `2` | Fixed stateless router count |
| `router.queueSize` | `100000` | Load-balancer sending queue |
| `backend.replicaCount` | `2` | Fixed stateful backend count |
| `backend.serviceGraph.storeTtl` | `10s` | Unpaired span retention |
| `backend.serviceGraph.storeMaxItems` | `10000` | Pairing-store limit |
| `backend.serviceGraph.metricsFlushInterval` | `5s` | Metric flush period |
| `streamContract.kafka.brokers` | example broker | Kafka bootstrap servers |
| `streamContract.kafka.security.protocol` | `SASL_SSL` | `PLAINTEXT`, `SASL_PLAINTEXT`, or `SASL_SSL` |
| `streamContract.kafka.security.existingSecret` | `servicegraph-kafka-auth` | Existing SASL credentials |
| `streamContract.topics.servicegraphMetrics` | `otel.servicegraph.metrics` | Metrics topic |
| `streamContract.topics.entityEvents` | `otel.entity.events` | Optional filtered OTLP entity-event logs topic |
| `entityEvents.enabled` | `false` | Add the router entity-event logs pipeline |

Resource, scheduling, ServiceAccount, image-pull, and security-context values are
also available in the chart.

## Flink

| Path | Default | Purpose |
| --- | --- | --- |
| `image.ref` | internal example | Complete runtime image |
| `application.clusterId` | `servicegraph-diff` | Standalone Flink cluster ID |
| `application.parallelism` | `3` | Job parallelism |
| `application.jobManagerReplicas` | `1` | Restartable JobManager count |
| `application.taskManagerReplicas` | `2` | Fixed TaskManager count |
| `application.taskManagerSlots` | `2` | Slots per TaskManager |
| `job.fixedJobId` | `000...001` | Stable Flink job ID |
| `job.allowNonRestoredState` | `false` | Allow reviewed upgrades to discard unmapped savepoint state |
| `job.groupId` | `graph-element-engine` | Kafka source group |
| `streamContract.topics.entityEvents` | `otel.entity.events` | Optional entity-event input topic |
| `entityEvents.enabled` | `false` | Add the independent entity-event Kafka source |
| `entityEvents.groupId` | `graph-element-engine-entities` | Entity-event source consumer group |
| `entityEvents.reportIntervalGraceSeconds` | `30` | Grace added to positive report intervals |
| `job.interactionTtlSeconds` | `300` | Delete inactivity threshold |
| `job.allowedLatenessSeconds` | `60` | Watermark out-of-order bound |
| `job.stateTtlSeconds` | `86400` | Keyed-state cleanup TTL |
| `job.checkpointIntervalMs` | `30000` | Checkpoint interval |
| `job.restartAttempts` | `3` | Fixed-delay attempts |
| `logging.rootLevel` | `INFO` | Flink console log level |
| `rbac.create` | `true` | Create ConfigMap-only runtime RBAC |
| `storage.createClaim` | `true` | Create state claim |
| `storage.existingClaim` | empty | Use existing state claim |
| `storage.storageClassName` | `rwx` | Shared storage class |
| `storage.accessModes` | `[ReadWriteMany]` | State volume access |
| `storage.retainClaim` | `true` | Keep created claim on uninstall |

Kafka security fields and every enabled topic name must match the Collector and
consumers.

## ArangoDB indexer

| Path | Default | Purpose |
| --- | --- | --- |
| `image.repository` | internal example | Indexer image |
| `image.tag` | `0.1.0` | Indexer image tag |
| `replicas` | `1` | Kafka indexer replicas |
| `consumerGroupId` | `servicegraph-arangodb-indexer` | Kafka group |
| `arangodb.urls` | local example | ArangoDB endpoints |
| `arangodb.database` | `servicegraph` | Existing database |
| `arangodb.graph` | `servicegraph` | Named graph |
| `arangodb.allowDatabaseCreation` | `false` | Local-only database creation |
| `arangodb.auth.existingSecret` | writer Secret | Writer credentials |

## Gremlin Server

| Path | Default | Purpose |
| --- | --- | --- |
| `image.repository` | internal example | Validated Gremlin image |
| `image.tag` | `0.1.0` | Gremlin image tag |
| `replicas` | `1` | Read-only server replicas |
| `service.port` | `8182` | GraphBinary/WebSocket port |
| `gremlin.evaluationTimeoutMs` | `30000` | Traversal timeout |
| `arangodb.auth.existingSecret` | reader Secret | Read-only credentials |
| `jvm.options` | bounded 384 MiB heap | JVM memory settings |

## Local ArangoDB

| Path | Default | Purpose |
| --- | --- | --- |
| `image.tag` | `3.12.9.4` | Local ArangoDB version |
| `persistence.size` | `2Gi` | Development data claim |
| `persistence.existingClaim` | empty | Optional existing RWO claim |

## Demo

| Path | Default | Purpose |
| --- | --- | --- |
| `collector.endpoint` | router OTLP HTTP URL | Trace destination |
| `traffic.emitIntervalSeconds` | `2` | Delay between batches |
| `traffic.topologyChangeIntervalSeconds` | `20` | Delay between graph changes |
| `traffic.initialEdges` | `2` | Initial active edges |
| `traffic.maxActiveEdges` | `6` | Maximum active edges |
| `traffic.requestsPerTick` | `3` | Traces per batch |
| `traffic.errorRate` | `0.08` | Failed trace probability |
| `traffic.serviceNamespace` | `shop` | Emitted service namespace |
| `traffic.instanceId` | `live-demo` | Instance-ID suffix |
| `traffic.randomSeed` | empty | Optional deterministic seed |

## Inspect exact defaults

```console
helm show values deploy/helm/servicegraph-collector
helm show values deploy/helm/servicegraph-flink
helm show values deploy/helm/servicegraph-arangodb
helm show values deploy/helm/servicegraph-indexer
helm show values deploy/helm/servicegraph-gremlin
helm show values deploy/helm/servicegraph-demo
```
