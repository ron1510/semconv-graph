# Flink Job Configuration

The Flink chart runs a standalone Session cluster and submits one PyFlink job
through its internal REST Service. The submitter, JobManager, and TaskManagers
use the same immutable runtime image.

## Application sizing

```yaml
application:
  clusterId: servicegraph-diff
  parallelism: 3
  jobManagerReplicas: 1
  taskManagerReplicas: 2
  taskManagerSlots: 2
  jobManagerProcessMemory: 1200m
  taskManagerProcessMemory: 1800m
```

Helm maintains the configured TaskManager replica count. Available execution
capacity is `taskManagerReplicas * taskManagerSlots`; it must cover job
parallelism. Kubernetes memory limits must remain above the corresponding
Flink process-memory values.

The cluster ID and fixed job ID must be unique within a namespace.

## Lifecycle settings

```yaml
job:
  fixedJobId: "00000000000000000000000000000001"
  allowNonRestoredState: false
  groupId: graph-element-engine
  interactionTtlSeconds: 300
  elementTtlSeconds:
    service: 900
    etl.pipeline: 86400
    etl.run: 3600
    etl.part.run: 1800
    calls: 300
  allowedLatenessSeconds: 60
  checkpointIntervalMs: 30000
  restartAttempts: 3
  restartDelaySeconds: 10
```

| Value | Meaning |
| --- | --- |
| `fixedJobId` | Stable 32-hex job ID used for recovery and duplicate prevention |
| `allowNonRestoredState` | Permit an upgrade to discard savepoint state that no longer maps to an operator |
| `interactionTtlSeconds` | Inactivity period before an internal contributor is retracted |
| `elementTtlSeconds` | Optional inactivity thresholds keyed by semantic node or relationship type |
| `allowedLatenessSeconds` | Out-of-order bound used to generate watermarks |
| `checkpointIntervalMs` | Source-offset and state checkpoint interval |
| `restartAttempts` | Fixed-delay job restart attempts |
| `restartDelaySeconds` | Delay between restart attempts |

Contributor expiry is owned by event-time and processing-time business timers.
Generic Flink state TTL is deliberately disabled because it cannot emit the
required graph delete when it removes state.

Each service-graph contribution uses the configured TTL for its semantic node
type, falling back to `interactionTtlSeconds`. An edge inherits the shorter TTL
of its endpoint node types. A configured relationship TTL can shorten that
period but cannot make the edge outlive either endpoint. Explicit entity-event
report intervals remain authoritative for those contributions.

Changing a type policy does not rewrite checkpointed snapshots immediately.
The new TTL applies when each contributor is next observed and its absolute
event-time and processing-time expiry timestamps are refreshed.

## Optional entity-event source

The standard entity-event source is disabled by default. Enable it with a
dedicated topic, consumer group, and report-interval grace:

```yaml
streamContract:
  topics:
    servicegraphMetrics: otel.servicegraph.metrics
    entityEvents: otel.entity.events
    interactionEvents: graph.elements.events

entityEvents:
  enabled: true
  groupId: graph-element-engine-entities
  reportIntervalGraceSeconds: 30
```

| Value | Meaning |
| --- | --- |
| `entityEvents.enabled` | Create the independent entity-event Kafka source; default `false` |
| `streamContract.topics.entityEvents` | OTLP JSON logs topic; required when enabled |
| `entityEvents.groupId` | Consumer group used only by the entity-event source |
| `entityEvents.reportIntervalGraceSeconds` | Nonnegative seconds added to a positive `entity.report.interval` |

The entity-event topic must differ from both the metrics input and graph output
topics. It uses the same Kafka brokers and security settings, starts from
committed offsets or `earliest`, and has automatic commits and automatic topic
creation disabled. Metrics continue to use `job.groupId`; enabling entity
events does not change or share that source's offsets.

### Parsing and reconciliation

Flink accepts `entity.state` and `entity.delete` OTLP JSON log records. It also
recognizes the compatibility `otel.entity.event.type` attribute when records
reach the topic without a standard event name. Unrelated logs are ignored;
malformed supported events increment `rejected_entity_events`, log a bounded
warning, and do not restart the job.

Each complete state is scoped to one semantic entity. Flink derives the source
key from the registered entity type and exact registered identity. OTLP Resource
and instrumentation Scope do not create independent contributors.

For a newer `entity.state`, Flink reconstructs the source node and every
registry-approved outgoing relationship. Outgoing relationships present in the
explicit source's previous state but omitted now are retracted immediately. An
`entity.delete` retracts that source's node and recorded outgoing
relationships. It does not yet remove incoming relationships owned by other
source entities.

Only entity types participating in generated `service_graph` relationships are
accepted, because that set owns the generated ArangoDB topology. A type can be
known to the upstream SDK and still be rejected when no projectable collection
exists. Relationship source type, relationship type, and target type must match
an exact generated relationship definition.

### Expiry precedence

For each explicit state contribution:

1. A positive `entity.report.interval` in seconds uses
   `interval + entityEvents.reportIntervalGraceSeconds`.
2. An absent or zero interval does not expire from inactivity and remains until
   a newer complete state or explicit delete retracts it.
3. Negative, non-integer, or otherwise malformed intervals reject that event.

Because report intervals arrive at runtime, Helm validates only the configured
grace. Positive intervals create business timers; absent and zero intervals
remain in checkpointed state until explicit reconciliation retracts them.

### Identity and output boundary

Entity IDs follow the exact identifying fields in the generated local semantic
model. Events with extra or missing `entity.id` keys are rejected rather than
merged under an incomplete identity. Output remains project schema `2.0` on
`graph.elements.events`; Flink does not emit standard OTel entity events or
historical state.

## Optional root-span discovery source

Enable the independent OTLP trace source with:

```yaml
streamContract:
  topics:
    rootSpans: otel.root.spans

rootSpanDiscovery:
  enabled: true
  groupId: graph-element-engine-root-spans
```

The source starts from committed offsets or `earliest` and uses the same Kafka
security settings as the metrics source. For each root span, Flink combines
scalar Resource and span attributes, rejects conflicting values for modeled
semantic fields, and calls the generated entity extractor once. The span end
timestamp becomes the observation timestamp.

Every identifiable semantic entity becomes a node contribution. `AppEndpoint`
is permitted only for server-kind roots. A valid root with no identifiable
entity produces nothing, and this lane never emits edges or metric deltas.

Contributor identity excludes trace IDs, span IDs, and timestamps. Repeated
equivalent executions therefore refresh one contributor, while changed
canonical semantic attributes create distinct contributors. Nodes inferred by
servicegraph metrics and root spans merge under the same deterministic element
ID and expire only after their final contributor disappears. A node discovered
only from a root span remains disconnected until servicegraph or an explicit
entity event supplies a relationship.

## Kafka contract

The input topic contains OTLP JSON metrics from the Collector. The output topic
contains authoritative graph-element commands. The legacy values key remains
unchanged to avoid Helm template churn:

```yaml
streamContract:
  kafka:
    brokers:
      - kafka.internal.example:9092
    security:
      protocol: SASL_SSL
      saslMechanism: SCRAM-SHA-256
      existingSecret: servicegraph-kafka-auth
  topics:
    servicegraphMetrics: otel.servicegraph.metrics
    entityEvents: otel.entity.events
    rootSpans: otel.root.spans
    interactionEvents: graph.elements.events
```

The source starts from committed offsets or `earliest` when the consumer group
has no offsets. Auto topic creation is disabled. The output sink is
at-least-once.

When enabled, the entity-event source follows the same offset and security
rules but uses `entityEvents.groupId` independently.

The root-span source likewise uses `rootSpanDiscovery.groupId` independently.

Supported protocols are `PLAINTEXT`, `SASL_PLAINTEXT`, and `SASL_SSL`.
Both SASL modes use the configured SCRAM credentials. `SASL_SSL` validates
broker certificates with the Flink image's default JVM trust store; no Kafka CA
file is mounted by the chart. `SASL_PLAINTEXT` sends credentials and records
without TLS and is appropriate only on a trusted internal network.

## Persistent state

The default chart uses one shared claim for:

- Kubernetes HA metadata;
- checkpoints;
- savepoints.

```yaml
storage:
  createClaim: true
  storageClassName: rwx
  size: 10Gi
  accessModes: [ReadWriteMany]
  retainClaim: true
```

Use `storage.existingClaim` to reference a pre-provisioned claim. A multi-node
cluster requires storage that all eligible JobManager and TaskManager nodes can
mount. The default file-based design is intentionally simple for internal
clusters; validate the storage system's availability guarantees separately.

With `retainClaim: true`, Helm annotates a created claim with
`helm.sh/resource-policy: keep`. Uninstalling the release does not delete it.

## Runtime and submission

Helm renders the JobManager and TaskManager Deployments directly. A
post-install Job waits for the REST Service and runs:

```text
flink run --detached -m servicegraph-diff-rest:8081 --pyModule otel_servicegraph_diff.flink_job
```

The submitter skips submission if the fixed job ID is already active and
rejects accidental reuse of a terminal job ID.

On every Helm upgrade, a `pre-upgrade` Job gracefully stops the active job and
records its savepoint path on the state claim. The JobManager and TaskManagers
then roll to the new Helm revision. The submitter runs as a `post-upgrade` hook
and restores the new package and job configuration from the savepoint with the
same fixed job ID.

Keep `application.clusterId`, `job.fixedJobId`, and the state claim unchanged
across this operation. The automatic upgrade fails closed if the active job is
missing, the savepoint cannot be created, or the new job cannot restore all
savepoint state. `job.allowNonRestoredState=true` relaxes only the final state
mapping check and should be used for reviewed topology changes.

Kubernetes HA stores job metadata pointers in ConfigMaps and durable metadata
on the state claim. When the JobManager pod is replaced, Flink recovers the
same job and restores keyed state and source progress from the latest completed
checkpoint. One JobManager means recovery includes brief downtime.

The runtime ServiceAccount needs namespace-scoped ConfigMap CRUD/list/watch.
Set `serviceAccount.create=false` and `rbac.create=false` to use a
platform-provided account. A workload does not select an existing RoleBinding
by name. The existing RoleBinding must already name the configured
ServiceAccount as a subject.

## Logs

Flink writes control-plane logs to JobManager stdout and operator, Kafka, and
Python worker logs to TaskManager stdout. Set `logging.rootLevel` to control
the root level; the default is `INFO`.

```console
kubectl logs -n servicegraph-system \
  deployment/processing-servicegraph-flink-jobmanager --follow
kubectl logs -n servicegraph-system \
  deployment/processing-servicegraph-flink-taskmanager --follow --prefix
kubectl logs -n servicegraph-system <pod-name> --previous
```

## Environment mapping

The chart maps values to these application variables:

| Environment variable | Helm value |
| --- | --- |
| `KAFKA_BOOTSTRAP_SERVERS` | `streamContract.kafka.brokers` |
| `INTERACTION_DIFF_INPUT_TOPIC` | `streamContract.topics.servicegraphMetrics` |
| `INTERACTION_DIFF_OUTPUT_TOPIC` | `streamContract.topics.interactionEvents` |
| `INTERACTION_DIFF_GROUP_ID` | `job.groupId` |
| `ENTITY_EVENTS_INPUT_TOPIC` | `streamContract.topics.entityEvents` when `entityEvents.enabled=true` |
| `ENTITY_EVENTS_GROUP_ID` | `entityEvents.groupId` when enabled |
| `ENTITY_EVENTS_REPORT_INTERVAL_GRACE_SECONDS` | `entityEvents.reportIntervalGraceSeconds` when enabled |
| `ROOT_SPANS_INPUT_TOPIC` | `streamContract.topics.rootSpans` when `rootSpanDiscovery.enabled=true` |
| `ROOT_SPANS_GROUP_ID` | `rootSpanDiscovery.groupId` when enabled |
| `INTERACTION_DIFF_TTL_SECONDS` | `job.interactionTtlSeconds` |
| `INTERACTION_DIFF_ALLOWED_LATENESS_SECONDS` | `job.allowedLatenessSeconds` |
| `FLINK_CHECKPOINT_INTERVAL_MS` | `job.checkpointIntervalMs` |
| `FLINK_PARALLELISM` | `application.parallelism` |

## Validate

```console
helm lint deploy/helm/servicegraph-flink
helm template processing deploy/helm/servicegraph-flink \
  --namespace servicegraph-system \
  --values internal-flink-values.yaml
```

The rendered output includes the complete runtime topology, the install and
upgrade submitter, and the pre-upgrade savepoint hook.
