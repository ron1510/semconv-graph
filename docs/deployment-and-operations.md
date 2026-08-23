# Kubernetes Deployment

This guide installs the service graph into an existing Kubernetes cluster. It
assumes Kafka is already available.

## Requirements

- Kubernetes 1.25 or newer;
- Helm 3;
- Kafka-compatible brokers reachable from the namespace;
- two pre-created topics, or three when entity events are enabled;
- ArangoDB 3.12 reachable from the namespace;
- an internal container registry;
- shared persistent storage for Flink;
- existing Kafka and ArangoDB writer/reader credential Secrets when authentication is
  enabled.

The charts create standard Kubernetes resources and no CRDs. Workloads run as
non-root users with privilege escalation disabled, all capabilities dropped,
read-only root filesystems, and `RuntimeDefault` seccomp.

## Build and publish images

Build the Flink runtime from the repository root:

```console
docker build \
  --file services/otel-servicegraph-diff/Dockerfile \
  --target runtime \
  --build-arg PIP_INDEX_URL=https://pypi.internal.example/simple \
  --secret id=maven_settings,src=$HOME/.m2/settings.xml \
  --tag registry.internal.example/extended-otel-flink-runtime:2.2.1-java11 \
  .
```

Build the indexer, Gremlin, and optional demo images:

```console
docker build --file services/servicegraph-indexer/Dockerfile \
  --tag registry.internal.example/extended-otel-servicegraph-indexer:0.1.0 .

docker build --file services/servicegraph-gremlin/Dockerfile \
  --tag registry.internal.example/extended-otel-servicegraph-gremlin:0.1.0 \
  services/servicegraph-gremlin

docker build --file services/servicegraph-demo/Dockerfile \
  --tag registry.internal.example/extended-otel-servicegraph-demo:0.1.1 .
```

Publish immutable tags or digests. The Flink image contains Python 3.12, the
application packages, Java serializers, and the Flink Kafka connector. No
runtime wheel side-loading is required.

Mirror the Collector image declared by the Collector chart when the cluster
cannot access public registries.

## Create Kafka topics

Create:

```text
otel.servicegraph.metrics
graph.elements.events
```

For opt-in OTel entity-event ingestion, also create:

```text
otel.entity.events
```

Choose partition counts for expected throughput and Flink parallelism. All
created topics should use retention and replication appropriate for your recovery
objectives. Disable automatic topic creation.

All installed components must use the same broker list, security configuration,
and topic names.

## Create the credentials Secret

For `SASL_PLAINTEXT` or `SASL_SSL`, create one Secret in the target
namespace containing:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: servicegraph-kafka-auth
type: Opaque
stringData:
  username: servicegraph
  password: replace-me
```

Use your secret-management system rather than committing this manifest.
`PLAINTEXT` requires no Secret. `SASL_PLAINTEXT` authenticates but does not
encrypt credentials or traffic and must be limited to a trusted internal
network. `SASL_SSL` uses the runtime image's default trust store; add private
certificate authorities to that trust store when building the image.

## Prepare values

Create `internal-collector-values.yaml`:

```yaml
image:
  repository: registry.internal.example/otelcol-contrib
  tag: "0.156.0"

streamContract:
  kafka:
    brokers: [kafka.internal.example:9093]
    security:
      protocol: SASL_SSL
      saslMechanism: SCRAM-SHA-256
      existingSecret: servicegraph-kafka-auth
      usernameKey: username
      passwordKey: password
  topics:
    servicegraphMetrics: otel.servicegraph.metrics
    entityEvents: otel.entity.events

entityEvents:
  enabled: true
```

Create `internal-flink-values.yaml`:

```yaml
image:
  ref: registry.internal.example/extended-otel-flink-runtime:2.2.1-java11

serviceAccount:
  create: false
  name: servicegraph-flink
rbac:
  create: false

streamContract:
  kafka:
    brokers: [kafka.internal.example:9093]
    security:
      protocol: SASL_SSL
      saslMechanism: SCRAM-SHA-256
      existingSecret: servicegraph-kafka-auth
      usernameKey: username
      passwordKey: password
  topics:
    servicegraphMetrics: otel.servicegraph.metrics
    entityEvents: otel.entity.events
    interactionEvents: graph.elements.events

entityEvents:
  enabled: true
  groupId: graph-element-engine-entities
  reportIntervalGraceSeconds: 30

storage:
  createClaim: false
  existingClaim: servicegraph-flink-state
```

The entity-event blocks are optional and default to disabled. Enable both
charts together and keep `streamContract.topics.entityEvents` identical. The
Flink entity-event consumer group is independent from `job.groupId` used by
service-graph metrics.

See [Collector configuration](configuration/collector.md), [Flink
configuration](configuration/flink.md), and the [Helm values
reference](reference/helm-values.md) before changing topology or state values.

Create `internal-indexer-values.yaml` and `internal-gremlin-values.yaml` for the
same Kafka contract and your existing ArangoDB service. See [ArangoDB and
Gremlin](deployment/arangodb-gremlin.md) for credentials, privileges, topology,
and traversal examples.

## Validate before installation

```console
helm lint deploy/helm/servicegraph-collector
helm lint deploy/helm/servicegraph-flink
helm lint deploy/helm/servicegraph-indexer
helm lint deploy/helm/servicegraph-gremlin

helm template collection deploy/helm/servicegraph-collector \
  --namespace servicegraph-system \
  --values internal-collector-values.yaml

helm template processing deploy/helm/servicegraph-flink \
  --namespace servicegraph-system \
  --values internal-flink-values.yaml

helm template indexer deploy/helm/servicegraph-indexer \
  --namespace servicegraph-system \
  --values internal-indexer-values.yaml

helm template gremlin deploy/helm/servicegraph-gremlin \
  --namespace servicegraph-system \
  --values internal-gremlin-values.yaml
```

Review rendered Secrets references, images, storage classes, RBAC, resource
limits, and namespace policy compatibility.

## Install collection and processing

```console
helm upgrade --install collection deploy/helm/servicegraph-collector \
  --namespace servicegraph-system \
  --create-namespace \
  --values internal-collector-values.yaml \
  --wait --timeout 5m

helm upgrade --install processing deploy/helm/servicegraph-flink \
  --namespace servicegraph-system \
  --values internal-flink-values.yaml \
  --wait \
  --timeout 10m
```

Helm creates the standalone JobManager and TaskManager Deployments before its
post-install submitter runs. Confirm both workloads are ready:

```console
kubectl rollout status deployment/processing-servicegraph-flink-jobmanager \
  --namespace servicegraph-system \
  --timeout=10m
kubectl rollout status deployment/processing-servicegraph-flink-taskmanager \
  --namespace servicegraph-system \
  --timeout=10m
```

The existing ServiceAccount needs ConfigMap CRUD/list/watch for Kubernetes HA.
It does not need Pod, Deployment, Service, CRD, or finalizer permissions.

Subsequent `helm upgrade` operations stop the active job with a savepoint,
roll the runtime image and configuration, and restore the same job ID from
that savepoint. Keep the cluster ID, fixed job ID, and state claim stable.

## Install projection and traversal access

The pre-install initializer creates or verifies the graph topology before the
indexer starts. Gremlin uses separate credentials with read-only database
access and `rw` limited to the provider's `TINKERPOP-GRAPH-VARIABLES`
collection:

```console
helm upgrade --install indexer deploy/helm/servicegraph-indexer \
  --namespace servicegraph-system \
  --values internal-indexer-values.yaml \
  --wait --timeout 5m

helm upgrade --install gremlin deploy/helm/servicegraph-gremlin \
  --namespace servicegraph-system \
  --values internal-gremlin-values.yaml \
  --wait --timeout 5m
```

Keep Gremlin internal to the cluster or use a local port-forward:

```console
kubectl port-forward --namespace servicegraph-system \
  service/gremlin-servicegraph-gremlin 8182:8182
```

Trusted clients use GraphBinary and traversal source `g`. There is no product
HTTP API or custom query language.

## Install optional demo traffic

```console
helm upgrade --install demo deploy/helm/servicegraph-demo \
  --namespace servicegraph-system \
  --values internal-demo-values.yaml \
  --wait --timeout 5m
```

Do not install the demo in a production telemetry namespace unless synthetic
services are explicitly desired.

## Verify end to end

1. Confirm both routers and both backends are ready.
2. Confirm the Flink job is `RUNNING`.
3. Confirm completed checkpoints continue increasing.
4. Send paired client/server traces to the router.
5. Confirm the metrics topic advances.
6. Confirm node and edge upserts appear.
7. Confirm the indexer commits offsets and documents appear in ArangoDB.
8. Traverse expected nodes and edges through Gremlin.
9. Stop the contributing telemetry and wait for the configured TTL.
10. Confirm Flink emits element deletes and ArangoDB removes the
    corresponding documents.

See [Monitoring](operations/monitoring.md) for commands and production signals.

## Uninstall behavior

Helm deletes the Flink Deployments, Service, and configuration. Kubernetes HA
ConfigMaps and the retained Flink claim can remain. Uninstalling the indexer or
Gremlin chart does not delete ArangoDB data. Inspect and preserve checkpoints
or savepoints before deleting runtime resources or storage.
