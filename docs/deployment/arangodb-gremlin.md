# ArangoDB and Gremlin

Flink remains the graph lifecycle authority. The access layer consumes the
unchanged schema `2.0` topic `graph.elements.events`, projects current state to
ArangoDB, and exposes the named graph through read-only Gremlin Server.

## Components

The deployment is split into three independent charts:

- `servicegraph-arangodb` is a single-node, development-only ArangoDB
  `3.12.9.4` StatefulSet. Production environments should use an externally
  operated ArangoDB deployment instead.
- `servicegraph-indexer` runs an idempotent initializer Job and one Kafka
  indexer Deployment. It has no Service, PVC, Kubernetes API access, or RBAC.
- `servicegraph-gremlin` runs TinkerPop `3.8.1` with ArangoDB provider `4.0.0`
  on Java 17 and exposes an internal Service on port `8182`.

The generated schema is committed at
`services/servicegraph-indexer/src/servicegraph_indexer/metadata/arangodb-graph-schema.json`.
Normal code generation and `--check` own it. The same content is packaged in
the Gremlin chart so provider topology cannot drift from indexer routing.

## ArangoDB preparation

The production database must exist unless `arangodb.allowDatabaseCreation` is
explicitly enabled. Give the initializer/indexer identity permission to create
and inspect collections, graphs, and indexes and to replace/delete documents in
the generated collections. The initializer is additive and idempotent. It
never drops collections or data and fails on incompatible collection types,
edge definitions, or named indexes.

Create one Secret for the writer:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: servicegraph-arangodb-writer
type: Opaque
stringData:
  username: servicegraph-indexer
  password: replace-me
```

Create a separate ArangoDB user with read-only database access. Provider
`4.0.0` rewrites its version document whenever it opens a graph, so grant this
user `rw` only on the initializer-created `TINKERPOP-GRAPH-VARIABLES`
collection. All generated vertex and edge collections remain read-only.
Expose the user in a second Secret:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: servicegraph-arangodb-reader
type: Opaque
stringData:
  username: servicegraph-reader
  password: replace-me
```

Gremlin is protected twice: the traversal source applies `ReadOnlyStrategy`,
and its ArangoDB credentials cannot write graph elements. The narrow metadata
grant does not permit vertex or edge mutations.

## Build images

```powershell
docker build --file services/servicegraph-indexer/Dockerfile `
  --tag registry.internal.example/extended-otel-servicegraph-indexer:0.1.0 .

docker build --file services/servicegraph-gremlin/Dockerfile `
  --tag registry.internal.example/extended-otel-servicegraph-gremlin:0.1.0 `
  services/servicegraph-gremlin
```

The Gremlin image is adapted from the validated sibling TinkerPop runtime. Set
`TINKERPOP_SERVER_URL` and `MAVEN_REPOSITORY_URL` build arguments when builds
must use internal artifact mirrors. An existing validated internal image can be
used directly through chart image values.

## Install

For local development only:

```powershell
helm upgrade --install arangodb deploy/helm/servicegraph-arangodb `
  --namespace servicegraph-system --create-namespace
```

Install the indexer against local or external ArangoDB:

```powershell
helm upgrade --install indexer deploy/helm/servicegraph-indexer `
  --namespace servicegraph-system `
  --set 'arangodb.urls[0]=http://arangodb-servicegraph-arangodb:8529' `
  --set arangodb.allowDatabaseCreation=true `
  --set 'streamContract.kafka.brokers[0]=kafka:9092' `
  --set streamContract.kafka.security.protocol=PLAINTEXT
```

Then install Gremlin:

```powershell
helm upgrade --install gremlin deploy/helm/servicegraph-gremlin `
  --namespace servicegraph-system `
  --set arangodb.host=arangodb-servicegraph-arangodb
```

Kafka security supports `PLAINTEXT`, `SASL_PLAINTEXT`, and `SASL_SSL` with
SCRAM-SHA-256. SASL credentials come from an existing Secret. No Kafka CA file
is mounted by this chart.

## Query

Port-forward the internal endpoint for local use:

```powershell
kubectl port-forward --namespace servicegraph-system `
  service/gremlin-servicegraph-gremlin 8182:8182
```

Install and use the typed GraphBinary client:

```console
pip install "extended-opentelemetry-semconv[gremlin]==0.5.0"
```

```python
from extended_otel_semconv.gremlin import SemanticGremlinClient

with SemanticGremlinClient("ws://127.0.0.1:8182/gremlin") as client:
    checkout = client.query(
        lambda g: g.V().has_label("service").has("service_name", "checkout")
    )
    dependencies = client.query(
        lambda g: g.V()
        .has_label("service")
        .has("service_name", "checkout")
        .out("calls")
    )
```

The typed client accepts only element-producing traversals and returns
generated semantic entity or edge models. Use `gremlin-python` directly for
intentional scalar, aggregate, map, or path results.

Labels are generated from semantic types: `service.instance` becomes
`service_instance`, `k8s.pod` becomes `k8s_pod`, and relationship `calls`
remains `calls`. Canonical fields remain under `attributes` and `metrics`, while
scalar Gremlin properties use aliases such as `service_name`, `k8s_pod_uid`,
and `service_graph_request_total`.

Gremlin is a trusted internal interface. Evaluation timeout and JVM resources
bound individual requests, but clients can still issue expensive traversals.
Do not expose port `8182` as an unauthenticated public endpoint.

## Replacement rollout

1. Uninstall `servicegraph-access`.
2. Leave Flink, its state, and `graph.elements.events` running.
3. Prepare ArangoDB and writer/reader credentials.
4. Install the indexer. Its new consumer group
   `servicegraph-arangodb-indexer` replays the compacted topic from earliest.
5. Install Gremlin Server and validate traversals.
6. Remove externally managed Elasticsearch only after the new projection has
   caught up and consumers have moved.

No Flink checkpoint, savepoint, fixed job ID, source group, or output topic is
reset by this replacement.
