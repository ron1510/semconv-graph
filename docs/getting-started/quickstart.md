# Local Demo

The persistent local demo is the shortest supported path to a queryable graph:

```text
seeded schema-2 lifecycle events
  -> Redpanda
  -> servicegraph-indexer
  -> ArangoDB
  -> Gremlin Server
  -> typed Python models
```

This focused path begins at the public Kafka lifecycle contract. It does **not**
run Collector or Flink, so it demonstrates projection and typed graph access,
not trace extraction or distributed lifecycle processing.

## Prerequisites

- CPython 3.12;
- a running Docker daemon;
- Kind;
- `kubectl`;
- Helm 3.

Commands below are PowerShell and run from the repository root.

## Install local dependencies

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e ".[dev]"
.\.venv\Scripts\python.exe -m pip install -e "packages/extended-opentelemetry-semconv[gremlin]"
.\.venv\Scripts\python.exe -m pip install -e services/servicegraph-indexer
```

## Start and inspect the graph

```powershell
.\.venv\Scripts\python.exe -m tools.local_demo up
.\.venv\Scripts\python.exe -m tools.local_demo status
.\.venv\Scripts\python.exe -m tools.local_demo query
```

`up` builds the current indexer and Gremlin images, creates the dedicated
`servicegraph-local-demo` Kind cluster, starts isolated Redpanda and ArangoDB
containers, installs the production charts, and seeds six services with five
dependency edges. It leaves the environment running so `query` can reconstruct
the vertices and edges as generated Pydantic models.

A cold Docker cache can make the first run take several minutes. Managed state
and the kubeconfig are kept under `.tmp/local-demo`.

## Inspect Kubernetes

```powershell
$env:KUBECONFIG = (Resolve-Path .tmp/local-demo/kubeconfig)
kubectl get pods --namespace servicegraph-local-demo
```

The normal `query` command owns its temporary Gremlin port-forward. For custom
queries, port-forward the internal Service and follow the
[typed client guide](../concepts/typed-gremlin-client.md):

```powershell
kubectl port-forward --namespace servicegraph-local-demo `
  service/servicegraph-gremlin 8182:8182
```

## Stop the environment

```powershell
.\.venv\Scripts\python.exe -m tools.local_demo down
```

Cleanup targets only the demo's fixed Kind cluster and exact Docker resource
names. It leaves unrelated containers and clusters untouched.

For automated lifecycle and restart assertions, run the opt-in
[focused E2E](../development/testing.md). For a complete deployment using real
trace input, follow [Kubernetes deployment](../deployment-and-operations.md).
The repository does not yet automate the complete Collector-to-Flink path.
