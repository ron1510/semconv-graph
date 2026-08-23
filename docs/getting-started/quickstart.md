# Five-Minute Quickstart

The repository's opt-in Kind fixture is the shortest verified local graph path:

```text
seeded schema-2 lifecycle events
  -> Redpanda
  -> servicegraph-indexer
  -> ArangoDB
  -> Gremlin Server
  -> typed Python models
```

This path begins at the public Kafka lifecycle contract. It does **not** run the
Collector or Flink, so it proves projection and graph access rather than trace
extraction or lifecycle processing.

## Prerequisites

- Python 3.12;
- a running Docker daemon;
- Kind;
- `kubectl`;
- Helm 3.

Commands below are PowerShell and run from the repository root.

## Install local dependencies

```powershell
python -m pip install -e ".[dev]"
python -m pip install -e "packages/extended-opentelemetry-semconv[gremlin]"
python -m pip install -e services/servicegraph-indexer
```

## Start the focused environment

```powershell
python -m pytest -m e2e --run-e2e --keep-e2e-cluster
```

The fixture builds the current indexer and Gremlin images, creates an isolated
Kind cluster, starts dedicated Redpanda and ArangoDB containers, installs the
production indexer and Gremlin charts, and injects representative node and edge
events. It verifies projection, typed traversal, replacement, deletion, Kafka
offset commits, read-only access, and restart persistence.

A cold Docker cache can make provisioning take longer than five minutes. At the
end, pytest prints the generated cluster name and kubeconfig path.

## Inspect Kubernetes

Set the printed kubeconfig, then inspect the namespace:

```powershell
$env:KUBECONFIG = '<printed kubeconfig path>'
kubectl get pods --namespace servicegraph-e2e
```

Port-forward the internal Gremlin Service:

```powershell
kubectl port-forward --namespace servicegraph-e2e `
  service/servicegraph-gremlin 8182:8182
```

Then use the [typed client example](../concepts/typed-gremlin-client.md) against
`ws://127.0.0.1:8182/gremlin`.

## Clean up

The `--keep-e2e-cluster` option intentionally preserves the generated Kind
cluster, Redpanda and ArangoDB containers, images, and kubeconfig. Use the exact
cluster and container names printed by the fixture when deleting them:

```powershell
kind delete cluster --name <printed cluster name>
docker rm --force <printed ArangoDB container> <printed Redpanda container>
```

Run without `--keep-e2e-cluster` when automatic cleanup is preferred:

```powershell
python -m pytest -m e2e --run-e2e
```

For a complete deployment using actual trace input, follow [Kubernetes
Deployment](../deployment-and-operations.md) and its manual end-to-end
verification checklist. The current repository does not automate the full
Collector-to-Flink path.
