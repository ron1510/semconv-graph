# Local Kind Environment

The repository's opt-in E2E fixture is the shortest verified local
ArangoDB/Gremlin environment. It creates an isolated Kind cluster, pinned
Redpanda and ArangoDB containers, the indexer, and read-only Gremlin Server:

```powershell
python -m pytest -m e2e --run-e2e --keep-e2e-cluster
```

The focused fixture injects exact schema-2 lifecycle events. It verifies:

- node and edge projection;
- incoming and outgoing Gremlin traversal;
- typed Pydantic reconstruction;
- deterministic replacement and replay;
- Kafka offset commits after successful writes;
- deletion of current-state graph elements;
- indexer and Gremlin restart persistence;
- rejection of graph mutations.

It does not install Collector or Flink. See the [Five-Minute
Quickstart](../getting-started/quickstart.md) for prerequisites and inspection
commands.

Without `--keep-e2e-cluster`, pytest removes the Kind cluster, Docker
containers, temporary images, and kubeconfig automatically. With the flag,
delete the exact resource names printed by the fixture after inspection.

## Complete telemetry path

The Collector, Flink, and synthetic traffic charts exist, but the focused Kind
fixture does not install them. Use [Kubernetes Deployment](../deployment-and-operations.md)
to configure Kafka, storage, images, and credentials, then perform the manual
end-to-end verification checklist in that guide.
