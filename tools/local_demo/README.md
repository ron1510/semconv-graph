# Local demo

The local demo reuses the repository's focused Kind E2E environment and production Helm charts. It builds the
indexer and Gremlin images, starts dedicated Redpanda and ArangoDB containers, installs the charts, and seeds a small
service dependency graph.

```powershell
python -m tools.local_demo up
python -m tools.local_demo status
python -m tools.local_demo query
python -m tools.local_demo down
```

`up` leaves the environment running. State and kubeconfig files live under `.tmp/local-demo`, which is gitignored.
The fixed cluster, containers, and images are reserved for this tool; `down` removes only those exact resources.

Prerequisites are Python 3.12 with the repository development dependencies, Docker, Kind, kubectl, and Helm.

This focused demo begins at the public schema-2 Kafka lifecycle contract. It does not run Collector or Flink and
therefore proves projection into ArangoDB and typed Gremlin access, not telemetry extraction or Flink processing.
