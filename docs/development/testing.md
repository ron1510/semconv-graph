# Testing

The repository has fast unit/component tests and an opt-in Kubernetes lifecycle
test. Use CPython 3.12.

## Fast suite

```console
python -m pytest -m "not e2e"
mvn -f services/otel-servicegraph-diff/runtime/java/pom.xml verify
python -m tools.semconv_codegen --check
python -m ruff check .
python -m pyright
```

The suite covers registry validation, deterministic generation, importable
models, graph lifecycle behavior, Flink state/timers, demo traffic, ArangoDB
topology initialization, document routing, edge-delete fanout, Kafka security,
commit-after-write behavior, generated edge models, and typed Gremlin traversal
validation. Native Java parity and operator tests run through Maven. The
committed golden fixtures preserve the original Python behavioral expectations
without requiring a Python Flink implementation.

Optional branch coverage has no numeric gate:

```console
python -m pytest -m "not e2e" --cov --cov-branch --cov-report=term-missing
```

## ArangoDB/Gremlin lifecycle test

The opt-in test needs Docker, Kind, `kubectl`, and Helm. It creates a uniquely
named Kind cluster, starts pinned Redpanda and ArangoDB containers on Kind's
Docker network, builds the production indexer and validated Gremlin images, and
installs both charts.

It publishes exact Flink schema-3 lifecycle envelopes and verifies:

- node and edge projection;
- incoming and outgoing GraphBinary traversals;
- typed Pydantic vertex and edge reconstruction;
- rejection of transformed Gremlin results;
- complete replacement and duplicate replay;
- Kafka offset commits after database writes;
- read-only mutation rejection;
- graph persistence across indexer and Gremlin pod replacement;
- node and edge deletion.

Flink and the Collectors are deliberately outside this focused deployment test.
Their behavior is covered by the Flink and semantic package tests; this test
starts at Flink's public Kafka contract so access-layer iteration stays fast.

```console
python -m pytest -m e2e --run-e2e
```

Cleanup removes the cluster, containers, kubeconfig, and temporary images.
Preserve them for debugging with:

```console
python -m pytest -m e2e --run-e2e --keep-e2e-cluster
```

Image builds honor `PIP_INDEX_URL`, `PIP_TRUSTED_HOST`,
`TINKERPOP_SERVER_URL`, and `MAVEN_REPOSITORY_URL`.
