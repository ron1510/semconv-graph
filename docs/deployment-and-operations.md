# Kubernetes deployment and operations

## Requirements

Provide Kubernetes, Helm 3, Kafka, ArangoDB, an image registry, and Secrets for Kafka and Arango credentials. Production Kafka and Arango operations remain platform responsibilities.

## Topics

Create these topics before enabling producers:

```text
otel.servicegraph.metrics   OTLP Protobuf only
graph.elements.events       schema-3 JSON only, cleanup.policy=compact
```

Choose partition counts for expected parallelism and keep them stable through the clean cutover. Automatic topic creation is disabled.

## Values

Collector and Flink must share the Kafka security block and metrics topic. Flink and the indexer must share the graph-event topic.

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
```

Flink also configures `interactionEvents: graph.elements.events`. The indexer reads that topic with its own consumer group.

## Install order

For a fresh installation or required clean cutover:

1. Deploy the generated Arango schema and indexer, then the read-only Gremlin service.
2. Deploy Flink and wait for its fixed job to reach `RUNNING` and complete checkpoints.
3. Deploy the Collector backends and routers.
4. Resume trace traffic.

This order ensures consumers validate the new contracts before producers publish them.

## Validation

```powershell
helm lint deploy/helm/servicegraph-collector
helm lint deploy/helm/servicegraph-flink
helm template collection deploy/helm/servicegraph-collector --set streamContract.kafka.security.protocol=PLAINTEXT
helm template processing deploy/helm/servicegraph-flink --set streamContract.kafka.security.protocol=PLAINTEXT
python -m tools.semconv_codegen --check
python -m pytest -m "not e2e and not arangodb"
```

Rendered Collector backends must contain `encoding: otlp_proto`, request-total/discovery filters, gzip, retries, and no logs pipeline. Rendered Flink resources must contain no entity-event topic or environment variables.

## End-to-end verification

Confirm that the metrics topic contains decodable `ExportMetricsServiceRequest` records, Flink consumes and checkpoints, the output topic contains only schema-3 events without `metrics`, the indexer group advances after successful writes, and typed Gremlin returns the expected nodes and relationships. Then allow the configured TTL to pass and verify final deletions.

## Uninstall behavior

Retained Flink state and Arango data are not ordinary chart-owned scratch data. Follow the explicit reset procedure before deleting persistent volumes or generated graph documents. Preserve unrelated databases, credentials, Secrets, and Kafka configuration.
