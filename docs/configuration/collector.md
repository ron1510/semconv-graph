# Collector configuration

The Collector turns traces into two forms of graph evidence. The service-graph connector pairs client and server spans and emits `traces_service_graph_request_total`. The optional root-discovery lane sends only root spans whose kind is neither client nor server through the spanmetrics connector, which emits `semconv.graph.discovery.calls`. Both lanes publish gzip-compressed OTLP Protobuf to `otel.servicegraph.metrics` with retries, queues, required acknowledgements, and automatic topic creation disabled.

## Trace routing

Two stateless routers receive OTLP/gRPC and OTLP/HTTP. Interaction traces are sent to the service-graph backend. In horizontal mode the load-balancing exporter routes by trace ID so both halves of a distributed trace reach the same connector instance. Single-writer mode sends all traces to one backend and avoids duplicate series.

When root-span discovery is enabled, the router keeps roots whose modeled resource and span attributes do not conflict, then routes by generated semantic identity attributes to a separate spanmetrics backend pool. Raw spans never enter Kafka.

## Evidence metrics

The interaction backend runs:

```text
otlp → memory_limiter → batch/traces → service_graph
service_graph → memory_limiter → cumulativetodelta → filter → batch → kafka
```

The filter retains only positive `traces_service_graph_request_total` datapoints. Failure totals are omitted because failures already contribute to the request total and the graph consumes only the existence and freshness of an interaction.

The discovery backend retains only positive, non-overflow `semconv.graph.discovery.calls` delta sums. Generated files `dimensions.yaml` and `root-span-discovery.yaml` keep Collector dimensions aligned with Java extraction and the Python SDK.

Both Kafka exporters use `encoding: otlp_proto`, gzip compression, `required_acks: -1`, bounded batches, unbounded retry duration, and the configured Kafka security contract.

## Main values

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

rootSpanDiscovery:
  enabled: false
```

The charts contain no logs pipeline or entity-event topic. Collector configuration is rendered and validated with `helm lint` and restricted-security template values.
