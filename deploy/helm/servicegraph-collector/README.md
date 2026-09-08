# Servicegraph Collector Chart

This chart deploys two stateless OTLP routers and a stateful service-graph
backend. The default `singleWriter` mode sends both routers directly to one
backend, so each service-graph metric series has one writer. The backend exports
OTLP JSON metrics to the configured Kafka topic. An optional router pipeline can
also select marked root spans for node-only discovery.

The backend converts connector-local cumulative counters to deltas, removes
zero datapoints, and keeps only the request and failed-request counters consumed
by Flink. The default 30-second flush and 256-point batch reduce Kafka traffic
without changing the graph lifecycle contract.

```powershell
helm upgrade --install servicegraph deploy/helm/servicegraph-collector `
  --namespace servicegraph-system --create-namespace `
  --values internal-collector-values.yaml
```

Values must provide the internal image, Kafka brokers, topic names, and an
existing Secret for `SASL_PLAINTEXT` or `SASL_SSL`. The chart creates neither topics nor
credentials. Router replicas remain fixed at two.

Enable selective root-span discovery in both the Collector and Flink charts:

```yaml
rootSpanDiscovery:
  enabled: true
streamContract:
  topics:
    rootSpans: otel.root.spans
```

The router retains only root spans whose `semconv.graph.discovery` attribute is
the boolean `true`. It exports them as gzip-compressed OTLP JSON after the span
ends; it does not alter the servicegraph pipeline.

Set `backend.mode=horizontal` with `backend.replicaCount` of at least two only
when one backend cannot hold the trace-pairing workload. Horizontal mode uses
trace-ID affinity across stable StatefulSet ordinals, but each additional
backend can become another writer for the same metric series.

Generate registry-derived dimensions with:

```powershell
python -m tools.semconv_codegen
```
