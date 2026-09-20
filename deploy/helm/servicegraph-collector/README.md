# Servicegraph Collector Chart

This chart deploys two stateless OTLP routers and a stateful service-graph
backend. The default `singleWriter` mode sends both routers directly to one
backend, so each service-graph metric series has one writer. The backend exports
OTLP Protobuf metrics to the configured Kafka topic. An optional router pipeline
also sends non-client/non-server root spans through spanmetrics for node-only
discovery.

The backend converts connector-local cumulative counters to deltas, removes
zero datapoints, and keeps only the request-total evidence consumed by Flink.
The default 30-second flush and 256-point batch reduce Kafka traffic
without changing the graph lifecycle contract.

```powershell
helm upgrade --install servicegraph deploy/helm/servicegraph-collector `
  --namespace servicegraph-system --create-namespace `
  --values internal-collector-values.yaml
```

Values must provide the internal image, Kafka brokers, topic names, and an
existing Secret for `SASL_PLAINTEXT` or `SASL_SSL`. The chart creates neither topics nor
credentials. Router replicas remain fixed at two.

Enable spanmetrics root discovery in the Collector chart:

```yaml
rootSpanDiscovery:
  enabled: true
  backend:
    replicaCount: 2
    metricsFlushInterval: 60s
```

The router keeps roots whose kind is neither client nor server, routes each
semantic identity to one of the dedicated spanmetrics backends, and publishes positive delta
`semconv.graph.discovery.calls` metrics to the existing metrics topic. Raw spans
do not enter Kafka, and the servicegraph pipeline remains unchanged.

Set `backend.mode=horizontal` with `backend.replicaCount` of at least two only
when one backend cannot hold the trace-pairing workload. Horizontal mode uses
trace-ID affinity across stable StatefulSet ordinals, but each additional
backend can become another writer for the same metric series.

Generate registry-derived dimensions with:

```powershell
python -m tools.semconv_codegen
```
