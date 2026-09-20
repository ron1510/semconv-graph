# Benchmarking

The native Java Flink workload harness runs a disposable Kind stack and sends
deterministic servicegraph OTLP Protobuf through Kafka:

```console
python -m benchmarks.flink --warmup 3 --iterations 20 --entities 2 --contributors 256 --output .tmp/flink-benchmark
```

The report checks complete service/calls payloads and request totals, then
records observed rate, batch latency, CPU, memory, checkpoint duration/size and
committed-offset lag. The harness uses Python for provisioning and measurement;
the Flink job itself is Java only.

These controlled Kafka-to-Kafka measurements do not establish saturating
capacity, Collector-to-Gremlin latency, horizontal scaling or infrastructure
cost. See the [benchmark guide](https://github.com/ron1510/semconv-graph/blob/main/benchmarks/README.md)
for boundaries and the archived matched Python/Java migration report. The old
Python in-process benchmark is available in Git history at `15124a6`.
