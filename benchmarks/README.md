# Native Java Flink measurements

The current evidence-only measurements are recorded in the
[2026-09-19 Protobuf report](results/protobuf-evidence-2026-09-19/README.md).

Run a controlled Kafka-to-Kafka workload in a disposable Kind environment:

```console
python -m benchmarks.flink --output .tmp/flink-benchmark
python -m benchmarks.flink --entities 2 --contributors 256 --warmup 3 --iterations 20 --output .tmp/flink-benchmark-dense
```

The Python harness generates OTLP input, provisions the Java job, checks Kafka
keys and complete final service/calls payloads, and records observed datapoint
rate, OTLP Protobuf payload size, emitted-event count, batch latency, cgroup
CPU/memory, checkpoint duration/size, and committed offset lag. It calculates
the expected service ring directly from the known workload; it does not
implement a lifecycle engine. At least one warmup batch is required. Repeated
unchanged evidence after warmup should refresh state without new public events.

This is a controlled batched workload, not saturating capacity or per-event
latency. It excludes Collector and Gremlin latency. Memory includes file cache
and RocksDB; anonymous resident memory is an RSS proxy, not JVM heap. Source
lag reflects checkpoint-committed offsets. A short local run does not prove
private-network stability. The Java golden/operator tests and complete E2E
suite cover the broader semantic and recovery contracts separately.

## Archived migration comparison

The [2026-09-17 report](results/java-migration-2026-09-17/README.md) retains the
matched Python/Java measurements and raw reports. The Python Flink job,
comparison harnesses and Python fixture generators were removed after the
comparison completed. Historical Python source is available at Git revision
`15124a6`. Recorded measurements are historical evidence, not current capacity.
