# Protobuf evidence-only measurements — 2026-09-19

These reports validate the clean evidence-only contract on the local disposable Kind environment. They are smoke measurements, not capacity claims.

## Wire size

The deterministic 16-entity, four-contributor workload contains 64 positive request datapoints. Its OTLP Protobuf record is **10,963 bytes**. Rendering the same protobuf message as compact OTLP JSON is **25,559 bytes**, so Protobuf is **42.9%** of that JSON representation (57.1% smaller) before Kafka gzip compression.

## In-process Java parser and lifecycle

`java-inprocess-benchmark.json` records 100 measured batches after 20 warmups on Java 17:

- parser/extraction throughput: **18,064 datapoints/s**;
- parser batch latency: **3.39 ms p50**, **5.13 ms p95**, **6.90 ms p99**;
- lifecycle contribution throughput: **174,160 contributions/s**;
- lifecycle batch latency: **0.74 ms p50**, **2.55 ms p95**, **4.86 ms p99**;
- active graph: 32 elements and 192 contributor snapshots;
- public events: 144 during the first effective batch and none for later unchanged evidence.

The parser measurement includes Protobuf decoding, semantic extraction, IDs, and contribution creation. It excludes Kafka, Flink network/runtime overhead, and RocksDB.

## Disposable E2E

`java-e2e-benchmark.json` records Collector-adjacent Kafka input through Java Flink and schema-3 Kafka output in the same environment used by the complete E2E suite:

- payload size: **10,963 bytes**;
- measured evidence rate: **120 datapoints/s** across three deliberately small batches;
- completion latency: **117.5 ms mean**, **120.9 ms p95**;
- warmup public events: 144;
- measured repeated-evidence events: **0**, matching the expected suppression;
- final graph: 32 elements with the expected hash;
- failed checkpoints: 0;
- sampled checkpoint duration: 56–154 ms;
- sampled incremental checkpoint size: 51,364–99,910 bytes;
- sampled full state size: 116,023–191,263 bytes;
- final source and indexer committed lag: 0.

The surrounding E2E also passed schema-3 replay/deletion, paired Collector interaction evidence, root discovery, malformed evidence rejection, TaskManager restart, checkpoint recovery, and idle final expiry.

The run is short, local, and intentionally unsaturated. It does not establish private-network reliability, six-hour stability, maximum throughput, or production sizing.
