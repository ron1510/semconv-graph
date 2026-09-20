# Java Flink refinement measurements

Recorded on 2026-09-17, with Java 17.0.17 in Linux Docker, a warm JVM and a fixed
1 GiB maximum heap. Raw baseline and refined reports are adjacent JSON files.
These are developer cost measurements, not cluster capacity or a physical
24-hour soak. Concurrent local activity and JIT/order effects affect timings;
the decreasing indexed timings at larger cardinalities are not a scaling claim.

## Results

All cardinalities preserve the baseline aggregate payload hash
`956b2daa359b684e7330bd671a1fe3fa98e2f328cdee635fa47f3527d11666a5`.

| Retained contributors | Original pure refresh (µs) | Refined full merge (µs) | Indexed refresh (µs) | Winner point reads per indexed refresh |
| --- | ---: | ---: | ---: | ---: |
| 256 | 648.6 | 830.1 | 24.0 | 1 |
| 4,096 | 1,998.7 | 2,251.9 | 10.9 | 1 |
| 16,384 | 4,273.6 | 6,730.6 | 4.7 | 1 |

The important change is the ordinary refresh path's bounded contributor reads.
The operator test independently verifies zero retained-state iterations and a
single snapshot write for a refresh among 2,000 contributors. Full merge remains
available for winner removal and expiry; it is not the normal refresh path.

In the refined run, representative contributor encoding/decoding averaged
51.7 µs with JSON and 29.8 µs with typed CBOR. The JSON value is 1,628 UTF-8 bytes;
the framed CBOR value is 1,535 bytes, a 5.7% reduction. This deliberately uses
long string descriptions, so binary encoding cannot remove most payload bytes.
The original JSON run averaged 52.4 µs. Internal copies now return immutable
domain values directly instead of performing a JSON round trip.

Public Kafka commands remain schema-2 JSON, and canonical JSON still determines
IDs and payload hashes. No Kafka protobuf contract migration was required.

## Reproduce

From the repository root:

```console
docker build --file benchmarks/docker/flink-state-cost.Dockerfile --output type=local,dest=.tmp/flink-state-cost .
```

The build checks formatting and runs the Java tests before writing
`state-cost.json`. The benchmark's pure/indexed merge timings exclude RocksDB
latency, state writes, timer scans and checkpoint work. Estimated value bytes
exclude RocksDB keys, aggregate indexes, SST compression and database overhead.

## Retention and recovery proof

Linux incremental RocksDB tests retain 2,048 contributors across a 24-hour
logical-clock boundary, refresh them, checkpoint, restore, and expire without
any new input. Separate tests restore legacy JSON state both idle and through a
second checkpoint containing mixed JSON/CBOR contributors. Randomized tests
compare indexed merging and the actual keyed operator against a full reference
merge, including partial attributes, ties, retractions and old observations.

Hot element keys still execute serially in Flink. Additional slot CPU is not
automatic parallel execution of one key. These measurements address avoidable
per-update work; a production canary must establish sustained checkpoint and
storage behavior on the target infrastructure.

## Completed validation

- 34 Java tests pass in the Linux runtime image build, including the state and
  recovery cases above. Maven's Google Java format check passes.
- 159 remaining Python tests pass; codegen, Ruff, Pyright, Helm lint/restricted
  renders, strict docs and whitespace checks pass.
- All six E2E scenarios pass in one full run (535.58 seconds): schema-2
  projection/replay/restarts, Collector servicegraph traffic, ETL root discovery,
  checkpointed contributor state with TaskManager replacement and idle expiry,
  observable malformed-input rejection, and controlled native workload parity.
  The run's owned Kind cluster and containers were cleaned up.

The adjacent `java-e2e-benchmark.json` records that final controlled workload:
192 measured datapoints, 192 expected/measured events, 32 final elements with
matching expected hashes, and zero final committed input/indexer lag. Its five
checkpoint samples take 70–156 ms and report zero failed checkpoints. This small
batched workload observed 95.3 datapoints/second and is not a saturation test or
a matched dense before/after comparison.

No private deployment, physical 24-hour soak, actual Helm savepoint upgrade, or
JobManager failover was performed. Nothing was committed or pushed.
