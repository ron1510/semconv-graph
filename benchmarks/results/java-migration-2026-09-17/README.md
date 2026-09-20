# Java Flink migration validation

Measured on 2026-09-17 against the retained Python implementation at repository
baseline `15124a6`. The Python implementation and comparison harnesses were
subsequently removed; this report records the completed migration validation.
The current Java-only measurement command is `python -m benchmarks.flink`.
These are local Docker Desktop/Kind observations, not a
capacity estimate or private-network stability result.

## Behavioral and deployment evidence

- Native Java 17/Flink 2.2.1 image builds and runs as a non-root user. All 27
  Java tests pass in Linux, including real incremental RocksDB snapshots,
  restoration into a fresh database, and expiry with no new input.
- Golden comparisons cover metric and entity-event ingestion, all 40 semantic
  entity models, strict field handling, source reconciliation, contributor
  merge/retraction/expiry, historical edge totals, Unicode/ID escaping,
  nanosecond/uint64 precision, and 3,900 float serialization cases.
- The six E2E scenarios passed across the full suite and a focused rerun:
  schema-2 projection and replay, paired Collector spans through typed Gremlin,
  normalized ETL root discovery/expiry, 256-contributor TaskManager recovery
  and idle expiry, observable malformed-input rejection, and native Kafka
  output matching the Python oracle.
- The full suite's recovery probe initially stalled opening a Gremlin
  WebSocket through a local port-forward. The delete had already been indexed;
  a process stack identified the connection wait. A bounded local probe
  transport and a fresh forward allowed the unchanged recovery assertion to
  pass against the retained cluster in 70 seconds. This changes test tooling,
  not the deployed Gremlin runtime or typed SDK.
- All 292 non-E2E Python tests, strict typing, generated output checks, strict documentation
  build, Helm lint/render checks, and whitespace checks pass. Windows cannot
  load the RocksDB JNI test dependency; the Linux image executes those tests
  rather than skipping them.
- Submission succeeds with the existing fixed job ID. State ownership remains
  granular MapState/ValueState with two coalesced timers and no State TTL.
  Upgrade/reset guards are tested; a real savepoint Helm upgrade and
  JobManager failover have not been exercised in this run.

## Matched distributed measurements

Both fresh stacks use identical payload bytes and timestamps, warmup counts,
cardinalities, parallelism, resources, TTL, checkpoint settings and storage.
The stacks run sequentially. A 60-second pause separates the small and dense
workloads, whose service names are distinct.

Common settings: one JobManager and one TaskManager, parallelism/slots 1,
1200m/1800m configured process memory, Kubernetes memory limits
1536Mi/2304Mi, CPU requests 500m/1 with no CPU limit, RocksDB incremental
checkpoints, 5-second interval and minimum pause, 600-second timeout, one
concurrent checkpoint and one tolerated failure, 45-second contributor TTL,
and a 2Gi Kind `standard` RWO checkpoint PVC. Local RocksDB uses a disk-backed
10Gi emptyDir. These are local test settings, not production PVC measurements.
Kafka uses Redpanda 26.1.6 with one partition/replica per topic, one broker
CPU and 512M configured memory; graph output is compacted.

Small workload: 16 services, four contributors, one warmup and three measured
batches. Dense workload: two services, 256 contributors, three warmup and 20
measured batches (10,240 datapoints). The dense service keys each merge both
client and server observations; each directed calls edge has 256 contributors.

| Workload | Runtime | Datapoints/s | Batch p95 ms | Sampled checkpoint median/max ms | TaskManager CPU cores | TaskManager memory/anonymous MiB |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Small | Python | 24.3 | 2,397 | 136 / 538 | 0.398 | 1,002 / 918 |
| Small | Java | 94.3 | 304 | 142 / 655 | 0.555 | 713 / 645 |
| Dense | Python | 49.5 | 10,829 | 4,779 / 7,671 | 0.935 | 1,152 / 1,069 |
| Dense | Java | 110.6 | 4,561 | 2,561 / 5,424 | 1.011 | 996 / 926 |

Every paired correctness check passes: identical input, workload and deployment
settings, both matching the Python oracle, identical final graph/metrics, and
identical output counts. Neither runtime failed a checkpoint. Dense Java's
observed rate is 2.23 times Python's; it uses slightly more TaskManager CPU.
Checkpoint statistics deduplicate checkpoint IDs and include traffic and idle
samples, not every completed checkpoint. Final dense incremental/full state
sizes are 75,381/929,994 bytes for Python and 63,879/493,347 bytes for Java.
Final input and indexer committed-offset lag is zero for all four runs.

Raw paired reports: [small comparison](small-comparison.json),
[dense comparison](dense-comparison.json), [small Python](small-python.json),
[small Java](small-java.json), [dense Python](dense-python.json),
[dense Java](dense-java.json). The archived configuration records the original shared timestamp and names.
The paired harness used for these measurements was removed with the Python
implementation after comparison; current Java-only measurements use
`python -m benchmarks.flink --output .tmp/flink-benchmark`.

Raw reports include complete final graph hashes and event counts checked
against the unchanged Python oracle, observed batch throughput/latency, cgroup
CPU and memory, checkpoints during traffic and afterward, and committed-offset
lag. Batch completion includes producer acknowledgement and consumer polling.
It is not per-event latency or saturating capacity. Memory includes all
container processes and file cache; anonymous resident memory is an RSS proxy,
not JVM heap. Lag reflects checkpoint-committed offsets.

## Pure-function measurements

| Workload | Stage | Python operations/s | Java operations/s |
| --- | --- | ---: | ---: |
| 100 entities x 3 contributors | Ingest datapoints | 1,054 | 6,838 |
| 100 entities x 3 contributors | Lifecycle contributions | 14,965 | 32,183 |
| 1 entity x 256 contributors | Ingest datapoints | 1,521 | 17,480 |
| 1 entity x 256 contributors | Lifecycle contributions | 5,023 | 13,366 |

Both runs use ten warmup and 20 measured iterations and match complete final
contributor state, events, counts and payload hashes. The dense pure workload
is node-only discovery, unlike the distributed calls workload. These Windows
host measurements ran alongside image-build preparation, use a fixed 512MiB
Java heap and Python's host allocator, and do not measure Flink/checkpoints,
cluster CPU/memory, lag or pipeline latency. Do not extrapolate their ratios
to deployed capacity. Raw data: [small](inprocess-small.json),
[dense](inprocess-dense.json).

## Remaining operational proof

After the user requested Java-only cleanup, the Python Flink source/package,
baseline Dockerfile, runtime selector, row serializers, comparison harnesses
and fixture generators were removed. CI/release packaging, local tooling and
docs now target only the Java application. Frozen parity fixtures and the raw
measurements remain. A fresh Java-only image build passes all 27 Java tests;
all 159 remaining Python tests and static/codegen/Helm/docs checks pass. The new
Java workload expectation matches both archived final graph hashes exactly.
The complete E2E was not repeated for this cleanup; the earlier E2E evidence
above applies to the migration. No private rollout or state reset occurred.

The previous private-network failure followed hours of slow lifecycle work
and repeated failed checkpoints. Java retains the contributor scan algorithm;
removing the Python boundary does not remove its cardinality-dependent cost.
Run an isolated private-network canary for at least six hours and longer than
the previous failure window, including dense shared-key contributors. Track
busy time, alignment/end-to-end checkpoint duration, failure reasons, storage
latency, memory and lag without increasing checkpoint tolerance or timeout.
Verify expiry after recovery with no new input. No private deployment, state
reset, topic deletion, projection truncation, commit or push was performed.

Python state is incompatible with native Java state. Use the explicit reset
procedure in `docs/operations/upgrades.md`; preserve the old claim, projection
and offsets for rollback. Historical Python source is available in Git at `15124a6`. The current tree
and Helm chart support only Java.
