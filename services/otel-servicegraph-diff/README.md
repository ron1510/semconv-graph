# OTel Servicegraph Diff

A native Java Flink 2.2.1 application that converts OpenTelemetry servicegraph
and spanmetrics discovery datapoints into lifecycle-managed semantic graph
events. It consumes OTLP Protobuf evidence, merges and expires contributors,
and publishes schema-3 element-keyed JSON events.

Build and test with:

```console
mvn -f services/otel-servicegraph-diff/runtime/java/pom.xml verify
```

The default Dockerfile builds Java 17 and submits
`io.extendedotel.flink.ServiceGraphJob`. Java is the only runtime. The previous
Python implementation is available in Git history at `15124a6`. The semantic
registry remains the common source for Java metadata and the Python SDK.

`GraphModel` defines closed node/edge, immutable contribution, and upsert/delete
types. `LifecyclePolicy` owns the per-type contributor lifetime, defaulting
to one day. `AttributeWinners` tracks aggregate attribute ownership;
`ElementLifecycleFunction` owns keyed state and timers. Normal refreshes use
point reads, while winner removal and expiry can scan remaining contributors.
Directly framed CBOR v2 carries internal state and transport; the public Kafka contract and
canonical IDs/hashes remain JSON.

Maven checks Google Java formatting during validation. Run `mvn spotless:apply`
from the Java module to format changes. The retained-state developer benchmark
is built with `benchmarks/docker/flink-state-cost.Dockerfile`.

Older Python, JSON, and CBOR-v1 checkpoints are incompatible. This rollout
requires the clean reset in `docs/operations/upgrades.md`. Later compatible
CBOR-v2 upgrades can keep the fixed job ID and canonical savepoints.
