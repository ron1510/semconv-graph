# Package Architecture

The semantic registry is the common source for the Python SDK and the Java
Flink application's semantic metadata. Code generation is repository tooling
and is not installed in runtime images.

## Semantic SDK

`extended-opentelemetry-semconv` exposes `extended_otel_semconv`. It contains
generated entities and edges, strict stored-element reconstruction, and runtime
relationship metadata. Java Flink creates graph identities; the SDK preserves
their `element_id` without recalculating it. Its base installation depends on Pydantic.
The optional `gremlin` extra validates element-preserving traversals and
reconstructs GraphBinary results as semantic models. The semantic core does
not import Gremlin runtime dependencies.

## Flink application

`services/otel-servicegraph-diff/runtime/java` is one Java 17 Maven application,
one fat JAR, and one deployment unit. `io.extendedotel.flink.ServiceGraphJob`
is the composition root and the only class in the root package.

- `config` validates runtime configuration.
- `operations` owns Helm submission and savepoint commands.
- `ingest` parses OTLP Protobuf and reports rejected evidence.
- `semantic` loads generated identity and relationship metadata.
- `model` contains Flink-independent top-level graph and lifecycle records.
- `lifecycle` owns contributor aggregation, keyed state, expiry, and timers.
- `serialization` owns explicit CBOR serializers and Flink type information.
- `transport` owns schema-3 Kafka event encoding.
- `util` contains the byte-only SHA-256 helper.

These are packages rather than Maven modules because they share one runtime,
release cadence, and dependency graph. The package boundaries make ownership
visible without adding artifact or build boundaries. `Element` is the sealed
node/edge interface; there is no Python Flink package and no individual
generated Java entity class hierarchy.

## Projection and tooling

The Python indexer projects Kafka lifecycle events into ArangoDB. Gremlin
serves read-only traversals and the optional SDK client reconstructs typed
entities. These packages remain independent of the Flink implementation.

`tools.semconv_codegen` owns the pinned upstream registry, extensions,
validation, model rendering, Java semantic metadata, Collector dimensions,
relationship metadata and ArangoDB topology generation. Architectural tests
enforce the Python semantic-core and code-generation dependency boundaries.
