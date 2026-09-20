# Package Architecture

The semantic registry is the common source for the Python SDK and the Java
Flink application's semantic metadata. Code generation is repository tooling
and is not installed in runtime images.

## Semantic SDK

`extended-opentelemetry-semconv` exposes `extended_otel_semconv`. It contains
generated entities and edges, deterministic identities, strict reconstruction,
and runtime relationship metadata. Its base installation depends on Pydantic.
The optional `gremlin` extra validates element-preserving traversals and
reconstructs GraphBinary results as semantic models. The semantic core does
not import Gremlin runtime dependencies.

## Flink application

`services/otel-servicegraph-diff/runtime/java` is a Java 17 Maven application.
`ServiceGraphJob` wires native Kafka sources and sink. `MetricParser` and
`EntityEventIngest` parse OTLP and produce contributions. `SemanticRegistry`
loads generated fields, identity rules and relationship metadata.

`GraphLifecycle` owns pure contributor aggregation, attribute merging, and expiry.
`ElementLifecycleFunction` owns Flink keyed state and coalesced timers.
`GraphModel.Element` represents semantic nodes and edges; there is no Python
Flink package and no individual generated Java entity class hierarchy.

## Projection and tooling

The Python indexer projects Kafka lifecycle events into ArangoDB. Gremlin
serves read-only traversals and the optional SDK client reconstructs typed
entities. These packages remain independent of the Flink implementation.

`tools.semconv_codegen` owns the pinned upstream registry, extensions,
validation, model rendering, Java semantic metadata, Collector dimensions,
relationship metadata and ArangoDB topology generation. Architectural tests
enforce the Python semantic-core and code-generation dependency boundaries.
