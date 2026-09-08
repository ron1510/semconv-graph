# Semantic Entity Model

The semantic model turns flat OpenTelemetry attributes into entities and typed
relationships.

## Registry layers

The effective registry is a merge of:

1. the pinned OpenTelemetry semantic-conventions model under
   `tools/semconv_codegen/upstream/otel-semconv/v1.43.0/model`;
2. project extensions under
   `tools/semconv_codegen/model/extensions`.

Extensions may reference upstream attributes and entities, but may not redefine
them. The upstream snapshot is build input and is never downloaded at runtime.

## Entities

An entity definition has:

- a stable entity name such as `service`, `k8s.pod`, or `app.endpoint`;
- one or more attribute references;
- at least one identifying attribute if a runtime class should be generated;
- optional descriptive metadata such as stability and a brief.

```yaml
- id: entity.app.endpoint
  type: entity
  name: app.endpoint
  stability: development
  attributes:
    - ref: service.name
      role: identifying
    - ref: service.namespace
      role: identifying
    - ref: http.request.method
      role: identifying
    - ref: http.route
      role: identifying
```

The generated class is immutable, validates exact registry types with Pydantic, and
constructs a deterministic ID from identifying values:

```text
app.endpoint:checkout-api:shop:POST:%2Fcheckout%2F%7Bcart_id%7D
```

Missing any identifying key means that entity is not observed. Present but
empty, incorrectly typed, or invalid enum values are rejected. Optional
attributes enrich an entity but do not change its identity. Array and template
attributes retain their registry types; template fields such as
`k8s.pod.label.<key>` are exposed as canonical dotted attributes.

### ETL hierarchy

The local ETL extension models observed execution topology with hierarchical
identity:

| Entity | Identifying fields | Descriptive fields |
| --- | --- | --- |
| `etl.pipeline` | `etl.pipeline.id` | `etl.pipeline.name` |
| `etl.run` | `etl.pipeline.id`, `etl.run.id` | `etl.run.name` |
| `etl.part.run` | `etl.pipeline.id`, `etl.run.id`, `etl.part.run.id` | `etl.part.name` |

The registry defines `Pipeline -> contains -> Run -> contains -> Part Run`.
Retries preserve the same logical Part Run identity. A later Pipeline Run uses
a new Run ID and therefore creates a new Run and Part Run.

Extraction is additive: Pipeline identity alone creates only a Pipeline;
adding Run identity creates the Run and first relationship; complete Part Run
identity creates the full hierarchy. ETL executions without a matched
interaction remain undiscovered.

## Relationships

A relationship names a directed edge between entity types:

```yaml
- id: relationship.service_exposes_app_endpoint
  type: relationship
  name: exposes
  source_entity: service
  target_entity: app.endpoint
  source_signals: [service_graph]
```

When both entity types are observed together, the runtime emits the configured
edge. Service-to-service dependencies are handled specially because their edge
type is derived from the Collector's `connection_type`:

| Connection type | Edge |
| --- | --- |
| unset or other | `calls` |
| `messaging_system` | `publishes_to` |
| `database` | `queries` |

Only a relationship explicitly allowed by the registry is emitted.

Code generation creates a frozen concrete Pydantic edge class for every
relationship definition. For example, `relationship.service_calls_service`
becomes `ServiceCallsServiceEdge`. Each class declares its relationship and
endpoint semantic types, validates endpoint IDs, and computes the same
deterministic edge ID used by Flink. Edge metrics and structural attributes are
preserved without embedding endpoint entities.

## From attributes to the live graph

The generation pipeline connects the model to runtime behavior:

1. `python -m tools.semconv_codegen` validates and merges both registry layers.
2. It builds a strict semantic intermediate representation and commits
   `semantic-entities.schema.json`.
3. Pinned `datamodel-code-generator` turns that JSON Schema into static Pydantic
   field classes, while the project generator adds semantic IDs, registries,
   and concrete edge models.
4. The generated Python remains static, importable, and IDE-friendly; no model
   generation or registry parsing occurs at runtime.
5. The same operation selects scalar attributes used by service-graph relationships.
6. Collector backends include those dimensions in their metrics.
7. Flink interprets client and server dimensions through the generated package.
8. Interaction events carry normalized graph nodes and edges.
9. Generic consumers can display custom types without embedding registry logic.

Template attributes ending in `.label`, `.annotation`, or `.selector` and
non-scalar attributes are excluded from Collector dimensions.

## Cardinality

Entity identity often requires high-cardinality values such as pod UIDs,
routes, or instance IDs. Including them as metric dimensions increases
service-graph time-series cardinality.

Treat every identifying attribute as a storage and throughput decision:

- use stable bounded names where possible;
- do not use request IDs, trace IDs, user IDs, or session IDs;
- estimate combinations across client, server, route, namespace, and workload;
- monitor Collector memory and Kafka throughput after adding dimensions.

The generator deliberately follows registry policy. It does not silently
remove a dimension because its cardinality appears risky.
