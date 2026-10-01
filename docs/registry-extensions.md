# Registry Extensions

Registry extensions define what your graph can observe. They are the primary
customization mechanism for the project.

## Layout

Place YAML files anywhere below
`tools/semconv_codegen/model/extensions`. Files are loaded
recursively in sorted order. Organize them by domain:

```text
tools/semconv_codegen/model/extensions/
  app/
    entities.yaml
  business/
    registry.yaml
    entities.yaml
    relationships.yaml
  graph/
    relationships.yaml
```

Each file contains a `groups` list. Recognized group types are
`attribute_group`, `entity`, and `relationship`.

## Define attributes

Use an attribute group only for attributes that are not already defined by the
pinned OpenTelemetry model:

```yaml
groups:
  - id: registry.business
    type: attribute_group
    attributes:
      - id: business.capability.name
        type: string
        stability: development
        brief: The stable name of a business capability.
```

Generated scalar types include `string`, `int`, `double`, `boolean`, and
enum-style mappings with `members`. Other types can exist in the registry but
are not selected as Collector service-graph dimensions.

## Define entities

```yaml
groups:
  - id: entity.business.capability
    type: entity
    name: business.capability
    stability: development
    brief: A capability implemented by one or more services.
    attributes:
      - ref: business.capability.name
        requirement_level: required
        role: identifying
```

Rules:

- `id` and `name` must be unique among extensions;
- the entity name must not redefine an upstream entity;
- every attribute reference must exist upstream or in extensions;
- at least one `role: identifying` reference is required for code generation;
- all identifying attributes are required to instantiate that entity;
- identity attributes may be strings, integers, booleans, or enums without
  floating-point members; floating-point, array, template, and `any` identities
  are rejected so IDs have one cross-language text representation;
- non-identifying attributes become optional generated fields.

Generated class names derive from entity names. For example,
`business.capability` becomes `BusinessCapability`.

Entity IDs contain the type and URL-encoded identifying values in registry
order:

```text
business.capability:order-fulfillment
```

Changing identifying attributes is an identity migration. Existing and new IDs
will coexist until old contributors expire or downstream state is rebuilt.

## Define relationships

```yaml
groups:
  - id: relationship.service_implements_business_capability
    type: relationship
    name: implements
    source_entity: service
    target_entity: business.capability
    source_signals: [service_graph, span_metrics]
    evidence_scope: observation
    stability: development
    brief: A service implements an observed business capability.
```

Rules:

- relationship IDs must be unique;
- source and target entities must exist;
- source signals may be `trace`, `service_graph`, or `span_metrics`;
- relationships consumed from either metric lane declare `evidence_scope`;
- `span_metrics` supports observation relationships only;
- interaction relationships come from `service_graph` and declare `connection_type`;
- both endpoint entities must be observed together;
- same-entity structural expansion is skipped;
- service-to-service dependencies require an explicitly allowed relationship.

The deployed runtime independently selects `service_graph` or `span_metrics`
relationships according to the metric lane. `trace` remains part of the
registry model for library-level graph operations and future raw-trace pipelines.

## Generated artifacts

Run:

```console
python -m tools.semconv_codegen
```

The code-generation module produces:

- domain modules below the package's `generated` directory;
- generated public package exports;
- packaged service-graph relationship metadata;
- packaged upstream lock metadata;
- Collector service-graph dimensions at:

```text
deploy/helm/servicegraph-collector/files/dimensions.yaml
```

It selects attributes from every entity participating in a
corresponding evidence-source relationship. It excludes non-scalar types and template
attributes ending in `.label`, `.annotation`, or `.selector`.

## Cardinality review

Before accepting generated dimensions, estimate:

```text
client combinations x server combinations x routes x connection types
```

Never use unbounded request, trace, session, or user identifiers as entity
attributes carried through service-graph metrics. Kubernetes UIDs and service
instance IDs are high-cardinality by nature; include them only when the graph
needs instance-level identity and the pipeline is sized accordingly.

## Validation

Use check mode before committing:

```console
python -m tools.semconv_codegen --check
python -m pytest -m "not e2e"
```

Validation catches upstream redefinitions, duplicate extensions, unknown
attribute references, unknown relationship endpoints, unsupported source
signals, and stale generated files.

## Deploy a registry change

1. Change the extension source.
2. Regenerate both artifact sets.
3. Review generated entity identity and dimensions.
4. Run tests and type checks.
5. Build a new immutable Flink runtime image.
6. Upgrade the Collector chart from the same commit.
7. Deploy Flink; rebuild disposable alpha state if the generated contract is incompatible.
8. Emit matching telemetry.
9. Verify the new entity and edge through the output topic or Gremlin.

The generated ArangoDB topology and property aliases support new scalar
dimensions after regeneration and deployment without storage-specific code.

## Upgrade the upstream snapshot

The package vendors one exact OpenTelemetry model and records its source in
`tools/semconv_codegen/upstream/otel-semconv.lock.json`.

1. Select an exact semantic-conventions release tag.
2. Extract only its `model` directory into a new versioned directory.
3. Update
   `tools/semconv_codegen/upstream/otel-semconv.lock.json`.
4. Update `UPSTREAM_MODEL` in both generators and related tests.
5. Delete extensions that the new upstream version now owns.
6. Regenerate all artifacts.
7. Review class, identity, dimension, and relationship changes.
8. Run the complete validation set.
9. Remove the old snapshot unless multiple versions are intentional.

An upstream upgrade can change the public Python API and graph identity. Treat
it as a compatibility-sensitive release.
