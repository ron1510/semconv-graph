# Typed Gremlin Client

Install the semantic SDK with typed GraphBinary support:

```console
pip install "extended-opentelemetry-semconv[gremlin]==0.6.0"
```

`SemanticGremlinClient` executes traversals whose final traversers are vertices
or edges. It hydrates complete ArangoDB properties internally and returns the
generated Pydantic model for each semantic type:

```python
from extended_otel_semconv import Service, ServiceCallsServiceEdge
from extended_otel_semconv.gremlin import SemanticGremlinClient

with SemanticGremlinClient("ws://servicegraph-gremlin:8182/gremlin") as client:
    dependencies = client.query(
        lambda g: g.V()
        .has_label("service")
        .has("service_name", "checkout")
        .out("calls")
    )
    calls = client.query(lambda g: g.E().has_label("calls"))

assert all(isinstance(entity, Service) for entity in dependencies)
assert all(isinstance(edge, ServiceCallsServiceEdge) for edge in calls)
```

Entity and edge models contain the stored `element_id` plus canonical semantic
fields. Their read-only `entity_id` and `edge_id` aliases return that same
value. The client does not calculate graph identities, perform follow-up
endpoint queries, or return Kafka or ArangoDB projection metadata.

## Typed traversal boundary

Navigation, filtering, ordering, deduplication, and range operations preserve
elements and are supported. The client appends its own `elementMap()` step to
hydrate the final elements.

Operations such as `values()`, `valueMap()`, `count()`, `project()`, `path()`,
`group()`, and `select()` change the result into an untyped value. The client
rejects these before network submission with
`UnsupportedSemanticTraversalError`:

```python
from extended_otel_semconv.gremlin import UnsupportedSemanticTraversalError

with SemanticGremlinClient("ws://servicegraph-gremlin:8182/gremlin") as client:
    try:
        client.query(lambda g: g.V().values("service_name"))
    except UnsupportedSemanticTraversalError as error:
        print(error)
```

Use `gremlin-python` directly when scalar, aggregate, map, path, or custom
provider results are intentional. The typed client has no raw fallback because
every successful call guarantees semantic Pydantic models.

Java is the identity authority. Reconstruction uses the stored semantic type
and endpoint types to select a generated model, then validates its fields.
Unknown semantic types, invalid relationship endpoints, incomplete identifying
attributes, and malformed element maps fail explicitly.
