# Getting Started

The project has two related use cases.

## Use the semantic package

Use the Python package to receive typed entities and relationships from the
read-only Gremlin endpoint. Java Flink creates every graph element and ID; the
client selects a generated Pydantic model and preserves the stored `element_id`.

```python
from extended_otel_semconv.gremlin import SemanticGremlinClient

with SemanticGremlinClient("ws://servicegraph-gremlin:8182/gremlin") as client:
    services = client.query(lambda g: g.V().has_label("service"))

for service in services:
    print(service.element_id, service.semantic_attributes())
```

The client validates returned semantic fields but does not derive entities or
recalculate their identities.

Install the package from the repository:

```console
python -m pip install ./packages/extended-opentelemetry-semconv
```

Python 3.12 is required.

## Run the live graph pipeline

Use the complete runtime when you need a continuously maintained topology. It
receives two complementary forms of graph evidence:

1. By default, applications emit normal OpenTelemetry client/server spans;
   Collector backends derive service-graph metrics and publish them to Kafka.
2. Optionally, the Collector sends root spans whose kind is neither client nor
   server through spanmetrics and publishes observation evidence for
   non-interacting executions, including transactions and observation edges.
3. Flink reconciles both evidence lanes into lifecycle-managed graph elements.
4. Consumers apply complete element `upsert` and `delete` commands.

See [Collector configuration](../configuration/collector.md) and [Flink
configuration](../configuration/flink.md).

The production deployment expects Kubernetes, Helm, Kafka-compatible brokers,
two pre-created topics, persistent storage for Flink, and an existing ArangoDB
3.12 deployment for the current-state property graph.

For an isolated demonstration, follow the [local Kind
quickstart](quickstart.md). For an existing cluster, follow the [Kubernetes
deployment guide](../deployment-and-operations.md).

## Understand the boundaries

The project intentionally does not:

- install or administer Kafka in production;
- create Kafka topics;
- modify your application instrumentation;
- infer staleness outside Flink;
- expose a custom HTTP query API;
- require a Flink Kubernetes Operator;
- require any Kubernetes CRD.

The supplied Redpanda instructions are only for local testing.
