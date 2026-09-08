# Getting Started

The project has two related use cases.

## Use the semantic package

Use the Python package when you need typed OpenTelemetry entities or want to
normalize attributes into stable entity identifiers.

```python
from extended_otel_semconv import entities_from_attributes

entities = entities_from_attributes(
    {
        "service.name": "checkout-api",
        "service.namespace": "shop",
        "service.instance.id": "checkout-api/pod-7f8b",
        "http.request.method": "POST",
        "http.route": "/checkout/{cart_id}",
    }
)

for entity in entities:
    print(entity.entity_type, entity.entity_id)
```

An entity is created only when all of its identifying attributes are present.
Entity IDs are deterministic and URL-encode each identifying part.

Install the package from the repository:

```console
python -m pip install ./packages/extended-opentelemetry-semconv
```

Python 3.12 is required.

## Run the live graph pipeline

Use the complete runtime when you need a continuously maintained topology. It
can receive graph evidence from three independent sources:

1. By default, applications emit normal OpenTelemetry client/server spans;
   Collector backends derive service-graph metrics and publish them to Kafka.
2. Optionally, the Collector router forwards explicitly marked root spans for
   node-only discovery of non-interacting executions.
3. Optionally, the Collector router forwards filtered `entity.state` and
   `entity.delete` OTLP logs to an independent Kafka topic.
4. Flink reconciles all enabled sources into lifecycle-managed graph elements.
5. Consumers apply complete element `upsert` and `delete` commands.

Entity-event ingestion is disabled by default. It accepts only entity and
relationship types represented in the generated `service_graph` topology; a
known upstream entity model outside that topology is rejected because the
ArangoDB projection has no generated collection for it. See [Collector
configuration](../configuration/collector.md), [Flink
configuration](../configuration/flink.md), and the [entity-event compatibility
matrix](../reference/otel-entity-conformance.md) for the exact contract.

The production deployment expects Kubernetes, Helm, Kafka-compatible brokers,
two pre-created topics plus one for each optional input, persistent
storage for Flink, and an existing ArangoDB 3.12 deployment for the
current-state property graph.

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
