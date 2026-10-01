# Extended OpenTelemetry Semantic Conventions

Generated Pydantic entity and relationship models built from OpenTelemetry
semantic conventions plus local extensions. Typed Gremlin graph access is
available as an optional dependency.

```console
pip install extended-opentelemetry-semconv==0.6.0
pip install "extended-opentelemetry-semconv[gremlin]==0.6.0"
```

The base installation contains no registry YAML parser, Collector protobuf
parser, graph lifecycle engine, or Gremlin dependency.

Java Flink owns graph construction and identity. The generated Pydantic models
accept the stored `element_id`, validate semantic fields, and expose read-only
`entity_id` or `edge_id` aliases without recreating identity.

The generated public model includes interaction-derived ETL topology:

```python
from extended_otel_semconv import EtlPartRun, EtlPipeline, EtlRun
```
