# Auto-Instrumented Endpoint

This example proves that ordinary FastAPI and HTTPX auto-instrumentation can
produce an `AppEndpoint`. The application files contain no OpenTelemetry
imports, manual spans, or manually assigned span attributes.

The flow is:

```text
HTTPX CLIENT span
  -> FastAPI SERVER span
  -> trace-affine Collector service-graph connector
  -> service-graph delta metric
  -> Flink graph contributions
  -> app.endpoint upsert
  -> ArangoDB
```

The example sends three identical requests so the asynchronous Collector flush
and cumulative-to-delta stages are visible immediately. Every request uses the
same normal HTTPX call.

## Prepare the application environment

Run these PowerShell commands from the repository root:

```powershell
py -3.12 -m venv .tmp/auto-instrumentation-venv
& .tmp/auto-instrumentation-venv/Scripts/python.exe -m pip install `
  --requirement examples/auto-instrumentation/requirements.txt
& .tmp/auto-instrumentation-venv/Scripts/opentelemetry-bootstrap.exe -a install
```

The bootstrap command discovers FastAPI and HTTPX and installs their matching
OpenTelemetry instrumentors. The OTLP exporter is explicitly installed because
instrumentation discovery does not choose an exporter.

## Forward the Collector router

The complete Collector, Kafka, Flink, ArangoDB, indexer, and Gremlin deployment
must already be running. Find the router Service and forward OTLP gRPC:

```powershell
kubectl get services --all-namespaces | Select-String "servicegraph-collector.*router"
kubectl port-forward --namespace <namespace> `
  service/<collector-router-service> 4317:4317
```

## Start the server

Open another terminal in the repository root:

```powershell
$env:OTEL_SERVICE_NAME = "checkout-api"
$env:OTEL_RESOURCE_ATTRIBUTES = "service.namespace=shop"
$env:OTEL_EXPORTER_OTLP_ENDPOINT = "http://127.0.0.1:4317"
$env:OTEL_EXPORTER_OTLP_PROTOCOL = "grpc"
$env:OTEL_TRACES_EXPORTER = "otlp_proto_grpc"
$env:OTEL_METRICS_EXPORTER = "none"
$env:OTEL_LOGS_EXPORTER = "none"
$env:OTEL_SEMCONV_STABILITY_OPT_IN = "http"
$env:OTEL_PYTHON_DISABLED_INSTRUMENTATIONS = "click,logging,threading"

& .tmp/auto-instrumentation-venv/Scripts/opentelemetry-instrument.exe `
  .tmp/auto-instrumentation-venv/Scripts/python.exe -m uvicorn `
  --app-dir examples/auto-instrumentation server:app `
  --host 127.0.0.1 --port 18081
```

`click` is disabled because instrumenting Uvicorn's process-wide Click command
can leave one active span around the server. FastAPI then observes that span and
may create `INTERNAL` request spans instead of extracting the incoming HTTP
context. Disabling it does not disable FastAPI or HTTPX instrumentation.

## Send requests

Open one more terminal and set the client resource and exporter configuration:

```powershell
$env:OTEL_SERVICE_NAME = "frontend"
$env:OTEL_RESOURCE_ATTRIBUTES = "service.namespace=shop"
$env:OTEL_EXPORTER_OTLP_ENDPOINT = "http://127.0.0.1:4317"
$env:OTEL_EXPORTER_OTLP_PROTOCOL = "grpc"
$env:OTEL_TRACES_EXPORTER = "otlp_proto_grpc"
$env:OTEL_METRICS_EXPORTER = "none"
$env:OTEL_LOGS_EXPORTER = "none"
$env:OTEL_SEMCONV_STABILITY_OPT_IN = "http"
$env:OTEL_PYTHON_DISABLED_INSTRUMENTATIONS = "click,logging,threading"

& .tmp/auto-instrumentation-venv/Scripts/opentelemetry-instrument.exe `
  .tmp/auto-instrumentation-venv/Scripts/python.exe `
  examples/auto-instrumentation/client.py
```

HTTPX supplies the CLIENT span and propagates its context. FastAPI supplies the
correlated SERVER span and its `http.route=/users/{user_id}` attribute. The
service namespace comes from the standard resource environment variable.

## Query the endpoint

Forward the Gremlin Service and use the typed SDK:

```powershell
kubectl port-forward --namespace <namespace> service/<gremlin-service> 8182:8182
& .venv/Scripts/python.exe
```

```python
from extended_otel_semconv import AppEndpoint
from extended_otel_semconv.gremlin import SemanticGremlinClient

with SemanticGremlinClient("ws://127.0.0.1:8182/gremlin") as client:
    elements = client.query(
        lambda g: g.V()
        .has_label("app_endpoint")
        .has("service_name", "checkout-api")
    )

endpoint = next(element for element in elements if isinstance(element, AppEndpoint))
print(endpoint.model_dump_json(indent=2))
```

The expected identifying attributes are:

```json
{
  "service.name": "checkout-api",
  "service.namespace": "shop",
  "http.request.method": "GET",
  "http.route": "/users/{user_id}"
}
```

If no endpoint appears, inspect the raw spans before changing the entity model.
The server span must be `SERVER`, share the client's trace ID, use the client
span ID as its parent, and contain `http.route`. Both spans must reach the same
stateful Collector backend.

## Add ETL execution context

ETL Pipeline identity is process-static when one deployment executes one
pipeline. Configure it as resource data on the ETL worker:

```powershell
$env:OTEL_SERVICE_NAME = "etl-worker"
$env:OTEL_RESOURCE_ATTRIBUTES = "service.namespace=data,etl.pipeline.id=extract-customers,etl.pipeline.name=Extract Customers"
```

Run and Part Run identities change without replacing the process, so they must
be span attributes. The reference
`examples/auto-instrumentation/etl_context.py` registers one span processor at
process startup and copies process-local execution context onto every span
started inside the wrappers. It does not create a tracer provider per execution
and does not propagate ETL ownership into downstream services.

`examples/auto-instrumentation/etl_client.py` demonstrates the intended
application boundary:

```python
configure_etl_instrumentation()

with (
    etl_run(run_id="demo-run-001", name="Scheduled customer extraction"),
    etl_part(part_run_id="load-customers", name="Load customers"),
):
    httpx.get("http://127.0.0.1:18081/users/123")
```

Launch it through the same `opentelemetry-instrument` command used by the plain
client. The HTTPX client spans receive the dynamic fields while the Pipeline
fields remain on the resource. The service-graph connector can use configured
dimensions from both locations.
