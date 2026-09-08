# Auto-Instrumentation Example

`server.py` and `client.py` contain ordinary FastAPI and HTTPX application code.
They rely entirely on OpenTelemetry distro auto-instrumentation to produce the
spans from which Semconv Graph creates an `AppEndpoint`.

`etl_client.py` adds the minimal explicit integration required for dynamic ETL
Run and Part Run identity. `etl_context.py` is a reference implementation for
the ETL platform team, not part of the published semantic SDK.

Follow the [complete guide](../../docs/getting-started/auto-instrumentation.md)
to run the example against an existing deployment.
