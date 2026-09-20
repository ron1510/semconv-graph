# Development

## Environment

Use Python 3.12. Create a virtual environment and install all repository
packages:

```console
python -m venv .venv
python -m pip install -e ".[dev,docs]"
python -m pip install -e "packages/extended-opentelemetry-semconv[gremlin]"
python -m pip install -e services/servicegraph-demo
python -m pip install -e services/servicegraph-indexer
```

The Flink application uses Java 17 and Maven. Python 3.12 is used for the SDK,
indexer, demo, code generation, and repository tooling.

## Generated files

Registry source is hand-written; the semantic JSON Schema, static Pydantic
Python, Java semantic metadata, relationship metadata, and Collector dimensions
are committed:

```console
python -m tools.semconv_codegen
```

The contributor dependency `datamodel-code-generator==0.71.0` is pinned in the
normal `dev` optional dependency. It is never imported by the published SDK.
Never manually patch generated modules, schemas, or dimensions. Change registry
source and regenerate.

## Validation

```console
python -m tools.semconv_codegen --check
python -m ruff check .
python -m pyright
python -m pytest -m "not e2e"
mvn -f services/otel-servicegraph-diff/runtime/java/pom.xml verify
helm lint deploy/helm/servicegraph-collector
helm lint deploy/helm/servicegraph-demo
helm lint deploy/helm/servicegraph-flink
helm lint deploy/helm/servicegraph-arangodb
helm lint deploy/helm/servicegraph-indexer
helm lint deploy/helm/servicegraph-gremlin
```

Tests focus on registry validation, generated artifacts, pure contributor
lifecycle transitions, Flink timers, demo topology, generated ArangoDB topology, native
projection, and Gremlin traversal behavior.

## Documentation

```console
python -m pip install mkdocs-material==9.7.7
python -m mkdocs build --strict
python -m mkdocs serve
```

Open `http://127.0.0.1:8000`. The generated `site/` directory is ignored and
must not be committed.

## Runtime images

The Flink release artifact is one immutable Java 17 image built by Maven:

```console
docker build \
  --file services/otel-servicegraph-diff/Dockerfile \
  --target runtime \
  --secret id=maven_settings,src=$HOME/.m2/settings.xml \
  --tag registry.internal.example/extended-otel-flink-runtime:2.2.1-java17 \
  .
```

The build packages the native Java Flink application and connector dependencies
into an executable JAR on Flink 2.2.1 Java 17. No Python dependency is required
by the Flink image. The build supports the Maven settings secret for internal
repositories. The access and demo images install Python packages directly.

For local Java execution, see [Run Flink locally](development/local-flink.md).

## Release checklist

1. Run generated-file, Python, Helm, and documentation checks.
2. Build images from a clean commit.
3. Run the complete local lifecycle test.
4. Record image digests.
5. Review event-schema and registry compatibility.
6. Publish immutable images and chart source.
7. Test installation using only published artifacts.
8. Describe entity, relationship, dimension, state, and event changes in the
   release notes.

Mirror Python, Maven, Flink, and Collector dependencies into internal
repositories when deployment environments cannot access public registries.
