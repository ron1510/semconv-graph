# Contributing

Thank you for helping improve Extended OpenTelemetry Semantic Conventions.
The project accepts focused bug fixes, semantic-registry additions, deployment
improvements, documentation, and well-scoped architectural changes.

## Before You Start

- Search existing issues and pull requests before opening a new one.
- Use an issue form for changes that affect public contracts, generated
  semantics, Flink state, Kafka events, ArangoDB topology, or deployment.
- Keep a pull request focused on one problem. Discuss broad redesigns before
  implementation.
- Never include credentials, private telemetry, customer data, or internal
  registry addresses in issues, tests, or logs.

## Development Environment

The supported development interpreter is CPython 3.12. The project uses pip
and standard PEP 621 metadata; uv is not required.

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e ".[dev,docs]" \
  -e "packages/extended-opentelemetry-semconv[gremlin]" \
  -e services/otel-servicegraph-diff \
  -e services/servicegraph-demo \
  -e services/servicegraph-indexer
```

On PowerShell, activate with `.\.venv\Scripts\Activate.ps1` and place the
editable installs on one line.

## Repository Boundaries

- `packages/extended-opentelemetry-semconv` is the public semantic SDK.
- `tools/semconv_codegen` owns registry validation and generated artifacts.
- `services/otel-servicegraph-diff` owns Collector ingest and Flink lifecycle
  processing.
- `services/servicegraph-indexer` owns Kafka-to-ArangoDB projection.
- `services/servicegraph-gremlin` owns the read-only Gremlin runtime.
- `deploy/helm` owns Kubernetes packaging without operators or CRDs.

Generated Python models, Collector dimensions, and graph schema files must be
changed through `tools.semconv_codegen`, not edited by hand.

## Validation

Run the checks affected by your change while iterating. Before requesting
review, run the complete non-E2E suite:

```bash
python -m tools.semconv_codegen --check
python -m ruff check .
python -m pyright
python -m pytest -m "not e2e"
python -m mkdocs build --strict
```

Lint and render every Helm chart when deployment behavior changes:

```bash
for chart in deploy/helm/*; do
  helm lint "$chart"
  helm template ci "$chart" >/dev/null
done
```

The focused E2E test provisions disposable Docker and Kind resources. It is
intentionally excluded from normal test runs and runs nightly in GitHub Actions:

```bash
python -m pytest -m e2e --run-e2e -v
```

## Semantic Changes

A semantic entity or relationship contribution should include:

- the operational problem it represents;
- canonical OpenTelemetry attributes and their source;
- deterministic identity fields;
- valid source and target types for relationships;
- extraction and lifecycle behavior;
- generator, model, and projection tests;
- regenerated committed artifacts.

Do not add optional attributes merely because they exist upstream. The
service-graph pipeline must be able to observe and transport them.

## Pull Requests

- Explain the user-visible behavior and important design decisions.
- Identify contracts or persisted state that change.
- Add tests proportional to the risk and failure modes.
- Keep generated changes in the same commit as their source change.
- Update `CHANGELOG.md` under `Unreleased` for user-visible changes.
- Confirm that no unrelated formatting or generated output is included.

Maintainers may ask for a change to be split when its parts can be reviewed or
released independently.

## Releases

Maintainers create repository release tags in the form `vX.Y.Z` from `main`.
Before tagging, bump every changed Python distribution and Helm chart and move
relevant changelog entries into a release section. The release workflow:

1. builds and inspects all Python distributions;
2. publishes new distribution versions to PyPI with trusted publishing;
3. publishes service images to GHCR under the repository tag;
4. publishes new chart versions to the owner's OCI `charts` namespace;
5. creates a GitHub release containing Python and chart artifacts.

Unchanged Python and chart versions are skipped so independently versioned
components can share one repository release. Each PyPI project must configure
this repository, the `Release` workflow, and the `pypi` environment as a trusted
publisher before the first release.
