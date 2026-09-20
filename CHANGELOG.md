# Changelog

All notable changes to this project are documented in this file. The format is
based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and component
versions follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Added

- Pull-request CI for code generation, Python quality checks, tests,
  documentation, Helm charts, package artifacts, and production images.
- Nightly and manually triggered focused Kind E2E validation.
- Main-branch GitHub Pages publication for the MkDocs site.
- Trusted-publishing release automation for PyPI, GHCR images, OCI Helm charts,
  and GitHub release artifacts.
- Contribution, governance, security, conduct, issue, and pull-request guidance.
- A persistent `tools.local_demo` environment with owned `up`, `status`,
  `query`, and `down` commands.
- A deterministic in-process parsing and lifecycle benchmark with optional JSON
  reports and explicit distributed-system non-claims.
- A native Java 17 Flink 2.2 lifecycle job with Protobuf ingestion, framed CBOR
  state, deterministic contributor merging, and checkpoint recovery coverage.
- Contract tests for Collector evidence routing and Java-only Flink deployment.
- Apache-2.0 licensing and the Semconv Graph working product identity.

### Changed

- Flink business timers now exclusively own contributor expiry; generic keyed
  state TTL can no longer remove graph state without publishing a delete.
- Collector interaction and root-discovery evidence now uses gzip-compressed
  OTLP Protobuf. Root discovery aggregates eligible non-client/non-server root
  spans with spanmetrics before Kafka.
- Graph events use schema 3 and generated ArangoDB graph schemas use version 2.
  Edges retain topology and attributes without request counters.
- Repeated identical evidence refreshes contributor deadlines without emitting
  another public upsert; topology or attribute changes still emit complete state.
- Flink state now uses the clean-cutover `java-cbor-v2` format. Existing Python,
  JSON, CBOR-v1, and schema-2 state or topic data require the documented reset.
- Release automation publishes only the reusable semantic SDK to PyPI while
  retaining service wheels as GitHub release artifacts.

### Fixed

### Removed

- The Python Flink implementation and runtime selector.
- The entity-event ingestion lane, reconciliation state, topic, credentials,
  Helm values, schemas, tests, and documentation.
- Edge request/failure counters and metric fields from graph events, storage,
  generated aliases, the Python SDK, and typed Gremlin reconstruction.

[Unreleased]: https://github.com/ron1510/semconv-graph/compare/main...HEAD
