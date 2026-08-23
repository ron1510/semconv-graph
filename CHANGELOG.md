# Changelog

All notable changes to this project are documented in this file. The format is
based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and component
versions follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Added

- Pull-request CI for code generation, Python quality checks, tests,
  documentation, Helm charts, package artifacts, and production images.
- Nightly and manually triggered focused Kind E2E validation.
- Trusted-publishing release automation for PyPI, GHCR images, OCI Helm charts,
  and GitHub release artifacts.
- Contribution, governance, security, conduct, issue, and pull-request guidance.
- A persistent `tools.local_demo` environment with owned `up`, `status`,
  `query`, and `down` commands.
- A deterministic in-process parsing and lifecycle benchmark with optional JSON
  reports and explicit distributed-system non-claims.
- An opt-in registered-topology adapter for OTel `entity.state` and
  `entity.delete` events.
- Apache-2.0 licensing and the Semconv Graph working product identity.

### Changed

- Flink business timers now exclusively own contributor expiry; generic keyed
  state TTL can no longer remove graph state without publishing a delete.
- Release automation publishes only the reusable semantic SDK to PyPI while
  retaining service wheels as GitHub release artifacts.

### Fixed

### Removed

[Unreleased]: https://github.com/ron1510/extended-opentelemetry-semconv/compare/main...HEAD
