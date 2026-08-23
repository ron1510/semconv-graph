## Problem

Describe the user or operational problem this change solves.

## Change

Describe the implementation and important design decisions.

## Contract Impact

- [ ] No public contract or persisted-state change
- [ ] Semantic registry or generated model change
- [ ] Flink state, operator UID, or timer change
- [ ] Kafka event or topic contract change
- [ ] ArangoDB collection, index, or Gremlin behavior change
- [ ] Helm values or Kubernetes resource change

Explain every checked contract change and its rollout or compatibility impact.

## Validation

List the exact commands and runtime paths exercised.

- [ ] `python -m tools.semconv_codegen --check`
- [ ] `python -m ruff check .`
- [ ] `python -m pyright`
- [ ] `python -m pytest -m "not e2e"`
- [ ] `python -m mkdocs build --strict`
- [ ] Relevant Helm charts linted and rendered
- [ ] Focused E2E run when the cross-service runtime changed

## Review Readiness

- [ ] The pull request is focused and contains no unrelated generated changes.
- [ ] Tests cover the important failure and replay behavior.
- [ ] User-visible changes are recorded under `CHANGELOG.md` `Unreleased`.
- [ ] No credentials, customer data, or private telemetry are included.
