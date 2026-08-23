# Governance

## Project Model

Extended OpenTelemetry Semantic Conventions is a maintainer-led open source
project. The maintainers are accountable for technical direction, releases,
security response, and a review process that protects runtime contracts while
remaining accessible to contributors.

The current lead maintainer is [@ron1510](https://github.com/ron1510).

## Roles

**Contributors** propose issues, documentation, code, tests, semantic
definitions, and operational evidence. Any participant may become a contributor
through accepted work.

**Reviewers** are contributors trusted to review an area of the repository.
Review authority is scoped by demonstrated knowledge rather than a repository-
wide title.

**Maintainers** merge changes, manage releases and project settings, resolve
cross-component design decisions, and handle security and conduct reports.

## Decisions

Routine changes are decided through pull-request review. Significant changes
should begin with an issue that states the problem, constraints, alternatives,
contract impact, rollout, and validation evidence.

Maintainers seek rough consensus, with technical arguments and operational
evidence carrying more weight than vote counts. When consensus cannot be
reached, the lead maintainer makes the decision and records the reasoning in the
issue or pull request.

Decisions preserve these project boundaries unless an accepted proposal changes
them explicitly:

- OpenTelemetry-derived semantics are registry-owned and generated.
- Flink owns contributor-aware graph lifecycle state.
- Kafka lifecycle events are the durable public stream contract.
- ArangoDB is a replayable current-state projection.
- Gremlin access is read-only.
- Kubernetes deployment requires no operator, CRD, or cluster-admin access.

## Becoming A Reviewer Or Maintainer

Maintainers may invite a contributor who has shown sustained, constructive work,
sound reviews, reliable follow-through, and good judgment in the relevant
domain. Maintainer status additionally requires willingness to support releases,
security response, and community health.

Privileges may be reduced after extended inactivity or misuse. Whenever
practical, role changes are documented publicly.

## Releases

Maintainers approve releases from `main` after required CI succeeds. Component
versions remain independent even though a repository tag publishes a coherent
set of Python, container, and Helm artifacts. Emergency security releases may
use an abbreviated review process but still require reproducible artifacts and
post-release documentation.
