# Security Policy

## Supported Versions

Security fixes are applied to the latest released version and the `main`
branch. Older releases are not maintained unless a maintainer explicitly says
otherwise in a security advisory.

## Reporting A Vulnerability

Do not open a public issue for a suspected vulnerability.

Use GitHub's private vulnerability reporting for this repository:

<https://github.com/ron1510/extended-opentelemetry-semconv/security/advisories/new>

Include the affected component and version, deployment assumptions, impact,
reproduction steps, and any proposed remediation. Remove credentials, customer
data, and private telemetry from the report.

A maintainer will acknowledge the report as soon as practical, validate the
impact, and coordinate disclosure and remediation with the reporter. Public
disclosure should wait until a fix or mitigation is available.

## Scope

Security reports may cover the Python SDK, Flink job, Kafka-to-ArangoDB
indexer, Gremlin runtime, container images, Helm charts, and release
infrastructure. Vulnerabilities in unmodified upstream dependencies should also
be reported upstream; report them here when this project's configuration or
integration materially changes their impact.
