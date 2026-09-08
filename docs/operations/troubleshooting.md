# Troubleshooting

Diagnose the pipeline in order. Start at OTLP ingress and move downstream.

## No graph elements appear

Check:

1. application spans contain a client and server side with the same trace ID;
2. both sides reach the router;
3. routers can resolve every backend endpoint configured by the selected mode;
4. the metrics topic receives OTLP JSON;
5. Flink is `RUNNING`;
6. output topic offsets advance;
7. projector offsets advance;
8. API readiness is healthy.

```console
kubectl get pods -n servicegraph-system
kubectl logs -n servicegraph-system deployment/servicegraph-collector-router
kubectl logs -n servicegraph-system statefulset/servicegraph-collector-backend
kubectl logs -n servicegraph-system deployment/indexer-servicegraph-indexer
kubectl logs -n servicegraph-system deployment/gremlin-servicegraph-gremlin
```

The service-graph connector cannot pair traces that are incomplete, sampled
inconsistently, or split across backends.

## Services appear but custom entities do not

Verify:

- every identifying attribute is present;
- the entity participates in a `service_graph` relationship;
- generated entities and dimensions are current;
- the Collector ConfigMap contains the attribute dimension;
- the Flink image includes the new generated package;
- the affected telemetry was emitted after deployment.

```console
python -m tools.semconv_codegen --check
```

Existing upserts do not gain new entities until new activity produces an
updated payload.

For root-span-only entities, also verify that root-span discovery is enabled in
both charts, the marker is the boolean `true` on a completed root span, the SDK
retains that span, and `otel.root.spans` advances. Such entities are expected to
have no edges unless another input source contributes a relationship.

## Graph elements never delete

Confirm:

- traffic for that exact client, server, connection type, and dimensions has
  actually stopped;
- the demo is not rotating the edge back in;
- `interactionTtlSeconds` is the expected value;
- processing-time timers are running;
- Flink checkpoints and TaskManagers are healthy.

Zero delta metrics do not refresh expiry. Any non-zero delta does.

## Graph elements churn or repeatedly reappear

Likely causes:

- cumulative metrics are reaching Flink without per-backend delta conversion;
- dimensions are unstable or high-cardinality;
- a counter stream repeatedly resets;
- multiple traffic sources use different attributes for the same logical edge;
- old and new Collector configurations are active simultaneously.

Inspect the input metric dimensions and corresponding element events. Confirm
Collector backend configuration includes
`cumulativetodelta/servicegraph`.

## Collector backends show unmatched spans

Check router configuration and DNS:

```console
kubectl get service -n servicegraph-system \
  servicegraph-collector-backend-headless
kubectl get endpoints -n servicegraph-system \
  servicegraph-collector-backend-headless
```

In single-writer mode, both routers must target the same backend Service. In
horizontal mode, both routers must have the same ordered ordinal resolver.
Tail or head sampling upstream must keep client and server spans consistently.

## Flink submitter failed

The submitter is expected to complete after submission. Inspect its logs:

```console
kubectl get jobs -n servicegraph-system
kubectl logs -n servicegraph-system job/processing-servicegraph-flink-submitter
```

Common causes:

- image pull failure;
- JobManager or REST Service not ready;
- PVC not bound;
- a terminal job already uses `job.fixedJobId`;
- missing Python or Java dependencies in the runtime image;
- Kafka security settings rejected at application startup.

The exact submitter name depends on the Helm release and overrides.

## Flink upgrade savepoint failed

A failed `pre-upgrade` hook prevents Helm from changing the runtime
Deployments. Inspect:

```console
kubectl logs -n servicegraph-system \
  job/processing-servicegraph-flink-upgrade-savepoint
```

The hook requires the configured fixed-ID job to be active, the existing REST
Service to be reachable, and the shared claim to be writable. Keep the cluster
ID, fixed job ID, and claim unchanged in the upgrade values.

If the savepoint succeeds but the post-upgrade submitter fails, inspect both
hook Jobs. The savepoint path is retained under `/flink-state/upgrades`; fix
the image or state incompatibility and retry the upgrade. The next pre-upgrade
hook can reuse `latest.savepoint` when the job is already stopped. Do not start
a fresh job without deciding whether losing the saved graph state is
acceptable.

## Flink runtime does not recover

Check the JobManager logs, HA ConfigMaps, and shared state claim:

```console
kubectl logs -n servicegraph-system \
  deployment/processing-servicegraph-flink-jobmanager
kubectl get configmaps -n servicegraph-system
kubectl get pvc -n servicegraph-system
```

The runtime ServiceAccount must have ConfigMap CRUD/list/watch. It does not
need finalizer permissions. Recovery also requires the existing claim and the
same stable cluster and job IDs.

## Flink rejects inputs

An increasing `rejected_inputs` counter means an OTLP payload or supported
service-graph datapoint failed parsing or semantic normalization. Inspect the
bounded reason and detail in TaskManager logs, then inspect a sample Kafka
record through an appropriately secured tool.

Only supported delta service-graph sums with finite, nonnegative values,
timestamps, and required client/server fields affect state. Zero deltas and
unrelated metrics are ignored.

An increasing `rejected_root_spans` counter means a selected OTLP trace payload
or modeled Resource/span attribute combination was invalid. Conflicting values
for a modeled semantic field reject that root; unknown and non-scalar
attributes are ignored.

## API reports ready but data is old

Compare:

- output-topic end offsets;
- indexer consumer-group offsets;
- indexer logs;
- ArangoDB document timestamps.

The indexer can be caught up while the graph still contains an element if
Flink has not emitted the expected delete. A projector restart can also replay
records safely because deterministic IDs make indexing and deletion
idempotent.

## Kafka authentication failures

For every chart, verify:

- protocol is `SASL_PLAINTEXT` or `SASL_SSL`;
- mechanism is `SCRAM-SHA-256`;
- the Secret exists in the same namespace;
- selected username and password keys exist;
- for `SASL_SSL`, the broker certificate matches its hostname and chains to a
  CA in the runtime image's default trust store.

Do not disable endpoint identification or certificate verification to hide a
name or trust-store configuration problem. `SASL_PLAINTEXT` is not a
certificate workaround because it sends credentials and traffic without TLS.
