# Upgrades and Recovery

The Collector, Flink application, and ArangoDB access services have
different upgrade behavior.

## Before every upgrade

1. Record current image digests and Helm values.
2. Confirm both Kafka topics are healthy.
3. Confirm the Flink job is `RUNNING`.
4. Confirm a recent checkpoint completed.
5. Render and review the new chart.
6. Confirm the new image can read the existing event and state schemas.

## Collector upgrade

The routers are stateless, but backends hold in-memory span-pairing and
cumulative-to-delta state. A restart can lose in-flight pairs and reset local
metric streams.

Use a controlled rollout and expect a short observation gap. Do not change
`backend.mode` or the horizontal replica count as part of an ordinary image
upgrade. Moving from horizontal to single-writer mode is a planned topology
change: routers roll to the direct exporter while the StatefulSet scales down,
and some in-flight pairs can be lost during convergence.

After rollout, verify:

- every configured backend endpoint resolves;
- the backend exports only positive request and failed-request deltas;
- input topic offsets advance;
- Flink does not produce a burst of incorrect graph-element churn.

Rollback to the previous horizontal topology with
`backend.mode=horizontal` and `backend.replicaCount=2`. The hash-ring change can
again split in-flight traces, but it requires no Kafka, Flink-state, or graph
data migration.

## Flink application upgrade

Every Helm upgrade is a controlled, stateful job replacement:

1. The `pre-upgrade` hook confirms the fixed-ID job is active.
2. It stops the job without draining and creates a canonical savepoint.
3. It records revision-specific and `latest.savepoint` handoff files under
   `/flink-state/upgrades` on the shared claim.
4. Helm rolls the JobManager and TaskManagers to the new revision.
5. The `post-upgrade` submitter loads the new package and configuration.
6. It restores the same fixed job ID from the recorded savepoint.

The pre-upgrade hook blocks resource changes when the savepoint fails. A
post-upgrade restoration failure leaves the savepoint and hook logs available,
but processing remains stopped until the incompatibility is fixed or the job
is manually restored. A later upgrade attempt can reuse `latest.savepoint`
when the previous attempt already stopped the job.

Keep `application.clusterId`, `job.fixedJobId`, and the state claim unchanged
across an automatic upgrade. Do not combine storage migration with an
application upgrade.

By default, every savepoint state entry must map to an operator in the new job.
Set `job.allowNonRestoredState=true` only when a reviewed code change
intentionally removes an operator and its state. This setting does not make
incompatible serializers or changed keys safe.

The current lifecycle implementation uses operator UID
`graph-v3-element-lifecycle` and keyed state descriptor
`graph-element-lifecycle-state-v3`. These identifiers support compatible
restoration within the direct-contribution implementation. Renaming operators,
changing key definitions, or changing serialized state models can make a
savepoint incompatible.

Version 3 deliberately cannot restore version 2 interaction and aggregate
state. Its first rollout requires fresh Flink state and a rebuilt output
projection; do not provide an old savepoint to the submitter.

Inspect the lifecycle Jobs after an upgrade:

```powershell
kubectl logs -n servicegraph-system `
  job/processing-servicegraph-flink-upgrade-savepoint
kubectl logs -n servicegraph-system `
  job/processing-servicegraph-flink-submitter
```

Then verify that the fixed-ID job is `RUNNING`, its restored checkpoint entry
references a savepoint, and a new checkpoint completes.

## JobManager failover test

Remove the JobManager pod and verify checkpoint recovery:

```powershell
$jobManagerPod = kubectl get pods --namespace servicegraph-system `
  -l app.kubernetes.io/instance=processing,app.kubernetes.io/component=jobmanager `
  -o jsonpath='{.items[0].metadata.name}'

kubectl delete pod $jobManagerPod --namespace servicegraph-system
kubectl rollout status deployment/processing-servicegraph-flink-jobmanager `
  --namespace servicegraph-system --timeout=5m
```

Submit new traffic after recovery. A ready control plane alone does not prove
the data path recovered.

## Access upgrade and replay

The access initializer runs before installation and upgrade. It accepts an
matching graph topology and refuses incompatible collection, edge-definition,
or named-index drift. Indexer replicas share one Kafka consumer group and use
deterministic ArangoDB keys, so replayed upserts and deletes are idempotent.

To rebuild the projection deliberately:

1. stop the projector Deployment;
2. confirm Kafka retention contains the required lifecycle history;
3. delete and recreate the index through an approved migration procedure;
4. reset or replace the projector consumer group;
5. restart the projector and monitor replay lag.

The chart never performs these destructive steps automatically.

## Registry and event-schema upgrades

Adding an entity can change:

- generated package exports;
- Collector dimensions and cardinality;
- graph nodes and edges in upsert events;
- payload hashes for affected graph elements.

Deploy the Collector dimensions and Flink runtime from the same registry
revision. Consumers should ignore unknown entity and edge types but validate
the event envelope.

Breaking event-contract changes require a new schema version and a migration
plan. Do not repurpose existing fields with new semantics.

## Rollback

A code rollback is safe only when the old image can read:

- the current Flink savepoint or checkpoint state;
- current Kafka records;
- the current graph-element event schema;
- the generated ArangoDB graph schema and Gremlin labels/property aliases.

Preserve the previous image digest, chart values, and generated upgrade
savepoint until post-upgrade verification is complete. Helm rollback changes
the Kubernetes resources but does not itself guarantee that a failed
application submission is running; verify or manually restore the job from the
preserved savepoint.
