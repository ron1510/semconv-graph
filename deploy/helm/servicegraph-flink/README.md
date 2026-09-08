# Servicegraph Flink Chart

This chart runs Flink 2.2.1 as a standalone Kubernetes Session cluster. Helm
owns one JobManager Deployment, a fixed TaskManager pool, the REST Service, and
the initial PyFlink submission Job. Flink does not create Kubernetes workloads
and the chart requires no CRDs or finalizer permissions.

Kubernetes HA retains the submitted job across a JobManager pod replacement.
The default uses one JobManager, two TaskManagers, and one RWX claim for HA
metadata, checkpoints, and savepoints. Recovery has brief downtime and resumes
from the latest successful checkpoint.

Selective root-span node discovery is disabled by default. To consume the
Collector's filtered OTLP trace topic, configure the same topic in both charts:

```yaml
rootSpanDiscovery:
  enabled: true
streamContract:
  topics:
    rootSpans: otel.root.spans
```

This independent source extracts semantic nodes only and merges them into the
same contributor lifecycle state. It never infers relationships from a lone
root span.

```powershell
helm upgrade --install servicegraph-flink deploy/helm/servicegraph-flink `
  --namespace servicegraph-system --create-namespace `
  --values internal-flink-values.yaml `
  --wait --timeout 10m
```

For a platform-provided ServiceAccount and PVC:

```yaml
serviceAccount:
  create: false
  name: servicegraph-flink
rbac:
  create: false
storage:
  createClaim: false
  existingClaim: servicegraph-flink-state
```

The ServiceAccount needs namespace-scoped ConfigMap create, delete, get, list,
patch, update, and watch permissions. It does not need permissions for Pods,
Deployments, Services, CRDs, or finalizers.

OpenShift should assign the namespace filesystem group. On Kubernetes
distributions that do not mutate the pod identity, set
`podSecurityContext.runAsUser`, `runAsGroup`, and `fsGroup` to allowed numeric
non-root IDs. The Flink image's built-in user is `9999`.

The post-install submitter waits for the REST endpoint and submits
`otel_servicegraph_diff.flink_job` with `job.fixedJobId`. A repeated install skips an
already active job with that ID.

Every Helm upgrade performs a stateful application replacement:

1. A `pre-upgrade` Job stops the active Flink job with a savepoint.
2. The savepoint path is recorded on the shared state claim.
3. Helm rolls the JobManager and TaskManagers to the new revision.
4. The `post-upgrade` submitter restores the new image and configuration from
   the savepoint with the same fixed job ID.

The savepoint must complete before Helm changes the runtime resources. A
restore failure leaves the savepoint available for diagnosis and recovery.
The upgrade causes brief processing downtime.

Keep `application.clusterId`, `job.fixedJobId`, and the state claim unchanged
across automatic upgrades. Set `job.allowNonRestoredState=true` only for an
intentional topology change that removes state which may be discarded.

Flink writes to container stdout:

```powershell
kubectl logs -n servicegraph-system `
  deployment/servicegraph-flink-servicegraph-flink-jobmanager --follow
kubectl logs -n servicegraph-system `
  deployment/servicegraph-flink-servicegraph-flink-taskmanager --follow --prefix
```

JobManager logs contain leadership, recovery, and checkpoint coordination.
TaskManager logs contain Kafka connector, operator, and Python worker output.
Use `kubectl logs <pod> --previous` after a container restart.

Upgrade lifecycle logs remain available through the hook Jobs:

```powershell
kubectl logs -n servicegraph-system `
  job/servicegraph-flink-servicegraph-flink-upgrade-savepoint
kubectl logs -n servicegraph-system `
  job/servicegraph-flink-servicegraph-flink-submitter
```
