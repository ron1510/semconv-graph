# Monitoring

Monitor the pipeline as one system. A healthy pod is not proof that telemetry is
moving.

## Collector

Watch:

- refused spans and failed exports;
- service-graph connector items awaiting pairing;
- backend memory and restarts;
- router queue growth;
- Kafka exporter queue failures and retry duration;
- unmatched spans caused by incomplete traces.
- discovery-backend load, series cardinality, and restarts when enabled;
- root spans dropped by the discovery conflict or client/server-kind filters;

Useful commands:

```console
kubectl get pods -n servicegraph-system \
  -l app.kubernetes.io/instance=collection

kubectl logs -n servicegraph-system \
  statefulset/servicegraph-collector-backend \
  --since=15m
```

In default single-writer mode, the one backend endpoint must resolve and the pod
must remain ready. A restart creates a short observation gap; existing graph
elements should remain while the gap stays below the Flink contributor TTL.
In horizontal mode, verify every configured ordinal and watch for uneven shard
load and repeated metric-series writers.

## Kafka

Track:

- input and output topic write rates;
- partition availability and under-replication;
- Flink consumer-group lag;
- ArangoDB indexer consumer-group lag;
- broker request latency and rejected writes;
- topic retention relative to recovery time.

An advancing input topic with a static output topic usually means Flink is
stopped, backpressured, rejecting records, or not detecting activity.

## Flink

Track:

- job state;
- completed, failed, and in-progress checkpoints;
- time since the last completed checkpoint;
- checkpoint duration and size;
- source lag and backpressure;
- TaskManager availability;
- JobManager leadership changes;
- restart count;
- `rejected_inputs` and its reason-specific ingest counters;
- shared checkpoint volume capacity;
- TaskManager `/flink-rocksdb` ephemeral-storage utilization.

Open the Flink UI:

```console
kubectl port-forward -n servicegraph-system \
  service/servicegraph-diff-rest 8081:8081
```

The job must remain `RUNNING`, and completed checkpoints must continue to
increase while traffic is present. Alert before `/flink-rocksdb` reaches 70%,
on two consecutive checkpoint failures, or when checkpoint duration and size
continue growing after the contributor TTL window instead of stabilizing.

For a Java migration canary, compare the recorded Python baseline with the
native Java image using identical traffic, contributor cardinality, CPU/memory
requests and limits, parallelism, checkpoint configuration, and storage. Record
correct input/output counts, throughput, CPU, RSS/heap, event latency,
checkpoint duration and incremental size, and both Kafka group lags after the
same warmup. Record storage latency separately; a language change does not
prove checkpoint-volume reliability or remove contributor merge scans.

Run the private-network canary for at least six hours and longer than the
previous failure window. Check for stable checkpoint sizes after the TTL
window, no sustained lag or restart loop, and expiry after recovery with no
new input. Local integration tests do not replace this operational canary.

The previous private-network failure followed several hours of lifecycle
processing that delayed checkpoints until repeated failures stopped the job.
Include many contributors sharing one element in the canary, rather than only
many independent elements. Track lifecycle busy time, checkpoint alignment
and end-to-end duration, failed checkpoint reasons, and input lag throughout
the run. Keep the existing checkpoint timeout and failure tolerance for the
comparison; increasing them would conceal the failure being investigated.

## ArangoDB and Gremlin access

Check:

Check the indexer Deployment, its Kafka group lag, ArangoDB server health and
storage, and Gremlin Server TCP readiness. A bounded traversal is a useful
functional probe:

```python
g.V().limit(1).count().next()
```

Compare the indexer group position with the output topic end offset when the
graph appears stale. Monitor ArangoDB request errors, disk use, collection
growth, and Gremlin evaluation timeouts.

## End-to-end canary

A useful canary emits a known paired trace periodically and verifies:

1. the metrics topic receives it;
2. the graph-element topic receives node and edge upserts;
3. the projection exposes the expected entities;
4. stopping the canary eventually produces a delete.

Use a bounded, recognizable service namespace so canary entities are easy to
filter and do not collide with production services.

## Suggested alerts

Alert on:

- any Flink job not `RUNNING`;
- checkpoint failure or excessive checkpoint age;
- sustained Kafka consumer lag;
- repeated Collector export failures;
- Collector or TaskManager restart loops;
- `rejected_inputs` increasing;
- discovery conflict-filter drops increasing;
- Flink state or ArangoDB storage nearing capacity;
- Gremlin readiness failure;
- indexer restarts or sustained consumer lag;
- no output events while input activity is present.
