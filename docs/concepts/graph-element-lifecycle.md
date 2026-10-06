# Graph Element Lifecycle

Flink is the authority for the current telemetry-derived graph. Public output
contains nodes and edges, never interactions. A Collector service-graph
datapoint is only an input fact from which Flink extracts graph-element
contributions.

## Contributions

Every accepted datapoint is converted through the generated semantic registry
into one contribution for each distinct node and edge it describes. The
contributor ID is a deterministic hash of the client, server, connection type,
and canonical scalar dimensions. Repeated positive evidence from the same
telemetry series therefore refreshes the same contributor.

Several contributors can update the same graph element. Nodes with the same
semantic ID are one node, and edges with the same source ID, relationship type,
and target ID are one edge. Flink stores no separate interaction state after
the contributions have been extracted.

## Distributed refresh coalescing

Extraction first keys contributions by the pair of element ID and contributor
ID. `ContributionCoalescer` immediately forwards a new pair or a changed
element. Repeated observations with the same element retain only the newest
timestamp and forward at most once per configured interval. An older
observation is ignored.

This first key distributes the contributors of a hot element across Flink
subtasks. It reduces repeated work before the second shuffle keys the stream by
element ID. Coalescer timers use processing time because they regulate runtime
work; they do not decide graph expiry. Silent coalescer cleanup removes only its
cache after the effective contributor TTL.

## Attribute merging

Optional attributes from different contributors complete one another. For
example, one contributor can supply a pod's zone while another supplies its
version. The authoritative node contains both.

If contributors provide different values for the same optional attribute,
Flink selects the newest observation. Equal timestamps are resolved by the
lexicographically smallest contributor ID. When a contributor expires, Flink
recomputes from the remaining snapshots, so a value can fall back or disappear.
A field remains while at least one active contributor still supplies it.

## Evidence

Collector request totals enter Flink only as positive freshness evidence. Their
numeric magnitude is discarded. Failed requests already contribute to request
totals, so failure counters are not ingested. Nodes and registry-defined edges
therefore carry current semantic topology and attributes without analytics.

## Expiry and output

Each element stores independent event-time and processing-time expiry for every
contributor. Event time follows telemetry timestamps; processing time guarantees
cleanup when input becomes idle. Refreshing a contributor records later expiry
timestamps, making callbacks from its older timers harmless. Generic Flink
state TTL is not used, because silent state cleanup cannot publish lifecycle
deletes.

The element-keyed lifecycle stage remains the sole graph authority. It emits a
complete `upsert` when merged
state changes and a `delete` only when the final contributor disappears.
Consumers apply these commands idempotently and never calculate their own stale
timeout.
