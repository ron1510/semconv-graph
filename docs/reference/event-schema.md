# Graph element event schema

Flink publishes canonical JSON records to `graph.elements.events`. Schema 3.0 is a clean contract break: edge counters and the `metrics` field do not exist. Kafka keys are always `element_id`, so compaction retains the latest complete state or deletion for each graph element.

## Envelope

| Field | Type | Meaning |
| --- | --- | --- |
| `schema_version` | string | Always `"3.0"` |
| `event_id` | string | Deterministic SHA-256 identity for this lifecycle transition |
| `event_type` | string | Always `graph_element_state_changed` |
| `element_id` | string | Stable node or relationship ID and Kafka key |
| `observed_at_unix_nano` | integer | Evidence or expiry time |
| `emitted_at_unix_ms` | integer | Processing time when Flink emitted the record |
| `operation` | string | `upsert` or `delete` |
| `payload_hash` | string or null | Canonical element hash for upserts |
| `element` | object or null | Complete node/edge state for upserts |

## Node upsert

```json
{
  "schema_version": "3.0",
  "event_id": "...",
  "event_type": "graph_element_state_changed",
  "element_id": "service:checkout",
  "observed_at_unix_nano": 1700000000000000000,
  "emitted_at_unix_ms": 1700000000000,
  "operation": "upsert",
  "payload_hash": "...",
  "element": {
    "kind": "node",
    "id": "service:checkout",
    "type": "service",
    "attributes": {"service.name": "checkout"}
  }
}
```

## Edge upsert

```json
{
  "schema_version": "3.0",
  "event_id": "...",
  "event_type": "graph_element_state_changed",
  "element_id": "edge:...",
  "observed_at_unix_nano": 1700000000000000000,
  "emitted_at_unix_ms": 1700000000000,
  "operation": "upsert",
  "payload_hash": "...",
  "element": {
    "kind": "edge",
    "id": "edge:...",
    "type": "calls",
    "source_id": "service:storefront",
    "target_id": "service:checkout",
    "attributes": {}
  }
}
```

## Delete

Deletes have `payload_hash: null` and `element: null`. The indexer removes the document selected by `element_id`; edge deletes are attempted across generated edge collections because the ID contains no semantic-type prefix.

The indexer rejects any version other than 3.0 and rejects an element containing `metrics`.
