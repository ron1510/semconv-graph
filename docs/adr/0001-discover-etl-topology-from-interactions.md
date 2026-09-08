---
status: accepted
---

# Discover ETL topology from interactions

ETL Pipeline, ETL Run, and ETL Part Run entities will be derived from ETL identity attributes on the service's interaction spans and processed through the existing service-graph metrics and Flink lifecycle path. This reuses the graph engine's contribution, merging, and staleness behavior without adding raw-span ingestion or another entity transport; it deliberately models observed ETL topology rather than authoritative inventory.

## Consequences

- Identity is hierarchical: Pipeline ID; Pipeline ID plus Run ID; and Pipeline ID plus Run ID plus Part Run ID.
- Retries preserve the logical Part Run ID, while a later Pipeline Run creates a new Part Run.
- Extraction is additive for each valid hierarchy prefix and introduces no ETL-specific buffering or state.
- Only model fields become service-graph dimensions. The accepted cost is higher metric cardinality and Kafka throughput.
- Executions without a matched interaction remain undiscovered.
- The first version models `Pipeline -> contains -> Run -> contains -> Part Run`; linking Services directly to ETLs is deferred.
- Success-rate monitoring and span-metrics design are separate decisions.
