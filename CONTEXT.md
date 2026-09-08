# Semantic Graph

The semantic graph represents current logical assets, their concrete executions, and the relationships that make operational impact explainable.

## Language

**ETL Pipeline**:
A durable logical ETL definition across executions and the target of aggregate success-rate monitoring.
_Avoid_: ETL CronJob, ETL Deployment

**ETL Run**:
One execution of an ETL Pipeline, identified independently from the workload that executes it.
_Avoid_: ETL instance, Kubernetes Job

**ETL Part Run**:
One logical function execution belonging to an ETL Run. Retries remain part of the same ETL Part Run.
_Avoid_: Part, child job
