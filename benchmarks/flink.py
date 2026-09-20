"""Controlled native Java Kafka-to-Flink workload; smoke measurements, not capacity."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import time
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Literal, TypedDict, cast

from pydantic import BaseModel, ConfigDict, Field

from benchmarks.servicegraph import BenchmarkContractError, DatasetConfig, generate_otlp_proto
from extended_otel_semconv import Service
from extended_otel_semconv.edges import edge_id
from tools.local_demo.environment import DemoEnvironment

INPUT_TOPIC = "otel.servicegraph.metrics"
OUTPUT_TOPIC = "graph.elements.events"


class GraphEvent(BaseModel):
    """Fields inspected by the measurement consumer; Java owns the wire implementation."""

    schema_version: Literal["3.0"]
    operation: Literal["upsert", "delete"]
    element_id: str
    element: dict[str, object] | None


class WorkloadConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    entities: int = Field(default=16, gt=1)
    contributors: int = Field(default=4, gt=0)
    seed: int = 20_260_823
    observed_at_unix_nano: int = Field(default_factory=time.time_ns, gt=0)
    service_prefix: str = Field(default="service", pattern=r"^[a-z0-9-]+$")
    iterations: int = Field(default=3, gt=0)
    warmup: int = Field(default=1, gt=0)
    checkpoint_samples: int = Field(default=2, gt=0)
    timeout_seconds: int = Field(default=120, gt=0)


class PodSample(TypedDict):
    pod: str
    role: str
    sampled_at_monotonic_ns: int
    cpu_usage_usec: int
    memory_current_bytes: int
    memory_anonymous_resident_bytes: int


class OffsetSample(TypedDict):
    input_end: int
    input_committed: int
    input_committed_lag: int
    output_end: int
    indexer_committed: int
    indexer_committed_lag: int


class CheckpointSample(TypedDict):
    completed: int
    failed: int
    latest_id: int | None
    duration_ms: int | None
    checkpointed_size_bytes: int | None
    full_state_size_bytes: int | None


class RuntimeReport(TypedDict):
    scope: Literal["controlled_distributed_flink_workload"]
    runtime: str
    image: str
    config: dict[str, object]
    deployment_settings: dict[str, object]
    payload_sha256: str
    payload_bytes: int
    datapoints_per_batch: int
    total_measured_datapoints: int
    measured_seconds: float
    datapoints_per_second: float
    batch_completion_latency_ms: dict[str, float]
    output_events_warmup: int
    output_events_measured: int
    expected_events_measured: int
    final_element_count: int
    final_elements_sha256: str
    expected_elements_sha256: str
    expected_matches: bool
    pods_before: list[PodSample]
    pods_after: list[PodSample]
    average_cpu_cores: dict[str, float]
    checkpoints: list[CheckpointSample]
    offsets: list[OffsetSample]
    limitations: list[str]


def expected_elements(payload: bytes, batches: int) -> dict[str, dict[str, object]]:
    """Calculate the known evidence-derived service ring without a lifecycle oracle."""
    from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2 import ExportMetricsServiceRequest

    versions: dict[str, tuple[int, str]] = {}
    pairs: set[tuple[str, str]] = set()
    document = ExportMetricsServiceRequest.FromString(payload)
    for resource in document.resource_metrics:
        for scope in resource.scope_metrics:
            for metric in scope.metrics:
                for point in metric.sum.data_points:
                    attributes = {attr.key: attr.value.string_value for attr in point.attributes}
                    client, server = attributes["client"], attributes["server"]
                    stamp = point.time_unix_nano
                    for side, service in (("client", client), ("server", server)):
                        previous = versions.get(service)
                        if previous is None or stamp > previous[0]:
                            versions[service] = (stamp, attributes[f"{side}_service.version"])
                    pairs.add((client, server))
    elements: dict[str, dict[str, object]] = {}
    for name, (_, version) in versions.items():
        service = Service(service_name=name, service_version=version)
        elements[service.entity_id] = {
            "kind": "node",
            "id": service.entity_id,
            "type": "service",
            "attributes": {"service.name": name, "service.version": version},
        }
    for client, server in pairs:
        source = Service(service_name=client).entity_id
        target = Service(service_name=server).entity_id
        identifier = edge_id(source, "calls", target)
        elements[identifier] = {
            "kind": "edge",
            "id": identifier,
            "type": "calls",
            "source_id": source,
            "target_id": target,
            "attributes": {},
        }
    return elements if batches else {}


def elements_hash(elements: Mapping[str, dict[str, object]]) -> str:
    encoded = json.dumps(elements, ensure_ascii=False, separators=(",", ":"), sort_keys=True, allow_nan=False)
    return hashlib.sha256(encoded.encode()).hexdigest()


def parse_counter_file(text: str) -> dict[str, int]:
    return {parts[0]: int(parts[1]) for line in text.splitlines() if len(parts := line.split()) == 2}


def cpu_cores(before: Sequence[PodSample], after: Sequence[PodSample]) -> dict[str, float]:
    previous = {sample["pod"]: sample for sample in before}
    result: dict[str, float] = {}
    for sample in after:
        old = previous.get(sample["pod"])
        if old is None:
            raise BenchmarkContractError("Flink pod was replaced during controlled measurement")
        elapsed = (sample["sampled_at_monotonic_ns"] - old["sampled_at_monotonic_ns"]) / 1_000_000_000
        usage = sample["cpu_usage_usec"] - old["cpu_usage_usec"]
        if elapsed <= 0 or usage < 0:
            raise BenchmarkContractError("cgroup counter reset or invalid sample interval")
        result[sample["role"]] = result.get(sample["role"], 0.0) + usage / 1_000_000 / elapsed
    if len(previous) != len(after):
        raise BenchmarkContractError("Flink pod count changed during controlled measurement")
    return result


def _object(value: object) -> dict[str, object]:
    if not isinstance(value, dict):
        raise BenchmarkContractError("expected JSON object in benchmark observation")
    return cast(dict[str, object], value)


def _flink_json(environment: DemoEnvironment, path: str) -> dict[str, object]:
    result = environment.kubectl(
        "get",
        "--raw",
        f"/api/v1/namespaces/{environment.namespace}/services/http:servicegraph-diff-rest:8081/proxy/{path}",
        timeout=30,
    )
    return _object(json.loads(result.stdout))


def _checkpoint(environment: DemoEnvironment, job_id: str) -> CheckpointSample:
    response = _flink_json(environment, f"jobs/{job_id}/checkpoints")
    counts = _object(response["counts"])
    latest = _object(response.get("latest", {})).get("completed")
    complete = _object(latest) if latest is not None else {}

    def number(key: str) -> int | None:
        value = complete.get(key)
        return int(cast(int, value)) if value is not None else None

    return {
        "completed": int(cast(int, counts["completed"])),
        "failed": int(cast(int, counts["failed"])),
        "latest_id": number("id"),
        "duration_ms": number("end_to_end_duration"),
        "checkpointed_size_bytes": number("checkpointed_size"),
        "full_state_size_bytes": number("state_size"),
    }


def _pods(environment: DemoEnvironment) -> list[PodSample]:
    response = _object(
        json.loads(
            environment.kubectl(
                "get",
                "pods",
                "-l",
                "app.kubernetes.io/instance=processing",
                "-o",
                "json",
                timeout=30,
            ).stdout
        )
    )
    samples: list[PodSample] = []
    for item in cast(list[object], response["items"]):
        metadata = _object(_object(item)["metadata"])
        role = _object(metadata["labels"]).get("app.kubernetes.io/component")
        if role not in {"jobmanager", "taskmanager"}:
            continue
        pod = cast(str, metadata["name"])
        cpu = environment.kubectl("exec", pod, "--", "cat", "/sys/fs/cgroup/cpu.stat", timeout=30).stdout
        memory = environment.kubectl("exec", pod, "--", "cat", "/sys/fs/cgroup/memory.current", timeout=30).stdout
        resident = environment.kubectl("exec", pod, "--", "cat", "/sys/fs/cgroup/memory.stat", timeout=30).stdout
        samples.append(
            {
                "pod": pod,
                "role": cast(str, role),
                "sampled_at_monotonic_ns": time.monotonic_ns(),
                "cpu_usage_usec": parse_counter_file(cpu)["usage_usec"],
                "memory_current_bytes": int(memory.strip()),
                "memory_anonymous_resident_bytes": parse_counter_file(resident)["anon"],
            }
        )
    if not samples or not any(sample["role"] == "taskmanager" for sample in samples):
        raise BenchmarkContractError("no Flink cgroup-v2 worker observations available")
    return samples


def _settings(environment: DemoEnvironment) -> dict[str, object]:
    values = _object(
        json.loads(
            environment.run(
                [
                    "helm",
                    "get",
                    "values",
                    "processing",
                    "--namespace",
                    environment.namespace,
                    "--all",
                    "--output",
                    "json",
                ],
                timeout=30,
            ).stdout
        )
    )
    application = _object(values["application"])
    return {
        "application": application,
        **{key: values[key] for key in ("job", "state", "resources", "storage")},
        "topics": _object(values["streamContract"])["topics"],
    }


def _latencies(samples: Sequence[float]) -> dict[str, float]:
    ordered = sorted(samples)
    return {
        "min": ordered[0],
        "p50": ordered[math.ceil(len(ordered) * 0.5) - 1],
        "p95": ordered[math.ceil(len(ordered) * 0.95) - 1],
        "max": ordered[-1],
        "mean": sum(ordered) / len(ordered),
    }


def measure(environment: DemoEnvironment, config: WorkloadConfig | None = None) -> RuntimeReport:
    # Kafka's dynamic API is confined to this integration boundary.
    from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer
    from kafka.structs import TopicPartition

    if environment.kafka_host_address is None or not environment.with_ingest_pipeline:
        raise BenchmarkContractError("measure requires a provisioned Flink ingest environment")
    config = config or WorkloadConfig()
    payload = generate_otlp_proto(
        DatasetConfig(config.entities, config.contributors, config.seed),
        base_timestamp_unix_nano=config.observed_at_unix_nano,
        service_prefix=config.service_prefix,
    )
    wire = payload
    targets = [expected_elements(payload, batch + 1) for batch in range(config.warmup + config.iterations)]
    expected = targets[-1]
    identifiers = set(expected)
    observed: dict[str, dict[str, object]] = {}
    address = environment.kafka_host_address
    consumer = KafkaConsumer(
        OUTPUT_TOPIC, bootstrap_servers=address, auto_offset_reset="earliest", enable_auto_commit=False, group_id=None
    )
    producer = KafkaProducer(bootstrap_servers=address, acks="all")
    admin = KafkaAdminClient(bootstrap_servers=address)

    def drain(timeout_ms: int = 100) -> int:
        count = 0
        for records in consumer.poll(timeout_ms=timeout_ms, max_records=10000).values():
            for record in records:
                raw = cast(bytes, record.value)
                document = _object(json.loads(raw))
                if document.get("element_id") not in identifiers:
                    continue
                event = GraphEvent.model_validate_json(raw)
                if cast(bytes | None, record.key) != event.element_id.encode():
                    raise BenchmarkContractError("Flink output did not use element_id as Kafka key")
                if event.operation == "upsert":
                    if event.element is None:
                        raise BenchmarkContractError("upsert output has no element")
                    observed[event.element_id] = event.element
                else:
                    observed.pop(event.element_id, None)
                count += 1
        return count

    def offsets() -> OffsetSample:
        partitions = [
            TopicPartition(topic, number)
            for topic in (INPUT_TOPIC, OUTPUT_TOPIC)
            for number in consumer.partitions_for_topic(topic) or ()
        ]
        ends = consumer.end_offsets(partitions)
        input_end = sum(offset for partition, offset in ends.items() if partition.topic == INPUT_TOPIC)
        output_end = sum(offset for partition, offset in ends.items() if partition.topic == OUTPUT_TOPIC)
        inputs = admin.list_consumer_group_offsets("graph-element-engine")
        outputs = admin.list_consumer_group_offsets("servicegraph-arangodb-indexer")
        input_committed = sum(item.offset for partition, item in inputs.items() if partition.topic == INPUT_TOPIC)
        indexer_committed = sum(item.offset for partition, item in outputs.items() if partition.topic == OUTPUT_TOPIC)
        return {
            "input_end": input_end,
            "input_committed": input_committed,
            "input_committed_lag": max(input_end - input_committed, 0),
            "output_end": output_end,
            "indexer_committed": indexer_committed,
            "indexer_committed_lag": max(output_end - indexer_committed, 0),
        }

    try:
        # Assignment and initial history scan establish that this dataset has a fresh lifetime.
        deadline = time.monotonic() + config.timeout_seconds
        while not consumer.assignment():
            consumer.poll(timeout_ms=1000)
            if time.monotonic() >= deadline:
                raise BenchmarkContractError("could not assign output partitions")
        consumer.seek_to_beginning()
        deadline = time.monotonic() + config.timeout_seconds
        while True:
            initial = drain()
            if initial:
                raise BenchmarkContractError("dataset already appears in this output topic; use fresh benchmark state")
            end = consumer.end_offsets(list(consumer.assignment()))
            if all((consumer.position(partition) or 0) >= offset for partition, offset in end.items()):
                break
            if time.monotonic() >= deadline:
                raise BenchmarkContractError("could not scan output history before measurement")
        overview = _flink_json(environment, "jobs/overview")
        jobs = cast(list[dict[str, object]], overview.get("jobs", []))
        if len(jobs) != 1 or jobs[0].get("state") != "RUNNING":
            raise BenchmarkContractError("expected exactly one RUNNING Flink job")
        job_id = cast(str, jobs[0]["jid"])
        settings = _settings(environment)
        checkpoints = [_checkpoint(environment, job_id)]
        offset_samples = [offsets()]
        warmup_events = 0
        measured_events = 0
        latencies: list[float] = []
        before: list[PodSample] = []
        started = 0.0
        finished = 0.0
        for batch in range(config.warmup + config.iterations):
            if batch == config.warmup:
                before = _pods(environment)
                started = time.perf_counter()
            target = targets[batch]
            batch_started = time.perf_counter()
            # kafka-python's acknowledgement future has no precise callable type metadata.
            producer.send(INPUT_TOPIC, value=wire).get(timeout=30)  # pyright: ignore[reportUnknownMemberType]
            producer.flush(timeout=30)
            emitted = 0
            deadline = time.monotonic() + config.timeout_seconds
            while observed != target:
                emitted += drain()
                if time.monotonic() >= deadline:
                    raise BenchmarkContractError(
                        f"Java output failed workload expectation at batch {batch + 1}; "
                        f"expected hash={elements_hash(target)} actual={elements_hash(observed)}"
                    )
            emitted += drain()
            finished = time.perf_counter()
            if observed != target:
                raise BenchmarkContractError("extra output changed final graph after batch completion")
            if batch < config.warmup:
                warmup_events += emitted
            else:
                measured_events += emitted
                latencies.append((finished - batch_started) * 1000)
            offset_samples.append(offsets())
            sample = _checkpoint(environment, job_id)
            if sample["latest_id"] != checkpoints[-1]["latest_id"] or sample["failed"] != checkpoints[-1]["failed"]:
                checkpoints.append(sample)
        after = _pods(environment)
        checkpoints.append(_checkpoint(environment, job_id))
        checkpoint_start = checkpoints[-1]["completed"]
        deadline = time.monotonic() + config.timeout_seconds
        while checkpoints[-1]["completed"] < checkpoint_start + config.checkpoint_samples:
            sample = _checkpoint(environment, job_id)
            if sample["latest_id"] != checkpoints[-1]["latest_id"]:
                checkpoints.append(sample)
            if time.monotonic() >= deadline:
                raise BenchmarkContractError("no completed checkpoints during distributed measurement")
            if checkpoints[-1]["completed"] < checkpoint_start + config.checkpoint_samples:
                time.sleep(1)
        offset_samples.append(offsets())
        elapsed = finished - started
        datapoints = config.entities * config.contributors * config.iterations
        final_hash = elements_hash(observed)
        expected_hash = elements_hash(expected)
        return {
            "scope": "controlled_distributed_flink_workload",
            "runtime": "java",
            "image": environment.flink_image,
            "config": config.model_dump(),
            "deployment_settings": settings,
            "payload_sha256": hashlib.sha256(wire).hexdigest(),
            "payload_bytes": len(wire),
            "datapoints_per_batch": config.entities * config.contributors,
            "total_measured_datapoints": datapoints,
            "measured_seconds": elapsed,
            "datapoints_per_second": datapoints / elapsed,
            "batch_completion_latency_ms": _latencies(latencies),
            "output_events_warmup": warmup_events,
            "output_events_measured": measured_events,
            "expected_events_measured": 0 if config.warmup else len(expected),
            "final_element_count": len(observed),
            "final_elements_sha256": final_hash,
            "expected_elements_sha256": expected_hash,
            "expected_matches": final_hash == expected_hash,
            "pods_before": before,
            "pods_after": after,
            "average_cpu_cores": cpu_cores(before, after),
            "checkpoints": checkpoints,
            "offsets": offset_samples,
            "limitations": [
                "Controlled batched workload; observed rate is not a saturating throughput/capacity claim.",
                "Kafka metrics to Kafka graph payloads; excludes Collector/Gremlin latency.",
                "Batch completion includes producer acknowledgement and polling; not per-event latency.",
                "CPU includes all container processes; memory.current includes file cache/RocksDB.",
                "Anonymous resident cgroup memory is a process RSS proxy, not JVM-only RSS or heap usage.",
                "Reported lag uses checkpoint-committed offsets and can lag records already processed.",
                "Short local runs do not establish private-network storage reliability or six-hour stability.",
            ],
        }
    finally:
        producer.close(timeout=10)
        consumer.close()
        admin.close()


def main(arguments: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--entities", type=int, default=16)
    parser.add_argument("--contributors", type=int, default=4)
    parser.add_argument("--iterations", type=int, default=3)
    parser.add_argument("--warmup", type=int, default=1)
    args = parser.parse_args(arguments)
    output = cast(Path, args.output).resolve()
    output.mkdir(parents=True, exist_ok=True)
    config = WorkloadConfig(
        entities=cast(int, args.entities),
        contributors=cast(int, args.contributors),
        iterations=cast(int, args.iterations),
        warmup=cast(int, args.warmup),
    )
    environment = DemoEnvironment(
        root=Path(__file__).resolve().parents[1], work_dir=output / "environment", with_ingest_pipeline=True
    )
    try:
        environment.provision()
        report = measure(environment, config)
        path = output / "java.json"
        path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        if not report["expected_matches"] or report["output_events_measured"] != report["expected_events_measured"]:
            raise BenchmarkContractError("workload graph or output event count failed; inspect java.json")
        print(f"Controlled native Java measurements: {path}")
    finally:
        environment.cleanup()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
