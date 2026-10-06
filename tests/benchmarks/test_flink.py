from __future__ import annotations

import pytest

from benchmarks.flink import (
    OperatorLoad,
    PodSample,
    cpu_cores,
    elements_hash,
    expected_elements,
    operator_deltas,
    parse_counter_file,
)
from benchmarks.servicegraph import BenchmarkContractError, DatasetConfig, generate_otlp_proto


def test_workload_expectation_is_stable_when_evidence_magnitude_is_discarded() -> None:
    payload = generate_otlp_proto(DatasetConfig(entities=3, contributors=2, seed=7))
    first = expected_elements(payload, 1)
    repeated = expected_elements(payload, 3)
    assert len(first) == len(repeated) == 6
    assert first["node:service:service-000000"]["attributes"] == {
        "service.name": "service-000000",
        "service.version": "version-0001",
    }
    assert first == repeated
    assert all("metrics" not in element for element in repeated.values())
    assert elements_hash(repeated) == elements_hash(dict(reversed(list(repeated.items()))))


def test_protobuf_workload_is_deterministic_and_compact() -> None:
    config = DatasetConfig(entities=16, contributors=4, seed=7)
    first = generate_otlp_proto(config)
    second = generate_otlp_proto(config)
    assert first == second
    assert len(first) > 0
    assert expected_elements(first, 1) == expected_elements(first, 100)


def _sample(pod: str, usage: int, stamp: int) -> PodSample:
    return {
        "pod": pod,
        "role": "taskmanager",
        "cpu_usage_usec": usage,
        "sampled_at_monotonic_ns": stamp,
        "memory_current_bytes": 4096,
        "memory_anonymous_resident_bytes": 2048,
    }


def test_cgroup_cpu_units_and_multiple_workers() -> None:
    before = [_sample("worker-a", 10_000, 1_000_000_000), _sample("worker-b", 20_000, 1_000_000_000)]
    after = [_sample("worker-a", 1_010_000, 3_000_000_000), _sample("worker-b", 2_020_000, 3_000_000_000)]
    assert cpu_cores(before, after) == {"taskmanager": 1.5}
    assert parse_counter_file("usage_usec 123\nuser_usec 100\nsystem_usec 23\n") == {
        "usage_usec": 123,
        "user_usec": 100,
        "system_usec": 23,
    }


def test_operator_counter_deltas_preserve_subtask_skew() -> None:
    before: OperatorLoad = {
        "coalescer": [
            {"subtask": 0, "records_in": 10, "suppressed_unchanged": 5},
            {"subtask": 1, "records_in": 10, "suppressed_unchanged": 4},
        ],
        "lifecycle": [
            {"subtask": 0, "records_in": 3, "busy_time_ms_per_second": 10.0},
            {"subtask": 1, "records_in": 2, "busy_time_ms_per_second": 20.0},
        ],
    }
    after: OperatorLoad = {
        "coalescer": [
            {"subtask": 0, "records_in": 30, "suppressed_unchanged": 20},
            {"subtask": 1, "records_in": 40, "suppressed_unchanged": 24},
        ],
        "lifecycle": [
            {"subtask": 0, "records_in": 5, "busy_time_ms_per_second": 100.0},
            {"subtask": 1, "records_in": 8, "busy_time_ms_per_second": 200.0},
        ],
    }
    assert operator_deltas(before, after) == {
        "coalescer": [
            {"subtask": 0, "records_in": 20, "suppressed_unchanged": 15},
            {"subtask": 1, "records_in": 30, "suppressed_unchanged": 20},
        ],
        "lifecycle": [
            {"subtask": 0, "records_in": 2},
            {"subtask": 1, "records_in": 6},
        ],
    }


@pytest.mark.parametrize(
    "after", [[_sample("replacement", 100, 3_000_000_000)], [_sample("worker", 1, 3_000_000_000)], []]
)
def test_pod_replacement_or_counter_reset_cannot_be_claimed_as_cpu_improvement(after: list[PodSample]) -> None:
    with pytest.raises(BenchmarkContractError):
        cpu_cores([_sample("worker", 10_000, 1_000_000_000)], after)
