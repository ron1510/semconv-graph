from __future__ import annotations

import pytest

from benchmarks.flink import (
    PodSample,
    cpu_cores,
    elements_hash,
    expected_elements,
    parse_counter_file,
)
from benchmarks.servicegraph import BenchmarkContractError, DatasetConfig, generate_otlp_proto


def test_workload_expectation_is_stable_when_evidence_magnitude_is_discarded() -> None:
    payload = generate_otlp_proto(DatasetConfig(entities=3, contributors=2, seed=7))
    first = expected_elements(payload, 1)
    repeated = expected_elements(payload, 3)
    assert len(first) == len(repeated) == 6
    assert first["service:service-000000"]["attributes"] == {
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


@pytest.mark.parametrize(
    "after", [[_sample("replacement", 100, 3_000_000_000)], [_sample("worker", 1, 3_000_000_000)], []]
)
def test_pod_replacement_or_counter_reset_cannot_be_claimed_as_cpu_improvement(after: list[PodSample]) -> None:
    with pytest.raises(BenchmarkContractError):
        cpu_cores([_sample("worker", 10_000, 1_000_000_000)], after)
