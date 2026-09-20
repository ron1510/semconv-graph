"""Deterministic OTLP datasets for native Java Flink measurements."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Final

SCHEMA_VERSION: Final = "1.0"
BASE_TIMESTAMP_UNIX_NANO: Final = 1_800_000_000_000_000_000


@dataclass(frozen=True, slots=True)
class DatasetConfig:
    entities: int = 100
    contributors: int = 3
    seed: int = 20_260_823

    def __post_init__(self) -> None:
        if self.entities <= 0:
            raise ValueError("entities must be greater than zero")
        if self.contributors <= 0:
            raise ValueError("contributors must be greater than zero")


class BenchmarkContractError(RuntimeError):
    """A workload or observation violates the benchmark contract."""


def generate_otlp_proto(
    config: DatasetConfig, *, base_timestamp_unix_nano: int = BASE_TIMESTAMP_UNIX_NANO, service_prefix: str = "service"
) -> bytes:
    """Return deterministic Collector servicegraph OTLP Protobuf for ``config``."""
    services = [f"{service_prefix}-{index:06d}" for index in range(config.entities)]
    ordered_services = sorted(services, key=lambda name: _digest(f"{config.seed}:{name}"))
    points: list[dict[str, object]] = []
    for source_index, client in enumerate(ordered_services):
        server = ordered_services[(source_index + 1) % len(ordered_services)]
        for contributor_index in range(config.contributors):
            token = f"{config.seed}:{client}:{server}:{contributor_index}"
            delta = int(_digest(token)[:8], 16) % 10 + 1
            observed_at = base_timestamp_unix_nano + len(points) * 1_000_000 + abs(config.seed) % 1_000_000
            attributes = {
                "client": client,
                "client_service.version": f"version-{contributor_index:04d}",
                "server": server,
                "server_service.version": f"version-{contributor_index:04d}",
            }
            points.append(
                {
                    "asInt": str(delta),
                    "attributes": [_otlp_attribute(name, value) for name, value in sorted(attributes.items())],
                    "startTimeUnixNano": str(observed_at - 1_000_000),
                    "timeUnixNano": str(observed_at),
                }
            )

    document = {
        "resourceMetrics": [
            {
                "scopeMetrics": [
                    {
                        "metrics": [
                            {
                                "name": "traces_service_graph_request_total",
                                "sum": {
                                    "aggregationTemporality": 1,
                                    "dataPoints": points,
                                    "isMonotonic": True,
                                },
                            }
                        ],
                        "scope": {"name": "servicegraph-benchmark", "version": SCHEMA_VERSION},
                    }
                ]
            }
        ]
    }
    from google.protobuf.json_format import ParseDict
    from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2 import ExportMetricsServiceRequest

    return ParseDict(document, ExportMetricsServiceRequest()).SerializeToString(deterministic=True)


def _otlp_attribute(name: str, value: str) -> dict[str, object]:
    return {"key": name, "value": {"stringValue": value}}


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()
