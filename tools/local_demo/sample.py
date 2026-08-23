"""Deterministic graph lifecycle events used by the local demo."""

from __future__ import annotations

import time
from collections.abc import Sequence

from extended_otel_semconv.edges import edge_id


def demo_events(*, observed_at_unix_nano: int | None = None) -> Sequence[dict[str, object]]:
    observed_at = time.time_ns() if observed_at_unix_nano is None else observed_at_unix_nano
    services = (
        ("storefront", "1.4.0"),
        ("checkout", "2.8.1"),
        ("payments", "3.2.0"),
        ("inventory", "1.9.3"),
        ("fraud", "0.7.5"),
        ("ledger", "4.1.2"),
    )
    dependencies = (
        ("storefront", "checkout", 420.0, 3.0),
        ("checkout", "payments", 287.0, 2.0),
        ("checkout", "inventory", 301.0, 0.0),
        ("checkout", "fraud", 276.0, 7.0),
        ("payments", "ledger", 284.0, 1.0),
    )
    events = [
        _upsert(
            f"service:{name}",
            {
                "id": f"service:{name}",
                "kind": "node",
                "type": "service",
                "attributes": {"service.name": name, "service.version": version},
            },
            observed_at,
        )
        for name, version in services
    ]
    events.extend(
        _dependency_event(source, target, requests, failures, observed_at)
        for source, target, requests, failures in dependencies
    )
    return events


def _dependency_event(
    source: str,
    target: str,
    requests: float,
    failures: float,
    observed_at_unix_nano: int,
) -> dict[str, object]:
    source_id = f"service:{source}"
    target_id = f"service:{target}"
    element_id = edge_id(source_id, "calls", target_id)
    return _upsert(
        element_id,
        {
            "id": element_id,
            "kind": "edge",
            "type": "calls",
            "source_id": source_id,
            "target_id": target_id,
            "attributes": {},
            "metrics": {
                "service_graph.request.total": requests,
                "service_graph.request.failed.total": failures,
            },
        },
        observed_at_unix_nano,
    )


def _upsert(
    element_id: str,
    element: dict[str, object],
    observed_at_unix_nano: int,
) -> dict[str, object]:
    event_id = f"local-demo-{element_id}"
    return {
        "schema_version": "2.0",
        "event_id": event_id,
        "event_type": "graph_element_state_changed",
        "operation": "upsert",
        "element_id": element_id,
        "payload_hash": f"hash-{event_id}",
        "observed_at_unix_nano": observed_at_unix_nano,
        "emitted_at_unix_ms": observed_at_unix_nano // 1_000_000,
        "element": element,
    }
