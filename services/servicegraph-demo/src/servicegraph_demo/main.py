from __future__ import annotations

import os
import random
import time
import urllib.request
from dataclasses import dataclass
from functools import cache
from hashlib import sha256
from typing import Final

from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest
from opentelemetry.proto.common.v1.common_pb2 import AnyValue, InstrumentationScope, KeyValue
from opentelemetry.proto.resource.v1.resource_pb2 import Resource
from opentelemetry.proto.trace.v1.trace_pb2 import ResourceSpans, ScopeSpans, Span, Status


@dataclass(frozen=True, slots=True)
class EtlExecution:
    pipeline_id: str
    pipeline_name: str
    run_id: str
    run_name: str
    part_run_id: str
    part_name: str


@dataclass(frozen=True, slots=True)
class Edge:
    client: str
    server: str
    method: str
    route: str
    etl: EtlExecution | None = None


EDGES: Final[tuple[Edge, ...]] = (
    Edge("storefront", "catalog-api", "GET", "/products/{product_id}"),
    Edge("storefront", "recommendations-api", "GET", "/recommendations"),
    Edge("storefront", "search-api", "GET", "/search"),
    Edge("storefront", "identity-api", "POST", "/sessions"),
    Edge("storefront", "checkout-api", "POST", "/checkout"),
    Edge("mobile-api", "identity-api", "POST", "/tokens"),
    Edge("mobile-api", "catalog-api", "GET", "/products/{product_id}"),
    Edge("mobile-api", "orders-api", "GET", "/orders/{order_id}"),
    Edge("checkout-api", "inventory-api", "POST", "/reservations"),
    Edge("checkout-api", "payments-api", "POST", "/payments"),
    Edge("checkout-api", "shipping-api", "POST", "/shipments"),
    Edge("checkout-api", "tax-api", "POST", "/quotes"),
    Edge("orders-api", "orders-worker", "POST", "/jobs/order-confirmation"),
    Edge("orders-worker", "inventory-api", "PATCH", "/stock/{sku}"),
    Edge("orders-worker", "notifications-api", "POST", "/notifications"),
    Edge("payments-api", "fraud-api", "POST", "/checks"),
    Edge("payments-api", "ledger-api", "POST", "/entries"),
    Edge("shipping-api", "notifications-api", "POST", "/notifications"),
    Edge("shipping-api", "warehouse-api", "POST", "/pick-lists"),
    Edge(
        "catalog-worker",
        "catalog-api",
        "PUT",
        "/products/{product_id}",
        EtlExecution(
            pipeline_id="catalog-refresh",
            pipeline_name="Refresh Catalog",
            run_id="demo-run-001",
            run_name="Scheduled catalog refresh",
            part_run_id="load-products",
            part_name="Load products",
        ),
    ),
    Edge("catalog-api", "pricing-api", "GET", "/prices/{sku}"),
    Edge("recommendations-api", "catalog-api", "GET", "/products/batch"),
    Edge("search-api", "catalog-api", "GET", "/products/search-index"),
    Edge("warehouse-api", "inventory-api", "PATCH", "/stock/{sku}"),
)

RUNTIMES: Final[tuple[tuple[str, str, str], ...]] = (
    ("python", "3.12.8", "cpython"),
    ("OpenJDK Runtime Environment", "21.0.5", "java"),
    ("go", "1.23.4", "go"),
    ("node", "22.12.0", "nodejs"),
)


@dataclass(frozen=True, slots=True)
class Config:
    endpoint: str
    emit_interval_seconds: float
    topology_change_interval_seconds: float
    initial_edges: int
    max_active_edges: int
    requests_per_tick: int
    error_rate: float
    namespace: str
    instance_id: str
    random_seed: int | None

    def validate(self) -> None:
        if not self.endpoint.startswith(("http://", "https://")):
            raise ValueError("OTLP_HTTP_ENDPOINT must use http:// or https://")
        if self.emit_interval_seconds <= 0 or self.topology_change_interval_seconds <= 0:
            raise ValueError("demo intervals must be positive")
        if not 1 <= self.initial_edges <= self.max_active_edges < len(EDGES):
            raise ValueError("edge counts must satisfy 1 <= initial <= max < available")
        if self.requests_per_tick < 1:
            raise ValueError("DEMO_REQUESTS_PER_TICK must be positive")
        if not 0 <= self.error_rate <= 1:
            raise ValueError("DEMO_ERROR_RATE must be between 0 and 1")


@cache
def demo_config_from_env() -> Config:
    seed_value = os.getenv("DEMO_RANDOM_SEED", "").strip()
    config = Config(
        endpoint=os.getenv(
            "OTLP_HTTP_ENDPOINT",
            "http://servicegraph-collector-router:4318/v1/traces",
        ),
        emit_interval_seconds=float(os.getenv("DEMO_EMIT_INTERVAL_SECONDS", "2")),
        topology_change_interval_seconds=float(os.getenv("DEMO_TOPOLOGY_CHANGE_INTERVAL_SECONDS", "20")),
        initial_edges=int(os.getenv("DEMO_INITIAL_EDGES", "10")),
        max_active_edges=int(os.getenv("DEMO_MAX_ACTIVE_EDGES", "18")),
        requests_per_tick=int(os.getenv("DEMO_REQUESTS_PER_TICK", "8")),
        error_rate=float(os.getenv("DEMO_ERROR_RATE", "0.08")),
        namespace=os.getenv("DEMO_SERVICE_NAMESPACE", "shop"),
        instance_id=os.getenv("DEMO_INSTANCE_ID", "live-demo"),
        random_seed=int(seed_value) if seed_value else None,
    )
    config.validate()
    return config


class Topology:
    def __init__(
        self,
        edges: tuple[Edge, ...],
        initial_edges: int,
        max_active_edges: int,
        rng: random.Random,
    ) -> None:
        shuffled = list(edges)
        rng.shuffle(shuffled)
        self._rng = rng
        self._max_active_edges = max_active_edges
        self._active = shuffled[:initial_edges]
        self._inactive = shuffled[initial_edges:]
        self._pending = list(self._active)

    @property
    def active(self) -> tuple[Edge, ...]:
        return tuple(self._active)

    def advance(self) -> tuple[Edge | None, Edge]:
        introduced = self._inactive.pop(0)
        retired: Edge | None = None
        if len(self._active) >= self._max_active_edges:
            retired = self._rng.choice(self._active)
            self._active.remove(retired)
            self._inactive.append(retired)

        self._active.append(introduced)
        self._pending.append(introduced)
        return retired, introduced

    def sample(self, count: int) -> tuple[Edge, ...]:
        selected: list[Edge] = []
        while self._pending and len(selected) < count:
            selected.append(self._pending.pop(0))
        selected.extend(self._rng.choice(self._active) for _ in range(count - len(selected)))
        return tuple(selected)


def build_request(
    edges: tuple[Edge, ...],
    *,
    namespace: str,
    instance_id: str,
    error_rate: float,
    rng: random.Random,
) -> ExportTraceServiceRequest:
    request = ExportTraceServiceRequest()
    for edge in edges:
        _add_trace(request, edge, namespace, instance_id, error_rate, rng)
    return request


def _add_trace(
    request: ExportTraceServiceRequest,
    edge: Edge,
    namespace: str,
    instance_id: str,
    error_rate: float,
    rng: random.Random,
) -> None:
    trace_id = rng.randbytes(16)
    client_span_id = rng.randbytes(8)
    server_span_id = rng.randbytes(8)
    start = time.time_ns()
    duration = rng.randint(5, 250) * 1_000_000
    failed = rng.random() < error_rate
    status = Status.STATUS_CODE_ERROR if failed else Status.STATUS_CODE_OK
    interaction_attributes = (
        _attribute("http.request.method", edge.method),
        _attribute("http.route", edge.route),
    )
    client_attributes = interaction_attributes + _etl_span_attributes(edge.etl)

    client_span = Span(
        trace_id=trace_id,
        span_id=client_span_id,
        name=f"{edge.method} {edge.route}",
        kind=Span.SPAN_KIND_CLIENT,
        start_time_unix_nano=start,
        end_time_unix_nano=start + duration,
        attributes=client_attributes,
        status=Status(code=status),
    )
    server_span = Span(
        trace_id=trace_id,
        span_id=server_span_id,
        parent_span_id=client_span_id,
        name=f"{edge.method} {edge.route}",
        kind=Span.SPAN_KIND_SERVER,
        start_time_unix_nano=start + 1_000_000,
        end_time_unix_nano=start + max(duration - 1_000_000, 1),
        attributes=interaction_attributes,
        status=Status(code=status),
    )
    request.resource_spans.extend(
        (
            _resource_spans(
                edge.client,
                namespace,
                instance_id,
                client_span,
                _etl_resource_attributes(edge.etl),
            ),
            _resource_spans(edge.server, namespace, instance_id, server_span),
        )
    )


def _resource_spans(
    service: str,
    namespace: str,
    instance_id: str,
    span: Span,
    extra_attributes: tuple[tuple[str, str], ...] = (),
) -> ResourceSpans:
    attributes = (*service_resource_attributes(service, namespace, instance_id), *extra_attributes)
    return ResourceSpans(
        resource=Resource(attributes=tuple(_attribute(key, value) for key, value in attributes)),
        scope_spans=(
            ScopeSpans(
                scope=InstrumentationScope(name="extended-otel-servicegraph-demo", version="0.1.1"),
                spans=(span,),
            ),
        ),
    )


def _etl_resource_attributes(execution: EtlExecution | None) -> tuple[tuple[str, str], ...]:
    if execution is None:
        return ()
    return (
        ("etl.pipeline.id", execution.pipeline_id),
        ("etl.pipeline.name", execution.pipeline_name),
    )


def _etl_span_attributes(execution: EtlExecution | None) -> tuple[KeyValue, ...]:
    if execution is None:
        return ()
    return (
        _attribute("semconv.graph.discovery", True),
        _attribute("etl.run.id", execution.run_id),
        _attribute("etl.run.name", execution.run_name),
        _attribute("etl.part.run.id", execution.part_run_id),
        _attribute("etl.part.name", execution.part_name),
    )


def service_resource_attributes(
    service: str,
    namespace: str,
    instance_id: str,
) -> tuple[tuple[str, str | int], ...]:
    digest = sha256(service.encode("utf-8")).hexdigest()
    node_number = int(digest[:2], 16) % 3 + 1
    runtime_name, runtime_version, language = RUNTIMES[int(digest[2:4], 16) % len(RUNTIMES)]
    workload_namespace = _workload_namespace(service)
    service_version = f"{int(digest[4:6], 16) % 3 + 1}.{int(digest[6:8], 16) % 10}.0"
    process_id = 10_000 + int(digest[8:12], 16)
    pod_uid = f"pod-{digest[:16]}"

    return (
        ("service.name", service),
        ("service.namespace", namespace),
        ("service.instance.id", f"{service}/{instance_id}"),
        ("service.version", service_version),
        ("service.criticality", _criticality(service)),
        ("k8s.cluster.name", "demo-production"),
        ("k8s.cluster.uid", "cluster-demo-production"),
        ("k8s.namespace.name", workload_namespace),
        ("k8s.node.name", f"worker-{node_number}"),
        ("k8s.node.uid", f"node-demo-{node_number}"),
        ("k8s.deployment.name", service),
        ("k8s.deployment.uid", f"deployment-{digest[:16]}"),
        ("k8s.pod.name", f"{service}-{digest[:6]}"),
        ("k8s.pod.uid", pod_uid),
        ("k8s.pod.hostname", f"{service}-{digest[:6]}"),
        ("k8s.container.name", service),
        ("k8s.container.restart_count", int(digest[12:14], 16) % 3),
        ("k8s.service.name", service),
        ("k8s.service.uid", f"service-{digest[:16]}"),
        ("k8s.service.type", "ClusterIP"),
        ("container.runtime.name", "containerd"),
        ("container.runtime.version", "2.0.1"),
        ("container.runtime.description", "Kubernetes CRI runtime"),
        ("process.pid", process_id),
        ("process.creation.time", "2026-01-01T00:00:00Z"),
        ("process.executable.build_id.htlhash", digest[:16]),
        ("process.executable.name", service),
        ("process.executable.path", f"/app/{service}"),
        ("process.runtime.name", runtime_name),
        ("process.runtime.version", runtime_version),
        ("process.runtime.description", f"{language} service runtime"),
        ("telemetry.sdk.name", "opentelemetry"),
        ("telemetry.sdk.language", language),
        ("telemetry.sdk.version", "1.44.0"),
        ("telemetry.distro.name", "extended-otel-demo"),
        ("telemetry.distro.version", "0.2.0"),
        ("vcs.repository.name", service),
        ("vcs.repository.url.full", f"https://github.example/platform/{service}"),
        ("vcs.ref.head.name", "main"),
        ("vcs.ref.head.revision", digest),
        ("vcs.ref.type", "branch"),
    )


def _workload_namespace(service: str) -> str:
    if service in {"storefront", "mobile-api", "identity-api"}:
        return "experience"
    if service in {"catalog-api", "catalog-worker", "pricing-api", "search-api", "recommendations-api"}:
        return "catalog"
    if service in {"inventory-api", "shipping-api", "warehouse-api"}:
        return "fulfillment"
    return "commerce"


def _criticality(service: str) -> str:
    if service in {"storefront", "checkout-api", "payments-api", "orders-api"}:
        return "critical"
    if service.endswith("-worker"):
        return "low"
    return "high"


def _attribute(key: str, value: str | bool | int) -> KeyValue:
    match value:
        case bool():
            encoded = AnyValue(bool_value=value)
        case int():
            encoded = AnyValue(int_value=value)
        case str():
            encoded = AnyValue(string_value=value)
    return KeyValue(key=key, value=encoded)


def _send(endpoint: str, request: ExportTraceServiceRequest) -> None:
    http_request = urllib.request.Request(
        endpoint,
        data=request.SerializeToString(),
        headers={"content-type": "application/x-protobuf"},
        method="POST",
    )
    with urllib.request.urlopen(http_request, timeout=10) as response:
        response.read()


def _edge_name(edge: Edge) -> str:
    return f"{edge.client}->{edge.server}"


def run(config: Config) -> None:
    rng = random.Random(config.random_seed)
    topology = Topology(EDGES, config.initial_edges, config.max_active_edges, rng)
    next_change = time.monotonic() + config.topology_change_interval_seconds
    print(f"active topology: {', '.join(_edge_name(edge) for edge in topology.active)}", flush=True)

    while True:
        now = time.monotonic()
        if now >= next_change:
            retired, introduced = topology.advance()
            change = f"introduced {_edge_name(introduced)}"
            if retired is not None:
                change += f"; retired {_edge_name(retired)}"
            print(f"{change}; active edges={len(topology.active)}", flush=True)
            next_change = now + config.topology_change_interval_seconds

        edges = topology.sample(config.requests_per_tick)
        request = build_request(
            edges,
            namespace=config.namespace,
            instance_id=config.instance_id,
            error_rate=config.error_rate,
            rng=rng,
        )
        try:
            _send(config.endpoint, request)
        except OSError as exc:
            print(f"failed to export traces to {config.endpoint}: {exc}", flush=True)
        else:
            print(f"exported {len(edges)} traces", flush=True)
        time.sleep(config.emit_interval_seconds)


def main() -> int:
    run(demo_config_from_env())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
