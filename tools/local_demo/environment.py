# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportMissingTypeStubs=false

from __future__ import annotations

import base64
import importlib.util
import json
import os
import shutil
import socket
import subprocess
import time
import urllib.error
import urllib.request
from collections.abc import Callable, Generator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, cast
from uuid import uuid4

if TYPE_CHECKING:
    from gremlin_python.process.graph_traversal import GraphTraversalSource

    from extended_otel_semconv.gremlin import SemanticGremlinClient

KIND_NODE_IMAGE = "kindest/node:v1.32.2"
REDPANDA_IMAGE = "docker.redpanda.com/redpandadata/redpanda:v26.1.6"
ARANGODB_IMAGE = "arangodb:3.12.9.4"
NAMESPACE = "servicegraph-e2e"
ARANGO_ROOT_PASSWORD = "servicegraph-e2e-root"
ARANGO_READER_PASSWORD = "servicegraph-e2e-reader"


class DemoEnvironmentError(RuntimeError):
    """Raised when the local environment cannot be operated safely."""


@dataclass(frozen=True)
class EnvironmentStatus:
    cluster: bool
    arangodb: bool
    redpanda: bool
    indexer: bool
    gremlin: bool

    @property
    def ready(self) -> bool:
        return all((self.cluster, self.arangodb, self.redpanda, self.indexer, self.gremlin))

    @property
    def exists(self) -> bool:
        return any((self.cluster, self.arangodb, self.redpanda))


@dataclass
class PortForward:
    process: subprocess.Popen[str]
    local_port: int

    def close(self) -> None:
        if self.process.poll() is not None:
            return
        self.process.terminate()
        try:
            self.process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait(timeout=5)


@dataclass
class DemoEnvironment:
    root: Path
    work_dir: Path
    cluster_name: str = field(default_factory=lambda: f"servicegraph-e2e-{uuid4().hex[:8]}")
    namespace: str = NAMESPACE
    resource_suffix: str | None = None
    image_tag_prefix: str = "e2e"
    announce_prefix: str = "servicegraph-e2e"
    arango_host_url: str | None = field(default=None, init=False)
    kafka_host_address: str | None = field(default=None, init=False)
    gremlin_forward: PortForward | None = field(default=None, init=False)

    @property
    def kubeconfig(self) -> Path:
        return self.work_dir / "kubeconfig"

    @property
    def suffix(self) -> str:
        return self.resource_suffix or self.cluster_name.removeprefix("servicegraph-e2e-")

    @property
    def indexer_image(self) -> str:
        return f"extended-otel-servicegraph-indexer:{self.image_tag}"

    @property
    def gremlin_image(self) -> str:
        return f"extended-otel-servicegraph-gremlin:{self.image_tag}"

    @property
    def image_tag(self) -> str:
        return f"{self.image_tag_prefix}-{self.suffix}"

    @property
    def arango_container(self) -> str:
        return f"servicegraph-arango-{self.suffix}"

    @property
    def redpanda_container(self) -> str:
        return f"servicegraph-kafka-{self.suffix}"

    @property
    def command_environment(self) -> dict[str, str]:
        return {**os.environ, "KUBECONFIG": str(self.kubeconfig)}

    def provision(self) -> None:
        self._announce("checking local prerequisites")
        self.check_prerequisites()
        self.work_dir.mkdir(parents=True, exist_ok=True)
        self._announce("building indexer and validated Gremlin runtime images")
        self._build_images()
        self._announce(f"creating Kind cluster {self.cluster_name}")
        self.run(
            [
                "kind",
                "create",
                "cluster",
                "--name",
                self.cluster_name,
                "--image",
                KIND_NODE_IMAGE,
                "--kubeconfig",
                str(self.kubeconfig),
                "--wait",
                "5m",
            ],
            timeout=600,
            include_kubeconfig=False,
        )
        for image in (self.indexer_image, self.gremlin_image):
            self.run(
                ["kind", "load", "docker-image", image, "--name", self.cluster_name],
                timeout=600,
                include_kubeconfig=False,
            )
        self.kubectl("create", "namespace", self.namespace)
        self._announce("starting Docker Redpanda and ArangoDB")
        self._start_arangodb()
        self._start_redpanda()
        self._create_secret("servicegraph-arangodb-writer", "root", ARANGO_ROOT_PASSWORD)
        self._announce("installing topology initializer and indexer")
        self._install_indexer()
        self._create_read_only_user()
        self._create_secret("servicegraph-arangodb-reader", "servicegraph-reader", ARANGO_READER_PASSWORD)
        self._announce("installing read-only Gremlin Server")
        self._install_gremlin()
        self._start_gremlin_forward()
        self._announce("ArangoDB/Gremlin projection environment is ready")

    def status(self) -> EnvironmentStatus:
        cluster = self._cluster_exists()
        return EnvironmentStatus(
            cluster=cluster,
            arangodb=self._container_running(self.arango_container),
            redpanda=self._container_running(self.redpanda_container),
            indexer=cluster and self._deployment_ready("servicegraph-indexer"),
            gremlin=cluster and self._deployment_ready("servicegraph-gremlin"),
        )

    def cleanup(self) -> None:
        self._close_gremlin_forward()
        self.run(
            ["docker", "rm", "--force", self.arango_container, self.redpanda_container],
            timeout=120,
            check=False,
            include_kubeconfig=False,
        )
        self.run(
            ["kind", "delete", "cluster", "--name", self.cluster_name],
            timeout=300,
            check=False,
            include_kubeconfig=False,
        )
        for image in (self.indexer_image, self.gremlin_image):
            self.run(["docker", "image", "rm", image], timeout=120, check=False, include_kubeconfig=False)
        self.kubeconfig.unlink(missing_ok=True)

    def close(self) -> None:
        self._close_gremlin_forward()

    def diagnostics(self) -> str:
        sections: list[str] = []
        if self.kubeconfig.exists():
            for command in (
                ["get", "pods", "-o", "wide"],
                ["get", "jobs"],
                ["get", "events", "--sort-by=.metadata.creationTimestamp"],
                ["logs", "deployment/servicegraph-indexer", "--tail=200"],
                ["logs", "deployment/servicegraph-indexer", "--previous", "--tail=200"],
                ["logs", "deployment/servicegraph-gremlin", "--tail=200"],
            ):
                result = self.kubectl(*command, check=False, timeout=60)
                sections.append(f"$ kubectl {' '.join(command)}\n{result.stdout}{result.stderr}")
        for container in (self.redpanda_container, self.arango_container):
            result = self.run(
                ["docker", "logs", "--tail", "100", container],
                timeout=60,
                check=False,
                include_kubeconfig=False,
            )
            sections.append(f"$ docker logs {container}\n{result.stdout}{result.stderr}")
        return "\n\n".join(sections)

    def produce_events(self, events: Sequence[dict[str, object]]) -> None:
        from kafka import KafkaProducer

        address = self.kafka_host_address
        if address is None:
            raise RuntimeError("Redpanda host address is not initialized")
        producer = KafkaProducer(
            bootstrap_servers=address,
            key_serializer=lambda value: cast(str, value).encode(),
            value_serializer=lambda value: json.dumps(cast(dict[str, object], value), separators=(",", ":")).encode(),
        )
        try:
            for event in events:
                producer.send("graph.elements.events", key=cast(str, event["element_id"]), value=event).get(timeout=30)
            producer.flush(timeout=30)
        finally:
            producer.close(timeout=10)

    def committed_offset(self) -> int:
        from kafka import KafkaAdminClient

        address = self.kafka_host_address
        if address is None:
            raise RuntimeError("Redpanda host address is not initialized")
        admin = KafkaAdminClient(bootstrap_servers=address)
        try:
            offsets = admin.list_consumer_group_offsets("servicegraph-arangodb-indexer")
            return max((metadata.offset for metadata in offsets.values()), default=0)
        finally:
            admin.close()

    @contextmanager
    def graph(self) -> Generator[GraphTraversalSource]:
        from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
        from gremlin_python.process.anonymous_traversal import traversal

        forward = self.gremlin_forward
        if forward is None:
            raise RuntimeError("Gremlin port-forward is not initialized")
        connection = DriverRemoteConnection(f"ws://127.0.0.1:{forward.local_port}/gremlin", "g")
        try:
            yield traversal().with_(connection)
        finally:
            connection.close()

    @contextmanager
    def semantic_client(self) -> Generator[SemanticGremlinClient]:
        from extended_otel_semconv.gremlin import SemanticGremlinClient

        forward = self.gremlin_forward
        if forward is None:
            raise RuntimeError("Gremlin port-forward is not initialized")
        with SemanticGremlinClient(f"ws://127.0.0.1:{forward.local_port}/gremlin") as client:
            yield client

    def restart_projection(self) -> None:
        self._close_gremlin_forward()
        self.kubectl("delete", "pod", "-l", "app.kubernetes.io/name=servicegraph-indexer")
        self.kubectl("delete", "pod", "-l", "app.kubernetes.io/name=servicegraph-gremlin")
        self.kubectl("rollout", "status", "deployment/servicegraph-indexer", "--timeout=180s")
        self.kubectl("rollout", "status", "deployment/servicegraph-gremlin", "--timeout=180s")
        self._start_gremlin_forward()

    def start_query_access(self) -> None:
        if self.gremlin_forward is None:
            self._start_gremlin_forward()

    def kubectl(self, *args: str, timeout: int = 120, check: bool = True) -> subprocess.CompletedProcess[str]:
        return self.run(["kubectl", "--namespace", self.namespace, *args], timeout=timeout, check=check)

    def run(
        self,
        command: Sequence[str],
        *,
        timeout: int,
        check: bool = True,
        include_kubeconfig: bool = True,
    ) -> subprocess.CompletedProcess[str]:
        result = subprocess.run(
            command,
            cwd=self.root,
            env=self.command_environment if include_kubeconfig else os.environ.copy(),
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=timeout,
        )
        if check and result.returncode != 0:
            rendered = subprocess.list2cmdline(list(command))
            raise RuntimeError(f"command failed ({result.returncode}): {rendered}\n{result.stdout}{result.stderr}")
        return result

    def check_prerequisites(
        self,
        *,
        require_helm: bool = True,
        require_kubectl: bool = True,
        require_kafka: bool = True,
        require_gremlin: bool = True,
    ) -> None:
        commands = ["docker", "kind"]
        if require_kubectl:
            commands.append("kubectl")
        if require_helm:
            commands.append("helm")
        missing = [command for command in commands if shutil.which(command) is None]
        if missing:
            raise DemoEnvironmentError(
                "missing local demo prerequisites: "
                f"{', '.join(missing)}. Install Docker, Kind, kubectl, and Helm, then retry."
            )
        modules: list[tuple[str, str]] = []
        if require_gremlin:
            modules.append(("extended-opentelemetry-semconv[gremlin]", "extended_otel_semconv.gremlin"))
        if require_kafka:
            modules.append(("kafka-python-ng", "kafka"))
        missing_modules = [package for package, module in modules if not _module_available(module)]
        if missing_modules:
            raise DemoEnvironmentError(
                "missing Python dependencies: "
                f"{', '.join(missing_modules)}. Install the repository dev dependencies with "
                'python -m pip install -e ".[dev]".'
            )
        result = self.run(
            ["docker", "version"],
            timeout=30,
            check=False,
            include_kubeconfig=False,
        )
        if result.returncode != 0:
            raise DemoEnvironmentError(
                "Docker is installed but its daemon is unavailable. "
                "Start Docker Desktop or the Docker service, then retry.\n"
                f"{result.stderr.strip()}"
            )

    def _cluster_exists(self) -> bool:
        result = self.run(
            ["kind", "get", "clusters"],
            timeout=30,
            check=False,
            include_kubeconfig=False,
        )
        return self.cluster_name in result.stdout.splitlines()

    def _container_running(self, name: str) -> bool:
        result = self.run(
            ["docker", "inspect", "--format", "{{.State.Running}}", name],
            timeout=30,
            check=False,
            include_kubeconfig=False,
        )
        return result.returncode == 0 and result.stdout.strip().lower() == "true"

    def _deployment_ready(self, name: str) -> bool:
        if not self.kubeconfig.exists():
            return False
        result = self.kubectl(
            "get",
            "deployment",
            name,
            "--output=jsonpath={.status.availableReplicas}",
            timeout=30,
            check=False,
        )
        return result.returncode == 0 and result.stdout.strip() not in ("", "0")

    def _build_images(self) -> None:
        pip_arguments: list[str] = []
        for name in ("PIP_INDEX_URL", "PIP_TRUSTED_HOST"):
            if value := os.getenv(name):
                pip_arguments.extend(("--build-arg", f"{name}={value}"))
        self.run(
            [
                "docker",
                "build",
                "--tag",
                self.indexer_image,
                "--file",
                "services/servicegraph-indexer/Dockerfile",
                *pip_arguments,
                ".",
            ],
            timeout=1200,
            include_kubeconfig=False,
        )
        gremlin_arguments: list[str] = []
        for name in ("TINKERPOP_SERVER_URL", "MAVEN_REPOSITORY_URL"):
            if value := os.getenv(name):
                gremlin_arguments.extend(("--build-arg", f"{name}={value}"))
        self.run(
            [
                "docker",
                "build",
                "--tag",
                self.gremlin_image,
                "--file",
                "services/servicegraph-gremlin/Dockerfile",
                *gremlin_arguments,
                "services/servicegraph-gremlin",
            ],
            timeout=1200,
            include_kubeconfig=False,
        )

    def _start_arangodb(self) -> None:
        port = _free_port()
        self.run(
            [
                "docker",
                "run",
                "--detach",
                "--rm",
                "--name",
                self.arango_container,
                "--network",
                "kind",
                "--publish",
                f"127.0.0.1:{port}:8529",
                "--env",
                f"ARANGO_ROOT_PASSWORD={ARANGO_ROOT_PASSWORD}",
                ARANGODB_IMAGE,
            ],
            timeout=180,
            include_kubeconfig=False,
        )
        self.arango_host_url = f"http://127.0.0.1:{port}"
        self._expose_container("servicegraph-arangodb", self.arango_container, 8529)
        wait_for("ArangoDB", 120, self._arango_ready)

    def _arango_ready(self) -> bool:
        try:
            self._arango_request("GET", "/_api/version")
            return True
        except urllib.error.URLError:
            return False

    def _start_redpanda(self) -> None:
        port = _free_port()
        internal_host = f"servicegraph-redpanda.{self.namespace}.svc"
        self.run(
            [
                "docker",
                "run",
                "--detach",
                "--rm",
                "--name",
                self.redpanda_container,
                "--network",
                "kind",
                "--publish",
                f"127.0.0.1:{port}:19092",
                REDPANDA_IMAGE,
                "redpanda",
                "start",
                "--mode",
                "dev-container",
                "--smp",
                "1",
                "--memory",
                "512M",
                "--reserve-memory",
                "0M",
                "--node-id",
                "0",
                "--check=false",
                "--kafka-addr",
                "internal://0.0.0.0:9092,external://0.0.0.0:19092",
                "--advertise-kafka-addr",
                f"internal://{internal_host}:9092,external://127.0.0.1:{port}",
            ],
            timeout=180,
            include_kubeconfig=False,
        )
        self.kafka_host_address = f"127.0.0.1:{port}"
        self._expose_container("servicegraph-redpanda", self.redpanda_container, 9092)
        wait_for("Redpanda", 120, self._create_topic)

    def _create_topic(self) -> bool:
        from kafka import KafkaAdminClient
        from kafka.admin import NewTopic

        address = self.kafka_host_address
        if address is None:
            return False
        try:
            admin = KafkaAdminClient(bootstrap_servers=address, request_timeout_ms=5_000)
            try:
                admin.create_topics(
                    (NewTopic("graph.elements.events", 1, 1, topic_configs={"cleanup.policy": "compact"}),)
                )
            finally:
                admin.close()
            return True
        except Exception:
            return False

    def _expose_container(self, service_name: str, container: str, port: int) -> None:
        address = self.run(
            ["docker", "inspect", "--format", '{{(index .NetworkSettings.Networks "kind").IPAddress}}', container],
            timeout=30,
            include_kubeconfig=False,
        ).stdout.strip()
        self._apply_json(
            {
                "apiVersion": "v1",
                "kind": "Service",
                "metadata": {"name": service_name, "namespace": self.namespace},
                "spec": {"ports": [{"name": "tcp", "port": port, "targetPort": port}]},
            }
        )
        self._apply_json(
            {
                "apiVersion": "discovery.k8s.io/v1",
                "kind": "EndpointSlice",
                "metadata": {
                    "name": service_name,
                    "namespace": self.namespace,
                    "labels": {"kubernetes.io/service-name": service_name},
                },
                "addressType": "IPv4",
                "ports": [{"name": "tcp", "protocol": "TCP", "port": port}],
                "endpoints": [{"addresses": [address]}],
            }
        )

    def _apply_json(self, manifest: dict[str, object]) -> None:
        result = subprocess.run(
            ["kubectl", "apply", "--filename", "-"],
            cwd=self.root,
            env=self.command_environment,
            input=json.dumps(manifest),
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=60,
        )
        if result.returncode != 0:
            raise RuntimeError(f"failed to apply test resource: {result.stdout}{result.stderr}")

    def _create_secret(self, name: str, username: str, password: str) -> None:
        self._apply_json(
            {
                "apiVersion": "v1",
                "kind": "Secret",
                "metadata": {"name": name, "namespace": self.namespace},
                "type": "Opaque",
                "stringData": {"username": username, "password": password},
            }
        )

    def _install_indexer(self) -> None:
        self.run(
            [
                "helm",
                "upgrade",
                "--install",
                "indexer",
                "deploy/helm/servicegraph-indexer",
                "--namespace",
                self.namespace,
                "--set",
                "fullnameOverride=servicegraph-indexer",
                "--set",
                "image.repository=extended-otel-servicegraph-indexer",
                "--set",
                f"image.tag={self.image_tag}",
                "--set",
                "image.pullPolicy=IfNotPresent",
                "--set",
                "arangodb.urls[0]=http://servicegraph-arangodb:8529",
                "--set",
                "arangodb.allowDatabaseCreation=true",
                "--set",
                "arangodb.verifyTls=false",
                "--set",
                "streamContract.kafka.brokers[0]=servicegraph-redpanda:9092",
                "--set",
                "streamContract.kafka.security.protocol=PLAINTEXT",
                "--set",
                "streamContract.kafka.security.existingSecret=",
                "--wait",
                "--timeout",
                "5m",
            ],
            timeout=360,
        )

    def _create_read_only_user(self) -> None:
        try:
            self._arango_request(
                "POST",
                "/_api/user",
                {"user": "servicegraph-reader", "passwd": ARANGO_READER_PASSWORD, "active": True},
            )
        except urllib.error.HTTPError as error:
            if error.code != 409:
                raise
        self._arango_request(
            "PUT",
            "/_api/user/servicegraph-reader/database/servicegraph",
            {"grant": "ro"},
        )
        self._arango_request(
            "PUT",
            "/_api/user/servicegraph-reader/database/servicegraph/TINKERPOP-GRAPH-VARIABLES",
            {"grant": "rw"},
        )

    def _arango_request(self, method: str, path: str, body: dict[str, object] | None = None) -> object:
        if self.arango_host_url is None:
            raise RuntimeError("ArangoDB host URL is not initialized")
        token = base64.b64encode(f"root:{ARANGO_ROOT_PASSWORD}".encode()).decode()
        request = urllib.request.Request(
            f"{self.arango_host_url}{path}",
            data=None if body is None else json.dumps(body).encode(),
            headers={"authorization": f"Basic {token}", "content-type": "application/json"},
            method=method,
        )
        with urllib.request.urlopen(request, timeout=10) as response:
            return json.loads(response.read())

    def _install_gremlin(self) -> None:
        self.run(
            [
                "helm",
                "upgrade",
                "--install",
                "gremlin",
                "deploy/helm/servicegraph-gremlin",
                "--namespace",
                self.namespace,
                "--set",
                "fullnameOverride=servicegraph-gremlin",
                "--set",
                "image.repository=extended-otel-servicegraph-gremlin",
                "--set",
                f"image.tag={self.image_tag}",
                "--set",
                "image.pullPolicy=IfNotPresent",
                "--set",
                "arangodb.host=servicegraph-arangodb",
                "--wait",
                "--timeout",
                "5m",
            ],
            timeout=360,
        )

    def _start_gremlin_forward(self) -> None:
        local_port = _free_port()
        process = subprocess.Popen(
            [
                "kubectl",
                "port-forward",
                "--namespace",
                self.namespace,
                "service/servicegraph-gremlin",
                f"{local_port}:8182",
                "--address",
                "127.0.0.1",
            ],
            cwd=self.root,
            env=self.command_environment,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        self.gremlin_forward = PortForward(process, local_port)

        def ready() -> bool:
            if process.poll() is not None:
                output = process.stdout.read() if process.stdout is not None else ""
                raise RuntimeError(f"Gremlin port-forward failed: {output}")
            try:
                with self.graph() as graph:
                    graph.V().limit(1).count().next()
                return True
            except Exception:
                return False

        wait_for("Gremlin GraphBinary endpoint", 60, ready)

    def _close_gremlin_forward(self) -> None:
        if self.gremlin_forward is not None:
            self.gremlin_forward.close()
            self.gremlin_forward = None

    def _announce(self, message: str) -> None:
        print(f"[{self.announce_prefix}] {message}", flush=True)


def wait_for[T](description: str, timeout_seconds: int, probe: Callable[[], T | None | bool]) -> T:
    deadline = time.monotonic() + timeout_seconds
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            result = probe()
            if result:
                return cast(T, result)
        except Exception as error:
            last_error = error
        time.sleep(2)
    detail = f": {last_error}" if last_error is not None else ""
    raise AssertionError(f"timed out waiting for {description}{detail}")


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def _module_available(module: str) -> bool:
    try:
        return importlib.util.find_spec(module) is not None
    except ModuleNotFoundError:
        return False
