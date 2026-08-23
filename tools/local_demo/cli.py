"""Cross-platform command line orchestration for the persistent local demo."""

# gremlin-python's traversal API is intentionally dynamic.
# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Final, Literal, cast

from extended_otel_semconv import SemanticEdge, SemanticEntity
from tools.local_demo.environment import DemoEnvironment, DemoEnvironmentError, EnvironmentStatus, wait_for
from tools.local_demo.sample import demo_events

CLUSTER_NAME: Final = "servicegraph-local-demo"
NAMESPACE: Final = "servicegraph-local-demo"
STATE_VERSION: Final = 1
type Command = Literal["up", "status", "query", "down"]


@dataclass(frozen=True)
class LocalDemoState:
    version: int
    cluster_name: str
    namespace: str


def repository_root() -> Path:
    return Path(__file__).resolve().parents[2]


def state_directory(root: Path) -> Path:
    return root / ".tmp" / "local-demo"


def make_environment(root: Path) -> DemoEnvironment:
    return DemoEnvironment(
        root=root,
        work_dir=state_directory(root),
        cluster_name=CLUSTER_NAME,
        namespace=NAMESPACE,
        resource_suffix="demo",
        image_tag_prefix="local",
        announce_prefix="servicegraph-demo",
    )


def up(environment: DemoEnvironment, state_path: Path) -> None:
    environment.check_prerequisites()
    current = environment.status()
    if current.ready:
        save_state(state_path, environment)
        print("The local demo is already running.")
        _print_next_steps()
        return
    if current.exists:
        raise DemoEnvironmentError(
            "the dedicated local demo environment exists but is not healthy. "
            "Run 'python -m tools.local_demo down' and then retry 'up'."
        )

    try:
        environment.provision()
        environment.produce_events(demo_events())
        wait_for("local demo graph", 60, lambda: _graph_has_demo_data(environment))
        save_state(state_path, environment)
    except Exception:
        try:
            diagnostics = environment.diagnostics()
            if diagnostics:
                print(f"\nLocal demo diagnostics:\n{diagnostics}", file=sys.stderr)
        except Exception as diagnostics_error:
            print(f"local demo diagnostics failed: {diagnostics_error}", file=sys.stderr)
        try:
            environment.cleanup()
        except Exception as cleanup_error:
            print(f"local demo cleanup failed: {cleanup_error}", file=sys.stderr)
        state_path.unlink(missing_ok=True)
        raise
    finally:
        environment.close()

    print("Local demo is running with six services and five dependency edges.")
    _print_next_steps()


def status(environment: DemoEnvironment, state_path: Path) -> EnvironmentStatus:
    environment.check_prerequisites(require_helm=False, require_kafka=False, require_gremlin=False)
    current = environment.status()
    payload = {"managed_state": load_state(state_path) is not None, **asdict(current), "ready": current.ready}
    print(json.dumps(payload, indent=2, sort_keys=True))
    return current


def query(environment: DemoEnvironment) -> list[dict[str, object]]:
    environment.check_prerequisites(require_helm=False, require_kafka=False)
    current = environment.status()
    if not current.ready:
        raise DemoEnvironmentError("the local demo is not ready. Run 'python -m tools.local_demo up' first.")
    try:
        environment.start_query_access()
        with environment.semantic_client() as client:
            elements = [*client.query(lambda g: g.V()), *client.query(lambda g: g.E())]
        result = sorted((_serialize_element(element) for element in elements), key=lambda item: str(item["id"]))
        print(json.dumps(result, indent=2, sort_keys=True))
        return result
    finally:
        environment.close()


def down(environment: DemoEnvironment, state_path: Path) -> None:
    environment.check_prerequisites(
        require_helm=False,
        require_kubectl=False,
        require_kafka=False,
        require_gremlin=False,
    )
    environment.cleanup()
    state_path.unlink(missing_ok=True)
    if state_path.parent.exists():
        shutil.rmtree(state_path.parent)
    print("Local demo resources were removed. Unrelated Docker containers and Kind clusters were left untouched.")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m tools.local_demo",
        description="Run the service graph projection demo in a dedicated Kind cluster.",
    )
    parser.add_argument("command", choices=("up", "status", "query", "down"))
    args = parser.parse_args(argv)
    command = cast(Command, args.command)
    root = repository_root()
    environment = make_environment(root)
    state_path = state_directory(root) / "state.json"
    try:
        match command:
            case "up":
                up(environment, state_path)
            case "status":
                return 0 if status(environment, state_path).ready else 1
            case "query":
                query(environment)
            case "down":
                down(environment, state_path)
    except (DemoEnvironmentError, RuntimeError, AssertionError, OSError, subprocess.SubprocessError) as error:
        print(f"local demo failed: {error}", file=sys.stderr)
        return 2
    return 0


def _graph_has_demo_data(environment: DemoEnvironment) -> bool:
    with environment.graph() as graph:
        vertex_count = cast(int, graph.V().has_label("service").count().next())
        edge_count = cast(int, graph.E().count().next())
        return vertex_count >= 6 and edge_count >= 5


def _serialize_element(element: SemanticEntity | SemanticEdge) -> dict[str, object]:
    if isinstance(element, SemanticEntity):
        return {
            "model": type(element).__name__,
            "kind": "node",
            "id": element.entity_id,
            "type": element.entity_type,
            "attributes": element.semantic_attributes(),
        }
    return {
        "model": type(element).__name__,
        "kind": "edge",
        "id": element.edge_id,
        "type": element.relationship_type,
        "source_id": element.source_id,
        "target_id": element.target_id,
        "attributes": dict(element.attributes),
        "metrics": dict(element.metrics),
    }


def save_state(path: Path, environment: DemoEnvironment) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    state = LocalDemoState(
        version=STATE_VERSION, cluster_name=environment.cluster_name, namespace=environment.namespace
    )
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(asdict(state), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(path)


def load_state(path: Path) -> LocalDemoState | None:
    if not path.exists():
        return None
    try:
        value: object = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(value, dict):
        return None
    data = cast(dict[object, object], value)
    if set(data) != {"version", "cluster_name", "namespace"}:
        return None
    version = data["version"]
    cluster_name = data["cluster_name"]
    namespace = data["namespace"]
    if (
        isinstance(version, bool)
        or not isinstance(version, int)
        or not isinstance(cluster_name, str)
        or not isinstance(namespace, str)
    ):
        return None
    state = LocalDemoState(version=version, cluster_name=cluster_name, namespace=namespace)
    if state != LocalDemoState(version=STATE_VERSION, cluster_name=CLUSTER_NAME, namespace=NAMESPACE):
        return None
    return state


def _print_next_steps() -> None:
    print("Inspect it with:")
    print("  python -m tools.local_demo status")
    print("  python -m tools.local_demo query")
    print("Stop it when finished:")
    print("  python -m tools.local_demo down")
