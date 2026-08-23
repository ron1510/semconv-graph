# pyright: reportPrivateUsage=false

from __future__ import annotations

import subprocess
from collections.abc import Sequence
from pathlib import Path

import pytest

from tools.local_demo.environment import DemoEnvironment, DemoEnvironmentError
from tools.local_demo.sample import demo_events


def _completed(command: Sequence[str], *, returncode: int = 0, stdout: str = "") -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(command, returncode, stdout, "")


def test_cleanup_targets_only_exact_owned_resources(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    commands: list[list[str]] = []

    def run(command: Sequence[str], **_: object) -> subprocess.CompletedProcess[str]:
        commands.append(list(command))
        return _completed(command)

    monkeypatch.setattr(subprocess, "run", run)
    environment = DemoEnvironment(
        root=tmp_path,
        work_dir=tmp_path / "state",
        cluster_name="servicegraph-local-demo",
        resource_suffix="demo",
        image_tag_prefix="local",
    )
    environment.work_dir.mkdir()
    environment.kubeconfig.touch()

    environment.cleanup()

    assert commands == [
        ["docker", "rm", "--force", "servicegraph-arango-demo", "servicegraph-kafka-demo"],
        ["kind", "delete", "cluster", "--name", "servicegraph-local-demo"],
        ["docker", "image", "rm", "extended-otel-servicegraph-indexer:local-demo"],
        ["docker", "image", "rm", "extended-otel-servicegraph-gremlin:local-demo"],
    ]
    assert all("*" not in part for command in commands for part in command)


def test_status_constructs_kind_docker_and_kubectl_checks(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    commands: list[list[str]] = []

    def run(command: Sequence[str], **_: object) -> subprocess.CompletedProcess[str]:
        command_list = list(command)
        commands.append(command_list)
        if command_list[:3] == ["kind", "get", "clusters"]:
            return _completed(command, stdout="servicegraph-local-demo\nother-cluster\n")
        if command_list[:2] == ["docker", "inspect"]:
            return _completed(command, stdout="true\n")
        if "deployment" in command_list:
            return _completed(command, stdout="1")
        return _completed(command)

    monkeypatch.setattr(subprocess, "run", run)
    environment = DemoEnvironment(
        root=tmp_path,
        work_dir=tmp_path / "state",
        cluster_name="servicegraph-local-demo",
        namespace="servicegraph-local-demo",
        resource_suffix="demo",
    )
    environment.work_dir.mkdir()
    environment.kubeconfig.touch()

    assert environment.status().ready
    assert commands[0] == ["kind", "get", "clusters"]
    assert [command[-1] for command in commands if command[:2] == ["docker", "inspect"]] == [
        "servicegraph-arango-demo",
        "servicegraph-kafka-demo",
    ]
    assert sum("deployment" in command for command in commands) == 2


def test_helm_commands_use_the_built_local_image_tag(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    commands: list[list[str]] = []

    def run(command: Sequence[str], **_: object) -> subprocess.CompletedProcess[str]:
        commands.append(list(command))
        return _completed(command)

    environment = DemoEnvironment(
        root=tmp_path,
        work_dir=tmp_path / "state",
        cluster_name="servicegraph-local-demo",
        namespace="servicegraph-local-demo",
        resource_suffix="demo",
        image_tag_prefix="local",
    )
    monkeypatch.setattr(environment, "run", run)

    environment._install_indexer()
    environment._install_gremlin()

    assert "image.tag=local-demo" in commands[0]
    assert "image.tag=local-demo" in commands[1]
    assert commands[0][:4] == ["helm", "upgrade", "--install", "indexer"]
    assert commands[1][:4] == ["helm", "upgrade", "--install", "gremlin"]


def test_prerequisite_error_names_missing_commands(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    def which(command: str) -> str | None:
        return None if command == "kind" else command

    monkeypatch.setattr("tools.local_demo.environment.shutil.which", which)
    environment = DemoEnvironment(root=tmp_path, work_dir=tmp_path / "state")

    with pytest.raises(DemoEnvironmentError, match="Kind|kind"):
        environment.check_prerequisites(require_kafka=False, require_gremlin=False)


def test_demo_events_are_deterministic_and_complete() -> None:
    first = demo_events(observed_at_unix_nano=123)
    second = demo_events(observed_at_unix_nano=123)

    assert first == second
    assert len(first) == 11
    assert sum(event["element"]["kind"] == "node" for event in first) == 6  # type: ignore[index]
    assert sum(event["element"]["kind"] == "edge" for event in first) == 5  # type: ignore[index]
    assert len({str(event["element_id"]) for event in first}) == 11
