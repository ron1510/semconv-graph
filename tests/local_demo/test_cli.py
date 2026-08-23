from __future__ import annotations

import json
from contextlib import nullcontext
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from extended_otel_semconv import Service, ServiceCallsServiceEdge
from tools.local_demo import cli
from tools.local_demo.environment import DemoEnvironment, DemoEnvironmentError, EnvironmentStatus


def _environment() -> MagicMock:
    environment = MagicMock(spec=DemoEnvironment)
    environment.cluster_name = cli.CLUSTER_NAME
    environment.namespace = cli.NAMESPACE
    return environment


def test_up_provisions_seeds_and_persists_state(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    environment = _environment()
    environment.status.return_value = EnvironmentStatus(False, False, False, False, False)
    state_path = tmp_path / "state" / "state.json"

    def graph_has_demo_data(_: DemoEnvironment) -> bool:
        return True

    monkeypatch.setattr(cli, "_graph_has_demo_data", graph_has_demo_data)

    cli.up(environment, state_path)

    environment.provision.assert_called_once_with()
    environment.produce_events.assert_called_once()
    environment.close.assert_called_once_with()
    assert cli.load_state(state_path) == cli.LocalDemoState(
        version=cli.STATE_VERSION,
        cluster_name=cli.CLUSTER_NAME,
        namespace=cli.NAMESPACE,
    )


def test_up_is_idempotent_when_environment_is_ready(tmp_path: Path) -> None:
    environment = _environment()
    environment.status.return_value = EnvironmentStatus(True, True, True, True, True)
    state_path = tmp_path / "state.json"

    cli.up(environment, state_path)

    environment.provision.assert_not_called()
    environment.produce_events.assert_not_called()
    assert cli.load_state(state_path) is not None


def test_up_rejects_partial_owned_environment(tmp_path: Path) -> None:
    environment = _environment()
    environment.status.return_value = EnvironmentStatus(True, False, False, False, False)

    with pytest.raises(DemoEnvironmentError, match="down"):
        cli.up(environment, tmp_path / "state.json")

    environment.provision.assert_not_called()
    environment.cleanup.assert_not_called()


def test_up_cleans_only_owned_environment_after_failure(tmp_path: Path) -> None:
    environment = _environment()
    environment.status.return_value = EnvironmentStatus(False, False, False, False, False)
    environment.provision.side_effect = RuntimeError("kind failed")
    environment.diagnostics.return_value = "diagnostics"
    state_path = tmp_path / "state.json"

    with pytest.raises(RuntimeError, match="kind failed"):
        cli.up(environment, state_path)

    environment.cleanup.assert_called_once_with()
    environment.close.assert_called_once_with()
    assert not state_path.exists()


def test_down_cleans_environment_and_removes_only_dedicated_state(tmp_path: Path) -> None:
    environment = _environment()
    state_path = tmp_path / "local-demo" / "state.json"
    unrelated = tmp_path / "unrelated.txt"
    state_path.parent.mkdir()
    state_path.write_text("{}", encoding="utf-8")
    unrelated.write_text("keep", encoding="utf-8")

    cli.down(environment, state_path)

    environment.cleanup.assert_called_once_with()
    assert not state_path.parent.exists()
    assert unrelated.read_text(encoding="utf-8") == "keep"


def test_status_reports_runtime_and_managed_state(capsys: pytest.CaptureFixture[str], tmp_path: Path) -> None:
    environment = _environment()
    environment.status.return_value = EnvironmentStatus(True, True, True, True, False)
    state_path = tmp_path / "state.json"
    cli.save_state(state_path, environment)

    result = cli.status(environment, state_path)

    assert not result.ready
    assert json.loads(capsys.readouterr().out) == {
        "arangodb": True,
        "cluster": True,
        "gremlin": False,
        "indexer": True,
        "managed_state": True,
        "ready": False,
        "redpanda": True,
    }


def test_invalid_or_foreign_state_is_not_adopted(tmp_path: Path) -> None:
    state_path = tmp_path / "state.json"
    state_path.write_text('{"version": 1, "cluster_name": "other", "namespace": "other"}', encoding="utf-8")

    assert cli.load_state(state_path) is None

    state_path.write_text("not-json", encoding="utf-8")
    assert cli.load_state(state_path) is None


def test_query_requires_ready_environment() -> None:
    environment = _environment()
    environment.status.return_value = EnvironmentStatus(False, False, False, False, False)

    with pytest.raises(DemoEnvironmentError, match="up"):
        cli.query(environment)

    environment.start_query_access.assert_not_called()


def test_query_returns_typed_nodes_and_edges(capsys: pytest.CaptureFixture[str]) -> None:
    environment = _environment()
    environment.status.return_value = EnvironmentStatus(True, True, True, True, True)
    service = Service.model_validate({"service.name": "checkout"})
    edge = ServiceCallsServiceEdge(source_id="service:storefront", target_id="service:checkout")
    client = MagicMock()
    client.query.side_effect = ([service], [edge])
    environment.semantic_client.return_value = nullcontext(client)

    result = cli.query(environment)

    assert {item["kind"] for item in result} == {"node", "edge"}
    assert any(item["id"] == "service:checkout" for item in result)
    assert json.loads(capsys.readouterr().out) == result
    environment.start_query_access.assert_called_once_with()
    environment.close.assert_called_once_with()


def test_main_returns_actionable_failure(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    environment = _environment()

    def fake_environment(_: Path) -> MagicMock:
        return environment

    monkeypatch.setattr(cli, "repository_root", lambda: Path("repo"))
    monkeypatch.setattr(cli, "make_environment", fake_environment)
    environment.check_prerequisites.side_effect = DemoEnvironmentError("install Kind")

    assert cli.main(["status"]) == 2
    assert "install Kind" in capsys.readouterr().err
