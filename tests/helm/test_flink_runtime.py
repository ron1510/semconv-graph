from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

CHART = Path(__file__).resolve().parents[2] / "deploy" / "helm" / "servicegraph-flink"
HELM = shutil.which("helm")
pytestmark = pytest.mark.skipif(HELM is None, reason="Helm CLI is required for submission contract renders")


def render(*settings: str, upgrade: bool = False) -> subprocess.CompletedProcess[str]:
    assert HELM is not None
    command = [HELM, "template", "migration", str(CHART), "--namespace", "servicegraph-system"]
    if upgrade:
        command.append("--is-upgrade")
    for setting in settings:
        command.extend(("--set", setting))
    return subprocess.run(command, check=False, capture_output=True, text=True)


def test_native_submission_preserves_recovery_and_security() -> None:
    result = render(upgrade=True)
    assert result.returncode == 0, result.stderr
    manifests = result.stdout
    assert "--class io.extendedotel.flink.ServiceGraphJob" in manifests
    assert "/opt/flink/usrlib/otel-servicegraph-diff.jar" in manifests
    assert "--pyModule" not in manifests
    assert "exec python" not in manifests
    assert "DeploymentCommands savepoint" in manifests
    assert "DeploymentCommands inspect" in manifests
    assert "DeploymentCommands record-runtime" in manifests
    assert "restore_args+=(--fromSavepoint" in manifests
    assert "--allowNonRestoredState" not in manifests
    assert "00000000000000000000000000000001" in manifests
    assert "high-availability.type: kubernetes" in manifests
    assert "state.backend.type: rocksdb" in manifests
    assert "execution.checkpointing.incremental: true" in manifests
    assert "state.backend.rocksdb.timer-service.factory: ROCKSDB" in manifests
    assert "runAsNonRoot: true" in manifests
    assert "readOnlyRootFilesystem: true" in manifests
    assert "allowPrivilegeEscalation: false" in manifests
    assert "automountServiceAccountToken: false" in manifests
    assert "-Dexecution.checkpointing.storage=filesystem" in manifests
    assert "-Dstate.backend.type=rocksdb" in manifests
    assert "-Dexecution.checkpointing.max-concurrent-checkpoints=1" in manifests


def test_restricted_platform_values_keep_narrow_ha_permissions() -> None:
    result = render(
        "serviceAccount.create=false",
        "serviceAccount.name=platform-flink",
        "rbac.create=false",
        "storage.createClaim=false",
        "storage.existingClaim=platform-state",
        "streamContract.kafka.security.protocol=SASL_PLAINTEXT",
        "podSecurityContext.runAsUser=10001",
        "podSecurityContext.runAsGroup=10001",
        "podSecurityContext.fsGroup=10001",
        upgrade=True,
    )
    assert result.returncode == 0, result.stderr
    assert "kind: Role" not in result.stdout
    assert "kind: PersistentVolumeClaim" not in result.stdout
    assert "serviceAccountName: platform-flink" in result.stdout
    assert "claimName: platform-state" in result.stdout
    assert "runAsUser: 10001" in result.stdout
    assert "fsGroup: 10001" in result.stdout
    assert 'value: "SASL_PLAINTEXT"' in result.stdout


def test_hashmap_omits_disposable_rocksdb_storage() -> None:
    result = render("state.backend=hashmap", "state.incrementalCheckpoints=false")
    assert result.returncode == 0, result.stderr
    assert "state.backend.type: hashmap" in result.stdout
    assert "state.backend.rocksdb" not in result.stdout
    assert "mountPath: /flink-rocksdb" not in result.stdout
    assert "-Dstate.backend.type=hashmap" in result.stdout
    assert "-Dexecution.checkpointing.incremental" not in result.stdout


def test_submission_captures_custom_checkpoint_settings_without_ha_credentials() -> None:
    result = render(
        "job.checkpointTimeoutMs=123000", "job.checkpointMinPauseMs=7000", "job.tolerableFailedCheckpoints=2"
    )
    assert result.returncode == 0, result.stderr
    submitter = result.stdout.split("# Source: servicegraph-flink/templates/submitter-job.yaml", 1)[1]
    assert '-Dexecution.checkpointing.timeout="123000 ms"' in submitter
    assert '-Dexecution.checkpointing.min-pause="7000 ms"' in submitter
    assert "-Dexecution.checkpointing.tolerable-failed-checkpoints=2" in submitter
    assert "automountServiceAccountToken: false" in submitter
    assert "-Dhigh-availability.type" not in submitter


@pytest.mark.parametrize("setting", ["application.runtime=python", "application.runtime=java", "state.backend=hashmap"])
def test_invalid_runtime_or_checkpoint_combination_is_rejected(setting: str) -> None:
    result = render(setting)
    assert result.returncode != 0
    assert "values don't meet the specifications of the schema" in result.stderr
