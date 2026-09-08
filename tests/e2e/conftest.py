from __future__ import annotations

from collections.abc import Iterator

import pytest

from tools.local_demo.environment import DemoEnvironment


@pytest.fixture(scope="session")
def e2e_environment(
    pytestconfig: pytest.Config,
    tmp_path_factory: pytest.TempPathFactory,
    request: pytest.FixtureRequest,
) -> Iterator[DemoEnvironment]:
    if not pytestconfig.getoption("--run-e2e"):
        pytest.skip("pass --run-e2e to provision the disposable Kind environment")

    root = pytestconfig.rootpath
    environment = DemoEnvironment(
        root=root,
        work_dir=tmp_path_factory.mktemp("servicegraph-e2e"),
        with_ingest_pipeline=True,
    )
    keep = bool(pytestconfig.getoption("--keep-e2e-cluster"))
    try:
        try:
            environment.provision()
        except Exception:
            print("\nE2E setup diagnostics:\n" + environment.diagnostics())
            raise
        yield environment
    finally:
        if request.session.testsfailed:
            print("\nE2E diagnostics:\n" + environment.diagnostics())
        if keep:
            print(f"\nKept Kind cluster {environment.cluster_name}; KUBECONFIG={environment.kubeconfig}")
        else:
            environment.cleanup()
