from __future__ import annotations

import sys
from pathlib import Path

import pytest
from dtruntime.launch import LaunchConfig, WorkerSupervisor, parse_args, worker_env


def test_parse_args_fixed_size_defaults() -> None:
    config = parse_args(
        [
            "--coordinator", "localhost:50051",
            "--run-name", "demo",
            "--nproc-per-node", "2",
            "train.py", "--epochs", "3",
        ]
    )
    assert config.coordinator == "localhost:50051"
    assert config.run_name == "demo"
    assert config.nproc_per_node == 2
    assert not config.is_elastic
    assert config.resolved_world_size() == 2
    assert config.script == "train.py"
    assert config.script_args == ["--epochs", "3"]


def test_parse_args_world_size_overrides_nproc() -> None:
    config = parse_args(
        [
            "--coordinator", "c:1",
            "--run-name", "demo",
            "--nproc-per-node", "2",
            "--world-size", "8",
            "train.py",
        ]
    )
    assert config.resolved_world_size() == 8


def test_parse_args_elastic_uses_max_workers_as_world_size() -> None:
    config = parse_args(
        [
            "--coordinator", "c:1",
            "--run-name", "demo",
            "--min-workers", "2",
            "--max-workers", "6",
            "train.py",
        ]
    )
    assert config.is_elastic
    assert config.resolved_world_size() == 6


def test_parse_args_rejects_max_below_min() -> None:
    with pytest.raises(SystemExit):
        parse_args(
            [
                "--coordinator", "c:1",
                "--run-name", "demo",
                "--min-workers", "4",
                "--max-workers", "2",
                "train.py",
            ]
        )


def test_parse_args_rejects_max_without_min() -> None:
    with pytest.raises(SystemExit):
        parse_args(
            ["--coordinator", "c:1", "--run-name", "demo", "--max-workers", "2", "train.py"]
        )


def test_worker_env_injects_coordinator_run_and_worker_identity() -> None:
    config = LaunchConfig(
        coordinator="coord:50051",
        run_name="demo",
        script="train.py",
        nproc_per_node=2,
        node_id="nodeA",
        worker_host="10.1.2.3",
        backend="gloo",
    )
    env = worker_env(config, "run-42", local_rank=1)
    assert env["DTRUNTIME_COORDINATOR_ADDR"] == "coord:50051"
    assert env["DTRUNTIME_RUN_ID"] == "run-42"
    assert env["DTRUNTIME_WORKER_ID"] == "nodeA-1"
    assert env["DTRUNTIME_LOCAL_RANK"] == "1"
    assert env["LOCAL_RANK"] == "1"
    assert env["DTRUNTIME_WORKER_HOST"] == "10.1.2.3"
    assert env["DTRUNTIME_TORCH_BACKEND"] == "gloo"


def _write_script(path: Path, body: str) -> str:
    path.write_text(body, encoding="utf-8")
    return str(path)


def _supervisor(tmp_path: Path, script_body: str, max_restarts: int = 3) -> WorkerSupervisor:
    script = _write_script(tmp_path / "worker.py", script_body)
    config = LaunchConfig(
        coordinator="unused:1",
        run_name="test",
        script=script,
        nproc_per_node=2,
        max_restarts=max_restarts,
        node_id="testnode",
    )
    supervisor = WorkerSupervisor(config, "run-test")
    # Keep these tests hermetic: no coordinator exists, so the pre-restart
    # lifecycle check reports "unknown / live" without touching gRPC.
    supervisor._run_state = lambda: 0
    return supervisor


def test_supervisor_returns_zero_when_all_workers_exit_cleanly(tmp_path: Path) -> None:
    supervisor = _supervisor(tmp_path, "import sys; sys.exit(0)\n")
    supervisor.start()
    assert supervisor.supervise() == 0


def test_supervisor_restarts_failed_worker_until_it_succeeds(tmp_path: Path) -> None:
    # The worker fails on its first attempt (marker file absent) and succeeds
    # after the supervisor restarts it.
    marker = tmp_path / "attempted-{rank}"
    body = f"""
import os, sys
marker = r"{marker}".format(rank=os.environ["DTRUNTIME_LOCAL_RANK"])
if not os.path.exists(marker):
    open(marker, "w").close()
    sys.exit(1)
sys.exit(0)
"""
    supervisor = _supervisor(tmp_path, body, max_restarts=4)
    supervisor.start()
    assert supervisor.supervise() == 0


def test_supervisor_fails_when_restart_budget_is_exhausted(tmp_path: Path) -> None:
    supervisor = _supervisor(tmp_path, "import sys; sys.exit(2)\n", max_restarts=1)
    supervisor.start()
    assert supervisor.supervise() == 1


def test_supervisor_uses_current_interpreter(tmp_path: Path) -> None:
    body = f"import sys; sys.exit(0 if sys.executable == {sys.executable!r} else 3)\n"
    supervisor = _supervisor(tmp_path, body)
    supervisor.start()
    assert supervisor.supervise() == 0
