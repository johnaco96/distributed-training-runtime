"""End-to-end torch.distributed integration: coordinator + launcher + DDP.

Gated behind ``DTRUNTIME_RUN_TORCH_INTEGRATION=1`` because it builds and runs
the Rust coordinator and spawns real multi-process gloo training.

Covered here:
- launcher registers the run and spawns workers,
- workers rendezvous through the coordinator, receive coordinator-assigned
  ranks, and train a DDP model with real gradient all-reduce,
- a crashed worker is restarted by the launcher, re-joins the rendezvous, and
  the surviving worker re-rendezvouses (rank replacement + elastic recovery).
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import time
import uuid
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
EXAMPLE = REPO_ROOT / "python" / "examples" / "pytorch_ddp_train.py"


def _gate() -> None:
    if os.environ.get("DTRUNTIME_RUN_TORCH_INTEGRATION") != "1":
        pytest.skip("set DTRUNTIME_RUN_TORCH_INTEGRATION=1 to run torch integration tests")
    pytest.importorskip("torch", reason="torch is required for this integration test")
    pytest.importorskip("grpc", reason="grpcio is required for this integration test")


def _free_tcp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _start_coordinator(
    log_dir: Path, extra_args: list[str] | None = None
) -> tuple[subprocess.Popen, str]:
    address = f"127.0.0.1:{_free_tcp_port()}"
    process = _restart_coordinator(log_dir, address, extra_args)
    return process, address


def _restart_coordinator(
    log_dir: Path, address: str, extra_args: list[str] | None = None
) -> subprocess.Popen:
    # Coordinator output goes to files, never PIPE: these tests keep the
    # coordinator running for minutes under polling load, and an undrained
    # 64 KB pipe buffer would eventually block its logging (and with it every
    # RPC handler).
    stdout = (log_dir / "coordinator.stdout.log").open("a", encoding="utf-8")
    stderr = (log_dir / "coordinator.stderr.log").open("a", encoding="utf-8")
    process = subprocess.Popen(
        [
            "cargo", "run", "-p", "runtime-coordinator", "--",
            "--addr", address,
            *(extra_args or []),
        ],
        cwd=REPO_ROOT,
        stdout=stdout,
        stderr=stderr,
    )
    import grpc

    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        if process.poll() is not None:
            pytest.fail(
                "coordinator exited early\n"
                f"stderr:\n{(log_dir / 'coordinator.stderr.log').read_text(encoding='utf-8')[-4000:]}"
            )
        channel = grpc.insecure_channel(address)
        try:
            grpc.channel_ready_future(channel).result(timeout=1)
            return process
        except Exception:
            pass
        finally:
            channel.close()
    pytest.fail(f"coordinator was not reachable at {address}")


def _stop(process: subprocess.Popen) -> None:
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=10)


def _run_launcher(
    address: str,
    run_name: str,
    test_dir: Path,
    extra_env: dict[str, str] | None = None,
    steps: int = 15,
    timeout: float = 600.0,
) -> subprocess.CompletedProcess:
    env = dict(os.environ)
    env.update(
        {
            "PYTHONPATH": str(REPO_ROOT / "python"),
            "DTRUNTIME_TEST_DIR": str(test_dir),
            "DTRUNTIME_HEARTBEAT_INTERVAL": "1",
            "DTRUNTIME_TEST_GRACE": "120",
            # Keep everything on loopback so hostname resolution quirks on CI
            # machines cannot break store connectivity.
            "DTRUNTIME_WORKER_HOST": "127.0.0.1",
        }
    )
    if extra_env:
        env.update(extra_env)
    command = [
        sys.executable,
        "-m",
        "dtruntime.launch",
        "--coordinator", address,
        "--run-name", run_name,
        "--nproc-per-node", "2",
        "--world-size", "2",
        "--backend", "gloo",
        "--max-restarts", "3",
        "--worker-host", "127.0.0.1",
        str(EXAMPLE),
        "--steps", str(steps),
    ]
    return subprocess.run(
        command,
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def _load_reports(test_dir: Path) -> list[dict]:
    return [
        json.loads(path.read_text(encoding="utf-8"))
        for path in sorted(test_dir.glob("report-*.json"))
    ]


def test_ddp_training_via_launcher_completes(tmp_path: Path) -> None:
    _gate()
    coordinator, address = _start_coordinator(tmp_path)
    try:
        result = _run_launcher(address, f"ddp-{uuid.uuid4().hex[:8]}", tmp_path)
        assert result.returncode == 0, (
            f"launcher failed\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )

        reports = _load_reports(tmp_path)
        assert len(reports) == 2, f"expected 2 worker reports, got {reports}"
        assert sorted(r["rank"] for r in reports) == [0, 1]
        assert all(r["world_size"] == 2 for r in reports)
        # DDP kept the replicas synchronised: parameter sums agree across ranks.
        assert all(r["param_sum_spread"] < 1e-5 for r in reports)
        # Both workers observed the same rendezvous round.
        assert len({r["round"] for r in reports}) == 1
    finally:
        _stop(coordinator)


def test_cancel_run_stops_training_without_restart(tmp_path: Path) -> None:
    """Admin cancellation: workers observe RUN_STATE_CANCELLED via heartbeat,
    exit, and the launcher stops without burning its restart budget."""
    _gate()
    import grpc  # noqa: F401 - gated above

    from dtruntime import RuntimeClient
    from dtruntime.generated import runtime_pb2

    coordinator, address = _start_coordinator(tmp_path)
    try:
        run_name = f"ddp-cancel-{uuid.uuid4().hex[:8]}"
        admin = RuntimeClient(address)
        run_id = admin.register_run(
            run_name=run_name,
            world_size=2,
            assignment_policy=runtime_pb2.ASSIGNMENT_POLICY_STATIC_RANK,
            storage_backend_kind=runtime_pb2.STORAGE_BACKEND_KIND_LOCAL_FILESYSTEM,
            idempotency_key=f"launch-{run_name}",
        ).run_id.value

        env = dict(os.environ)
        env.update(
            {
                "PYTHONPATH": str(REPO_ROOT / "python"),
                "DTRUNTIME_TEST_DIR": str(tmp_path),
                "DTRUNTIME_HEARTBEAT_INTERVAL": "1",
                "DTRUNTIME_WORKER_HOST": "127.0.0.1",
                # ~0.1s per step x 3000 steps: training stays busy until cancelled.
                "DTRUNTIME_TEST_STEP_DELAY": "0.1",
            }
        )
        launcher = subprocess.Popen(
            [
                sys.executable, "-m", "dtruntime.launch",
                "--coordinator", address,
                "--run-name", run_name,
                "--run-id", run_id,
                "--nproc-per-node", "2",
                "--world-size", "2",
                "--backend", "gloo",
                "--worker-host", "127.0.0.1",
                str(EXAMPLE), "--steps", "3000",
            ],
            cwd=REPO_ROOT,
            env=env,
        )

        # Give the workers time to rendezvous and start stepping, then cancel.
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            status = admin.get_run_status(run_id)
            if status.state == runtime_pb2.RUN_STATE_RUNNING:
                break
            assert launcher.poll() is None, "launcher exited before training started"
            time.sleep(1)
        else:
            pytest.fail("run never reached RUNNING")

        admin.cancel_run(run_id, reason="integration test")

        code = launcher.wait(timeout=120)
        assert code == 3, f"launcher must exit 3 for a cancelled run, got {code}"
        status = admin.get_run_status(run_id)
        assert status.state == runtime_pb2.RUN_STATE_CANCELLED
        assert "integration test" in status.state_reason
        admin.close()
    finally:
        _stop(coordinator)


def test_coordinator_restart_mid_training_is_transparent(tmp_path: Path) -> None:
    """Coordinator failover semantics: kill the coordinator mid-training and
    restart it on the same metadata DB.  The process group is independent of
    the coordinator, worker heartbeats retry through the outage, run/worker
    state rehydrates from the store, and training completes normally."""
    _gate()
    from dtruntime import RuntimeClient
    from dtruntime.generated import runtime_pb2

    db_path = tmp_path / "coordinator.db"
    coordinator, address = _start_coordinator(
        tmp_path, extra_args=["--metadata-db", str(db_path)]
    )
    launcher = None
    try:
        run_name = f"ddp-failover-{uuid.uuid4().hex[:8]}"
        env = dict(os.environ)
        env.update(
            {
                "PYTHONPATH": str(REPO_ROOT / "python"),
                "DTRUNTIME_TEST_DIR": str(tmp_path),
                "DTRUNTIME_HEARTBEAT_INTERVAL": "1",
                "DTRUNTIME_WORKER_HOST": "127.0.0.1",
                # ~40s of training: enough to cover the coordinator outage.
                "DTRUNTIME_TEST_STEP_DELAY": "0.1",
            }
        )
        launcher = subprocess.Popen(
            [
                sys.executable, "-m", "dtruntime.launch",
                "--coordinator", address,
                "--run-name", run_name,
                "--nproc-per-node", "2",
                "--world-size", "2",
                "--backend", "gloo",
                "--worker-host", "127.0.0.1",
                str(EXAMPLE), "--steps", "400",
            ],
            cwd=REPO_ROOT,
            env=env,
        )

        admin = RuntimeClient(address)
        deadline = time.monotonic() + 120
        run_id = None
        while time.monotonic() < deadline:
            try:
                run_id = admin.register_run(
                    run_name=run_name,
                    world_size=2,
                    assignment_policy=runtime_pb2.ASSIGNMENT_POLICY_STATIC_RANK,
                    storage_backend_kind=runtime_pb2.STORAGE_BACKEND_KIND_LOCAL_FILESYSTEM,
                    idempotency_key=f"launch-{run_name}",
                ).run_id.value
                if admin.get_run_status(run_id).state == runtime_pb2.RUN_STATE_RUNNING:
                    break
            except Exception:
                pass
            time.sleep(1)
        else:
            pytest.fail("run never reached RUNNING")
        admin.close()

        # Hard-kill the coordinator mid-training and restart on the same DB
        # and address.
        coordinator.kill()
        coordinator.wait(timeout=30)
        time.sleep(3)
        coordinator = _restart_coordinator(
            tmp_path, address, extra_args=["--metadata-db", str(db_path)]
        )

        code = launcher.wait(timeout=300)
        assert code == 0, "training must survive a coordinator restart"

        reports = _load_reports(tmp_path)
        assert len(reports) == 2
        assert all(r["param_sum_spread"] < 1e-5 for r in reports)

        # The rehydrated run reached COMPLETED via graceful deregistration.
        admin = RuntimeClient(address)
        status = admin.get_run_status(run_id)
        assert status.state == runtime_pb2.RUN_STATE_COMPLETED, status.state_reason
        admin.close()
    finally:
        if launcher is not None and launcher.poll() is None:
            launcher.kill()
        _stop(coordinator)


def test_worker_crash_restart_and_rerendezvous(tmp_path: Path) -> None:
    _gate()
    coordinator, address = _start_coordinator(tmp_path)
    try:
        result = _run_launcher(
            address,
            f"ddp-crash-{uuid.uuid4().hex[:8]}",
            tmp_path,
            extra_env={
                "DTRUNTIME_TEST_FAIL_RANK": "1",
                "DTRUNTIME_TEST_FAIL_STEP": "5",
            },
        )
        assert result.returncode == 0, (
            f"launcher failed\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )

        crash_markers = list(tmp_path.glob("crashed-*"))
        assert crash_markers, "the fail hook never fired; test exercised nothing"

        reports = _load_reports(tmp_path)
        assert len(reports) == 2, f"expected 2 worker reports, got {reports}"
        assert sorted(r["rank"] for r in reports) == [0, 1]
        assert all(r["world_size"] == 2 for r in reports)
        assert all(r["param_sum_spread"] < 1e-5 for r in reports)
        # Everyone finished on the same (post-crash) rendezvous round.
        assert len({r["round"] for r in reports}) == 1
    finally:
        _stop(coordinator)
