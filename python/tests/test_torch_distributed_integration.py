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


def _start_coordinator(log_dir: Path) -> tuple[subprocess.Popen, str]:
    port = _free_tcp_port()
    address = f"127.0.0.1:{port}"
    # Coordinator output goes to files, never PIPE: these tests keep the
    # coordinator running for minutes under polling load, and an undrained
    # 64 KB pipe buffer would eventually block its logging (and with it every
    # RPC handler).
    stdout = (log_dir / "coordinator.stdout.log").open("w", encoding="utf-8")
    stderr = (log_dir / "coordinator.stderr.log").open("w", encoding="utf-8")
    process = subprocess.Popen(
        ["cargo", "run", "-p", "runtime-coordinator", "--", "--addr", address],
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
            return process, address
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
