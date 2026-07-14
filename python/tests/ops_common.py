"""Shared helpers for the operational tests (failover / chaos / soak).

These tests exercise the real Rust coordinator (and, for chaos/soak, the
`runtime-bench` load generator). They locate the prebuilt release binaries so a
test run never pays a `cargo` compile; if a binary is missing the caller skips
with a build hint.
"""

from __future__ import annotations

import json
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Optional

REPO_ROOT = Path(__file__).resolve().parents[2]


def _exe(name: str) -> str:
    return f"{name}.exe" if sys.platform == "win32" else name


def release_binary(name: str) -> Optional[Path]:
    """Path to a prebuilt release binary, or None if it is not built."""
    candidate = REPO_ROOT / "target" / "release" / _exe(name)
    return candidate if candidate.exists() else None


def require_binary(name: str):
    import pytest

    path = release_binary(name)
    if path is None:
        pytest.skip(
            f"{name} release binary not found — build it first: "
            f"cargo build --release -p {name}"
        )
    return path


def free_tcp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def start_coordinator(binary: Path, address: str, extra_args: list[str]) -> subprocess.Popen:
    """Spawn the coordinator; caller is responsible for stopping it."""
    return subprocess.Popen(
        [str(binary), "--addr", address, *extra_args],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )


def wait_for_grpc(address: str, process: subprocess.Popen, timeout: float = 60.0) -> None:
    import grpc

    deadline = time.monotonic() + timeout
    last_error: Optional[Exception] = None
    while time.monotonic() < deadline:
        if process.poll() is not None:
            out = process.stdout.read() if process.stdout else ""
            raise AssertionError(
                f"coordinator exited early (code {process.returncode}):\n{out}"
            )
        channel = grpc.insecure_channel(address)
        try:
            grpc.channel_ready_future(channel).result(timeout=1)
            return
        except Exception as exc:  # noqa: BLE001 - server still starting
            last_error = exc
        finally:
            channel.close()
    raise AssertionError(f"coordinator not reachable at {address}: {last_error}")


def hard_kill(process: subprocess.Popen) -> None:
    """Simulate a crash: SIGKILL-equivalent, no graceful shutdown."""
    if process.poll() is not None:
        return
    process.kill()
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        pass


def stop_process(process: subprocess.Popen) -> None:
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=10)


def run_bench_logical_workers(
    bench: Path,
    *,
    workers: int,
    duration_secs: int,
    out_json: Path,
    failure_rate: float = 0.0,
    recover_failed_workers: bool = False,
    checkpoint_every_secs: int = 0,
    heartbeat_interval_ms: int = 1000,
    concurrency: int = 64,
    timeout: float = 600.0,
) -> dict:
    """Run `runtime-bench logical-workers` (it self-manages a coordinator) and
    return the parsed JSON summary."""
    cmd = [
        str(bench),
        "logical-workers",
        "--start-coordinator",
        "--workers", str(workers),
        "--duration-secs", str(duration_secs),
        "--concurrency", str(concurrency),
        "--heartbeat-interval-ms", str(heartbeat_interval_ms),
        "--failure-rate", str(failure_rate),
        "--checkpoint-every-secs", str(checkpoint_every_secs),
        "--out-json", str(out_json),
    ]
    if recover_failed_workers:
        cmd.append("--recover-failed-workers")
    result = subprocess.run(
        cmd, capture_output=True, text=True, timeout=timeout, cwd=str(REPO_ROOT)
    )
    if result.returncode != 0:
        raise AssertionError(
            f"runtime-bench failed (code {result.returncode}):\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
    return json.loads(out_json.read_text())
