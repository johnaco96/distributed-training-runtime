"""Shared harness for the tests that drive real runtime processes.

Used by every suite that spawns the Rust coordinator or the `runtime-bench`
load generator — integration, TLS, failover, chaos, and soak. Binaries are
located from the prebuilt `target/release` output so a test run never pays a
`cargo` compile; when one is missing the caller skips with a build hint.

Keeping port allocation and process start/stop here means the suites cannot
drift into subtly different versions of the same setup.
"""

from __future__ import annotations

import contextlib
import json
import socket
import subprocess
import sys
import tempfile
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


def start_coordinator(
    binary: Path,
    address: str,
    extra_args: list[str],
    *,
    output=None,
) -> subprocess.Popen:
    """Spawn the coordinator; caller is responsible for stopping it.

    Pass ``output`` (an open file handle) for any coordinator that outlives a
    single short check. The default ``PIPE`` is only safe when the process is
    short-lived: nothing drains it, so once the OS pipe buffer fills, the
    coordinator blocks forever on its next log write and the test hangs.
    :func:`running_coordinator` always redirects to a file for this reason.
    """
    return subprocess.Popen(
        [str(binary), "--addr", address, *extra_args],
        stdout=output if output is not None else subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )


def wait_for_grpc(
    address: str,
    process: subprocess.Popen,
    timeout: float = 60.0,
    log_path: Optional[Path] = None,
) -> None:
    """Block until the coordinator serves gRPC, or raise with its output.

    ``log_path`` is where the process's output was redirected, if it was; it is
    read back to explain an early exit when there is no pipe to read.
    """
    import grpc

    def _output() -> str:
        if log_path is not None and log_path.exists():
            return log_path.read_text(encoding="utf-8", errors="replace")
        return process.stdout.read() if process.stdout else ""

    deadline = time.monotonic() + timeout
    last_error: Optional[Exception] = None
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise AssertionError(
                f"coordinator exited early (code {process.returncode}):\n{_output()}"
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


@contextlib.contextmanager
def running_coordinator(extra_args: Optional[list[str]] = None, *, timeout: float = 60.0):
    """Yield the address of a coordinator that is up and serving gRPC.

    Composes port allocation, spawn, readiness wait, and teardown so a test
    fixture is three lines instead of twenty. Skips (rather than fails) when
    the release binary is not built, matching the rest of the harness.

    Usage::

        with running_coordinator(["--storage-root", str(tmp)]) as addr:
            ...
    """
    binary = require_binary("runtime-coordinator")
    address = f"127.0.0.1:{free_tcp_port()}"
    with tempfile.TemporaryDirectory(prefix="dtr-coordinator-") as tmp:
        # Output goes to a file, never an undrained pipe: a module-scoped
        # coordinator logs steadily and would otherwise block once the pipe
        # buffer filled, hanging the whole suite.
        log_path = Path(tmp) / "coordinator.log"
        with log_path.open("w", encoding="utf-8") as log:
            process = start_coordinator(binary, address, extra_args or [], output=log)
            try:
                wait_for_grpc(address, process, timeout=timeout, log_path=log_path)
                yield address
            finally:
                stop_process(process)


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
