"""Helpers shared by the demo entry points (run_demo.py, run_benchmark.py):
console banners, locating/building the release coordinator, and waiting for
its gRPC port to come up."""

from __future__ import annotations

import socket
import subprocess
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]


def _c(text: str, code: str) -> str:
    return f"\033[{code}m{text}\033[0m" if sys.stdout.isatty() else text


def banner(text: str) -> None:
    line = "=" * 62
    print(f"\n{_c(line, '36')}\n {_c(text, '1;36')}\n{_c(line, '36')}")


def coordinator_binary() -> str:
    """Path to the release coordinator, building it once if missing."""
    exe = "runtime-coordinator" + (".exe" if sys.platform == "win32" else "")
    rel = REPO / "target" / "release" / exe
    if not rel.exists():
        print("  release coordinator not found - building it (one-time)...")
        subprocess.run(
            ["cargo", "build", "--release", "-p", "runtime-coordinator"],
            cwd=REPO, check=True,
        )
    return str(rel)


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_grpc(address: str, proc: subprocess.Popen, timeout: float = 60) -> None:
    """Block until the coordinator answers gRPC at *address* (or die trying)."""
    import grpc
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if proc.poll() is not None:
            raise SystemExit("coordinator exited before it was ready")
        ch = grpc.insecure_channel(address)
        try:
            grpc.channel_ready_future(ch).result(timeout=1)
            return
        except Exception:
            pass
        finally:
            ch.close()
    raise SystemExit(f"coordinator not reachable at {address}")
