"""Large-checkpoint performance test.

Gated behind ``DTRUNTIME_RUN_PERF_TESTS=1`` (in addition to torch/grpc being
installed): saves and restores a ~200 MB model through the coordinator data
plane and reports throughput.  The assertions are deliberately loose — the
goal is catching pathological regressions (accidental O(n²) buffering,
per-chunk fsyncs), not benchmarking the machine.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import uuid
from pathlib import Path

import pytest

from test_torch_distributed_integration import (  # noqa: E402
    EXAMPLE,
    REPO_ROOT,
    _start_coordinator,
    _stop,
)

PERF_SCRIPT = REPO_ROOT / "python" / "tests" / "perf_checkpoint_worker.py"


def test_large_checkpoint_save_restore_throughput(tmp_path: Path) -> None:
    if os.environ.get("DTRUNTIME_RUN_PERF_TESTS") != "1":
        pytest.skip("set DTRUNTIME_RUN_PERF_TESTS=1 to run checkpoint perf tests")
    pytest.importorskip("torch")
    pytest.importorskip("grpc")

    store = tmp_path / "store"
    coordinator, address = _start_coordinator(
        tmp_path, extra_args=["--storage-root", str(store)]
    )
    try:
        env = dict(os.environ)
        env.update(
            {
                "PYTHONPATH": str(REPO_ROOT / "python"),
                "DTRUNTIME_COORDINATOR_ADDR": address,
                "DTRUNTIME_PERF_OUT": str(tmp_path / "perf.json"),
                "DTRUNTIME_WORKER_HOST": "127.0.0.1",
            }
        )
        started = time.monotonic()
        result = subprocess.run(
            [sys.executable, str(PERF_SCRIPT)],
            cwd=REPO_ROOT,
            env=env,
            capture_output=True,
            text=True,
            timeout=900,
        )
        elapsed = time.monotonic() - started
        assert result.returncode == 0, (
            f"perf worker failed\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
        metrics = json.loads((tmp_path / "perf.json").read_text(encoding="utf-8"))
        print(
            f"\ncheckpoint perf: {metrics['size_mb']:.0f} MB | "
            f"save {metrics['save_seconds']:.1f}s "
            f"({metrics['save_mb_per_s']:.1f} MB/s) | "
            f"restore {metrics['restore_seconds']:.1f}s "
            f"({metrics['restore_mb_per_s']:.1f} MB/s) | total {elapsed:.1f}s"
        )
        # Loose sanity budgets: localhost loopback should sustain far more
        # than 5 MB/s in both directions.
        assert metrics["save_mb_per_s"] > 5, metrics
        assert metrics["restore_mb_per_s"] > 5, metrics
        assert metrics["restored_matches"], "restored tensors must match saved ones"
    finally:
        _stop(coordinator)
