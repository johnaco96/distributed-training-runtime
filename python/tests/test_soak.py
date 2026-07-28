"""Soak test: sustain a worker fleet against the coordinator for an extended
period and assert stable, non-degrading behavior.

Gated by DTRUNTIME_RUN_SOAK_TESTS=1 and the prebuilt release binaries. The
duration defaults to a short run so it stays CI-friendly; set
DTRUNTIME_SOAK_DURATION_SECS (e.g. 3600) for a real multi-hour soak.
"""

from __future__ import annotations

import os

import harness
import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get("DTRUNTIME_RUN_SOAK_TESTS") != "1",
    reason="set DTRUNTIME_RUN_SOAK_TESTS=1 to run the soak test",
)


def test_sustained_load_stays_stable(tmp_path):
    harness.require_binary("runtime-coordinator")
    bench = harness.require_binary("runtime-bench")
    workers = int(os.environ.get("DTRUNTIME_SOAK_WORKERS", "100"))
    duration = int(os.environ.get("DTRUNTIME_SOAK_DURATION_SECS", "60"))

    summary = harness.run_bench_logical_workers(
        bench,
        workers=workers,
        duration_secs=duration,
        heartbeat_interval_ms=500,
        checkpoint_every_secs=10,
        out_json=tmp_path / "soak.json",
        timeout=duration + 300,
    )

    # The coordinator serviced the whole window without dying.
    assert summary["operation_count"] > 0, summary
    assert summary["success_count"] > 0, summary
    # Steady-state health: an overwhelming majority of ops succeed.
    success_ratio = summary["success_count"] / max(summary["operation_count"], 1)
    assert success_ratio > 0.99, f"soak degraded: success_ratio={success_ratio}"
    # The run actually lasted roughly the requested window (didn't bail early).
    assert summary["elapsed_ms"] >= duration * 1000 * 0.8, summary
    # Tail latency stayed bounded (control-plane RPCs are cheap).
    assert summary["p99_latency_ms"] < 2000, summary
