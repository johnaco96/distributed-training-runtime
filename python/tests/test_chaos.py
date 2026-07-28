"""Chaos / preemption test: inject worker failures during a live load and
assert the coordinator stays healthy and recovers the fleet.

Gated by DTRUNTIME_RUN_CHAOS_TESTS=1 and the prebuilt release binaries
(cargo build --release -p runtime-coordinator -p runtime-bench).
"""

from __future__ import annotations

import os

import harness
import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get("DTRUNTIME_RUN_CHAOS_TESTS") != "1",
    reason="set DTRUNTIME_RUN_CHAOS_TESTS=1 to run the chaos/preemption test",
)


def test_worker_preemption_and_recovery_under_load(tmp_path):
    """~30% of workers are killed and recovered mid-run while checkpoints flow.
    The coordinator must survive and keep servicing RPCs to the end."""
    harness.require_binary("runtime-coordinator")
    bench = harness.require_binary("runtime-bench")
    workers = int(os.environ.get("DTRUNTIME_CHAOS_WORKERS", "40"))
    duration = int(os.environ.get("DTRUNTIME_CHAOS_DURATION_SECS", "20"))

    summary = harness.run_bench_logical_workers(
        bench,
        workers=workers,
        duration_secs=duration,
        failure_rate=0.3,
        recover_failed_workers=True,
        checkpoint_every_secs=5,
        heartbeat_interval_ms=500,
        out_json=tmp_path / "chaos.json",
        timeout=duration + 180,
    )

    # The run completed (bench exited 0, enforced by run_bench) and did real
    # work despite the injected worker churn.
    assert summary["operation_count"] > 0, summary
    assert summary["success_count"] > 0, summary
    # The full worker lifecycle ran under chaos (registration + heartbeats +
    # checkpoint RPCs all exercised).
    extra = summary.get("extra", {})
    assert "heartbeat_latency_ms" in extra, summary
    assert "checkpoint_commit_latency_ms" in extra, summary
    # A non-trivial majority of operations succeeded through the churn — the
    # coordinator stayed healthy and recovered the fleet rather than collapsing.
    success_ratio = summary["success_count"] / max(summary["operation_count"], 1)
    assert success_ratio > 0.5, f"too many failures under chaos: {success_ratio}"
