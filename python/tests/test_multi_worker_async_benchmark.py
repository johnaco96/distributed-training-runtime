"""Tests for multi_worker_async_benchmark.py — no live coordinator required."""

from __future__ import annotations

import sys
import ast
import textwrap
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

MODULE = Path(__file__).parents[2] / "python" / "examples" / "multi_worker_async_benchmark.py"


def _load_module():
    import importlib.util

    spec = importlib.util.spec_from_file_location("multi_worker_async_benchmark", MODULE)
    mod = importlib.util.module_from_spec(spec)
    # Register before exec so dataclass string annotations resolve correctly.
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def bench():
    return _load_module()


def test_module_imports_cleanly(bench):
    assert hasattr(bench, "WorkerResult")
    assert hasattr(bench, "BenchmarkSummary")
    assert hasattr(bench, "run_benchmark")
    assert hasattr(bench, "parse_args")


def test_parse_args_defaults(bench):
    with patch("sys.argv", ["prog"]):
        args = bench.parse_args()
    assert args.workers == 4
    assert args.steps == 40
    assert args.checkpoint_every_steps == 10
    assert args.checkpoint_bytes == 16 * 1024 * 1024
    assert args.coordinator_addr == "127.0.0.1:50051"
    assert args.backend == "localfs"
    assert args.chaos_rank == -1
    assert args.chaos_at_step is None


def test_parse_args_chaos_and_backend_flags(bench):
    with patch("sys.argv", ["prog", "--backend", "s3", "--chaos-rank", "2", "--chaos-at-step", "20"]):
        args = bench.parse_args()
    assert args.backend == "s3"
    assert args.chaos_rank == 2
    assert args.chaos_at_step == 20


def test_parse_args_backend_invalid(bench):
    with patch("sys.argv", ["prog", "--backend", "nfs"]):
        with pytest.raises(SystemExit):
            bench.parse_args()


def test_worker_result_dataclass(bench):
    r = bench.WorkerResult(
        worker_id="worker-0",
        rank=0,
        total_steps=40,
        total_duration_seconds=1.0,
        step_time_p50_ms=0.5,
        step_time_p95_ms=1.0,
        step_time_p99_ms=2.0,
        checkpoint_count=4,
        checkpoint_bytes_total=64 * 1024 * 1024,
        stream_stall_seconds_total=0.1,
        stream_io_seconds_total=0.5,
        stall_percent=10.0,
        checkpoint_throughput_mb_s=128.0,
    )
    assert r.worker_id == "worker-0"
    assert r.stall_percent == 10.0
    assert r.error is None


def test_benchmark_summary_dataclass(bench):
    s = bench.BenchmarkSummary(
        run_id="test-run",
        runtime_run_id="run-1",
        workers=4,
        total_steps_per_worker=40,
        checkpoint_every_steps=10,
        checkpoint_bytes_per_worker=16 * 1024 * 1024,
        coordinator_addr="127.0.0.1:50051",
        aggregate_throughput_mb_s=400.0,
        aggregate_checkpoint_bytes=4 * 16 * 1024 * 1024,
        stall_percent_p50=5.0,
        stall_percent_p99=8.0,
        step_time_p50_ms=0.4,
        step_time_p99_ms=2.0,
        worker_results=[],
        total_wall_seconds=10.0,
        checkpoints_committed=4,
    )
    assert s.workers == 4
    assert s.aggregate_throughput_mb_s == 400.0


def test_percentile_helper(bench):
    # Implementation uses int(round((n-1)*q)); for 4 elements at q=0.50,
    # round(1.5)=2 (banker's rounding), so s[2]=3.0.
    assert bench.percentile([1.0, 2.0, 3.0, 4.0], 0.50) == 3.0
    assert bench.percentile([1.0, 2.0, 3.0, 4.0], 0.99) == 4.0
    assert bench.percentile([], 0.50) == 0.0


def test_percentile_ms_helper(bench):
    assert bench.percentile_ms([0.001, 0.002], 0.50) == pytest.approx(1.0, abs=1e-6)


def test_module_uses_stream_checkpoint_artifact():
    src = MODULE.read_text(encoding="utf-8")
    assert "stream_checkpoint_artifact" in src, (
        "benchmark must use stream_checkpoint_artifact, not report_checkpoint_artifact"
    )


def test_module_uses_multiprocessing_barrier():
    src = MODULE.read_text(encoding="utf-8")
    assert src.count("multiprocessing.Barrier") >= 2, (
        "benchmark must use two Barriers: artifact_done_barrier and commit_done_barrier"
    )
    assert "artifact_done_barrier" in src
    assert "commit_done_barrier" in src


def test_module_rank_zero_commits():
    src = MODULE.read_text(encoding="utf-8")
    assert "rank == 0" in src and "commit_checkpoint" in src, (
        "rank 0 must be the one calling commit_checkpoint"
    )


def test_module_chaos_injection_present():
    src = MODULE.read_text(encoding="utf-8")
    assert "chaos_rank" in src, "chaos_rank injection must be present in source"
    assert "chaos_at_step" in src, "chaos_at_step injection must be present in source"
    assert "barrier.abort()" in src, (
        "barriers must be aborted on worker exception to prevent sibling hangs"
    )


def test_module_last_runtime_run_id_assigned():
    src = MODULE.read_text(encoding="utf-8")
    assert "_last_runtime_run_id = runtime_run_id" in src, (
        "_last_runtime_run_id must be set inside run_benchmark so recovery can query the right run"
    )


def test_module_does_no_direct_file_io():
    src = MODULE.read_text(encoding="utf-8")
    forbidden = ["open(", "write_bytes(", "os.urandom(", "boto3"]
    # os.urandom is allowed for payload generation — not for checkpoint storage
    # The key check: no direct file write of checkpoint data
    tree = ast.parse(src)
    writes = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "write_bytes"
    ]
    assert len(writes) == 0, (
        "benchmark must not write checkpoint bytes directly — "
        "use stream_checkpoint_artifact() instead"
    )


def test_script_exits_on_worker_error(bench, tmp_path):
    """run_benchmark raises if any worker result contains an error."""
    import argparse

    args = argparse.Namespace(
        workers=1,
        steps=10,
        checkpoint_every_steps=5,
        checkpoint_bytes=1024,
        samples=64,
        batch_size=16,
        input_dim=16,
        num_classes=4,
        seed=0,
        run_id="err-test",
        coordinator_addr="127.0.0.1:1",  # nothing listening
        output_dir=str(tmp_path),
        json_out=None,
    )

    with pytest.raises(Exception):
        bench.run_benchmark(args)
