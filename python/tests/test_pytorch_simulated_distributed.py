from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

EXAMPLE_PATH = Path(__file__).resolve().parents[1] / "examples" / "pytorch_simulated_distributed.py"


def _torch_installed() -> bool:
    return importlib.util.find_spec("torch") is not None


def _run_example(tmp_path: Path, *extra: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            sys.executable,
            str(EXAMPLE_PATH),
            "--samples",
            "32",
            "--batch-size",
            "8",
            "--workers",
            "2",
            "--shards",
            "4",
            "--out-dir",
            str(tmp_path),
            *extra,
        ],
        env={**os.environ, "PYTHONPATH": "python"},
        cwd=Path(__file__).resolve().parents[2],
        capture_output=True,
        text=True,
        check=False,
    )


def test_parse_args_defaults() -> None:
    spec = importlib.util.spec_from_file_location("pytorch_simulated_distributed", EXAMPLE_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    old_argv = sys.argv
    try:
        sys.argv = ["pytorch_simulated_distributed.py"]
        args = module.parse_args()
    finally:
        sys.argv = old_argv

    assert args.workers == 2
    assert args.epochs == 2
    assert args.samples == 128
    assert args.batch_size == 8
    assert args.shards is None
    assert args.resume is False


def test_shard_assignment_is_deterministic() -> None:
    spec = importlib.util.spec_from_file_location("pytorch_simulated_distributed", EXAMPLE_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    shard_lists = module.build_shards(16, 4)
    assignments = module.assign_shards(2, shard_lists)
    assert assignments == {0: [0, 2], 1: [1, 3]}


@pytest.mark.skipif(not _torch_installed(), reason="torch is not installed")
def test_simulated_distributed_writes_metrics_and_checkpoint(tmp_path: Path) -> None:
    output_dir = tmp_path / "dist-smoke"
    result = _run_example(output_dir, "--epochs", "1")
    assert result.returncode == 0, result.stderr

    metrics = json.loads((output_dir / "metrics.json").read_text(encoding="utf-8"))
    assert metrics["final_epoch"] == 1
    assert metrics["workers"] == 2
    assert metrics["shards"] == 4
    assert len(metrics["worker_metrics"]) == 2

    checkpoints_dir = output_dir / "checkpoints"
    assert (checkpoints_dir / "epoch-0001.pt").exists()
    assert (checkpoints_dir / "latest.json").exists()
    assert (output_dir / "shard_assignments.json").exists()


@pytest.mark.skipif(not _torch_installed(), reason="torch is not installed")
def test_resume_continues_to_next_epoch(tmp_path: Path) -> None:
    output_dir = tmp_path / "dist-resume"
    first = _run_example(output_dir, "--epochs", "1")
    assert first.returncode == 0, first.stderr

    second = _run_example(output_dir, "--epochs", "2", "--resume")
    assert second.returncode == 0, second.stderr
    assert "epoch=2" in second.stdout

    metrics = json.loads((output_dir / "metrics.json").read_text(encoding="utf-8"))
    assert metrics["final_epoch"] == 2
    assert metrics["resumed_from_checkpoint"] is True
    latest = json.loads((output_dir / "checkpoints" / "latest.json").read_text(encoding="utf-8"))
    assert latest["epoch"] == 2
    assert latest["checkpoint_path"] == "epoch-0002.pt"


@pytest.mark.skipif(not _torch_installed(), reason="torch is not installed")
def test_no_checkpoint_writes_only_metrics(tmp_path: Path) -> None:
    output_dir = tmp_path / "dist-no-ckpt"
    result = _run_example(output_dir, "--epochs", "1", "--no-checkpoint")
    assert result.returncode == 0, result.stderr

    metrics = json.loads((output_dir / "metrics.json").read_text(encoding="utf-8"))
    assert metrics["checkpoint_path"] is None
    assert not (output_dir / "checkpoints" / "latest.json").exists()
