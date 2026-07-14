from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

EXAMPLE_PATH = Path(__file__).resolve().parents[1] / "examples" / "pytorch_train.py"


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
    spec = importlib.util.spec_from_file_location("pytorch_train", EXAMPLE_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    old_argv = sys.argv
    try:
        sys.argv = ["pytorch_train.py"]
        args = module.parse_args()
    finally:
        sys.argv = old_argv

    assert args.epochs == 2
    assert args.batch_size == 32
    assert args.samples == 256
    assert args.checkpoint_every == 1
    assert args.resume is False
    assert args.no_checkpoint is False


@pytest.mark.skipif(not _torch_installed(), reason="torch is not installed")
def test_pytorch_example_writes_metrics_and_checkpoint(tmp_path: Path) -> None:
    output_dir = tmp_path / "pytorch-smoke"
    result = _run_example(output_dir, "--epochs", "1")
    assert result.returncode == 0, result.stderr

    metrics_path = output_dir / "metrics.json"
    assert metrics_path.exists()
    metrics = json.loads(metrics_path.read_text(encoding="utf-8"))
    assert metrics["final_epoch"] == 1
    assert metrics["resumed_from_checkpoint"] is False

    checkpoints_dir = output_dir / "checkpoints"
    assert (checkpoints_dir / "epoch-0001.pt").exists()
    latest_path = checkpoints_dir / "latest.json"
    assert latest_path.exists()
    latest = json.loads(latest_path.read_text(encoding="utf-8"))
    assert latest["epoch"] == 1


@pytest.mark.skipif(not _torch_installed(), reason="torch is not installed")
def test_resume_continues_to_next_epoch(tmp_path: Path) -> None:
    output_dir = tmp_path / "pytorch-resume"
    first = _run_example(output_dir, "--epochs", "1")
    assert first.returncode == 0, first.stderr

    second = _run_example(output_dir, "--epochs", "2", "--resume")
    assert second.returncode == 0, second.stderr
    assert "epoch=2" in second.stdout

    metrics = json.loads((output_dir / "metrics.json").read_text(encoding="utf-8"))
    assert metrics["final_epoch"] == 2
    assert metrics["resumed_from_checkpoint"] is True
    checkpoints_dir = output_dir / "checkpoints"
    assert (checkpoints_dir / "epoch-0002.pt").exists()
    latest = json.loads((checkpoints_dir / "latest.json").read_text(encoding="utf-8"))
    assert latest["checkpoint_path"] == "epoch-0002.pt"
    assert latest["epoch"] == 2


@pytest.mark.skipif(not _torch_installed(), reason="torch is not installed")
def test_no_checkpoint_writes_only_metrics(tmp_path: Path) -> None:
    output_dir = tmp_path / "pytorch-no-ckpt"
    result = _run_example(output_dir, "--epochs", "1", "--no-checkpoint")
    assert result.returncode == 0, result.stderr

    metrics = json.loads((output_dir / "metrics.json").read_text(encoding="utf-8"))
    assert metrics["checkpoint_path"] is None
    assert not (output_dir / "checkpoints" / "latest.json").exists()


@pytest.mark.skipif(not _torch_installed(), reason="torch is not installed")
def test_resume_without_latest_checkpoint_fails(tmp_path: Path) -> None:
    output_dir = tmp_path / "pytorch-missing"
    result = _run_example(output_dir, "--epochs", "2", "--resume")
    assert result.returncode != 0
    assert "latest checkpoint metadata not found" in result.stderr
