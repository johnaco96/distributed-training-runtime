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


@pytest.mark.skipif(not _torch_installed(), reason="torch is not installed")
def test_pytorch_example_writes_metrics(tmp_path: Path) -> None:
    output_dir = tmp_path / "pytorch-smoke"
    result = subprocess.run(
        [
            sys.executable,
            str(EXAMPLE_PATH),
            "--epochs",
            "1",
            "--samples",
            "32",
            "--batch-size",
            "8",
            "--out-dir",
            str(output_dir),
        ],
        env={**os.environ, "PYTHONPATH": "python"},
        cwd=Path(__file__).resolve().parents[2],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr

    metrics_path = output_dir / "metrics.json"
    assert metrics_path.exists()
    metrics = json.loads(metrics_path.read_text(encoding="utf-8"))
    assert "final_loss" in metrics
    assert "final_accuracy" in metrics
