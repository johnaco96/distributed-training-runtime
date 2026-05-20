from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
from pathlib import Path

import pytest

MODULE = "python/examples/pytorch_coordinator_train.py"


def _torch_available() -> bool:
    return importlib.util.find_spec("torch") is not None


def _protobuf_available() -> bool:
    return importlib.util.find_spec("google.protobuf") is not None


requires_deps = pytest.mark.skipif(
    not _torch_available() or not _protobuf_available(),
    reason="requires torch and google.protobuf",
)


@requires_deps
def test_smoke_with_checkpoint(tmp_path: Path) -> None:
    out_dir = tmp_path / "coord"
    cmd = [
        sys.executable,
        MODULE,
        "--workers",
        "2",
        "--epochs",
        "1",
        "--samples",
        "32",
        "--batch-size",
        "8",
        "--shards",
        "4",
        "--out-dir",
        str(out_dir),
    ]
    subprocess.run(cmd, check=True, env={**dict(), **{"PYTHONPATH": "python"}})
    metrics = json.loads((out_dir / "metrics.json").read_text(encoding="utf-8"))
    assert metrics["workers"] == 2
    assert metrics["shards"] == 4
    assert metrics["final_epoch"] == 1
    assert metrics["worker_metrics"]
    assert metrics["shard_assignments"]
    assert (out_dir / "checkpoints" / "latest.json").exists()


@requires_deps
def test_no_checkpoint(tmp_path: Path) -> None:
    out_dir = tmp_path / "nockpt"
    cmd = [
        sys.executable,
        MODULE,
        "--epochs",
        "1",
        "--samples",
        "32",
        "--batch-size",
        "8",
        "--out-dir",
        str(out_dir),
        "--no-checkpoint",
    ]
    subprocess.run(cmd, check=True, env={**dict(), **{"PYTHONPATH": "python"}})
    metrics = json.loads((out_dir / "metrics.json").read_text(encoding="utf-8"))
    assert metrics["checkpoint_id"] is None
    assert not (out_dir / "checkpoints" / "latest.json").exists()


@requires_deps
def test_resume(tmp_path: Path) -> None:
    out_dir = tmp_path / "resume"
    base = [
        sys.executable,
        MODULE,
        "--workers",
        "2",
        "--samples",
        "32",
        "--batch-size",
        "8",
        "--shards",
        "4",
        "--out-dir",
        str(out_dir),
    ]
    subprocess.run([*base, "--epochs", "1"], check=True, env={"PYTHONPATH": "python"})
    subprocess.run([*base, "--epochs", "2", "--resume"], check=True, env={"PYTHONPATH": "python"})
    metrics = json.loads((out_dir / "metrics.json").read_text(encoding="utf-8"))
    assert metrics["final_epoch"] == 2
    assert metrics["resumed_from_checkpoint"] is True
