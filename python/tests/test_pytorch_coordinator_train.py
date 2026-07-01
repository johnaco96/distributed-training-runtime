from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
import os
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


def subprocess_env() -> dict[str, str]:
    env = os.environ.copy()
    env["PYTHONPATH"] = "python"
    return env


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
    subprocess.run(cmd, check=True, env=subprocess_env())
    metrics = json.loads((out_dir / "metrics.json").read_text(encoding="utf-8"))
    assert metrics["workers"] == 2
    assert metrics["shards"] == 4
    assert metrics["final_epoch"] == 1
    assert metrics["worker_metrics"]
    assert metrics["shard_assignments"]
    latest = json.loads((out_dir / "checkpoints" / "latest.json").read_text(encoding="utf-8"))
    assert len(latest["checkpoint_paths"]) == 2
    assert metrics["latest_checkpoint_metadata"]["artifact_count"] == 2


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
    subprocess.run(cmd, check=True, env=subprocess_env())
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
    subprocess.run([*base, "--epochs", "1"], check=True, env=subprocess_env())
    subprocess.run([*base, "--epochs", "2", "--resume"], check=True, env=subprocess_env())
    metrics = json.loads((out_dir / "metrics.json").read_text(encoding="utf-8"))
    assert metrics["final_epoch"] == 2
    assert metrics["resumed_from_checkpoint"] is True


def test_example_uses_separate_admin_and_worker_identity_paths() -> None:
    source = Path(MODULE).read_text(encoding="utf-8")
    assert "admin_client.register_run(" in source
    assert "worker_clients[worker_id].register_worker(" in source
    # Shard requests moved into ShardLoader; it must be constructed with the
    # worker-scoped client so GetShardAssignment carries the worker identity.
    assert "worker_clients[worker_id], run_id, worker_id, dataset_id" in source
    assert "worker_clients[worker_id].heartbeat(" in source
    assert "admin_client.commit_checkpoint(" in source


def test_example_has_no_stale_single_client_runtime_pb2_usage() -> None:
    source = Path(MODULE).read_text(encoding="utf-8")
    assert "client._runtime_pb2" not in source
    assert "admin_client._runtime_pb2" not in source
    assert "from dtruntime.generated import runtime_pb2" in source


def test_example_has_no_legacy_single_client_register_run_path() -> None:
    source = Path(MODULE).read_text(encoding="utf-8")
    assert "client.stub.RegisterRun" not in source
