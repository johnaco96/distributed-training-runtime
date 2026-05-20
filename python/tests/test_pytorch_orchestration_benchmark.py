from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

MODULE_PATH = Path(__file__).resolve().parents[1] / "examples" / "benchmark_pytorch_orchestration.py"


def _torch_available() -> bool:
    return importlib.util.find_spec("torch") is not None


def _protobuf_available() -> bool:
    return importlib.util.find_spec("google.protobuf") is not None


requires_deps = pytest.mark.skipif(
    not _torch_available() or not _protobuf_available(), reason="requires torch and google.protobuf"
)


def _load_module():
    spec = importlib.util.spec_from_file_location("benchmark_pytorch_orchestration", MODULE_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@requires_deps
def test_parse_args_defaults() -> None:
    module = _load_module()
    old_argv = sys.argv
    try:
        sys.argv = ["benchmark_pytorch_orchestration.py"]
        args = module.parse_args()
    finally:
        sys.argv = old_argv

    assert args.runs == 3
    assert args.epochs == 2
    assert args.samples == 128
    assert args.batch_size == 16
    assert args.workers == 2
    assert args.shards == 4
    assert args.keep_outputs is False
    assert args.skip_resume is False


@requires_deps
def test_one_run_outputs_json_and_csv(tmp_path: Path) -> None:
    out_dir = tmp_path / "bench"
    json_out = tmp_path / "summary.json"
    csv_out = tmp_path / "results.csv"

    cmd = [
        sys.executable,
        str(MODULE_PATH),
        "--runs",
        "1",
        "--epochs",
        "1",
        "--samples",
        "32",
        "--batch-size",
        "8",
        "--workers",
        "2",
        "--shards",
        "4",
        "--out-dir",
        str(out_dir),
        "--json-out",
        str(json_out),
        "--csv-out",
        str(csv_out),
    ]
    result = subprocess.run(cmd, env={**os.environ, "PYTHONPATH": "python"}, capture_output=True, text=True, check=False)
    assert result.returncode == 0, result.stderr
    assert json_out.exists()
    assert csv_out.exists()

    payload = json.loads(json_out.read_text(encoding="utf-8"))
    run0 = payload["runs"][0]
    assert "standalone_fresh_seconds" in run0
    assert "coordinator_fresh_seconds" in run0
    assert "fresh_overhead_seconds" in run0
    assert payload["successful_runs"] == 1
    assert isinstance(payload["settings"]["out_dir"], str)
    if payload["settings"].get("json_out") is not None:
        assert isinstance(payload["settings"]["json_out"], str)
    if payload["settings"].get("csv_out") is not None:
        assert isinstance(payload["settings"]["csv_out"], str)


@requires_deps
def test_skip_resume_omits_resume_times(tmp_path: Path) -> None:
    out_dir = tmp_path / "bench-skip"
    json_out = tmp_path / "summary-skip.json"
    cmd = [
        sys.executable,
        str(MODULE_PATH),
        "--runs",
        "1",
        "--epochs",
        "1",
        "--samples",
        "32",
        "--batch-size",
        "8",
        "--workers",
        "2",
        "--shards",
        "4",
        "--out-dir",
        str(out_dir),
        "--json-out",
        str(json_out),
        "--skip-resume",
    ]
    result = subprocess.run(cmd, env={**os.environ, "PYTHONPATH": "python"}, capture_output=True, text=True, check=False)
    assert result.returncode == 0, result.stderr
    payload = json.loads(json_out.read_text(encoding="utf-8"))
    run0 = payload["runs"][0]
    assert run0["standalone_resume_seconds"] is None
    assert run0["coordinator_resume_seconds"] is None
    assert run0["fresh_overhead_percent"] is not None
