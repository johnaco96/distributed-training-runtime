from __future__ import annotations

import csv
import json
import os
import subprocess
import sys
from pathlib import Path

MODULE = Path(__file__).resolve().parents[1] / "examples" / "linux_scaling_benchmark.py"


def test_summary_schema_and_comparison_csv(tmp_path: Path) -> None:
    out_dir = tmp_path / "out"
    cmd = [sys.executable, str(MODULE), "--workers", "1,2", "--samples", "100", "--checkpoint-every-steps", "20", "--output-dir", str(out_dir)]
    r = subprocess.run(cmd, env={**os.environ, "PYTHONPATH": "python"}, capture_output=True, text=True, check=False)
    assert r.returncode == 0, r.stderr

    summary = json.loads((out_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary["schema_version"] == "v2"
    assert summary["real_checkpoint_writes"] is True
    assert "invalid_metrics" in summary

    with (out_dir / "comparison.csv").open("r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    assert rows
    assert rows[0]["coordinator_cpu_avg_percent"] == ""
    assert float(rows[0]["total_duration_seconds"]) > 0


def test_report_has_required_sections(tmp_path: Path) -> None:
    out_dir = tmp_path / "out2"
    cmd = [sys.executable, str(MODULE), "--workers", "1", "--samples", "10", "--output-dir", str(out_dir)]
    r = subprocess.run(cmd, env={**os.environ, "PYTHONPATH": "python"}, capture_output=True, text=True, check=False)
    assert r.returncode == 0, r.stderr
    report = (out_dir / "report.md").read_text(encoding="utf-8")
    for section in [
        "## Environment",
        "## Benchmark parameters",
        "## Worker scaling table",
        "## Checkpoint stall results",
        "## Interop overhead results",
        "## Profiling tools collected/skipped",
    ]:
        assert section in report


def test_coordinator_mode_fails_cleanly(tmp_path: Path) -> None:
    out_dir = tmp_path / "out3"
    cmd = [sys.executable, str(MODULE), "--mode", "coordinator-grpc", "--workers", "1", "--samples", "2", "--output-dir", str(out_dir)]
    r = subprocess.run(cmd, env={**os.environ, "PYTHONPATH": "python"}, capture_output=True, text=True, check=False)
    assert r.returncode != 0
    assert "real coordinator mode not implemented" in (r.stderr + r.stdout)
