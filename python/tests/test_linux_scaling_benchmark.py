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
    cmd = [sys.executable, str(MODULE), "--workers", "1,2", "--samples", "100", "--output-dir", str(out_dir)]
    r = subprocess.run(cmd, env={**os.environ, "PYTHONPATH": "python"}, capture_output=True, text=True, check=False)
    assert r.returncode == 0, r.stderr

    summary = json.loads((out_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary["schema_version"] == "v1"
    assert "checkpoint_stall_results" in summary
    assert "interop_overhead_results" in summary

    with (out_dir / "comparison.csv").open("r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    assert rows
    assert rows[0]["benchmark_name"] == "linux-scaling-checkpoint-suite"
    assert "caveats" in rows[0]


def test_report_avoids_overclaims(tmp_path: Path) -> None:
    out_dir = tmp_path / "out2"
    cmd = [sys.executable, str(MODULE), "--workers", "1", "--samples", "10", "--output-dir", str(out_dir)]
    r = subprocess.run(cmd, env={**os.environ, "PYTHONPATH": "python"}, capture_output=True, text=True, check=False)
    assert r.returncode == 0, r.stderr
    report = (out_dir / "report.md").read_text(encoding="utf-8")
    assert "OpenAI" not in report
    assert "Anthropic" not in report
