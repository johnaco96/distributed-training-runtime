from __future__ import annotations

from pathlib import Path


def test_linux_scaling_script_contains_required_artifact_paths() -> None:
    script = Path("scripts/run_linux_scaling_benchmark.sh").read_text(encoding="utf-8")
    assert "summary.json" in script or "linux_scaling_benchmark.py" in script
    assert "system/uname.txt" in script
    assert "system/lscpu.txt" in script


def test_collect_profile_script_checks_tools() -> None:
    script = Path("scripts/collect_linux_profile.sh").read_text(encoding="utf-8")
    for tool in ["perf", "pidstat", "mpstat", "iostat", "sar", "ss", "free", "df", "uname", "lscpu", "ps", "top"]:
        assert tool in script
