from __future__ import annotations

from pathlib import Path


def test_linux_scaling_script_uses_python_bin() -> None:
    script = Path("scripts/run_linux_scaling_benchmark.sh").read_text(encoding="utf-8")
    assert 'PYTHON_BIN="${PYTHON_BIN:-python3}"' in script
    assert '"$PYTHON_BIN" python/examples/linux_scaling_benchmark.py' in script
    assert "python python/examples/linux_scaling_benchmark.py" not in script


def test_collect_profile_script_checks_tools() -> None:
    script = Path("scripts/collect_linux_profile.sh").read_text(encoding="utf-8")
    for tool in ["perf", "pidstat", "mpstat", "iostat", "sar", "ss", "free", "df", "uname", "lscpu", "ps", "top"]:
        assert tool in script


def test_gitattributes_enforces_lf_for_shell() -> None:
    attrs = Path(".gitattributes").read_text(encoding="utf-8")
    assert "*.sh text eol=lf" in attrs
