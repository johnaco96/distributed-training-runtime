#!/usr/bin/env python3
"""Build the optional Rust accelerator and drop it into the dtruntime package.

Usage:
    python scripts/build_native.py [--debug]

Compiles crates/dtruntime-native (PyO3, abi3 for CPython >= 3.11) with cargo
and copies the produced cdylib to python/dtruntime/_native.pyd (Windows) or
_native.so (Linux/macOS), where `from dtruntime import _native` finds it.
The SDK works without this — the pure-Python path is the fallback — but the
DTRS decode/fetch hot path runs in Rust when it is present.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
PACKAGE_DIR = REPO_ROOT / "python" / "dtruntime"


def main() -> int:
    if sys.version_info < (3, 11):
        print(
            "warning: dtruntime._native targets CPython >= 3.11 (abi3-py311); "
            f"this interpreter is {sys.version.split()[0]} and will not load it.",
            file=sys.stderr,
        )
    profile = "debug" if "--debug" in sys.argv[1:] else "release"
    cmd = ["cargo", "build", "-p", "dtruntime-native"]
    if profile == "release":
        cmd.append("--release")
    env = dict(os.environ)
    # Make sure pyo3's build script resolves the same interpreter running us.
    env.setdefault("PYO3_PYTHON", sys.executable)
    print("+", " ".join(cmd))
    subprocess.run(cmd, cwd=REPO_ROOT, env=env, check=True)

    target = REPO_ROOT / "target" / profile
    if sys.platform == "win32":
        built, dest_name = target / "dtruntime_native.dll", "_native.pyd"
    elif sys.platform == "darwin":
        built, dest_name = target / "libdtruntime_native.dylib", "_native.so"
    else:
        built, dest_name = target / "libdtruntime_native.so", "_native.so"
    if not built.is_file():
        print(f"error: expected build artifact not found: {built}", file=sys.stderr)
        return 1

    dest = PACKAGE_DIR / dest_name
    shutil.copyfile(built, dest)
    print(f"installed {dest}")

    check = subprocess.run(
        [sys.executable, "-c", "from dtruntime import _native; print(_native.__version__)"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )
    if check.returncode == 0:
        print(f"import check OK (dtruntime._native {check.stdout.strip()})")
    else:
        print(f"import check failed:\n{check.stderr}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
