#!/usr/bin/env bash
set -euo pipefail
PYTHON_BIN="${PYTHON_BIN:-python3}"
if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
  echo "PYTHON_BIN '$PYTHON_BIN' not found. Set PYTHON_BIN=/path/to/python3" >&2
  exit 1
fi
OUT_DIR="${1:-.dtruntime-results/checkpoint-stall-$(date -u +%Y%m%dT%H%M%SZ)}"
mkdir -p "$OUT_DIR"
"$PYTHON_BIN" python/examples/linux_scaling_benchmark.py --workers 1,2 --samples 1000 --checkpoint-bytes $((16*1024*1024)) --output-dir "$OUT_DIR" --mode synthetic-local
