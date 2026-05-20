#!/usr/bin/env bash
set -euo pipefail

if [[ "$(uname -s)" != "Linux" ]]; then
  echo "This benchmark must run on Linux for profiling evidence." >&2
  exit 1
fi

PYTHON_BIN="${PYTHON_BIN:-python3}"
if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
  echo "PYTHON_BIN '$PYTHON_BIN' not found. Set PYTHON_BIN=/path/to/python3" >&2
  exit 1
fi

OUT_DIR=""
ARGS=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --output-dir) OUT_DIR="$2"; shift 2;;
    *) ARGS+=("$1"); shift;;
  esac
done

if [[ -z "$OUT_DIR" ]]; then
  TS="$(date -u +%Y%m%dT%H%M%SZ)"
  OUT_DIR=".dtruntime-results/linux-scaling-${TS}"
fi
mkdir -p "$OUT_DIR/system" "$OUT_DIR/perf" "$OUT_DIR/runs"

"$PYTHON_BIN" python/examples/linux_scaling_benchmark.py --output-dir "$OUT_DIR" "${ARGS[@]}"

bash scripts/collect_linux_profile.sh "$OUT_DIR"

echo "completed: $OUT_DIR"
