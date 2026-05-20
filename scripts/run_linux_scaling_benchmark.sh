#!/usr/bin/env bash
set -euo pipefail

if [[ "$(uname -s)" != "Linux" ]]; then
  echo "This benchmark must run on Linux for profiling evidence." >&2
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

python python/examples/linux_scaling_benchmark.py --output-dir "$OUT_DIR" "${ARGS[@]}"

uname -a > "$OUT_DIR/system/uname.txt" || true
lscpu > "$OUT_DIR/system/lscpu.txt" || true
free -h > "$OUT_DIR/system/free.txt" || true
df -h > "$OUT_DIR/system/df.txt" || true
ss -s > "$OUT_DIR/system/ss.txt" || true

echo "completed: $OUT_DIR"
