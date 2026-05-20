#!/usr/bin/env bash
set -euo pipefail
OUT_DIR="${1:-.dtruntime-results/linux-profile-$(date -u +%Y%m%dT%H%M%SZ)}"
mkdir -p "$OUT_DIR/system" "$OUT_DIR/perf"
for cmd in perf pidstat mpstat iostat sar ss free df uname lscpu ps top; do
  if command -v "$cmd" >/dev/null 2>&1; then
    echo "$cmd=available"
  else
    echo "$cmd=missing"
  fi
done > "$OUT_DIR/tools.txt"
