#!/usr/bin/env bash
set -euo pipefail
OUT_DIR="${1:-.dtruntime-results/linux-profile-$(date -u +%Y%m%dT%H%M%SZ)}"
mkdir -p "$OUT_DIR/system" "$OUT_DIR/perf"

available=()
skipped=()
errors=()
for cmd in perf pidstat mpstat iostat sar ss free df uname lscpu ps top; do
  if command -v "$cmd" >/dev/null 2>&1; then available+=("$cmd"); else skipped+=("$cmd"); fi
done

uname -a > "$OUT_DIR/system/uname.txt" 2>/dev/null || errors+=("uname failed")
lscpu > "$OUT_DIR/system/lscpu.txt" 2>/dev/null || errors+=("lscpu failed")
free -h > "$OUT_DIR/system/free.txt" 2>/dev/null || errors+=("free failed")
df -h > "$OUT_DIR/system/df.txt" 2>/dev/null || errors+=("df failed")
ss -s > "$OUT_DIR/system/ss.txt" 2>/dev/null || errors+=("ss failed")
ps aux > "$OUT_DIR/system/ps.txt" 2>/dev/null || errors+=("ps failed")
top -b -n 1 > "$OUT_DIR/system/top.txt" 2>/dev/null || true

printf '%s
' "${available[@]}" > "$OUT_DIR/profiling_tools_available.txt"
printf '%s
' "${skipped[@]}" > "$OUT_DIR/profiling_tools_skipped.txt"
printf '%s
' "${errors[@]}" > "$OUT_DIR/profiling_tool_errors.txt"
