#!/usr/bin/env bash
set -euo pipefail
OUT_DIR="${1:?output dir required}"
mkdir -p "$OUT_DIR/system" "$OUT_DIR/perf"
record_unavailable(){ echo "unavailable: $2" > "$1"; }
for t in perf pidstat mpstat iostat sar kubectl top ps ss free df uname lscpu; do command -v "$t" >/dev/null 2>&1 || true; done
uname -a > "$OUT_DIR/system/uname.txt" 2>&1 || record_unavailable "$OUT_DIR/system/uname.txt" "uname missing"
lscpu > "$OUT_DIR/system/lscpu.txt" 2>&1 || record_unavailable "$OUT_DIR/system/lscpu.txt" "lscpu missing"
free -h > "$OUT_DIR/system/free.txt" 2>&1 || record_unavailable "$OUT_DIR/system/free.txt" "free missing"
df -h > "$OUT_DIR/system/df.txt" 2>&1 || record_unavailable "$OUT_DIR/system/df.txt" "df missing"
ss -s > "$OUT_DIR/system/ss.txt" 2>&1 || record_unavailable "$OUT_DIR/system/ss.txt" "ss missing"
ps aux > "$OUT_DIR/system/ps.txt" 2>&1 || record_unavailable "$OUT_DIR/system/ps.txt" "ps missing"
top -b -n 1 > "$OUT_DIR/system/top.txt" 2>&1 || record_unavailable "$OUT_DIR/system/top.txt" "top unavailable"
cat /proc/net/dev > "$OUT_DIR/system/proc-net-dev-before.txt" 2>/dev/null || record_unavailable "$OUT_DIR/system/proc-net-dev-before.txt" "/proc/net/dev unavailable"
if command -v pidstat >/dev/null 2>&1; then pidstat 1 3 > "$OUT_DIR/system/pidstat.txt" 2>&1; else record_unavailable "$OUT_DIR/system/pidstat.txt" "pidstat missing"; fi
if command -v iostat >/dev/null 2>&1; then iostat -x 1 3 > "$OUT_DIR/system/iostat.txt" 2>&1; else record_unavailable "$OUT_DIR/system/iostat.txt" "iostat missing"; fi
if command -v mpstat >/dev/null 2>&1; then mpstat 1 3 > "$OUT_DIR/system/mpstat.txt" 2>&1; else record_unavailable "$OUT_DIR/system/mpstat.txt" "mpstat missing"; fi
if command -v sar >/dev/null 2>&1; then sar 1 3 > "$OUT_DIR/system/sar.txt" 2>&1; else record_unavailable "$OUT_DIR/system/sar.txt" "sar missing"; fi
if command -v perf >/dev/null 2>&1; then perf stat -a sleep 2 > "$OUT_DIR/perf/perf-stat.txt" 2>&1 || true; else record_unavailable "$OUT_DIR/perf/perf-stat.txt" "perf missing"; fi
cat /proc/net/dev > "$OUT_DIR/system/proc-net-dev-after.txt" 2>/dev/null || record_unavailable "$OUT_DIR/system/proc-net-dev-after.txt" "/proc/net/dev unavailable"
