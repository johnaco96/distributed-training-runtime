#!/usr/bin/env bash
set -euo pipefail
OUT_DIR="${1:-.dtruntime-results/checkpoint-stall-$(date -u +%Y%m%dT%H%M%SZ)}"
mkdir -p "$OUT_DIR"
python python/examples/linux_scaling_benchmark.py --workers 1,2 --samples 1000 --checkpoint-bytes $((16*1024*1024)) --output-dir "$OUT_DIR"
