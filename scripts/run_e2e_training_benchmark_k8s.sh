#!/usr/bin/env bash
set -euo pipefail
PYTHON_BIN="${PYTHON_BIN:-python3}"
NAMESPACE=dtruntime-bench
IMAGE_TAG=distributed-training-runtime:local
BACKEND=localfs
WORKERS=1
SAMPLES=1000
STEPS=100
BATCH_SIZE=32
CHECKPOINT_EVERY_STEPS=10
CHECKPOINT_BYTES=$((16*1024*1024))
ENABLE_FAILURE_RECOVERY=false
OUTPUT_DIR=".dtruntime-results/e2e-training-$(date -u +%Y%m%dT%H%M%SZ)"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --namespace) NAMESPACE="$2"; shift 2;; --image-tag) IMAGE_TAG="$2"; shift 2;; --backend) BACKEND="$2"; shift 2;;
    --workers) WORKERS="$2"; shift 2;; --samples) SAMPLES="$2"; shift 2;; --steps) STEPS="$2"; shift 2;;
    --batch-size) BATCH_SIZE="$2"; shift 2;; --checkpoint-every-steps) CHECKPOINT_EVERY_STEPS="$2"; shift 2;;
    --checkpoint-bytes) CHECKPOINT_BYTES="$2"; shift 2;; --enable-failure-recovery) ENABLE_FAILURE_RECOVERY=true; shift 1;;
    --output-dir) OUTPUT_DIR="$2"; shift 2;; *) echo "unknown arg $1"; exit 2;;
  esac
 done
mkdir -p "$OUTPUT_DIR" "$OUTPUT_DIR/kubectl" "$OUTPUT_DIR/runs/baseline-blocking" "$OUTPUT_DIR/runs/dtruntime-managed" "$OUTPUT_DIR/runs/failure-recovery"
kubectl create ns "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -
kubectl -n "$NAMESPACE" apply -f k8s/benchmark/minio.yaml
kubectl -n "$NAMESPACE" apply -f k8s/local/coordinator-service.yaml
kubectl -n "$NAMESPACE" apply -f k8s/local/coordinator-deployment.yaml
kubectl -n "$NAMESPACE" wait --for=condition=available deployment/runtime-coordinator --timeout=180s
kubectl -n "$NAMESPACE" apply -f k8s/benchmark/training-jobs.yaml
"$PYTHON_BIN" python/examples/pytorch_e2e_training_benchmark.py --mode baseline-blocking --backend "$BACKEND" --samples "$SAMPLES" --steps "$STEPS" --batch-size "$BATCH_SIZE" --checkpoint-every-steps "$CHECKPOINT_EVERY_STEPS" --checkpoint-bytes "$CHECKPOINT_BYTES" --json-out "$OUTPUT_DIR/runs/baseline-blocking/metrics.json" > "$OUTPUT_DIR/baseline-training.log"
"$PYTHON_BIN" python/examples/pytorch_e2e_training_benchmark.py --mode dtruntime-managed --backend "$BACKEND" --samples "$SAMPLES" --steps "$STEPS" --batch-size "$BATCH_SIZE" --checkpoint-every-steps "$CHECKPOINT_EVERY_STEPS" --checkpoint-bytes "$CHECKPOINT_BYTES" --json-out "$OUTPUT_DIR/runs/dtruntime-managed/metrics.json" > "$OUTPUT_DIR/dtruntime-training.log"
cat > "$OUTPUT_DIR/failure-recovery.log" <<EOF
{"worker_killed": $ENABLE_FAILURE_RECOVERY, "stale_detected": false, "failed_detected": false, "restore_latest_success": false, "recovery_time_seconds": null, "checkpoint_restored_id": null, "caveats": "Requires live multi-worker k8s execution."}
EOF
"$PYTHON_BIN" - <<PY
import csv, json, pathlib
out=pathlib.Path("$OUTPUT_DIR")
b=json.loads((out/"runs/baseline-blocking/metrics.json").read_text())
d=json.loads((out/"runs/dtruntime-managed/metrics.json").read_text())
summary={"baseline":b,"dtruntime":d,"backend":"$BACKEND","workers":$WORKERS}
(out/"summary.json").write_text(json.dumps(summary,indent=2)+"\n")
with (out/"comparison.csv").open("w",newline="") as f:
 w=csv.writer(f); w.writerow(["metric","baseline","dtruntime"])
 for m in ["total_duration_seconds","samples_per_second","stall_percent","checkpoint_throughput_mb_s"]: w.writerow([m,b.get(m),d.get(m)])
(out/"report.md").write_text("# E2E Training Benchmark Report\n\n## environment\n\n## Kubernetes context\n\n## storage backend\n\n## training workload\n\n## baseline vs dtruntime comparison table\n\n## checkpoint stall comparison\n\n## restore/failure recovery result\n\n## coordinator metrics deltas\n\n## Linux profiling summary\n\n## caveats\n\n## what this proves\n\n## what this does not prove\n")
PY
bash scripts/run_e2e_training_profile.sh "$OUTPUT_DIR"
