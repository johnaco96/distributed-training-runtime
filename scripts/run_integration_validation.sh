#!/usr/bin/env bash
set -euo pipefail

TS="$(date -u +%Y%m%dT%H%M%SZ)"
ROOT=".dtruntime-results/integration-${TS}"
COORD_ADDR="${DTRUNTIME_COORDINATOR_ADDR:-127.0.0.1:50051}"
METRICS_ADDR="${DTRUNTIME_METRICS_ADDR:-http://127.0.0.1:9090}"

mkdir -p "$ROOT"/{localfs-managed,minio-managed,failure-recovery,multi-worker}

run_localfs() {
  local out="$ROOT/localfs-managed"
  PYTHONPATH=python python python/examples/pytorch_e2e_training_benchmark.py \
    --mode dtruntime-managed --backend localfs --run-id "integration-localfs-${TS}" \
    --worker-id worker-0 --shard-count 4 --steps 20 --checkpoint-every-steps 10 \
    --checkpoint-bytes $((16*1024*1024)) --output-dir "$out" --json-out "$out/result.json"
  curl -fsS "$METRICS_ADDR/metrics" > "$out/coordinator-metrics-after.prom"
  find "$out/checkpoints" -type f | sort > "$out/checkpoint-files.txt"
  PYTHONPATH=python python python/examples/restore_latest_checkpoint.py --run-id "integration-localfs-${TS}" --expect-backend localfs --local-root "$out" --json-out "$out/restore-latest.json"
  date -u +%Y-%m-%dT%H:%M:%SZ > "$out/time.txt"
}

run_multiworker() {
  local out="$ROOT/multi-worker"
  local run_id="integration-multi-${TS}"
  for wid in 0 1 2 3; do
    PYTHONPATH=python python python/examples/pytorch_e2e_training_benchmark.py \
      --mode dtruntime-managed --backend localfs --run-id "$run_id" --worker-id "worker-$wid" \
      --shard-count 4 --steps 4 --checkpoint-every-steps 4 --checkpoint-bytes $((2*1024*1024)) \
      --output-dir "$out/worker-$wid" > "$out/worker-$wid.log" 2>&1
  done
  curl -fsS "$METRICS_ADDR/metrics" > "$out/coordinator-metrics-after.prom"
}

run_failure_recovery() {
  local out="$ROOT/failure-recovery"
  local run_id="integration-localfs-${TS}"
  PYTHONPATH=python python python/examples/restore_latest_checkpoint.py --run-id "$run_id" --expect-backend localfs --local-root "$ROOT/localfs-managed" --json-out "$out/restore-output.json" > "$out/recovery-log.txt"
}

run_minio() {
  local out="$ROOT/minio-managed"
  local run_id="integration-minio-${TS}"
  : "${DTRUNTIME_S3_BUCKET:?set DTRUNTIME_S3_BUCKET for minio validation}"
  PYTHONPATH=python python python/examples/pytorch_e2e_training_benchmark.py \
    --mode dtruntime-managed --backend minio --run-id "$run_id" --worker-id worker-0 \
    --shard-count 4 --steps 10 --checkpoint-every-steps 5 --checkpoint-bytes $((4*1024*1024)) \
    --output-dir "$out" --json-out "$out/result.json"
  curl -fsS "$METRICS_ADDR/metrics" > "$out/coordinator-metrics-after.prom"
  python - <<'PY' > "$out/object-list.txt"
import boto3, os
s3=boto3.client('s3',endpoint_url=os.getenv('DTRUNTIME_S3_ENDPOINT'),region_name=os.getenv('DTRUNTIME_S3_REGION','us-east-1'),aws_access_key_id=os.getenv('DTRUNTIME_S3_ACCESS_KEY_ID'),aws_secret_access_key=os.getenv('DTRUNTIME_S3_SECRET_ACCESS_KEY'))
resp=s3.list_objects_v2(Bucket=os.environ['DTRUNTIME_S3_BUCKET'])
for o in resp.get('Contents',[]):
    print(f"{o['Key']}\t{o['Size']}")
PY
  PYTHONPATH=python python python/examples/restore_latest_checkpoint.py --run-id "$run_id" --expect-backend minio --json-out "$out/restore-latest.json"
  date -u +%Y-%m-%dT%H:%M:%SZ > "$out/time.txt"
}

run_localfs
run_failure_recovery
run_multiworker
if [[ -n "${DTRUNTIME_S3_ENDPOINT:-}" ]]; then
  run_minio
else
  echo "minio not run: DTRUNTIME_S3_ENDPOINT not set" > "$ROOT/minio-managed/result.json"
fi

echo "Integration results: $ROOT"
