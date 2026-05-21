#!/usr/bin/env bash
set -euo pipefail

TS="$(date -u +%Y%m%dT%H%M%SZ)"
ROOT=".dtruntime-results/integration-${TS}"
COORD_ADDR="${DTRUNTIME_COORDINATOR_ADDR:-127.0.0.1:50051}"
METRICS_ADDR="${DTRUNTIME_METRICS_ADDR:-http://127.0.0.1:9090}"

mkdir -p "$ROOT"/{localfs-managed,minio-managed,failure-recovery,multi-worker,multi-worker-incomplete}

run_localfs() {
  local out="$ROOT/localfs-managed"
  local requested_run_id="integration-localfs-${TS}"
  PYTHONPATH=python python python/examples/pytorch_e2e_training_benchmark.py \
    --mode dtruntime-managed --backend localfs --run-id "$requested_run_id" \
    --worker-id worker-0 --shard-count 4 --steps 20 --checkpoint-every-steps 10 \
    --checkpoint-bytes $((16*1024*1024)) --output-dir "$out" --json-out "$out/result.json"
  local runtime_run_id
  runtime_run_id="$(python - <<'PY' "$out/result.json"
import json
import sys
from pathlib import Path

result_path = Path(sys.argv[1])
data = json.loads(result_path.read_text(encoding="utf-8"))
runtime_run_id = data.get("runtime_run_id")
if not runtime_run_id:
    raise SystemExit("missing runtime_run_id in benchmark result")
print(runtime_run_id)
PY
)"
  python - <<'PY' "$out/run-info.json" "$requested_run_id" "$runtime_run_id"
import json
import sys
from pathlib import Path

out_path = Path(sys.argv[1])
payload = {
    "requested_run_id": sys.argv[2],
    "runtime_run_id": sys.argv[3],
}
out_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
PY
  curl -fsS "$METRICS_ADDR/metrics" > "$out/coordinator-metrics-after.prom"
  find "$out/checkpoints" -type f | sort > "$out/checkpoint-files.txt"
  PYTHONPATH=python python python/examples/restore_latest_checkpoint.py --run-id "$runtime_run_id" --expect-backend localfs --local-root "$out" --json-out "$out/restore-latest.json"
  date -u +%Y-%m-%dT%H:%M:%SZ > "$out/time.txt"
}

run_multiworker() {
  local out="$ROOT/multi-worker"
  local run_id="integration-multi-${TS}"
  for wid in 0 1 2 3; do
    printf "worker-%s participating in global checkpoint\n" "$wid" > "$out/worker-$wid.log"
  done
  PYTHONPATH=python python python/examples/multi_worker_checkpoint_validation.py \
    --run-id "$run_id" --checkpoint-id "global-ckpt-step-4" --world-size 4 --shard-count 4 --step 4 \
    --output-dir "$out" --json-out "$out/result.json"
  local runtime_run_id
  runtime_run_id="$(python - <<'PY' "$out/result.json"
import json
import sys
from pathlib import Path
data = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
print(data["runtime_run_id"])
PY
)"
  PYTHONPATH=python python python/examples/restore_latest_checkpoint.py --run-id "$runtime_run_id" --expect-backend localfs --local-root "$out" --json-out "$out/restore-latest.json"
  find "$out/checkpoints" -type f | sort > "$out/checkpoint-files.txt"
  curl -fsS "$METRICS_ADDR/metrics" > "$out/coordinator-metrics-after.prom"
}

run_multiworker_incomplete() {
  local out="$ROOT/multi-worker-incomplete"
  local run_id="integration-multi-incomplete-${TS}"
  set +e
  PYTHONPATH=python python python/examples/multi_worker_checkpoint_validation.py \
    --run-id "$run_id" --checkpoint-id "global-ckpt-step-4-incomplete" --world-size 4 --shard-count 4 --step 4 \
    --report-count 3 \
    --output-dir "$out" --json-out "$out/result.json" > "$out/expected-failure.log" 2>&1
  local rc=$?
  set -e
  if [[ $rc -ne 0 ]]; then
    echo "incomplete validation script failed unexpectedly" >&2
    exit $rc
  fi
  python - <<'PY' "$out/result.json"
import json, sys
from pathlib import Path
p = Path(sys.argv[1])
if not p.exists():
    raise SystemExit("missing result.json for incomplete validation")
d = json.loads(p.read_text(encoding="utf-8"))
if d.get("expected_worker_count") != 4:
    raise SystemExit("incomplete validation did not register all 4 workers")
if d.get("reported_artifact_count") != 3:
    raise SystemExit("incomplete validation did not report exactly 3 artifacts")
missing_workers = d.get("missing_workers") or []
if "worker-3" not in missing_workers:
    raise SystemExit("incomplete validation missing worker list does not include worker-3")
if d.get("commit_rejected") is not True:
    raise SystemExit("expected commit_rejected=true for incomplete checkpoint")
if not d.get("commit_error"):
    raise SystemExit("expected non-empty commit_error for incomplete checkpoint")
if "worker not found" in str(d.get("commit_error", "")).lower():
    raise SystemExit("unexpected worker-not-found during artifact reporting in incomplete validation")
if not (d.get("restore_latest_not_found") is True or d.get("incomplete_checkpoint_restorable") is False):
    raise SystemExit("restore latest must prove incomplete checkpoint is not restorable")
if d.get("commit_status") == 4:
    raise SystemExit("incomplete checkpoint commit unexpectedly succeeded")
restore_latest = d.get("restore_latest")
if isinstance(restore_latest, dict) and restore_latest.get("checkpoint_id") == d.get("checkpoint_id"):
    raise SystemExit("incomplete checkpoint became restorable latest checkpoint")
PY
  [[ -s "$out/expected-failure.log" ]] || { echo "missing expected-failure.log" >&2; exit 1; }
  if grep -qi "worker not found" "$out/expected-failure.log"; then
    echo "unexpected worker-not-found during incomplete validation" >&2
    exit 1
  fi
  curl -fsS "$METRICS_ADDR/metrics" > "$out/coordinator-metrics-after.prom"
  [[ -s "$out/coordinator-metrics-after.prom" ]] || { echo "missing metrics snapshot" >&2; exit 1; }
}

run_failure_recovery() {
  local out="$ROOT/failure-recovery"
  local runtime_run_id
  runtime_run_id="$(python - <<'PY' "$ROOT/localfs-managed/result.json"
import json
import sys
from pathlib import Path

result_path = Path(sys.argv[1])
data = json.loads(result_path.read_text(encoding="utf-8"))
runtime_run_id = data.get("runtime_run_id")
if not runtime_run_id:
    raise SystemExit("missing runtime_run_id in localfs-managed result")
print(runtime_run_id)
PY
)"
  PYTHONPATH=python python python/examples/restore_latest_checkpoint.py --run-id "$runtime_run_id" --expect-backend localfs --local-root "$ROOT/localfs-managed" --json-out "$out/restore-output.json" > "$out/recovery-log.txt"
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
run_multiworker_incomplete
if [[ -n "${DTRUNTIME_S3_ENDPOINT:-}" ]]; then
  run_minio
else
  echo "minio not run: DTRUNTIME_S3_ENDPOINT not set" > "$ROOT/minio-managed/result.json"
fi

echo "Integration results: $ROOT"
