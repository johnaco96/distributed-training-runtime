# Multi-node checkpoint benchmark: dtruntime vs torchrun + DCP

Measures **checkpoint stall time**, **end-to-end durability time**, and
**recovery time** for multi-GB model state, comparing:

- **dtruntime** — this repo's coordinator + `AsyncCheckpointer` staged pipeline
  (snapshot → local staging → MinIO upload → 2PC commit)
- **torchrun + PyTorch DCP** — `dcp.async_save` / `dcp.save` to local disk,
  the standard OSS baseline

All numbers are CPU-tensor state; this benchmark isolates the checkpoint data
path and coordination, not GPU math.

## Topology (3 VMs, e.g. 8 vCPU / 32 GB each)

| Host | Runs |
|---|---|
| `storage` | PostgreSQL + MinIO (`storage-compose.yml`) |
| `coord`   | runtime-coordinator (Postgres-backed, HA lock) |
| `worker1`..`workerN` | one `bench_dtr.py` rank each; also the torchrun ranks |

## Setup

On every VM (Ubuntu 22.04+):

```bash
sudo apt-get update && sudo apt-get install -y build-essential protobuf-compiler libprotobuf-dev python3-venv docker.io
curl https://sh.rustup.rs -sSf | sh -s -- -y && source ~/.cargo/env
git clone https://github.com/johnaco96/distributed-training-runtime && cd distributed-training-runtime
cargo build --release -p runtime-coordinator -p runtime-uploader
python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]" && pip install torch --index-url https://download.pytorch.org/whl/cpu
```

**storage:** `docker compose -f benchmarks/multinode/storage-compose.yml up -d`

**coord:**

```bash
./target/release/runtime-coordinator \
  --addr 0.0.0.0:50051 \
  --metadata-db-url postgres://dtruntime:dtruntime-bench@<storage-ip>:5432/dtruntime \
  --storage-root /tmp/dtr-artifacts
```

**every worker:** export the S3 env once:

```bash
export DTRUNTIME_S3_ENDPOINT=http://<storage-ip>:9000
export DTRUNTIME_S3_ACCESS_KEY=dtruntime
export DTRUNTIME_S3_SECRET_KEY=dtruntime-bench
export DTRUNTIME_UPLOADER_BIN=$PWD/target/release/runtime-uploader
```

## Run — dtruntime (rank r on worker r)

```bash
python benchmarks/multinode/bench_dtr.py \
  --addr <coord-ip>:50051 --rank <r> --world-size 2 \
  --state-mb 2048 --checkpoints 3 \
  --dest s3://checkpoints/bench --staging-dir /nvme/dtr-staging \
  --run-key bench-$(date +%s) --out results/dtr-rank<r>.json
```

## Run — DCP baseline (same hosts, same state size)

```bash
torchrun --nnodes=2 --node-rank=<r> --nproc-per-node=1 \
  --master-addr=<worker1-ip> --master-port=29500 \
  benchmarks/multinode/bench_dcp.py \
  --state-mb 2048 --checkpoints 3 --ckpt-dir /nvme/dcp-ckpts \
  --mode async --out-prefix results/dcp
# repeat with --mode sync for the blocking baseline
```

## Failure injection

- **Worker recovery:** `kill -9` one `bench_dtr.py` rank mid-run, then rerun it
  with `--measure-recovery` (same `--run-key`). Reported as `recovery_s` —
  time from process start to the latest committed checkpoint restored.
  DCP equivalent: rerun `bench_dcp.py --measure-recovery`.
- **Coordinator recovery:** `kill -9` the coordinator on `coord`, restart the
  same command. Workers continue without re-registration (durable run state
  machine); time the gap with `time` around the restart plus the first
  successful RPC.

## Report

Collect every `results/*.json` onto one machine and:

```bash
python benchmarks/multinode/report.py results/ --out docs/perf/multinode-report.md
```

## Local dress rehearsal (single machine, Docker)

Validates the entire harness before spending VM time. Numbers are
loopback-only — never publish them as the multi-node results.

```bash
docker compose -f benchmarks/multinode/storage-compose.yml up -d
./target/release/runtime-coordinator --addr 127.0.0.1:50061 \
  --metadata-db-url postgres://dtruntime:dtruntime-bench@127.0.0.1:5432/dtruntime \
  --storage-root /tmp/dtr-artifacts &
# two ranks, one terminal each (or background), then the DCP baseline, then report.py
```
