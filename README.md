# Distributed Training Runtime

A coordinator service for distributed ML training jobs. Workers register, receive shard assignments, and commit checkpoints through a gRPC API. The coordinator handles fault detection, shard lease management, crash recovery, and durable state so your training loop doesn't have to.

## Why

Large-scale training jobs need infrastructure that PyTorch and your training loop don't provide:

- **Which worker loads which data?** The coordinator assigns shards deterministically by rank with cache-locality hints. No two workers load the same data, and restarts are reproducible.
- **What happens when a worker crashes mid-epoch?** Per-shard progress is persisted. On restart, workers resume from their last reported record offset — no data is re-processed or skipped.
- **How do you safely commit a checkpoint across many workers?** Two-phase commit: each worker votes independently; the checkpoint becomes globally committed only when all votes are in.
- **What if the coordinator itself crashes?** A standby replica holds a PostgreSQL advisory lock and takes over within ~5 seconds. The Kubernetes readiness probe routes traffic only to the leader.

## Quick start

```bash
# Build the coordinator
cargo build --release -p runtime-coordinator

# Run with SQLite (no external dependencies)
./target/release/runtime-coordinator \
  --addr 0.0.0.0:50051 \
  --metadata-db /tmp/dtr.db \
  --storage-root /tmp/dtr-artifacts
```

```python
from pathlib import Path

import numpy as np

from dtruntime.client import connect
from dtruntime.generated import runtime_pb2
from dtruntime.shard_loader import ShardLoader, build_and_write_shards

client = connect("localhost:50051")

# Register a run and this worker
run = client.register_run(
    run_name="quickstart",
    world_size=1,
    assignment_policy=runtime_pb2.ASSIGNMENT_POLICY_STATIC_RANK,
    storage_backend_kind=runtime_pb2.STORAGE_BACKEND_KIND_LOCAL_FILESYSTEM,
    idempotency_key="quickstart-run",
)
run_id = run.run_id.value
worker = client.register_worker(run_id=run_id, worker_id="worker-0", rank=0)
worker_id = worker.worker_id.value

# Write shard files and register the dataset
features = np.random.rand(10000, 128).astype(np.float32)
labels = np.random.randint(0, 10, 10000).astype(np.int64)
shards = build_and_write_shards(Path("/data/shards"), features, labels, shard_count=16)
client.register_dataset(
    run_id=run_id,
    name="quickstart-data",
    uri="file:///data/shards",
    format="npz",
    shards=shards,
    dataset_id="dataset",
)

# Load shards in the training loop
loader = ShardLoader(client, run_id, worker_id, "dataset")

while True:
    shard_ids, features, labels = loader.load_next_shard_as_tensors()
    if not shard_ids:
        break
    # train on features, labels ...
```

See `python/examples/` for complete working examples including PyTorch integration, multi-worker orchestration, and checkpoint save/restore.

## Features

| Feature | Details |
|---|---|
| Shard assignment | Deterministic by rank; cache-aware placement; configurable shard count |
| Crash recovery | Per-shard progress offset persisted to SQLite or PostgreSQL; workers resume on restart |
| Distributed checkpointing | Two-phase commit; globally committed only when all registered workers vote |
| Coordinator HA | PostgreSQL advisory lock leader election; standby takeover in ~5 s |
| S3-compatible storage | Checkpoint artifacts to any S3-compatible store (MinIO, AWS S3); configurable concurrency |
| TLS / mTLS | cert-manager-compatible; ECDSA P-256 certs with automatic rotation |
| Role-based auth | Admin vs. worker identity enforced on all RPCs |
| Audit logging | Structured events written to PostgreSQL for security-relevant operations |
| Observability | Prometheus `/metrics`, `/healthz`, `/readyz` on a separate HTTP port |

## Architecture

```
┌─────────────────────────────────────────────────────┐
│  Python training workers (rank 0 … N-1)             │
│  dtruntime.client  ──gRPC──►  runtime-coordinator   │
│  dtruntime.shard_loader        (Rust, tokio, tonic) │
└─────────────────────────────────────────────────────┘
                            │
             ┌──────────────┴──────────────┐
             │                             │
      SQLite (dev)               PostgreSQL (prod + HA)
      shard_progress              advisory lock
      checkpoint_commits          audit_events
      audit_events
```

The coordinator is a single Rust binary. Workers connect over gRPC and interact through a small set of RPCs:

| RPC | Purpose |
|---|---|
| `RegisterRun` | Start a training run, declare world size |
| `RegisterWorker` | Register a worker, receive a stable worker ID |
| `RegisterDataset` | Upload shard metadata (URIs, sizes, checksums) |
| `GetShardAssignment` | Request the next shard(s) for this worker |
| `ReportShardProgress` | Persist per-shard progress offset |
| `RenewShardLease` | Extend lease on an in-progress shard |
| `Heartbeat` | Signal liveness; coordinator marks workers failed after deadline |
| `BeginCheckpoint` | Open a checkpoint transaction |
| `ReportCheckpointArtifact` | Register an artifact (name, URI, size, checksum) |
| `CommitCheckpoint` | Cast a commit vote; checkpoint goes global when all workers vote |
| `GetLatestCheckpoint` | Fetch the last globally committed checkpoint manifest |

## Deployment

### Development (SQLite, no TLS)

```bash
./target/release/runtime-coordinator \
  --addr 0.0.0.0:50051 \
  --metadata-db /tmp/dtr.db \
  --storage-root /tmp/dtr-artifacts
```

### Production (PostgreSQL, TLS, HA)

```bash
./target/release/runtime-coordinator \
  --addr 0.0.0.0:50051 \
  --metadata-db-url "postgresql://user:pass@postgres/dtruntime" \
  --tls-cert /tls/tls.crt \
  --tls-key /tls/tls.key \
  --max-s3-concurrent-ops 128
```

Deploy two replicas. One acquires the PostgreSQL advisory lock and binds the gRPC port; the other polls in standby. The Kubernetes readiness probe (TCP on port 50051) keeps the standby out of the Service endpoints until it takes over.

### Kubernetes

```bash
# Install cert-manager, then apply the PKI resources
kubectl apply -f k8s/prod/cert-manager/cluster-issuer.yaml
kubectl apply -n dtruntime -f k8s/prod/cert-manager/coordinator-certificate.yaml

# Deploy the 2-replica coordinator
kubectl apply -f k8s/prod/coordinator-deployment.yaml
```

See [docs/kubernetes-local.md](docs/kubernetes-local.md) for the full local cluster runbook using Docker Desktop or kind.

## Python SDK

```bash
pip install -e python/
```

Key entry points:

- **`RuntimeClient`** (`dtruntime.client`) — gRPC client with TLS, mTLS, and auth header support. Reads credentials from constructor args or `DTRUNTIME_TLS_*` / `DTRUNTIME_AUTH_*` env vars.
- **`ShardLoader`** (`dtruntime.shard_loader`) — request shard assignment, read bytes from `file://` or `s3://` URIs, report progress, all in one call.
- **`build_and_write_shards`** (`dtruntime.shard_loader`) — split a numpy dataset into `.npz` shard files and return the metadata list ready to pass to `register_dataset`.

## Building from source

**Requirements:** Rust 1.75+, Python 3.10+, Protocol Buffers compiler (`protoc`)

```bash
# Rust workspace
cargo build --workspace
cargo test --workspace

# Python package and tests
pip install -e "python/[dev]"
PYTHONPATH=python python -m pytest python/tests

# Regenerate protobuf bindings after editing proto/runtime.proto
python scripts/generate_python_proto.py
```

## Benchmarking

```bash
# Compare standalone vs coordinator-driven PyTorch training
PYTHONPATH=python python python/examples/benchmark_pytorch_orchestration.py \
  --runs 3 --epochs 2 --samples 512 --batch-size 32 --workers 4 --shards 8

# Simulate 1000 logical workers against a live coordinator
cargo run -p runtime-bench -- logical-workers \
  --workers 1000 --duration-secs 60 --concurrency 128 \
  --shards 10000 --checkpoint-every-secs 30 \
  --out-json results.json --out-csv results.csv
```

## Documentation

- [Quickstart](docs/quickstart.md)
- [Architecture](docs/architecture.md)
- [Dataset sharding](docs/dataset-sharding.md)
- [Checkpoint protocol](docs/checkpoint-protocol.md)
- [Failure recovery](docs/failure-recovery.md)
- [High availability](docs/high-availability.md)
- [Security architecture](docs/security-architecture.md)
- [Observability](docs/observability.md)
- [Kubernetes local runbook](docs/kubernetes-local.md)
- [Benchmarks](docs/benchmarks.md)
- [Troubleshooting](docs/troubleshooting.md)
