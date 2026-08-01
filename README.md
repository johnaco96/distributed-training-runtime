# Distributed Training Runtime

[![CI](https://github.com/johnaco96/distributed-training-runtime/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/johnaco96/distributed-training-runtime/actions/workflows/ci.yml)
[![Compatibility](https://github.com/johnaco96/distributed-training-runtime/actions/workflows/compat.yml/badge.svg?branch=main)](https://github.com/johnaco96/distributed-training-runtime/actions/workflows/compat.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](https://github.com/johnaco96/distributed-training-runtime/blob/main/LICENSE)

A **high-performance runtime for distributed PyTorch training** — a Rust core + Python SDK that manage worker rendezvous, rank assignment, sharded data loading, checkpointing to object storage, and elastic recovery when the launcher can replace a failed worker. The Python API stays a handful of lines. The control plane runs in Rust; optional native components accelerate shard I/O and worker-direct checkpoint transfer, with Python fallbacks when they are not built.

It is *not* a training framework (it uses `torch.distributed`) and *not* a GPU scheduler. It supplies coordination and data paths that an elastic training loop can use to replace a failed worker and restore the last committed checkpoint; work after that checkpoint is replayed.

**Measured in the documented local benchmark runs** (details, environment, and repro commands in [Performance](#performance-measured)): the coordinator handled **5,000 simulated workers at ~99k RPC/s** (p99 25 ms, zero failed RPCs); the optional Rust data plane loaded shards at **~1M records/s (2 GB/s), 5.8× the pure-Python path**, with a zero-copy buffer handoff; and the worker-direct checkpoint path restored from S3 after its local staging files were deleted. Results are not multi-node production measurements.

## See it work in one command

```bash
pip install -e ".[dev]" && pip install torch      # one-time setup
python demo/run_demo.py
```

This starts the Rust coordinator, launches a **real 2-worker gloo DDP training job** with checkpointing, **crashes one worker mid-training**, and shows the survivors re-form the process group and resume from the last checkpoint. All local, all CPU, ~30 seconds:

```
[3/3] Verifying recovery from the workers' reports...
      [OK] worker 1 crashed at step 8 (injected)
      [OK] survivors re-rendezvoused and re-formed the process group
      [OK] resumed from checkpoint step 4
      [OK] both ranks finished all 20 steps (2/2 reports)

 SUCCESS - the run survived a worker crash and finished.
```

## Real training numbers

The 30-second demo above uses a toy model and no dataset. `demo/run_benchmark.py` exercises the same recovery path with torchvision ResNet-18 and CIFAR-10 across 2 local gloo DDP workers. It terminates one worker during training and resumes from the last distributed checkpoint:

```bash
pip install -e ".[demo]"          # adds torch + torchvision
python demo/run_benchmark.py      # downloads CIFAR-10 once (~168 MB), then trains
```

Example output recorded from one laptop CPU run (2 workers × batch 32, 100 steps). The script writes a fresh `results.json`; the original result file is not tracked, so treat these figures as illustrative rather than a reproducible baseline:

| Metric | Measured |
|---|---|
| Throughput | **128 img/s** aggregate · 506 ms/step/rank |
| Loss (start → end) | **2.28 → 1.37** |
| Accuracy (start → end) | **0.19 → 0.49** — vs. 0.10 random baseline |
| Distributed checkpoint (DCP) | **~760 ms**, ~135 MB each |
| Fault injected | worker killed at step 60 |
| Recovered from | committed checkpoint at step 49, in **~0.7 s** |
| Total wall time | **~104 s** (incl. crash + rebuild + resume) |

This benchmark evaluates recovery behavior rather than model quality. In the recorded run, the launcher replaced the failed local worker and the elastic training loop restored the last globally committed checkpoint. Training then replayed the work performed after that checkpoint.

## Performance (measured)

Every number below has a reproduction command and an environment disclosure in its linked writeup — and where a result has a caveat, the writeup says so.

**Control plane at scale** — [benchmark methodology and results](https://github.com/johnaco96/distributed-training-runtime/blob/main/docs/benchmarks/control-plane-scale.md) (WSL2 loopback, simulated protocol-level workers):

| | |
|---|---|
| 5,000 workers, 1 s heartbeats | full offered load, **0 failed RPCs**, p99 **12.7 ms** |
| 5,000 workers, 100 ms heartbeats | **~99k RPC/s sustained** (full offered load), p99 **24.9 ms** |
| Coordinator memory at that load | **56 MB** RSS |
| How it got there | `perf` flamegraph found two O(workers) roster scans running on *every* heartbeat; fixing them took throughput from 40k → 99k RPC/s (**2.4×**) |

The load generator runs every simulated worker as its own concurrent task over pooled HTTP/2 channels with a bounded in-flight budget, so the offered load is exactly what the flags specify — the writeup details the methodology.

**Rust data plane** — [benchmark methodology and results](https://github.com/johnaco96/distributed-training-runtime/blob/main/docs/benchmarks/data-plane-throughput.md) (local NVMe; s3:// leg vs MinIO):

| | Pure Python | Rust (`_native`) |
|---|---|---|
| Batch shard load | 172k rec/s | **999k rec/s (2.0 GB/s) — 5.8×** |
| Prefetching stream, disk, incl. SHA-256 | — | **1.23M rec/s** |
| Prefetching stream, S3 (MinIO), incl. SHA-256 | — | **792 MB/s** |

**Checkpoint durability** — [distributed checkpoint design](https://github.com/johnaco96/distributed-training-runtime/blob/main/docs/checkpointing-distributed.md): `AsyncCheckpointer` measures the synchronous snapshot time as `t_stall_s`, then serializes and transfers in the background. Worker-direct S3 transfer uses the Rust uploader when built and boto3 otherwise; 2PC itself is coordinator metadata. MinIO-gated tests delete local staging before restore and reject a tampered S3 object by checksum.

## The problem it solves

Distributed training requires coordination for worker membership, rank assignment, failure handling, and checkpoint selection. This runtime centralizes those responsibilities in a durable service:

- **Rendezvous & rank assignment** — workers find each other through the coordinator (no external etcd to operate); the launcher preserves worker identities and ranks when possible.
- **Elastic failure recovery** — a dead worker is detected via heartbeats; survivors re-rendezvous and resume. (That's the demo.)
- **Distributed checkpointing with 2-phase commit** — a checkpoint becomes committed only when every expected worker's artifacts and commit vote are in. `DistributedCheckpointer` selects globally committed state and can report corruption and retry an older committed generation.
- **Worker-direct S3 checkpoints** — with `AsyncCheckpointer`, artifact bytes move directly between the worker and object storage (the coordinator holds metadata + commit state). The optional Rust uploader handles both directions when built; boto3 is the fallback. MinIO-gated tests cover loss of local staging and tamper detection.
- **Coordinator durability** — run/worker state is persisted (SQLite or PostgreSQL), so the coordinator can restart without losing the job.
- **Sharded data, decoded in Rust** — deterministic per-rank shard assignment with a local cache, and a native module (`dtruntime._native`) that fetches, integrity-checks, and batch-decodes shards with the GIL released, handing tensors to PyTorch **zero-copy** through the buffer protocol. Pure-Python fallback included (`DTRUNTIME_NATIVE=0`), asserted bit-identical.

## Architecture

```
   Python training workers                     Rust coordinator (control plane)
  ┌──────────────────────────────┐  gRPC/proto  ┌──────────────────────────────┐
  │ dtruntime SDK (Python API)   │◄────────────►│ rendezvous state machine     │
  │  torch_dist  (rendezvous, DDP/FSDP)         │ run/worker lifecycle + leases│
  │  checkpointing / async_checkpoint (2PC)     │ 2-phase checkpoint commit    │
  │  data        (IterableDataset)              │ failure detection / recovery │
  │  launch      (torchrun-style supervisor)    │ durable metadata (SQLite/PG) │
  ├──────────────────────────────┤              └──────────────────────────────┘
  │ Optional Rust data movers    │                     portable DCP artifacts
  │  _native: shard fetch+decode,│                     can cross coordinator;
  │  zero-copy buffers, prefetch │                     worker-direct S3 bytes
  │  runtime-uploader: checkpoint│                     bypass it
  │  stream up/down + sha256     │
  └──────────────┬───────────────┘
                 │ artifact bytes (streamed, verified)
          ┌──────▼──────┐
          │ local / S3  │  (checkpoints, shards — MinIO or any S3 API)
          └─────────────┘
```

## Why this tech stack

| Choice | Why |
|---|---|
| **Rust** for the coordinator | The always-on control plane benefits from memory safety, concurrency, and predictable latency. Built on `tokio` + `tonic`. In the documented loopback benchmark, 5,000 simulated workers used 56 MB peak RSS. |
| **Rust** for the I/O hot paths | Shard decode and checkpoint transfer are byte-moving loops where Python pays per-object overhead on every record. The PyO3 extension decodes with the GIL released and hands PyTorch a zero-copy view (5.8× measured); the uploader streams artifacts to/from S3 without buffering them in RAM. |
| **Python** for the SDK | ML engineers and PyTorch live in Python. The SDK meets them there, wrapping `torch.distributed`, Distributed Checkpoint (DCP), and `DataLoader` so integrating is a handful of lines (see `docs/pytorch-integration.md`). Every Rust acceleration hides behind the same API with a pure-Python fallback. |
| **gRPC + Protobuf** | A strongly typed, versioned contract between the Rust control plane and Python workers. The compatibility snapshot detects incompatible changes to the protocol. |
| **`torch.distributed` (gloo/NCCL)** | PyTorch provides the collective operations; this runtime provides coordination around them. |
| **SQLite → PostgreSQL** | SQLite for zero-dependency local/dev; PostgreSQL for durable metadata and advisory-lock leader/standby operation. The coordinator rehydrates its state on restart from either backend; real multi-replica failover has not been validated here. |

## Repository layout

```
crates/
  runtime-coordinator   # the service: rendezvous, lifecycle, leases, 2PC checkpoint, gRPC
  runtime-proto         # the gRPC/protobuf contract
  runtime-storage       # object-store abstraction (local + S3: streaming put/get, multipart)
  runtime-checkpoint    # checkpoint artifact/manifest logic
  runtime-uploader      # streams checkpoint artifacts worker <-> S3 (subprocess data mover)
  runtime-bench         # gRPC load generator (thousands of simulated workers, p50/p99)
  dtruntime-native      # PyO3 extension: shard fetch + decode + prefetch, zero-copy tensors
python/dtruntime/       # the Python SDK (torch_dist, checkpointing, data, launch, client, admin CLI)
demo/                   # run_demo.py (30s toy recovery) + run_benchmark.py (real ResNet-18/CIFAR-10)
benchmarks/             # dataloading A/B bench + 3-VM multinode checkpoint harness
docs/                   # architecture, integration, checkpointing, data, lifecycle + measured benchmarks
scripts/                # build_native.py, flamegraph + Linux profiling tooling
```

## Try more

```bash
cargo test --workspace --exclude dtruntime-native    # Rust tests
pytest python/tests                                  # Python tests
python scripts/build_native.py                       # build the Rust data-plane accelerator (CPython >= 3.11)
python benchmarks/dataloading/bench_dtrs.py          # A/B: pure Python vs Rust shard loading
cargo run --release -p runtime-bench -- logical-workers \
    --workers 5000 --duration-secs 30 --concurrency 1024 \
    --heartbeat-interval-ms 100 --start-coordinator # the 5000-worker control-plane benchmark
python -m dtruntime.launch --help                    # the torchrun-style launcher
dtruntime-admin --help                               # inspect/manage runs from the CLI
```

Integrate your own script in a few lines: **[PyTorch integration guide](https://github.com/johnaco96/distributed-training-runtime/blob/main/docs/pytorch-integration.md)**.
Contributions are welcome; see the [contribution guide](https://github.com/johnaco96/distributed-training-runtime/blob/main/CONTRIBUTING.md). Please report
security issues through the [private reporting process](https://github.com/johnaco96/distributed-training-runtime/blob/main/SECURITY.md).

## Operational surface

Beyond the training path, the coordinator ships the pieces a control plane needs to be run by someone other than its author:

| | |
|---|---|
| **Authentication** | Layered and enforced per-RPC: OIDC/JWT bearer tokens (signature, `exp`/`nbf`, issuer/audience, algorithm pinning, JWKS or HMAC key sets with **hot reload** for secret rotation) → SPIFFE X.509-SVID identity from the verified mTLS peer certificate → dev metadata headers, which hardened deployments disable. |
| **Authorization** | Role/permission RBAC (`admin`/`operator`/`viewer`/`worker`) with worker-self scoping, so a worker can only act on its own identity. |
| **Durable metadata** | SQLite (WAL) or PostgreSQL, with a versioned migration ledger; the coordinator rehydrates full run/worker/checkpoint state on restart. |
| **HA** | PostgreSQL advisory-lock leader election — standby replicas don't bind the gRPC port, so a readiness probe routes traffic only to the leader. |
| **Observability** | Prometheus `/metrics`, `/healthz`, `/readyz`, and an NDJSON audit log of every authorization decision. |
| **Admin** | `dtruntime-admin` — inspect runs/workers/shards/checkpoints, pause/resume/retry/drain, verify checkpoint integrity, and a `doctor` diagnostic. |

## Scaling to production

This branch is a prototype validated locally rather than against a managed cloud identity provider or multi-node cluster. The separate [`production-hardening`](https://github.com/johnaco96/distributed-training-runtime/tree/production-hardening) branch contains broader Kubernetes, operator, observability, security-evidence, and release assets. That branch is also prototype-grade and has not been validated as a production deployment. See the **[path to production](https://github.com/johnaco96/distributed-training-runtime/blob/main/docs/PATH-TO-PRODUCTION.md)** for details.

## Status and limitations

Local validation covers multi-process gloo training, checkpoint restore, MinIO object storage, and `perf`-profiled load testing with 5,000 simulated workers. The scale measurements use a single machine, loopback networking, and protocol-level workers; they do not represent cluster performance. Multi-node GPU training and managed-cloud deployment have not been validated. This is a portfolio/reference implementation focused on coordination, recovery semantics, and measured local performance.
