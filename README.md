# Distributed Training Runtime

A **high-performance runtime for distributed PyTorch training** — a Rust core + Python SDK that manage the parts PyTorch doesn't: worker rendezvous, rank assignment, sharded data loading, fault-tolerant checkpointing to object storage, and **automatic recovery when a worker dies**. The Python API stays a handful of lines; the byte-moving I/O paths (shard decode/fetch, checkpoint upload/restore) and the always-on control plane run in Rust.

It is *not* a training framework (it uses `torch.distributed`) and *not* a GPU scheduler. It's the coordination brain and data plane that let a training job survive worker crashes and resume from the last checkpoint without losing progress.

**Measured, reproducibly** (details + repro commands in [Performance](#performance-measured)): the coordinator sustains **5,000 concurrently heartbeating workers at ~99k RPC/s** (p99 25 ms, zero failed RPCs); the Rust data plane loads shards at **~1M records/s (2 GB/s), 5.8× the pure-Python path**, handing tensors to PyTorch zero-copy; checkpoint restore works from S3 alone after a node loses every local file.

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

The 30-second demo above uses a toy model so it needs no dataset. To prove the same machinery works with a **real model on real data**, `demo/run_benchmark.py` trains a genuine **torchvision ResNet-18 on the real CIFAR-10 dataset** across 2 gloo DDP workers, crashes a worker mid-training, and resumes from the last distributed checkpoint:

```bash
pip install -e ".[demo]"          # adds torch + torchvision
python demo/run_benchmark.py      # downloads CIFAR-10 once (~168 MB), then trains
```

Measured on a laptop CPU (2 workers × batch 32, 100 steps, `results.json` written alongside):

| Metric | Measured |
|---|---|
| Throughput | **128 img/s** aggregate · 506 ms/step/rank |
| Loss (start → end) | **2.28 → 1.37** — the model genuinely learns |
| Accuracy (start → end) | **0.19 → 0.49** — vs. 0.10 random baseline |
| Distributed checkpoint (DCP) | **~760 ms**, ~135 MB each |
| Fault injected | worker killed at step 60 |
| Recovered from | committed checkpoint at step 49, in **~0.7 s** |
| Total wall time | **~104 s** (incl. crash + rebuild + resume) |

These are *systems* numbers — the point isn't state-of-the-art accuracy (it's a sub-epoch CPU run), it's that a real DDP job **loses a worker and resumes from a globally-consistent checkpoint without losing progress**, and that failure detection is bounded (a configurable process-group timeout, not the 5-minute default).

## Performance (measured)

Every number below has a reproduction command and an environment disclosure in its linked writeup — and where a result has a caveat, the writeup says so.

**Control plane at scale** — [docs/benchmarks/control-plane-scale.md](docs/benchmarks/control-plane-scale.md) (WSL2 loopback, simulated protocol-level workers):

| | |
|---|---|
| 5,000 workers, 1 s heartbeats | full offered load, **0 failed RPCs**, p99 **12.7 ms** |
| 5,000 workers, 100 ms heartbeats | **~99k RPC/s sustained** (full offered load), p99 **24.9 ms** |
| Coordinator memory at that load | **56 MB** RSS |
| How it got there | `perf` flamegraph found two O(workers) roster scans running on *every* heartbeat; fixing them took throughput from 40k → 99k RPC/s (**2.4×**) |

The load generator runs every simulated worker as its own concurrent task over pooled HTTP/2 channels with a bounded in-flight budget, so the offered load is exactly what the flags specify — the writeup details the methodology.

**Rust data plane** — [docs/benchmarks/data-plane-throughput.md](docs/benchmarks/data-plane-throughput.md) (local NVMe; s3:// leg vs MinIO):

| | Pure Python | Rust (`_native`) |
|---|---|---|
| Batch shard load | 172k rec/s | **999k rec/s (2.0 GB/s) — 5.8×** |
| Prefetching stream, disk, incl. SHA-256 | — | **1.23M rec/s** |
| Prefetching stream, S3 (MinIO), incl. SHA-256 | — | **792 MB/s** |

**Checkpoint durability** — [docs/checkpointing-distributed.md](docs/checkpointing-distributed.md): training stalls only for the in-memory snapshot (`t_stall_s`); upload, 2PC commit, and restore stream through Rust. Verified against real MinIO: a worker whose staging disk is wiped restores from S3 alone, and a tampered S3 object fails the manifest checksum instead of loading bad state.

## The problem it solves

Run training across many processes/machines and someone has to answer: *Are all N workers here? What rank is each? One just crashed — restart, shrink, or fail? Where's the last good checkpoint, and is it intact?* Vanilla `torchrun` handles the happy path thinly. This runtime centralizes those decisions in a **durable, recoverable service**:

- **Rendezvous & rank assignment** — workers find each other through the coordinator (no external etcd to operate); ranks are stable across restarts.
- **Elastic failure recovery** — a dead worker is detected via heartbeats; survivors re-rendezvous and resume. (That's the demo.)
- **Distributed checkpointing with 2-phase commit** — a checkpoint becomes "committed" only when *every* worker's artifacts are in, so a crash always resumes from a globally-consistent state. Corrupt checkpoints are detected and fall back to the previous good one.
- **Worker-direct S3 checkpoints** — with `AsyncCheckpointer`, artifact bytes stream worker ↔ object storage through Rust in both directions (the coordinator holds only metadata + the commit); training stalls only for the in-memory snapshot, and a worker that loses its entire local disk restores from S3 with the manifest checksum verified. Tested against real MinIO, including tamper detection.
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
  │ Rust data plane (in-process) │                     metadata only — no
  │  _native: shard fetch+decode,│                     checkpoint/shard bytes
  │  zero-copy tensors, prefetch │                     cross the coordinator
  │  runtime-uploader: checkpoint│                     on the S3 path
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
| **Rust** for the coordinator | It's the always-on control plane that must *never* be the thing that takes the cluster down. Memory safety (no segfaults/GC pauses), fearless concurrency for thousands of workers heartbeating at once, and predictable low latency — exactly what a stateful control plane needs. Built on `tokio` + `tonic`. Measured: 5,000 heartbeating workers in 56 MB of RSS. |
| **Rust** for the I/O hot paths | Shard decode and checkpoint transfer are byte-moving loops where Python pays per-object overhead on every record. The PyO3 extension decodes with the GIL released and hands PyTorch a zero-copy view (5.8× measured); the uploader streams artifacts to/from S3 without buffering them in RAM. |
| **Python** for the SDK | ML engineers and PyTorch live in Python. The SDK meets them there, wrapping `torch.distributed`, Distributed Checkpoint (DCP), and `DataLoader` so integrating is a handful of lines (see `docs/pytorch-integration.md`). Every Rust acceleration hides behind the same API with a pure-Python fallback. |
| **gRPC + Protobuf** | A strongly-typed, versioned, cross-language contract between the Rust control plane and Python workers — the natural seam for a two-language system, and it makes backward-compatibility enforceable. |
| **`torch.distributed` (gloo/NCCL)** | Reuse PyTorch's battle-tested collectives. The runtime owns *coordination*, not the math — the right boundary. |
| **SQLite → PostgreSQL** | SQLite for zero-dependency local/dev; PostgreSQL for durable, HA metadata in production. The coordinator rehydrates its state on restart from whichever is configured. |

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

Integrate your own script in a few lines: **[docs/pytorch-integration.md](docs/pytorch-integration.md)**.

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

This branch is a **clean, focused prototype** — it proves the hard distributed-systems parts work, and everything above is validated locally, not against a managed cloud identity provider or a multi-node cluster. The remaining distance to a real cluster is mostly *deployment* surface: a Kubernetes operator, Helm packaging, SLO/alerting rules, image signing, and Volcano/Kueue scheduling integration. A **full implementation of all of it lives on the [`production-hardening`](../../tree/production-hardening) branch**; see **[docs/PATH-TO-PRODUCTION.md](docs/PATH-TO-PRODUCTION.md)** for the map from here to there.

## Status & honest limits

Validated locally with real multi-process (gloo) training, checkpoint restore, live MinIO object storage, and `perf`-profiled load testing at 5,000 simulated workers. The scale numbers are **single-machine, loopback, protocol-level** — they bound the control plane, not a cluster. **Not** yet run on a real multi-node GPU cluster or managed cloud — the same rendezvous/NCCL path is used, but that's the next milestone, not a proven claim. This is a portfolio/reference-grade runtime focused on correct coordination, recovery semantics, and measured performance.
