# Support matrix

What the Distributed Training Runtime supports today, and the explicit limits.
"Validated" means exercised by automated tests and/or local runs; "documented"
means implemented to a stable contract but not run against the live external
system. See [status-and-limitations.md](status-and-limitations.md) for the
authoritative non-goals.

## Training frameworks

| Framework / strategy | Status | Notes |
|---|---|---|
| PyTorch DDP | Validated | `ctx.wrap_model(strategy="ddp")`; multi-process gloo (CPU) validated locally and on kind. |
| PyTorch FSDP | Validated (build/unit) | `strategy="fsdp"`; requires an FSDP-capable torch. |
| DeepSpeed | Documented | `strategy="deepspeed"`; `deepspeed.initialize` passthrough; requires deepspeed installed. |
| Plain / custom | Supported | `strategy="none"` — you manage parallelism; still gets rendezvous + checkpoint + admin. |
| torch.distributed backends | gloo (validated), NCCL (documented) | NCCL uses the same rendezvous; not yet cluster-proven on multi-node GPU. |
| PyTorch versions | ≥ 2.0 recommended | DCP checkpointing requires `torch.distributed.checkpoint`. |

Rendezvous is coordinator-backed (no external etcd/c10d store to operate). See
[torch-distributed.md](torch-distributed.md) and
[pytorch-integration.md](pytorch-integration.md).

## Storage backends (checkpoints & artifacts)

| Backend | Status | Notes |
|---|---|---|
| Local filesystem (`file://`) | Validated | Default; `STORAGE_BACKEND_KIND_LOCAL_FILESYSTEM`. |
| S3-compatible (`s3://`) | Validated vs MinIO | `STORAGE_BACKEND_KIND_S3_COMPATIBLE`; multipart, retry/jitter, error classification, SSE. Not run against AWS S3 at scale. |

See [storage-hardening.md](storage-hardening.md), [object-storage.md](object-storage.md).

## Metadata store

| Store | Status | Use |
|---|---|---|
| SQLite | Validated | Single-node / dev; WAL + versioned migrations. |
| PostgreSQL | Validated (local) | Production; HA via advisory lock, durable metadata. |

See [metadata-persistence.md](metadata-persistence.md), [schema-migrations.md](schema-migrations.md).

## Schedulers / orchestration

| System | Status | Integration |
|---|---|---|
| Kubernetes Jobs (`kubernetes`) | Validated on kind | Default operator backend (Indexed Jobs). |
| Volcano (`volcano`) | Documented | Gang scheduling; rendered to Volcano's contract. Meaningful gang proof needs a multi-node cluster (see `tools/evidence/multinode`). |
| Kueue (`kueue`) | **Validated vs live Kueue** | Queue admission proven against Kueue v0.8.1: the operator-rendered Jobs are quota-admitted/gated (`evidence/schedulers/`). |
| Slurm | Documented (submitter) | `srun python -m dtruntime.launch …`. |
| Ray | Documented (submitter) | Actors call the launcher / `torch_dist.init()`. |
| SageMaker | Documented (submitter) | Container entrypoint execs the launcher. |
| AWS Batch | Documented (submitter) | Multi-node parallel job runs the launcher. |

The launcher contract (`python -m dtruntime.launch`) is the stable extension
point for out-of-cluster submitters. See [schedulers.md](schedulers.md),
[kubernetes-operator.md](kubernetes-operator.md).

## Dataset formats (streaming data pipeline)

| Format | Notes |
|---|---|
| `npz` | NumPy archives (default). |
| `dtrs` | Native runtime shard format. |
| `jsonl` | JSON Lines. |
| `parquet` | Requires the `[parquet]` extra (pyarrow). |
| `webdataset` | tar-based WebDataset shards. |

Custom formats via `dtruntime.data.register_format`. See
[data-pipeline.md](data-pipeline.md).

## Platforms

| Dimension | Support |
|---|---|
| Coordinator/SDK host builds (CI) | Linux, macOS, Windows |
| Python SDK | 3.9 – 3.12, OS-independent |
| Container images | linux/amd64, linux/arm64 (multi-arch, signed + provenance) |
| Kubernetes | ≥ 1.24 |
| Accelerators | CPU (gloo, validated); NVIDIA GPU (NCCL, documented) |

Host build/unit tests run on all three OSes ([compat.yml](../.github/workflows/compat.yml));
production container images target Linux. See
[production-deployment.md](production-deployment.md).

## Security

| Capability | Status |
|---|---|
| TLS / mTLS | Validated |
| OIDC / JWT bearer auth | Validated (local, minted tokens) |
| SPIFFE X.509-SVID | **Validated vs real SPIRE** — a SPIRE-minted SVID authenticates over mTLS; unmapped SVID rejected (`evidence/spiffe/`) |
| RBAC (admin/operator/viewer/worker) | Validated |
| Audit → DB + NDJSON/SIEM | **Validated end-to-end** — ingested into Loki via Vector (`evidence/siem/`) |
| Secret/key hot rotation | Validated |

See [security-auth.md](security-auth.md), [threat-model.md](threat-model.md).

## Known limits and non-goals

- **Scale evidence is single-node/simulated** — control-plane benchmarks use
  simulated workers on a dev machine, not a real multi-node GPU cluster (see
  [benchmarks.md](benchmarks.md)). Local simulated scaling is **not** proof of
  cluster scale.
- **Not validated on a managed cloud cluster at scale**, nor against live
  Volcano / AWS-S3 at scale. (SPIRE, Kueue, and SIEM ingestion **are** now proven
  against the real software locally — see `evidence/`.)
- **No web dashboard/UI** — the `dtruntime-admin` CLI is the operator surface.
- **NCCL multi-node GPU** uses the same rendezvous protocol but is not yet
  cluster-proven.
- **HA** is 2-replica leader/standby via a PostgreSQL advisory lock (not a
  multi-leader/consensus design).
- Nothing is auto-published — images/packages are built by a tag-triggered
  workflow the maintainer controls ([RELEASING.md](RELEASING.md)).

This is a portfolio/prototype-grade runtime focused on correct coordination and
recovery semantics; treat the above limits as hard boundaries when evaluating it
for production use.
