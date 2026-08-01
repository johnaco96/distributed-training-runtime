# Distributed checkpointing

`dtruntime.checkpointing.DistributedCheckpointer` saves and restores model and
optimizer state for real multi-process/multi-node training, built on
`torch.distributed.checkpoint` (DCP) with coordinator-managed storage and
two-phase commit. The checkpoint RPC surface itself is defined in
[proto/runtime.proto](../proto/runtime.proto).

## Usage

```python
from dtruntime import torch_dist
from dtruntime.checkpointing import DistributedCheckpointer

ctx = torch_dist.init()
checkpointer = DistributedCheckpointer(ctx)

def train(c):
    model = c.wrap_model(build_model(), strategy="ddp")   # or "fsdp"
    optimizer = make_optimizer(model)

    restored = checkpointer.restore(model, optimizer)     # collective
    start = restored.step + 1 if restored else 0

    for step in range(start, total_steps):
        c.raise_if_membership_changed()
        train_step(model, optimizer)
        if (step + 1) % 100 == 0:
            checkpointer.save(model, optimizer, step=step)  # collective

ctx.run_elastic(train)
```

All ranks call `save`/`restore` collectively (DCP exchanges write/read plans
over the process group). Each rank uploads its own shards to the coordinator;
the checkpoint becomes restorable only after **every** rank's artifacts are
stored and every rank has voted to commit (2PC).

## What each requirement maps to

| Requirement | How it works |
|---|---|
| Distributed model + optimizer state | `torch.distributed.checkpoint.state_dict.get_state_dict` normalises DDP/FSDP wrappers; DCP writes sharded state per rank |
| Restore across machines | Artifacts are fetched back through the coordinator (`FetchCheckpointArtifact` streaming RPC), so workers on any node can restore regardless of where the coordinator stores bytes (local disk or S3 write-through) |
| World-size compatibility | DCP's load planner reshards saved state onto the current world size — save from 2 ranks, restore into 1 or 4 (validated in e2e: save@2 → restore@1 with identical parameters) |
| Parallelism-strategy compatibility | The saved format is wrapper-agnostic (fully-qualified parameter names); save from DDP and load into FSDP or a plain module and vice versa |
| Partial-upload recovery | Per-artifact retries with idempotent re-streaming; an interrupted save leaves a non-committed checkpoint that is never selected for restore and is GC'd |
| Corruption detection & repair | Artifacts are checksummed at write (post-write verification), re-checksummed while streaming out, and verified against the manifest on download. A mismatch triggers `ReportCheckpointCorrupt`: the checkpoint is marked FAILED, the latest pointer falls back to the previous committed checkpoint, and restore retries automatically (validated in e2e with deliberately corrupted artifacts) |
| Retention / GC | `--checkpoint-keep-last <N>` on the coordinator keeps the newest N committed checkpoints per run; the reconcile task drops the older records **and reclaims their artifacts** — local files, and objects on the worker-direct S3 path through the coordinator's S3 client. Deletes are idempotent, so a sweep interrupted part-way converges on the next tick. A coordinator with no S3 backend configured logs the objects it cannot reach rather than dropping them silently |
| Async save | `save_async` stages state (DCP async staging where available) and runs upload + 2PC on a background thread; `handle.result()` waits for the global commit. For the fully staged worker-direct S3 pipeline see `AsyncCheckpointer` below |
| Performance | The multinode A/B harness (`benchmarks/multinode/bench_dtr.py`) measures per-checkpoint stall/local/committed timings against a torchrun+DCP baseline |

## Failure interactions

- A worker dying mid-save leaves the checkpoint uncommitted; survivors'
  commit votes never complete it, training recovers via re-rendezvous
  ([run-lifecycle.md](run-lifecycle.md)), and the next `save` uses a fresh
  checkpoint id. The partial checkpoint ages out via retention.
- After elastic membership changes, restore with the **new** world size —
  resharding is exactly the DCP path validated above.
- Restoring a run that has no committed checkpoint returns `None` (fresh
  start), so the same training script handles cold starts and resumes.

## Operational notes

- Checkpoint reads (`GetLatestCheckpoint`, `ListCheckpoints`,
  `FetchCheckpointArtifact`) are allowed for worker identities as well as
  admins — restore runs under worker credentials.
- `ListCheckpoints` surfaces all checkpoints with states for admin
  inspection; corrupt reports are audited (`CheckpointCorruptReported`).
- Large-scale note: routing checkpoint bytes through the coordinator is the
  portable default for small clusters. For S3-capable clusters, use the
  worker-direct path below — no checkpoint byte ever crosses the coordinator.

## Worker-direct S3 checkpoints (`AsyncCheckpointer`)

For large models, streaming every artifact through the coordinator is a
bottleneck. `dtruntime.AsyncCheckpointer` moves the bytes worker-side and
keeps the coordinator as pure metadata + 2PC commit authority:

```python
from dtruntime import AsyncCheckpointer

ckptr = AsyncCheckpointer(
    client, run_id, worker_id,
    staging_dir="/nvme/ckpt-staging",          # fast local disk
    dest="s3://my-bucket/runs/run-1",           # or file://... or None
)
handle = ckptr.save(state_dict, step=1000)      # blocks only for the snapshot
# ... training continues while staging/upload/commit run in the background ...
handle.wait()                                   # optional barrier
state, info = ckptr.restore_latest()            # local staging fast path,
                                                # else streamed back from S3
```

The pipeline: **stage 0** in-memory snapshot (the only training stall,
reported as `handle.t_stall_s`) → **stage 1** serialize + fsync to local
staging → **stage 2** stream to S3 via the Rust `runtime-uploader` binary
(`--download` reverses it for restore; boto3 is the fallback when the binary
isn't built) → **stage 3** report `{uri, size, sha256}` to the coordinator
and cast this worker's 2PC commit vote.

Restore prefers the local staged copy when its SHA-256 matches the committed
manifest; otherwise the artifact is streamed back from S3 (written to
`.part`, renamed on success) and verified against the manifest checksum —
a tampered or truncated object fails restore instead of loading bad state.
This is tested end-to-end against MinIO, including total loss of the local
staging directory (`python/tests/test_async_checkpoint.py`, gated behind
`DTRUNTIME_RUN_MINIO_TESTS=1`).

S3 configuration comes from `DTRUNTIME_S3_ENDPOINT`, `DTRUNTIME_S3_REGION`,
`DTRUNTIME_S3_ACCESS_KEY`, `DTRUNTIME_S3_SECRET_KEY` (MinIO: any non-AWS
endpoint switches to path-style addressing automatically). The Rust data
mover is auto-detected under `target/{release,debug}` or on `PATH`;
`DTRUNTIME_UPLOADER_BIN` overrides the location explicitly.
