# Distributed checkpointing

`dtruntime.checkpointing.DistributedCheckpointer` saves and restores model and
optimizer state for real multi-process/multi-node training, built on
`torch.distributed.checkpoint` (DCP) with coordinator-managed storage and
two-phase commit. It complements the low-level checkpoint RPC documentation
in [checkpoint-protocol.md](checkpoint-protocol.md).

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
| Retention / GC | `--checkpoint-keep-last <N>` on the coordinator keeps the newest N committed checkpoints per run; older records and their local artifacts are garbage-collected by the reconcile task. For S3-backed artifacts use bucket lifecycle rules until Phase-6 S3 delete support lands |
| Async save | `save_async` stages state (DCP async staging where available) and runs upload + 2PC on a background thread; `handle.result()` waits for the global commit |
| Performance | `python/tests/test_checkpoint_performance.py` (gate: `DTRUNTIME_RUN_PERF_TESTS=1`) pushes a ~200 MB model through save + restore and asserts sane throughput |

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
- Large-scale note: routing restore bytes through the coordinator is the
  portable default. For very large models on S3-capable clusters, point
  workers at shared storage instead — direct S3 read support is part of the
  Phase-6 storage hardening.
