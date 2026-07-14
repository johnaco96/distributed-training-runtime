# Integrating an existing PyTorch training script

This guide shows how to take a training script you already have and run it on
the Distributed Training Runtime: coordinator-backed rendezvous, DDP/FSDP/
DeepSpeed, distributed checkpointing, and (optionally) the streaming data
pipeline and elastic recovery.

The integration is intentionally small — a handful of lines around your existing
model/optimizer/loop. It complements [torch-distributed.md](torch-distributed.md)
(rendezvous mechanics), [checkpointing-distributed.md](checkpointing-distributed.md),
and [data-pipeline.md](data-pipeline.md).

## Prerequisites

```bash
pip install dtruntime            # the SDK
pip install torch                # your existing torch install
```

A coordinator must be reachable (address `host:50051`). For local runs see
[quickstart.md](quickstart.md); for clusters see
[PATH-TO-PRODUCTION.md](PATH-TO-PRODUCTION.md).

## Step 1 — initialize the context instead of `init_process_group`

Replace your manual `torch.distributed.init_process_group(...)` (or `torchrun`
bootstrap) with `dtruntime.torch_dist.init()`. It registers the worker, starts
heartbeats, performs the coordinator rendezvous, and initializes the process
group for you.

```python
from dtruntime.torch_dist import init

ctx = init()          # reads DTRUNTIME_* env injected by the launcher
rank, world_size = ctx.rank, ctx.world_size
device = f"cuda:{ctx.local_rank}" if torch.cuda.is_available() else "cpu"
```

`init()` picks up `coordinator_addr`, `run_id`, `worker_id`, and backend from the
`DTRUNTIME_*` environment variables the launcher sets; pass them explicitly if
you bootstrap workers yourself.

## Step 2 — wrap your model

Use `ctx.wrap_model(...)` with the strategy you want. Your model construction is
otherwise unchanged.

```python
model = MyModel().to(device)
model = ctx.wrap_model(model, strategy="ddp")   # "ddp" | "fsdp" | "deepspeed" | "none"
optimizer = torch.optim.AdamW(model.parameters(), lr=3e-4)
```

- `ddp` → `DistributedDataParallel` (device from `ctx.local_rank` on CUDA).
- `fsdp` → `FullyShardedDataParallel`.
- `deepspeed` → `deepspeed.initialize` (pass DeepSpeed kwargs through).

## Step 3 — restore + checkpoint through the coordinator

Swap your ad-hoc `torch.save`/`load` for `DistributedCheckpointer`, which does a
collective DCP save, uploads artifacts, and commits them via 2-phase commit
through the coordinator (so a crash resumes from the last *globally committed*
checkpoint). See [checkpointing-distributed.md](checkpointing-distributed.md).

```python
from dtruntime.checkpointing import DistributedCheckpointer

ckpt = DistributedCheckpointer(ctx)
restored = ckpt.restore(model, optimizer)          # None if no committed checkpoint
start_step = (restored.step + 1) if restored else 0

for step in range(start_step, total_steps):
    train_one_step(model, optimizer, batch)
    if step % 500 == 0:
        ckpt.save(model, optimizer, step=step)     # blocks until globally committed
        # or: ckpt.save_async(model, optimizer, step=step)
```

## Step 4 — shut down cleanly

```python
ctx.shutdown()   # deregister (graceful), stop heartbeats, destroy the group
```

## Step 5 — launch it

Use the launcher (a torchrun-style supervisor) — it registers the run, injects
the `DTRUNTIME_*` env, and spawns `--nproc-per-node` processes:

```bash
python -m dtruntime.launch \
  --coordinator coordinator:50051 \
  --run-name my-run \
  --nproc-per-node 8 \
  train.py -- --epochs 10 --batch-size 64
```

Everything after `train.py` (or after `--`) is passed through to your script.
Multi-node: run the same command on each node with a shared `--run-name`; the
launcher registers the run idempotently and each node's workers join the same
rendezvous.

## Minimal before/after

```diff
- torch.distributed.init_process_group("nccl")
- rank = torch.distributed.get_rank()
- model = DDP(MyModel().cuda(), device_ids=[local_rank])
+ from dtruntime.torch_dist import init
+ from dtruntime.checkpointing import DistributedCheckpointer
+ ctx = init()
+ rank = ctx.rank
+ model = ctx.wrap_model(MyModel().cuda(), strategy="ddp")
+ ckpt = DistributedCheckpointer(ctx)
+ restored = ckpt.restore(model, optimizer)
  ...
- torch.distributed.destroy_process_group()
+ ctx.shutdown()
```

## Optional — elastic training (survive worker failures/preemption)

For runs that should tolerate workers joining/leaving (spot instances, node
failures), structure the loop with `run_elastic`. Build the model/optimizer
*inside* `train_fn` so they bind to each freshly-formed process group, and call
`raise_if_membership_changed()` at step boundaries:

```python
def train_fn(ctx):
    model = ctx.wrap_model(MyModel().to(device), strategy="ddp")
    optimizer = torch.optim.AdamW(model.parameters(), lr=3e-4)
    ckpt = DistributedCheckpointer(ctx)
    restored = ckpt.restore(model, optimizer)
    start = (restored.step + 1) if restored else 0
    for step in range(start, total_steps):
        train_one_step(model, optimizer, next_batch(ctx.rank, ctx.world_size))
        ctx.raise_if_membership_changed()          # re-rendezvous if the fleet changed
        if step % 500 == 0:
            ckpt.save(model, optimizer, step=step)

ctx.run_elastic(train_fn)
```

Launch with elastic bounds so the run keeps going within a worker range:

```bash
python -m dtruntime.launch --coordinator coordinator:50051 --run-name my-run \
  --nproc-per-node 8 --min-workers 6 --max-workers 16 train.py
```

On a membership change the process group is torn down and re-formed (possibly at
a new world size and rank); `train_fn` re-runs and restores from the latest
committed checkpoint — so **checkpoint regularly**. See
[run-lifecycle.md](run-lifecycle.md) and
[failure-recovery.md](failure-recovery.md).

## Optional — the streaming data pipeline

To read sharded datasets with deterministic per-epoch shuffling, coordinator-
assigned shards, and a local disk cache, use `DistributedShardDataset` with a
PyTorch `DataLoader` (drop-in `IterableDataset`). See
[data-pipeline.md](data-pipeline.md).

```python
from dtruntime.data import DistributedShardDataset
dataset = DistributedShardDataset(client=ctx.client, run_id=ctx.run_id,
                                  worker_id=ctx.worker_id, dataset_id="my-dataset")
loader = torch.utils.data.DataLoader(dataset, batch_size=64, num_workers=4)
```

## What changes vs. plain torchrun

| Concern | Plain torch.distributed / torchrun | With dtruntime |
|---|---|---|
| Rendezvous | c10d store / etcd you operate | Coordinator-backed, no extra store |
| Rank assignment | static, launcher-assigned | Coordinator-assigned, stable across rejoins |
| Elastic membership | manual / limited | `run_elastic` + coordinator failure detection |
| Checkpoint durability | your `torch.save` | DCP + 2PC commit + corruption fallback |
| Data sharding | your `DistributedSampler` | Optional coordinator shard assignment + cache |
| Observability / admin | none | metrics, audit, `dtruntime-admin` |

## Troubleshooting

- **`coordinator_addr is required`** — run under `python -m dtruntime.launch`, or
  pass `init(coordinator_addr=..., run_id=..., worker_id=...)`.
- **Rendezvous times out** — check worker↔coordinator connectivity and that the
  expected number of workers actually started; inspect with
  `dtruntime-admin doctor <run>`.
- **Training error re-raised after a collective failure** — a `RuntimeError`
  that is *not* confirmed as a membership change within the grace window is a
  genuine bug and propagates (by design). See
  [troubleshooting.md](troubleshooting.md).
