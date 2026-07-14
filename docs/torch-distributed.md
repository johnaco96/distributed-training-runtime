# Real torch.distributed execution

The runtime launches and orchestrates real `torch.distributed` training:
workers form process groups through a coordinator-backed rendezvous, receive
coordinator-assigned ranks, and recover from worker death by re-rendezvousing
with rank replacement. Elastic runs grow and shrink between configured bounds.

## Quick start

Start the coordinator, then launch training on each node with the same
command:

```bash
python -m dtruntime.launch \
    --coordinator coordinator:50051 \
    --run-name my-run \
    --nproc-per-node 2 \
    --world-size 4 \
    train.py --epochs 10
```

`--world-size` is the total across all nodes (here: 2 nodes x 2 processes).
Every node's launcher registers the run with the same idempotency key, so they
converge on a single run; the coordinator assigns globally unique ranks as
workers join the rendezvous — there is no per-node rank arithmetic.

Inside `train.py`:

```python
import torch.nn as nn
from dtruntime import torch_dist

ctx = torch_dist.init()          # rendezvous + init_process_group
model = ctx.wrap_model(build_model(), strategy="ddp")   # or "fsdp", "deepspeed"

for step, batch in enumerate(loader):
    ctx.raise_if_membership_changed()   # cheap; enables elastic recovery
    ...

ctx.shutdown()
```

`torch_dist.init()` reads the `DTRUNTIME_*` environment variables the launcher
injects (coordinator address, run id, worker id, local rank), registers the
worker, starts a background heartbeat, joins the rendezvous, and calls
`torch.distributed.init_process_group` with the coordinator-elected TCPStore
endpoint. The backend defaults to `nccl` when CUDA is available and `gloo`
otherwise (override with `--backend` or `DTRUNTIME_TORCH_BACKEND`).

## Rendezvous protocol

1. Each worker calls `JoinRendezvous` with a reachable host address and a
   freshly bound free port (its TCPStore proposal).
2. The coordinator collects participants into a **round**. A fixed-size round
   completes when exactly `world_size` workers joined; an elastic round
   completes at `max_world_size` immediately, or `rendezvous_settle_timeout_ms`
   after `min_world_size` is reached.
3. On completion the coordinator assigns ranks — workers that held a rank in
   the previous round keep it when possible; replacements fill freed ranks —
   and the rank-0 participant's host/port becomes the c10d TCPStore endpoint.
4. Workers poll `GetRendezvousState` until the round completes, then call
   `init_process_group(init_method="tcp://<store_addr>:<store_port>", ...)`.

Membership changes invalidate the round: when a worker dies (heartbeat
failure) or a new worker joins after completion, the coordinator bumps the
round number. Live workers observe the bump through the `rendezvous_round`
field on heartbeat responses and re-join.

Rendezvous state is in-memory only; round numbers are wall-clock seeded so
they stay monotonic across coordinator restarts and HA failovers, after which
workers simply re-rendezvous.

## Failure handling and elasticity

**Worker death.** The launcher restarts a non-zero-exiting worker while its
`--max-restarts` budget lasts. The replacement re-joins the rendezvous (same
worker id, so it reclaims its previous rank when it still fits), which bumps
the round. Survivors notice via heartbeat — or via the `RuntimeError` their
next collective raises — and re-rendezvous.

**Elastic world size.** Register the run with bounds instead of a fixed size:

```bash
python -m dtruntime.launch \
    --coordinator coordinator:50051 --run-name my-run \
    --nproc-per-node 4 --min-workers 2 --max-workers 8 \
    train.py
```

Training then runs at whatever world size the rendezvous settles on, shrinking
when workers are lost and growing when replacements or new nodes join.

**Writing an elastic training loop.** Wrap the per-round training body in
`run_elastic`; construct (or restore from the latest checkpoint) the model,
DDP wrapper, and optimizer *inside* the function so they bind to the fresh
process group after a re-rendezvous:

```python
def train(ctx):
    model = ctx.wrap_model(build_or_restore_model(), strategy="ddp")
    optimizer = make_optimizer(model)
    for step in range(start_step, total_steps):
        ctx.raise_if_membership_changed()
        train_step(model, optimizer, next(batches))

ctx = torch_dist.init()
ctx.run_elastic(train)
```

`run_elastic` re-rendezvouses and re-invokes the function on membership
change. A `RuntimeError` from a collective is treated as a membership change
only when the coordinator confirms a new round within a grace window
(default 150 s, covering the heartbeat failure-detection interval); otherwise
it propagates — genuine training bugs are not swallowed.

## Strategies

`ctx.wrap_model(model, strategy=...)` supports:

| Strategy | Wrapper | Notes |
|---|---|---|
| `ddp` | `torch.nn.parallel.DistributedDataParallel` | Default. Moves the model to `cuda:{local_rank}` and sets `device_ids` when CUDA is available. |
| `fsdp` | `torch.distributed.fsdp.FullyShardedDataParallel` | Pass FSDP kwargs (`sharding_strategy`, `auto_wrap_policy`, ...) through `wrap_model`. |
| `deepspeed` | `deepspeed.initialize` | Returns the DeepSpeed engine tuple. Requires `pip install deepspeed` (Linux). |
| `none` | — | Returns the model unchanged (single-worker debugging). |

## Launcher reference

```
python -m dtruntime.launch
  --coordinator HOST:PORT     coordinator gRPC address (required)
  --run-name NAME             run name and idempotency scope (required)
  --run-id ID                 join an existing run instead of registering
  --nproc-per-node N          training processes on this node (default 1)
  --world-size N              fixed total across nodes (default: nproc-per-node)
  --min-workers N             elastic lower bound (enables elastic mode)
  --max-workers N             elastic upper bound
  --settle-timeout-ms MS      elastic settle window (0 = server default, 30 s)
  --max-restarts N            worker restarts before the node fails (default 3)
  --backend NAME              torch backend (default: nccl if CUDA else gloo)
  --node-id ID                stable node identifier (default: hostname)
  --worker-host ADDR          address other nodes reach this node on
  script.py [args...]
```

Environment injected into each worker: `DTRUNTIME_COORDINATOR_ADDR`,
`DTRUNTIME_RUN_ID`, `DTRUNTIME_WORKER_ID` (`<node-id>-<local-rank>`),
`DTRUNTIME_LOCAL_RANK`, and the conventional `LOCAL_RANK`.

Tuning knobs read by `torch_dist`:

- `DTRUNTIME_HEARTBEAT_INTERVAL` — seconds between worker heartbeats
  (default 10; the demos use 1 for snappy failure detection).
- `DTRUNTIME_PG_TIMEOUT` — `torch.distributed` process-group timeout in
  seconds (default 300). A surviving rank blocks inside a collective until
  this fires when a peer dies, so it bounds failure-detection latency — the
  benchmark sets 30.
- `DTRUNTIME_WORKER_HOST` — address other ranks use to reach this node's
  rendezvous master (defaults to the hostname; the demos pin 127.0.0.1).

## Validation

- `python/tests/test_rendezvous.py`, `test_launch.py`, `test_strategies.py` —
  unit tests, no coordinator or GPU required.
- `python/tests/test_torch_distributed_integration.py` — end-to-end: builds
  the Rust coordinator, launches 2-process gloo DDP training through
  `dtruntime.launch`, asserts replicas stayed synchronised, then kills a
  worker mid-training and asserts the restart → re-rendezvous → completion
  path. Gate: `DTRUNTIME_RUN_TORCH_INTEGRATION=1`.
- Working example: [python/examples/pytorch_ddp_train.py](../python/examples/pytorch_ddp_train.py).

Validated locally with multi-process gloo on a single machine. Multi-node
NCCL execution follows the same rendezvous path but has not been validated on
a real GPU cluster yet; see [status-and-limitations.md](../README.md).
