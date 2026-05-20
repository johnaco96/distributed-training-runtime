# ML Integration (Phase 18)

For end-to-end component and flow context, see [Architecture Overview](architecture.md).

## Goal

Phase 18 introduces real ML-facing examples that can be connected to runtime checkpointing flows.

## Phase 18A (complete)

Phase 18A added a minimal local PyTorch training example:

- deterministic synthetic dataset (no downloads)
- tiny CPU-friendly MLP model
- simple train/eval loop
- CLI tuning flags for local experimentation
- `metrics.json` output for quick validation

## Phase 18B (complete)

Phase 18B extends the local PyTorch example with real local checkpoint state save/restore.

### New CLI flags

- `--checkpoint-every N` (default `1`): write a checkpoint every `N` epochs.
- `--resume`: require a latest checkpoint and continue from `epoch + 1`.
- `--no-checkpoint`: disable checkpoint writes (useful for quick smoke runs).

### Checkpoint layout

Under `--out-dir`:

- `checkpoints/epoch-0001.pt`
- `checkpoints/epoch-0002.pt`
- ...
- `checkpoints/latest.json`
- `metrics.json`

### Checkpoint contents (`.pt`)

`torch.save(...)` stores:

- `model_state_dict`
- `optimizer_state_dict`
- `scheduler_state_dict`
- `epoch`
- `global_step`
- `seed`
- `config` (CLI argument snapshot)
- `train_loss`
- `eval_loss`
- `eval_accuracy`

### Latest pointer (`latest.json`)

Contains:

- `checkpoint_path` (relative filename)
- `epoch`
- `global_step`
- final loss/accuracy fields
- `created_at` timestamp

### Resume behavior

When `--resume` is passed:

1. Read `checkpoints/latest.json`.
2. Load the referenced `.pt` checkpoint.
3. Restore model/optimizer/scheduler state.
4. Resume from `restored_epoch + 1`.

If `--resume` is passed and checkpoint metadata is missing, the example exits with a clear error.

### Metrics output (`metrics.json`)

Includes:

- `final_epoch`
- `final_train_loss`
- `final_loss`
- `final_accuracy`
- `resumed_from_checkpoint`
- `checkpoint_path` (or `null` when disabled)
- `global_step`
- `epochs_requested`
- `samples`
- `batch_size`

## Current limitations

- local PyTorch checkpoint files only
- not wired to coordinator checkpoint RPC/artifact metadata flow yet
- not distributed training yet
- no multi-worker simulated ML training yet
- no cloud/object-storage requirement for this example
- no GPU requirement


## Phase 18C (complete)

Phase 18C adds `python/examples/pytorch_simulated_distributed.py`, a deterministic single-process simulation of distributed-style training concepts.

### What it simulates

- multiple logical workers (`--workers`)
- deterministic dataset shards (`--shards`)
- deterministic worker/shard assignment (`shard_id % workers == worker_rank`)
- per-worker local training from a shared global model
- FedAvg-style aggregation by averaging worker model parameters each epoch

### Important scope limits

- this is **not** `torch.distributed`
- this is **not** multi-machine training
- this does **not** prove distributed scaling
- this is a local simulation to demonstrate runtime concepts

### Checkpoint and metrics outputs

Under `--out-dir` (default `.dtruntime-pytorch-distributed`):

- `checkpoints/epoch-0001.pt`, `epoch-0002.pt`, ...
- `checkpoints/latest.json`
- `metrics.json`
- `shard_assignments.json`

Checkpoint state includes:

- global model state dict
- epoch and global step
- sanitized config snapshot
- shard assignments and worker metrics
- summary metric fields

### Resume behavior

When `--resume` is set:

1. load `checkpoints/latest.json`
2. load referenced checkpoint file
3. restore global model state
4. continue from `restored_epoch + 1`

If checkpoint metadata or referenced checkpoint file is missing, the script exits with a clear error.


## Phase 18D (complete)

Phase 18D finalizes documentation and validation workflow:

- adds a dedicated runbook: `docs/ml-runbook.md`
- consolidates smoke/resume/no-checkpoint commands for both examples
- adds final validation checklist and cleanup guidance
- clarifies explicit non-goals and current limitations

## Phase 18 summary

- **18A**: single-worker deterministic PyTorch training example with `metrics.json`.
- **18B**: local checkpoint save/restore for model, optimizer, scheduler, plus `latest.json` pointer.
- **18C**: single-process simulated distributed-style training with deterministic shard assignment and FedAvg-style parameter averaging.
- **18D**: runbook + final validation cleanup/documentation pass.

Single-worker and simulated-distributed examples are intentionally different: the former validates a local training/checkpoint loop, while the latter validates local simulation of worker/shard behavior and aggregation semantics in one process.

Checkpoint state currently includes model state and progress/metrics metadata, and resume behavior starts from `restored_epoch + 1` using `checkpoints/latest.json` as the required pointer file.

For command-level usage and cleanup steps, see `docs/ml-runbook.md`.


## Phase 20B: Coordinator-driven PyTorch orchestration

Added `python/examples/pytorch_coordinator_train.py`, a local single-process PyTorch example that exercises coordinator control-plane calls during training: worker registration, heartbeat, dataset/shard registration, coordinator shard assignment requests, checkpoint artifact reporting/commit, and latest checkpoint lookup. This is intentionally not `torch.distributed` or multi-node training.


## Phase 20C: Local PyTorch orchestration benchmark

Use `python/examples/benchmark_pytorch_orchestration.py` to compare local standalone training (`pytorch_train.py`) versus local coordinator-driven training (`pytorch_coordinator_train.py`). It records wall-clock timings, overhead (seconds and percent), final loss/accuracy, checkpoint/resume status, and coordinator RPC summary when available. Results are emitted as JSON and CSV for reproducibility and automation.

Example:

`PYTHONPATH=python python python/examples/benchmark_pytorch_orchestration.py --runs 3 --epochs 2 --samples 128 --batch-size 16 --workers 2 --shards 4 --out-dir .dtruntime-pytorch-bench`

Smoke:

`PYTHONPATH=python python python/examples/benchmark_pytorch_orchestration.py --runs 1 --epochs 1 --samples 32 --batch-size 8 --workers 2 --shards 4 --out-dir .dtruntime-pytorch-bench-smoke`

Caveats: this is local-only and environment-specific. Coordinator timings include coordinator startup in the current implementation (no warmed/external coordinator benchmark mode yet). These numbers do **not** prove distributed multi-node scaling, Kubernetes performance, or production throughput.
