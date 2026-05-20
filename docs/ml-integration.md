# ML Integration (Phase 18)

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
