# ML Integration (Phase 18)

## Goal

Phase 18 introduces real ML-facing examples that can be connected to runtime checkpointing flows.

## Phase 18A (this change)

Phase 18A adds a minimal local PyTorch training example:

- deterministic synthetic dataset (no downloads)
- tiny CPU-friendly MLP model
- simple train/eval loop
- CLI tuning flags for local experimentation
- `metrics.json` output for quick validation
- placeholder checkpoint directory layout under `out-dir/checkpoints` for upcoming checkpoint wiring

Run:

```bash
PYTHONPATH=python python python/examples/pytorch_train.py --epochs 1 --samples 32 --batch-size 8 --out-dir .dtruntime-pytorch-smoke
```

Notes:

- CPU-only by default
- no external datasets
- no `torchvision` dependency
- no cloud storage setup required

## Phase 18B (next)

Planned additions:

- save model/optimizer/scheduler state
- restore from latest checkpoint
- connect checkpoint artifacts to runtime checkpoint metadata flow

## Current limitations

- not distributed training yet
- no multi-worker simulated ML training yet
- no cloud/object-storage requirement for this example
- no GPU requirement
