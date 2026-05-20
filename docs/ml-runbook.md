# ML Integration Runbook (Phase 18D)

## Purpose

Phase 18 provides local, CPU-friendly ML integration examples that exercise deterministic training loops plus local checkpoint save/restore behavior:

- single-worker training (`python/examples/pytorch_train.py`)
- simulated distributed-style training (`python/examples/pytorch_simulated_distributed.py`)

This runbook is for local smoke validation and reproducible development workflows.

## Prerequisites

- Create/activate a Python virtual environment.
- Use `PYTHONPATH=python` when running examples/tests.
- Install `torch` to run PyTorch examples.
- CPU-only execution is sufficient (no GPU required).
- No external dataset downloads are required (synthetic deterministic data is generated locally).

## Single-worker example

### Fresh training

```bash
PYTHONPATH=python python python/examples/pytorch_train.py \
  --epochs 1 --samples 32 --batch-size 8 \
  --out-dir .dtruntime-pytorch-smoke
```

### Resume from latest checkpoint

```bash
PYTHONPATH=python python python/examples/pytorch_train.py \
  --epochs 2 --samples 32 --batch-size 8 \
  --out-dir .dtruntime-pytorch-smoke --resume
```

### No-checkpoint mode

```bash
PYTHONPATH=python python python/examples/pytorch_train.py \
  --epochs 1 --samples 32 --batch-size 8 \
  --out-dir .dtruntime-pytorch-no-checkpoint --no-checkpoint
```

### Inspect output files

```bash
cat .dtruntime-pytorch-smoke/metrics.json
cat .dtruntime-pytorch-smoke/checkpoints/latest.json
```

### Cleanup

```bash
rm -rf .dtruntime-pytorch-smoke .dtruntime-pytorch-no-checkpoint
```

## Simulated distributed-style example

### Fresh run

```bash
PYTHONPATH=python python python/examples/pytorch_simulated_distributed.py \
  --workers 2 --epochs 1 --samples 32 --batch-size 8 --shards 4 \
  --out-dir .dtruntime-pytorch-dist-smoke
```

### Resume from latest checkpoint

```bash
PYTHONPATH=python python python/examples/pytorch_simulated_distributed.py \
  --workers 2 --epochs 2 --samples 32 --batch-size 8 --shards 4 \
  --out-dir .dtruntime-pytorch-dist-smoke --resume
```

### No-checkpoint mode

```bash
PYTHONPATH=python python python/examples/pytorch_simulated_distributed.py \
  --workers 2 --epochs 1 --samples 32 --batch-size 8 --shards 4 \
  --out-dir .dtruntime-pytorch-dist-no-checkpoint --no-checkpoint
```

### Inspect output files

```bash
cat .dtruntime-pytorch-dist-smoke/metrics.json
cat .dtruntime-pytorch-dist-smoke/shard_assignments.json
cat .dtruntime-pytorch-dist-smoke/checkpoints/latest.json
```

### Cleanup

```bash
rm -rf .dtruntime-pytorch-dist-smoke .dtruntime-pytorch-dist-no-checkpoint
```

## Validation checklist

Run the following as a final pass for Phase 18 work:

```bash
cargo fmt
cargo check --workspace
cargo test --workspace
PYTHONPATH=python python -m pytest python/tests
PYTHONPATH=python python python/examples/pytorch_train.py --epochs 1 --samples 32 --batch-size 8 --out-dir .dtruntime-pytorch-smoke
PYTHONPATH=python python python/examples/pytorch_train.py --epochs 2 --samples 32 --batch-size 8 --out-dir .dtruntime-pytorch-smoke --resume
PYTHONPATH=python python python/examples/pytorch_simulated_distributed.py --workers 2 --epochs 1 --samples 32 --batch-size 8 --shards 4 --out-dir .dtruntime-pytorch-dist-smoke
PYTHONPATH=python python python/examples/pytorch_simulated_distributed.py --workers 2 --epochs 2 --samples 32 --batch-size 8 --shards 4 --out-dir .dtruntime-pytorch-dist-smoke --resume
PYTHONPATH=python python python/examples/local_demo.py
DTRUNTIME_RUN_LOCAL_INTEGRATION=1 PYTHONPATH=python python -m pytest python/tests/test_local_integration.py -v
git status --short
```

## Expected output shape

`metrics.json` from both examples should include at least:

- `final_epoch`
- `final_loss`
- `final_accuracy`
- `resumed_from_checkpoint`
- `global_step`

Simulated distributed output also includes:

- `worker_metrics`

## Honest scope boundaries

Phase 18 does **not** prove:

- real `torch.distributed` training
- real multi-machine training
- GPU-dependent workflows
- production orchestration/scheduling
- coordinator-driven ML training orchestration (future work)
- cloud/S3 checkpoint restore flows for ML examples (future work)


## Coordinator-driven PyTorch example (Phase 20B)

Fresh run:

```bash
PYTHONPATH=python python python/examples/pytorch_coordinator_train.py --workers 2 --epochs 1 --samples 32 --batch-size 8 --shards 4 --out-dir .dtruntime-pytorch-coordinator
```

Resume:

```bash
PYTHONPATH=python python python/examples/pytorch_coordinator_train.py --workers 2 --epochs 2 --samples 32 --batch-size 8 --shards 4 --out-dir .dtruntime-pytorch-coordinator --resume
```

No checkpoint:

```bash
PYTHONPATH=python python python/examples/pytorch_coordinator_train.py --workers 2 --epochs 1 --samples 32 --batch-size 8 --shards 4 --out-dir .dtruntime-pytorch-coordinator-no-checkpoint --no-checkpoint
```
