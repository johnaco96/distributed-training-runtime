"""Distributed training strategy wrappers: DDP, FSDP, and DeepSpeed.

All imports are deferred so the SDK stays importable without torch installed;
each strategy raises a clear error naming the missing dependency instead.
"""

from __future__ import annotations

from typing import Any

SUPPORTED_STRATEGIES = ("none", "ddp", "fsdp", "deepspeed")


class StrategyError(RuntimeError):
    """Raised when a strategy cannot be applied."""


def wrap_model(
    model: Any,
    strategy: str = "ddp",
    local_rank: int = 0,
    **kwargs: Any,
) -> Any:
    """Wrap ``model`` for distributed training.

    Must be called after ``torch.distributed.init_process_group`` (which
    :func:`dtruntime.torch_dist.init` performs).

    - ``"ddp"`` — ``torch.nn.parallel.DistributedDataParallel``.  On CUDA the
      model is moved to ``cuda:{local_rank}`` and ``device_ids`` is set.
    - ``"fsdp"`` — ``torch.distributed.fsdp.FullyShardedDataParallel``.
    - ``"deepspeed"`` — ``deepspeed.initialize``; pass DeepSpeed arguments
      (``config``, ``optimizer``, ``model_parameters``, ...) as ``kwargs`` and
      receive the DeepSpeed engine tuple ``(engine, optimizer, dataloader,
      lr_scheduler)``.
    - ``"none"`` — return the model unchanged (single-worker debugging).
    """
    if strategy not in SUPPORTED_STRATEGIES:
        raise StrategyError(
            f"unknown strategy {strategy!r}; supported: {', '.join(SUPPORTED_STRATEGIES)}"
        )

    if strategy == "none":
        return model

    if strategy == "deepspeed":
        return _wrap_deepspeed(model, **kwargs)

    torch = _import_torch()
    if not torch.distributed.is_initialized():
        raise StrategyError(
            "torch.distributed is not initialised; call dtruntime.torch_dist.init() first"
        )

    if strategy == "ddp":
        return _wrap_ddp(torch, model, local_rank, **kwargs)
    return _wrap_fsdp(torch, model, local_rank, **kwargs)


def _import_torch() -> Any:
    try:
        import torch
    except ImportError as exc:  # pragma: no cover - torch present in CI
        raise StrategyError(
            "torch is required for this strategy: pip install torch"
        ) from exc
    return torch


def _wrap_ddp(torch: Any, model: Any, local_rank: int, **kwargs: Any) -> Any:
    from torch.nn.parallel import DistributedDataParallel

    if torch.cuda.is_available():
        device = torch.device("cuda", local_rank)
        torch.cuda.set_device(device)
        model = model.to(device)
        kwargs.setdefault("device_ids", [local_rank])
    return DistributedDataParallel(model, **kwargs)


def _wrap_fsdp(torch: Any, model: Any, local_rank: int, **kwargs: Any) -> Any:
    try:
        from torch.distributed.fsdp import FullyShardedDataParallel
    except ImportError as exc:  # pragma: no cover - depends on torch version
        raise StrategyError(
            "FSDP is unavailable in this torch build; use strategy='ddp' or upgrade torch"
        ) from exc

    if torch.cuda.is_available():
        device = torch.device("cuda", local_rank)
        torch.cuda.set_device(device)
        model = model.to(device)
        kwargs.setdefault("device_id", device)
    return FullyShardedDataParallel(model, **kwargs)


def _wrap_deepspeed(model: Any, **kwargs: Any) -> Any:
    try:
        import deepspeed
    except ImportError as exc:
        raise StrategyError(
            "deepspeed is required for strategy='deepspeed': pip install deepspeed "
            "(note: DeepSpeed supports Linux; on other platforms use ddp or fsdp)"
        ) from exc
    return deepspeed.initialize(model=model, **kwargs)
