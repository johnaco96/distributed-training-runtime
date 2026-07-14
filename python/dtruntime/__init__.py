"""Python SDK for the Distributed Training Runtime."""

from .async_checkpoint import AsyncCheckpointer, CheckpointError, CheckpointHandle
from .client import RuntimeClient, connect

__all__ = [
    "AsyncCheckpointer",
    "CheckpointError",
    "CheckpointHandle",
    "RuntimeClient",
    "connect",
]

# torch-dependent entry points live in submodules so importing dtruntime never
# requires torch (async_checkpoint and checkpointing import torch lazily):
#   dtruntime.torch_dist.init()  — coordinator-backed torch.distributed setup
#   dtruntime.strategies.wrap_model() — DDP / FSDP / DeepSpeed wrappers
#   python -m dtruntime.launch  — torchrun-style worker launcher
