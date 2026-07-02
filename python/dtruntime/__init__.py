"""Python SDK for the Distributed Training Runtime."""

from .client import RuntimeClient, connect

__all__ = ["RuntimeClient", "connect"]

# torch-dependent entry points live in submodules so importing dtruntime never
# requires torch:
#   dtruntime.torch_dist.init()  — coordinator-backed torch.distributed setup
#   dtruntime.strategies.wrap_model() — DDP / FSDP / DeepSpeed wrappers
#   python -m dtruntime.launch  — torchrun-style worker launcher
