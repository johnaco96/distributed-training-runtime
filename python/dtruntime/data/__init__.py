"""Production data pipeline for the Distributed Training Runtime.

Streaming, shufflable, cache-backed dataset loading that plugs into PyTorch
``DataLoader``:

- :class:`~dtruntime.data.dataset.DistributedShardDataset` — the top-level
  ``IterableDataset``.
- :mod:`~dtruntime.data.formats` — pluggable shard formats (npz, dtrs, jsonl,
  parquet, webdataset) and a registry for custom ones.
- :class:`~dtruntime.data.cache.ShardCache` — size-bounded LRU disk cache.
- :mod:`~dtruntime.data.shuffle` — deterministic epoch shuffling.
- :mod:`~dtruntime.data.versioning` — content-addressed dataset versions.
"""

from dtruntime.data.cache import ChecksumMismatch, ShardCache
from dtruntime.data.dataset import DistributedShardDataset
from dtruntime.data.formats import (
    features_label_transform,
    get_format,
    register_format,
    supported_formats,
)
from dtruntime.data.versioning import (
    compute_dataset_version,
    register_versioned_dataset,
    verify_dataset_version,
)

__all__ = [
    "DistributedShardDataset",
    "ShardCache",
    "ChecksumMismatch",
    "get_format",
    "register_format",
    "supported_formats",
    "features_label_transform",
    "compute_dataset_version",
    "register_versioned_dataset",
    "verify_dataset_version",
]
