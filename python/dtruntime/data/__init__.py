"""Sharded data pipeline for the Distributed Training Runtime.

Owns the full lifecycle of a shard — authoring it, reading it back, and
streaming it into a PyTorch ``DataLoader``.

Producing shards (run once, by rank 0 or a prep job):

- :mod:`~dtruntime.data.shard_writer` — write a dataset out as per-shard
  ``.npz`` files plus the metadata ``client.register_dataset()`` expects.

Consuming them (every worker, every epoch):

- :class:`~dtruntime.data.dataset.DistributedShardDataset` — the top-level
  ``IterableDataset``.
- :mod:`~dtruntime.data.formats` — pluggable shard formats (npz, dtrs, jsonl,
  parquet, webdataset) and a registry for custom ones.
- :mod:`~dtruntime.data.shard_reader` — the DTRS v1 binary format and its
  reader, with the optional Rust fast path.
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
from dtruntime.data.shard_reader import ShardReader
from dtruntime.data.shard_writer import build_and_write_shards, write_shard_file
from dtruntime.data.versioning import (
    compute_dataset_version,
    register_versioned_dataset,
    verify_dataset_version,
)

__all__ = [
    # Authoring
    "build_and_write_shards",
    "write_shard_file",
    # Consumption
    "DistributedShardDataset",
    "ShardReader",
    "ShardCache",
    "ChecksumMismatch",
    "get_format",
    "register_format",
    "supported_formats",
    "features_label_transform",
    # Versioning
    "compute_dataset_version",
    "register_versioned_dataset",
    "verify_dataset_version",
]
