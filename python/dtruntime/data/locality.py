"""Multi-node data locality and DataLoader-worker sharding.

In a failure-free epoch with a fixed world size and rank mapping, two levels of
splitting give each record one reader and avoid cross-node data movement:

1. **Rank locality (across nodes)** — the coordinator assigns each rank a
   deterministic subset of shards (``shard_index % world_size == rank``).  For
   the same mapping, the subset does not change between epochs, so a rank streams only its own
   shards from its **local** cache; nothing is shuffled *between* nodes.  Only
   the visiting order changes per epoch (see :mod:`dtruntime.data.shuffle`).

2. **DataLoader-worker locality (within a node)** — when a rank uses
   ``DataLoader(num_workers=N)``, each worker process must read a disjoint
   slice of the rank's shards, or records would be duplicated N times.
   :func:`split_for_dataloader_worker` partitions the rank's shards by worker.

The fixed rank→shard mapping is what makes the disk cache worth having: a
warm cache from a previous epoch (or a resumed run) is reused instead of
re-downloaded.
"""

from __future__ import annotations

from typing import List, Sequence, TypeVar

T = TypeVar("T")


def split_for_dataloader_worker(
    shards: Sequence[T], worker_index: int, num_workers: int
) -> List[T]:
    """Return the sub-list of ``shards`` this DataLoader worker should read.

    Round-robin assignment (``shards[worker_index::num_workers]``) balances
    shard counts across workers even when the total is not divisible.  With
    ``num_workers <= 1`` the worker gets every shard.
    """
    if num_workers <= 1:
        return list(shards)
    return list(shards[worker_index::num_workers])


def current_dataloader_worker() -> tuple[int, int]:
    """``(worker_index, num_workers)`` for the calling context.

    Returns ``(0, 1)`` outside a DataLoader worker (single-process loading).
    """
    try:
        import torch.utils.data as tud

        info = tud.get_worker_info()
    except Exception:  # noqa: BLE001 - torch missing or not in a worker
        info = None
    if info is None:
        return 0, 1
    return int(info.id), int(info.num_workers)


def cache_hint_shard_ids(
    shards: Sequence[object], attr: str = "shard_id"
) -> List[str]:
    """Extract shard ids to send as the coordinator's cache-locality hint.

    The coordinator's cache-aware placement policy prefers to re-assign shards
    a worker already holds; reporting the warm set keeps assignments sticky.
    """
    return [getattr(s, attr) for s in shards]
