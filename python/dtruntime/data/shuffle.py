"""Deterministic, epoch-aware shuffling for distributed data loading.

Shuffling must be **deterministic given (base_seed, epoch)** so that:

- every rank and every DataLoader worker computes a *consistent* view without
  extra coordination (they all derive the same per-epoch permutations from the
  shared seed), and
- a crashed worker that resumes at the same epoch reproduces the exact order,
  so resume-from-offset stays correct.

Two levels of shuffle are supported and composed:

1. **Shard-order shuffle** — the order in which a worker visits *its own*
   shards changes each epoch (cheap; keeps whole-shard cache locality).
2. **Within-shard record shuffle** — records inside a shard are permuted using
   a buffered reservoir shuffle so large shards never fully materialise.

Both are seeded from ``derive_seed(base_seed, epoch, salt)`` so the streams are
independent yet reproducible.
"""

from __future__ import annotations

import hashlib
from typing import Iterable, Iterator, List, Sequence, TypeVar

T = TypeVar("T")


def derive_seed(base_seed: int, epoch: int, salt: str = "") -> int:
    """Derive a stable 63-bit seed from ``(base_seed, epoch, salt)``.

    Uses SHA-256 rather than arithmetic mixing so different salts (e.g.
    ``"shards"`` vs ``"records:shard-3"``) never accidentally correlate.
    """
    material = f"{base_seed}:{epoch}:{salt}".encode("utf-8")
    digest = hashlib.sha256(material).digest()
    return int.from_bytes(digest[:8], "big") & ((1 << 63) - 1)


def shuffled_order(items: Sequence[T], base_seed: int, epoch: int, salt: str = "shards") -> List[T]:
    """Return a deterministic per-epoch permutation of ``items``.

    Uses numpy's PCG64 generator for a stable, high-quality permutation that
    does not depend on Python's hash randomisation.
    """
    import numpy as np

    rng = np.random.default_rng(derive_seed(base_seed, epoch, salt))
    order = rng.permutation(len(items))
    return [items[i] for i in order]


def reservoir_shuffle(
    records: Iterable[T],
    buffer_size: int,
    base_seed: int,
    epoch: int,
    salt: str = "records",
) -> Iterator[T]:
    """Stream ``records`` in a shuffled order using a bounded buffer.

    Fills a buffer of up to ``buffer_size`` records, then for each new record
    swaps it with a random slot and emits the displaced one — the standard
    streaming shuffle used by tf.data / WebDataset.  ``buffer_size <= 1``
    disables shuffling (pass-through), which keeps small/streaming shards cheap.

    Determinism: the RNG is seeded from ``(base_seed, epoch, salt)`` so a given
    shard at a given epoch always yields the same order.
    """
    if buffer_size <= 1:
        yield from records
        return

    import numpy as np

    rng = np.random.default_rng(derive_seed(base_seed, epoch, salt))
    buffer: List[T] = []
    for record in records:
        if len(buffer) < buffer_size:
            buffer.append(record)
            continue
        idx = int(rng.integers(0, buffer_size))
        yield buffer[idx]
        buffer[idx] = record
    # Drain the buffer in a shuffled order.
    order = rng.permutation(len(buffer))
    for i in order:
        yield buffer[i]
