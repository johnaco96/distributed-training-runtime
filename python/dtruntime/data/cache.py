"""Local disk cache for dataset shards.

Shards are the unit of locality: a worker downloads each assigned shard once,
reuses it across every epoch, and the coordinator's per-rank striping keeps a
rank's shard set stable so its cache stays warm run-to-run (see
:mod:`dtruntime.data.locality`).

:class:`ShardCache` is a size-bounded, thread-safe, LRU disk cache:

- ``get(uri, checksum, ...)`` returns a local path, downloading on a miss;
- downloads are atomic (temp file + ``os.replace``), so a crashed download
  never leaves a half-written shard that looks cached;
- checksums are verified after download and, optionally, on every hit
  (``verify_on_hit``) to catch bit-rot before it reaches training;
- when the cache exceeds ``max_bytes`` the least-recently-used shards not
  currently pinned are evicted.

The cache keys on a hash of the URI, so ``file://`` shards are also "cached"
(hard-copied once) which keeps the read path uniform across backends.
"""

from __future__ import annotations

import hashlib
import os
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Dict, Optional

# Reuse the backend-aware byte fetcher from the existing reader for non-file
# schemes (s3://, ...).
from dtruntime._util import file_uri_to_path as _file_uri_to_path
from dtruntime._util import sha256_file as _sha256_file
from dtruntime.data.shard_reader import _fetch_shard_bytes


class ChecksumMismatch(RuntimeError):
    """Raised when a shard's bytes do not match its registered checksum."""


def _default_fetch(uri: str, **kwargs) -> bytes:
    """Read shard bytes for any supported URI, robust to file:// conventions."""
    path = _file_uri_to_path(uri)
    if path is not None:
        return path.read_bytes()
    return _fetch_shard_bytes(uri, **kwargs)


@dataclass
class _Entry:
    path: Path
    size: int
    last_used: float = field(default_factory=time.monotonic)
    pins: int = 0


class ShardCache:
    """Size-bounded LRU disk cache for shard files."""

    def __init__(
        self,
        root: str | Path,
        max_bytes: int = 8 * 1024**3,
        *,
        verify_on_hit: bool = False,
        fetch: Optional[Callable[..., bytes]] = None,
    ) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.max_bytes = int(max_bytes)
        self.verify_on_hit = verify_on_hit
        self._fetch = fetch or _default_fetch
        self._lock = threading.Lock()
        self._entries: Dict[str, _Entry] = {}
        # Per-key in-flight downloads so concurrent get()s for the same shard
        # fetch once (and never os.replace over a file another thread has open,
        # which fails on Windows).
        self._inflight: Dict[str, threading.Event] = {}
        self._reload_existing()
        # Stats (best-effort, for observability/tests).
        self.hits = 0
        self.misses = 0
        self.evictions = 0

    # ── public API ───────────────────────────────────────────────────────────

    def get(
        self,
        uri: str,
        checksum: Optional[str] = None,
        *,
        pin: bool = False,
        **fetch_kwargs,
    ) -> Path:
        """Return a local path to the shard at ``uri``, downloading on a miss.

        ``checksum`` (sha256 hex) is verified after any download and on hits
        when ``verify_on_hit`` is set.  Set ``pin=True`` to protect the shard
        from eviction until :meth:`unpin` is called (use while streaming it).
        """
        key = self._key(uri)
        while True:
            with self._lock:
                entry = self._entries.get(key)
                if entry is not None and entry.path.exists():
                    if self.verify_on_hit and checksum:
                        actual = _sha256_file(entry.path)
                        if actual != checksum:
                            # Corrupt on disk — drop and re-fetch below.
                            self._remove_locked(key)
                            entry = None
                    if entry is not None:
                        entry.last_used = time.monotonic()
                        if pin:
                            entry.pins += 1
                        self.hits += 1
                        return entry.path

                inflight = self._inflight.get(key)
                if inflight is None:
                    # We own this download.
                    inflight = threading.Event()
                    self._inflight[key] = inflight
                    owner = True
                else:
                    owner = False

            if not owner:
                # Another thread is fetching this shard; wait and re-check.
                inflight.wait()
                continue

            # Fetch outside the lock so other shards' gets are not blocked.
            try:
                data = self._fetch(uri, **fetch_kwargs)
                if checksum:
                    actual = hashlib.sha256(data).hexdigest()
                    if actual != checksum:
                        raise ChecksumMismatch(
                            f"shard {uri} checksum mismatch: got {actual}, expected {checksum}"
                        )
                path = self.root / key
                tmp = self.root / f"{key}.{os.getpid()}.{threading.get_ident()}.tmp"
                tmp.write_bytes(data)
                os.replace(tmp, path)
                with self._lock:
                    self._entries[key] = _Entry(
                        path=path, size=len(data), pins=1 if pin else 0
                    )
                    self.misses += 1
                    self._evict_locked(protect=key)
                    return path
            finally:
                with self._lock:
                    self._inflight.pop(key, None)
                inflight.set()

    def unpin(self, uri: str) -> None:
        key = self._key(uri)
        with self._lock:
            entry = self._entries.get(key)
            if entry is not None and entry.pins > 0:
                entry.pins -= 1

    def contains(self, uri: str) -> bool:
        key = self._key(uri)
        with self._lock:
            entry = self._entries.get(key)
            return entry is not None and entry.path.exists()

    @property
    def total_bytes(self) -> int:
        with self._lock:
            return sum(e.size for e in self._entries.values())

    # ── internals ────────────────────────────────────────────────────────────

    @staticmethod
    def _key(uri: str) -> str:
        return hashlib.sha256(uri.encode("utf-8")).hexdigest()[:32]

    def _reload_existing(self) -> None:
        # Adopt any shards left on disk from a previous run (warm restart).
        for path in self.root.iterdir():
            if path.is_file() and not path.name.endswith(".tmp"):
                self._entries[path.name] = _Entry(path=path, size=path.stat().st_size)

    def _remove_locked(self, key: str) -> None:
        entry = self._entries.pop(key, None)
        if entry is not None:
            try:
                entry.path.unlink()
            except OSError:
                pass

    def _evict_locked(self, protect: Optional[str] = None) -> None:
        total = sum(e.size for e in self._entries.values())
        if total <= self.max_bytes:
            return
        # LRU order, skipping pinned and the just-added shard.
        candidates = sorted(
            (
                (k, e)
                for k, e in self._entries.items()
                if e.pins == 0 and k != protect
            ),
            key=lambda kv: kv[1].last_used,
        )
        for key, entry in candidates:
            if total <= self.max_bytes:
                break
            self._remove_locked(key)
            total -= entry.size
            self.evictions += 1
