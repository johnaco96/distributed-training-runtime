# Data pipeline

`dtruntime.data` is a streaming, shufflable, cache-backed dataset layer that
plugs into PyTorch `DataLoader`. It turns the coordinator's shard-assignment
protocol into a data path: workers read their assigned shards
from a warm local cache, records are shuffled deterministically per epoch, and
progress is reported for resume. The shard-assignment RPCs it builds on are
defined in [proto/runtime.proto](../proto/runtime.proto); when the Rust
accelerator is built (`python scripts/build_native.py`), DTRS shards decode
through `dtruntime._native` with measured numbers in
[benchmarks/data-plane-throughput.md](benchmarks/data-plane-throughput.md).

## Quick start

```python
from torch.utils.data import DataLoader
from dtruntime.data import DistributedShardDataset
from dtruntime.data.formats import features_label_transform

ds = DistributedShardDataset(
    client=client, run_id=run_id, worker_id=worker_id,
    dataset_id="dataset", dataset_format="npz",
    transform=features_label_transform,
    shuffle=True, seed=1234,
    cache_dir="/var/cache/dtruntime", cache_max_bytes=8 << 30,
    prefetch_shards=2, prefetch_records=512,
)
loader = DataLoader(ds, batch_size=32, num_workers=4)

for epoch in range(epochs):
    ds.set_epoch(epoch)                # re-shuffles deterministically
    for features, labels in loader:
        train_step(features, labels)
```

## How the requirements map

| Requirement | Implementation |
|---|---|
| **Streaming** | Shards are the unit of caching/locality and are downloaded as complete cache files. JSONL iterates by line, WebDataset by tar member, Parquet by row group, and DTRS by record/batch. NPZ loading materializes its arrays in memory. |
| **Epoch shuffling** | Two composed, deterministic shuffles keyed on `(seed, epoch)`: shard visiting order (`shuffle.shuffled_order`) and within-shard records (a bounded reservoir shuffle, `shuffle.reservoir_shuffle`). With the same inputs and library behavior, the same `(seed, epoch)` reproduces order; different epochs reorder. This does not make failure recovery exactly once. |
| **Versioning** | Content-addressed: `compute_dataset_version(shards)` hashes the ordered `(shard_id, checksum)` pairs into `sha256:...`. `register_versioned_dataset` sets it at registration; `verify_dataset_version` pins a run to exact data and catches drift. |
| **Cache management** | `ShardCache`: size-bounded LRU disk cache with atomic downloads, per-key download dedup, checksum verification, pinning during streaming, and adoption of shards left on disk from a previous run (warm restart). |
| **Prefetching / backpressure** | `ShardPrefetcher` warms the next `prefetch_shards` shards with a thread pool and streams decoded records through a bounded `queue.Queue(maxsize=prefetch_records)`. A slow training loop blocks the producers (backpressure) instead of reading the whole dataset into memory. |
| **Formats beyond .npz** | Pluggable registry: `npz`, `dtrs` (the binary format), `jsonl`, `parquet` (pyarrow), `webdataset` (tar). Register custom formats with `register_format`. |
| **DataLoader / IterableDataset** | `DistributedShardDataset` **is** a `torch.utils.data.IterableDataset`; pass it straight to `DataLoader`. In a failure-free epoch, `num_workers>0` splits the rank's shards into disjoint round-robin slices. Failure recovery is not exactly once. |
| **Multi-node data locality** | For a fixed world size and rank mapping, the coordinator assigns shards by `shard_index % world_size == rank`; epochs change order, not ownership. A world-size/rank change can change ownership. Warm-shard cache hints reorder eligible candidates under cache-aware placement; they do not pin a shard to a host. |

## Determinism and resume

All shuffling is a pure function of `(seed, epoch)`, so:

- every rank and DataLoader worker computes a consistent view with **no extra
  coordination** (they derive the same permutations from the shared seed);
- the same inputs reproduce the same shuffle order. Cross-environment order
  still depends on format/library behavior;
- the coordinator's per-shard `progress_offset` is applied only for epoch 0.
  It is a best-effort hint reported after records are decoded and enqueued,
  not after the training loop consumes them. A crash can replay records or
  skip records that were buffered but not consumed.

`set_epoch(epoch)` must be called before each pass (as with PyTorch's
`DistributedSampler`).

## Formats

```python
from dtruntime.data import get_format, supported_formats, register_format
# ['dtrs', 'jsonl', 'npz', 'parquet', 'webdataset']

class MyFormat:
    name = "myfmt"
    def iter_records(self, path, *, start=0): ...   # yield dict records
    def count_records(self, path): ...              # or return None
register_format(MyFormat())
```

Records are `dict[str, Any]`; a `transform` maps a record to whatever the
training loop consumes. `features_label_transform` handles the common tabular
case (`npz`/`dtrs`) → `(features_tensor, label_tensor)`.

Parquet requires the optional dependency: `pip install "dtruntime[parquet]"`.

## Cache

```python
from dtruntime.data import ShardCache
cache = ShardCache("/var/cache/dtruntime", max_bytes=8 << 30, verify_on_hit=False)
path = cache.get(uri, checksum, pin=True)   # downloads once, verifies, pins
cache.unpin(uri)
```

Downloads are atomic (`temp + os.replace`), so a crash never leaves a
half-written shard that looks cached. Concurrent gets of the same shard within
one process fetch once. Set `verify_on_hit=True` to re-checksum on every read (catches bit-rot at
the cost of I/O); by default hits are trusted after the download-time check.

## Validation

- `python/tests/test_data_formats.py`, `test_data_cache_shuffle.py`,
  `test_data_dataset.py` — unit tests (formats incl. parquet/webdataset,
  cache LRU/pin/dedup/checksum, deterministic shuffle, versioning, DataLoader
  worker split, torch `DataLoader` integration). No coordinator required.
- `python/tests/test_data_integration.py` — two ranks stream a coordinator-
  registered versioned dataset and, in the failure-free test, cover it once
  with no overlap,
  keeping each rank's shard set stable across epochs. Gate:
  `DTRUNTIME_RUN_LOCAL_INTEGRATION=1`.
- Working example:
  [python/examples/data_pipeline_demo.py](../python/examples/data_pipeline_demo.py).
- Standalone dataset-authoring CLI (generate synthetic DTRS shards and upload
  them to S3/MinIO):
  [python/tools/generate_shard_dataset.py](../python/tools/generate_shard_dataset.py).
