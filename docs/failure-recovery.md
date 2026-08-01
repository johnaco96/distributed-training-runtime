# Failure recovery behavior

This document describes the behavior implemented in this repository. It is
not a production SLA or a design promise for features that are not present.

## Worker and run recovery

Workers register with a stable `worker_id`, send heartbeats, and participate
in coordinator-managed rendezvous rounds. The coordinator expects a heartbeat
within 30 seconds and marks a silent worker failed after 90 seconds. A
non-graceful departure invalidates the completed rendezvous round and moves the
run to `RECOVERING` while the surviving and replacement workers form a new
round.

The bundled launcher can restart a failed local process while its configured
restart budget remains. It reuses the worker identity and rank when possible.
An external scheduler or node provisioner is not included, so replacing a
lost machine is the deployment's responsibility. Training code must use
`TrainingContext.run_elastic` (or implement equivalent retry and restore
logic) to rebuild the process group and resume after a membership change.

The protobuf exposes `REGISTERING`, `ACTIVE`, `STALE`, `FAILED`,
`RECOVERING`, `REMOVED`, and `COMPLETED` worker states. The current heartbeat
reconciler uses `ACTIVE`, `STALE`, and `FAILED` for liveness; explicit
deregistration can produce `REMOVED` or `COMPLETED`. The presence of an enum
value does not imply a separate automated transition for every value.

## Shards and progress

Shard ownership is currently deterministic: a rank receives shards whose
index satisfies `shard_index % world_size == rank`. Cache information can
change the order of that rank's candidates, but not their owner. Ownership can
therefore change when the rendezvous world size or rank mapping changes.

The shard RPCs carry lease fields, and the coordinator validates that progress
and renewal reports come from the active worker that owns the shard. The
current implementation does **not** expire shard leases and dynamically
reassign individual shards to another active rank. Lease epochs are currently
fixed at `1`.

Dataset progress is a best-effort resume hint, not an exactly-once cursor. The
Python prefetcher reports an offset after it has decoded and enqueued a shard's
records; some queued records may not yet have been consumed by the training
loop. A crash can therefore replay records or skip buffered records. Exact
sample accounting across failures is not provided.

## Checkpoint recovery

The coordinator is authoritative for checkpoint state. A checkpoint is
restorable only after all expected artifacts have been reported and every
expected worker has voted, moving it to `COMMITTED`. An interrupted attempt
remains non-restorable.

`DistributedCheckpointer` verifies downloaded artifacts against the committed
manifest. When it reports a corrupt committed checkpoint, the coordinator
marks that checkpoint failed, recomputes the latest committed checkpoint, and
the checkpointer retries an older generation (up to its bounded retry limit).
`AsyncCheckpointer` verifies checksums and fails on corruption, but does not
perform that automatic report-and-fallback loop.

Retention keeps the newest configured number of committed checkpoints and
removes older committed artifacts. Failed or aborted attempts are removed once
a newer committed checkpoint exists. An attempt left forever in
`INITIATED` or `UPLOAD_IN_PROGRESS` is non-restorable but is not currently
aged out automatically.

Both local filesystem and S3-compatible storage are implemented. The portable
DCP path transfers checkpoint artifacts through coordinator streaming RPCs.
`AsyncCheckpointer` can instead upload directly from the worker to S3; it uses
the Rust uploader when that optional binary is available and boto3 otherwise.

## Coordinator restart

With SQLite or PostgreSQL metadata configured, a coordinator restart
rehydrates runs, workers, datasets, shard progress, checkpoints, commit votes,
and persisted idempotency mappings. Rendezvous rounds are in memory and are
re-created; their numbers are seeded monotonically so a restarted coordinator
does not reuse an old round number.

An already-created PyTorch process group can continue compute and collectives
that do not need the coordinator during a short coordinator outage.
Coordinator-dependent operations such as heartbeats, rendezvous, data
assignment, and checkpoint metadata calls can fail or retry until service is
restored.

There is no filesystem or bucket scan that reconstructs checkpoint metadata or
repairs a separate `LATEST` file. Latest-checkpoint authority is the persisted
coordinator metadata. Objects that exist in storage without corresponding
committed metadata are not selected for restore.

PostgreSQL advisory locking provides a single leader with standby processes;
it is not a consensus-replicated, multi-leader control plane. This repository
has local persistence and restart tests, but does not claim a validated
multi-replica production deployment.

## Explicit boundaries

- At-least-once/best-effort data progress, not exactly-once processing.
- No automatic replacement-machine provisioning.
- No dynamic per-shard lease expiration and reassignment.
- No automatic cleanup for checkpoints stuck indefinitely in a nonterminal
  upload state.
- No multi-region recovery or production availability guarantee.
- Multi-node GPU/NCCL and managed-cloud failover have not been validated in
  this repository.
