# Checkpoint cleanup (Phase 17D)

Checkpoint cleanup is an explicit library helper in `runtime-checkpoint` for safely deleting old or partial checkpoint data.

## API

`CheckpointCleanupPolicy`:
- `keep_latest` (default `true`)
- `keep_last_n_committed`
- `remove_pending`
- `remove_orphan_artifacts`

`CheckpointStore::cleanup_checkpoints(run_id, policy)` returns `CheckpointCleanupReport`:
- `deleted_keys`
- `retained_keys`
- `skipped_keys`
- `errors`

## Safety rules

- Uses backend-neutral `StorageBackend::list_prefix` and `delete` APIs.
- Deletes only under `runs/{run_id}/checkpoints/*`.
- Never deletes the latest committed checkpoint when `keep_latest=true` and `LATEST` is valid.
- If `LATEST` is missing/invalid, committed-checkpoint deletion is skipped conservatively.

## Pending cleanup

When `remove_pending=true`, checkpoints that have a `manifest.pending.json` but are not committed can be deleted.

## Committed retention cleanup

If committed checkpoints can be identified safely (`manifest.committed.json` + `COMMITTED` marker), cleanup retains:
- latest (if configured), and
- last N committed IDs (current conservative ordering by checkpoint ID string).

Older committed checkpoints outside retention are deleted.

## Orphan artifact cleanup

When deleting a checkpoint, worker artifact keys under that checkpoint prefix are deleted if `remove_orphan_artifacts=true`.
If false, worker artifact keys are retained.

## Current limitations

- No background cleanup scheduler/daemon.
- No cleanup RPC exposed via wire protocol.
- No production retention controller yet.
- Conservative behavior when latest pointer is missing/invalid.
