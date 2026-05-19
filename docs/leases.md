# Lease Semantics (Phase 17C)

## Ownership model

Shard ownership is deterministic for the current coordinator implementation:
- ownership is computed from `shard_index % world_size == worker_rank`
- only the deterministic owner may renew the shard lease
- only the deterministic owner may report shard progress

## Lease epoch

Current coordinator behavior uses a fixed lease epoch (`lease_epoch == 1`) for assigned shards.
Requests with a different epoch are rejected as stale (`FailedPrecondition`).

## Renewal behavior

`renew_shard_lease` validates:
- run exists
- worker exists
- shard exists
- run `world_size > 0`
- worker is current deterministic owner
- worker is currently `Active`
- lease epoch is current (currently `1`)

On success, renewal returns lease metadata (`lease_state`, `lease_expires_at`, and `server_time`).

## Progress reporting behavior

`report_shard_progress` validates:
- run exists
- worker exists
- dataset exists
- shard exists under that dataset
- run `world_size > 0`
- worker is current deterministic owner
- worker is currently `Active`
- lease epoch is current (currently `1`)

Only valid owner reports are persisted to coordinator shard-progress state.

## Expected client flow

1. Register worker.
2. Heartbeat.
3. Request shard assignment.
4. Report progress with `worker_id`, `dataset_id`, `shard_id`, and `lease_epoch`.
5. Renew lease before expiry.

If a worker becomes stale/failed, it must heartbeat again to reactivate before lease renewals or progress reports are accepted.

## Current limitations

- No external distributed lease manager or consensus system.
- No production scheduler/lease rebalancer yet.
- Local coordinator process state is the source of truth.
- Protocol version negotiation is not implemented yet.
