# Failure Recovery Model (MVP)

## 1. Status

- **Status:** Draft MVP design for implementation.
- **Maturity:** Architecture-aligned recovery guidance; not production-ready.
- **Scope:** Recovery behavior for worker failures, shard leases, checkpoint finalization, restore selection, and coordinator restart reconciliation.

## 2. Goals

- Define a practical, coordinator-authoritative recovery model for the current runtime.
- Keep recovery logic aligned with control-plane state managed by the Rust coordinator over gRPC/protobuf.
- Preserve control-plane vs data-plane separation: recovery decisions in coordinator, payload I/O in workers/storage.
- Ensure only committed checkpoints are restorable.
- Ensure restore selects the latest committed checkpoint, not the newest directory by timestamp/name.
- Provide implementation-friendly at-least-once recovery semantics for MVP.

## 3. Non-goals

- Exactly-once processing semantics for shard progress or checkpoint writes.
- Full fault tolerance with consensus-backed HA coordinator replication.
- Multi-region disaster recovery.
- Production-grade SLA guarantees.
- End-to-end elimination of duplicate work during worker retries/restarts.

## 4. Failure model overview

The coordinator is authoritative for recovery decisions. Workers send heartbeats and lease renewals; the coordinator interprets missed heartbeats, expired leases, and invalid reports as failure/staleness signals and drives state transitions.

MVP assumes common failure classes:

- Worker process crash or hang.
- Worker network partition from coordinator.
- Worker restarts with stale local state.
- Coordinator restart with persisted storage state present.
- Interrupted checkpoint upload/validation/commit.
- Stale or missing `LATEST` pointer.

Recovery prioritizes safety and determinism over minimal rework:

- Expire stale leases and reassign shards.
- Reject stale/late control reports.
- Treat partial checkpoint data as non-restorable.
- Reconstruct authoritative in-memory coordinator state from persisted metadata when needed.

## 5. Control plane vs data plane recovery responsibilities

**Control plane (coordinator authority):**

- Worker liveness evaluation from heartbeat + lease state.
- Lease expiration and shard reassignment decisions.
- Checkpoint state transitions (`INITIATED`/`UPLOAD_IN_PROGRESS`/`VALIDATING`/`COMMITTED`/`FAILED`/`ABORTED`).
- `COMMITTED`/`LATEST` authority and repair decisions.
- Validation and acceptance/rejection of late or duplicate worker reports.
- Recovery actions and metadata reconciliation on coordinator restart.

**Data plane (worker/storage path):**

- Reading shard payloads.
- Writing checkpoint artifact blobs.
- Retrying uploads/reads at worker/storage client level.

Data-plane success alone does not make state authoritative; coordinator control-plane commit/lease state does.

## 6. Key concepts

- **run_id:** Logical training run namespace for workers, shards, and checkpoints.
- **worker_id:** Stable worker identity within a run.
- **dataset_id:** Logical identifier for registered dataset metadata.
- **shard_id:** Logical identifier for one dataset shard.
- **shard lease:** Time-bounded coordinator grant of temporary processing ownership for `shard_id` by `worker_id`.
- **checkpoint_id:** Logical checkpoint version identifier within a run.
- **heartbeat:** Periodic worker liveness signal to coordinator.
- **lease expiration:** Time after which a shard lease is no longer valid unless renewed.
- **stale worker:** Worker that has missed heartbeat expectations and/or has expired leases but is not yet fully removed.
- **failed worker:** Worker deemed unavailable for continued ownership; coordinator recovers by revoking authority and reassigning work.
- **recovery action:** Coordinator-issued state transition or decision (for example: expire lease, reassign shard, fail checkpoint, ignore late report, repair `LATEST`, restore latest committed checkpoint, reconcile metadata from storage).

## 7. Worker lifecycle states

Recommended coordinator-visible worker lifecycle:

1. **REGISTERING** - Worker joining run; not yet eligible for stable assignments.
2. **ACTIVE** - Heartbeats and lease renewals are timely; worker can hold shard leases and participate in checkpoints.
3. **STALE** - Heartbeats are delayed or missing beyond soft threshold; existing leases may approach expiration.
4. **FAILED** - Hard timeout and/or lease-expiration criteria met; worker loses ownership authority.
5. **RECOVERING** - Worker or replacement worker rejoining and reconciling assignments/checkpoint participation.
6. **REMOVED** - Worker membership closed for this run; no authority for updates.

State transitions are coordinator-owned and may be driven by timers, heartbeat updates, and explicit worker leave/abort signals.

## 8. Checkpoint lifecycle recovery

Checkpoint recovery behavior follows checkpoint protocol rules:

- Coordinator is sole authority for checkpoint terminal state.
- Only checkpoints with committed metadata and commit marker are restorable.
- Partial uploads without successful coordinator commit remain non-restorable.

On checkpoint disruption:

- If required artifacts are missing, lease owners fail, or validation cannot complete in time, coordinator marks checkpoint `FAILED` or `ABORTED`.
- Coordinator must not write/retain commit artifacts in a way that makes a failed/aborted checkpoint appear committed.
- A new checkpoint attempt can be initiated; at-least-once semantics allow duplicate/retried work.

## 9. Shard lease recovery

Shard leases are revocable by timeout. Recovery path:

1. Worker misses heartbeat and/or lease renewal window.
2. Coordinator marks worker `STALE` and tracks approaching lease expiry.
3. On lease expiration, coordinator executes **expire shard lease**.
4. Expired shard becomes eligible for **reassign shard** action.
5. Coordinator assigns new lease to eligible active worker.

Important rule: once expired, prior lease holder reports for that lease epoch are stale and must be validated/rejected against current lease state.

## 10. Heartbeat and timeout behavior

MVP uses configurable timing windows with two thresholds:

- **Stale threshold:** missed heartbeat window that transitions worker toward `STALE`.
- **Failure threshold:** longer missed window and/or lease expiration that transitions to `FAILED`.

Guidance:

- Heartbeat timeout detection and lease expiration are complementary signals.
- Lease expiration is sufficient to revoke shard ownership even if delayed heartbeats later arrive.
- Coordinator clocks/timers are authoritative for state transitions in MVP.

## 11. Coordinator restart recovery

After restart, coordinator reconciles in-memory control state from persisted metadata/storage:

- Rebuild run/checkpoint metadata index.
- Re-evaluate checkpoint states from committed manifests + commit markers.
- Reconstruct/repair `LATEST` if missing/stale.
- Recreate shard lease state conservatively (treat uncertain or expired leases as not owned).
- Transition pre-restart workers to `STALE`/`FAILED` unless fresh heartbeats/renewals arrive.

Recovery action: **reconcile coordinator metadata from storage**.

## 12. Interrupted checkpoint recovery

If coordinator or workers fail mid-checkpoint:

- `manifest.pending` or partial artifacts alone do not imply restorable status.
- Without final committed manifest + commit marker, checkpoint remains non-restorable.
- Coordinator should mark interrupted checkpoint `FAILED`/`ABORTED` on reconciliation if completion cannot be proven.
- Subsequent restore must skip such checkpoints.

Recovery action: **mark checkpoint failed or aborted**.

## 13. Stale LATEST pointer recovery

`LATEST` is a convenience pointer, not ultimate truth. If missing or stale:

1. Scan checkpoint metadata for committed manifests and commit markers.
2. Determine latest committed checkpoint using coordinator restore ordering policy.
3. Rewrite/repair `LATEST` to that checkpoint.

Restore rule: use **latest committed checkpoint**, never newest directory by name/time.

Recovery action: **repair LATEST**.

## 14. Late worker and duplicate report handling

Workers may send delayed or replayed progress/checkpoint reports after lease reassignment or checkpoint state transition.

Coordinator validation requirements before accepting report:

- Worker is currently authorized for the referenced run/shard/checkpoint stage.
- Lease epoch is current and unexpired for shard progress updates.
- Checkpoint state still accepts the given artifact/report transition.
- Idempotency/replay checks pass for duplicate submissions.

If validation fails, coordinator performs **ignore late report**.

## 15. Progress offset recovery assumptions

MVP progress handling assumptions:

- Progress offsets are advisory but coordinator-tracked.
- Reassigned workers may resume from last accepted offset, with potential replay.
- System provides at-least-once semantics; duplicate sample/record processing can occur after failure.
- Exactly-once offset advancement is out of scope for MVP.

## 16. Local filesystem MVP assumptions

Local filesystem backend is first target:

- Single-node/local-path behavior is acceptable for initial implementation and tests.
- Coordinator metadata objects and checkpoint artifacts are persisted in filesystem layout defined by protocol docs.
- Atomicity guarantees depend on local filesystem primitives and conservative write ordering.
- Recovery logic must not assume distributed lock service or external consensus.

## 17. Future S3/object-store recovery concerns

For future object-store backend:

- Listing consistency and read-after-write behavior may vary by provider/config.
- `LATEST` repair and committed-checkpoint discovery should tolerate delayed visibility.
- Multipart upload leftovers and partial object writes require explicit cleanup/reconciliation logic.
- Object versioning/etag semantics may affect duplicate report validation and manifest integrity checks.

These concerns are planned follow-up work, not MVP-complete behavior.

## 18. Explicit MVP scope

Included in MVP:

- Coordinator-authoritative worker stale/failed detection via heartbeat timeout and lease expiration.
- Lease expiration and shard reassignment.
- Coordinator-authoritative checkpoint terminal states.
- Restore selection from latest committed checkpoint only.
- `LATEST` repair by scanning committed metadata/markers.
- Validation and rejection of late/invalid worker reports.
- At-least-once recovery semantics.

## 19. Out-of-scope items

- Exactly-once data processing or checkpoint commit semantics.
- HA coordinator failover with consensus and linearizable multi-node control plane.
- Automatic cross-region recovery.
- Production-grade admission control, tenant isolation, and security hardening.
- Zero-duplication replay guarantees after worker/node failures.

## 20. Risks and follow-ups

- **Risk:** Aggressive heartbeat/lease timeout values can cause false failure detection and excess shard churn.
  - **Follow-up:** Define default timeout profiles and tuning guidance.
- **Risk:** Restart reconciliation bugs can misclassify checkpoint/shard authority.
  - **Follow-up:** Add deterministic reconciliation tests over interrupted flows.
- **Risk:** `LATEST` drift can lead to incorrect restore if repair logic is weak.
  - **Follow-up:** Add startup repair routine and validation checks.
- **Risk:** At-least-once replay can create downstream duplicate effects.
  - **Follow-up:** Document consumer-side/idempotent training-step mitigation patterns.
