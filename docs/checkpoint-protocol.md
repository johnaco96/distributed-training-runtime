# Checkpoint Protocol (MVP)

## 1. Status

- **Status:** Draft MVP protocol for implementation.
- **Maturity:** Design-level guidance aligned to current architecture docs; not production-ready.
- **Scope:** Control-plane/data-plane contract for checkpoint creation, commit, and restore lookup.

## 2. Goals

- Define a practical checkpoint protocol for the current MVP architecture.
- Keep checkpoint commit authority in the Rust coordinator.
- Keep large artifact bytes off the control plane.
- Make restore behavior deterministic by relying on committed checkpoint state.
- Keep implementation straightforward for upcoming protobuf, Rust, and Python SDK work.

## 3. Non-goals

- Exactly-once checkpoint creation semantics.
- HA coordinator replication and distributed consensus.
- Production-grade security hardening and multi-tenant isolation.
- Cross-region replication or geo-failover.
- Snapshot delta/compaction optimization in MVP.

## 4. Control plane vs data plane

**Control plane (gRPC/protobuf: Python SDK/worker helpers <-> Rust coordinator):**

- Create checkpoint intent and metadata records.
- Track artifact declarations and upload status.
- Validate checkpoint completeness.
- Transition checkpoint state to committed/failed/aborted.
- Write/repair coordinator-owned metadata objects (`manifest.*`, `COMMITTED`, `LATEST`).

**Data plane (workers <-> storage backend):**

- Upload checkpoint artifact payloads directly to storage.
- Do not stream large payloads through coordinator RPCs.
- Use worker-scoped object paths for ownership boundaries.

This separation is required so checkpoint throughput scales with storage I/O while control RPCs remain small and stateful.

## 5. Key concepts

- **run_id:** Unique logical training run identifier. Namespaces all checkpoint state for one run.
- **checkpoint_id:** Unique checkpoint version identifier within a run (UUID/monotonic token).
- **step:** Training step/iteration associated with the checkpoint (for ordering and restore policy).
- **worker_id:** Stable worker identity for artifact ownership within a checkpoint.
- **artifact:** A worker-produced payload object (e.g., model shard, optimizer shard, RNG state).
- **manifest:** Coordinator-owned metadata document listing expected artifacts, observed artifact properties, step, and checkpoint state.
- **commit marker:** Coordinator-owned `COMMITTED` marker indicating checkpoint finalization.
- **latest pointer:** Coordinator-owned `LATEST` pointer to the most recent committed checkpoint for fast restore lookup.

## 6. Checkpoint object/key layout

Logical layout for MVP:

```text
runs/{run_id}/checkpoints/{checkpoint_id}/manifest.pending.json
runs/{run_id}/checkpoints/{checkpoint_id}/workers/{worker_id}/{artifact_name}
runs/{run_id}/checkpoints/{checkpoint_id}/manifest.committed.json
runs/{run_id}/checkpoints/{checkpoint_id}/COMMITTED
runs/{run_id}/LATEST
```

Notes:

- `manifest.pending.json` is created/updated by coordinator before commit.
- Workers only write artifact payloads under their own `workers/{worker_id}/...` prefix.
- `manifest.committed.json` and `COMMITTED` are written only by coordinator after validation.
- `LATEST` is a coordinator-owned pointer (content can be checkpoint_id plus optional step/timestamp).

## 7. Checkpoint ownership rules

- Coordinator owns checkpoint metadata lifecycle and commit state.
- Coordinator is the only writer for:
  - `manifest.pending.json`
  - `manifest.committed.json`
  - `COMMITTED`
  - `LATEST`
- Workers own only artifact payload writes beneath their own worker path.
- Workers must not write shared top-level checkpoint metadata objects.
- Clients treat coordinator state as authoritative even if raw storage contains extra/partial files.

## 8. Checkpoint state machine

Recommended MVP states (coordinator-internal):

1. `INITIATED` - checkpoint intent accepted, pending manifest created.
2. `UPLOAD_IN_PROGRESS` - workers uploading declared artifacts.
3. `VALIDATING` - coordinator validating required artifacts and metadata.
4. `COMMITTED` - committed manifest + marker persisted; restorable.
5. `FAILED` - validation or finalization failure.
6. `ABORTED` - explicitly canceled by client/runtime.

Transition rules:

- Only coordinator transitions state.
- `COMMITTED` is terminal and restorable.
- `FAILED`/`ABORTED` are terminal and non-restorable.
- No path should mark a checkpoint restorable before `COMMITTED` transition completes.

## 9. Successful checkpoint flow

1. Worker/SDK requests `BeginCheckpoint(run_id, step, checkpoint_id?)`.
2. Coordinator allocates/accepts checkpoint_id, records `INITIATED`, writes `manifest.pending.json`.
3. Coordinator returns artifact plan (expected workers/artifact names, upload paths).
4. Workers upload artifacts directly to storage under `workers/{worker_id}/...`.
5. Workers report upload completion metadata (artifact path, size, checksum) via control RPC.
6. Coordinator transitions to `VALIDATING` when required reports received.
7. Coordinator validates expected artifact set + reported metadata (+ optional storage existence checks).
8. On success, coordinator writes `manifest.committed.json`, then `COMMITTED`, then updates `LATEST`.
9. Coordinator transitions checkpoint state to `COMMITTED` and returns commit success.

## 10. Interrupted/failed checkpoint flow

Failure examples:

- Worker crash during upload.
- Missing artifact reports.
- Checksum/size mismatch.
- Storage write/read inconsistency during validation.
- Coordinator restart during in-progress checkpoint.

Handling:

- Coordinator marks checkpoint `FAILED` or `ABORTED` when completion criteria cannot be met.
- Partial artifacts may remain in storage but are non-restorable.
- No `COMMITTED` marker means checkpoint is invalid for restore.
- Retries should use idempotent RPCs keyed by `(run_id, checkpoint_id, worker_id, artifact_name)` where practical.

## 11. Latest committed checkpoint restore flow

1. Restore request asks coordinator for latest committed checkpoint for `run_id`.
2. Coordinator first reads `LATEST` pointer.
3. Coordinator verifies pointed checkpoint has valid `manifest.committed.json` and `COMMITTED` marker.
4. If valid, coordinator returns checkpoint metadata/artifact map to client.
5. If `LATEST` is missing/stale/corrupt, coordinator falls back to scan committed checkpoints and selects newest by coordinator commit ordering (not by directory timestamp).
6. Coordinator may repair/rewrite `LATEST` during this reconciliation.

Rule: latest lookup must prefer latest **committed** checkpoint, never newest directory by raw timestamp.

## 12. Idempotency and retry behavior

- Control-plane RPCs should support safe retries for transport failures.
- `BeginCheckpoint` should return existing in-progress checkpoint when same idempotency key/checkpoint_id is reused.
- Worker artifact completion reports should be upsert/idempotent per artifact identity.
- `CommitCheckpoint` should be idempotent:
  - return success if already committed,
  - return terminal failure if previously failed/aborted.
- MVP provides at-least-once style retry safety, not exactly-once semantics.

## 13. Checksum and size validation

- Each artifact report should include:
  - artifact logical name,
  - storage path,
  - byte size,
  - checksum (algorithm configurable; simple default acceptable for MVP).
- Coordinator validates report completeness and consistency against manifest expectations.
- Optional existence/stat checks can be performed before commit finalization.
- Any mismatch prevents commit.

## 14. Partial checkpoint cleanup policy

- Partial checkpoint fragments are expected under failures and must never be considered valid checkpoints.
- MVP cleanup policy:
  - mark terminal `FAILED`/`ABORTED` in coordinator metadata,
  - leave artifact garbage in place initially,
  - optionally run best-effort async cleanup later.
- Cleanup is advisory; restorable validity is determined only by committed metadata + marker.

## 15. Startup reconciliation behavior

On coordinator startup/restart:

1. Enumerate in-progress or uncertain checkpoints from coordinator state.
2. Reconcile storage metadata objects.
3. For each checkpoint:
   - if `manifest.committed.json` + `COMMITTED` are present and coherent, mark `COMMITTED`;
   - otherwise mark non-committed terminal or resumable per policy.
4. Recompute latest committed checkpoint per run from committed records.
5. Repair stale/missing `LATEST` pointers by rewriting from committed manifests + commit markers.

This ensures stale pointers or leftover partial directories do not break restore correctness.

## 16. Local filesystem MVP assumptions

- Single-node or shared POSIX-like filesystem used for early development/testing.
- Coordinator and workers can access a common root path.
- File create/rename semantics are sufficient for marker/pointer updates in MVP.
- Limited concurrency and failure injection compared with real distributed object stores.

## 17. Future S3/object-store concerns

When adding S3-compatible backend:

- No atomic rename; protocol should avoid relying on rename for commit semantics.
- LIST consistency and pagination behavior may vary across providers.
- Use explicit commit marker + committed manifest as durable truth.
- Consider conditional writes (etag/version) for pointer races.
- Validate checksum/size with object metadata where available.
- Background garbage collection and lifecycle rules become more important.
- Phase 12A adds only the S3-compatible storage foundation; full coordinator
  upload/restore artifact wiring is expected in Phase 12B.

## 18. Explicit MVP scope

Included now:

- Coordinator-owned checkpoint state machine.
- Worker direct artifact uploads under worker-specific prefixes.
- Two-phase metadata style: pending manifest then committed manifest + marker.
- `LATEST` pointer with startup repair.
- Restore based on latest committed checkpoint.

## 19. Out-of-scope items

- Exactly-once distributed transaction semantics.
- Multi-coordinator leader election/replicated state machine.
- Encryption/KMS/key-rotation policy.
- Quota enforcement and cost-aware lifecycle controls.
- Cross-run deduplication and incremental checkpoint graph compaction.
- Full production observability/SLO definitions.

## 20. Risks and follow-ups

- **Risk:** coordinator/storage mismatch after crashes.
  - **Follow-up:** define precise reconciliation RPC/state persistence behavior.
- **Risk:** artifact report spoofing or corruption in untrusted environments.
  - **Follow-up:** add authn/authz and stronger integrity validation model.
- **Risk:** `LATEST` pointer races under concurrent checkpoint commits.
  - **Follow-up:** define coordinator serialization/compare-and-set strategy.
- **Risk:** storage garbage growth from failed checkpoints.
  - **Follow-up:** implement periodic garbage collector with retention policy.
- **Risk:** protocol drift between Python SDK and Rust coordinator.
  - **Follow-up:** codify protobuf messages/RPCs directly from this doc in next milestone.
