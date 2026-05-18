# Dataset Registration and Sharding Model (MVP)

## 1. Status

- **Status:** Draft MVP design for implementation.
- **Maturity:** Architecture-aligned design guidance; not production-ready.
- **Scope:** Control-plane contracts and coordinator state model for dataset registration, shard assignment, leases, and recovery-aware progress tracking.

## 2. Goals

- Define an MVP dataset registration and sharding model compatible with the current runtime architecture.
- Keep authority for dataset metadata and shard assignment in the Rust coordinator.
- Keep control-plane RPCs small (metadata, leases, decisions), with payload reads in the data plane.
- Provide deterministic, implementation-friendly sharding behavior for early milestones.
- Stay compatible with checkpoint protocol concepts (`run_id`, `worker_id`, and failure/recovery behavior).

## 3. Non-goals

- Exactly-once shard processing guarantees.
- Production-grade scheduler fairness and load balancing.
- HA coordinator replication/consensus.
- Full multi-tenant security/isolation policy.
- Advanced adaptive sharding heuristics and autoscaling-aware policies.

## 4. Control plane vs data plane

**Control plane (Python SDK / worker helpers <-> Rust coordinator over gRPC/protobuf):**

- Register datasets and shard metadata.
- Create run-scoped assignment state.
- Lease shards to workers and renew leases.
- Record progress offsets and completion markers.
- Detect stale workers (heartbeat/lease timeout) and reassign shards.

**Data plane (workers <-> storage backend):**

- Read dataset shard payload bytes directly from local filesystem (MVP) or object storage (future).
- Perform actual I/O for records/batches.
- Never route large dataset payloads through coordinator control RPCs.

Coordinator responses should return only metadata needed for data-plane reads (e.g., shard URI/path, optional range hints, lease metadata), not large payload content.

## 5. Key concepts

- **dataset_id:** Stable logical identifier for a registered dataset definition.
- **run_id:** Logical training run identifier that namespaces worker membership and assignment state.
- **worker_id:** Stable worker identity within a run (also used by checkpoint protocol).
- **shard_id:** Stable identifier of one logical shard belonging to a dataset.
- **epoch:** Logical pass counter over the dataset for a run.
- **shard lease:** Time-bounded coordinator grant giving one worker temporary processing ownership of a shard.
- **assignment policy:** Coordinator strategy used to pick shard->worker mappings.
- **progress_offset:** Worker-reported shard progress marker (record index, byte offset, or framework-defined cursor).

## 6. Dataset registration flow

1. Client (typically via Python SDK) calls dataset registration RPC with dataset metadata and shard descriptors.
2. Coordinator validates request shape and deduplicates by idempotency key or `(name, version)` policy (implementation choice).
3. Coordinator writes canonical dataset metadata and shard metadata in coordinator-owned state.
4. Coordinator returns `dataset_id` and accepted shard summary.
5. Worker flow later references `dataset_id` when joining run data assignment.

Registration is control-plane metadata only. Actual shard payload locations are referenced by path/URI; payload bytes remain in storage/data source.

## 7. Dataset metadata model

Recommended MVP metadata fields:

- `dataset_id`
- `name`
- `uri` (base location or manifest location)
- `format` (e.g., parquet, jsonl, binary-records)
- `shard_count`
- `registered_at`
- `version`
- `metadata` (free-form map for user/system hints)

Notes:

- Coordinator is authoritative for this record.
- `version` should support deterministic reruns and registration updates.
- `uri` is a locator, not inline data.

## 8. Shard metadata model

Recommended MVP shard fields:

- `shard_id`
- `dataset_id`
- `uri`/`path`
- `byte_range` or `record_range` (optional)
- `size_bytes` (optional)
- `checksum` (optional)
- `assigned_worker_id` (optional, run-scoped assignment state)
- `lease_expires_at` (optional)
- `progress_offset` (optional)

Notes:

- Physical shard descriptor fields (`uri`, range, size, checksum) are dataset-owned metadata.
- Assignment/lease/progress fields are run-scoped dynamic state managed by coordinator.

## 9. Initial shard assignment policies

MVP should start with deterministic, easy-to-implement policies:

1. **Worker-rank-based static partitioning (primary MVP default).**
2. Optional fallback queue policy for reassignment after failures.

Policy selection should be explicit in run config so replay/recovery can reuse the same logic.

## 10. Worker-rank-based sharding

Primary policy:

- Workers have stable logical ranks (e.g., `0..world_size-1`) mapped to `worker_id`.
- Deterministic mapping example: assign shard `i` initially to rank `i % world_size`.
- All workers can derive expected ownership from coordinator-published policy inputs.
- Coordinator still remains authoritative and returns explicit lease decisions.

Why this first:

- Simple and deterministic.
- Easy to implement in Rust coordinator and Python clients.
- Compatible with recovery: coordinator can reissue leases when original owner becomes stale.

## 11. Shard lease and renewal behavior

- A shard assignment is represented as a lease with `lease_expires_at`.
- Worker must renew lease periodically via control-plane heartbeat/renew RPC.
- Renewal is accepted only if worker still matches assignment and run is active.
- Coordinator may return a new expiration timestamp on each renewal.
- Worker should stop processing a shard if renewal fails or assignment is revoked.

Lease duration should be short enough for timely failover but long enough to avoid excessive control traffic.

## 12. Stale worker detection and shard reassignment

- Coordinator tracks worker liveness (heartbeat timestamp and/or missed renewals).
- Worker is stale when heartbeat timeout or lease expiry threshold is exceeded.
- Stale worker's active shard leases are marked expired.
- Expired shards become eligible for reassignment to healthy workers.
- Reassignment events should be recorded in run assignment state for audit/debug.

This behavior aligns with failure recovery expectations: control-plane authority recovers assignment progress without requiring worker-side consensus.

## 13. Duplicate processing assumptions

MVP processing semantics are **at-least-once**, not exactly-once.

- Duplicate shard processing may occur after lease expiry, worker crash, or delayed network traffic.
- Coordinator should tolerate late progress updates by validating lease/version context.
- Training pipeline logic should not assume global exactly-once consumption for MVP.

## 14. Progress tracking assumptions

- Workers report periodic `progress_offset` per leased shard.
- Offset meaning is format/runtime dependent (record index, byte position, cursor token).
- Coordinator stores latest accepted offset with timestamp and lease context.
- Progress metadata is used for observability and best-effort duplicate detection/resume hints.

MVP does not require transactional coupling between data reads and progress writes.

## 15. Determinism and reproducibility

For reproducible runs, the runtime should capture:

- `dataset_id` and `version`
- shard list/order and assignment policy id
- run parameters affecting worker rank/world size
- epoch index and assignment snapshots (or deterministic recomputation inputs)

Determinism target is assignment behavior under the same inputs; failures can still cause at-least-once reprocessing.

## 16. Local filesystem MVP assumptions

Initial backend assumptions:

- Shard URIs/paths resolve on worker-accessible local/shared filesystem.
- Coordinator stores metadata only; it does not proxy file contents.
- Path normalization and existence checks may be best-effort at registration time.
- Consistency expectations are those of local/shared filesystems in development/test deployments.

## 17. Future S3/object-store concerns

When adding S3-compatible storage, follow the same control/data separation:

- Coordinator returns object URIs/keys and metadata only.
- Workers perform object reads directly with data-plane credentials.
- Consider eventual consistency/listing lag and retry semantics.
- Define per-worker credential handling and security boundaries.
- Consider range-read behavior, object versioning, and checksum validation strategy.

These are future hardening topics, not required for MVP completion.

## 18. Explicit MVP scope

In scope now:

- Dataset registration with coordinator-owned metadata.
- Deterministic shard metadata representation.
- Worker-rank-based initial assignment policy.
- Lease/renew/reassign loop for stale workers.
- Progress offset storage for recovery/debug visibility.
- At-least-once processing semantics.

## 19. Out-of-scope items

- Exactly-once data processing guarantees.
- Cross-run global deduplication.
- Sophisticated load-aware dynamic balancing.
- Multi-coordinator consensus/leader election.
- Production-grade authn/authz and tenant isolation.
- Dataset mutation transactions during active runs.

## 20. Risks and follow-ups

Key risks:

- Lease timeout tuning may cause either slow failover or excess duplicate work.
- Ambiguous progress offset semantics across formats may reduce interoperability.
- Worker clock skew can complicate lease-expiry reasoning if not coordinator-time-based.
- Shared filesystem assumptions may not hold in all distributed environments.

Follow-ups:

- Specify protobuf RPCs/messages for dataset register, shard claim, renew, progress update.
- Define run-state persistence model for assignment snapshots and recovery replay.
- Add explicit conflict rules for late/duplicate progress updates after reassignment.
- Add metrics/tracing for lease churn, duplicate assignments, and stale-worker recovery latency.
- Define S3 backend credential and consistency strategy before object-store rollout.
