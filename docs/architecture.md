# Distributed Training Data and Checkpoint Runtime Architecture (Initial)

## Project Goal

Build a staged, practical runtime that improves **dataset coordination** and **checkpoint reliability** for distributed ML training jobs without replacing the training framework itself.

The runtime is intended to:

- let training clients register datasets and request deterministic shard assignments,
- coordinate worker progress and membership,
- provide checkpoint metadata + storage orchestration,
- recover from common worker/node failures with clear consistency rules.

The architecture is intentionally MVP-first and not production-ready.

## High-Level System Overview

The system is split into:

1. **Python SDK (client-side control integration)**
2. **Rust Coordinator (authoritative control service)**
3. **Worker Runtime (per-training-worker execution companion)**
4. **Storage Layer (local/object store backends)**

Communication between Python SDK and Rust coordinator is over **gRPC**, with protocol shape defined by **protobuf** in `proto/runtime.proto`.

At a high level:

- users integrate the Python SDK in training code,
- SDK issues control requests (dataset registration, shard claims, checkpoint intents),
- coordinator stores runtime metadata/state and returns decisions,
- workers execute data reads/writes and report progress,
- storage backends persist checkpoint artifacts and metadata snapshots.

## Python SDK Role

The Python SDK is the user-facing integration layer for training scripts and orchestrators.

Primary responsibilities:

- expose ergonomic APIs for dataset registration, run/session lifecycle, shard acquisition, and checkpoint commit,
- translate SDK calls into gRPC requests to the coordinator,
- perform lightweight client-side validation and retries for transient transport issues,
- surface coordinator decisions and errors in framework-friendly Python exceptions/results.

Non-goals for SDK in initial architecture:

- no embedded consensus logic,
- no independent authority for shard ownership or checkpoint finalization,
- no hidden background daemons that mutate state without explicit API calls.

## Rust Coordinator Role

The Rust coordinator is the authoritative control-plane service.

Primary responsibilities:

- maintain canonical state for runs, workers, dataset partitions, and checkpoint versions,
- assign or lease dataset shards to workers deterministically,
- serialize checkpoint transitions (intent -> upload in progress -> committed/aborted),
- provide idempotent RPC semantics where practical,
- handle failure detection signals/timeouts and drive recovery state transitions.

Coordinator design intent for MVP:

- single-coordinator process model (with persistence) before HA replication,
- explicit state-machine transitions rather than implicit side effects,
- conservative consistency over peak throughput for control operations.

## Worker Runtime Role

The worker runtime is a lightweight companion component used by training workers.

Primary responsibilities:

- request/renew shard leases,
- read assigned data shards according to coordinator-issued plans,
- stage checkpoint payloads (model/optimizer/state blobs) to storage,
- report heartbeats, progress offsets, and checkpoint upload status.

In early milestones, this can be implemented as Python-side runtime helpers using SDK calls; later it may evolve into a separate process/agent if needed for isolation.

## gRPC / Protobuf Role

gRPC + protobuf defines a strict interface contract between SDK/workers and coordinator.

Responsibilities:

- typed request/response schemas for run lifecycle, sharding, checkpoint operations, and health/reporting,
- backward/forward-compatible schema evolution discipline,
- language interoperability (Python clients, Rust server).

Guidelines for early protocol evolution:

- keep RPC surface narrow for MVP,
- prefer additive protobuf changes,
- encode idempotency keys and version fields where replay/retry is expected.

## Storage Layer Role

The storage layer provides durable persistence for metadata snapshots and checkpoint artifacts.

Planned backend model:

- **Local filesystem backend** for development/testing,
- **Object storage backend** (S3-compatible abstraction) for distributed environments.

Separation of concerns:

- coordinator owns metadata/state transitions,
- storage backend owns blob durability and retrieval,
- checkpoint commit succeeds only after coordinator validates required artifact presence.

## Dataset Sharding Model

Initial sharding approach is deterministic and lease-based.

Concepts:

- dataset registered as logical collection + shard definition metadata,
- shards assigned to workers using deterministic policy (e.g., ordered round-robin / range mapping with epoch seed),
- leases include owner worker id + expiry,
- renewal required to keep ownership active.

MVP behavior:

- at-least-once shard processing is acceptable initially,
- duplicate shard processing may occur after failures but should be detectable via progress metadata,
- strict exactly-once semantics are out of MVP scope.

## Checkpointing Model

Checkpointing is modeled as a coordinator-managed transaction-like lifecycle.

Proposed phases:

1. **BeginCheckpoint**: worker obtains checkpoint intent/version.
2. **UploadArtifacts**: worker writes checkpoint data to storage under versioned paths.
3. **ReportArtifacts**: worker reports manifest + checksums/lengths.
4. **CommitCheckpoint**: coordinator validates completeness and marks checkpoint committed.

Design goals:

- avoid partially visible committed checkpoints,
- support resumable uploads/retries where possible,
- make latest committed checkpoint lookup deterministic.

## Failure Recovery Model

Failure recovery focuses on practical, common failure classes first.

Handled in MVP:

- worker crash / lost heartbeat -> lease expiry -> shard reassignment,
- transient coordinator-client RPC failures -> idempotent retry guidance,
- interrupted checkpoint upload -> checkpoint remains uncommitted and recoverable/retriable.

Deferred:

- multi-coordinator failover,
- region-scale disaster recovery automation,
- zero-data-loss guarantees under arbitrary split-brain.

## Control Plane vs Data Plane Separation

**Control plane** (coordinator + RPC APIs):

- run lifecycle/state,
- shard ownership and leases,
- checkpoint state transitions,
- failure state decisions.

**Data plane** (workers + storage):

- dataset shard reads,
- checkpoint artifact transfer,
- artifact verification inputs (size/checksum).

Reason for separation:

- keep high-frequency bulk data transfer out of coordinator,
- allow independent scaling profiles,
- reduce blast radius of control-path failures.

## MVP Scope

The MVP should include:

- Python SDK ↔ Rust coordinator gRPC integration,
- run/session registration and worker heartbeats,
- deterministic shard assignment + lease renewal/reassignment,
- basic checkpoint lifecycle with local filesystem backend,
- recovery from worker loss and interrupted uploads,
- minimal observability (structured logs + core metrics counters).

## Out-of-Scope (Initial)

Not in MVP:

- coordinator HA/consensus replication,
- advanced scheduling/placement optimization,
- full framework-specific plugins for every ML ecosystem,
- multi-tenant quota/isolation policy engine,
- hard real-time performance tuning,
- formal exactly-once processing guarantees.

## First Milestone Plan

Milestone 1 (foundation slice):

1. define minimal protobuf RPC set for run registration, heartbeat, shard lease, and checkpoint begin/commit,
2. implement Rust coordinator with in-memory state + periodic metadata persistence,
3. implement Python SDK client methods for the minimal RPC set,
4. integrate local filesystem storage backend for checkpoint artifacts,
5. run a single-node multi-worker simulation demonstrating:
   - shard lease distribution,
   - worker failure + reassignment,
   - checkpoint commit + resume from latest committed version.

Success criteria for Milestone 1:

- repeatable integration test scenario passes,
- no manual coordinator state edits required for recovery path,
- architecture assumptions documented and updated based on findings.

## Risks and Follow-Ups

Key risks:

- **Protocol churn risk**: rapid API iteration may break SDK/coordinator compatibility.
- **State model ambiguity**: unclear transition rules can cause recovery edge-case bugs.
- **Storage consistency variance**: object-store semantics (listing/read-after-write differences) may affect commit logic.
- **Operational visibility gaps**: insufficient metrics/log context may slow incident diagnosis.

Follow-ups:

- define explicit state-machine diagrams for shard and checkpoint transitions,
- add compatibility/versioning policy for protobuf and SDK,
- validate object-store behavior assumptions with targeted conformance tests,
- design path from single-coordinator MVP to HA architecture once control semantics stabilize.
