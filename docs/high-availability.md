# High Availability Plan (Phase 20F-5 Secure Design Track)

## Current status

- Coordinator deployment is currently single-replica.
- Local Kubernetes Deployment uses `replicas: 1`.
- No leader election is implemented.
- No active/passive coordinator failover semantics are implemented.
- No production HA claim is made.

## Why replicas alone are unsafe

Running multiple coordinator replicas without semantic safety controls risks:

- Duplicate checkpoint commits.
- Conflicting worker liveness reconciliation.
- Lease epoch races.
- Idempotency store divergence.
- Latest-checkpoint pointer races.
- Inconsistent metadata persistence and read/write ordering.

## Required HA architecture

Before HA replica count changes are allowed, the control plane needs:

- Durable external metadata store.
- Leader election.
- Fencing token / coordinator epoch.
- Transactional checkpoint metadata updates.
- Durable idempotency key store.
- Safe lease/liveness reconciliation rules.
- Safe recovery after leader restart.
- Explicit failover test coverage.

## Proposed HA phases

- **HA-1:** Durable metadata hardening and schema review.
- **HA-2:** Leader election design.
- **HA-3:** Active/passive coordinator with fencing.
- **HA-4:** Failover simulation tests.
- **HA-5:** Kubernetes HA manifests only after code semantics are safe.
- **HA-6:** Operational runbook and rollback plan.

## Acceptance criteria

HA is not considered ready until all are true:

- Checkpoint state cannot be double-committed during failover.
- Worker lease state remains consistent during leader transitions.
- Idempotency guarantees survive coordinator restart/failover.
- Latest checkpoint pointer remains monotonic/safe.
- Stale/failure reconciliation runs only on active leader.
- Automated tests cover leader restart and failover behavior.

## Risk register

- Split-brain behavior.
- Duplicate checkpoint commits.
- Stale leader mutating state after leadership loss.
- Delayed lease reconciliation causing worker instability.
- Metadata database outage during leadership transition.
- Clock skew affecting lease/fencing assumptions.

## Explicit non-claims

This phase does **not** implement:
- Coordinator replication semantics.
- Leader election runtime behavior.
- Active/passive failover runtime behavior.
- Production HA Kubernetes rollout.
