# High Availability

## Current status

PostgreSQL-backed HA is implemented via advisory lock with support for 2-replica coordinator deployments. The coordinator uses a PostgreSQL advisory lock for leader election; replica coordinators wait for the lock before serving requests. This provides basic active/passive semantics with durable metadata.

For single-node and local/dev deployments, SQLite metadata persistence is also supported.

## Why replicas alone are unsafe without semantic controls

Running multiple coordinator replicas without semantic safety controls risks:

- Duplicate checkpoint commits.
- Conflicting worker liveness reconciliation.
- Lease epoch races.
- Idempotency store divergence.
- Latest-checkpoint pointer races.
- Inconsistent metadata persistence and read/write ordering.

The advisory lock approach avoids these risks by ensuring only one coordinator serves traffic at a time.

## Implemented HA controls

- PostgreSQL advisory lock for single-writer guarantee.
- 2-replica coordinator support for failover.
- Durable metadata in PostgreSQL for checkpoint and run state.
- Two-phase commit (2PC) for checkpoint commits.
- Shard progress persistence.

## Remaining work before production HA claims

- Full automated failover test coverage.
- Kubernetes HA manifests with proper liveness/readiness probes for multi-replica.
- Operational runbook and rollback plan.
- Explicit fencing token / coordinator epoch (currently advisory lock provides this implicitly).

## Acceptance criteria

HA is not considered fully production-ready until all are true:

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

This does **not** yet implement:
- Production HA Kubernetes rollout with full validation.
- Full distributed failover test suite.
- Automatic HA Kubernetes manifests.
