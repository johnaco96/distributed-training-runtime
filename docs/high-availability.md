# High Availability Plan (Phase 20F-1)

## Current status

- Coordinator deployment model is currently single-replica.
- HA coordinator replication is not implemented in this phase.

## Why HA is not simply `replicas: 3`

Multiple replicas alone are insufficient without consistency controls. HA requires correctness across leadership, metadata durability, and write serialization semantics.

## Core HA requirements

- Durable metadata store.
- Leader election.
- Fencing against split-brain writers.
- Idempotency correctness under retries/failover.
- Checkpoint consistency across failover boundaries.
- Lease/liveness reconciliation after coordinator transitions.
- Deterministic failover testing.

## Proposed implementation phases

1. Durable metadata hardening.
2. Leader election model.
3. Active/passive coordinator failover behavior.
4. Failover and recovery correctness tests.
5. Read scaling only if safety/correctness is preserved.

## Non-claims

This document does not claim HA is implemented today. It defines required design direction before implementation.
