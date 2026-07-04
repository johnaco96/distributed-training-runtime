# Failure recovery and operator actions

This is the operator-facing guide to what the runtime recovers **automatically**
and what you can **do manually** when a run needs intervention. It ties together
the mechanics in [failure-recovery.md](failure-recovery.md) and
[run-lifecycle.md](run-lifecycle.md), the actions in [admin-cli.md](admin-cli.md),
and the alert responses in [runbooks-oncall.md](runbooks-oncall.md).

## What recovers automatically

| Event | Automatic behavior |
|---|---|
| **Worker dies between steps** | The launcher restarts it (within `--max-restarts`); it re-joins the rendezvous, the round bumps, survivors re-form the group. Run passes `RECOVERING` → `RUNNING`. |
| **Worker dies mid-collective** | Survivors' collective raises; `run_elastic` waits out the grace window and, once the coordinator confirms a new round, re-rendezvouses. A genuine bug (no membership change) is re-raised. |
| **Worker preempted/leaves gracefully** | `DeregisterWorker` takes effect immediately (no timeout); the run completes when all workers deregister as completed. |
| **Worker goes silent** | Heartbeat detection: stale after ~30 s, failed after ~90 s; the reconcile loop fails it and survivors re-form. |
| **Coordinator crash/restart** | On restart against the same metadata store, durable run/worker/idempotency state rehydrates; in-flight training reconnects transparently (validated by `test_failover.py`). |
| **Coordinator leader failure (HA)** | The standby acquires the PostgreSQL advisory lock (~5 s) and starts serving; readiness gates traffic to the new leader. |
| **Stuck run** | A non-terminal run with no heartbeats past `--stuck-after-secs` is marked `FAILED` with a reason. |
| **Corrupt checkpoint** | Detected via checksum on write/stream/download; the latest pointer falls back to the previous committed checkpoint (up to 3 generations). |
| **Old checkpoints** | Retention GC keeps the newest `--checkpoint-keep-last` and reclaims the rest. |

## Run lifecycle at a glance

`PENDING → RUNNING ⇄ RECOVERING`, with terminal `COMPLETED` / `FAILED` /
`CANCELLED`, plus operator-driven `PAUSED` and `DRAINING`. Full state machine and
transition table: [run-lifecycle.md](run-lifecycle.md).

## Operator actions

Drive these with `dtruntime-admin` (see [admin-cli.md](admin-cli.md)) or the
equivalent `RuntimeClient`/gRPC calls. All are audited with an optional reason.

| Action | Command | When to use |
|---|---|---|
| **Inspect** | `dtruntime-admin runs describe <run>` | First stop — state, workers, checkpoint. |
| **Diagnose** | `dtruntime-admin doctor <run>` | Flags stuck workers and bad checkpoints. |
| **Pause** | `dtruntime-admin run pause <run>` | Freeze a run (maintenance window) without losing it. |
| **Resume** | `dtruntime-admin run resume <run>` | Un-pause; returns to `RUNNING` (or `PENDING` if a fresh launch is needed). |
| **Drain** | `dtruntime-admin run drain <run>` | Graceful stop: workers finish, checkpoint, and exit. |
| **Retry** | `dtruntime-admin run retry <run>` | Restart a `FAILED` run from the last committed checkpoint; bumps the retry counter. |
| **Cancel** | `dtruntime-admin run cancel <run>` | Terminal stop. |
| **Verify checkpoint** | `dtruntime-admin checkpoint verify <run> <ckpt>` | Confirm a checkpoint is restorable (download + checksum). Add `--report-corrupt` to force fallback. |
| **List failures / active jobs** | `dtruntime-admin runs list --state failed` / `--state running` | Fleet-wide triage. |

## Playbook: "a run is …"

**… stuck (no progress, workers alive).**
`dtruntime-admin doctor <run>` → if a worker hangs, `run drain` or `run cancel`
then `run retry` to resume from the last checkpoint. See
[WorkersSilentButPresent](runbooks-oncall.md#workerssilentbutpresent).

**… failing repeatedly.**
`runs describe` for the `state_reason`; `checkpoints list --committed-only` and
`checkpoint verify` to confirm a good restore point exists; fix the root cause
(bad data, OOM, storage), then `run retry`. Watch the `retry_count`.

**… losing checkpoints / commit stalled.**
Check the storage backend and that every expected worker reported artifacts (a
2PC commit needs all owners). See
[CheckpointCommitStalled](runbooks-oncall.md#checkpointcommitstalled) and
[CheckpointFailuresElevated](runbooks-oncall.md#checkpointfailureselevated).

**… on nodes about to be reclaimed (spot/maintenance).**
`run drain` for a graceful checkpoint-and-exit, or rely on elastic mode
(`--min/--max-workers`) so the run continues on the survivors.

**… needs to stop for a cluster change.**
`run pause`, do the work, `run resume`.

## Coordinator-level recovery

- **Restart / rolling update**: safe with a shared metadata store; state
  rehydrates. With PostgreSQL HA the standby covers the leader restart.
- **Metadata store**: PostgreSQL for HA + durability; SQLite is single-node/dev.
  See [metadata-persistence.md](metadata-persistence.md) and
  [high-availability.md](high-availability.md).
- **Tuning knobs**: `--reconcile-interval-secs`, `--stuck-after-secs`,
  `--terminal-run-retention-secs`, `--checkpoint-keep-last`.

## Alert → runbook mapping

Every Prometheus alert links to a response procedure in
[runbooks-oncall.md](runbooks-oncall.md); SLOs and error budgets are in
[slos.md](slos.md). Validate these paths in staging with the gated failover /
chaos / soak tests ([observability-ops-tests.md](observability-ops-tests.md)).
