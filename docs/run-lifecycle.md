# Run lifecycle and failure semantics

This document defines the durable run lifecycle state machine and the exact
behavior for every failure class: worker death (during training and during
gradient synchronization), coordinator failover during active RPCs, stuck
jobs, and cleanup. It complements [failure-recovery.md](failure-recovery.md)
(shard leases, checkpoint recovery) and
[torch-distributed.md](torch-distributed.md) (rendezvous mechanics).

## Lifecycle states

Every run carries a durable state, persisted to the metadata store (SQLite or
PostgreSQL) and rehydrated on coordinator restart:

```
        ┌──────────── PAUSED ◄──── (admin pause)
        ▼               ▲
PENDING ──► RUNNING ◄──►│──► RECOVERING
               │        └─(admin resume)  │
               ├─► COMPLETED  ├─► FAILED ──► (admin retry) ──► PENDING
               │              │
               ├───► DRAINING ─┘  (admin drain; graceful stop)
               └────► CANCELLED ◄──── (admin, from any live state)
```

| State | Meaning | Entered when |
|---|---|---|
| `PENDING` | Registered; no rendezvous round has completed | `RegisterRun`, or admin `RetryRun`/`ResumeRun` (no workers) |
| `RUNNING` | Workers hold ranks in a completed round | Rendezvous round completes, or admin `ResumeRun` (workers present) |
| `RECOVERING` | A completed round was invalidated; survivors are re-forming the group | Worker fails or leaves non-gracefully |
| `PAUSED` | Administratively paused; not making progress, not terminal | `PauseRun` (from PENDING/RUNNING/RECOVERING) |
| `DRAINING` | Workers finish current work, checkpoint, and exit gracefully | `DrainRun` (from PENDING/RUNNING/RECOVERING/PAUSED) |
| `COMPLETED` | All workers deregistered as completed | Last `DeregisterWorker(COMPLETED)` |
| `FAILED` | Stuck, or no live workers remain after errors | Stuck detection / roster exhaustion |
| `CANCELLED` | Administratively stopped | `CancelRun` |

## Admin lifecycle operations

Operators drive these transitions with the `dtruntime-admin run <action>`
commands (see [admin-cli.md](admin-cli.md)) or the corresponding
`RuntimeClient`/gRPC calls. `PAUSED` and `DRAINING` are **non-terminal** — a
paused run is resumable and a draining run still completes normally.

| RPC / CLI | Allowed from | Effect |
|---|---|---|
| `PauseRun` / `run pause` | PENDING, RUNNING, RECOVERING | → PAUSED |
| `ResumeRun` / `run resume` | PAUSED | → RUNNING if active workers remain, else PENDING |
| `RetryRun` / `run retry` | FAILED | → PENDING; clears the stale worker roster + rendezvous and increments the durable `retry_count`, so a fresh launch re-rendezvouses and resumes from the latest committed checkpoint |
| `DrainRun` / `run drain` | PENDING, RUNNING, RECOVERING, PAUSED | → DRAINING |
| `CancelRun` / `run cancel` | any live state | → CANCELLED (terminal) |

An action attempted from a disallowed state returns `FAILED_PRECONDITION`
(never a silent no-op). Every action is recorded in the audit log with its
optional operator-supplied reason. The per-run `retry_count` is persisted
(schema migration v2) and survives coordinator restarts; it surfaces in
`ListRuns` / `runs list`.

`COMPLETED`, `FAILED`, and `CANCELLED` are **terminal and final** — later
transition attempts (including cancellation racing completion) are ignored,
and terminal runs reject `RegisterWorker` and `JoinRendezvous` with
`FAILED_PRECONDITION`. Workers observe the run state on every heartbeat
response and exit when it turns terminal; launchers check the state before
restarting a failed worker so a cancelled run is never kept alive by restart
loops.

## Failure semantics by scenario

**Worker death during training (between collectives).** The launcher restarts
the process (while `--max-restarts` lasts). The replacement re-joins the
rendezvous, which bumps the round; survivors observe the bump via heartbeat
and re-rendezvous at their next step boundary. The run passes through
`RECOVERING` and returns to `RUNNING` when the new round completes. Detection
is immediate when a replacement joins; otherwise it is heartbeat-driven
(stale after 30 s, failed after 90 s).

**Worker death during gradient synchronization.** Survivors are inside a
collective when the peer dies; the collective raises `RuntimeError` (usually
within seconds via connection reset, at worst at the process-group timeout).
`run_elastic` treats that error as a membership change **only if the
coordinator confirms a new rendezvous round within the grace window**
(default 150 s, covering the full heartbeat failure-detection interval);
otherwise the error propagates — genuine training bugs are not swallowed.

**Graceful departure (`DeregisterWorker`).** Immediate, no timeout involved:
`COMPLETED` marks the worker done without invalidating the round (in DDP all
ranks finish together); `PREEMPTED`/`ERROR` invalidate the round exactly like
a detected failure, so elastic scale-down (spot reclaim, drain) re-forms the
group without waiting 90 s. `dtruntime.torch_dist` deregisters automatically
on `TrainingContext.shutdown()` / context-manager exit.

**Coordinator failover / restart during active RPCs.** In-flight RPCs fail
with a transport error; the contract is:

- every mutating RPC carries an idempotency key, so client retries are safe;
- run, worker, shard-progress, and 2PC-vote state rehydrate from the metadata
  store on startup (including `RegisterRun` idempotency mappings, so a
  re-registering launcher gets the same run back);
- rendezvous state is deliberately in-memory. Completed process groups do not
  depend on the coordinator, so **training continues through the outage**;
  workers' heartbeats retry until the new leader answers. Workers that were
  mid-rendezvous re-assert membership (the SDK re-joins automatically on
  transient errors), and round numbers are wall-clock-seeded so a restarted
  coordinator can never reuse a round a worker still holds.

**Stuck jobs.** The coordinator's periodic reconcile task fails any
non-terminal run whose entire roster has been silent past
`--stuck-after-secs` (default 1800), with the silence duration recorded in
`state_reason`. This catches runs whose workers all vanished without
deregistering, or that never attracted workers at all.

**Cleanup.** Terminal runs keep durable rows forever (history/audit) but
their in-memory footprint — worker rosters, datasets, checkpoint records,
rendezvous state, shard progress, idempotency keys — is dropped
`--terminal-run-retention-secs` (default 86400) after the terminal
transition. The same sweep removes orphaned per-run maps whose run no longer
exists.

## Coordinator configuration

```
--reconcile-interval-secs <N>        background reconcile cadence (default 10)
--stuck-after-secs <N>               silence threshold for stuck detection (default 1800)
--terminal-run-retention-secs <N>    in-memory retention for terminal runs (default 86400)
```

## Admin operations

`CancelRun` (admin role) is the first lifecycle admin operation; pause,
resume, retry, and drain arrive with the admin CLI work. From Python:

```python
client.cancel_run(run_id, reason="preempting for higher-priority job")
status = client.get_run_status(run_id)   # .state, .state_reason
```

Cancelled workers exit with a distinct path (`RunTerminated` raised at the
next step boundary), launchers exit `3` for cancelled/failed runs and `0`
when the run completed elsewhere.

## Validation

- Rust unit tests: every transition above, terminal finality, stuck
  detection, retention cleanup, and restart rehydration
  (`cargo test -p runtime-coordinator`, `state::tests`).
- Integration tests (`DTRUNTIME_RUN_TORCH_INTEGRATION=1`):
  - worker crash → restart → re-rendezvous → completion;
  - `CancelRun` mid-training → workers exit, launcher stops without
    restarts;
  - coordinator hard-kill + restart mid-training on the same metadata DB →
    training completes transparently and the rehydrated run reaches
    `COMPLETED`.
