# Service Level Objectives (SLOs)

These SLOs define what "healthy" means for the coordinator control plane and how
its error budgets drive alerting. The SLIs are precomputed as Prometheus
recording rules (`observability/prometheus/recording.rules.yml`) and the burn
alerts live in `observability/prometheus/alerts.rules.yml`. On-call response is
in [runbooks-oncall.md](runbooks-oncall.md).

The coordinator is the control plane, not the training data path: these SLOs
cover control-plane availability and durability semantics, not model throughput
(that is covered by [benchmarks.md](benchmarks.md)).

## SLO summary

| # | SLO | SLI | Objective | Window |
|---|---|---|---|---|
| 1 | **Coordinator availability** | Fraction of scrapes where `up == 1` | **99.9%** | 30 days |
| 2 | **Checkpoint durability** | `committed / started` checkpoints (equivalently `1 − failure_ratio`) | **99%** | 30 days |
| 3 | **Worker-fleet health** | Fraction of known workers that are `active` (not stale/failed) | **≥ 90%** | steady-state |
| 4 | **Rendezvous formation** | Runs that reach `RUNNING` within the settle window after quorum | **99%** | per run |

SLO 4 is currently tracked operationally (run lifecycle state + logs) rather
than by a dedicated metric; the others are metric-backed and alerted.

## 1) Availability — 99.9%

- **SLI**: `avg_over_time(up{job="dtruntime-coordinator"}[w])`.
- **Budget**: 0.1% ≈ **43m 12s** of unavailability per 30 days.
- **Alerts** (multiwindow, multi-burn-rate, per the Google SRE workbook):
  - **Fast burn** (14.4×, 5m∧1h) → page (`CoordinatorAvailabilityFastBurn`). At
    this rate the 30-day budget is gone in ~2 days.
  - **Slow burn** (6×, 30m∧6h) → ticket (`CoordinatorAvailabilitySlowBurn`).
  - `CoordinatorDown` / `CoordinatorAbsent` fire directly on `up == 0` / target
    absence for fast, unambiguous outages.
- HA note: with the PostgreSQL advisory-lock standby, a single replica restart
  should stay within budget; only a full control-plane outage burns it.

## 2) Checkpoint durability — 99%

- **SLI**: `dtruntime:checkpoint_commit_success:ratio_rate30m` (and the inverse
  `dtruntime:checkpoint_failure:ratio_rate5m`).
- **Why it matters**: a failed checkpoint means a crash loses training progress
  back to the previous good checkpoint. This is the durability contract behind
  failure recovery.
- **Alerts**:
  - `CheckpointFailuresElevated` — failure ratio > 1% for 15m (critical).
  - `CheckpointCommitStalled` — checkpoints starting but *none* committing for
    30m (2PC/storage wedged; critical).

## 3) Worker-fleet health — ≥ 90% active

- **SLI**: `1 − dtruntime:workers_unhealthy:ratio`.
- **Alerts**:
  - `HighStaleWorkerRatio` — > 25% stale/failed for 10m (warning; likely
    network partition, node pressure, or mass preemption).
  - `WorkersSilentButPresent` — active workers but zero heartbeats for 5m
    (possible stuck run; cross-check with `dtruntime-admin doctor`).

## Error-budget policy

- **Budget healthy**: ship normally.
- **Budget < 25% remaining**: freeze risky changes; prioritize reliability work
  for the affected SLO.
- **Budget exhausted**: change freeze except reliability fixes and security
  patches until the budget recovers.

## Reviewing SLOs

Re-baseline objectives after each phase that changes the control-plane surface
(new RPCs, storage backends, or HA topology). Targets here are starting points
validated in local/simulated environments — tighten or loosen them against
observed production behavior before committing to an external SLA.
