# Admin & operator CLI (`dtruntime-admin`)

`dtruntime-admin` is the operator-facing command-line tool for inspecting and
managing runs against a live coordinator. It wraps the coordinator's admin gRPC
surface (the same RPCs the CLI calls are available to any client via
[`dtruntime.client.RuntimeClient`](../python/dtruntime/client.py)) and adds
structured, remediation-oriented error reporting.

It complements [run-lifecycle.md](run-lifecycle.md) (the state machine the admin
actions drive) and [failure-recovery.md](failure-recovery.md) (checkpoint and
lease recovery).

## Installation

The console script is installed with the Python package:

```bash
pip install -e .          # from the repo root
dtruntime-admin --help
```

## Connecting

| Option | Env var | Default |
|---|---|---|
| `-a, --address HOST:PORT` | `DTRUNTIME_COORDINATOR_ADDRESS` | `127.0.0.1:50051` |
| `--timeout SECONDS` | — | none (block) |
| `--json` | — | human tables |

TLS and caller identity reuse the standard `RuntimeClient` environment
variables:

- **TLS**: `DTRUNTIME_TLS_CA_CERT`, `DTRUNTIME_TLS_CLIENT_CERT`,
  `DTRUNTIME_TLS_CLIENT_KEY`.
- **Auth**: `DTRUNTIME_AUTH_SUBJECT`, `DTRUNTIME_AUTH_ROLE`,
  `DTRUNTIME_AUTH_WORKER_ID`. Admin operations require the `admin` role when the
  coordinator runs with `--auth-mode metadata`.

```bash
export DTRUNTIME_COORDINATOR_ADDRESS=coordinator:50051
export DTRUNTIME_AUTH_SUBJECT=ops@example.com
export DTRUNTIME_AUTH_ROLE=admin
```

## Commands

### Inspecting

```bash
dtruntime-admin runs list [--state STATE]      # all runs, or filter to one state
dtruntime-admin runs describe RUN_ID           # a run + its worker roster
dtruntime-admin workers list RUN_ID [--state STATE]
dtruntime-admin shards list RUN_ID [--dataset DATASET_ID]
dtruntime-admin checkpoints list RUN_ID [--committed-only] [--limit N]
```

`--state` accepts the short lifecycle name (`running`, `failed`, `paused`,
`draining`, …) or the full `RUN_STATE_*` / `WORKER_STATE_*` enum name.

`runs list` doubles as the **active-jobs** and **failures** view: filter to
`running` for active jobs, `failed` for failures. `workers list --state failed`
lists failed workers; `shards list` shows shard **leases** and progress offsets.

### Managing runs

```bash
dtruntime-admin run cancel RUN_ID [--reason TEXT]   # terminal
dtruntime-admin run pause  RUN_ID [--reason TEXT]   # PENDING/RUNNING/RECOVERING -> PAUSED
dtruntime-admin run resume RUN_ID                   # PAUSED -> RUNNING or PENDING
dtruntime-admin run retry  RUN_ID                   # FAILED -> PENDING, bumps retry counter
dtruntime-admin run drain  RUN_ID [--reason TEXT]   # -> DRAINING (graceful stop)
```

`--reason` is recorded in the durable audit log (see
[audit-logging.md](audit-logging.md)). Each action prints the run's new state;
an action invalid from the run's current state returns a `FAILED_PRECONDITION`
error (exit code 4) rather than silently doing nothing.

| Action | Allowed from | Result |
|---|---|---|
| `pause` | PENDING, RUNNING, RECOVERING | PAUSED |
| `resume` | PAUSED | RUNNING (workers present) or PENDING (fresh launch needed) |
| `retry` | FAILED | PENDING; clears the stale roster and rendezvous, bumps `retry_count`, resumes from the latest committed checkpoint |
| `drain` | PENDING, RUNNING, RECOVERING, PAUSED | DRAINING |
| `cancel` | any live state | CANCELLED (terminal, idempotent) |

### Debugging tools

```bash
dtruntime-admin doctor [RUN_ID]
dtruntime-admin checkpoint verify RUN_ID CHECKPOINT_ID [--worker-id ID] [--report-corrupt]
```

- **`doctor`** with no run id is a coordinator liveness/handshake probe: it
  reports reachability, negotiated protocol version, and advertised
  capabilities. With a `RUN_ID` it diagnoses **stuck workers** (STALE/FAILED/
  RECOVERING states, overdue heartbeat deadlines, a live run with zero active
  workers) and **bad checkpoints** (no committed checkpoint, or checkpoints in a
  failed/aborted state). It exits non-zero (4) when it finds problems, so it is
  usable in alerting or CI health checks.

- **`checkpoint verify`** downloads every artifact of a checkpoint through the
  coordinator into a temp directory and re-checks each artifact's size and
  SHA-256 against the manifest — the fastest way to confirm whether a checkpoint
  is safe to restore from. With `--report-corrupt` it marks a failing checkpoint
  corrupt so the coordinator's "latest" pointer falls back to the newest
  remaining committed checkpoint.

## Output & scripting

Every command supports `--json` for machine-readable output. Errors in JSON mode
are emitted to stderr as `{"error", "status", "remediation"}`. Human mode prints
`error: <what> \n  hint: <what to do>`.

### Exit codes

| Code | Meaning |
|---|---|
| 0 | success |
| 1 | unexpected error |
| 2 | usage / invalid argument |
| 3 | run or checkpoint not found |
| 4 | invalid state for the action (`FAILED_PRECONDITION`) / doctor found issues / verify failed |
| 5 | coordinator unreachable (`UNAVAILABLE`) |
| 6 | authentication / permission denied |
| 7 | request timed out (`DEADLINE_EXCEEDED`) |

## Examples

```bash
# What's running right now?
dtruntime-admin runs list --state running

# A run looks stuck — diagnose it.
dtruntime-admin doctor run-42

# Pause for a maintenance window, then resume.
dtruntime-admin run pause run-42 --reason "gpu node cordon"
dtruntime-admin run resume run-42

# A run failed; confirm the latest checkpoint is good, then retry.
dtruntime-admin checkpoints list run-42 --committed-only
dtruntime-admin checkpoint verify run-42 ckpt-2026-07-03T22-00
dtruntime-admin run retry run-42

# Scripting: fail CI if any run is in a failed state.
test "$(dtruntime-admin --json runs list --state failed | jq length)" -eq 0
```
