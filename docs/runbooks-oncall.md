# On-call runbooks

Response procedures for the alerts in
`observability/prometheus/alerts.rules.yml`. Each alert's `runbook_url` links to
the matching section here. SLO context is in [slos.md](slos.md); failure
semantics in [run-lifecycle.md](run-lifecycle.md) and
[failure-recovery.md](failure-recovery.md).

## General triage toolkit

- **Health**: `curl http://<coord>:<metrics-addr>/healthz` (liveness),
  `/readyz` (readiness), `/metrics` (Prometheus).
- **Runs**: `dtruntime-admin runs list`, `dtruntime-admin runs describe <run>`.
- **Diagnose a run**: `dtruntime-admin doctor <run>` — flags stuck workers and
  bad checkpoints.
- **Workers / shards / checkpoints**: `dtruntime-admin workers list <run>`,
  `shards list <run>`, `checkpoints list <run>`.
- **Logs**: coordinator stdout (structured tracing) and the audit stream
  (DB / `--audit-log-file` NDJSON).
- **Metrics to pull**: `up`, `dtruntime_active_workers`,
  `dtruntime_stale_workers`, `dtruntime_checkpoints_started_total` vs
  `_committed_total` / `_failures_total`, `dtruntime_auth_denied_total`.

---

## CoordinatorDown

**Symptom**: `up{job="dtruntime-coordinator"} == 0` for >2m. The control plane is
unreachable; workers cannot rendezvous, heartbeat, or commit checkpoints.

**Diagnose**
1. Is the process/pod running? `kubectl get pods -n dtruntime` /
   `kubectl describe pod`. Check for `CrashLoopBackOff`, OOMKill, or a failed
   readiness probe.
2. Check logs for a panic, a metadata-store connection failure, or a bind error
   on the gRPC/metrics port.
3. If PostgreSQL-backed: is the DB reachable? A standby holding the advisory
   lock keeps its gRPC port unbound by design — confirm whether this instance is
   the standby (expected) or the leader has failed.

**Mitigate**
- Crash loop → read the panic; if metadata-store related, verify DB/credentials.
- Node/resource issue → reschedule; raise memory limits if OOMKilled.
- Leader failed with HA → the standby should acquire the lock and bind within
  its retry interval; if not, restart the standby.
- In-flight training tolerates a coordinator restart on the same metadata DB —
  state rehydrates (see [Failover test](#failover-and-recovery-verification)).

**Escalate** if the metadata store itself is down or corrupt.

## CoordinatorAbsent

**Symptom**: Prometheus has no `dtruntime-coordinator` target for >5m — a
monitoring/discovery gap, not necessarily an outage.

**Diagnose**: check the scrape config / `ServiceMonitor` selector, the
coordinator `--metrics-addr`, and network policy between Prometheus and the
coordinator. Cross-check `/healthz` directly.

**Mitigate**: fix the target list or service discovery. Confirm real liveness
out-of-band before assuming the service is healthy.

## AvailabilitySLO

Covers `CoordinatorAvailabilityFastBurn` / `CoordinatorAvailabilitySlowBurn` —
the availability error budget is burning faster than allowed (see
[slos.md](slos.md#1-availability--999)).

**Diagnose**: correlate the burn window with deploys, node churn, DB latency,
or repeated restarts. Fast burn usually means an ongoing/flapping outage; slow
burn means chronic low-grade unavailability (e.g. slow readiness, periodic OOM).

**Mitigate**: treat fast burn like `CoordinatorDown`. For slow burn, address the
chronic cause (right-size resources, fix a flapping probe, tune DB pool) and,
per the error-budget policy, freeze risky changes until the budget recovers.

## CheckpointFailuresElevated

**Symptom**: `dtruntime:checkpoint_failure:ratio_rate5m > 0.01` for 15m. Runs
are failing to durably persist progress — a crash now loses more work.

**Diagnose**
1. `dtruntime-admin checkpoints list <run>` — look for `failed`/`aborted`.
2. `dtruntime-admin checkpoint verify <run> <ckpt>` — checksum/size the
   artifacts to distinguish upload failures from corruption.
3. Storage backend: S3/MinIO reachable and not throttling/5xx? Bucket
   permissions and SSE/KMS config valid? Local disk full?
4. Worker logs for upload retries exhausted; coordinator logs for 2PC commit
   rejections.

**Mitigate**: restore storage capacity/permissions; if a specific checkpoint is
corrupt, `checkpoint verify --report-corrupt` so the latest pointer falls back.
Reduce `--checkpoint-every` pressure if the backend is saturated.

## CheckpointCommitStalled

**Symptom**: checkpoints started in the last 30m but **zero** committed — the 2PC
commit path or storage is wedged.

**Diagnose**: are all expected workers reporting artifacts (a 2PC commit needs
every owner's artifacts)? Check for a stuck/dead worker holding up the barrier
(`dtruntime-admin doctor <run>`), and storage-backend availability.

**Mitigate**: recover or evict the stuck worker so the run re-rendezvouses;
restore the storage backend. As a last resort, cancel and `retry` the run to
resume from the last committed checkpoint.

## HighStaleWorkerRatio

**Symptom**: >25% of the fleet stale/failed for 10m — network partition, node
pressure, or a mass preemption.

**Diagnose**: `dtruntime-admin workers list <run> --state stale` (and `failed`).
Correlate with node events (`kubectl get events`), spot/preemption notices, and
network health between workers and the coordinator.

**Mitigate**: the reconcile loop fails genuinely dead workers and the run
re-forms rendezvous with survivors (elastic bounds permitting). Add capacity or
move off preemptible nodes if churn is the cause. Watch that the run returns to
`RUNNING` rather than getting stuck in `RECOVERING`.

## WorkersSilentButPresent

**Symptom**: active workers but zero heartbeats for 5m — the run may be stuck
(deadlocked collective, hung dataloader) even though processes are alive.

**Diagnose**: `dtruntime-admin doctor <run>`; inspect a worker (stack dump / GPU
utilization). Distinguish "silent but working" (long step) from a true hang.

**Mitigate**: if hung, let the failure detector/stuck-run reconcile mark it and
recover, or `dtruntime-admin run drain`/`cancel` then `retry` to resume from the
last checkpoint.

## HighAuthDenialRate

**Symptom**: sustained >1/s authorization denials — misconfigured client, an
expired credential/rotated key, or a probing attacker.

**Diagnose**: inspect the audit stream (`--audit-log-file` NDJSON) for the
`AuthMetadataRejected` / `AuthorizationDenied` reasons and subjects. Common
causes: expired JWT, a key rotation that dropped a still-in-use `kid`, a client
using the wrong role, or an unauthenticated caller.

**Mitigate**: if legitimate (rotation dropped a key too early), restore the key
to the JWKS overlap window (see [secret-rotation.md](secret-rotation.md)). If a
client is misconfigured, fix its role/token. If it looks like probing, confirm
network exposure is intended and consider rate-limiting upstream.

---

## Failover and recovery verification

The `test_failover.py`, `test_chaos.py`, and `test_soak.py` gated tests
(`docs/observability-ops-tests.md`) exercise coordinator failover, worker
preemption/chaos, and long-running stability. Run them in staging after
infrastructure changes to validate these runbooks end to end.
