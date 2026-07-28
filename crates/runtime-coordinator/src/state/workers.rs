//! Worker registration, heartbeat liveness, graceful deregistration, and the
//! rendezvous barrier that assigns ranks for `torch.distributed`.

use super::*;

impl CoordinatorState {
    pub(super) fn ensure_worker_active_for_lease_actions(
        worker: &WorkerRecord,
    ) -> Result<(), WorkerState> {
        if worker.state == WorkerState::Active as i32 {
            return Ok(());
        }
        Err(WorkerState::try_from(worker.state).unwrap_or(WorkerState::Unspecified))
    }

    fn persist_worker(&self, worker: &WorkerRecord, world_size: u32) {
        if let Some(store) = &self.metadata_store {
            store
                .save_worker(worker, world_size)
                .expect("persist worker");
        }
    }

    pub fn worker_counts(&self, run_id: &RunId) -> Option<(u64, u64)> {
        let arc = self.workers_by_run.get(&run_id.value)?;
        let workers = arc.read().unwrap();
        let active = workers
            .values()
            .filter(|w| w.state == WorkerState::Active as i32)
            .count() as u64;
        let stale = workers
            .values()
            .filter(|w| w.state == WorkerState::Stale as i32)
            .count() as u64;
        Some((active, stale))
    }

    /// Active/stale/failed totals across every run, for the periodic
    /// reconcile task to refresh the worker gauges. Heartbeats only refresh
    /// the active/stale gauges on a state flip, so without this a worker
    /// that dies silently would leave the gauges frozen at their last value.
    pub fn global_worker_counts(&self) -> (u64, u64, u64) {
        let mut active = 0u64;
        let mut stale = 0u64;
        let mut failed = 0u64;
        for entry in self.workers_by_run.iter() {
            let workers = entry.value().read().unwrap();
            for w in workers.values() {
                if w.state == WorkerState::Active as i32 {
                    active += 1;
                } else if w.state == WorkerState::Stale as i32 {
                    stale += 1;
                } else if w.state == WorkerState::Failed as i32 {
                    failed += 1;
                }
            }
        }
        (active, stale, failed)
    }

    /// Reconciles in-memory worker liveness against the current server time.
    ///
    /// Marks active workers stale when heartbeat deadlines pass and stale
    /// workers failed when the longer failure deadline passes.  Workers that
    /// newly transition to FAILED are removed from their run's rendezvous:
    /// a completed round they held a rank in is invalidated so the surviving
    /// workers re-rendezvous (rank replacement / elastic shrink).
    pub fn reconcile_worker_liveness(&self) {
        let now = SystemTime::now();
        let newly_failed = reconcile_worker_liveness(&self.runs, &self.workers_by_run, now);
        for (run_id, worker_id) in newly_failed {
            let invalidated = self
                .rendezvous_by_run
                .get(&run_id)
                .map(|r| r.lock().unwrap().on_worker_failed(&worker_id, now))
                .unwrap_or(false);
            if invalidated {
                tracing::info!(
                    run_id = %run_id,
                    worker_id = %worker_id,
                    "worker failure invalidated rendezvous round; remaining workers must re-join"
                );
                self.set_run_state(
                    &run_id,
                    RunState::Recovering,
                    format!(
                        "worker {worker_id} failed (heartbeat timeout); re-forming process group"
                    ),
                );
            }
        }
    }

    /// Graceful worker departure.  Takes effect immediately: the rendezvous
    /// membership changes now rather than after the heartbeat failure
    /// deadline, and the run lifecycle advances when the roster empties.
    pub fn deregister_worker(
        &self,
        run_id: &str,
        worker_id: &str,
        reason: DeregisterReason,
    ) -> Result<(i32, i32), DeregisterWorkerError> {
        if !self.runs.contains_key(run_id) {
            return Err(DeregisterWorkerError::RunNotFound);
        }
        let workers_arc = self
            .workers_by_run
            .get(run_id)
            .ok_or(DeregisterWorkerError::WorkerNotFound)?
            .clone();

        let new_worker_state = match reason {
            DeregisterReason::Completed => WorkerState::Completed,
            DeregisterReason::Preempted => WorkerState::Removed,
            _ => WorkerState::Failed,
        };
        let (worker_snapshot, world_size) = {
            let mut workers = workers_arc.write().unwrap();
            let worker = workers
                .get_mut(worker_id)
                .ok_or(DeregisterWorkerError::WorkerNotFound)?;
            worker.state = new_worker_state as i32;
            let snapshot = worker.clone();
            let world_size = self
                .runs
                .get(run_id)
                .map(|r| r.world_size)
                .unwrap_or_default();
            (snapshot, world_size)
        };
        refresh_active_worker_count(&self.runs, &self.workers_by_run, run_id);
        self.persist_worker(&worker_snapshot, world_size);

        let now = SystemTime::now();
        // A completed worker leaving is expected (everyone finishes together
        // under DDP) and must not force survivors to re-rendezvous; any other
        // departure invalidates the round like a failure would.
        if reason != DeregisterReason::Completed {
            let invalidated = self
                .rendezvous_by_run
                .get(run_id)
                .map(|r| r.lock().unwrap().on_worker_failed(worker_id, now))
                .unwrap_or(false);
            if invalidated {
                self.set_run_state(
                    run_id,
                    RunState::Recovering,
                    format!(
                        "worker {worker_id} deregistered ({reason:?}); re-forming process group"
                    ),
                );
            }
        }

        // Run completion/failure from the roster.
        let roster: Vec<i32> = workers_arc
            .read()
            .unwrap()
            .values()
            .map(|w| w.state)
            .collect();
        let all = |target: WorkerState| -> bool {
            !roster.is_empty() && roster.iter().all(|s| *s == target as i32)
        };
        if all(WorkerState::Completed) {
            self.set_run_state(run_id, RunState::Completed, "all workers completed");
        } else if !roster.is_empty()
            && roster.iter().all(|s| {
                *s == WorkerState::Failed as i32
                    || *s == WorkerState::Removed as i32
                    || *s == WorkerState::Completed as i32
            })
            && roster.contains(&(WorkerState::Failed as i32))
        {
            self.set_run_state(
                run_id,
                RunState::Failed,
                "no live workers remain and at least one reported an error",
            );
        }

        let run_state = self.runs.get(run_id).map(|r| r.state).unwrap_or_default();
        Ok((new_worker_state as i32, run_state))
    }

    /// Workers for a run (optionally filtered to one state), by rank then id.
    ///
    /// Returns `None` only when the run itself does not exist; a run with no
    /// registered workers (e.g. still PENDING, or just retried) yields
    /// `Some(vec![])` so callers can tell "unknown run" from "no workers yet".
    pub fn list_workers(
        &self,
        run_id: &str,
        state_filter: Option<i32>,
    ) -> Option<Vec<WorkerRecord>> {
        if !self.runs.contains_key(run_id) {
            return None;
        }
        let mut workers: Vec<WorkerRecord> = match self.workers_by_run.get(run_id) {
            Some(arc) => arc
                .read()
                .unwrap()
                .values()
                .filter(|w| state_filter.is_none_or(|s| w.state == s))
                .cloned()
                .collect(),
            None => Vec::new(),
        };
        workers.sort_by(|a, b| a.rank.cmp(&b.rank).then(a.worker_id.cmp(&b.worker_id)));
        Some(workers)
    }

    /// Fetch or lazily create the rendezvous for a run using the run's
    /// world-size / elasticity configuration.
    fn rendezvous_for_run(&self, run: &RunRecord) -> Arc<Mutex<RunRendezvous>> {
        self.rendezvous_by_run
            .entry(run.run_id.clone())
            .or_insert_with(|| {
                let settle_timeout = if run.rendezvous_settle_timeout_ms > 0 {
                    Duration::from_millis(u64::from(run.rendezvous_settle_timeout_ms))
                } else {
                    crate::rendezvous::DEFAULT_SETTLE_TIMEOUT
                };
                Arc::new(Mutex::new(RunRendezvous::new(RendezvousConfig {
                    world_size: run.world_size,
                    min_world_size: run.min_world_size,
                    max_world_size: run.max_world_size,
                    settle_timeout,
                })))
            })
            .value()
            .clone()
    }

    fn require_registered_worker(
        &self,
        run_id: &str,
        worker_id: &str,
    ) -> Result<RunRecord, RendezvousError> {
        let run = self
            .runs
            .get(run_id)
            .map(|r| r.clone())
            .ok_or(RendezvousError::RunNotFound)?;
        let known = self
            .workers_by_run
            .get(run_id)
            .map(|arc| arc.read().unwrap().contains_key(worker_id))
            .unwrap_or(false);
        if !known {
            return Err(RendezvousError::WorkerNotFound);
        }
        Ok(run)
    }

    pub fn join_rendezvous(
        &self,
        run_id: &str,
        worker_id: &str,
        host: &str,
        store_port: u32,
    ) -> Result<RendezvousSnapshot, RendezvousError> {
        if host.is_empty() {
            return Err(RendezvousError::MissingHost);
        }
        let run = self.require_registered_worker(run_id, worker_id)?;
        if run.is_terminal() {
            return Err(RendezvousError::RunTerminal(run.state));
        }
        let rendezvous = self.rendezvous_for_run(&run);
        let snapshot =
            rendezvous
                .lock()
                .unwrap()
                .join(worker_id, host, store_port, SystemTime::now());
        if snapshot.complete {
            self.set_run_state(
                run_id,
                RunState::Running,
                format!(
                    "rendezvous round {} complete (world size {})",
                    snapshot.round, snapshot.world_size
                ),
            );
        }
        Ok(snapshot)
    }

    pub fn get_rendezvous_state(
        &self,
        run_id: &str,
        worker_id: &str,
    ) -> Result<RendezvousSnapshot, RendezvousError> {
        let run = self.require_registered_worker(run_id, worker_id)?;
        if run.is_terminal() {
            return Err(RendezvousError::RunTerminal(run.state));
        }
        let rendezvous = self.rendezvous_for_run(&run);
        let snapshot = rendezvous
            .lock()
            .unwrap()
            .state_for(worker_id, SystemTime::now());
        if snapshot.complete {
            // Elastic settle windows complete lazily on polls, so the
            // PENDING/RECOVERING → RUNNING transition can land here too.
            self.set_run_state(
                run_id,
                RunState::Running,
                format!(
                    "rendezvous round {} complete (world size {})",
                    snapshot.round, snapshot.world_size
                ),
            );
        }
        Ok(snapshot)
    }

    /// Latest rendezvous round for a run, or 0 when no rendezvous exists.
    /// Reported on heartbeat responses so workers can cheaply detect
    /// membership changes.
    pub fn current_rendezvous_round(&self, run_id: &str) -> u64 {
        self.rendezvous_by_run
            .get(run_id)
            .map(|r| r.lock().unwrap().current_round())
            .unwrap_or(0)
    }

    pub fn register_worker(&self, request: RegisterWorkerRequest) -> Option<RegisteredWorker> {
        let run_id = request.run_id.as_ref()?.value.clone();

        if !self.runs.contains_key(&run_id) {
            return None;
        }

        let world_size = self
            .runs
            .get(&run_id)
            .expect("run must exist when registering worker")
            .world_size;

        let idempotency_key = worker_idempotency_key(&run_id, &request.idempotency_key);
        if !idempotency_key.is_empty() {
            if let Some(existing_worker_id) = self.worker_idempotency_keys.get(&idempotency_key) {
                let existing_worker_id = existing_worker_id.clone();
                let record = self
                    .workers_by_run
                    .get(&run_id)
                    .and_then(|arc| arc.read().unwrap().get(&existing_worker_id).cloned())
                    .expect("worker idempotency key must reference an existing worker");
                return Some(RegisteredWorker {
                    worker_id: WorkerId {
                        value: record.worker_id.clone(),
                    },
                    state: record.state,
                    lease_renewal_deadline: record.registered_at + HEARTBEAT_INTERVAL,
                    server_time: record.registered_at,
                    is_new: false,
                });
            }
        }

        let worker_id = request
            .worker_id
            .as_ref()
            .map(|w| w.value.as_str())
            .filter(|v| !v.is_empty())
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| self.generate_worker_id());
        let now = SystemTime::now();
        let next_heartbeat_deadline = now + HEARTBEAT_INTERVAL;
        let failure_deadline = now + WORKER_FAILURE_INTERVAL;
        let worker = WorkerRecord {
            run_id: run_id.clone(),
            worker_id: worker_id.clone(),
            rank: request.rank,
            host: request.host,
            sdk_version: request.sdk_version,
            protocol_version: request.protocol_version,
            state: WorkerState::Active as i32,
            registered_at: now,
            last_heartbeat_at: now,
            next_heartbeat_deadline,
            failure_deadline,
        };

        let workers_arc = self
            .workers_by_run
            .entry(run_id.clone())
            .or_insert_with(|| Arc::new(RwLock::new(HashMap::new())))
            .value()
            .clone();
        workers_arc
            .write()
            .unwrap()
            .insert(worker_id.clone(), worker.clone());

        refresh_active_worker_count(&self.runs, &self.workers_by_run, &run_id);

        if !request.idempotency_key.is_empty() {
            self.persist_idempotency_key("worker", &idempotency_key, &worker_id);
            self.worker_idempotency_keys
                .insert(idempotency_key, worker_id.clone());
        }

        self.persist_worker(&worker, world_size);

        Some(RegisteredWorker {
            worker_id: WorkerId { value: worker_id },
            state: WorkerState::Active as i32,
            lease_renewal_deadline: next_heartbeat_deadline,
            server_time: now,
            is_new: true,
        })
    }

    pub fn record_heartbeat(
        &self,
        run_id: &RunId,
        worker_id: &WorkerId,
    ) -> Option<WorkerHeartbeat> {
        if !self.runs.contains_key(&run_id.value) {
            return None;
        }

        let workers_arc = self.workers_by_run.get(&run_id.value)?;
        let mut workers = workers_arc.write().unwrap();
        let worker = workers.get_mut(&worker_id.value)?;

        let (run_state, run_terminal, world_size) = {
            let run = self
                .runs
                .get(&run_id.value)
                .expect("run must exist when recording heartbeat");
            (run.state, run.is_terminal(), run.world_size)
        };

        let now = SystemTime::now();
        let next_heartbeat_deadline = now + HEARTBEAT_INTERVAL;
        let failure_deadline = now + WORKER_FAILURE_INTERVAL;
        worker.last_heartbeat_at = now;
        worker.next_heartbeat_deadline = next_heartbeat_deadline;
        worker.failure_deadline = failure_deadline;
        // A heartbeat re-activates a stale/failed worker, but never resurrects
        // one that already deregistered, and never revives workers of a
        // terminal run — the response tells them to exit instead.
        let prev_state = worker.state;
        if !run_terminal
            && worker.state != WorkerState::Completed as i32
            && worker.state != WorkerState::Removed as i32
        {
            worker.state = WorkerState::Active as i32;
        }
        let state = worker.state;
        let state_changed = state != prev_state;
        let worker_snapshot = worker.clone();
        drop(workers);

        // Recounting active workers walks the whole roster; only pay that
        // when this heartbeat actually flipped the worker's state.
        if state_changed {
            refresh_active_worker_count(&self.runs, &self.workers_by_run, &run_id.value);
        }
        self.persist_worker(&worker_snapshot, world_size);

        Some(WorkerHeartbeat {
            state,
            next_heartbeat_deadline,
            server_time: now,
            run_state,
            state_changed,
        })
    }

    #[cfg(test)]
    pub fn set_worker_heartbeat_deadline_for_test(
        &self,
        run_id: &RunId,
        worker_id: &WorkerId,
        deadline: SystemTime,
    ) -> bool {
        self.workers_by_run
            .get(&run_id.value)
            .and_then(|arc| {
                arc.write().unwrap().get_mut(&worker_id.value).map(|w| {
                    w.next_heartbeat_deadline = deadline;
                })
            })
            .is_some()
    }

    #[cfg(test)]
    pub fn set_worker_failure_deadline_for_test(
        &self,
        run_id: &RunId,
        worker_id: &WorkerId,
        deadline: SystemTime,
    ) -> bool {
        self.workers_by_run
            .get(&run_id.value)
            .and_then(|arc| {
                arc.write().unwrap().get_mut(&worker_id.value).map(|w| {
                    w.failure_deadline = deadline;
                })
            })
            .is_some()
    }

    /// Test helper: read a single worker record without going through the gRPC layer.
    #[cfg(test)]
    pub fn get_worker_for_test(
        &self,
        run_id: &RunId,
        worker_id: &WorkerId,
    ) -> Option<WorkerRecord> {
        self.workers_by_run
            .get(&run_id.value)
            .and_then(|arc| arc.read().unwrap().get(&worker_id.value).cloned())
    }

    fn generate_worker_id(&self) -> String {
        let id = self.next_worker_id.fetch_add(1, Ordering::Relaxed) + 1;
        format!("worker-{id}")
    }
}

/// Returns the (run_id, worker_id) pairs of workers that newly transitioned
/// to FAILED during this pass, so the caller can propagate the failure to
/// per-run rendezvous state.
fn reconcile_worker_liveness(
    runs: &DashMap<String, RunRecord>,
    workers_by_run: &DashMap<String, Arc<RwLock<HashMap<String, WorkerRecord>>>>,
    now: SystemTime,
) -> Vec<(String, String)> {
    let mut changed_run_ids = Vec::new();
    let mut newly_failed = Vec::new();

    for run_entry in workers_by_run.iter() {
        let run_id = run_entry.key().clone();
        let workers_arc = run_entry.value().clone();
        let mut changed = false;
        {
            let mut workers = workers_arc.write().unwrap();
            for worker in workers.values_mut() {
                if worker.state == WorkerState::Active as i32 {
                    if worker.next_heartbeat_deadline < now {
                        worker.state = WorkerState::Stale as i32;
                        changed = true;
                    }
                } else if worker.state == WorkerState::Stale as i32 && worker.failure_deadline < now
                {
                    worker.state = WorkerState::Failed as i32;
                    newly_failed.push((run_id.clone(), worker.worker_id.clone()));
                    changed = true;
                }
            }
        }
        if changed {
            changed_run_ids.push(run_id);
        }
    }

    for run_id in changed_run_ids {
        refresh_active_worker_count(runs, workers_by_run, &run_id);
    }

    newly_failed
}

pub(super) fn refresh_active_worker_count(
    runs: &DashMap<String, RunRecord>,
    workers_by_run: &DashMap<String, Arc<RwLock<HashMap<String, WorkerRecord>>>>,
    run_id: &str,
) {
    let active_worker_count = workers_by_run
        .get(run_id)
        .map(|arc| {
            arc.read()
                .unwrap()
                .values()
                .filter(|w| w.state == WorkerState::Active as i32)
                .count() as u32
        })
        .unwrap_or_default();
    if let Some(mut run) = runs.get_mut(run_id) {
        run.active_worker_count = active_worker_count;
    }
}

fn worker_idempotency_key(run_id: &str, idempotency_key: &str) -> String {
    scoped_idempotency_key(run_id, idempotency_key)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::test_support::*;

    #[test]
    fn active_worker_count_is_unchanged_before_heartbeat_deadline() {
        let state = CoordinatorState::default();
        let run_id = state.register_run(register_request("run-key")).run_id;
        let worker_id = state
            .register_worker(worker_request(run_id.clone(), "worker-key"))
            .expect("worker should register")
            .worker_id;

        assert!(state.set_worker_heartbeat_deadline_for_test(
            &run_id,
            &worker_id,
            SystemTime::now() + Duration::from_secs(60),
        ));

        state.reconcile_worker_liveness();

        let record = state.get_run(&run_id).expect("run should exist");
        let worker = state
            .get_worker_for_test(&run_id, &worker_id)
            .expect("worker should exist");
        assert_eq!(record.active_worker_count, 1);
        assert_eq!(worker.state, WorkerState::Active as i32);
    }

    #[test]
    fn reconcile_worker_liveness_marks_expired_active_workers_stale() {
        let state = CoordinatorState::default();
        let run_id = state.register_run(register_request("run-key")).run_id;
        let worker_id = state
            .register_worker(worker_request(run_id.clone(), "worker-key"))
            .expect("worker should register")
            .worker_id;

        assert!(state.set_worker_heartbeat_deadline_for_test(
            &run_id,
            &worker_id,
            SystemTime::UNIX_EPOCH,
        ));

        state.reconcile_worker_liveness();

        let record = state.get_run(&run_id).expect("run should exist");
        let worker = state
            .get_worker_for_test(&run_id, &worker_id)
            .expect("worker should exist");
        assert_eq!(worker.state, WorkerState::Stale as i32);
        assert_eq!(record.active_worker_count, 0);
    }

    #[test]
    fn heartbeat_reactivates_stale_worker() {
        let state = CoordinatorState::default();
        let run_id = state.register_run(register_request("run-key")).run_id;
        let worker_id = state
            .register_worker(worker_request(run_id.clone(), "worker-key"))
            .expect("worker should register")
            .worker_id;
        assert!(state.set_worker_heartbeat_deadline_for_test(
            &run_id,
            &worker_id,
            SystemTime::UNIX_EPOCH,
        ));
        state.reconcile_worker_liveness();

        let heartbeat = state
            .record_heartbeat(&run_id, &worker_id)
            .expect("stale worker heartbeat should succeed");

        let record = state.get_run(&run_id).expect("run should exist");
        assert_eq!(heartbeat.state, WorkerState::Active as i32);
        assert_eq!(record.active_worker_count, 1);
    }

    #[test]
    fn reconcile_worker_liveness_marks_stale_workers_failed_after_failure_deadline() {
        let state = CoordinatorState::default();
        let run_id = state.register_run(register_request("run-key")).run_id;
        let worker_id = state
            .register_worker(worker_request(run_id.clone(), "worker-key"))
            .expect("worker should register")
            .worker_id;

        assert!(state.set_worker_heartbeat_deadline_for_test(
            &run_id,
            &worker_id,
            SystemTime::UNIX_EPOCH,
        ));
        state.reconcile_worker_liveness();
        assert!(state.set_worker_failure_deadline_for_test(
            &run_id,
            &worker_id,
            SystemTime::UNIX_EPOCH,
        ));

        state.reconcile_worker_liveness();

        let record = state.get_run(&run_id).expect("run should exist");
        let worker = state
            .get_worker_for_test(&run_id, &worker_id)
            .expect("worker should exist");
        assert_eq!(worker.state, WorkerState::Failed as i32);
        assert_eq!(record.active_worker_count, 0);
    }

    #[test]
    fn heartbeat_reactivates_failed_worker() {
        let state = CoordinatorState::default();
        let run_id = state.register_run(register_request("run-key")).run_id;
        let worker_id = state
            .register_worker(worker_request(run_id.clone(), "worker-key"))
            .expect("worker should register")
            .worker_id;
        assert!(state.set_worker_heartbeat_deadline_for_test(
            &run_id,
            &worker_id,
            SystemTime::UNIX_EPOCH,
        ));
        state.reconcile_worker_liveness();
        assert!(state.set_worker_failure_deadline_for_test(
            &run_id,
            &worker_id,
            SystemTime::UNIX_EPOCH,
        ));
        state.reconcile_worker_liveness();

        let heartbeat = state
            .record_heartbeat(&run_id, &worker_id)
            .expect("failed worker heartbeat should succeed");

        let record = state.get_run(&run_id).expect("run should exist");
        let worker = state
            .get_worker_for_test(&run_id, &worker_id)
            .expect("worker should exist");
        assert_eq!(heartbeat.state, WorkerState::Active as i32);
        assert_eq!(worker.state, WorkerState::Active as i32);
        assert_eq!(record.active_worker_count, 1);
    }

    #[test]
    fn worker_death_simulation_marks_worker_failed() {
        let state = CoordinatorState::default();
        let run_id = state.register_run(register_request("run-key")).run_id;
        let worker_id = state
            .register_worker(worker_request(run_id.clone(), "worker-key"))
            .expect("worker should register")
            .worker_id;

        assert_eq!(
            state
                .get_run(&run_id)
                .expect("run should exist")
                .active_worker_count,
            1
        );

        assert!(state.set_worker_heartbeat_deadline_for_test(
            &run_id,
            &worker_id,
            SystemTime::UNIX_EPOCH,
        ));
        state.reconcile_worker_liveness();

        let stale_worker = state
            .get_worker_for_test(&run_id, &worker_id)
            .expect("worker should exist");
        assert_eq!(stale_worker.state, WorkerState::Stale as i32);

        assert!(state.set_worker_failure_deadline_for_test(
            &run_id,
            &worker_id,
            SystemTime::UNIX_EPOCH,
        ));
        state.reconcile_worker_liveness();

        let failed_worker = state
            .get_worker_for_test(&run_id, &worker_id)
            .expect("worker should exist");
        assert_eq!(failed_worker.state, WorkerState::Failed as i32);
    }

    #[test]
    fn failed_worker_is_excluded_from_run_status() {
        let state = CoordinatorState::default();
        let run_id = state.register_run(register_request("run-key")).run_id;
        let worker_id = state
            .register_worker(worker_request(run_id.clone(), "worker-key"))
            .expect("worker should register")
            .worker_id;

        assert!(state.set_worker_heartbeat_deadline_for_test(
            &run_id,
            &worker_id,
            SystemTime::UNIX_EPOCH,
        ));
        state.reconcile_worker_liveness();
        assert!(state.set_worker_failure_deadline_for_test(
            &run_id,
            &worker_id,
            SystemTime::UNIX_EPOCH,
        ));
        state.reconcile_worker_liveness();

        let run = state.get_run(&run_id).expect("run should exist");
        assert_eq!(run.active_worker_count, 0);
    }

    #[test]
    fn failed_worker_record_is_retained() {
        let state = CoordinatorState::default();
        let run_id = state.register_run(register_request("run-key")).run_id;
        let worker_id = state
            .register_worker(worker_request(run_id.clone(), "worker-key"))
            .expect("worker should register")
            .worker_id;

        assert!(state.set_worker_heartbeat_deadline_for_test(
            &run_id,
            &worker_id,
            SystemTime::UNIX_EPOCH,
        ));
        state.reconcile_worker_liveness();
        assert!(state.set_worker_failure_deadline_for_test(
            &run_id,
            &worker_id,
            SystemTime::UNIX_EPOCH,
        ));
        state.reconcile_worker_liveness();

        let worker = state.get_worker_for_test(&run_id, &worker_id);
        assert!(worker.is_some(), "failed worker record should be retained");
    }

    #[test]
    fn heartbeat_after_simulated_death_reactivates_worker() {
        let state = CoordinatorState::default();
        let run_id = state.register_run(register_request("run-key")).run_id;
        let worker_id = state
            .register_worker(worker_request(run_id.clone(), "worker-key"))
            .expect("worker should register")
            .worker_id;

        assert!(state.set_worker_heartbeat_deadline_for_test(
            &run_id,
            &worker_id,
            SystemTime::UNIX_EPOCH,
        ));
        state.reconcile_worker_liveness();
        assert!(state.set_worker_failure_deadline_for_test(
            &run_id,
            &worker_id,
            SystemTime::UNIX_EPOCH,
        ));
        state.reconcile_worker_liveness();

        let heartbeat = state
            .record_heartbeat(&run_id, &worker_id)
            .expect("failed worker heartbeat should succeed");
        let worker = state
            .get_worker_for_test(&run_id, &worker_id)
            .expect("worker should exist");
        let run = state.get_run(&run_id).expect("run should exist");

        assert_eq!(heartbeat.state, WorkerState::Active as i32);
        assert_eq!(worker.state, WorkerState::Active as i32);
        assert_eq!(run.active_worker_count, 1);
    }

    #[test]
    fn run_starts_pending_and_becomes_running_when_rendezvous_completes() {
        let state = CoordinatorState::default();
        let run_id = register_lifecycle_run(&state, 2);
        assert_eq!(run_state_of(&state, &run_id), RunState::Pending as i32);

        register_lifecycle_worker(&state, &run_id, "w0");
        register_lifecycle_worker(&state, &run_id, "w1");
        state.join_rendezvous(&run_id.value, "w0", "h0", 0).unwrap();
        assert_eq!(run_state_of(&state, &run_id), RunState::Pending as i32);
        state.join_rendezvous(&run_id.value, "w1", "h1", 0).unwrap();
        assert_eq!(run_state_of(&state, &run_id), RunState::Running as i32);
    }

    #[test]
    fn worker_heartbeat_failure_moves_running_run_to_recovering() {
        let state = CoordinatorState::default();
        let run_id = register_lifecycle_run(&state, 2);
        register_lifecycle_worker(&state, &run_id, "w0");
        register_lifecycle_worker(&state, &run_id, "w1");
        state.join_rendezvous(&run_id.value, "w0", "h0", 0).unwrap();
        state.join_rendezvous(&run_id.value, "w1", "h1", 0).unwrap();
        assert_eq!(run_state_of(&state, &run_id), RunState::Running as i32);

        // Drive w1 through stale → failed via expired deadlines.
        let past = SystemTime::now() - Duration::from_secs(1);
        let w1 = WorkerId {
            value: "w1".to_string(),
        };
        assert!(state.set_worker_heartbeat_deadline_for_test(&run_id, &w1, past));
        assert!(state.set_worker_failure_deadline_for_test(&run_id, &w1, past));
        // Two passes: active → stale, then stale → failed.
        state.reconcile_worker_liveness();
        state.reconcile_worker_liveness();

        assert_eq!(run_state_of(&state, &run_id), RunState::Recovering as i32);
        let run = state.get_run(&run_id).unwrap();
        assert!(
            run.state_reason.contains("w1"),
            "reason: {}",
            run.state_reason
        );

        // Replacement joins; survivors re-join → RUNNING again.
        state.join_rendezvous(&run_id.value, "w0", "h0", 0).unwrap();
        register_lifecycle_worker(&state, &run_id, "w2");
        state.join_rendezvous(&run_id.value, "w2", "h2", 0).unwrap();
        assert_eq!(run_state_of(&state, &run_id), RunState::Running as i32);
    }

    #[test]
    fn all_workers_completing_marks_run_completed_and_terminal_is_final() {
        let state = CoordinatorState::default();
        let run_id = register_lifecycle_run(&state, 2);
        register_lifecycle_worker(&state, &run_id, "w0");
        register_lifecycle_worker(&state, &run_id, "w1");

        let (worker_state, run_state) = state
            .deregister_worker(&run_id.value, "w0", DeregisterReason::Completed)
            .unwrap();
        assert_eq!(worker_state, WorkerState::Completed as i32);
        assert_ne!(run_state, RunState::Completed as i32);

        let (_, run_state) = state
            .deregister_worker(&run_id.value, "w1", DeregisterReason::Completed)
            .unwrap();
        assert_eq!(run_state, RunState::Completed as i32);

        // Terminal states are final: cancel after completion is a no-op.
        state.cancel_run(&run_id.value, "too late").unwrap();
        assert_eq!(run_state_of(&state, &run_id), RunState::Completed as i32);
    }

    #[test]
    fn completed_worker_is_not_resurrected_by_heartbeat() {
        let state = CoordinatorState::default();
        let run_id = register_lifecycle_run(&state, 2);
        register_lifecycle_worker(&state, &run_id, "w0");
        state
            .deregister_worker(&run_id.value, "w0", DeregisterReason::Completed)
            .unwrap();

        let heartbeat = state
            .record_heartbeat(
                &run_id,
                &WorkerId {
                    value: "w0".to_string(),
                },
            )
            .expect("heartbeat still answered");
        assert_eq!(heartbeat.state, WorkerState::Completed as i32);
    }

    #[test]
    fn preempted_deregistration_invalidates_round_and_recovers() {
        let state = CoordinatorState::default();
        let run_id = register_lifecycle_run(&state, 2);
        register_lifecycle_worker(&state, &run_id, "w0");
        register_lifecycle_worker(&state, &run_id, "w1");
        state.join_rendezvous(&run_id.value, "w0", "h0", 0).unwrap();
        state.join_rendezvous(&run_id.value, "w1", "h1", 0).unwrap();

        let (worker_state, run_state) = state
            .deregister_worker(&run_id.value, "w1", DeregisterReason::Preempted)
            .unwrap();
        assert_eq!(worker_state, WorkerState::Removed as i32);
        assert_eq!(run_state, RunState::Recovering as i32);
    }
}
