//! Dataset registration and the shard data plane: deterministic per-worker
//! assignment, lease renewal, and resumable progress tracking.

use super::*;

impl CoordinatorState {
    fn persist_dataset(&self, dataset: &DatasetRecord) {
        if let Some(store) = &self.metadata_store {
            store.save_dataset(dataset).expect("persist dataset");
        }
    }

    fn persist_shard_progress(&self, progress: &ShardProgressRecord) {
        if let Some(store) = &self.metadata_store {
            let millis = progress
                .updated_at
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            let entry = ShardProgressEntry {
                run_id: progress.run_id.clone(),
                dataset_id: progress.dataset_id.clone(),
                shard_id: progress.shard_id.clone(),
                worker_id: progress.worker_id.clone(),
                lease_epoch: progress.lease_epoch,
                progress_offset: progress.progress_offset,
                is_complete: progress.is_complete,
                updated_at_millis: millis,
            };
            // Shard progress is an at-least-once resume hint: if this write is
            // lost the worker simply re-processes a few records after a
            // coordinator restart.  Log and continue rather than crashing the
            // coordinator over a transient DB error in the hot path.
            if let Err(e) = store.save_shard_progress(&entry) {
                tracing::error!(
                    error = %e,
                    run_id = %entry.run_id,
                    shard_id = %entry.shard_id,
                    "failed to persist shard progress (resume offset may be stale)"
                );
            }
        }
    }

    /// Shard-progress / lease records for a run (optionally one dataset).
    pub fn list_shard_progress(
        &self,
        run_id: &str,
        dataset_id: Option<&str>,
    ) -> Vec<ShardProgressRecord> {
        let mut out: Vec<ShardProgressRecord> = self
            .shard_progress
            .iter()
            .filter(|e| e.run_id == run_id && dataset_id.is_none_or(|d| e.dataset_id == d))
            .map(|e| e.clone())
            .collect();
        out.sort_by(|a, b| {
            a.dataset_id
                .cmp(&b.dataset_id)
                .then(a.shard_id.cmp(&b.shard_id))
        });
        out
    }

    pub fn get_dataset(&self, run_id: &RunId, dataset_id: &DatasetId) -> Option<DatasetRecord> {
        self.datasets_by_run
            .get(&run_id.value)
            .and_then(|arc| arc.read().unwrap().get(&dataset_id.value).cloned())
    }

    pub fn register_dataset(&self, request: RegisterDatasetRequest) -> Option<RegisteredDataset> {
        let run_id = request.run_id.as_ref()?.value.clone();

        if !self.runs.contains_key(&run_id) {
            return None;
        }

        let idempotency_key = dataset_idempotency_key(&run_id, &request.idempotency_key);
        if !idempotency_key.is_empty() {
            if let Some(dataset_id) = self.dataset_idempotency_keys.get(&idempotency_key) {
                let dataset_id = dataset_id.clone();
                let record = self
                    .datasets_by_run
                    .get(&run_id)
                    .and_then(|arc| arc.read().unwrap().get(&dataset_id).cloned())
                    .expect("dataset idempotency key must reference an existing dataset");
                return Some(RegisteredDataset {
                    dataset_id: DatasetId {
                        value: record.dataset_id.clone(),
                    },
                    accepted_shard_count: record.shards.len() as u32,
                    registered_at: record.registered_at,
                });
            }
        }

        let dataset = request.dataset.unwrap_or_default();
        let dataset_id =
            dataset_id_from_metadata(&dataset).unwrap_or_else(|| self.generate_dataset_id());
        let registered_at = SystemTime::now();
        let shards = request
            .shards
            .into_iter()
            .map(|shard| self.shard_record(shard, &dataset_id))
            .collect::<Vec<_>>();

        let record = DatasetRecord {
            run_id: run_id.clone(),
            dataset_id: dataset_id.clone(),
            name: dataset.name,
            dataset_uri: dataset.dataset_uri,
            format: dataset.format,
            shard_count: dataset.shard_count,
            dataset_version: dataset.dataset_version,
            labels: dataset.labels,
            registered_at,
            shards,
        };
        let accepted_shard_count = record.shards.len() as u32;

        let datasets_arc = self
            .datasets_by_run
            .entry(run_id.clone())
            .or_insert_with(|| Arc::new(RwLock::new(HashMap::new())))
            .value()
            .clone();
        datasets_arc
            .write()
            .unwrap()
            .insert(dataset_id.clone(), record.clone());

        if !request.idempotency_key.is_empty() {
            self.persist_idempotency_key("dataset", &idempotency_key, &dataset_id);
            self.dataset_idempotency_keys
                .insert(idempotency_key, dataset_id.clone());
        }

        self.persist_dataset(&record);
        Some(RegisteredDataset {
            dataset_id: DatasetId { value: dataset_id },
            accepted_shard_count,
            registered_at,
        })
    }

    pub fn get_shard_assignments(
        &self,
        run_id: &RunId,
        worker_id: &WorkerId,
        dataset_id: &DatasetId,
        cached_shard_hints: Option<&[String]>,
    ) -> Result<ShardAssignments, GetShardAssignmentsError> {
        let run = self
            .runs
            .get(&run_id.value)
            .ok_or(GetShardAssignmentsError::RunNotFound)?
            .clone();

        let worker = self
            .workers_by_run
            .get(&run_id.value)
            .and_then(|arc| arc.read().unwrap().get(&worker_id.value).cloned())
            .ok_or(GetShardAssignmentsError::WorkerNotFound)?;

        let dataset = self
            .datasets_by_run
            .get(&run_id.value)
            .and_then(|arc| arc.read().unwrap().get(&dataset_id.value).cloned())
            .ok_or(GetShardAssignmentsError::DatasetNotFound)?;

        if run.world_size == 0 {
            return Err(GetShardAssignmentsError::EmptyWorldSize);
        }

        let server_time = SystemTime::now();
        let lease_expires_at = server_time + SHARD_LEASE_DURATION;
        let world_size = run.world_size as usize;
        let worker_rank = worker.rank as usize;

        let mut candidate_shards: Vec<ShardRecord> = dataset
            .shards
            .iter()
            .enumerate()
            .filter(|(shard_index, _)| *shard_index % world_size == worker_rank)
            .map(|(_, shard)| shard.clone())
            .collect();

        if self.placement_policy == DatasetPlacementPolicy::CacheAware {
            let hint_set: HashSet<&str> = cached_shard_hints
                .unwrap_or(&[])
                .iter()
                .map(String::as_str)
                .collect();
            candidate_shards.sort_by_key(|shard| {
                if hint_set.contains(shard.shard_id.as_str()) {
                    0usize
                } else {
                    1usize
                }
            });
        }

        let assignments = candidate_shards
            .into_iter()
            .map(|shard| ShardAssignmentRecord {
                run_id: run.run_id.clone(),
                dataset_id: dataset.dataset_id.clone(),
                shard_id: shard.shard_id.clone(),
                worker_id: worker.worker_id.clone(),
                lease_epoch: 1,
                lease_state: ShardLeaseState::Leased as i32,
                lease_expires_at,
                progress_offset: self
                    .shard_progress
                    .get(&ShardProgressKey {
                        run_id: run.run_id.clone(),
                        dataset_id: dataset.dataset_id.clone(),
                        shard_id: shard.shard_id.clone(),
                    })
                    .map(|p| p.progress_offset)
                    .unwrap_or_default(),
                shard_uri: shard.shard_uri,
                checksum: shard.checksum,
                checksum_algorithm: shard.checksum_algorithm,
                size_bytes: shard.size_bytes,
            })
            .collect();

        Ok(ShardAssignments {
            assignments,
            assignment_policy: run.assignment_policy,
            server_time,
        })
    }

    pub fn renew_shard_lease(
        &self,
        run_id: &RunId,
        worker_id: &WorkerId,
        shard_id: &ShardId,
        lease_epoch: u64,
    ) -> Result<RenewedShardLease, RenewShardLeaseError> {
        let run = self
            .runs
            .get(&run_id.value)
            .ok_or(RenewShardLeaseError::RunNotFound)?
            .clone();

        let worker = self
            .workers_by_run
            .get(&run_id.value)
            .and_then(|arc| arc.read().unwrap().get(&worker_id.value).cloned())
            .ok_or(RenewShardLeaseError::WorkerNotFound)?;

        let shard_index = self
            .datasets_by_run
            .get(&run_id.value)
            .and_then(|arc| {
                let datasets = arc.read().unwrap();
                datasets.values().find_map(|dataset| {
                    dataset
                        .shards
                        .iter()
                        .enumerate()
                        .find(|(_, shard)| shard.shard_id == shard_id.value)
                        .map(|(i, _)| i)
                })
            })
            .ok_or(RenewShardLeaseError::ShardNotFound)?;

        if run.world_size == 0 {
            return Err(RenewShardLeaseError::EmptyWorldSize);
        }

        let world_size = run.world_size as usize;
        let worker_rank = worker.rank as usize;
        if shard_index % world_size != worker_rank {
            return Err(RenewShardLeaseError::NotShardOwner);
        }
        Self::ensure_worker_active_for_lease_actions(&worker)
            .map_err(|_| RenewShardLeaseError::WorkerNotActive)?;

        if lease_epoch != 1 {
            return Err(RenewShardLeaseError::InvalidLeaseEpoch);
        }

        let server_time = SystemTime::now();
        Ok(RenewedShardLease {
            accepted: true,
            lease_state: ShardLeaseState::Leased as i32,
            lease_expires_at: server_time + SHARD_LEASE_DURATION,
            server_time,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn report_shard_progress(
        &self,
        run_id: &RunId,
        worker_id: &WorkerId,
        dataset_id: &DatasetId,
        shard_id: &ShardId,
        lease_epoch: u64,
        progress_offset: i64,
        is_complete: bool,
    ) -> Result<ReportedShardProgress, ReportShardProgressError> {
        let run = self
            .runs
            .get(&run_id.value)
            .ok_or(ReportShardProgressError::RunNotFound)?
            .clone();

        let worker = self
            .workers_by_run
            .get(&run_id.value)
            .and_then(|arc| arc.read().unwrap().get(&worker_id.value).cloned())
            .ok_or(ReportShardProgressError::WorkerNotFound)?;

        let (shard_index, _) = self
            .datasets_by_run
            .get(&run_id.value)
            .and_then(|arc| {
                let datasets = arc.read().unwrap();
                datasets.get(&dataset_id.value).and_then(|dataset| {
                    dataset
                        .shards
                        .iter()
                        .enumerate()
                        .find(|(_, shard)| shard.shard_id == shard_id.value)
                        .map(|(i, s)| (i, s.clone()))
                })
            })
            .ok_or(ReportShardProgressError::ShardNotFound)?;

        // Verify the dataset exists (separate from the shard check above).
        if !self
            .datasets_by_run
            .get(&run_id.value)
            .is_some_and(|arc| arc.read().unwrap().contains_key(&dataset_id.value))
        {
            return Err(ReportShardProgressError::DatasetNotFound);
        }

        if run.world_size == 0 {
            return Err(ReportShardProgressError::EmptyWorldSize);
        }

        let world_size = run.world_size as usize;
        let worker_rank = worker.rank as usize;
        if shard_index % world_size != worker_rank {
            return Err(ReportShardProgressError::NotShardOwner);
        }
        Self::ensure_worker_active_for_lease_actions(&worker)
            .map_err(|_| ReportShardProgressError::WorkerNotActive)?;

        if lease_epoch != 1 {
            return Err(ReportShardProgressError::InvalidLeaseEpoch);
        }

        let key = ShardProgressKey {
            run_id: run_id.value.clone(),
            dataset_id: dataset_id.value.clone(),
            shard_id: shard_id.value.clone(),
        };
        let record = ShardProgressRecord {
            run_id: run_id.value.clone(),
            dataset_id: dataset_id.value.clone(),
            shard_id: shard_id.value.clone(),
            worker_id: worker_id.value.clone(),
            lease_epoch,
            progress_offset,
            is_complete,
            updated_at: SystemTime::now(),
        };
        // persist shard progress to the durable store so we can
        // hydrate it on coordinator restart (resume-from-mid-shard).
        self.persist_shard_progress(&record);
        self.shard_progress.insert(key, record);

        Ok(ReportedShardProgress {
            accepted: true,
            stored_progress_offset: progress_offset,
            lease_state: ShardLeaseState::Leased as i32,
        })
    }

    pub fn get_shard_progress(
        &self,
        run_id: &RunId,
        dataset_id: &DatasetId,
        shard_id: &ShardId,
    ) -> Option<ShardProgressRecord> {
        self.shard_progress
            .get(&ShardProgressKey {
                run_id: run_id.value.clone(),
                dataset_id: dataset_id.value.clone(),
                shard_id: shard_id.value.clone(),
            })
            .map(|r| r.clone())
    }

    fn generate_dataset_id(&self) -> String {
        let id = self.next_dataset_id.fetch_add(1, Ordering::Relaxed) + 1;
        format!("dataset-{id}")
    }

    fn generate_shard_id(&self) -> String {
        let id = self.next_shard_id.fetch_add(1, Ordering::Relaxed) + 1;
        format!("shard-{id}")
    }

    fn shard_record(&self, shard: ShardMetadata, dataset_id: &str) -> ShardRecord {
        let shard_id = shard
            .shard_id
            .as_ref()
            .map(|s| s.value.as_str())
            .filter(|v| !v.is_empty())
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| self.generate_shard_id());
        let shard_dataset_id = shard
            .dataset_id
            .as_ref()
            .map(|d| d.value.as_str())
            .filter(|v| !v.is_empty())
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| dataset_id.to_string());

        ShardRecord {
            shard_id,
            dataset_id: shard_dataset_id,
            shard_uri: shard.shard_uri,
            byte_range_start: shard.byte_range_start,
            byte_range_end: shard.byte_range_end,
            size_bytes: shard.size_bytes,
            checksum: shard.checksum,
            checksum_algorithm: shard.checksum_algorithm,
        }
    }
}

fn dataset_idempotency_key(run_id: &str, idempotency_key: &str) -> String {
    scoped_idempotency_key(run_id, idempotency_key)
}

fn dataset_id_from_metadata(dataset: &DatasetMetadata) -> Option<String> {
    dataset
        .dataset_id
        .as_ref()
        .map(|d| d.value.as_str())
        .filter(|v| !v.is_empty())
        .map(ToOwned::to_owned)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::test_support::*;

    use tempfile::tempdir;

    #[test]
    fn stale_worker_cannot_renew_shard_lease() {
        let state = CoordinatorState::default();
        let run_id = state.register_run(register_request("run-key")).run_id;
        let worker_id = state
            .register_worker(worker_request(run_id.clone(), "worker-key"))
            .expect("worker should register")
            .worker_id;
        let _dataset = state
            .register_dataset(dataset_request(run_id.clone(), "dataset-key"))
            .expect("dataset should register");
        assert!(state.set_worker_heartbeat_deadline_for_test(
            &run_id,
            &worker_id,
            SystemTime::UNIX_EPOCH,
        ));
        state.reconcile_worker_liveness();
        let error = state
            .renew_shard_lease(
                &run_id,
                &worker_id,
                &ShardId {
                    value: "shard-test-0".to_string(),
                },
                1,
            )
            .expect_err("stale worker should not renew leases");
        assert_eq!(error, RenewShardLeaseError::WorkerNotActive);
    }

    #[test]
    fn failed_worker_cannot_report_shard_progress() {
        let state = CoordinatorState::default();
        let run_id = state.register_run(register_request("run-key")).run_id;
        let worker_id = state
            .register_worker(worker_request(run_id.clone(), "worker-key"))
            .expect("worker should register")
            .worker_id;
        let dataset = state
            .register_dataset(dataset_request(run_id.clone(), "dataset-key"))
            .expect("dataset should register");
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
        let dataset_id = dataset.dataset_id;
        let shard_id = ShardId {
            value: "shard-test-0".to_string(),
        };
        let error = state
            .report_shard_progress(&run_id, &worker_id, &dataset_id, &shard_id, 1, 42, false)
            .expect_err("failed worker should not report progress");
        assert_eq!(error, ReportShardProgressError::WorkerNotActive);
    }

    #[test]
    fn shard_progress_survives_coordinator_restart() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("coordinator.db");
        let store = SqliteMetadataStore::open(&path).unwrap();
        store.init_schema().unwrap();
        let state = CoordinatorState::with_metadata_store(Arc::new(store));

        let run_id = state.register_run(register_request("run-key")).run_id;
        let worker_id = state
            .register_worker(worker_request(run_id.clone(), "worker-key"))
            .expect("worker should register")
            .worker_id;
        let dataset = state
            .register_dataset(dataset_request(run_id.clone(), "dataset-key"))
            .expect("dataset should register");
        let dataset_id = dataset.dataset_id;
        let shard_id = ShardId {
            value: "shard-test-0".to_string(),
        };

        let reported = state
            .report_shard_progress(&run_id, &worker_id, &dataset_id, &shard_id, 1, 42, false)
            .expect("progress report should be accepted");
        assert_eq!(reported.stored_progress_offset, 42);
        drop(state);

        // Simulate a coordinator restart.
        let reopened = SqliteMetadataStore::open(&path).unwrap();
        reopened.init_schema().unwrap();
        let recreated = CoordinatorState::with_metadata_store(Arc::new(reopened));

        // The offset must be visible immediately after hydration, without the
        // worker having to re-send a progress report.
        let progress = recreated
            .get_shard_progress(&run_id, &dataset_id, &shard_id)
            .expect("shard progress should be hydrated after restart");
        assert_eq!(progress.progress_offset, 42);
        assert_eq!(progress.worker_id, worker_id.value);
        assert!(!progress.is_complete);
    }
}
