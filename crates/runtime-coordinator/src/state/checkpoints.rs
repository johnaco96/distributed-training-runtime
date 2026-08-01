//! The distributed checkpoint two-phase commit: a checkpoint becomes committed
//! only once every expected worker has reported its artifacts and voted, so a
//! crash always resumes from a globally consistent state.

use super::*;

impl CoordinatorState {
    fn persist_checkpoint(&self, run_id: &str, idem: &str, rec: &CoordinatorCheckpointRecord) {
        if let Some(store) = &self.metadata_store {
            store
                .save_checkpoint(run_id, idem, rec)
                .expect("persist checkpoint");
        }
    }

    fn persist_artifact(&self, run_id: &str, checkpoint_id: &str, artifact: &ArtifactRecord) {
        if let Some(store) = &self.metadata_store {
            store
                .save_checkpoint_artifact(run_id, checkpoint_id, artifact)
                .expect("persist artifact");
        }
    }

    pub fn begin_checkpoint(
        &self,
        run_id: String,
        worker_id: String,
        checkpoint_id: String,
        step: u64,
        idempotency_key: String,
    ) -> Result<CoordinatorCheckpointRecord, BeginCheckpointError> {
        if worker_id.trim().is_empty() {
            return Err(BeginCheckpointError::InvalidWorkerId);
        }
        if !self.runs.contains_key(&run_id) {
            return Err(BeginCheckpointError::RunNotFound);
        }
        if !self
            .workers_by_run
            .get(&run_id)
            .is_some_and(|arc| arc.read().unwrap().contains_key(&worker_id))
        {
            return Err(BeginCheckpointError::WorkerNotFound);
        }

        let scoped_key = checkpoint_idempotency_key(&run_id, &idempotency_key);

        // Atomic idempotency check via DashMap entry shard lock.
        // If the key already exists, return the previously-created checkpoint.
        if !scoped_key.is_empty() {
            if let Some(existing_id) = self.checkpoint_idempotency_keys.get(&scoped_key) {
                let existing_id = existing_id.clone();
                let record = self
                    .checkpoints_by_run
                    .get(&run_id)
                    .and_then(|arc| arc.read().unwrap().get(&existing_id).cloned())
                    .expect("checkpoint idempotency key must reference an existing checkpoint");
                return Ok(record);
            }
        }

        let checkpoint_id = if checkpoint_id.trim().is_empty() {
            self.generate_checkpoint_id()
        } else {
            checkpoint_id
        };

        // Get or create the per-run checkpoint map.
        let checkpoints_arc = self
            .checkpoints_by_run
            .entry(run_id.clone())
            .or_insert_with(|| Arc::new(RwLock::new(HashMap::new())))
            .value()
            .clone();
        let mut checkpoints = checkpoints_arc.write().unwrap();

        if let Some(existing) = checkpoints.get(&checkpoint_id) {
            // Any worker may join an in-progress checkpoint for the same step.
            if existing.checkpoint.step == step {
                return Ok(existing.clone());
            }
            return Err(BeginCheckpointError::CheckpointConflict);
        }

        let now_millis = now_millis();
        let expected_worker_ids = self
            .workers_by_run
            .get(&run_id)
            .map(|arc| {
                arc.read()
                    .unwrap()
                    .values()
                    .filter(|w| w.state == WorkerState::Active as i32)
                    .map(|w| w.worker_id.clone())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        let record = CoordinatorCheckpointRecord {
            checkpoint: CheckpointRecord::new(
                DomainRunId(run_id.clone()),
                DomainCheckpointId(checkpoint_id.clone()),
                step,
                now_millis,
            ),
            initiated_by_worker_id: worker_id,
            committed_at_millis: None,
            expected_artifact_count: expected_worker_ids.len() as u32,
            expected_worker_ids,
        };
        checkpoints.insert(checkpoint_id.clone(), record.clone());
        drop(checkpoints);

        if !idempotency_key.is_empty() {
            self.persist_idempotency_key("checkpoint", &scoped_key, &checkpoint_id);
            self.checkpoint_idempotency_keys
                .insert(scoped_key, checkpoint_id);
        }
        Ok(record)
    }

    #[cfg(test)]
    pub fn get_checkpoint(
        &self,
        run_id: &RunId,
        checkpoint_id: &str,
    ) -> Option<CoordinatorCheckpointRecord> {
        self.checkpoints_by_run
            .get(&run_id.value)
            .and_then(|arc| arc.read().unwrap().get(checkpoint_id).cloned())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn report_checkpoint_artifact(
        &self,
        run_id: String,
        worker_id: String,
        checkpoint_id: String,
        artifact_name: String,
        artifact_uri: String,
        size_bytes: u64,
        checksum: String,
        checksum_algorithm: String,
        idempotency_key: String,
    ) -> Result<CoordinatorCheckpointRecord, ReportCheckpointArtifactError> {
        if artifact_name.trim().is_empty() {
            return Err(ReportCheckpointArtifactError::InvalidInput(
                "artifact_name must be non-empty",
            ));
        }
        if artifact_uri.trim().is_empty() {
            return Err(ReportCheckpointArtifactError::InvalidInput(
                "artifact_uri must be non-empty",
            ));
        }
        if !self.runs.contains_key(&run_id) {
            return Err(ReportCheckpointArtifactError::RunNotFound);
        }
        if !self
            .workers_by_run
            .get(&run_id)
            .is_some_and(|arc| arc.read().unwrap().contains_key(&worker_id))
        {
            return Err(ReportCheckpointArtifactError::WorkerNotFound);
        }

        let scoped_idempotency_key =
            checkpoint_artifact_idempotency_key(&run_id, &checkpoint_id, &idempotency_key);
        if !idempotency_key.is_empty()
            && self
                .checkpoint_artifact_idempotency_keys
                .contains_key(&scoped_idempotency_key)
        {
            return self
                .checkpoints_by_run
                .get(&run_id)
                .and_then(|arc| arc.read().unwrap().get(&checkpoint_id).cloned())
                .ok_or(ReportCheckpointArtifactError::CheckpointNotFound);
        }

        let checkpoints_arc = self
            .checkpoints_by_run
            .get(&run_id)
            .ok_or(ReportCheckpointArtifactError::CheckpointNotFound)?
            .value()
            .clone();
        let mut checkpoints = checkpoints_arc.write().unwrap();

        let record = checkpoints
            .get_mut(&checkpoint_id)
            .ok_or(ReportCheckpointArtifactError::CheckpointNotFound)?;

        let now_millis = now_millis();
        if record.checkpoint.state == DomainCheckpointState::Initiated {
            record
                .checkpoint
                .transition_to(DomainCheckpointState::UploadInProgress, now_millis)
                .map_err(map_checkpoint_error)?;
        }
        let domain_worker_id = DomainWorkerId(worker_id.clone());
        record
            .checkpoint
            .add_artifact(
                ArtifactRecord {
                    worker_id: domain_worker_id,
                    artifact_name,
                    storage_key: artifact_uri,
                    size_bytes,
                    checksum,
                    checksum_algorithm,
                },
                now_millis,
            )
            .map_err(map_checkpoint_error)?;
        // Dedupe per (worker, artifact_name): a worker may legitimately report
        // several distinct artifacts (distributed checkpoint formats write
        // multiple files per rank), but re-reporting the *same* artifact name
        // must carry identical metadata (idempotent retry) — anything else is
        // a conflict.
        let latest = record
            .checkpoint
            .artifacts
            .last()
            .expect("artifact list must contain at least one item")
            .clone();
        let last_index = record.checkpoint.artifacts.len() - 1;
        if let Some(first) = record.checkpoint.artifacts[..last_index]
            .iter()
            .find(|a| a.worker_id.0 == worker_id && a.artifact_name == latest.artifact_name)
        {
            if first != &latest {
                return Err(ReportCheckpointArtifactError::InvalidInput(
                    "worker already reported conflicting artifact metadata",
                ));
            }
            record.checkpoint.artifacts.pop();
        }
        let response = record.clone();
        drop(checkpoints);

        if let Some(last) = response.checkpoint.artifacts.last() {
            self.persist_artifact(&run_id, &checkpoint_id, last);
        }
        self.persist_checkpoint(&run_id, "", &response);
        if !idempotency_key.is_empty() {
            self.persist_idempotency_key(
                "checkpoint_artifact",
                &scoped_idempotency_key,
                &checkpoint_id,
            );
            self.checkpoint_artifact_idempotency_keys
                .insert(scoped_idempotency_key, checkpoint_id);
        }
        Ok(response)
    }

    pub fn commit_checkpoint(
        &self,
        run_id: String,
        checkpoint_id: String,
        worker_id: String, // per-worker 2PC commit vote
        _idempotency_key: String,
    ) -> Result<CommitCheckpointResult, CommitCheckpointError> {
        if run_id.trim().is_empty() {
            return Err(CommitCheckpointError::InvalidInput(
                "run_id must be non-empty",
            ));
        }
        if checkpoint_id.trim().is_empty() {
            return Err(CommitCheckpointError::InvalidInput(
                "checkpoint_id must be non-empty",
            ));
        }
        if !self.runs.contains_key(&run_id) {
            return Err(CommitCheckpointError::RunNotFound);
        }

        let checkpoints_arc = self
            .checkpoints_by_run
            .get(&run_id)
            .ok_or(CommitCheckpointError::CheckpointNotFound)?
            .value()
            .clone();

        // Fast path: already globally committed (idempotent re-commit).
        {
            let checkpoints = checkpoints_arc.read().unwrap();
            if let Some(record) = checkpoints.get(&checkpoint_id) {
                if record.checkpoint.state == DomainCheckpointState::Committed {
                    return Ok(CommitCheckpointResult {
                        record: record.clone(),
                        committed_at_millis: record
                            .committed_at_millis
                            .unwrap_or(record.checkpoint.updated_at_millis),
                        globally_committed: true,
                    });
                }
            }
        }

        // record this worker's commit vote (idempotent via HashSet insert).
        let all_voted = if !worker_id.is_empty() {
            let commit_key = (run_id.clone(), checkpoint_id.clone());
            let set_arc = self
                .checkpoint_commits
                .entry(commit_key)
                .or_insert_with(|| Arc::new(Mutex::new(HashSet::new())))
                .clone();
            let mut set = set_arc.lock().unwrap();
            // Persist the vote *before* recording it in memory: if the DB
            // write fails we return Unavailable and the worker retries, at
            // which point persistence is re-attempted.  Inserting into the
            // in-memory set first would make the retry a no-op and the vote
            // would never become durable.
            if !set.contains(&worker_id) {
                if let Some(store) = &self.metadata_store {
                    store
                        .record_checkpoint_commit(&run_id, &checkpoint_id, &worker_id, now_millis())
                        .map_err(|e| {
                            tracing::error!(
                                error = %e,
                                run_id = %run_id,
                                checkpoint_id = %checkpoint_id,
                                worker_id = %worker_id,
                                "failed to persist checkpoint commit vote"
                            );
                            CommitCheckpointError::PersistenceUnavailable
                        })?;
                }
                set.insert(worker_id.clone());
            }
            let commit_count = set.len();
            drop(set);

            // Check whether every worker registered at begin_checkpoint has
            // voted.
            let checkpoints = checkpoints_arc.read().unwrap();
            let record = checkpoints
                .get(&checkpoint_id)
                .ok_or(CommitCheckpointError::CheckpointNotFound)?;
            commit_count >= record.expected_worker_ids.len()
        } else {
            // Legacy path: no worker_id supplied → skip vote-count gate and
            // rely solely on artifact completeness (backward compatible).
            true
        };

        if !all_voted {
            // Some workers haven't committed yet — return a pending result.
            let checkpoints = checkpoints_arc.read().unwrap();
            let record = checkpoints
                .get(&checkpoint_id)
                .ok_or(CommitCheckpointError::CheckpointNotFound)?;
            return Ok(CommitCheckpointResult {
                record: record.clone(),
                committed_at_millis: record.checkpoint.updated_at_millis,
                globally_committed: false,
            });
        }

        // All votes in — acquire the write lock and perform the global commit.
        let mut checkpoints = checkpoints_arc.write().unwrap();

        let record = checkpoints
            .get_mut(&checkpoint_id)
            .ok_or(CommitCheckpointError::CheckpointNotFound)?;

        // Double-check under write lock (another worker may have beaten us).
        if record.checkpoint.state == DomainCheckpointState::Committed {
            let committed_at = record
                .committed_at_millis
                .unwrap_or(record.checkpoint.updated_at_millis);
            return Ok(CommitCheckpointResult {
                record: record.clone(),
                committed_at_millis: committed_at,
                globally_committed: true,
            });
        }

        if record.checkpoint.artifacts.is_empty() {
            return Err(CommitCheckpointError::InvalidState);
        }
        if record.expected_worker_ids.is_empty() {
            return Err(CommitCheckpointError::InvalidState);
        }
        // Completeness = per-worker coverage, not a fixed artifact count:
        // distributed checkpoint formats (e.g. torch DCP) legitimately write a
        // variable number of files per rank (rank 0 adds metadata files).
        // Every worker registered at begin must have reported at least one
        // artifact, and no artifacts may come from unexpected workers.
        let reported_workers: std::collections::BTreeSet<String> = record
            .checkpoint
            .artifacts
            .iter()
            .map(|a| a.worker_id.0.clone())
            .collect();
        let expected_workers: std::collections::BTreeSet<String> =
            record.expected_worker_ids.iter().cloned().collect();
        if reported_workers != expected_workers {
            return Err(CommitCheckpointError::InvalidState);
        }

        if matches!(
            record.checkpoint.state,
            DomainCheckpointState::Failed | DomainCheckpointState::Aborted
        ) {
            return Err(CommitCheckpointError::InvalidState);
        }

        let now_millis_val = now_millis();
        if record.checkpoint.state == DomainCheckpointState::UploadInProgress {
            record
                .checkpoint
                .transition_to(DomainCheckpointState::Validating, now_millis_val)
                .map_err(map_commit_checkpoint_error)?;
        }
        if record.checkpoint.state == DomainCheckpointState::Validating {
            record
                .checkpoint
                .transition_to(DomainCheckpointState::Committed, now_millis_val)
                .map_err(map_commit_checkpoint_error)?;
        }
        if record.checkpoint.state != DomainCheckpointState::Committed {
            return Err(CommitCheckpointError::InvalidState);
        }
        record.committed_at_millis = Some(now_millis_val);
        let committed_at_millis = now_millis_val;

        let response = record.clone();
        drop(checkpoints);

        // Update the run's latest checkpoint pointer.
        if let Some(mut run) = self.runs.get_mut(&run_id) {
            run.latest_checkpoint_id = Some(CheckpointId {
                value: checkpoint_id.clone(),
            });
            run.latest_checkpoint_state = CheckpointState::Committed as i32;
        }

        if let Some(store) = &self.metadata_store {
            store
                .mark_checkpoint_committed(
                    &run_id,
                    &response.checkpoint.checkpoint_id.0,
                    committed_at_millis,
                )
                .expect("mark committed");
        }
        Ok(CommitCheckpointResult {
            record: response,
            committed_at_millis,
            globally_committed: true,
        })
    }

    pub fn get_latest_checkpoint(
        &self,
        run_id: String,
    ) -> Result<CoordinatorCheckpointRecord, GetLatestCheckpointError> {
        if run_id.trim().is_empty() {
            return Err(GetLatestCheckpointError::InvalidInput(
                "run_id must be non-empty",
            ));
        }

        // Try the persistence back-end first (survives coordinator restarts).
        if let Some(store) = &self.metadata_store {
            if let Some(record) = store
                .load_latest_checkpoint(&run_id)
                .expect("load latest checkpoint")
            {
                return Ok(record);
            }
        }

        let run = self
            .runs
            .get(&run_id)
            .ok_or(GetLatestCheckpointError::RunNotFound)?;
        let latest_checkpoint_id = run
            .latest_checkpoint_id
            .as_ref()
            .ok_or(GetLatestCheckpointError::NoCommittedCheckpoint)?
            .value
            .clone();
        drop(run);

        let record = self
            .checkpoints_by_run
            .get(&run_id)
            .and_then(|arc| arc.read().unwrap().get(&latest_checkpoint_id).cloned())
            .ok_or(GetLatestCheckpointError::LatestCheckpointMissing)?;

        if record.checkpoint.state != DomainCheckpointState::Committed {
            return Err(GetLatestCheckpointError::LatestCheckpointNotCommitted);
        }

        Ok(record)
    }

    /// All checkpoints for a run, newest step first.  Uses the durable store
    /// when configured (so pre-restart checkpoints are included), otherwise
    /// the in-memory records.
    pub fn list_checkpoints(
        &self,
        run_id: &str,
        committed_only: bool,
        limit: usize,
    ) -> Result<Vec<CoordinatorCheckpointRecord>, ListCheckpointsError> {
        if !self.runs.contains_key(run_id) {
            return Err(ListCheckpointsError::RunNotFound);
        }
        let mut records = if let Some(store) = &self.metadata_store {
            store
                .load_checkpoints_for_run(run_id)
                .map_err(|_| ListCheckpointsError::PersistenceUnavailable)?
        } else {
            let mut records: Vec<CoordinatorCheckpointRecord> = self
                .checkpoints_by_run
                .get(run_id)
                .map(|arc| arc.read().unwrap().values().cloned().collect())
                .unwrap_or_default();
            records.sort_by(|a, b| {
                b.checkpoint.step.cmp(&a.checkpoint.step).then_with(|| {
                    b.checkpoint
                        .checkpoint_id
                        .0
                        .cmp(&a.checkpoint.checkpoint_id.0)
                })
            });
            records
        };
        if committed_only {
            records.retain(|r| r.checkpoint.state == DomainCheckpointState::Committed);
        }
        if limit > 0 {
            records.truncate(limit);
        }
        Ok(records)
    }

    /// Mark a checkpoint corrupt: the record becomes FAILED and the run's
    /// latest-checkpoint pointer falls back to the newest remaining committed
    /// checkpoint.  Returns the fallback checkpoint id, if any remains.
    pub fn report_checkpoint_corrupt(
        &self,
        run_id: &str,
        checkpoint_id: &str,
        reason: &str,
    ) -> Result<Option<String>, ReportCorruptError> {
        if !self.runs.contains_key(run_id) {
            return Err(ReportCorruptError::RunNotFound);
        }
        let known_in_memory = self
            .checkpoints_by_run
            .get(run_id)
            .map(|arc| arc.read().unwrap().contains_key(checkpoint_id))
            .unwrap_or(false);
        let known_in_store = if let Some(store) = &self.metadata_store {
            store
                .load_checkpoints_for_run(run_id)
                .map_err(|_| ReportCorruptError::PersistenceUnavailable)?
                .iter()
                .any(|r| r.checkpoint.checkpoint_id.0 == checkpoint_id)
        } else {
            false
        };
        if !known_in_memory && !known_in_store {
            return Err(ReportCorruptError::CheckpointNotFound);
        }

        tracing::warn!(
            run_id = %run_id,
            checkpoint_id = %checkpoint_id,
            reason = %reason,
            "checkpoint reported corrupt; marking failed and recomputing latest"
        );

        // Fail the record in memory and in the store.
        if let Some(arc) = self.checkpoints_by_run.get(run_id) {
            if let Some(record) = arc.write().unwrap().get_mut(checkpoint_id) {
                record.checkpoint.state = DomainCheckpointState::Failed;
            }
        }
        if let Some(store) = &self.metadata_store {
            store
                .mark_checkpoint_failed(run_id, checkpoint_id)
                .map_err(|_| ReportCorruptError::PersistenceUnavailable)?;
        }

        // Recompute the latest committed checkpoint, excluding the failed one.
        let fallback = self
            .list_checkpoints(run_id, true, 0)
            .map_err(|_| ReportCorruptError::PersistenceUnavailable)?
            .into_iter()
            .find(|r| r.checkpoint.checkpoint_id.0 != checkpoint_id);

        if let Some(mut run) = self.runs.get_mut(run_id) {
            match &fallback {
                Some(record) => {
                    run.latest_checkpoint_id = Some(CheckpointId {
                        value: record.checkpoint.checkpoint_id.0.clone(),
                    });
                    run.latest_checkpoint_state = CheckpointState::Committed as i32;
                }
                None => {
                    run.latest_checkpoint_id = None;
                    run.latest_checkpoint_state = CheckpointState::Unspecified as i32;
                }
            }
        }
        if let Some(store) = &self.metadata_store {
            let latest = fallback.as_ref().map(|record| {
                (
                    record.checkpoint.checkpoint_id.0.as_str(),
                    record.committed_at_millis.unwrap_or_default(),
                )
            });
            store
                .set_latest_checkpoint(run_id, latest)
                .map_err(|_| ReportCorruptError::PersistenceUnavailable)?;
        }

        Ok(fallback.map(|r| r.checkpoint.checkpoint_id.0))
    }

    /// Retention/GC policy: keep the newest `keep_last` committed checkpoints
    /// per run and drop the rest (records in memory + store).  Non-committed
    /// (failed/aborted) checkpoints older than the newest committed one are
    /// also collected.
    ///
    /// Returns a [`ReclaimedCheckpoint`] per removed checkpoint, **including
    /// each artifact's URI**.  The caller must delete those artifacts: once the
    /// records are gone nothing else knows the objects exist, so dropping this
    /// return value leaks storage permanently.
    pub fn enforce_checkpoint_retention(&self, keep_last: usize) -> Vec<ReclaimedCheckpoint> {
        if keep_last == 0 {
            return Vec::new();
        }
        let run_ids: Vec<String> = self.runs.iter().map(|r| r.key().clone()).collect();
        let mut removed = Vec::new();
        for run_id in run_ids {
            let Ok(records) = self.list_checkpoints(&run_id, false, 0) else {
                continue;
            };
            let mut committed_seen = 0usize;
            // Artifact URIs are captured here, while the records still exist —
            // after the delete below there is no way to recover them.
            let mut doomed: Vec<(String, Vec<String>)> = Vec::new();
            let doom = |record: &CoordinatorCheckpointRecord| {
                (
                    record.checkpoint.checkpoint_id.0.clone(),
                    record
                        .checkpoint
                        .artifacts
                        .iter()
                        .map(|a| a.storage_key.clone())
                        .collect(),
                )
            };
            for record in &records {
                if record.checkpoint.state == DomainCheckpointState::Committed {
                    committed_seen += 1;
                    if committed_seen > keep_last {
                        doomed.push(doom(record));
                    }
                } else if record.checkpoint.state == DomainCheckpointState::Failed
                    || record.checkpoint.state == DomainCheckpointState::Aborted
                {
                    // Failed/aborted checkpoints are only GC'd once at least
                    // one newer committed checkpoint exists, so an operator
                    // can still inspect a fresh failure.
                    if committed_seen >= 1 {
                        doomed.push(doom(record));
                    }
                }
            }
            for (checkpoint_id, artifact_uris) in doomed {
                if let Some(arc) = self.checkpoints_by_run.get(&run_id) {
                    arc.write().unwrap().remove(&checkpoint_id);
                }
                if let Some(store) = &self.metadata_store {
                    if let Err(e) = store.delete_checkpoint(&run_id, &checkpoint_id) {
                        tracing::warn!(
                            run_id = %run_id,
                            checkpoint_id = %checkpoint_id,
                            error = %e,
                            "checkpoint retention: failed to delete rows (will retry next sweep)"
                        );
                        continue;
                    }
                }
                self.checkpoint_commits
                    .remove(&(run_id.clone(), checkpoint_id.clone()));
                tracing::info!(
                    run_id = %run_id,
                    checkpoint_id = %checkpoint_id,
                    artifacts = artifact_uris.len(),
                    "checkpoint retention: removed checkpoint records"
                );
                removed.push(ReclaimedCheckpoint {
                    run_id: run_id.clone(),
                    checkpoint_id,
                    artifact_uris,
                });
            }
        }
        removed
    }

    fn generate_checkpoint_id(&self) -> String {
        let id = self.next_checkpoint_id.fetch_add(1, Ordering::Relaxed) + 1;
        format!("checkpoint-{id}")
    }
}

fn checkpoint_idempotency_key(run_id: &str, idempotency_key: &str) -> String {
    scoped_idempotency_key(run_id, idempotency_key)
}

fn checkpoint_artifact_idempotency_key(
    run_id: &str,
    checkpoint_id: &str,
    idempotency_key: &str,
) -> String {
    if idempotency_key.is_empty() {
        return String::new();
    }
    format!("{run_id}:{checkpoint_id}:{idempotency_key}")
}

fn map_checkpoint_error(error: CheckpointError) -> ReportCheckpointArtifactError {
    match error {
        CheckpointError::InvalidArtifact(_) => ReportCheckpointArtifactError::InvalidInput(
            "artifact_name and artifact_uri must be non-empty",
        ),
        CheckpointError::TerminalState { .. } | CheckpointError::InvalidTransition { .. } => {
            ReportCheckpointArtifactError::InvalidState
        }
        _ => ReportCheckpointArtifactError::InvalidState,
    }
}

fn map_commit_checkpoint_error(error: CheckpointError) -> CommitCheckpointError {
    match error {
        CheckpointError::InvalidTransition { .. } | CheckpointError::TerminalState { .. } => {
            CommitCheckpointError::InvalidState
        }
        _ => CommitCheckpointError::InvalidState,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::test_support::*;

    use tempfile::tempdir;

    #[test]
    fn begin_checkpoint_requires_existing_run_and_worker() {
        let state = CoordinatorState::default();
        let err = state
            .begin_checkpoint(
                "missing".to_string(),
                "worker-1".to_string(),
                "c1".to_string(),
                10,
                String::new(),
            )
            .expect_err("missing run should fail");
        assert_eq!(err, BeginCheckpointError::RunNotFound);

        let run_id = state.register_run(register_request("run-key")).run_id;
        let err = state
            .begin_checkpoint(
                run_id.value.clone(),
                "missing-worker".to_string(),
                "c1".to_string(),
                10,
                String::new(),
            )
            .expect_err("missing worker should fail");
        assert_eq!(err, BeginCheckpointError::WorkerNotFound);
    }

    #[test]
    fn begin_checkpoint_generates_id_and_supports_idempotency() {
        let state = CoordinatorState::default();
        let run_id = state.register_run(register_request("run-key")).run_id;
        let worker_id = state
            .register_worker(worker_request(run_id.clone(), "worker-key"))
            .expect("worker registration should succeed")
            .worker_id;

        let first = state
            .begin_checkpoint(
                run_id.value.clone(),
                worker_id.value.clone(),
                String::new(),
                55,
                "idem-1".to_string(),
            )
            .expect("begin checkpoint should succeed");
        assert_eq!(first.checkpoint.checkpoint_id.0, "checkpoint-1");
        assert_eq!(
            first.checkpoint.state,
            runtime_checkpoint::CheckpointState::Initiated
        );

        let second = state
            .begin_checkpoint(
                run_id.value.clone(),
                worker_id.value,
                "different-checkpoint".to_string(),
                77,
                "idem-1".to_string(),
            )
            .expect("idempotent begin should return first");
        assert_eq!(
            second.checkpoint.checkpoint_id,
            first.checkpoint.checkpoint_id
        );
        assert_eq!(second.checkpoint.step, first.checkpoint.step);
    }

    #[test]
    fn committed_checkpoint_survives_state_recreation() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("coordinator.db");
        let store = SqliteMetadataStore::open(&path).unwrap();
        store.init_schema().unwrap();
        let state = CoordinatorState::with_metadata_store(Arc::new(store));

        let run_id = state.register_run(register_request("run-key")).run_id;
        let worker_0 = state
            .register_worker(worker_request(run_id.clone(), "worker-0"))
            .unwrap()
            .worker_id;
        let worker_1 = state
            .register_worker(RegisterWorkerRequest {
                worker_id: Some(WorkerId {
                    value: "worker-1".to_string(),
                }),
                rank: 1,
                ..worker_request(run_id.clone(), "worker-1")
            })
            .unwrap()
            .worker_id;

        state
            .register_dataset(RegisterDatasetRequest {
                idempotency_key: "dataset-key".to_string(),
                run_id: Some(run_id.clone()),
                dataset: Some(DatasetMetadata {
                    dataset_id: Some(DatasetId {
                        value: "dataset-1".to_string(),
                    }),
                    name: "training".to_string(),
                    shard_count: 2,
                    ..DatasetMetadata::default()
                }),
                shards: vec![
                    ShardMetadata {
                        shard_id: Some(ShardId {
                            value: "shard-0".to_string(),
                        }),
                        shard_uri: "file:///dataset/0".to_string(),
                        size_bytes: 100,
                        ..ShardMetadata::default()
                    },
                    ShardMetadata {
                        shard_id: Some(ShardId {
                            value: "shard-1".to_string(),
                        }),
                        shard_uri: "file:///dataset/1".to_string(),
                        size_bytes: 200,
                        ..ShardMetadata::default()
                    },
                ],
            })
            .expect("dataset should register");

        state
            .begin_checkpoint(
                run_id.value.clone(),
                worker_0.value.clone(),
                "checkpoint-1".to_string(),
                10,
                "begin".to_string(),
            )
            .unwrap();
        state
            .report_checkpoint_artifact(
                run_id.value.clone(),
                worker_0.value.clone(),
                "checkpoint-1".to_string(),
                "model.bin".to_string(),
                "file:///ckpt/worker-0-model.bin".to_string(),
                111,
                "sum0".to_string(),
                "sha256".to_string(),
                "art-0".to_string(),
            )
            .unwrap();
        state
            .report_checkpoint_artifact(
                run_id.value.clone(),
                worker_1.value.clone(),
                "checkpoint-1".to_string(),
                "model.bin".to_string(),
                "file:///ckpt/worker-1-model.bin".to_string(),
                222,
                "sum1".to_string(),
                "sha256".to_string(),
                "art-1".to_string(),
            )
            .unwrap();
        state
            .commit_checkpoint(
                run_id.value.clone(),
                "checkpoint-1".to_string(),
                String::new(), // legacy: no worker_id → skip vote gate
                "commit".to_string(),
            )
            .unwrap();

        drop(state);

        let reopened_store = SqliteMetadataStore::open(&path).unwrap();
        reopened_store.init_schema().unwrap();
        let recreated = CoordinatorState::with_metadata_store(Arc::new(reopened_store));

        let latest = recreated
            .get_latest_checkpoint(run_id.value.clone())
            .unwrap();
        assert_eq!(latest.checkpoint.checkpoint_id.0, "checkpoint-1");
        assert_eq!(latest.checkpoint.state, DomainCheckpointState::Committed);
        assert_eq!(latest.checkpoint.artifacts.len(), 2);
    }

    #[test]
    fn get_latest_checkpoint_requires_existing_run_and_commit() {
        let state = CoordinatorState::default();
        let err = state
            .get_latest_checkpoint("missing".to_string())
            .expect_err("missing run should fail");
        assert_eq!(err, GetLatestCheckpointError::RunNotFound);

        let run_id = state.register_run(register_request("run-key")).run_id;
        let err = state
            .get_latest_checkpoint(run_id.value.clone())
            .expect_err("run without committed checkpoint should fail");
        assert_eq!(err, GetLatestCheckpointError::NoCommittedCheckpoint);
    }

    #[test]
    fn get_latest_checkpoint_returns_only_committed_record() {
        let state = CoordinatorState::default();
        let run_id = state.register_run(register_request("run-key")).run_id;
        let worker_id = state
            .register_worker(worker_request(run_id.clone(), "worker-key"))
            .unwrap()
            .worker_id;

        state
            .begin_checkpoint(
                run_id.value.clone(),
                worker_id.value.clone(),
                "ckpt-pending".to_string(),
                1,
                "begin-1".to_string(),
            )
            .unwrap();
        let err = state
            .get_latest_checkpoint(run_id.value.clone())
            .expect_err("uncommitted checkpoint should not be latest");
        assert_eq!(err, GetLatestCheckpointError::NoCommittedCheckpoint);

        state
            .begin_checkpoint(
                run_id.value.clone(),
                worker_id.value.clone(),
                "ckpt-committed".to_string(),
                2,
                "begin-2".to_string(),
            )
            .unwrap();
        state
            .report_checkpoint_artifact(
                run_id.value.clone(),
                worker_id.value.clone(),
                "ckpt-committed".to_string(),
                "model.bin".to_string(),
                "s3://bucket/model.bin".to_string(),
                10,
                "sum".to_string(),
                "sha256".to_string(),
                "artifact-1".to_string(),
            )
            .unwrap();
        state
            .commit_checkpoint(
                run_id.value.clone(),
                "ckpt-committed".to_string(),
                String::new(),
                "commit-1".to_string(),
            )
            .unwrap();

        let latest = state
            .get_latest_checkpoint(run_id.value.clone())
            .expect("latest committed checkpoint should be returned");
        assert_eq!(latest.checkpoint.checkpoint_id.0, "ckpt-committed");
        assert_eq!(latest.checkpoint.state, DomainCheckpointState::Committed);
        assert_eq!(latest.checkpoint.artifacts.len(), 1);

        state
            .begin_checkpoint(
                run_id.value.clone(),
                worker_id.value.clone(),
                "ckpt-committed-2".to_string(),
                3,
                "begin-3".to_string(),
            )
            .unwrap();
        state
            .report_checkpoint_artifact(
                run_id.value.clone(),
                worker_id.value,
                "ckpt-committed-2".to_string(),
                "model-2.bin".to_string(),
                "s3://bucket/model-2.bin".to_string(),
                11,
                "sum2".to_string(),
                "sha256".to_string(),
                "artifact-2".to_string(),
            )
            .unwrap();
        state
            .commit_checkpoint(
                run_id.value.clone(),
                "ckpt-committed-2".to_string(),
                String::new(),
                "commit-2".to_string(),
            )
            .unwrap();

        let latest = state
            .get_latest_checkpoint(run_id.value)
            .expect("newest committed checkpoint should be latest");
        assert_eq!(latest.checkpoint.checkpoint_id.0, "ckpt-committed-2");
    }

    #[test]
    fn report_checkpoint_artifact_updates_state_and_is_idempotent() {
        let state = CoordinatorState::default();
        let run_id = state.register_run(register_request("run-key")).run_id;
        let worker_id = state
            .register_worker(worker_request(run_id.clone(), "worker-key"))
            .unwrap()
            .worker_id;
        state
            .begin_checkpoint(
                run_id.value.clone(),
                worker_id.value.clone(),
                "ckpt-1".to_string(),
                1,
                "checkpoint-key".to_string(),
            )
            .unwrap();

        let reported = state
            .report_checkpoint_artifact(
                run_id.value.clone(),
                worker_id.value.clone(),
                "ckpt-1".to_string(),
                "model.bin".to_string(),
                "s3://bucket/model.bin".to_string(),
                10,
                "sum".to_string(),
                "sha256".to_string(),
                "artifact-key".to_string(),
            )
            .unwrap();
        assert_eq!(
            reported.checkpoint.state,
            DomainCheckpointState::UploadInProgress
        );
        assert_eq!(reported.checkpoint.artifacts.len(), 1);

        let repeated = state
            .report_checkpoint_artifact(
                run_id.value.clone(),
                worker_id.value.clone(),
                "ckpt-1".to_string(),
                "model.bin".to_string(),
                "s3://bucket/model.bin".to_string(),
                10,
                "sum".to_string(),
                "sha256".to_string(),
                "artifact-key".to_string(),
            )
            .unwrap();
        assert_eq!(repeated.checkpoint.artifacts.len(), 1);
    }

    #[test]
    fn commit_checkpoint_requires_existing_run_and_checkpoint() {
        let state = CoordinatorState::default();
        let err = state
            .commit_checkpoint(
                "missing".to_string(),
                "ckpt-1".to_string(),
                String::new(),
                String::new(),
            )
            .expect_err("missing run should fail");
        assert_eq!(err, CommitCheckpointError::RunNotFound);

        let run_id = state.register_run(register_request("run-key")).run_id;
        let err = state
            .commit_checkpoint(
                run_id.value,
                "missing-ckpt".to_string(),
                String::new(),
                String::new(),
            )
            .expect_err("missing checkpoint should fail");
        assert_eq!(err, CommitCheckpointError::CheckpointNotFound);
    }

    #[test]
    fn commit_checkpoint_requires_artifacts() {
        let state = CoordinatorState::default();
        let run_id = state.register_run(register_request("run-key")).run_id;
        let worker_id = state
            .register_worker(worker_request(run_id.clone(), "worker-key"))
            .unwrap()
            .worker_id;
        state
            .begin_checkpoint(
                run_id.value.clone(),
                worker_id.value,
                "ckpt-1".to_string(),
                1,
                "checkpoint-key".to_string(),
            )
            .unwrap();

        let err = state
            .commit_checkpoint(
                run_id.value,
                "ckpt-1".to_string(),
                String::new(),
                String::new(),
            )
            .expect_err("checkpoint without artifacts should fail");
        assert_eq!(err, CommitCheckpointError::InvalidState);
    }

    #[test]
    fn commit_checkpoint_transitions_and_updates_latest() {
        let state = CoordinatorState::default();
        let run_id = state.register_run(register_request("run-key")).run_id;
        let worker_id = state
            .register_worker(worker_request(run_id.clone(), "worker-key"))
            .unwrap()
            .worker_id;
        state
            .begin_checkpoint(
                run_id.value.clone(),
                worker_id.value.clone(),
                "ckpt-1".to_string(),
                1,
                "checkpoint-key".to_string(),
            )
            .unwrap();
        state
            .report_checkpoint_artifact(
                run_id.value.clone(),
                worker_id.value,
                "ckpt-1".to_string(),
                "model.bin".to_string(),
                "s3://bucket/model.bin".to_string(),
                10,
                "sum".to_string(),
                "sha256".to_string(),
                "artifact-key".to_string(),
            )
            .unwrap();

        let committed = state
            .commit_checkpoint(
                run_id.value.clone(),
                "ckpt-1".to_string(),
                String::new(),
                "commit-1".to_string(),
            )
            .expect("commit should succeed");
        assert_eq!(
            committed.record.checkpoint.state,
            DomainCheckpointState::Committed
        );
        assert!(committed.record.committed_at_millis.is_some());
        assert_eq!(
            committed.committed_at_millis,
            committed.record.committed_at_millis.unwrap()
        );

        let run = state.get_run(&run_id).unwrap();
        assert_eq!(
            run.latest_checkpoint_id,
            Some(CheckpointId {
                value: "ckpt-1".to_string()
            })
        );
        assert_eq!(
            run.latest_checkpoint_state,
            CheckpointState::Committed as i32
        );

        let repeated = state
            .commit_checkpoint(
                run_id.value,
                "ckpt-1".to_string(),
                String::new(),
                "commit-2".to_string(),
            )
            .expect("repeated commit should be harmless");
        assert_eq!(
            repeated.record.checkpoint.state,
            DomainCheckpointState::Committed
        );
    }

    #[test]
    fn commit_checkpoint_with_worker_votes_waits_for_all_workers() {
        let state = CoordinatorState::default();
        let (run_id, worker_a, worker_b) = setup_two_worker_checkpoint(&state, "ckpt-2pc");

        // First vote: recorded but the global commit must wait.
        let first = state
            .commit_checkpoint(
                run_id.value.clone(),
                "ckpt-2pc".to_string(),
                worker_a.value.clone(),
                "c1".to_string(),
            )
            .expect("first vote should be accepted");
        assert!(!first.globally_committed);
        assert_ne!(
            first.record.checkpoint.state,
            DomainCheckpointState::Committed
        );

        // Duplicate vote from the same worker is idempotent — still pending.
        let duplicate = state
            .commit_checkpoint(
                run_id.value.clone(),
                "ckpt-2pc".to_string(),
                worker_a.value.clone(),
                "c1-dup".to_string(),
            )
            .expect("duplicate vote should be accepted");
        assert!(!duplicate.globally_committed);

        // Second worker's vote completes the global commit.
        let second = state
            .commit_checkpoint(
                run_id.value.clone(),
                "ckpt-2pc".to_string(),
                worker_b.value.clone(),
                "c2".to_string(),
            )
            .expect("final vote should commit");
        assert!(second.globally_committed);
        assert_eq!(
            second.record.checkpoint.state,
            DomainCheckpointState::Committed
        );

        // Any further commit is an idempotent no-op reporting global commit.
        let repeat = state
            .commit_checkpoint(
                run_id.value,
                "ckpt-2pc".to_string(),
                worker_a.value,
                "c3".to_string(),
            )
            .expect("post-commit vote should be harmless");
        assert!(repeat.globally_committed);
    }

    #[test]
    fn checkpoint_commit_votes_survive_coordinator_restart() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("coordinator.db");
        let store = SqliteMetadataStore::open(&path).unwrap();
        store.init_schema().unwrap();
        let state = CoordinatorState::with_metadata_store(Arc::new(store));

        let (run_id, worker_a, worker_b) = setup_two_worker_checkpoint(&state, "ckpt-2pc");
        let first = state
            .commit_checkpoint(
                run_id.value.clone(),
                "ckpt-2pc".to_string(),
                worker_a.value.clone(),
                "c1".to_string(),
            )
            .expect("first vote should be accepted");
        assert!(!first.globally_committed);
        drop(state);

        // Simulate a coordinator restart: reopen the store and rebuild state.
        let reopened = SqliteMetadataStore::open(&path).unwrap();
        reopened.init_schema().unwrap();
        let recreated = CoordinatorState::with_metadata_store(Arc::new(reopened));

        // Clients rebuild run/checkpoint state via idempotent re-registration.
        let (run_id, _, worker_b_again) = setup_two_worker_checkpoint(&recreated, "ckpt-2pc");
        assert_eq!(worker_b_again.value, worker_b.value);

        // Worker A's pre-restart vote was hydrated from the store, so
        // worker B's single vote must complete the global commit.
        let second = recreated
            .commit_checkpoint(
                run_id.value,
                "ckpt-2pc".to_string(),
                worker_b.value,
                "c2".to_string(),
            )
            .expect("post-restart vote should commit");
        assert!(second.globally_committed);
        assert_eq!(
            second.record.checkpoint.state,
            DomainCheckpointState::Committed
        );
    }

    #[test]
    fn list_checkpoints_orders_newest_first_and_respects_filters() {
        let state = CoordinatorState::default();
        let (run_id, worker) = state_with_committed_checkpoints(&state, 3);
        // One extra non-committed checkpoint at a higher step.
        state
            .begin_checkpoint(
                run_id.value.clone(),
                worker.value.clone(),
                "ckpt-open".to_string(),
                9,
                "begin-open".to_string(),
            )
            .expect("begin");

        let all = state.list_checkpoints(&run_id.value, false, 0).unwrap();
        assert_eq!(all.len(), 4);
        assert_eq!(all[0].checkpoint.checkpoint_id.0, "ckpt-open");

        let committed = state.list_checkpoints(&run_id.value, true, 0).unwrap();
        assert_eq!(committed.len(), 3);
        assert_eq!(committed[0].checkpoint.checkpoint_id.0, "ckpt-3");

        let limited = state.list_checkpoints(&run_id.value, true, 1).unwrap();
        assert_eq!(limited.len(), 1);

        assert_eq!(
            state.list_checkpoints("nope", false, 0),
            Err(ListCheckpointsError::RunNotFound)
        );
    }

    #[test]
    fn corrupt_report_fails_checkpoint_and_falls_back_to_previous() {
        let dir = tempdir().unwrap();
        let store = SqliteMetadataStore::open(dir.path().join("ck.db")).unwrap();
        store.init_schema().unwrap();
        let state = CoordinatorState::with_metadata_store(Arc::new(store));
        let (run_id, _) = state_with_committed_checkpoints(&state, 2);

        assert_eq!(
            state
                .get_latest_checkpoint(run_id.value.clone())
                .unwrap()
                .checkpoint
                .checkpoint_id
                .0,
            "ckpt-2"
        );

        let fallback = state
            .report_checkpoint_corrupt(&run_id.value, "ckpt-2", "checksum mismatch")
            .expect("report succeeds");
        assert_eq!(fallback.as_deref(), Some("ckpt-1"));
        assert_eq!(
            state
                .get_latest_checkpoint(run_id.value.clone())
                .unwrap()
                .checkpoint
                .checkpoint_id
                .0,
            "ckpt-1"
        );

        // Corrupting the last remaining committed checkpoint leaves none.
        let fallback = state
            .report_checkpoint_corrupt(&run_id.value, "ckpt-1", "unreadable")
            .expect("report succeeds");
        assert_eq!(fallback, None);
        assert!(state.get_latest_checkpoint(run_id.value.clone()).is_err());

        assert_eq!(
            state.report_checkpoint_corrupt(&run_id.value, "nope", ""),
            Err(ReportCorruptError::CheckpointNotFound)
        );
    }

    #[test]
    fn retention_keeps_newest_committed_checkpoints() {
        let dir = tempdir().unwrap();
        let store = SqliteMetadataStore::open(dir.path().join("gc.db")).unwrap();
        store.init_schema().unwrap();
        let state = CoordinatorState::with_metadata_store(Arc::new(store));
        let (run_id, _) = state_with_committed_checkpoints(&state, 3);

        // keep_last == 0 disables GC entirely.
        assert!(state.enforce_checkpoint_retention(0).is_empty());

        let removed = state.enforce_checkpoint_retention(1);
        assert_eq!(
            removed,
            vec![
                ReclaimedCheckpoint {
                    run_id: run_id.value.clone(),
                    checkpoint_id: "ckpt-2".to_string(),
                    artifact_uris: vec!["file:///a/ckpt-2/model.bin".to_string()],
                },
                ReclaimedCheckpoint {
                    run_id: run_id.value.clone(),
                    checkpoint_id: "ckpt-1".to_string(),
                    artifact_uris: vec!["file:///a/ckpt-1/model.bin".to_string()],
                },
            ]
        );
        let remaining = state.list_checkpoints(&run_id.value, false, 0).unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].checkpoint.checkpoint_id.0, "ckpt-3");
        // The latest pointer still resolves.
        assert_eq!(
            state
                .get_latest_checkpoint(run_id.value.clone())
                .unwrap()
                .checkpoint
                .checkpoint_id
                .0,
            "ckpt-3"
        );
        // Idempotent: nothing more to remove.
        assert!(state.enforce_checkpoint_retention(1).is_empty());
    }

    /// Retention deletes metadata only. If the sweep did not hand back every
    /// artifact URI, worker-direct S3 objects would leak forever: once the
    /// records are gone, nothing else knows the objects exist.
    #[test]
    fn retention_reports_every_artifact_uri_including_s3() {
        let state = CoordinatorState::default();
        let run_id = state.register_run(register_request("s3-gc-run")).run_id;
        let worker = state
            .register_worker(worker_request(run_id.clone(), "s3-gc-worker"))
            .expect("worker registers")
            .worker_id;

        // Two checkpoints, each with a multi-file (DCP-style) artifact set
        // written straight to S3 by the worker.
        for step in 1..=2u64 {
            let checkpoint_id = format!("s3-ckpt-{step}");
            state
                .begin_checkpoint(
                    run_id.value.clone(),
                    worker.value.clone(),
                    checkpoint_id.clone(),
                    step,
                    format!("begin-{checkpoint_id}"),
                )
                .expect("begin");
            for shard in 0..2 {
                state
                    .report_checkpoint_artifact(
                        run_id.value.clone(),
                        worker.value.clone(),
                        checkpoint_id.clone(),
                        format!("__{shard}_0.distcp"),
                        format!("s3://ckpt-bucket/runs/r/{checkpoint_id}/__{shard}_0.distcp"),
                        10,
                        "sum".to_string(),
                        "sha256".to_string(),
                        format!("artifact-{checkpoint_id}-{shard}"),
                    )
                    .expect("artifact");
            }
            state
                .commit_checkpoint(
                    run_id.value.clone(),
                    checkpoint_id.clone(),
                    format!("commit-{checkpoint_id}"),
                    String::new(),
                )
                .expect("commit");
        }

        let removed = state.enforce_checkpoint_retention(1);
        assert_eq!(removed.len(), 1, "one checkpoint beyond keep-last");
        assert_eq!(removed[0].checkpoint_id, "s3-ckpt-1");
        assert_eq!(
            removed[0].artifact_uris,
            vec![
                "s3://ckpt-bucket/runs/r/s3-ckpt-1/__0_0.distcp".to_string(),
                "s3://ckpt-bucket/runs/r/s3-ckpt-1/__1_0.distcp".to_string(),
            ],
            "every artifact of the dropped checkpoint must be reported, not just the first"
        );
    }
}
