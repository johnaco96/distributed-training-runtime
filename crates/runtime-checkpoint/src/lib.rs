#![forbid(unsafe_code)]

//! Checkpoint domain model and object-storage manifest machinery.
//!
//! Two distinct surfaces live here, with different consumers:
//!
//! - **Domain types** (`CheckpointRecord`, `CheckpointState`,
//!   `ArtifactRecord`, `CheckpointManifest`, checksum helpers) — the shared
//!   vocabulary of the checkpoint protocol, used by the coordinator's live
//!   2PC commit path (`runtime-coordinator`), which persists its authority
//!   in SQLite/PostgreSQL and builds manifests from these types.
//! - **`CheckpointStore`** — a self-contained implementation of the
//!   manifest/commit protocol *directly over object storage*
//!   (`manifest.pending.json` → artifacts → integrity re-verify →
//!   `manifest.committed.json` + `COMMITTED` marker + `LATEST` pointer,
//!   plus retention cleanup). It is exercised by `runtime-bench`'s
//!   checkpoint benchmarks and validated against real MinIO in
//!   `tests/s3_minio.rs`; it is the coordinator-free storage layout a
//!   future fully worker-direct commit path would use.

use runtime_storage::{
    checkpoint_committed_marker, checkpoint_manifest_committed, checkpoint_manifest_pending,
    checkpoint_worker_artifact, latest_pointer, StorageBackend, StorageError, StorageKey,
    StorageOperationPolicy,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::error::Error;
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct CheckpointId(pub String);
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct RunId(pub String);
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct WorkerId(pub String);

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ChecksumAlgorithm {
    Sha256,
}

impl ChecksumAlgorithm {
    pub fn parse(raw: &str) -> Result<Self, ArtifactValidationError> {
        let normalized = raw.trim().to_ascii_lowercase().replace('-', "");
        match normalized.as_str() {
            "sha256" => Ok(Self::Sha256),
            _ => Err(ArtifactValidationError::UnsupportedChecksumAlgorithm(
                raw.trim().to_string(),
            )),
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Sha256 => "sha256",
        }
    }
}

pub fn compute_checksum(algorithm: ChecksumAlgorithm, bytes: &[u8]) -> String {
    match algorithm {
        ChecksumAlgorithm::Sha256 => {
            let mut hasher = Sha256::new();
            hasher.update(bytes);
            format!("{:x}", hasher.finalize())
        }
    }
}

pub fn verify_checksum(
    algorithm: ChecksumAlgorithm,
    bytes: &[u8],
    expected_digest: &str,
) -> Result<(), ArtifactValidationError> {
    let computed = compute_checksum(algorithm, bytes);
    let expected = expected_digest.trim().to_ascii_lowercase();
    if computed == expected {
        Ok(())
    } else {
        Err(ArtifactValidationError::ChecksumMismatch {
            algorithm: algorithm.as_str().to_string(),
            expected,
            actual: computed,
        })
    }
}

pub fn validate_artifact_integrity(
    storage: &impl StorageBackend,
    key: &StorageKey,
    expected_size_bytes: u64,
    checksum_algorithm: &str,
    expected_checksum: &str,
) -> Result<(), ArtifactValidationError> {
    let bytes = storage.get(key).map_err(|err| match err {
        StorageError::NotFound(_) => ArtifactValidationError::MissingArtifact {
            key: key.as_str().to_string(),
        },
        other => ArtifactValidationError::StorageReadFailure {
            key: key.as_str().to_string(),
            source: other,
        },
    })?;

    let actual_size_bytes = bytes.len() as u64;
    if actual_size_bytes != expected_size_bytes {
        return Err(ArtifactValidationError::SizeMismatch {
            key: key.as_str().to_string(),
            expected_size_bytes,
            actual_size_bytes,
        });
    }

    let algorithm = ChecksumAlgorithm::parse(checksum_algorithm)?;
    verify_checksum(algorithm, &bytes, expected_checksum)
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CheckpointArtifactUpload {
    pub worker_id: WorkerId,
    pub artifact_name: String,
    pub storage_key: StorageKey,
    pub bytes: Vec<u8>,
    pub checksum_algorithm: ChecksumAlgorithm,
}

pub fn upload_checkpoint_artifacts_concurrently(
    storage: &(impl StorageBackend + Sync),
    uploads: Vec<CheckpointArtifactUpload>,
    policy: &StorageOperationPolicy,
) -> Result<Vec<ArtifactRecord>, CheckpointError> {
    policy.validate().map_err(CheckpointError::Storage)?;
    if uploads.is_empty() {
        return Ok(Vec::new());
    }

    let mut out = Vec::with_capacity(uploads.len());
    for chunk in uploads.chunks(policy.max_concurrent_operations) {
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::scope(|scope| {
            for upload in chunk.iter().cloned() {
                let tx = tx.clone();
                scope.spawn(move || {
                    let digest = compute_checksum(upload.checksum_algorithm, &upload.bytes);
                    let size_bytes = upload.bytes.len() as u64;
                    let result = (|| -> Result<ArtifactRecord, CheckpointError> {
                        storage.put(&upload.storage_key, &upload.bytes)?;
                        validate_artifact_integrity(
                            storage,
                            &upload.storage_key,
                            size_bytes,
                            upload.checksum_algorithm.as_str(),
                            &digest,
                        )
                        .map_err(CheckpointError::ArtifactValidation)?;
                        Ok(ArtifactRecord {
                            worker_id: upload.worker_id,
                            artifact_name: upload.artifact_name,
                            storage_key: upload.storage_key.to_string(),
                            size_bytes,
                            checksum: digest,
                            checksum_algorithm: upload.checksum_algorithm.as_str().to_string(),
                        })
                    })();
                    let _ = tx.send(result);
                });
            }
        });
        drop(tx);
        for _ in 0..chunk.len() {
            let item = rx.recv().map_err(|e| {
                CheckpointError::Storage(StorageError::Io(std::io::Error::other(e.to_string())))
            })??;
            out.push(item);
        }
    }
    Ok(out)
}
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckpointState {
    Initiated,
    UploadInProgress,
    Validating,
    Committed,
    Failed,
    Aborted,
}
impl CheckpointState {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Committed | Self::Failed | Self::Aborted)
    }
    fn can_transition_to(self, next: CheckpointState) -> bool {
        matches!(
            (self, next),
            (Self::Initiated, Self::UploadInProgress)
                | (Self::UploadInProgress, Self::Validating)
                | (Self::Validating, Self::Committed)
                | (Self::Initiated, Self::Failed)
                | (Self::UploadInProgress, Self::Failed)
                | (Self::Validating, Self::Failed)
                | (Self::Initiated, Self::Aborted)
                | (Self::UploadInProgress, Self::Aborted)
                | (Self::Validating, Self::Aborted)
        )
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ArtifactRecord {
    pub worker_id: WorkerId,
    pub artifact_name: String,
    pub storage_key: String,
    pub size_bytes: u64,
    pub checksum: String,
    pub checksum_algorithm: String,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CheckpointRecord {
    pub run_id: RunId,
    pub checkpoint_id: CheckpointId,
    pub step: u64,
    pub state: CheckpointState,
    pub artifacts: Vec<ArtifactRecord>,
    pub created_at_millis: u64,
    pub updated_at_millis: u64,
}
impl CheckpointRecord {
    pub fn new(run_id: RunId, checkpoint_id: CheckpointId, step: u64, now_millis: u64) -> Self {
        Self {
            run_id,
            checkpoint_id,
            step,
            state: CheckpointState::Initiated,
            artifacts: Vec::new(),
            created_at_millis: now_millis,
            updated_at_millis: now_millis,
        }
    }
    pub fn transition_to(
        &mut self,
        next: CheckpointState,
        now_millis: u64,
    ) -> Result<(), CheckpointError> {
        if self.state.is_terminal() {
            return Err(CheckpointError::TerminalState {
                current: self.state,
                requested: next,
            });
        }
        if !self.state.can_transition_to(next) {
            return Err(CheckpointError::InvalidTransition {
                from: self.state,
                to: next,
            });
        }
        self.state = next;
        self.updated_at_millis = now_millis;
        Ok(())
    }
    pub fn add_artifact(
        &mut self,
        artifact: ArtifactRecord,
        now_millis: u64,
    ) -> Result<(), CheckpointError> {
        if self.state.is_terminal() {
            return Err(CheckpointError::TerminalState {
                current: self.state,
                requested: self.state,
            });
        }
        if artifact.artifact_name.trim().is_empty() || artifact.storage_key.trim().is_empty() {
            return Err(CheckpointError::InvalidArtifact(
                "artifact_name and storage_key must be non-empty".into(),
            ));
        }
        self.artifacts.push(artifact);
        self.updated_at_millis = now_millis;
        Ok(())
    }
    pub fn is_restorable(&self) -> bool {
        self.state == CheckpointState::Committed
    }
    pub fn is_partial(&self) -> bool {
        matches!(
            self.state,
            CheckpointState::Initiated
                | CheckpointState::UploadInProgress
                | CheckpointState::Validating
        )
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct ManifestArtifact {
    pub worker_id: String,
    pub artifact_name: String,
    pub storage_key: String,
    pub size_bytes: u64,
    pub checksum: String,
    pub checksum_algorithm: String,
}
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct CheckpointManifest {
    pub schema_version: u32,
    pub run_id: String,
    pub checkpoint_id: String,
    pub step: u64,
    pub state: CheckpointState,
    pub created_at_millis: u64,
    pub committed_at_millis: Option<u64>,
    pub artifacts: Vec<ManifestArtifact>,
}
impl CheckpointManifest {
    pub const SCHEMA_VERSION: u32 = 1;
    pub fn from_record(
        record: &CheckpointRecord,
        committed_at_millis: Option<u64>,
    ) -> Result<Self, CheckpointError> {
        let manifest = Self {
            schema_version: Self::SCHEMA_VERSION,
            run_id: record.run_id.0.clone(),
            checkpoint_id: record.checkpoint_id.0.clone(),
            step: record.step,
            state: record.state,
            created_at_millis: record.created_at_millis,
            committed_at_millis,
            artifacts: record
                .artifacts
                .iter()
                .cloned()
                .map(ManifestArtifact::from)
                .collect(),
        };
        manifest.validate_for_commit()?;
        Ok(manifest)
    }
    pub fn to_json_bytes(&self) -> Result<Vec<u8>, CheckpointError> {
        serde_json::to_vec_pretty(self)
            .map_err(|e| CheckpointError::Serialization(format!("serialize manifest: {e}")))
    }
    pub fn from_json_bytes(bytes: &[u8]) -> Result<Self, CheckpointError> {
        serde_json::from_slice(bytes)
            .map_err(|e| CheckpointError::Serialization(format!("deserialize manifest: {e}")))
    }
    pub fn is_committed(&self) -> bool {
        self.state == CheckpointState::Committed
    }
    pub fn is_restorable(&self) -> bool {
        self.is_committed() && self.committed_at_millis.is_some()
    }
    pub fn validate_for_commit(&self) -> Result<(), CheckpointError> {
        if self.is_committed() {
            if self.committed_at_millis.is_none() {
                return Err(CheckpointError::ManifestValidation(
                    "committed manifest must include committed_at_millis".into(),
                ));
            }
            if self.artifacts.is_empty() {
                return Err(CheckpointError::ManifestValidation(
                    "committed manifest must include at least one artifact".into(),
                ));
            }
        }
        Ok(())
    }
}
impl From<ArtifactRecord> for ManifestArtifact {
    fn from(v: ArtifactRecord) -> Self {
        Self {
            worker_id: v.worker_id.0,
            artifact_name: v.artifact_name,
            storage_key: v.storage_key,
            size_bytes: v.size_bytes,
            checksum: v.checksum,
            checksum_algorithm: v.checksum_algorithm,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CheckpointCleanupPolicy {
    pub keep_latest: bool,
    pub keep_last_n_committed: usize,
    pub remove_pending: bool,
    pub remove_orphan_artifacts: bool,
}

impl Default for CheckpointCleanupPolicy {
    fn default() -> Self {
        Self {
            keep_latest: true,
            keep_last_n_committed: 1,
            remove_pending: true,
            remove_orphan_artifacts: true,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Default)]
pub struct CheckpointCleanupReport {
    pub deleted_keys: Vec<StorageKey>,
    pub retained_keys: Vec<StorageKey>,
    pub skipped_keys: Vec<StorageKey>,
    pub errors: Vec<String>,
}

pub struct CheckpointStore<B: StorageBackend> {
    storage: B,
}
impl<B: StorageBackend> CheckpointStore<B> {
    pub fn new(storage: B) -> Self {
        Self { storage }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn write_artifact(
        &self,
        record: &mut CheckpointRecord,
        worker_id: WorkerId,
        artifact_name: &str,
        bytes: &[u8],
        checksum: String,
        checksum_algorithm: String,
        now_millis: u64,
    ) -> Result<ArtifactRecord, CheckpointError> {
        let key = checkpoint_worker_artifact(
            &record.run_id.0,
            &record.checkpoint_id.0,
            &worker_id.0,
            artifact_name,
        );
        self.storage.put(&key, bytes)?;
        let artifact = ArtifactRecord {
            worker_id,
            artifact_name: artifact_name.to_string(),
            storage_key: key.as_str().to_string(),
            size_bytes: bytes.len() as u64,
            checksum,
            checksum_algorithm,
        };
        if record.state == CheckpointState::Initiated {
            record.transition_to(CheckpointState::UploadInProgress, now_millis)?;
        }
        record.add_artifact(artifact.clone(), now_millis)?;
        Ok(artifact)
    }

    pub fn write_pending_manifest(
        &self,
        record: &CheckpointRecord,
    ) -> Result<CheckpointManifest, CheckpointError> {
        let manifest = CheckpointManifest::from_record(record, None)?;
        let key = checkpoint_manifest_pending(&record.run_id.0, &record.checkpoint_id.0);
        self.storage.put(&key, &manifest.to_json_bytes()?)?;
        Ok(manifest)
    }

    pub fn commit_checkpoint(
        &self,
        record: &mut CheckpointRecord,
        committed_at_millis: u64,
    ) -> Result<CheckpointManifest, CheckpointError> {
        if record.artifacts.is_empty() {
            return Err(CheckpointError::ManifestValidation(
                "cannot commit checkpoint without artifacts".into(),
            ));
        }
        if record.state == CheckpointState::Initiated {
            record.transition_to(CheckpointState::UploadInProgress, committed_at_millis)?;
        }
        if record.state == CheckpointState::UploadInProgress {
            record.transition_to(CheckpointState::Validating, committed_at_millis)?;
        }
        for artifact in &record.artifacts {
            let storage_key = StorageKey::new(&artifact.storage_key).map_err(|err| match err {
                StorageError::InvalidKey(message) => {
                    CheckpointError::InvalidArtifact(format!("invalid storage key: {message}"))
                }
                other => CheckpointError::Storage(other),
            })?;
            validate_artifact_integrity(
                &self.storage,
                &storage_key,
                artifact.size_bytes,
                &artifact.checksum_algorithm,
                &artifact.checksum,
            )
            .map_err(CheckpointError::ArtifactValidation)?;
        }
        if record.state == CheckpointState::Validating {
            record.transition_to(CheckpointState::Committed, committed_at_millis)?;
        }

        let manifest = CheckpointManifest::from_record(record, Some(committed_at_millis))?;
        manifest.validate_for_commit()?;

        self.storage.put(
            &checkpoint_manifest_committed(&record.run_id.0, &record.checkpoint_id.0),
            &manifest.to_json_bytes()?,
        )?;
        self.storage.put(
            &checkpoint_committed_marker(&record.run_id.0, &record.checkpoint_id.0),
            b"committed\n",
        )?;
        self.storage.put(
            &latest_pointer(&record.run_id.0),
            record.checkpoint_id.0.as_bytes(),
        )?;
        Ok(manifest)
    }

    pub fn cleanup_checkpoints(
        &self,
        run_id: &RunId,
        policy: CheckpointCleanupPolicy,
    ) -> Result<CheckpointCleanupReport, CheckpointError> {
        let mut report = CheckpointCleanupReport::default();
        let run_prefix = StorageKey::new(format!("runs/{}/checkpoints", run_id.0))
            .map_err(CheckpointError::Storage)?;
        let keys = self.storage.list_prefix(&run_prefix)?;
        let latest_id = match self.storage.get(&latest_pointer(&run_id.0)) {
            Ok(raw) => {
                let candidate = String::from_utf8(raw)
                    .map_err(|e| {
                        CheckpointError::Serialization(format!("latest pointer is not utf-8: {e}"))
                    })?
                    .trim()
                    .to_string();
                if candidate.is_empty() {
                    None
                } else {
                    let marker = checkpoint_committed_marker(&run_id.0, &candidate);
                    if self.storage.exists(&marker)? {
                        Some(candidate)
                    } else {
                        None
                    }
                }
            }
            Err(StorageError::NotFound(_)) => None,
            Err(e) => return Err(CheckpointError::Storage(e)),
        };

        let mut checkpoints: std::collections::BTreeMap<String, Vec<StorageKey>> =
            std::collections::BTreeMap::new();
        for key in keys {
            let parts: Vec<&str> = key.as_str().split('/').collect();
            if parts.len() >= 4
                && parts[0] == "runs"
                && parts[1] == run_id.0
                && parts[2] == "checkpoints"
            {
                checkpoints
                    .entry(parts[3].to_string())
                    .or_default()
                    .push(key);
            }
        }

        let latest_invalid = latest_id.is_none();
        let mut committed_ids: Vec<String> = checkpoints
            .iter()
            .filter_map(|(id, ks)| {
                let has_manifest = ks
                    .iter()
                    .any(|k| k.as_str().ends_with("/manifest.committed.json"));
                let has_marker = ks.iter().any(|k| k.as_str().ends_with("/COMMITTED"));
                if has_manifest && has_marker {
                    Some(id.clone())
                } else {
                    None
                }
            })
            .collect();
        committed_ids.sort();

        let mut retained: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
        if policy.keep_latest {
            if let Some(id) = &latest_id {
                retained.insert(id.clone());
            }
        }
        if !latest_invalid {
            for id in committed_ids
                .iter()
                .rev()
                .take(policy.keep_last_n_committed)
            {
                retained.insert(id.clone());
            }
        }

        for (checkpoint_id, mut checkpoint_keys) in checkpoints {
            checkpoint_keys.sort();
            let has_pending = checkpoint_keys
                .iter()
                .any(|k| k.as_str().ends_with("/manifest.pending.json"));
            let has_manifest = checkpoint_keys
                .iter()
                .any(|k| k.as_str().ends_with("/manifest.committed.json"));
            let has_marker = checkpoint_keys
                .iter()
                .any(|k| k.as_str().ends_with("/COMMITTED"));
            let is_committed = has_manifest && has_marker;
            let should_delete = if is_committed {
                if latest_invalid {
                    false
                } else {
                    !retained.contains(&checkpoint_id)
                }
            } else {
                policy.remove_pending && has_pending
            };

            if should_delete {
                for key in checkpoint_keys {
                    if !policy.remove_orphan_artifacts && key.as_str().contains("/workers/") {
                        report.retained_keys.push(key);
                        continue;
                    }
                    match self.storage.delete(&key) {
                        Ok(()) => report.deleted_keys.push(key),
                        Err(e) => report
                            .errors
                            .push(format!("delete {} failed: {e}", key.as_str())),
                    }
                }
            } else if is_committed && latest_invalid {
                report.skipped_keys.extend(checkpoint_keys);
            } else {
                report.retained_keys.extend(checkpoint_keys);
            }
        }
        Ok(report)
    }

    pub fn load_latest_manifest(
        &self,
        run_id: &RunId,
    ) -> Result<CheckpointManifest, CheckpointError> {
        let latest_raw = self
            .storage
            .get(&latest_pointer(&run_id.0))
            .map_err(|err| match err {
                StorageError::NotFound(_) => CheckpointError::NoLatestCheckpoint,
                other => CheckpointError::Storage(other),
            })?;
        let checkpoint_id = String::from_utf8(latest_raw)
            .map_err(|e| {
                CheckpointError::Serialization(format!("latest pointer is not utf-8: {e}"))
            })?
            .trim()
            .to_string();
        if checkpoint_id.is_empty() {
            return Err(CheckpointError::NoLatestCheckpoint);
        }

        let marker = checkpoint_committed_marker(&run_id.0, &checkpoint_id);
        if !self.storage.exists(&marker)? {
            return Err(CheckpointError::MissingCommitMarker {
                run_id: run_id.0.clone(),
                checkpoint_id,
            });
        }

        let manifest_bytes = self
            .storage
            .get(&checkpoint_manifest_committed(&run_id.0, &checkpoint_id))?;
        let manifest = CheckpointManifest::from_json_bytes(&manifest_bytes)?;
        manifest.validate_for_commit()?;
        Ok(manifest)
    }
}

#[derive(Debug)]
pub enum CheckpointError {
    InvalidTransition {
        from: CheckpointState,
        to: CheckpointState,
    },
    TerminalState {
        current: CheckpointState,
        requested: CheckpointState,
    },
    InvalidArtifact(String),
    Serialization(String),
    ManifestValidation(String),
    ArtifactValidation(ArtifactValidationError),
    Storage(StorageError),
    MissingCommitMarker {
        run_id: String,
        checkpoint_id: String,
    },
    NoLatestCheckpoint,
}

#[derive(Debug)]
pub enum ArtifactValidationError {
    MissingArtifact {
        key: String,
    },
    SizeMismatch {
        key: String,
        expected_size_bytes: u64,
        actual_size_bytes: u64,
    },
    UnsupportedChecksumAlgorithm(String),
    ChecksumMismatch {
        algorithm: String,
        expected: String,
        actual: String,
    },
    StorageReadFailure {
        key: String,
        source: StorageError,
    },
}
impl Display for ArtifactValidationError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingArtifact { key } => write!(f, "artifact not found: {key}"),
            Self::SizeMismatch {
                key,
                expected_size_bytes,
                actual_size_bytes,
            } => write!(
                f,
                "artifact size mismatch for {key}: expected {expected_size_bytes} bytes, got {actual_size_bytes}"
            ),
            Self::UnsupportedChecksumAlgorithm(algorithm) => {
                write!(f, "unsupported checksum algorithm: {algorithm}")
            }
            Self::ChecksumMismatch {
                algorithm,
                expected,
                actual,
            } => write!(
                f,
                "artifact checksum mismatch using {algorithm}: expected {expected}, got {actual}"
            ),
            Self::StorageReadFailure { key, source } => {
                write!(f, "failed to read artifact {key}: {source}")
            }
        }
    }
}
impl Error for ArtifactValidationError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::StorageReadFailure { source, .. } => Some(source),
            _ => None,
        }
    }
}
impl Display for CheckpointError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidTransition { from, to } => {
                write!(f, "invalid checkpoint transition: {from:?} -> {to:?}")
            }
            Self::TerminalState { current, requested } => write!(
                f,
                "cannot transition terminal checkpoint state: {current:?} -> {requested:?}"
            ),
            Self::InvalidArtifact(m) => write!(f, "invalid artifact metadata: {m}"),
            Self::Serialization(m) => write!(f, "checkpoint manifest serialization error: {m}"),
            Self::ManifestValidation(m) => write!(f, "checkpoint manifest validation error: {m}"),
            Self::ArtifactValidation(err) => write!(f, "artifact validation error: {err}"),
            Self::Storage(err) => write!(f, "checkpoint storage error: {err}"),
            Self::MissingCommitMarker {
                run_id,
                checkpoint_id,
            } => write!(
                f,
                "missing commit marker for run {run_id} checkpoint {checkpoint_id}"
            ),
            Self::NoLatestCheckpoint => write!(f, "no latest checkpoint for run"),
        }
    }
}
impl Error for CheckpointError {}
impl From<StorageError> for CheckpointError {
    fn from(value: StorageError) -> Self {
        Self::Storage(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use runtime_storage::{
        checkpoint_manifest_pending, checkpoint_worker_artifact, latest_pointer, LocalStorage,
        StorageKey,
    };
    use std::collections::BTreeMap;
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};
    fn ids() -> (RunId, CheckpointId, WorkerId) {
        (
            RunId("run-1".into()),
            CheckpointId("ckpt-1".into()),
            WorkerId("worker-1".into()),
        )
    }
    fn artifact(worker_id: WorkerId) -> ArtifactRecord {
        ArtifactRecord {
            worker_id,
            artifact_name: "model-shard-0".into(),
            storage_key: "runs/run-1/checkpoints/ckpt-1/workers/worker-1/model-shard-0".into(),
            size_bytes: 128,
            checksum: "abc123".into(),
            checksum_algorithm: "sha256".into(),
        }
    }
    fn checksum_sha256(bytes: &[u8]) -> String {
        compute_checksum(ChecksumAlgorithm::Sha256, bytes)
    }
    fn test_dir(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("runtime-checkpoint-{name}-{nanos}"));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[derive(Clone, Default)]
    struct TrackingMemoryStorage {
        entries: std::sync::Arc<std::sync::RwLock<BTreeMap<StorageKey, Vec<u8>>>>,
        in_flight: std::sync::Arc<std::sync::atomic::AtomicUsize>,
        max_seen: std::sync::Arc<std::sync::atomic::AtomicUsize>,
        fail_put: std::sync::Arc<std::sync::atomic::AtomicBool>,
    }

    impl TrackingMemoryStorage {
        fn max_seen(&self) -> usize {
            self.max_seen.load(std::sync::atomic::Ordering::SeqCst)
        }
        fn set_fail_put(&self, v: bool) {
            self.fail_put.store(v, std::sync::atomic::Ordering::SeqCst);
        }
    }

    impl StorageBackend for TrackingMemoryStorage {
        fn put(&self, key: &StorageKey, bytes: &[u8]) -> Result<(), StorageError> {
            if self.fail_put.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(StorageError::Io(std::io::Error::other("forced failure")));
            }
            let now = self
                .in_flight
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                + 1;
            loop {
                let prev = self.max_seen.load(std::sync::atomic::Ordering::SeqCst);
                if now <= prev {
                    break;
                }
                if self
                    .max_seen
                    .compare_exchange(
                        prev,
                        now,
                        std::sync::atomic::Ordering::SeqCst,
                        std::sync::atomic::Ordering::SeqCst,
                    )
                    .is_ok()
                {
                    break;
                }
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
            self.entries
                .write()
                .unwrap()
                .insert(key.clone(), bytes.to_vec());
            self.in_flight
                .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }
        fn get(&self, key: &StorageKey) -> Result<Vec<u8>, StorageError> {
            self.entries
                .read()
                .unwrap()
                .get(key)
                .cloned()
                .ok_or_else(|| StorageError::NotFound(key.clone()))
        }
        fn exists(&self, key: &StorageKey) -> Result<bool, StorageError> {
            Ok(self.entries.read().unwrap().contains_key(key))
        }
        fn list_prefix(&self, prefix: &StorageKey) -> Result<Vec<StorageKey>, StorageError> {
            let p = format!("{}/", prefix.as_str());
            let mut keys: Vec<_> = self
                .entries
                .read()
                .unwrap()
                .keys()
                .filter(|k| k.as_str().starts_with(&p))
                .cloned()
                .collect();
            keys.sort();
            Ok(keys)
        }
        fn delete(&self, key: &StorageKey) -> Result<(), StorageError> {
            self.entries.write().unwrap().remove(key);
            Ok(())
        }
    }

    #[derive(Clone, Default)]
    struct MemoryStorage {
        entries: std::sync::Arc<std::sync::RwLock<BTreeMap<StorageKey, Vec<u8>>>>,
    }

    impl StorageBackend for MemoryStorage {
        fn put(&self, key: &StorageKey, bytes: &[u8]) -> Result<(), StorageError> {
            self.entries
                .write()
                .unwrap()
                .insert(key.clone(), bytes.to_vec());
            Ok(())
        }

        fn get(&self, key: &StorageKey) -> Result<Vec<u8>, StorageError> {
            self.entries
                .read()
                .unwrap()
                .get(key)
                .cloned()
                .ok_or_else(|| StorageError::NotFound(key.clone()))
        }

        fn exists(&self, key: &StorageKey) -> Result<bool, StorageError> {
            Ok(self.entries.read().unwrap().contains_key(key))
        }

        fn list_prefix(&self, prefix: &StorageKey) -> Result<Vec<StorageKey>, StorageError> {
            let mut out = self
                .entries
                .read()
                .unwrap()
                .keys()
                .filter(|key| key.as_str().starts_with(prefix.as_str()))
                .cloned()
                .collect::<Vec<_>>();
            out.sort();
            Ok(out)
        }

        fn delete(&self, key: &StorageKey) -> Result<(), StorageError> {
            self.entries.write().unwrap().remove(key);
            Ok(())
        }
    }

    #[test]
    fn new_checkpoint_starts_in_initiated() {
        let (run_id, checkpoint_id, _) = ids();
        let record = CheckpointRecord::new(run_id, checkpoint_id, 42, 1000);
        assert_eq!(record.state, CheckpointState::Initiated);
        assert!(record.is_partial());
    }
    #[test]
    fn valid_successful_transition_path_reaches_committed() {
        let (run_id, checkpoint_id, _) = ids();
        let mut record = CheckpointRecord::new(run_id, checkpoint_id, 42, 1000);
        record
            .transition_to(CheckpointState::UploadInProgress, 1010)
            .unwrap();
        record
            .transition_to(CheckpointState::Validating, 1020)
            .unwrap();
        record
            .transition_to(CheckpointState::Committed, 1030)
            .unwrap();
        assert_eq!(record.state, CheckpointState::Committed);
    }
    #[test]
    fn pending_manifest_is_not_restorable() {
        let (run_id, checkpoint_id, worker_id) = ids();
        let mut record = CheckpointRecord::new(run_id, checkpoint_id, 1, 1000);
        record.add_artifact(artifact(worker_id), 1001).unwrap();
        record
            .transition_to(CheckpointState::UploadInProgress, 1002)
            .unwrap();
        let manifest = CheckpointManifest::from_record(&record, None).unwrap();
        assert!(!manifest.is_restorable());
    }
    #[test]
    fn write_artifact_writes_bytes_and_metadata() {
        let dir = test_dir("artifact");
        let store = CheckpointStore::new(LocalStorage::new(&dir).unwrap());
        let (run_id, checkpoint_id, worker_id) = ids();
        let mut record = CheckpointRecord::new(run_id, checkpoint_id, 1, 1000);
        let out = store
            .write_artifact(
                &mut record,
                worker_id.clone(),
                "model.bin",
                b"hello",
                "sum".into(),
                "sha256".into(),
                1001,
            )
            .unwrap();
        assert_eq!(record.state, CheckpointState::UploadInProgress);
        assert_eq!(record.artifacts.len(), 1);
        assert_eq!(out.worker_id, worker_id);
        let bytes = store
            .storage
            .get(&checkpoint_worker_artifact(
                "run-1",
                "ckpt-1",
                "worker-1",
                "model.bin",
            ))
            .unwrap();
        assert_eq!(bytes, b"hello");
        fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn write_pending_manifest_writes_file() {
        let dir = test_dir("pending");
        let store = CheckpointStore::new(LocalStorage::new(&dir).unwrap());
        let (run_id, checkpoint_id, worker_id) = ids();
        let mut record = CheckpointRecord::new(run_id, checkpoint_id, 1, 1000);
        store
            .write_artifact(
                &mut record,
                worker_id,
                "a.bin",
                b"x",
                "c".into(),
                "sha256".into(),
                1001,
            )
            .unwrap();
        let manifest = store.write_pending_manifest(&record).unwrap();
        assert_eq!(manifest.committed_at_millis, None);
        assert!(store
            .storage
            .exists(&checkpoint_manifest_pending("run-1", "ckpt-1"))
            .unwrap());
        fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn commit_writes_committed_marker_and_latest() {
        let dir = test_dir("commit");
        let store = CheckpointStore::new(LocalStorage::new(&dir).unwrap());
        let (run_id, checkpoint_id, worker_id) = ids();
        let mut record = CheckpointRecord::new(run_id, checkpoint_id, 1, 1000);
        let bytes = b"x";
        store
            .write_artifact(
                &mut record,
                worker_id,
                "a.bin",
                bytes,
                checksum_sha256(bytes),
                "sha256".into(),
                bytes.len() as u64,
            )
            .unwrap();
        let manifest = store.commit_checkpoint(&mut record, 2000).unwrap();
        assert_eq!(manifest.state, CheckpointState::Committed);
        assert!(store
            .storage
            .exists(&checkpoint_manifest_committed("run-1", "ckpt-1"))
            .unwrap());
        assert!(store
            .storage
            .exists(&checkpoint_committed_marker("run-1", "ckpt-1"))
            .unwrap());
        assert_eq!(
            store.storage.get(&latest_pointer("run-1")).unwrap(),
            b"ckpt-1"
        );
        fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn load_latest_manifest_returns_committed() {
        let dir = test_dir("latest");
        let store = CheckpointStore::new(LocalStorage::new(&dir).unwrap());
        let (run_id, checkpoint_id, worker_id) = ids();
        let mut record = CheckpointRecord::new(run_id, checkpoint_id, 1, 1000);
        let bytes = b"x";
        store
            .write_artifact(
                &mut record,
                worker_id,
                "a.bin",
                bytes,
                checksum_sha256(bytes),
                "sha256".into(),
                bytes.len() as u64,
            )
            .unwrap();
        store.commit_checkpoint(&mut record, 2000).unwrap();
        let latest = store.load_latest_manifest(&RunId("run-1".into())).unwrap();
        assert_eq!(latest.checkpoint_id, "ckpt-1");
        fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn load_latest_fails_when_only_pending_exists() {
        let dir = test_dir("only-pending");
        let store = CheckpointStore::new(LocalStorage::new(&dir).unwrap());
        let (run_id, checkpoint_id, worker_id) = ids();
        let mut record = CheckpointRecord::new(run_id, checkpoint_id, 1, 1000);
        store
            .write_artifact(
                &mut record,
                worker_id,
                "a.bin",
                b"x",
                "c".into(),
                "sha256".into(),
                1001,
            )
            .unwrap();
        store.write_pending_manifest(&record).unwrap();
        let err = store
            .load_latest_manifest(&RunId("run-1".into()))
            .unwrap_err();
        assert!(matches!(err, CheckpointError::NoLatestCheckpoint));
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn interrupted_checkpoint_with_only_pending_manifest_is_not_restorable() {
        let dir = test_dir("interrupted-pending-only");
        let store = CheckpointStore::new(LocalStorage::new(&dir).unwrap());
        let (run_id, checkpoint_id, worker_id) = ids();
        let mut record = CheckpointRecord::new(run_id.clone(), checkpoint_id.clone(), 7, 1000);

        store
            .write_artifact(
                &mut record,
                worker_id,
                "weights.bin",
                b"artifact-bytes",
                "sum".into(),
                "sha256".into(),
                1001,
            )
            .unwrap();
        store.write_pending_manifest(&record).unwrap();

        let err = store.load_latest_manifest(&run_id).unwrap_err();
        assert!(matches!(err, CheckpointError::NoLatestCheckpoint));

        let pending_bytes = store
            .storage
            .get(&checkpoint_manifest_pending(&run_id.0, &checkpoint_id.0))
            .unwrap();
        let pending_manifest = CheckpointManifest::from_json_bytes(&pending_bytes).unwrap();
        assert!(!pending_manifest.is_restorable());

        fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn load_latest_fails_without_commit_marker() {
        let dir = test_dir("missing-marker");
        let storage = LocalStorage::new(&dir).unwrap();
        let store = CheckpointStore::new(storage.clone());
        let (run_id, checkpoint_id, worker_id) = ids();
        let mut record = CheckpointRecord::new(run_id, checkpoint_id, 1, 1000);
        store
            .write_artifact(
                &mut record,
                worker_id,
                "a.bin",
                b"x",
                "c".into(),
                "sha256".into(),
                1001,
            )
            .unwrap();
        record
            .transition_to(CheckpointState::Validating, 1002)
            .unwrap();
        record
            .transition_to(CheckpointState::Committed, 1003)
            .unwrap();
        let manifest = CheckpointManifest::from_record(&record, Some(1003)).unwrap();
        storage
            .put(
                &checkpoint_manifest_committed("run-1", "ckpt-1"),
                &manifest.to_json_bytes().unwrap(),
            )
            .unwrap();
        storage.put(&latest_pointer("run-1"), b"ckpt-1").unwrap();
        let err = store
            .load_latest_manifest(&RunId("run-1".into()))
            .unwrap_err();
        assert!(matches!(err, CheckpointError::MissingCommitMarker { .. }));
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn newer_partial_checkpoint_does_not_replace_previous_committed_checkpoint() {
        let dir = test_dir("newer-partial-does-not-win");
        let store = CheckpointStore::new(LocalStorage::new(&dir).unwrap());
        let run_id = RunId("run-partial".into());
        let worker_id = WorkerId("worker-a".into());

        let mut checkpoint_1 =
            CheckpointRecord::new(run_id.clone(), CheckpointId("ckpt-1".into()), 10, 1000);
        let bytes_a = b"a";
        store
            .write_artifact(
                &mut checkpoint_1,
                worker_id.clone(),
                "a.bin",
                bytes_a,
                checksum_sha256(bytes_a),
                "sha256".into(),
                bytes_a.len() as u64,
            )
            .unwrap();
        store.commit_checkpoint(&mut checkpoint_1, 2000).unwrap();
        let latest_after_commit = store.load_latest_manifest(&run_id).unwrap();
        assert_eq!(latest_after_commit.checkpoint_id, "ckpt-1");

        let mut checkpoint_2 =
            CheckpointRecord::new(run_id.clone(), CheckpointId("ckpt-2".into()), 11, 3000);
        store
            .write_artifact(
                &mut checkpoint_2,
                worker_id,
                "b.bin",
                b"b",
                "sum-b".into(),
                "sha256".into(),
                3001,
            )
            .unwrap();
        store.write_pending_manifest(&checkpoint_2).unwrap();

        let latest_after_newer_partial = store.load_latest_manifest(&run_id).unwrap();
        assert_eq!(latest_after_newer_partial.checkpoint_id, "ckpt-1");
        assert_eq!(latest_after_newer_partial.step, 10);

        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn checkpoint_missing_committed_marker_is_not_restorable() {
        let dir = test_dir("missing-committed-marker");
        let storage = LocalStorage::new(&dir).unwrap();
        let store = CheckpointStore::new(storage.clone());
        let run_id = RunId("run-marker".into());
        let checkpoint_id = CheckpointId("ckpt-marker".into());
        let worker_id = WorkerId("worker-z".into());
        let mut record = CheckpointRecord::new(run_id.clone(), checkpoint_id.clone(), 5, 1000);

        store
            .write_artifact(
                &mut record,
                worker_id,
                "state.bin",
                b"state",
                "sum".into(),
                "sha256".into(),
                1001,
            )
            .unwrap();
        record
            .transition_to(CheckpointState::Validating, 1002)
            .unwrap();
        record
            .transition_to(CheckpointState::Committed, 1003)
            .unwrap();
        let committed_manifest = CheckpointManifest::from_record(&record, Some(1003)).unwrap();
        storage
            .put(
                &checkpoint_manifest_committed(&run_id.0, &checkpoint_id.0),
                &committed_manifest.to_json_bytes().unwrap(),
            )
            .unwrap();
        storage
            .put(&latest_pointer(&run_id.0), checkpoint_id.0.as_bytes())
            .unwrap();

        let err = store.load_latest_manifest(&run_id).unwrap_err();
        assert!(matches!(err, CheckpointError::MissingCommitMarker { .. }));

        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn committed_checkpoint_survives_store_recreation() {
        let dir = test_dir("survives-store-recreation");
        let run_id = RunId("run-recreate".into());
        let checkpoint_id = CheckpointId("ckpt-recreate".into());
        let worker_id = WorkerId("worker-recreate".into());

        {
            let first_store = CheckpointStore::new(LocalStorage::new(&dir).unwrap());
            let mut record =
                CheckpointRecord::new(run_id.clone(), checkpoint_id.clone(), 123, 1000);
            let bytes = b"tensor";
            first_store
                .write_artifact(
                    &mut record,
                    worker_id.clone(),
                    "tensor.bin",
                    bytes,
                    checksum_sha256(bytes),
                    "sha256".into(),
                    bytes.len() as u64,
                )
                .unwrap();
            first_store.commit_checkpoint(&mut record, 2000).unwrap();
        }

        let second_store = CheckpointStore::new(LocalStorage::new(&dir).unwrap());
        let latest = second_store.load_latest_manifest(&run_id).unwrap();
        assert_eq!(latest.checkpoint_id, checkpoint_id.0);
        assert_eq!(latest.step, 123);
        assert!(latest.is_restorable());

        fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn commit_fails_without_artifacts() {
        let dir = test_dir("no-artifacts");
        let store = CheckpointStore::new(LocalStorage::new(&dir).unwrap());
        let (run_id, checkpoint_id, _) = ids();
        let mut record = CheckpointRecord::new(run_id, checkpoint_id, 1, 1000);
        let err = store.commit_checkpoint(&mut record, 2000).unwrap_err();
        assert!(matches!(err, CheckpointError::ManifestValidation(_)));
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn checkpoint_store_works_with_non_local_trait_backend() {
        let store = CheckpointStore::new(MemoryStorage::default());
        let mut record = CheckpointRecord::new(
            RunId("run-memory".into()),
            CheckpointId("ckpt-memory".into()),
            9,
            1000,
        );
        let bytes = b"abc";
        store
            .write_artifact(
                &mut record,
                WorkerId("worker-memory".into()),
                "weights.bin",
                bytes,
                checksum_sha256(bytes),
                "sha256".into(),
                bytes.len() as u64,
            )
            .unwrap();
        store.write_pending_manifest(&record).unwrap();
        let committed = store.commit_checkpoint(&mut record, 1002).unwrap();
        let latest = store
            .load_latest_manifest(&RunId("run-memory".into()))
            .unwrap();
        assert_eq!(latest.checkpoint_id, committed.checkpoint_id);
    }

    #[test]
    fn commit_validates_artifact_integrity_success() {
        let storage = MemoryStorage::default();
        let store = CheckpointStore::new(storage);
        let mut record = CheckpointRecord::new(
            RunId("run-v".into()),
            CheckpointId("ckpt-v".into()),
            1,
            1000,
        );
        let bytes = b"valid-artifact";
        let checksum = compute_checksum(ChecksumAlgorithm::Sha256, bytes);
        store
            .write_artifact(
                &mut record,
                WorkerId("worker-1".into()),
                "weights.bin",
                bytes,
                checksum,
                "sha256".into(),
                1001,
            )
            .unwrap();

        let committed = store.commit_checkpoint(&mut record, 1002).unwrap();
        assert!(committed.is_restorable());
    }

    #[test]
    fn commit_rejects_missing_artifact() {
        let store = CheckpointStore::new(MemoryStorage::default());
        let mut record = CheckpointRecord::new(
            RunId("run-m".into()),
            CheckpointId("ckpt-m".into()),
            1,
            1000,
        );
        let mut a = artifact(WorkerId("worker-1".into()));
        a.storage_key = "runs/run-m/checkpoints/ckpt-m/workers/worker-1/missing.bin".into();
        record.add_artifact(a, 1001).unwrap();
        let err = store.commit_checkpoint(&mut record, 1002).unwrap_err();
        assert!(matches!(
            err,
            CheckpointError::ArtifactValidation(ArtifactValidationError::MissingArtifact { .. })
        ));
    }

    #[test]
    fn commit_rejects_size_mismatch() {
        let storage = MemoryStorage::default();
        let key =
            StorageKey::new("runs/run-s/checkpoints/ckpt-s/workers/worker-1/data.bin").unwrap();
        let bytes = b"abc";
        storage.put(&key, bytes).unwrap();
        let mut record = CheckpointRecord::new(
            RunId("run-s".into()),
            CheckpointId("ckpt-s".into()),
            1,
            1000,
        );
        let mut a = artifact(WorkerId("worker-1".into()));
        a.storage_key = key.to_string();
        a.size_bytes = 99;
        a.checksum = compute_checksum(ChecksumAlgorithm::Sha256, bytes);
        record.add_artifact(a, 1001).unwrap();
        let err = CheckpointStore::new(storage)
            .commit_checkpoint(&mut record, 1002)
            .unwrap_err();
        assert!(matches!(
            err,
            CheckpointError::ArtifactValidation(ArtifactValidationError::SizeMismatch { .. })
        ));
    }

    #[test]
    fn commit_rejects_checksum_mismatch() {
        let storage = MemoryStorage::default();
        let key =
            StorageKey::new("runs/run-c/checkpoints/ckpt-c/workers/worker-1/data.bin").unwrap();
        let bytes = b"abc";
        storage.put(&key, bytes).unwrap();
        let mut record = CheckpointRecord::new(
            RunId("run-c".into()),
            CheckpointId("ckpt-c".into()),
            1,
            1000,
        );
        let mut a = artifact(WorkerId("worker-1".into()));
        a.storage_key = key.to_string();
        a.size_bytes = bytes.len() as u64;
        a.checksum = "deadbeef".into();
        record.add_artifact(a, 1001).unwrap();
        let err = CheckpointStore::new(storage)
            .commit_checkpoint(&mut record, 1002)
            .unwrap_err();
        assert!(matches!(
            err,
            CheckpointError::ArtifactValidation(ArtifactValidationError::ChecksumMismatch { .. })
        ));
    }

    #[test]
    fn commit_rejects_unsupported_checksum_algorithm() {
        let storage = MemoryStorage::default();
        let key =
            StorageKey::new("runs/run-a/checkpoints/ckpt-a/workers/worker-1/data.bin").unwrap();
        let bytes = b"abc";
        storage.put(&key, bytes).unwrap();
        let mut record = CheckpointRecord::new(
            RunId("run-a".into()),
            CheckpointId("ckpt-a".into()),
            1,
            1000,
        );
        let mut a = artifact(WorkerId("worker-1".into()));
        a.storage_key = key.to_string();
        a.size_bytes = bytes.len() as u64;
        a.checksum_algorithm = "sha512".into();
        a.checksum = compute_checksum(ChecksumAlgorithm::Sha256, bytes);
        record.add_artifact(a, 1001).unwrap();
        let err = CheckpointStore::new(storage)
            .commit_checkpoint(&mut record, 1002)
            .unwrap_err();
        assert!(matches!(
            err,
            CheckpointError::ArtifactValidation(
                ArtifactValidationError::UnsupportedChecksumAlgorithm(_)
            )
        ));
    }

    #[test]
    fn failed_validation_checkpoint_is_not_restorable() {
        let store = CheckpointStore::new(MemoryStorage::default());
        let mut record = CheckpointRecord::new(
            RunId("run-fr".into()),
            CheckpointId("ckpt-fr".into()),
            1,
            1000,
        );
        let mut a = artifact(WorkerId("worker-1".into()));
        a.storage_key = "runs/run-fr/checkpoints/ckpt-fr/workers/worker-1/missing.bin".into();
        record.add_artifact(a, 1001).unwrap();
        let _ = store.commit_checkpoint(&mut record, 1002).unwrap_err();
        let latest = store.load_latest_manifest(&RunId("run-fr".into()));
        assert!(matches!(latest, Err(CheckpointError::NoLatestCheckpoint)));
    }

    #[test]
    fn failed_validation_does_not_replace_previous_committed_checkpoint() {
        let store = CheckpointStore::new(MemoryStorage::default());
        let run_id = RunId("run-prev".into());
        let mut good =
            CheckpointRecord::new(run_id.clone(), CheckpointId("ckpt-good".into()), 1, 1000);
        let checksum = compute_checksum(ChecksumAlgorithm::Sha256, b"ok");
        store
            .write_artifact(
                &mut good,
                WorkerId("worker-1".into()),
                "weights.bin",
                b"ok",
                checksum,
                "sha256".into(),
                1001,
            )
            .unwrap();
        store.commit_checkpoint(&mut good, 1002).unwrap();

        let mut bad =
            CheckpointRecord::new(run_id.clone(), CheckpointId("ckpt-bad".into()), 2, 1003);
        let mut a = artifact(WorkerId("worker-1".into()));
        a.storage_key = "runs/run-prev/checkpoints/ckpt-bad/workers/worker-1/missing.bin".into();
        bad.add_artifact(a, 1004).unwrap();
        let _ = store.commit_checkpoint(&mut bad, 1005).unwrap_err();

        let latest = store.load_latest_manifest(&run_id).unwrap();
        assert_eq!(latest.checkpoint_id, "ckpt-good");
    }

    #[test]
    fn concurrent_upload_writes_all_artifacts_and_returns_metadata() {
        let storage = MemoryStorage::default();
        let policy = StorageOperationPolicy::default();
        let uploads = vec![
            CheckpointArtifactUpload {
                worker_id: WorkerId("w1".into()),
                artifact_name: "a.bin".into(),
                storage_key: StorageKey::new("runs/r/checkpoints/c/workers/w1/a.bin").unwrap(),
                bytes: b"aaa".to_vec(),
                checksum_algorithm: ChecksumAlgorithm::Sha256,
            },
            CheckpointArtifactUpload {
                worker_id: WorkerId("w2".into()),
                artifact_name: "b.bin".into(),
                storage_key: StorageKey::new("runs/r/checkpoints/c/workers/w2/b.bin").unwrap(),
                bytes: b"bbb".to_vec(),
                checksum_algorithm: ChecksumAlgorithm::Sha256,
            },
        ];
        let results = upload_checkpoint_artifacts_concurrently(&storage, uploads, &policy).unwrap();
        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|r| r.checksum_algorithm == "sha256"));
    }

    #[test]
    fn concurrent_upload_metadata_uses_real_sha256() {
        let storage = MemoryStorage::default();
        let policy = StorageOperationPolicy::default();
        let uploads = vec![CheckpointArtifactUpload {
            worker_id: WorkerId("w1".into()),
            artifact_name: "x".into(),
            storage_key: StorageKey::new("runs/r/checkpoints/c/workers/w1/x").unwrap(),
            bytes: b"hello world".to_vec(),
            checksum_algorithm: ChecksumAlgorithm::Sha256,
        }];
        let results = upload_checkpoint_artifacts_concurrently(&storage, uploads, &policy).unwrap();
        assert_eq!(
            results[0].checksum,
            "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
        );
    }

    #[test]
    fn concurrent_upload_respects_concurrency_limit() {
        let storage = TrackingMemoryStorage::default();
        let policy = StorageOperationPolicy {
            max_concurrent_operations: 2,
            ..Default::default()
        };
        let uploads: Vec<_> = (0..8)
            .map(|i| CheckpointArtifactUpload {
                worker_id: WorkerId(format!("w{i}")),
                artifact_name: format!("a{i}.bin"),
                storage_key: StorageKey::new(format!("runs/r/checkpoints/c/workers/w{i}/a{i}.bin"))
                    .unwrap(),
                bytes: vec![i as u8],
                checksum_algorithm: ChecksumAlgorithm::Sha256,
            })
            .collect();
        let _ = upload_checkpoint_artifacts_concurrently(&storage, uploads, &policy).unwrap();
        assert!(storage.max_seen() <= 2);
    }

    #[test]
    fn concurrent_upload_rejects_zero_concurrency_policy() {
        let storage = MemoryStorage::default();
        let policy = StorageOperationPolicy {
            max_concurrent_operations: 0,
            ..Default::default()
        };
        let err = upload_checkpoint_artifacts_concurrently(&storage, vec![], &policy).unwrap_err();
        assert!(matches!(
            err,
            CheckpointError::Storage(StorageError::InvalidKey(_))
        ));
    }

    #[test]
    fn concurrent_upload_failure_returns_error() {
        let storage = TrackingMemoryStorage::default();
        storage.set_fail_put(true);
        let policy = StorageOperationPolicy::default();
        let uploads = vec![CheckpointArtifactUpload {
            worker_id: WorkerId("w1".into()),
            artifact_name: "x".into(),
            storage_key: StorageKey::new("runs/r/checkpoints/c/workers/w1/x").unwrap(),
            bytes: b"x".to_vec(),
            checksum_algorithm: ChecksumAlgorithm::Sha256,
        }];
        assert!(upload_checkpoint_artifacts_concurrently(&storage, uploads, &policy).is_err());
    }

    #[test]
    fn uploaded_metadata_can_be_committed_successfully() {
        let store = CheckpointStore::new(MemoryStorage::default());
        let mut record = CheckpointRecord::new(
            RunId("run-up".into()),
            CheckpointId("ckpt-up".into()),
            1,
            1000,
        );
        let policy = StorageOperationPolicy::default();
        let uploads = vec![
            CheckpointArtifactUpload {
                worker_id: WorkerId("w1".into()),
                artifact_name: "a.bin".into(),
                storage_key: StorageKey::new("runs/run-up/checkpoints/ckpt-up/workers/w1/a.bin")
                    .unwrap(),
                bytes: b"abc".to_vec(),
                checksum_algorithm: ChecksumAlgorithm::Sha256,
            },
            CheckpointArtifactUpload {
                worker_id: WorkerId("w2".into()),
                artifact_name: "b.bin".into(),
                storage_key: StorageKey::new("runs/run-up/checkpoints/ckpt-up/workers/w2/b.bin")
                    .unwrap(),
                bytes: b"xyz".to_vec(),
                checksum_algorithm: ChecksumAlgorithm::Sha256,
            },
        ];
        let artifacts =
            upload_checkpoint_artifacts_concurrently(&store.storage, uploads, &policy).unwrap();
        for a in artifacts {
            record.add_artifact(a, 1001).unwrap();
        }
        let committed = store.commit_checkpoint(&mut record, 2000).unwrap();
        assert_eq!(committed.artifacts.len(), 2);
    }

    #[test]
    fn cleanup_preserves_latest_and_removes_pending_when_enabled() {
        let store = CheckpointStore::new(MemoryStorage::default());
        let run = RunId("run-clean".into());
        let mut c1 = CheckpointRecord::new(run.clone(), CheckpointId("ckpt-1".into()), 1, 1000);
        let mut c2 = CheckpointRecord::new(run.clone(), CheckpointId("ckpt-2".into()), 2, 1001);
        store
            .write_artifact(
                &mut c1,
                WorkerId("w".into()),
                "a",
                b"1",
                checksum_sha256(b"1"),
                "sha256".into(),
                1,
            )
            .unwrap();
        store.commit_checkpoint(&mut c1, 2000).unwrap();
        store
            .write_artifact(
                &mut c2,
                WorkerId("w".into()),
                "b",
                b"2",
                checksum_sha256(b"2"),
                "sha256".into(),
                2,
            )
            .unwrap();
        store.write_pending_manifest(&c2).unwrap();
        let report = store
            .cleanup_checkpoints(&run, CheckpointCleanupPolicy::default())
            .unwrap();
        assert!(report
            .deleted_keys
            .iter()
            .any(|k| k.as_str().contains("ckpt-2/manifest.pending.json")));
        assert_eq!(
            store.load_latest_manifest(&run).unwrap().checkpoint_id,
            "ckpt-1"
        );
    }

    #[test]
    fn cleanup_keeps_pending_when_disabled() {
        let store = CheckpointStore::new(MemoryStorage::default());
        let run = RunId("run-keep-pending".into());
        let mut c1 = CheckpointRecord::new(run.clone(), CheckpointId("ckpt-p".into()), 1, 1000);
        store
            .write_artifact(
                &mut c1,
                WorkerId("w".into()),
                "a",
                b"1",
                checksum_sha256(b"1"),
                "sha256".into(),
                1,
            )
            .unwrap();
        store.write_pending_manifest(&c1).unwrap();
        let report = store
            .cleanup_checkpoints(
                &run,
                CheckpointCleanupPolicy {
                    remove_pending: false,
                    ..Default::default()
                },
            )
            .unwrap();
        assert!(report
            .retained_keys
            .iter()
            .any(|k| k.as_str().contains("manifest.pending.json")));
    }

    #[test]
    fn cleanup_skips_committed_when_latest_missing() {
        let store = CheckpointStore::new(MemoryStorage::default());
        let run = RunId("run-no-latest".into());
        let mut c1 = CheckpointRecord::new(run.clone(), CheckpointId("ckpt-1".into()), 1, 1000);
        store
            .write_artifact(
                &mut c1,
                WorkerId("w".into()),
                "a",
                b"1",
                checksum_sha256(b"1"),
                "sha256".into(),
                1,
            )
            .unwrap();
        store.commit_checkpoint(&mut c1, 2000).unwrap();
        store.storage.delete(&latest_pointer(&run.0)).unwrap();
        let report = store
            .cleanup_checkpoints(&run, CheckpointCleanupPolicy::default())
            .unwrap();
        assert!(!report.skipped_keys.is_empty());
    }

    #[test]
    fn checksum_algorithm_parsing_and_canonical_name() {
        assert_eq!(
            ChecksumAlgorithm::parse("sha256").unwrap(),
            ChecksumAlgorithm::Sha256
        );
        assert_eq!(
            ChecksumAlgorithm::parse("SHA256").unwrap(),
            ChecksumAlgorithm::Sha256
        );
        assert_eq!(
            ChecksumAlgorithm::parse("sha-256").unwrap(),
            ChecksumAlgorithm::Sha256
        );
        assert_eq!(ChecksumAlgorithm::Sha256.as_str(), "sha256");
        assert!(matches!(
            ChecksumAlgorithm::parse("md5"),
            Err(ArtifactValidationError::UnsupportedChecksumAlgorithm(_))
        ));
    }

    #[test]
    fn compute_and_verify_sha256_checksum() {
        let bytes = b"hello world";
        let digest = compute_checksum(ChecksumAlgorithm::Sha256, bytes);
        assert_eq!(
            digest,
            "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
        );
        verify_checksum(ChecksumAlgorithm::Sha256, bytes, &digest).unwrap();
        assert!(matches!(
            verify_checksum(ChecksumAlgorithm::Sha256, bytes, "deadbeef"),
            Err(ArtifactValidationError::ChecksumMismatch { .. })
        ));
    }

    #[test]
    fn validate_artifact_integrity_success() {
        let storage = MemoryStorage::default();
        let key = StorageKey::new("runs/r/checkpoints/c/workers/w/model.bin").unwrap();
        let bytes = b"artifact-bytes";
        storage.put(&key, bytes).unwrap();
        let checksum = compute_checksum(ChecksumAlgorithm::Sha256, bytes);
        validate_artifact_integrity(&storage, &key, bytes.len() as u64, "sha256", &checksum)
            .unwrap();
    }

    #[test]
    fn validate_artifact_integrity_failures() {
        let storage = MemoryStorage::default();
        let key = StorageKey::new("runs/r/checkpoints/c/workers/w/model.bin").unwrap();
        let missing = validate_artifact_integrity(&storage, &key, 1, "sha256", "abc");
        assert!(matches!(
            missing,
            Err(ArtifactValidationError::MissingArtifact { .. })
        ));

        let bytes = b"artifact-bytes";
        storage.put(&key, bytes).unwrap();
        let checksum = compute_checksum(ChecksumAlgorithm::Sha256, bytes);

        assert!(matches!(
            validate_artifact_integrity(&storage, &key, 999, "sha256", &checksum),
            Err(ArtifactValidationError::SizeMismatch { .. })
        ));
        assert!(matches!(
            validate_artifact_integrity(&storage, &key, bytes.len() as u64, "sha512", &checksum),
            Err(ArtifactValidationError::UnsupportedChecksumAlgorithm(_))
        ));
        assert!(matches!(
            validate_artifact_integrity(&storage, &key, bytes.len() as u64, "sha256", "deadbeef"),
            Err(ArtifactValidationError::ChecksumMismatch { .. })
        ));
    }
}
