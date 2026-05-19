#![forbid(unsafe_code)]

use runtime_storage::{
    checkpoint_committed_marker, checkpoint_manifest_committed, checkpoint_manifest_pending,
    checkpoint_worker_artifact, latest_pointer, StorageBackend, StorageError, StorageKey,
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

pub struct CheckpointStore<B: StorageBackend> {
    storage: B,
}
impl<B: StorageBackend> CheckpointStore<B> {
    pub fn new(storage: B) -> Self {
        Self { storage }
    }

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
