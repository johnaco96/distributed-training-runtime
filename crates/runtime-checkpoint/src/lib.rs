#![forbid(unsafe_code)]

//! Checkpoint state machine, metadata models, and validation helpers for MVP.
//!
//! This crate intentionally focuses on in-memory state machine behavior only.
//! It does not integrate with storage or coordinator RPCs yet.

use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct CheckpointId(pub String);

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct RunId(pub String);

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct WorkerId(pub String);

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
        matches!(
            self,
            CheckpointState::Committed | CheckpointState::Failed | CheckpointState::Aborted
        )
    }

    fn can_transition_to(self, next: CheckpointState) -> bool {
        matches!(
            (self, next),
            (
                CheckpointState::Initiated,
                CheckpointState::UploadInProgress
            ) | (
                CheckpointState::UploadInProgress,
                CheckpointState::Validating
            ) | (CheckpointState::Validating, CheckpointState::Committed)
                | (CheckpointState::Initiated, CheckpointState::Failed)
                | (CheckpointState::UploadInProgress, CheckpointState::Failed)
                | (CheckpointState::Validating, CheckpointState::Failed)
                | (CheckpointState::Initiated, CheckpointState::Aborted)
                | (CheckpointState::UploadInProgress, CheckpointState::Aborted)
                | (CheckpointState::Validating, CheckpointState::Aborted)
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
                "artifact_name and storage_key must be non-empty".to_string(),
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
            .map_err(|err| CheckpointError::Serialization(format!("serialize manifest: {err}")))
    }

    pub fn from_json_bytes(bytes: &[u8]) -> Result<Self, CheckpointError> {
        serde_json::from_slice(bytes)
            .map_err(|err| CheckpointError::Serialization(format!("deserialize manifest: {err}")))
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
                    "committed manifest must include committed_at_millis".to_string(),
                ));
            }
            if self.artifacts.is_empty() {
                return Err(CheckpointError::ManifestValidation(
                    "committed manifest must include at least one artifact".to_string(),
                ));
            }
        }

        Ok(())
    }
}

impl From<ArtifactRecord> for ManifestArtifact {
    fn from(value: ArtifactRecord) -> Self {
        Self {
            worker_id: value.worker_id.0,
            artifact_name: value.artifact_name,
            storage_key: value.storage_key,
            size_bytes: value.size_bytes,
            checksum: value.checksum,
            checksum_algorithm: value.checksum_algorithm,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
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
}

impl Display for CheckpointError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            CheckpointError::InvalidTransition { from, to } => {
                write!(f, "invalid checkpoint transition: {from:?} -> {to:?}")
            }
            CheckpointError::TerminalState { current, requested } => write!(
                f,
                "cannot transition terminal checkpoint state: {current:?} -> {requested:?}"
            ),
            CheckpointError::InvalidArtifact(message) => {
                write!(f, "invalid artifact metadata: {message}")
            }
            CheckpointError::Serialization(message) => {
                write!(f, "checkpoint manifest serialization error: {message}")
            }
            CheckpointError::ManifestValidation(message) => {
                write!(f, "checkpoint manifest validation error: {message}")
            }
        }
    }
}

impl Error for CheckpointError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn ids() -> (RunId, CheckpointId, WorkerId) {
        (
            RunId("run-1".to_string()),
            CheckpointId("ckpt-1".to_string()),
            WorkerId("worker-1".to_string()),
        )
    }

    fn artifact(worker_id: WorkerId) -> ArtifactRecord {
        ArtifactRecord {
            worker_id,
            artifact_name: "model-shard-0".to_string(),
            storage_key: "runs/run-1/checkpoints/ckpt-1/workers/worker-1/model-shard-0".to_string(),
            size_bytes: 128,
            checksum: "abc123".to_string(),
            checksum_algorithm: "sha256".to_string(),
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
            .expect("initiated -> upload should be valid");
        record
            .transition_to(CheckpointState::Validating, 1020)
            .expect("upload -> validating should be valid");
        record
            .transition_to(CheckpointState::Committed, 1030)
            .expect("validating -> committed should be valid");

        assert_eq!(record.state, CheckpointState::Committed);
        assert_eq!(record.updated_at_millis, 1030);
    }

    #[test]
    fn committed_checkpoint_is_restorable() {
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

        assert!(record.is_restorable());
        assert!(!record.is_partial());
    }

    #[test]
    fn partial_checkpoint_is_not_restorable() {
        let (run_id, checkpoint_id, _) = ids();
        let mut record = CheckpointRecord::new(run_id, checkpoint_id, 42, 1000);
        record
            .transition_to(CheckpointState::UploadInProgress, 1010)
            .unwrap();

        assert!(!record.is_restorable());
        assert!(record.is_partial());
    }

    #[test]
    fn failed_checkpoint_is_not_restorable() {
        let (run_id, checkpoint_id, _) = ids();
        let mut record = CheckpointRecord::new(run_id, checkpoint_id, 42, 1000);
        record.transition_to(CheckpointState::Failed, 1001).unwrap();

        assert!(!record.is_restorable());
        assert!(!record.is_partial());
    }

    #[test]
    fn aborted_checkpoint_is_not_restorable() {
        let (run_id, checkpoint_id, _) = ids();
        let mut record = CheckpointRecord::new(run_id, checkpoint_id, 42, 1000);
        record
            .transition_to(CheckpointState::Aborted, 1001)
            .unwrap();

        assert!(!record.is_restorable());
        assert!(!record.is_partial());
    }

    #[test]
    fn terminal_states_cannot_transition() {
        let (run_id, checkpoint_id, _) = ids();
        let mut record = CheckpointRecord::new(run_id, checkpoint_id, 42, 1000);
        record.transition_to(CheckpointState::Failed, 1001).unwrap();

        let err = record
            .transition_to(CheckpointState::Committed, 1002)
            .expect_err("terminal states should not transition");
        assert!(matches!(err, CheckpointError::TerminalState { .. }));
    }

    #[test]
    fn invalid_transition_is_rejected() {
        let (run_id, checkpoint_id, _) = ids();
        let mut record = CheckpointRecord::new(run_id, checkpoint_id, 42, 1000);

        let err = record
            .transition_to(CheckpointState::Committed, 1001)
            .expect_err("initiated -> committed should be invalid");
        assert!(matches!(err, CheckpointError::InvalidTransition { .. }));
    }

    #[test]
    fn artifact_can_be_added_before_commit() {
        let (run_id, checkpoint_id, worker_id) = ids();
        let mut record = CheckpointRecord::new(run_id, checkpoint_id, 42, 1000);

        record
            .add_artifact(artifact(worker_id), 1001)
            .expect("artifact add should succeed before terminal state");

        assert_eq!(record.artifacts.len(), 1);
        assert_eq!(record.updated_at_millis, 1001);
    }

    #[test]
    fn artifact_cannot_be_added_after_commit() {
        let (run_id, checkpoint_id, worker_id) = ids();
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

        let err = record
            .add_artifact(artifact(worker_id), 1040)
            .expect_err("cannot add artifact once committed");
        assert!(matches!(err, CheckpointError::TerminalState { .. }));
    }

    #[test]
    fn is_partial_behaves_correctly_for_in_progress_states() {
        let (run_id, checkpoint_id, _) = ids();
        let mut record = CheckpointRecord::new(run_id, checkpoint_id, 42, 1000);

        assert!(record.is_partial());
        record
            .transition_to(CheckpointState::UploadInProgress, 1010)
            .unwrap();
        assert!(record.is_partial());
        record
            .transition_to(CheckpointState::Validating, 1020)
            .unwrap();
        assert!(record.is_partial());
        record
            .transition_to(CheckpointState::Committed, 1030)
            .unwrap();
        assert!(!record.is_partial());
    }

    #[test]
    fn manifest_from_committed_record_succeeds() {
        let (run_id, checkpoint_id, worker_id) = ids();
        let mut record = CheckpointRecord::new(run_id, checkpoint_id, 42, 1000);
        record.add_artifact(artifact(worker_id), 1005).unwrap();
        record
            .transition_to(CheckpointState::UploadInProgress, 1010)
            .unwrap();
        record
            .transition_to(CheckpointState::Validating, 1020)
            .unwrap();
        record
            .transition_to(CheckpointState::Committed, 1030)
            .unwrap();

        let manifest = CheckpointManifest::from_record(&record, Some(1030)).unwrap();

        assert_eq!(manifest.state, CheckpointState::Committed);
        assert_eq!(manifest.artifacts.len(), 1);
        assert_eq!(manifest.committed_at_millis, Some(1030));
    }

    #[test]
    fn manifest_json_round_trip_preserves_key_fields() {
        let manifest = CheckpointManifest {
            schema_version: CheckpointManifest::SCHEMA_VERSION,
            run_id: "run-1".to_string(),
            checkpoint_id: "ckpt-1".to_string(),
            step: 42,
            state: CheckpointState::Committed,
            created_at_millis: 1000,
            committed_at_millis: Some(1030),
            artifacts: vec![ManifestArtifact {
                worker_id: "worker-1".to_string(),
                artifact_name: "model-shard-0".to_string(),
                storage_key: "runs/run-1/checkpoints/ckpt-1/workers/worker-1/model-shard-0"
                    .to_string(),
                size_bytes: 128,
                checksum: "abc123".to_string(),
                checksum_algorithm: "sha256".to_string(),
            }],
        };

        let bytes = manifest.to_json_bytes().unwrap();
        let decoded = CheckpointManifest::from_json_bytes(&bytes).unwrap();

        assert_eq!(decoded.run_id, manifest.run_id);
        assert_eq!(decoded.checkpoint_id, manifest.checkpoint_id);
        assert_eq!(decoded.step, manifest.step);
        assert_eq!(decoded.state, manifest.state);
        assert_eq!(decoded.artifacts, manifest.artifacts);
    }

    #[test]
    fn committed_manifest_with_timestamp_is_restorable() {
        let manifest = CheckpointManifest {
            schema_version: CheckpointManifest::SCHEMA_VERSION,
            run_id: "run-1".to_string(),
            checkpoint_id: "ckpt-1".to_string(),
            step: 42,
            state: CheckpointState::Committed,
            created_at_millis: 1000,
            committed_at_millis: Some(1030),
            artifacts: vec![ManifestArtifact {
                worker_id: "worker-1".to_string(),
                artifact_name: "model-shard-0".to_string(),
                storage_key: "runs/run-1/checkpoints/ckpt-1/workers/worker-1/model-shard-0"
                    .to_string(),
                size_bytes: 128,
                checksum: "abc123".to_string(),
                checksum_algorithm: "sha256".to_string(),
            }],
        };

        assert!(manifest.is_restorable());
    }

    #[test]
    fn non_committed_manifest_is_not_restorable() {
        let manifest = CheckpointManifest {
            schema_version: CheckpointManifest::SCHEMA_VERSION,
            run_id: "run-1".to_string(),
            checkpoint_id: "ckpt-1".to_string(),
            step: 42,
            state: CheckpointState::Validating,
            created_at_millis: 1000,
            committed_at_millis: Some(1030),
            artifacts: vec![],
        };

        assert!(!manifest.is_restorable());
    }

    #[test]
    fn committed_manifest_without_timestamp_fails_validation() {
        let manifest = CheckpointManifest {
            schema_version: CheckpointManifest::SCHEMA_VERSION,
            run_id: "run-1".to_string(),
            checkpoint_id: "ckpt-1".to_string(),
            step: 42,
            state: CheckpointState::Committed,
            created_at_millis: 1000,
            committed_at_millis: None,
            artifacts: vec![ManifestArtifact {
                worker_id: "worker-1".to_string(),
                artifact_name: "model-shard-0".to_string(),
                storage_key: "k".to_string(),
                size_bytes: 128,
                checksum: "abc123".to_string(),
                checksum_algorithm: "sha256".to_string(),
            }],
        };

        let err = manifest.validate_for_commit().unwrap_err();
        assert!(matches!(err, CheckpointError::ManifestValidation(_)));
    }

    #[test]
    fn committed_manifest_without_artifacts_fails_validation() {
        let manifest = CheckpointManifest {
            schema_version: CheckpointManifest::SCHEMA_VERSION,
            run_id: "run-1".to_string(),
            checkpoint_id: "ckpt-1".to_string(),
            step: 42,
            state: CheckpointState::Committed,
            created_at_millis: 1000,
            committed_at_millis: Some(1030),
            artifacts: vec![],
        };

        let err = manifest.validate_for_commit().unwrap_err();
        assert!(matches!(err, CheckpointError::ManifestValidation(_)));
    }

    #[test]
    fn invalid_json_returns_error() {
        let err = CheckpointManifest::from_json_bytes(br#"{"run_id":"missing fields"}"#)
            .expect_err("invalid json should fail deserialization");
        assert!(matches!(err, CheckpointError::Serialization(_)));
    }
}
