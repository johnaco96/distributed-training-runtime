#![forbid(unsafe_code)]

//! Storage abstractions and backend integrations.
//!
//! MVP scope provides a synchronous storage abstraction and a local filesystem
//! backend for checkpoint/dataset metadata and artifacts.

use std::error::Error;
use std::fmt::{Display, Formatter};
use std::fs;
use std::io;
use std::path::{Component, Path, PathBuf};

pub mod object_key;
#[cfg(feature = "s3")]
pub mod s3;

pub use object_key::storage_key_to_object_key;
#[cfg(feature = "s3")]
pub use s3::{S3Storage, S3StorageConfig};

/// Logical storage key using slash-separated segments.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct StorageKey(String);

impl StorageKey {
    /// Parse and validate a logical key.
    pub fn new(raw: impl AsRef<str>) -> Result<Self, StorageError> {
        let raw = raw.as_ref().replace('\\', "/");
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return Err(StorageError::InvalidKey("key cannot be empty".to_string()));
        }
        if trimmed.starts_with('/') {
            return Err(StorageError::InvalidKey(
                "absolute keys are not allowed".to_string(),
            ));
        }

        let mut normalized = Vec::new();
        for segment in trimmed.split('/') {
            if segment.is_empty() {
                continue;
            }
            if segment == "." || segment == ".." {
                return Err(StorageError::InvalidKey(format!(
                    "path traversal segment is not allowed: {segment}"
                )));
            }
            normalized.push(segment);
        }

        if normalized.is_empty() {
            return Err(StorageError::InvalidKey("key cannot be empty".to_string()));
        }

        Ok(Self(normalized.join("/")))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    fn to_path_under(&self, root: &Path) -> Result<PathBuf, StorageError> {
        let mut full = root.to_path_buf();
        for segment in self.0.split('/') {
            if segment == "." || segment == ".." || segment.is_empty() {
                return Err(StorageError::InvalidKey(format!(
                    "invalid segment in key: {segment}"
                )));
            }
            full.push(segment);
        }

        for comp in full.components() {
            if matches!(comp, Component::ParentDir) {
                return Err(StorageError::InvalidKey(
                    "resolved path escapes storage root".to_string(),
                ));
            }
        }

        Ok(full)
    }
}

impl Display for StorageKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Storage errors for MVP backends.
#[derive(Debug)]
pub enum StorageError {
    Io(io::Error),
    InvalidKey(String),
    NotFound(StorageKey),
}

impl Display for StorageError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageError::Io(err) => write!(f, "io error: {err}"),
            StorageError::InvalidKey(msg) => write!(f, "invalid key: {msg}"),
            StorageError::NotFound(key) => write!(f, "key not found: {key}"),
        }
    }
}

impl Error for StorageError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            StorageError::Io(err) => Some(err),
            StorageError::InvalidKey(_) | StorageError::NotFound(_) => None,
        }
    }
}

impl From<io::Error> for StorageError {
    fn from(value: io::Error) -> Self {
        StorageError::Io(value)
    }
}

pub trait StorageBackend {
    fn put(&self, key: &StorageKey, bytes: &[u8]) -> Result<(), StorageError>;
    fn get(&self, key: &StorageKey) -> Result<Vec<u8>, StorageError>;
    fn exists(&self, key: &StorageKey) -> Result<bool, StorageError>;
    fn list_prefix(&self, prefix: &StorageKey) -> Result<Vec<StorageKey>, StorageError>;
    fn delete(&self, key: &StorageKey) -> Result<(), StorageError>;
}

#[derive(Debug, Clone)]
pub struct LocalStorage {
    root: PathBuf,
}

impl LocalStorage {
    pub fn new(root: impl Into<PathBuf>) -> Result<Self, StorageError> {
        let root = root.into();
        fs::create_dir_all(&root)?;
        Ok(Self { root })
    }
}

impl StorageBackend for LocalStorage {
    fn put(&self, key: &StorageKey, bytes: &[u8]) -> Result<(), StorageError> {
        let path = key.to_path_under(&self.root)?;
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(path, bytes)?;
        Ok(())
    }

    fn get(&self, key: &StorageKey) -> Result<Vec<u8>, StorageError> {
        let path = key.to_path_under(&self.root)?;
        match fs::read(path) {
            Ok(bytes) => Ok(bytes),
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                Err(StorageError::NotFound(key.clone()))
            }
            Err(err) => Err(StorageError::Io(err)),
        }
    }

    fn exists(&self, key: &StorageKey) -> Result<bool, StorageError> {
        let path = key.to_path_under(&self.root)?;
        Ok(path.is_file())
    }

    fn list_prefix(&self, prefix: &StorageKey) -> Result<Vec<StorageKey>, StorageError> {
        let prefix_path = prefix.to_path_under(&self.root)?;
        if !prefix_path.exists() {
            return Ok(Vec::new());
        }

        let mut out = Vec::new();
        fn walk(dir: &Path, root: &Path, out: &mut Vec<StorageKey>) -> Result<(), StorageError> {
            for entry in fs::read_dir(dir)? {
                let entry = entry?;
                let path = entry.path();
                if path.is_dir() {
                    walk(&path, root, out)?;
                } else if path.is_file() {
                    let rel = path
                        .strip_prefix(root)
                        .map_err(|_| StorageError::InvalidKey("path escaped root".to_string()))?;
                    let rel_str = rel
                        .to_string_lossy()
                        .replace(std::path::MAIN_SEPARATOR, "/");
                    out.push(StorageKey::new(rel_str)?);
                }
            }
            Ok(())
        }

        walk(&prefix_path, &self.root, &mut out)?;
        out.sort();
        Ok(out)
    }

    fn delete(&self, key: &StorageKey) -> Result<(), StorageError> {
        let path = key.to_path_under(&self.root)?;
        match fs::remove_file(path) {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(StorageError::Io(err)),
        }
    }
}

pub fn checkpoint_manifest_pending(run_id: &str, checkpoint_id: &str) -> StorageKey {
    StorageKey::new(format!(
        "runs/{run_id}/checkpoints/{checkpoint_id}/manifest.pending.json"
    ))
    .expect("checkpoint key segments are constant-safe")
}

pub fn checkpoint_worker_artifact(
    run_id: &str,
    checkpoint_id: &str,
    worker_id: &str,
    artifact_name: &str,
) -> StorageKey {
    StorageKey::new(format!(
        "runs/{run_id}/checkpoints/{checkpoint_id}/workers/{worker_id}/{artifact_name}"
    ))
    .expect("checkpoint key segments are constant-safe")
}

pub fn checkpoint_manifest_committed(run_id: &str, checkpoint_id: &str) -> StorageKey {
    StorageKey::new(format!(
        "runs/{run_id}/checkpoints/{checkpoint_id}/manifest.committed.json"
    ))
    .expect("checkpoint key segments are constant-safe")
}

pub fn checkpoint_committed_marker(run_id: &str, checkpoint_id: &str) -> StorageKey {
    StorageKey::new(format!(
        "runs/{run_id}/checkpoints/{checkpoint_id}/COMMITTED"
    ))
    .expect("checkpoint key segments are constant-safe")
}

pub fn latest_pointer(run_id: &str) -> StorageKey {
    StorageKey::new(format!("runs/{run_id}/LATEST"))
        .expect("checkpoint key segments are constant-safe")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn test_dir(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time should be monotonic")
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("runtime-storage-{name}-{nanos}"));
        fs::create_dir_all(&dir).expect("create temp test dir");
        dir
    }

    #[test]
    fn storage_key_rejects_traversal() {
        assert!(StorageKey::new("runs/../etc/passwd").is_err());
    }

    #[test]
    fn storage_key_rejects_absolute() {
        assert!(StorageKey::new("/etc/passwd").is_err());
    }

    #[test]
    fn put_get_exists_delete_roundtrip() {
        let dir = test_dir("roundtrip");
        let storage = LocalStorage::new(&dir).expect("storage init");
        let key =
            StorageKey::new("runs/r1/checkpoints/c1/manifest.pending.json").expect("valid key");

        storage.put(&key, b"hello").expect("put should work");
        assert!(storage.exists(&key).expect("exists should work"));
        assert_eq!(storage.get(&key).expect("get should work"), b"hello");

        storage.delete(&key).expect("delete should work");
        assert!(!storage.exists(&key).expect("exists after delete"));
        storage.delete(&key).expect("delete missing should be ok");

        fs::remove_dir_all(dir).expect("cleanup");
    }

    #[test]
    fn list_prefix_returns_written_keys() {
        let dir = test_dir("list-prefix");
        let storage = LocalStorage::new(&dir).expect("storage init");
        let k1 = StorageKey::new("runs/r1/checkpoints/c1/workers/w1/model.bin").expect("valid key");
        let k2 = StorageKey::new("runs/r1/checkpoints/c1/workers/w2/optim.bin").expect("valid key");
        let k3 =
            StorageKey::new("runs/r2/checkpoints/c2/manifest.pending.json").expect("valid key");

        storage.put(&k1, b"a").expect("put");
        storage.put(&k2, b"b").expect("put");
        storage.put(&k3, b"c").expect("put");

        let prefix = StorageKey::new("runs/r1/checkpoints/c1").expect("valid prefix");
        let keys = storage.list_prefix(&prefix).expect("list prefix");
        assert_eq!(keys, vec![k1, k2]);

        fs::remove_dir_all(dir).expect("cleanup");
    }

    #[test]
    fn checkpoint_helpers_expected_layout() {
        assert_eq!(
            checkpoint_manifest_pending("runA", "cp1").as_str(),
            "runs/runA/checkpoints/cp1/manifest.pending.json"
        );
        assert_eq!(
            checkpoint_worker_artifact("runA", "cp1", "w1", "model.bin").as_str(),
            "runs/runA/checkpoints/cp1/workers/w1/model.bin"
        );
        assert_eq!(
            checkpoint_manifest_committed("runA", "cp1").as_str(),
            "runs/runA/checkpoints/cp1/manifest.committed.json"
        );
        assert_eq!(
            checkpoint_committed_marker("runA", "cp1").as_str(),
            "runs/runA/checkpoints/cp1/COMMITTED"
        );
        assert_eq!(latest_pointer("runA").as_str(), "runs/runA/LATEST");
    }

    #[test]
    fn local_storage_cannot_escape_root_with_malicious_key() {
        let dir = test_dir("escape-root");
        let storage = LocalStorage::new(&dir).expect("storage init");
        let key = StorageKey::new("../../outside");
        assert!(key.is_err());

        let safe = StorageKey::new("runs/r1/file.txt").expect("safe key");
        storage.put(&safe, b"ok").expect("safe put");
        assert_eq!(storage.get(&safe).expect("safe get"), b"ok");

        fs::remove_dir_all(dir).expect("cleanup");
    }

    #[test]
    fn traversal_rejected_before_object_key_conversion() {
        let key = StorageKey::new("runs/../bad");
        assert!(key.is_err());
    }
}
