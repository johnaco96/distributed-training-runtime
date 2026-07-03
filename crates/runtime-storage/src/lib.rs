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
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

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

/// Storage errors for backend operations.
#[derive(Debug)]
pub enum StorageError {
    Io(io::Error),
    InvalidKey(String),
    NotFound(StorageKey),
    Timeout {
        operation: &'static str,
        timeout: Duration,
    },
    /// A classified backend (e.g. S3) error.  `retryable` distinguishes
    /// transient failures (throttling, 5xx, connection resets) that the retry
    /// loop should re-attempt from permanent ones (auth, validation, missing
    /// bucket) that will never succeed on retry.  `code` carries the
    /// backend's error code (e.g. `AccessDenied`, `SlowDown`) when known.
    Backend {
        operation: &'static str,
        retryable: bool,
        code: Option<String>,
        message: String,
    },
}

impl Display for StorageError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageError::Io(err) => write!(f, "io error: {err}"),
            StorageError::InvalidKey(msg) => write!(f, "invalid key: {msg}"),
            StorageError::NotFound(key) => write!(f, "key not found: {key}"),
            StorageError::Timeout { operation, timeout } => {
                write!(
                    f,
                    "storage operation '{operation}' timed out after {timeout:?}"
                )
            }
            StorageError::Backend {
                operation,
                retryable,
                code,
                message,
            } => write!(
                f,
                "backend error in '{operation}' ({}{}): {message}",
                if *retryable { "retryable" } else { "permanent" },
                code.as_ref()
                    .map(|c| format!(", code={c}"))
                    .unwrap_or_default(),
            ),
        }
    }
}

impl Error for StorageError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            StorageError::Io(err) => Some(err),
            StorageError::InvalidKey(_)
            | StorageError::NotFound(_)
            | StorageError::Timeout { .. }
            | StorageError::Backend { .. } => None,
        }
    }
}

impl From<io::Error> for StorageError {
    fn from(value: io::Error) -> Self {
        StorageError::Io(value)
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct StorageOperationPolicy {
    pub operation_timeout: Duration,
    pub max_retries: u32,
    pub retry_initial_backoff: Duration,
    pub retry_max_backoff: Duration,
    pub max_concurrent_operations: usize,
}

impl Default for StorageOperationPolicy {
    fn default() -> Self {
        Self {
            operation_timeout: Duration::from_secs(30),
            max_retries: 3,
            retry_initial_backoff: Duration::from_millis(100),
            retry_max_backoff: Duration::from_secs(2),
            // 64 concurrent S3 operations allows ~64 simultaneous workers before
            // throttling kicks in. The old default of 16 was a hard ceiling that
            // collapsed throughput at >16 workers on S3-backed deployments.
            // Override at runtime via --max-s3-concurrent-ops <N>.
            max_concurrent_operations: 64,
        }
    }
}

impl StorageOperationPolicy {
    pub fn validate(&self) -> Result<(), StorageError> {
        if self.max_concurrent_operations == 0 {
            return Err(StorageError::InvalidKey(
                "max_concurrent_operations must be greater than zero".to_string(),
            ));
        }
        Ok(())
    }
}

pub fn is_retryable_storage_error(err: &StorageError) -> bool {
    match err {
        StorageError::InvalidKey(_) | StorageError::NotFound(_) => false,
        StorageError::Io(_) | StorageError::Timeout { .. } => true,
        // Honour the backend's classification: permanent errors (auth,
        // validation, missing bucket) must NOT be retried, or every such
        // failure pays the full retry budget in latency for nothing.
        StorageError::Backend { retryable, .. } => *retryable,
    }
}

/// Exponential backoff for a retry attempt, capped at `retry_max_backoff`.
///
/// Deterministic; see [`retry_backoff_with_jitter`] for the production form
/// that adds full jitter to avoid synchronised retries (thundering herd).
pub fn retry_backoff_for_attempt(
    policy: &StorageOperationPolicy,
    retry_attempt_index: u32,
) -> Duration {
    let multiplier = 1_u32.checked_shl(retry_attempt_index.min(31)).unwrap_or(0);
    let backoff = policy.retry_initial_backoff.saturating_mul(multiplier);
    std::cmp::min(backoff, policy.retry_max_backoff)
}

/// Exponential backoff with **full jitter**: a uniformly random delay in
/// `[0, exponential_backoff]`.
///
/// Full jitter (AWS's recommended form) de-correlates retries across many
/// workers that failed at the same instant — critical when a throttling event
/// or a coordinator blip makes hundreds of workers retry together.  `rand01`
/// is a caller-supplied uniform sample in `[0, 1)` so this stays pure and
/// unit-testable.
pub fn retry_backoff_with_jitter(
    policy: &StorageOperationPolicy,
    retry_attempt_index: u32,
    rand01: f64,
) -> Duration {
    let ceiling = retry_backoff_for_attempt(policy, retry_attempt_index);
    let frac = rand01.clamp(0.0, 1.0);
    ceiling.mul_f64(frac)
}

#[cfg(feature = "s3")]
pub async fn run_with_timeout_retry<F, Fut, T>(
    policy: &StorageOperationPolicy,
    operation_name: &'static str,
    mut operation: F,
) -> Result<T, StorageError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, StorageError>>,
{
    let mut attempt = 0_u32;
    loop {
        let call = tokio::time::timeout(policy.operation_timeout, operation()).await;
        let result = match call {
            Ok(result) => result,
            Err(_) => Err(StorageError::Timeout {
                operation: operation_name,
                timeout: policy.operation_timeout,
            }),
        };
        match result {
            Ok(value) => return Ok(value),
            Err(err) => {
                if attempt >= policy.max_retries || !is_retryable_storage_error(&err) {
                    return Err(err);
                }
                // Full jitter so a throttling event does not resynchronise all
                // retrying workers.  A cheap time-seeded sample keeps the
                // storage crate dependency-free; determinism is only needed in
                // the pure unit-tested helper.
                let rand01 = jitter_sample();
                let delay = retry_backoff_with_jitter(policy, attempt, rand01);
                tokio::time::sleep(delay).await;
                attempt += 1;
            }
        }
    }
}

/// Cheap, dependency-free uniform sample in `[0, 1)` for retry jitter, seeded
/// from the wall clock and thread id.  Not cryptographic; only used to
/// de-correlate retry timing across workers.
#[cfg(feature = "s3")]
fn jitter_sample() -> f64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .hash(&mut hasher);
    std::thread::current().id().hash(&mut hasher);
    // Top 53 bits → uniform double in [0, 1).
    (hasher.finish() >> 11) as f64 / (1u64 << 53) as f64
}

pub trait StorageBackend {
    fn put(&self, key: &StorageKey, bytes: &[u8]) -> Result<(), StorageError>;
    fn get(&self, key: &StorageKey) -> Result<Vec<u8>, StorageError>;
    fn exists(&self, key: &StorageKey) -> Result<bool, StorageError>;
    fn list_prefix(&self, prefix: &StorageKey) -> Result<Vec<StorageKey>, StorageError>;
    fn delete(&self, key: &StorageKey) -> Result<(), StorageError>;
}

pub type PutManyResult = Result<Vec<(StorageKey, Result<(), StorageError>)>, StorageError>;
pub type GetManyResult = Result<Vec<(StorageKey, Result<Vec<u8>, StorageError>)>, StorageError>;
pub type ExistsManyResult = Result<Vec<(StorageKey, Result<bool, StorageError>)>, StorageError>;

pub fn put_many<B>(
    backend: &B,
    items: Vec<(StorageKey, Vec<u8>)>,
    policy: StorageOperationPolicy,
) -> PutManyResult
where
    B: StorageBackend + Sync,
{
    policy.validate()?;
    let shared = Arc::new(Mutex::new(
        items
            .into_iter()
            .map(|(key, bytes)| (key, Arc::new(bytes)))
            .collect::<Vec<_>>(),
    ));
    let out = Arc::new(Mutex::new(Vec::new()));
    thread::scope(|scope| {
        for _ in 0..policy.max_concurrent_operations {
            let shared = Arc::clone(&shared);
            let out = Arc::clone(&out);
            scope.spawn(move || loop {
                let item = shared.lock().expect("work lock").pop();
                let Some((key, bytes)) = item else { break };
                let result = backend.put(&key, bytes.as_slice());
                out.lock().expect("result lock").push((key, result));
            });
        }
    });
    let results = {
        let mut guard = out.lock().expect("result lock");
        guard.drain(..).collect()
    };

    Ok(results)
}

pub fn get_many<B>(
    backend: &B,
    keys: Vec<StorageKey>,
    policy: StorageOperationPolicy,
) -> GetManyResult
where
    B: StorageBackend + Sync,
{
    policy.validate()?;
    let shared = Arc::new(Mutex::new(keys));
    let out = Arc::new(Mutex::new(Vec::new()));
    thread::scope(|scope| {
        for _ in 0..policy.max_concurrent_operations {
            let shared = Arc::clone(&shared);
            let out = Arc::clone(&out);
            scope.spawn(move || loop {
                let item = shared.lock().expect("work lock").pop();
                let Some(key) = item else { break };
                let result = backend.get(&key);
                out.lock().expect("result lock").push((key, result));
            });
        }
    });
    let results = {
        let mut guard = out.lock().expect("result lock");
        guard.drain(..).collect()
    };

    Ok(results)
}

pub fn exists_many<B>(
    backend: &B,
    keys: Vec<StorageKey>,
    policy: StorageOperationPolicy,
) -> ExistsManyResult
where
    B: StorageBackend + Sync,
{
    policy.validate()?;
    let shared = Arc::new(Mutex::new(keys));
    let out = Arc::new(Mutex::new(Vec::new()));
    thread::scope(|scope| {
        for _ in 0..policy.max_concurrent_operations {
            let shared = Arc::clone(&shared);
            let out = Arc::clone(&out);
            scope.spawn(move || loop {
                let item = shared.lock().expect("work lock").pop();
                let Some(key) = item else { break };
                let result = backend.exists(&key);
                out.lock().expect("result lock").push((key, result));
            });
        }
    });
    let results = {
        let mut guard = out.lock().expect("result lock");
        guard.drain(..).collect()
    };

    Ok(results)
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
    #[cfg(feature = "s3")]
    use std::sync::atomic::{AtomicUsize, Ordering};
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

    #[test]
    fn default_policy_values_are_sane() {
        let policy = StorageOperationPolicy::default();
        assert_eq!(policy.operation_timeout, Duration::from_secs(30));
        assert_eq!(policy.max_retries, 3);
        assert_eq!(policy.retry_initial_backoff, Duration::from_millis(100));
        assert_eq!(policy.retry_max_backoff, Duration::from_secs(2));
        assert_eq!(policy.max_concurrent_operations, 64);
    }

    #[test]
    fn policy_rejects_zero_max_concurrency() {
        let policy = StorageOperationPolicy {
            max_concurrent_operations: 0,
            ..Default::default()
        };
        assert!(policy.validate().is_err());
    }

    #[test]
    fn backoff_is_capped() {
        let p = StorageOperationPolicy::default();
        assert_eq!(retry_backoff_for_attempt(&p, 0), Duration::from_millis(100));
        assert_eq!(retry_backoff_for_attempt(&p, 1), Duration::from_millis(200));
        assert_eq!(retry_backoff_for_attempt(&p, 8), Duration::from_secs(2));
    }

    #[cfg(feature = "s3")]
    #[tokio::test]
    async fn retry_helper_succeeds_after_transient_failures() {
        let policy = StorageOperationPolicy {
            operation_timeout: Duration::from_millis(50),
            max_retries: 3,
            retry_initial_backoff: Duration::from_millis(1),
            retry_max_backoff: Duration::from_millis(2),
            max_concurrent_operations: 2,
        };
        let tries = Arc::new(AtomicUsize::new(0));
        let value = run_with_timeout_retry(&policy, "retry-ok", {
            let tries = tries.clone();
            move || {
                let tries = tries.clone();
                async move {
                    let n = tries.fetch_add(1, Ordering::SeqCst);
                    if n < 2 {
                        Err(StorageError::Io(io::Error::other("transient")))
                    } else {
                        Ok(7usize)
                    }
                }
            }
        })
        .await
        .expect("eventually succeeds");
        assert_eq!(value, 7);
        assert_eq!(tries.load(Ordering::SeqCst), 3);
    }

    #[cfg(feature = "s3")]
    #[tokio::test]
    async fn retry_helper_stops_after_max_retries() {
        let policy = StorageOperationPolicy {
            max_retries: 2,
            retry_initial_backoff: Duration::from_millis(1),
            retry_max_backoff: Duration::from_millis(2),
            ..Default::default()
        };
        let tries = Arc::new(AtomicUsize::new(0));
        let err = run_with_timeout_retry(&policy, "retry-fail", {
            let tries = tries.clone();
            move || {
                let tries = tries.clone();
                async move {
                    tries.fetch_add(1, Ordering::SeqCst);
                    Err::<(), _>(StorageError::Io(io::Error::other("always fails")))
                }
            }
        })
        .await
        .expect_err("should fail");
        assert!(matches!(err, StorageError::Io(_)));
        assert_eq!(tries.load(Ordering::SeqCst), 3);
    }

    #[cfg(feature = "s3")]
    #[tokio::test]
    async fn retry_helper_does_not_retry_invalid_key_errors() {
        let policy = StorageOperationPolicy::default();
        let tries = Arc::new(AtomicUsize::new(0));
        let err = run_with_timeout_retry(&policy, "invalid-key", {
            let tries = tries.clone();
            move || {
                let tries = tries.clone();
                async move {
                    tries.fetch_add(1, Ordering::SeqCst);
                    Err::<(), _>(StorageError::InvalidKey("bad".into()))
                }
            }
        })
        .await
        .expect_err("should fail");
        assert!(matches!(err, StorageError::InvalidKey(_)));
        assert_eq!(tries.load(Ordering::SeqCst), 1);
    }

    #[cfg(feature = "s3")]
    #[tokio::test]
    async fn timeout_helper_returns_timeout_error() {
        let policy = StorageOperationPolicy {
            operation_timeout: Duration::from_millis(10),
            max_retries: 0,
            ..Default::default()
        };
        let err = run_with_timeout_retry(&policy, "slow-op", || async {
            tokio::time::sleep(Duration::from_millis(30)).await;
            Ok::<(), StorageError>(())
        })
        .await
        .expect_err("should timeout");
        assert!(matches!(err, StorageError::Timeout { .. }));
    }

    #[test]
    fn batch_helpers_put_get_exists_roundtrip() {
        let dir = test_dir("batch-roundtrip");
        let storage = LocalStorage::new(&dir).expect("storage");
        let policy = StorageOperationPolicy {
            max_concurrent_operations: 2,
            ..Default::default()
        };
        let items = vec![
            (StorageKey::new("runs/r1/a.bin").unwrap(), b"a".to_vec()),
            (StorageKey::new("runs/r1/b.bin").unwrap(), b"bb".to_vec()),
        ];
        let put_results = put_many(&storage, items.clone(), policy).expect("put_many");
        assert!(put_results.iter().all(|(_, r)| r.is_ok()));

        let keys = items.iter().map(|(k, _)| k.clone()).collect::<Vec<_>>();
        let mut get_results = get_many(&storage, keys.clone(), policy).expect("get_many");
        get_results.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(get_results[0].1.as_ref().unwrap(), b"a");
        assert_eq!(get_results[1].1.as_ref().unwrap(), b"bb");

        let mut exists_results = exists_many(&storage, keys, policy).expect("exists_many");
        exists_results.sort_by(|a, b| a.0.cmp(&b.0));
        assert!(exists_results.iter().all(|(_, r)| matches!(r, Ok(true))));
        fs::remove_dir_all(dir).expect("cleanup");
    }

    #[test]
    fn batch_helpers_report_per_item_failures() {
        let dir = test_dir("batch-failures");
        let storage = LocalStorage::new(&dir).expect("storage");
        let policy = StorageOperationPolicy::default();
        let keys = vec![
            StorageKey::new("runs/r1/missing.bin").unwrap(),
            StorageKey::new("runs/r1/existing.bin").unwrap(),
        ];
        storage.put(&keys[1], b"ok").expect("seed");
        let results = get_many(&storage, keys, policy).expect("get_many");
        assert!(results
            .iter()
            .any(|(_, r)| matches!(r, Err(StorageError::NotFound(_)))));
        assert!(results
            .iter()
            .any(|(_, r)| matches!(r, Ok(bytes) if bytes == b"ok")));
        fs::remove_dir_all(dir).expect("cleanup");
    }

    #[test]
    fn batch_helpers_reject_zero_concurrency_policy() {
        let dir = test_dir("batch-invalid-policy");
        let storage = LocalStorage::new(&dir).expect("storage");
        let bad_policy = StorageOperationPolicy {
            max_concurrent_operations: 0,
            ..Default::default()
        };
        let err = exists_many(
            &storage,
            vec![StorageKey::new("runs/r1/any.bin").unwrap()],
            bad_policy,
        )
        .expect_err("invalid policy");
        assert!(matches!(err, StorageError::InvalidKey(_)));
        fs::remove_dir_all(dir).expect("cleanup");
    }

    #[test]
    fn backend_errors_honour_the_retryable_flag() {
        let permanent = StorageError::Backend {
            operation: "s3-put",
            retryable: false,
            code: Some("AccessDenied".into()),
            message: "denied".into(),
        };
        let transient = StorageError::Backend {
            operation: "s3-put",
            retryable: true,
            code: Some("SlowDown".into()),
            message: "slow".into(),
        };
        assert!(!is_retryable_storage_error(&permanent));
        assert!(is_retryable_storage_error(&transient));
    }

    #[test]
    fn jitter_backoff_stays_within_exponential_ceiling() {
        let policy = StorageOperationPolicy::default();
        for attempt in 0..5 {
            let ceiling = retry_backoff_for_attempt(&policy, attempt);
            // Full range of jitter fractions never exceeds the ceiling and
            // reaches (nearly) zero at the low end.
            assert_eq!(
                retry_backoff_with_jitter(&policy, attempt, 0.0),
                Duration::ZERO
            );
            assert_eq!(retry_backoff_with_jitter(&policy, attempt, 1.0), ceiling);
            let mid = retry_backoff_with_jitter(&policy, attempt, 0.5);
            assert!(mid <= ceiling && mid >= Duration::ZERO);
        }
        // Out-of-range fractions are clamped.
        let ceiling = retry_backoff_for_attempt(&policy, 2);
        assert_eq!(retry_backoff_with_jitter(&policy, 2, 5.0), ceiling);
        assert_eq!(retry_backoff_with_jitter(&policy, 2, -1.0), Duration::ZERO);
    }
}
