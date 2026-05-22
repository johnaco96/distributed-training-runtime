#![forbid(unsafe_code)]

//! Dataset metadata and shard prefetch support.

use runtime_checkpoint::{compute_checksum, ChecksumAlgorithm};
use runtime_storage::{
    is_retryable_storage_error, retry_backoff_for_attempt, StorageBackend, StorageError,
    StorageKey, StorageOperationPolicy,
};
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DatasetPrefetchItem {
    pub shard_id: String,
    pub storage_key: StorageKey,
    pub expected_size_bytes: Option<u64>,
    pub checksum_algorithm: Option<String>,
    pub checksum: Option<String>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DatasetPrefetchSuccess {
    pub shard_id: String,
    pub storage_key: StorageKey,
    pub bytes_read: u64,
}

#[derive(Debug)]
pub enum DatasetPrefetchError {
    Storage(StorageError),
    InvalidChecksumMetadata,
    SizeMismatch {
        expected: u64,
        actual: u64,
    },
    UnsupportedChecksumAlgorithm {
        algorithm: String,
    },
    ChecksumMismatch {
        algorithm: String,
        expected: String,
        actual: String,
    },
}

#[derive(Debug)]
pub struct DatasetPrefetchResult {
    pub shard_id: String,
    pub storage_key: StorageKey,
    pub result: Result<DatasetPrefetchSuccess, DatasetPrefetchError>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DatasetPrefetchSummary {
    pub requested_shards: usize,
    pub successful_shards: usize,
    pub failed_shards: usize,
    pub bytes_read: u64,
    pub duration: Duration,
}

#[derive(Debug)]
pub struct DatasetPrefetchBatchResult {
    pub results: Vec<DatasetPrefetchResult>,
    pub summary: DatasetPrefetchSummary,
}

pub fn prefetch_dataset_shards<B: StorageBackend + Sync>(
    storage: &B,
    items: &[DatasetPrefetchItem],
    policy: &StorageOperationPolicy,
) -> Result<Vec<DatasetPrefetchResult>, StorageError> {
    Ok(prefetch_dataset_shards_with_summary(storage, items, policy)?.results)
}

pub fn prefetch_dataset_shards_with_summary<B: StorageBackend + Sync>(
    storage: &B,
    items: &[DatasetPrefetchItem],
    policy: &StorageOperationPolicy,
) -> Result<DatasetPrefetchBatchResult, StorageError> {
    policy.validate()?;
    let started = Instant::now();
    let max = policy.max_concurrent_operations;

    let mut results = Vec::with_capacity(items.len());
    for chunk in items.chunks(max) {
        std::thread::scope(|scope| {
            let mut handles = Vec::with_capacity(chunk.len());
            for item in chunk {
                handles.push(scope.spawn(|| prefetch_one(storage, item.clone(), policy)));
            }
            for handle in handles {
                results.push(handle.join().expect("prefetch worker should not panic"));
            }
        });
    }

    let successful_shards = results.iter().filter(|r| r.result.is_ok()).count();
    let bytes_read = results
        .iter()
        .filter_map(|r| r.result.as_ref().ok().map(|ok| ok.bytes_read))
        .sum::<u64>();
    let requested_shards = results.len();
    Ok(DatasetPrefetchBatchResult {
        results,
        summary: DatasetPrefetchSummary {
            requested_shards,
            successful_shards,
            failed_shards: requested_shards.saturating_sub(successful_shards),
            bytes_read,
            duration: started.elapsed(),
        },
    })
}

fn prefetch_one<B: StorageBackend>(
    storage: &B,
    item: DatasetPrefetchItem,
    policy: &StorageOperationPolicy,
) -> DatasetPrefetchResult {
    let result = (|| {
        validate_checksum_metadata(&item)?;
        let bytes = get_with_retry(storage, &item.storage_key, policy)
            .map_err(DatasetPrefetchError::Storage)?;
        let actual_size = bytes.len() as u64;

        if let Some(expected_size) = item.expected_size_bytes {
            if expected_size != actual_size {
                return Err(DatasetPrefetchError::SizeMismatch {
                    expected: expected_size,
                    actual: actual_size,
                });
            }
        }

        if let (Some(algorithm), Some(expected_checksum)) =
            (&item.checksum_algorithm, &item.checksum)
        {
            let algorithm = ChecksumAlgorithm::parse(algorithm).map_err(|_| {
                DatasetPrefetchError::UnsupportedChecksumAlgorithm {
                    algorithm: algorithm.clone(),
                }
            })?;
            let actual_checksum = compute_checksum(algorithm, &bytes);
            if actual_checksum != *expected_checksum {
                return Err(DatasetPrefetchError::ChecksumMismatch {
                    algorithm: algorithm.as_str().to_string(),
                    expected: expected_checksum.clone(),
                    actual: actual_checksum,
                });
            }
        }

        Ok(DatasetPrefetchSuccess {
            shard_id: item.shard_id.clone(),
            storage_key: item.storage_key.clone(),
            bytes_read: actual_size,
        })
    })();

    DatasetPrefetchResult {
        shard_id: item.shard_id,
        storage_key: item.storage_key,
        result,
    }
}

fn get_with_retry<B: StorageBackend>(
    storage: &B,
    key: &StorageKey,
    policy: &StorageOperationPolicy,
) -> Result<Vec<u8>, StorageError> {
    let mut attempt = 0_u32;
    loop {
        match storage.get(key) {
            Ok(bytes) => return Ok(bytes),
            Err(err) => {
                if attempt >= policy.max_retries || !is_retryable_storage_error(&err) {
                    return Err(err);
                }
                std::thread::sleep(retry_backoff_for_attempt(policy, attempt));
                attempt += 1;
            }
        }
    }
}

fn validate_checksum_metadata(item: &DatasetPrefetchItem) -> Result<(), DatasetPrefetchError> {
    match (&item.checksum_algorithm, &item.checksum) {
        (None, None) | (Some(_), Some(_)) => Ok(()),
        _ => Err(DatasetPrefetchError::InvalidChecksumMetadata),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use runtime_checkpoint::ChecksumAlgorithm;
    use runtime_storage::LocalStorage;
    use runtime_storage::StorageError as SE;
    use std::collections::BTreeMap;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, RwLock};
    use std::time::Duration;

    fn temp_dir(name: &str) -> PathBuf {
        let mut dir = std::env::temp_dir();
        let unique = format!("runtime_dataset_prefetch_{name}_{}", std::process::id());
        dir.push(unique);
        if dir.exists() {
            std::fs::remove_dir_all(&dir).unwrap();
        }
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn prefetch_reads_all_requested_shards() {
        let root = temp_dir("all");
        let storage = LocalStorage::new(root).unwrap();
        let key_a = StorageKey::new("datasets/run-a/shard-1.bin").unwrap();
        let key_b = StorageKey::new("datasets/run-a/shard-2.bin").unwrap();
        storage.put(&key_a, b"abc").unwrap();
        storage.put(&key_b, b"defgh").unwrap();

        let items = vec![
            DatasetPrefetchItem {
                shard_id: "s1".into(),
                storage_key: key_a,
                expected_size_bytes: None,
                checksum_algorithm: None,
                checksum: None,
            },
            DatasetPrefetchItem {
                shard_id: "s2".into(),
                storage_key: key_b,
                expected_size_bytes: None,
                checksum_algorithm: None,
                checksum: None,
            },
        ];
        let out =
            prefetch_dataset_shards(&storage, &items, &StorageOperationPolicy::default()).unwrap();
        assert_eq!(out.len(), 2);
        assert!(out.iter().all(|r| r.result.is_ok()));
    }

    #[test]
    fn prefetch_reports_missing_shard_without_panicking() {
        let root = temp_dir("missing");
        let storage = LocalStorage::new(root).unwrap();
        let key = StorageKey::new("datasets/run-a/missing.bin").unwrap();
        let items = vec![DatasetPrefetchItem {
            shard_id: "s1".into(),
            storage_key: key.clone(),
            expected_size_bytes: None,
            checksum_algorithm: None,
            checksum: None,
        }];
        let out =
            prefetch_dataset_shards(&storage, &items, &StorageOperationPolicy::default()).unwrap();
        assert_eq!(out.len(), 1);
        assert!(
            matches!(out[0].result, Err(DatasetPrefetchError::Storage(StorageError::NotFound(ref k))) if *k == key)
        );
    }

    #[test]
    fn prefetch_validates_expected_size() {
        let root = temp_dir("size_ok");
        let storage = LocalStorage::new(root).unwrap();
        let key = StorageKey::new("datasets/run-a/s.bin").unwrap();
        storage.put(&key, b"1234").unwrap();
        let items = vec![DatasetPrefetchItem {
            shard_id: "s".into(),
            storage_key: key,
            expected_size_bytes: Some(4),
            checksum_algorithm: None,
            checksum: None,
        }];
        let out =
            prefetch_dataset_shards(&storage, &items, &StorageOperationPolicy::default()).unwrap();
        assert!(out[0].result.is_ok());
    }

    #[test]
    fn prefetch_rejects_size_mismatch() {
        let root = temp_dir("size_bad");
        let storage = LocalStorage::new(root).unwrap();
        let key = StorageKey::new("datasets/run-a/s.bin").unwrap();
        storage.put(&key, b"1234").unwrap();
        let items = vec![DatasetPrefetchItem {
            shard_id: "s".into(),
            storage_key: key,
            expected_size_bytes: Some(5),
            checksum_algorithm: None,
            checksum: None,
        }];
        let out =
            prefetch_dataset_shards(&storage, &items, &StorageOperationPolicy::default()).unwrap();
        assert!(matches!(
            out[0].result,
            Err(DatasetPrefetchError::SizeMismatch {
                expected: 5,
                actual: 4
            })
        ));
    }

    #[test]
    fn prefetch_validates_checksum_when_provided() {
        let root = temp_dir("checksum_ok");
        let storage = LocalStorage::new(root).unwrap();
        let key = StorageKey::new("datasets/run-a/s.bin").unwrap();
        let bytes = b"hello";
        storage.put(&key, bytes).unwrap();
        let digest = compute_checksum(ChecksumAlgorithm::Sha256, bytes);
        let items = vec![DatasetPrefetchItem {
            shard_id: "s".into(),
            storage_key: key,
            expected_size_bytes: None,
            checksum_algorithm: Some("sha256".into()),
            checksum: Some(digest),
        }];
        let out =
            prefetch_dataset_shards(&storage, &items, &StorageOperationPolicy::default()).unwrap();
        assert!(out[0].result.is_ok());
    }

    #[test]
    fn prefetch_rejects_checksum_mismatch() {
        let root = temp_dir("checksum_bad");
        let storage = LocalStorage::new(root).unwrap();
        let key = StorageKey::new("datasets/run-a/s.bin").unwrap();
        storage.put(&key, b"hello").unwrap();
        let items = vec![DatasetPrefetchItem {
            shard_id: "s".into(),
            storage_key: key,
            expected_size_bytes: None,
            checksum_algorithm: Some("sha256".into()),
            checksum: Some("deadbeef".into()),
        }];
        let out =
            prefetch_dataset_shards(&storage, &items, &StorageOperationPolicy::default()).unwrap();
        assert!(matches!(
            out[0].result,
            Err(DatasetPrefetchError::ChecksumMismatch { .. })
        ));
    }

    #[test]
    fn prefetch_rejects_incomplete_checksum_metadata() {
        let root = temp_dir("checksum_meta");
        let storage = LocalStorage::new(root).unwrap();
        let key = StorageKey::new("datasets/run-a/s.bin").unwrap();
        storage.put(&key, b"hello").unwrap();
        let items = vec![DatasetPrefetchItem {
            shard_id: "s".into(),
            storage_key: key,
            expected_size_bytes: None,
            checksum_algorithm: Some("sha256".into()),
            checksum: None,
        }];
        let out =
            prefetch_dataset_shards(&storage, &items, &StorageOperationPolicy::default()).unwrap();
        assert!(matches!(
            out[0].result,
            Err(DatasetPrefetchError::InvalidChecksumMetadata)
        ));
    }

    #[test]
    fn prefetch_rejects_zero_concurrency_policy() {
        let storage = TrackingMemoryStorage::default();
        let key = StorageKey::new("datasets/run-a/s.bin").unwrap();
        storage.put(&key, b"x").unwrap();
        let items = vec![DatasetPrefetchItem {
            shard_id: "s".into(),
            storage_key: key,
            expected_size_bytes: None,
            checksum_algorithm: None,
            checksum: None,
        }];
        let policy = StorageOperationPolicy {
            max_concurrent_operations: 0,
            ..StorageOperationPolicy::default()
        };
        let err = prefetch_dataset_shards(&storage, &items, &policy).unwrap_err();
        assert!(matches!(err, StorageError::InvalidKey(_)));
    }

    #[test]
    fn prefetch_respects_concurrency_limit() {
        let storage = TrackingMemoryStorage::new_with_delay(Duration::from_millis(10));
        let mut items = Vec::new();
        for i in 0..5 {
            let key = StorageKey::new(format!("datasets/run-a/s{i}.bin")).unwrap();
            storage.put(&key, b"data").unwrap();
            items.push(DatasetPrefetchItem {
                shard_id: format!("s{i}"),
                storage_key: key,
                expected_size_bytes: None,
                checksum_algorithm: None,
                checksum: None,
            });
        }
        let policy = StorageOperationPolicy {
            max_concurrent_operations: 2,
            ..StorageOperationPolicy::default()
        };
        let out = prefetch_dataset_shards(&storage, &items, &policy).unwrap();
        assert_eq!(out.len(), 5);
        assert!(out.iter().all(|r| r.result.is_ok()));
        assert!(storage.max_seen() <= 2, "max seen {}", storage.max_seen());
    }

    struct FlakyStorage {
        failures_left: AtomicUsize,
        bytes: Vec<u8>,
    }

    impl runtime_storage::StorageBackend for FlakyStorage {
        fn get(&self, _key: &StorageKey) -> Result<Vec<u8>, SE> {
            let prev = self.failures_left.fetch_sub(1, Ordering::SeqCst);
            if prev > 0 {
                return Err(SE::Io(std::io::Error::other("transient")));
            }
            Ok(self.bytes.clone())
        }
        fn put(&self, _key: &StorageKey, _bytes: &[u8]) -> Result<(), SE> {
            Ok(())
        }
        fn delete(&self, _key: &StorageKey) -> Result<(), SE> {
            Ok(())
        }
        fn exists(&self, _key: &StorageKey) -> Result<bool, SE> {
            Ok(true)
        }
        fn list_prefix(&self, _prefix: &StorageKey) -> Result<Vec<StorageKey>, SE> {
            Ok(vec![])
        }
    }

    #[test]
    fn prefetch_retries_transient_error_then_succeeds() {
        let storage = FlakyStorage {
            failures_left: AtomicUsize::new(1),
            bytes: b"abc".to_vec(),
        };
        let item = DatasetPrefetchItem {
            shard_id: "s1".into(),
            storage_key: StorageKey::new("datasets/r/s1").unwrap(),
            expected_size_bytes: Some(3),
            checksum_algorithm: None,
            checksum: None,
        };
        let out =
            prefetch_dataset_shards(&storage, &[item], &StorageOperationPolicy::default()).unwrap();
        assert!(out[0].result.is_ok());
    }

    #[test]
    fn prefetch_retry_limit_fails() {
        let storage = FlakyStorage {
            failures_left: AtomicUsize::new(10),
            bytes: b"abc".to_vec(),
        };
        let policy = StorageOperationPolicy {
            max_retries: 1,
            ..StorageOperationPolicy::default()
        };
        let item = DatasetPrefetchItem {
            shard_id: "s1".into(),
            storage_key: StorageKey::new("datasets/r/s1").unwrap(),
            expected_size_bytes: None,
            checksum_algorithm: None,
            checksum: None,
        };
        let out = prefetch_dataset_shards(&storage, &[item], &policy).unwrap();
        assert!(matches!(
            out[0].result,
            Err(DatasetPrefetchError::Storage(SE::Io(_)))
        ));
    }

    #[test]
    fn prefetch_summary_metrics_are_reported() {
        let root = temp_dir("summary");
        let storage = LocalStorage::new(root).unwrap();
        let key_ok = StorageKey::new("datasets/run-a/ok.bin").unwrap();
        storage.put(&key_ok, b"abcd").unwrap();
        let key_miss = StorageKey::new("datasets/run-a/missing.bin").unwrap();
        let items = vec![
            DatasetPrefetchItem {
                shard_id: "ok".into(),
                storage_key: key_ok,
                expected_size_bytes: None,
                checksum_algorithm: None,
                checksum: None,
            },
            DatasetPrefetchItem {
                shard_id: "missing".into(),
                storage_key: key_miss,
                expected_size_bytes: None,
                checksum_algorithm: None,
                checksum: None,
            },
        ];
        let out = prefetch_dataset_shards_with_summary(
            &storage,
            &items,
            &StorageOperationPolicy::default(),
        )
        .unwrap();
        assert_eq!(out.summary.requested_shards, 2);
        assert_eq!(out.summary.successful_shards, 1);
        assert_eq!(out.summary.failed_shards, 1);
        assert_eq!(out.summary.bytes_read, 4);
    }

    #[derive(Default, Clone)]
    struct TrackingMemoryStorage {
        entries: Arc<RwLock<BTreeMap<StorageKey, Vec<u8>>>>,
        in_flight: Arc<AtomicUsize>,
        max_seen: Arc<AtomicUsize>,
        get_delay: Duration,
    }

    struct InFlightGuard<'a> {
        in_flight: &'a AtomicUsize,
    }

    impl Drop for InFlightGuard<'_> {
        fn drop(&mut self) {
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
        }
    }

    impl TrackingMemoryStorage {
        fn new_with_delay(get_delay: Duration) -> Self {
            Self {
                get_delay,
                ..Self::default()
            }
        }

        fn max_seen(&self) -> usize {
            self.max_seen.load(Ordering::SeqCst)
        }
    }

    impl StorageBackend for TrackingMemoryStorage {
        fn put(&self, key: &StorageKey, bytes: &[u8]) -> Result<(), StorageError> {
            self.entries
                .write()
                .unwrap()
                .insert(key.clone(), bytes.to_vec());
            Ok(())
        }

        fn get(&self, key: &StorageKey) -> Result<Vec<u8>, StorageError> {
            let now_in_flight = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            let _guard = InFlightGuard {
                in_flight: &self.in_flight,
            };
            let mut previous = self.max_seen.load(Ordering::SeqCst);
            while now_in_flight > previous
                && self
                    .max_seen
                    .compare_exchange(previous, now_in_flight, Ordering::SeqCst, Ordering::SeqCst)
                    .is_err()
            {
                previous = self.max_seen.load(Ordering::SeqCst);
            }
            std::thread::sleep(self.get_delay);
            let out = self
                .entries
                .read()
                .unwrap()
                .get(key)
                .cloned()
                .ok_or_else(|| StorageError::NotFound(key.clone()));
            out
        }

        fn exists(&self, key: &StorageKey) -> Result<bool, StorageError> {
            Ok(self.entries.read().unwrap().contains_key(key))
        }

        fn list_prefix(&self, prefix: &StorageKey) -> Result<Vec<StorageKey>, StorageError> {
            Ok(self
                .entries
                .read()
                .unwrap()
                .keys()
                .filter(|k| k.as_str().starts_with(prefix.as_str()))
                .cloned()
                .collect())
        }

        fn delete(&self, key: &StorageKey) -> Result<(), StorageError> {
            self.entries.write().unwrap().remove(key);
            Ok(())
        }
    }
}
