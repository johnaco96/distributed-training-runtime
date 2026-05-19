#![forbid(unsafe_code)]

//! Dataset metadata and shard prefetch support.

use runtime_checkpoint::{compute_checksum, ChecksumAlgorithm};
use runtime_storage::{StorageBackend, StorageError, StorageKey, StorageOperationPolicy};

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

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DatasetPrefetchResult {
    pub shard_id: String,
    pub storage_key: StorageKey,
    pub result: Result<DatasetPrefetchSuccess, DatasetPrefetchError>,
}

pub fn prefetch_dataset_shards<B: StorageBackend + Sync>(
    storage: &B,
    items: &[DatasetPrefetchItem],
    policy: &StorageOperationPolicy,
) -> Result<Vec<DatasetPrefetchResult>, StorageError> {
    policy.validate()?;
    let max = policy.max_concurrent_operations as usize;

    let mut out = Vec::with_capacity(items.len());
    for chunk in items.chunks(max) {
        std::thread::scope(|scope| {
            let mut handles = Vec::with_capacity(chunk.len());
            for item in chunk {
                handles.push(scope.spawn(|| prefetch_one(storage, item.clone())));
            }
            for handle in handles {
                out.push(handle.join().expect("prefetch worker should not panic"));
            }
        });
    }
    Ok(out)
}

fn prefetch_one<B: StorageBackend>(
    storage: &B,
    item: DatasetPrefetchItem,
) -> DatasetPrefetchResult {
    let result = (|| {
        validate_checksum_metadata(&item)?;

        let bytes = storage
            .get(&item.storage_key)
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
    use std::collections::BTreeMap;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Condvar, Mutex};

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
        assert!(matches!(err, StorageError::InvalidPolicy(_)));
    }

    #[test]
    fn prefetch_respects_concurrency_limit() {
        let storage = TrackingMemoryStorage::with_gate(5);
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

    #[derive(Default, Clone)]
    struct TrackingMemoryStorage {
        entries: Arc<Mutex<BTreeMap<StorageKey, Vec<u8>>>>,
        state: Arc<(Mutex<GateState>, Condvar)>,
        gate_target: usize,
    }

    #[derive(Default)]
    struct GateState {
        active: usize,
        max_seen: usize,
        entered: usize,
    }

    impl TrackingMemoryStorage {
        fn with_gate(gate_target: usize) -> Self {
            Self {
                gate_target,
                ..Self::default()
            }
        }

        fn max_seen(&self) -> usize {
            self.state.0.lock().unwrap().max_seen
        }
    }

    impl StorageBackend for TrackingMemoryStorage {
        fn put(&self, key: &StorageKey, bytes: &[u8]) -> Result<(), StorageError> {
            self.entries
                .lock()
                .unwrap()
                .insert(key.clone(), bytes.to_vec());
            Ok(())
        }

        fn get(&self, key: &StorageKey) -> Result<Vec<u8>, StorageError> {
            static SPIN: AtomicUsize = AtomicUsize::new(0);
            let (lock, cv) = &*self.state;
            let mut st = lock.lock().unwrap();
            st.active += 1;
            st.entered += 1;
            st.max_seen = st.max_seen.max(st.active);
            if self.gate_target > 0 {
                while st.entered < self.gate_target && st.active < self.gate_target {
                    st = cv.wait(st).unwrap();
                }
                cv.notify_all();
            }
            drop(st);
            for _ in 0..1000 {
                let _ = SPIN.fetch_add(1, Ordering::Relaxed);
            }
            let out = self
                .entries
                .lock()
                .unwrap()
                .get(key)
                .cloned()
                .ok_or_else(|| StorageError::NotFound(key.clone()));
            let mut st = lock.lock().unwrap();
            st.active -= 1;
            cv.notify_all();
            out
        }

        fn exists(&self, key: &StorageKey) -> Result<bool, StorageError> {
            Ok(self.entries.lock().unwrap().contains_key(key))
        }

        fn list_prefix(&self, prefix: &StorageKey) -> Result<Vec<StorageKey>, StorageError> {
            Ok(self
                .entries
                .lock()
                .unwrap()
                .keys()
                .filter(|k| k.as_str().starts_with(prefix.as_str()))
                .cloned()
                .collect())
        }

        fn delete(&self, key: &StorageKey) -> Result<(), StorageError> {
            self.entries.lock().unwrap().remove(key);
            Ok(())
        }
    }
}
