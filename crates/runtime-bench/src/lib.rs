#![forbid(unsafe_code)]

use runtime_checkpoint::{
    upload_checkpoint_artifacts_concurrently, CheckpointArtifactUpload, CheckpointId,
    CheckpointRecord, CheckpointStore, ChecksumAlgorithm, RunId, WorkerId,
};
use runtime_storage::{LocalStorage, StorageBackend, StorageKey, StorageOperationPolicy};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkSample {
    pub latency_ms: f64,
    pub bytes: u64,
    pub success: bool,
    pub retry_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LatencySummary {
    pub p50_latency_ms: f64,
    pub p90_latency_ms: f64,
    pub p99_latency_ms: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkSummary {
    pub benchmark: String,
    pub operation_count: u64,
    pub success_count: u64,
    pub failure_count: u64,
    pub failure_rate: f64,
    pub total_bytes: u64,
    pub elapsed_ms: f64,
    pub mb_per_sec: f64,
    pub p50_latency_ms: f64,
    pub p90_latency_ms: f64,
    pub p99_latency_ms: f64,
    pub retry_count: u64,
}

impl BenchmarkSummary {
    pub fn from_samples(
        benchmark: impl Into<String>,
        samples: &[BenchmarkSample],
        elapsed_ms: f64,
    ) -> Self {
        let operation_count = samples.len() as u64;
        let success_count = samples.iter().filter(|s| s.success).count() as u64;
        let failure_count = operation_count.saturating_sub(success_count);
        let total_bytes = samples.iter().map(|s| s.bytes).sum::<u64>();
        let retry_count = samples.iter().map(|s| s.retry_count).sum::<u64>();
        let failure_rate = if operation_count == 0 {
            0.0
        } else {
            failure_count as f64 / operation_count as f64
        };
        let mb_per_sec = if elapsed_ms <= 0.0 {
            0.0
        } else {
            (total_bytes as f64 / (1024.0 * 1024.0)) / (elapsed_ms / 1000.0)
        };
        let latencies: Vec<f64> = samples.iter().map(|s| s.latency_ms).collect();
        let latency = latency_summary(&latencies);
        Self {
            benchmark: benchmark.into(),
            operation_count,
            success_count,
            failure_count,
            failure_rate,
            total_bytes,
            elapsed_ms,
            mb_per_sec,
            p50_latency_ms: latency.p50_latency_ms,
            p90_latency_ms: latency.p90_latency_ms,
            p99_latency_ms: latency.p99_latency_ms,
            retry_count,
        }
    }

    pub fn csv_header() -> &'static str {
        "benchmark,operation_count,success_count,failure_count,failure_rate,total_bytes,elapsed_ms,mb_per_sec,p50_latency_ms,p90_latency_ms,p99_latency_ms,retry_count"
    }

    pub fn to_csv_row(&self) -> String {
        format!(
            "{},{},{},{},{:.6},{},{:.3},{:.3},{:.3},{:.3},{:.3},{}",
            self.benchmark,
            self.operation_count,
            self.success_count,
            self.failure_count,
            self.failure_rate,
            self.total_bytes,
            self.elapsed_ms,
            self.mb_per_sec,
            self.p50_latency_ms,
            self.p90_latency_ms,
            self.p99_latency_ms,
            self.retry_count
        )
    }
}

pub fn percentile_ms(samples: &[f64], percentile: f64) -> f64 {
    if samples.is_empty() {
        return 0.0;
    }
    let mut sorted = samples.to_vec();
    sorted.sort_by(|a, b| a.total_cmp(b));
    let p = percentile.clamp(0.0, 1.0);
    let rank = (p * (sorted.len() as f64 - 1.0)).ceil() as usize;
    sorted[rank]
}

pub fn latency_summary(samples: &[f64]) -> LatencySummary {
    LatencySummary {
        p50_latency_ms: percentile_ms(samples, 0.50),
        p90_latency_ms: percentile_ms(samples, 0.90),
        p99_latency_ms: percentile_ms(samples, 0.99),
    }
}

#[derive(Debug, Clone)]
fn with_storage_root<F>(
    root: Option<PathBuf>,
    keep_data: bool,
    f: F,
) -> Result<BenchmarkSummary, String>
where
    F: FnOnce(&Path) -> Result<BenchmarkSummary, String>,
{
    let temp = tempfile::tempdir().map_err(|e| e.to_string())?;
    let use_root = root.unwrap_or_else(|| temp.path().to_path_buf());
    fs::create_dir_all(&use_root).map_err(|e| e.to_string())?;
    let result = f(&use_root);
    if keep_data {
        std::mem::forget(temp);
    }
    result
}

pub fn run_dataset_read(
    items: usize,
    payload_bytes: usize,
    concurrency: usize,
    root: Option<PathBuf>,
    keep_data: bool,
) -> Result<BenchmarkSummary, String> {
    with_storage_root(root, keep_data, |root| {
        let storage = LocalStorage::new(root.to_path_buf()).map_err(|e| e.to_string())?;
        let mut keys = Vec::with_capacity(items);
        let payload = vec![7_u8; payload_bytes];
        for i in 0..items {
            let key = StorageKey::new(format!("bench/dataset/shard-{i}.bin"))
                .map_err(|e| e.to_string())?;
            storage.put(&key, &payload).map_err(|e| e.to_string())?;
            keys.push(key);
        }
        let start = Instant::now();
        let results = runtime_storage::get_many(
            storage,
            keys,
            StorageOperationPolicy {
                max_concurrent_operations: concurrency,
                ..StorageOperationPolicy::default()
            },
        )
        .map_err(|e| e.to_string())?;
        let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
        let mut samples = Vec::new();
        for (_key, res) in results {
            match res {
                Ok(bytes) => samples.push(BenchmarkSample {
                    latency_ms: if items > 0 {
                        elapsed_ms / items as f64
                    } else {
                        0.0
                    },
                    bytes: bytes.len() as u64,
                    success: true,
                    retry_count: 0,
                }),
                Err(_) => samples.push(BenchmarkSample {
                    latency_ms: if items > 0 {
                        elapsed_ms / items as f64
                    } else {
                        0.0
                    },
                    bytes: 0,
                    success: false,
                    retry_count: 0,
                }),
            }
        }
        Ok(BenchmarkSummary::from_samples(
            "dataset-read",
            &samples,
            elapsed_ms,
        ))
    })
}

pub fn run_checkpoint_write(
    artifacts: usize,
    payload_bytes: usize,
    concurrency: usize,
    root: Option<PathBuf>,
    keep_data: bool,
) -> Result<BenchmarkSummary, String> {
    with_storage_root(root, keep_data, |root| {
        let storage = LocalStorage::new(root.to_path_buf()).map_err(|e| e.to_string())?;
        let payload = vec![9_u8; payload_bytes];
        let uploads: Vec<CheckpointArtifactUpload> = (0..artifacts)
            .map(|i| CheckpointArtifactUpload {
                worker_id: WorkerId("worker-0".into()),
                artifact_name: format!("artifact-{i}.bin"),
                storage_key: runtime_storage::checkpoint_worker_artifact(
                    "bench-run",
                    "ckpt-1",
                    "worker-0",
                    &format!("artifact-{i}.bin"),
                ),
                bytes: payload.clone(),
                checksum_algorithm: ChecksumAlgorithm::Sha256,
            })
            .collect();
        let policy = StorageOperationPolicy {
            max_concurrent_operations: concurrency,
            ..StorageOperationPolicy::default()
        };
        let start = Instant::now();
        let records = upload_checkpoint_artifacts_concurrently(storage, uploads, &policy)
            .map_err(|e| e.to_string())?;
        let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
        let mut ckpt = CheckpointRecord::new(
            RunId("bench-run".into()),
            CheckpointId("ckpt-1".into()),
            1,
            1,
        );
        for r in &records {
            ckpt.add_artifact(r.clone(), 2).map_err(|e| e.to_string())?;
        }
        let store =
            CheckpointStore::new(LocalStorage::new(root.to_path_buf()).map_err(|e| e.to_string())?);
        store
            .commit_checkpoint(&mut ckpt, 3)
            .map_err(|e| e.to_string())?;
        let samples: Vec<BenchmarkSample> = records
            .into_iter()
            .map(|r| BenchmarkSample {
                latency_ms: if artifacts > 0 {
                    elapsed_ms / artifacts as f64
                } else {
                    0.0
                },
                bytes: r.size_bytes,
                success: true,
                retry_count: 0,
            })
            .collect();
        Ok(BenchmarkSummary::from_samples(
            "checkpoint-write",
            &samples,
            elapsed_ms,
        ))
    })
}

pub fn run_checkpoint_restore(
    iterations: usize,
    artifacts: usize,
    payload_bytes: usize,
    concurrency: usize,
    root: Option<PathBuf>,
    keep_data: bool,
) -> Result<BenchmarkSummary, String> {
    with_storage_root(root, keep_data, |root| {
        let storage = LocalStorage::new(root.to_path_buf()).map_err(|e| e.to_string())?;
        let _ = run_checkpoint_write(
            artifacts,
            payload_bytes,
            concurrency,
            Some(root.to_path_buf()),
            true,
        )?;
        let store =
            CheckpointStore::new(LocalStorage::new(root.to_path_buf()).map_err(|e| e.to_string())?);
        let mut samples = Vec::with_capacity(iterations);
        let mut elapsed_total_ms = 0.0;
        for _ in 0..iterations {
            let t0 = Instant::now();
            let loaded = store.load_latest_manifest(&RunId("bench-run".into()));
            let elapsed_ms = t0.elapsed().as_secs_f64() * 1000.0;
            elapsed_total_ms += elapsed_ms;
            samples.push(BenchmarkSample {
                latency_ms: elapsed_ms,
                bytes: loaded
                    .as_ref()
                    .ok()
                    .map(|m| m.to_json_bytes().unwrap_or_default().len() as u64)
                    .unwrap_or(0),
                success: loaded.is_ok(),
                retry_count: 0,
            });
        }
        Ok(BenchmarkSummary::from_samples(
            "checkpoint-restore",
            &samples,
            elapsed_total_ms,
        ))
    })
}

pub fn write_outputs(
    summary: &BenchmarkSummary,
    csv: Option<&Path>,
    json: Option<&Path>,
) -> Result<(), String> {
    if let Some(path) = csv {
        fs::write(
            path,
            format!(
                "{}\n{}\n",
                BenchmarkSummary::csv_header(),
                summary.to_csv_row()
            ),
        )
        .map_err(|e| e.to_string())?;
    }
    if let Some(path) = json {
        fs::write(
            path,
            serde_json::to_vec_pretty(summary).map_err(|e| e.to_string())?,
        )
        .map_err(|e| e.to_string())?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn percentile_empty() {
        assert_eq!(percentile_ms(&[], 0.5), 0.0);
    }
    #[test]
    fn percentile_single() {
        assert_eq!(percentile_ms(&[2.0], 0.9), 2.0);
    }
    #[test]
    fn percentile_unsorted() {
        let v = [9.0, 1.0, 5.0, 3.0];
        assert_eq!(percentile_ms(&v, 0.5), 5.0);
    }
    #[test]
    fn percentile_known() {
        let v = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0];
        assert_eq!(percentile_ms(&v, 0.5), 6.0);
        assert_eq!(percentile_ms(&v, 0.9), 10.0);
        assert_eq!(percentile_ms(&v, 0.99), 10.0);
    }
    #[test]
    fn summary_calc() {
        let s = BenchmarkSummary::from_samples(
            "x",
            &[
                BenchmarkSample {
                    latency_ms: 1.0,
                    bytes: 100,
                    success: true,
                    retry_count: 0,
                },
                BenchmarkSample {
                    latency_ms: 2.0,
                    bytes: 0,
                    success: false,
                    retry_count: 0,
                },
            ],
            10.0,
        );
        assert_eq!(s.operation_count, 2);
        assert_eq!(s.failure_count, 1);
    }
    #[test]
    fn csv_has_headers() {
        assert!(BenchmarkSummary::csv_header().contains("benchmark,operation_count"));
    }
    #[test]
    fn dataset_small() {
        let s = run_dataset_read(4, 64, 2, None, false).unwrap();
        assert_eq!(s.operation_count, 4);
    }
    #[test]
    fn checkpoint_write_small() {
        let s = run_checkpoint_write(2, 128, 2, None, false).unwrap();
        assert_eq!(s.success_count, 2);
    }
    #[test]
    fn checkpoint_restore_small() {
        let s = run_checkpoint_restore(3, 2, 128, 2, None, false).unwrap();
        assert_eq!(s.operation_count, 3);
    }
}
