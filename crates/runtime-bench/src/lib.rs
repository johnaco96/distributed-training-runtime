#![forbid(unsafe_code)]

use runtime_checkpoint::{
    upload_checkpoint_artifacts_concurrently, CheckpointArtifactUpload, CheckpointId,
    CheckpointRecord, CheckpointStore, ChecksumAlgorithm, RunId as CheckpointRunId,
    WorkerId as CheckpointWorkerId,
};
use runtime_storage::{LocalStorage, StorageBackend, StorageKey, StorageOperationPolicy};
use serde::{Deserialize, Serialize};

use runtime_proto::runtime::v1::coordinator_service_client::CoordinatorServiceClient;
use runtime_proto::runtime::v1::{
    AssignmentPolicy, HeartbeatRequest, RegisterRunRequest, RegisterWorkerRequest,
    RunId as ProtoRunId, StorageBackendKind, WorkerId as ProtoWorkerId,
};
use std::fs;
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::Duration;
use std::time::Instant;
use tokio::runtime::Runtime;
use tonic::Request;

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
            &storage,
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
                worker_id: CheckpointWorkerId("worker-0".into()),
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
        let records = upload_checkpoint_artifacts_concurrently(&storage, uploads, &policy)
            .map_err(|e| e.to_string())?;
        let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
        let mut ckpt = CheckpointRecord::new(
            CheckpointRunId("bench-run".into()),
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
        let _storage = LocalStorage::new(root.to_path_buf()).map_err(|e| e.to_string())?;
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
            let loaded = store.load_latest_manifest(&CheckpointRunId("bench-run".into()));
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

pub fn scaling_benchmark_name(workers: usize) -> String {
    format!("worker-rpc-{workers}-workers")
}

pub fn parse_workers_list(input: &str) -> Result<Vec<usize>, String> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err("workers list cannot be empty".into());
    }
    trimmed
        .split(',')
        .map(|part| {
            let value = part.trim();
            if value.is_empty() {
                return Err("workers list cannot contain empty values".to_string());
            }
            let workers: isize = value
                .parse()
                .map_err(|_| format!("invalid worker count '{value}'"))?;
            if workers <= 0 {
                return Err(format!("worker count must be > 0, got {workers}"));
            }
            Ok(workers as usize)
        })
        .collect()
}

pub fn run_worker_rpc_with_name(
    workers: usize,
    heartbeats: usize,
    addr: Option<String>,
    start_coordinator: bool,
    benchmark_name: impl Into<String>,
) -> Result<BenchmarkSummary, String> {
    let mut summary = run_worker_rpc(workers, heartbeats, addr, start_coordinator)?;
    summary.benchmark = benchmark_name.into();
    Ok(summary)
}

pub fn run_scaling_workers(
    worker_counts: &[usize],
    heartbeats: usize,
    addr: Option<String>,
    start_coordinator: bool,
) -> Result<Vec<BenchmarkSummary>, String> {
    worker_counts
        .iter()
        .enumerate()
        .map(|(index, workers)| {
            run_worker_rpc_with_name(
                *workers,
                heartbeats,
                addr.clone(),
                if index == 0 { start_coordinator } else { false },
                scaling_benchmark_name(*workers),
            )
        })
        .collect()
}

pub fn write_outputs_many(
    summaries: &[BenchmarkSummary],
    csv: Option<&Path>,
    json: Option<&Path>,
) -> Result<(), String> {
    if let Some(path) = csv {
        let mut body = String::from(BenchmarkSummary::csv_header());
        for summary in summaries {
            body.push('\n');
            body.push_str(&summary.to_csv_row());
        }
        body.push('\n');
        fs::write(path, body).map_err(|e| e.to_string())?;
    }
    if let Some(path) = json {
        fs::write(
            path,
            serde_json::to_vec_pretty(summaries).map_err(|e| e.to_string())?,
        )
        .map_err(|e| e.to_string())?;
    }
    Ok(())
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

    #[test]
    fn parse_workers_list_ok() {
        assert_eq!(
            parse_workers_list("1,2,4,8,16").unwrap(),
            vec![1, 2, 4, 8, 16]
        );
    }

    #[test]
    fn parse_workers_list_rejects_invalid() {
        assert!(parse_workers_list("").is_err());
        assert!(parse_workers_list("0").is_err());
        assert!(parse_workers_list("-1").is_err());
        assert!(parse_workers_list("abc").is_err());
        assert!(parse_workers_list("1,,2").is_err());
    }

    #[test]
    fn write_outputs_many_writes_multiple_rows() {
        let dir = tempfile::tempdir().unwrap();
        let csv_path = dir.path().join("bench.csv");
        let json_path = dir.path().join("bench.json");
        let summaries = vec![
            BenchmarkSummary::from_samples("worker-rpc-1-workers", &[], 1.0),
            BenchmarkSummary::from_samples("worker-rpc-2-workers", &[], 2.0),
        ];
        write_outputs_many(&summaries, Some(&csv_path), Some(&json_path)).unwrap();
        let csv = fs::read_to_string(csv_path).unwrap();
        assert!(csv.contains("worker-rpc-1-workers"));
        assert!(csv.contains("worker-rpc-2-workers"));
        let json: serde_json::Value =
            serde_json::from_slice(&fs::read(json_path).unwrap()).unwrap();
        assert_eq!(json.as_array().unwrap().len(), 2);
    }

    #[test]
    fn scaling_summary_name() {
        assert_eq!(scaling_benchmark_name(16), "worker-rpc-16-workers");
    }

    #[test]
    fn worker_rpc_summary_name() {
        let s = BenchmarkSummary::from_samples("worker-rpc", &[], 0.0);
        assert_eq!(s.benchmark, "worker-rpc");
    }
}

pub fn run_worker_rpc(
    workers: usize,
    heartbeats: usize,
    addr: Option<String>,
    start_coordinator: bool,
) -> Result<BenchmarkSummary, String> {
    let runtime = Runtime::new().map_err(|e| e.to_string())?;
    runtime.block_on(run_worker_rpc_async(
        workers,
        heartbeats,
        addr,
        start_coordinator,
    ))
}

async fn run_worker_rpc_async(
    workers: usize,
    heartbeats: usize,
    addr: Option<String>,
    start_coordinator: bool,
) -> Result<BenchmarkSummary, String> {
    let addr = addr.unwrap_or_else(|| "127.0.0.1:50051".to_string());
    let mut child = if start_coordinator {
        Some(start_local_coordinator(&addr)?)
    } else {
        None
    };
    let endpoint = format!("http://{addr}");
    let mut client = CoordinatorServiceClient::connect(endpoint)
        .await
        .map_err(|e| e.to_string())?;

    let mut samples = Vec::with_capacity(1 + workers + (workers * heartbeats));
    let start = Instant::now();

    let t0 = Instant::now();
    let run_res = client
        .register_run(Request::new(RegisterRunRequest {
            idempotency_key: "bench-register-run".into(),
            run_name: "runtime-bench-worker-rpc".into(),
            assignment_policy: AssignmentPolicy::StaticRank as i32,
            storage_backend_kind: StorageBackendKind::LocalFilesystem as i32,
            world_size: workers as u32,
            run_version: "bench".into(),
        }))
        .await;
    let run_id = run_res
        .as_ref()
        .ok()
        .and_then(|r| r.get_ref().run_id.clone())
        .unwrap_or(ProtoRunId {
            value: "missing-run".into(),
        });
    samples.push(BenchmarkSample {
        latency_ms: t0.elapsed().as_secs_f64() * 1000.0,
        bytes: 0,
        success: run_res.is_ok(),
        retry_count: 0,
    });

    let mut worker_ids = Vec::with_capacity(workers);
    for i in 0..workers {
        let worker_id = ProtoWorkerId {
            value: format!("worker-{i}"),
        };
        let t = Instant::now();
        let resp = client
            .register_worker(Request::new(RegisterWorkerRequest {
                idempotency_key: format!("bench-register-worker-{i}"),
                run_id: Some(run_id.clone()),
                worker_id: Some(worker_id.clone()),
                rank: i as u32,
                host: "localhost".into(),
                sdk_version: "bench".into(),
                protocol_version: "v1".into(),
            }))
            .await;
        samples.push(BenchmarkSample {
            latency_ms: t.elapsed().as_secs_f64() * 1000.0,
            bytes: 0,
            success: resp.is_ok(),
            retry_count: 0,
        });
        if resp.is_ok() {
            worker_ids.push(worker_id);
        }
    }

    for h in 0..heartbeats {
        for worker_id in &worker_ids {
            let t = Instant::now();
            let resp = client
                .heartbeat(Request::new(HeartbeatRequest {
                    run_id: Some(run_id.clone()),
                    worker_id: Some(worker_id.clone()),
                    idempotency_key: format!("bench-heartbeat-{}-{}", worker_id.value, h),
                    client_time: None,
                    active_shard_ids: Vec::new(),
                }))
                .await;
            samples.push(BenchmarkSample {
                latency_ms: t.elapsed().as_secs_f64() * 1000.0,
                bytes: 0,
                success: resp.is_ok(),
                retry_count: 0,
            });
        }
    }

    if let Some(mut c) = child.take() {
        let _ = c.kill();
        let _ = c.wait();
    }
    Ok(BenchmarkSummary::from_samples(
        "worker-rpc",
        &samples,
        start.elapsed().as_secs_f64() * 1000.0,
    ))
}

fn start_local_coordinator(addr: &str) -> Result<Child, String> {
    let mut child = Command::new("cargo")
        .args(["run", "-p", "runtime-coordinator", "--", "--addr", addr])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|e| e.to_string())?;
    for _ in 0..80 {
        if TcpStream::connect(addr).is_ok() {
            return Ok(child);
        }
        if let Ok(Some(_)) = child.try_wait() {
            return Err("runtime-coordinator exited before benchmark client connected".into());
        }
        thread::sleep(Duration::from_millis(100));
    }
    let _ = child.kill();
    let _ = child.wait();
    Err(format!("timed out waiting for coordinator at {addr}"))
}
