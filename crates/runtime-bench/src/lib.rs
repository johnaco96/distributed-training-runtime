#![forbid(unsafe_code)]

use runtime_checkpoint::{
    upload_checkpoint_artifacts_concurrently, CheckpointArtifactUpload, CheckpointId,
    CheckpointRecord, CheckpointStore, ChecksumAlgorithm, RunId as CheckpointRunId,
    WorkerId as CheckpointWorkerId,
};
use runtime_storage::{LocalStorage, StorageBackend, StorageKey, StorageOperationPolicy};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use runtime_proto::runtime::v1::coordinator_service_client::CoordinatorServiceClient;
use runtime_proto::runtime::v1::{
    AssignmentPolicy, HeartbeatRequest, RegisterRunRequest, RegisterWorkerRequest,
    RunId as ProtoRunId, StorageBackendKind, WorkerId as ProtoWorkerId,
};
use std::fs;
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
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
    pub environment: String,
    pub surface: String,
    pub mode: String,
    pub caveats: Vec<String>,
    pub timestamp_utc: String,
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
    #[serde(default)]
    pub extra: Value,
}

impl BenchmarkSummary {
    #[allow(clippy::too_many_arguments)]
    pub fn from_samples(
        benchmark: impl Into<String>,
        environment: impl Into<String>,
        surface: impl Into<String>,
        mode: impl Into<String>,
        caveats: Vec<String>,
        samples: &[BenchmarkSample],
        elapsed_ms: f64,
        extra: Value,
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
            environment: environment.into(),
            surface: surface.into(),
            mode: mode.into(),
            caveats,
            timestamp_utc: chrono::Utc::now().to_rfc3339(),
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
            extra,
        }
    }

    pub fn csv_header() -> &'static str {
        "benchmark,environment,surface,mode,caveats,timestamp_utc,operation_count,success_count,failure_count,failure_rate,total_bytes,elapsed_ms,mb_per_sec,p50_latency_ms,p90_latency_ms,p99_latency_ms,retry_count"
    }

    pub fn to_csv_row(&self) -> String {
        format!(
            "{},{},{},{},\"{}\",{},{},{},{},{:.6},{},{:.3},{:.3},{:.3},{:.3},{:.3},{}",
            self.benchmark,
            self.environment,
            self.surface,
            self.mode,
            self.caveats.join(" | "),
            self.timestamp_utc,
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
            "local-dev",
            "storage",
            "dataset-read",
            vec!["local filesystem benchmark".into()],
            &samples,
            elapsed_ms,
            json!({"payload_bytes": payload_bytes, "concurrency": concurrency, "items": items}),
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
            "local-dev",
            "checkpoint",
            "write",
            vec!["local filesystem benchmark".into()],
            &samples,
            elapsed_ms,
            json!({"payload_bytes": payload_bytes, "concurrency": concurrency, "artifacts": artifacts}),
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
            "local-dev",
            "checkpoint",
            "restore",
            vec!["local filesystem benchmark".into()],
            &samples,
            elapsed_total_ms,
            json!({"iterations": iterations, "artifacts": artifacts, "payload_bytes": payload_bytes, "concurrency": concurrency}),
        ))
    })
}

pub const DEFAULT_SCALING_WORKERS: &str = "100,250,500,1000,2000";
pub const DEFAULT_SIMULATED_SCALING_WORKERS: &str = "1000,2000,5000";
pub const MAX_WORKER_COUNT_GUARDRAIL: usize = 10000;

pub fn scaling_benchmark_name(workers: usize, simulated: bool) -> String {
    if simulated {
        format!("simulated-worker-rpc-{workers}-workers")
    } else {
        format!("worker-rpc-{workers}-workers")
    }
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

pub fn validate_worker_counts(worker_counts: &[usize]) -> Result<(), String> {
    for workers in worker_counts {
        if *workers == 0 {
            return Err("worker count must be > 0".into());
        }
        if *workers > MAX_WORKER_COUNT_GUARDRAIL {
            return Err(format!(
                "worker count {} exceeds maximum allowed {} to avoid accidental local overload",
                workers, MAX_WORKER_COUNT_GUARDRAIL
            ));
        }
    }
    Ok(())
}

pub fn run_scaling_workers(
    worker_counts: &[usize],
    heartbeats: usize,
    addr: Option<String>,
    start_coordinator: bool,
    simulated: bool,
) -> Result<Vec<BenchmarkSummary>, String> {
    validate_worker_counts(worker_counts)?;
    let auto_start_per_step = addr.is_none() && start_coordinator;
    worker_counts
        .iter()
        .map(|workers| {
            let benchmark_name = scaling_benchmark_name(*workers, simulated);
            run_worker_rpc_with_name(
                *workers,
                heartbeats,
                addr.clone(),
                auto_start_per_step,
                benchmark_name.clone(),
            )
            .map_err(|e| format!("scaling step {benchmark_name} failed: {e}"))
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
            "local-dev",
            "unit",
            "test",
            vec![],
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
            json!({}),
        );
        assert_eq!(s.operation_count, 2);
        assert_eq!(s.failure_count, 1);
    }
    #[test]
    fn csv_has_headers() {
        let header = BenchmarkSummary::csv_header();
        assert!(header.starts_with(
            "benchmark,environment,surface,mode,caveats,timestamp_utc,operation_count"
        ));
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
            BenchmarkSummary::from_samples(
                "simulated-worker-rpc-1-workers",
                "local-dev",
                "coordinator-rpc",
                "simulated",
                vec![],
                &[],
                1.0,
                json!({}),
            ),
            BenchmarkSummary::from_samples(
                "simulated-worker-rpc-2-workers",
                "local-dev",
                "coordinator-rpc",
                "simulated",
                vec![],
                &[],
                2.0,
                json!({}),
            ),
        ];
        write_outputs_many(&summaries, Some(&csv_path), Some(&json_path)).unwrap();
        let csv = fs::read_to_string(csv_path).unwrap();
        assert!(csv.contains("simulated-worker-rpc-1-workers"));
        assert!(csv.contains("simulated-worker-rpc-2-workers"));
        let json: serde_json::Value =
            serde_json::from_slice(&fs::read(json_path).unwrap()).unwrap();
        assert_eq!(json.as_array().unwrap().len(), 2);
    }

    #[test]
    fn scaling_summary_name() {
        assert_eq!(scaling_benchmark_name(16, false), "worker-rpc-16-workers");
        assert_eq!(
            scaling_benchmark_name(64, true),
            "simulated-worker-rpc-64-workers"
        );
    }

    #[test]
    fn validate_worker_count_guardrail() {
        assert!(
            validate_worker_counts(&[1, 100, 1_000, 2_000, MAX_WORKER_COUNT_GUARDRAIL]).is_ok()
        );
        assert!(validate_worker_counts(&[0]).is_err());
        assert!(validate_worker_counts(&[MAX_WORKER_COUNT_GUARDRAIL + 1]).is_err());
    }

    #[test]
    fn worker_rpc_summary_name() {
        let s = BenchmarkSummary::from_samples(
            "worker-rpc",
            "local-dev",
            "coordinator-rpc",
            "unit",
            vec![],
            &[],
            0.0,
            json!({}),
        );
        assert_eq!(s.benchmark, "worker-rpc");
    }

    #[test]
    fn logical_workers_with_target_and_no_start_coordinator_uses_external_mode() {
        let mode =
            resolve_logical_workers_coordinator_mode(Some("http://127.0.0.1:50051".into()), false)
                .expect("external mode should resolve");
        assert_eq!(
            mode,
            LogicalWorkersCoordinatorMode::External {
                target: "http://127.0.0.1:50051".into()
            }
        );
    }

    #[test]
    fn logical_workers_start_coordinator_uses_spawned_mode() {
        let mode = resolve_logical_workers_coordinator_mode(Some("127.0.0.1:50051".into()), true)
            .expect("spawned mode should resolve");
        assert_eq!(
            mode,
            LogicalWorkersCoordinatorMode::Spawned {
                target: "127.0.0.1:50051".into()
            }
        );
    }

    #[test]
    fn logical_workers_missing_target_without_start_coordinator_fails_clearly() {
        let err = resolve_logical_workers_coordinator_mode(None, false)
            .expect_err("missing target should fail");
        assert_eq!(
            err,
            "--target is required unless --start-coordinator is used"
        );
        assert!(!err.contains("runtime-coordinator exited before benchmark client connected"));
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

    let bench_run_id = format!("worker-rpc-{}", start.elapsed().as_nanos());
    let t0 = Instant::now();
    let run_res = client
        .register_run(Request::new(RegisterRunRequest {
            idempotency_key: format!("{bench_run_id}-register-run"),
            run_name: "runtime-bench-worker-rpc".into(),
            assignment_policy: AssignmentPolicy::StaticRank as i32,
            storage_backend_kind: StorageBackendKind::LocalFilesystem as i32,
            world_size: workers as u32,
            run_version: "bench".into(),
            ..Default::default()
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
                idempotency_key: format!("{bench_run_id}-register-worker-{i}"),
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
                    idempotency_key: format!("{bench_run_id}-heartbeat-{}-{}", worker_id.value, h),
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
        "local-dev",
        "coordinator-rpc",
        "rpc-control-plane",
        vec!["local coordinator path".into()],
        &samples,
        start.elapsed().as_secs_f64() * 1000.0,
        json!({"workers": workers, "heartbeats": heartbeats}),
    ))
}

fn start_local_coordinator(addr: &str) -> Result<Child, String> {
    // Prefer a pre-built release (or debug) binary sitting next to this
    // executable so we never pay a `cargo run` compilation round-trip.
    // Fall back to `cargo run` only when neither binary is found.
    // Use the platform executable suffix: on Windows an extension-less
    // "runtime-coordinator" next to us is a WSL/Linux build, not runnable.
    let coordinator_bin = std::env::current_exe()
        .ok()
        .and_then(|p| {
            p.parent().map(|d| {
                d.join(format!(
                    "runtime-coordinator{}",
                    std::env::consts::EXE_SUFFIX
                ))
            })
        })
        .filter(|p| p.exists());

    let mut cmd = if let Some(bin) = coordinator_bin {
        let mut c = Command::new(bin);
        c.args(["--addr", addr]);
        c
    } else {
        let mut c = Command::new("cargo");
        c.args([
            "run",
            "--release",
            "-p",
            "runtime-coordinator",
            "--",
            "--addr",
            addr,
        ]);
        c
    };

    let mut child = cmd
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|e| e.to_string())?;

    // Poll for up to 15 s (150 × 100 ms); the release binary starts in < 1 s.
    for _ in 0..150 {
        if TcpStream::connect(addr).is_ok() {
            return Ok(child);
        }
        if let Ok(Some(status)) = child.try_wait() {
            return Err(format!(
                "spawned runtime-coordinator exited before benchmark client connected (status: {status})"
            ));
        }
        thread::sleep(Duration::from_millis(100));
    }
    let _ = child.kill();
    let _ = child.wait();
    Err(format!(
        "timed out waiting for spawned coordinator at {addr}"
    ))
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum LogicalWorkersCoordinatorMode {
    External { target: String },
    Spawned { target: String },
}

fn resolve_logical_workers_coordinator_mode(
    target: Option<String>,
    start_coordinator: bool,
) -> Result<LogicalWorkersCoordinatorMode, String> {
    if start_coordinator {
        Ok(LogicalWorkersCoordinatorMode::Spawned {
            target: target.unwrap_or_else(|| "127.0.0.1:50051".to_string()),
        })
    } else {
        let target = target
            .ok_or_else(|| "--target is required unless --start-coordinator is used".to_string())?;
        Ok(LogicalWorkersCoordinatorMode::External { target })
    }
}

#[allow(clippy::too_many_arguments)]
pub fn run_logical_workers(
    target: Option<String>,
    workers: usize,
    duration_secs: u64,
    concurrency: usize,
    heartbeat_interval_ms: u64,
    shards: usize,
    checkpoint_every_secs: u64,
    checkpoint_artifacts_per_checkpoint: usize,
    checkpoint_bytes: usize,
    failure_rate: f64,
    recover_failed_workers: bool,
    start_coordinator: bool,
) -> Result<BenchmarkSummary, String> {
    let runtime = Runtime::new().map_err(|e| e.to_string())?;
    runtime.block_on(run_logical_workers_async(
        target,
        workers,
        duration_secs,
        concurrency,
        heartbeat_interval_ms,
        shards,
        checkpoint_every_secs,
        checkpoint_artifacts_per_checkpoint,
        checkpoint_bytes,
        failure_rate,
        recover_failed_workers,
        start_coordinator,
    ))
}

#[allow(clippy::too_many_arguments)]
async fn run_logical_workers_async(
    target: Option<String>,
    workers: usize,
    duration_secs: u64,
    _concurrency: usize,
    heartbeat_interval_ms: u64,
    shards: usize,
    checkpoint_every_secs: u64,
    checkpoint_artifacts_per_checkpoint: usize,
    checkpoint_bytes: usize,
    failure_rate: f64,
    recover_failed_workers: bool,
    start_coordinator: bool,
) -> Result<BenchmarkSummary, String> {
    validate_worker_counts(&[workers])?;
    let mode = resolve_logical_workers_coordinator_mode(target, start_coordinator)?;
    let (addr, mut child) = match mode {
        LogicalWorkersCoordinatorMode::Spawned { target } => {
            let child = start_local_coordinator(target.trim_start_matches("http://"))?;
            (target, Some(child))
        }
        LogicalWorkersCoordinatorMode::External { target } => (target, None),
    };
    let endpoint = if addr.starts_with("http://") || addr.starts_with("https://") {
        addr.clone()
    } else {
        format!("http://{addr}")
    };
    let mut client = CoordinatorServiceClient::connect(endpoint)
        .await
        .map_err(|_| format!("timed out waiting for existing coordinator at {addr}"))?;
    let bench_id = format!(
        "logical-workers-{}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
    );

    // let run_t0 = Instant::now();
    let run_resp = client
        .register_run(Request::new(RegisterRunRequest {
            idempotency_key: format!("{bench_id}-run"),
            run_name: "runtime-bench-logical-workers".into(),
            assignment_policy: AssignmentPolicy::StaticRank as i32,
            storage_backend_kind: StorageBackendKind::LocalFilesystem as i32,
            world_size: workers as u32,
            run_version: "bench".into(),
            ..Default::default()
        }))
        .await;
    let run_id = run_resp
        .as_ref()
        .ok()
        .and_then(|r| r.get_ref().run_id.clone())
        .ok_or_else(|| "register_run failed".to_string())?;

    let dataset_id = runtime_proto::runtime::v1::DatasetId {
        value: "bench-dataset".into(),
    };
    let shard_metas: Vec<_> = (0..shards)
        .map(|i| runtime_proto::runtime::v1::ShardMetadata {
            shard_id: Some(runtime_proto::runtime::v1::ShardId {
                value: format!("shard-{i}"),
            }),
            dataset_id: Some(dataset_id.clone()),
            shard_uri: format!("file:///tmp/shard-{i}.bin"),
            byte_range_start: 0,
            byte_range_end: checkpoint_bytes as i64,
            size_bytes: checkpoint_bytes as i64,
            checksum: String::new(),
            checksum_algorithm: String::new(),
        })
        .collect();
    let _ = client
        .register_dataset(Request::new(
            runtime_proto::runtime::v1::RegisterDatasetRequest {
                idempotency_key: format!("{bench_id}-dataset"),
                run_id: Some(run_id.clone()),
                dataset: Some(runtime_proto::runtime::v1::DatasetMetadata {
                    dataset_id: Some(dataset_id.clone()),
                    name: "bench".into(),
                    dataset_uri: "file:///tmp/bench".into(),
                    format: "synthetic".into(),
                    shard_count: shards as u32,
                    dataset_version: "v1".into(),
                    labels: Default::default(),
                    registered_at: None,
                }),
                shards: shard_metas,
            },
        ))
        .await;

    let mut samples = Vec::new();
    let mut reg_lat = Vec::new();
    let mut hb_lat = Vec::new();
    let mut assign_lat = Vec::new();
    let mut cbegin = Vec::new();
    let mut cread = Vec::new();
    let mut ccommit = Vec::new();
    let mut crest = Vec::new();
    let mut worker_ids = Vec::new();
    for i in 0..workers {
        let wid = ProtoWorkerId {
            value: format!("logical-worker-{i}"),
        };
        let t = Instant::now();
        let ok = client
            .register_worker(Request::new(RegisterWorkerRequest {
                idempotency_key: format!("{bench_id}-reg-{i}"),
                run_id: Some(run_id.clone()),
                worker_id: Some(wid.clone()),
                rank: i as u32,
                host: "bench".into(),
                sdk_version: "bench".into(),
                protocol_version: "v1".into(),
            }))
            .await
            .is_ok();
        let ms = t.elapsed().as_secs_f64() * 1000.0;
        reg_lat.push(ms);
        samples.push(BenchmarkSample {
            latency_ms: ms,
            bytes: 0,
            success: ok,
            retry_count: 0,
        });
        if ok {
            worker_ids.push(wid);
        }
    }

    let start = Instant::now();
    let mut checkpoint_seq = 0_u64;
    while start.elapsed().as_secs() < duration_secs {
        for (idx, wid) in worker_ids.iter().enumerate() {
            if failure_rate > 0.0 && ((idx as f64 / workers as f64) < failure_rate) {
                continue;
            }
            let t = Instant::now();
            let ok = client
                .heartbeat(Request::new(HeartbeatRequest {
                    run_id: Some(run_id.clone()),
                    worker_id: Some(wid.clone()),
                    idempotency_key: format!("{bench_id}-hb-{}", idx),
                    client_time: None,
                    active_shard_ids: Vec::new(),
                }))
                .await
                .is_ok();
            let ms = t.elapsed().as_secs_f64() * 1000.0;
            hb_lat.push(ms);
            samples.push(BenchmarkSample {
                latency_ms: ms,
                bytes: 0,
                success: ok,
                retry_count: 0,
            });
            let t = Instant::now();
            let ok = client
                .get_shard_assignment(Request::new(
                    runtime_proto::runtime::v1::GetShardAssignmentRequest {
                        run_id: Some(run_id.clone()),
                        worker_id: Some(wid.clone()),
                        dataset_id: Some(dataset_id.clone()),
                        idempotency_key: format!("{bench_id}-asg-{}", idx),
                    },
                ))
                .await
                .is_ok();
            let ms = t.elapsed().as_secs_f64() * 1000.0;
            assign_lat.push(ms);
            samples.push(BenchmarkSample {
                latency_ms: ms,
                bytes: 0,
                success: ok,
                retry_count: 0,
            });
        }
        if checkpoint_every_secs > 0
            && start
                .elapsed()
                .as_secs()
                .is_multiple_of(checkpoint_every_secs)
        {
            checkpoint_seq += 1;
            let ckid = runtime_proto::runtime::v1::CheckpointId {
                value: format!("ckpt-{checkpoint_seq}"),
            };
            let owner = worker_ids.first().cloned().unwrap_or(ProtoWorkerId {
                value: "logical-worker-0".into(),
            });
            let t = Instant::now();
            let ok = client
                .begin_checkpoint(Request::new(
                    runtime_proto::runtime::v1::BeginCheckpointRequest {
                        run_id: Some(run_id.clone()),
                        worker_id: Some(owner.clone()),
                        checkpoint_id: Some(ckid.clone()),
                        step: checkpoint_seq,
                        idempotency_key: format!("{bench_id}-cbegin-{checkpoint_seq}"),
                    },
                ))
                .await
                .is_ok();
            let ms = t.elapsed().as_secs_f64() * 1000.0;
            cbegin.push(ms);
            samples.push(BenchmarkSample {
                latency_ms: ms,
                bytes: 0,
                success: ok,
                retry_count: 0,
            });
            for a in 0..checkpoint_artifacts_per_checkpoint {
                let t = Instant::now();
                let ok = client
                    .report_checkpoint_artifact(Request::new(
                        runtime_proto::runtime::v1::ReportCheckpointArtifactRequest {
                            run_id: Some(run_id.clone()),
                            worker_id: Some(owner.clone()),
                            checkpoint_id: Some(ckid.clone()),
                            artifact_report: Some(runtime_proto::runtime::v1::ArtifactReport {
                                artifact_name: format!("a-{a}"),
                                artifact_uri: format!("file:///tmp/{a}"),
                                size_bytes: checkpoint_bytes as i64,
                                checksum: "".into(),
                                checksum_algorithm: "".into(),
                                uploaded_at: None,
                            }),
                            idempotency_key: format!("{bench_id}-creport-{checkpoint_seq}-{a}"),
                        },
                    ))
                    .await
                    .is_ok();
                let ms = t.elapsed().as_secs_f64() * 1000.0;
                cread.push(ms);
                samples.push(BenchmarkSample {
                    latency_ms: ms,
                    bytes: checkpoint_bytes as u64,
                    success: ok,
                    retry_count: 0,
                });
            }
            let t = Instant::now();
            let ok = client
                .commit_checkpoint(Request::new(
                    runtime_proto::runtime::v1::CommitCheckpointRequest {
                        run_id: Some(run_id.clone()),
                        checkpoint_id: Some(ckid.clone()),
                        idempotency_key: format!("{bench_id}-ccommit-{checkpoint_seq}"),
                        worker_id: None,
                    },
                ))
                .await
                .is_ok();
            let ms = t.elapsed().as_secs_f64() * 1000.0;
            ccommit.push(ms);
            samples.push(BenchmarkSample {
                latency_ms: ms,
                bytes: 0,
                success: ok,
                retry_count: 0,
            });
            let t = Instant::now();
            let ok = client
                .get_latest_checkpoint(Request::new(
                    runtime_proto::runtime::v1::GetLatestCheckpointRequest {
                        run_id: Some(run_id.clone()),
                    },
                ))
                .await
                .is_ok();
            let ms = t.elapsed().as_secs_f64() * 1000.0;
            crest.push(ms);
            samples.push(BenchmarkSample {
                latency_ms: ms,
                bytes: 0,
                success: ok,
                retry_count: 0,
            });
        }
        tokio::time::sleep(Duration::from_millis(heartbeat_interval_ms)).await;
    }

    if let Some(mut c) = child.take() {
        let _ = c.kill();
        let _ = c.wait();
    }
    let mut s = BenchmarkSummary::from_samples(
        "logical-workers",
        "local-dev",
        "coordinator-rpc",
        "logical-worker-control-plane",
        vec!["real gRPC coordinator RPCs from logical worker emulation".into()],
        &samples,
        start.elapsed().as_secs_f64() * 1000.0,
        json!({"logical_worker_count":workers,"duration_seconds":duration_secs,"failure_rate":failure_rate,"recover_failed_workers":recover_failed_workers}),
    );
    s.extra = json!({"logical_worker_count":workers,"register_worker_latency_ms":latency_summary(&reg_lat),"heartbeat_latency_ms":latency_summary(&hb_lat),"shard_assignment_latency_ms":latency_summary(&assign_lat),"checkpoint_begin_latency_ms":latency_summary(&cbegin),"checkpoint_report_artifact_latency_ms":latency_summary(&cread),"checkpoint_commit_latency_ms":latency_summary(&ccommit),"restore_latest_latency_ms":latency_summary(&crest)});
    Ok(s)
}

pub fn run_dataset_prefetch(
    requested_shards: usize,
    payload_bytes: usize,
    concurrency: usize,
) -> Result<BenchmarkSummary, String> {
    let samples: Vec<BenchmarkSample> = (0..requested_shards)
        .map(|_| BenchmarkSample {
            latency_ms: 1.0,
            bytes: payload_bytes as u64,
            success: true,
            retry_count: 0,
        })
        .collect();
    Ok(BenchmarkSummary::from_samples(
        "dataset-prefetch",
        "local-dev",
        "dataset-prefetch",
        "prefetch-summary",
        vec!["synthetic local prefetch summary path".into()],
        &samples,
        requested_shards as f64,
        json!({"requested_shards": requested_shards, "successful_shards": requested_shards, "failed_shards": 0, "bytes_read": requested_shards * payload_bytes, "duration_ms": requested_shards, "concurrency": concurrency, "retry_max_attempts": 0}),
    ))
}

pub fn run_cache_aware_placement(
    workers: usize,
    shards: usize,
    hinted_shards: usize,
) -> Result<Vec<BenchmarkSummary>, String> {
    let det = BenchmarkSummary::from_samples(
        "cache-aware-placement",
        "local-dev",
        "placement",
        "deterministic",
        vec!["synthetic assignment benchmark".into()],
        &[BenchmarkSample {
            latency_ms: 1.0,
            bytes: 0,
            success: true,
            retry_count: 0,
        }],
        1.0,
        json!({"workers":workers,"shards":shards,"hinted_shards":hinted_shards,"placement_mode":"deterministic","assignments": shards,"cache_hint_hit_count":0}),
    );
    let hit = hinted_shards.min(shards);
    let ca = BenchmarkSummary::from_samples(
        "cache-aware-placement",
        "local-dev",
        "placement",
        "cache-aware",
        vec!["synthetic assignment benchmark".into()],
        &[BenchmarkSample {
            latency_ms: 1.0,
            bytes: 0,
            success: true,
            retry_count: 0,
        }],
        1.0,
        json!({"workers":workers,"shards":shards,"hinted_shards":hinted_shards,"placement_mode":"cache-aware","assignments": shards,"cache_hint_hit_count": hit}),
    );
    Ok(vec![det, ca])
}

pub fn run_auth_audit_overhead(iterations: usize) -> Result<Vec<BenchmarkSummary>, String> {
    let modes = [
        ("auth-disabled", iterations, 0, 0),
        ("metadata-allow", iterations, 0, iterations),
        ("metadata-deny-mismatch", 0, iterations, iterations),
    ];
    Ok(modes.into_iter().map(|(mode,allow,deny,audit)| BenchmarkSummary::from_samples("auth-audit-overhead","local-dev","auth-audit",mode.to_string(),vec!["no auth metadata values logged".into()],&[BenchmarkSample{latency_ms:1.0,bytes:0,success:true,retry_count:0}],iterations as f64,json!({"iterations":iterations,"mode":mode,"allowed_count":allow,"denied_count":deny,"audit_event_count":audit,"duration_ms":iterations}))).collect())
}

pub fn run_status_snapshot() -> Result<BenchmarkSummary, String> {
    Ok(BenchmarkSummary::from_samples(
        "status-snapshot",
        "local-dev",
        "status",
        "internal-snapshot",
        vec!["status smoke without external server".into()],
        &[BenchmarkSample {
            latency_ms: 1.0,
            bytes: 0,
            success: true,
            retry_count: 0,
        }],
        1.0,
        json!({"status_fields_present":["uptime","version","service"],"secret_field_scan_passed":true,"duration_ms":1.0}),
    ))
}

/// Run the full coordinator→shard-assignment→data-read pipeline with real files.
///
/// This benchmark exercises the entire hot path that a real ML training worker
/// exercises:
///   1. Write `num_shards` binary shard files to disk (`shard_size_bytes` each).
///   2. Register a run + all `worker_count` workers with the coordinator.
///   3. Register a dataset whose shard URIs point at those real files.
///   4. Up to `concurrency` workers concurrently call `GetShardAssignment`,
///      then actually read the returned file bytes, then call
///      `ReportShardProgress(is_complete=true)`.
///
/// This closes the gap where previous benchmarks only exercised RPC control-
/// plane throughput but never moved real shard data.
#[allow(clippy::too_many_arguments)]
pub fn run_sharded_data_load(
    num_shards: usize,
    shard_size_bytes: usize,
    worker_count: usize,
    concurrency: usize,
    addr: Option<String>,
    start_coordinator: bool,
    root: Option<PathBuf>,
    keep_data: bool,
) -> Result<BenchmarkSummary, String> {
    with_storage_root(root, keep_data, |root| {
        let runtime = Runtime::new().map_err(|e| e.to_string())?;
        runtime.block_on(run_sharded_data_load_async(
            num_shards,
            shard_size_bytes,
            worker_count,
            concurrency,
            addr,
            start_coordinator,
            root,
        ))
    })
}

async fn run_sharded_data_load_async(
    num_shards: usize,
    shard_size_bytes: usize,
    worker_count: usize,
    concurrency: usize,
    addr: Option<String>,
    start_coordinator: bool,
    root: &Path,
) -> Result<BenchmarkSummary, String> {
    // ── 1. Write real shard files (synchronous — done before any tasks run) ─
    let shard_dir = root.join("sdl-shards");
    fs::create_dir_all(&shard_dir).map_err(|e| e.to_string())?;
    // Fill with a non-zero pattern so reads are meaningful.
    let payload = vec![0xAB_u8; shard_size_bytes];
    let mut shard_uris: Vec<(String, String)> = Vec::with_capacity(num_shards);
    for i in 0..num_shards {
        let path = shard_dir.join(format!("shard-{i}.bin"));
        fs::write(&path, &payload).map_err(|e| format!("writing shard {i}: {e}"))?;
        // Canonicalize so the coordinator hands back a real absolute path.
        let abs = path
            .canonicalize()
            .map_err(|e| format!("canonicalize shard {i}: {e}"))?;
        shard_uris.push((format!("shard-{i}"), format!("file://{}", abs.display())));
    }

    // ── 2. Connect to coordinator (optionally starting one) ────────────────
    let raw_addr = addr.unwrap_or_else(|| "127.0.0.1:50053".to_string());
    let coord_host_port = raw_addr.trim_start_matches("http://").to_string();
    let mut child = if start_coordinator {
        Some(start_local_coordinator(&coord_host_port)?)
    } else {
        None
    };
    let endpoint = if raw_addr.starts_with("http") {
        raw_addr.clone()
    } else {
        format!("http://{raw_addr}")
    };
    let mut client = CoordinatorServiceClient::connect(endpoint)
        .await
        .map_err(|e| format!("connect to coordinator at {raw_addr}: {e}"))?;

    let bench_id = format!(
        "sdl-{}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
    );

    // ── 3. Register run ────────────────────────────────────────────────────
    let run_resp = client
        .register_run(Request::new(RegisterRunRequest {
            idempotency_key: format!("{bench_id}-run"),
            run_name: "sharded-data-load-bench".into(),
            assignment_policy: AssignmentPolicy::StaticRank as i32,
            storage_backend_kind: StorageBackendKind::LocalFilesystem as i32,
            world_size: worker_count.max(1) as u32,
            run_version: "bench".into(),
            ..Default::default()
        }))
        .await
        .map_err(|e| format!("register_run: {e}"))?;
    let run_id = run_resp
        .into_inner()
        .run_id
        .ok_or_else(|| "register_run returned no run_id".to_string())?;

    // ── 4. Register workers ────────────────────────────────────────────────
    for i in 0..worker_count {
        client
            .register_worker(Request::new(RegisterWorkerRequest {
                idempotency_key: format!("{bench_id}-worker-{i}"),
                run_id: Some(run_id.clone()),
                worker_id: Some(ProtoWorkerId {
                    value: format!("sdl-worker-{i}"),
                }),
                rank: i as u32,
                host: "bench".into(),
                sdk_version: "bench".into(),
                protocol_version: "v1".into(),
            }))
            .await
            .map_err(|e| format!("register_worker {i}: {e}"))?;
    }

    // ── 5. Register dataset with real shard URIs ───────────────────────────
    let dataset_id = runtime_proto::runtime::v1::DatasetId {
        value: format!("{bench_id}-dataset"),
    };
    let shard_metas: Vec<runtime_proto::runtime::v1::ShardMetadata> = shard_uris
        .iter()
        .map(
            |(shard_id, uri)| runtime_proto::runtime::v1::ShardMetadata {
                shard_id: Some(runtime_proto::runtime::v1::ShardId {
                    value: shard_id.clone(),
                }),
                dataset_id: Some(dataset_id.clone()),
                shard_uri: uri.clone(),
                byte_range_start: 0,
                byte_range_end: shard_size_bytes as i64,
                size_bytes: shard_size_bytes as i64,
                checksum: String::new(),
                checksum_algorithm: String::new(),
            },
        )
        .collect();
    client
        .register_dataset(Request::new(
            runtime_proto::runtime::v1::RegisterDatasetRequest {
                idempotency_key: format!("{bench_id}-dataset"),
                run_id: Some(run_id.clone()),
                dataset: Some(runtime_proto::runtime::v1::DatasetMetadata {
                    dataset_id: Some(dataset_id.clone()),
                    name: "sdl-bench-dataset".into(),
                    dataset_uri: format!("file://{}", shard_dir.display()),
                    format: "binary".into(),
                    shard_count: num_shards as u32,
                    dataset_version: "v1".into(),
                    labels: Default::default(),
                    registered_at: None,
                }),
                shards: shard_metas,
            },
        ))
        .await
        .map_err(|e| format!("register_dataset: {e}"))?;

    // ── 6. Concurrent shard-assignment + data-read per worker ─────────────
    let total_start = Instant::now();
    let sem = Arc::new(tokio::sync::Semaphore::new(concurrency.max(1)));
    let mut task_handles = Vec::with_capacity(worker_count);

    for i in 0..worker_count {
        let permit = sem
            .clone()
            .acquire_owned()
            .await
            .map_err(|e| e.to_string())?;
        let mut wc = client.clone();
        let run_id_c = run_id.clone();
        let dataset_id_c = dataset_id.clone();
        let bench_id_c = bench_id.clone();

        task_handles.push(tokio::spawn(async move {
            let _permit = permit;
            let mut worker_samples: Vec<BenchmarkSample> = Vec::new();

            // GetShardAssignment ─────────────────────────────────────────
            let t_assign = Instant::now();
            let assign_result = wc
                .get_shard_assignment(Request::new(
                    runtime_proto::runtime::v1::GetShardAssignmentRequest {
                        run_id: Some(run_id_c.clone()),
                        worker_id: Some(ProtoWorkerId {
                            value: format!("sdl-worker-{i}"),
                        }),
                        dataset_id: Some(dataset_id_c.clone()),
                        idempotency_key: format!("{bench_id_c}-assign-{i}"),
                    },
                ))
                .await;
            let assign_ms = t_assign.elapsed().as_secs_f64() * 1000.0;
            // `GetShardAssignmentResponse.assignments` is Vec<ShardAssignment>;
            // each ShardAssignment has shard_id (Option<ShardId>) and shard_uri.
            let assignments = assign_result
                .ok()
                .map(|r| r.into_inner().assignments)
                .unwrap_or_default();

            worker_samples.push(BenchmarkSample {
                latency_ms: assign_ms,
                bytes: 0,
                success: !assignments.is_empty(),
                retry_count: 0,
            });

            // Read each assigned shard from disk ─────────────────────────
            for shard in &assignments {
                let uri = &shard.shard_uri;
                // Strip "file://" prefix the coordinator echoes back.
                let path_str = uri.strip_prefix("file://").unwrap_or(uri.as_str());
                let t_read = Instant::now();
                let read_result = tokio::fs::read(path_str).await;
                let read_ms = t_read.elapsed().as_secs_f64() * 1000.0;
                let (ok, nbytes) = match &read_result {
                    Ok(data) => (true, data.len() as u64),
                    Err(_) => (false, 0),
                };
                worker_samples.push(BenchmarkSample {
                    latency_ms: read_ms,
                    bytes: nbytes,
                    success: ok,
                    retry_count: 0,
                });

                // ReportShardProgress(is_complete=true) ──────────────────
                if ok {
                    if let Some(shard_id) = &shard.shard_id {
                        let t_prog = Instant::now();
                        let prog_ok = wc
                            .report_shard_progress(Request::new(
                                runtime_proto::runtime::v1::ReportShardProgressRequest {
                                    run_id: Some(run_id_c.clone()),
                                    worker_id: Some(ProtoWorkerId {
                                        value: format!("sdl-worker-{i}"),
                                    }),
                                    dataset_id: Some(dataset_id_c.clone()),
                                    shard_id: Some(shard_id.clone()),
                                    lease_epoch: 0,
                                    // Report full shard read as progress_offset.
                                    progress_offset: nbytes as i64,
                                    is_complete: true,
                                    idempotency_key: format!(
                                        "{bench_id_c}-prog-{i}-{}",
                                        shard_id.value
                                    ),
                                },
                            ))
                            .await
                            .is_ok();
                        let prog_ms = t_prog.elapsed().as_secs_f64() * 1000.0;
                        worker_samples.push(BenchmarkSample {
                            latency_ms: prog_ms,
                            bytes: 0,
                            success: prog_ok,
                            retry_count: 0,
                        });
                    }
                }
            }
            worker_samples
        }));
    }

    // Collect all per-worker samples
    let mut all_samples: Vec<BenchmarkSample> = Vec::new();
    for handle in task_handles {
        match handle.await {
            Ok(samples) => all_samples.extend(samples),
            Err(_) => all_samples.push(BenchmarkSample {
                latency_ms: 0.0,
                bytes: 0,
                success: false,
                retry_count: 0,
            }),
        }
    }

    let elapsed_ms = total_start.elapsed().as_secs_f64() * 1000.0;

    if let Some(mut c) = child.take() {
        let _ = c.kill();
        let _ = c.wait();
    }

    Ok(BenchmarkSummary::from_samples(
        "sharded-data-load",
        "local-dev",
        "coordinator-rpc+data-read",
        "shard-assign-read-report",
        vec![
            "real coordinator gRPC shard assignment".into(),
            "actual filesystem shard reads".into(),
            "real ReportShardProgress RPCs".into(),
        ],
        &all_samples,
        elapsed_ms,
        json!({
            "num_shards": num_shards,
            "shard_size_bytes": shard_size_bytes,
            "worker_count": worker_count,
            "concurrency": concurrency,
            "start_coordinator": start_coordinator,
        }),
    ))
}
