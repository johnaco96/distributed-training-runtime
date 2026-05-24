#![forbid(unsafe_code)]

use clap::{Parser, Subcommand};
use runtime_bench::{
    parse_workers_list, run_auth_audit_overhead, run_cache_aware_placement, run_checkpoint_restore,
    run_checkpoint_write, run_dataset_prefetch, run_dataset_read, run_logical_workers,
    run_scaling_workers, run_sharded_data_load, run_status_snapshot, run_worker_rpc,
    validate_worker_counts, write_outputs, write_outputs_many, DEFAULT_SCALING_WORKERS,
    DEFAULT_SIMULATED_SCALING_WORKERS,
};
use std::path::PathBuf;

#[derive(Parser)]
struct Cli {
    #[command(subcommand)]
    cmd: Command,
}

#[derive(Subcommand)]
enum Command {
    DatasetRead {
        #[arg(long, default_value_t = 128)]
        items: usize,
        #[arg(long, default_value_t = 65536)]
        payload_bytes: usize,
        #[arg(long, default_value_t = 16)]
        concurrency: usize,
        #[arg(long)]
        out_csv: Option<PathBuf>,
        #[arg(long)]
        out_json: Option<PathBuf>,
        #[arg(long)]
        root: Option<PathBuf>,
        #[arg(long, default_value_t = false)]
        keep_data: bool,
    },
    CheckpointWrite {
        #[arg(long, default_value_t = 16)]
        artifacts: usize,
        #[arg(long, default_value_t = 1048576)]
        payload_bytes: usize,
        #[arg(long, default_value_t = 16)]
        concurrency: usize,
        #[arg(long)]
        out_csv: Option<PathBuf>,
        #[arg(long)]
        out_json: Option<PathBuf>,
        #[arg(long)]
        root: Option<PathBuf>,
        #[arg(long, default_value_t = false)]
        keep_data: bool,
    },
    CheckpointRestore {
        #[arg(long, default_value_t = 100)]
        iterations: usize,
        #[arg(long, default_value_t = 16)]
        artifacts: usize,
        #[arg(long, default_value_t = 1048576)]
        payload_bytes: usize,
        #[arg(long, default_value_t = 16)]
        concurrency: usize,
        #[arg(long)]
        out_csv: Option<PathBuf>,
        #[arg(long)]
        out_json: Option<PathBuf>,
        #[arg(long)]
        root: Option<PathBuf>,
        #[arg(long, default_value_t = false)]
        keep_data: bool,
    },
    ScalingWorkers {
        #[arg(long)]
        workers_list: Option<String>,
        #[arg(long, default_value_t = false)]
        simulate_large: bool,
        #[arg(long, default_value_t = 10)]
        heartbeats: usize,
        #[arg(long)]
        addr: Option<String>,
        #[arg(long, default_value_t = false)]
        start_coordinator: bool,
        #[arg(long)]
        out_csv: Option<PathBuf>,
        #[arg(long)]
        out_json: Option<PathBuf>,
    },

    DatasetPrefetch {
        #[arg(long, default_value_t = 32)]
        requested_shards: usize,
        #[arg(long, default_value_t = 65536)]
        payload_bytes: usize,
        #[arg(long, default_value_t = 8)]
        concurrency: usize,
        #[arg(long)]
        out_csv: Option<PathBuf>,
        #[arg(long)]
        out_json: Option<PathBuf>,
    },
    CacheAwarePlacement {
        #[arg(long, default_value_t = 4)]
        workers: usize,
        #[arg(long, default_value_t = 16)]
        shards: usize,
        #[arg(long, default_value_t = 6)]
        hinted_shards: usize,
        #[arg(long)]
        out_csv: Option<PathBuf>,
        #[arg(long)]
        out_json: Option<PathBuf>,
    },
    AuthAuditOverhead {
        #[arg(long, default_value_t = 1000)]
        iterations: usize,
        #[arg(long)]
        out_csv: Option<PathBuf>,
        #[arg(long)]
        out_json: Option<PathBuf>,
    },
    StatusSnapshot {
        #[arg(long)]
        out_csv: Option<PathBuf>,
        #[arg(long)]
        out_json: Option<PathBuf>,
    },

    LogicalWorkers {
        #[arg(long)]
        target: Option<String>,
        #[arg(long, default_value_t = 1000)]
        workers: usize,
        #[arg(long, default_value_t = 30)]
        duration_secs: u64,
        #[arg(long, default_value_t = 64)]
        concurrency: usize,
        #[arg(long, default_value_t = 1000)]
        heartbeat_interval_ms: u64,
        #[arg(long, default_value_t = 1000)]
        shards: usize,
        #[arg(long, default_value_t = 0)]
        checkpoint_every_secs: u64,
        #[arg(long, default_value_t = 2)]
        checkpoint_artifacts_per_checkpoint: usize,
        #[arg(long, default_value_t = 1048576)]
        checkpoint_bytes: usize,
        #[arg(long, default_value_t = 0.0)]
        failure_rate: f64,
        #[arg(long, default_value_t = false)]
        recover_failed_workers: bool,
        #[arg(long, default_value_t = false)]
        start_coordinator: bool,
        #[arg(long)]
        out_csv: Option<PathBuf>,
        #[arg(long)]
        out_json: Option<PathBuf>,
    },

    WorkerRpc {
        #[arg(long, default_value_t = 8)]
        workers: usize,
        #[arg(long, default_value_t = 10)]
        heartbeats: usize,
        #[arg(long)]
        addr: Option<String>,
        #[arg(long, default_value_t = true)]
        start_coordinator: bool,
        #[arg(long)]
        out_csv: Option<PathBuf>,
        #[arg(long)]
        out_json: Option<PathBuf>,
    },

    /// End-to-end benchmark: coordinator assigns real shard URIs, workers read
    /// actual file bytes, and progress is reported back via gRPC.
    ///
    /// Unlike other benchmarks this exercises the full data-plane:
    ///   coordinator RPC → shard assignment → filesystem read → progress report.
    ShardedDataLoad {
        /// Number of shard files to create (default 64, ~1 MiB each = 64 MiB total).
        #[arg(long, default_value_t = 64)]
        num_shards: usize,
        /// Bytes per shard file written to disk.
        #[arg(long, default_value_t = 1_048_576)]
        shard_size_bytes: usize,
        /// Simulated worker count (each worker calls GetShardAssignment + reads).
        #[arg(long, default_value_t = 8)]
        worker_count: usize,
        /// Max concurrent worker tasks (limits parallelism, not worker count).
        #[arg(long, default_value_t = 16)]
        concurrency: usize,
        /// Coordinator address; defaults to 127.0.0.1:50053.
        #[arg(long)]
        addr: Option<String>,
        /// When true, spawn a local coordinator subprocess automatically.
        #[arg(long, default_value_t = true)]
        start_coordinator: bool,
        /// Directory root for shard files; uses a tempdir if omitted.
        #[arg(long)]
        root: Option<PathBuf>,
        /// Keep shard files after the benchmark finishes.
        #[arg(long, default_value_t = false)]
        keep_data: bool,
        #[arg(long)]
        out_csv: Option<PathBuf>,
        #[arg(long)]
        out_json: Option<PathBuf>,
    },
}

fn main() -> Result<(), String> {
    let cli = Cli::parse();
    let (summaries, csv, json) = match cli.cmd {
        Command::DatasetRead {
            items,
            payload_bytes,
            concurrency,
            out_csv,
            out_json,
            root,
            keep_data,
        } => (
            vec![run_dataset_read(
                items,
                payload_bytes,
                concurrency,
                root,
                keep_data,
            )?],
            out_csv,
            out_json,
        ),
        Command::CheckpointWrite {
            artifacts,
            payload_bytes,
            concurrency,
            out_csv,
            out_json,
            root,
            keep_data,
        } => (
            vec![run_checkpoint_write(
                artifacts,
                payload_bytes,
                concurrency,
                root,
                keep_data,
            )?],
            out_csv,
            out_json,
        ),
        Command::CheckpointRestore {
            iterations,
            artifacts,
            payload_bytes,
            concurrency,
            out_csv,
            out_json,
            root,
            keep_data,
        } => (
            vec![run_checkpoint_restore(
                iterations,
                artifacts,
                payload_bytes,
                concurrency,
                root,
                keep_data,
            )?],
            out_csv,
            out_json,
        ),
        Command::ScalingWorkers {
            workers_list,
            heartbeats,
            addr,
            start_coordinator,
            out_csv,
            out_json,
            simulate_large,
        } => {
            let default_workers = if simulate_large {
                DEFAULT_SIMULATED_SCALING_WORKERS
            } else {
                DEFAULT_SCALING_WORKERS
            };
            let worker_counts =
                parse_workers_list(workers_list.as_deref().unwrap_or(default_workers))?;
            validate_worker_counts(&worker_counts)?;
            (
                run_scaling_workers(
                    &worker_counts,
                    heartbeats,
                    addr,
                    start_coordinator,
                    simulate_large,
                )?,
                out_csv,
                out_json,
            )
        }
        Command::DatasetPrefetch {
            requested_shards,
            payload_bytes,
            concurrency,
            out_csv,
            out_json,
        } => (
            vec![run_dataset_prefetch(
                requested_shards,
                payload_bytes,
                concurrency,
            )?],
            out_csv,
            out_json,
        ),
        Command::CacheAwarePlacement {
            workers,
            shards,
            hinted_shards,
            out_csv,
            out_json,
        } => (
            run_cache_aware_placement(workers, shards, hinted_shards)?,
            out_csv,
            out_json,
        ),
        Command::AuthAuditOverhead {
            iterations,
            out_csv,
            out_json,
        } => (run_auth_audit_overhead(iterations)?, out_csv, out_json),
        Command::StatusSnapshot { out_csv, out_json } => {
            (vec![run_status_snapshot()?], out_csv, out_json)
        }
        Command::LogicalWorkers {
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
            out_csv,
            out_json,
        } => (
            vec![run_logical_workers(
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
            )?],
            out_csv,
            out_json,
        ),
        Command::WorkerRpc {
            workers,
            heartbeats,
            addr,
            start_coordinator,
            out_csv,
            out_json,
        } => (
            vec![run_worker_rpc(
                workers,
                heartbeats,
                addr,
                start_coordinator,
            )?],
            out_csv,
            out_json,
        ),
        Command::ShardedDataLoad {
            num_shards,
            shard_size_bytes,
            worker_count,
            concurrency,
            addr,
            start_coordinator,
            root,
            keep_data,
            out_csv,
            out_json,
        } => (
            vec![run_sharded_data_load(
                num_shards,
                shard_size_bytes,
                worker_count,
                concurrency,
                addr,
                start_coordinator,
                root,
                keep_data,
            )?],
            out_csv,
            out_json,
        ),
    };

    for summary in &summaries {
        println!(
            "benchmark={} ops={} success={} fail={} failure_rate={:.4} bytes={} elapsed_ms={:.3} mb_per_sec={:.3} p50={:.3} p90={:.3} p99={:.3} retries={}",
            summary.benchmark,
            summary.operation_count,
            summary.success_count,
            summary.failure_count,
            summary.failure_rate,
            summary.total_bytes,
            summary.elapsed_ms,
            summary.mb_per_sec,
            summary.p50_latency_ms,
            summary.p90_latency_ms,
            summary.p99_latency_ms,
            summary.retry_count
        );
    }

    if summaries.len() == 1 {
        write_outputs(&summaries[0], csv.as_deref(), json.as_deref())?;
    } else {
        write_outputs_many(&summaries, csv.as_deref(), json.as_deref())?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn scaling_workers_subcommand_parses() {
        let cli = Cli::parse_from([
            "runtime-bench",
            "scaling-workers",
            "--workers-list",
            "1,2,4,8,16",
            "--simulate-large",
            "--heartbeats",
            "10",
        ]);
        match cli.cmd {
            Command::ScalingWorkers {
                workers_list,
                heartbeats,
                simulate_large,
                ..
            } => {
                assert_eq!(workers_list.as_deref(), Some("1,2,4,8,16"));
                assert_eq!(heartbeats, 10);
                assert!(simulate_large);
            }
            _ => panic!("expected scaling-workers subcommand"),
        }
    }

    #[test]
    fn scaling_workers_simulated_defaults() {
        let cli = Cli::parse_from(["runtime-bench", "scaling-workers", "--simulate-large"]);
        match cli.cmd {
            Command::ScalingWorkers {
                workers_list,
                simulate_large,
                ..
            } => {
                assert!(simulate_large);
                assert!(workers_list.is_none());
                let parsed = parse_workers_list(DEFAULT_SIMULATED_SCALING_WORKERS).unwrap();
                assert_eq!(parsed, vec![1000, 2000, 5000]);
            }
            _ => panic!("expected scaling-workers subcommand"),
        }
    }

    #[test]
    fn worker_rpc_subcommand_parses() {
        let cli = Cli::parse_from([
            "runtime-bench",
            "worker-rpc",
            "--workers",
            "2",
            "--heartbeats",
            "3",
        ]);

        match cli.cmd {
            Command::WorkerRpc {
                workers,
                heartbeats,
                ..
            } => {
                assert_eq!(workers, 2);
                assert_eq!(heartbeats, 3);
            }
            _ => panic!("expected worker-rpc subcommand"),
        }
    }

    #[test]
    fn logical_workers_subcommand_external_mode_parses() {
        let cli = Cli::parse_from([
            "runtime-bench",
            "logical-workers",
            "--target",
            "http://127.0.0.1:50051",
            "--workers",
            "100",
        ]);
        match cli.cmd {
            Command::LogicalWorkers {
                target,
                workers,
                start_coordinator,
                ..
            } => {
                assert_eq!(target.as_deref(), Some("http://127.0.0.1:50051"));
                assert_eq!(workers, 100);
                assert!(!start_coordinator);
            }
            _ => panic!("expected logical-workers subcommand"),
        }
    }

    #[test]
    fn logical_workers_subcommand_spawned_mode_parses() {
        let cli = Cli::parse_from([
            "runtime-bench",
            "logical-workers",
            "--start-coordinator",
            "--target",
            "http://127.0.0.1:50051",
        ]);
        match cli.cmd {
            Command::LogicalWorkers {
                start_coordinator, ..
            } => {
                assert!(start_coordinator);
            }
            _ => panic!("expected logical-workers subcommand"),
        }
    }

    #[test]
    fn sharded_data_load_subcommand_parses_defaults() {
        let cli = Cli::parse_from(["runtime-bench", "sharded-data-load"]);
        match cli.cmd {
            Command::ShardedDataLoad {
                num_shards,
                shard_size_bytes,
                worker_count,
                concurrency,
                addr,
                start_coordinator,
                ..
            } => {
                assert_eq!(num_shards, 64);
                assert_eq!(shard_size_bytes, 1_048_576);
                assert_eq!(worker_count, 8);
                assert_eq!(concurrency, 16);
                assert!(addr.is_none());
                assert!(start_coordinator);
            }
            _ => panic!("expected sharded-data-load subcommand"),
        }
    }

    #[test]
    fn sharded_data_load_subcommand_parses_custom() {
        let cli = Cli::parse_from([
            "runtime-bench",
            "sharded-data-load",
            "--num-shards",
            "32",
            "--shard-size-bytes",
            "65536",
            "--worker-count",
            "4",
            "--concurrency",
            "4",
            "--addr",
            "127.0.0.1:50053",
        ]);
        match cli.cmd {
            Command::ShardedDataLoad {
                num_shards,
                shard_size_bytes,
                worker_count,
                concurrency,
                addr,
                ..
            } => {
                assert_eq!(num_shards, 32);
                assert_eq!(shard_size_bytes, 65536);
                assert_eq!(worker_count, 4);
                assert_eq!(concurrency, 4);
                assert_eq!(addr.as_deref(), Some("127.0.0.1:50053"));
            }
            _ => panic!("expected sharded-data-load subcommand"),
        }
    }
}
