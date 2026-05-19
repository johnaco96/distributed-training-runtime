#![forbid(unsafe_code)]

use clap::{Parser, Subcommand};
use runtime_bench::{
    parse_workers_list, run_checkpoint_restore, run_checkpoint_write, run_dataset_read,
    run_scaling_workers, run_worker_rpc, validate_worker_counts, write_outputs, write_outputs_many,
    DEFAULT_SCALING_WORKERS, DEFAULT_SIMULATED_SCALING_WORKERS,
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
        #[arg(long, default_value_t = true)]
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
                assert_eq!(parsed, vec![32, 64, 128]);
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
}
