#![forbid(unsafe_code)]

use clap::{Parser, Subcommand};
use runtime_bench::{
    run_checkpoint_restore, run_checkpoint_write, run_dataset_read, write_outputs,
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
}

fn main() -> Result<(), String> {
    let cli = Cli::parse();
    let (summary, csv, json) = match cli.cmd {
        Command::DatasetRead {
            items,
            payload_bytes,
            concurrency,
            out_csv,
            out_json,
            root,
            keep_data,
        } => (
            run_dataset_read(items, payload_bytes, concurrency, root, keep_data)?,
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
            run_checkpoint_write(artifacts, payload_bytes, concurrency, root, keep_data)?,
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
            run_checkpoint_restore(
                iterations,
                artifacts,
                payload_bytes,
                concurrency,
                root,
                keep_data,
            )?,
            out_csv,
            out_json,
        ),
    };
    println!("benchmark={} ops={} success={} fail={} failure_rate={:.4} bytes={} elapsed_ms={:.3} mb_per_sec={:.3} p50={:.3} p90={:.3} p99={:.3} retries={}", summary.benchmark, summary.operation_count, summary.success_count, summary.failure_count, summary.failure_rate, summary.total_bytes, summary.elapsed_ms, summary.mb_per_sec, summary.p50_latency_ms, summary.p90_latency_ms, summary.p99_latency_ms, summary.retry_count);
    write_outputs(&summary, csv.as_deref(), json.as_deref())?;
    Ok(())
}
