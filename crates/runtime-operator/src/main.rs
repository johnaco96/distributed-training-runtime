#![forbid(unsafe_code)]

use clap::Parser;

#[derive(Parser)]
#[command(
    name = "runtime-operator",
    about = "Kubernetes operator for dtruntime TrainingRun resources"
)]
struct Args {
    /// Log filter (overridden by RUST_LOG).
    #[arg(long, default_value = "info")]
    log_level: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(args.log_level));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .compact()
        .init();

    let client = kube::Client::try_default().await?;
    tracing::info!("runtime-operator starting; watching TrainingRun resources in all namespaces");
    runtime_operator::reconciler::run_controller(client).await;
    Ok(())
}
