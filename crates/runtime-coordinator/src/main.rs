#![forbid(unsafe_code)]

use std::net::SocketAddr;
use std::sync::Arc;

use runtime_coordinator::init_tracing_from_env;
use runtime_coordinator::persistence::SqliteMetadataStore;
use runtime_coordinator::service::CoordinatorServiceImpl;
use runtime_coordinator::state::CoordinatorState;
use runtime_proto::runtime::v1::coordinator_service_server::CoordinatorServiceServer;
use tonic::transport::Server;
use tracing::info;

const DEFAULT_ADDR: &str = "127.0.0.1:50051";

#[derive(Debug, PartialEq, Eq)]
struct CoordinatorCliArgs {
    addr: String,
    metadata_db: Option<String>,
    show_help: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_tracing_from_env();

    let args = coordinator_args(std::env::args().skip(1))?;
    if args.show_help {
        print_usage();
        return Ok(());
    }

    let addr: SocketAddr = args.addr.parse()?;
    info!(%addr, "runtime-coordinator configured listen address");

    let state = if let Some(path) = args.metadata_db {
        info!(metadata_db_path = %path, "runtime-coordinator using sqlite metadata store");
        let store = SqliteMetadataStore::open(path)?;
        store.init_schema()?;
        Arc::new(CoordinatorState::with_metadata_store(store))
    } else {
        Arc::new(CoordinatorState::default())
    };
    let service = CoordinatorServiceImpl::new(state);

    info!(%addr, "starting runtime-coordinator gRPC server");

    Server::builder()
        .add_service(CoordinatorServiceServer::new(service))
        .serve(addr)
        .await?;

    info!(%addr, "runtime-coordinator server shut down gracefully");

    Ok(())
}

fn print_usage() {
    println!("Usage: runtime-coordinator [--addr <ADDR>] [--metadata-db <PATH>] [--help|-h]");
    println!("\nOptions:");
    println!("  --addr <ADDR>         Coordinator listen address (default: {DEFAULT_ADDR})");
    println!("  --metadata-db <PATH>  Enable SQLite metadata persistence at PATH");
    println!("  --help, -h            Show this help message");
}

fn coordinator_args(
    args: impl IntoIterator<Item = String>,
) -> Result<CoordinatorCliArgs, Box<dyn std::error::Error>> {
    let mut args = args.into_iter();
    let mut parsed = CoordinatorCliArgs {
        addr: String::from(DEFAULT_ADDR),
        metadata_db: None,
        show_help: false,
    };

    while let Some(arg) = args.next() {
        if arg == "--addr" {
            parsed.addr = args.next().ok_or("--addr requires an address")?;
        } else if arg == "--metadata-db" {
            parsed.metadata_db = Some(args.next().ok_or("--metadata-db requires a path")?);
        } else if arg == "--help" || arg == "-h" {
            parsed.show_help = true;
        } else {
            return Err(format!("unexpected argument: {arg}").into());
        }
    }

    Ok(parsed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_args() {
        let parsed = coordinator_args(Vec::<String>::new()).expect("default args should parse");
        assert_eq!(
            parsed,
            CoordinatorCliArgs {
                addr: DEFAULT_ADDR.to_string(),
                metadata_db: None,
                show_help: false,
            }
        );
    }

    #[test]
    fn custom_addr() {
        let parsed = coordinator_args(vec!["--addr".to_string(), "127.0.0.1:60000".to_string()])
            .expect("custom addr should parse");
        assert_eq!(parsed.addr, "127.0.0.1:60000");
        assert_eq!(parsed.metadata_db, None);
        assert!(!parsed.show_help);
    }

    #[test]
    fn metadata_db_path() {
        let parsed = coordinator_args(vec![
            "--metadata-db".to_string(),
            ".dtruntime-demo/coordinator.db".to_string(),
        ])
        .expect("metadata-db should parse");
        assert_eq!(
            parsed.metadata_db,
            Some(".dtruntime-demo/coordinator.db".to_string())
        );
    }

    #[test]
    fn help_flag() {
        let parsed = coordinator_args(vec!["--help".to_string()]).expect("help should parse");
        assert!(parsed.show_help);
    }

    #[test]
    fn unknown_arg_errors() {
        let err = coordinator_args(vec!["--wat".to_string()]).expect_err("should error");
        assert_eq!(err.to_string(), "unexpected argument: --wat");
    }
}
