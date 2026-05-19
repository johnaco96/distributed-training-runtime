#![forbid(unsafe_code)]

use std::net::SocketAddr;
use std::sync::Arc;

use runtime_coordinator::persistence::SqliteMetadataStore;
use runtime_coordinator::service::CoordinatorServiceImpl;
use runtime_coordinator::state::CoordinatorState;
use runtime_proto::runtime::v1::coordinator_service_server::CoordinatorServiceServer;
use tonic::transport::Server;
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_target(false)
        .compact()
        .init();

    let (addr_raw, metadata_db) = coordinator_args()?;
    let addr: SocketAddr = addr_raw.parse()?;
    let state = if let Some(path) = metadata_db {
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

    Ok(())
}

fn coordinator_args() -> Result<(String, Option<String>), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let mut addr = String::from("127.0.0.1:50051");
    let mut metadata_db = None;

    while let Some(arg) = args.next() {
        if arg == "--addr" {
            addr = args.next().ok_or("--addr requires an address")?;
        } else if arg == "--metadata-db" {
            metadata_db = Some(args.next().ok_or("--metadata-db requires a path")?);
        } else {
            return Err(format!("unexpected argument: {arg}").into());
        }
    }

    Ok((addr, metadata_db))
}
