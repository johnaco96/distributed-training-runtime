#![forbid(unsafe_code)]

use std::net::SocketAddr;
use std::sync::Arc;

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

    let addr: SocketAddr = coordinator_addr()?.parse()?;
    let state = Arc::new(CoordinatorState::default());
    let service = CoordinatorServiceImpl::new(state);

    info!(%addr, "starting runtime-coordinator gRPC server");

    Server::builder()
        .add_service(CoordinatorServiceServer::new(service))
        .serve(addr)
        .await?;

    Ok(())
}

fn coordinator_addr() -> Result<String, Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let mut addr = String::from("127.0.0.1:50051");

    while let Some(arg) = args.next() {
        if arg == "--addr" {
            addr = args.next().ok_or("--addr requires an address")?;
        } else {
            return Err(format!("unexpected argument: {arg}").into());
        }
    }

    Ok(addr)
}
