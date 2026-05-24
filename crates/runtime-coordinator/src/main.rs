#![forbid(unsafe_code)]

use std::fs;
use std::io::{Read, Write};
use std::net::SocketAddr;
use std::net::TcpListener;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::SystemTime;

use runtime_coordinator::audit::{AuditEvent, AuditEventKind, AuditLogger, AuditOutcome, DbAuditSink};
use runtime_coordinator::auth::{AuthMode, Authorizer};
use runtime_storage::S3StorageConfig;
use runtime_coordinator::ha_lock::CoordinatorLockGuard;
use runtime_coordinator::init_tracing_from_env;
use runtime_coordinator::persistence::{MetadataStore, SqliteMetadataStore};
use runtime_coordinator::pg_store::PostgresMetadataStore;
use runtime_coordinator::service::CoordinatorServiceImpl;
use runtime_coordinator::state::CoordinatorState;
use runtime_coordinator::telemetry::render_prometheus_metrics;
use runtime_proto::runtime::v1::coordinator_service_server::CoordinatorServiceServer;
use tonic::transport::{Certificate, Identity, Server, ServerTlsConfig};
use tracing::info;

const DEFAULT_ADDR: &str = "127.0.0.1:50051";

#[derive(Debug, PartialEq, Eq)]
struct CoordinatorCliArgs {
    addr: String,
    metadata_db: Option<String>,
    metadata_db_url: Option<String>,
    storage_root: Option<PathBuf>,
    tls_cert: Option<PathBuf>,
    tls_key: Option<PathBuf>,
    tls_client_ca: Option<PathBuf>,
    auth_mode: AuthMode,
    status_addr: Option<String>,
    metrics_addr: Option<String>,
    s3_endpoint: Option<String>,
    s3_bucket: Option<String>,
    s3_access_key: Option<String>,
    s3_secret_key: Option<String>,
    s3_region: String,
    max_s3_concurrent_ops: Option<usize>,
    /// Re-read each streamed artifact from disk after write and verify its
    /// SHA-256 matches the inline checksum.  Defaults to `true`.  Disable
    /// with `--no-verify-checkpoint-artifacts` for maximum throughput when
    /// storage reliability is otherwise guaranteed.
    verify_checkpoint_artifacts: bool,
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

    // Gap 5: build the audit logger; upgraded to DbAuditSink once the metadata
    // store is initialised below.  Start with tracing-only so startup events
    // are always captured even if the DB initialisation fails.
    let mut audit = AuditLogger::tracing();

    let addr: SocketAddr = args.addr.parse()?;
    info!(%addr, "runtime-coordinator configured listen address");

    let _lock_guard = if let Some(path) = &args.metadata_db {
        Some(CoordinatorLockGuard::acquire(std::path::Path::new(path))?)
    } else {
        None
    };

    // Coordinator HA advisory-lock key: "DTRNCOOR" in ASCII (fits i64).
    const PG_HA_LOCK_KEY: i64 = 0x4452_544e_434f_4f52;

    let state = if let Some(ref url) = args.metadata_db_url {
        // PostgreSQL: distributed, HA-capable, allows multiple coordinator replicas.
        info!(
            url = %redact_pg_url(url),
            "runtime-coordinator using postgres metadata store"
        );
        let store = PostgresMetadataStore::connect(url)
            .await
            .map_err(|e| format!("failed to connect to postgres metadata store: {e}"))?;
        store
            .init_schema()
            .await
            .map_err(|e| format!("failed to init postgres schema: {e}"))?;

        // Gap 3: PostgreSQL advisory lock for coordinator HA.
        // Only one replica holds the lock at a time.  The standby polls until
        // the leader releases it (process exit / connection drop), then takes over.
        // While in standby the gRPC port is not bound so the k8s readiness probe
        // (tcpSocket:50051) fails → k8s routes no traffic to the standby.
        loop {
            match store.try_acquire_advisory_lock(PG_HA_LOCK_KEY).await {
                Ok(true) => {
                    info!("coordinator HA: acquired PostgreSQL advisory lock — this replica is the leader");
                    break;
                }
                Ok(false) => {
                    info!("coordinator HA: advisory lock held by another replica — standing by (retry in 5s)");
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                }
                Err(e) => {
                    return Err(format!("failed to check PostgreSQL advisory lock: {e}").into());
                }
            }
        }

        let store: Arc<dyn MetadataStore> = Arc::new(store);
        // Gap 5: wire the durable DB audit sink now that the store is ready.
        audit = AuditLogger::new(Arc::new(DbAuditSink::new(Arc::clone(&store))));
        Arc::new(CoordinatorState::with_metadata_store(store))
    } else if let Some(ref path) = args.metadata_db {
        // SQLite: single-node, local dev / small deployments.
        info!(metadata_db_path = %path, "runtime-coordinator using sqlite metadata store");
        let store = SqliteMetadataStore::open(path)?;
        store.init_schema()?;
        let store: Arc<dyn MetadataStore> = Arc::new(store);
        // Gap 5: wire the durable DB audit sink.
        audit = AuditLogger::new(Arc::new(DbAuditSink::new(Arc::clone(&store))));
        Arc::new(CoordinatorState::with_metadata_store(store))
    } else {
        Arc::new(CoordinatorState::default())
    };
    let started_at = SystemTime::now();
    let tls_enabled = args.tls_cert.is_some();
    let mtls_enabled = args.tls_client_ca.is_some();
    let auth_mode = args.auth_mode;
    let mut service =
        CoordinatorServiceImpl::with_authorizer(state.clone(), Authorizer::new(args.auth_mode))
            .with_artifact_verification(args.verify_checkpoint_artifacts);
    if args.verify_checkpoint_artifacts {
        info!("runtime-coordinator checkpoint artifact post-write verification enabled");
    } else {
        info!("runtime-coordinator checkpoint artifact post-write verification DISABLED");
    }
    if let Some(ref root) = args.storage_root {
        let root = std::fs::canonicalize(root).unwrap_or_else(|_| root.clone());
        info!(storage_root = %root.display(), "runtime-coordinator storage root configured");
        std::fs::create_dir_all(&root)?;
        service = service.with_storage_root(root);
    }
    if let (Some(bucket), Some(access_key), Some(secret_key)) =
        (&args.s3_bucket, &args.s3_access_key, &args.s3_secret_key)
    {
        let s3_cfg = S3StorageConfig {
            endpoint_url: args.s3_endpoint.clone(),
            region: args.s3_region.clone(),
            bucket: bucket.clone(),
            access_key_id: access_key.clone(),
            secret_access_key: secret_key.clone(),
            force_path_style: args.s3_endpoint.is_some(),
            key_prefix: None,
            max_concurrent_operations: args.max_s3_concurrent_ops,
        };
        service = service
            .with_s3_backend(s3_cfg)
            .await
            .map_err(|e| format!("failed to initialise S3 backend: {e}"))?;
        info!(bucket = %bucket, "runtime-coordinator S3 backend configured");
    }
    let metrics_service = service.clone();
    let coordinator_ready = Arc::new(AtomicBool::new(false));

    info!(%addr, "starting runtime-coordinator gRPC server");
    audit.emit(AuditEvent::new(
        AuditEventKind::CoordinatorStarted,
        AuditOutcome::Success,
    ));
    audit.emit(AuditEvent::new(
        AuditEventKind::AuthModeConfigured,
        AuditOutcome::Success,
    ));

    if let Some(status_addr) = &args.status_addr {
        let status_state = state.clone();
        let status_audit = audit.clone();
        let status_addr = status_addr.clone();
        std::thread::spawn(move || {
            if let Err(e) = serve_status_endpoint(
                status_addr,
                status_state,
                status_audit,
                auth_mode,
                tls_enabled,
                mtls_enabled,
                started_at,
            ) {
                tracing::warn!(error = %e, "status endpoint stopped");
            }
        });
    }

    if let Some(metrics_addr) = &args.metrics_addr {
        let metrics_service = metrics_service.clone();
        let metrics_audit = audit.clone();
        let metrics_addr = metrics_addr.clone();
        let metrics_ready = coordinator_ready.clone();
        std::thread::spawn(move || {
            if let Err(e) =
                serve_metrics_endpoint(metrics_addr, metrics_service, metrics_audit, metrics_ready)
            {
                tracing::warn!(error = %e, "metrics endpoint stopped");
            }
        });
    }

    let mut builder = Server::builder();
    if let Some(tls) = load_tls_config(&args, &audit)? {
        builder = builder.tls_config(tls)?;
    }

    coordinator_ready.store(true, Ordering::Release);

    builder
        .add_service(CoordinatorServiceServer::new(service))
        .serve(addr)
        .await?;

    info!(%addr, "runtime-coordinator server shut down gracefully");

    Ok(())
}

fn load_tls_config(
    args: &CoordinatorCliArgs,
    audit: &AuditLogger,
) -> Result<Option<ServerTlsConfig>, Box<dyn std::error::Error>> {
    match (&args.tls_cert, &args.tls_key) {
        (None, None) => {
            if args.tls_client_ca.is_some() {
                return Err("--tls-client-ca requires --tls-cert and --tls-key".into());
            }
            info!("runtime-coordinator transport security disabled; serving plaintext gRPC");
            audit.emit(AuditEvent::new(
                AuditEventKind::TlsEnabled,
                AuditOutcome::Failure,
            ));
            Ok(None)
        }
        (Some(_), None) => Err("--tls-cert requires --tls-key".into()),
        (None, Some(_)) => Err("--tls-key requires --tls-cert".into()),
        (Some(cert_path), Some(key_path)) => {
            let cert = fs::read(cert_path).map_err(|e| {
                format!(
                    "failed to read --tls-cert file {}: {e}",
                    cert_path.display()
                )
            })?;
            let key = fs::read(key_path).map_err(|e| {
                format!("failed to read --tls-key file {}: {e}", key_path.display())
            })?;

            let mut tls = ServerTlsConfig::new().identity(Identity::from_pem(cert, key));
            if let Some(ca_path) = &args.tls_client_ca {
                let ca = fs::read(ca_path).map_err(|e| {
                    format!(
                        "failed to read --tls-client-ca file {}: {e}",
                        ca_path.display()
                    )
                })?;
                tls = tls.client_ca_root(Certificate::from_pem(ca));
                info!(
                    "runtime-coordinator TLS enabled with client certificate verification (mTLS)"
                );
                audit.emit(AuditEvent::new(
                    AuditEventKind::MtlsEnabled,
                    AuditOutcome::Success,
                ));
                audit.emit(AuditEvent::new(
                    AuditEventKind::TlsEnabled,
                    AuditOutcome::Success,
                ));
            } else {
                info!("runtime-coordinator TLS enabled without client certificate verification");
                audit.emit(AuditEvent::new(
                    AuditEventKind::TlsEnabled,
                    AuditOutcome::Success,
                ));
            }
            Ok(Some(tls))
        }
    }
}

fn print_usage() {
    println!("Usage: runtime-coordinator [--addr <ADDR>] [--metadata-db <PATH>] [--metadata-db-url <URL>] [--storage-root <PATH>] [--auth-mode <disabled|metadata>] [--tls-cert <PATH> --tls-key <PATH> [--tls-client-ca <PATH>]] [--status-addr <ADDR>] [--metrics-addr <ADDR>] [--s3-endpoint <URL>] [--s3-bucket <NAME>] [--s3-access-key <KEY>] [--s3-secret-key <SECRET>] [--s3-region <REGION>] [--max-s3-concurrent-ops <N>] [--help|-h]");
    println!("\nOptions:");
    println!("  --addr <ADDR>              Coordinator listen address (default: {DEFAULT_ADDR})");
    println!("  --metadata-db <PATH>       Enable SQLite metadata persistence at PATH (single-node)");
    println!("  --metadata-db-url <URL>    PostgreSQL DSN for HA metadata (env: DTRUNTIME_METADATA_DB_URL)");
    println!("                             Example: postgres://user:pass@host:5432/dtruntime");
    println!("  --storage-root <PATH>      Local staging area for StreamCheckpointArtifact (required)");
    println!("  --auth-mode <MODE>         Authorization mode: disabled|metadata (env: DTRUNTIME_AUTH_MODE)");
    println!("  --tls-cert <PATH>          Enable TLS with certificate chain PEM at PATH (requires --tls-key)");
    println!(
        "  --tls-key <PATH>           Enable TLS with private key PEM at PATH (requires --tls-cert)"
    );
    println!("  --tls-client-ca <PATH>     Require mTLS client certs signed by CA PEM at PATH");
    println!("  --metrics-addr <ADDR>      Enable Prometheus /metrics endpoint at ADDR");
    println!("  --s3-endpoint <URL>        S3/MinIO endpoint URL (env: DTRUNTIME_S3_ENDPOINT)");
    println!("  --s3-bucket <NAME>         S3 bucket for checkpoint artifact upload (env: DTRUNTIME_S3_BUCKET)");
    println!("  --s3-access-key <KEY>      S3 access key ID (env: DTRUNTIME_S3_ACCESS_KEY)");
    println!("  --s3-secret-key <SECRET>   S3 secret access key (env: DTRUNTIME_S3_SECRET_KEY)");
    println!("  --s3-region <REGION>       S3 region (default: us-east-1, env: DTRUNTIME_S3_REGION)");
    println!("  --max-s3-concurrent-ops N  Max concurrent S3 operations (default: 64).");
    println!("                             Set >= world_size to avoid semaphore stalls at scale.");
    println!("  --no-verify-checkpoint-artifacts");
    println!("                             Disable post-write SHA-256 re-read of checkpoint");
    println!("                             artifacts (default: verification on).");
    println!("  --help, -h                 Show this help message");
}

fn coordinator_args(
    args: impl IntoIterator<Item = String>,
) -> Result<CoordinatorCliArgs, Box<dyn std::error::Error>> {
    let mut args = args.into_iter();
    let mut parsed = CoordinatorCliArgs {
        addr: String::from(DEFAULT_ADDR),
        metadata_db: None,
        metadata_db_url: std::env::var("DTRUNTIME_METADATA_DB_URL").ok(),
        storage_root: None,
        tls_cert: None,
        tls_key: None,
        tls_client_ca: None,
        auth_mode: std::env::var("DTRUNTIME_AUTH_MODE")
            .ok()
            .as_deref()
            .unwrap_or("disabled")
            .parse()
            .map_err(|e: String| format!("invalid DTRUNTIME_AUTH_MODE: {e}"))?,
        status_addr: None,
        metrics_addr: None,
        s3_endpoint: std::env::var("DTRUNTIME_S3_ENDPOINT").ok(),
        s3_bucket: std::env::var("DTRUNTIME_S3_BUCKET").ok(),
        s3_access_key: std::env::var("DTRUNTIME_S3_ACCESS_KEY").ok(),
        s3_secret_key: std::env::var("DTRUNTIME_S3_SECRET_KEY").ok(),
        s3_region: std::env::var("DTRUNTIME_S3_REGION")
            .unwrap_or_else(|_| "us-east-1".to_string()),
        max_s3_concurrent_ops: None,
        verify_checkpoint_artifacts: true,
        show_help: false,
    };

    while let Some(arg) = args.next() {
        if arg == "--addr" {
            parsed.addr = args.next().ok_or("--addr requires an address")?;
        } else if arg == "--metadata-db" {
            parsed.metadata_db = Some(args.next().ok_or("--metadata-db requires a path")?);
        } else if arg == "--storage-root" {
            parsed.storage_root = Some(PathBuf::from(
                args.next().ok_or("--storage-root requires a path")?,
            ));
        } else if arg == "--auth-mode" {
            parsed.auth_mode = args
                .next()
                .ok_or("--auth-mode requires a value")?
                .parse()
                .map_err(|e: String| format!("invalid --auth-mode value: {e}"))?;
        } else if arg == "--tls-cert" {
            parsed.tls_cert = Some(PathBuf::from(
                args.next().ok_or("--tls-cert requires a path")?,
            ));
        } else if arg == "--tls-key" {
            parsed.tls_key = Some(PathBuf::from(
                args.next().ok_or("--tls-key requires a path")?,
            ));
        } else if arg == "--tls-client-ca" {
            parsed.tls_client_ca = Some(PathBuf::from(
                args.next().ok_or("--tls-client-ca requires a path")?,
            ));
        } else if arg == "--status-addr" {
            parsed.status_addr = Some(args.next().ok_or("--status-addr requires an address")?);
        } else if arg == "--metrics-addr" {
            parsed.metrics_addr = Some(args.next().ok_or("--metrics-addr requires an address")?);
        } else if arg == "--s3-endpoint" {
            parsed.s3_endpoint = Some(args.next().ok_or("--s3-endpoint requires a URL")?);
        } else if arg == "--s3-bucket" {
            parsed.s3_bucket = Some(args.next().ok_or("--s3-bucket requires a name")?);
        } else if arg == "--s3-access-key" {
            parsed.s3_access_key = Some(args.next().ok_or("--s3-access-key requires a value")?);
        } else if arg == "--s3-secret-key" {
            parsed.s3_secret_key = Some(args.next().ok_or("--s3-secret-key requires a value")?);
        } else if arg == "--s3-region" {
            parsed.s3_region = args.next().ok_or("--s3-region requires a value")?;
        } else if arg == "--max-s3-concurrent-ops" {
            let raw = args
                .next()
                .ok_or("--max-s3-concurrent-ops requires a value")?;
            let n: usize = raw
                .parse()
                .map_err(|_| "--max-s3-concurrent-ops must be a positive integer")?;
            if n == 0 {
                return Err("--max-s3-concurrent-ops must be greater than zero".into());
            }
            parsed.max_s3_concurrent_ops = Some(n);
        } else if arg == "--metadata-db-url" {
            parsed.metadata_db_url =
                Some(args.next().ok_or("--metadata-db-url requires a URL")?);
        } else if arg == "--no-verify-checkpoint-artifacts" {
            parsed.verify_checkpoint_artifacts = false;
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
    fn default_args() -> Result<(), Box<dyn std::error::Error>> {
        let parsed = coordinator_args(Vec::<String>::new()).expect("default args should parse");
        assert_eq!(
            parsed,
            CoordinatorCliArgs {
                addr: DEFAULT_ADDR.to_string(),
                metadata_db: None,
                metadata_db_url: std::env::var("DTRUNTIME_METADATA_DB_URL").ok(),
                tls_cert: None,
                tls_key: None,
                tls_client_ca: None,
                auth_mode: std::env::var("DTRUNTIME_AUTH_MODE")
                    .ok()
                    .as_deref()
                    .unwrap_or("disabled")
                    .parse()
                    .map_err(|e: String| format!("invalid DTRUNTIME_AUTH_MODE: {e}"))?,
                status_addr: None,
                metrics_addr: None,
                storage_root: None,
                s3_endpoint: std::env::var("DTRUNTIME_S3_ENDPOINT").ok(),
                s3_bucket: std::env::var("DTRUNTIME_S3_BUCKET").ok(),
                s3_access_key: std::env::var("DTRUNTIME_S3_ACCESS_KEY").ok(),
                s3_secret_key: std::env::var("DTRUNTIME_S3_SECRET_KEY").ok(),
                s3_region: std::env::var("DTRUNTIME_S3_REGION")
                    .unwrap_or_else(|_| "us-east-1".to_string()),
                max_s3_concurrent_ops: None,
                verify_checkpoint_artifacts: true,
                show_help: false,
            }
        );
        let audit = AuditLogger::tracing();
        assert!(load_tls_config(&parsed, &audit)
            .expect("load tls")
            .is_none());
        Ok(())
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
    fn tls_cert_without_key_errors() {
        let parsed = coordinator_args(vec!["--tls-cert".to_string(), "server.crt".to_string()])
            .expect("args parse");
        let audit = AuditLogger::tracing();
        let err = load_tls_config(&parsed, &audit).expect_err("should error");
        assert_eq!(err.to_string(), "--tls-cert requires --tls-key");
    }

    #[test]
    fn tls_key_without_cert_errors() {
        let parsed = coordinator_args(vec!["--tls-key".to_string(), "server.key".to_string()])
            .expect("args parse");
        let audit = AuditLogger::tracing();
        let err = load_tls_config(&parsed, &audit).expect_err("should error");
        assert_eq!(err.to_string(), "--tls-key requires --tls-cert");
    }

    #[test]
    fn tls_cert_and_key_parse() {
        let parsed = coordinator_args(vec![
            "--tls-cert".to_string(),
            "missing.crt".to_string(),
            "--tls-key".to_string(),
            "missing.key".to_string(),
        ])
        .expect("args parse");
        assert!(parsed.tls_cert.is_some());
        assert!(parsed.tls_key.is_some());
        let audit = AuditLogger::tracing();
        let err = load_tls_config(&parsed, &audit).expect_err("file read should fail");
        assert!(err
            .to_string()
            .starts_with("failed to read --tls-cert file"));
    }

    #[test]
    fn tls_client_ca_without_server_tls_errors() {
        let parsed = coordinator_args(vec!["--tls-client-ca".to_string(), "ca.crt".to_string()])
            .expect("args parse");
        let audit = AuditLogger::tracing();
        let err = load_tls_config(&parsed, &audit).expect_err("should error");
        assert_eq!(
            err.to_string(),
            "--tls-client-ca requires --tls-cert and --tls-key"
        );
    }

    #[test]
    fn help_flag() {
        let parsed = coordinator_args(vec!["--help".to_string()]).expect("help should parse");
        assert!(parsed.show_help);
    }

    #[test]
    fn auth_mode_parse() {
        let parsed = coordinator_args(vec!["--auth-mode".to_string(), "metadata".to_string()])
            .expect("auth mode should parse");
        assert_eq!(parsed.auth_mode, AuthMode::Metadata);
    }
    #[test]
    fn unknown_arg_errors() {
        let err = coordinator_args(vec!["--wat".to_string()]).expect_err("should error");
        assert_eq!(err.to_string(), "unexpected argument: --wat");
    }
}

#[cfg(test)]
mod ha_lock_tests {
    use super::*;

    #[test]
    fn metadata_lock_prevents_second_coordinator() {
        let base = std::env::temp_dir().join(format!("dtruntime_lock_test_{}", std::process::id()));
        let db = base.join("coordinator.db");
        let _first = CoordinatorLockGuard::acquire(&db).expect("first lock");
        let second = CoordinatorLockGuard::acquire(&db);
        assert!(second.is_err());
    }
}

/// Strip credentials from a PostgreSQL DSN before emitting it to logs.
fn redact_pg_url(url: &str) -> String {
    if let (Some(scheme_end), Some(at_pos)) = (url.find("://"), url.rfind('@')) {
        let scheme = &url[..scheme_end + 3];
        let host_onwards = &url[at_pos..];
        format!("{scheme}***{host_onwards}")
    } else {
        url.to_string()
    }
}

fn serve_status_endpoint(
    addr: String,
    state: Arc<CoordinatorState>,
    audit: AuditLogger,
    auth_mode: AuthMode,
    tls_enabled: bool,
    mtls_enabled: bool,
    started_at: SystemTime,
) -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind(&addr)?;
    info!(status_addr = %addr, "runtime-coordinator status endpoint enabled");
    for stream in listener.incoming() {
        let mut stream = match stream {
            Ok(s) => s,
            Err(_) => continue,
        };
        let mut buf = [0_u8; 1024];
        let read = stream.read(&mut buf).unwrap_or(0);
        let req = String::from_utf8_lossy(&buf[..read]);
        let is_get_status = req.starts_with("GET /status ");
        let response = if is_get_status {
            let snapshot = state.status_snapshot();
            let uptime_secs = started_at.elapsed().map(|d| d.as_secs()).unwrap_or(0);
            serde_json::to_string(&serde_json::json!({
                "health": "ok",
                "auth_mode": format!("{:?}", auth_mode).to_ascii_lowercase(),
                "tls_enabled": tls_enabled,
                "mtls_enabled": mtls_enabled,
                "run_count": snapshot.run_count,
                "worker_count": snapshot.worker_count,
                "active_worker_count": snapshot.active_worker_count,
                "checkpoint_count": snapshot.checkpoint_count,
                "latest_checkpoint_id": snapshot.latest_checkpoint_id,
                "audit_event_count": audit.emitted_count(),
                "uptime_secs": uptime_secs
            }))
            .unwrap_or_else(|_| String::from("{\"health\":\"error\"}"))
        } else {
            String::from("method not allowed")
        };
        let header = if is_get_status {
            format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                response.len()
            )
        } else {
            format!(
                "HTTP/1.1 405 Method Not Allowed\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                response.len()
            )
        };
        let _ = stream.write_all(header.as_bytes());
        let _ = stream.write_all(response.as_bytes());
    }
    Ok(())
}

fn serve_metrics_endpoint(
    addr: String,
    service: CoordinatorServiceImpl,
    audit: AuditLogger,
    ready: Arc<AtomicBool>,
) -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind(&addr)?;
    info!(metrics_addr = %addr, "runtime-coordinator metrics endpoint enabled");
    for stream in listener.incoming() {
        let mut stream = match stream {
            Ok(s) => s,
            Err(_) => continue,
        };
        let mut buf = [0_u8; 1024];
        let read = stream.read(&mut buf).unwrap_or(0);
        let req = String::from_utf8_lossy(&buf[..read]);
        let response =
            build_metrics_http_response(&req, &service, &audit, ready.load(Ordering::Acquire));
        let _ = stream.write_all(response.as_bytes());
        let _ = stream.flush();
    }
    Ok(())
}

fn build_metrics_http_response(
    request: &str,
    service: &CoordinatorServiceImpl,
    audit: &AuditLogger,
    ready: bool,
) -> String {
    let is_get_metrics = request.starts_with("GET /metrics ");
    let (status, content_type, body) = if request.starts_with("GET /healthz ") {
        ("200 OK", "text/plain; charset=utf-8", String::from("ok\n"))
    } else if request.starts_with("GET /readyz ") {
        if ready {
            ("200 OK", "text/plain; charset=utf-8", String::from("ok\n"))
        } else {
            (
                "503 Service Unavailable",
                "text/plain; charset=utf-8",
                String::from("not ready\n"),
            )
        }
    } else if is_get_metrics {
        (
            "200 OK",
            "text/plain; version=0.0.4; charset=utf-8",
            render_prometheus_metrics(&service.metrics_snapshot(), audit.emitted_count()),
        )
    } else if request.starts_with("GET ") {
        (
            "404 Not Found",
            "text/plain; charset=utf-8",
            String::from("not found"),
        )
    } else {
        (
            "405 Method Not Allowed",
            "text/plain; charset=utf-8",
            String::from("method not allowed"),
        )
    };

    format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
}

#[cfg(test)]
mod metrics_http_tests {
    use super::*;

    fn test_service_and_audit() -> (CoordinatorServiceImpl, AuditLogger) {
        (
            CoordinatorServiceImpl::new(Arc::new(CoordinatorState::default())),
            AuditLogger::tracing(),
        )
    }

    #[test]
    fn metrics_http_response_is_strict_http_11() {
        let (service, audit) = test_service_and_audit();
        let response =
            build_metrics_http_response("GET /metrics HTTP/1.1\r\n\r\n", &service, &audit, true);

        assert!(response.starts_with("HTTP/1.1 200 OK\r\n"));
        assert!(response.contains("Content-Type: text/plain; version=0.0.4; charset=utf-8\r\n"));
        assert!(response.contains("Connection: close\r\n"));
        assert!(response.contains("# HELP dtruntime_runs_total"));
        assert!(response.contains("# TYPE dtruntime_runs_total counter"));
    }

    #[test]
    fn unknown_path_returns_404() {
        let (service, audit) = test_service_and_audit();
        let response =
            build_metrics_http_response("GET /wat HTTP/1.1\r\n\r\n", &service, &audit, true);

        assert!(response.starts_with("HTTP/1.1 404 Not Found\r\n"));
    }

    #[test]
    fn unsupported_method_returns_405() {
        let (service, audit) = test_service_and_audit();
        let response =
            build_metrics_http_response("POST /metrics HTTP/1.1\r\n\r\n", &service, &audit, true);

        assert!(response.starts_with("HTTP/1.1 405 Method Not Allowed\r\n"));
    }

    #[test]
    fn metrics_output_does_not_contain_secret_markers() {
        let (service, audit) = test_service_and_audit();
        let response =
            build_metrics_http_response("GET /metrics HTTP/1.1\r\n\r\n", &service, &audit, true);
        let body = response
            .split_once("\r\n\r\n")
            .map(|(_, body)| body)
            .unwrap_or_default();

        for marker in [
            "PRIVATE KEY",
            "BEGIN RSA",
            "BEGIN EC",
            "BEGIN OPENSSH",
            "AKIA",
            "xoxb-",
            "password=",
            "secret=",
            "token=",
        ] {
            assert!(
                !body.contains(marker),
                "metrics body leaked marker: {marker}"
            );
        }
    }

    #[test]
    fn healthz_returns_200_ok() {
        let (service, audit) = test_service_and_audit();
        let response =
            build_metrics_http_response("GET /healthz HTTP/1.1\r\n\r\n", &service, &audit, true);
        assert!(response.starts_with("HTTP/1.1 200 OK\r\n"));
        assert!(response.contains("Content-Type: text/plain; charset=utf-8\r\n"));
    }

    #[test]
    fn readyz_returns_200_when_ready() {
        let (service, audit) = test_service_and_audit();
        let response =
            build_metrics_http_response("GET /readyz HTTP/1.1\r\n\r\n", &service, &audit, true);
        assert!(response.starts_with("HTTP/1.1 200 OK\r\n"));
    }

    #[test]
    fn readyz_returns_503_when_not_ready() {
        let (service, audit) = test_service_and_audit();
        let response =
            build_metrics_http_response("GET /readyz HTTP/1.1\r\n\r\n", &service, &audit, false);
        assert!(response.starts_with("HTTP/1.1 503 Service Unavailable\r\n"));
    }

    #[test]
    fn health_response_does_not_contain_secret_markers() {
        let (service, audit) = test_service_and_audit();
        let response =
            build_metrics_http_response("GET /healthz HTTP/1.1\r\n\r\n", &service, &audit, true);
        let body = response
            .split_once("\r\n\r\n")
            .map(|(_, body)| body)
            .unwrap_or_default();
        for marker in ["PRIVATE KEY", "AKIA", "password=", "secret=", "token="] {
            assert!(
                !body.contains(marker),
                "health body leaked marker: {marker}"
            );
        }
    }
}
