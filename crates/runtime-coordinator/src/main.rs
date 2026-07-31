#![forbid(unsafe_code)]

use std::fs;
use std::io::{Read, Write};
use std::net::SocketAddr;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use clap::{ArgAction, Parser};

use runtime_coordinator::audit::{
    AuditEvent, AuditEventKind, AuditLogger, AuditOutcome, AuditSink, DbAuditSink, FanoutAuditSink,
    JsonLinesAuditSink, TracingAuditSink,
};
use runtime_coordinator::auth::{AuthMode, Authenticator};
use runtime_coordinator::ha_lock::CoordinatorLockGuard;
use runtime_coordinator::init_tracing_from_env;
use runtime_coordinator::persistence::PostgresMetadataStore;
use runtime_coordinator::persistence::{MetadataStore, SqliteMetadataStore};
use runtime_coordinator::security::jwt::{JwtConfig, JwtValidator};
use runtime_coordinator::security::keyset::{KeySource, ReloadableKeySet};
use runtime_coordinator::security::spiffe::SpiffePolicy;
use runtime_coordinator::service::CoordinatorServiceImpl;
use runtime_coordinator::state::{CoordinatorState, DatasetPlacementPolicy, ReclaimedCheckpoint};
use runtime_coordinator::telemetry::render_prometheus_metrics;
use runtime_coordinator::RuntimeMetrics;
use runtime_proto::runtime::v1::coordinator_service_server::CoordinatorServiceServer;
use runtime_storage::S3StorageConfig;
use tonic::transport::{Certificate, Identity, Server, ServerTlsConfig};
use tracing::info;

const DEFAULT_ADDR: &str = "127.0.0.1:50051";

/// The Rust control plane for the Distributed Training Runtime: worker
/// rendezvous, run/worker lifecycle, shard leases, and the two-phase
/// checkpoint commit, over gRPC.
///
/// Every flag has a `DTRUNTIME_*` environment equivalent, so the same binary
/// is configured identically from a shell, a systemd unit, or a container.
#[derive(Debug, PartialEq, Eq, Parser)]
#[command(name = "runtime-coordinator", version, about, long_about = None)]
struct CoordinatorCliArgs {
    /// Coordinator gRPC listen address.
    #[arg(long, default_value = DEFAULT_ADDR, value_name = "ADDR")]
    addr: String,

    /// Enable SQLite metadata persistence at this path (single-node).
    #[arg(long, value_name = "PATH")]
    metadata_db: Option<String>,

    /// PostgreSQL DSN for HA metadata. Takes precedence over --metadata-db.
    #[arg(long, env = "DTRUNTIME_METADATA_DB_URL", value_name = "URL")]
    metadata_db_url: Option<String>,

    /// Root directory for checkpoint artifacts written through the coordinator.
    #[arg(long, value_name = "PATH")]
    storage_root: Option<PathBuf>,

    // ── TLS / mTLS ──
    /// Server certificate chain (PEM); requires --tls-key.
    #[arg(long, value_name = "PATH")]
    tls_cert: Option<PathBuf>,

    /// Server private key (PEM); requires --tls-cert.
    #[arg(long, value_name = "PATH")]
    tls_key: Option<PathBuf>,

    /// Client CA bundle (PEM). Setting it requires mTLS from every client.
    #[arg(long, value_name = "PATH")]
    tls_client_ca: Option<PathBuf>,

    /// Whether authorization is enforced.
    #[arg(
        long,
        env = "DTRUNTIME_AUTH_MODE",
        default_value = "disabled",
        value_parser = parse_auth_mode,
        value_name = "disabled|metadata"
    )]
    auth_mode: AuthMode,

    // ── OIDC / JWT bearer-token validation ──
    /// Required token issuer (`iss`); recommended for production.
    #[arg(long, env = "DTRUNTIME_OIDC_ISSUER", value_name = "ISS")]
    oidc_issuer: Option<String>,

    /// Accepted token audience (`aud`). Repeat, or pass a comma-separated list.
    #[arg(
        long = "oidc-audience",
        env = "DTRUNTIME_OIDC_AUDIENCE",
        value_delimiter = ',',
        value_name = "AUD"
    )]
    oidc_audiences: Vec<String>,

    /// JWKS document to validate RS/ES/EdDSA (or oct) signatures against.
    #[arg(long, env = "DTRUNTIME_OIDC_JWKS_FILE", value_name = "PATH")]
    oidc_jwks_file: Option<PathBuf>,

    /// Raw HMAC shared-secret file (HS256) — dev/simple deployments.
    #[arg(long, env = "DTRUNTIME_OIDC_HMAC_SECRET_FILE", value_name = "PATH")]
    oidc_hmac_secret_file: Option<PathBuf>,

    /// PEM public key file for a single asymmetric key.
    #[arg(long, env = "DTRUNTIME_OIDC_PUBLIC_KEY_FILE", value_name = "PATH")]
    oidc_public_key_file: Option<PathBuf>,

    /// Algorithm for --oidc-hmac-secret-file / --oidc-public-key-file.
    #[arg(long, env = "DTRUNTIME_OIDC_ALGORITHM", value_name = "ALG")]
    oidc_algorithm: Option<String>,

    /// Claim carrying roles/groups (default `roles`).
    #[arg(long, env = "DTRUNTIME_OIDC_ROLE_CLAIM", value_name = "CLAIM")]
    oidc_role_claim: Option<String>,

    // ── SPIFFE / SPIRE workload identity (via mTLS SVID) ──
    /// Required SPIFFE trust domain; enables SPIFFE auth when set.
    #[arg(long, env = "DTRUNTIME_SPIFFE_TRUST_DOMAIN", value_name = "DOMAIN")]
    spiffe_trust_domain: Option<String>,

    /// File mapping SPIFFE paths to roles (`pattern = role[,role]`).
    #[arg(long, env = "DTRUNTIME_SPIFFE_ROLE_MAP_FILE", value_name = "PATH")]
    spiffe_role_map_file: Option<PathBuf>,

    /// Reject dev metadata-header identities (production hardening).
    #[arg(
        long = "auth-disable-header-identity",
        env = "DTRUNTIME_AUTH_DISABLE_HEADER_IDENTITY",
        action = ArgAction::SetTrue
    )]
    disable_header_identity: bool,

    /// Append structured JSON (NDJSON/SIEM) audit records to this file.
    #[arg(long, env = "DTRUNTIME_AUDIT_LOG_FILE", value_name = "PATH")]
    audit_log_file: Option<PathBuf>,

    // ── operational endpoints ──
    /// Serve the JSON /status endpoint on this address.
    #[arg(long, value_name = "ADDR")]
    status_addr: Option<String>,

    /// Serve Prometheus /metrics, /healthz, and /readyz on this address.
    #[arg(long, value_name = "ADDR")]
    metrics_addr: Option<String>,

    // ── S3-compatible object storage ──
    /// S3 endpoint URL (e.g. a MinIO server).
    #[arg(long, env = "DTRUNTIME_S3_ENDPOINT", value_name = "URL")]
    s3_endpoint: Option<String>,

    /// S3 bucket for checkpoint artifacts.
    #[arg(long, env = "DTRUNTIME_S3_BUCKET", value_name = "NAME")]
    s3_bucket: Option<String>,

    /// S3 access key id.
    #[arg(long, env = "DTRUNTIME_S3_ACCESS_KEY", value_name = "KEY")]
    s3_access_key: Option<String>,

    /// S3 secret access key.
    #[arg(long, env = "DTRUNTIME_S3_SECRET_KEY", value_name = "SECRET")]
    s3_secret_key: Option<String>,

    /// S3 region.
    #[arg(
        long,
        env = "DTRUNTIME_S3_REGION",
        default_value = "us-east-1",
        value_name = "REGION"
    )]
    s3_region: String,

    /// Cap concurrent S3 operations (multipart parts, batch gets).
    #[arg(long, value_parser = parse_nonzero_usize, value_name = "N")]
    max_s3_concurrent_ops: Option<usize>,

    /// Server-side encryption for uploads: `AES256` (SSE-S3) or `aws:kms`.
    #[arg(long, env = "DTRUNTIME_S3_SSE", value_name = "MODE")]
    s3_sse: Option<String>,

    /// KMS key id/ARN for SSE-KMS.
    #[arg(long, env = "DTRUNTIME_S3_SSE_KMS_KEY_ID", value_name = "ID")]
    s3_sse_kms_key_id: Option<String>,

    /// Skip re-reading each streamed artifact to verify its SHA-256 after
    /// write. Verification is on by default; disable it only when storage
    /// integrity is guaranteed some other way.
    #[arg(long = "no-verify-checkpoint-artifacts", action = ArgAction::SetFalse)]
    verify_checkpoint_artifacts: bool,

    // ── background reconcile loop ──
    /// How often the reconcile task runs (liveness, stuck-run detection,
    /// retention cleanup).
    #[arg(long, default_value_t = 10, value_name = "N")]
    reconcile_interval_secs: u64,

    /// A non-terminal run with no worker heartbeat for this long is marked
    /// FAILED ("stuck").
    #[arg(long, default_value_t = 1800, value_name = "N")]
    stuck_after_secs: u64,

    /// How long terminal runs keep their in-memory state before cleanup
    /// (durable rows are retained).
    #[arg(long, default_value_t = 24 * 60 * 60, value_name = "N")]
    terminal_run_retention_secs: u64,

    /// Keep the newest N committed checkpoints per run and garbage-collect the
    /// rest (records + local artifacts). 0 disables.
    #[arg(long, default_value_t = 0, value_name = "N")]
    checkpoint_keep_last: usize,

    /// Reorder each worker's shard assignments to prefer shards it reports as
    /// locally cached (the `x-dtruntime-cached-shards` gRPC header).
    #[arg(long, action = ArgAction::SetTrue)]
    cache_aware_placement: bool,
}

fn parse_auth_mode(value: &str) -> Result<AuthMode, String> {
    value.parse()
}

fn parse_nonzero_usize(value: &str) -> Result<usize, String> {
    match value.parse::<usize>() {
        Ok(0) => Err("must be greater than zero".to_string()),
        Ok(n) => Ok(n),
        Err(_) => Err("must be a positive integer".to_string()),
    }
}

/// Parse coordinator arguments, excluding the program name.
///
/// Split out from [`CoordinatorCliArgs::parse`] so tests can drive parsing
/// with an explicit argument vector.
fn coordinator_args(
    args: impl IntoIterator<Item = String>,
) -> Result<CoordinatorCliArgs, clap::Error> {
    CoordinatorCliArgs::try_parse_from(
        std::iter::once("runtime-coordinator".to_string()).chain(args),
    )
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // The dependency tree enables both rustls crypto providers (ring +
    // aws-lc-rs), so rustls 0.23 cannot auto-select one and panics when TLS is
    // built. Install the ring provider explicitly before any TLS is set up.
    // (install_default errors only if a provider is already installed.)
    let _ = rustls::crypto::ring::default_provider().install_default();

    init_tracing_from_env();

    // Same parse path the tests drive; `exit` renders clap's --help/--version
    // and usage errors and terminates with the conventional exit code.
    let args = coordinator_args(std::env::args().skip(1)).unwrap_or_else(|e| e.exit());

    // Collect audit sinks; the durable DB sink is appended once the metadata
    // store is initialised below, and the JSON/SIEM sink when --audit-log-file
    // is set. The composed logger is built after state init (before any RPCs).
    let mut audit_sinks: Vec<Arc<dyn AuditSink>> = Vec::new();

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

        // PostgreSQL advisory lock for coordinator HA.
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
        // wire the durable DB audit sink now that the store is ready.
        audit_sinks.push(Arc::new(DbAuditSink::new(Arc::clone(&store))));
        CoordinatorState::with_metadata_store(store)
    } else if let Some(ref path) = args.metadata_db {
        // SQLite: single-node, local dev / small deployments.
        info!(metadata_db_path = %path, "runtime-coordinator using sqlite metadata store");
        let store = SqliteMetadataStore::open(path)?;
        store.init_schema()?;
        let store: Arc<dyn MetadataStore> = Arc::new(store);
        // wire the durable DB audit sink.
        audit_sinks.push(Arc::new(DbAuditSink::new(Arc::clone(&store))));
        CoordinatorState::with_metadata_store(store)
    } else {
        CoordinatorState::default()
    };
    let state = {
        let mut state = state;
        if args.cache_aware_placement {
            info!("dataset placement policy: cache-aware (workers' cached-shard hints reorder their assignments)");
            state.set_placement_policy(DatasetPlacementPolicy::CacheAware);
        }
        Arc::new(state)
    };
    let started_at = SystemTime::now();
    let tls_enabled = args.tls_cert.is_some();
    let mtls_enabled = args.tls_client_ca.is_some();
    let auth_mode = args.auth_mode;

    // Compose the audit pipeline: durable DB sink (if a store is configured) +
    // optional JSON/NDJSON SIEM file sink, always with a tracing fallback.
    if let Some(ref path) = args.audit_log_file {
        match JsonLinesAuditSink::to_file(path) {
            Ok(sink) => {
                info!(path = %path.display(), "structured JSON audit log (SIEM) enabled");
                audit_sinks.push(Arc::new(sink));
            }
            Err(e) => {
                return Err(
                    format!("failed to open --audit-log-file {}: {e}", path.display()).into(),
                )
            }
        }
    }
    if audit_sinks.is_empty() {
        audit_sinks.push(Arc::new(TracingAuditSink));
    }
    let audit = AuditLogger::new(Arc::new(FanoutAuditSink::new(audit_sinks)));

    // Build the layered authenticator (JWT / SPIFFE / dev metadata headers).
    let authenticator = build_authenticator(&args, &audit)?;
    let metrics = Arc::new(RuntimeMetrics::default());
    let mut service = CoordinatorServiceImpl::with_metrics_authorizer_and_audit(
        state.clone(),
        metrics.clone(),
        authenticator,
        audit.clone(),
    )
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
            server_side_encryption: args.s3_sse.clone(),
            sse_kms_key_id: args.s3_sse_kms_key_id.clone(),
            ..Default::default()
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
            let result = serve_operational_endpoint(status_addr, "status", |req| {
                build_status_http_response(
                    req,
                    &status_state,
                    &status_audit,
                    auth_mode,
                    tls_enabled,
                    mtls_enabled,
                    started_at,
                )
            });
            if let Err(e) = result {
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
            let result = serve_operational_endpoint(metrics_addr, "metrics", |req| {
                build_metrics_http_response(
                    req,
                    &metrics_service,
                    &metrics_audit,
                    metrics_ready.load(Ordering::Acquire),
                )
            });
            if let Err(e) = result {
                tracing::warn!(error = %e, "metrics endpoint stopped");
            }
        });
    }

    // Failure-semantics reconcile loop: worker liveness, stuck-run detection,
    // and retention cleanup of terminal runs run continuously rather than
    // only piggybacked on RPCs, so failures are detected even when no client
    // is polling.
    {
        let reconcile_state = state.clone();
        let reconcile_metrics = metrics.clone();
        let interval = std::time::Duration::from_secs(args.reconcile_interval_secs.max(1));
        let stuck_after = std::time::Duration::from_secs(args.stuck_after_secs.max(1));
        let retention = std::time::Duration::from_secs(args.terminal_run_retention_secs.max(1));
        let checkpoint_keep_last = args.checkpoint_keep_last;
        let gc_storage_root = args.storage_root.clone();
        // Share the service's S3 client rather than building a second one, so
        // the sweep reuses its connection pool and concurrency limiter.
        let gc_s3 = metrics_service.s3_backend();
        info!(
            interval_secs = interval.as_secs(),
            stuck_after_secs = stuck_after.as_secs(),
            terminal_run_retention_secs = retention.as_secs(),
            checkpoint_keep_last,
            "starting periodic reconcile task"
        );
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                ticker.tick().await;
                reconcile_state.reconcile_worker_liveness();
                // Heartbeats only refresh the worker gauges on a state flip;
                // this periodic recount keeps them honest for workers that
                // died silently and will never heartbeat again.
                let (active, stale, failed) = reconcile_state.global_worker_counts();
                reconcile_metrics.set_active_workers(active);
                reconcile_metrics.set_stale_workers(stale);
                reconcile_metrics.set_failed_workers(failed);
                let stuck = reconcile_state.detect_stuck_runs(stuck_after);
                if stuck > 0 {
                    tracing::warn!(count = stuck, "marked stuck run(s) as failed");
                }
                reconcile_state.cleanup_terminal_runs(retention);

                // Checkpoint retention: drop records beyond keep-last, then
                // reclaim their bytes. Retention only deletes metadata, so
                // whatever is not deleted here leaks permanently — after the
                // records are gone nothing knows the artifacts exist.
                if checkpoint_keep_last > 0 {
                    for reclaimed in
                        reconcile_state.enforce_checkpoint_retention(checkpoint_keep_last)
                    {
                        reclaim_checkpoint_artifacts(
                            &reclaimed,
                            gc_storage_root.as_ref(),
                            gc_s3.as_deref(),
                        )
                        .await;
                    }
                }
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

/// Resolve the JWT verification key source from the OIDC flags, if any.
fn jwt_key_source(
    args: &CoordinatorCliArgs,
) -> Result<Option<KeySource>, Box<dyn std::error::Error>> {
    let algorithm = |default: jsonwebtoken::Algorithm| -> Result<jsonwebtoken::Algorithm, Box<dyn std::error::Error>> {
        match &args.oidc_algorithm {
            Some(a) => a
                .parse()
                .map_err(|_| format!("invalid --oidc-algorithm: {a}").into()),
            None => Ok(default),
        }
    };
    if let Some(path) = &args.oidc_jwks_file {
        return Ok(Some(KeySource::Jwks { path: path.clone() }));
    }
    if let Some(path) = &args.oidc_hmac_secret_file {
        return Ok(Some(KeySource::HmacSecret {
            path: path.clone(),
            algorithm: algorithm(jsonwebtoken::Algorithm::HS256)?,
        }));
    }
    if let Some(path) = &args.oidc_public_key_file {
        return Ok(Some(KeySource::PublicKeyPem {
            path: path.clone(),
            algorithm: algorithm(jsonwebtoken::Algorithm::RS256)?,
        }));
    }
    if args.oidc_issuer.is_some() || !args.oidc_audiences.is_empty() {
        return Err("OIDC issuer/audience configured but no signing key source \
             (--oidc-jwks-file / --oidc-hmac-secret-file / --oidc-public-key-file)"
            .into());
    }
    Ok(None)
}

/// Build the layered authenticator from the auth/OIDC/SPIFFE flags.
fn build_authenticator(
    args: &CoordinatorCliArgs,
    audit: &AuditLogger,
) -> Result<Authenticator, Box<dyn std::error::Error>> {
    let mut auth = Authenticator::new(args.auth_mode);

    if let Some(source) = jwt_key_source(args)? {
        let keys = Arc::new(
            ReloadableKeySet::load(source).map_err(|e| format!("OIDC key load failed: {e}"))?,
        );
        let mut cfg = JwtConfig {
            issuer: args.oidc_issuer.clone(),
            audiences: args.oidc_audiences.clone(),
            ..Default::default()
        };
        if let Some(rc) = &args.oidc_role_claim {
            cfg.role_claim = rc.clone();
        }
        auth = auth.with_jwt(Arc::new(JwtValidator::new(cfg, keys)));
        info!("OIDC/JWT bearer-token authentication enabled");
        audit.emit(AuditEvent::new(
            AuditEventKind::AuthModeConfigured,
            AuditOutcome::Success,
        ));
    }

    if let Some(td) = &args.spiffe_trust_domain {
        let path = args
            .spiffe_role_map_file
            .as_ref()
            .ok_or("--spiffe-trust-domain requires --spiffe-role-map-file")?;
        let text = std::fs::read_to_string(path).map_err(|e| {
            format!(
                "failed to read --spiffe-role-map-file {}: {e}",
                path.display()
            )
        })?;
        let rules = SpiffePolicy::parse_role_rules(&text)
            .map_err(|e| format!("invalid SPIFFE role map: {e}"))?;
        auth = auth.with_spiffe(Arc::new(SpiffePolicy::new(td.clone(), rules)));
        info!(trust_domain = %td, "SPIFFE/SPIRE mTLS workload identity enabled");
        audit.emit(AuditEvent::new(
            AuditEventKind::AuthModeConfigured,
            AuditOutcome::Success,
        ));
    }

    if args.disable_header_identity {
        auth = auth.with_metadata_fallback(false);
        info!("dev metadata-header identity fallback DISABLED (production hardening)");
    }

    Ok(auth)
}

/// Delete the bytes behind a checkpoint the retention sweep just dropped.
///
/// Artifacts reach three different places depending on how they were written,
/// so each URI is routed by scheme:
///
/// * `s3://…` — the worker-direct path, deleted through the coordinator's S3
///   client. Without this the objects would leak forever: retention has
///   already removed the only records naming them.
/// * `file://…` or a bare absolute path — a worker-direct write to a shared
///   filesystem.
/// * anything else — a coordinator-relative storage key, covered by removing
///   the run's checkpoint directory below.
///
/// Failures are logged, never fatal: the sweep is periodic and idempotent, so
/// a transient S3 error simply retries on the next tick. Deleting an object
/// that is already gone is not an error.
async fn reclaim_checkpoint_artifacts(
    reclaimed: &ReclaimedCheckpoint,
    storage_root: Option<&PathBuf>,
    s3: Option<&runtime_storage::S3Storage>,
) {
    for uri in &reclaimed.artifact_uris {
        if uri.starts_with("s3://") {
            let Some(s3) = s3 else {
                tracing::warn!(
                    run_id = %reclaimed.run_id,
                    checkpoint_id = %reclaimed.checkpoint_id,
                    %uri,
                    "checkpoint retention: S3 artifact cannot be reclaimed — no S3 backend \
                     is configured on this coordinator; delete it via a bucket lifecycle rule"
                );
                continue;
            };
            // The key is addressed through the configured client, so its
            // bucket (and any key prefix) applies.
            let key = runtime_storage::uri::parse_s3_uri(uri)
                .map_err(|e| e.to_string())
                .and_then(|(_bucket, key)| {
                    runtime_storage::StorageKey::new(&key).map_err(|e| e.to_string())
                });
            match key {
                Ok(key) => {
                    if let Err(e) = s3.delete_async(&key).await {
                        tracing::warn!(
                            run_id = %reclaimed.run_id,
                            checkpoint_id = %reclaimed.checkpoint_id,
                            %uri,
                            error = %e,
                            "checkpoint retention: S3 artifact delete failed (retries next sweep)"
                        );
                    }
                }
                Err(e) => tracing::warn!(
                    run_id = %reclaimed.run_id, %uri, error = %e,
                    "checkpoint retention: unusable S3 artifact URI, skipping"
                ),
            }
        } else if uri.starts_with("file://") || Path::new(uri).is_absolute() {
            let path = runtime_storage::uri::uri_to_local_path(uri);
            if let Err(e) = tokio::fs::remove_file(&path).await {
                if e.kind() != std::io::ErrorKind::NotFound {
                    tracing::warn!(
                        run_id = %reclaimed.run_id,
                        path = %path.display(),
                        error = %e,
                        "checkpoint retention: artifact file removal failed"
                    );
                }
            }
        }
    }

    // Artifacts streamed through the coordinator live under its storage root,
    // one directory per checkpoint.
    if let Some(root) = storage_root {
        let mut dir = root.clone();
        dir.push("runs");
        dir.push(&reclaimed.run_id);
        dir.push("checkpoints");
        dir.push(&reclaimed.checkpoint_id);
        if let Err(e) = tokio::fs::remove_dir_all(&dir).await {
            if e.kind() != std::io::ErrorKind::NotFound {
                tracing::warn!(
                    path = %dir.display(),
                    error = %e,
                    "checkpoint retention: artifact directory removal failed"
                );
            }
        }
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

/// How long a client has to send its request line before we drop it.
///
/// Without this a single client that opens a socket and never writes would
/// block the accept loop forever, taking `/healthz` and `/readyz` down with
/// it — which a Kubernetes liveness probe would (correctly) read as a dead
/// coordinator. These endpoints serve short, self-contained GETs, so a couple
/// of seconds is generous.
const STATUS_IO_TIMEOUT: Duration = Duration::from_secs(5);

/// Minimal blocking HTTP/1.1 server for the operational endpoints.
///
/// Deliberately hand-rolled and single-threaded rather than another async
/// stack: these serve infrequent, tiny, unauthenticated GETs (Prometheus
/// scrapes and kubelet probes) on a separate port from the gRPC API, and
/// keeping them off the tokio runtime means a wedged scrape can never
/// interfere with worker RPCs. `handler` maps a raw request head to a
/// complete response.
fn serve_operational_endpoint(
    addr: String,
    label: &'static str,
    handler: impl Fn(&str) -> String,
) -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind(&addr)?;
    info!(endpoint = label, %addr, "runtime-coordinator operational endpoint enabled");
    for stream in listener.incoming() {
        let mut stream = match stream {
            Ok(s) => s,
            Err(_) => continue,
        };
        // Both directions are bounded: a client that connects and stalls, or
        // one that stops reading mid-response, is dropped instead of parking
        // the loop.
        let _ = stream.set_read_timeout(Some(STATUS_IO_TIMEOUT));
        let _ = stream.set_write_timeout(Some(STATUS_IO_TIMEOUT));

        let mut buf = [0_u8; 1024];
        let read = match stream.read(&mut buf) {
            Ok(n) => n,
            Err(_) => continue, // timed out or reset — nothing to reply to
        };
        let response = handler(&String::from_utf8_lossy(&buf[..read]));
        let _ = stream.write_all(response.as_bytes());
        let _ = stream.flush();
    }
    Ok(())
}

/// Frame a complete HTTP/1.1 response. `Connection: close` because every
/// handler here serves one request per connection.
fn http_response(status: &str, content_type: &str, body: &str) -> String {
    format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
}

const TEXT_PLAIN: &str = "text/plain; charset=utf-8";

fn build_status_http_response(
    request: &str,
    state: &CoordinatorState,
    audit: &AuditLogger,
    auth_mode: AuthMode,
    tls_enabled: bool,
    mtls_enabled: bool,
    started_at: SystemTime,
) -> String {
    if !request.starts_with("GET ") {
        return http_response("405 Method Not Allowed", TEXT_PLAIN, "method not allowed");
    }
    if !request.starts_with("GET /status ") {
        return http_response("404 Not Found", TEXT_PLAIN, "not found");
    }

    let snapshot = state.status_snapshot();
    let uptime_secs = started_at.elapsed().map(|d| d.as_secs()).unwrap_or(0);
    let body = serde_json::to_string(&serde_json::json!({
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
    .unwrap_or_else(|_| String::from("{\"health\":\"error\"}"));
    http_response("200 OK", "application/json", &body)
}

fn build_metrics_http_response(
    request: &str,
    service: &CoordinatorServiceImpl,
    audit: &AuditLogger,
    ready: bool,
) -> String {
    if request.starts_with("GET /healthz ") {
        return http_response("200 OK", TEXT_PLAIN, "ok\n");
    }
    if request.starts_with("GET /readyz ") {
        return if ready {
            http_response("200 OK", TEXT_PLAIN, "ok\n")
        } else {
            http_response("503 Service Unavailable", TEXT_PLAIN, "not ready\n")
        };
    }
    if request.starts_with("GET /metrics ") {
        let body = render_prometheus_metrics(&service.metrics_snapshot(), audit.emitted_count());
        return http_response("200 OK", "text/plain; version=0.0.4; charset=utf-8", &body);
    }
    if request.starts_with("GET ") {
        return http_response("404 Not Found", TEXT_PLAIN, "not found");
    }
    http_response("405 Method Not Allowed", TEXT_PLAIN, "method not allowed")
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
                oidc_issuer: std::env::var("DTRUNTIME_OIDC_ISSUER").ok(),
                oidc_audiences: std::env::var("DTRUNTIME_OIDC_AUDIENCE")
                    .ok()
                    .map(|v| {
                        v.split(',')
                            .map(|s| s.trim().to_string())
                            .filter(|s| !s.is_empty())
                            .collect()
                    })
                    .unwrap_or_default(),
                oidc_jwks_file: std::env::var("DTRUNTIME_OIDC_JWKS_FILE")
                    .ok()
                    .map(PathBuf::from),
                oidc_hmac_secret_file: std::env::var("DTRUNTIME_OIDC_HMAC_SECRET_FILE")
                    .ok()
                    .map(PathBuf::from),
                oidc_public_key_file: std::env::var("DTRUNTIME_OIDC_PUBLIC_KEY_FILE")
                    .ok()
                    .map(PathBuf::from),
                oidc_algorithm: std::env::var("DTRUNTIME_OIDC_ALGORITHM").ok(),
                oidc_role_claim: std::env::var("DTRUNTIME_OIDC_ROLE_CLAIM").ok(),
                spiffe_trust_domain: std::env::var("DTRUNTIME_SPIFFE_TRUST_DOMAIN").ok(),
                spiffe_role_map_file: std::env::var("DTRUNTIME_SPIFFE_ROLE_MAP_FILE")
                    .ok()
                    .map(PathBuf::from),
                disable_header_identity: std::env::var("DTRUNTIME_AUTH_DISABLE_HEADER_IDENTITY")
                    .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
                    .unwrap_or(false),
                audit_log_file: std::env::var("DTRUNTIME_AUDIT_LOG_FILE")
                    .ok()
                    .map(PathBuf::from),
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
                s3_sse: std::env::var("DTRUNTIME_S3_SSE").ok(),
                s3_sse_kms_key_id: std::env::var("DTRUNTIME_S3_SSE_KMS_KEY_ID").ok(),
                verify_checkpoint_artifacts: true,
                reconcile_interval_secs: 10,
                stuck_after_secs: 1800,
                terminal_run_retention_secs: 24 * 60 * 60,
                checkpoint_keep_last: 0,
                cache_aware_placement: false,
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
    fn help_flag_renders_usage_for_every_option() {
        let err = coordinator_args(vec!["--help".to_string()]).expect_err("--help short-circuits");
        assert_eq!(err.kind(), clap::error::ErrorKind::DisplayHelp);

        // The help text is generated from the struct, so it cannot drift out of
        // sync with the flags the way the previous hand-written usage could.
        let help = err.to_string();
        for flag in [
            "--addr",
            "--metadata-db",
            "--metadata-db-url",
            "--auth-mode",
            "--oidc-issuer",
            "--spiffe-trust-domain",
            "--tls-cert",
            "--metrics-addr",
            "--s3-bucket",
            "--checkpoint-keep-last",
            "--no-verify-checkpoint-artifacts",
        ] {
            assert!(help.contains(flag), "help text is missing {flag}");
        }
    }

    #[test]
    fn auth_mode_parse() {
        let parsed = coordinator_args(vec!["--auth-mode".to_string(), "metadata".to_string()])
            .expect("auth mode should parse");
        assert_eq!(parsed.auth_mode, AuthMode::Metadata);
    }

    #[test]
    fn invalid_auth_mode_is_rejected() {
        let err = coordinator_args(vec!["--auth-mode".to_string(), "banana".to_string()])
            .expect_err("should error");
        assert!(
            err.to_string().contains("unsupported auth mode"),
            "unhelpful error: {err}"
        );
    }

    #[test]
    fn unknown_arg_errors() {
        let err = coordinator_args(vec!["--wat".to_string()]).expect_err("should error");
        assert_eq!(err.kind(), clap::error::ErrorKind::UnknownArgument);
        assert!(err.to_string().contains("--wat"), "unhelpful error: {err}");
    }

    #[test]
    fn missing_flag_value_errors() {
        let err = coordinator_args(vec!["--addr".to_string()]).expect_err("should error");
        assert_eq!(err.kind(), clap::error::ErrorKind::InvalidValue);
    }

    #[test]
    fn no_verify_flag_clears_the_default() {
        let on = coordinator_args(Vec::new()).expect("defaults");
        assert!(
            on.verify_checkpoint_artifacts,
            "verification is on by default"
        );

        let off = coordinator_args(vec!["--no-verify-checkpoint-artifacts".to_string()])
            .expect("flag should parse");
        assert!(!off.verify_checkpoint_artifacts);
    }

    #[test]
    fn oidc_audience_repeats_and_splits() {
        let parsed = coordinator_args(vec![
            "--oidc-audience".to_string(),
            "api".to_string(),
            "--oidc-audience".to_string(),
            "cli,web".to_string(),
        ])
        .expect("audiences should parse");
        assert_eq!(parsed.oidc_audiences, vec!["api", "cli", "web"]);
    }

    #[test]
    fn max_s3_concurrent_ops_rejects_zero() {
        let err = coordinator_args(vec!["--max-s3-concurrent-ops".to_string(), "0".to_string()])
            .expect_err("zero should be rejected");
        assert!(
            err.to_string().contains("greater than zero"),
            "unhelpful error: {err}"
        );
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

    #[test]
    fn metadata_lock_is_reacquirable_after_release() {
        // The OS lock dies with the holder (even on SIGKILL), so a stale lock
        // FILE on disk must never block a restarted coordinator.
        let base = std::env::temp_dir().join(format!(
            "dtruntime_lock_test_release_{}",
            std::process::id()
        ));
        let db = base.join("coordinator.db");
        let first = CoordinatorLockGuard::acquire(&db).expect("first lock");
        let lock_path = first.path().to_path_buf();
        drop(first);
        assert!(
            lock_path.exists(),
            "lock file remains on disk after release"
        );
        let second = CoordinatorLockGuard::acquire(&db);
        assert!(
            second.is_ok(),
            "lock must be reacquirable once the previous holder is gone"
        );
    }
}

#[cfg(test)]
mod retention_gc_tests {
    use super::*;

    fn reclaimed(uris: &[&str]) -> ReclaimedCheckpoint {
        ReclaimedCheckpoint {
            run_id: "run-1".to_string(),
            checkpoint_id: "ckpt-1".to_string(),
            artifact_uris: uris.iter().map(|s| s.to_string()).collect(),
        }
    }

    #[tokio::test]
    async fn reclaims_worker_direct_files_and_the_coordinator_directory() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_path_buf();

        // A worker-direct artifact written to a shared filesystem.
        let direct = root.join("direct-artifact.bin");
        std::fs::write(&direct, b"bytes").unwrap();

        // An artifact streamed through the coordinator lives under its root.
        let coord_dir = root
            .join("runs")
            .join("run-1")
            .join("checkpoints")
            .join("ckpt-1");
        std::fs::create_dir_all(&coord_dir).unwrap();
        std::fs::write(coord_dir.join("model.bin"), b"bytes").unwrap();

        // A sibling checkpoint that must be left alone.
        let keep = root
            .join("runs")
            .join("run-1")
            .join("checkpoints")
            .join("ckpt-2");
        std::fs::create_dir_all(&keep).unwrap();
        std::fs::write(keep.join("model.bin"), b"bytes").unwrap();

        let uri = format!(
            "file:///{}",
            direct.display().to_string().replace('\\', "/")
        );
        reclaim_checkpoint_artifacts(&reclaimed(&[&uri]), Some(&root), None).await;

        assert!(!direct.exists(), "worker-direct file was not reclaimed");
        assert!(
            !coord_dir.exists(),
            "coordinator artifact dir was not reclaimed"
        );
        assert!(keep.exists(), "retention must not touch other checkpoints");
    }

    /// The sweep runs every reconcile tick; a re-run after a partial failure
    /// must converge rather than error.
    #[tokio::test]
    async fn reclaiming_already_deleted_artifacts_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_path_buf();
        let missing = root.join("gone.bin");
        let uri = format!(
            "file:///{}",
            missing.display().to_string().replace('\\', "/")
        );

        for _ in 0..2 {
            reclaim_checkpoint_artifacts(&reclaimed(&[&uri]), Some(&root), None).await;
        }
    }

    /// Without a configured S3 backend the objects cannot be deleted. That must
    /// warn and continue — never panic, and never block the rest of the sweep.
    #[tokio::test]
    async fn s3_artifact_without_backend_is_skipped_not_fatal() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_path_buf();
        let local = root.join("local.bin");
        std::fs::write(&local, b"bytes").unwrap();
        let local_uri = format!("file:///{}", local.display().to_string().replace('\\', "/"));

        reclaim_checkpoint_artifacts(
            &reclaimed(&["s3://bucket/runs/r/ckpt-1/model.bin", &local_uri]),
            Some(&root),
            None,
        )
        .await;

        // The unreclaimable S3 entry must not stop the local one being cleaned.
        assert!(
            !local.exists(),
            "a skipped S3 URI aborted the rest of the sweep"
        );
    }
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

    // ── status endpoint ──────────────────────────────────────────────────────

    fn status_response(request: &str) -> String {
        build_status_http_response(
            request,
            &CoordinatorState::default(),
            &AuditLogger::tracing(),
            AuthMode::Disabled,
            false,
            false,
            SystemTime::now(),
        )
    }

    #[test]
    fn status_returns_json_snapshot() {
        let response = status_response("GET /status HTTP/1.1\r\n\r\n");
        assert!(response.starts_with("HTTP/1.1 200 OK\r\n"));
        assert!(response.contains("Content-Type: application/json\r\n"));
        assert!(response.contains("Connection: close\r\n"));

        let body = response.split_once("\r\n\r\n").unwrap().1;
        let parsed: serde_json::Value = serde_json::from_str(body).expect("status body is JSON");
        assert_eq!(parsed["health"], "ok");
        assert_eq!(parsed["auth_mode"], "disabled");
        assert_eq!(parsed["run_count"], 0);
    }

    #[test]
    fn status_unknown_path_returns_404_and_non_get_returns_405() {
        assert!(status_response("GET /nope HTTP/1.1\r\n\r\n").starts_with("HTTP/1.1 404 Not Found"));
        assert!(status_response("POST /status HTTP/1.1\r\n\r\n")
            .starts_with("HTTP/1.1 405 Method Not Allowed"));
    }

    #[test]
    fn status_content_length_matches_body_for_every_route() {
        for request in [
            "GET /status HTTP/1.1\r\n\r\n",
            "GET /nope HTTP/1.1\r\n\r\n",
            "POST /status HTTP/1.1\r\n\r\n",
        ] {
            let response = status_response(request);
            let (head, body) = response.split_once("\r\n\r\n").expect("framed response");
            let declared: usize = head
                .lines()
                .find_map(|l| l.strip_prefix("Content-Length: "))
                .expect("Content-Length header")
                .parse()
                .expect("numeric Content-Length");
            assert_eq!(declared, body.len(), "bad framing for {request:?}");
        }
    }

    /// A client that connects and never sends must not wedge the accept loop:
    /// before the read timeout existed, this hung forever and took `/healthz`
    /// and `/readyz` down with it.
    #[test]
    fn silent_client_does_not_block_later_requests() {
        use std::io::BufRead;

        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("addr");
        drop(listener); // hand the port to the server thread

        std::thread::spawn(move || {
            let _ = serve_operational_endpoint(addr.to_string(), "test", |_| {
                http_response("200 OK", TEXT_PLAIN, "ok\n")
            });
        });

        // Wait for the listener to come up.
        let stall = loop {
            match std::net::TcpStream::connect(addr) {
                Ok(s) => break s,
                Err(_) => std::thread::sleep(Duration::from_millis(20)),
            }
        };
        // `stall` is connected but deliberately sends nothing, and is held
        // open for the rest of the test.

        // A well-behaved client must still be served, without waiting out the
        // full read timeout on the stalled one.
        let started = std::time::Instant::now();
        let mut good = std::net::TcpStream::connect(addr).expect("second connect");
        good.set_read_timeout(Some(Duration::from_secs(30))).ok();
        good.write_all(b"GET /healthz HTTP/1.1\r\n\r\n")
            .expect("write");
        let mut line = String::new();
        std::io::BufReader::new(&good)
            .read_line(&mut line)
            .expect("read status line");

        assert!(
            line.starts_with("HTTP/1.1 200 OK"),
            "unexpected status line: {line:?}"
        );
        assert!(
            started.elapsed() < STATUS_IO_TIMEOUT * 3,
            "second request waited {:?} — the stalled client blocked the loop",
            started.elapsed()
        );
        drop(stall);
    }
}
