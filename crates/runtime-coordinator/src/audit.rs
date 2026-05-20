#![forbid(unsafe_code)]

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuditOutcome {
    Success,
    Failure,
    Rejected,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuditEventKind {
    CoordinatorStarted,
    TlsEnabled,
    MtlsEnabled,
    AuthModeConfigured,
    AuthMetadataAccepted,
    AuthMetadataRejected,
    AuthorizationAllowed,
    AuthorizationDenied,
    RunRegistered,
    WorkerRegistered,
    WorkerHeartbeat,
    DatasetRegistered,
    ShardAssignmentRequested,
    ShardLeaseRenewed,
    ShardProgressReported,
    CheckpointStarted,
    CheckpointArtifactReported,
    CheckpointCommitted,
    LatestCheckpointRead,
    RuntimeInfoRead,
    SecurityConfigurationError,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AuditEvent {
    pub kind: Option<AuditEventKind>,
    pub outcome: Option<AuditOutcome>,
    pub subject: Option<String>,
    pub role: Option<String>,
    pub worker_id: Option<String>,
    pub run_id: Option<String>,
    pub dataset_id: Option<String>,
    pub checkpoint_id: Option<String>,
    pub reason: Option<String>,
    pub request_name: Option<String>,
}

impl AuditEvent {
    pub fn new(kind: AuditEventKind, outcome: AuditOutcome) -> Self {
        Self {
            kind: Some(kind),
            outcome: Some(outcome),
            ..Self::default()
        }
    }
}

pub trait AuditSink: Send + Sync + 'static {
    fn emit(&self, event: AuditEvent);
}

#[derive(Clone, Debug, Default)]
pub struct TracingAuditSink;
impl AuditSink for TracingAuditSink {
    fn emit(&self, event: AuditEvent) {
        tracing::info!(
            target: "runtime_coordinator::audit",
            kind = ?event.kind,
            outcome = ?event.outcome,
            subject = ?event.subject,
            role = ?event.role,
            worker_id = ?event.worker_id,
            run_id = ?event.run_id,
            dataset_id = ?event.dataset_id,
            checkpoint_id = ?event.checkpoint_id,
            reason = ?event.reason,
            request_name = ?event.request_name,
            "audit event"
        );
    }
}

#[derive(Clone)]
pub struct AuditLogger {
    sink: Arc<dyn AuditSink>,
    emitted_count: Arc<AtomicU64>,
}
impl std::fmt::Debug for AuditLogger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuditLogger").finish()
    }
}
impl AuditLogger {
    pub fn new(sink: Arc<dyn AuditSink>) -> Self {
        Self {
            sink,
            emitted_count: Arc::new(AtomicU64::new(0)),
        }
    }
    pub fn tracing() -> Self {
        Self::new(Arc::new(TracingAuditSink))
    }
    pub fn emit(&self, event: AuditEvent) {
        self.emitted_count.fetch_add(1, Ordering::Relaxed);
        self.sink.emit(event);
    }

    pub fn emitted_count(&self) -> u64 {
        self.emitted_count.load(Ordering::Relaxed)
    }
}

#[derive(Clone, Debug, Default)]
pub struct InMemoryAuditSink {
    events: Arc<Mutex<Vec<AuditEvent>>>,
}
impl InMemoryAuditSink {
    pub fn snapshot(&self) -> Vec<AuditEvent> {
        self.events.lock().expect("audit mutex").clone()
    }
}
impl AuditSink for InMemoryAuditSink {
    fn emit(&self, event: AuditEvent) {
        if let Ok(mut g) = self.events.lock() {
            g.push(event);
        }
    }
}
