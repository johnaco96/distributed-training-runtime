#![forbid(unsafe_code)]

use std::io::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use crate::persistence::{AuditEventEntry, MetadataStore};

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
    RunCancelled,
    RunPaused,
    RunResumed,
    RunRetried,
    RunDrained,
    WorkerRegistered,
    WorkerDeregistered,
    WorkerHeartbeat,
    DatasetRegistered,
    ShardAssignmentRequested,
    ShardLeaseRenewed,
    ShardProgressReported,
    CheckpointStarted,
    CheckpointArtifactReported,
    CheckpointCommitted,
    CheckpointCorruptReported,
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

// ─── Gap 5: durable database-backed audit sink ───────────────────────────────

/// Audit sink that writes every event to the coordinator's metadata store.
///
/// Both SQLite and PostgreSQL back-ends are supported; the store's
/// `insert_audit_event` implementation handles the differences.
///
/// **Performance note**: `emit` is called synchronously in the gRPC handler
/// hot path.  On PostgreSQL the store uses `block_in_place` internally so the
/// call parks the current Tokio worker thread and drives the async INSERT.
/// At current coordinator scale this is acceptable; for very high event rates
/// consider a background channel writer.
pub struct DbAuditSink {
    store: Arc<dyn MetadataStore>,
}

impl DbAuditSink {
    pub fn new(store: Arc<dyn MetadataStore>) -> Self {
        Self { store }
    }
}

impl std::fmt::Debug for DbAuditSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DbAuditSink").finish()
    }
}

impl AuditSink for DbAuditSink {
    fn emit(&self, event: AuditEvent) {
        let occurred_at_millis = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let entry = AuditEventEntry {
            occurred_at_millis,
            kind: event.kind.map(|k| format!("{k:?}")),
            outcome: event.outcome.map(|o| format!("{o:?}")),
            subject: event.subject,
            role: event.role,
            worker_id: event.worker_id,
            run_id: event.run_id,
            dataset_id: event.dataset_id,
            checkpoint_id: event.checkpoint_id,
            reason: event.reason,
            request_name: event.request_name,
        };
        if let Err(e) = self.store.insert_audit_event(&entry) {
            // Log but do not panic — a broken audit sink should not take down
            // the coordinator.
            tracing::warn!(error = %e, "failed to persist audit event to DB");
        }
    }
}

// ─── SIEM: newline-delimited JSON audit sink ─────────────────────────────────

/// An audit sink that writes one JSON object per line (NDJSON) to a writer —
/// a file, stdout, or a pipe. The stable, flat schema is designed for direct
/// ingestion by log shippers (Fluent Bit, Vector, Filebeat) into a SIEM
/// (Splunk, Elastic, Loki, Chronicle). Each record carries a
/// `schema: "dtruntime.audit.v1"` tag and an RFC3339-ish millisecond timestamp.
///
/// No secrets or key material are ever written (see [`AuditEvent`] fields).
pub struct JsonLinesAuditSink {
    writer: Mutex<Box<dyn Write + Send>>,
    emitted: AtomicU64,
}

impl std::fmt::Debug for JsonLinesAuditSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JsonLinesAuditSink")
            .field("emitted", &self.emitted.load(Ordering::Relaxed))
            .finish()
    }
}

impl JsonLinesAuditSink {
    /// Append audit records to a file, creating it if necessary.
    pub fn to_file(path: impl AsRef<std::path::Path>) -> std::io::Result<Self> {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;
        Ok(Self::to_writer(Box::new(file)))
    }

    /// Write audit records to an arbitrary writer (stdout, a pipe, a buffer).
    pub fn to_writer(writer: Box<dyn Write + Send>) -> Self {
        Self {
            writer: Mutex::new(writer),
            emitted: AtomicU64::new(0),
        }
    }

    pub fn emitted_count(&self) -> u64 {
        self.emitted.load(Ordering::Relaxed)
    }

    fn record(event: &AuditEvent) -> serde_json::Value {
        let occurred_at_millis = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        serde_json::json!({
            "schema": "dtruntime.audit.v1",
            "occurred_at_millis": occurred_at_millis,
            "kind": event.kind.map(|k| format!("{k:?}")),
            "outcome": event.outcome.map(|o| format!("{o:?}")),
            "subject": event.subject,
            "role": event.role,
            "worker_id": event.worker_id,
            "run_id": event.run_id,
            "dataset_id": event.dataset_id,
            "checkpoint_id": event.checkpoint_id,
            "reason": event.reason,
            "request": event.request_name,
        })
    }
}

impl AuditSink for JsonLinesAuditSink {
    fn emit(&self, event: AuditEvent) {
        let line = Self::record(&event).to_string();
        if let Ok(mut w) = self.writer.lock() {
            // A failed audit write must not take down the coordinator; surface
            // it and continue (the DB sink, if composed, still records it).
            if let Err(e) = writeln!(w, "{line}").and_then(|_| w.flush()) {
                tracing::warn!(error = %e, "failed to write JSON audit line");
                return;
            }
            self.emitted.fetch_add(1, Ordering::Relaxed);
        }
    }
}

// ─── fan-out to multiple sinks ───────────────────────────────────────────────

/// Broadcasts each event to several sinks (e.g. DB + JSON/SIEM + tracing).
#[derive(Clone, Default)]
pub struct FanoutAuditSink {
    sinks: Vec<Arc<dyn AuditSink>>,
}

impl std::fmt::Debug for FanoutAuditSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FanoutAuditSink")
            .field("sinks", &self.sinks.len())
            .finish()
    }
}

impl FanoutAuditSink {
    pub fn new(sinks: Vec<Arc<dyn AuditSink>>) -> Self {
        Self { sinks }
    }

    pub fn push(&mut self, sink: Arc<dyn AuditSink>) {
        self.sinks.push(sink);
    }

    pub fn is_empty(&self) -> bool {
        self.sinks.is_empty()
    }
}

impl AuditSink for FanoutAuditSink {
    fn emit(&self, event: AuditEvent) {
        for sink in &self.sinks {
            sink.emit(event.clone());
        }
    }
}

#[cfg(test)]
mod sink_tests {
    use super::*;

    #[derive(Clone, Default)]
    struct CountingSink(Arc<AtomicU64>);
    impl AuditSink for CountingSink {
        fn emit(&self, _event: AuditEvent) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn json_lines_sink_writes_ndjson_records() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("audit.ndjson");
        let sink = JsonLinesAuditSink::to_file(&path).unwrap();
        sink.emit(AuditEvent {
            kind: Some(AuditEventKind::AuthorizationDenied),
            outcome: Some(AuditOutcome::Rejected),
            subject: Some("alice".into()),
            role: Some("viewer".into()),
            reason: Some("permission_denied".into()),
            request_name: Some("cancel_run".into()),
            ..AuditEvent::default()
        });
        assert_eq!(sink.emitted_count(), 1);
        let contents = std::fs::read_to_string(&path).unwrap();
        let line = contents.lines().next().unwrap();
        let value: serde_json::Value = serde_json::from_str(line).unwrap();
        assert_eq!(value["schema"], "dtruntime.audit.v1");
        assert_eq!(value["kind"], "AuthorizationDenied");
        assert_eq!(value["subject"], "alice");
        assert_eq!(value["request"], "cancel_run");
    }

    #[test]
    fn fanout_broadcasts_to_all_sinks() {
        let a = Arc::new(CountingSink::default());
        let b = Arc::new(CountingSink::default());
        let fan = FanoutAuditSink::new(vec![a.clone(), b.clone()]);
        fan.emit(AuditEvent::new(
            AuditEventKind::CoordinatorStarted,
            AuditOutcome::Success,
        ));
        assert_eq!(a.0.load(Ordering::Relaxed), 1);
        assert_eq!(b.0.load(Ordering::Relaxed), 1);
    }
}
