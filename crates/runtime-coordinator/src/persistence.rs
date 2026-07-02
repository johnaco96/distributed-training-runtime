#![forbid(unsafe_code)]

use std::path::Path;
use std::sync::{Arc, Mutex};

use rusqlite::{params, Connection, OptionalExtension};

use crate::state::{
    CoordinatorCheckpointRecord, DatasetRecord, RunRecord, ShardRecord, WorkerRecord,
};
use runtime_checkpoint::{
    ArtifactRecord, CheckpointId as DomainCheckpointId, CheckpointRecord,
    CheckpointState as DomainCheckpointState, RunId as DomainRunId, WorkerId as DomainWorkerId,
};
use runtime_proto::runtime::v1::CheckpointId as ProtoCheckpointId;

/// Boxed error returned by all [`MetadataStore`] methods.
///
/// Using a boxed trait object keeps the trait object-safe and avoids forcing a
/// single error type on SQLite and PostgreSQL implementations.
pub type PersistResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync + 'static>>;

// ─── persistence data transfer types ─────────────────────────────────────────

/// A single shard-progress record as seen by the persistence layer.
#[derive(Clone, Debug)]
pub struct ShardProgressEntry {
    pub run_id: String,
    pub dataset_id: String,
    pub shard_id: String,
    pub worker_id: String,
    pub lease_epoch: u64,
    pub progress_offset: i64,
    pub is_complete: bool,
    pub updated_at_millis: u64,
}

/// A single audit-event record as seen by the persistence layer.
#[derive(Clone, Debug)]
pub struct AuditEventEntry {
    pub occurred_at_millis: u64,
    pub kind: Option<String>,
    pub outcome: Option<String>,
    pub subject: Option<String>,
    pub role: Option<String>,
    pub worker_id: Option<String>,
    pub run_id: Option<String>,
    pub dataset_id: Option<String>,
    pub checkpoint_id: Option<String>,
    pub reason: Option<String>,
    pub request_name: Option<String>,
}

/// Trait that abstracts over the coordinator's metadata persistence back-end.
///
/// Both [`SqliteMetadataStore`] (single-node, local dev) and
/// [`crate::pg_store::PostgresMetadataStore`] (distributed, production)
/// implement this trait so [`crate::state::CoordinatorState`] can hold an
/// `Arc<dyn MetadataStore>` without caring which back-end is in use.
pub trait MetadataStore: Send + Sync {
    // ── original methods ──────────────────────────────────────────────────────
    fn save_run(&self, run: &RunRecord) -> PersistResult<()>;
    /// Load every persisted run.  Called once on coordinator startup to
    /// re-hydrate the run registry (durable run lifecycle states).
    fn load_all_runs(&self) -> PersistResult<Vec<RunRecord>>;
    fn save_worker(&self, w: &WorkerRecord, world_size: u32) -> PersistResult<()>;
    fn load_workers_for_run(&self, run_id: &str) -> PersistResult<Vec<(String, u32, u32, i32)>>;
    fn save_dataset(&self, d: &DatasetRecord) -> PersistResult<()>;
    fn save_checkpoint(
        &self,
        run_id: &str,
        idempotency_key: &str,
        rec: &CoordinatorCheckpointRecord,
    ) -> PersistResult<()>;
    fn save_checkpoint_artifact(
        &self,
        run_id: &str,
        checkpoint_id: &str,
        a: &ArtifactRecord,
    ) -> PersistResult<()>;
    fn mark_checkpoint_committed(
        &self,
        run_id: &str,
        checkpoint_id: &str,
        committed_at_millis: u64,
    ) -> PersistResult<()>;
    fn load_latest_checkpoint(
        &self,
        run_id: &str,
    ) -> PersistResult<Option<CoordinatorCheckpointRecord>>;
    /// All checkpoints for a run, newest step first, with artifacts.
    fn load_checkpoints_for_run(
        &self,
        run_id: &str,
    ) -> PersistResult<Vec<CoordinatorCheckpointRecord>>;
    /// Mark a checkpoint FAILED (corrupt-checkpoint reporting).
    fn mark_checkpoint_failed(&self, run_id: &str, checkpoint_id: &str) -> PersistResult<()>;
    /// Point (or clear) the run's latest-committed-checkpoint pointer.
    fn set_latest_checkpoint(&self, run_id: &str, latest: Option<(&str, u64)>)
        -> PersistResult<()>;
    /// Remove a checkpoint's rows entirely (retention GC).
    fn delete_checkpoint(&self, run_id: &str, checkpoint_id: &str) -> PersistResult<()>;
    fn load_dataset(
        &self,
        run_id: &str,
        dataset_id: &str,
    ) -> PersistResult<Option<(String, Vec<ShardRecord>)>>;

    // ── Gap 1: shard-progress persistence (resume-from-mid-shard) ────────────

    /// Upsert a shard-progress record.  Keyed on (run_id, dataset_id, shard_id).
    fn save_shard_progress(&self, p: &ShardProgressEntry) -> PersistResult<()>;

    /// Load all shard-progress records (across all runs).  Called once on
    /// coordinator startup to re-hydrate the in-memory DashMap.
    fn load_all_shard_progress(&self) -> PersistResult<Vec<ShardProgressEntry>>;

    // ── Gap 2: distributed 2PC checkpoint commit tracking ────────────────────

    /// Record that *worker_id* voted to commit *checkpoint_id*.
    /// Idempotent — duplicate calls are silently ignored.
    fn record_checkpoint_commit(
        &self,
        run_id: &str,
        checkpoint_id: &str,
        worker_id: &str,
        committed_at_millis: u64,
    ) -> PersistResult<()>;

    /// Return every (run_id, checkpoint_id, worker_id) commit vote.
    /// Called once on coordinator startup to re-hydrate the in-memory vote
    /// set for any in-progress checkpoints.
    fn load_all_checkpoint_commits(&self) -> PersistResult<Vec<(String, String, String)>>;

    // ── Gap 5: durable audit log ─────────────────────────────────────────────

    /// Append one audit event to durable storage.
    fn insert_audit_event(&self, event: &AuditEventEntry) -> PersistResult<()>;

    // ── durable run state machine (full hydration on restart) ────────────────

    /// Upsert one idempotency-key mapping.
    /// `scope` IN (run, worker, dataset, checkpoint, checkpoint_artifact).
    fn save_idempotency_key(&self, scope: &str, key: &str, value: &str) -> PersistResult<()>;

    /// Return every (scope, key, value) idempotency mapping.
    fn load_all_idempotency_keys(&self) -> PersistResult<Vec<(String, String, String)>>;

    /// Return every worker across all runs. Deadline fields are placeholders;
    /// the caller must re-arm liveness deadlines after a restart.
    fn load_all_workers(&self) -> PersistResult<Vec<WorkerRecord>>;

    /// Return every dataset (with shards) across all runs.
    fn load_all_datasets(&self) -> PersistResult<Vec<DatasetRecord>>;

    /// Return every checkpoint (any state) with artifacts and the expected
    /// worker set, as `(run_id, record)` pairs.
    fn load_all_checkpoints(&self) -> PersistResult<Vec<(String, CoordinatorCheckpointRecord)>>;
}

#[derive(Clone, Debug)]
pub struct SqliteMetadataStore {
    conn: Arc<Mutex<Connection>>,
}

impl SqliteMetadataStore {
    pub fn open(path: impl AsRef<Path>) -> rusqlite::Result<Self> {
        let conn = Connection::open(path)?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    pub fn init_schema(&self) -> rusqlite::Result<()> {
        let conn = self.conn.lock().expect("sqlite lock poisoned");
        conn.execute_batch(
            "
CREATE TABLE IF NOT EXISTS runs (
  run_id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  status INTEGER NOT NULL,
  created_at_millis INTEGER NOT NULL,
  idempotency_key TEXT,
  run_state INTEGER NOT NULL DEFAULT 1,
  state_reason TEXT NOT NULL DEFAULT '',
  world_size INTEGER NOT NULL DEFAULT 0,
  min_world_size INTEGER NOT NULL DEFAULT 0,
  max_world_size INTEGER NOT NULL DEFAULT 0,
  settle_timeout_ms INTEGER NOT NULL DEFAULT 0,
  assignment_policy INTEGER NOT NULL DEFAULT 0,
  storage_backend_kind INTEGER NOT NULL DEFAULT 0,
  run_version TEXT NOT NULL DEFAULT '',
  latest_checkpoint_id TEXT
);
CREATE TABLE IF NOT EXISTS workers (
  run_id TEXT NOT NULL,
  worker_id TEXT NOT NULL,
  rank INTEGER NOT NULL,
  world_size INTEGER NOT NULL,
  status INTEGER NOT NULL,
  last_heartbeat_millis INTEGER,
  idempotency_key TEXT,
  host TEXT NOT NULL DEFAULT '',
  sdk_version TEXT NOT NULL DEFAULT '',
  protocol_version TEXT NOT NULL DEFAULT '',
  registered_at_millis INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (run_id, worker_id)
);
CREATE TABLE IF NOT EXISTS datasets (
  run_id TEXT NOT NULL,
  dataset_id TEXT NOT NULL,
  name TEXT NOT NULL,
  num_shards INTEGER NOT NULL,
  idempotency_key TEXT,
  dataset_uri TEXT NOT NULL DEFAULT '',
  format TEXT NOT NULL DEFAULT '',
  dataset_version TEXT NOT NULL DEFAULT '',
  labels_json TEXT NOT NULL DEFAULT '{}',
  registered_at_millis INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (run_id, dataset_id)
);
CREATE TABLE IF NOT EXISTS shards (
  run_id TEXT NOT NULL,
  dataset_id TEXT NOT NULL,
  shard_id TEXT NOT NULL,
  shard_index INTEGER NOT NULL,
  shard_uri TEXT NOT NULL,
  num_records INTEGER,
  size_bytes INTEGER,
  byte_range_start INTEGER NOT NULL DEFAULT 0,
  byte_range_end INTEGER NOT NULL DEFAULT 0,
  checksum TEXT NOT NULL DEFAULT '',
  checksum_algorithm TEXT NOT NULL DEFAULT '',
  PRIMARY KEY (run_id, dataset_id, shard_id)
);
CREATE TABLE IF NOT EXISTS checkpoints (
  run_id TEXT NOT NULL,
  checkpoint_id TEXT NOT NULL,
  state INTEGER NOT NULL,
  initiated_by_worker_id TEXT NOT NULL,
  step INTEGER NOT NULL,
  created_at_millis INTEGER NOT NULL,
  updated_at_millis INTEGER NOT NULL,
  committed_at_millis INTEGER,
  idempotency_key TEXT,
  expected_worker_ids_json TEXT NOT NULL DEFAULT '[]',
  PRIMARY KEY (run_id, checkpoint_id)
);
-- Durable idempotency-key maps: one row per in-memory DashMap entry so
-- retried RPCs stay idempotent across coordinator restarts.
-- scope IN (run, worker, dataset, checkpoint, checkpoint_artifact).
CREATE TABLE IF NOT EXISTS idempotency_keys (
  scope TEXT NOT NULL,
  key   TEXT NOT NULL,
  value TEXT NOT NULL,
  PRIMARY KEY (scope, key)
);
CREATE TABLE IF NOT EXISTS checkpoint_artifacts (
  run_id TEXT NOT NULL,
  checkpoint_id TEXT NOT NULL,
  worker_id TEXT NOT NULL,
  artifact_name TEXT NOT NULL,
  artifact_uri TEXT NOT NULL,
  size_bytes INTEGER NOT NULL,
  checksum TEXT NOT NULL,
  checksum_algorithm TEXT NOT NULL,
  idempotency_key TEXT,
  PRIMARY KEY (run_id, checkpoint_id, worker_id, artifact_name)
);
CREATE TABLE IF NOT EXISTS latest_checkpoints (
  run_id TEXT PRIMARY KEY,
  checkpoint_id TEXT NOT NULL,
  committed_at_millis INTEGER NOT NULL
);
-- Gap 1: shard progress — enables resume-from-mid-shard across coordinator restarts
CREATE TABLE IF NOT EXISTS shard_progress (
  run_id             TEXT NOT NULL,
  dataset_id         TEXT NOT NULL,
  shard_id           TEXT NOT NULL,
  worker_id          TEXT NOT NULL,
  lease_epoch        INTEGER NOT NULL,
  progress_offset    INTEGER NOT NULL,
  is_complete        INTEGER NOT NULL,
  updated_at_millis  INTEGER NOT NULL,
  PRIMARY KEY (run_id, dataset_id, shard_id)
);
-- Gap 2: per-worker checkpoint commit votes (distributed 2PC)
CREATE TABLE IF NOT EXISTS checkpoint_commits (
  run_id              TEXT NOT NULL,
  checkpoint_id       TEXT NOT NULL,
  worker_id           TEXT NOT NULL,
  committed_at_millis INTEGER NOT NULL,
  PRIMARY KEY (run_id, checkpoint_id, worker_id)
);
-- Gap 5: durable audit log
CREATE TABLE IF NOT EXISTS audit_events (
  id                 INTEGER PRIMARY KEY AUTOINCREMENT,
  occurred_at_millis INTEGER NOT NULL,
  kind               TEXT,
  outcome            TEXT,
  subject            TEXT,
  role               TEXT,
  worker_id          TEXT,
  run_id             TEXT,
  dataset_id         TEXT,
  checkpoint_id      TEXT,
  reason             TEXT,
  request_name       TEXT
);
",
        )?;
        drop(conn);
        self.migrate_schema()
    }

    /// Add columns introduced after a table was first created (idempotent).
    ///
    /// `CREATE TABLE IF NOT EXISTS` covers fresh databases only; databases
    /// created by earlier builds need the new columns retrofitted.  Column
    /// presence is checked via PRAGMA rather than swallowing ALTER errors.
    /// (A full migration framework lands with the Phase 6 storage work.)
    fn migrate_schema(&self) -> rusqlite::Result<()> {
        let conn = self.conn.lock().expect("sqlite lock poisoned");
        let additions: &[(&str, &str, &str)] = &[
            ("runs", "run_state", "INTEGER NOT NULL DEFAULT 1"),
            ("runs", "state_reason", "TEXT NOT NULL DEFAULT ''"),
            ("runs", "world_size", "INTEGER NOT NULL DEFAULT 0"),
            ("runs", "min_world_size", "INTEGER NOT NULL DEFAULT 0"),
            ("runs", "max_world_size", "INTEGER NOT NULL DEFAULT 0"),
            ("runs", "settle_timeout_ms", "INTEGER NOT NULL DEFAULT 0"),
            ("runs", "assignment_policy", "INTEGER NOT NULL DEFAULT 0"),
            ("runs", "storage_backend_kind", "INTEGER NOT NULL DEFAULT 0"),
            ("runs", "run_version", "TEXT NOT NULL DEFAULT ''"),
            ("runs", "latest_checkpoint_id", "TEXT"),
            ("workers", "host", "TEXT NOT NULL DEFAULT ''"),
            ("workers", "sdk_version", "TEXT NOT NULL DEFAULT ''"),
            ("workers", "protocol_version", "TEXT NOT NULL DEFAULT ''"),
            (
                "workers",
                "registered_at_millis",
                "INTEGER NOT NULL DEFAULT 0",
            ),
            ("datasets", "dataset_uri", "TEXT NOT NULL DEFAULT ''"),
            ("datasets", "format", "TEXT NOT NULL DEFAULT ''"),
            ("datasets", "dataset_version", "TEXT NOT NULL DEFAULT ''"),
            ("datasets", "labels_json", "TEXT NOT NULL DEFAULT '{}'"),
            (
                "datasets",
                "registered_at_millis",
                "INTEGER NOT NULL DEFAULT 0",
            ),
            ("shards", "byte_range_start", "INTEGER NOT NULL DEFAULT 0"),
            ("shards", "byte_range_end", "INTEGER NOT NULL DEFAULT 0"),
            ("shards", "checksum", "TEXT NOT NULL DEFAULT ''"),
            ("shards", "checksum_algorithm", "TEXT NOT NULL DEFAULT ''"),
            (
                "checkpoints",
                "expected_worker_ids_json",
                "TEXT NOT NULL DEFAULT '[]'",
            ),
        ];
        for (table, column, decl) in additions {
            let exists: bool = conn
                .prepare(&format!("PRAGMA table_info({table})"))?
                .query_map([], |r| r.get::<_, String>(1))?
                .collect::<rusqlite::Result<Vec<_>>>()?
                .iter()
                .any(|name| name == column);
            if !exists {
                conn.execute_batch(&format!("ALTER TABLE {table} ADD COLUMN {column} {decl}"))?;
            }
        }
        Ok(())
    }

    pub fn save_run(&self, run: &RunRecord) -> rusqlite::Result<()> {
        let conn = self.conn.lock().expect("sqlite");
        conn.execute(
            "INSERT OR REPLACE INTO runs(run_id,name,status,created_at_millis,idempotency_key,\
             run_state,state_reason,world_size,min_world_size,max_world_size,settle_timeout_ms,\
             assignment_policy,storage_backend_kind,run_version,latest_checkpoint_id) \
             VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15)",
            params![
                run.run_id,
                run.run_name,
                run.latest_checkpoint_state,
                to_millis(run.registered_at),
                if run.idempotency_key.is_empty() {
                    None
                } else {
                    Some(run.idempotency_key.as_str())
                },
                run.state,
                run.state_reason,
                run.world_size,
                run.min_world_size,
                run.max_world_size,
                run.rendezvous_settle_timeout_ms,
                run.assignment_policy,
                run.storage_backend_kind,
                run.run_version,
                run.latest_checkpoint_id.as_ref().map(|c| c.value.clone()),
            ],
        )?;
        Ok(())
    }

    pub fn load_all_runs(&self) -> rusqlite::Result<Vec<RunRecord>> {
        let conn = self.conn.lock().expect("sqlite");
        let mut stmt = conn.prepare(
            "SELECT run_id,name,created_at_millis,idempotency_key,run_state,state_reason,\
             world_size,min_world_size,max_world_size,settle_timeout_ms,assignment_policy,\
             storage_backend_kind,run_version,status,latest_checkpoint_id FROM runs \
             ORDER BY run_id",
        )?;
        let runs = stmt
            .query_map([], |r| {
                Ok(RunRecord {
                    run_id: r.get(0)?,
                    run_name: r.get(1)?,
                    registered_at: from_millis(r.get::<_, u64>(2)?),
                    idempotency_key: r.get::<_, Option<String>>(3)?.unwrap_or_default(),
                    state: r.get(4)?,
                    state_reason: r.get(5)?,
                    world_size: r.get(6)?,
                    min_world_size: r.get(7)?,
                    max_world_size: r.get(8)?,
                    rendezvous_settle_timeout_ms: r.get(9)?,
                    assignment_policy: r.get(10)?,
                    storage_backend_kind: r.get(11)?,
                    run_version: r.get(12)?,
                    active_worker_count: 0,
                    latest_checkpoint_state: r.get(13)?,
                    latest_checkpoint_id: r
                        .get::<_, Option<String>>(14)?
                        .map(|value| ProtoCheckpointId { value }),
                    state_updated_at: std::time::SystemTime::now(),
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(runs)
    }
    pub fn save_worker(&self, w: &WorkerRecord, world_size: u32) -> rusqlite::Result<()> {
        let conn = self.conn.lock().expect("sqlite");
        conn.execute(
            "INSERT OR REPLACE INTO workers(run_id,worker_id,rank,world_size,status,\
             last_heartbeat_millis,idempotency_key,host,sdk_version,protocol_version,\
             registered_at_millis) VALUES(?1,?2,?3,?4,?5,?6,NULL,?7,?8,?9,?10)",
            params![
                w.run_id,
                w.worker_id,
                w.rank,
                world_size,
                w.state,
                to_millis(w.last_heartbeat_at),
                w.host,
                w.sdk_version,
                w.protocol_version,
                to_millis(w.registered_at)
            ],
        )?;
        Ok(())
    }

    pub fn load_all_workers(&self) -> rusqlite::Result<Vec<WorkerRecord>> {
        let conn = self.conn.lock().expect("sqlite");
        let mut stmt = conn.prepare(
            "SELECT run_id,worker_id,rank,status,last_heartbeat_millis,host,\
             sdk_version,protocol_version,registered_at_millis FROM workers",
        )?;
        let rows = stmt
            .query_map([], |r| {
                let last_heartbeat =
                    from_millis(r.get::<_, Option<i64>>(4)?.unwrap_or(0).max(0) as u64);
                Ok(WorkerRecord {
                    run_id: r.get(0)?,
                    worker_id: r.get(1)?,
                    rank: r.get::<_, i64>(2)? as u32,
                    state: r.get(3)?,
                    last_heartbeat_at: last_heartbeat,
                    host: r.get(5)?,
                    sdk_version: r.get(6)?,
                    protocol_version: r.get(7)?,
                    registered_at: from_millis(r.get::<_, i64>(8)?.max(0) as u64),
                    // Deadlines are re-armed by CoordinatorState hydration —
                    // pre-crash deadlines must not be trusted after a restart.
                    next_heartbeat_deadline: last_heartbeat,
                    failure_deadline: last_heartbeat,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }
    pub fn load_workers_for_run(
        &self,
        run_id: &str,
    ) -> rusqlite::Result<Vec<(String, u32, u32, i32)>> {
        let conn = self.conn.lock().expect("sqlite");
        let mut stmt = conn.prepare(
            "SELECT worker_id, rank, world_size, status FROM workers WHERE run_id=?1 ORDER BY worker_id",
        )?;

        let workers = stmt
            .query_map(params![run_id], |r| {
                Ok((
                    r.get(0)?,
                    r.get::<_, i64>(1)? as u32,
                    r.get::<_, i64>(2)? as u32,
                    r.get::<_, i32>(3)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;

        Ok(workers)
    }
    pub fn save_dataset(&self, d: &DatasetRecord) -> rusqlite::Result<()> {
        let labels_json = serde_json::to_string(&d.labels).unwrap_or_else(|_| "{}".to_string());
        let conn = self.conn.lock().expect("sqlite");
        conn.execute(
            "INSERT OR REPLACE INTO datasets(run_id,dataset_id,name,num_shards,\
             idempotency_key,dataset_uri,format,dataset_version,labels_json,\
             registered_at_millis) VALUES(?1,?2,?3,?4,NULL,?5,?6,?7,?8,?9)",
            params![
                d.run_id,
                d.dataset_id,
                d.name,
                d.shard_count,
                d.dataset_uri,
                d.format,
                d.dataset_version,
                labels_json,
                to_millis(d.registered_at)
            ],
        )?;
        for (idx, s) in d.shards.iter().enumerate() {
            conn.execute(
                "INSERT OR REPLACE INTO shards(run_id,dataset_id,shard_id,shard_index,\
                 shard_uri,num_records,size_bytes,byte_range_start,byte_range_end,\
                 checksum,checksum_algorithm) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)",
                params![
                    d.run_id,
                    d.dataset_id,
                    s.shard_id,
                    idx as i64,
                    s.shard_uri,
                    Option::<i64>::None,
                    s.size_bytes,
                    s.byte_range_start,
                    s.byte_range_end,
                    s.checksum,
                    s.checksum_algorithm
                ],
            )?;
        }
        Ok(())
    }

    pub fn load_all_datasets(&self) -> rusqlite::Result<Vec<DatasetRecord>> {
        let conn = self.conn.lock().expect("sqlite");
        let mut stmt = conn.prepare(
            "SELECT run_id,dataset_id,name,num_shards,dataset_uri,format,\
             dataset_version,labels_json,registered_at_millis FROM datasets",
        )?;
        let mut datasets = stmt
            .query_map([], |r| {
                let labels_json: String = r.get(7)?;
                Ok(DatasetRecord {
                    run_id: r.get(0)?,
                    dataset_id: r.get(1)?,
                    name: r.get(2)?,
                    shard_count: r.get::<_, i64>(3)? as u32,
                    dataset_uri: r.get(4)?,
                    format: r.get(5)?,
                    dataset_version: r.get(6)?,
                    labels: serde_json::from_str(&labels_json).unwrap_or_default(),
                    registered_at: from_millis(r.get::<_, i64>(8)?.max(0) as u64),
                    shards: Vec::new(),
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;

        let mut shard_stmt = conn.prepare(
            "SELECT shard_id,shard_uri,size_bytes,byte_range_start,byte_range_end,\
             checksum,checksum_algorithm FROM shards WHERE run_id=?1 AND dataset_id=?2 \
             ORDER BY shard_index",
        )?;
        for dataset in &mut datasets {
            dataset.shards = shard_stmt
                .query_map(params![dataset.run_id, dataset.dataset_id], |r| {
                    Ok(ShardRecord {
                        shard_id: r.get(0)?,
                        dataset_id: dataset.dataset_id.clone(),
                        shard_uri: r.get(1)?,
                        size_bytes: r.get::<_, Option<i64>>(2)?.unwrap_or(0),
                        byte_range_start: r.get(3)?,
                        byte_range_end: r.get(4)?,
                        checksum: r.get(5)?,
                        checksum_algorithm: r.get(6)?,
                    })
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
        }
        Ok(datasets)
    }

    pub fn save_checkpoint(
        &self,
        run_id: &str,
        idempotency_key: &str,
        rec: &CoordinatorCheckpointRecord,
    ) -> rusqlite::Result<()> {
        let expected_json =
            serde_json::to_string(&rec.expected_worker_ids).unwrap_or_else(|_| "[]".to_string());
        let conn = self.conn.lock().expect("sqlite");
        conn.execute("INSERT OR REPLACE INTO checkpoints(run_id,checkpoint_id,state,initiated_by_worker_id,step,created_at_millis,updated_at_millis,committed_at_millis,idempotency_key,expected_worker_ids_json) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",params![run_id,rec.checkpoint.checkpoint_id.0, rec.checkpoint.state as i32, rec.initiated_by_worker_id, rec.checkpoint.step as i64, rec.checkpoint.created_at_millis as i64, rec.checkpoint.updated_at_millis as i64, rec.committed_at_millis.map(|v| v as i64), if idempotency_key.is_empty(){None::<String>}else{Some(idempotency_key.to_string())}, expected_json])?;
        Ok(())
    }

    /// Load every checkpoint (any state, committed or in-flight) with its
    /// artifacts and the expected worker set recorded at BeginCheckpoint.
    pub fn load_all_checkpoints(
        &self,
    ) -> rusqlite::Result<Vec<(String, CoordinatorCheckpointRecord)>> {
        let conn = self.conn.lock().expect("sqlite");
        let mut stmt = conn.prepare(
            "SELECT run_id,checkpoint_id,state,initiated_by_worker_id,step,\
             created_at_millis,updated_at_millis,committed_at_millis,\
             expected_worker_ids_json FROM checkpoints",
        )?;
        type CkptRow = (
            String,
            String,
            i32,
            String,
            i64,
            i64,
            i64,
            Option<i64>,
            String,
        );
        let rows: Vec<CkptRow> = stmt
            .query_map([], |r| {
                Ok((
                    r.get(0)?,
                    r.get(1)?,
                    r.get(2)?,
                    r.get(3)?,
                    r.get(4)?,
                    r.get(5)?,
                    r.get(6)?,
                    r.get(7)?,
                    r.get(8)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;

        let mut artifact_stmt = conn.prepare(
            "SELECT worker_id,artifact_name,artifact_uri,size_bytes,checksum,\
             checksum_algorithm FROM checkpoint_artifacts \
             WHERE run_id=?1 AND checkpoint_id=?2 ORDER BY worker_id,artifact_name",
        )?;
        let mut records = Vec::with_capacity(rows.len());
        for (run_id, ckpt_id, state, initiator, step, created, updated, committed, expected_json) in
            rows
        {
            let artifacts = artifact_stmt
                .query_map(params![run_id, ckpt_id], |r| {
                    Ok(ArtifactRecord {
                        worker_id: DomainWorkerId(r.get(0)?),
                        artifact_name: r.get(1)?,
                        storage_key: r.get(2)?,
                        size_bytes: r.get::<_, i64>(3)? as u64,
                        checksum: r.get(4)?,
                        checksum_algorithm: r.get(5)?,
                    })
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            let expected_worker_ids: Vec<String> =
                serde_json::from_str(&expected_json).unwrap_or_default();
            records.push((
                run_id.clone(),
                CoordinatorCheckpointRecord {
                    checkpoint: CheckpointRecord {
                        run_id: DomainRunId(run_id),
                        checkpoint_id: DomainCheckpointId(ckpt_id),
                        step: step as u64,
                        state: checkpoint_state_from_i32(state),
                        artifacts,
                        created_at_millis: created as u64,
                        updated_at_millis: updated as u64,
                    },
                    initiated_by_worker_id: initiator,
                    committed_at_millis: committed.map(|v| v as u64),
                    expected_artifact_count: expected_worker_ids.len() as u32,
                    expected_worker_ids,
                },
            ));
        }
        Ok(records)
    }

    pub fn save_idempotency_key_inner(
        &self,
        scope: &str,
        key: &str,
        value: &str,
    ) -> rusqlite::Result<()> {
        let conn = self.conn.lock().expect("sqlite");
        conn.execute(
            "INSERT OR REPLACE INTO idempotency_keys(scope,key,value) VALUES(?1,?2,?3)",
            params![scope, key, value],
        )?;
        Ok(())
    }

    pub fn load_all_idempotency_keys_inner(
        &self,
    ) -> rusqlite::Result<Vec<(String, String, String)>> {
        let conn = self.conn.lock().expect("sqlite");
        let mut stmt = conn.prepare("SELECT scope,key,value FROM idempotency_keys")?;
        let rows = stmt
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }
    pub fn save_checkpoint_artifact(
        &self,
        run_id: &str,
        checkpoint_id: &str,
        a: &ArtifactRecord,
    ) -> rusqlite::Result<()> {
        let conn = self.conn.lock().expect("sqlite");
        conn.execute("INSERT OR REPLACE INTO checkpoint_artifacts(run_id,checkpoint_id,worker_id,artifact_name,artifact_uri,size_bytes,checksum,checksum_algorithm,idempotency_key) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,NULL)",params![run_id,checkpoint_id,a.worker_id.0,a.artifact_name,a.storage_key,a.size_bytes as i64,a.checksum,a.checksum_algorithm])?;
        Ok(())
    }
    pub fn mark_checkpoint_committed(
        &self,
        run_id: &str,
        checkpoint_id: &str,
        committed_at_millis: u64,
    ) -> rusqlite::Result<()> {
        let conn = self.conn.lock().expect("sqlite");
        conn.execute("UPDATE checkpoints SET committed_at_millis=?3,state=?4,updated_at_millis=?3 WHERE run_id=?1 AND checkpoint_id=?2",params![run_id,checkpoint_id,committed_at_millis as i64,DomainCheckpointState::Committed as i32])?;
        conn.execute("INSERT INTO latest_checkpoints(run_id,checkpoint_id,committed_at_millis) VALUES(?1,?2,?3) ON CONFLICT(run_id) DO UPDATE SET checkpoint_id=excluded.checkpoint_id,committed_at_millis=excluded.committed_at_millis",params![run_id,checkpoint_id,committed_at_millis as i64])?;
        Ok(())
    }

    pub fn load_latest_checkpoint(
        &self,
        run_id: &str,
    ) -> rusqlite::Result<Option<CoordinatorCheckpointRecord>> {
        let conn = self.conn.lock().expect("sqlite");
        let row: Option<(String, i64)> = conn
            .query_row(
                "SELECT checkpoint_id, committed_at_millis FROM latest_checkpoints WHERE run_id=?1",
                params![run_id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;
        let Some((checkpoint_id, committed_at)) = row else {
            return Ok(None);
        };
        let ck = conn.query_row("SELECT step,created_at_millis,updated_at_millis,initiated_by_worker_id FROM checkpoints WHERE run_id=?1 AND checkpoint_id=?2 AND state=?3", params![run_id,checkpoint_id,DomainCheckpointState::Committed as i32], |r| Ok((r.get::<_,i64>(0)?,r.get::<_,i64>(1)?,r.get::<_,i64>(2)?,r.get::<_,String>(3)?))).optional()?;
        let Some((step, created, updated, initiator)) = ck else {
            return Ok(None);
        };
        let mut stmt=conn.prepare("SELECT worker_id,artifact_name,artifact_uri,size_bytes,checksum,checksum_algorithm FROM checkpoint_artifacts WHERE run_id=?1 AND checkpoint_id=?2 ORDER BY worker_id,artifact_name")?;
        let artifacts = stmt
            .query_map(params![run_id, checkpoint_id], |r| {
                Ok(ArtifactRecord {
                    worker_id: DomainWorkerId(r.get(0)?),
                    artifact_name: r.get(1)?,
                    storage_key: r.get(2)?,
                    size_bytes: r.get::<_, i64>(3)? as u64,
                    checksum: r.get(4)?,
                    checksum_algorithm: r.get(5)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        let expected_worker_ids = artifacts
            .iter()
            .map(|artifact| artifact.worker_id.0.clone())
            .collect::<Vec<_>>();
        Ok(Some(CoordinatorCheckpointRecord {
            checkpoint: CheckpointRecord {
                run_id: DomainRunId(run_id.to_string()),
                checkpoint_id: DomainCheckpointId(checkpoint_id.clone()),
                step: step as u64,
                state: DomainCheckpointState::Committed,
                artifacts,
                created_at_millis: created as u64,
                updated_at_millis: updated as u64,
            },
            initiated_by_worker_id: initiator,
            committed_at_millis: Some(committed_at as u64),
            expected_artifact_count: expected_worker_ids.len() as u32,
            expected_worker_ids,
        }))
    }

    pub fn load_checkpoints_for_run(
        &self,
        run_id: &str,
    ) -> rusqlite::Result<Vec<CoordinatorCheckpointRecord>> {
        let conn = self.conn.lock().expect("sqlite");
        let mut stmt = conn.prepare(
            "SELECT checkpoint_id,step,state,created_at_millis,updated_at_millis,\
             committed_at_millis,initiated_by_worker_id FROM checkpoints \
             WHERE run_id=?1 ORDER BY step DESC, checkpoint_id DESC",
        )?;
        type CkRow = (String, i64, i32, i64, i64, Option<i64>, String);
        let rows: Vec<CkRow> = stmt
            .query_map(params![run_id], |r| {
                Ok((
                    r.get(0)?,
                    r.get(1)?,
                    r.get(2)?,
                    r.get(3)?,
                    r.get(4)?,
                    r.get(5)?,
                    r.get(6)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;

        let mut artifact_stmt = conn.prepare(
            "SELECT worker_id,artifact_name,artifact_uri,size_bytes,checksum,checksum_algorithm \
             FROM checkpoint_artifacts WHERE run_id=?1 AND checkpoint_id=?2 \
             ORDER BY worker_id,artifact_name",
        )?;
        let mut records = Vec::with_capacity(rows.len());
        for (checkpoint_id, step, state, created, updated, committed_at, initiator) in rows {
            let artifacts = artifact_stmt
                .query_map(params![run_id, checkpoint_id], |r| {
                    Ok(ArtifactRecord {
                        worker_id: DomainWorkerId(r.get(0)?),
                        artifact_name: r.get(1)?,
                        storage_key: r.get(2)?,
                        size_bytes: r.get::<_, i64>(3)? as u64,
                        checksum: r.get(4)?,
                        checksum_algorithm: r.get(5)?,
                    })
                })?
                .collect::<Result<Vec<_>, _>>()?;
            let expected_worker_ids: Vec<String> =
                artifacts.iter().map(|a| a.worker_id.0.clone()).collect();
            records.push(CoordinatorCheckpointRecord {
                checkpoint: CheckpointRecord {
                    run_id: DomainRunId(run_id.to_string()),
                    checkpoint_id: DomainCheckpointId(checkpoint_id),
                    step: step as u64,
                    state: checkpoint_state_from_i32(state),
                    artifacts,
                    created_at_millis: created as u64,
                    updated_at_millis: updated as u64,
                },
                initiated_by_worker_id: initiator,
                committed_at_millis: committed_at.map(|v| v as u64),
                expected_artifact_count: expected_worker_ids.len() as u32,
                expected_worker_ids,
            });
        }
        Ok(records)
    }

    pub fn mark_checkpoint_failed(
        &self,
        run_id: &str,
        checkpoint_id: &str,
    ) -> rusqlite::Result<()> {
        let conn = self.conn.lock().expect("sqlite");
        conn.execute(
            "UPDATE checkpoints SET state=?3, updated_at_millis=?4 WHERE run_id=?1 AND checkpoint_id=?2",
            params![
                run_id,
                checkpoint_id,
                DomainCheckpointState::Failed as i32,
                now_millis_i64()
            ],
        )?;
        Ok(())
    }

    pub fn set_latest_checkpoint(
        &self,
        run_id: &str,
        latest: Option<(&str, u64)>,
    ) -> rusqlite::Result<()> {
        let conn = self.conn.lock().expect("sqlite");
        match latest {
            Some((checkpoint_id, committed_at_millis)) => {
                conn.execute(
                    "INSERT OR REPLACE INTO latest_checkpoints(run_id,checkpoint_id,committed_at_millis) VALUES(?1,?2,?3)",
                    params![run_id, checkpoint_id, committed_at_millis as i64],
                )?;
            }
            None => {
                conn.execute(
                    "DELETE FROM latest_checkpoints WHERE run_id=?1",
                    params![run_id],
                )?;
            }
        }
        Ok(())
    }

    pub fn delete_checkpoint(&self, run_id: &str, checkpoint_id: &str) -> rusqlite::Result<()> {
        let conn = self.conn.lock().expect("sqlite");
        conn.execute(
            "DELETE FROM checkpoint_artifacts WHERE run_id=?1 AND checkpoint_id=?2",
            params![run_id, checkpoint_id],
        )?;
        conn.execute(
            "DELETE FROM checkpoint_commits WHERE run_id=?1 AND checkpoint_id=?2",
            params![run_id, checkpoint_id],
        )?;
        conn.execute(
            "DELETE FROM checkpoints WHERE run_id=?1 AND checkpoint_id=?2",
            params![run_id, checkpoint_id],
        )?;
        Ok(())
    }

    pub fn load_dataset(
        &self,
        run_id: &str,
        dataset_id: &str,
    ) -> rusqlite::Result<Option<(String, Vec<ShardRecord>)>> {
        let conn = self.conn.lock().expect("sqlite");
        let name: Option<String> = conn
            .query_row(
                "SELECT name FROM datasets WHERE run_id=?1 AND dataset_id=?2",
                params![run_id, dataset_id],
                |r| r.get(0),
            )
            .optional()?;
        let Some(name) = name else { return Ok(None) };
        let mut stmt=conn.prepare("SELECT shard_id,shard_uri,size_bytes FROM shards WHERE run_id=?1 AND dataset_id=?2 ORDER BY shard_index")?;
        let shards = stmt
            .query_map(params![run_id, dataset_id], |r| {
                Ok(ShardRecord {
                    shard_id: r.get(0)?,
                    dataset_id: dataset_id.to_string(),
                    shard_uri: r.get(1)?,
                    byte_range_start: 0,
                    byte_range_end: 0,
                    size_bytes: r.get(2)?,
                    checksum: String::new(),
                    checksum_algorithm: String::new(),
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Some((name, shards)))
    }

    // ── Gap 1: shard progress ────────────────────────────────────────────────

    pub fn save_shard_progress_inner(&self, p: &ShardProgressEntry) -> rusqlite::Result<()> {
        let conn = self.conn.lock().expect("sqlite");
        conn.execute(
            "INSERT INTO shard_progress(run_id,dataset_id,shard_id,worker_id,lease_epoch,progress_offset,is_complete,updated_at_millis) \
             VALUES(?1,?2,?3,?4,?5,?6,?7,?8) \
             ON CONFLICT(run_id,dataset_id,shard_id) DO UPDATE SET \
               worker_id=excluded.worker_id,lease_epoch=excluded.lease_epoch,\
               progress_offset=excluded.progress_offset,is_complete=excluded.is_complete,\
               updated_at_millis=excluded.updated_at_millis",
            params![
                p.run_id, p.dataset_id, p.shard_id, p.worker_id,
                p.lease_epoch as i64, p.progress_offset,
                if p.is_complete { 1i64 } else { 0i64 },
                p.updated_at_millis as i64
            ],
        )?;
        Ok(())
    }

    pub fn load_all_shard_progress_inner(&self) -> rusqlite::Result<Vec<ShardProgressEntry>> {
        let conn = self.conn.lock().expect("sqlite");
        let mut stmt = conn.prepare(
            "SELECT run_id,dataset_id,shard_id,worker_id,lease_epoch,progress_offset,is_complete,updated_at_millis \
             FROM shard_progress",
        )?;
        let entries = stmt
            .query_map([], |r| {
                Ok(ShardProgressEntry {
                    run_id: r.get(0)?,
                    dataset_id: r.get(1)?,
                    shard_id: r.get(2)?,
                    worker_id: r.get(3)?,
                    lease_epoch: r.get::<_, i64>(4)? as u64,
                    progress_offset: r.get(5)?,
                    is_complete: r.get::<_, i64>(6)? != 0,
                    updated_at_millis: r.get::<_, i64>(7)? as u64,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(entries)
    }

    // ── Gap 2: checkpoint commit votes (2PC) ─────────────────────────────────

    pub fn record_checkpoint_commit_inner(
        &self,
        run_id: &str,
        checkpoint_id: &str,
        worker_id: &str,
        committed_at_millis: u64,
    ) -> rusqlite::Result<()> {
        let conn = self.conn.lock().expect("sqlite");
        conn.execute(
            "INSERT OR IGNORE INTO checkpoint_commits(run_id,checkpoint_id,worker_id,committed_at_millis) \
             VALUES(?1,?2,?3,?4)",
            params![run_id, checkpoint_id, worker_id, committed_at_millis as i64],
        )?;
        Ok(())
    }

    pub fn load_all_checkpoint_commits_inner(
        &self,
    ) -> rusqlite::Result<Vec<(String, String, String)>> {
        let conn = self.conn.lock().expect("sqlite");
        let mut stmt = conn.prepare(
            "SELECT run_id,checkpoint_id,worker_id FROM checkpoint_commits ORDER BY run_id,checkpoint_id",
        )?;
        let rows = stmt
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }

    // ── Gap 5: audit events ──────────────────────────────────────────────────

    pub fn insert_audit_event_inner(&self, event: &AuditEventEntry) -> rusqlite::Result<()> {
        let conn = self.conn.lock().expect("sqlite");
        conn.execute(
            "INSERT INTO audit_events(occurred_at_millis,kind,outcome,subject,role,worker_id,run_id,dataset_id,checkpoint_id,reason,request_name) \
             VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)",
            params![
                event.occurred_at_millis as i64,
                event.kind, event.outcome, event.subject, event.role,
                event.worker_id, event.run_id, event.dataset_id,
                event.checkpoint_id, event.reason, event.request_name
            ],
        )?;
        Ok(())
    }
}

/// Implement the unified [`MetadataStore`] trait for `SqliteMetadataStore`.
///
/// Each method is a thin delegation that boxes any `rusqlite::Error` into the
/// trait's `PersistResult` error type.
impl MetadataStore for SqliteMetadataStore {
    fn save_run(&self, run: &RunRecord) -> PersistResult<()> {
        self.save_run(run).map_err(|e| Box::new(e) as _)
    }
    fn load_all_runs(&self) -> PersistResult<Vec<RunRecord>> {
        self.load_all_runs().map_err(|e| Box::new(e) as _)
    }
    fn save_worker(&self, w: &WorkerRecord, world_size: u32) -> PersistResult<()> {
        self.save_worker(w, world_size)
            .map_err(|e| Box::new(e) as _)
    }
    fn load_workers_for_run(&self, run_id: &str) -> PersistResult<Vec<(String, u32, u32, i32)>> {
        self.load_workers_for_run(run_id)
            .map_err(|e| Box::new(e) as _)
    }
    fn save_dataset(&self, d: &DatasetRecord) -> PersistResult<()> {
        self.save_dataset(d).map_err(|e| Box::new(e) as _)
    }
    fn save_checkpoint(
        &self,
        run_id: &str,
        idempotency_key: &str,
        rec: &CoordinatorCheckpointRecord,
    ) -> PersistResult<()> {
        self.save_checkpoint(run_id, idempotency_key, rec)
            .map_err(|e| Box::new(e) as _)
    }
    fn save_checkpoint_artifact(
        &self,
        run_id: &str,
        checkpoint_id: &str,
        a: &ArtifactRecord,
    ) -> PersistResult<()> {
        self.save_checkpoint_artifact(run_id, checkpoint_id, a)
            .map_err(|e| Box::new(e) as _)
    }
    fn mark_checkpoint_committed(
        &self,
        run_id: &str,
        checkpoint_id: &str,
        committed_at_millis: u64,
    ) -> PersistResult<()> {
        self.mark_checkpoint_committed(run_id, checkpoint_id, committed_at_millis)
            .map_err(|e| Box::new(e) as _)
    }
    fn load_latest_checkpoint(
        &self,
        run_id: &str,
    ) -> PersistResult<Option<CoordinatorCheckpointRecord>> {
        self.load_latest_checkpoint(run_id)
            .map_err(|e| Box::new(e) as _)
    }
    fn load_checkpoints_for_run(
        &self,
        run_id: &str,
    ) -> PersistResult<Vec<CoordinatorCheckpointRecord>> {
        self.load_checkpoints_for_run(run_id)
            .map_err(|e| Box::new(e) as _)
    }
    fn mark_checkpoint_failed(&self, run_id: &str, checkpoint_id: &str) -> PersistResult<()> {
        self.mark_checkpoint_failed(run_id, checkpoint_id)
            .map_err(|e| Box::new(e) as _)
    }
    fn set_latest_checkpoint(
        &self,
        run_id: &str,
        latest: Option<(&str, u64)>,
    ) -> PersistResult<()> {
        self.set_latest_checkpoint(run_id, latest)
            .map_err(|e| Box::new(e) as _)
    }
    fn delete_checkpoint(&self, run_id: &str, checkpoint_id: &str) -> PersistResult<()> {
        self.delete_checkpoint(run_id, checkpoint_id)
            .map_err(|e| Box::new(e) as _)
    }
    fn load_dataset(
        &self,
        run_id: &str,
        dataset_id: &str,
    ) -> PersistResult<Option<(String, Vec<ShardRecord>)>> {
        self.load_dataset(run_id, dataset_id)
            .map_err(|e| Box::new(e) as _)
    }
    fn save_shard_progress(&self, p: &ShardProgressEntry) -> PersistResult<()> {
        self.save_shard_progress_inner(p)
            .map_err(|e| Box::new(e) as _)
    }
    fn load_all_shard_progress(&self) -> PersistResult<Vec<ShardProgressEntry>> {
        self.load_all_shard_progress_inner()
            .map_err(|e| Box::new(e) as _)
    }
    fn record_checkpoint_commit(
        &self,
        run_id: &str,
        checkpoint_id: &str,
        worker_id: &str,
        committed_at_millis: u64,
    ) -> PersistResult<()> {
        self.record_checkpoint_commit_inner(run_id, checkpoint_id, worker_id, committed_at_millis)
            .map_err(|e| Box::new(e) as _)
    }
    fn load_all_checkpoint_commits(&self) -> PersistResult<Vec<(String, String, String)>> {
        self.load_all_checkpoint_commits_inner()
            .map_err(|e| Box::new(e) as _)
    }
    fn insert_audit_event(&self, event: &AuditEventEntry) -> PersistResult<()> {
        self.insert_audit_event_inner(event)
            .map_err(|e| Box::new(e) as _)
    }
    fn save_idempotency_key(&self, scope: &str, key: &str, value: &str) -> PersistResult<()> {
        self.save_idempotency_key_inner(scope, key, value)
            .map_err(|e| Box::new(e) as _)
    }
    fn load_all_idempotency_keys(&self) -> PersistResult<Vec<(String, String, String)>> {
        self.load_all_idempotency_keys_inner()
            .map_err(|e| Box::new(e) as _)
    }
    fn load_all_workers(&self) -> PersistResult<Vec<WorkerRecord>> {
        self.load_all_workers().map_err(|e| Box::new(e) as _)
    }
    fn load_all_datasets(&self) -> PersistResult<Vec<DatasetRecord>> {
        self.load_all_datasets().map_err(|e| Box::new(e) as _)
    }
    fn load_all_checkpoints(&self) -> PersistResult<Vec<(String, CoordinatorCheckpointRecord)>> {
        self.load_all_checkpoints().map_err(|e| Box::new(e) as _)
    }
}

fn to_millis(t: std::time::SystemTime) -> i64 {
    t.duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn from_millis(millis: u64) -> std::time::SystemTime {
    std::time::UNIX_EPOCH + std::time::Duration::from_millis(millis)
}

fn now_millis_i64() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

/// Round-trip a persisted `state as i32` back to the domain enum.
pub(crate) fn checkpoint_state_from_i32(value: i32) -> DomainCheckpointState {
    use DomainCheckpointState as S;
    for candidate in [
        S::Initiated,
        S::UploadInProgress,
        S::Validating,
        S::Committed,
        S::Failed,
        S::Aborted,
    ] {
        if candidate as i32 == value {
            return candidate;
        }
    }
    S::Initiated
}

#[cfg(test)]
mod tests {
    use super::*;
    use runtime_proto::runtime::v1::WorkerState;
    use tempfile::tempdir;

    #[test]
    fn sqlite_store_initializes_schema() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("meta.db");
        let store = SqliteMetadataStore::open(&path).unwrap();
        store.init_schema().unwrap();
        store.init_schema().unwrap();
    }

    #[test]
    fn dataset_metadata_survives_persistence_reload() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("meta.db");
        let store = SqliteMetadataStore::open(&path).unwrap();
        store.init_schema().unwrap();

        let dataset = DatasetRecord {
            run_id: "run-1".to_string(),
            dataset_id: "dataset-1".to_string(),
            name: "training-set".to_string(),
            dataset_uri: "file:///datasets/training".to_string(),
            format: "parquet".to_string(),
            shard_count: 2,
            dataset_version: "v1".to_string(),
            labels: std::collections::HashMap::new(),
            registered_at: std::time::SystemTime::now(),
            shards: vec![
                ShardRecord {
                    shard_id: "shard-0".to_string(),
                    dataset_id: "dataset-1".to_string(),
                    shard_uri: "file:///datasets/training/0.parquet".to_string(),
                    byte_range_start: 0,
                    byte_range_end: 10,
                    size_bytes: 100,
                    checksum: "sum0".to_string(),
                    checksum_algorithm: "sha256".to_string(),
                },
                ShardRecord {
                    shard_id: "shard-1".to_string(),
                    dataset_id: "dataset-1".to_string(),
                    shard_uri: "file:///datasets/training/1.parquet".to_string(),
                    byte_range_start: 11,
                    byte_range_end: 22,
                    size_bytes: 200,
                    checksum: "sum1".to_string(),
                    checksum_algorithm: "sha256".to_string(),
                },
            ],
        };
        store.save_dataset(&dataset).unwrap();

        let reopened = SqliteMetadataStore::open(&path).unwrap();
        let (name, shards) = reopened
            .load_dataset("run-1", "dataset-1")
            .unwrap()
            .expect("dataset should load");

        assert_eq!(name, "training-set");
        assert_eq!(shards.len(), 2);
        assert_eq!(shards[0].shard_id, "shard-0");
        assert_eq!(shards[0].shard_uri, "file:///datasets/training/0.parquet");
        assert_eq!(shards[0].size_bytes, 100);
        assert_eq!(shards[1].shard_id, "shard-1");
        assert_eq!(shards[1].shard_uri, "file:///datasets/training/1.parquet");
        assert_eq!(shards[1].size_bytes, 200);
    }

    #[test]
    fn committed_checkpoint_survives_persistence_reload() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("meta.db");
        let store = SqliteMetadataStore::open(&path).unwrap();
        store.init_schema().unwrap();

        let checkpoint = CoordinatorCheckpointRecord {
            checkpoint: CheckpointRecord::new(
                DomainRunId("run-1".to_string()),
                DomainCheckpointId("checkpoint-a".to_string()),
                42,
                1000,
            ),
            initiated_by_worker_id: "worker-0".to_string(),
            committed_at_millis: None,
            expected_worker_ids: vec!["worker-0".to_string(), "worker-1".to_string()],
            expected_artifact_count: 2,
        };
        store
            .save_checkpoint("run-1", "begin-a", &checkpoint)
            .unwrap();

        let artifact_0 = ArtifactRecord {
            worker_id: DomainWorkerId("worker-0".to_string()),
            artifact_name: "model.bin".to_string(),
            storage_key: "file:///ckpt/a/worker-0-model.bin".to_string(),
            size_bytes: 111,
            checksum: "abc".to_string(),
            checksum_algorithm: "sha256".to_string(),
        };
        let artifact_1 = ArtifactRecord {
            worker_id: DomainWorkerId("worker-1".to_string()),
            artifact_name: "model.bin".to_string(),
            storage_key: "file:///ckpt/a/worker-1-model.bin".to_string(),
            size_bytes: 222,
            checksum: "def".to_string(),
            checksum_algorithm: "sha256".to_string(),
        };
        store
            .save_checkpoint_artifact("run-1", "checkpoint-a", &artifact_0)
            .unwrap();
        store
            .save_checkpoint_artifact("run-1", "checkpoint-a", &artifact_1)
            .unwrap();
        store
            .mark_checkpoint_committed("run-1", "checkpoint-a", 2000)
            .unwrap();

        let reopened = SqliteMetadataStore::open(&path).unwrap();
        let latest = reopened
            .load_latest_checkpoint("run-1")
            .unwrap()
            .expect("latest committed checkpoint should load");

        assert_eq!(latest.checkpoint.checkpoint_id.0, "checkpoint-a");
        assert_eq!(latest.checkpoint.state, DomainCheckpointState::Committed);
        assert_eq!(latest.checkpoint.artifacts.len(), 2);
        assert_eq!(latest.checkpoint.artifacts[0], artifact_0);
        assert_eq!(latest.checkpoint.artifacts[1], artifact_1);
    }

    #[test]
    fn newer_partial_checkpoint_does_not_replace_committed_after_reload() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("meta.db");
        let store = SqliteMetadataStore::open(&path).unwrap();
        store.init_schema().unwrap();

        let committed = CoordinatorCheckpointRecord {
            checkpoint: CheckpointRecord::new(
                DomainRunId("run-1".to_string()),
                DomainCheckpointId("checkpoint-a".to_string()),
                1,
                100,
            ),
            initiated_by_worker_id: "worker-0".to_string(),
            committed_at_millis: None,
            expected_worker_ids: vec!["worker-0".to_string()],
            expected_artifact_count: 1,
        };
        store
            .save_checkpoint("run-1", "idem-a", &committed)
            .unwrap();
        store
            .mark_checkpoint_committed("run-1", "checkpoint-a", 150)
            .unwrap();

        let partial = CoordinatorCheckpointRecord {
            checkpoint: CheckpointRecord::new(
                DomainRunId("run-1".to_string()),
                DomainCheckpointId("checkpoint-b".to_string()),
                2,
                200,
            ),
            initiated_by_worker_id: "worker-1".to_string(),
            committed_at_millis: None,
            expected_worker_ids: vec!["worker-1".to_string()],
            expected_artifact_count: 1,
        };
        store.save_checkpoint("run-1", "idem-b", &partial).unwrap();
        store
            .save_checkpoint_artifact(
                "run-1",
                "checkpoint-b",
                &ArtifactRecord {
                    worker_id: DomainWorkerId("worker-1".to_string()),
                    artifact_name: "model.bin".to_string(),
                    storage_key: "file:///ckpt/b/worker-1-model.bin".to_string(),
                    size_bytes: 333,
                    checksum: "ghi".to_string(),
                    checksum_algorithm: "sha256".to_string(),
                },
            )
            .unwrap();

        let reopened = SqliteMetadataStore::open(&path).unwrap();
        let latest = reopened
            .load_latest_checkpoint("run-1")
            .unwrap()
            .expect("committed checkpoint should remain latest");
        assert_eq!(latest.checkpoint.checkpoint_id.0, "checkpoint-a");
    }
    #[test]
    fn worker_metadata_survives_persistence_reload() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("meta.db");
        let store = SqliteMetadataStore::open(&path).unwrap();
        store.init_schema().unwrap();
        let worker = WorkerRecord {
            run_id: "run-1".to_string(),
            worker_id: "worker-0".to_string(),
            rank: 2,
            host: "localhost".to_string(),
            sdk_version: "0.1.0".to_string(),
            protocol_version: "v1".to_string(),
            state: WorkerState::Active as i32,
            registered_at: std::time::SystemTime::now(),
            last_heartbeat_at: std::time::SystemTime::now(),
            next_heartbeat_deadline: std::time::SystemTime::now(),
            failure_deadline: std::time::SystemTime::now(),
        };
        store.save_worker(&worker, 4).unwrap();
        store.save_worker(&worker, 4).unwrap();

        let reopened = SqliteMetadataStore::open(&path).unwrap();
        let workers = reopened.load_workers_for_run("run-1").unwrap();
        assert_eq!(workers.len(), 1);
        assert_eq!(workers[0].0, "worker-0");
        assert_eq!(workers[0].1, 2);
        assert_eq!(workers[0].2, 4);
    }
}
