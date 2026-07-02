#![forbid(unsafe_code)]

//! PostgreSQL-backed metadata store for the coordinator.
//!
//! [`PostgresMetadataStore`] implements [`crate::persistence::MetadataStore`]
//! using a `sqlx::PgPool`, making it safe to share across multiple coordinator
//! replicas (as opposed to the single-node SQLite store).
//!
//! ## Sync wrapper
//!
//! The coordinator's `CoordinatorState` persistence helpers are called from
//! synchronous code paths inside a Tokio `multi_thread` runtime. The PostgreSQL
//! queries are inherently async (sqlx).  We bridge the two with
//! `tokio::task::block_in_place` + `Handle::current().block_on(...)`, which
//! parks the current Tokio worker thread and drives the async future to
//! completion without spawning a new OS thread or creating a nested runtime.
//! This is safe on the multi-thread scheduler (the coordinator uses
//! `#[tokio::main]` which defaults to `multi_thread`).
//!
//! ## Schema
//!
//! The schema is identical to the SQLite schema defined in
//! [`crate::persistence::SqliteMetadataStore::init_schema`] modulo two
//! differences:
//!
//! 1. `INTEGER PRIMARY KEY` → `BIGINT PRIMARY KEY` (PostgreSQL convention).
//! 2. `INSERT OR REPLACE` is not valid PostgreSQL syntax; we use
//!    `INSERT … ON CONFLICT DO UPDATE SET …` throughout (which was already the
//!    pattern used in the SQLite commit path).

use sqlx::PgPool;
use tracing::info;

use crate::persistence::checkpoint_state_from_i32;
use crate::persistence::{AuditEventEntry, MetadataStore, PersistResult, ShardProgressEntry};
use crate::state::{
    CoordinatorCheckpointRecord, DatasetRecord, RunRecord, ShardRecord, WorkerRecord,
};
use runtime_checkpoint::{
    ArtifactRecord, CheckpointId as DomainCheckpointId, CheckpointRecord,
    CheckpointState as DomainCheckpointState, RunId as DomainRunId, WorkerId as DomainWorkerId,
};
use runtime_proto::runtime::v1::CheckpointId as ProtoCheckpointId;

/// Production-grade, replicated metadata store backed by PostgreSQL.
///
/// Create with [`PostgresMetadataStore::connect`], then call
/// [`PostgresMetadataStore::init_schema`] before handing it to
/// [`crate::state::CoordinatorState::with_metadata_store`].
#[derive(Clone, Debug)]
pub struct PostgresMetadataStore {
    pool: PgPool,
}

impl PostgresMetadataStore {
    /// Open a connection pool to the given PostgreSQL DSN.
    ///
    /// The DSN should be a `postgres://` or `postgresql://` URL, e.g.:
    /// `postgres://user:pass@host:5432/dtruntime`
    pub async fn connect(url: &str) -> Result<Self, sqlx::Error> {
        info!(url = %redact_pg_url(url), "postgres metadata store: connecting");
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(16)
            .connect(url)
            .await?;
        info!("postgres metadata store: connected");
        Ok(Self { pool })
    }

    /// Create all required tables (idempotent — safe to call on every startup).
    ///
    /// Uses `sqlx::raw_sql` (sqlx 0.8+) which accepts multiple semicolon-
    /// separated DDL statements without wrapping them in a prepared statement.
    pub async fn init_schema(&self) -> Result<(), sqlx::Error> {
        sqlx::raw_sql(
            r#"
CREATE TABLE IF NOT EXISTS runs (
  run_id               TEXT PRIMARY KEY,
  name                 TEXT NOT NULL,
  status               INTEGER NOT NULL,
  created_at_millis    BIGINT NOT NULL,
  idempotency_key      TEXT
);

-- Run lifecycle columns (idempotent for databases created before they existed;
-- a real migration framework lands with the Phase 6 storage work).
ALTER TABLE runs ADD COLUMN IF NOT EXISTS run_state INTEGER NOT NULL DEFAULT 1;
ALTER TABLE runs ADD COLUMN IF NOT EXISTS state_reason TEXT NOT NULL DEFAULT '';
ALTER TABLE runs ADD COLUMN IF NOT EXISTS world_size INTEGER NOT NULL DEFAULT 0;
ALTER TABLE runs ADD COLUMN IF NOT EXISTS min_world_size INTEGER NOT NULL DEFAULT 0;
ALTER TABLE runs ADD COLUMN IF NOT EXISTS max_world_size INTEGER NOT NULL DEFAULT 0;
ALTER TABLE runs ADD COLUMN IF NOT EXISTS settle_timeout_ms INTEGER NOT NULL DEFAULT 0;
ALTER TABLE runs ADD COLUMN IF NOT EXISTS assignment_policy INTEGER NOT NULL DEFAULT 0;
ALTER TABLE runs ADD COLUMN IF NOT EXISTS storage_backend_kind INTEGER NOT NULL DEFAULT 0;
ALTER TABLE runs ADD COLUMN IF NOT EXISTS run_version TEXT NOT NULL DEFAULT '';
ALTER TABLE runs ADD COLUMN IF NOT EXISTS latest_checkpoint_id TEXT;

CREATE TABLE IF NOT EXISTS workers (
  run_id               TEXT NOT NULL,
  worker_id            TEXT NOT NULL,
  rank                 INTEGER NOT NULL,
  world_size           INTEGER NOT NULL,
  status               INTEGER NOT NULL,
  last_heartbeat_millis BIGINT,
  idempotency_key      TEXT,
  PRIMARY KEY (run_id, worker_id)
);

CREATE TABLE IF NOT EXISTS datasets (
  run_id               TEXT NOT NULL,
  dataset_id           TEXT NOT NULL,
  name                 TEXT NOT NULL,
  num_shards           INTEGER NOT NULL,
  idempotency_key      TEXT,
  PRIMARY KEY (run_id, dataset_id)
);

CREATE TABLE IF NOT EXISTS shards (
  run_id               TEXT NOT NULL,
  dataset_id           TEXT NOT NULL,
  shard_id             TEXT NOT NULL,
  shard_index          INTEGER NOT NULL,
  shard_uri            TEXT NOT NULL,
  num_records          BIGINT,
  size_bytes           BIGINT,
  PRIMARY KEY (run_id, dataset_id, shard_id)
);

CREATE TABLE IF NOT EXISTS checkpoints (
  run_id                   TEXT NOT NULL,
  checkpoint_id            TEXT NOT NULL,
  state                    INTEGER NOT NULL,
  initiated_by_worker_id   TEXT NOT NULL,
  step                     BIGINT NOT NULL,
  created_at_millis        BIGINT NOT NULL,
  updated_at_millis        BIGINT NOT NULL,
  committed_at_millis      BIGINT,
  expected_worker_ids_json TEXT NOT NULL DEFAULT '[]',
  idempotency_key          TEXT,
  PRIMARY KEY (run_id, checkpoint_id)
);

CREATE TABLE IF NOT EXISTS checkpoint_artifacts (
  run_id               TEXT NOT NULL,
  checkpoint_id        TEXT NOT NULL,
  worker_id            TEXT NOT NULL,
  artifact_name        TEXT NOT NULL,
  artifact_uri         TEXT NOT NULL,
  size_bytes           BIGINT NOT NULL,
  checksum             TEXT NOT NULL,
  checksum_algorithm   TEXT NOT NULL,
  idempotency_key      TEXT,
  PRIMARY KEY (run_id, checkpoint_id, worker_id, artifact_name)
);

CREATE TABLE IF NOT EXISTS latest_checkpoints (
  run_id               TEXT PRIMARY KEY,
  checkpoint_id        TEXT NOT NULL,
  committed_at_millis  BIGINT NOT NULL
);

-- Gap 1: shard progress — enables resume-from-mid-shard across coordinator restarts
CREATE TABLE IF NOT EXISTS shard_progress (
  run_id             TEXT NOT NULL,
  dataset_id         TEXT NOT NULL,
  shard_id           TEXT NOT NULL,
  worker_id          TEXT NOT NULL,
  lease_epoch        BIGINT NOT NULL,
  progress_offset    BIGINT NOT NULL,
  is_complete        BOOLEAN NOT NULL,
  updated_at_millis  BIGINT NOT NULL,
  PRIMARY KEY (run_id, dataset_id, shard_id)
);

-- Gap 2: per-worker checkpoint commit votes (distributed 2PC)
-- Full-hydration columns (idempotent for databases created before they
-- existed; a real migration framework lands with the Phase 6 storage work).
ALTER TABLE workers ADD COLUMN IF NOT EXISTS host TEXT NOT NULL DEFAULT '';
ALTER TABLE workers ADD COLUMN IF NOT EXISTS sdk_version TEXT NOT NULL DEFAULT '';
ALTER TABLE workers ADD COLUMN IF NOT EXISTS protocol_version TEXT NOT NULL DEFAULT '';
ALTER TABLE workers ADD COLUMN IF NOT EXISTS registered_at_millis BIGINT NOT NULL DEFAULT 0;
ALTER TABLE datasets ADD COLUMN IF NOT EXISTS dataset_uri TEXT NOT NULL DEFAULT '';
ALTER TABLE datasets ADD COLUMN IF NOT EXISTS format TEXT NOT NULL DEFAULT '';
ALTER TABLE datasets ADD COLUMN IF NOT EXISTS dataset_version TEXT NOT NULL DEFAULT '';
ALTER TABLE datasets ADD COLUMN IF NOT EXISTS labels_json TEXT NOT NULL DEFAULT '{}';
ALTER TABLE datasets ADD COLUMN IF NOT EXISTS registered_at_millis BIGINT NOT NULL DEFAULT 0;
ALTER TABLE shards ADD COLUMN IF NOT EXISTS byte_range_start BIGINT NOT NULL DEFAULT 0;
ALTER TABLE shards ADD COLUMN IF NOT EXISTS byte_range_end BIGINT NOT NULL DEFAULT 0;
ALTER TABLE shards ADD COLUMN IF NOT EXISTS checksum TEXT NOT NULL DEFAULT '';
ALTER TABLE shards ADD COLUMN IF NOT EXISTS checksum_algorithm TEXT NOT NULL DEFAULT '';
ALTER TABLE checkpoints ADD COLUMN IF NOT EXISTS expected_worker_ids_json TEXT NOT NULL DEFAULT '[]';

-- Durable idempotency-key maps: one row per in-memory DashMap entry so
-- retried RPCs stay idempotent across coordinator restarts.
-- scope IN (run, worker, dataset, checkpoint, checkpoint_artifact).
CREATE TABLE IF NOT EXISTS idempotency_keys (
  scope TEXT NOT NULL,
  key   TEXT NOT NULL,
  value TEXT NOT NULL,
  PRIMARY KEY (scope, key)
);

CREATE TABLE IF NOT EXISTS checkpoint_commits (
  run_id              TEXT NOT NULL,
  checkpoint_id       TEXT NOT NULL,
  worker_id           TEXT NOT NULL,
  committed_at_millis BIGINT NOT NULL,
  PRIMARY KEY (run_id, checkpoint_id, worker_id)
);

-- Gap 5: durable audit log
CREATE TABLE IF NOT EXISTS audit_events (
  id                 BIGSERIAL PRIMARY KEY,
  occurred_at_millis BIGINT NOT NULL,
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
"#,
        )
        .execute(&self.pool)
        .await?;
        info!("postgres metadata store: schema initialised");
        Ok(())
    }

    /// Attempt to acquire a PostgreSQL session-level advisory lock.
    ///
    /// Returns `true` if the lock was acquired (this replica is now the
    /// leader), `false` if another replica already holds it (standby).
    ///
    /// The lock is automatically released when the database connection pool
    /// is dropped (i.e. when the coordinator process exits), so the standby
    /// takes over within one retry interval.
    pub async fn try_acquire_advisory_lock(&self, key: i64) -> Result<bool, sqlx::Error> {
        let row: (bool,) = sqlx::query_as("SELECT pg_try_advisory_lock($1)")
            .bind(key)
            .fetch_one(&self.pool)
            .await?;
        Ok(row.0)
    }

    /// Helper: run an async closure synchronously inside a Tokio worker thread.
    ///
    /// Uses `block_in_place` so the Tokio scheduler can park this thread and
    /// avoid blocking the entire runtime.
    fn block<F, T>(&self, f: F) -> T
    where
        F: std::future::Future<Output = T>,
    {
        tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(f))
    }
}

impl MetadataStore for PostgresMetadataStore {
    fn save_run(&self, run: &RunRecord) -> PersistResult<()> {
        self.block(async {
            sqlx::query(
                r#"
INSERT INTO runs (run_id, name, status, created_at_millis, idempotency_key,
                  run_state, state_reason, world_size, min_world_size, max_world_size,
                  settle_timeout_ms, assignment_policy, storage_backend_kind, run_version)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
ON CONFLICT (run_id) DO UPDATE SET
  name = EXCLUDED.name,
  status = EXCLUDED.status,
  created_at_millis = EXCLUDED.created_at_millis,
  idempotency_key = EXCLUDED.idempotency_key,
  run_state = EXCLUDED.run_state,
  state_reason = EXCLUDED.state_reason,
  world_size = EXCLUDED.world_size,
  min_world_size = EXCLUDED.min_world_size,
  max_world_size = EXCLUDED.max_world_size,
  settle_timeout_ms = EXCLUDED.settle_timeout_ms,
  assignment_policy = EXCLUDED.assignment_policy,
  storage_backend_kind = EXCLUDED.storage_backend_kind,
  run_version = EXCLUDED.run_version
"#,
            )
            .bind(&run.run_id)
            .bind(&run.run_name)
            .bind(run.latest_checkpoint_state)
            .bind(to_millis(run.registered_at))
            .bind(if run.idempotency_key.is_empty() {
                None
            } else {
                Some(run.idempotency_key.as_str())
            })
            .bind(run.state)
            .bind(&run.state_reason)
            .bind(run.world_size as i32)
            .bind(run.min_world_size as i32)
            .bind(run.max_world_size as i32)
            .bind(run.rendezvous_settle_timeout_ms as i32)
            .bind(run.assignment_policy)
            .bind(run.storage_backend_kind)
            .bind(&run.run_version)
            .execute(&self.pool)
            .await
            .map(|_| ())
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
        })
    }

    fn load_all_runs(&self) -> PersistResult<Vec<RunRecord>> {
        self.block(async {
            type Row = (
                String,
                String,
                i64,
                Option<String>,
                i32,
                String,
                i32,
                i32,
                i32,
                i32,
                i32,
                i32,
                String,
                i32,
                Option<String>,
            );
            let rows: Vec<Row> = sqlx::query_as(
                "SELECT run_id, name, created_at_millis, idempotency_key, run_state, state_reason, \
                 world_size, min_world_size, max_world_size, settle_timeout_ms, assignment_policy, \
                 storage_backend_kind, run_version, status, latest_checkpoint_id FROM runs ORDER BY run_id",
            )
            .fetch_all(&self.pool)
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

            Ok(rows
                .into_iter()
                .map(
                    |(
                        run_id,
                        run_name,
                        created_at_millis,
                        idempotency_key,
                        state,
                        state_reason,
                        world_size,
                        min_world_size,
                        max_world_size,
                        settle_timeout_ms,
                        assignment_policy,
                        storage_backend_kind,
                        run_version,
                        status,
                        latest_checkpoint_id,
                    )| RunRecord {
                        run_id,
                        run_name,
                        registered_at: from_millis(created_at_millis.max(0) as u64),
                        idempotency_key: idempotency_key.unwrap_or_default(),
                        state,
                        state_reason,
                        world_size: world_size.max(0) as u32,
                        min_world_size: min_world_size.max(0) as u32,
                        max_world_size: max_world_size.max(0) as u32,
                        rendezvous_settle_timeout_ms: settle_timeout_ms.max(0) as u32,
                        assignment_policy,
                        storage_backend_kind,
                        run_version,
                        active_worker_count: 0,
                        latest_checkpoint_state: status,
                        latest_checkpoint_id: latest_checkpoint_id
                            .map(|value| ProtoCheckpointId { value }),
                        state_updated_at: std::time::SystemTime::now(),
                    },
                )
                .collect())
        })
    }

    fn save_worker(&self, w: &WorkerRecord, world_size: u32) -> PersistResult<()> {
        self.block(async {
            sqlx::query(
                r#"
INSERT INTO workers (run_id, worker_id, rank, world_size, status, last_heartbeat_millis,
                     idempotency_key, host, sdk_version, protocol_version, registered_at_millis)
VALUES ($1, $2, $3, $4, $5, $6, NULL, $7, $8, $9, $10)
ON CONFLICT (run_id, worker_id) DO UPDATE SET
  rank = EXCLUDED.rank,
  world_size = EXCLUDED.world_size,
  status = EXCLUDED.status,
  last_heartbeat_millis = EXCLUDED.last_heartbeat_millis,
  host = EXCLUDED.host,
  sdk_version = EXCLUDED.sdk_version,
  protocol_version = EXCLUDED.protocol_version,
  registered_at_millis = EXCLUDED.registered_at_millis
"#,
            )
            .bind(&w.run_id)
            .bind(&w.worker_id)
            .bind(w.rank as i32)
            .bind(world_size as i32)
            .bind(w.state)
            .bind(to_millis(w.last_heartbeat_at))
            .bind(&w.host)
            .bind(&w.sdk_version)
            .bind(&w.protocol_version)
            .bind(to_millis(w.registered_at))
            .execute(&self.pool)
            .await
            .map(|_| ())
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
        })
    }

    fn load_workers_for_run(&self, run_id: &str) -> PersistResult<Vec<(String, u32, u32, i32)>> {
        self.block(async {
            let rows: Vec<(String, i32, i32, i32)> = sqlx::query_as(
                "SELECT worker_id, rank, world_size, status FROM workers WHERE run_id = $1 ORDER BY worker_id",
            )
            .bind(run_id)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

            Ok(rows
                .into_iter()
                .map(|(wid, rank, ws, status)| (wid, rank as u32, ws as u32, status))
                .collect())
        })
    }

    fn save_dataset(&self, d: &DatasetRecord) -> PersistResult<()> {
        self.block(async {
            sqlx::query(
                r#"
INSERT INTO datasets (run_id, dataset_id, name, num_shards, idempotency_key,
                      dataset_uri, format, dataset_version, labels_json, registered_at_millis)
VALUES ($1, $2, $3, $4, NULL, $5, $6, $7, $8, $9)
ON CONFLICT (run_id, dataset_id) DO UPDATE SET
  name = EXCLUDED.name,
  num_shards = EXCLUDED.num_shards,
  dataset_uri = EXCLUDED.dataset_uri,
  format = EXCLUDED.format,
  dataset_version = EXCLUDED.dataset_version,
  labels_json = EXCLUDED.labels_json,
  registered_at_millis = EXCLUDED.registered_at_millis
"#,
            )
            .bind(&d.run_id)
            .bind(&d.dataset_id)
            .bind(&d.name)
            .bind(d.shard_count as i32)
            .bind(&d.dataset_uri)
            .bind(&d.format)
            .bind(&d.dataset_version)
            .bind(serde_json::to_string(&d.labels).unwrap_or_else(|_| "{}".to_string()))
            .bind(to_millis(d.registered_at))
            .execute(&self.pool)
            .await
            .map(|_| ())
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

            for (idx, s) in d.shards.iter().enumerate() {
                sqlx::query(
                    r#"
INSERT INTO shards (run_id, dataset_id, shard_id, shard_index, shard_uri, num_records, size_bytes,
                    byte_range_start, byte_range_end, checksum, checksum_algorithm)
VALUES ($1, $2, $3, $4, $5, NULL, $6, $7, $8, $9, $10)
ON CONFLICT (run_id, dataset_id, shard_id) DO UPDATE SET
  shard_index = EXCLUDED.shard_index,
  shard_uri   = EXCLUDED.shard_uri,
  size_bytes  = EXCLUDED.size_bytes,
  byte_range_start = EXCLUDED.byte_range_start,
  byte_range_end   = EXCLUDED.byte_range_end,
  checksum         = EXCLUDED.checksum,
  checksum_algorithm = EXCLUDED.checksum_algorithm
"#,
                )
                .bind(&d.run_id)
                .bind(&d.dataset_id)
                .bind(&s.shard_id)
                .bind(idx as i32)
                .bind(&s.shard_uri)
                .bind(s.size_bytes)
                .bind(s.byte_range_start)
                .bind(s.byte_range_end)
                .bind(&s.checksum)
                .bind(&s.checksum_algorithm)
                .execute(&self.pool)
                .await
                .map(|_| ())
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
            }
            Ok(())
        })
    }

    fn save_checkpoint(
        &self,
        run_id: &str,
        idempotency_key: &str,
        rec: &CoordinatorCheckpointRecord,
    ) -> PersistResult<()> {
        let idem: Option<String> = if idempotency_key.is_empty() {
            None
        } else {
            Some(idempotency_key.to_string())
        };
        self.block(async {
            sqlx::query(
                r#"
INSERT INTO checkpoints
  (run_id, checkpoint_id, state, initiated_by_worker_id, step,
   created_at_millis, updated_at_millis, committed_at_millis, idempotency_key,
   expected_worker_ids_json)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
ON CONFLICT (run_id, checkpoint_id) DO UPDATE SET
  state                  = EXCLUDED.state,
  initiated_by_worker_id = EXCLUDED.initiated_by_worker_id,
  step                   = EXCLUDED.step,
  updated_at_millis      = EXCLUDED.updated_at_millis,
  committed_at_millis    = EXCLUDED.committed_at_millis,
  idempotency_key        = EXCLUDED.idempotency_key,
  expected_worker_ids_json = EXCLUDED.expected_worker_ids_json
"#,
            )
            .bind(run_id)
            .bind(&rec.checkpoint.checkpoint_id.0)
            .bind(rec.checkpoint.state as i32)
            .bind(&rec.initiated_by_worker_id)
            .bind(rec.checkpoint.step as i64)
            .bind(rec.checkpoint.created_at_millis as i64)
            .bind(rec.checkpoint.updated_at_millis as i64)
            .bind(rec.committed_at_millis.map(|v| v as i64))
            .bind(idem)
            .bind(
                serde_json::to_string(&rec.expected_worker_ids)
                    .unwrap_or_else(|_| "[]".to_string()),
            )
            .execute(&self.pool)
            .await
            .map(|_| ())
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
        })
    }

    fn save_checkpoint_artifact(
        &self,
        run_id: &str,
        checkpoint_id: &str,
        a: &ArtifactRecord,
    ) -> PersistResult<()> {
        self.block(async {
            sqlx::query(
                r#"
INSERT INTO checkpoint_artifacts
  (run_id, checkpoint_id, worker_id, artifact_name, artifact_uri,
   size_bytes, checksum, checksum_algorithm, idempotency_key)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NULL)
ON CONFLICT (run_id, checkpoint_id, worker_id, artifact_name) DO UPDATE SET
  artifact_uri       = EXCLUDED.artifact_uri,
  size_bytes         = EXCLUDED.size_bytes,
  checksum           = EXCLUDED.checksum,
  checksum_algorithm = EXCLUDED.checksum_algorithm
"#,
            )
            .bind(run_id)
            .bind(checkpoint_id)
            .bind(&a.worker_id.0)
            .bind(&a.artifact_name)
            .bind(&a.storage_key)
            .bind(a.size_bytes as i64)
            .bind(&a.checksum)
            .bind(&a.checksum_algorithm)
            .execute(&self.pool)
            .await
            .map(|_| ())
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
        })
    }

    fn mark_checkpoint_committed(
        &self,
        run_id: &str,
        checkpoint_id: &str,
        committed_at_millis: u64,
    ) -> PersistResult<()> {
        let ts = committed_at_millis as i64;
        self.block(async {
            // Update the checkpoint row to Committed state.
            sqlx::query(
                r#"
UPDATE checkpoints
SET committed_at_millis = $3,
    state               = $4,
    updated_at_millis   = $3
WHERE run_id = $1 AND checkpoint_id = $2
"#,
            )
            .bind(run_id)
            .bind(checkpoint_id)
            .bind(ts)
            .bind(DomainCheckpointState::Committed as i32)
            .execute(&self.pool)
            .await
            .map(|_| ())
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

            // Upsert the latest_checkpoints pointer — only advances forward.
            sqlx::query(
                r#"
INSERT INTO latest_checkpoints (run_id, checkpoint_id, committed_at_millis)
VALUES ($1, $2, $3)
ON CONFLICT (run_id) DO UPDATE SET
  checkpoint_id       = EXCLUDED.checkpoint_id,
  committed_at_millis = EXCLUDED.committed_at_millis
"#,
            )
            .bind(run_id)
            .bind(checkpoint_id)
            .bind(ts)
            .execute(&self.pool)
            .await
            .map(|_| ())
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
        })
    }

    fn load_latest_checkpoint(
        &self,
        run_id: &str,
    ) -> PersistResult<Option<CoordinatorCheckpointRecord>> {
        self.block(async {
            // Look up the latest committed checkpoint ID.
            let row: Option<(String, i64)> = sqlx::query_as(
                "SELECT checkpoint_id, committed_at_millis FROM latest_checkpoints WHERE run_id = $1",
            )
            .bind(run_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

            let Some((checkpoint_id, committed_at)) = row else {
                return Ok(None);
            };

            // Load the checkpoint row.
            let ck: Option<(i64, i64, i64, String)> = sqlx::query_as(
                "SELECT step, created_at_millis, updated_at_millis, initiated_by_worker_id \
                 FROM checkpoints WHERE run_id = $1 AND checkpoint_id = $2 AND state = $3",
            )
            .bind(run_id)
            .bind(&checkpoint_id)
            .bind(DomainCheckpointState::Committed as i32)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

            let Some((step, created, updated, initiator)) = ck else {
                return Ok(None);
            };

            // Load artifacts.
            let artifact_rows: Vec<(String, String, String, i64, String, String)> =
                sqlx::query_as(
                    "SELECT worker_id, artifact_name, artifact_uri, size_bytes, checksum, checksum_algorithm \
                     FROM checkpoint_artifacts \
                     WHERE run_id = $1 AND checkpoint_id = $2 \
                     ORDER BY worker_id, artifact_name",
                )
                .bind(run_id)
                .bind(&checkpoint_id)
                .fetch_all(&self.pool)
                .await
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

            let artifacts: Vec<ArtifactRecord> = artifact_rows
                .into_iter()
                .map(|(wid, name, uri, size, cksum, algo)| ArtifactRecord {
                    worker_id: DomainWorkerId(wid),
                    artifact_name: name,
                    storage_key: uri,
                    size_bytes: size as u64,
                    checksum: cksum,
                    checksum_algorithm: algo,
                })
                .collect();

            let expected_worker_ids = artifacts
                .iter()
                .map(|a| a.worker_id.0.clone())
                .collect::<Vec<_>>();

            Ok(Some(CoordinatorCheckpointRecord {
                checkpoint: CheckpointRecord {
                    run_id: DomainRunId(run_id.to_string()),
                    checkpoint_id: DomainCheckpointId(checkpoint_id),
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
        })
    }

    fn load_checkpoints_for_run(
        &self,
        run_id: &str,
    ) -> PersistResult<Vec<CoordinatorCheckpointRecord>> {
        self.block(async {
            type CkRow = (String, i64, i32, i64, i64, Option<i64>, String);
            let rows: Vec<CkRow> = sqlx::query_as(
                "SELECT checkpoint_id, step, state, created_at_millis, updated_at_millis, \
                 committed_at_millis, initiated_by_worker_id \
                 FROM checkpoints WHERE run_id = $1 ORDER BY step DESC, checkpoint_id DESC",
            )
            .bind(run_id)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

            let mut records = Vec::with_capacity(rows.len());
            for (checkpoint_id, step, state, created, updated, committed_at, initiator) in rows {
                let artifact_rows: Vec<(String, String, String, i64, String, String)> =
                    sqlx::query_as(
                        "SELECT worker_id, artifact_name, artifact_uri, size_bytes, checksum, checksum_algorithm \
                         FROM checkpoint_artifacts WHERE run_id = $1 AND checkpoint_id = $2 \
                         ORDER BY worker_id, artifact_name",
                    )
                    .bind(run_id)
                    .bind(&checkpoint_id)
                    .fetch_all(&self.pool)
                    .await
                    .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
                let artifacts: Vec<ArtifactRecord> = artifact_rows
                    .into_iter()
                    .map(|(wid, name, uri, size, cksum, algo)| ArtifactRecord {
                        worker_id: DomainWorkerId(wid),
                        artifact_name: name,
                        storage_key: uri,
                        size_bytes: size as u64,
                        checksum: cksum,
                        checksum_algorithm: algo,
                    })
                    .collect();
                let expected_worker_ids: Vec<String> =
                    artifacts.iter().map(|a| a.worker_id.0.clone()).collect();
                records.push(CoordinatorCheckpointRecord {
                    checkpoint: CheckpointRecord {
                        run_id: DomainRunId(run_id.to_string()),
                        checkpoint_id: DomainCheckpointId(checkpoint_id),
                        step: step as u64,
                        state: crate::persistence::checkpoint_state_from_i32(state),
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
        })
    }

    fn mark_checkpoint_failed(&self, run_id: &str, checkpoint_id: &str) -> PersistResult<()> {
        self.block(async {
            sqlx::query(
                "UPDATE checkpoints SET state = $3, updated_at_millis = $4 \
                 WHERE run_id = $1 AND checkpoint_id = $2",
            )
            .bind(run_id)
            .bind(checkpoint_id)
            .bind(DomainCheckpointState::Failed as i32)
            .bind(to_millis(std::time::SystemTime::now()))
            .execute(&self.pool)
            .await
            .map(|_| ())
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
        })
    }

    fn set_latest_checkpoint(
        &self,
        run_id: &str,
        latest: Option<(&str, u64)>,
    ) -> PersistResult<()> {
        self.block(async {
            match latest {
                Some((checkpoint_id, committed_at_millis)) => sqlx::query(
                    "INSERT INTO latest_checkpoints (run_id, checkpoint_id, committed_at_millis) \
                     VALUES ($1, $2, $3) \
                     ON CONFLICT (run_id) DO UPDATE SET \
                     checkpoint_id = EXCLUDED.checkpoint_id, \
                     committed_at_millis = EXCLUDED.committed_at_millis",
                )
                .bind(run_id)
                .bind(checkpoint_id)
                .bind(committed_at_millis as i64)
                .execute(&self.pool)
                .await
                .map(|_| ())
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>),
                None => sqlx::query("DELETE FROM latest_checkpoints WHERE run_id = $1")
                    .bind(run_id)
                    .execute(&self.pool)
                    .await
                    .map(|_| ())
                    .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>),
            }
        })
    }

    fn delete_checkpoint(&self, run_id: &str, checkpoint_id: &str) -> PersistResult<()> {
        self.block(async {
            for sql in [
                "DELETE FROM checkpoint_artifacts WHERE run_id = $1 AND checkpoint_id = $2",
                "DELETE FROM checkpoint_commits WHERE run_id = $1 AND checkpoint_id = $2",
                "DELETE FROM checkpoints WHERE run_id = $1 AND checkpoint_id = $2",
            ] {
                sqlx::query(sql)
                    .bind(run_id)
                    .bind(checkpoint_id)
                    .execute(&self.pool)
                    .await
                    .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
            }
            Ok(())
        })
    }

    fn load_dataset(
        &self,
        run_id: &str,
        dataset_id: &str,
    ) -> PersistResult<Option<(String, Vec<ShardRecord>)>> {
        self.block(async {
            let name: Option<(String,)> =
                sqlx::query_as("SELECT name FROM datasets WHERE run_id = $1 AND dataset_id = $2")
                    .bind(run_id)
                    .bind(dataset_id)
                    .fetch_optional(&self.pool)
                    .await
                    .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

            let Some((name,)) = name else {
                return Ok(None);
            };

            let shard_rows: Vec<(String, String, i64)> = sqlx::query_as(
                "SELECT shard_id, shard_uri, size_bytes \
                 FROM shards WHERE run_id = $1 AND dataset_id = $2 ORDER BY shard_index",
            )
            .bind(run_id)
            .bind(dataset_id)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

            let shards = shard_rows
                .into_iter()
                .map(|(sid, uri, sz)| ShardRecord {
                    shard_id: sid,
                    dataset_id: dataset_id.to_string(),
                    shard_uri: uri,
                    byte_range_start: 0,
                    byte_range_end: 0,
                    size_bytes: sz,
                    checksum: String::new(),
                    checksum_algorithm: String::new(),
                })
                .collect();

            Ok(Some((name, shards)))
        })
    }

    // ── Gap 1: shard progress ────────────────────────────────────────────────

    fn save_shard_progress(&self, p: &ShardProgressEntry) -> PersistResult<()> {
        self.block(async {
            sqlx::query(
                r#"
INSERT INTO shard_progress
  (run_id, dataset_id, shard_id, worker_id, lease_epoch, progress_offset, is_complete, updated_at_millis)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (run_id, dataset_id, shard_id) DO UPDATE SET
  worker_id         = EXCLUDED.worker_id,
  lease_epoch       = EXCLUDED.lease_epoch,
  progress_offset   = EXCLUDED.progress_offset,
  is_complete       = EXCLUDED.is_complete,
  updated_at_millis = EXCLUDED.updated_at_millis
"#,
            )
            .bind(&p.run_id)
            .bind(&p.dataset_id)
            .bind(&p.shard_id)
            .bind(&p.worker_id)
            .bind(p.lease_epoch as i64)
            .bind(p.progress_offset)
            .bind(p.is_complete)
            .bind(p.updated_at_millis as i64)
            .execute(&self.pool)
            .await
            .map(|_| ())
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
        })
    }

    fn load_all_shard_progress(&self) -> PersistResult<Vec<ShardProgressEntry>> {
        // (run_id, dataset_id, shard_id, worker_id, lease_epoch,
        //  progress_offset, is_complete, updated_at_millis)
        type ShardProgressRow = (String, String, String, String, i64, i64, bool, i64);
        self.block(async {
            let rows: Vec<ShardProgressRow> = sqlx::query_as(
                "SELECT run_id, dataset_id, shard_id, worker_id, lease_epoch, \
                     progress_offset, is_complete, updated_at_millis FROM shard_progress",
            )
            .fetch_all(&self.pool)
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

            Ok(rows
                .into_iter()
                .map(
                    |(run_id, dataset_id, shard_id, worker_id, le, po, ic, ua)| {
                        ShardProgressEntry {
                            run_id,
                            dataset_id,
                            shard_id,
                            worker_id,
                            lease_epoch: le as u64,
                            progress_offset: po,
                            is_complete: ic,
                            updated_at_millis: ua as u64,
                        }
                    },
                )
                .collect())
        })
    }

    // ── Gap 2: checkpoint commit votes ───────────────────────────────────────

    fn record_checkpoint_commit(
        &self,
        run_id: &str,
        checkpoint_id: &str,
        worker_id: &str,
        committed_at_millis: u64,
    ) -> PersistResult<()> {
        self.block(async {
            sqlx::query(
                r#"
INSERT INTO checkpoint_commits (run_id, checkpoint_id, worker_id, committed_at_millis)
VALUES ($1, $2, $3, $4)
ON CONFLICT (run_id, checkpoint_id, worker_id) DO NOTHING
"#,
            )
            .bind(run_id)
            .bind(checkpoint_id)
            .bind(worker_id)
            .bind(committed_at_millis as i64)
            .execute(&self.pool)
            .await
            .map(|_| ())
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
        })
    }

    fn load_all_checkpoint_commits(&self) -> PersistResult<Vec<(String, String, String)>> {
        self.block(async {
            let rows: Vec<(String, String, String)> = sqlx::query_as(
                "SELECT run_id, checkpoint_id, worker_id FROM checkpoint_commits \
                 ORDER BY run_id, checkpoint_id",
            )
            .fetch_all(&self.pool)
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
            Ok(rows)
        })
    }

    // ── Gap 5: durable audit log ─────────────────────────────────────────────

    fn save_idempotency_key(&self, scope: &str, key: &str, value: &str) -> PersistResult<()> {
        self.block(async {
            sqlx::query(
                "INSERT INTO idempotency_keys (scope, key, value) VALUES ($1, $2, $3) \
                 ON CONFLICT (scope, key) DO UPDATE SET value = EXCLUDED.value",
            )
            .bind(scope)
            .bind(key)
            .bind(value)
            .execute(&self.pool)
            .await
            .map(|_| ())
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
        })
    }
    fn load_all_idempotency_keys(&self) -> PersistResult<Vec<(String, String, String)>> {
        self.block(async {
            sqlx::query_as("SELECT scope, key, value FROM idempotency_keys")
                .fetch_all(&self.pool)
                .await
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
        })
    }
    fn load_all_workers(&self) -> PersistResult<Vec<WorkerRecord>> {
        type WorkerRow = (
            String,
            String,
            i32,
            i32,
            Option<i64>,
            String,
            String,
            String,
            i64,
        );
        self.block(async {
            let rows: Vec<WorkerRow> = sqlx::query_as(
                "SELECT run_id, worker_id, rank, status, last_heartbeat_millis, host, \
                 sdk_version, protocol_version, registered_at_millis FROM workers",
            )
            .fetch_all(&self.pool)
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
            Ok(rows
                .into_iter()
                .map(
                    |(run_id, worker_id, rank, status, hb, host, sdk, proto, registered)| {
                        let last_heartbeat = from_millis(hb.unwrap_or(0).max(0) as u64);
                        WorkerRecord {
                            run_id,
                            worker_id,
                            rank: rank as u32,
                            state: status,
                            last_heartbeat_at: last_heartbeat,
                            host,
                            sdk_version: sdk,
                            protocol_version: proto,
                            registered_at: from_millis(registered.max(0) as u64),
                            next_heartbeat_deadline: last_heartbeat,
                            failure_deadline: last_heartbeat,
                        }
                    },
                )
                .collect())
        })
    }
    fn load_all_datasets(&self) -> PersistResult<Vec<DatasetRecord>> {
        type DatasetRow = (
            String,
            String,
            String,
            i32,
            String,
            String,
            String,
            String,
            i64,
        );
        type ShardRow = (String, String, Option<i64>, i64, i64, String, String);
        self.block(async {
            let rows: Vec<DatasetRow> = sqlx::query_as(
                "SELECT run_id, dataset_id, name, num_shards, dataset_uri, format, \
                 dataset_version, labels_json, registered_at_millis FROM datasets",
            )
            .fetch_all(&self.pool)
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
            let mut datasets = Vec::with_capacity(rows.len());
            for (run_id, dataset_id, name, num_shards, uri, format, version, labels_json, reg) in
                rows
            {
                let shard_rows: Vec<ShardRow> = sqlx::query_as(
                    "SELECT shard_id, shard_uri, size_bytes, byte_range_start, byte_range_end, \
                     checksum, checksum_algorithm FROM shards \
                     WHERE run_id = $1 AND dataset_id = $2 ORDER BY shard_index",
                )
                .bind(&run_id)
                .bind(&dataset_id)
                .fetch_all(&self.pool)
                .await
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
                let shards = shard_rows
                    .into_iter()
                    .map(|(sid, suri, size, start, end, sum, alg)| ShardRecord {
                        shard_id: sid,
                        dataset_id: dataset_id.clone(),
                        shard_uri: suri,
                        size_bytes: size.unwrap_or(0),
                        byte_range_start: start,
                        byte_range_end: end,
                        checksum: sum,
                        checksum_algorithm: alg,
                    })
                    .collect();
                datasets.push(DatasetRecord {
                    run_id,
                    dataset_id,
                    name,
                    shard_count: num_shards as u32,
                    dataset_uri: uri,
                    format,
                    dataset_version: version,
                    labels: serde_json::from_str(&labels_json).unwrap_or_default(),
                    registered_at: from_millis(reg.max(0) as u64),
                    shards,
                });
            }
            Ok(datasets)
        })
    }
    fn load_all_checkpoints(&self) -> PersistResult<Vec<(String, CoordinatorCheckpointRecord)>> {
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
        type ArtifactRow = (String, String, String, i64, String, String);
        self.block(async {
            let rows: Vec<CkptRow> = sqlx::query_as(
                "SELECT run_id, checkpoint_id, state, initiated_by_worker_id, step, \
                 created_at_millis, updated_at_millis, committed_at_millis, \
                 expected_worker_ids_json FROM checkpoints",
            )
            .fetch_all(&self.pool)
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
            let mut records = Vec::with_capacity(rows.len());
            for (run_id, ckpt_id, state, initiator, step, created, updated, committed, expected) in
                rows
            {
                let artifact_rows: Vec<ArtifactRow> = sqlx::query_as(
                    "SELECT worker_id, artifact_name, artifact_uri, size_bytes, checksum, \
                     checksum_algorithm FROM checkpoint_artifacts \
                     WHERE run_id = $1 AND checkpoint_id = $2 ORDER BY worker_id, artifact_name",
                )
                .bind(&run_id)
                .bind(&ckpt_id)
                .fetch_all(&self.pool)
                .await
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
                let artifacts = artifact_rows
                    .into_iter()
                    .map(|(wid, name, uri, size, sum, alg)| ArtifactRecord {
                        worker_id: DomainWorkerId(wid),
                        artifact_name: name,
                        storage_key: uri,
                        size_bytes: size as u64,
                        checksum: sum,
                        checksum_algorithm: alg,
                    })
                    .collect();
                let expected_worker_ids: Vec<String> =
                    serde_json::from_str(&expected).unwrap_or_default();
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
        })
    }

    fn insert_audit_event(&self, event: &AuditEventEntry) -> PersistResult<()> {
        self.block(async {
            sqlx::query(
                r#"
INSERT INTO audit_events
  (occurred_at_millis, kind, outcome, subject, role, worker_id,
   run_id, dataset_id, checkpoint_id, reason, request_name)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
"#,
            )
            .bind(event.occurred_at_millis as i64)
            .bind(&event.kind)
            .bind(&event.outcome)
            .bind(&event.subject)
            .bind(&event.role)
            .bind(&event.worker_id)
            .bind(&event.run_id)
            .bind(&event.dataset_id)
            .bind(&event.checkpoint_id)
            .bind(&event.reason)
            .bind(&event.request_name)
            .execute(&self.pool)
            .await
            .map(|_| ())
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
        })
    }
}

/// Strip credentials from a PostgreSQL DSN before logging it.
fn redact_pg_url(url: &str) -> String {
    // Simplistic redaction: replace everything between :// and @ with ***
    if let (Some(scheme_end), Some(at_pos)) = (url.find("://"), url.rfind('@')) {
        let scheme = &url[..scheme_end + 3];
        let host_onwards = &url[at_pos..];
        format!("{scheme}***{host_onwards}")
    } else {
        url.to_string()
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
