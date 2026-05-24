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

use crate::persistence::{MetadataStore, PersistResult};
use crate::state::{
    CoordinatorCheckpointRecord, DatasetRecord, RunRecord, ShardRecord, WorkerRecord,
};
use runtime_checkpoint::{
    ArtifactRecord, CheckpointId as DomainCheckpointId, CheckpointRecord,
    CheckpointState as DomainCheckpointState, RunId as DomainRunId, WorkerId as DomainWorkerId,
};

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
    pub async fn init_schema(&self) -> Result<(), sqlx::Error> {
        sqlx::query(
            r#"
CREATE TABLE IF NOT EXISTS runs (
  run_id               TEXT PRIMARY KEY,
  name                 TEXT NOT NULL,
  status               INTEGER NOT NULL,
  created_at_millis    BIGINT NOT NULL,
  idempotency_key      TEXT
);

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
"#,
        )
        .execute(&self.pool)
        .await?;
        info!("postgres metadata store: schema initialised");
        Ok(())
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
INSERT INTO runs (run_id, name, status, created_at_millis, idempotency_key)
VALUES ($1, $2, $3, $4, NULL)
ON CONFLICT (run_id) DO UPDATE SET
  name = EXCLUDED.name,
  status = EXCLUDED.status,
  created_at_millis = EXCLUDED.created_at_millis
"#,
            )
            .bind(&run.run_id)
            .bind(&run.run_name)
            .bind(run.latest_checkpoint_state)
            .bind(to_millis(run.registered_at))
            .execute(&self.pool)
            .await
            .map(|_| ())
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
        })
    }

    fn save_worker(&self, w: &WorkerRecord, world_size: u32) -> PersistResult<()> {
        self.block(async {
            sqlx::query(
                r#"
INSERT INTO workers (run_id, worker_id, rank, world_size, status, last_heartbeat_millis, idempotency_key)
VALUES ($1, $2, $3, $4, $5, $6, NULL)
ON CONFLICT (run_id, worker_id) DO UPDATE SET
  rank = EXCLUDED.rank,
  world_size = EXCLUDED.world_size,
  status = EXCLUDED.status,
  last_heartbeat_millis = EXCLUDED.last_heartbeat_millis
"#,
            )
            .bind(&w.run_id)
            .bind(&w.worker_id)
            .bind(w.rank as i32)
            .bind(world_size as i32)
            .bind(w.state)
            .bind(to_millis(w.last_heartbeat_at))
            .execute(&self.pool)
            .await
            .map(|_| ())
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
        })
    }

    fn load_workers_for_run(
        &self,
        run_id: &str,
    ) -> PersistResult<Vec<(String, u32, u32, i32)>> {
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
INSERT INTO datasets (run_id, dataset_id, name, num_shards, idempotency_key)
VALUES ($1, $2, $3, $4, NULL)
ON CONFLICT (run_id, dataset_id) DO UPDATE SET
  name = EXCLUDED.name,
  num_shards = EXCLUDED.num_shards
"#,
            )
            .bind(&d.run_id)
            .bind(&d.dataset_id)
            .bind(&d.name)
            .bind(d.shard_count as i32)
            .execute(&self.pool)
            .await
            .map(|_| ())
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

            for (idx, s) in d.shards.iter().enumerate() {
                sqlx::query(
                    r#"
INSERT INTO shards (run_id, dataset_id, shard_id, shard_index, shard_uri, num_records, size_bytes)
VALUES ($1, $2, $3, $4, $5, NULL, $6)
ON CONFLICT (run_id, dataset_id, shard_id) DO UPDATE SET
  shard_index = EXCLUDED.shard_index,
  shard_uri   = EXCLUDED.shard_uri,
  size_bytes  = EXCLUDED.size_bytes
"#,
                )
                .bind(&d.run_id)
                .bind(&d.dataset_id)
                .bind(&s.shard_id)
                .bind(idx as i32)
                .bind(&s.shard_uri)
                .bind(s.size_bytes)
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
   created_at_millis, updated_at_millis, committed_at_millis, idempotency_key)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
ON CONFLICT (run_id, checkpoint_id) DO UPDATE SET
  state                  = EXCLUDED.state,
  initiated_by_worker_id = EXCLUDED.initiated_by_worker_id,
  step                   = EXCLUDED.step,
  updated_at_millis      = EXCLUDED.updated_at_millis,
  committed_at_millis    = EXCLUDED.committed_at_millis,
  idempotency_key        = EXCLUDED.idempotency_key
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

    fn load_dataset(
        &self,
        run_id: &str,
        dataset_id: &str,
    ) -> PersistResult<Option<(String, Vec<ShardRecord>)>> {
        self.block(async {
            let name: Option<(String,)> = sqlx::query_as(
                "SELECT name FROM datasets WHERE run_id = $1 AND dataset_id = $2",
            )
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
