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
  idempotency_key TEXT
);
CREATE TABLE IF NOT EXISTS workers (
  run_id TEXT NOT NULL,
  worker_id TEXT NOT NULL,
  rank INTEGER NOT NULL,
  world_size INTEGER NOT NULL,
  status INTEGER NOT NULL,
  last_heartbeat_millis INTEGER,
  idempotency_key TEXT,
  PRIMARY KEY (run_id, worker_id)
);
CREATE TABLE IF NOT EXISTS datasets (
  run_id TEXT NOT NULL,
  dataset_id TEXT NOT NULL,
  name TEXT NOT NULL,
  num_shards INTEGER NOT NULL,
  idempotency_key TEXT,
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
  PRIMARY KEY (run_id, checkpoint_id)
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
",
        )
    }

    pub fn save_run(&self, run: &RunRecord) -> rusqlite::Result<()> {
        let conn = self.conn.lock().expect("sqlite");
        conn.execute("INSERT OR REPLACE INTO runs(run_id,name,status,created_at_millis,idempotency_key) VALUES(?1,?2,?3,?4,NULL)",params![run.run_id,run.run_name,run.latest_checkpoint_state, to_millis(run.registered_at)])?;
        Ok(())
    }
    pub fn save_worker(&self, w: &WorkerRecord, world_size: u32) -> rusqlite::Result<()> {
        let conn = self.conn.lock().expect("sqlite");
        conn.execute("INSERT OR REPLACE INTO workers(run_id,worker_id,rank,world_size,status,last_heartbeat_millis,idempotency_key) VALUES(?1,?2,?3,?4,?5,?6,NULL)",params![w.run_id,w.worker_id,w.rank,world_size,w.state,to_millis(w.last_heartbeat_at)])?;
        Ok(())
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
        let conn = self.conn.lock().expect("sqlite");
        conn.execute("INSERT OR REPLACE INTO datasets(run_id,dataset_id,name,num_shards,idempotency_key) VALUES(?1,?2,?3,?4,NULL)",params![d.run_id,d.dataset_id,d.name,d.shard_count])?;
        for (idx, s) in d.shards.iter().enumerate() {
            conn.execute("INSERT OR REPLACE INTO shards(run_id,dataset_id,shard_id,shard_index,shard_uri,num_records,size_bytes) VALUES(?1,?2,?3,?4,?5,?6,?7)",params![d.run_id,d.dataset_id,s.shard_id,idx as i64,s.shard_uri,Option::<i64>::None,s.size_bytes])?;
        }
        Ok(())
    }
    pub fn save_checkpoint(
        &self,
        run_id: &str,
        idempotency_key: &str,
        rec: &CoordinatorCheckpointRecord,
    ) -> rusqlite::Result<()> {
        let conn = self.conn.lock().expect("sqlite");
        conn.execute("INSERT OR REPLACE INTO checkpoints(run_id,checkpoint_id,state,initiated_by_worker_id,step,created_at_millis,updated_at_millis,committed_at_millis,idempotency_key) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9)",params![run_id,rec.checkpoint.checkpoint_id.0, rec.checkpoint.state as i32, rec.initiated_by_worker_id, rec.checkpoint.step as i64, rec.checkpoint.created_at_millis as i64, rec.checkpoint.updated_at_millis as i64, rec.committed_at_millis.map(|v| v as i64), if idempotency_key.is_empty(){None::<String>}else{Some(idempotency_key.to_string())}])?;
        Ok(())
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
        }))
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
}

fn to_millis(t: std::time::SystemTime) -> i64 {
    t.duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
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
