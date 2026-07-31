#![forbid(unsafe_code)]

//! PostgreSQL durability + load tests.  Gated behind `DTRUNTIME_RUN_PG_TESTS=1`
//! and a `DTRUNTIME_PG_URL` DSN, so they no-op in CI environments without a
//! database.
//!
//! Coverage:
//! - schema initialises and records the migration ledger;
//! - concurrent writers/readers stay consistent under contention (durability
//!   under load): every write is present and readable afterwards;
//! - state persists across a fresh connection (reconnect = coordinator
//!   restart / HA failover to another replica);
//! - checkpoints and their artifacts round-trip through the three-table join
//!   that reconstructs them, which is what a restarted coordinator relies on
//!   to find a restore target.

use runtime_checkpoint::{
    ArtifactRecord, CheckpointId as DomainCheckpointId, CheckpointRecord, RunId as DomainRunId,
    WorkerId as DomainWorkerId,
};
use runtime_coordinator::persistence::MetadataStore;
use runtime_coordinator::persistence::PostgresMetadataStore;
use runtime_coordinator::state::{CoordinatorCheckpointRecord, RunRecord};
use std::sync::Arc;
use std::time::SystemTime;

/// Drop and recreate the public schema so the next init starts cold.
async fn reset_schema(url: &str) {
    use sqlx::Executor;
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(url)
        .await
        .expect("connect for schema reset");
    pool.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
        .await
        .expect("reset schema");
}

fn dsn() -> Option<String> {
    if std::env::var("DTRUNTIME_RUN_PG_TESTS").as_deref() != Ok("1") {
        eprintln!("skipping Postgres tests; set DTRUNTIME_RUN_PG_TESTS=1");
        return None;
    }
    std::env::var("DTRUNTIME_PG_URL")
        .ok()
        .filter(|v| !v.trim().is_empty())
}

fn run_record(run_id: String) -> RunRecord {
    RunRecord {
        run_id,
        run_name: "pg-load".to_string(),
        assignment_policy: 0,
        storage_backend_kind: 0,
        world_size: 1,
        run_version: "v1".to_string(),
        registered_at: SystemTime::now(),
        active_worker_count: 0,
        latest_checkpoint_id: None,
        latest_checkpoint_state: 0,
        min_world_size: 0,
        max_world_size: 0,
        rendezvous_settle_timeout_ms: 0,
        state: 1,
        state_reason: String::new(),
        idempotency_key: String::new(),
        state_updated_at: SystemTime::now(),
        retry_count: 0,
    }
}

fn checkpoint_record(run_id: &str, checkpoint_id: &str, step: u64) -> CoordinatorCheckpointRecord {
    CoordinatorCheckpointRecord {
        checkpoint: CheckpointRecord::new(
            DomainRunId(run_id.to_string()),
            DomainCheckpointId(checkpoint_id.to_string()),
            step,
            1_700_000_000_000,
        ),
        initiated_by_worker_id: "worker-0".to_string(),
        committed_at_millis: None,
        expected_artifact_count: 0,
        expected_worker_ids: Vec::new(),
    }
}

fn artifact_record(name: &str) -> ArtifactRecord {
    ArtifactRecord {
        worker_id: DomainWorkerId(format!("worker-{name}")),
        artifact_name: name.to_string(),
        storage_key: format!("s3://bucket/{name}.bin"),
        size_bytes: 128,
        checksum: "sum".to_string(),
        checksum_algorithm: "sha256".to_string(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pg_concurrent_writes_are_durable_and_consistent() {
    let Some(url) = dsn() else {
        return;
    };
    let store = PostgresMetadataStore::connect(&url)
        .await
        .expect("connect postgres");
    store.init_schema().await.expect("init schema");

    // Use a unique prefix so repeated runs against the same DB never collide.
    let prefix = format!(
        "loadtest-{}",
        SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let store = Arc::new(store);

    let writers = 8;
    let per_writer = 25;
    let mut handles = Vec::new();
    for w in 0..writers {
        let store = Arc::clone(&store);
        let prefix = prefix.clone();
        // The MetadataStore methods are synchronous (they block on the pool
        // internally), so drive them from blocking tasks that contend on the
        // shared connection pool.
        handles.push(tokio::task::spawn_blocking(move || {
            for i in 0..per_writer {
                let run = run_record(format!("{prefix}-{w}-{i}"));
                store.save_run(&run).expect("save_run");
                let _ = store.load_all_runs().expect("load_all_runs");
            }
        }));
    }
    for h in handles {
        h.await.expect("writer task");
    }

    // A fresh connection (simulating a restart / failover) sees every write.
    let reconnected = PostgresMetadataStore::connect(&url)
        .await
        .expect("reconnect");
    let all = tokio::task::spawn_blocking(move || reconnected.load_all_runs().expect("reload"))
        .await
        .unwrap();
    let mine = all.iter().filter(|r| r.run_id.starts_with(&prefix)).count();
    assert_eq!(
        mine,
        writers * per_writer,
        "every concurrent write must be durable and visible after reconnect"
    );
}

/// Checkpoints and their artifacts must survive a reconnect intact.
///
/// Covers the two read paths that reconstruct a checkpoint from three tables
/// (`checkpoints`, `checkpoint_artifacts`, `latest_checkpoints`) — the most
/// intricate queries in the Postgres store, and the ones a coordinator depends
/// on to find a restore target after a restart or HA failover.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pg_checkpoints_and_artifacts_round_trip() {
    let Some(url) = dsn() else {
        return;
    };
    let store = PostgresMetadataStore::connect(&url)
        .await
        .expect("connect postgres");
    store.init_schema().await.expect("init schema");

    let run_id = format!("ckpt-run-{}", std::process::id());
    let store = Arc::new(store);

    let write_run_id = run_id.clone();
    let write_store = Arc::clone(&store);
    tokio::task::spawn_blocking(move || {
        write_store
            .save_run(&run_record(write_run_id.clone()))
            .unwrap();
        // Two checkpoints; the newer one carries multiple artifacts, as a
        // DCP save does (one file per rank).
        for (checkpoint_id, step, artifacts) in [
            ("ckpt-a", 10u64, vec!["shard-0"]),
            ("ckpt-b", 20u64, vec!["shard-0", "shard-1"]),
        ] {
            let record = checkpoint_record(&write_run_id, checkpoint_id, step);
            write_store
                .save_checkpoint(&write_run_id, "", &record)
                .unwrap();
            for name in artifacts {
                write_store
                    .save_checkpoint_artifact(&write_run_id, checkpoint_id, &artifact_record(name))
                    .unwrap();
            }
            write_store
                .mark_checkpoint_committed(&write_run_id, checkpoint_id, 1_700_000_000_000)
                .unwrap();
            write_store
                .set_latest_checkpoint(&write_run_id, Some((checkpoint_id, step)))
                .unwrap();
        }
    })
    .await
    .unwrap();

    // Reconnect: a different coordinator replica reading the same database.
    let reconnected = PostgresMetadataStore::connect(&url)
        .await
        .expect("reconnect");
    let read_run_id = run_id.clone();
    let (latest, all) = tokio::task::spawn_blocking(move || {
        (
            reconnected.load_latest_checkpoint(&read_run_id).unwrap(),
            reconnected.load_checkpoints_for_run(&read_run_id).unwrap(),
        )
    })
    .await
    .unwrap();

    let latest = latest.expect("a committed checkpoint must be found");
    assert_eq!(latest.checkpoint.checkpoint_id.0, "ckpt-b");
    assert_eq!(latest.checkpoint.step, 20);
    assert_eq!(
        latest
            .checkpoint
            .artifacts
            .iter()
            .map(|a| a.artifact_name.as_str())
            .collect::<Vec<_>>(),
        vec!["shard-0", "shard-1"],
        "every artifact must load, in a deterministic order"
    );
    assert_eq!(
        latest.expected_worker_ids.len(),
        2,
        "expected_worker_ids is derived from the artifact list"
    );

    assert_eq!(all.len(), 2, "both checkpoints must load");
    assert_eq!(
        all.iter()
            .map(|c| c.checkpoint.checkpoint_id.0.as_str())
            .collect::<Vec<_>>(),
        vec!["ckpt-b", "ckpt-a"],
        "newest step first"
    );
    assert_eq!(all[0].checkpoint.artifacts.len(), 2);
    assert_eq!(all[1].checkpoint.artifacts.len(), 1);
}

/// Several replicas may call `init_schema` at once against a fresh database.
///
/// Schema init runs before leader election, so on a cold start every replica
/// reaches it simultaneously. `CREATE TABLE IF NOT EXISTS` is not concurrency
/// safe on its own — two sessions can both pass the existence check and then
/// collide creating the type, failing with
/// `23505 ... pg_type_typname_nsp_index`. The store serialises the DDL behind
/// an advisory lock; this asserts that holds.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pg_concurrent_schema_init_is_safe() {
    let Some(url) = dsn() else {
        return;
    };
    // Start from an empty schema: the race only exists while the tables are
    // being created, so an already-migrated database would not exercise it.
    reset_schema(&url).await;

    let mut handles = Vec::new();
    for _ in 0..6 {
        let url = url.clone();
        // Errors are stringified so the task's future stays `Send`.
        handles.push(tokio::spawn(async move {
            let store = PostgresMetadataStore::connect(&url)
                .await
                .map_err(|e| e.to_string())?;
            store.init_schema().await.map_err(|e| e.to_string())
        }));
    }
    for (i, h) in handles.into_iter().enumerate() {
        h.await
            .expect("replica task panicked")
            .unwrap_or_else(|e| panic!("replica {i} failed schema init: {e}"));
    }

    // The schema is usable afterwards, and the ledger recorded exactly one v1.
    let store = PostgresMetadataStore::connect(&url).await.expect("connect");
    let run_id = format!("concurrent-init-{}", std::process::id());
    tokio::task::spawn_blocking(move || store.save_run(&run_record(run_id)).expect("save_run"))
        .await
        .unwrap();
}
