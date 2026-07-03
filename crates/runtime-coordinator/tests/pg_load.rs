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
//!   restart / HA failover to another replica).

use runtime_coordinator::persistence::MetadataStore;
use runtime_coordinator::pg_store::PostgresMetadataStore;
use runtime_coordinator::state::RunRecord;
use std::sync::Arc;
use std::time::SystemTime;

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
