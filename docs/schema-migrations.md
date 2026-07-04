# Database schema migrations

The coordinator's metadata schema evolves through a **versioned, forward-only
migration runner** shared in spirit by both metadata backends. Every schema
change is a numbered migration, applied in order and recorded in a
`schema_migrations` ledger, so upgrades are deterministic and a database can
report exactly which version it is at.

## Model

```
schema_migrations(version INTEGER PRIMARY KEY, name TEXT, applied_at_millis INTEGER)
```

On startup `init_schema`:

1. creates the `schema_migrations` ledger if absent;
2. applies every migration whose `version` is not yet recorded, in ascending
   order, **each inside its own transaction**;
3. records the version + name + timestamp after each successful apply.

A failed migration rolls back and leaves the ledger untouched, so the next
startup retries it cleanly. `init_schema` is idempotent and runs on every
boot.

- **SQLite** (`crates/runtime-coordinator/src/persistence.rs`): migrations are
  `SqliteMigration { version, name, apply: fn(&Connection) }` entries in
  `SQLITE_MIGRATIONS`, applied via `run_sqlite_migrations` using
  `unchecked_transaction`. `latest_sqlite_migration_version()` reports the
  head version.
- **PostgreSQL** (`crates/runtime-coordinator/src/pg_store.rs`): the baseline
  DDL uses `CREATE TABLE IF NOT EXISTS` / `ADD COLUMN IF NOT EXISTS`, then the
  ledger row for v1 is inserted with `ON CONFLICT DO NOTHING`.

Migration **v1 (`baseline_schema`)** is the full current schema. It folds in
the idempotent column retrofit (PRAGMA-checked `ALTER TABLE ... ADD COLUMN`),
so a database created before the ledger existed — including pre-Phase-3
databases missing lifecycle/data-pipeline columns — converges to v1 on the
next startup without losing rows.

## Rules for adding a migration

1. **Append, never edit.** A migration that has shipped is immutable — never
   change its SQL or renumber it. Add a new entry with the next version.
2. **Contiguous, increasing versions** starting at 1.
3. **Transactional and idempotent-safe.** Prefer `IF NOT EXISTS` / additive
   DDL so a partially-applied change (e.g. crash mid-migration on a backend
   without transactional DDL) can be re-run.
4. **Backfills** that touch data go in the same migration as the DDL they
   depend on.

Example — adding a `priority` column would be:

```rust
// SQLITE_MIGRATIONS
SqliteMigration {
    version: 2,
    name: "runs_add_priority",
    apply: |conn| {
        conn.execute_batch("ALTER TABLE runs ADD COLUMN priority INTEGER NOT NULL DEFAULT 0;")?;
        Ok(())
    },
},
```

and the matching PostgreSQL `ALTER TABLE runs ADD COLUMN IF NOT EXISTS ...` +
`INSERT INTO schema_migrations (2, 'runs_add_priority', ...) ON CONFLICT DO
NOTHING`.

## Durability

SQLite is opened in **WAL mode** with `synchronous=NORMAL`, `busy_timeout=5s`,
and `foreign_keys=ON`:

- **WAL** — readers and the writer never block each other, and the log is
  crash-safe (a torn commit rolls back on reopen).
- **synchronous=NORMAL** — fsync at checkpoint boundaries: durable across a
  process crash (only full OS/power loss can drop the last WAL frames, the
  standard WAL trade-off) with far less fsync overhead than FULL.
- **busy_timeout** — a contended lock waits and retries instead of failing.

PostgreSQL provides durability inherently; the coordinator adds HA via
advisory-lock leader election ([high-availability.md](high-availability.md)).

## Validation

- `migrations_are_recorded_and_idempotent`, `wal_mode_is_enabled_for_durability`,
  and `migration_runner_upgrades_a_pre_ledger_database` (persistence unit
  tests) cover the ledger, WAL, and legacy-upgrade paths.
- `concurrent_writers_and_readers_stay_consistent` (SQLite) and the gated
  `crates/runtime-coordinator/tests/pg_load.rs` (PostgreSQL,
  `DTRUNTIME_RUN_PG_TESTS=1`) are durability/load tests: many threads write +
  read concurrently and every write is durable and visible after reopening /
  reconnecting.
