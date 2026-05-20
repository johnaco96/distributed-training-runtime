use crate::RuntimeMetricsSnapshot;

pub fn render_prometheus_metrics(
    snapshot: &RuntimeMetricsSnapshot,
    audit_events_total: u64,
) -> String {
    let mut out = String::new();
    macro_rules! m {
        ($n:expr,$t:expr,$h:expr,$v:expr) => {{
            out.push_str(&format!(
                "# HELP {} {}\n# TYPE {} {}\n{} {}\n",
                $n, $h, $n, $t, $n, $v
            ));
        }};
    }
    m!(
        "dtruntime_runs_total",
        "counter",
        "Total registered runs.",
        snapshot.runs_total
    );
    m!(
        "dtruntime_registered_datasets_total",
        "counter",
        "Total registered datasets.",
        snapshot.datasets_total
    );
    m!(
        "dtruntime_workers_total",
        "counter",
        "Total worker registrations.",
        snapshot.workers_total
    );
    m!(
        "dtruntime_active_workers",
        "gauge",
        "Current number of active workers.",
        snapshot.active_workers
    );
    m!(
        "dtruntime_stale_workers",
        "gauge",
        "Current number of stale workers.",
        snapshot.stale_workers
    );
    m!(
        "dtruntime_failed_workers",
        "gauge",
        "Current number of failed workers.",
        snapshot.failed_workers
    );
    m!(
        "dtruntime_heartbeats_total",
        "counter",
        "Total worker heartbeats.",
        snapshot.heartbeats_total
    );
    m!(
        "dtruntime_shard_assignments_total",
        "counter",
        "Total shard assignments.",
        snapshot.shard_assignment_count
    );
    m!(
        "dtruntime_checkpoints_started_total",
        "counter",
        "Total checkpoints started.",
        snapshot.checkpoints_started_total
    );
    m!(
        "dtruntime_checkpoints_committed_total",
        "counter",
        "Total checkpoints committed.",
        snapshot.checkpoint_writes
    );
    m!(
        "dtruntime_checkpoint_artifacts_reported_total",
        "counter",
        "Total checkpoint artifacts reported.",
        snapshot.checkpoint_artifacts_reported_total
    );
    m!(
        "dtruntime_checkpoint_failures_total",
        "counter",
        "Total checkpoint failures.",
        snapshot.failed_writes
    );
    m!(
        "dtruntime_latest_checkpoint_reads_total",
        "counter",
        "Total latest-checkpoint reads.",
        snapshot.latest_checkpoint_reads_total
    );
    m!(
        "dtruntime_storage_read_bytes_total",
        "counter",
        "Total storage bytes read.",
        snapshot.storage_bytes_read
    );
    m!(
        "dtruntime_storage_write_bytes_total",
        "counter",
        "Total storage bytes written.",
        snapshot.storage_bytes_written
    );
    m!(
        "dtruntime_checkpoint_write_bytes_total",
        "counter",
        "Total checkpoint write bytes.",
        snapshot.storage_bytes_written
    );
    m!(
        "dtruntime_audit_events_total",
        "counter",
        "Total audit events emitted.",
        audit_events_total
    );
    m!(
        "dtruntime_auth_allowed_total",
        "counter",
        "Total auth allow decisions.",
        snapshot.auth_allowed_total
    );
    m!(
        "dtruntime_auth_denied_total",
        "counter",
        "Total auth deny decisions.",
        snapshot.auth_denied_total
    );
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RuntimeMetricsSnapshot;

    #[test]
    fn renders_help_type_and_values() {
        let s = RuntimeMetricsSnapshot {
            active_workers: 2,
            checkpoint_writes: 3,
            shard_assignment_count: 4,
            ..RuntimeMetricsSnapshot::default()
        };
        let out = render_prometheus_metrics(&s, 9);
        assert!(out.contains("# HELP dtruntime_active_workers"));
        assert!(out.contains("# TYPE dtruntime_active_workers gauge"));
        assert!(out.contains("dtruntime_active_workers 2"));
        assert!(out.contains("dtruntime_checkpoints_committed_total 3"));
        assert!(out.contains("dtruntime_shard_assignments_total 4"));
        assert!(out.contains("dtruntime_audit_events_total 9"));
    }

    #[test]
    fn does_not_expose_secret_markers() {
        let out = render_prometheus_metrics(&RuntimeMetricsSnapshot::default(), 0);
        for bad in ["PRIVATE KEY", "AKIA", "token=", "secret="] {
            assert!(!out.contains(bad));
        }
    }
}
