//! TrainingRun custom resource definition.
//!
//! A `TrainingRun` declares a coordinator-backed distributed training job:
//! which image and script to run, how many nodes and processes, fixed or
//! elastic world size, and how the workload should be scheduled (GPUs,
//! placement constraints, scheduler backend).  The operator renders it into
//! Kubernetes objects — optionally a coordinator Deployment + Service, and an
//! Indexed Job whose pods each run `python -m dtruntime.launch`.

use k8s_openapi::api::core::v1::{
    Affinity, EnvVar, ResourceRequirements, Toleration, TopologySpreadConstraint,
};
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(CustomResource, Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[kube(
    group = "dtruntime.io",
    version = "v1alpha1",
    kind = "TrainingRun",
    namespaced,
    status = "TrainingRunStatus",
    shortname = "trun",
    printcolumn = r#"{"name":"Phase","type":"string","jsonPath":".status.phase"}"#,
    printcolumn = r#"{"name":"Nodes","type":"integer","jsonPath":".spec.nodes"}"#,
    printcolumn = r#"{"name":"Age","type":"date","jsonPath":".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct TrainingRunSpec {
    /// Container image containing dtruntime and the training script.
    pub image: String,

    /// Training script and its arguments, passed to `python -m dtruntime.launch`
    /// (e.g. `["train.py", "--epochs", "10"]`).
    pub script: Vec<String>,

    /// Number of worker pods (one launcher per pod).
    pub nodes: u32,

    /// Training processes per pod.
    #[serde(default = "default_nproc_per_node")]
    pub nproc_per_node: u32,

    /// Coordinator to register with.  Exactly one of `address` (external
    /// coordinator) or `manage` (operator-managed coordinator) must be set.
    pub coordinator: CoordinatorSpec,

    /// Elastic world-size bounds.  Absent = fixed world size
    /// (nodes * nprocPerNode).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub elastic: Option<ElasticSpec>,

    /// torch.distributed backend (default: nccl when GPUs are requested,
    /// gloo otherwise).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backend: Option<String>,

    /// Worker restarts each launcher tolerates before failing its node.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_restarts: Option<u32>,

    /// Pod-level failure budget: how many times worker pods may fail before
    /// the whole TrainingRun is marked Failed (Job backoffLimit).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backoff_limit: Option<i32>,

    /// Compute resources per worker container.  Request accelerators here
    /// (e.g. `limits: {"nvidia.com/gpu": "8"}`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resources: Option<ResourceRequirements>,

    /// Placement, topology, and scheduler-integration knobs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scheduling: Option<SchedulingSpec>,

    /// Extra environment variables for worker containers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub env: Option<Vec<EnvVar>>,

    /// ServiceAccount for worker pods.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_account_name: Option<String>,
}

fn default_nproc_per_node() -> u32 {
    1
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CoordinatorSpec {
    /// Address of an existing coordinator (host:port).  Mutually exclusive
    /// with `manage`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub address: Option<String>,

    /// Let the operator deploy a coordinator for this run.  Mutually
    /// exclusive with `address`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub manage: Option<ManagedCoordinatorSpec>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ManagedCoordinatorSpec {
    /// Coordinator image (runtime-coordinator binary).
    pub image: String,

    /// Extra arguments appended to the coordinator command (TLS flags,
    /// metadata DB URL, storage configuration, ...).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub args: Vec<String>,

    /// Compute resources for the coordinator container.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resources: Option<ResourceRequirements>,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ElasticSpec {
    /// Minimum workers for a rendezvous round to complete.
    pub min_workers: u32,
    /// Maximum workers admitted to a round.
    pub max_workers: u32,
    /// How long the coordinator waits for extra workers after minWorkers is
    /// reached (milliseconds; 0 = coordinator default).
    #[serde(default)]
    pub settle_timeout_ms: u32,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SchedulingSpec {
    /// Scheduler backend: "kubernetes" (default), "volcano", or "kueue".
    /// Backends for Slurm, Ray, SageMaker, and AWS Batch are external
    /// submitters — see docs/schedulers.md.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backend: Option<String>,

    /// Queue name for queue-based backends (Volcano queue / Kueue LocalQueue).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue: Option<String>,

    /// Plain node selector (e.g. accelerator or zone labels).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_selector: Option<std::collections::BTreeMap<String, String>>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tolerations: Option<Vec<Toleration>>,

    /// Full affinity spec for topology-aware placement (e.g. pack workers
    /// into one zone / rack with podAffinity).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub affinity: Option<Affinity>,

    /// Spread constraints for fault-domain distribution.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topology_spread_constraints: Option<Vec<TopologySpreadConstraint>>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub priority_class_name: Option<String>,

    /// Override the pod schedulerName directly (set implicitly by the
    /// volcano backend).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scheduler_name: Option<String>,

    /// Runtime class (e.g. for GPU device-plugin runtimes).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime_class_name: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TrainingRunStatus {
    /// Pending | Running | Succeeded | Failed
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<String>,

    /// Human-readable explanation of the current phase.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,

    /// Worker pods currently running.
    #[serde(default)]
    pub active_nodes: i32,

    /// Worker pods that completed successfully.
    #[serde(default)]
    pub succeeded_nodes: i32,

    /// Worker pods that failed permanently.
    #[serde(default)]
    pub failed_nodes: i32,

    /// Generation last acted on by the operator.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,
}

pub mod phase {
    pub const PENDING: &str = "Pending";
    pub const RUNNING: &str = "Running";
    pub const SUCCEEDED: &str = "Succeeded";
    pub const FAILED: &str = "Failed";
}

/// Validation performed before rendering; returned messages become status
/// conditions and events rather than panics.
pub fn validate(spec: &TrainingRunSpec) -> Result<(), String> {
    if spec.image.is_empty() {
        return Err("spec.image must not be empty".into());
    }
    if spec.script.is_empty() {
        return Err("spec.script must contain at least the script path".into());
    }
    if spec.nodes == 0 {
        return Err("spec.nodes must be >= 1".into());
    }
    if spec.nproc_per_node == 0 {
        return Err("spec.nprocPerNode must be >= 1".into());
    }
    match (&spec.coordinator.address, &spec.coordinator.manage) {
        (Some(_), Some(_)) => {
            return Err("spec.coordinator: address and manage are mutually exclusive".into())
        }
        (None, None) => return Err("spec.coordinator: one of address or manage is required".into()),
        _ => {}
    }
    if let Some(elastic) = &spec.elastic {
        if elastic.min_workers == 0 {
            return Err("spec.elastic.minWorkers must be >= 1".into());
        }
        if elastic.max_workers < elastic.min_workers {
            return Err("spec.elastic.maxWorkers must be >= minWorkers".into());
        }
    }
    if let Some(scheduling) = &spec.scheduling {
        if let Some(backend) = &scheduling.backend {
            if !crate::scheduler::KNOWN_BACKENDS.contains(&backend.as_str()) {
                return Err(format!(
                    "spec.scheduling.backend {backend:?} is not supported in-cluster; \
                     supported: {}; Slurm/Ray/SageMaker/AWS Batch integrate as external \
                     submitters (docs/schedulers.md)",
                    crate::scheduler::KNOWN_BACKENDS.join(", ")
                ));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn minimal_spec() -> TrainingRunSpec {
        TrainingRunSpec {
            image: "ghcr.io/example/trainer:latest".into(),
            script: vec!["train.py".into()],
            nodes: 2,
            nproc_per_node: 1,
            coordinator: CoordinatorSpec {
                address: Some("coordinator:50051".into()),
                manage: None,
            },
            elastic: None,
            backend: None,
            max_restarts: None,
            backoff_limit: None,
            resources: None,
            scheduling: None,
            env: None,
            service_account_name: None,
        }
    }

    #[test]
    fn minimal_spec_is_valid() {
        assert_eq!(validate(&minimal_spec()), Ok(()));
    }

    #[test]
    fn coordinator_must_be_exactly_one_of_address_or_manage() {
        let mut spec = minimal_spec();
        spec.coordinator.manage = Some(ManagedCoordinatorSpec {
            image: "coord:latest".into(),
            ..Default::default()
        });
        assert!(validate(&spec).unwrap_err().contains("mutually exclusive"));

        spec.coordinator.address = None;
        spec.coordinator.manage = None;
        assert!(validate(&spec).unwrap_err().contains("required"));
    }

    #[test]
    fn elastic_bounds_are_validated() {
        let mut spec = minimal_spec();
        spec.elastic = Some(ElasticSpec {
            min_workers: 4,
            max_workers: 2,
            settle_timeout_ms: 0,
        });
        assert!(validate(&spec).unwrap_err().contains("maxWorkers"));
    }

    #[test]
    fn unknown_scheduler_backend_is_rejected_with_guidance() {
        let mut spec = minimal_spec();
        spec.scheduling = Some(SchedulingSpec {
            backend: Some("slurm".into()),
            ..Default::default()
        });
        let err = validate(&spec).unwrap_err();
        assert!(err.contains("external"), "err should point at docs: {err}");
    }

    #[test]
    fn zero_nodes_rejected() {
        let mut spec = minimal_spec();
        spec.nodes = 0;
        assert!(validate(&spec).is_err());
    }
}
