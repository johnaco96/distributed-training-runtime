//! Pure rendering of a TrainingRun into Kubernetes objects.
//!
//! Everything here is deterministic spec → objects with no API calls, so the
//! translation (launcher command, GPU resources, placement constraints,
//! indexed-job wiring) is unit-tested without a cluster.

use std::collections::BTreeMap;

use k8s_openapi::api::apps::v1::{Deployment, DeploymentSpec};
use k8s_openapi::api::batch::v1::{Job, JobSpec};
use k8s_openapi::api::core::v1::{
    Container, ContainerPort, EnvVar, EnvVarSource, ObjectFieldSelector, PodSpec, PodTemplateSpec,
    Service, ServicePort, ServiceSpec,
};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{LabelSelector, ObjectMeta};
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use kube::{Resource, ResourceExt};

use crate::crd::TrainingRun;

pub const COORDINATOR_PORT: i32 = 50051;
pub const MANAGED_BY: &str = "runtime-operator";

/// Objects the operator applies for one TrainingRun.  Scheduler backends may
/// adapt these (labels, schedulerName, extra objects) before apply.
#[derive(Clone, Debug, Default)]
pub struct RenderedWorkload {
    pub coordinator_deployment: Option<Deployment>,
    pub coordinator_service: Option<Service>,
    pub worker_job: Option<Job>,
    /// Additional scheduler-specific objects (e.g. a Volcano PodGroup),
    /// applied as untyped JSON with server-side apply.
    pub extra_objects: Vec<serde_json::Value>,
}

pub fn labels_for(tr: &TrainingRun, component: &str) -> BTreeMap<String, String> {
    BTreeMap::from([
        (
            "app.kubernetes.io/name".to_string(),
            "dtruntime".to_string(),
        ),
        (
            "app.kubernetes.io/managed-by".to_string(),
            MANAGED_BY.to_string(),
        ),
        (
            "app.kubernetes.io/component".to_string(),
            component.to_string(),
        ),
        ("dtruntime.io/training-run".to_string(), tr.name_any()),
    ])
}

pub fn coordinator_service_name(tr: &TrainingRun) -> String {
    format!("{}-coordinator", tr.name_any())
}

pub fn worker_job_name(tr: &TrainingRun) -> String {
    format!("{}-workers", tr.name_any())
}

/// Coordinator address workers connect to: the explicit external address, or
/// the managed coordinator's in-namespace Service DNS name.
pub fn coordinator_address(tr: &TrainingRun) -> String {
    if let Some(address) = &tr.spec.coordinator.address {
        return address.clone();
    }
    format!("{}:{}", coordinator_service_name(tr), COORDINATOR_PORT)
}

fn owner_meta(tr: &TrainingRun, name: String, component: &str) -> ObjectMeta {
    ObjectMeta {
        name: Some(name),
        namespace: tr.namespace(),
        labels: Some(labels_for(tr, component)),
        owner_references: tr.controller_owner_ref(&()).map(|r| vec![r]),
        ..Default::default()
    }
}

/// Build the `python -m dtruntime.launch ...` command for a worker pod.
pub fn launcher_command(tr: &TrainingRun) -> Vec<String> {
    let spec = &tr.spec;
    let mut command = vec![
        "python".to_string(),
        "-m".to_string(),
        "dtruntime.launch".to_string(),
        "--coordinator".to_string(),
        coordinator_address(tr),
        "--run-name".to_string(),
        tr.name_any(),
        "--nproc-per-node".to_string(),
        spec.nproc_per_node.to_string(),
        // Indexed Jobs export JOB_COMPLETION_INDEX; $(VAR) is expanded by the
        // kubelet, giving every pod a stable node identity across restarts.
        "--node-id".to_string(),
        format!("{}-node-$(JOB_COMPLETION_INDEX)", tr.name_any()),
    ];
    match &spec.elastic {
        Some(elastic) => {
            command.extend([
                "--min-workers".to_string(),
                elastic.min_workers.to_string(),
                "--max-workers".to_string(),
                elastic.max_workers.to_string(),
            ]);
            if elastic.settle_timeout_ms > 0 {
                command.extend([
                    "--settle-timeout-ms".to_string(),
                    elastic.settle_timeout_ms.to_string(),
                ]);
            }
        }
        None => {
            command.extend([
                "--world-size".to_string(),
                (spec.nodes * spec.nproc_per_node).to_string(),
            ]);
        }
    }
    if let Some(backend) = &spec.backend {
        command.extend(["--backend".to_string(), backend.clone()]);
    }
    if let Some(max_restarts) = spec.max_restarts {
        command.extend(["--max-restarts".to_string(), max_restarts.to_string()]);
    }
    // Pod IP is the address other nodes' workers reach this node's TCPStore on.
    command.extend([
        "--worker-host".to_string(),
        "$(DTRUNTIME_POD_IP)".to_string(),
    ]);
    command.extend(spec.script.iter().cloned());
    command
}

fn worker_env(tr: &TrainingRun) -> Vec<EnvVar> {
    let mut env = vec![EnvVar {
        name: "DTRUNTIME_POD_IP".to_string(),
        value_from: Some(EnvVarSource {
            field_ref: Some(ObjectFieldSelector {
                field_path: "status.podIP".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    }];
    if let Some(extra) = &tr.spec.env {
        env.extend(extra.iter().cloned());
    }
    env
}

pub fn render_worker_job(tr: &TrainingRun) -> Job {
    let spec = &tr.spec;
    let scheduling = spec.scheduling.clone().unwrap_or_default();

    let container = Container {
        name: "worker".to_string(),
        image: Some(spec.image.clone()),
        command: Some(launcher_command(tr)),
        env: Some(worker_env(tr)),
        resources: spec.resources.clone(),
        ..Default::default()
    };

    let pod_spec = PodSpec {
        containers: vec![container],
        // The launcher owns in-pod restarts; pod-level restarts are handled
        // by the Job controller so failures count against backoffLimit.
        restart_policy: Some("Never".to_string()),
        node_selector: scheduling.node_selector.clone(),
        tolerations: scheduling.tolerations.clone(),
        affinity: scheduling.affinity.clone(),
        topology_spread_constraints: scheduling.topology_spread_constraints.clone(),
        priority_class_name: scheduling.priority_class_name.clone(),
        scheduler_name: scheduling.scheduler_name.clone(),
        runtime_class_name: scheduling.runtime_class_name.clone(),
        service_account_name: spec.service_account_name.clone(),
        ..Default::default()
    };

    Job {
        metadata: owner_meta(tr, worker_job_name(tr), "worker"),
        spec: Some(JobSpec {
            completions: Some(spec.nodes as i32),
            parallelism: Some(spec.nodes as i32),
            completion_mode: Some("Indexed".to_string()),
            backoff_limit: Some(spec.backoff_limit.unwrap_or(6)),
            template: PodTemplateSpec {
                metadata: Some(ObjectMeta {
                    labels: Some(labels_for(tr, "worker")),
                    ..Default::default()
                }),
                spec: Some(pod_spec),
            },
            ..Default::default()
        }),
        status: None,
    }
}

pub fn render_coordinator(tr: &TrainingRun) -> Option<(Deployment, Service)> {
    let managed = tr.spec.coordinator.manage.as_ref()?;
    let name = coordinator_service_name(tr);
    let labels = labels_for(tr, "coordinator");

    let mut command = vec![
        "runtime-coordinator".to_string(),
        "--addr".to_string(),
        format!("0.0.0.0:{COORDINATOR_PORT}"),
    ];
    command.extend(managed.args.iter().cloned());

    let deployment = Deployment {
        metadata: owner_meta(tr, name.clone(), "coordinator"),
        spec: Some(DeploymentSpec {
            replicas: Some(1),
            selector: LabelSelector {
                match_labels: Some(labels.clone()),
                ..Default::default()
            },
            template: PodTemplateSpec {
                metadata: Some(ObjectMeta {
                    labels: Some(labels.clone()),
                    ..Default::default()
                }),
                spec: Some(PodSpec {
                    containers: vec![Container {
                        name: "coordinator".to_string(),
                        image: Some(managed.image.clone()),
                        command: Some(command),
                        resources: managed.resources.clone(),
                        ports: Some(vec![ContainerPort {
                            container_port: COORDINATOR_PORT,
                            name: Some("grpc".to_string()),
                            ..Default::default()
                        }]),
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
            },
            ..Default::default()
        }),
        status: None,
    };

    let service = Service {
        metadata: owner_meta(tr, name, "coordinator"),
        spec: Some(ServiceSpec {
            selector: Some(labels),
            ports: Some(vec![ServicePort {
                port: COORDINATOR_PORT,
                target_port: Some(IntOrString::Int(COORDINATOR_PORT)),
                name: Some("grpc".to_string()),
                ..Default::default()
            }]),
            ..Default::default()
        }),
        status: None,
    };

    Some((deployment, service))
}

/// Render the base workload (before scheduler-backend adaptation).
pub fn render(tr: &TrainingRun) -> RenderedWorkload {
    let coordinator = render_coordinator(tr);
    let (coordinator_deployment, coordinator_service) = match coordinator {
        Some((d, s)) => (Some(d), Some(s)),
        None => (None, None),
    };
    RenderedWorkload {
        coordinator_deployment,
        coordinator_service,
        worker_job: Some(render_worker_job(tr)),
        extra_objects: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::{
        CoordinatorSpec, ElasticSpec, ManagedCoordinatorSpec, SchedulingSpec, TrainingRunSpec,
    };
    use k8s_openapi::api::core::v1::ResourceRequirements;
    use k8s_openapi::apimachinery::pkg::api::resource::Quantity;

    fn training_run(mutator: impl FnOnce(&mut TrainingRunSpec)) -> TrainingRun {
        let mut spec = TrainingRunSpec {
            image: "ghcr.io/example/trainer:v1".into(),
            script: vec!["train.py".into(), "--epochs".into(), "3".into()],
            nodes: 4,
            nproc_per_node: 2,
            coordinator: CoordinatorSpec {
                address: Some("coordinator.dtruntime:50051".into()),
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
        };
        mutator(&mut spec);
        let mut tr = TrainingRun::new("mnist", spec);
        tr.metadata.namespace = Some("training".into());
        tr.metadata.uid = Some("uid-123".into());
        tr
    }

    #[test]
    fn launcher_command_fixed_world_size() {
        let tr = training_run(|_| {});
        let command = launcher_command(&tr);
        let joined = command.join(" ");
        assert!(joined.starts_with("python -m dtruntime.launch"));
        assert!(joined.contains("--coordinator coordinator.dtruntime:50051"));
        assert!(joined.contains("--run-name mnist"));
        assert!(joined.contains("--nproc-per-node 2"));
        assert!(
            joined.contains("--world-size 8"),
            "4 nodes x 2 procs: {joined}"
        );
        assert!(joined.contains("--node-id mnist-node-$(JOB_COMPLETION_INDEX)"));
        assert!(joined.contains("--worker-host $(DTRUNTIME_POD_IP)"));
        assert!(joined.ends_with("train.py --epochs 3"));
    }

    #[test]
    fn launcher_command_elastic_bounds() {
        let tr = training_run(|spec| {
            spec.elastic = Some(ElasticSpec {
                min_workers: 2,
                max_workers: 8,
                settle_timeout_ms: 5000,
            });
        });
        let joined = launcher_command(&tr).join(" ");
        assert!(joined.contains("--min-workers 2"));
        assert!(joined.contains("--max-workers 8"));
        assert!(joined.contains("--settle-timeout-ms 5000"));
        assert!(!joined.contains("--world-size"));
    }

    #[test]
    fn worker_job_is_indexed_with_one_pod_per_node() {
        let tr = training_run(|_| {});
        let job = render_worker_job(&tr);
        let spec = job.spec.expect("job spec");
        assert_eq!(spec.completions, Some(4));
        assert_eq!(spec.parallelism, Some(4));
        assert_eq!(spec.completion_mode.as_deref(), Some("Indexed"));
        let pod = spec.template.spec.expect("pod spec");
        assert_eq!(pod.restart_policy.as_deref(), Some("Never"));
        assert_eq!(job.metadata.name.as_deref(), Some("mnist-workers"));
        assert_eq!(
            job.metadata.owner_references.as_ref().map(|o| o.len()),
            Some(1),
            "worker job must be owned by the TrainingRun for cascade deletion"
        );
    }

    #[test]
    fn gpu_resources_and_placement_flow_into_pod_spec() {
        let tr = training_run(|spec| {
            spec.resources = Some(ResourceRequirements {
                limits: Some(BTreeMap::from([(
                    "nvidia.com/gpu".to_string(),
                    Quantity("8".to_string()),
                )])),
                ..Default::default()
            });
            spec.scheduling = Some(SchedulingSpec {
                node_selector: Some(BTreeMap::from([(
                    "cloud.google.com/gke-accelerator".to_string(),
                    "nvidia-h100-80gb".to_string(),
                )])),
                priority_class_name: Some("training-high".to_string()),
                ..Default::default()
            });
        });
        let job = render_worker_job(&tr);
        let pod = job.spec.unwrap().template.spec.unwrap();
        let container = &pod.containers[0];
        assert_eq!(
            container
                .resources
                .as_ref()
                .and_then(|r| r.limits.as_ref())
                .and_then(|l| l.get("nvidia.com/gpu"))
                .map(|q| q.0.as_str()),
            Some("8")
        );
        assert_eq!(
            pod.node_selector
                .as_ref()
                .and_then(|s| s.get("cloud.google.com/gke-accelerator")),
            Some(&"nvidia-h100-80gb".to_string())
        );
        assert_eq!(pod.priority_class_name.as_deref(), Some("training-high"));
    }

    #[test]
    fn managed_coordinator_renders_deployment_service_and_workers_use_its_dns() {
        let tr = training_run(|spec| {
            spec.coordinator = CoordinatorSpec {
                address: None,
                manage: Some(ManagedCoordinatorSpec {
                    image: "ghcr.io/example/coordinator:v1".into(),
                    args: vec!["--metadata-db".into(), "/data/dtr.db".into()],
                    resources: None,
                }),
            };
        });
        let workload = render(&tr);
        let deployment = workload.coordinator_deployment.expect("deployment");
        let service = workload.coordinator_service.expect("service");
        assert_eq!(
            deployment.metadata.name.as_deref(),
            Some("mnist-coordinator")
        );
        assert_eq!(service.metadata.name.as_deref(), Some("mnist-coordinator"));
        let command = deployment.spec.unwrap().template.spec.unwrap().containers[0]
            .command
            .clone()
            .unwrap();
        assert!(command.join(" ").contains("--metadata-db /data/dtr.db"));

        let job_command = launcher_command(&tr).join(" ");
        assert!(
            job_command.contains("--coordinator mnist-coordinator:50051"),
            "workers must target the managed coordinator service: {job_command}"
        );
    }

    #[test]
    fn external_coordinator_renders_no_coordinator_objects() {
        let workload = render(&training_run(|_| {}));
        assert!(workload.coordinator_deployment.is_none());
        assert!(workload.coordinator_service.is_none());
        assert!(workload.worker_job.is_some());
    }

    #[test]
    fn pod_ip_env_uses_downward_api() {
        let tr = training_run(|_| {});
        let job = render_worker_job(&tr);
        let env = job.spec.unwrap().template.spec.unwrap().containers[0]
            .env
            .clone()
            .unwrap();
        let pod_ip = env.iter().find(|e| e.name == "DTRUNTIME_POD_IP").unwrap();
        assert_eq!(
            pod_ip
                .value_from
                .as_ref()
                .and_then(|v| v.field_ref.as_ref())
                .map(|f| f.field_path.as_str()),
            Some("status.podIP")
        );
    }
}
