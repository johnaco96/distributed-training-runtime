//! Scheduler backends: how a rendered TrainingRun workload is adapted for the
//! cluster's scheduling system.
//!
//! In-cluster backends implement [`SchedulerBackend`] and adjust the rendered
//! Kubernetes objects:
//!
//! - `kubernetes` (default) — plain kube-scheduler Jobs, no adaptation.
//! - `volcano` — gang scheduling: sets `schedulerName: volcano`, creates a
//!   PodGroup with `minMember = nodes`, and annotates worker pods into it.
//!   Prevents deadlock where a fixed-size rendezvous waits forever on pods
//!   the cluster can only partially schedule.
//! - `kueue` — quota/queueing: labels the Job with
//!   `kueue.x-k8s.io/queue-name` and creates it suspended; Kueue admits it
//!   when quota is available.
//!
//! Out-of-cluster systems (Slurm, Ray, SageMaker, AWS Batch) are not
//! `SchedulerBackend`s — they do not schedule Kubernetes pods.  They
//! integrate one level higher as workload submitters that run
//! `python -m dtruntime.launch` on nodes they allocate; the contract and
//! per-system notes live in docs/schedulers.md.

use kube::ResourceExt;
use serde_json::json;

use crate::crd::TrainingRun;
use crate::render::{labels_for, RenderedWorkload};

pub const KNOWN_BACKENDS: [&str; 3] = ["kubernetes", "volcano", "kueue"];

pub const VOLCANO_SCHEDULER_NAME: &str = "volcano";
pub const VOLCANO_PODGROUP_ANNOTATION: &str = "scheduling.k8s.io/group-name";
pub const KUEUE_QUEUE_LABEL: &str = "kueue.x-k8s.io/queue-name";

#[derive(Debug, thiserror::Error)]
pub enum SchedulerError {
    #[error("scheduler backend {0:?} requires spec.scheduling.queue")]
    QueueRequired(&'static str),
    #[error("unknown scheduler backend {0:?}")]
    Unknown(String),
}

pub trait SchedulerBackend: Send + Sync {
    fn name(&self) -> &'static str;

    /// Adapt the rendered workload for this scheduling system.
    fn adapt(
        &self,
        workload: &mut RenderedWorkload,
        tr: &TrainingRun,
    ) -> Result<(), SchedulerError>;
}

/// Select the backend from `spec.scheduling.backend` (default: kubernetes).
pub fn backend_for(tr: &TrainingRun) -> Result<Box<dyn SchedulerBackend>, SchedulerError> {
    let name = tr
        .spec
        .scheduling
        .as_ref()
        .and_then(|s| s.backend.as_deref())
        .unwrap_or("kubernetes");
    match name {
        "kubernetes" => Ok(Box::new(KubernetesBackend)),
        "volcano" => Ok(Box::new(VolcanoBackend)),
        "kueue" => Ok(Box::new(KueueBackend)),
        other => Err(SchedulerError::Unknown(other.to_string())),
    }
}

/// Plain kube-scheduler: the rendered Job is already correct.
pub struct KubernetesBackend;

impl SchedulerBackend for KubernetesBackend {
    fn name(&self) -> &'static str {
        "kubernetes"
    }

    fn adapt(&self, _: &mut RenderedWorkload, _: &TrainingRun) -> Result<(), SchedulerError> {
        Ok(())
    }
}

/// Volcano gang scheduling.
pub struct VolcanoBackend;

impl SchedulerBackend for VolcanoBackend {
    fn name(&self) -> &'static str {
        "volcano"
    }

    fn adapt(
        &self,
        workload: &mut RenderedWorkload,
        tr: &TrainingRun,
    ) -> Result<(), SchedulerError> {
        let group_name = format!("{}-workers", tr.name_any());

        if let Some(job) = workload.worker_job.as_mut() {
            if let Some(spec) = job.spec.as_mut() {
                let template = &mut spec.template;
                let pod_meta = template.metadata.get_or_insert_with(Default::default);
                pod_meta
                    .annotations
                    .get_or_insert_with(Default::default)
                    .insert(VOLCANO_PODGROUP_ANNOTATION.to_string(), group_name.clone());
                if let Some(pod_spec) = template.spec.as_mut() {
                    // Respect an explicit override; otherwise pods must go to
                    // the Volcano scheduler for gang semantics to apply.
                    if pod_spec.scheduler_name.is_none() {
                        pod_spec.scheduler_name = Some(VOLCANO_SCHEDULER_NAME.to_string());
                    }
                }
            }
        }

        let mut podgroup_spec = json!({
            // Gang size: all worker pods, so a fixed-size rendezvous never
            // waits on pods that cannot be scheduled.
            "minMember": tr.spec.nodes,
        });
        if let Some(queue) = tr.spec.scheduling.as_ref().and_then(|s| s.queue.as_ref()) {
            podgroup_spec["queue"] = json!(queue);
        }
        workload.extra_objects.push(json!({
            "apiVersion": "scheduling.volcano.sh/v1beta1",
            "kind": "PodGroup",
            "metadata": {
                "name": group_name,
                "namespace": tr.namespace(),
                "labels": labels_for(tr, "podgroup"),
            },
            "spec": podgroup_spec,
        }));
        Ok(())
    }
}

/// Kueue admission control.
pub struct KueueBackend;

impl SchedulerBackend for KueueBackend {
    fn name(&self) -> &'static str {
        "kueue"
    }

    fn adapt(
        &self,
        workload: &mut RenderedWorkload,
        tr: &TrainingRun,
    ) -> Result<(), SchedulerError> {
        let queue = tr
            .spec
            .scheduling
            .as_ref()
            .and_then(|s| s.queue.clone())
            .ok_or(SchedulerError::QueueRequired("kueue"))?;

        if let Some(job) = workload.worker_job.as_mut() {
            job.metadata
                .labels
                .get_or_insert_with(Default::default)
                .insert(KUEUE_QUEUE_LABEL.to_string(), queue);
            if let Some(spec) = job.spec.as_mut() {
                // Kueue unsuspends the Job once quota is admitted.
                spec.suspend = Some(true);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::{CoordinatorSpec, SchedulingSpec, TrainingRunSpec};
    use crate::render::render;

    fn training_run(backend: Option<&str>, queue: Option<&str>) -> TrainingRun {
        let mut tr = TrainingRun::new(
            "gpt",
            TrainingRunSpec {
                image: "trainer:v1".into(),
                script: vec!["train.py".into()],
                nodes: 3,
                nproc_per_node: 1,
                coordinator: CoordinatorSpec {
                    address: Some("c:50051".into()),
                    manage: None,
                },
                elastic: None,
                backend: None,
                max_restarts: None,
                backoff_limit: None,
                resources: None,
                scheduling: Some(SchedulingSpec {
                    backend: backend.map(String::from),
                    queue: queue.map(String::from),
                    ..Default::default()
                }),
                env: None,
                service_account_name: None,
            },
        );
        tr.metadata.namespace = Some("training".into());
        tr.metadata.uid = Some("uid-1".into());
        tr
    }

    #[test]
    fn default_backend_is_kubernetes_noop() {
        let tr = training_run(None, None);
        let backend = backend_for(&tr).unwrap();
        assert_eq!(backend.name(), "kubernetes");
        let mut workload = render(&tr);
        backend.adapt(&mut workload, &tr).unwrap();
        assert!(workload.extra_objects.is_empty());
        let job = workload.worker_job.unwrap();
        assert_eq!(job.spec.unwrap().suspend, None);
    }

    #[test]
    fn volcano_backend_sets_gang_scheduling() {
        let tr = training_run(Some("volcano"), Some("research"));
        let backend = backend_for(&tr).unwrap();
        let mut workload = render(&tr);
        backend.adapt(&mut workload, &tr).unwrap();

        let job = workload.worker_job.unwrap();
        let template = job.spec.unwrap().template;
        assert_eq!(
            template.spec.unwrap().scheduler_name.as_deref(),
            Some("volcano")
        );
        assert_eq!(
            template
                .metadata
                .unwrap()
                .annotations
                .unwrap()
                .get(VOLCANO_PODGROUP_ANNOTATION)
                .map(String::as_str),
            Some("gpt-workers")
        );

        assert_eq!(workload.extra_objects.len(), 1);
        let podgroup = &workload.extra_objects[0];
        assert_eq!(podgroup["kind"], "PodGroup");
        assert_eq!(podgroup["spec"]["minMember"], 3);
        assert_eq!(podgroup["spec"]["queue"], "research");
    }

    #[test]
    fn kueue_backend_labels_and_suspends_job() {
        let tr = training_run(Some("kueue"), Some("team-a"));
        let backend = backend_for(&tr).unwrap();
        let mut workload = render(&tr);
        backend.adapt(&mut workload, &tr).unwrap();

        let job = workload.worker_job.unwrap();
        assert_eq!(
            job.metadata
                .labels
                .unwrap()
                .get(KUEUE_QUEUE_LABEL)
                .map(String::as_str),
            Some("team-a")
        );
        assert_eq!(job.spec.unwrap().suspend, Some(true));
    }

    #[test]
    fn kueue_backend_requires_queue() {
        let tr = training_run(Some("kueue"), None);
        let backend = backend_for(&tr).unwrap();
        let mut workload = render(&tr);
        assert!(matches!(
            backend.adapt(&mut workload, &tr),
            Err(SchedulerError::QueueRequired("kueue"))
        ));
    }

    #[test]
    fn unknown_backend_errors() {
        let tr = training_run(Some("slurm"), None);
        assert!(matches!(backend_for(&tr), Err(SchedulerError::Unknown(_))));
    }
}
