//! TrainingRun controller: watches TrainingRun resources and drives the
//! cluster toward the rendered workload with server-side apply.
//!
//! Reconcile flow:
//! 1. validate the spec — invalid specs mark the run `Failed` with the
//!    validation message and are not retried until edited;
//! 2. render the base objects and let the scheduler backend adapt them;
//! 3. server-side apply everything (owner references make deletion cascade);
//! 4. project the worker Job's status onto `status.phase`
//!    (Pending → Running → Succeeded/Failed).

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::batch::v1::Job;
use k8s_openapi::api::core::v1::Service;
use kube::api::{Api, ApiResource, DynamicObject, Patch, PatchParams};
use kube::core::GroupVersionKind;
use kube::runtime::controller::{Action, Controller};
use kube::runtime::watcher;
use kube::{Client, ResourceExt};
use serde_json::json;
use tracing::{info, warn};

use crate::crd::{phase, validate, TrainingRun, TrainingRunStatus};
use crate::render::{render, worker_job_name};
use crate::scheduler::backend_for;

pub const FIELD_MANAGER: &str = "runtime-operator";

const REQUEUE_RUNNING: Duration = Duration::from_secs(30);
const REQUEUE_ERROR: Duration = Duration::from_secs(15);

#[derive(Debug, thiserror::Error)]
pub enum ReconcileError {
    #[error("kube api error: {0}")]
    Kube(#[from] kube::Error),
    #[error("TrainingRun has no namespace")]
    MissingNamespace,
    #[error("extra object is missing apiVersion/kind")]
    MalformedExtraObject,
}

pub struct Context {
    pub client: Client,
}

pub async fn run_controller(client: Client) {
    let training_runs: Api<TrainingRun> = Api::all(client.clone());
    let jobs: Api<Job> = Api::all(client.clone());
    let context = Arc::new(Context { client });

    Controller::new(training_runs, watcher::Config::default())
        .owns(jobs, watcher::Config::default())
        .shutdown_on_signal()
        .run(reconcile, error_policy, context)
        .for_each(|result| async move {
            match result {
                Ok((obj, _)) => info!(object = %obj.name, "reconciled"),
                Err(err) => warn!(error = %err, "reconcile failed"),
            }
        })
        .await;
}

fn error_policy(_tr: Arc<TrainingRun>, _error: &ReconcileError, _ctx: Arc<Context>) -> Action {
    Action::requeue(REQUEUE_ERROR)
}

pub async fn reconcile(tr: Arc<TrainingRun>, ctx: Arc<Context>) -> Result<Action, ReconcileError> {
    let namespace = tr.namespace().ok_or(ReconcileError::MissingNamespace)?;
    let name = tr.name_any();

    if let Err(message) =
        validate(&tr.spec).and_then(|()| backend_for(&tr).map(|_| ()).map_err(|e| e.to_string()))
    {
        warn!(%name, %message, "invalid TrainingRun spec");
        patch_status(
            &ctx.client,
            &tr,
            TrainingRunStatus {
                phase: Some(phase::FAILED.to_string()),
                message: Some(message),
                observed_generation: tr.metadata.generation,
                ..Default::default()
            },
        )
        .await?;
        // No requeue: nothing changes until the spec is edited, which
        // triggers a new reconcile.
        return Ok(Action::await_change());
    }

    // Render and adapt.
    let mut workload = render(&tr);
    let backend = backend_for(&tr).expect("validated above");
    if let Err(err) = backend.adapt(&mut workload, &tr) {
        patch_status(
            &ctx.client,
            &tr,
            TrainingRunStatus {
                phase: Some(phase::FAILED.to_string()),
                message: Some(err.to_string()),
                observed_generation: tr.metadata.generation,
                ..Default::default()
            },
        )
        .await?;
        return Ok(Action::await_change());
    }

    // Apply objects (server-side apply; ownerReferences cascade deletion).
    let params = PatchParams::apply(FIELD_MANAGER);
    if let Some(deployment) = &workload.coordinator_deployment {
        let api: Api<Deployment> = Api::namespaced(ctx.client.clone(), &namespace);
        api.patch(
            deployment.name_any().as_str(),
            &params,
            &Patch::Apply(deployment),
        )
        .await?;
    }
    if let Some(service) = &workload.coordinator_service {
        let api: Api<Service> = Api::namespaced(ctx.client.clone(), &namespace);
        api.patch(service.name_any().as_str(), &params, &Patch::Apply(service))
            .await?;
    }
    if let Some(job) = &workload.worker_job {
        let api: Api<Job> = Api::namespaced(ctx.client.clone(), &namespace);
        api.patch(job.name_any().as_str(), &params, &Patch::Apply(job))
            .await?;
    }
    for extra in &workload.extra_objects {
        apply_dynamic(&ctx.client, &namespace, extra, &params).await?;
    }

    // Project Job status onto the TrainingRun.
    let jobs: Api<Job> = Api::namespaced(ctx.client.clone(), &namespace);
    let status = match jobs.get_opt(&worker_job_name(&tr)).await? {
        Some(job) => project_status(&tr, &job),
        None => TrainingRunStatus {
            phase: Some(phase::PENDING.to_string()),
            message: Some("worker job not yet visible".to_string()),
            observed_generation: tr.metadata.generation,
            ..Default::default()
        },
    };
    let terminal = matches!(
        status.phase.as_deref(),
        Some(phase::SUCCEEDED) | Some(phase::FAILED)
    );
    patch_status(&ctx.client, &tr, status).await?;

    if terminal {
        Ok(Action::await_change())
    } else {
        Ok(Action::requeue(REQUEUE_RUNNING))
    }
}

/// Derive the TrainingRun phase from the worker Job's status.
pub fn project_status(tr: &TrainingRun, job: &Job) -> TrainingRunStatus {
    let job_status = job.status.clone().unwrap_or_default();
    let active = job_status.active.unwrap_or(0);
    let succeeded = job_status.succeeded.unwrap_or(0);
    let failed = job_status.failed.unwrap_or(0);
    let completions = job
        .spec
        .as_ref()
        .and_then(|s| s.completions)
        .unwrap_or(tr.spec.nodes as i32);

    let job_condition = job_status
        .conditions
        .unwrap_or_default()
        .into_iter()
        .find(|c| (c.type_ == "Complete" || c.type_ == "Failed") && c.status == "True");

    let (phase_value, message) = match job_condition {
        Some(c) if c.type_ == "Complete" => (
            phase::SUCCEEDED,
            format!("{succeeded}/{completions} worker nodes completed"),
        ),
        Some(c) => (
            phase::FAILED,
            c.message
                .unwrap_or_else(|| format!("{failed} worker pod(s) failed permanently")),
        ),
        None if active > 0 => (
            phase::RUNNING,
            format!("{active} worker node(s) running, {succeeded} completed"),
        ),
        None => (
            phase::PENDING,
            "waiting for worker pods to be scheduled".to_string(),
        ),
    };

    TrainingRunStatus {
        phase: Some(phase_value.to_string()),
        message: Some(message),
        active_nodes: active,
        succeeded_nodes: succeeded,
        failed_nodes: failed,
        observed_generation: tr.metadata.generation,
    }
}

async fn patch_status(
    client: &Client,
    tr: &TrainingRun,
    status: TrainingRunStatus,
) -> Result<(), ReconcileError> {
    let namespace = tr.namespace().ok_or(ReconcileError::MissingNamespace)?;
    let api: Api<TrainingRun> = Api::namespaced(client.clone(), &namespace);
    api.patch_status(
        &tr.name_any(),
        &PatchParams::apply(FIELD_MANAGER),
        &Patch::Merge(json!({ "status": status })),
    )
    .await?;
    Ok(())
}

/// Apply a scheduler-specific object (e.g. Volcano PodGroup) as a dynamic
/// resource so the operator does not need compiled types for every scheduler
/// CRD.
async fn apply_dynamic(
    client: &Client,
    namespace: &str,
    object: &serde_json::Value,
    params: &PatchParams,
) -> Result<(), ReconcileError> {
    let api_version = object["apiVersion"]
        .as_str()
        .ok_or(ReconcileError::MalformedExtraObject)?;
    let kind = object["kind"]
        .as_str()
        .ok_or(ReconcileError::MalformedExtraObject)?;
    let name = object["metadata"]["name"]
        .as_str()
        .ok_or(ReconcileError::MalformedExtraObject)?;

    let (group, version) = match api_version.split_once('/') {
        Some((g, v)) => (g, v),
        None => ("", api_version),
    };
    let gvk = GroupVersionKind::gvk(group, version, kind);
    let resource = ApiResource::from_gvk(&gvk);
    let api: Api<DynamicObject> = Api::namespaced_with(client.clone(), namespace, &resource);
    api.patch(name, params, &Patch::Apply(object)).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::{CoordinatorSpec, TrainingRunSpec};
    use k8s_openapi::api::batch::v1::{JobCondition, JobStatus};

    fn training_run() -> TrainingRun {
        TrainingRun::new(
            "tr",
            TrainingRunSpec {
                image: "img".into(),
                script: vec!["train.py".into()],
                nodes: 2,
                nproc_per_node: 1,
                coordinator: CoordinatorSpec {
                    address: Some("c:1".into()),
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
            },
        )
    }

    fn job_with_status(status: JobStatus) -> Job {
        Job {
            status: Some(status),
            ..Default::default()
        }
    }

    #[test]
    fn no_activity_projects_pending() {
        let status = project_status(&training_run(), &job_with_status(JobStatus::default()));
        assert_eq!(status.phase.as_deref(), Some(phase::PENDING));
    }

    #[test]
    fn active_pods_project_running() {
        let status = project_status(
            &training_run(),
            &job_with_status(JobStatus {
                active: Some(2),
                ..Default::default()
            }),
        );
        assert_eq!(status.phase.as_deref(), Some(phase::RUNNING));
        assert_eq!(status.active_nodes, 2);
    }

    #[test]
    fn complete_condition_projects_succeeded() {
        let status = project_status(
            &training_run(),
            &job_with_status(JobStatus {
                succeeded: Some(2),
                conditions: Some(vec![JobCondition {
                    type_: "Complete".into(),
                    status: "True".into(),
                    ..Default::default()
                }]),
                ..Default::default()
            }),
        );
        assert_eq!(status.phase.as_deref(), Some(phase::SUCCEEDED));
        assert_eq!(status.succeeded_nodes, 2);
    }

    #[test]
    fn failed_condition_projects_failed_with_message() {
        let status = project_status(
            &training_run(),
            &job_with_status(JobStatus {
                failed: Some(7),
                conditions: Some(vec![JobCondition {
                    type_: "Failed".into(),
                    status: "True".into(),
                    message: Some("BackoffLimitExceeded".into()),
                    ..Default::default()
                }]),
                ..Default::default()
            }),
        );
        assert_eq!(status.phase.as_deref(), Some(phase::FAILED));
        assert_eq!(status.message.as_deref(), Some("BackoffLimitExceeded"));
        assert_eq!(status.failed_nodes, 7);
    }
}
