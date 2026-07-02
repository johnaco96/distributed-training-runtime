#![forbid(unsafe_code)]

//! Kubernetes operator for the Distributed Training Runtime.
//!
//! Watches `TrainingRun` custom resources and materialises them as
//! Kubernetes workloads: optionally a managed coordinator (Deployment +
//! Service) and an Indexed Job of worker pods, each running
//! `python -m dtruntime.launch`.  Scheduler backends adapt the workload for
//! plain Kubernetes, Volcano gang scheduling, or Kueue quota admission.

pub mod crd;
pub mod reconciler;
pub mod render;
pub mod scheduler;
