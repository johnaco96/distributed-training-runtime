#![forbid(unsafe_code)]

//! Shared domain types and common coordinator/runtime utilities.
//!
//! Provides shared domain types and common utilities used across the runtime coordinator and related crates.

/// Placeholder marker for the `runtime-core` crate.
#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
pub struct RuntimeCorePlaceholder;
