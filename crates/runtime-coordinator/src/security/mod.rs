#![forbid(unsafe_code)]

//! Production security building blocks for the coordinator control plane.
//!
//! - [`rbac`]: the role/permission model (admin/operator/viewer/worker).
//! - [`jwt`]: OIDC/JWT bearer-token validation (resource-server side).
//! - [`spiffe`]: SPIFFE X.509-SVID identity from mTLS peer certificates.
//! - [`keyset`]: hot-rotatable JWT verification keys (secret rotation).
//!
//! These are composed by [`crate::auth::Authenticator`], which layers the
//! credential sources (bearer token → SPIFFE SVID → dev metadata header) into a
//! single verified [`crate::auth::AuthContext`].

pub mod jwt;
pub mod keyset;
pub mod rbac;
pub mod spiffe;
