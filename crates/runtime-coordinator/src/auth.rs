#![forbid(unsafe_code)]

use std::str::FromStr;

use tonic::metadata::MetadataMap;
use tonic::{Code, Status};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuthMode {
    Disabled,
    Metadata,
}

impl FromStr for AuthMode {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "disabled" => Ok(Self::Disabled),
            "metadata" => Ok(Self::Metadata),
            _ => Err(format!("unsupported auth mode: {value}")),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuthRole {
    Admin,
    Worker,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthContext {
    pub subject: String,
    pub role: AuthRole,
    pub worker_id: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuthorizationError {
    MissingIdentity,
    InvalidRole,
    PermissionDenied,
    WorkerIdentityMismatch,
}

impl AuthorizationError {
    pub fn to_status(self) -> Status {
        match self {
            Self::MissingIdentity | Self::InvalidRole => {
                Status::new(Code::Unauthenticated, self.as_str())
            }
            Self::PermissionDenied | Self::WorkerIdentityMismatch => {
                Status::new(Code::PermissionDenied, self.as_str())
            }
        }
    }

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::MissingIdentity => "missing caller identity metadata",
            Self::InvalidRole => "invalid caller role metadata",
            Self::PermissionDenied => "caller is not authorized for this operation",
            Self::WorkerIdentityMismatch => "worker identity does not match request worker_id",
        }
    }
}

#[derive(Clone, Debug)]
pub struct Authorizer {
    mode: AuthMode,
}

impl Authorizer {
    pub fn new(mode: AuthMode) -> Self {
        Self { mode }
    }

    pub fn mode(&self) -> AuthMode {
        self.mode
    }

    pub(crate) fn context(
        &self,
        metadata: &MetadataMap,
    ) -> Result<Option<AuthContext>, AuthorizationError> {
        if self.mode == AuthMode::Disabled {
            return Ok(None);
        }

        let subject = metadata
            .get("x-dtruntime-subject")
            .and_then(|v| v.to_str().ok())
            .filter(|v| !v.trim().is_empty())
            .ok_or(AuthorizationError::MissingIdentity)?;

        let role = match metadata
            .get("x-dtruntime-role")
            .and_then(|v| v.to_str().ok())
            .map(|v| v.trim().to_ascii_lowercase())
            .as_deref()
        {
            Some("admin") => AuthRole::Admin,
            Some("worker") => AuthRole::Worker,
            _ => return Err(AuthorizationError::InvalidRole),
        };

        let worker_id = metadata
            .get("x-dtruntime-worker-id")
            .and_then(|v| v.to_str().ok())
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty());

        Ok(Some(AuthContext {
            subject: subject.to_string(),
            role,
            worker_id,
        }))
    }

    pub fn require_admin(&self, metadata: &MetadataMap) -> Result<(), Status> {
        let context = self
            .context(metadata)
            .map_err(AuthorizationError::to_status)?;
        if let Some(ctx) = context {
            if ctx.role != AuthRole::Admin {
                return Err(AuthorizationError::PermissionDenied.to_status());
            }
        }
        Ok(())
    }

    pub fn require_worker_capability(&self, metadata: &MetadataMap) -> Result<(), Status> {
        let context = self
            .context(metadata)
            .map_err(AuthorizationError::to_status)?;
        if let Some(ctx) = context {
            if ctx.role != AuthRole::Worker {
                return Err(AuthorizationError::PermissionDenied.to_status());
            }
        }
        Ok(())
    }

    pub fn allow_admin_or_worker_self(
        &self,
        metadata: &MetadataMap,
        request_worker_id: &str,
    ) -> Result<(), Status> {
        let context = self
            .context(metadata)
            .map_err(AuthorizationError::to_status)?;
        if let Some(ctx) = context {
            match ctx.role {
                AuthRole::Admin => Ok(()),
                AuthRole::Worker => {
                    let provided = ctx
                        .worker_id
                        .ok_or(AuthorizationError::MissingIdentity)
                        .map_err(AuthorizationError::to_status)?;
                    if provided == request_worker_id {
                        Ok(())
                    } else {
                        Err(AuthorizationError::WorkerIdentityMismatch.to_status())
                    }
                }
            }
        } else {
            Ok(())
        }
    }
}
