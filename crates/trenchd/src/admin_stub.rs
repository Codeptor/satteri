//! Non-Unix build stub for the Unix-only local administration surface.

use std::path::Path;

use serde::Serialize;
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use crate::readiness::ReadinessSnapshot;

/// Read-only authority request retained for cross-platform binary builds.
#[allow(
    dead_code,
    reason = "the non-Unix stub has no local transport that can construct a request"
)]
pub enum AuthorityRequest {
    /// Returns the current authority-owned status projection.
    Status {
        /// Bounded one-shot response path.
        respond_to: oneshot::Sender<DaemonStatus>,
    },
}

/// Stable local daemon status shape.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DaemonStatus {
    /// Stable process-owned run identifier.
    pub run_id: String,
    /// Whether durable recovery/reconciliation completed before subscription.
    pub reconciled: bool,
    /// Requested daemon lifecycle mode; neither mode enables execution alone.
    pub mode: DaemonMode,
    /// Whether the authority has an installed typed strategy/recovery executor.
    pub execution_enabled: bool,
    /// Hierarchical strategy/execution readiness.
    pub readiness: ReadinessSnapshot,
}

/// Paper daemon lifecycle mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DaemonMode {
    /// Read public facts only; execution is sealed off.
    CollectionOnly,
}

/// Unix-only admin bind placeholder for cross-platform compilation.
pub struct AdminServer;

impl AdminServer {
    /// Reports that Unix peer-authenticated administration is unavailable.
    pub async fn bind(_path: impl AsRef<Path>) -> Result<Self, AdminError> {
        Err(AdminError::UnsupportedPlatform)
    }

    /// Retains the Unix server API without introducing a TCP fallback.
    pub async fn serve(
        self,
        _authority: mpsc::Sender<AuthorityRequest>,
        _cancellation: CancellationToken,
    ) -> Result<(), AdminError> {
        Err(AdminError::UnsupportedPlatform)
    }
}

/// Refuses non-Unix status requests rather than opening another transport.
pub async fn request_status(_path: impl AsRef<Path>) -> Result<serde_json::Value, AdminError> {
    Err(AdminError::UnsupportedPlatform)
}

/// Creates the same bounded authority channel used by the Unix implementation.
#[must_use]
pub fn authority_channel() -> (
    mpsc::Sender<AuthorityRequest>,
    mpsc::Receiver<AuthorityRequest>,
) {
    mpsc::channel(32)
}

/// Unix-only administration error.
#[derive(Debug, Error)]
pub enum AdminError {
    /// Local peer-authenticated Unix administration is not available here.
    #[error("local admin sockets require Unix")]
    UnsupportedPlatform,
}
