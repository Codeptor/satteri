//! Authenticated Unix-domain status administration.
//!
//! Phase 1 is deliberately read-only. The socket has no TCP equivalent and
//! never exposes a database handle, raw SQL, or any execution action.

use std::fs;
use std::io;
#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Component, Path, PathBuf};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::readiness::ReadinessSnapshot;

const ADMIN_SCHEMA_VERSION: u8 = 1;
const MAX_ADMIN_FRAME_BYTES: usize = 4 * 1024;
const MAX_ADMIN_CONNECTIONS: usize = 16;
const ADMIN_CHANNEL_CAPACITY: usize = 32;

/// Strict, versioned local admin request envelope.
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum WireEnvelope {
    /// Returns daemon lifecycle, reconciliation, and readiness state.
    Status { schema_version: u8 },
}

/// Read-only authority-loop requests accepted from authenticated local peers.
pub enum AuthorityRequest {
    /// Returns the current authority-owned status projection.
    Status {
        /// Bounded one-shot response path.
        respond_to: oneshot::Sender<DaemonStatus>,
    },
}

/// A stable read-only daemon status returned by the local protocol.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DaemonStatus {
    /// Stable process-owned run identifier.
    pub run_id: String,
    /// Whether durable recovery/reconciliation completed before subscription.
    pub reconciled: bool,
    /// Requested daemon lifecycle mode; neither mode enables execution alone.
    pub mode: DaemonMode,
    /// Whether the authority has an installed typed strategy/recovery executor.
    ///
    /// This is false until source facts, recovery completion, and strategy
    /// activation are routed together through the sole writer.
    pub execution_enabled: bool,
    /// Hierarchical strategy/execution readiness.
    pub readiness: ReadinessSnapshot,
}

/// The active paper daemon mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DaemonMode {
    /// Read public facts only; execution is sealed off.
    CollectionOnly,
}

#[derive(Debug, Serialize)]
struct WireResponse<'a> {
    schema_version: u8,
    ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    status: Option<&'a DaemonStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<&'a str>,
}

/// Bound local admin listener with a private filesystem socket.
pub struct AdminServer {
    listener: UnixListener,
    socket_path: PathBuf,
    daemon_uid: u32,
}

impl AdminServer {
    /// Creates the sole local administration listener under a private directory.
    ///
    /// Existing sockets are not removed: a stale or attacker-owned node must be
    /// inspected by the operator rather than silently replaced.
    pub async fn bind(path: impl AsRef<Path>) -> Result<Self, AdminError> {
        let socket_path = path.as_ref();
        validate_socket_path(socket_path)?;
        let parent = socket_path.parent().ok_or(AdminError::InvalidSocketPath)?;
        let daemon_uid = daemon_uid()?;
        secure_runtime_directory(parent, daemon_uid)?;
        if socket_path.exists() {
            return Err(AdminError::SocketAlreadyExists);
        }
        let listener = UnixListener::bind(socket_path).map_err(AdminError::Bind)?;
        set_socket_permissions(socket_path, daemon_uid)?;
        Ok(Self {
            listener,
            socket_path: socket_path.to_owned(),
            daemon_uid,
        })
    }

    /// Serves authenticated bounded local requests until cancellation.
    pub async fn serve(
        self,
        authority: mpsc::Sender<AuthorityRequest>,
        cancellation: CancellationToken,
    ) -> Result<(), AdminError> {
        let mut connections = JoinSet::new();
        loop {
            tokio::select! {
                _ = cancellation.cancelled() => break,
                joined = connections.join_next(), if !connections.is_empty() => {
                    if let Some(Err(error)) = joined {
                        tracing::warn!(error = %error, "admin connection task terminated unexpectedly");
                    }
                }
                accepted = self.listener.accept() => {
                    let (stream, _) = accepted.map_err(AdminError::Accept)?;
                    if connections.len() >= MAX_ADMIN_CONNECTIONS {
                        tracing::warn!("admin connection limit reached");
                        drop(stream);
                        continue;
                    }
                    let authority = authority.clone();
                    let cancellation = cancellation.clone();
                    let daemon_uid = self.daemon_uid;
                    connections.spawn(async move {
                        if let Err(error) = handle_connection(stream, daemon_uid, authority, cancellation).await {
                            tracing::debug!(error = %error, "rejected local admin request");
                        }
                    });
                }
            }
        }
        while let Some(joined) = connections.join_next().await {
            if let Err(error) = joined {
                tracing::warn!(error = %error, "admin connection task did not drain cleanly");
            }
        }
        remove_socket(&self.socket_path);
        Ok(())
    }
}

async fn handle_connection(
    mut stream: UnixStream,
    daemon_uid: u32,
    authority: mpsc::Sender<AuthorityRequest>,
    cancellation: CancellationToken,
) -> Result<(), AdminError> {
    let peer = stream.peer_cred().map_err(AdminError::PeerCredentials)?;
    let peer_uid = peer.uid();
    if !peer_uid_allowed(daemon_uid, peer_uid) {
        return Err(AdminError::UnauthorizedPeer);
    }
    let body = tokio::select! {
        _ = cancellation.cancelled() => return Ok(()),
        result = read_frame(&mut stream) => result?,
    };
    let envelope: WireEnvelope =
        serde_json::from_slice(&body).map_err(|_| AdminError::InvalidRequest)?;
    let WireEnvelope::Status { schema_version } = envelope;
    if schema_version != ADMIN_SCHEMA_VERSION {
        write_error(&mut stream, "unsupported_schema").await?;
        return Err(AdminError::UnsupportedSchema);
    }
    let (respond_to, response) = oneshot::channel();
    tokio::select! {
        _ = cancellation.cancelled() => return Ok(()),
        result = authority.send(AuthorityRequest::Status { respond_to }) => {
            result.map_err(|_| AdminError::AuthorityUnavailable)?;
        }
    }
    let status = tokio::select! {
        _ = cancellation.cancelled() => return Ok(()),
        result = response => result.map_err(|_| AdminError::AuthorityUnavailable)?,
    };
    write_response(
        &mut stream,
        WireResponse {
            schema_version: ADMIN_SCHEMA_VERSION,
            ok: true,
            status: Some(&status),
            error: None,
        },
    )
    .await
}

async fn write_error(stream: &mut UnixStream, code: &'static str) -> Result<(), AdminError> {
    write_response(
        stream,
        WireResponse {
            schema_version: ADMIN_SCHEMA_VERSION,
            ok: false,
            status: None,
            error: Some(code),
        },
    )
    .await
}

async fn write_response(
    stream: &mut UnixStream,
    response: WireResponse<'_>,
) -> Result<(), AdminError> {
    let payload = serde_json::to_vec(&response).map_err(|_| AdminError::InvalidResponse)?;
    write_frame(stream, &payload).await
}

/// Reads the bounded status response from the configured local socket.
pub async fn request_status(path: impl AsRef<Path>) -> Result<serde_json::Value, AdminError> {
    let mut stream = UnixStream::connect(path)
        .await
        .map_err(AdminError::Connect)?;
    let request = serde_json::to_vec(&serde_json::json!({
        "schema_version": ADMIN_SCHEMA_VERSION,
        "type": "status",
    }))
    .map_err(|_| AdminError::InvalidRequest)?;
    write_frame(&mut stream, &request).await?;
    let response = read_frame(&mut stream).await?;
    serde_json::from_slice(&response).map_err(|_| AdminError::InvalidResponse)
}

async fn read_frame(stream: &mut UnixStream) -> Result<Vec<u8>, AdminError> {
    let declared = stream
        .read_u32()
        .await
        .map_err(|source| frame_io("reading frame length", source))?;
    let length = usize::try_from(declared).map_err(|_| AdminError::FrameTooLarge)?;
    if length == 0 || length > MAX_ADMIN_FRAME_BYTES {
        return Err(AdminError::FrameTooLarge);
    }
    let mut body = vec![0_u8; length];
    stream
        .read_exact(&mut body)
        .await
        .map_err(|source| frame_io("reading frame body", source))?;
    Ok(body)
}

async fn write_frame(stream: &mut UnixStream, body: &[u8]) -> Result<(), AdminError> {
    let length = u32::try_from(body.len()).map_err(|_| AdminError::FrameTooLarge)?;
    if body.is_empty() || body.len() > MAX_ADMIN_FRAME_BYTES {
        return Err(AdminError::FrameTooLarge);
    }
    stream
        .write_u32(length)
        .await
        .map_err(|source| frame_io("writing frame length", source))?;
    stream
        .write_all(body)
        .await
        .map_err(|source| frame_io("writing frame body", source))?;
    stream
        .flush()
        .await
        .map_err(|source| frame_io("flushing frame", source))
}

fn peer_uid_allowed(daemon_uid: u32, peer_uid: u32) -> bool {
    peer_uid == daemon_uid || peer_uid == 0
}

fn daemon_uid() -> Result<u32, AdminError> {
    #[cfg(target_os = "linux")]
    {
        Ok(rustix::process::getuid().as_raw())
    }
    #[cfg(not(target_os = "linux"))]
    {
        Err(AdminError::UnsupportedPlatform)
    }
}

fn validate_socket_path(path: &Path) -> Result<(), AdminError> {
    if !path.is_absolute()
        || path.file_name().is_none()
        || path.extension().is_none_or(|ext| ext != "sock")
    {
        return Err(AdminError::InvalidSocketPath);
    }
    Ok(())
}

fn secure_runtime_directory(path: &Path, daemon_uid: u32) -> Result<(), AdminError> {
    reject_symlink_components(path)?;
    fs::create_dir_all(path).map_err(AdminError::RuntimeDirectory)?;
    reject_symlink_components(path)?;
    let metadata = fs::symlink_metadata(path).map_err(AdminError::RuntimeDirectory)?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(AdminError::InvalidRuntimeDirectory);
    }
    #[cfg(unix)]
    {
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))
            .map_err(AdminError::RuntimeDirectory)?;
        if fs::metadata(path)
            .map_err(AdminError::RuntimeDirectory)?
            .permissions()
            .mode()
            & 0o777
            != 0o700
        {
            return Err(AdminError::InvalidRuntimeDirectory);
        }
        if fs::metadata(path)
            .map_err(AdminError::RuntimeDirectory)?
            .uid()
            != daemon_uid
        {
            return Err(AdminError::RuntimeDirectoryOwner);
        }
    }
    Ok(())
}

fn set_socket_permissions(path: &Path, daemon_uid: u32) -> Result<(), AdminError> {
    #[cfg(unix)]
    {
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))
            .map_err(AdminError::SocketMode)?;
        if fs::metadata(path)
            .map_err(AdminError::SocketMode)?
            .permissions()
            .mode()
            & 0o777
            != 0o600
        {
            return Err(AdminError::SocketMode(io::Error::other(
                "socket mode is not 0600",
            )));
        }
        if fs::metadata(path).map_err(AdminError::SocketMode)?.uid() != daemon_uid {
            return Err(AdminError::SocketOwner);
        }
        Ok(())
    }
    #[cfg(not(unix))]
    {
        let _ = (path, daemon_uid);
        Err(AdminError::UnsupportedPlatform)
    }
}

fn reject_symlink_components(path: &Path) -> Result<(), AdminError> {
    let mut current = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => current.push(prefix.as_os_str()),
            Component::RootDir => current.push(component.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => return Err(AdminError::InvalidRuntimeDirectory),
            Component::Normal(segment) => {
                current.push(segment);
                match fs::symlink_metadata(&current) {
                    Ok(metadata) if metadata.file_type().is_symlink() => {
                        return Err(AdminError::InvalidRuntimeDirectory);
                    }
                    Ok(_) => {}
                    Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                    Err(error) => return Err(AdminError::RuntimeDirectory(error)),
                }
            }
        }
    }
    Ok(())
}

fn remove_socket(path: &Path) {
    if let Err(error) = fs::remove_file(path)
        && error.kind() != io::ErrorKind::NotFound
    {
        tracing::warn!(error = %error, socket = %path.display(), "failed to remove local admin socket");
    }
}

fn frame_io(operation: &'static str, source: io::Error) -> AdminError {
    if source.kind() == io::ErrorKind::UnexpectedEof {
        AdminError::TruncatedFrame
    } else {
        AdminError::FrameIo { operation, source }
    }
}

/// An admin socket path, permission, peer, protocol, or authority failure.
#[derive(Debug, Error)]
pub enum AdminError {
    /// Unix peer credentials are required for local administration.
    #[cfg(not(target_os = "linux"))]
    #[error("local admin sockets require Linux peer credentials")]
    UnsupportedPlatform,
    /// The configured socket was not an absolute `.sock` pathname.
    #[error("admin socket path is invalid")]
    InvalidSocketPath,
    /// The runtime directory could not be created or inspected.
    #[error("admin runtime directory operation failed")]
    RuntimeDirectory(#[source] io::Error),
    /// The runtime directory was not a private non-symlink directory.
    #[error("admin runtime directory is not private")]
    InvalidRuntimeDirectory,
    /// The private runtime directory is not owned by the daemon UID.
    #[error("admin runtime directory is not owned by the daemon UID")]
    RuntimeDirectoryOwner,
    /// A socket already exists at the configured target.
    #[error("admin socket already exists")]
    SocketAlreadyExists,
    /// The Unix socket could not bind or accept a local peer.
    #[error("admin socket operation failed")]
    Bind(#[source] io::Error),
    /// The Unix socket could not accept a local peer.
    #[error("admin socket accept failed")]
    Accept(#[source] io::Error),
    /// The Unix socket mode could not be restricted to 0600.
    #[error("admin socket could not be restricted to mode 0600")]
    SocketMode(#[source] io::Error),
    /// The bound socket is not owned by the daemon UID.
    #[error("admin socket is not owned by the daemon UID")]
    SocketOwner,
    /// The local client could not connect to the socket.
    #[error("admin socket connection failed")]
    Connect(#[source] io::Error),
    /// The kernel did not expose a usable peer UID.
    #[error("admin peer credentials are unavailable")]
    PeerCredentials(#[source] io::Error),
    /// The peer UID was neither the daemon owner nor root.
    #[error("admin peer is not authorized")]
    UnauthorizedPeer,
    /// The declared frame size was zero or beyond the fixed bound.
    #[error("admin frame exceeds the fixed size bound")]
    FrameTooLarge,
    /// The client disconnected before a complete frame arrived.
    #[error("admin frame is truncated")]
    TruncatedFrame,
    /// Frame transport failed without a truncation condition.
    #[error("admin frame I/O failed while {operation}")]
    FrameIo {
        /// Bounded frame operation.
        operation: &'static str,
        /// Underlying local I/O failure.
        #[source]
        source: io::Error,
    },
    /// The local JSON request had an unknown type or invalid shape.
    #[error("admin request is invalid")]
    InvalidRequest,
    /// The local request named an unsupported protocol version.
    #[error("admin schema version is unsupported")]
    UnsupportedSchema,
    /// The authority loop exited before answering the bounded request.
    #[error("daemon authority loop is unavailable")]
    AuthorityUnavailable,
    /// A response could not be serialized or was not valid bounded JSON.
    #[error("admin response is invalid")]
    InvalidResponse,
}

/// Creates the bounded authority request channel used by the daemon app.
#[must_use]
pub fn authority_channel() -> (
    mpsc::Sender<AuthorityRequest>,
    mpsc::Receiver<AuthorityRequest>,
) {
    mpsc::channel(ADMIN_CHANNEL_CAPACITY)
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt;

    use tokio::io::AsyncWriteExt;
    use tokio_util::sync::CancellationToken;

    use super::{
        AdminError, AdminServer, AuthorityRequest, DaemonMode, DaemonStatus, authority_channel,
        peer_uid_allowed, request_status,
    };
    use crate::readiness::Readiness;

    fn status() -> DaemonStatus {
        DaemonStatus {
            run_id: "run-admin-test".to_owned(),
            reconciled: true,
            mode: DaemonMode::CollectionOnly,
            execution_enabled: false,
            readiness: Readiness::default().snapshot(),
        }
    }

    async fn running_server() -> (
        tempfile::TempDir,
        std::path::PathBuf,
        CancellationToken,
        tokio::task::JoinHandle<Result<(), AdminError>>,
    ) {
        let directory = tempfile::tempdir().expect("fixture directory");
        std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700))
            .expect("private fixture directory");
        let socket = directory.path().join("trenchd.sock");
        let server = AdminServer::bind(&socket)
            .await
            .expect("admin server binds");
        assert_eq!(
            std::fs::metadata(&socket)
                .expect("socket metadata")
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        let (sender, mut receiver) = authority_channel();
        let cancellation = CancellationToken::new();
        let worker_cancellation = cancellation.clone();
        tokio::spawn(async move {
            while let Some(request) = receiver.recv().await {
                match request {
                    AuthorityRequest::Status { respond_to } => {
                        let _ = respond_to.send(status());
                    }
                }
            }
        });
        let task = tokio::spawn(server.serve(sender, worker_cancellation));
        (directory, socket, cancellation, task)
    }

    #[tokio::test]
    async fn status_uses_a_private_versioned_local_protocol() {
        let (_directory, socket, cancellation, task) = running_server().await;
        let response = request_status(&socket).await.expect("status response");
        assert_eq!(response["ok"], true);
        assert_eq!(response["status"]["run_id"], "run-admin-test");
        assert_eq!(response["status"]["execution_enabled"], false);
        cancellation.cancel();
        task.await
            .expect("admin task join")
            .expect("admin shutdown");
        assert!(!socket.exists());
    }

    #[test]
    fn non_owner_peers_are_rejected_and_root_is_permitted() {
        assert!(!peer_uid_allowed(1_000, 1_001));
        assert!(peer_uid_allowed(1_000, 1_000));
        assert!(peer_uid_allowed(1_000, 0));
    }

    #[test]
    fn status_wire_shape_is_strict_and_decodable() {
        let envelope: super::WireEnvelope =
            serde_json::from_slice(br#"{"schema_version":1,"type":"status"}"#)
                .expect("valid status request");
        assert!(matches!(
            envelope,
            super::WireEnvelope::Status { schema_version: 1 }
        ));
    }

    #[tokio::test]
    async fn unsupported_schema_unknown_request_and_oversized_frames_are_rejected() {
        let (_directory, socket, cancellation, task) = running_server().await;
        let mut stream = tokio::net::UnixStream::connect(&socket)
            .await
            .expect("connect admin");
        super::write_frame(&mut stream, br#"{"schema_version":9,"type":"status"}"#)
            .await
            .expect("write frame");
        let response = super::read_frame(&mut stream)
            .await
            .expect("error response");
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&response).expect("JSON")["error"],
            "unsupported_schema"
        );

        let mut unknown = tokio::net::UnixStream::connect(&socket)
            .await
            .expect("connect admin");
        super::write_frame(&mut unknown, br#"{"schema_version":1,"type":"shutdown"}"#)
            .await
            .expect("write frame");
        let error = super::read_frame(&mut unknown)
            .await
            .expect_err("unknown request must close");
        assert!(matches!(error, AdminError::TruncatedFrame));

        let mut oversized = tokio::net::UnixStream::connect(&socket)
            .await
            .expect("connect admin");
        oversized
            .write_u32(4_097)
            .await
            .expect("write oversized length");
        oversized.shutdown().await.expect("close oversized stream");

        let mut truncated = tokio::net::UnixStream::connect(&socket)
            .await
            .expect("connect admin");
        truncated.write_u32(8).await.expect("write length");
        truncated
            .write_all(b"{}")
            .await
            .expect("write partial body");
        truncated.shutdown().await.expect("close partial stream");

        cancellation.cancel();
        task.await
            .expect("admin task join")
            .expect("admin shutdown");
    }
}
