//! Single-writer SQLite journal for durable paper-trading transitions.

use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

#[cfg(unix)]
use std::fs::Permissions;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use sqlx::migrate::MigrateError;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqliteSynchronous};
use sqlx::{Connection, SqliteConnection};
use thiserror::Error;

const BUSY_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_ID_LENGTH: usize = 128;
const MAX_CODE_LENGTH: usize = 64;

/// Errors returned by the SQLite journal.
#[derive(Debug, Error)]
pub enum StoreError {
    /// The requested database path is not a safe file target.
    #[error("invalid database path: {reason}")]
    InvalidPath { reason: &'static str },
    /// An identifier or code does not satisfy the journal contract.
    #[error("invalid {field}: {reason}")]
    InvalidInput {
        field: &'static str,
        reason: &'static str,
    },
    /// A filesystem operation failed.
    #[error("filesystem operation failed while {operation}")]
    Filesystem {
        operation: &'static str,
        #[source]
        source: std::io::Error,
    },
    /// SQLite failed to open, configure, or execute a journal operation.
    #[error("database operation failed")]
    Database(#[source] sqlx::Error),
    /// An embedded schema migration failed.
    #[error("database migration failed")]
    Migration(#[source] MigrateError),
    /// SQLite did not retain a required durability setting.
    #[error("database durability setting mismatch for {pragma}")]
    PragmaMismatch { pragma: &'static str },
    /// A deterministic failure was requested after the event insert.
    #[error("deterministic transaction failure injected")]
    InjectedFailure,
}

impl From<sqlx::Error> for StoreError {
    fn from(source: sqlx::Error) -> Self {
        Self::Database(source)
    }
}

/// The visible paper ledgers supported by the initial schema.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LedgerId {
    /// Deterministic rules-only strategy ledger.
    RulesOnly,
    /// Frozen ML champion strategy ledger.
    MlChampion,
}

impl LedgerId {
    fn as_str(self) -> &'static str {
        match self {
            Self::RulesOnly => "rules_only",
            Self::MlChampion => "ml_champion",
        }
    }
}

/// Input for registering a journal run.
#[derive(Clone, Copy, Debug)]
pub struct RunInput<'a> {
    /// Stable identifier for the run.
    pub run_id: &'a str,
    /// Explicit UTC Unix timestamp in nanoseconds.
    pub started_at_ns: i64,
}

/// Immutable normalized event appended to the journal.
#[derive(Clone, Copy, Debug)]
pub struct EventInput<'a> {
    /// Owning run identifier.
    pub run_id: &'a str,
    /// Globally unique event identifier.
    pub event_id: &'a str,
    /// Explicit UTC Unix timestamp in nanoseconds.
    pub event_time_ns: i64,
    /// Stable machine-readable event kind.
    pub kind: &'a str,
    /// JSON event payload.
    pub payload_json: &'a str,
}

/// Rejected risk decision committed with its source event.
#[derive(Clone, Copy, Debug)]
pub struct RiskRejectionInput<'a> {
    /// Owning run identifier.
    pub run_id: &'a str,
    /// Globally unique decision identifier.
    pub decision_id: &'a str,
    /// Source event identifier.
    pub event_id: &'a str,
    /// Independently accounted paper ledger.
    pub ledger_id: LedgerId,
    /// Explicit UTC Unix timestamp in nanoseconds.
    pub decided_at_ns: i64,
    /// Stable machine-readable rejection reason.
    pub reason_code: &'a str,
    /// JSON details for the rejection.
    pub details_json: &'a str,
}

/// Controls the deterministic boundary of an event/rejection append.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AtomicAppend {
    /// Commit both rows atomically.
    Commit,
    /// Roll back after inserting the event and before inserting the decision.
    FailAfterEvent,
}

/// Counts of the two transition rows written by the atomic append API.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct JournalCounts {
    /// Event rows for the requested run.
    pub events: i64,
    /// Risk-decision rows for the requested run.
    pub risk_decisions: i64,
}

/// Required connection durability settings observed from SQLite.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PragmaSettings {
    /// SQLite journal mode, normalized to lowercase.
    pub journal_mode: String,
    /// SQLite synchronous level (`2` is `FULL`).
    pub synchronous: i64,
    /// Foreign-key enforcement flag (`1` is enabled).
    pub foreign_keys: i64,
}

/// Durable journal backed by one exclusive SQLite connection.
///
/// Write methods require `&mut self`, making the single-writer boundary explicit
/// to callers. The underlying connection is never exposed.
pub struct SqliteStore {
    connection: SqliteConnection,
}

impl SqliteStore {
    /// Opens an explicit file database, configures durability, and applies migrations.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] when the path is not a file target, filesystem
    /// permissions cannot be secured, SQLite cannot be configured, a migration
    /// fails, or a required PRAGMA does not match after initialization.
    pub async fn open(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        let path = resolve_database_path(path.as_ref())?;
        secure_parent_directory(&path)?;

        let options = SqliteConnectOptions::new()
            .filename(&path)
            .create_if_missing(true)
            .foreign_keys(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Full)
            .busy_timeout(BUSY_TIMEOUT);
        let mut connection = SqliteConnection::connect_with(&options).await?;
        secure_database_file(&path)?;

        sqlx::query("PRAGMA journal_mode = WAL")
            .execute(&mut connection)
            .await?;
        sqlx::query("PRAGMA synchronous = FULL")
            .execute(&mut connection)
            .await?;
        sqlx::query("PRAGMA foreign_keys = ON")
            .execute(&mut connection)
            .await?;
        sqlx::query("PRAGMA busy_timeout = 5000")
            .execute(&mut connection)
            .await?;

        sqlx::migrate!("./migrations")
            .run(&mut connection)
            .await
            .map_err(StoreError::Migration)?;

        let mut store = Self { connection };
        store.verify_pragmas().await?;
        Ok(store)
    }

    /// Registers the owning run for subsequent child records.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] for an invalid identifier/timestamp or a database error.
    pub async fn create_run(&mut self, input: RunInput<'_>) -> Result<(), StoreError> {
        validate_id("run_id", input.run_id)?;
        validate_timestamp("started_at_ns", input.started_at_ns)?;

        sqlx::query("INSERT INTO runs (run_id, started_at_ns) VALUES (?1, ?2)")
            .bind(input.run_id)
            .bind(input.started_at_ns)
            .execute(&mut self.connection)
            .await?;
        Ok(())
    }

    /// Appends an event and its risk rejection in one atomic transaction.
    ///
    /// `AtomicAppend::FailAfterEvent` is a deterministic recovery probe: it
    /// explicitly rolls back before returning [`StoreError::InjectedFailure`].
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] for invalid inputs, inconsistent event/rejection
    /// ownership, the recovery failpoint, or a database error.
    pub async fn append_event_and_risk_rejection(
        &mut self,
        event: EventInput<'_>,
        rejection: RiskRejectionInput<'_>,
        behavior: AtomicAppend,
    ) -> Result<(), StoreError> {
        validate_event(event)?;
        validate_rejection(rejection)?;
        if event.run_id != rejection.run_id {
            return Err(StoreError::InvalidInput {
                field: "run_id",
                reason: "event and rejection run IDs differ",
            });
        }
        if event.event_id != rejection.event_id {
            return Err(StoreError::InvalidInput {
                field: "event_id",
                reason: "event and rejection event IDs differ",
            });
        }

        let mut transaction = self.connection.begin().await?;
        sqlx::query(
            "INSERT INTO events \
             (event_id, run_id, event_time_ns, event_kind, payload_json) \
             VALUES (?1, ?2, ?3, ?4, ?5)",
        )
        .bind(event.event_id)
        .bind(event.run_id)
        .bind(event.event_time_ns)
        .bind(event.kind)
        .bind(event.payload_json)
        .execute(&mut *transaction)
        .await?;

        if behavior == AtomicAppend::FailAfterEvent {
            transaction.rollback().await?;
            return Err(StoreError::InjectedFailure);
        }

        sqlx::query(
            "INSERT INTO risk_decisions \
             (decision_id, run_id, event_id, ledger_id, decided_at_ns, outcome, reason_code, details_json) \
             VALUES (?1, ?2, ?3, ?4, ?5, 'rejected', ?6, ?7)",
        )
        .bind(rejection.decision_id)
        .bind(rejection.run_id)
        .bind(rejection.event_id)
        .bind(rejection.ledger_id.as_str())
        .bind(rejection.decided_at_ns)
        .bind(rejection.reason_code)
        .bind(rejection.details_json)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    /// Returns event and risk-decision row counts for one validated run ID.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] for an invalid run ID or a database error.
    pub async fn journal_counts(&mut self, run_id: &str) -> Result<JournalCounts, StoreError> {
        validate_id("run_id", run_id)?;
        let (events, risk_decisions) = sqlx::query_as::<_, (i64, i64)>(
            "SELECT \
                 (SELECT COUNT(*) FROM events WHERE run_id = ?1), \
                 (SELECT COUNT(*) FROM risk_decisions WHERE run_id = ?1)",
        )
        .bind(run_id)
        .fetch_one(&mut self.connection)
        .await?;
        Ok(JournalCounts {
            events,
            risk_decisions,
        })
    }

    /// Returns the exact integer Unix-nanosecond timestamp for an event.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] for an invalid event ID, a missing row, or a database error.
    pub async fn event_time_ns(&mut self, event_id: &str) -> Result<i64, StoreError> {
        validate_id("event_id", event_id)?;
        sqlx::query_scalar("SELECT event_time_ns FROM events WHERE event_id = ?1")
            .bind(event_id)
            .fetch_one(&mut self.connection)
            .await
            .map_err(StoreError::Database)
    }

    /// Reads the required durability PRAGMAs from the private connection.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] when SQLite cannot read a PRAGMA.
    pub async fn pragma_settings(&mut self) -> Result<PragmaSettings, StoreError> {
        read_pragmas(&mut self.connection).await
    }

    async fn verify_pragmas(&mut self) -> Result<(), StoreError> {
        let pragmas = read_pragmas(&mut self.connection).await?;
        if !pragmas.journal_mode.eq_ignore_ascii_case("wal") {
            return Err(StoreError::PragmaMismatch {
                pragma: "journal_mode",
            });
        }
        if pragmas.synchronous != 2 {
            return Err(StoreError::PragmaMismatch {
                pragma: "synchronous",
            });
        }
        if pragmas.foreign_keys != 1 {
            return Err(StoreError::PragmaMismatch {
                pragma: "foreign_keys",
            });
        }
        let busy_timeout: i64 = sqlx::query_scalar("PRAGMA busy_timeout")
            .fetch_one(&mut self.connection)
            .await?;
        if busy_timeout != 5_000 {
            return Err(StoreError::PragmaMismatch {
                pragma: "busy_timeout",
            });
        }
        Ok(())
    }
}

async fn read_pragmas(connection: &mut SqliteConnection) -> Result<PragmaSettings, StoreError> {
    let journal_mode = sqlx::query_scalar("PRAGMA journal_mode")
        .fetch_one(&mut *connection)
        .await?;
    let synchronous = sqlx::query_scalar("PRAGMA synchronous")
        .fetch_one(&mut *connection)
        .await?;
    let foreign_keys = sqlx::query_scalar("PRAGMA foreign_keys")
        .fetch_one(connection)
        .await?;
    Ok(PragmaSettings {
        journal_mode,
        synchronous,
        foreign_keys,
    })
}

fn resolve_database_path(path: &Path) -> Result<PathBuf, StoreError> {
    if path.as_os_str().is_empty() {
        return Err(StoreError::InvalidPath {
            reason: "path is empty",
        });
    }
    let file_name = path.file_name().ok_or(StoreError::InvalidPath {
        reason: "path must name a database file",
    })?;
    if path.is_dir() {
        return Err(StoreError::InvalidPath {
            reason: "path targets a directory",
        });
    }

    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|source| StoreError::Filesystem {
                operation: "resolving the current directory",
                source,
            })?
            .join(path)
    };
    let parent = absolute.parent().ok_or(StoreError::InvalidPath {
        reason: "path has no parent directory",
    })?;
    if parent == Path::new("/") {
        return Err(StoreError::InvalidPath {
            reason: "database parent cannot be the filesystem root",
        });
    }
    fs::create_dir_all(parent).map_err(|source| StoreError::Filesystem {
        operation: "creating the database directory",
        source,
    })?;
    let resolved_parent = parent
        .canonicalize()
        .map_err(|source| StoreError::Filesystem {
            operation: "resolving the database directory",
            source,
        })?;
    Ok(resolved_parent.join(file_name))
}

fn secure_parent_directory(path: &Path) -> Result<(), StoreError> {
    let parent = path.parent().ok_or(StoreError::InvalidPath {
        reason: "path has no parent directory",
    })?;
    #[cfg(unix)]
    fs::set_permissions(parent, Permissions::from_mode(0o700)).map_err(|source| {
        StoreError::Filesystem {
            operation: "securing the database directory",
            source,
        }
    })?;
    Ok(())
}

fn secure_database_file(path: &Path) -> Result<(), StoreError> {
    #[cfg(unix)]
    fs::set_permissions(path, Permissions::from_mode(0o600)).map_err(|source| {
        StoreError::Filesystem {
            operation: "securing the database file",
            source,
        }
    })?;
    Ok(())
}

fn validate_event(input: EventInput<'_>) -> Result<(), StoreError> {
    validate_id("run_id", input.run_id)?;
    validate_id("event_id", input.event_id)?;
    validate_code("event kind", input.kind)?;
    validate_timestamp("event_time_ns", input.event_time_ns)
}

fn validate_rejection(input: RiskRejectionInput<'_>) -> Result<(), StoreError> {
    validate_id("run_id", input.run_id)?;
    validate_id("decision_id", input.decision_id)?;
    validate_id("event_id", input.event_id)?;
    validate_code("reason code", input.reason_code)?;
    validate_timestamp("decided_at_ns", input.decided_at_ns)
}

fn validate_id(field: &'static str, value: &str) -> Result<(), StoreError> {
    if value.is_empty() || value.len() > MAX_ID_LENGTH {
        return Err(StoreError::InvalidInput {
            field,
            reason: "must contain between 1 and 128 bytes",
        });
    }
    if !value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'))
    {
        return Err(StoreError::InvalidInput {
            field,
            reason: "contains unsupported characters",
        });
    }
    Ok(())
}

fn validate_code(field: &'static str, value: &str) -> Result<(), StoreError> {
    if value.is_empty() || value.len() > MAX_CODE_LENGTH {
        return Err(StoreError::InvalidInput {
            field,
            reason: "must contain between 1 and 64 bytes",
        });
    }
    if !value
        .bytes()
        .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
    {
        return Err(StoreError::InvalidInput {
            field,
            reason: "must use lowercase ASCII letters, digits, or underscores",
        });
    }
    Ok(())
}

fn validate_timestamp(field: &'static str, value: i64) -> Result<(), StoreError> {
    if value < 0 {
        return Err(StoreError::InvalidInput {
            field,
            reason: "must be a non-negative Unix nanosecond timestamp",
        });
    }
    Ok(())
}
