//! Single-writer SQLite journal for durable paper-trading transitions.

use std::fs;
use std::io::ErrorKind;
use std::path::{Component, Path, PathBuf};
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
const MAX_ENGINE_RECORDS: usize = 1_024;

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
    /// The supplied event identity conflicts with immutable source evidence.
    #[error("existing source event does not match this engine batch")]
    EventConflict,
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
    /// Authoritative normalized source/venue UTC Unix timestamp in nanoseconds.
    ///
    /// This is deliberately distinct from the engine processing/as-of boundary
    /// stored by the companion checkpoint. Both are bound to the same immutable
    /// causal event ID by the admission and foreign-key contract.
    pub event_time_ns: i64,
    /// Stable machine-readable event kind.
    pub kind: &'a str,
    /// JSON event payload.
    pub payload_json: &'a str,
}

/// Stable category for one persistence-ready engine record.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum EngineRecordKind {
    /// Immutable source, universe, feature, or executable-book commitment.
    Snapshot,
    /// Un-sized strategy candidate or rejection evidence.
    Signal,
    /// Strategy cost-acceptance intent.
    Intent,
    /// Sealed-risk quote or consumption evidence.
    Risk,
    /// Paper broker order lifecycle evidence.
    Order,
    /// Actual primary-taker fill evidence.
    Fill,
    /// Isolated ledger state transition evidence.
    Ledger,
    /// Breaker or terminal-state transition evidence.
    Breaker,
}

impl EngineRecordKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Snapshot => "snapshot",
            Self::Signal => "signal",
            Self::Intent => "intent",
            Self::Risk => "risk",
            Self::Order => "order",
            Self::Fill => "fill",
            Self::Ledger => "ledger",
            Self::Breaker => "breaker",
        }
    }
}

/// One ordered immutable engine record serialized by the paper daemon.
#[derive(Clone, Copy, Debug)]
pub(crate) struct EngineRecordInput<'a> {
    kind: EngineRecordKind,
    payload_json: &'a str,
}

impl<'a> EngineRecordInput<'a> {
    /// Creates one typed, JSON-encoded engine record.
    #[must_use]
    pub(crate) const fn new(kind: EngineRecordKind, payload_json: &'a str) -> Self {
        Self { kind, payload_json }
    }
}

/// Deterministic replay checkpoint committed with an engine batch.
#[derive(Clone, Copy, Debug)]
pub(crate) struct EngineCheckpointInput<'a> {
    /// Stable checkpoint identifier.
    pub checkpoint_id: &'a str,
    /// Independently accounted paper ledger whose state this checkpoint captures.
    pub ledger_id: LedgerId,
    /// Explicit UTC Unix timestamp in nanoseconds.
    pub at_ns: i64,
    /// Canonical digest of the complete post-batch engine state.
    pub state_digest: &'a str,
    /// Canonical JSON serialization of the complete post-batch engine state.
    pub state_json: &'a str,
}

/// One source event, its ordered durable evidence, and successor-state checkpoint.
#[derive(Clone, Copy, Debug)]
pub(crate) struct EngineBatchInput<'a> {
    /// Owning normalized source event.
    pub event: EventInput<'a>,
    /// Independently accounted paper ledger that owns this durable evidence.
    pub ledger_id: LedgerId,
    /// Ordered source/signal/risk/broker/ledger/breaker evidence.
    pub records: &'a [EngineRecordInput<'a>],
    /// Exactly one complete successor-state checkpoint.
    pub checkpoint: EngineCheckpointInput<'a>,
}

/// Deterministic engine-batch transaction behavior used by recovery tests.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AtomicEngineAppend {
    /// Commit the complete source event, evidence sequence, and checkpoint.
    Commit,
    /// Roll back immediately after source-event insertion.
    FailAfterEvent,
    /// Roll back after all evidence records and before the checkpoint.
    FailAfterRecords,
}

/// Result of admitting one engine batch at the sole SQLite writer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EngineAppendOutcome {
    /// A new source event and every accompanying record committed atomically.
    Committed {
        /// Number of ordered evidence records committed with the event.
        record_count: usize,
    },
    /// An already-admitted event was ignored and only its audit counter advanced.
    Duplicate {
        /// Total durable duplicate attempts after this idempotent application.
        duplicate_attempts: i64,
    },
}

/// Durable admission status obtained before calling the pure engine.
///
/// The sole writer reads this for the exact `(run, ledger, event)` key and
/// maps it to `trench_core::EventAdmission` before invoking `Engine::apply`.
/// A duplicate must be forwarded as a core no-op, then acknowledged through
/// [`SqliteStore::append_engine_outcome`] so its audit counter advances.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EngineAdmission {
    /// No committed engine batch exists for this source event and ledger.
    New,
    /// A committed engine batch exists; the stored count excludes this attempt.
    Duplicate {
        /// Existing durable duplicate-attempt count before acknowledgement.
        duplicate_attempts: i64,
    },
}

/// Opaque durable-admission proof binding one core outcome to one writer key.
///
/// Only [`SqliteStore::engine_admission`] can create this proof. The daemon
/// maps [`Self::admission`] to `trench_core::engine::EventAdmission` before
/// applying the pure engine, then presents the exact proof to
/// [`SqliteStore::append_engine_outcome`].
#[derive(Debug, Eq, PartialEq)]
pub struct EngineAdmissionPermit {
    admission: EngineAdmission,
    run_id: String,
    ledger_id: LedgerId,
    event_id: String,
    event_time_ns: i64,
    kind: String,
    payload_json: String,
}

impl EngineAdmissionPermit {
    /// Returns the durable pre-application admission result.
    #[must_use]
    pub const fn admission(&self) -> EngineAdmission {
        self.admission
    }

    /// Maps the durable result to the pure core's explicit admission input.
    #[must_use]
    pub const fn core_admission(&self) -> trench_core::engine::EventAdmission {
        match self.admission {
            EngineAdmission::New => trench_core::engine::EventAdmission::New,
            EngineAdmission::Duplicate { .. } => trench_core::engine::EventAdmission::Duplicate,
        }
    }

    fn matches(&self, event: EventInput<'_>, ledger_id: LedgerId) -> bool {
        self.run_id == event.run_id
            && self.ledger_id == ledger_id
            && self.event_id == event.event_id
            && self.event_time_ns == event.event_time_ns
            && self.kind == event.kind
            && self.payload_json == event.payload_json
    }
}

/// Durable row counts for the engine-specific atomic journal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EngineJournalCounts {
    /// Immutable normalized source events, stored once per run/event.
    pub events: i64,
    /// Ledger-scoped admitted engine batches.
    pub admissions: i64,
    /// Ordered source/signal/risk/broker/ledger/breaker records.
    pub records: i64,
    /// Complete successor-state checkpoints.
    pub checkpoints: i64,
    /// Reapplied event attempts retained for audit.
    pub duplicate_attempts: i64,
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

        let options = SqliteConnectOptions::new()
            .filename(&path)
            .create_if_missing(true)
            .foreign_keys(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Full)
            .busy_timeout(BUSY_TIMEOUT);
        let mut connection = SqliteConnection::connect_with(&options).await?;

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

    /// Reads ledger-scoped source-event admission before pure engine evaluation.
    ///
    /// `trenchd` is the sole writer, so it must call this immediately before
    /// mapping the result to `trench_core::EventAdmission` and applying the
    /// pure engine. It must then call [`Self::append_engine_outcome`] with the
    /// same immutable source event and ledger; that method rechecks admission
    /// and increments duplicate audit state atomically when appropriate.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] for an invalid identity or a database error.
    pub async fn engine_admission(
        &mut self,
        event: EventInput<'_>,
        ledger_id: LedgerId,
    ) -> Result<EngineAdmissionPermit, StoreError> {
        validate_event(event)?;
        validate_json("event payload", event.payload_json)?;
        let existing_event = sqlx::query_as::<_, (String, i64, String, String)>(
            "SELECT run_id, event_time_ns, event_kind, payload_json \
             FROM events WHERE event_id = ?1",
        )
        .bind(event.event_id)
        .fetch_optional(&mut self.connection)
        .await?;
        match existing_event {
            Some((run_id, event_time_ns, kind, payload_json))
                if run_id == event.run_id
                    && event_time_ns == event.event_time_ns
                    && kind == event.kind
                    && payload_json == event.payload_json => {}
            Some(_) => return Err(StoreError::EventConflict),
            None => {
                return Ok(EngineAdmissionPermit {
                    admission: EngineAdmission::New,
                    run_id: event.run_id.to_owned(),
                    ledger_id,
                    event_id: event.event_id.to_owned(),
                    event_time_ns: event.event_time_ns,
                    kind: event.kind.to_owned(),
                    payload_json: event.payload_json.to_owned(),
                });
            }
        }
        let duplicate_attempts = sqlx::query_scalar::<_, i64>(
            "SELECT duplicate_attempts \
             FROM engine_event_admissions \
             WHERE run_id = ?1 AND ledger_id = ?2 AND event_id = ?3",
        )
        .bind(event.run_id)
        .bind(ledger_id.as_str())
        .bind(event.event_id)
        .fetch_optional(&mut self.connection)
        .await?;
        Ok(EngineAdmissionPermit {
            admission: match duplicate_attempts {
                Some(duplicate_attempts) => EngineAdmission::Duplicate { duplicate_attempts },
                None => EngineAdmission::New,
            },
            run_id: event.run_id.to_owned(),
            ledger_id,
            event_id: event.event_id.to_owned(),
            event_time_ns: event.event_time_ns,
            kind: event.kind.to_owned(),
            payload_json: event.payload_json.to_owned(),
        })
    }

    /// Atomically persists a new engine batch or acknowledges a known duplicate.
    ///
    /// The raw normalized source event is immutable and stored exactly once per
    /// `(run, event)`. Every engine admission, ordered evidence record, and
    /// checkpoint is independently scoped by `(run, ledger, event)`, allowing
    /// rules-only and ML-champion ledgers to evaluate the same market event.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] for invalid inputs, a conflicting source event,
    /// the requested recovery failpoint, or a database error. No partial batch
    /// is durable on error.
    pub(crate) async fn append_engine_batch(
        &mut self,
        batch: EngineBatchInput<'_>,
        behavior: AtomicEngineAppend,
    ) -> Result<EngineAppendOutcome, StoreError> {
        validate_engine_batch(batch)?;

        let mut transaction = self.connection.begin().await?;
        let existing_event = sqlx::query_as::<_, (String, i64, String, String)>(
            "SELECT run_id, event_time_ns, event_kind, payload_json \
             FROM events WHERE event_id = ?1",
        )
        .bind(batch.event.event_id)
        .fetch_optional(&mut *transaction)
        .await?;
        match existing_event {
            Some((run_id, event_time_ns, kind, payload_json))
                if run_id == batch.event.run_id
                    && event_time_ns == batch.event.event_time_ns
                    && kind == batch.event.kind
                    && payload_json == batch.event.payload_json => {}
            Some(_) => return Err(StoreError::EventConflict),
            None => {
                sqlx::query(
                    "INSERT INTO events \
                     (event_id, run_id, event_time_ns, event_kind, payload_json) \
                     VALUES (?1, ?2, ?3, ?4, ?5)",
                )
                .bind(batch.event.event_id)
                .bind(batch.event.run_id)
                .bind(batch.event.event_time_ns)
                .bind(batch.event.kind)
                .bind(batch.event.payload_json)
                .execute(&mut *transaction)
                .await?;
            }
        }

        if behavior == AtomicEngineAppend::FailAfterEvent {
            transaction.rollback().await?;
            return Err(StoreError::InjectedFailure);
        }

        let duplicate_attempts = sqlx::query_scalar::<_, i64>(
            "SELECT duplicate_attempts \
             FROM engine_event_admissions \
             WHERE run_id = ?1 AND ledger_id = ?2 AND event_id = ?3",
        )
        .bind(batch.event.run_id)
        .bind(batch.ledger_id.as_str())
        .bind(batch.event.event_id)
        .fetch_optional(&mut *transaction)
        .await?;
        if duplicate_attempts.is_some() {
            sqlx::query(
                "UPDATE engine_event_admissions \
                 SET duplicate_attempts = duplicate_attempts + 1 \
                 WHERE run_id = ?1 AND ledger_id = ?2 AND event_id = ?3",
            )
            .bind(batch.event.run_id)
            .bind(batch.ledger_id.as_str())
            .bind(batch.event.event_id)
            .execute(&mut *transaction)
            .await?;
            let duplicate_attempts = sqlx::query_scalar::<_, i64>(
                "SELECT duplicate_attempts \
                 FROM engine_event_admissions \
                 WHERE run_id = ?1 AND ledger_id = ?2 AND event_id = ?3",
            )
            .bind(batch.event.run_id)
            .bind(batch.ledger_id.as_str())
            .bind(batch.event.event_id)
            .fetch_one(&mut *transaction)
            .await?;
            transaction.commit().await?;
            return Ok(EngineAppendOutcome::Duplicate { duplicate_attempts });
        }

        sqlx::query(
            "INSERT INTO engine_event_admissions \
             (run_id, ledger_id, event_id, admitted_at_ns) \
             VALUES (?1, ?2, ?3, ?4)",
        )
        .bind(batch.event.run_id)
        .bind(batch.ledger_id.as_str())
        .bind(batch.event.event_id)
        .bind(batch.checkpoint.at_ns)
        .execute(&mut *transaction)
        .await?;

        for (sequence, record) in batch.records.iter().enumerate() {
            let sequence = i64::try_from(sequence).map_err(|_| StoreError::InvalidInput {
                field: "record sequence",
                reason: "exceeds SQLite integer range",
            })?;
            sqlx::query(
                "INSERT INTO engine_batch_records \
                 (run_id, ledger_id, event_id, sequence, record_kind, payload_json) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            )
            .bind(batch.event.run_id)
            .bind(batch.ledger_id.as_str())
            .bind(batch.event.event_id)
            .bind(sequence)
            .bind(record.kind.as_str())
            .bind(record.payload_json)
            .execute(&mut *transaction)
            .await?;
        }

        if behavior == AtomicEngineAppend::FailAfterRecords {
            transaction.rollback().await?;
            return Err(StoreError::InjectedFailure);
        }

        sqlx::query(
            "INSERT INTO engine_checkpoints \
             (checkpoint_id, run_id, ledger_id, event_id, as_of_time_ns, state_digest, state_json) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        )
        .bind(batch.checkpoint.checkpoint_id)
        .bind(batch.event.run_id)
        .bind(batch.ledger_id.as_str())
        .bind(batch.event.event_id)
        .bind(batch.checkpoint.at_ns)
        .bind(batch.checkpoint.state_digest)
        .bind(batch.checkpoint.state_json)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(EngineAppendOutcome::Committed {
            record_count: batch.records.len(),
        })
    }

    /// Persists a real core-engine outcome through the sole atomic journal path.
    ///
    /// This is the production writer boundary. It accepts no caller-authored
    /// record JSON: the pure core supplies a one-way, secret-free persistence
    /// projection whose event identity is verified against `event` before any
    /// durable mutation is attempted. `event.event_time_ns` retains the raw
    /// source/venue time, while the projection checkpoint retains the engine
    /// processing/as-of time; their intentional two-time relationship is
    /// causally bound by this exact admitted event ID.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] for a causality mismatch or any atomic batch
    /// persistence error.
    pub async fn append_engine_outcome(
        &mut self,
        permit: EngineAdmissionPermit,
        event: EventInput<'_>,
        outcome: &trench_core::engine::EngineOutcome,
    ) -> Result<EngineAppendOutcome, StoreError> {
        self.append_engine_outcome_with_behavior(permit, event, outcome, AtomicEngineAppend::Commit)
            .await
    }

    /// Internal outcome path with a deterministic transaction failpoint.
    ///
    /// Production always uses [`Self::append_engine_outcome`]; crate-local
    /// recovery tests use this to prove that a real core projection cannot
    /// leave partial source, evidence, or checkpoint rows behind.
    pub(crate) async fn append_engine_outcome_with_behavior(
        &mut self,
        permit: EngineAdmissionPermit,
        event: EventInput<'_>,
        outcome: &trench_core::engine::EngineOutcome,
        behavior: AtomicEngineAppend,
    ) -> Result<EngineAppendOutcome, StoreError> {
        let projection = outcome.persistence_batch();
        if projection.event_id() != event.event_id {
            return Err(StoreError::InvalidInput {
                field: "event_id",
                reason: "event and engine outcome causality IDs differ",
            });
        }
        let ledger_id = match outcome.state().ledger().ledger_id() {
            trench_core::domain::LedgerId::RulesOnly => LedgerId::RulesOnly,
            trench_core::domain::LedgerId::MlChampion => LedgerId::MlChampion,
        };
        if !permit.matches(event, ledger_id) {
            return Err(StoreError::InvalidInput {
                field: "engine admission",
                reason: "permit does not match this outcome key",
            });
        }
        let current_permit = self.engine_admission(event, ledger_id).await?;
        if current_permit.admission() != permit.admission() {
            return Err(StoreError::InvalidInput {
                field: "engine admission",
                reason: "permit is stale relative to durable admission",
            });
        }
        match (permit.admission(), outcome.is_duplicate_noop()) {
            (EngineAdmission::New, true) => {
                return Err(StoreError::InvalidInput {
                    field: "engine admission",
                    reason: "new admission cannot persist a duplicate no-op",
                });
            }
            (EngineAdmission::Duplicate { .. }, false) => {
                return Err(StoreError::InvalidInput {
                    field: "engine admission",
                    reason: "duplicate admission must persist the duplicate no-op",
                });
            }
            _ => {}
        }
        let records = projection
            .records()
            .iter()
            .map(|record| {
                EngineRecordInput::new(
                    match record.kind() {
                        trench_core::engine::EnginePersistenceKind::Snapshot => {
                            EngineRecordKind::Snapshot
                        }
                        trench_core::engine::EnginePersistenceKind::Signal => {
                            EngineRecordKind::Signal
                        }
                        trench_core::engine::EnginePersistenceKind::Intent => {
                            EngineRecordKind::Intent
                        }
                        trench_core::engine::EnginePersistenceKind::Risk => EngineRecordKind::Risk,
                        trench_core::engine::EnginePersistenceKind::Order => {
                            EngineRecordKind::Order
                        }
                        trench_core::engine::EnginePersistenceKind::Fill => EngineRecordKind::Fill,
                        trench_core::engine::EnginePersistenceKind::Ledger => {
                            EngineRecordKind::Ledger
                        }
                        trench_core::engine::EnginePersistenceKind::Breaker => {
                            EngineRecordKind::Breaker
                        }
                    },
                    record.payload_json(),
                )
            })
            .collect::<Vec<_>>();
        let checkpoint = projection.checkpoint();
        self.append_engine_batch(
            EngineBatchInput {
                event,
                ledger_id,
                records: &records,
                checkpoint: EngineCheckpointInput {
                    checkpoint_id: checkpoint.checkpoint_id(),
                    ledger_id,
                    at_ns: checkpoint.at().value(),
                    state_digest: checkpoint.state_digest(),
                    state_json: checkpoint.state_json(),
                },
            },
            behavior,
        )
        .await
    }

    /// Returns durable raw-event and ledger-scoped engine-journal counts.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError`] for an invalid run ID or a database error.
    pub async fn engine_journal_counts(
        &mut self,
        run_id: &str,
    ) -> Result<EngineJournalCounts, StoreError> {
        validate_id("run_id", run_id)?;
        let (events, admissions, records, checkpoints, duplicate_attempts) =
            sqlx::query_as::<_, (i64, i64, i64, i64, i64)>(
                "SELECT \
                     (SELECT COUNT(*) FROM events WHERE run_id = ?1), \
                     (SELECT COUNT(*) FROM engine_event_admissions WHERE run_id = ?1), \
                     (SELECT COUNT(*) FROM engine_batch_records WHERE run_id = ?1), \
                     (SELECT COUNT(*) FROM engine_checkpoints WHERE run_id = ?1), \
                     COALESCE((SELECT SUM(duplicate_attempts) \
                               FROM engine_event_admissions WHERE run_id = ?1), 0)",
            )
            .bind(run_id)
            .fetch_one(&mut self.connection)
            .await?;
        Ok(EngineJournalCounts {
            events,
            admissions,
            records,
            checkpoints,
            duplicate_attempts,
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
    path.file_name().ok_or(StoreError::InvalidPath {
        reason: "path must name a database file",
    })?;
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
    reject_symlink_components(&absolute)?;
    ensure_private_directory(parent)?;
    ensure_private_database_file(&absolute)?;
    Ok(absolute)
}

fn reject_symlink_components(path: &Path) -> Result<(), StoreError> {
    let mut current = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => current.push(prefix.as_os_str()),
            Component::RootDir => current.push(component.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                return Err(StoreError::InvalidPath {
                    reason: "path must not contain parent-directory traversal",
                });
            }
            Component::Normal(segment) => {
                current.push(segment);
                match fs::symlink_metadata(&current) {
                    Ok(metadata) if metadata.file_type().is_symlink() => {
                        return Err(StoreError::InvalidPath {
                            reason: "database path must not contain symlinks",
                        });
                    }
                    Ok(_) => {}
                    Err(source) if source.kind() == ErrorKind::NotFound => {}
                    Err(source) => {
                        return Err(StoreError::Filesystem {
                            operation: "inspecting the database path",
                            source,
                        });
                    }
                }
            }
        }
    }
    Ok(())
}

fn ensure_private_directory(path: &Path) -> Result<(), StoreError> {
    let mut missing = Vec::new();
    let mut current = path;
    loop {
        match fs::symlink_metadata(current) {
            Ok(metadata) => {
                if !metadata.file_type().is_dir() {
                    return Err(StoreError::InvalidPath {
                        reason: "database parent must be a directory",
                    });
                }
                break;
            }
            Err(source) if source.kind() == ErrorKind::NotFound => {
                missing.push(current.to_path_buf());
                current = current.parent().ok_or(StoreError::InvalidPath {
                    reason: "path has no parent directory",
                })?;
            }
            Err(source) => {
                return Err(StoreError::Filesystem {
                    operation: "inspecting the database directory",
                    source,
                });
            }
        }
    }

    for directory in missing.iter().rev() {
        match fs::create_dir(directory) {
            Ok(()) => {
                #[cfg(unix)]
                fs::set_permissions(directory, Permissions::from_mode(0o700)).map_err(
                    |source| StoreError::Filesystem {
                        operation: "securing a newly created database directory",
                        source,
                    },
                )?;
            }
            Err(source) if source.kind() == ErrorKind::AlreadyExists => {}
            Err(source) => {
                return Err(StoreError::Filesystem {
                    operation: "creating the database directory",
                    source,
                });
            }
        }
    }

    let metadata = fs::symlink_metadata(path).map_err(|source| StoreError::Filesystem {
        operation: "inspecting the database directory",
        source,
    })?;
    if !metadata.file_type().is_dir() || metadata.file_type().is_symlink() {
        return Err(StoreError::InvalidPath {
            reason: "database parent must be a non-symlink directory",
        });
    }
    #[cfg(unix)]
    if metadata.permissions().mode() & 0o777 != 0o700 {
        return Err(StoreError::InvalidPath {
            reason: "database parent must have mode 0700",
        });
    }
    Ok(())
}

fn ensure_private_database_file(path: &Path) -> Result<(), StoreError> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(source) if source.kind() == ErrorKind::NotFound => {
            fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(path)
                .map_err(|source| StoreError::Filesystem {
                    operation: "creating the database file",
                    source,
                })?;
            #[cfg(unix)]
            fs::set_permissions(path, Permissions::from_mode(0o600)).map_err(|source| {
                StoreError::Filesystem {
                    operation: "securing a newly created database file",
                    source,
                }
            })?;
            fs::symlink_metadata(path).map_err(|source| StoreError::Filesystem {
                operation: "inspecting the database file",
                source,
            })?
        }
        Err(source) => {
            return Err(StoreError::Filesystem {
                operation: "inspecting the database file",
                source,
            });
        }
    };

    if !metadata.file_type().is_file() || metadata.file_type().is_symlink() {
        return Err(StoreError::InvalidPath {
            reason: "database path must be a non-symlink regular file",
        });
    }
    #[cfg(unix)]
    if metadata.permissions().mode() & 0o777 != 0o600 {
        return Err(StoreError::InvalidPath {
            reason: "database file must have mode 0600",
        });
    }
    Ok(())
}

fn validate_event(input: EventInput<'_>) -> Result<(), StoreError> {
    validate_id("run_id", input.run_id)?;
    validate_id("event_id", input.event_id)?;
    validate_code("event kind", input.kind)?;
    validate_timestamp("event_time_ns", input.event_time_ns)
}

fn validate_engine_batch(input: EngineBatchInput<'_>) -> Result<(), StoreError> {
    validate_event(input.event)?;
    validate_json("event payload", input.event.payload_json)?;
    if input.records.is_empty() || input.records.len() > MAX_ENGINE_RECORDS {
        return Err(StoreError::InvalidInput {
            field: "engine records",
            reason: "must contain between 1 and 1024 records",
        });
    }
    for record in input.records {
        validate_json("engine record payload", record.payload_json)?;
    }
    validate_id("checkpoint_id", input.checkpoint.checkpoint_id)?;
    validate_timestamp("checkpoint as_of_time_ns", input.checkpoint.at_ns)?;
    validate_id("checkpoint state_digest", input.checkpoint.state_digest)?;
    validate_json("checkpoint state_json", input.checkpoint.state_json)?;
    if input.ledger_id != input.checkpoint.ledger_id {
        return Err(StoreError::InvalidInput {
            field: "ledger_id",
            reason: "batch and checkpoint ledgers differ",
        });
    }
    Ok(())
}

fn validate_json(field: &'static str, value: &str) -> Result<(), StoreError> {
    serde_json::from_str::<serde_json::Value>(value).map_err(|_| StoreError::InvalidInput {
        field,
        reason: "must be valid JSON",
    })?;
    Ok(())
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
