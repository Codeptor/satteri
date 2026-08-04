//! The daemon's sole SQLite writer and engine-admission boundary.

use thiserror::Error;
use trench_core::domain::LedgerId;
use trench_core::engine::{EngineError, EngineOutcome, EventAdmission};
use trench_storage::sqlite::{
    EngineJournalCounts, EngineJournalHistory, EventInput, LedgerId as StoreLedgerId, RunInput,
    SqliteStore, StoreError,
};

/// Owned normalized source evidence submitted to the authority loop.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceEvent {
    run_id: String,
    event_id: String,
    event_time_ns: i64,
    kind: String,
    payload_json: String,
}

impl SourceEvent {
    /// Creates one immutable normalized source-event envelope.
    #[must_use]
    pub fn new(
        run_id: impl Into<String>,
        event_id: impl Into<String>,
        event_time_ns: i64,
        kind: impl Into<String>,
        payload_json: impl Into<String>,
    ) -> Self {
        Self {
            run_id: run_id.into(),
            event_id: event_id.into(),
            event_time_ns,
            kind: kind.into(),
            payload_json: payload_json.into(),
        }
    }

    fn input(&self) -> EventInput<'_> {
        EventInput {
            run_id: &self.run_id,
            event_id: &self.event_id,
            event_time_ns: self.event_time_ns,
            kind: &self.kind,
            payload_json: &self.payload_json,
        }
    }
}

/// The only component that owns a mutable SQLite connection.
pub struct EngineWriter {
    store: SqliteStore,
    run_id: String,
}

impl EngineWriter {
    /// Opens the WAL store and records the daemon-owned run before admission.
    ///
    /// No network task receives the underlying store or a database handle.
    #[cfg(test)]
    pub async fn open(
        path: impl AsRef<std::path::Path>,
        run_id: impl Into<String>,
        started_at_ns: i64,
    ) -> Result<Self, WriterError> {
        let run_id = run_id.into();
        let mut store = SqliteStore::open(path).await?;
        if store.has_engine_history().await? {
            return Err(WriterError::PriorHistory);
        }
        store
            .create_run(RunInput {
                run_id: &run_id,
                started_at_ns,
            })
            .await?;
        Ok(Self { store, run_id })
    }

    /// Inspects the immutable SQLite journal before any daemon run is created.
    ///
    /// Startup must reconstruct the executable state from independently
    /// committed Parquet source/recovery evidence and compare it to this
    /// history before calling [`Self::open_after_reconstruction`].
    pub async fn inspect_history(
        path: impl AsRef<std::path::Path>,
    ) -> Result<EngineJournalHistory, WriterError> {
        let mut store = SqliteStore::open(path).await?;
        store.engine_journal_history().await.map_err(Into::into)
    }

    /// Opens a new daemon-owned run only after the supplied journal witness
    /// was deterministically reconstructed by the authority path.
    ///
    /// The database is read again before mutation so a concurrent or changed
    /// history invalidates the restoration rather than allowing a stale proof
    /// to append fresh source facts.
    pub async fn open_after_reconstruction(
        path: impl AsRef<std::path::Path>,
        run_id: impl Into<String>,
        started_at_ns: i64,
        reconstructed: &EngineJournalHistory,
    ) -> Result<Self, WriterError> {
        let run_id = run_id.into();
        let mut store = SqliteStore::open(path).await?;
        if store.engine_journal_history().await? != *reconstructed {
            return Err(WriterError::HistoryChanged);
        }
        store
            .create_run(RunInput {
                run_id: &run_id,
                started_at_ns,
            })
            .await?;
        Ok(Self { store, run_id })
    }

    /// Returns the durable run identity owned by this writer.
    #[must_use]
    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    /// Admits, applies, and appends exactly one pure engine transition.
    ///
    /// The closure runs only after the durable admission is read. Its successor
    /// state is returned only after the matching atomic append succeeds, so the
    /// authority loop cannot advance in-memory state past durable evidence.
    pub async fn admit_apply_append<F>(
        &mut self,
        ledger: LedgerId,
        event: &SourceEvent,
        apply: F,
    ) -> Result<EngineOutcome, WriterError>
    where
        F: FnOnce(EventAdmission) -> Result<EngineOutcome, EngineError>,
    {
        if event.run_id != self.run_id {
            return Err(WriterError::RunMismatch);
        }
        let permit = self
            .store
            .engine_admission(event.input(), store_ledger(ledger))
            .await?;
        let outcome = apply(permit.core_admission())?;
        let outcome_ledger = outcome.state().ledger().ledger_id();
        if outcome_ledger != ledger {
            return Err(WriterError::LedgerMismatch {
                expected: ledger,
                actual: outcome_ledger,
            });
        }
        self.store
            .append_engine_outcome(permit, event.input(), &outcome)
            .await?;
        Ok(outcome)
    }

    /// Returns current durable counts for this writer's run.
    pub async fn journal_counts(&mut self) -> Result<EngineJournalCounts, WriterError> {
        self.store
            .engine_journal_counts(&self.run_id)
            .await
            .map_err(Into::into)
    }
}

fn store_ledger(ledger: LedgerId) -> StoreLedgerId {
    match ledger {
        LedgerId::RulesOnly => StoreLedgerId::RulesOnly,
        LedgerId::MlChampion => StoreLedgerId::MlChampion,
    }
}

/// A writer-opening, admission, pure-engine, or atomic-append failure.
#[derive(Debug, Error)]
pub enum WriterError {
    /// Existing journal checkpoints require a complete replay-state restorer.
    #[cfg(test)]
    #[error("prior engine history requires deterministic state reconstruction")]
    PriorHistory,
    /// Immutable SQLite evidence changed after authority reconstruction and
    /// before the new writer run could be created.
    #[error("historical engine journal changed during deterministic reconstruction")]
    HistoryChanged,
    /// The submitted event was not scoped to this durable run.
    #[error("source event belongs to a different daemon run")]
    RunMismatch,
    /// The pure outcome claimed a different ledger than the admitted one.
    #[error("engine outcome ledger differs from the admitted ledger")]
    LedgerMismatch {
        /// Ledger admitted before pure evaluation.
        expected: LedgerId,
        /// Ledger carried by the pure successor state.
        actual: LedgerId,
    },
    /// Durable storage rejected or failed the write path.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// The pure core engine rejected the explicitly supplied transition.
    #[error(transparent)]
    Engine(#[from] EngineError),
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

    use super::{EngineWriter, SourceEvent};
    use trench_core::broker::{BrokerConfig, BrokerRunContext, PaperBroker};
    use trench_core::domain::{EventId, LedgerId, RunId, Usdc};
    use trench_core::engine::{
        Engine, EngineContext, EngineEvent, EngineState, SnapshotBindings, StrategyFingerprints,
    };
    use trench_core::event::{DurationNs, TimestampNs};
    use trench_core::ledger::LedgerState;
    use trench_core::universe::UniverseSelector;

    fn timestamp(value: i64) -> TimestampNs {
        TimestampNs::new(i128::from(value)).expect("fixture timestamp")
    }

    fn initial_state(at: TimestampNs) -> EngineState {
        let ledger = LedgerState::new(LedgerId::RulesOnly, at).expect("fixture ledger");
        let broker = PaperBroker::new(
            BrokerConfig::new(
                Usdc::new(rust_decimal::Decimal::ONE).expect("fixture USDC"),
                DurationNs::new(1_000_000_000).expect("fixture duration"),
            )
            .expect("fixture broker config"),
            BrokerRunContext::new(
                RunId::new("run-writer-test").expect("fixture run"),
                "a".repeat(64),
                "b".repeat(64),
            )
            .expect("fixture broker context"),
            at,
        );
        EngineState::new(ledger, broker, BTreeMap::new())
    }

    fn context(admission: trench_core::engine::EventAdmission) -> EngineContext {
        let at = timestamp(0);
        let universe = UniverseSelector::select(at, Vec::new()).expect("empty universe");
        let activation = UniverseSelector::activate(&universe, None, at).expect("activation");
        EngineContext::new(
            admission,
            SnapshotBindings::new(BTreeMap::new(), activation),
            StrategyFingerprints::new("rules", "ml"),
        )
    }

    #[tokio::test]
    async fn only_the_writer_can_admit_apply_and_append_an_engine_event() {
        let directory = tempfile::tempdir().expect("fixture directory");
        #[cfg(unix)]
        std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700))
            .expect("private fixture directory");
        let database = directory.path().join("trench.sqlite");
        let mut writer = EngineWriter::open(&database, "run-writer-test", 0)
            .await
            .expect("writer opens");
        let source = SourceEvent::new(
            writer.run_id(),
            "advance-time-1",
            1,
            "advance_time",
            r#"{"at_ns":1}"#,
        );

        let outcome = writer
            .admit_apply_append(LedgerId::RulesOnly, &source, |admission| {
                Engine::apply(
                    EngineEvent::AdvanceTime {
                        event_id: EventId::new("advance-time-1").expect("fixture event"),
                        at: timestamp(1),
                    },
                    initial_state(timestamp(0)),
                    &context(admission),
                )
            })
            .await
            .expect("atomic engine append");
        assert!(!outcome.is_duplicate_noop());
        let counts = writer.journal_counts().await.expect("journal counts");
        assert_eq!(counts.events, 1);
        assert_eq!(counts.admissions, 1);
        assert_eq!(counts.checkpoints, 1);

        let duplicate = writer
            .admit_apply_append(LedgerId::RulesOnly, &source, |admission| {
                Engine::apply(
                    EngineEvent::AdvanceTime {
                        event_id: EventId::new("advance-time-1").expect("fixture event"),
                        at: timestamp(1),
                    },
                    initial_state(timestamp(0)),
                    &context(admission),
                )
            })
            .await
            .expect("duplicate acknowledgement");
        assert!(duplicate.is_duplicate_noop());
        assert_eq!(
            writer
                .journal_counts()
                .await
                .expect("journal counts")
                .duplicate_attempts,
            1
        );
        drop(writer);
        assert!(matches!(
            EngineWriter::open(&database, "run-writer-next", 2).await,
            Err(super::WriterError::PriorHistory)
        ));
    }
}
