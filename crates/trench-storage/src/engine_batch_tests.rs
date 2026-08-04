use std::collections::BTreeMap;

use crate::sqlite::{
    AtomicEngineAppend, EngineAdmission, EngineAppendOutcome, EngineBatchInput,
    EngineCheckpointInput, EngineJournalCounts, EngineRecordInput, EngineRecordKind, EventInput,
    LedgerId, RunInput, SqliteStore, StoreError,
};
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::{Connection, SqliteConnection};
use trench_core::broker::{BrokerConfig, BrokerRunContext, PaperBroker};
use trench_core::domain::{EventId, LedgerId as CoreLedgerId, Market, RunId, Usdc};
use trench_core::engine::test_support::critical_lifecycle_fixtures;
use trench_core::engine::{
    Engine, EngineContext, EngineEvent, EngineOutcome, EngineState, EventAdmission,
    SnapshotBindings, StrategyFingerprints,
};
use trench_core::event::{DurationNs, TimestampNs};
use trench_core::ledger::LedgerState;
use trench_core::universe::UniverseSelector;

const RUN_ID: &str = "engine-run-2026-08-04";
const EVENT_ID: &str = "engine-event-0001";
const EVENT_TIME_NS: i64 = 1_785_715_200_123_456_789;
const RECORDS: [EngineRecordInput<'static>; 8] = [
    EngineRecordInput::new(EngineRecordKind::Snapshot, r#"{"book":"b3:book"}"#),
    EngineRecordInput::new(EngineRecordKind::Signal, r#"{"signal":"b3:signal"}"#),
    EngineRecordInput::new(EngineRecordKind::Intent, r#"{"intent":"b3:intent"}"#),
    EngineRecordInput::new(EngineRecordKind::Risk, r#"{"quote":"b3:quote"}"#),
    EngineRecordInput::new(EngineRecordKind::Order, r#"{"order":"b3:order"}"#),
    EngineRecordInput::new(EngineRecordKind::Fill, r#"{"fill":"b3:fill"}"#),
    EngineRecordInput::new(EngineRecordKind::Ledger, r#"{"ledger":"b3:ledger"}"#),
    EngineRecordInput::new(EngineRecordKind::Breaker, r#"{"breaker":"clear"}"#),
];

fn database_path(temp_dir: &tempfile::TempDir) -> std::path::PathBuf {
    temp_dir.path().join("private").join("engine.sqlite3")
}

async fn open_with_run(path: &std::path::Path) -> SqliteStore {
    let mut store = SqliteStore::open(path).await.expect("database should open");
    store
        .create_run(RunInput {
            run_id: RUN_ID,
            started_at_ns: EVENT_TIME_NS,
        })
        .await
        .expect("run should be created");
    store
}

fn event(event_id: &'static str) -> EventInput<'static> {
    EventInput {
        run_id: RUN_ID,
        event_id,
        event_time_ns: EVENT_TIME_NS,
        kind: "engine_boundary",
        payload_json: r#"{"market":"BTC","source":"book"}"#,
    }
}

fn real_event(event_id: &'static str) -> EventInput<'static> {
    EventInput {
        run_id: RUN_ID,
        event_id,
        event_time_ns: 0,
        kind: "market_recovered",
        payload_json: r#"{"market":"BTC","source":"recovery"}"#,
    }
}

fn real_noop_event(event_id: &'static str) -> EventInput<'static> {
    EventInput {
        run_id: RUN_ID,
        event_id,
        event_time_ns: 0,
        kind: "advance_time",
        payload_json: r#"{"source":"scheduler"}"#,
    }
}

fn digest(byte: char) -> String {
    std::iter::repeat_n(byte, 64).collect()
}

fn real_state_context(admission: EventAdmission) -> (EngineState, EngineContext) {
    let at = TimestampNs::new(0).expect("epoch should be representable");
    let universe = UniverseSelector::select(at, std::iter::empty())
        .expect("empty universe snapshot should be valid");
    let activation =
        UniverseSelector::activate(&universe, None, at).expect("epoch activation should be valid");
    let ledger = LedgerState::new(CoreLedgerId::RulesOnly, at)
        .expect("isolated synthetic ledger should open");
    let broker = PaperBroker::new(
        BrokerConfig::new(
            Usdc::new(rust_decimal::Decimal::ONE).expect("minimum notional should be positive"),
            DurationNs::new(1).expect("duration should be representable"),
        )
        .expect("broker config should be valid"),
        BrokerRunContext::new(
            RunId::new("engine-storage-run").expect("run ID should be valid"),
            digest('a'),
            digest('b'),
        )
        .expect("broker context should be valid"),
        at,
    );
    let state = EngineState::new(ledger, broker, BTreeMap::new());
    let context = EngineContext::new(
        admission,
        SnapshotBindings::new(BTreeMap::new(), activation),
        StrategyFingerprints::new(digest('c'), digest('d')),
    );
    (state, context)
}

fn real_outcome(event_id: &'static str, admission: EventAdmission) -> EngineOutcome {
    let at = TimestampNs::new(0).expect("epoch should be representable");
    let (state, context) = real_state_context(admission);
    Engine::apply(
        EngineEvent::MarketRecovered {
            event_id: EventId::new(event_id).expect("event ID should be valid"),
            at,
            market: Market::new("BTC").expect("market should be valid"),
        },
        state,
        &context,
    )
    .expect("real engine outcome should be produced")
}

fn real_flat_outcome(event_id: &'static str, admission: EventAdmission) -> EngineOutcome {
    let at = TimestampNs::new(0).expect("epoch should be representable");
    let (state, context) = real_state_context(admission);
    Engine::apply(
        EngineEvent::AdvanceTime {
            event_id: EventId::new(event_id).expect("event ID should be valid"),
            at,
        },
        state,
        &context,
    )
    .expect("real flat engine outcome should be produced")
}

fn batch(
    event_id: &'static str,
    ledger_id: LedgerId,
    checkpoint_id: &'static str,
) -> EngineBatchInput<'static> {
    EngineBatchInput {
        event: event(event_id),
        ledger_id,
        records: &RECORDS,
        checkpoint: EngineCheckpointInput {
            checkpoint_id,
            ledger_id,
            at_ns: EVENT_TIME_NS,
            state_digest: "b3:engine-state",
            state_json: r#"{"broker":"open","ledger":"isolated"}"#,
        },
    }
}

#[tokio::test]
async fn engine_batch_is_atomic_idempotent_and_audited() {
    let temp_dir = tempfile::tempdir().expect("temporary directory should be created");
    let path = database_path(&temp_dir);
    let mut store = open_with_run(&path).await;

    assert_eq!(
        store
            .engine_admission(event(EVENT_ID), LedgerId::RulesOnly)
            .await
            .expect("new event should have an admission status")
            .admission(),
        EngineAdmission::New
    );
    let committed = store
        .append_engine_batch(
            batch(EVENT_ID, LedgerId::RulesOnly, "engine-checkpoint-rules"),
            AtomicEngineAppend::Commit,
        )
        .await
        .expect("engine batch should commit");
    assert_eq!(
        committed,
        EngineAppendOutcome::Committed { record_count: 8 }
    );
    assert_eq!(
        store
            .engine_journal_counts(RUN_ID)
            .await
            .expect("counts should be readable"),
        EngineJournalCounts {
            events: 1,
            admissions: 1,
            records: 8,
            checkpoints: 1,
            duplicate_attempts: 0,
        }
    );

    drop(store);

    let mut reopened = SqliteStore::open(&path)
        .await
        .expect("database should reopen");
    assert_eq!(
        reopened
            .engine_admission(event(EVENT_ID), LedgerId::RulesOnly)
            .await
            .expect("restarted writer should identify duplicate before evaluation")
            .admission(),
        EngineAdmission::Duplicate {
            duplicate_attempts: 0
        }
    );
    let duplicate = reopened
        .append_engine_batch(
            batch(EVENT_ID, LedgerId::RulesOnly, "engine-checkpoint-rules"),
            AtomicEngineAppend::Commit,
        )
        .await
        .expect("duplicate should be acknowledged atomically");
    assert_eq!(
        duplicate,
        EngineAppendOutcome::Duplicate {
            duplicate_attempts: 1
        }
    );
    assert_eq!(
        reopened
            .engine_journal_counts(RUN_ID)
            .await
            .expect("reopened counts should be readable"),
        EngineJournalCounts {
            events: 1,
            admissions: 1,
            records: 8,
            checkpoints: 1,
            duplicate_attempts: 1,
        }
    );
}

#[tokio::test]
async fn engine_batch_failure_rolls_back_event_records_and_checkpoint() {
    let temp_dir = tempfile::tempdir().expect("temporary directory should be created");
    let path = database_path(&temp_dir);
    let mut store = open_with_run(&path).await;

    let error = store
        .append_engine_batch(
            batch(
                "engine-event-rollback",
                LedgerId::RulesOnly,
                "engine-checkpoint-rollback",
            ),
            AtomicEngineAppend::FailAfterRecords,
        )
        .await
        .expect_err("failpoint should roll back the entire engine batch");
    assert!(matches!(error, StoreError::InjectedFailure));
    assert_eq!(
        store
            .engine_journal_counts(RUN_ID)
            .await
            .expect("counts should be readable"),
        EngineJournalCounts {
            events: 0,
            admissions: 0,
            records: 0,
            checkpoints: 0,
            duplicate_attempts: 0,
        }
    );
}

#[tokio::test]
async fn shared_source_event_is_stored_once_with_independent_ledger_batches() {
    let temp_dir = tempfile::tempdir().expect("temporary directory should be created");
    let path = database_path(&temp_dir);
    let mut store = open_with_run(&path).await;

    for (ledger_id, checkpoint_id) in [
        (LedgerId::RulesOnly, "engine-checkpoint-rules"),
        (LedgerId::MlChampion, "engine-checkpoint-ml"),
    ] {
        let result = store
            .append_engine_batch(
                batch(EVENT_ID, ledger_id, checkpoint_id),
                AtomicEngineAppend::Commit,
            )
            .await
            .expect("each ledger should commit the shared source event once");
        assert_eq!(result, EngineAppendOutcome::Committed { record_count: 8 });
    }

    assert_eq!(
        store
            .engine_journal_counts(RUN_ID)
            .await
            .expect("counts should be readable"),
        EngineJournalCounts {
            events: 1,
            admissions: 2,
            records: 16,
            checkpoints: 2,
            duplicate_attempts: 0,
        }
    );
}

#[tokio::test]
async fn same_ledger_event_with_changed_source_evidence_is_rejected_before_admission() {
    let temp_dir = tempfile::tempdir().expect("temporary directory should be created");
    let path = database_path(&temp_dir);
    let mut store = open_with_run(&path).await;
    store
        .append_engine_batch(
            batch(EVENT_ID, LedgerId::RulesOnly, "engine-checkpoint-rules"),
            AtomicEngineAppend::Commit,
        )
        .await
        .expect("initial batch should commit");

    let mut conflicting = batch(EVENT_ID, LedgerId::RulesOnly, "engine-checkpoint-conflict");
    conflicting.event.payload_json = r#"{"market":"BTC","source":"changed"}"#;
    assert!(matches!(
        store
            .engine_admission(conflicting.event, LedgerId::RulesOnly)
            .await,
        Err(StoreError::EventConflict)
    ));
    assert!(matches!(
        store
            .append_engine_batch(conflicting, AtomicEngineAppend::Commit)
            .await,
        Err(StoreError::EventConflict)
    ));
    assert_eq!(
        store
            .engine_journal_counts(RUN_ID)
            .await
            .expect("counts should be readable"),
        EngineJournalCounts {
            events: 1,
            admissions: 1,
            records: 8,
            checkpoints: 1,
            duplicate_attempts: 0,
        }
    );
}

#[tokio::test]
async fn real_engine_outcomes_persist_through_the_one_way_atomic_mapper() {
    let temp_dir = tempfile::tempdir().expect("temporary directory should be created");
    let path = database_path(&temp_dir);
    let mut store = open_with_run(&path).await;

    let recovered_event = real_event("engine-real-recovered");
    let recovered_permit = store
        .engine_admission(recovered_event, LedgerId::RulesOnly)
        .await
        .expect("new real event should be admitted");
    assert_eq!(recovered_permit.admission(), EngineAdmission::New);
    let recovered = real_outcome("engine-real-recovered", recovered_permit.core_admission());
    let recovered_projection = recovered.persistence_batch();
    assert_eq!(recovered_projection.records().len(), 2);
    assert!(recovered_projection.records().iter().all(|record| {
        serde_json::from_str::<serde_json::Value>(record.payload_json()).is_ok()
    }));
    assert!(recovered_projection.records().iter().any(|record| {
        serde_json::from_str::<serde_json::Value>(record.payload_json()).is_ok_and(|payload| {
            payload["record"]["type"] == "market_recovered" && payload["record"]["market"] == "BTC"
        })
    }));
    let checkpoint =
        serde_json::from_str::<serde_json::Value>(recovered_projection.checkpoint().state_json())
            .expect("typed checkpoint projection should be JSON");
    assert_eq!(checkpoint["schema_version"], 1);
    assert!(checkpoint["ledger"].is_object());
    assert!(checkpoint["broker"].is_object());
    assert!(checkpoint["risk_policy_digests"].is_object());
    assert!(checkpoint["recovered_markets"].is_object());
    assert_eq!(
        store
            .append_engine_outcome(recovered_permit, recovered_event, &recovered,)
            .await
            .expect("recovered engine outcome should commit"),
        EngineAppendOutcome::Committed { record_count: 2 }
    );

    let duplicate_one = real_flat_outcome("engine-real-noop-one", EventAdmission::New);
    let duplicate_two = real_flat_outcome("engine-real-noop-two", EventAdmission::New);
    assert_eq!(
        duplicate_one
            .persistence_batch()
            .checkpoint()
            .state_digest(),
        duplicate_two
            .persistence_batch()
            .checkpoint()
            .state_digest(),
        "two no-op outcomes retain the same successor state"
    );
    assert_ne!(
        duplicate_one
            .persistence_batch()
            .checkpoint()
            .checkpoint_id(),
        duplicate_two
            .persistence_batch()
            .checkpoint()
            .checkpoint_id(),
        "checkpoint identity is bound to causal event identity, not state alone"
    );
    for (event, outcome) in [
        (real_noop_event("engine-real-noop-one"), &duplicate_one),
        (real_noop_event("engine-real-noop-two"), &duplicate_two),
    ] {
        let permit = store
            .engine_admission(event, LedgerId::RulesOnly)
            .await
            .expect("new no-op event should be admitted");
        assert_eq!(
            store
                .append_engine_outcome(permit, event, outcome)
                .await
                .expect("real no-op should commit once per causal event"),
            EngineAppendOutcome::Committed { record_count: 1 }
        );
    }

    drop(store);
    let mut reopened = SqliteStore::open(&path)
        .await
        .expect("database should reopen");
    let duplicate_permit = reopened
        .engine_admission(recovered_event, LedgerId::RulesOnly)
        .await
        .expect("restarted writer should return duplicate admission");
    assert!(matches!(
        duplicate_permit.admission(),
        EngineAdmission::Duplicate {
            duplicate_attempts: 0
        }
    ));
    let duplicate_outcome =
        real_outcome("engine-real-recovered", duplicate_permit.core_admission());
    assert!(duplicate_outcome.is_duplicate_noop());
    assert_eq!(
        reopened
            .append_engine_outcome(duplicate_permit, recovered_event, &duplicate_outcome,)
            .await
            .expect("duplicate no-op should only advance durable audit state"),
        EngineAppendOutcome::Duplicate {
            duplicate_attempts: 1
        }
    );

    assert_eq!(
        reopened
            .engine_journal_counts(RUN_ID)
            .await
            .expect("counts should be readable"),
        EngineJournalCounts {
            events: 3,
            admissions: 3,
            records: 4,
            checkpoints: 3,
            duplicate_attempts: 1,
        }
    );
}

#[tokio::test]
async fn real_lifecycle_outcomes_commit_typed_rows_without_cross_ledger_or_partial_writes() {
    let temp_dir = tempfile::tempdir().expect("temporary directory should be created");
    let path = database_path(&temp_dir);
    let mut store = open_with_run(&path).await;
    let fixtures = critical_lifecycle_fixtures();
    let expected_records = fixtures
        .iter()
        .map(|fixture| i64::try_from(fixture.outcome().persistence_batch().records().len()))
        .collect::<Result<Vec<_>, _>>()
        .expect("fixture record counts should fit SQLite");

    let first = fixtures.first().expect("partial-exit fixture should exist");
    let first_event = EventInput {
        run_id: RUN_ID,
        event_id: first.event_id(),
        event_time_ns: first.event_time_ns(),
        kind: first.kind(),
        payload_json: first.payload_json(),
    };
    let cross_ledger_permit = store
        .engine_admission(first_event, LedgerId::MlChampion)
        .await
        .expect("foreign ledger source validation should be read-only");
    assert!(matches!(
        store
            .append_engine_outcome(cross_ledger_permit, first_event, first.outcome())
            .await,
        Err(StoreError::InvalidInput {
            field: "engine admission",
            ..
        })
    ));

    let failed_permit = store
        .engine_admission(first_event, LedgerId::RulesOnly)
        .await
        .expect("real partial-exit outcome should be admitted before the failpoint");
    assert!(matches!(
        store
            .append_engine_outcome_with_behavior(
                failed_permit,
                first_event,
                first.outcome(),
                AtomicEngineAppend::FailAfterRecords,
            )
            .await,
        Err(StoreError::InjectedFailure)
    ));
    assert_eq!(
        store
            .engine_journal_counts(RUN_ID)
            .await
            .expect("rollback counts should be readable"),
        EngineJournalCounts {
            events: 0,
            admissions: 0,
            records: 0,
            checkpoints: 0,
            duplicate_attempts: 0,
        }
    );

    for (fixture, expected_record_count) in fixtures.iter().zip(&expected_records) {
        let event = EventInput {
            run_id: RUN_ID,
            event_id: fixture.event_id(),
            event_time_ns: fixture.event_time_ns(),
            kind: fixture.kind(),
            payload_json: fixture.payload_json(),
        };
        let permit = store
            .engine_admission(event, LedgerId::RulesOnly)
            .await
            .expect("real lifecycle event should be admitted");
        assert_eq!(
            store
                .append_engine_outcome(permit, event, fixture.outcome())
                .await
                .expect("real lifecycle outcome should commit atomically"),
            EngineAppendOutcome::Committed {
                record_count: usize::try_from(*expected_record_count)
                    .expect("fixture record count should fit usize"),
            }
        );
    }

    assert_eq!(
        store
            .engine_journal_counts(RUN_ID)
            .await
            .expect("committed counts should be readable"),
        EngineJournalCounts {
            events: 4,
            admissions: 4,
            records: expected_records.iter().sum(),
            checkpoints: 4,
            duplicate_attempts: 0,
        }
    );
    drop(store);

    let options = SqliteConnectOptions::new()
        .filename(&path)
        .foreign_keys(true);
    let mut connection = SqliteConnection::connect_with(&options)
        .await
        .expect("schema connection should open");
    let ml_admissions: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM engine_event_admissions WHERE run_id = ?1 AND ledger_id = ?2",
    )
    .bind(RUN_ID)
    .bind("ml_champion")
    .fetch_one(&mut connection)
    .await
    .expect("foreign-ledger admission count should be readable");
    assert_eq!(ml_admissions, 0, "rules outcomes must not leak to ML rows");

    let raw_and_as_of = sqlx::query_as::<_, (String, i64, i64)>(
        "SELECT events.event_id, events.event_time_ns, engine_checkpoints.as_of_time_ns \
         FROM events JOIN engine_checkpoints \
              ON events.run_id = engine_checkpoints.run_id \
             AND events.event_id = engine_checkpoints.event_id \
         WHERE events.run_id = ?1 AND engine_checkpoints.ledger_id = ?2 \
         ORDER BY events.event_id",
    )
    .bind(RUN_ID)
    .bind("rules_only")
    .fetch_all(&mut connection)
    .await
    .expect("source and as-of times should be queryable");
    assert_eq!(raw_and_as_of.len(), fixtures.len());
    for (event_id, source_at, as_of_at) in &raw_and_as_of {
        let fixture = fixtures
            .iter()
            .find(|fixture| fixture.event_id() == event_id)
            .expect("every persisted source event should have one fixture");
        assert_eq!(*source_at, fixture.event_time_ns());
        assert_eq!(
            *as_of_at,
            fixture.outcome().persistence_batch().at().value()
        );
        assert!(
            source_at < as_of_at,
            "fixture must retain distinct source and processing/as-of times"
        );
    }

    let rows = sqlx::query_as::<_, (String, String, String)>(
        "SELECT event_id, record_kind, payload_json FROM engine_batch_records \
         WHERE run_id = ?1 AND ledger_id = ?2 ORDER BY event_id, sequence",
    )
    .bind(RUN_ID)
    .bind("rules_only")
    .fetch_all(&mut connection)
    .await
    .expect("typed lifecycle evidence should be queryable");
    let rows = rows
        .into_iter()
        .map(|(event_id, kind, payload)| {
            (
                event_id,
                kind,
                serde_json::from_str::<serde_json::Value>(&payload)
                    .expect("all persisted engine payloads must remain typed JSON"),
            )
        })
        .collect::<Vec<_>>();

    assert!(rows.iter().any(|(event_id, kind, payload)| {
        event_id == "fixture-partial-exit-book"
            && kind == "fill"
            && payload["record"]["type"] == "broker_applied"
            && payload["record"]["broker_transition"]["records"]
                .as_array()
                .is_some_and(|records| {
                    records.iter().any(|record| {
                        record["type"] == "taker_fill"
                            && record["walk"]["remaining_quantity"] != "0"
                    })
                })
    }));
    assert!(rows.iter().any(|(event_id, kind, payload)| {
        event_id == "fixture-partial-exit-book"
            && kind == "ledger"
            && payload["record"]["ledger_transition"]["kind"] == "position_reduced"
    }));
    assert!(rows.iter().any(|(event_id, kind, payload)| {
        event_id == "fixture-funding-debit"
            && kind == "fill"
            && payload["record"]["broker_transition"]["records"]
                .as_array()
                .is_some_and(|records| {
                    records.iter().any(|record| {
                        record["type"] == "funding"
                            && record["amount"]
                                .as_str()
                                .is_some_and(|amount| amount != "0" && !amount.starts_with('-'))
                    })
                })
    }));
    assert!(rows.iter().any(|(event_id, kind, payload)| {
        event_id == "fixture-funding-credit"
            && kind == "fill"
            && payload["record"]["broker_transition"]["records"]
                .as_array()
                .is_some_and(|records| {
                    records.iter().any(|record| {
                        record["type"] == "funding"
                            && record["amount"]
                                .as_str()
                                .is_some_and(|amount| amount.starts_with('-'))
                    })
                })
    }));
    for event_id in ["fixture-funding-debit", "fixture-funding-credit"] {
        assert!(rows.iter().any(|(stored_event_id, kind, payload)| {
            stored_event_id == event_id
                && kind == "ledger"
                && payload["record"]["ledger_transition"]["kind"] == "funding_applied"
        }));
    }
    assert!(rows.iter().any(|(event_id, kind, payload)| {
        event_id == "fixture-liquidation-gap-book"
            && kind == "fill"
            && payload["record"]["broker_transition"]["records"]
                .as_array()
                .is_some_and(|records| {
                    records
                        .iter()
                        .any(|record| record["type"] == "liquidation_loss")
                })
    }));
    assert!(rows.iter().any(|(event_id, kind, payload)| {
        event_id == "fixture-liquidation-gap-book"
            && kind == "ledger"
            && payload["record"]["ledger_transition"]["kind"] == "position_liquidated"
    }));
}

#[tokio::test]
async fn outcome_permit_binds_source_evidence_and_cannot_be_reused_after_commit() {
    let temp_dir = tempfile::tempdir().expect("temporary directory should be created");
    let path = database_path(&temp_dir);
    let mut store = open_with_run(&path).await;

    let event = real_event("engine-permit-evidence");
    let permit = store
        .engine_admission(event, LedgerId::RulesOnly)
        .await
        .expect("new event should be admitted");
    let mut changed_event = event;
    changed_event.payload_json = r#"{"market":"BTC","source":"forged"}"#;
    let changed_outcome = real_outcome(event.event_id, permit.core_admission());
    assert!(matches!(
        store
            .append_engine_outcome(permit, changed_event, &changed_outcome,)
            .await,
        Err(StoreError::InvalidInput {
            field: "engine admission",
            ..
        })
    ));

    let event = real_event("engine-permit-stale");
    let stale_permit = store
        .engine_admission(event, LedgerId::RulesOnly)
        .await
        .expect("first new permit should be issued");
    let current_permit = store
        .engine_admission(event, LedgerId::RulesOnly)
        .await
        .expect("second new permit should be issued before commit");
    let committed_outcome = real_outcome(event.event_id, current_permit.core_admission());
    store
        .append_engine_outcome(current_permit, event, &committed_outcome)
        .await
        .expect("current permit should commit");
    let stale_outcome = real_outcome(event.event_id, stale_permit.core_admission());
    assert!(matches!(
        store
            .append_engine_outcome(stale_permit, event, &stale_outcome,)
            .await,
        Err(StoreError::InvalidInput {
            field: "engine admission",
            ..
        })
    ));
    assert_eq!(
        store
            .engine_journal_counts(RUN_ID)
            .await
            .expect("counts should be readable"),
        EngineJournalCounts {
            events: 1,
            admissions: 1,
            records: 2,
            checkpoints: 1,
            duplicate_attempts: 0,
        }
    );
}

#[tokio::test]
async fn schema_rejects_evidence_without_an_exact_ledger_admission() {
    let temp_dir = tempfile::tempdir().expect("temporary directory should be created");
    let path = database_path(&temp_dir);
    let mut store = open_with_run(&path).await;
    store
        .append_engine_batch(
            batch(EVENT_ID, LedgerId::RulesOnly, "engine-checkpoint-rules"),
            AtomicEngineAppend::Commit,
        )
        .await
        .expect("rules ledger batch should commit");
    drop(store);

    let options = SqliteConnectOptions::new()
        .filename(&path)
        .foreign_keys(true);
    let mut connection = SqliteConnection::connect_with(&options)
        .await
        .expect("schema connection should open");
    let record_error = sqlx::query(
        "INSERT INTO engine_batch_records \
         (run_id, ledger_id, event_id, sequence, record_kind, payload_json) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
    )
    .bind(RUN_ID)
    .bind("ml_champion")
    .bind(EVENT_ID)
    .bind(0_i64)
    .bind("snapshot")
    .bind(r#"{"book":"orphan"}"#)
    .execute(&mut connection)
    .await
    .expect_err("record without ML admission must be rejected");
    assert!(
        record_error
            .to_string()
            .contains("FOREIGN KEY constraint failed")
    );

    let checkpoint_error = sqlx::query(
        "INSERT INTO engine_checkpoints \
         (checkpoint_id, run_id, ledger_id, event_id, as_of_time_ns, state_digest, state_json) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
    )
    .bind("engine-checkpoint-orphan")
    .bind(RUN_ID)
    .bind("ml_champion")
    .bind(EVENT_ID)
    .bind(EVENT_TIME_NS)
    .bind("b3:orphan-state")
    .bind(r#"{"broker":"orphan"}"#)
    .execute(&mut connection)
    .await
    .expect_err("checkpoint without ML admission must be rejected");
    assert!(
        checkpoint_error
            .to_string()
            .contains("FOREIGN KEY constraint failed")
    );

    let rewrite_error = sqlx::query(
        "UPDATE engine_batch_records SET payload_json = ?1 \
         WHERE run_id = ?2 AND ledger_id = ?3 AND event_id = ?4 AND sequence = ?5",
    )
    .bind(r#"{"book":"rewritten"}"#)
    .bind(RUN_ID)
    .bind("rules_only")
    .bind(EVENT_ID)
    .bind(0_i64)
    .execute(&mut connection)
    .await
    .expect_err("engine evidence must be immutable");
    assert!(
        rewrite_error
            .to_string()
            .contains("engine batch records are immutable")
    );
    let delete_error = sqlx::query(
        "DELETE FROM engine_checkpoints \
         WHERE run_id = ?1 AND ledger_id = ?2 AND event_id = ?3",
    )
    .bind(RUN_ID)
    .bind("rules_only")
    .bind(EVENT_ID)
    .execute(&mut connection)
    .await
    .expect_err("engine checkpoints must be append-only");
    assert!(
        delete_error
            .to_string()
            .contains("engine checkpoints are append-only")
    );
}
