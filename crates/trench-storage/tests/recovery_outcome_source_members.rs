use std::fs;

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use rust_decimal_macros::dec;
use tempfile::TempDir;
use trench_core::{
    domain::{Market, Price, Quantity, Side},
    event::{BookLevel, BookSnapshot, CandleInterval, CompletedCandle, MarketEvent, TimestampNs},
    validation::TimeRange,
};
use trench_storage::{
    parquet::{DataProvenance, ParquetStore},
    recovery_outcomes::{
        ReconciledRecoveryOutcome, RecoveryOutcomeSource, RecoveryOutcomeStatus,
        RecoveryOutcomeStore, RecoveryRequestCursors, RecoverySourceReference,
    },
    research_plan::{ResearchMemberLocator, ResearchSourcePlanBuilder},
    research_runs::AvailabilityKey,
};

const FIFTEEN_MINUTES_NS: i64 = 900_000_000_000;

fn digest(character: char) -> String {
    format!("b3:{}", character.to_string().repeat(64))
}

fn timestamp(value: i64) -> TimestampNs {
    TimestampNs::new(i128::from(value)).expect("fixture timestamp")
}

fn range(start: i64, end: i64) -> TimeRange {
    TimeRange::new(timestamp(start), timestamp(end)).expect("fixture range")
}

fn book(event_time: i64, received_at: i64, sequence: u64) -> MarketEvent {
    MarketEvent::book_snapshot(
        timestamp(event_time),
        timestamp(received_at),
        Market::new("SOL").expect("market"),
        BookSnapshot::new(
            sequence,
            vec![BookLevel::new(
                Price::new(dec!(99)).expect("bid price"),
                Quantity::new(dec!(1)).expect("bid quantity"),
            )],
            vec![BookLevel::new(
                Price::new(dec!(101)).expect("ask price"),
                Quantity::new(dec!(1)).expect("ask quantity"),
            )],
        ),
    )
    .expect("book snapshot")
}

fn completed_candle() -> MarketEvent {
    let candle = CompletedCandle::new(
        CandleInterval::FifteenMinutes,
        timestamp(0),
        Price::new(dec!(100)).expect("open"),
        Price::new(dec!(101)).expect("high"),
        Price::new(dec!(99)).expect("low"),
        Price::new(dec!(100)).expect("close"),
        Quantity::new(dec!(1)).expect("volume"),
        1,
    )
    .expect("completed candle");
    MarketEvent::completed_candle(
        timestamp(FIFTEEN_MINUTES_NS),
        timestamp(FIFTEEN_MINUTES_NS),
        Market::new("SOL").expect("market"),
        candle,
    )
    .expect("completed candle event")
}

fn trade(event_time: i64, received_at: i64, trade_id: u64) -> MarketEvent {
    MarketEvent::trade(
        timestamp(event_time),
        timestamp(received_at),
        Market::new("SOL").expect("market"),
        trench_core::event::Trade::new(
            trade_id,
            Side::Buy,
            Price::new(dec!(100)).expect("price"),
            Quantity::new(dec!(1)).expect("quantity"),
        )
        .expect("trade"),
    )
    .expect("trade event")
}

fn reference(event: &MarketEvent, member_manifest_digest: &str) -> RecoverySourceReference {
    RecoverySourceReference::new(
        member_manifest_digest,
        AvailabilityKey::new(
            event.received_at(),
            event.event_time(),
            event.event_id().clone(),
        )
        .expect("availability key"),
    )
    .expect("source reference")
}

fn outcome(
    anchor_member_manifest_digest: &str,
    candle_member_manifest_digest: &str,
    anchor: &MarketEvent,
    candle: &MarketEvent,
) -> ReconciledRecoveryOutcome {
    let anchor = reference(anchor, anchor_member_manifest_digest);
    let completion = reference(candle, candle_member_manifest_digest);
    ReconciledRecoveryOutcome::new(
        "sol-gap-1",
        1,
        Market::new("SOL").expect("market"),
        RecoveryRequestCursors::new(None, None).expect("request cursors"),
        RecoveryOutcomeStatus::Reconciled,
        RecoveryOutcomeSource::CapturedTrades,
        timestamp(FIFTEEN_MINUTES_NS),
        anchor,
        Vec::new(),
        vec![completion.clone()],
        completion,
    )
    .expect("reconciled outcome")
}

fn store() -> (TempDir, ParquetStore) {
    let root = TempDir::new().expect("temporary root");
    #[cfg(unix)]
    fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700)).expect("private root");
    let provenance = DataProvenance::new(digest('a'), digest('b'), ParquetStore::schema_hash())
        .expect("provenance");
    let store = ParquetStore::open(root.path(), provenance).expect("store");
    (root, store)
}

#[test]
fn companion_outcome_reopens_only_from_its_immutable_locator() {
    let (root, store) = store();
    let anchor = book(FIFTEEN_MINUTES_NS - 1_000, FIFTEEN_MINUTES_NS - 1_000, 1);
    let candle = completed_candle();
    let manifests = store
        .write_events(&[anchor.clone(), candle.clone()])
        .expect("source events");
    assert_eq!(manifests.len(), 2);
    let anchor_manifest = manifests
        .iter()
        .find(|manifest| manifest.min_event_time() == anchor.event_time())
        .expect("book manifest");
    let candle_manifest = manifests
        .iter()
        .find(|manifest| manifest.min_event_time() == candle.event_time())
        .expect("candle manifest");
    let outcome = outcome(
        &anchor_manifest.manifest_digest(),
        &candle_manifest.manifest_digest(),
        &anchor,
        &candle,
    );
    let outcomes = RecoveryOutcomeStore::open(&store).expect("outcome store");
    let locator = outcomes.publish(&outcome).expect("published outcome");

    assert_eq!(
        outcomes.open_member(&locator).expect("reopened outcome"),
        outcome
    );

    let draft = ResearchSourcePlanBuilder::new(
        range(0, FIFTEEN_MINUTES_NS),
        range(FIFTEEN_MINUTES_NS, FIFTEEN_MINUTES_NS * 2),
    )
    .expect("windows")
    .with_recovery_outcomes(vec![locator])
    .expect("outcome selection")
    .build(
        &store,
        vec![
            ResearchMemberLocator::legacy(anchor_manifest),
            ResearchMemberLocator::legacy(candle_manifest),
        ],
        Vec::new(),
    )
    .expect("source-plan draft");
    let verified = draft
        .publish_to(&store, root.path().join("source-plan"))
        .expect("verified source plan");
    assert_eq!(verified.recovery_outcomes(), std::slice::from_ref(&outcome));
}

#[test]
fn companion_outcome_rejects_foreign_provenance_and_unselected_raw_members() {
    let (root, store) = store();
    let anchor = book(FIFTEEN_MINUTES_NS - 1_000, FIFTEEN_MINUTES_NS - 1_000, 1);
    let candle = completed_candle();
    let manifests = store
        .write_events(&[anchor.clone(), candle.clone()])
        .expect("source events");
    let anchor_manifest = manifests
        .iter()
        .find(|manifest| manifest.min_event_time() == anchor.event_time())
        .expect("book manifest");
    let candle_manifest = manifests
        .iter()
        .find(|manifest| manifest.min_event_time() == candle.event_time())
        .expect("candle manifest");
    let outcome = outcome(
        &anchor_manifest.manifest_digest(),
        &candle_manifest.manifest_digest(),
        &anchor,
        &candle,
    );
    let outcomes = RecoveryOutcomeStore::open(&store).expect("outcome store");
    let locator = outcomes.publish(&outcome).expect("published outcome");

    let foreign = ParquetStore::open(
        root.path(),
        DataProvenance::new(digest('c'), digest('b'), ParquetStore::schema_hash())
            .expect("foreign provenance"),
    )
    .expect("foreign store handle");
    assert!(
        RecoveryOutcomeStore::open(&foreign)
            .expect("foreign outcome store handle")
            .open_member(&locator)
            .is_err()
    );

    let draft = ResearchSourcePlanBuilder::new(
        range(0, FIFTEEN_MINUTES_NS),
        range(FIFTEEN_MINUTES_NS, FIFTEEN_MINUTES_NS * 2),
    )
    .expect("windows")
    .with_recovery_outcomes(vec![locator])
    .expect("outcome selection")
    .build(
        &store,
        vec![ResearchMemberLocator::legacy(anchor_manifest)],
        Vec::new(),
    );
    assert!(draft.is_err());
}

#[test]
fn companion_outcome_rejects_a_tampered_published_payload() {
    let (root, store) = store();
    let anchor = book(FIFTEEN_MINUTES_NS - 1_000, FIFTEEN_MINUTES_NS - 1_000, 1);
    let candle = completed_candle();
    let manifests = store
        .write_events(&[anchor.clone(), candle.clone()])
        .expect("source events");
    let anchor_manifest = manifests
        .iter()
        .find(|manifest| manifest.min_event_time() == anchor.event_time())
        .expect("book manifest");
    let candle_manifest = manifests
        .iter()
        .find(|manifest| manifest.min_event_time() == candle.event_time())
        .expect("candle manifest");
    let outcome = outcome(
        &anchor_manifest.manifest_digest(),
        &candle_manifest.manifest_digest(),
        &anchor,
        &candle,
    );
    let outcomes = RecoveryOutcomeStore::open(&store).expect("outcome store");
    let locator = outcomes.publish(&outcome).expect("published outcome");
    let payload = root
        .path()
        .join("recovery-outcomes")
        .join(format!("outcome-{}.out", locator.outcome_id()))
        .join("outcome.json");
    fs::write(payload, b"{}\n").expect("tamper companion payload");

    assert!(outcomes.open_member(&locator).is_err());
}

#[test]
fn companion_outcome_rejects_unexpected_member_entries() {
    let (root, store) = store();
    let anchor = book(FIFTEEN_MINUTES_NS - 1_000, FIFTEEN_MINUTES_NS - 1_000, 1);
    let candle = completed_candle();
    let manifests = store
        .write_events(&[anchor.clone(), candle.clone()])
        .expect("source events");
    let anchor_manifest = manifests
        .iter()
        .find(|manifest| manifest.min_event_time() == anchor.event_time())
        .expect("book manifest");
    let candle_manifest = manifests
        .iter()
        .find(|manifest| manifest.min_event_time() == candle.event_time())
        .expect("candle manifest");
    let outcome = outcome(
        &anchor_manifest.manifest_digest(),
        &candle_manifest.manifest_digest(),
        &anchor,
        &candle,
    );
    let outcomes = RecoveryOutcomeStore::open(&store).expect("outcome store");
    let locator = outcomes.publish(&outcome).expect("published outcome");
    fs::write(
        root.path()
            .join("recovery-outcomes")
            .join(format!("outcome-{}.out", locator.outcome_id()))
            .join("injected"),
        b"unexpected",
    )
    .expect("inject unexpected entry");

    assert!(outcomes.open_member(&locator).is_err());
}

#[test]
fn availability_anchor_is_the_exact_maximum_key_including_event_id_ties() {
    let recovery_anchor = reference(&book(800, 800, 1), &digest('a'));
    let backfill = reference(
        &trade(FIFTEEN_MINUTES_NS, FIFTEEN_MINUTES_NS, 1),
        &digest('b'),
    );
    let official = reference(&completed_candle(), &digest('c'));
    let (lower, maximum) = if backfill.key() < official.key() {
        (backfill.clone(), official.clone())
    } else {
        (official.clone(), backfill.clone())
    };

    assert!(
        ReconciledRecoveryOutcome::new(
            "sol-gap-1",
            1,
            Market::new("SOL").expect("market"),
            RecoveryRequestCursors::new(None, None).expect("request cursors"),
            RecoveryOutcomeStatus::Reconciled,
            RecoveryOutcomeSource::CapturedTrades,
            timestamp(FIFTEEN_MINUTES_NS),
            recovery_anchor.clone(),
            vec![backfill.clone()],
            vec![official.clone()],
            lower,
        )
        .is_err()
    );

    assert!(
        ReconciledRecoveryOutcome::new(
            "sol-gap-1",
            1,
            Market::new("SOL").expect("market"),
            RecoveryRequestCursors::new(None, None).expect("request cursors"),
            RecoveryOutcomeStatus::Reconciled,
            RecoveryOutcomeSource::CapturedTrades,
            timestamp(FIFTEEN_MINUTES_NS),
            recovery_anchor,
            vec![backfill],
            vec![official],
            maximum,
        )
        .is_ok()
    );
}
