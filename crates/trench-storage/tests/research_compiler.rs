use std::fs;

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use rust_decimal_macros::dec;
use tempfile::TempDir;
use trench_core::{
    domain::{Market, Price, Quantity, Side},
    event::{
        BookLevel, BookSnapshot, CandleInterval, CompletedCandle, MarketEvent, TimestampNs, Trade,
    },
    validation::TimeRange,
};
use trench_storage::{
    parquet::{DataProvenance, ParquetStore},
    recovery_outcomes::{
        ReconciledRecoveryOutcome, RecoveryOutcomeSource, RecoveryOutcomeStatus,
        RecoveryOutcomeStore, RecoveryRequestCursors, RecoverySourceReference,
    },
    research_compile::{ResearchEvidenceCompiler, ResearchExclusionReason},
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

fn candle_with_interval(
    interval: CandleInterval,
    open_at: i64,
    received_at: i64,
    volume: i64,
) -> MarketEvent {
    let candle = CompletedCandle::new(
        interval,
        timestamp(open_at),
        Price::new(dec!(100)).expect("open"),
        Price::new(dec!(101)).expect("high"),
        Price::new(dec!(99)).expect("low"),
        Price::new(dec!(100)).expect("close"),
        Quantity::new(rust_decimal::Decimal::from(volume)).expect("volume"),
        1,
    )
    .expect("candle");
    MarketEvent::completed_candle(
        timestamp(open_at + interval.duration().value()),
        timestamp(received_at),
        Market::new("SOL").expect("market"),
        candle,
    )
    .expect("completed candle")
}

fn candle(open_at: i64, received_at: i64, volume: i64) -> MarketEvent {
    candle_with_interval(CandleInterval::FifteenMinutes, open_at, received_at, volume)
}

fn late_candle() -> MarketEvent {
    candle(0, FIFTEEN_MINUTES_NS + 1, 1)
}

fn timely_candle() -> MarketEvent {
    candle(0, FIFTEEN_MINUTES_NS, 1)
}

fn timely_candle_at(open_at: i64) -> MarketEvent {
    candle(open_at, open_at + FIFTEEN_MINUTES_NS, 1)
}

fn trade(event_time: i64, received_at: i64, trade_id: u64) -> MarketEvent {
    MarketEvent::trade(
        timestamp(event_time),
        timestamp(received_at),
        Market::new("SOL").expect("market"),
        Trade::new(
            trade_id,
            Side::Buy,
            Price::new(dec!(100)).expect("price"),
            Quantity::new(dec!(1)).expect("quantity"),
        )
        .expect("trade"),
    )
    .expect("trade")
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

fn key(event: &MarketEvent) -> AvailabilityKey {
    AvailabilityKey::new(
        event.received_at(),
        event.event_time(),
        event.event_id().clone(),
    )
    .expect("availability key")
}

fn source_reference(event: &MarketEvent, manifest_digest: &str) -> RecoverySourceReference {
    RecoverySourceReference::new(manifest_digest, key(event)).expect("source reference")
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

fn publish_plan(
    root: &TempDir,
    store: &ParquetStore,
    manifests: &[trench_storage::parquet::PartitionManifest],
    outcome: Option<ReconciledRecoveryOutcome>,
) -> trench_storage::research_runs::VerifiedResearchSourcePlan {
    let builder = ResearchSourcePlanBuilder::new(
        range(0, FIFTEEN_MINUTES_NS),
        range(FIFTEEN_MINUTES_NS, FIFTEEN_MINUTES_NS * 2),
    )
    .expect("windows");
    let builder = if let Some(outcome) = outcome {
        let locator = RecoveryOutcomeStore::open(store)
            .expect("outcome store")
            .publish(&outcome)
            .expect("publish outcome");
        builder
            .with_recovery_outcomes(vec![locator])
            .expect("select outcome")
    } else {
        builder
    };
    let draft = builder
        .build(
            store,
            manifests
                .iter()
                .map(ResearchMemberLocator::legacy)
                .collect(),
            Vec::new(),
        )
        .expect("source plan draft");
    draft
        .publish_to(store, root.path().join("source-plan"))
        .expect("source plan")
}

fn plan_with(
    events: &[MarketEvent],
) -> (
    TempDir,
    trench_storage::research_runs::VerifiedResearchSourcePlan,
) {
    let (root, store) = store();
    let manifests = store.write_events(events).expect("source partitions");
    let plan = publish_plan(&root, &store, &manifests, None);
    (root, plan)
}

fn plan_with_recovery(
    events: &[MarketEvent],
    recovery_anchor: &MarketEvent,
    official_candle: Option<&MarketEvent>,
    status: RecoveryOutcomeStatus,
    source: RecoveryOutcomeSource,
    completed_through: TimestampNs,
) -> (
    TempDir,
    trench_storage::research_runs::VerifiedResearchSourcePlan,
) {
    let (root, store) = store();
    let manifests = store.write_events(events).expect("source partitions");
    let manifest_for = |event: &MarketEvent| {
        manifests
            .iter()
            .find(|manifest| {
                store
                    .read_partition(manifest)
                    .expect("reopen partition")
                    .iter()
                    .any(|candidate| candidate.event_id() == event.event_id())
            })
            .expect("event manifest")
    };
    let anchor = source_reference(
        recovery_anchor,
        &manifest_for(recovery_anchor).manifest_digest(),
    );
    let official_candle_references = official_candle
        .into_iter()
        .map(|event| source_reference(event, &manifest_for(event).manifest_digest()))
        .collect::<Vec<_>>();
    let availability_anchor = std::iter::once(&anchor)
        .chain(&official_candle_references)
        .max_by_key(|reference| reference.key())
        .expect("nonempty recovery proof")
        .clone();
    let outcome = ReconciledRecoveryOutcome::new(
        "sol-gap-1",
        1,
        Market::new("SOL").expect("market"),
        RecoveryRequestCursors::new(None, None).expect("request cursors"),
        status,
        source,
        completed_through,
        anchor,
        Vec::new(),
        official_candle_references,
        availability_anchor,
    )
    .expect("recovery outcome");
    let plan = publish_plan(&root, &store, &manifests, Some(outcome));
    (root, plan)
}

fn books_between(lower: &AvailabilityKey, upper: &AvailabilityKey, at: i64) -> Vec<MarketEvent> {
    let mut books = (1..10_000)
        .map(|sequence| book(at, at, sequence))
        .filter(|candidate| key(candidate) > *lower && key(candidate) < *upper)
        .collect::<Vec<_>>();
    books.sort_by_key(key);
    books
}

#[test]
fn late_candle_is_excluded_at_original_boundary() {
    let (_root, source_plan) = plan_with(&[late_candle()]);

    let compiled = ResearchEvidenceCompiler::new()
        .compile(&source_plan)
        .expect("causal compilation");

    assert!(compiled.decisions().is_empty());
    assert_eq!(compiled.excluded_gaps().len(), 1);
    assert_eq!(
        compiled.excluded_gaps()[0].range(),
        range(0, FIFTEEN_MINUTES_NS)
    );
    assert_eq!(
        compiled.excluded_gaps()[0].reason(),
        ResearchExclusionReason::LateSource
    );
}

#[test]
fn exact_boundary_candle_never_admits_the_first_later_fact() {
    let candle = timely_candle();
    let (_root, source_plan) = plan_with(&[
        candle.clone(),
        trade(FIFTEEN_MINUTES_NS + 1, FIFTEEN_MINUTES_NS + 1, 1),
    ]);

    let compiled = ResearchEvidenceCompiler::new()
        .compile(&source_plan)
        .expect("causal compilation");

    assert_eq!(compiled.excluded_gaps(), []);
    assert_eq!(compiled.decisions().len(), 1);
    assert_eq!(
        compiled.decisions()[0].decision_at(),
        timestamp(FIFTEEN_MINUTES_NS)
    );
    assert_eq!(
        compiled.decisions()[0].source_event_ids(),
        &[candle.event_id().clone()]
    );
}

#[test]
fn verified_companion_recovery_releases_only_after_its_availability_anchor() {
    let recovery_anchor = book(100, 100, 1);
    let recovery_candle = timely_candle();
    let post_recovery_book = book(
        FIFTEEN_MINUTES_NS * 2 - 500_000_000,
        FIFTEEN_MINUTES_NS * 2 - 500_000_000,
        2,
    );
    let decision_candle = timely_candle_at(FIFTEEN_MINUTES_NS);
    let (_root, source_plan) = plan_with_recovery(
        &[
            recovery_anchor.clone(),
            recovery_candle.clone(),
            post_recovery_book,
            decision_candle,
        ],
        &recovery_anchor,
        Some(&recovery_candle),
        RecoveryOutcomeStatus::Reconciled,
        RecoveryOutcomeSource::CapturedTrades,
        recovery_candle.event_time(),
    );

    let compiled = ResearchEvidenceCompiler::new()
        .compile(&source_plan)
        .expect("causal compilation");

    assert_eq!(compiled.decisions().len(), 1);
    assert_eq!(compiled.recovery_witnesses().len(), 1);
    assert_eq!(compiled.excluded_gaps().len(), 1);
    assert_eq!(
        compiled.excluded_gaps()[0].reason(),
        ResearchExclusionReason::RecoveryFence
    );
}

#[test]
fn recovery_post_book_compares_the_full_availability_key_including_event_id() {
    let fifteen = candle_with_interval(
        CandleInterval::FifteenMinutes,
        FIFTEEN_MINUTES_NS * 3,
        FIFTEEN_MINUTES_NS * 4,
        1,
    );
    let hour = candle_with_interval(CandleInterval::OneHour, 0, FIFTEEN_MINUTES_NS * 4, 1);
    let (recovery_candle, decision_candle) = if key(&fifteen) < key(&hour) {
        (fifteen, hour)
    } else {
        (hour, fifteen)
    };
    let books = books_between(
        &key(&recovery_candle),
        &key(&decision_candle),
        FIFTEEN_MINUTES_NS * 4,
    );
    assert!(books.len() >= 3, "fixture needs ordered event-id ties");
    let pre_anchor_book = books[0].clone();
    let recovery_anchor = books[1].clone();
    let post_anchor_book = books[2].clone();
    let (_root, pre_source_plan) = plan_with_recovery(
        &[
            recovery_anchor.clone(),
            pre_anchor_book,
            recovery_candle.clone(),
            decision_candle.clone(),
        ],
        &recovery_anchor,
        Some(&recovery_candle),
        RecoveryOutcomeStatus::Reconciled,
        RecoveryOutcomeSource::CapturedTrades,
        recovery_candle.event_time(),
    );
    let pre_compiled = ResearchEvidenceCompiler::new()
        .compile(&pre_source_plan)
        .expect("causal compilation");
    assert_eq!(pre_compiled.decisions().len(), 1);
    assert!(pre_compiled.recovery_witnesses().is_empty());
    assert_eq!(pre_compiled.excluded_gaps().len(), 1);
    assert_eq!(
        pre_compiled.excluded_gaps()[0].reason(),
        ResearchExclusionReason::RecoveryFence
    );

    let (_root, post_source_plan) = plan_with_recovery(
        &[
            recovery_anchor.clone(),
            recovery_candle.clone(),
            post_anchor_book,
            decision_candle,
        ],
        &recovery_anchor,
        Some(&recovery_candle),
        RecoveryOutcomeStatus::Reconciled,
        RecoveryOutcomeSource::CapturedTrades,
        recovery_candle.event_time(),
    );
    let post_compiled = ResearchEvidenceCompiler::new()
        .compile(&post_source_plan)
        .expect("causal compilation");
    assert_eq!(post_compiled.decisions().len(), 2);
    assert_eq!(post_compiled.recovery_witnesses().len(), 1);
}

#[test]
fn outcome_release_after_a_decision_never_backdates_a_recovery_witness() {
    let recovery_anchor = book(100, 100, 1);
    let decision_candle = timely_candle_at(FIFTEEN_MINUTES_NS);
    let late_official_candle = candle(0, FIFTEEN_MINUTES_NS * 2 + 1, 2);
    let (_root, source_plan) = plan_with_recovery(
        &[
            recovery_anchor.clone(),
            decision_candle,
            late_official_candle.clone(),
        ],
        &recovery_anchor,
        Some(&late_official_candle),
        RecoveryOutcomeStatus::Reconciled,
        RecoveryOutcomeSource::CapturedTrades,
        timestamp(FIFTEEN_MINUTES_NS),
    );

    let compiled = ResearchEvidenceCompiler::new()
        .compile(&source_plan)
        .expect("causal compilation");

    assert_eq!(compiled.decisions().len(), 1);
    assert!(compiled.recovery_witnesses().is_empty());
}

#[test]
fn unavailable_companion_is_auditable_but_never_releases_entries() {
    let recovery_anchor = book(100, 100, 1);
    let decision_candle = timely_candle();
    let (_root, source_plan) = plan_with_recovery(
        &[recovery_anchor.clone(), decision_candle],
        &recovery_anchor,
        None,
        RecoveryOutcomeStatus::Unavailable,
        RecoveryOutcomeSource::Unavailable,
        timestamp(FIFTEEN_MINUTES_NS),
    );

    assert_eq!(source_plan.recovery_outcomes().len(), 1);
    let compiled = ResearchEvidenceCompiler::new()
        .compile(&source_plan)
        .expect("causal compilation");
    assert!(compiled.decisions().is_empty());
    assert!(compiled.recovery_witnesses().is_empty());
    assert_eq!(
        compiled.excluded_gaps()[0].reason(),
        ResearchExclusionReason::RecoveryFence
    );
}
