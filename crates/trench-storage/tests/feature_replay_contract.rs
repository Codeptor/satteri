use std::fs;

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use rust_decimal_macros::dec;
use tempfile::TempDir;
use trench_core::{
    domain::{Market, Price, Quantity, Side, Sleeve},
    event::{CandleInterval, CompletedCandle, MarketEvent, TimestampNs, Trade},
    validation::TimeRange,
};
use trench_storage::{
    feature_replay::{FeatureInputWitness, FeatureReplayError, RecomputedFeatureContract},
    parquet::{DataProvenance, ParquetStore},
    research_plan::{ResearchMemberLocator, ResearchSourcePlanBuilder},
    research_runs::{AvailabilitySourceReference, ResearchSourcePlan, VerifiedResearchSourcePlan},
};

const FIFTEEN_MINUTES_NS: i64 = 900_000_000_000;

fn digest(character: char) -> String {
    character.to_string().repeat(64)
}

fn timestamp(value: i64) -> TimestampNs {
    TimestampNs::new(i128::from(value)).expect("fixture timestamp")
}

fn range(start: i64, end: i64) -> TimeRange {
    TimeRange::new(timestamp(start), timestamp(end)).expect("fixture range")
}

fn market(value: &str) -> Market {
    Market::new(value).expect("fixture market")
}

fn decision_candle() -> MarketEvent {
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
        market("SOL"),
        candle,
    )
    .expect("decision event")
}

fn timely_trade() -> MarketEvent {
    MarketEvent::trade(
        timestamp(1),
        timestamp(2),
        market("SOL"),
        Trade::new(
            1,
            Side::Buy,
            Price::new(dec!(100)).expect("price"),
            Quantity::new(dec!(1)).expect("quantity"),
        )
        .expect("trade"),
    )
    .expect("timely trade")
}

fn late_trade() -> MarketEvent {
    MarketEvent::trade(
        timestamp(3),
        timestamp(FIFTEEN_MINUTES_NS + 1),
        market("SOL"),
        Trade::new(
            2,
            Side::Buy,
            Price::new(dec!(100)).expect("price"),
            Quantity::new(dec!(1)).expect("quantity"),
        )
        .expect("trade"),
    )
    .expect("late trade")
}

struct Fixture {
    _root: TempDir,
    _store: ParquetStore,
    plan: VerifiedResearchSourcePlan,
    decision: MarketEvent,
    references: Vec<AvailabilitySourceReference>,
}

fn fixture(include_late_source: bool) -> Fixture {
    let root = TempDir::new().expect("temporary root");
    #[cfg(unix)]
    fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700)).expect("private root");
    let provenance = DataProvenance::new(
        format!("b3:{}", digest('a')),
        format!("b3:{}", digest('b')),
        ParquetStore::schema_hash(),
    )
    .expect("provenance");
    let store = ParquetStore::open(root.path(), provenance).expect("store");
    let decision = decision_candle();
    let events = std::iter::once(timely_trade())
        .chain(std::iter::once(decision.clone()))
        .chain(include_late_source.then(late_trade))
        .collect::<Vec<_>>();
    let manifests = store.write_events(&events).expect("source partition");
    let plan_directory = root.path().join("source-plan");
    let published = ResearchSourcePlanBuilder::new(
        range(0, FIFTEEN_MINUTES_NS + 2),
        range(FIFTEEN_MINUTES_NS + 2, FIFTEEN_MINUTES_NS * 2),
    )
    .expect("windows")
    .build(
        &store,
        manifests
            .iter()
            .map(ResearchMemberLocator::legacy)
            .collect(),
        Vec::new(),
    )
    .expect("source plan")
    .publish_to(&store, &plan_directory)
    .expect("published source plan");
    let references = published
        .availability_run()
        .records()
        .collect::<Result<Vec<_>, _>>()
        .expect("published source records")
        .into_iter()
        .map(|record| record.source_reference())
        .collect::<Vec<_>>();
    let plan = ResearchSourcePlan::open_from(&store, &plan_directory).expect("reopened plan");
    Fixture {
        _root: root,
        _store: store,
        plan,
        decision,
        references,
    }
}

fn witness(fixture: &Fixture) -> FeatureInputWitness {
    FeatureInputWitness::new(
        fixture.decision.event_id().clone(),
        market("SOL"),
        Sleeve::FifteenMinute,
        fixture.decision.event_time(),
        digest('a'),
        digest('b'),
        digest('c'),
        digest('d'),
        fixture.references.clone(),
    )
    .expect("feature input witness")
}

#[test]
fn witness_replays_only_timely_exact_source_facts_without_a_snapshot() {
    let fixture = fixture(false);
    let witness = witness(&fixture);
    let verified = witness
        .verify_against(&fixture.plan)
        .expect("verified inputs");
    let event_ids = verified
        .clone()
        .recompute(|events| {
            Ok::<_, ()>(
                events
                    .iter()
                    .map(|event| event.event_id().clone())
                    .collect::<Vec<_>>(),
            )
        })
        .expect("recomputation hook");
    assert_eq!(event_ids.len(), fixture.references.len());
    verified
        .verify_recomputed_contract(RecomputedFeatureContract {
            market: &market("SOL"),
            sleeve: Sleeve::FifteenMinute,
            decision_at: fixture.decision.event_time(),
            universe_activation_digest: &digest('a'),
            feature_schema_digest: &digest('b'),
            input_range_digest: &digest('c'),
            long_history_digest: &digest('d'),
        })
        .expect("matching recomputation contract");
}

#[test]
fn witness_rejects_wrong_decision_market() {
    let fixture = fixture(false);
    let witness = FeatureInputWitness::new(
        fixture.decision.event_id().clone(),
        market("BTC"),
        Sleeve::FifteenMinute,
        fixture.decision.event_time(),
        digest('a'),
        digest('b'),
        digest('c'),
        digest('d'),
        fixture.references.clone(),
    )
    .expect("syntactically valid witness");
    assert!(matches!(
        witness.verify_against(&fixture.plan),
        Err(FeatureReplayError::DecisionCoordinateMismatch)
    ));
}

#[test]
fn witness_rejects_wrong_decision_time() {
    let fixture = fixture(false);
    let witness = FeatureInputWitness::new(
        fixture.decision.event_id().clone(),
        market("SOL"),
        Sleeve::FifteenMinute,
        timestamp(FIFTEEN_MINUTES_NS + 1),
        digest('a'),
        digest('b'),
        digest('c'),
        digest('d'),
        fixture.references.clone(),
    )
    .expect("syntactically valid witness");
    assert!(matches!(
        witness.verify_against(&fixture.plan),
        Err(FeatureReplayError::DecisionCoordinateMismatch)
    ));
}

#[test]
fn witness_rejects_wrong_universe_recomputation_commitment() {
    let fixture = fixture(false);
    let verified = witness(&fixture)
        .verify_against(&fixture.plan)
        .expect("verified inputs");
    assert!(matches!(
        verified.verify_recomputed_contract(RecomputedFeatureContract {
            market: &market("SOL"),
            sleeve: Sleeve::FifteenMinute,
            decision_at: fixture.decision.event_time(),
            universe_activation_digest: &digest('e'),
            feature_schema_digest: &digest('b'),
            input_range_digest: &digest('c'),
            long_history_digest: &digest('d'),
        }),
        Err(FeatureReplayError::RecomputedContractMismatch)
    ));
}

#[test]
fn witness_rejects_late_source_fact() {
    let fixture = fixture(true);
    assert!(matches!(
        witness(&fixture).verify_against(&fixture.plan),
        Err(FeatureReplayError::LateSource)
    ));
}

#[test]
fn witness_rejects_digest_tampering() {
    let fixture = fixture(false);
    let mut wire = serde_json::to_value(witness(&fixture)).expect("witness wire");
    wire["input_range_digest"] = serde_json::Value::String(digest('e'));
    let tampered: FeatureInputWitness = serde_json::from_value(wire).expect("syntactic wire");
    assert!(matches!(
        tampered.verify_against(&fixture.plan),
        Err(FeatureReplayError::InvalidWitness { .. })
    ));
}
