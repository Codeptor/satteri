mod support;

use std::collections::BTreeMap;

#[cfg(unix)]
use std::{fs, os::unix::fs::PermissionsExt};

use rust_decimal_macros::dec;
use tempfile::TempDir;
use trench_core::{
    domain::{Market, Price, Quantity},
    event::{CandleInterval, CompletedCandle, MarketEvent},
};
use trench_storage::{
    parquet::{DataProvenance, ParquetStore, PartitionManifest},
    recovery_outcomes::{
        RecoveryOutcomeSource, RecoveryOutcomeStore, RecoveryRequestCursors,
        RecoverySourceReference,
    },
    research_compile::{
        ResearchEvidenceCompiler, ResearchExclusionReason, TypedWitnessKind, TypedWitnessStatus,
    },
    research_plan::{ResearchMemberLocator, ResearchSourcePlanBuilder},
    research_runs::AvailabilityKey,
};

use support::{
    FIFTEEN_MINUTES_NS, ONE_HOUR_NS, VerifiedRecoveryFixture, book, market, range, timestamp,
    trade, verified_recovery,
};

fn digest(character: char) -> String {
    format!("b3:{}", character.to_string().repeat(64))
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

fn candle(open_at: i64, received_at: i64) -> MarketEvent {
    let candle = CompletedCandle::new(
        CandleInterval::FifteenMinutes,
        timestamp(open_at),
        Price::new(dec!(100)).expect("open"),
        Price::new(dec!(101)).expect("high"),
        Price::new(dec!(99)).expect("low"),
        Price::new(dec!(100)).expect("close"),
        Quantity::new(dec!(1)).expect("volume"),
        1,
    )
    .expect("candle");
    MarketEvent::completed_candle(
        timestamp(open_at + FIFTEEN_MINUTES_NS),
        timestamp(received_at),
        market(),
        candle,
    )
    .expect("completed candle")
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

fn manifest_for<'a>(
    store: &ParquetStore,
    manifests: &'a [PartitionManifest],
    event: &MarketEvent,
) -> &'a PartitionManifest {
    manifests
        .iter()
        .find(|manifest| {
            store
                .read_partition(manifest)
                .expect("reopen source partition")
                .iter()
                .any(|candidate| candidate.event_id() == event.event_id())
        })
        .expect("source manifest")
}

fn publish_plan(
    root: &TempDir,
    store: &ParquetStore,
    manifests: &[PartitionManifest],
    recovery_outcomes: Vec<trench_storage::recovery_outcomes::RecoveryOutcomeLocator>,
) -> trench_storage::research_runs::VerifiedResearchSourcePlan {
    let builder =
        ResearchSourcePlanBuilder::new(range(0, ONE_HOUR_NS), range(ONE_HOUR_NS, ONE_HOUR_NS * 2))
            .expect("windows");
    let builder = if recovery_outcomes.is_empty() {
        builder
    } else {
        builder
            .with_recovery_outcomes(recovery_outcomes)
            .expect("recovery outcomes")
    };
    builder
        .build(
            store,
            manifests
                .iter()
                .map(ResearchMemberLocator::legacy)
                .collect(),
            Vec::new(),
        )
        .expect("source-plan draft")
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
    let plan = publish_plan(&root, &store, &manifests, Vec::new());
    (root, plan)
}

struct RecoveryPlanFixture {
    root: TempDir,
    store: ParquetStore,
    manifests: Vec<PartitionManifest>,
    recovery: VerifiedRecoveryFixture,
    predecessor: RecoverySourceReference,
    snapshot: RecoverySourceReference,
    local_trades: Vec<RecoverySourceReference>,
    official_candles: Vec<RecoverySourceReference>,
    raw_proof: BTreeMap<RecoverySourceReference, MarketEvent>,
}

impl RecoveryPlanFixture {
    fn new(extra_events: Vec<MarketEvent>) -> Self {
        let (root, store) = store();
        let recovery = verified_recovery();
        let events = std::iter::once(recovery.predecessor.clone())
            .chain(std::iter::once(recovery.snapshot.clone()))
            .chain(recovery.local_trades.iter().cloned())
            .chain(recovery.official_candles.iter().cloned())
            .chain(extra_events)
            .collect::<Vec<_>>();
        let manifests = store.write_events(&events).expect("source events");
        let source_ref = |event: &MarketEvent| {
            source_reference(
                event,
                &manifest_for(&store, &manifests, event).manifest_digest(),
            )
        };
        let predecessor = source_ref(&recovery.predecessor);
        let snapshot = source_ref(&recovery.snapshot);
        let mut local_pairs = recovery
            .local_trades
            .iter()
            .map(|event| (source_ref(event), event.clone()))
            .collect::<Vec<_>>();
        local_pairs.sort_by(|left, right| left.0.key().event_id().cmp(right.0.key().event_id()));
        let local_trades = local_pairs
            .iter()
            .map(|(reference, _)| reference.clone())
            .collect::<Vec<_>>();
        let mut official_pairs = recovery
            .official_candles
            .iter()
            .map(|event| (source_ref(event), event.clone()))
            .collect::<Vec<_>>();
        official_pairs.sort_by(|left, right| left.0.key().event_id().cmp(right.0.key().event_id()));
        let official_candles = official_pairs
            .iter()
            .map(|(reference, _)| reference.clone())
            .collect::<Vec<_>>();
        let raw_proof = std::iter::once((&predecessor, &recovery.predecessor))
            .chain(std::iter::once((&snapshot, &recovery.snapshot)))
            .chain(
                local_pairs
                    .iter()
                    .map(|(reference, event)| (reference, event)),
            )
            .chain(
                official_pairs
                    .iter()
                    .map(|(reference, event)| (reference, event)),
            )
            .map(|(reference, event)| (reference.clone(), event.clone()))
            .collect();
        Self {
            root,
            store,
            manifests,
            recovery,
            predecessor,
            snapshot,
            local_trades,
            official_candles,
            raw_proof,
        }
    }

    fn availability_anchor(&self) -> RecoverySourceReference {
        std::iter::once(&self.predecessor)
            .chain(std::iter::once(&self.snapshot))
            .chain(&self.local_trades)
            .chain(&self.official_candles)
            .max_by_key(|reference| reference.key())
            .expect("raw proof")
            .clone()
    }

    fn verified_plan(&self) -> trench_storage::research_runs::VerifiedResearchSourcePlan {
        let locator = RecoveryOutcomeStore::open(&self.store)
            .expect("outcome store")
            .publish_verified(
                &self.recovery.result,
                Some(self.predecessor.clone()),
                self.predecessor.clone(),
                self.snapshot.clone(),
                self.local_trades.clone(),
                self.official_candles.clone(),
                self.availability_anchor(),
                &self.raw_proof,
            )
            .expect("publish verified witness");
        publish_plan(&self.root, &self.store, &self.manifests, vec![locator])
    }
}

fn unavailable_plan() -> (
    TempDir,
    trench_storage::research_runs::VerifiedResearchSourcePlan,
) {
    let (root, store) = store();
    let predecessor = trade(1, 1, 1);
    let snapshot = book(1_000, 1_000, 1);
    let post_recovery_book = book(
        ONE_HOUR_NS + FIFTEEN_MINUTES_NS - 500_000_000,
        ONE_HOUR_NS + FIFTEEN_MINUTES_NS - 500_000_000,
        9,
    );
    let decision = candle(ONE_HOUR_NS, ONE_HOUR_NS + FIFTEEN_MINUTES_NS);
    let manifests = store
        .write_events(&[
            predecessor.clone(),
            snapshot.clone(),
            post_recovery_book,
            decision,
        ])
        .expect("source events");
    let predecessor = source_reference(
        &predecessor,
        &manifest_for(&store, &manifests, &predecessor).manifest_digest(),
    );
    let snapshot = source_reference(
        &snapshot,
        &manifest_for(&store, &manifests, &snapshot).manifest_digest(),
    );
    let locator = RecoveryOutcomeStore::open(&store)
        .expect("outcome store")
        .publish_unavailable(
            "SOL:1",
            1,
            Market::new("SOL").expect("market"),
            RecoveryRequestCursors::new(Some(predecessor.clone()), Some(predecessor))
                .expect("request cursors"),
            RecoveryOutcomeSource::Unavailable,
            timestamp(ONE_HOUR_NS),
            snapshot.clone(),
            snapshot,
        )
        .expect("publish unavailable");
    let plan = publish_plan(&root, &store, &manifests, vec![locator]);
    (root, plan)
}

#[test]
fn late_candle_is_excluded_at_original_boundary() {
    let late = candle(0, FIFTEEN_MINUTES_NS + 1);
    let (_root, source_plan) = plan_with(&[late]);
    let compiled = ResearchEvidenceCompiler::new()
        .compile(&source_plan)
        .expect("causal compilation");
    assert_eq!(
        compiled.excluded_gaps()[0].reason(),
        ResearchExclusionReason::LateSource
    );
    assert_eq!(
        compiled.typed_witness_status(),
        TypedWitnessStatus::NoTimelyDecisions
    );
}

#[test]
fn exact_boundary_candle_never_admits_the_first_later_fact() {
    let candle = candle(0, FIFTEEN_MINUTES_NS);
    let (_root, source_plan) = plan_with(&[
        candle.clone(),
        trade(FIFTEEN_MINUTES_NS + 1, FIFTEEN_MINUTES_NS + 1, 9),
    ]);
    let compiled = ResearchEvidenceCompiler::new()
        .compile(&source_plan)
        .expect("causal compilation");
    assert_eq!(
        compiled.decisions()[0].source_event_ids(),
        &[candle.event_id().clone()]
    );
    assert_eq!(
        compiled.typed_witness_status(),
        TypedWitnessStatus::Pending {
            decision_count: 1,
            missing: vec![
                TypedWitnessKind::Recovery,
                TypedWitnessKind::Universe,
                TypedWitnessKind::Feature,
                TypedWitnessKind::Risk,
            ],
        }
    );
}

#[test]
fn verified_companion_recovery_releases_only_after_its_availability_anchor() {
    let fixture = RecoveryPlanFixture::new(vec![
        book(
            ONE_HOUR_NS + FIFTEEN_MINUTES_NS - 500_000_000,
            ONE_HOUR_NS + FIFTEEN_MINUTES_NS - 500_000_000,
            9,
        ),
        candle(ONE_HOUR_NS, ONE_HOUR_NS + FIFTEEN_MINUTES_NS),
    ]);
    let source_plan = fixture.verified_plan();
    let compiled = ResearchEvidenceCompiler::new()
        .compile(&source_plan)
        .expect("causal compilation");
    assert_eq!(compiled.recovery_witnesses().len(), 1);
    assert!(
        compiled
            .decisions()
            .iter()
            .any(|decision| decision.decision_at() > timestamp(ONE_HOUR_NS))
    );
}

#[test]
fn unavailable_companion_is_auditable_but_never_releases_entries() {
    let (_root, source_plan) = unavailable_plan();
    let compiled = ResearchEvidenceCompiler::new()
        .compile(&source_plan)
        .expect("causal compilation");
    assert!(compiled.recovery_witnesses().is_empty());
    assert_eq!(
        compiled.excluded_gaps()[0].reason(),
        ResearchExclusionReason::RecoveryFence
    );
}
