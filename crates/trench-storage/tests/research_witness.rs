#![allow(dead_code)]

mod support;

#[cfg(unix)]
use std::{fs, os::unix::fs::PermissionsExt};

use rust_decimal_macros::dec;
use tempfile::TempDir;
use trench_core::{
    book::OrderBook,
    domain::{Leverage, Market, Price, Quantity, Usdc},
    event::{
        AssetContext, BookLevel, BookSnapshot, DurationNs, FundingRate, MarketEvent, Metadata,
    },
    risk::sizing::{
        ConservativeCosts, ImpactBand, ImpactCurve, RiskLimits, RiskRequest, RiskSnapshot,
        VenueConstraints,
    },
    universe::ListingState,
};
use trench_storage::{
    parquet::{DataProvenance, ParquetStore},
    research_plan::{ResearchMemberLocator, ResearchSourcePlanBuilder},
    research_runs::AvailabilitySourceReference,
    research_sidecar::{AvailabilityCutoff, UniverseCandidateInput, UniverseDepthInput},
    research_witness::{UniverseCandidateEvidence, compile_risk_policy, compile_universe},
};

use support::{market, range, timestamp};

fn digest(character: char) -> String {
    format!("b3:{}", character.to_string().repeat(64))
}

fn core_digest(character: char) -> String {
    character.to_string().repeat(64)
}

fn source_plan(
    root: &TempDir,
    store: &ParquetStore,
    manifests: &[trench_storage::parquet::PartitionManifest],
) -> trench_storage::research_runs::VerifiedResearchSourcePlan {
    ResearchSourcePlanBuilder::new(range(0, 1), range(1, 2))
        .expect("source windows")
        .build(
            store,
            manifests
                .iter()
                .map(ResearchMemberLocator::legacy)
                .collect(),
            Vec::new(),
        )
        .expect("source plan draft")
        .publish_to(store, root.path().join("source-plan"))
        .expect("verified source plan")
}

fn candidate() -> UniverseCandidateInput {
    let depth = UniverseDepthInput::new(dec!(50_000), dec!(75_000), dec!(100_000))
        .expect("monotonic depth");
    UniverseCandidateInput::new(
        market(),
        true,
        ListingState::Active,
        true,
        true,
        true,
        20,
        30,
        dec!(0.995),
        true,
        dec!(0.999),
        dec!(6_000_000),
        dec!(2_000_000),
        dec!(10),
        depth.clone(),
        depth,
    )
    .expect("candidate input")
}

fn source_events() -> Vec<MarketEvent> {
    let market = Market::new("SOL").expect("market");
    vec![
        MarketEvent::metadata(
            timestamp(0),
            timestamp(0),
            market.clone(),
            Metadata::new(3, 20, true),
        )
        .expect("metadata"),
        MarketEvent::asset_context(
            timestamp(0),
            timestamp(0),
            market.clone(),
            AssetContext::new(
                Price::new(dec!(100)).expect("mark"),
                Price::new(dec!(100)).expect("oracle"),
                Some(Price::new(dec!(100)).expect("mid")),
                Quantity::new(dec!(20_000)).expect("open interest"),
                Usdc::new(dec!(6_000_000)).expect("volume"),
                FundingRate::new(dec!(0)),
            ),
        )
        .expect("context"),
        MarketEvent::book_snapshot(
            timestamp(0),
            timestamp(0),
            market,
            BookSnapshot::new(
                1,
                vec![BookLevel::new(
                    Price::new(dec!(99)).expect("bid"),
                    Quantity::new(dec!(1_000)).expect("bid quantity"),
                )],
                vec![BookLevel::new(
                    Price::new(dec!(101)).expect("ask"),
                    Quantity::new(dec!(1_000)).expect("ask quantity"),
                )],
            ),
        )
        .expect("book"),
    ]
}

fn source_references(
    store: &ParquetStore,
    manifests: &[trench_storage::parquet::PartitionManifest],
    events: &[MarketEvent],
) -> Vec<AvailabilitySourceReference> {
    events
        .iter()
        .map(|event| {
            let manifest = manifests
                .iter()
                .find(|manifest| {
                    store
                        .read_partition(manifest)
                        .expect("partition")
                        .iter()
                        .any(|candidate| candidate.event_id() == event.event_id())
                })
                .expect("event manifest");
            AvailabilitySourceReference::new(
                manifest.manifest_digest(),
                event.received_at(),
                event.event_time(),
                event.event_id().clone(),
            )
            .expect("source reference")
        })
        .collect()
}

#[test]
fn source_run_populates_and_activates_universe_from_checked_raw_inputs() {
    let root = TempDir::new().expect("temporary root");
    #[cfg(unix)]
    fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700)).expect("private root");
    let provenance = DataProvenance::new(digest('a'), digest('b'), ParquetStore::schema_hash())
        .expect("provenance");
    let store = ParquetStore::open(root.path(), provenance).expect("store");
    let events = source_events();
    let manifests = store.write_events(&events).expect("source partitions");
    let plan = source_plan(&root, &store, &manifests);
    let references = source_references(&store, &manifests, &events);
    let evidence =
        UniverseCandidateEvidence::new(candidate(), references).expect("candidate evidence");
    let compiled = compile_universe(&plan, timestamp(0), timestamp(0), None, vec![evidence])
        .expect("universe compile");

    assert_eq!(compiled.snapshot().as_of_time(), timestamp(0));
    assert!(compiled.activation().is_effective_for(timestamp(0)));
    assert_eq!(compiled.candidates().len(), 1);

    let witness = compiled
        .sidecar_witness(
            "universe-record",
            AvailabilityCutoff::new(timestamp(0), timestamp(0), events[0].event_id().clone())
                .expect("cutoff"),
            range(0, 1),
        )
        .expect("sidecar witness");
    assert_eq!(witness.candidate_inputs().len(), 1);
}

#[test]
fn source_run_reopens_risk_policy_and_binds_the_exact_book_reference() {
    let root = TempDir::new().expect("temporary root");
    #[cfg(unix)]
    fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700)).expect("private root");
    let provenance = DataProvenance::new(digest('a'), digest('b'), ParquetStore::schema_hash())
        .expect("provenance");
    let store = ParquetStore::open(root.path(), provenance).expect("store");
    let events = source_events();
    let manifests = store.write_events(&events).expect("source partitions");
    let plan = source_plan(&root, &store, &manifests);
    let references = source_references(&store, &manifests, &events);
    let mut sorted_references = references.clone();
    sorted_references.sort();
    let book_reference = sorted_references
        .iter()
        .find(|reference| reference.event_id() == *events[2].event_id())
        .expect("book reference")
        .clone();
    let book_index = sorted_references
        .iter()
        .position(|reference| reference == &book_reference)
        .expect("book index");
    let executable_book = OrderBook::apply_snapshot(
        None,
        &events[2],
        DurationNs::new(i64::MAX as i128).expect("book horizon"),
    )
    .expect("book");
    let tiers = trench_core::risk::liquidation::MaintenanceTiers::new(vec![
        trench_core::risk::liquidation::MaintenanceTier::new(
            Usdc::new(dec!(0)).expect("tier lower"),
            None,
            dec!(0.025),
            Usdc::new(dec!(0)).expect("tier deduction"),
        )
        .expect("tier"),
    ])
    .expect("tiers");
    let policy = RiskRequest::new(
        RiskSnapshot::new(
            timestamp(0),
            timestamp(1),
            Usdc::new(dec!(100)).expect("equity"),
            core_digest('c'),
            executable_book.commitment_digest(),
            core_digest('d'),
            core_digest('e'),
            core_digest('f'),
        )
        .expect("snapshot"),
        VenueConstraints::new(
            3,
            Usdc::new(dec!(1)).expect("minimum"),
            Usdc::new(dec!(500)).expect("maximum"),
            Usdc::new(dec!(500)).expect("executable"),
            Leverage::new(20).expect("leverage"),
            tiers,
        )
        .expect("constraints"),
        ConservativeCosts::new(
            dec!(0.00075),
            dec!(0.00075),
            ImpactCurve::new(vec![
                ImpactBand::new(None, dec!(0.0005), dec!(0.001)).expect("impact band"),
            ])
            .expect("impact curve"),
            dec!(0.0001),
            dec!(0.0002),
            4,
        )
        .expect("costs"),
        RiskLimits::new(
            Usdc::new(dec!(1)).expect("risk budget"),
            dec!(0.25),
            dec!(2.5),
        )
        .expect("limits"),
    )
    .into_policy();
    let artifact = policy.canonical_json().expect("canonical policy");
    let compiled = compile_risk_policy(
        &plan,
        &artifact,
        &market(),
        timestamp(0),
        references.clone(),
    )
    .expect("risk compile");
    assert_eq!(compiled.source_references(), sorted_references.as_slice());
    let witness = compiled
        .sidecar_witness(
            "risk-record",
            trench_core::domain::EventId::new("b3:".to_owned() + &"1".repeat(64))
                .expect("decision id"),
            timestamp(0),
            AvailabilityCutoff::new(timestamp(0), timestamp(0), events[0].event_id().clone())
                .expect("cutoff"),
            range(0, 1),
            sorted_references[..book_index].to_vec(),
            book_reference,
            sorted_references[book_index + 1..].to_vec(),
            Vec::new(),
        )
        .expect("risk witness");
    assert!(witness.expected_policy_digest().starts_with("b3:"));
}
