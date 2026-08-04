use std::fs;
use std::path::Path;

#[cfg(unix)]
use std::os::unix::fs::{PermissionsExt, symlink};

use tempfile::TempDir;
use trench_core::{
    domain::{EventId, Market, Price, Quantity, Side},
    event::{MarketEvent, TimestampNs, Trade},
    validation::TimeRange,
};
use trench_storage::{
    parquet::{DataProvenance, ParquetStore},
    research_plan::{ResearchMemberLocator, ResearchSourcePlanBuilder},
    research_runs::VerifiedResearchSourcePlan,
    research_sidecar::{
        AvailabilityCutoff, DecisionIndexShard, DecisionWitnessIndex, ExcludedGap, ExclusionReason,
        FeatureWitness, RawRiskWitness, RecoverySource, RecoveryStatus, RecoveryWitness,
        ResearchSidecar, ResearchSidecarWriter, UniverseWitness, WitnessKind, WitnessReference,
        WitnessShard,
    },
};

fn digest(character: char) -> String {
    format!("b3:{}", character.to_string().repeat(64))
}

fn event_id(character: char) -> EventId {
    EventId::new(digest(character)).expect("fixture event identifier")
}

fn timestamp(value: i64) -> TimestampNs {
    TimestampNs::new(i128::from(value)).expect("fixture timestamp")
}

fn range(start: i64, end: i64) -> TimeRange {
    TimeRange::new(timestamp(start), timestamp(end)).expect("fixture range")
}

fn cutoff(received_at: i64, event_time: i64, id: EventId) -> AvailabilityCutoff {
    AvailabilityCutoff::new(timestamp(received_at), timestamp(event_time), id)
        .expect("fixture availability cutoff")
}

fn trade(event_time: i64, received_at: i64, trade_id: u64) -> MarketEvent {
    MarketEvent::trade(
        timestamp(event_time),
        timestamp(received_at),
        Market::new("SOL").expect("fixture market"),
        Trade::new(
            trade_id,
            Side::Buy,
            Price::new(rust_decimal_macros::dec!(100)).expect("fixture price"),
            Quantity::new(rust_decimal_macros::dec!(1)).expect("fixture quantity"),
        )
        .expect("fixture trade"),
    )
    .expect("fixture event")
}

fn secure(root: &TempDir) {
    #[cfg(not(unix))]
    let _ = root;
    #[cfg(unix)]
    fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700))
        .expect("fixture root should be private");
}

fn private_directory(path: &Path) {
    fs::create_dir(path).expect("fixture directory");
    #[cfg(unix)]
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
        .expect("fixture directory should be private");
}

fn plan(root: &TempDir, output: &str, trade_id: u64) -> (ParquetStore, VerifiedResearchSourcePlan) {
    let provenance = DataProvenance::new(digest('a'), digest('b'), ParquetStore::schema_hash())
        .expect("fixture provenance");
    let store = ParquetStore::open(root.path(), provenance).expect("fixture store");
    let manifests = store
        .write_events(&[trade(100, 101, trade_id)])
        .expect("fixture partition");
    let draft = ResearchSourcePlanBuilder::new(range(0, 500), range(500, 1_000))
        .expect("fixture windows")
        .build(
            &store,
            vec![ResearchMemberLocator::legacy(&manifests[0])],
            Vec::new(),
        )
        .expect("fixture draft");
    let output = root.path().join(output);
    let plan = draft.publish_to(&store, output).expect("fixture plan");
    (store, plan)
}

fn witness_shards(decision_id: &EventId) -> Vec<WitnessShard> {
    let inputs = ['a', 'b', 'c', 'd']
        .into_iter()
        .map(event_id)
        .collect::<Vec<_>>();
    vec![
        WitnessShard::recovery(
            "recovery",
            vec![
                RecoveryWitness::new(
                    "recovery-record",
                    cutoff(100, 99, inputs[0].clone()),
                    range(0, 100),
                    "recovery-request",
                    RecoveryStatus::Complete,
                    RecoverySource::Captured,
                    timestamp(99),
                    inputs[0].clone(),
                    Vec::new(),
                    digest('e'),
                )
                .expect("recovery witness"),
            ],
        )
        .expect("recovery shard"),
        WitnessShard::universe(
            "universe",
            vec![
                UniverseWitness::new(
                    "universe-record",
                    timestamp(0),
                    cutoff(100, 99, inputs[1].clone()),
                    range(0, 100),
                    vec![inputs[1].clone()],
                    digest('f'),
                    digest('4'),
                )
                .expect("universe witness"),
            ],
        )
        .expect("universe shard"),
        WitnessShard::feature(
            "feature",
            vec![
                FeatureWitness::new(
                    "feature-record",
                    decision_id.clone(),
                    timestamp(99),
                    cutoff(100, 99, inputs[2].clone()),
                    range(0, 100),
                    vec![inputs[2].clone()],
                    digest('0'),
                    digest('1'),
                )
                .expect("feature witness"),
            ],
        )
        .expect("feature shard"),
        WitnessShard::risk(
            "risk",
            vec![
                RawRiskWitness::new(
                    "risk-record",
                    decision_id.clone(),
                    timestamp(99),
                    cutoff(100, 99, inputs[3].clone()),
                    range(0, 100),
                    Vec::new(),
                    inputs[3].clone(),
                    Vec::new(),
                    Vec::new(),
                    digest('2'),
                )
                .expect("risk witness"),
            ],
        )
        .expect("risk shard"),
    ]
}

fn decision_with_references(
    decision_character: char,
    cutoff_received_at: i64,
    recovery: WitnessReference,
    universe: WitnessReference,
    feature: WitnessReference,
    risk: WitnessReference,
) -> DecisionWitnessIndex {
    let inputs = ['a', 'b', 'c', 'd']
        .into_iter()
        .map(event_id)
        .collect::<Vec<_>>();
    DecisionWitnessIndex::new(
        event_id(decision_character),
        cutoff(cutoff_received_at, 99, event_id('d')),
        range(0, 100),
        inputs,
        recovery,
        universe,
        feature,
        risk,
    )
    .expect("fixture decision")
}

fn decision(decision_character: char, cutoff_received_at: i64) -> DecisionWitnessIndex {
    decision_with_references(
        decision_character,
        cutoff_received_at,
        WitnessReference::new("recovery", "recovery-record").expect("fixture reference"),
        WitnessReference::new("universe", "universe-record").expect("fixture reference"),
        WitnessReference::new("feature", "feature-record").expect("fixture reference"),
        WitnessReference::new("risk", "risk-record").expect("fixture reference"),
    )
}

fn writer(plan: &VerifiedResearchSourcePlan) -> ResearchSidecarWriter {
    ResearchSidecarWriter::new(plan)
        .expect("writer")
        .with_witness_shards(witness_shards(&event_id('9')))
        .expect("witness shards")
        .with_decision_index_shards(vec![
            DecisionIndexShard::new("decisions-0", vec![decision('9', 100)])
                .expect("decision index shard"),
        ])
        .expect("decision shards")
        .with_excluded_gaps(vec![
            ExcludedGap::new(range(200, 300), ExclusionReason::Unavailable).expect("excluded gap"),
        ])
        .expect("excluded gaps")
}

#[test]
fn atomic_publish_reopens_verified_sidecar() {
    let root = TempDir::new().expect("temporary root");
    secure(&root);
    let (_store, source_plan) = plan(&root, "source-plan", 1);
    let final_directory = root.path().join("sidecar");

    let sidecar = writer(&source_plan)
        .publish_to(&final_directory)
        .expect("atomically published sidecar");
    let reopened = ResearchSidecar::open_from(&final_directory, &source_plan)
        .expect("reopened verified sidecar");

    assert_eq!(sidecar.digest(), reopened.digest());
    assert_eq!(
        reopened.source_plan_digest(),
        source_plan.source_plan_digest()
    );
    assert_eq!(
        reopened
            .decision(event_id('9'))
            .expect("decision")
            .decision_id(),
        event_id('9').as_str()
    );
    assert_eq!(
        reopened
            .witness(&WitnessReference::new("feature", "feature-record").expect("reference"))
            .expect("typed raw feature witness")
            .kind(),
        WitnessKind::Feature
    );
    assert_eq!(reopened.excluded_gaps().len(), 1);
}

#[test]
fn excluded_only_sidecar_is_valid_and_adjacent_gaps_are_merged() {
    let root = TempDir::new().expect("temporary root");
    secure(&root);
    let (_store, source_plan) = plan(&root, "source-plan", 1);
    let final_directory = root.path().join("excluded-only");

    let sidecar = ResearchSidecarWriter::new(&source_plan)
        .expect("writer")
        .with_excluded_gaps(vec![
            ExcludedGap::new(range(0, 100), ExclusionReason::Unavailable).expect("first gap"),
            ExcludedGap::new(range(100, 200), ExclusionReason::Unavailable).expect("second gap"),
        ])
        .expect("gaps")
        .publish_to(&final_directory)
        .expect("excluded-only sidecar");

    assert!(sidecar.decision_indices().is_empty());
    assert_eq!(sidecar.excluded_gaps().len(), 1);
    assert_eq!(sidecar.excluded_gaps()[0].range(), range(0, 200));
}

#[test]
fn identical_retry_is_idempotent_but_an_empty_final_directory_conflicts() {
    let root = TempDir::new().expect("temporary root");
    secure(&root);
    let (_store, source_plan) = plan(&root, "source-plan", 1);
    let final_directory = root.path().join("sidecar");
    let first = writer(&source_plan)
        .publish_to(&final_directory)
        .expect("first sidecar");
    let retry = writer(&source_plan)
        .publish_to(&final_directory)
        .expect("idempotent retry");
    assert_eq!(first.digest(), retry.digest());

    let conflicting = root.path().join("conflicting");
    private_directory(&conflicting);
    assert!(writer(&source_plan).publish_to(&conflicting).is_err());
}

#[test]
fn interrupted_stage_and_tampered_payload_are_unreadable() {
    let root = TempDir::new().expect("temporary root");
    secure(&root);
    let (_store, source_plan) = plan(&root, "source-plan", 1);
    let staged = root.path().join(".sidecar-interrupted.tmp");
    private_directory(&staged);
    assert!(ResearchSidecar::open_from(&staged, &source_plan).is_err());

    let final_directory = root.path().join("sidecar");
    writer(&source_plan)
        .publish_to(&final_directory)
        .expect("published sidecar");
    fs::write(final_directory.join("witness-recovery.json"), b"tampered")
        .expect("tamper witness payload");
    assert!(ResearchSidecar::open_from(&final_directory, &source_plan).is_err());
}

#[test]
fn source_plan_drift_and_oversized_manifest_are_rejected() {
    let root = TempDir::new().expect("temporary root");
    secure(&root);
    let (_store, source_plan) = plan(&root, "source-plan", 1);
    let final_directory = root.path().join("sidecar");
    writer(&source_plan)
        .publish_to(&final_directory)
        .expect("published sidecar");

    let other_root = TempDir::new().expect("other temporary root");
    secure(&other_root);
    let (_other_store, drifted_plan) = plan(&other_root, "source-plan", 2);
    assert!(ResearchSidecar::open_from(&final_directory, &drifted_plan).is_err());

    fs::write(
        final_directory.join("research-sidecar.json"),
        vec![b'x'; 1_048_577],
    )
    .expect("oversized manifest");
    assert!(ResearchSidecar::open_from(&final_directory, &source_plan).is_err());
}

#[test]
fn duplicate_or_out_of_order_decisions_are_rejected_across_index_shards() {
    let root = TempDir::new().expect("temporary root");
    secure(&root);
    let (_store, source_plan) = plan(&root, "source-plan", 1);

    let duplicate = ResearchSidecarWriter::new(&source_plan)
        .expect("writer")
        .with_witness_shards(witness_shards(&event_id('9')))
        .expect("witnesses")
        .with_decision_index_shards(vec![
            DecisionIndexShard::new("decisions-0", vec![decision('9', 100)]).expect("first shard"),
            DecisionIndexShard::new("decisions-1", vec![decision('9', 101)]).expect("second shard"),
        ])
        .expect("decision shards");
    assert!(duplicate.publish_to(root.path().join("duplicate")).is_err());

    let out_of_order = ResearchSidecarWriter::new(&source_plan)
        .expect("writer")
        .with_witness_shards(witness_shards(&event_id('8')))
        .expect("witnesses")
        .with_decision_index_shards(vec![
            DecisionIndexShard::new("decisions-0", vec![decision('8', 200)]).expect("later shard"),
            DecisionIndexShard::new("decisions-1", vec![decision('8', 100)])
                .expect("earlier shard"),
        ])
        .expect("decision shards");
    assert!(
        out_of_order
            .publish_to(root.path().join("out-of-order"))
            .is_err()
    );
}

#[test]
fn misbound_references_or_late_witnesses_are_rejected() {
    let root = TempDir::new().expect("temporary root");
    secure(&root);
    let (_store, source_plan) = plan(&root, "source-plan", 1);

    let misbound = decision_with_references(
        '9',
        100,
        WitnessReference::new("universe", "universe-record").expect("wrong reference"),
        WitnessReference::new("universe", "universe-record").expect("universe reference"),
        WitnessReference::new("feature", "feature-record").expect("feature reference"),
        WitnessReference::new("risk", "risk-record").expect("risk reference"),
    );
    let writer = ResearchSidecarWriter::new(&source_plan)
        .expect("writer")
        .with_witness_shards(witness_shards(&event_id('9')))
        .expect("witnesses")
        .with_decision_index_shards(vec![
            DecisionIndexShard::new("decisions-0", vec![misbound]).expect("decision shard"),
        ])
        .expect("decision shards");
    assert!(writer.publish_to(root.path().join("misbound")).is_err());

    let late_feature = FeatureWitness::new(
        "feature-record",
        event_id('9'),
        timestamp(99),
        cutoff(101, 100, event_id('c')),
        range(0, 100),
        vec![event_id('c')],
        digest('0'),
        digest('1'),
    )
    .expect("late fixture record");
    let mut shards = witness_shards(&event_id('9'));
    shards.retain(|shard| shard.kind() != WitnessKind::Feature);
    shards.push(WitnessShard::feature("feature", vec![late_feature]).expect("late feature shard"));
    let writer = ResearchSidecarWriter::new(&source_plan)
        .expect("writer")
        .with_witness_shards(shards)
        .expect("witnesses")
        .with_decision_index_shards(vec![
            DecisionIndexShard::new("decisions-0", vec![decision('9', 100)])
                .expect("decision shard"),
        ])
        .expect("decision shards");
    assert!(writer.publish_to(root.path().join("late")).is_err());
}

#[test]
fn decision_inputs_must_exactly_bind_referenced_raw_inputs() {
    let root = TempDir::new().expect("temporary root");
    secure(&root);
    let (_store, source_plan) = plan(&root, "source-plan", 1);
    let references = [
        WitnessReference::new("recovery", "recovery-record").expect("recovery reference"),
        WitnessReference::new("universe", "universe-record").expect("universe reference"),
        WitnessReference::new("feature", "feature-record").expect("feature reference"),
        WitnessReference::new("risk", "risk-record").expect("risk reference"),
    ];
    let missing_anchor = DecisionWitnessIndex::new(
        event_id('9'),
        cutoff(100, 99, event_id('d')),
        range(0, 100),
        vec![event_id('b'), event_id('c'), event_id('d')],
        references[0].clone(),
        references[1].clone(),
        references[2].clone(),
        references[3].clone(),
    )
    .expect("well-formed but incomplete index");
    let writer = ResearchSidecarWriter::new(&source_plan)
        .expect("writer")
        .with_witness_shards(witness_shards(&event_id('9')))
        .expect("witnesses")
        .with_decision_index_shards(vec![
            DecisionIndexShard::new("decisions-0", vec![missing_anchor]).expect("decision shard"),
        ])
        .expect("decision shards");
    assert!(
        writer
            .publish_to(root.path().join("missing-anchor"))
            .is_err()
    );

    let unrelated_input = DecisionWitnessIndex::new(
        event_id('9'),
        cutoff(100, 99, event_id('d')),
        range(0, 100),
        vec![
            event_id('a'),
            event_id('b'),
            event_id('c'),
            event_id('d'),
            event_id('e'),
        ],
        references[0].clone(),
        references[1].clone(),
        references[2].clone(),
        references[3].clone(),
    )
    .expect("well-formed but over-inclusive index");
    let writer = ResearchSidecarWriter::new(&source_plan)
        .expect("writer")
        .with_witness_shards(witness_shards(&event_id('9')))
        .expect("witnesses")
        .with_decision_index_shards(vec![
            DecisionIndexShard::new("decisions-0", vec![unrelated_input]).expect("decision shard"),
        ])
        .expect("decision shards");
    assert!(
        writer
            .publish_to(root.path().join("unrelated-input"))
            .is_err()
    );
}

#[cfg(unix)]
#[test]
fn symbolic_link_sidecar_component_is_rejected() {
    let root = TempDir::new().expect("temporary root");
    secure(&root);
    let (_store, source_plan) = plan(&root, "source-plan", 1);
    let final_directory = root.path().join("sidecar");
    writer(&source_plan)
        .publish_to(&final_directory)
        .expect("published sidecar");
    let witness = final_directory.join("witness-recovery.json");
    fs::remove_file(&witness).expect("remove original witness");
    symlink("/dev/null", &witness).expect("replace witness with symlink");

    assert!(ResearchSidecar::open_from(&final_directory, &source_plan).is_err());
}
