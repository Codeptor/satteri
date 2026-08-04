use std::fs;
#[cfg(unix)]
use std::os::unix::fs::{PermissionsExt, symlink};

use rust_decimal_macros::dec;
use serde_json::Value;
use tempfile::TempDir;
use trench_core::domain::{EventId, Market, Price, Quantity, Side};
use trench_core::event::{Bbo, BookLevel, MarketEvent, TimestampNs, Trade};
use trench_core::validation::TimeRange;
use trench_storage::parquet::{
    CaptureBatchManifest, DataProvenance, ParquetStore, PartitionManifest,
};
use trench_storage::research_plan::{
    CompleteCoverage, ContinuityMemberBinding, ContinuityProof, ContinuitySource,
    CoverageDeclaration, CoverageEventRef, CoverageTarget, CoverageUnavailableReason,
    CoverageWitness, ResearchMemberLocator, ResearchPlanError, ResearchSourcePlanBuilder,
    SourceStreamKind,
};

fn provenance() -> DataProvenance {
    DataProvenance::new(digest('a'), digest('b'), ParquetStore::schema_hash())
        .expect("fixture provenance should be valid")
}

fn digest(character: char) -> String {
    format!("b3:{}", character.to_string().repeat(64))
}

fn timestamp(value: i64) -> TimestampNs {
    TimestampNs::new(i128::from(value)).expect("fixture timestamp should be valid")
}

fn trade(at: i64, trade_id: u64) -> MarketEvent {
    trade_for("SOL", at, trade_id)
}

fn trade_for(market: &str, at: i64, trade_id: u64) -> MarketEvent {
    MarketEvent::trade(
        timestamp(at),
        timestamp(at + 1),
        Market::new(market).expect("fixture market should be valid"),
        Trade::new(
            trade_id,
            Side::Buy,
            Price::new(dec!(100)).expect("fixture price should be valid"),
            Quantity::new(dec!(2)).expect("fixture quantity should be valid"),
        )
        .expect("fixture trade should be valid"),
    )
    .expect("fixture event should be valid")
}

fn bbo(at: i64, sequence: u64) -> MarketEvent {
    MarketEvent::bbo(
        timestamp(at),
        timestamp(at + 1),
        Market::new("SOL").expect("fixture market should be valid"),
        Bbo::new(
            sequence,
            BookLevel::new(
                Price::new(dec!(99)).expect("fixture price should be valid"),
                Quantity::new(dec!(1)).expect("fixture quantity should be valid"),
            ),
            BookLevel::new(
                Price::new(dec!(101)).expect("fixture price should be valid"),
                Quantity::new(dec!(1)).expect("fixture quantity should be valid"),
            ),
        )
        .expect("fixture BBO should be valid"),
    )
    .expect("fixture event should be valid")
}

fn secure(root: &TempDir) {
    #[cfg(unix)]
    fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700))
        .expect("fixture root should be private");
}

fn legacy_partition_directory(root: &TempDir, manifest: &PartitionManifest) -> std::path::PathBuf {
    root.path()
        .join("partitions")
        .join("date=utc-day-0")
        .join("kind=trade")
        .join("market=534f4c")
        .join(format!("part-{}.part", manifest.partition_id()))
}

fn capture_partition_directory(
    root: &TempDir,
    batch_id: &str,
    manifest: &PartitionManifest,
) -> std::path::PathBuf {
    root.path()
        .join("capture-batches")
        .join(format!("batch-{batch_id}.batch"))
        .join("partitions")
        .join("date=utc-day-0")
        .join("kind=trade")
        .join("market=534f4c")
        .join(format!("part-{}.part", manifest.partition_id()))
}

fn capture_batch_id(provenance: &Value, partitions: &[Value]) -> String {
    let mut hasher = blake3::Hasher::new_derive_key("trench.parquet.capture-batch-id.v1");
    hasher.update(&[1]);
    for field in ["config_digest", "code_digest", "schema_hash"] {
        let value = provenance[field]
            .as_str()
            .expect("fixture provenance field should be a string");
        hasher.update(&(value.len() as u64).to_be_bytes());
        hasher.update(value.as_bytes());
    }
    for partition in partitions {
        let partition_id = partition["partition_id"]
            .as_str()
            .expect("fixture partition identifier should be a string");
        hasher.update(&(partition_id.len() as u64).to_be_bytes());
        hasher.update(partition_id.as_bytes());
    }
    format!("b3:{}", hasher.finalize().to_hex())
}

#[test]
fn direct_legacy_member_read_revalidates_partition() {
    let root = TempDir::new().expect("temporary root should be created");
    secure(&root);
    let store = ParquetStore::open(root.path(), provenance()).expect("store should open");
    let events = [trade(1_000, 1)];
    let [manifest]: [PartitionManifest; 1] = store
        .write_events(&events)
        .expect("legacy partition should commit")
        .try_into()
        .expect("fixture should create one partition");

    let opened = store
        .open_legacy_member(
            &manifest.identity(),
            manifest.partition_id(),
            &manifest.manifest_digest(),
        )
        .expect("verified legacy member");

    assert_eq!(opened.manifest(), &manifest);
    assert_eq!(opened.read_all().expect("canonical rows"), events);
}

#[test]
fn direct_capture_member_read_revalidates_batch_and_partition() {
    let root = TempDir::new().expect("temporary root should be created");
    secure(&root);
    let store = ParquetStore::open(root.path(), provenance()).expect("store should open");
    let events = [trade(1_000, 1)];
    let batch = store
        .write_capture_batch(&events)
        .expect("capture batch should commit");
    let [manifest]: [PartitionManifest; 1] = batch
        .partitions()
        .to_vec()
        .try_into()
        .expect("fixture should create one capture partition");

    let opened = store
        .open_capture_member(
            batch.batch_id(),
            &manifest.identity(),
            manifest.partition_id(),
            &batch.manifest_digest(),
            &manifest.manifest_digest(),
        )
        .expect("verified capture member");

    assert_eq!(opened.manifest(), &manifest);
    assert_eq!(opened.read_all().expect("canonical rows"), events);
}

#[test]
fn direct_capture_member_rejects_a_missing_unrelated_batch_member() {
    const NANOS_PER_DAY: i64 = 86_400_000_000_000;

    let root = TempDir::new().expect("temporary root should be created");
    secure(&root);
    let store = ParquetStore::open(root.path(), provenance()).expect("store should open");
    let selected_event = trade(1_000, 1);
    let unrelated_event = trade(NANOS_PER_DAY + 1_000, 2);
    let batch = store
        .write_capture_batch(&[selected_event.clone(), unrelated_event])
        .expect("capture batch should commit");
    let selected = batch
        .partitions()
        .iter()
        .find(|manifest| manifest.min_event_time() == selected_event.event_time())
        .expect("fixture capture should retain the selected partition");
    let unrelated = batch
        .partitions()
        .iter()
        .find(|manifest| manifest.min_event_time() == timestamp(NANOS_PER_DAY + 1_000))
        .expect("fixture capture should retain the unrelated partition");
    let unrelated_directory = root
        .path()
        .join("capture-batches")
        .join(format!("batch-{}.batch", batch.batch_id()))
        .join("partitions")
        .join("date=utc-day-1")
        .join("kind=trade")
        .join("market=534f4c")
        .join(format!("part-{}.part", unrelated.partition_id()));
    fs::remove_dir_all(unrelated_directory).expect("fixture should remove the unrelated member");

    assert!(
        store
            .open_capture_member(
                batch.batch_id(),
                &selected.identity(),
                selected.partition_id(),
                &batch.manifest_digest(),
                &selected.manifest_digest(),
            )
            .is_err()
    );
}

#[test]
fn direct_member_rejects_missing_partition() {
    let root = TempDir::new().expect("temporary root should be created");
    secure(&root);
    let store = ParquetStore::open(root.path(), provenance()).expect("store should open");
    let events = [trade(1_000, 1)];
    let [manifest]: [PartitionManifest; 1] = store
        .write_events(&events)
        .expect("legacy partition should commit")
        .try_into()
        .expect("fixture should create one partition");
    fs::remove_dir_all(legacy_partition_directory(&root, &manifest))
        .expect("fixture should remove selected member");

    let error = store
        .open_legacy_member(
            &manifest.identity(),
            manifest.partition_id(),
            &manifest.manifest_digest(),
        )
        .expect_err("missing selected member must fail");

    assert!(matches!(
        error,
        trench_storage::parquet::ParquetError::MissingPartition { .. }
    ));
}

#[test]
fn direct_member_rejects_partition_manifest_digest_drift() {
    let root = TempDir::new().expect("temporary root should be created");
    secure(&root);
    let store = ParquetStore::open(root.path(), provenance()).expect("store should open");
    let events = [trade(1_000, 1)];
    let [manifest]: [PartitionManifest; 1] = store
        .write_events(&events)
        .expect("legacy partition should commit")
        .try_into()
        .expect("fixture should create one partition");

    let error = store
        .open_legacy_member(&manifest.identity(), manifest.partition_id(), &digest('d'))
        .expect_err("partition manifest digest drift must fail");

    assert!(matches!(
        error,
        trench_storage::parquet::ParquetError::ManifestMismatch { .. }
    ));
}

#[test]
fn direct_capture_member_rejects_capture_manifest_digest_drift() {
    let root = TempDir::new().expect("temporary root should be created");
    secure(&root);
    let store = ParquetStore::open(root.path(), provenance()).expect("store should open");
    let events = [trade(1_000, 1)];
    let batch = store
        .write_capture_batch(&events)
        .expect("capture batch should commit");
    let [manifest]: [PartitionManifest; 1] = batch
        .partitions()
        .to_vec()
        .try_into()
        .expect("fixture should create one capture partition");

    let error = store
        .open_capture_member(
            batch.batch_id(),
            &manifest.identity(),
            manifest.partition_id(),
            &digest('d'),
            &manifest.manifest_digest(),
        )
        .expect_err("capture manifest digest drift must fail");

    assert!(matches!(
        error,
        trench_storage::parquet::ParquetError::CaptureBatchManifestMismatch { .. }
    ));
}

#[test]
fn direct_capture_member_rejects_duplicate_partition_identity() {
    let root = TempDir::new().expect("temporary root should be created");
    secure(&root);
    let store = ParquetStore::open(root.path(), provenance()).expect("store should open");
    let batch = store
        .write_capture_batch(&[trade(1_000, 1), bbo(1_000, 7)])
        .expect("capture batch should commit");
    let member = batch.partitions()[0].clone();
    let original_directory = root
        .path()
        .join("capture-batches")
        .join(format!("batch-{}.batch", batch.batch_id()));
    let marker = original_directory.join("capture-batch.json");
    let mut wire: Value = serde_json::from_slice(
        &fs::read(&marker).expect("fixture capture marker should be readable"),
    )
    .expect("fixture capture marker should be valid JSON");
    let provenance = wire["provenance"].clone();
    let partitions = wire["partitions"]
        .as_array_mut()
        .expect("fixture capture marker should contain partitions");
    partitions[1] = partitions[0].clone();
    partitions[1]["partition_id"] = Value::String(digest('e'));
    let replacement_batch_id = capture_batch_id(&provenance, partitions);
    wire["batch_id"] = Value::String(replacement_batch_id.clone());
    let replacement: CaptureBatchManifest =
        serde_json::from_value(wire).expect("fixture replacement marker should deserialize");
    let replacement_directory = root
        .path()
        .join("capture-batches")
        .join(format!("batch-{replacement_batch_id}.batch"));
    fs::rename(&original_directory, &replacement_directory)
        .expect("fixture should rename replacement capture directory");
    fs::write(
        replacement_directory.join("capture-batch.json"),
        serde_json::to_vec(&replacement).expect("fixture replacement marker should serialize"),
    )
    .expect("fixture replacement marker should write");

    let error = store
        .open_capture_member(
            &replacement_batch_id,
            &member.identity(),
            member.partition_id(),
            &replacement.manifest_digest(),
            &member.manifest_digest(),
        )
        .expect_err("capture markers cannot repeat one requested identity");

    assert!(matches!(
        error,
        trench_storage::parquet::ParquetError::InvalidCaptureBatch { .. }
    ));
}

#[cfg(unix)]
#[test]
fn direct_legacy_member_rejects_a_symlinked_member_directory() {
    let root = TempDir::new().expect("temporary root should be created");
    secure(&root);
    let store = ParquetStore::open(root.path(), provenance()).expect("store should open");
    let events = [trade(1_000, 1)];
    let [manifest]: [PartitionManifest; 1] = store
        .write_events(&events)
        .expect("legacy partition should commit")
        .try_into()
        .expect("fixture should create one partition");
    let directory = legacy_partition_directory(&root, &manifest);
    let replacement = root.path().join("replacement-partition");
    fs::rename(&directory, &replacement).expect("fixture should move selected member");
    symlink(&replacement, &directory).expect("fixture should substitute member symlink");

    assert!(
        store
            .open_legacy_member(
                &manifest.identity(),
                manifest.partition_id(),
                &manifest.manifest_digest(),
            )
            .is_err()
    );
}

#[cfg(unix)]
#[test]
fn direct_legacy_member_rejects_a_symlinked_manifest() {
    let root = TempDir::new().expect("temporary root should be created");
    secure(&root);
    let store = ParquetStore::open(root.path(), provenance()).expect("store should open");
    let events = [trade(1_000, 1)];
    let [manifest]: [PartitionManifest; 1] = store
        .write_events(&events)
        .expect("legacy partition should commit")
        .try_into()
        .expect("fixture should create one partition");
    let manifest_path = legacy_partition_directory(&root, &manifest).join("manifest.json");
    let replacement = root.path().join("replacement-manifest.json");
    fs::rename(&manifest_path, &replacement).expect("fixture should move manifest");
    symlink(&replacement, &manifest_path).expect("fixture should substitute manifest symlink");

    assert!(
        store
            .open_legacy_member(
                &manifest.identity(),
                manifest.partition_id(),
                &manifest.manifest_digest(),
            )
            .is_err()
    );
}

#[cfg(unix)]
#[test]
fn direct_legacy_member_rejects_a_symlinked_root_ancestor() {
    let parent = TempDir::new().expect("temporary parent should be created");
    secure(&parent);
    let ancestor = parent.path().join("configured-root-ancestor");
    fs::create_dir(&ancestor).expect("fixture root ancestor should be created");
    fs::set_permissions(&ancestor, fs::Permissions::from_mode(0o700))
        .expect("fixture root ancestor should be private");
    let root = ancestor.join("store");
    fs::create_dir(&root).expect("fixture store root should be created");
    fs::set_permissions(&root, fs::Permissions::from_mode(0o700))
        .expect("fixture store root should be private");
    let store = ParquetStore::open(&root, provenance()).expect("store should open");
    let events = [trade(1_000, 1)];
    let [manifest]: [PartitionManifest; 1] = store
        .write_events(&events)
        .expect("legacy partition should commit")
        .try_into()
        .expect("fixture should create one partition");
    let replacement = parent.path().join("replacement-root-ancestor");
    fs::rename(&ancestor, &replacement).expect("fixture should move root ancestor");
    symlink(&replacement, &ancestor).expect("fixture should substitute root ancestor symlink");

    assert!(
        store
            .open_legacy_member(
                &manifest.identity(),
                manifest.partition_id(),
                &manifest.manifest_digest(),
            )
            .is_err()
    );
}

#[cfg(unix)]
#[test]
fn opened_member_reads_through_its_verified_directory_after_path_replacement() {
    let root = TempDir::new().expect("temporary root should be created");
    secure(&root);
    let store = ParquetStore::open(root.path(), provenance()).expect("store should open");
    let events = [trade(1_000, 1)];
    let [manifest]: [PartitionManifest; 1] = store
        .write_events(&events)
        .expect("legacy partition should commit")
        .try_into()
        .expect("fixture should create one partition");
    let opened = store
        .open_legacy_member(
            &manifest.identity(),
            manifest.partition_id(),
            &manifest.manifest_digest(),
        )
        .expect("member should open before replacement");
    let directory = legacy_partition_directory(&root, &manifest);
    let moved = root.path().join("moved-partition");
    fs::rename(&directory, &moved).expect("fixture should move selected member");
    fs::create_dir(&directory).expect("fixture should replace selected member path");
    fs::set_permissions(&directory, fs::Permissions::from_mode(0o700))
        .expect("fixture replacement directory should be private");

    assert_eq!(
        opened.read_all().expect("opened member should stay valid"),
        events
    );
}

#[cfg(unix)]
#[test]
fn direct_capture_member_rejects_a_symlinked_payload() {
    let root = TempDir::new().expect("temporary root should be created");
    secure(&root);
    let store = ParquetStore::open(root.path(), provenance()).expect("store should open");
    let events = [trade(1_000, 1)];
    let batch = store
        .write_capture_batch(&events)
        .expect("capture batch should commit");
    let [manifest]: [PartitionManifest; 1] = batch
        .partitions()
        .to_vec()
        .try_into()
        .expect("fixture should create one capture partition");
    let payload =
        capture_partition_directory(&root, batch.batch_id(), &manifest).join("events.parquet");
    let replacement = root.path().join("replacement-events.parquet");
    fs::rename(&payload, &replacement).expect("fixture should move payload");
    symlink(&replacement, &payload).expect("fixture should substitute payload symlink");

    assert!(
        store
            .open_capture_member(
                batch.batch_id(),
                &manifest.identity(),
                manifest.partition_id(),
                &batch.manifest_digest(),
                &manifest.manifest_digest(),
            )
            .is_err()
    );
}

fn range(start: i64, end: i64) -> TimeRange {
    TimeRange::new(timestamp(start), timestamp(end)).expect("fixture range should be valid")
}

struct SourceFixture {
    _root: TempDir,
    store: ParquetStore,
    legacy: PartitionManifest,
    capture: CaptureBatchManifest,
    capture_member: PartitionManifest,
    predecessor: MarketEvent,
    first: MarketEvent,
    interior: MarketEvent,
    last: MarketEvent,
    successor: MarketEvent,
}

fn source_fixture() -> SourceFixture {
    let root = TempDir::new().expect("temporary root should be created");
    secure(&root);
    let store = ParquetStore::open(root.path(), provenance()).expect("store should open");
    let predecessor = trade(500, 1);
    let first = trade(1_000, 2);
    let interior = trade(1_250, 3);
    let last = trade(1_500, 4);
    let successor = trade(2_000, 5);
    let [legacy]: [PartitionManifest; 1] = store
        .write_events(&[
            predecessor.clone(),
            first.clone(),
            interior.clone(),
            last.clone(),
            successor.clone(),
        ])
        .expect("legacy source should commit")
        .try_into()
        .expect("fixture should create one legacy partition");
    let capture = store
        .write_capture_batch(&[bbo(1_250, 1)])
        .expect("capture source should commit");
    let [capture_member]: [PartitionManifest; 1] = capture
        .partitions()
        .to_vec()
        .try_into()
        .expect("fixture should create one capture partition");

    SourceFixture {
        _root: root,
        store,
        legacy,
        capture,
        capture_member,
        predecessor,
        first,
        interior,
        last,
        successor,
    }
}

fn complete_coverage(fixture: &SourceFixture) -> CoverageDeclaration {
    let interval = range(1_000, 2_000);
    let proof = ContinuityProof::new(
        ContinuitySource::rest_page_chain(vec![b"page-1".to_vec(), b"page-2".to_vec()])
            .expect("bounded page chain should be valid"),
        proof_members(fixture),
        interval,
        Some(CoverageEventRef::new(
            fixture.legacy.manifest_digest(),
            fixture.predecessor.event_id().clone(),
            fixture.predecessor.event_time(),
        )),
        Some(CoverageEventRef::new(
            fixture.legacy.manifest_digest(),
            fixture.successor.event_id().clone(),
            fixture.successor.event_time(),
        )),
    )
    .expect("continuity proof should be valid");
    let complete = CompleteCoverage::events(
        proof,
        CoverageEventRef::new(
            fixture.legacy.manifest_digest(),
            fixture.first.event_id().clone(),
            fixture.first.event_time(),
        ),
        CoverageEventRef::new(
            fixture.legacy.manifest_digest(),
            fixture.last.event_id().clone(),
            fixture.last.event_time(),
        ),
    )
    .expect("complete coverage should be valid");
    CoverageDeclaration::new(
        CoverageTarget::new(
            Market::new("SOL").expect("fixture market should be valid"),
            SourceStreamKind::Trade,
        ),
        interval,
        CoverageWitness::Complete(complete),
    )
    .expect("coverage declaration should be valid")
}

fn source_members(fixture: &SourceFixture) -> Vec<ResearchMemberLocator> {
    vec![
        ResearchMemberLocator::capture(&fixture.capture, &fixture.capture_member)
            .expect("capture locator should be valid"),
        ResearchMemberLocator::legacy(&fixture.legacy),
    ]
}

fn proof_members(fixture: &SourceFixture) -> Vec<ContinuityMemberBinding> {
    vec![ContinuityMemberBinding::from_manifest(&fixture.legacy)]
}

fn max_sized_continuity_source() -> ContinuitySource {
    ContinuitySource::rest_page_chain((0..4).map(|_| vec![0_u8; 64 * 1_024]).collect::<Vec<_>>())
        .expect("maximum per-proof source should be valid")
}

#[test]
fn source_plan_canonically_sorts_legacy_and_capture_members() {
    let fixture = source_fixture();
    let draft = ResearchSourcePlanBuilder::new(range(0, 1_000), range(1_000, 2_000))
        .expect("contiguous windows should be valid")
        .build(
            &fixture.store,
            source_members(&fixture),
            vec![complete_coverage(&fixture)],
        )
        .expect("source plan should build");

    assert!(matches!(
        draft.members().first(),
        Some(ResearchMemberLocator::LegacyPartition { .. })
    ));
    assert_eq!(draft.members().len(), 2);
    assert!(draft.member_set_digest().starts_with("b3:"));
    let wire = String::from_utf8(draft.canonical_json().expect("canonical JSON"))
        .expect("plan JSON should be UTF-8");
    assert!(!wire.contains("source_plan_digest"));
    assert!(!wire.contains("root"));
    assert!(!wire.contains("path"));
}

#[test]
fn source_plan_rejects_duplicate_member_identity() {
    let fixture = source_fixture();
    let locator = ResearchMemberLocator::legacy(&fixture.legacy);
    let error = ResearchSourcePlanBuilder::new(range(0, 1_000), range(1_000, 2_000))
        .expect("contiguous windows should be valid")
        .build(
            &fixture.store,
            vec![locator.clone(), locator],
            vec![complete_coverage(&fixture)],
        )
        .expect_err("duplicate locators must fail");

    assert!(matches!(error, ResearchPlanError::DuplicateMember { .. }));
}

#[test]
fn source_plan_rejects_a_store_with_mismatched_provenance() {
    let fixture = source_fixture();
    let mismatched = DataProvenance::new(digest('c'), digest('d'), ParquetStore::schema_hash())
        .expect("alternate provenance should be valid");
    let store = ParquetStore::open_existing(fixture.store.root(), mismatched)
        .expect("opening root with a prospective provenance should not read members");
    let error = ResearchSourcePlanBuilder::new(range(0, 1_000), range(1_000, 2_000))
        .expect("contiguous windows should be valid")
        .build(
            &store,
            source_members(&fixture),
            vec![complete_coverage(&fixture)],
        )
        .expect_err("mixed provenance must fail before draft construction");

    assert!(matches!(error, ResearchPlanError::Storage(_)));
}

#[test]
fn source_plan_requires_contiguous_non_overlapping_warmup_and_evaluation() {
    let overlap = ResearchSourcePlanBuilder::new(range(0, 1_500), range(1_000, 2_000));
    let gap = ResearchSourcePlanBuilder::new(range(0, 900), range(1_000, 2_000));
    let reversed = ResearchSourcePlanBuilder::new(range(1_000, 2_000), range(0, 1_000));

    assert!(matches!(overlap, Err(ResearchPlanError::InvalidWindows)));
    assert!(matches!(gap, Err(ResearchPlanError::InvalidWindows)));
    assert!(matches!(reversed, Err(ResearchPlanError::InvalidWindows)));
}

#[test]
fn source_plan_rejects_complete_coverage_with_unverified_event_reference() {
    let fixture = source_fixture();
    let interval = range(1_000, 2_000);
    let proof = ContinuityProof::new(
        ContinuitySource::archive_manifest(b"archive-index".to_vec())
            .expect("archive evidence should be valid"),
        proof_members(&fixture),
        interval,
        None,
        None,
    )
    .expect("continuity proof should be valid");
    let complete = CompleteCoverage::events(
        proof,
        CoverageEventRef::new(
            fixture.legacy.manifest_digest(),
            fixture.first.event_id().clone(),
            timestamp(1_001),
        ),
        CoverageEventRef::new(
            fixture.legacy.manifest_digest(),
            fixture.last.event_id().clone(),
            fixture.last.event_time(),
        ),
    )
    .expect("typed complete coverage should construct");
    let coverage = CoverageDeclaration::new(
        CoverageTarget::new(
            Market::new("SOL").expect("fixture market should be valid"),
            SourceStreamKind::Trade,
        ),
        interval,
        CoverageWitness::Complete(complete),
    )
    .expect("typed declaration should construct");

    let error = ResearchSourcePlanBuilder::new(range(0, 1_000), range(1_000, 2_000))
        .expect("contiguous windows should be valid")
        .build(&fixture.store, source_members(&fixture), vec![coverage])
        .expect_err("forged event time must fail");

    assert!(matches!(error, ResearchPlanError::InvalidCoverageEvidence));
}

#[test]
fn source_plan_observed_no_events_requires_the_same_verified_continuity_proof() {
    let fixture = source_fixture();
    let interval = range(1_000, 2_000);
    let proof = ContinuityProof::new(
        ContinuitySource::websocket_sequence_range(b"heartbeats".to_vec(), 8, 12)
            .expect("sequence proof should be valid"),
        proof_members(&fixture),
        interval,
        Some(CoverageEventRef::new(
            fixture.legacy.manifest_digest(),
            fixture.predecessor.event_id().clone(),
            timestamp(501),
        )),
        Some(CoverageEventRef::new(
            fixture.legacy.manifest_digest(),
            fixture.successor.event_id().clone(),
            fixture.successor.event_time(),
        )),
    )
    .expect("continuity proof should be valid");
    let no_events = CompleteCoverage::observed_no_events(proof)
        .expect("typed no-event coverage should construct");
    let coverage = CoverageDeclaration::new(
        CoverageTarget::new(
            Market::new("SOL").expect("fixture market should be valid"),
            SourceStreamKind::Trade,
        ),
        interval,
        CoverageWitness::ObservedNoEvents(no_events),
    )
    .expect("typed declaration should construct");

    let error = ResearchSourcePlanBuilder::new(range(0, 1_000), range(1_000, 2_000))
        .expect("contiguous windows should be valid")
        .build(&fixture.store, source_members(&fixture), vec![coverage])
        .expect_err("no-event proof still needs verified source anchors");

    assert!(matches!(error, ResearchPlanError::InvalidCoverageEvidence));
}

#[test]
fn source_plan_keeps_unavailable_coverage_without_claiming_completeness() {
    let fixture = source_fixture();
    let coverage = CoverageDeclaration::new(
        CoverageTarget::new(
            Market::new("SOL").expect("fixture market should be valid"),
            SourceStreamKind::Funding,
        ),
        range(1_000, 2_000),
        CoverageWitness::Unavailable {
            reason: CoverageUnavailableReason::NotCaptured,
        },
    )
    .expect("unavailable coverage is a valid declaration");
    let draft = ResearchSourcePlanBuilder::new(range(0, 1_000), range(1_000, 2_000))
        .expect("contiguous windows should be valid")
        .build(&fixture.store, source_members(&fixture), vec![coverage])
        .expect("unavailable coverage must not assert completeness");

    assert!(!draft.coverage()[0].is_complete());
}

#[test]
fn source_plan_rejects_observed_no_events_when_matching_rows_exist_in_its_interval() {
    let fixture = source_fixture();
    let interval = range(1_000, 2_000);
    let proof = ContinuityProof::new(
        ContinuitySource::archive_manifest(b"archive-index".to_vec())
            .expect("archive evidence should be valid"),
        proof_members(&fixture),
        interval,
        Some(CoverageEventRef::new(
            fixture.legacy.manifest_digest(),
            fixture.predecessor.event_id().clone(),
            fixture.predecessor.event_time(),
        )),
        Some(CoverageEventRef::new(
            fixture.legacy.manifest_digest(),
            fixture.successor.event_id().clone(),
            fixture.successor.event_time(),
        )),
    )
    .expect("continuity proof should be valid");
    let coverage = CoverageDeclaration::new(
        CoverageTarget::new(
            Market::new("SOL").expect("fixture market should be valid"),
            SourceStreamKind::Trade,
        ),
        interval,
        CoverageWitness::ObservedNoEvents(
            CompleteCoverage::observed_no_events(proof).expect("typed no-event coverage"),
        ),
    )
    .expect("coverage declaration should be valid");

    let error = ResearchSourcePlanBuilder::new(range(0, 1_000), range(1_000, 2_000))
        .expect("contiguous windows should be valid")
        .build(&fixture.store, source_members(&fixture), vec![coverage])
        .expect_err("observed-no-events must reject actual source rows");

    assert!(matches!(error, ResearchPlanError::InvalidCoverageEvidence));
}

#[test]
fn source_plan_requires_complete_coverage_to_name_actual_interval_endpoints() {
    let fixture = source_fixture();
    let interval = range(1_000, 2_000);
    let proof = ContinuityProof::new(
        ContinuitySource::archive_manifest(b"archive-index".to_vec())
            .expect("archive evidence should be valid"),
        proof_members(&fixture),
        interval,
        Some(CoverageEventRef::new(
            fixture.legacy.manifest_digest(),
            fixture.predecessor.event_id().clone(),
            fixture.predecessor.event_time(),
        )),
        Some(CoverageEventRef::new(
            fixture.legacy.manifest_digest(),
            fixture.successor.event_id().clone(),
            fixture.successor.event_time(),
        )),
    )
    .expect("continuity proof should be valid");
    let coverage = CoverageDeclaration::new(
        CoverageTarget::new(
            Market::new("SOL").expect("fixture market should be valid"),
            SourceStreamKind::Trade,
        ),
        interval,
        CoverageWitness::Complete(
            CompleteCoverage::events(
                proof,
                CoverageEventRef::new(
                    fixture.legacy.manifest_digest(),
                    fixture.interior.event_id().clone(),
                    fixture.interior.event_time(),
                ),
                CoverageEventRef::new(
                    fixture.legacy.manifest_digest(),
                    fixture.last.event_id().clone(),
                    fixture.last.event_time(),
                ),
            )
            .expect("typed complete coverage"),
        ),
    )
    .expect("coverage declaration should be valid");

    let error = ResearchSourcePlanBuilder::new(range(0, 1_000), range(1_000, 2_000))
        .expect("contiguous windows should be valid")
        .build(&fixture.store, source_members(&fixture), vec![coverage])
        .expect_err("complete coverage must name the actual first matching row");

    assert!(matches!(error, ResearchPlanError::InvalidCoverageEvidence));
}

#[test]
fn source_plan_rejects_proof_that_does_not_bind_its_referenced_member() {
    let fixture = source_fixture();
    let interval = range(1_000, 2_000);
    let proof = ContinuityProof::new(
        ContinuitySource::archive_manifest(b"archive-index".to_vec())
            .expect("archive evidence should be valid"),
        vec![ContinuityMemberBinding::from_manifest(
            &fixture.capture_member,
        )],
        interval,
        Some(CoverageEventRef::new(
            fixture.legacy.manifest_digest(),
            fixture.predecessor.event_id().clone(),
            fixture.predecessor.event_time(),
        )),
        Some(CoverageEventRef::new(
            fixture.legacy.manifest_digest(),
            fixture.successor.event_id().clone(),
            fixture.successor.event_time(),
        )),
    )
    .expect("continuity proof should be structurally valid");
    let coverage = CoverageDeclaration::new(
        CoverageTarget::new(
            Market::new("SOL").expect("fixture market should be valid"),
            SourceStreamKind::Trade,
        ),
        interval,
        CoverageWitness::Complete(
            CompleteCoverage::events(
                proof,
                CoverageEventRef::new(
                    fixture.legacy.manifest_digest(),
                    fixture.first.event_id().clone(),
                    fixture.first.event_time(),
                ),
                CoverageEventRef::new(
                    fixture.legacy.manifest_digest(),
                    fixture.last.event_id().clone(),
                    fixture.last.event_time(),
                ),
            )
            .expect("typed complete coverage"),
        ),
    )
    .expect("coverage declaration should be valid");

    let error = ResearchSourcePlanBuilder::new(range(0, 1_000), range(1_000, 2_000))
        .expect("contiguous windows should be valid")
        .build(&fixture.store, source_members(&fixture), vec![coverage])
        .expect_err("proof members must bind every referenced source row");

    assert!(matches!(error, ResearchPlanError::InvalidCoverageEvidence));
}

fn observed_no_events_coverage(
    fixture: &SourceFixture,
    predecessor: CoverageEventRef,
) -> CoverageDeclaration {
    let interval = range(1_000, 2_000);
    let proof = ContinuityProof::new(
        ContinuitySource::archive_manifest(b"archive-index".to_vec())
            .expect("archive evidence should be valid"),
        proof_members(fixture),
        interval,
        Some(predecessor),
        Some(CoverageEventRef::new(
            fixture.legacy.manifest_digest(),
            fixture.successor.event_id().clone(),
            fixture.successor.event_time(),
        )),
    )
    .expect("continuity proof should be valid");
    CoverageDeclaration::new(
        CoverageTarget::new(
            Market::new("SOL").expect("fixture market should be valid"),
            SourceStreamKind::Trade,
        ),
        interval,
        CoverageWitness::ObservedNoEvents(
            CompleteCoverage::observed_no_events(proof).expect("typed no-event coverage"),
        ),
    )
    .expect("coverage declaration should be valid")
}

#[test]
fn source_plan_rejects_oversized_public_locator_and_coverage_identifiers() {
    let fixture = source_fixture();
    let mut locator = ResearchMemberLocator::legacy(&fixture.legacy);
    let ResearchMemberLocator::LegacyPartition { partition_id, .. } = &mut locator else {
        unreachable!("legacy constructor must create legacy locator");
    };
    *partition_id = "x".repeat(1_024);
    let locator_error = ResearchSourcePlanBuilder::new(range(0, 1_000), range(1_000, 2_000))
        .expect("contiguous windows should be valid")
        .build(&fixture.store, vec![locator], vec![])
        .expect_err("oversized locator identifiers must fail before resolution");
    assert!(matches!(locator_error, ResearchPlanError::InvalidLocator));

    let manifest_error = ResearchSourcePlanBuilder::new(range(0, 1_000), range(1_000, 2_000))
        .expect("contiguous windows should be valid")
        .build(
            &fixture.store,
            source_members(&fixture),
            vec![observed_no_events_coverage(
                &fixture,
                CoverageEventRef::new(
                    "x".repeat(1_024),
                    fixture.first.event_id().clone(),
                    fixture.first.event_time(),
                ),
            )],
        )
        .expect_err("coverage manifest digest must be bounded and canonical");
    assert!(matches!(
        manifest_error,
        ResearchPlanError::InvalidCoverageEvidence
    ));

    let event_error = ResearchSourcePlanBuilder::new(range(0, 1_000), range(1_000, 2_000))
        .expect("contiguous windows should be valid")
        .build(
            &fixture.store,
            source_members(&fixture),
            vec![observed_no_events_coverage(
                &fixture,
                CoverageEventRef::new(
                    fixture.legacy.manifest_digest(),
                    EventId::new("x".repeat(1_024)).expect("synthetic unbounded identifier"),
                    fixture.first.event_time(),
                ),
            )],
        )
        .expect_err("coverage event identifier must be bounded and canonical");
    assert!(matches!(
        event_error,
        ResearchPlanError::InvalidCoverageEvidence
    ));
}

#[test]
fn source_plan_rejects_aggregate_continuity_evidence_above_the_plan_budget() {
    let fixture = source_fixture();
    let interval = range(1_000, 2_000);
    let coverage = (0..5)
        .map(|_| {
            let proof = ContinuityProof::new(
                max_sized_continuity_source(),
                proof_members(&fixture),
                interval,
                None,
                None,
            )
            .expect("individually bounded proof should be valid");
            CoverageDeclaration::new(
                CoverageTarget::new(
                    Market::new("SOL").expect("fixture market should be valid"),
                    SourceStreamKind::Trade,
                ),
                interval,
                CoverageWitness::ObservedNoEvents(
                    CompleteCoverage::observed_no_events(proof).expect("typed no-event coverage"),
                ),
            )
            .expect("typed coverage should be valid")
        })
        .collect::<Vec<_>>();

    let error = ResearchSourcePlanBuilder::new(range(0, 1_000), range(1_000, 2_000))
        .expect("contiguous windows should be valid")
        .build(&fixture.store, source_members(&fixture), coverage)
        .expect_err("aggregate proof evidence must be bounded across the source plan");

    assert!(matches!(error, ResearchPlanError::ResourceLimit));
}

#[test]
fn source_plan_rejects_aggregate_continuity_bindings_above_the_plan_budget() {
    let fixture = source_fixture();
    let coverage = (0..4_097_i64)
        .map(|offset| {
            let interval = range(1_000 + offset, 1_001 + offset);
            let proof = ContinuityProof::new(
                ContinuitySource::archive_manifest(b"evidence".to_vec())
                    .expect("bounded evidence should be valid"),
                proof_members(&fixture),
                interval,
                None,
                None,
            )
            .expect("individually bounded proof should be valid");
            CoverageDeclaration::new(
                CoverageTarget::new(
                    Market::new("SOL").expect("fixture market should be valid"),
                    SourceStreamKind::Trade,
                ),
                interval,
                CoverageWitness::ObservedNoEvents(
                    CompleteCoverage::observed_no_events(proof).expect("typed no-event coverage"),
                ),
            )
            .expect("typed coverage should be valid")
        })
        .collect::<Vec<_>>();

    let error = ResearchSourcePlanBuilder::new(range(0, 1_000), range(1_000, 5_097))
        .expect("contiguous windows should be valid")
        .build(&fixture.store, source_members(&fixture), coverage)
        .expect_err("aggregate proof bindings must be bounded across the source plan");

    assert!(matches!(error, ResearchPlanError::ResourceLimit));
}

#[test]
fn source_plan_rejects_aggregate_rest_page_witnesses_above_the_plan_budget() {
    let fixture = source_fixture();
    let coverage = (0..1_025_i64)
        .map(|offset| {
            let interval = range(1_000 + offset, 1_001 + offset);
            let proof = ContinuityProof::new(
                ContinuitySource::rest_page_chain(vec![b"page".to_vec()])
                    .expect("bounded page evidence should be valid"),
                proof_members(&fixture),
                interval,
                None,
                None,
            )
            .expect("individually bounded proof should be valid");
            CoverageDeclaration::new(
                CoverageTarget::new(
                    Market::new("SOL").expect("fixture market should be valid"),
                    SourceStreamKind::Trade,
                ),
                interval,
                CoverageWitness::ObservedNoEvents(
                    CompleteCoverage::observed_no_events(proof).expect("typed no-event coverage"),
                ),
            )
            .expect("typed coverage should be valid")
        })
        .collect::<Vec<_>>();

    let error = ResearchSourcePlanBuilder::new(range(0, 1_000), range(1_000, 2_025))
        .expect("contiguous windows should be valid")
        .build(&fixture.store, source_members(&fixture), coverage)
        .expect_err("aggregate REST page witnesses must be bounded across the source plan");

    assert!(matches!(error, ResearchPlanError::ResourceLimit));
}

#[test]
fn source_plan_canonical_json_rejects_an_oversized_serialized_plan() {
    let root = TempDir::new().expect("temporary root should be created");
    secure(&root);
    let store = ParquetStore::open(root.path(), provenance()).expect("store should open");
    let mut members = Vec::new();
    let mut coverage = Vec::new();

    for (index, market) in ["SOL", "ETH", "BTC"].into_iter().enumerate() {
        let trade_id = u64::try_from(index).expect("fixture index should fit") * 10;
        let predecessor = trade_for(market, 500, trade_id + 1);
        let first = trade_for(market, 1_000, trade_id + 2);
        let last = trade_for(market, 1_500, trade_id + 3);
        let successor = trade_for(market, 2_000, trade_id + 4);
        let [manifest]: [PartitionManifest; 1] = store
            .write_events(&[
                predecessor.clone(),
                first.clone(),
                last.clone(),
                successor.clone(),
            ])
            .expect("fixture source should commit")
            .try_into()
            .expect("each fixture market should create one partition");
        members.push(ResearchMemberLocator::legacy(&manifest));

        let interval = range(1_000, 2_000);
        let proof = ContinuityProof::new(
            max_sized_continuity_source(),
            vec![ContinuityMemberBinding::from_manifest(&manifest)],
            interval,
            Some(CoverageEventRef::new(
                manifest.manifest_digest(),
                predecessor.event_id().clone(),
                predecessor.event_time(),
            )),
            Some(CoverageEventRef::new(
                manifest.manifest_digest(),
                successor.event_id().clone(),
                successor.event_time(),
            )),
        )
        .expect("individually bounded proof should be valid");
        let complete = CompleteCoverage::events(
            proof,
            CoverageEventRef::new(
                manifest.manifest_digest(),
                first.event_id().clone(),
                first.event_time(),
            ),
            CoverageEventRef::new(
                manifest.manifest_digest(),
                last.event_id().clone(),
                last.event_time(),
            ),
        )
        .expect("typed complete coverage");
        coverage.push(
            CoverageDeclaration::new(
                CoverageTarget::new(
                    Market::new(market).expect("fixture market should be valid"),
                    SourceStreamKind::Trade,
                ),
                interval,
                CoverageWitness::Complete(complete),
            )
            .expect("typed coverage should be valid"),
        );
    }

    let draft = ResearchSourcePlanBuilder::new(range(0, 1_000), range(1_000, 2_000))
        .expect("contiguous windows should be valid")
        .build(&store, members, coverage)
        .expect("aggregate evidence remains within the plan budget");

    assert!(matches!(
        draft.canonical_json(),
        Err(ResearchPlanError::ResourceLimit)
    ));
}
