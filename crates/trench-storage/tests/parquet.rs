use std::fs;
use std::io::{Seek, SeekFrom, Write};
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use rust_decimal_macros::dec;
use tempfile::TempDir;
use trench_core::domain::{Market, Price, Quantity, Side};
use trench_core::event::{
    Bbo, BookLevel, BookSnapshot, Funding, FundingRate, MarketEvent, MarketEventKind, TimestampNs,
    Trade,
};
use trench_storage::parquet::{
    CaptureBatchFailure, DataProvenance, ParquetError, ParquetStore, PartitionFailure,
    PartitionManifest,
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
    MarketEvent::trade(
        timestamp(at),
        timestamp(at + 1),
        Market::new("SOL").expect("fixture market should be valid"),
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

fn historical_funding(at: i64) -> MarketEvent {
    MarketEvent::funding(
        timestamp(at),
        timestamp(at + 1),
        Market::new("SOL").expect("fixture market should be valid"),
        Funding::historical(FundingRate::new(dec!(0.00001))),
    )
    .expect("fixture event should be valid")
}

fn temp_siblings(root: &TempDir) -> Vec<std::path::PathBuf> {
    let mut paths = Vec::new();
    let mut pending = vec![root.path().to_path_buf()];
    while let Some(directory) = pending.pop() {
        for entry in fs::read_dir(directory).expect("fixture directory should be readable") {
            let entry = entry.expect("fixture entry should be readable");
            let path = entry.path();
            if path.is_dir() {
                pending.push(path.clone());
            }
            if path
                .file_name()
                .is_some_and(|name| name.to_string_lossy().ends_with(".tmp"))
            {
                paths.push(path);
            }
        }
    }
    paths
}

fn complete_partitions(root: &TempDir) -> Vec<std::path::PathBuf> {
    let mut paths = Vec::new();
    let mut pending = vec![root.path().to_path_buf()];
    while let Some(directory) = pending.pop() {
        for entry in fs::read_dir(directory).expect("fixture directory should be readable") {
            let entry = entry.expect("fixture entry should be readable");
            let path = entry.path();
            if path.is_dir() {
                pending.push(path.clone());
            }
            if path
                .file_name()
                .is_some_and(|name| name.to_string_lossy().ends_with(".part"))
            {
                paths.push(path);
            }
        }
    }
    paths
}

fn staged_capture_batches(root: &TempDir) -> Vec<std::path::PathBuf> {
    let batches = root.path().join("capture-batches");
    fs::read_dir(batches)
        .expect("capture batch directory should be readable")
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| {
            path.file_name()
                .is_some_and(|name| name.to_string_lossy().ends_with(".batch.tmp"))
        })
        .collect()
}

fn secure(root: &TempDir) {
    #[cfg(unix)]
    fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700))
        .expect("fixture root should be private");
}

#[test]
fn interrupted_partition_is_left_as_an_ignored_temporary_sibling() {
    let root = TempDir::new().expect("temporary root should be created");
    secure(&root);
    let store = ParquetStore::open(root.path(), provenance()).expect("store should open");

    let error = store
        .write_events_with_failure(&[trade(1_000, 1)], PartitionFailure::BeforeRename)
        .expect_err("injected pre-rename failure should escape");

    assert!(error.is_injected_failure());
    assert_eq!(store.partitions().expect("temporary paths are ignored"), []);
    assert_eq!(temp_siblings(&root).len(), 1);
}

#[test]
fn completed_partition_reopens_and_matches_its_immutable_manifest() {
    let root = TempDir::new().expect("temporary root should be created");
    secure(&root);
    let store = ParquetStore::open(root.path(), provenance()).expect("store should open");
    let events = [trade(1_000, 1), trade(2_000, 2)];

    let manifests = store
        .write_events(&events)
        .expect("partition should commit");
    let [manifest]: [PartitionManifest; 1] = manifests
        .try_into()
        .expect("same market/day/kind should create one partition");

    assert_eq!(manifest.row_count(), 2);
    assert_eq!(manifest.min_event_time(), timestamp(1_000));
    assert_eq!(manifest.max_event_time(), timestamp(2_000));
    assert_eq!(manifest.provenance(), &provenance());
    assert_eq!(
        store
            .read_partition(&manifest)
            .expect("manifest and parquet contents should validate"),
        events
    );
}

#[test]
fn event_kind_partitioning_keeps_distinct_normalized_facts_separate() {
    let root = TempDir::new().expect("temporary root should be created");
    secure(&root);
    let store = ParquetStore::open(root.path(), provenance()).expect("store should open");

    let manifests = store
        .write_events(&[trade(1_000, 1), bbo(1_000, 7)])
        .expect("partitions should commit");

    assert_eq!(manifests.len(), 2);
    assert!(manifests.iter().all(|manifest| manifest.row_count() == 1));
}

#[test]
fn capture_batch_publishes_all_partition_kinds_at_one_commit_boundary() {
    let root = TempDir::new().expect("temporary root should be created");
    secure(&root);
    let store = ParquetStore::open(root.path(), provenance()).expect("store should open");

    let manifests = store
        .write_capture_batch(&[trade(1_000, 1), bbo(1_000, 7)])
        .expect("capture should publish");

    assert_eq!(manifests.len(), 2);
    assert_eq!(
        store
            .partitions()
            .expect("committed capture should be replayable"),
        manifests
    );
    for partition in &manifests {
        assert_eq!(
            store
                .read_partition(partition)
                .expect("committed capture partition should reopen")
                .len(),
            1
        );
    }
}

#[test]
fn capture_batch_retry_is_idempotent_only_for_the_exact_committed_capture() {
    let root = TempDir::new().expect("temporary root should be created");
    secure(&root);
    let store = ParquetStore::open(root.path(), provenance()).expect("store should open");
    let events = [trade(1_000, 1), bbo(1_000, 7)];

    let committed = store
        .write_capture_batch(&events)
        .expect("first capture should publish");
    let retried = store
        .write_capture_batch(&events)
        .expect("exact capture retry should validate and remain idempotent");

    assert_eq!(retried, committed);
    assert_eq!(complete_partitions(&root).len(), committed.len());
    assert!(staged_capture_batches(&root).is_empty());
    assert_eq!(
        store
            .partitions()
            .expect("exact retry must leave a replayable store"),
        committed
    );
}

#[test]
fn capture_batch_rejects_mixed_legacy_overlap_before_staging() {
    let root = TempDir::new().expect("temporary root should be created");
    secure(&root);
    let store = ParquetStore::open(root.path(), provenance()).expect("store should open");
    let existing = trade(1_000, 1);
    store
        .write_events(std::slice::from_ref(&existing))
        .expect("legacy partition should commit");

    let error = store
        .write_capture_batch(&[existing, bbo(1_000, 7)])
        .expect_err("mixed legacy/capture overlap must fail before staging");

    assert!(matches!(error, ParquetError::DuplicateEvent { .. }));
    assert_eq!(complete_partitions(&root).len(), 1);
    assert!(staged_capture_batches(&root).is_empty());
}

#[test]
fn capture_batch_rejects_mixed_prior_capture_overlap_before_staging() {
    let root = TempDir::new().expect("temporary root should be created");
    secure(&root);
    let store = ParquetStore::open(root.path(), provenance()).expect("store should open");
    let existing = trade(1_000, 1);
    let committed = store
        .write_capture_batch(&[existing.clone(), bbo(1_000, 7)])
        .expect("first capture should publish");

    let error = store
        .write_capture_batch(&[existing, historical_funding(1_000)])
        .expect_err("mixed capture overlap must fail before staging");

    assert!(matches!(error, ParquetError::DuplicateEvent { .. }));
    assert_eq!(complete_partitions(&root).len(), committed.len());
    assert!(staged_capture_batches(&root).is_empty());
    assert_eq!(
        store
            .partitions()
            .expect("rejected overlap must preserve the old capture"),
        committed
    );
}

#[test]
fn legacy_write_rejects_an_existing_capture_partition_before_staging() {
    let root = TempDir::new().expect("temporary root should be created");
    secure(&root);
    let store = ParquetStore::open(root.path(), provenance()).expect("store should open");
    let event = trade(1_000, 1);
    let [committed]: [PartitionManifest; 1] = store
        .write_capture_batch(std::slice::from_ref(&event))
        .expect("capture should publish")
        .try_into()
        .expect("fixture capture should have one partition");

    let error = store
        .write_events(std::slice::from_ref(&event))
        .expect_err("legacy write must not republish a captured partition");

    assert!(matches!(error, ParquetError::DuplicateEvent { .. }));
    assert_eq!(complete_partitions(&root).len(), 1);
    assert!(temp_siblings(&root).is_empty());
    assert_eq!(
        store
            .read_partition(&committed)
            .expect("rejected legacy write must preserve the capture"),
        [event]
    );
}

#[test]
fn later_capture_partition_fsync_failure_exposes_no_facts_and_is_cleaned_on_restart() {
    let root = TempDir::new().expect("temporary root should be created");
    secure(&root);
    let store = ParquetStore::open(root.path(), provenance()).expect("store should open");

    let error = store
        .write_capture_batch_with_failure(
            &[trade(1_000, 1), bbo(1_000, 7)],
            CaptureBatchFailure::BeforePartitionFileSync { partition_index: 1 },
        )
        .expect_err("later staged partition fsync should fail");

    assert!(error.is_injected_failure());
    assert_eq!(
        store
            .partitions()
            .expect("uncommitted capture must stay invisible"),
        []
    );
    assert_eq!(staged_capture_batches(&root).len(), 1);
    drop(store);

    let reopened = ParquetStore::open(root.path(), provenance()).expect("restart should recover");
    assert_eq!(
        reopened
            .partitions()
            .expect("failed staging must remain absent after restart"),
        []
    );
    assert_eq!(staged_capture_batches(&root).len(), 0);
}

#[test]
fn later_capture_manifest_fsync_failure_exposes_no_facts() {
    let root = TempDir::new().expect("temporary root should be created");
    secure(&root);
    let store = ParquetStore::open(root.path(), provenance()).expect("store should open");

    let error = store
        .write_capture_batch_with_failure(
            &[trade(1_000, 1), bbo(1_000, 7)],
            CaptureBatchFailure::BeforePartitionManifestSync { partition_index: 1 },
        )
        .expect_err("later staged partition manifest fsync should fail");

    assert!(error.is_injected_failure());
    assert_eq!(
        store
            .partitions()
            .expect("uncommitted capture must stay invisible"),
        []
    );
}

#[test]
fn historical_funding_round_trips_without_an_imputed_mark() {
    let root = TempDir::new().expect("temporary root should be created");
    secure(&root);
    let store = ParquetStore::open(root.path(), provenance()).expect("store should open");
    let event = historical_funding(1_000);
    let [manifest]: [PartitionManifest; 1] = store
        .write_events(std::slice::from_ref(&event))
        .expect("historical funding partition should commit")
        .try_into()
        .expect("one funding partition");

    let [reopened]: [MarketEvent; 1] = store
        .read_partition(&manifest)
        .expect("historical funding partition should reopen")
        .try_into()
        .expect("one historical funding event");
    assert_eq!(reopened, event);
    assert!(
        matches!(reopened.kind(), MarketEventKind::Funding(funding) if funding.mark_price().is_none())
    );
}

#[test]
fn misplaced_complete_partition_is_rejected_before_replay() {
    let root = TempDir::new().expect("temporary root should be created");
    secure(&root);
    let store = ParquetStore::open(root.path(), provenance()).expect("store should open");
    store
        .write_events(&[trade(1_000, 1)])
        .expect("partition should commit");
    let [partition]: [std::path::PathBuf; 1] = complete_partitions(&root)
        .try_into()
        .expect("fixture should contain one complete partition");
    let relocated_parent = root
        .path()
        .join("partitions/date=utc-day-0/kind=trade/market=424144");
    fs::create_dir(&relocated_parent).expect("relocated market directory should be created");
    #[cfg(unix)]
    fs::set_permissions(&relocated_parent, fs::Permissions::from_mode(0o700))
        .expect("relocated market directory should be private");
    let name = partition
        .file_name()
        .expect("fixture partition should have a name");
    fs::rename(&partition, relocated_parent.join(name)).expect("fixture partition should move");

    assert!(store.partitions().is_err());
}

#[test]
fn overlapping_append_is_rejected_as_a_cross_partition_duplicate() {
    let root = TempDir::new().expect("temporary root should be created");
    secure(&root);
    let store = ParquetStore::open(root.path(), provenance()).expect("store should open");
    let first = trade(1_000, 1);
    store
        .write_events(std::slice::from_ref(&first))
        .expect("first partition should commit");
    let error = store
        .write_events(&[first, trade(2_000, 2)])
        .expect_err("overlapping append must fail before publishing a partition");

    assert!(matches!(error, ParquetError::DuplicateEvent { .. }));
    assert_eq!(complete_partitions(&root).len(), 1);
    assert_eq!(
        store
            .partitions()
            .expect("root must remain replayable after rejected append")
            .len(),
        1
    );
}

#[test]
fn foreign_provenance_is_fenced_before_creating_a_new_partition() {
    let root = TempDir::new().expect("temporary root should be created");
    secure(&root);
    let first_store = ParquetStore::open(root.path(), provenance()).expect("store should open");
    first_store
        .write_events(&[trade(1_000, 1)])
        .expect("first partition should commit");

    let foreign_provenance =
        DataProvenance::new(digest('c'), digest('b'), ParquetStore::schema_hash())
            .expect("foreign fixture provenance should be valid");
    let foreign_store =
        ParquetStore::open(root.path(), foreign_provenance).expect("foreign store should open");

    let error = foreign_store
        .write_events(&[trade(2_000, 2)])
        .expect_err("foreign provenance must be rejected before a write");

    assert!(error.is_provenance_mismatch());
    assert_eq!(complete_partitions(&root).len(), 1);
    assert_eq!(
        first_store
            .partitions()
            .expect("original root must remain replayable")
            .len(),
        1
    );
}

#[test]
fn oversized_book_is_rejected_before_partition_materialization() {
    let root = TempDir::new().expect("temporary root should be created");
    secure(&root);
    let store = ParquetStore::open(root.path(), provenance()).expect("store should open");
    let level = BookLevel::new(
        Price::new(dec!(99)).expect("fixture price should be valid"),
        Quantity::new(dec!(1)).expect("fixture quantity should be valid"),
    );
    let event = MarketEvent::book_snapshot(
        timestamp(1_000),
        timestamp(1_001),
        Market::new("SOL").expect("fixture market should be valid"),
        BookSnapshot::new(1, vec![level; 2_001], vec![level]),
    )
    .expect("fixture event should be valid");

    let error = store
        .write_events(&[event])
        .expect_err("oversized book should not reach Arrow allocation");
    assert!(error.is_resource_limit());
}

#[test]
fn invalid_parquet_footer_is_rejected_before_arrow_metadata_decode() {
    let root = TempDir::new().expect("temporary root should be created");
    secure(&root);
    let store = ParquetStore::open(root.path(), provenance()).expect("store should open");
    store
        .write_events(&[trade(1_000, 1)])
        .expect("partition should commit");
    let [partition]: [std::path::PathBuf; 1] = complete_partitions(&root)
        .try_into()
        .expect("fixture should contain one complete partition");
    let path = partition.join("events.parquet");
    let mut file = fs::OpenOptions::new()
        .write(true)
        .open(path)
        .expect("fixture parquet should open for corruption");
    file.seek(SeekFrom::End(-4))
        .and_then(|_| file.write_all(b"BAD!"))
        .expect("fixture parquet footer should corrupt");

    assert!(store.partitions().is_err());
}
