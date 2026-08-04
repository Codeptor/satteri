use std::fs;
use std::io::{Seek, SeekFrom, Write};
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use rust_decimal_macros::dec;
use tempfile::TempDir;
use trench_core::domain::{Market, Price, Quantity, Side};
use trench_core::event::{Bbo, BookLevel, BookSnapshot, MarketEvent, TimestampNs, Trade};
use trench_storage::parquet::{
    DataProvenance, ParquetError, ParquetStore, PartitionFailure, PartitionManifest,
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
