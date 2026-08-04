use std::fs;
#[cfg(unix)]
use std::os::unix::fs::{PermissionsExt, symlink};

use rust_decimal_macros::dec;
use serde_json::Value;
use tempfile::TempDir;
use trench_core::domain::{Market, Price, Quantity, Side};
use trench_core::event::{Bbo, BookLevel, MarketEvent, TimestampNs, Trade};
use trench_storage::parquet::{
    CaptureBatchManifest, DataProvenance, ParquetStore, PartitionManifest,
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
