use rust_decimal_macros::dec;
use std::fs;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use tempfile::TempDir;
use trench_core::domain::{Market, Price, Quantity, Side};
use trench_core::event::{Bbo, BookLevel, MarketEvent, MarketEventKind, TimestampNs, Trade};
use trench_storage::parquet::{DataProvenance, ParquetStore};
use trench_storage::replay::{DeterministicReplay, ReplayPlan};

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

fn market() -> Market {
    Market::new("SOL").expect("fixture market should be valid")
}

fn trade(at: i64, trade_id: u64) -> MarketEvent {
    MarketEvent::trade(
        timestamp(at),
        timestamp(at + 1),
        market(),
        Trade::new(
            trade_id,
            Side::Buy,
            Price::new(dec!(100)).expect("fixture price should be valid"),
            Quantity::new(dec!(2)).expect("fixture quantity should be valid"),
        )
        .expect("fixture trade should be valid"),
    )
    .expect("fixture trade event should be valid")
}

fn bbo(at: i64) -> MarketEvent {
    MarketEvent::bbo(
        timestamp(at),
        timestamp(at + 1),
        market(),
        Bbo::new(
            7,
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
    .expect("fixture BBO event should be valid")
}

fn secure(root: &TempDir) {
    #[cfg(unix)]
    fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700))
        .expect("fixture root should be private");
}

#[test]
fn replay_merges_partitions_by_event_time_kind_and_event_identity() {
    let root = TempDir::new().expect("temporary root should be created");
    secure(&root);
    let store = ParquetStore::open(root.path(), provenance()).expect("store should open");
    let bbo = bbo(1_000);
    let first_trade = trade(1_000, 1);
    let second_trade = trade(2_000, 2);

    store
        .write_events(&[second_trade.clone(), first_trade.clone(), bbo.clone()])
        .expect("partitions should commit");

    let replay = DeterministicReplay::open(root.path(), provenance())
        .expect("replay should validate every complete partition");
    let events = replay.events();

    assert_eq!(events.len(), 3);
    assert!(matches!(events[0].kind(), MarketEventKind::Bbo(_)));
    assert_eq!(events[1], first_trade);
    assert_eq!(events[2], second_trade);
    assert_eq!(
        replay.digest(),
        DeterministicReplay::digest_events(events).expect("fixture digest should serialize")
    );
}

#[test]
fn replay_reads_capture_partitions_only_after_their_batch_commit() {
    let root = TempDir::new().expect("temporary root should be created");
    secure(&root);
    let store = ParquetStore::open(root.path(), provenance()).expect("store should open");
    let bbo = bbo(1_000);
    let trade = trade(1_000, 1);

    store
        .write_capture_batch(&[trade.clone(), bbo.clone()])
        .expect("capture should publish");

    let replay = DeterministicReplay::open(root.path(), provenance())
        .expect("replay should consume the complete capture batch");
    assert_eq!(replay.events(), &[bbo, trade]);
}

#[test]
fn replay_rejects_partitions_from_a_different_frozen_config() {
    let root = TempDir::new().expect("temporary root should be created");
    secure(&root);
    let store = ParquetStore::open(root.path(), provenance()).expect("store should open");
    store
        .write_events(&[trade(1_000, 1)])
        .expect("partition should commit");

    let other = DataProvenance::new(digest('c'), digest('b'), ParquetStore::schema_hash())
        .expect("fixture provenance should be valid");
    let error = DeterministicReplay::open(root.path(), other)
        .expect_err("replay must not mix frozen configurations");

    assert!(error.is_provenance_mismatch());
}

#[test]
fn replay_does_not_create_a_missing_store_root() {
    let parent = TempDir::new().expect("temporary parent should be created");
    secure(&parent);
    let missing = parent.path().join("missing");

    assert!(DeterministicReplay::open(&missing, provenance()).is_err());
    assert!(!missing.exists());
}

#[test]
fn explicit_replay_plan_reopens_only_its_frozen_partition_window() {
    let root = TempDir::new().expect("temporary root should be created");
    secure(&root);
    let store = ParquetStore::open(root.path(), provenance()).expect("store should open");
    let manifests = store
        .write_events(&[trade(1_000, 1)])
        .expect("partition should commit");
    let plan = ReplayPlan::new(provenance(), manifests).expect("plan should be bounded and valid");

    let replay = DeterministicReplay::open_plan(root.path(), plan)
        .expect("selected immutable partition should replay");
    assert_eq!(replay.events().len(), 1);
}

#[test]
fn replay_plan_round_trips_through_an_atomic_restart_manifest() {
    let root = TempDir::new().expect("temporary root should be created");
    secure(&root);
    let store = ParquetStore::open(root.path(), provenance()).expect("store should open");
    let manifests = store
        .write_events(&[trade(1_000, 1)])
        .expect("partition should commit");
    let plan = ReplayPlan::new(provenance(), manifests).expect("plan should be valid");
    let imports = root.path().join("imports");
    fs::create_dir(&imports).expect("private plan directory should be created");
    #[cfg(unix)]
    fs::set_permissions(&imports, fs::Permissions::from_mode(0o700))
        .expect("private plan directory should be private");
    let path = imports.join("basic.json");

    plan.write_to(&path)
        .expect("plan manifest should publish atomically");
    let reloaded = ReplayPlan::read_from(&path).expect("plan manifest should reopen");

    assert_eq!(reloaded, plan);
    assert_eq!(
        DeterministicReplay::open_plan(root.path(), reloaded)
            .expect("persisted plan should support restart replay")
            .events()
            .len(),
        1
    );
}

#[cfg(unix)]
#[test]
fn replay_plan_rejects_a_symlinked_ancestor_before_any_write() {
    use std::os::unix::fs::symlink;

    let root = TempDir::new().expect("temporary root should be created");
    secure(&root);
    let store = ParquetStore::open(root.path(), provenance()).expect("store should open");
    let manifests = store
        .write_events(&[trade(1_000, 1)])
        .expect("partition should commit");
    let plan = ReplayPlan::new(provenance(), manifests).expect("plan should be valid");
    let imports = root.path().join("imports");
    fs::create_dir(&imports).expect("private plan directory should be created");
    fs::set_permissions(&imports, fs::Permissions::from_mode(0o700))
        .expect("private plan directory should be private");
    let link = root.path().join("imports-link");
    symlink(&imports, &link).expect("fixture symlink should be created");
    let plan_path = link.join("should-not-exist.json");

    assert!(plan.write_to(&plan_path).is_err());
    assert!(!imports.join("should-not-exist.json").exists());
}
