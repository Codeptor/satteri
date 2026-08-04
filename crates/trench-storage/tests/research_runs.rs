use std::fs;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use rust_decimal_macros::dec;
use tempfile::TempDir;
use trench_core::{
    domain::{Market, Price, Quantity, Side},
    event::{MarketEvent, TimestampNs, Trade},
    validation::TimeRange,
};
use trench_storage::{
    parquet::{DataProvenance, ParquetStore},
    research_plan::{ResearchMemberLocator, ResearchSourcePlanBuilder},
    research_runs::{
        AvailabilityDigestRecord, ResearchRunError, ResearchSourcePlan, availability_run_digest,
    },
};

const NANOS_PER_DAY: i64 = 86_400_000_000_000;

fn digest(character: char) -> String {
    format!("b3:{}", character.to_string().repeat(64))
}

fn timestamp(value: i64) -> TimestampNs {
    TimestampNs::new(i128::from(value)).expect("fixture timestamp")
}

fn range(start: i64, end: i64) -> TimeRange {
    TimeRange::new(timestamp(start), timestamp(end)).expect("fixture range")
}

fn trade(event_time: i64, received_at: i64, trade_id: u64) -> MarketEvent {
    MarketEvent::trade(
        timestamp(event_time),
        timestamp(received_at),
        Market::new("SOL").expect("fixture market"),
        Trade::new(
            trade_id,
            Side::Buy,
            Price::new(dec!(100)).expect("fixture price"),
            Quantity::new(dec!(1)).expect("fixture quantity"),
        )
        .expect("fixture trade"),
    )
    .expect("fixture event")
}

fn secure(root: &TempDir) {
    #[cfg(unix)]
    fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700))
        .expect("fixture root should be private");
}

fn create_private_directory(path: &std::path::Path) {
    fs::create_dir(path).expect("fixture private directory");
    #[cfg(unix)]
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
        .expect("fixture private directory mode");
}

fn legacy_partition_parent(root: &std::path::Path, day: u64) -> std::path::PathBuf {
    let date = root.join("partitions").join(format!("date=utc-day-{day}"));
    create_private_directory(&date);
    let kind = date.join("kind=trade");
    create_private_directory(&kind);
    let market = kind.join("market=534f4c");
    create_private_directory(&market);
    market
}

#[test]
fn multi_pass_plan_preserves_canonical_availability_order() {
    let root = TempDir::new().expect("temporary root");
    secure(&root);
    let provenance = DataProvenance::new(digest('a'), digest('b'), ParquetStore::schema_hash())
        .expect("fixture provenance");
    let store = ParquetStore::open(root.path(), provenance).expect("store");
    let mut facts = (0_u64..65)
        .map(|day| {
            let base = i64::try_from(day).expect("day") * NANOS_PER_DAY;
            trade(base + 100, base + 101, day + 10)
        })
        .collect::<Vec<_>>();
    facts.extend([
        trade(200, 900, 100),
        trade(300, 301, 101),
        trade(400, 500, 102),
        trade(400, 500, 103),
    ]);
    let manifests = store.write_events(&facts).expect("partitions");
    assert_eq!(manifests.len(), 65);
    let locators = manifests
        .iter()
        .map(ResearchMemberLocator::legacy)
        .collect::<Vec<_>>();
    let draft = ResearchSourcePlanBuilder::new(
        range(0, NANOS_PER_DAY * 32),
        range(NANOS_PER_DAY * 32, NANOS_PER_DAY * 66),
    )
    .expect("windows")
    .build(&store, locators, Vec::new())
    .expect("draft");

    let plan = draft
        .publish_to(&store, root.path().join("plan"))
        .expect("published final plan");
    let expected = {
        let mut facts = facts;
        facts.sort_by(|left, right| {
            left.received_at()
                .cmp(&right.received_at())
                .then_with(|| left.event_time().cmp(&right.event_time()))
                .then_with(|| left.event_id().cmp(right.event_id()))
        });
        facts
            .into_iter()
            .map(|fact| fact.event_id().clone())
            .collect::<Vec<_>>()
    };
    let actual = plan
        .availability_run()
        .records()
        .collect::<Result<Vec<_>, _>>()
        .expect("streamed final run")
        .into_iter()
        .map(|record| record.event().event_id().clone())
        .collect::<Vec<_>>();
    assert_eq!(actual, expected);
    assert_eq!(plan.availability_run().record_count(), 69);
    assert_eq!(plan.merge_passes(), 2);

    let reopened = ResearchSourcePlan::open_from(&store, root.path().join("plan"))
        .expect("reopened final plan");
    assert_eq!(reopened.source_plan_digest(), plan.source_plan_digest());
}

#[test]
fn multi_pass_merge_is_equivalent_to_reference() {
    const PARTITIONS: u64 = 65;
    const ROWS_PER_PARTITION: u64 = 1_539;

    let root = TempDir::new().expect("temporary root");
    secure(&root);
    let provenance = DataProvenance::new(digest('a'), digest('b'), ParquetStore::schema_hash())
        .expect("fixture provenance");
    let store = ParquetStore::open(root.path(), provenance).expect("store");
    let mut facts =
        Vec::with_capacity(usize::try_from(PARTITIONS * ROWS_PER_PARTITION).expect("capacity"));
    let mut locators = Vec::with_capacity(usize::try_from(PARTITIONS).expect("capacity"));
    for day in 0..PARTITIONS {
        let base = i64::try_from(day).expect("day") * NANOS_PER_DAY;
        let partition = (0..ROWS_PER_PARTITION)
            .map(|row| {
                let event_time = base + 1_000 + i64::try_from(row * 1_000).expect("row");
                let received_at = if day == 0 && row == 0 {
                    base + 2_000_000
                } else {
                    event_time + 1
                };
                trade(event_time, received_at, day * ROWS_PER_PARTITION + row + 1)
            })
            .collect::<Vec<_>>();
        facts.extend(partition.iter().cloned());
        // The ordinary discovery/replay writer is intentionally bounded below this
        // fixture's aggregate. Commit each final legacy partition independently,
        // then place its already-verified immutable directory below the test root.
        // Task 3 subsequently opens only these exact locators, never discovery.
        let source = TempDir::new().expect("temporary source root");
        secure(&source);
        let source_store = ParquetStore::open(
            source.path(),
            DataProvenance::new(digest('a'), digest('b'), ParquetStore::schema_hash())
                .expect("fixture provenance"),
        )
        .expect("source store");
        let manifests = source_store.write_events(&partition).expect("partition");
        assert_eq!(manifests.len(), 1);
        let source_directory = source
            .path()
            .join("partitions")
            .join(format!("date=utc-day-{day}"))
            .join("kind=trade")
            .join("market=534f4c")
            .join(format!("part-{}.part", manifests[0].partition_id()));
        fs::rename(
            source_directory,
            legacy_partition_parent(root.path(), day)
                .join(format!("part-{}.part", manifests[0].partition_id())),
        )
        .expect("move immutable fixture partition");
        locators.push(ResearchMemberLocator::legacy(&manifests[0]));
    }
    assert_eq!(facts.len(), 100_035);
    let draft = ResearchSourcePlanBuilder::new(
        range(0, NANOS_PER_DAY * 32),
        range(NANOS_PER_DAY * 32, NANOS_PER_DAY * 66),
    )
    .expect("windows")
    .build(&store, locators.clone(), Vec::new())
    .expect("draft");
    let source_for_day = draft
        .members()
        .iter()
        .enumerate()
        .map(|(ordinal, member)| {
            let day = locators
                .iter()
                .position(|locator| locator == member)
                .expect("draft member should originate from one fixture day");
            (
                u32::try_from(ordinal).expect("bounded member ordinal"),
                day,
                member.partition_manifest_digest().to_owned(),
            )
        })
        .collect::<Vec<_>>();
    let plan = draft
        .publish_to(&store, root.path().join("scale-plan"))
        .expect("published scale plan");
    let mut expected = facts;
    expected.sort_by(|left, right| {
        left.received_at()
            .cmp(&right.received_at())
            .then_with(|| left.event_time().cmp(&right.event_time()))
            .then_with(|| left.event_id().cmp(right.event_id()))
    });
    let expected_digest = availability_run_digest(expected.iter().map(|fact| {
        let day = usize::try_from(fact.event_time().value().div_euclid(NANOS_PER_DAY))
            .expect("fixture day");
        let (ordinal, _, manifest_digest) = source_for_day
            .iter()
            .find(|(_, source_day, _)| *source_day == day)
            .expect("fixture event should map to its direct source member");
        AvailabilityDigestRecord::new(fact, *ordinal, manifest_digest, draft.member_set_digest())
    }))
    .expect("independent reference digest");
    let actual = plan
        .availability_run()
        .records()
        .collect::<Result<Vec<_>, _>>()
        .expect("validated scale cursor")
        .into_iter()
        .map(|record| record.event().event_id().clone())
        .collect::<Vec<_>>();
    assert_eq!(
        actual,
        expected
            .into_iter()
            .map(|fact| fact.event_id().clone())
            .collect::<Vec<_>>()
    );
    assert_eq!(plan.availability_run().record_count(), 100_035);
    assert_eq!(plan.availability_run().digest(), expected_digest.digest());
    assert_eq!(
        plan.availability_run().record_count(),
        expected_digest.record_count()
    );
    assert!(plan.merge_passes() >= 2);
}

#[test]
fn open_rejects_tampered_final_run_and_manifest() {
    let root = TempDir::new().expect("temporary root");
    secure(&root);
    let provenance = DataProvenance::new(digest('a'), digest('b'), ParquetStore::schema_hash())
        .expect("fixture provenance");
    let store = ParquetStore::open(root.path(), provenance).expect("store");
    let manifests = store
        .write_events(&[trade(100, 101, 1)])
        .expect("partition");
    let draft = ResearchSourcePlanBuilder::new(range(0, 500), range(500, 1_000))
        .expect("windows")
        .build(
            &store,
            vec![ResearchMemberLocator::legacy(&manifests[0])],
            Vec::new(),
        )
        .expect("draft");
    let run_plan_directory = root.path().join("tampered-run");
    draft
        .publish_to(&store, &run_plan_directory)
        .expect("published plan");
    fs::write(run_plan_directory.join("availability.run"), b"tampered").expect("tamper final run");
    assert!(ResearchSourcePlan::open_from(&store, &run_plan_directory).is_err());

    let manifest_plan_directory = root.path().join("tampered-manifest");
    draft
        .publish_to(&store, &manifest_plan_directory)
        .expect("published plan");
    fs::write(
        manifest_plan_directory.join("research-plan.json"),
        b"{\"version\":1}",
    )
    .expect("tamper final manifest");
    assert!(ResearchSourcePlan::open_from(&store, &manifest_plan_directory).is_err());

    #[cfg(unix)]
    {
        let symlink_plan_directory = root.path().join("symlinked-payload");
        draft
            .publish_to(&store, &symlink_plan_directory)
            .expect("published plan");
        fs::remove_file(symlink_plan_directory.join("availability.run")).expect("remove final run");
        std::os::unix::fs::symlink("/dev/null", symlink_plan_directory.join("availability.run"))
            .expect("swap final run for symlink");
        assert!(ResearchSourcePlan::open_from(&store, &symlink_plan_directory).is_err());
    }
}

#[test]
fn publish_never_clobbers_an_existing_final_plan() {
    let root = TempDir::new().expect("temporary root");
    secure(&root);
    let provenance = DataProvenance::new(digest('a'), digest('b'), ParquetStore::schema_hash())
        .expect("fixture provenance");
    let store = ParquetStore::open(root.path(), provenance).expect("store");
    let first_manifest = store
        .write_events(&[trade(100, 101, 1)])
        .expect("first partition");
    let first_draft = ResearchSourcePlanBuilder::new(range(0, 500), range(500, 1_000))
        .expect("windows")
        .build(
            &store,
            vec![ResearchMemberLocator::legacy(&first_manifest[0])],
            Vec::new(),
        )
        .expect("first draft");
    let final_directory = root.path().join("published");
    let first = first_draft
        .publish_to(&store, &final_directory)
        .expect("first publish");
    let original_run = fs::read(final_directory.join("availability.run")).expect("original run");
    let original_manifest =
        fs::read(final_directory.join("research-plan.json")).expect("original manifest");

    let replay = first_draft
        .publish_to(&store, &final_directory)
        .expect("idempotent publish");
    assert_eq!(replay.source_plan_digest(), first.source_plan_digest());
    assert_eq!(
        fs::read(final_directory.join("availability.run")).expect("idempotent run"),
        original_run
    );
    assert_eq!(
        fs::read(final_directory.join("research-plan.json")).expect("idempotent manifest"),
        original_manifest
    );

    let second_manifest = store
        .write_events(&[trade(NANOS_PER_DAY + 100, NANOS_PER_DAY + 101, 2)])
        .expect("second partition");
    let conflicting_draft = ResearchSourcePlanBuilder::new(
        range(NANOS_PER_DAY, NANOS_PER_DAY + 500),
        range(NANOS_PER_DAY + 500, NANOS_PER_DAY + 1_000),
    )
    .expect("windows")
    .build(
        &store,
        vec![ResearchMemberLocator::legacy(&second_manifest[0])],
        Vec::new(),
    )
    .expect("conflicting draft");
    assert!(matches!(
        conflicting_draft.publish_to(&store, &final_directory),
        Err(ResearchRunError::ConflictingFinalPlan)
    ));
    assert_eq!(
        fs::read(final_directory.join("availability.run")).expect("preserved run"),
        original_run
    );
    assert_eq!(
        fs::read(final_directory.join("research-plan.json")).expect("preserved manifest"),
        original_manifest
    );
}
