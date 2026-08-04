mod support;

use std::{collections::BTreeMap, fs};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use tempfile::TempDir;
use trench_core::event::MarketEvent;
use trench_storage::{
    parquet::{DataProvenance, ParquetStore, PartitionManifest},
    recovery_outcomes::{RecoveryOutcomeStore, RecoverySourceReference},
    research_plan::{ResearchMemberLocator, ResearchSourcePlanBuilder},
    research_runs::AvailabilityKey,
};

use support::{ONE_HOUR_NS, VerifiedRecoveryFixture, range, verified_recovery};

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

fn reference(event: &MarketEvent, manifest_digest: &str) -> RecoverySourceReference {
    RecoverySourceReference::new(
        manifest_digest,
        AvailabilityKey::new(
            event.received_at(),
            event.event_time(),
            event.event_id().clone(),
        )
        .expect("availability key"),
    )
    .expect("source reference")
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

struct PublishedFixture {
    root: TempDir,
    store: ParquetStore,
    manifests: Vec<PartitionManifest>,
    fixture: VerifiedRecoveryFixture,
    predecessor: RecoverySourceReference,
    trade_predecessor: RecoverySourceReference,
    snapshot: RecoverySourceReference,
    local_trades: Vec<RecoverySourceReference>,
    official_candles: Vec<RecoverySourceReference>,
    raw_proof: BTreeMap<RecoverySourceReference, MarketEvent>,
}

impl PublishedFixture {
    fn new() -> Self {
        let (root, store) = store();
        let fixture = verified_recovery();
        let events = std::iter::once(fixture.predecessor.clone())
            .chain(std::iter::once(fixture.snapshot.clone()))
            .chain(fixture.local_trades.iter().cloned())
            .chain(fixture.official_candles.iter().cloned())
            .collect::<Vec<_>>();
        let manifests = store.write_events(&events).expect("source events");
        let source_ref = |event: &MarketEvent| {
            reference(
                event,
                &manifest_for(&store, &manifests, event).manifest_digest(),
            )
        };
        let predecessor = source_ref(&fixture.predecessor);
        let trade_predecessor = predecessor.clone();
        let snapshot = source_ref(&fixture.snapshot);
        let mut local_trade_pairs = fixture
            .local_trades
            .iter()
            .map(|event| (source_ref(event), event.clone()))
            .collect::<Vec<_>>();
        local_trade_pairs
            .sort_by(|left, right| left.0.key().event_id().cmp(right.0.key().event_id()));
        let local_trades = local_trade_pairs
            .iter()
            .map(|(reference, _)| reference.clone())
            .collect::<Vec<_>>();
        let mut official_candle_pairs = fixture
            .official_candles
            .iter()
            .map(|event| (source_ref(event), event.clone()))
            .collect::<Vec<_>>();
        official_candle_pairs
            .sort_by(|left, right| left.0.key().event_id().cmp(right.0.key().event_id()));
        let official_candles = official_candle_pairs
            .iter()
            .map(|(reference, _)| reference.clone())
            .collect::<Vec<_>>();
        let raw_proof = std::iter::once((&predecessor, &fixture.predecessor))
            .chain(std::iter::once((&snapshot, &fixture.snapshot)))
            .chain(
                local_trade_pairs
                    .iter()
                    .map(|(reference, event)| (reference, event)),
            )
            .chain(
                official_candle_pairs
                    .iter()
                    .map(|(reference, event)| (reference, event)),
            )
            .map(|(reference, event)| (reference.clone(), event.clone()))
            .collect();
        Self {
            root,
            store,
            manifests,
            fixture,
            predecessor,
            trade_predecessor,
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
            .expect("nonempty raw proof")
            .clone()
    }

    fn publish(&self) -> trench_storage::recovery_outcomes::RecoveryOutcomeLocator {
        RecoveryOutcomeStore::open(&self.store)
            .expect("outcome store")
            .publish_verified(
                &self.fixture.result,
                Some(self.predecessor.clone()),
                self.trade_predecessor.clone(),
                self.snapshot.clone(),
                self.local_trades.clone(),
                self.official_candles.clone(),
                self.availability_anchor(),
                &self.raw_proof,
            )
            .expect("publish verified outcome")
    }
}

#[test]
fn companion_outcome_reopens_only_from_its_immutable_locator() {
    let fixture = PublishedFixture::new();
    let locator = fixture.publish();
    let reopened = RecoveryOutcomeStore::open(&fixture.store)
        .expect("outcome store")
        .open_member(&locator)
        .expect("reopen outcome");
    assert_eq!(
        reopened.completed_through(),
        support::timestamp(ONE_HOUR_NS)
    );

    let draft =
        ResearchSourcePlanBuilder::new(range(0, ONE_HOUR_NS), range(ONE_HOUR_NS, ONE_HOUR_NS * 2))
            .expect("windows")
            .with_recovery_outcomes(vec![locator])
            .expect("outcome selection")
            .build(
                &fixture.store,
                fixture
                    .manifests
                    .iter()
                    .map(ResearchMemberLocator::legacy)
                    .collect(),
                Vec::new(),
            )
            .expect("source-plan draft");
    let verified = draft
        .publish_to(&fixture.store, fixture.root.path().join("source-plan"))
        .expect("verified source plan");
    assert_eq!(verified.recovery_outcomes().len(), 1);
}

#[test]
fn companion_outcome_rejects_foreign_provenance() {
    let fixture = PublishedFixture::new();
    let locator = fixture.publish();
    let foreign = ParquetStore::open(
        fixture.root.path(),
        DataProvenance::new(digest('c'), digest('b'), ParquetStore::schema_hash())
            .expect("foreign provenance"),
    )
    .expect("foreign store handle");
    assert!(
        RecoveryOutcomeStore::open(&foreign)
            .expect("foreign outcome store")
            .open_member(&locator)
            .is_err()
    );
}

#[test]
fn companion_outcome_requires_every_witness_input_reference() {
    let fixture = PublishedFixture::new();
    let outcomes = RecoveryOutcomeStore::open(&fixture.store).expect("outcome store");
    assert!(
        outcomes
            .publish_verified(
                &fixture.fixture.result,
                Some(fixture.predecessor.clone()),
                fixture.trade_predecessor.clone(),
                fixture.snapshot.clone(),
                Vec::new(),
                fixture.official_candles.clone(),
                fixture.availability_anchor(),
                &fixture.raw_proof,
            )
            .is_err()
    );
    assert!(
        outcomes
            .publish_verified(
                &fixture.fixture.result,
                Some(fixture.predecessor.clone()),
                fixture.trade_predecessor.clone(),
                fixture.snapshot.clone(),
                fixture.local_trades.clone(),
                Vec::new(),
                fixture.availability_anchor(),
                &fixture.raw_proof,
            )
            .is_err()
    );
}

#[test]
fn companion_outcome_rejects_a_tampered_published_payload() {
    let fixture = PublishedFixture::new();
    let locator = fixture.publish();
    let payload = fixture
        .root
        .path()
        .join("recovery-outcomes")
        .join(format!("outcome-{}.out", locator.outcome_id()))
        .join("outcome.json");
    fs::write(payload, b"{}\n").expect("tamper companion payload");

    assert!(
        RecoveryOutcomeStore::open(&fixture.store)
            .expect("outcome store")
            .open_member(&locator)
            .is_err()
    );
}

#[test]
fn companion_outcome_rejects_unexpected_member_entries() {
    let fixture = PublishedFixture::new();
    let locator = fixture.publish();
    fs::write(
        fixture
            .root
            .path()
            .join("recovery-outcomes")
            .join(format!("outcome-{}.out", locator.outcome_id()))
            .join("injected"),
        b"unexpected",
    )
    .expect("inject unexpected entry");

    assert!(
        RecoveryOutcomeStore::open(&fixture.store)
            .expect("outcome store")
            .open_member(&locator)
            .is_err()
    );
}
