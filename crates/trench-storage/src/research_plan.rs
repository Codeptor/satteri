//! Immutable, path-free source selections for offline research.

use std::{
    collections::{BTreeMap, BTreeSet},
    io::{self, Write},
};

use blake3::Hasher;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use trench_core::domain::{EventId, Market};
use trench_core::event::{MarketEvent, MarketEventKind, TimestampNs};
use trench_core::validation::TimeRange;

use crate::parquet::{
    CaptureBatchManifest, DataProvenance, OpenedPartitionMember, ParquetError, ParquetStore,
    PartitionIdentity, PartitionManifest,
};
use crate::recovery_outcomes::{
    MAX_TOTAL_RECOVERY_PROOF_REFERENCES, RecoveryOutcomeError, RecoveryOutcomeLocator,
    RecoveryOutcomeStore,
};

const SOURCE_PLAN_VERSION: u8 = 2;
const MAX_SOURCE_MEMBERS: usize = 4_096;
const MAX_RECOVERY_OUTCOMES: usize = 4_096;
const MAX_COVERAGE_DECLARATIONS: usize = 16_384;
const MAX_SOURCE_BYTES: usize = 64 * 1_024;
const MAX_PAGE_CHAIN_PAGES: usize = 128;
const MAX_PROOF_SOURCE_BYTES: usize = 256 * 1_024;
const MAX_SOURCE_PLAN_JSON_BYTES: usize = 1_048_576;
const MAX_TOTAL_CONTINUITY_EVIDENCE_BYTES: usize = MAX_SOURCE_PLAN_JSON_BYTES;
const MAX_TOTAL_CONTINUITY_BINDINGS: usize = MAX_SOURCE_MEMBERS;
const MAX_TOTAL_REST_PAGE_WITNESSES: usize = 1_024;
const BLAKE3_DIGEST_BYTES: usize = 67;

/// A read-only, exact locator for one selected committed source member.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum ResearchMemberLocator {
    /// A partition committed through the legacy partition layout.
    LegacyPartition {
        identity: PartitionIdentity,
        partition_id: String,
        partition_manifest_digest: String,
    },
    /// A partition committed as part of one atomic capture batch.
    CapturePartition {
        batch_id: String,
        identity: PartitionIdentity,
        partition_id: String,
        batch_manifest_digest: String,
        partition_manifest_digest: String,
    },
}

impl ResearchMemberLocator {
    /// Creates the exact legacy locator for a committed partition manifest.
    #[must_use]
    pub fn legacy(manifest: &PartitionManifest) -> Self {
        Self::LegacyPartition {
            identity: manifest.identity(),
            partition_id: manifest.partition_id().to_owned(),
            partition_manifest_digest: manifest.manifest_digest(),
        }
    }

    /// Creates a capture-member locator after checking batch membership.
    pub fn capture(
        batch: &CaptureBatchManifest,
        manifest: &PartitionManifest,
    ) -> Result<Self, ResearchPlanError> {
        if batch.provenance() != manifest.provenance()
            || !batch.partitions().iter().any(|member| member == manifest)
        {
            return Err(ResearchPlanError::InvalidLocator);
        }
        Ok(Self::CapturePartition {
            batch_id: batch.batch_id().to_owned(),
            identity: manifest.identity(),
            partition_id: manifest.partition_id().to_owned(),
            batch_manifest_digest: batch.manifest_digest(),
            partition_manifest_digest: manifest.manifest_digest(),
        })
    }

    /// Returns the selected partition's immutable manifest digest.
    #[must_use]
    pub fn partition_manifest_digest(&self) -> &str {
        match self {
            Self::LegacyPartition {
                partition_manifest_digest,
                ..
            }
            | Self::CapturePartition {
                partition_manifest_digest,
                ..
            } => partition_manifest_digest,
        }
    }

    fn validate(&self) -> Result<(), ResearchPlanError> {
        match self {
            Self::LegacyPartition {
                partition_id,
                partition_manifest_digest,
                ..
            } => {
                validate_locator_digest(partition_id)?;
                validate_locator_digest(partition_manifest_digest)?;
            }
            Self::CapturePartition {
                batch_id,
                partition_id,
                batch_manifest_digest,
                partition_manifest_digest,
                ..
            } => {
                for value in [
                    batch_id.as_str(),
                    partition_id.as_str(),
                    batch_manifest_digest.as_str(),
                    partition_manifest_digest.as_str(),
                ] {
                    validate_locator_digest(value)?;
                }
            }
        }
        Ok(())
    }

    fn member_identity(&self) -> MemberIdentity {
        match self {
            Self::LegacyPartition {
                identity,
                partition_id,
                ..
            } => MemberIdentity::Legacy {
                identity: identity.clone(),
                partition_id: partition_id.clone(),
            },
            Self::CapturePartition {
                batch_id,
                identity,
                partition_id,
                ..
            } => MemberIdentity::Capture {
                batch_id: batch_id.clone(),
                identity: identity.clone(),
                partition_id: partition_id.clone(),
            },
        }
    }

    pub(crate) fn open(&self, store: &ParquetStore) -> Result<OpenedPartitionMember, ParquetError> {
        match self {
            Self::LegacyPartition {
                identity,
                partition_id,
                partition_manifest_digest,
            } => store.open_legacy_member(identity, partition_id, partition_manifest_digest),
            Self::CapturePartition {
                batch_id,
                identity,
                partition_id,
                batch_manifest_digest,
                partition_manifest_digest,
            } => store.open_capture_member(
                batch_id,
                identity,
                partition_id,
                batch_manifest_digest,
                partition_manifest_digest,
            ),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum MemberIdentity {
    Legacy {
        identity: PartitionIdentity,
        partition_id: String,
    },
    Capture {
        batch_id: String,
        identity: PartitionIdentity,
        partition_id: String,
    },
}

/// A normalized source stream for which continuity may be declared.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum SourceStreamKind {
    /// Point-in-time venue metadata.
    Metadata,
    /// Point-in-time asset context.
    AssetContext,
    /// Full executable L2 snapshots.
    BookSnapshot,
    /// Best-bid/best-ask updates.
    Bbo,
    /// Public trades.
    Trade,
    /// Funding observations.
    Funding,
    /// Completed candle observations.
    CompletedCandle,
}

impl SourceStreamKind {
    fn matches(self, event: &MarketEvent) -> bool {
        matches!(
            (self, event.kind()),
            (Self::Metadata, MarketEventKind::Metadata(_))
                | (Self::AssetContext, MarketEventKind::AssetContext(_))
                | (Self::BookSnapshot, MarketEventKind::BookSnapshot(_))
                | (Self::Bbo, MarketEventKind::Bbo(_))
                | (Self::Trade, MarketEventKind::Trade(_))
                | (Self::Funding, MarketEventKind::Funding(_))
                | (Self::CompletedCandle, MarketEventKind::CompletedCandle(_))
        )
    }

    fn from_event(event: &MarketEvent) -> Self {
        match event.kind() {
            MarketEventKind::Metadata(_) => Self::Metadata,
            MarketEventKind::AssetContext(_) => Self::AssetContext,
            MarketEventKind::BookSnapshot(_) => Self::BookSnapshot,
            MarketEventKind::Bbo(_) => Self::Bbo,
            MarketEventKind::Trade(_) => Self::Trade,
            MarketEventKind::Funding(_) => Self::Funding,
            MarketEventKind::CompletedCandle(_) => Self::CompletedCandle,
        }
    }
}

/// The market and stream addressed by one coverage declaration.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub struct CoverageTarget {
    market: Market,
    stream: SourceStreamKind,
}

impl<'de> Deserialize<'de> for CoverageTarget {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            market: String,
            stream: SourceStreamKind,
        }

        let wire = Wire::deserialize(deserializer)?;
        let market = Market::new(wire.market).map_err(serde::de::Error::custom)?;
        Ok(Self::new(market, wire.stream))
    }
}

impl CoverageTarget {
    /// Creates one typed coverage target.
    #[must_use]
    pub const fn new(market: Market, stream: SourceStreamKind) -> Self {
        Self { market, stream }
    }

    /// Returns the target market.
    #[must_use]
    pub const fn market(&self) -> &Market {
        &self.market
    }

    /// Returns the target source stream.
    #[must_use]
    pub const fn stream(&self) -> SourceStreamKind {
        self.stream
    }

    fn from_event(event: &MarketEvent) -> Self {
        Self {
            market: event.market().clone(),
            stream: SourceStreamKind::from_event(event),
        }
    }
}

/// One verified row identity used as a continuity boundary.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub struct CoverageEventRef {
    partition_manifest_digest: String,
    event_id: EventId,
    event_time_ns: i64,
}

impl<'de> Deserialize<'de> for CoverageEventRef {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            partition_manifest_digest: String,
            event_id: String,
            event_time_ns: i64,
        }

        let wire = Wire::deserialize(deserializer)?;
        let event_id = EventId::new(wire.event_id).map_err(serde::de::Error::custom)?;
        TimestampNs::new(i128::from(wire.event_time_ns)).map_err(serde::de::Error::custom)?;
        Ok(Self {
            partition_manifest_digest: wire.partition_manifest_digest,
            event_id,
            event_time_ns: wire.event_time_ns,
        })
    }
}

impl CoverageEventRef {
    /// Creates a source-row reference that the builder must resolve exactly.
    #[must_use]
    pub fn new(
        partition_manifest_digest: String,
        event_id: EventId,
        event_time: TimestampNs,
    ) -> Self {
        Self {
            partition_manifest_digest,
            event_id,
            event_time_ns: event_time.value(),
        }
    }

    /// Returns the committed partition manifest that contains this event.
    #[must_use]
    pub fn partition_manifest_digest(&self) -> &str {
        &self.partition_manifest_digest
    }

    /// Returns the normalized event identity.
    #[must_use]
    pub const fn event_id(&self) -> &EventId {
        &self.event_id
    }

    /// Returns the asserted authoritative event time.
    #[must_use]
    pub fn event_time(&self) -> TimestampNs {
        TimestampNs::new(i128::from(self.event_time_ns))
            .expect("coverage event references retain validated timestamps")
    }

    fn validate(&self) -> Result<(), ResearchPlanError> {
        validate_blake3_digest(&self.partition_manifest_digest)?;
        validate_blake3_digest(self.event_id.as_str())
    }
}

/// Bounded, nonempty upstream source bytes retained by a continuity artifact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BoundedSourceBytes(Vec<u8>);

impl BoundedSourceBytes {
    /// Validates and retains one bounded upstream payload.
    pub fn new(bytes: Vec<u8>) -> Result<Self, ResearchPlanError> {
        if bytes.is_empty() || bytes.len() > MAX_SOURCE_BYTES {
            return Err(ResearchPlanError::ResourceLimit);
        }
        Ok(Self(bytes))
    }

    fn len(&self) -> usize {
        self.0.len()
    }
}

impl<'de> Deserialize<'de> for BoundedSourceBytes {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Self::new(Vec::<u8>::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

/// A typed upstream continuity artifact, never a Boolean or assertion string.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum ContinuitySource {
    /// A bounded archive-manifest payload.
    ArchiveManifest { source_bytes: BoundedSourceBytes },
    /// A bounded ordered chain of captured REST page payloads.
    RestPageChain { pages: Vec<BoundedSourceBytes> },
    /// A bounded WebSocket heartbeat/sequence witness.
    WebSocketSequenceRange {
        source_bytes: BoundedSourceBytes,
        first_sequence: u64,
        last_sequence: u64,
    },
}

impl ContinuitySource {
    /// Creates an archive-manifest proof payload.
    pub fn archive_manifest(source_bytes: Vec<u8>) -> Result<Self, ResearchPlanError> {
        Ok(Self::ArchiveManifest {
            source_bytes: BoundedSourceBytes::new(source_bytes)?,
        })
    }

    /// Creates a captured REST page-chain payload.
    pub fn rest_page_chain(pages: Vec<Vec<u8>>) -> Result<Self, ResearchPlanError> {
        if pages.is_empty() || pages.len() > MAX_PAGE_CHAIN_PAGES {
            return Err(ResearchPlanError::ResourceLimit);
        }
        let pages = pages
            .into_iter()
            .map(BoundedSourceBytes::new)
            .collect::<Result<Vec<_>, _>>()?;
        let source = Self::RestPageChain { pages };
        source.validate()?;
        Ok(source)
    }

    /// Creates a captured WebSocket heartbeat/sequence-range payload.
    pub fn websocket_sequence_range(
        source_bytes: Vec<u8>,
        first_sequence: u64,
        last_sequence: u64,
    ) -> Result<Self, ResearchPlanError> {
        let source = Self::WebSocketSequenceRange {
            source_bytes: BoundedSourceBytes::new(source_bytes)?,
            first_sequence,
            last_sequence,
        };
        source.validate()?;
        Ok(source)
    }

    fn validate(&self) -> Result<(), ResearchPlanError> {
        let total = self.evidence_bytes();
        if total > MAX_PROOF_SOURCE_BYTES {
            return Err(ResearchPlanError::ResourceLimit);
        }
        if let Self::WebSocketSequenceRange {
            first_sequence,
            last_sequence,
            ..
        } = self
            && first_sequence > last_sequence
        {
            return Err(ResearchPlanError::InvalidCoverageEvidence);
        }
        Ok(())
    }

    fn evidence_bytes(&self) -> usize {
        match self {
            Self::ArchiveManifest { source_bytes }
            | Self::WebSocketSequenceRange { source_bytes, .. } => source_bytes.len(),
            Self::RestPageChain { pages } => pages.iter().map(BoundedSourceBytes::len).sum(),
        }
    }

    fn rest_page_witnesses(&self) -> usize {
        match self {
            Self::RestPageChain { pages } => pages.len(),
            Self::ArchiveManifest { .. } | Self::WebSocketSequenceRange { .. } => 0,
        }
    }
}

/// One selected member whose exact normalized content supports a proof.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContinuityMemberBinding {
    partition_manifest_digest: String,
    partition_content_digest: String,
}

impl ContinuityMemberBinding {
    /// Creates a typed proof binding from a verified committed member manifest.
    #[must_use]
    pub fn from_manifest(manifest: &PartitionManifest) -> Self {
        Self {
            partition_manifest_digest: manifest.manifest_digest(),
            partition_content_digest: manifest.content_digest().to_owned(),
        }
    }

    fn validate(&self) -> Result<(), ResearchPlanError> {
        validate_blake3_digest(&self.partition_manifest_digest)?;
        validate_blake3_digest(&self.partition_content_digest)
    }
}

/// Bounded, typed continuity evidence over one exact half-open interval.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContinuityProof {
    source: ContinuitySource,
    supporting_members: Vec<ContinuityMemberBinding>,
    source_digest: String,
    range: TimeRangeWire,
    predecessor: Option<CoverageEventRef>,
    successor: Option<CoverageEventRef>,
}

impl ContinuityProof {
    /// Creates continuity evidence and derives its digest from source, bindings, and range.
    ///
    /// Boundary references are separately validated and serialized, but are not digest inputs.
    pub fn new(
        source: ContinuitySource,
        mut supporting_members: Vec<ContinuityMemberBinding>,
        range: TimeRange,
        predecessor: Option<CoverageEventRef>,
        successor: Option<CoverageEventRef>,
    ) -> Result<Self, ResearchPlanError> {
        source.validate()?;
        if supporting_members.is_empty() || supporting_members.len() > MAX_SOURCE_MEMBERS {
            return Err(ResearchPlanError::InvalidCoverageEvidence);
        }
        supporting_members.sort();
        let mut previous = None;
        for member in &supporting_members {
            member.validate()?;
            if previous
                .replace(member.partition_manifest_digest.as_str())
                .is_some_and(|prior| prior == member.partition_manifest_digest)
            {
                return Err(ResearchPlanError::InvalidCoverageEvidence);
            }
        }
        let source_digest = continuity_source_digest(&source, &supporting_members, range)?;
        Ok(Self {
            source,
            supporting_members,
            source_digest,
            range: range.into(),
            predecessor,
            successor,
        })
    }

    /// Returns the derived digest of the canonical typed source artifact.
    #[must_use]
    pub fn source_digest(&self) -> &str {
        &self.source_digest
    }

    fn range(&self) -> TimeRange {
        self.range.range()
    }

    fn validate(&self) -> Result<(), ResearchPlanError> {
        self.source.validate()?;
        if self.supporting_members.is_empty() || self.supporting_members.len() > MAX_SOURCE_MEMBERS
        {
            return Err(ResearchPlanError::InvalidCoverageEvidence);
        }
        let mut previous = None;
        for member in &self.supporting_members {
            member.validate()?;
            if previous
                .replace(member.partition_manifest_digest.as_str())
                .is_some_and(|prior| prior >= member.partition_manifest_digest.as_str())
            {
                return Err(ResearchPlanError::InvalidCoverageEvidence);
            }
        }
        if self.source_digest
            != continuity_source_digest(&self.source, &self.supporting_members, self.range())?
        {
            return Err(ResearchPlanError::InvalidCoverageEvidence);
        }
        Ok(())
    }

    fn binds(&self, reference: &CoverageEventRef) -> bool {
        self.supporting_members
            .binary_search_by(|member| {
                member
                    .partition_manifest_digest
                    .as_str()
                    .cmp(reference.partition_manifest_digest())
            })
            .is_ok()
    }

    fn evidence_bytes(&self) -> usize {
        self.source.evidence_bytes()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
enum CoverageObservation {
    Events {
        first: CoverageEventRef,
        last: CoverageEventRef,
    },
    NoEvents,
}

/// Evidence for either an observed eventful interval or a proved quiet interval.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompleteCoverage {
    proof: ContinuityProof,
    observation: CoverageObservation,
}

impl CompleteCoverage {
    /// Creates coverage that observed a first and final normalized event.
    pub fn events(
        proof: ContinuityProof,
        first: CoverageEventRef,
        last: CoverageEventRef,
    ) -> Result<Self, ResearchPlanError> {
        if event_ref_key(&first) > event_ref_key(&last) {
            return Err(ResearchPlanError::InvalidCoverageEvidence);
        }
        Ok(Self {
            proof,
            observation: CoverageObservation::Events { first, last },
        })
    }

    /// Creates coverage that proves no event occurred in the interval.
    pub fn observed_no_events(proof: ContinuityProof) -> Result<Self, ResearchPlanError> {
        Ok(Self {
            proof,
            observation: CoverageObservation::NoEvents,
        })
    }

    fn proof(&self) -> &ContinuityProof {
        &self.proof
    }
}

/// Closed reasons for an explicitly unavailable source interval.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum CoverageUnavailableReason {
    /// The collector never committed this interval.
    NotCaptured,
    /// Upstream historical retention did not include the interval.
    UpstreamRetentionExpired,
    /// An upstream source explicitly declared the interval unavailable.
    UpstreamUnavailable,
    /// Available evidence conflicts and cannot prove continuity.
    ConflictingEvidence,
}

/// A coverage result: proved eventful, proved quiet, or explicitly unavailable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum CoverageWitness {
    /// Complete continuity with observed source events.
    Complete(CompleteCoverage),
    /// Complete continuity proving no source events occurred.
    ObservedNoEvents(CompleteCoverage),
    /// An interval for which no coverage claim is made.
    Unavailable {
        /// Machine-readable reason for the unavailable interval.
        reason: CoverageUnavailableReason,
    },
}

/// One exact coverage declaration for one target and one half-open interval.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CoverageDeclaration {
    target: CoverageTarget,
    range: TimeRangeWire,
    witness: CoverageWitness,
}

impl CoverageDeclaration {
    /// Creates a coverage declaration and binds complete proof ranges exactly.
    pub fn new(
        target: CoverageTarget,
        range: TimeRange,
        witness: CoverageWitness,
    ) -> Result<Self, ResearchPlanError> {
        if let CoverageWitness::Complete(coverage) | CoverageWitness::ObservedNoEvents(coverage) =
            &witness
            && coverage.proof().range() != range
        {
            return Err(ResearchPlanError::InvalidCoverageEvidence);
        }
        Ok(Self {
            target,
            range: range.into(),
            witness,
        })
    }

    /// Returns whether this declaration proves interval coverage.
    #[must_use]
    pub const fn is_complete(&self) -> bool {
        matches!(
            self.witness,
            CoverageWitness::Complete(_) | CoverageWitness::ObservedNoEvents(_)
        )
    }

    fn range(&self) -> TimeRange {
        self.range.range()
    }

    fn sort_key(&self) -> (&CoverageTarget, TimeRangeWire) {
        (&self.target, self.range)
    }
}

/// An in-memory source-plan draft. It has no persistence or open/publish API.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResearchSourcePlanDraft {
    provenance: DataProvenance,
    members: Vec<ResearchMemberLocator>,
    coverage: Vec<CoverageDeclaration>,
    recovery_outcomes: Vec<RecoveryOutcomeLocator>,
    warmup: TimeRange,
    evaluation: TimeRange,
    member_set_digest: String,
}

impl ResearchSourcePlanDraft {
    /// Returns canonically ordered, path-free source locators.
    #[must_use]
    pub fn members(&self) -> &[ResearchMemberLocator] {
        &self.members
    }

    /// Returns canonically ordered coverage declarations.
    #[must_use]
    pub fn coverage(&self) -> &[CoverageDeclaration] {
        &self.coverage
    }

    /// Returns path-free immutable recovery companion members selected for this source plan.
    #[must_use]
    pub fn recovery_outcomes(&self) -> &[RecoveryOutcomeLocator] {
        &self.recovery_outcomes
    }

    /// Returns the sole pre-run source commitment available in Task 2.
    #[must_use]
    pub fn member_set_digest(&self) -> &str {
        &self.member_set_digest
    }

    pub(crate) const fn provenance(&self) -> &DataProvenance {
        &self.provenance
    }

    /// Returns bounded canonical JSON for inspection and later Task 3 staging.
    pub fn canonical_json(&self) -> Result<Vec<u8>, ResearchPlanError> {
        let mut writer = BoundedJsonWriter::new(MAX_SOURCE_PLAN_JSON_BYTES);
        let wire = ResearchSourcePlanDraftWire {
            version: SOURCE_PLAN_VERSION,
            provenance: &self.provenance,
            warmup: self.warmup.into(),
            evaluation: self.evaluation.into(),
            members: &self.members,
            coverage: &self.coverage,
            recovery_outcomes: &self.recovery_outcomes,
            member_set_digest: &self.member_set_digest,
        };
        match serde_json::to_writer(&mut writer, &wire) {
            Ok(()) => Ok(writer.into_bytes()),
            Err(_) if writer.exceeded_limit() => Err(ResearchPlanError::ResourceLimit),
            Err(error) => Err(ResearchPlanError::Json(error)),
        }
    }

    pub(crate) fn wire(&self) -> ResearchSourcePlanWire {
        ResearchSourcePlanWire {
            version: SOURCE_PLAN_VERSION,
            provenance: self.provenance.clone(),
            warmup: self.warmup.into(),
            evaluation: self.evaluation.into(),
            members: self.members.clone(),
            coverage: self.coverage.clone(),
            recovery_outcomes: self.recovery_outcomes.clone(),
            member_set_digest: self.member_set_digest.clone(),
        }
    }

    pub(crate) fn from_wire(
        store: &ParquetStore,
        wire: ResearchSourcePlanWire,
    ) -> Result<Self, ResearchPlanError> {
        if wire.version != SOURCE_PLAN_VERSION {
            return Err(ResearchPlanError::InvalidFinalPlan);
        }
        let warmup = wire.warmup.try_range()?;
        let evaluation = wire.evaluation.try_range()?;
        validate_deserialized_coverage(&wire.coverage)?;
        let draft = ResearchSourcePlanBuilder::new(warmup, evaluation)?
            .with_recovery_outcomes(wire.recovery_outcomes)?
            .build(store, wire.members, wire.coverage)?;
        if draft.provenance != wire.provenance || draft.member_set_digest != wire.member_set_digest
        {
            return Err(ResearchPlanError::InvalidFinalPlan);
        }
        Ok(draft)
    }
}

fn validate_deserialized_coverage(
    coverage: &[CoverageDeclaration],
) -> Result<(), ResearchPlanError> {
    for declaration in coverage {
        let declared_range = declaration.range.try_range()?;
        let Some(complete) = complete_coverage(declaration) else {
            continue;
        };
        if complete.proof.range.try_range()? != declared_range {
            return Err(ResearchPlanError::InvalidCoverageEvidence);
        }
        complete.proof.validate()?;
    }
    Ok(())
}

/// Canonical owned source-plan fields persisted only by the Task-3 final-plan writer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ResearchSourcePlanWire {
    pub(crate) version: u8,
    pub(crate) provenance: DataProvenance,
    pub(crate) warmup: TimeRangeWire,
    pub(crate) evaluation: TimeRangeWire,
    pub(crate) members: Vec<ResearchMemberLocator>,
    pub(crate) coverage: Vec<CoverageDeclaration>,
    pub(crate) recovery_outcomes: Vec<RecoveryOutcomeLocator>,
    pub(crate) member_set_digest: String,
}

/// Validates exact selected members beneath a configured store before producing a draft.
#[derive(Debug, Clone)]
pub struct ResearchSourcePlanBuilder {
    warmup: TimeRange,
    evaluation: TimeRange,
    recovery_outcomes: Vec<RecoveryOutcomeLocator>,
}

impl ResearchSourcePlanBuilder {
    /// Creates a builder for contiguous half-open warmup and evaluation windows.
    pub fn new(warmup: TimeRange, evaluation: TimeRange) -> Result<Self, ResearchPlanError> {
        if warmup.end() != evaluation.start() {
            return Err(ResearchPlanError::InvalidWindows);
        }
        Ok(Self {
            warmup,
            evaluation,
            recovery_outcomes: Vec::new(),
        })
    }

    /// Selects exact immutable recovery companion members for later causal compilation.
    pub fn with_recovery_outcomes(
        mut self,
        mut recovery_outcomes: Vec<RecoveryOutcomeLocator>,
    ) -> Result<Self, ResearchPlanError> {
        if recovery_outcomes.len() > MAX_RECOVERY_OUTCOMES {
            return Err(ResearchPlanError::ResourceLimit);
        }
        recovery_outcomes.sort();
        if recovery_outcomes.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(ResearchPlanError::InvalidRecoveryOutcome);
        }
        for locator in &recovery_outcomes {
            locator.validate()?;
        }
        self.recovery_outcomes = recovery_outcomes;
        Ok(self)
    }

    /// Resolves every member without discovery and returns an unpublishable draft.
    pub fn build(
        self,
        store: &ParquetStore,
        mut members: Vec<ResearchMemberLocator>,
        mut coverage: Vec<CoverageDeclaration>,
    ) -> Result<ResearchSourcePlanDraft, ResearchPlanError> {
        if members.is_empty() {
            return Err(ResearchPlanError::EmptyMembers);
        }
        if members.len() > MAX_SOURCE_MEMBERS || coverage.len() > MAX_COVERAGE_DECLARATIONS {
            return Err(ResearchPlanError::ResourceLimit);
        }
        validate_aggregate_continuity_evidence(&coverage)?;
        members.sort();
        let mut identities = BTreeSet::new();
        let mut manifest_digests = BTreeSet::new();
        let mut manifests = BTreeMap::new();
        let mut provenance = None;
        for locator in &members {
            locator.validate()?;
            if !identities.insert(locator.member_identity())
                || !manifest_digests.insert(locator.partition_manifest_digest().to_owned())
            {
                return Err(ResearchPlanError::DuplicateMember {
                    partition_manifest_digest: locator.partition_manifest_digest().to_owned(),
                });
            }
            let opened = locator.open(store)?;
            let actual = opened.manifest();
            if actual.manifest_digest() != locator.partition_manifest_digest() {
                return Err(ResearchPlanError::InvalidLocator);
            }
            match &provenance {
                Some(expected) if expected != actual.provenance() => {
                    return Err(ResearchPlanError::MixedProvenance);
                }
                Some(_) => {}
                None => provenance = Some(actual.provenance().clone()),
            }
            manifests.insert(actual.manifest_digest(), actual.clone());
        }
        let provenance = provenance.expect("nonempty members establish provenance");
        let outcome_store = RecoveryOutcomeStore::open(store)?;
        let mut outcome_references = 0_usize;
        for locator in &self.recovery_outcomes {
            let outcome = outcome_store.open_member(locator)?;
            outcome_references = outcome_references
                .checked_add(outcome.source_references().count())
                .ok_or(ResearchPlanError::ResourceLimit)?;
            if outcome_references > MAX_TOTAL_RECOVERY_PROOF_REFERENCES {
                return Err(ResearchPlanError::ResourceLimit);
            }
            if outcome
                .source_references()
                .any(|reference| !manifest_digests.contains(reference.member_manifest_digest()))
            {
                return Err(ResearchPlanError::InvalidRecoveryOutcome);
            }
        }
        coverage.sort_by(|left, right| left.sort_key().cmp(&right.sort_key()));
        self.validate_coverage_ranges(&coverage)?;
        let mut validator = CoverageValidator::new(&coverage, &manifests)?;
        for locator in &members {
            let digest = locator.partition_manifest_digest().to_owned();
            for event in locator.open(store)?.read_all()? {
                validator.observe(&digest, event);
            }
        }
        validator.finish(&coverage)?;
        let member_set_digest = member_set_digest(&provenance, &members)?;
        Ok(ResearchSourcePlanDraft {
            provenance,
            members,
            coverage,
            recovery_outcomes: self.recovery_outcomes,
            warmup: self.warmup,
            evaluation: self.evaluation,
            member_set_digest,
        })
    }

    fn validate_coverage_ranges(
        &self,
        coverage: &[CoverageDeclaration],
    ) -> Result<(), ResearchPlanError> {
        let requested = TimeRange::new(self.warmup.start(), self.evaluation.end())
            .expect("contiguous builder windows produce a nonempty source interval");
        let mut previous = BTreeMap::<CoverageTarget, TimeRange>::new();
        for declaration in coverage {
            let range = declaration.range();
            if range.start() < requested.start() || range.end() > requested.end() {
                return Err(ResearchPlanError::InvalidCoverageRange);
            }
            if let Some(prior) = previous.insert(declaration.target.clone(), range)
                && prior.end() > range.start()
            {
                return Err(ResearchPlanError::InvalidCoverageRange);
            }
        }
        Ok(())
    }
}

fn validate_aggregate_continuity_evidence(
    coverage: &[CoverageDeclaration],
) -> Result<(), ResearchPlanError> {
    let (evidence_bytes, bindings, rest_page_witnesses) =
        coverage.iter().filter_map(complete_coverage).try_fold(
            (0_usize, 0_usize, 0_usize),
            |(evidence_bytes, bindings, rest_page_witnesses), coverage| {
                let evidence_bytes = evidence_bytes
                    .checked_add(coverage.proof().evidence_bytes())
                    .ok_or(ResearchPlanError::ResourceLimit)?;
                let bindings = bindings
                    .checked_add(coverage.proof().supporting_members.len())
                    .ok_or(ResearchPlanError::ResourceLimit)?;
                let rest_page_witnesses = rest_page_witnesses
                    .checked_add(coverage.proof().source.rest_page_witnesses())
                    .ok_or(ResearchPlanError::ResourceLimit)?;
                Ok::<_, ResearchPlanError>((evidence_bytes, bindings, rest_page_witnesses))
            },
        )?;
    if evidence_bytes > MAX_TOTAL_CONTINUITY_EVIDENCE_BYTES
        || bindings > MAX_TOTAL_CONTINUITY_BINDINGS
        || rest_page_witnesses > MAX_TOTAL_REST_PAGE_WITNESSES
    {
        return Err(ResearchPlanError::ResourceLimit);
    }
    Ok(())
}

#[derive(Debug, Default)]
struct MatchingSourceEvents {
    first: Option<MarketEvent>,
    last: Option<MarketEvent>,
}

#[derive(Debug, Clone, Copy)]
struct CoverageRangeIndex {
    range: TimeRange,
    index: usize,
}

struct CoverageValidator {
    ranges: BTreeMap<CoverageTarget, Vec<CoverageRangeIndex>>,
    references: BTreeSet<CoverageEventRef>,
    referenced_events: BTreeMap<CoverageEventRef, MarketEvent>,
    matching: Vec<MatchingSourceEvents>,
}

impl CoverageValidator {
    fn new(
        coverage: &[CoverageDeclaration],
        manifests: &BTreeMap<String, PartitionManifest>,
    ) -> Result<Self, ResearchPlanError> {
        let mut ranges = BTreeMap::<CoverageTarget, Vec<CoverageRangeIndex>>::new();
        let mut references = BTreeSet::new();
        for (index, declaration) in coverage.iter().enumerate() {
            ranges
                .entry(declaration.target.clone())
                .or_default()
                .push(CoverageRangeIndex {
                    range: declaration.range(),
                    index,
                });
            let Some(complete) = complete_coverage(declaration) else {
                continue;
            };
            complete.proof.validate()?;
            for binding in &complete.proof.supporting_members {
                let manifest = manifests
                    .get(&binding.partition_manifest_digest)
                    .ok_or(ResearchPlanError::InvalidCoverageEvidence)?;
                if manifest.content_digest() != binding.partition_content_digest {
                    return Err(ResearchPlanError::InvalidCoverageEvidence);
                }
            }
            for reference in coverage_references(complete) {
                reference.validate()?;
                if !complete.proof.binds(reference) {
                    return Err(ResearchPlanError::InvalidCoverageEvidence);
                }
                references.insert(reference.clone());
            }
        }
        Ok(Self {
            ranges,
            references,
            referenced_events: BTreeMap::new(),
            matching: (0..coverage.len())
                .map(|_| MatchingSourceEvents::default())
                .collect(),
        })
    }

    fn observe(&mut self, member_digest: &str, event: MarketEvent) {
        let reference = CoverageEventRef::new(
            member_digest.to_owned(),
            event.event_id().clone(),
            event.event_time(),
        );
        if self.references.contains(&reference) {
            self.referenced_events.insert(reference, event.clone());
        }
        let target = CoverageTarget::from_event(&event);
        let Some(ranges) = self.ranges.get(&target) else {
            return;
        };
        let candidate =
            ranges.partition_point(|candidate| candidate.range.end() <= event.event_time());
        let Some(CoverageRangeIndex { range, index }) = ranges.get(candidate).copied() else {
            return;
        };
        if !contains(range, event.event_time()) {
            return;
        }
        let matching = &mut self.matching[index];
        if matching
            .first
            .as_ref()
            .is_none_or(|first| event_key(&event) < event_key(first))
        {
            matching.first = Some(event.clone());
        }
        if matching
            .last
            .as_ref()
            .is_none_or(|last| event_key(&event) > event_key(last))
        {
            matching.last = Some(event);
        }
    }

    fn finish(self, coverage: &[CoverageDeclaration]) -> Result<(), ResearchPlanError> {
        for (index, declaration) in coverage.iter().enumerate() {
            let Some(complete) = complete_coverage(declaration) else {
                continue;
            };
            let range = declaration.range();
            let predecessor = complete
                .proof
                .predecessor
                .as_ref()
                .ok_or(ResearchPlanError::InvalidCoverageEvidence)?;
            let successor = complete
                .proof
                .successor
                .as_ref()
                .ok_or(ResearchPlanError::InvalidCoverageEvidence)?;
            let predecessor_event = self.resolve(predecessor, declaration)?;
            let successor_event = self.resolve(successor, declaration)?;
            if predecessor_event.event_time() >= range.start()
                || successor_event.event_time() < range.end()
            {
                return Err(ResearchPlanError::InvalidCoverageEvidence);
            }
            match (&declaration.witness, &complete.observation) {
                (CoverageWitness::Complete(_), CoverageObservation::Events { first, last }) => {
                    let first_event = self.resolve(first, declaration)?;
                    let last_event = self.resolve(last, declaration)?;
                    if !contains(range, first_event.event_time())
                        || !contains(range, last_event.event_time())
                        || event_key(&first_event) > event_key(&last_event)
                        || self.matching[index].first.as_ref() != Some(&first_event)
                        || self.matching[index].last.as_ref() != Some(&last_event)
                    {
                        return Err(ResearchPlanError::InvalidCoverageEvidence);
                    }
                }
                (CoverageWitness::ObservedNoEvents(_), CoverageObservation::NoEvents)
                    if self.matching[index].first.is_none() => {}
                _ => return Err(ResearchPlanError::InvalidCoverageEvidence),
            }
        }
        Ok(())
    }

    fn resolve(
        &self,
        reference: &CoverageEventRef,
        declaration: &CoverageDeclaration,
    ) -> Result<MarketEvent, ResearchPlanError> {
        let event = self
            .referenced_events
            .get(reference)
            .cloned()
            .ok_or(ResearchPlanError::InvalidCoverageEvidence)?;
        if event.event_time() != reference.event_time()
            || event.market() != declaration.target.market()
            || !declaration.target.stream().matches(&event)
        {
            return Err(ResearchPlanError::InvalidCoverageEvidence);
        }
        Ok(event)
    }
}

fn complete_coverage(declaration: &CoverageDeclaration) -> Option<&CompleteCoverage> {
    match &declaration.witness {
        CoverageWitness::Complete(coverage) | CoverageWitness::ObservedNoEvents(coverage) => {
            Some(coverage)
        }
        CoverageWitness::Unavailable { .. } => None,
    }
}

fn coverage_references(coverage: &CompleteCoverage) -> Vec<&CoverageEventRef> {
    let mut references = Vec::with_capacity(4);
    if let Some(predecessor) = coverage.proof.predecessor.as_ref() {
        references.push(predecessor);
    }
    if let Some(successor) = coverage.proof.successor.as_ref() {
        references.push(successor);
    }
    if let CoverageObservation::Events { first, last } = &coverage.observation {
        references.extend([first, last]);
    }
    references
}

fn contains(range: TimeRange, timestamp: TimestampNs) -> bool {
    range.start() <= timestamp && timestamp < range.end()
}

fn event_ref_key(reference: &CoverageEventRef) -> (i64, &EventId) {
    (reference.event_time_ns, &reference.event_id)
}

fn event_key(event: &MarketEvent) -> (TimestampNs, &EventId) {
    (event.event_time(), event.event_id())
}

fn continuity_source_digest(
    source: &ContinuitySource,
    supporting_members: &[ContinuityMemberBinding],
    range: TimeRange,
) -> Result<String, ResearchPlanError> {
    let canonical = serde_json::to_vec(&ContinuityDigestWire {
        source,
        supporting_members,
        range: range.into(),
    })?;
    let mut hasher = Hasher::new_derive_key("trench.research.continuity-source.v1");
    hasher.update(&(canonical.len() as u64).to_be_bytes());
    hasher.update(&canonical);
    Ok(format!("b3:{}", hasher.finalize().to_hex()))
}

#[derive(Serialize)]
struct ContinuityDigestWire<'a> {
    source: &'a ContinuitySource,
    supporting_members: &'a [ContinuityMemberBinding],
    range: TimeRangeWire,
}

fn validate_blake3_digest(value: &str) -> Result<(), ResearchPlanError> {
    let Some(hex) = value.strip_prefix("b3:") else {
        return Err(ResearchPlanError::InvalidCoverageEvidence);
    };
    if value.len() != BLAKE3_DIGEST_BYTES
        || hex.len() != blake3::OUT_LEN * 2
        || !hex
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(ResearchPlanError::InvalidCoverageEvidence);
    }
    Ok(())
}

fn validate_locator_digest(value: &str) -> Result<(), ResearchPlanError> {
    validate_blake3_digest(value).map_err(|_| ResearchPlanError::InvalidLocator)
}

fn member_set_digest(
    provenance: &DataProvenance,
    members: &[ResearchMemberLocator],
) -> Result<String, ResearchPlanError> {
    let canonical = serde_json::to_vec(&MemberSetWire {
        version: SOURCE_PLAN_VERSION,
        provenance,
        members,
    })?;
    let mut hasher = Hasher::new_derive_key("trench.research.member-set.v1");
    hasher.update(&(canonical.len() as u64).to_be_bytes());
    hasher.update(&canonical);
    Ok(format!("b3:{}", hasher.finalize().to_hex()))
}

#[derive(Serialize)]
struct MemberSetWire<'a> {
    version: u8,
    provenance: &'a DataProvenance,
    members: &'a [ResearchMemberLocator],
}

#[derive(Serialize)]
struct ResearchSourcePlanDraftWire<'a> {
    version: u8,
    provenance: &'a DataProvenance,
    warmup: TimeRangeWire,
    evaluation: TimeRangeWire,
    members: &'a [ResearchMemberLocator],
    coverage: &'a [CoverageDeclaration],
    recovery_outcomes: &'a [RecoveryOutcomeLocator],
    member_set_digest: &'a str,
}

struct BoundedJsonWriter {
    bytes: Vec<u8>,
    limit: usize,
    exceeded_limit: bool,
}

impl BoundedJsonWriter {
    fn new(limit: usize) -> Self {
        Self {
            bytes: Vec::with_capacity(limit),
            limit,
            exceeded_limit: false,
        }
    }

    fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }

    fn exceeded_limit(&self) -> bool {
        self.exceeded_limit
    }
}

impl Write for BoundedJsonWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self
            .bytes
            .len()
            .checked_add(bytes.len())
            .is_none_or(|length| length > self.limit)
        {
            self.exceeded_limit = true;
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "canonical JSON exceeds its bounded output",
            ));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TimeRangeWire {
    start_ns: i64,
    end_ns: i64,
}

impl From<TimeRange> for TimeRangeWire {
    fn from(range: TimeRange) -> Self {
        Self {
            start_ns: range.start().value(),
            end_ns: range.end().value(),
        }
    }
}

impl TimeRangeWire {
    fn try_range(self) -> Result<TimeRange, ResearchPlanError> {
        let start = TimestampNs::new(i128::from(self.start_ns))
            .map_err(|_| ResearchPlanError::InvalidFinalPlan)?;
        let end = TimestampNs::new(i128::from(self.end_ns))
            .map_err(|_| ResearchPlanError::InvalidFinalPlan)?;
        TimeRange::new(start, end).map_err(|_| ResearchPlanError::InvalidFinalPlan)
    }

    fn range(self) -> TimeRange {
        self.try_range()
            .expect("source plan ranges retain validated timestamps")
    }
}

/// Source-plan construction failure.
#[derive(Debug, Error)]
pub enum ResearchPlanError {
    /// Exact member resolution failed.
    #[error(transparent)]
    Storage(#[from] ParquetError),
    /// A descriptor-bound recovery companion member was invalid or drifted.
    #[error(transparent)]
    RecoveryOutcome(#[from] RecoveryOutcomeError),
    /// No exact committed source member was selected.
    #[error("research source plan requires at least one member")]
    EmptyMembers,
    /// The same physical or content-addressed source was selected twice.
    #[error("research source plan repeats source member {partition_manifest_digest}")]
    DuplicateMember { partition_manifest_digest: String },
    /// A caller tried to create a capture locator outside its committed batch.
    #[error("research source locator is not committed by its capture batch")]
    InvalidLocator,
    /// Verified members do not share exactly one provenance tuple.
    #[error("research source members have mixed provenance")]
    MixedProvenance,
    /// Warmup and evaluation must be adjacent non-overlapping half-open ranges.
    #[error("research warmup and evaluation ranges must be contiguous")]
    InvalidWindows,
    /// A coverage declaration was outside the requested horizon or overlapped its peer.
    #[error("research coverage ranges are not canonical")]
    InvalidCoverageRange,
    /// A proof, boundary, digest, or referenced source row was invalid.
    #[error("research continuity evidence is invalid")]
    InvalidCoverageEvidence,
    /// A selected recovery companion did not bind only selected raw members.
    #[error("research recovery companion evidence is invalid")]
    InvalidRecoveryOutcome,
    /// A bounded source-plan input or canonical JSON output exceeded its limit.
    #[error("research source plan resource limit exceeded")]
    ResourceLimit,
    /// Canonical JSON serialization failed.
    #[error("research source plan JSON serialization failed")]
    Json(#[from] serde_json::Error),
    /// A persisted final-plan payload was malformed, noncanonical, or internally inconsistent.
    #[error("research final plan is invalid")]
    InvalidFinalPlan,
}
