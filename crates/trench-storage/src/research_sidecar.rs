//! Immutable, recomputable raw-witness sidecars for offline research.

#![cfg_attr(not(unix), allow(dead_code))]

use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    io::{self, Read},
    path::Path,
};

#[cfg(unix)]
use std::{fs, os::unix::io::AsRawFd, path::Component};

#[cfg(unix)]
use std::io::Write;

#[cfg(unix)]
use rustix::fs::{
    AtFlags, FileType, Mode, OFlags, RenameFlags, fstat, mkdirat, open, openat, renameat_with,
    unlinkat,
};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use trench_core::{domain::EventId, event::TimestampNs, validation::TimeRange};

use crate::research_runs::{AvailabilitySourceReference, VerifiedResearchSourcePlan};

const SIDECAR_VERSION: u8 = 1;
const SIDECAR_MANIFEST: &str = "research-sidecar.json";
const DIGEST_BYTES: usize = 67;
const MAX_MANIFEST_BYTES: u64 = 1_048_576;
const MAX_SHARD_BYTES: u64 = 1_048_576;
const MAX_TOTAL_BYTES: u64 = 16 * 1_048_576;
const MAX_SHARDS: usize = 512;
const MAX_RECORDS_PER_SHARD: usize = 8_192;
const MAX_TOTAL_RECORDS: usize = 65_536;
const MAX_REFS_PER_DECISION: usize = 8_192;
const MAX_EXCLUDED_GAPS: usize = 16_384;
const MAX_IDENTIFIER_BYTES: usize = 256;
const MAX_RAW_INPUT_REFERENCES: usize = 1_000_000;

/// The complete source-availability coordinate committed by every decision.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AvailabilityCutoff {
    received_at_ns: i64,
    event_time_ns: i64,
    event_id: String,
}

impl AvailabilityCutoff {
    /// Builds one checked complete availability cutoff.
    pub fn new(
        received_at: TimestampNs,
        event_time: TimestampNs,
        event_id: EventId,
    ) -> Result<Self, ResearchSidecarError> {
        let value = Self {
            received_at_ns: received_at.value(),
            event_time_ns: event_time.value(),
            event_id: event_id.as_str().to_owned(),
        };
        value.validate()?;
        Ok(value)
    }

    fn validate(&self) -> Result<(), ResearchSidecarError> {
        TimestampNs::new(i128::from(self.received_at_ns))?;
        TimestampNs::new(i128::from(self.event_time_ns))?;
        validate_digest(&self.event_id)?;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WitnessKind {
    Recovery,
    Universe,
    Feature,
    Risk,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExclusionReason {
    Unavailable,
    LateSource,
    StaleInput,
    InvalidWitness,
}

/// Typed reconciliation outcome captured from raw recovery facts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryStatus {
    Complete,
    Unavailable,
}

/// The raw source class used to complete a recovery request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoverySource {
    Captured,
    Archive,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RangeWire {
    start_ns: i64,
    end_ns: i64,
}

impl RangeWire {
    fn new(range: TimeRange) -> Self {
        Self {
            start_ns: range.start().value(),
            end_ns: range.end().value(),
        }
    }

    fn range(&self) -> Result<TimeRange, ResearchSidecarError> {
        Ok(TimeRange::new(
            TimestampNs::new(i128::from(self.start_ns))?,
            TimestampNs::new(i128::from(self.end_ns))?,
        )?)
    }
}

/// One normalized unavailable interval. Adjacent equal-reason intervals normalize to one gap.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExcludedGap {
    range: RangeWire,
    reason: ExclusionReason,
}

impl ExcludedGap {
    /// Creates one explicit excluded interval.
    pub fn new(range: TimeRange, reason: ExclusionReason) -> Result<Self, ResearchSidecarError> {
        let value = Self {
            range: RangeWire::new(range),
            reason,
        };
        value.validate()?;
        Ok(value)
    }

    /// Returns the half-open excluded interval.
    #[must_use]
    pub fn range(&self) -> TimeRange {
        self.range.range().expect("validated excluded range")
    }

    fn validate(&self) -> Result<(), ResearchSidecarError> {
        self.range.range()?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryWitness {
    record_id: String,
    cutoff: AvailabilityCutoff,
    source_range: RangeWire,
    request_id: String,
    status: RecoveryStatus,
    source: RecoverySource,
    completed_at_ns: i64,
    anchor: AvailabilitySourceReference,
    backfill_references: Vec<AvailabilitySourceReference>,
    expected_boundary_digest: String,
}

impl RecoveryWitness {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        record_id: impl Into<String>,
        cutoff: AvailabilityCutoff,
        source_range: TimeRange,
        request_id: impl Into<String>,
        status: RecoveryStatus,
        source: RecoverySource,
        completed_at: TimestampNs,
        anchor: AvailabilitySourceReference,
        backfill_references: Vec<AvailabilitySourceReference>,
        expected_boundary_digest: impl Into<String>,
    ) -> Result<Self, ResearchSidecarError> {
        let value = Self {
            record_id: record_id.into(),
            cutoff,
            source_range: RangeWire::new(source_range),
            request_id: request_id.into(),
            status,
            source,
            completed_at_ns: completed_at.value(),
            anchor,
            backfill_references,
            expected_boundary_digest: expected_boundary_digest.into(),
        };
        value.validate()?;
        Ok(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UniverseWitness {
    record_id: String,
    hour_at_ns: i64,
    cutoff: AvailabilityCutoff,
    source_range: RangeWire,
    candidate_references: Vec<AvailabilitySourceReference>,
    expected_snapshot_digest: String,
    expected_activation_digest: String,
}

impl UniverseWitness {
    pub fn new(
        record_id: impl Into<String>,
        hour_at: TimestampNs,
        cutoff: AvailabilityCutoff,
        source_range: TimeRange,
        candidate_references: Vec<AvailabilitySourceReference>,
        expected_snapshot_digest: impl Into<String>,
        expected_activation_digest: impl Into<String>,
    ) -> Result<Self, ResearchSidecarError> {
        let value = Self {
            record_id: record_id.into(),
            hour_at_ns: hour_at.value(),
            cutoff,
            source_range: RangeWire::new(source_range),
            candidate_references,
            expected_snapshot_digest: expected_snapshot_digest.into(),
            expected_activation_digest: expected_activation_digest.into(),
        };
        value.validate()?;
        Ok(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeatureWitness {
    record_id: String,
    decision_id: String,
    decision_at_ns: i64,
    cutoff: AvailabilityCutoff,
    source_range: RangeWire,
    input_references: Vec<AvailabilitySourceReference>,
    expected_snapshot_digest: String,
    expected_long_history_digest: String,
}

impl FeatureWitness {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        record_id: impl Into<String>,
        decision_id: EventId,
        decision_at: TimestampNs,
        cutoff: AvailabilityCutoff,
        source_range: TimeRange,
        input_references: Vec<AvailabilitySourceReference>,
        expected_snapshot_digest: impl Into<String>,
        expected_long_history_digest: impl Into<String>,
    ) -> Result<Self, ResearchSidecarError> {
        let value = Self {
            record_id: record_id.into(),
            decision_id: decision_id.as_str().to_owned(),
            decision_at_ns: decision_at.value(),
            cutoff,
            source_range: RangeWire::new(source_range),
            input_references,
            expected_snapshot_digest: expected_snapshot_digest.into(),
            expected_long_history_digest: expected_long_history_digest.into(),
        };
        value.validate()?;
        Ok(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawRiskWitness {
    record_id: String,
    decision_id: String,
    decision_at_ns: i64,
    cutoff: AvailabilityCutoff,
    source_range: RangeWire,
    venue_constraint_references: Vec<AvailabilitySourceReference>,
    book_reference: AvailabilitySourceReference,
    impact_references: Vec<AvailabilitySourceReference>,
    funding_references: Vec<AvailabilitySourceReference>,
    expected_policy_digest: String,
}

impl RawRiskWitness {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        record_id: impl Into<String>,
        decision_id: EventId,
        decision_at: TimestampNs,
        cutoff: AvailabilityCutoff,
        source_range: TimeRange,
        venue_constraint_references: Vec<AvailabilitySourceReference>,
        book_reference: AvailabilitySourceReference,
        impact_references: Vec<AvailabilitySourceReference>,
        funding_references: Vec<AvailabilitySourceReference>,
        expected_policy_digest: impl Into<String>,
    ) -> Result<Self, ResearchSidecarError> {
        let value = Self {
            record_id: record_id.into(),
            decision_id: decision_id.as_str().to_owned(),
            decision_at_ns: decision_at.value(),
            cutoff,
            source_range: RangeWire::new(source_range),
            venue_constraint_references,
            book_reference,
            impact_references,
            funding_references,
            expected_policy_digest: expected_policy_digest.into(),
        };
        value.validate()?;
        Ok(value)
    }
}

/// Typed raw witness payload. It deliberately never contains a derived snapshot or policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum WitnessRecord {
    Recovery(RecoveryWitness),
    Universe(UniverseWitness),
    Feature(FeatureWitness),
    Risk(RawRiskWitness),
}

impl WitnessRecord {
    /// Returns this record's one permitted raw-witness contract kind.
    #[must_use]
    pub const fn kind(&self) -> WitnessKind {
        match self {
            Self::Recovery(_) => WitnessKind::Recovery,
            Self::Universe(_) => WitnessKind::Universe,
            Self::Feature(_) => WitnessKind::Feature,
            Self::Risk(_) => WitnessKind::Risk,
        }
    }

    /// Returns the stable record identity inside its typed shard.
    #[must_use]
    pub fn record_id(&self) -> &str {
        match self {
            Self::Recovery(value) => &value.record_id,
            Self::Universe(value) => &value.record_id,
            Self::Feature(value) => &value.record_id,
            Self::Risk(value) => &value.record_id,
        }
    }

    fn cutoff(&self) -> &AvailabilityCutoff {
        match self {
            Self::Recovery(value) => &value.cutoff,
            Self::Universe(value) => &value.cutoff,
            Self::Feature(value) => &value.cutoff,
            Self::Risk(value) => &value.cutoff,
        }
    }

    fn source_range(&self) -> &RangeWire {
        match self {
            Self::Recovery(value) => &value.source_range,
            Self::Universe(value) => &value.source_range,
            Self::Feature(value) => &value.source_range,
            Self::Risk(value) => &value.source_range,
        }
    }

    fn decision_id(&self) -> Option<&str> {
        match self {
            Self::Feature(value) => Some(&value.decision_id),
            Self::Risk(value) => Some(&value.decision_id),
            Self::Recovery(_) | Self::Universe(_) => None,
        }
    }

    fn append_raw_input_references(
        &self,
        references: &mut Vec<AvailabilitySourceReference>,
    ) -> Result<(), ResearchSidecarError> {
        match self {
            Self::Recovery(value) => append_references(
                references,
                std::iter::once(&value.anchor).chain(value.backfill_references.iter()),
            ),
            Self::Universe(value) => {
                append_references(references, value.candidate_references.iter())
            }
            Self::Feature(value) => append_references(references, value.input_references.iter()),
            Self::Risk(value) => append_references(
                references,
                value
                    .venue_constraint_references
                    .iter()
                    .chain(std::iter::once(&value.book_reference))
                    .chain(value.impact_references.iter())
                    .chain(value.funding_references.iter()),
            ),
        }
    }

    fn validate(&self) -> Result<(), ResearchSidecarError> {
        match self {
            Self::Recovery(value) => value.validate(),
            Self::Universe(value) => value.validate(),
            Self::Feature(value) => value.validate(),
            Self::Risk(value) => value.validate(),
        }
    }
}

fn append_references<'a>(
    target: &mut Vec<AvailabilitySourceReference>,
    values: impl IntoIterator<Item = &'a AvailabilitySourceReference>,
) -> Result<(), ResearchSidecarError> {
    for value in values {
        if target.len() == MAX_RAW_INPUT_REFERENCES {
            return Err(ResearchSidecarError::ResourceLimit);
        }
        target.push(value.clone());
    }
    Ok(())
}

impl UniverseWitness {
    fn validate(&self) -> Result<(), ResearchSidecarError> {
        validate_identifier(&self.record_id)?;
        self.cutoff.validate()?;
        self.source_range.range()?;
        validate_references(&self.candidate_references)?;
        validate_digest(&self.expected_snapshot_digest)?;
        validate_digest(&self.expected_activation_digest)?;
        Ok(())
    }
}

impl RecoveryWitness {
    fn validate(&self) -> Result<(), ResearchSidecarError> {
        validate_identifier(&self.record_id)?;
        validate_identifier(&self.request_id)?;
        self.cutoff.validate()?;
        self.source_range.range()?;
        TimestampNs::new(i128::from(self.completed_at_ns))?;
        validate_references(std::iter::once(&self.anchor).chain(self.backfill_references.iter()))?;
        validate_digest(&self.expected_boundary_digest)
    }
}

impl FeatureWitness {
    fn validate(&self) -> Result<(), ResearchSidecarError> {
        validate_identifier(&self.record_id)?;
        validate_digest(&self.decision_id)?;
        TimestampNs::new(i128::from(self.decision_at_ns))?;
        self.cutoff.validate()?;
        self.source_range.range()?;
        validate_references(&self.input_references)?;
        validate_digest(&self.expected_snapshot_digest)?;
        validate_digest(&self.expected_long_history_digest)
    }
}

impl RawRiskWitness {
    fn validate(&self) -> Result<(), ResearchSidecarError> {
        validate_identifier(&self.record_id)?;
        validate_digest(&self.decision_id)?;
        TimestampNs::new(i128::from(self.decision_at_ns))?;
        self.cutoff.validate()?;
        self.source_range.range()?;
        validate_references(
            self.venue_constraint_references
                .iter()
                .chain(std::iter::once(&self.book_reference))
                .chain(self.impact_references.iter())
                .chain(self.funding_references.iter()),
        )?;
        validate_digest(&self.expected_policy_digest)
    }
}

/// One named shard containing one raw witness kind.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WitnessShard {
    name: String,
    kind: WitnessKind,
    records: Vec<WitnessRecord>,
}

impl WitnessShard {
    /// Creates one recovery-witness shard.
    pub fn recovery(
        name: impl Into<String>,
        records: Vec<RecoveryWitness>,
    ) -> Result<Self, ResearchSidecarError> {
        Self::new(
            name,
            WitnessKind::Recovery,
            records.into_iter().map(WitnessRecord::Recovery).collect(),
        )
    }

    /// Creates one universe-witness shard.
    pub fn universe(
        name: impl Into<String>,
        records: Vec<UniverseWitness>,
    ) -> Result<Self, ResearchSidecarError> {
        Self::new(
            name,
            WitnessKind::Universe,
            records.into_iter().map(WitnessRecord::Universe).collect(),
        )
    }

    /// Creates one feature-witness shard.
    pub fn feature(
        name: impl Into<String>,
        records: Vec<FeatureWitness>,
    ) -> Result<Self, ResearchSidecarError> {
        Self::new(
            name,
            WitnessKind::Feature,
            records.into_iter().map(WitnessRecord::Feature).collect(),
        )
    }

    /// Creates one raw-risk-witness shard.
    pub fn risk(
        name: impl Into<String>,
        records: Vec<RawRiskWitness>,
    ) -> Result<Self, ResearchSidecarError> {
        Self::new(
            name,
            WitnessKind::Risk,
            records.into_iter().map(WitnessRecord::Risk).collect(),
        )
    }

    pub fn new(
        name: impl Into<String>,
        kind: WitnessKind,
        records: Vec<WitnessRecord>,
    ) -> Result<Self, ResearchSidecarError> {
        let value = Self {
            name: name.into(),
            kind,
            records,
        };
        value.validate()?;
        Ok(value)
    }

    #[must_use]
    pub const fn kind(&self) -> WitnessKind {
        self.kind
    }

    fn validate(&self) -> Result<(), ResearchSidecarError> {
        validate_component(&self.name)?;
        if self.records.is_empty() || self.records.len() > MAX_RECORDS_PER_SHARD {
            return Err(ResearchSidecarError::ResourceLimit);
        }
        let mut ids = BTreeSet::new();
        for record in &self.records {
            record.validate()?;
            if record.kind() != self.kind || !ids.insert(record.record_id()) {
                return Err(ResearchSidecarError::InvalidSidecar {
                    reason: "witness shard does not contain unique records of its declared kind",
                });
            }
        }
        Ok(())
    }
}

/// A typed location in one witness shard. No alternate decision-to-witness map exists.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WitnessReference {
    shard: String,
    record_id: String,
}

impl WitnessReference {
    pub fn new(
        shard: impl Into<String>,
        record_id: impl Into<String>,
    ) -> Result<Self, ResearchSidecarError> {
        let value = Self {
            shard: shard.into(),
            record_id: record_id.into(),
        };
        validate_component(&value.shard)?;
        validate_identifier(&value.record_id)?;
        Ok(value)
    }

    /// Returns the canonical witness-shard component.
    #[must_use]
    pub fn shard(&self) -> &str {
        &self.shard
    }

    /// Returns the immutable raw-witness record identity.
    #[must_use]
    pub fn record_id(&self) -> &str {
        &self.record_id
    }
}

/// The sole immutable mapping from a decision to its four raw witness contracts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DecisionWitnessIndex {
    decision_id: String,
    cutoff: AvailabilityCutoff,
    source_range: RangeWire,
    input_references: Vec<AvailabilitySourceReference>,
    recovery: WitnessReference,
    universe: WitnessReference,
    feature: WitnessReference,
    risk: WitnessReference,
}

impl DecisionWitnessIndex {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        decision_id: EventId,
        cutoff: AvailabilityCutoff,
        source_range: TimeRange,
        input_references: Vec<AvailabilitySourceReference>,
        recovery: WitnessReference,
        universe: WitnessReference,
        feature: WitnessReference,
        risk: WitnessReference,
    ) -> Result<Self, ResearchSidecarError> {
        let value = Self {
            decision_id: decision_id.as_str().to_owned(),
            cutoff,
            source_range: RangeWire::new(source_range),
            input_references,
            recovery,
            universe,
            feature,
            risk,
        };
        value.validate()?;
        Ok(value)
    }

    /// Returns the checked decision identity as a value.
    #[must_use]
    pub fn decision_id(&self) -> &str {
        &self.decision_id
    }

    /// Returns the checked decision identity as a value.
    #[must_use]
    pub fn decision_id_value(&self) -> EventId {
        EventId::new(self.decision_id.clone()).expect("validated decision ID")
    }

    /// Returns the four ordered raw-witness references required for this decision.
    #[must_use]
    pub fn witnesses(&self) -> [&WitnessReference; 4] {
        [&self.recovery, &self.universe, &self.feature, &self.risk]
    }

    fn validate(&self) -> Result<(), ResearchSidecarError> {
        validate_digest(&self.decision_id)?;
        self.cutoff.validate()?;
        self.source_range.range()?;
        if self.input_references.len() > MAX_REFS_PER_DECISION {
            return Err(ResearchSidecarError::ResourceLimit);
        }
        validate_references(&self.input_references)?;
        for reference in [&self.recovery, &self.universe, &self.feature, &self.risk] {
            validate_component(&reference.shard)?;
            validate_identifier(&reference.record_id)?;
        }
        Ok(())
    }
}

/// One bounded decision-index shard.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DecisionIndexShard {
    name: String,
    records: Vec<DecisionWitnessIndex>,
}

impl DecisionIndexShard {
    pub fn new(
        name: impl Into<String>,
        records: Vec<DecisionWitnessIndex>,
    ) -> Result<Self, ResearchSidecarError> {
        let value = Self {
            name: name.into(),
            records,
        };
        value.validate()?;
        Ok(value)
    }

    fn validate(&self) -> Result<(), ResearchSidecarError> {
        validate_component(&self.name)?;
        if self.records.is_empty() || self.records.len() > MAX_RECORDS_PER_SHARD {
            return Err(ResearchSidecarError::ResourceLimit);
        }
        self.records
            .iter()
            .try_for_each(DecisionWitnessIndex::validate)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ShardManifest {
    name: String,
    file: String,
    digest: String,
    record_count: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SidecarManifest {
    version: u8,
    source_plan_digest: String,
    member_set_digest: String,
    provenance: crate::parquet::DataProvenance,
    availability_run_digest: String,
    availability_run_records: u64,
    witness_shards: Vec<ShardManifest>,
    decision_shards: Vec<ShardManifest>,
    excluded_gaps: Vec<ExcludedGap>,
    sidecar_digest: String,
}

/// Immutable verified sidecar, always opened against an already verified source plan.
#[derive(Debug, Clone)]
pub struct ResearchSidecar {
    digest: String,
    source_plan_digest: String,
    decisions: BTreeMap<String, DecisionWitnessIndex>,
    witnesses: BTreeMap<(String, String), WitnessRecord>,
    excluded_gaps: Vec<ExcludedGap>,
}

impl ResearchSidecar {
    /// Opens one complete final sidecar only after binding it to the supplied verified plan.
    pub fn open_from(
        directory: impl AsRef<Path>,
        source_plan: &VerifiedResearchSourcePlan,
    ) -> Result<Self, ResearchSidecarError> {
        open_sidecar(directory.as_ref(), source_plan)
    }

    #[must_use]
    pub fn digest(&self) -> &str {
        &self.digest
    }

    #[must_use]
    pub fn source_plan_digest(&self) -> &str {
        &self.source_plan_digest
    }

    #[must_use]
    pub fn decision(&self, decision_id: EventId) -> Option<&DecisionWitnessIndex> {
        self.decisions.get(decision_id.as_str())
    }

    #[must_use]
    pub fn decision_indices(&self) -> Vec<&DecisionWitnessIndex> {
        self.decisions.values().collect()
    }

    /// Returns one immutable typed raw witness by its index-bound reference.
    #[must_use]
    pub fn witness(&self, reference: &WitnessReference) -> Option<&WitnessRecord> {
        self.witnesses
            .get(&(reference.shard.clone(), reference.record_id.clone()))
    }

    fn raw_input_references(
        &self,
    ) -> Result<Vec<AvailabilitySourceReference>, ResearchSidecarError> {
        let mut references = Vec::new();
        for witness in self.witnesses.values() {
            witness.append_raw_input_references(&mut references)?;
        }
        Ok(references)
    }

    #[must_use]
    pub fn excluded_gaps(&self) -> &[ExcludedGap] {
        &self.excluded_gaps
    }
}

/// Single-use builder for one immutable sidecar.
#[derive(Debug, Clone)]
pub struct ResearchSidecarWriter {
    source_plan_digest: String,
    member_set_digest: String,
    provenance: crate::parquet::DataProvenance,
    availability_run_digest: String,
    availability_run_records: u64,
    witness_shards: Vec<WitnessShard>,
    decision_shards: Vec<DecisionIndexShard>,
    excluded_gaps: Vec<ExcludedGap>,
}

impl ResearchSidecarWriter {
    /// Starts a sidecar builder bound to exactly one verified source plan.
    pub fn new(source_plan: &VerifiedResearchSourcePlan) -> Result<Self, ResearchSidecarError> {
        let value = Self {
            source_plan_digest: source_plan.source_plan_digest().to_owned(),
            member_set_digest: source_plan.draft().member_set_digest().to_owned(),
            provenance: source_plan.provenance().clone(),
            availability_run_digest: source_plan.availability_run().digest().to_owned(),
            availability_run_records: source_plan.availability_run().record_count(),
            witness_shards: Vec::new(),
            decision_shards: Vec::new(),
            excluded_gaps: Vec::new(),
        };
        validate_digest(&value.source_plan_digest)?;
        validate_digest(&value.member_set_digest)?;
        validate_digest(&value.availability_run_digest)?;
        if value.availability_run_records == 0 {
            return Err(ResearchSidecarError::InvalidSidecar {
                reason: "verified source plan has an empty availability run",
            });
        }
        Ok(value)
    }

    pub fn with_witness_shards(
        mut self,
        shards: Vec<WitnessShard>,
    ) -> Result<Self, ResearchSidecarError> {
        validate_witness_shards(&shards)?;
        self.witness_shards = shards;
        Ok(self)
    }

    pub fn with_decision_index_shards(
        mut self,
        shards: Vec<DecisionIndexShard>,
    ) -> Result<Self, ResearchSidecarError> {
        validate_decision_shard_shapes(&shards)?;
        self.decision_shards = shards;
        Ok(self)
    }

    pub fn with_excluded_gaps(
        mut self,
        gaps: Vec<ExcludedGap>,
    ) -> Result<Self, ResearchSidecarError> {
        self.excluded_gaps = normalize_gaps(gaps)?;
        Ok(self)
    }

    /// Validates, fsyncs, and publishes one final immutable sidecar without replacement.
    pub fn publish_to(
        self,
        final_directory: impl AsRef<Path>,
    ) -> Result<ResearchSidecar, ResearchSidecarError> {
        publish_sidecar(self, final_directory.as_ref())
    }
}

fn validate_witness_shards(shards: &[WitnessShard]) -> Result<(), ResearchSidecarError> {
    if shards.len() > MAX_SHARDS {
        return Err(ResearchSidecarError::ResourceLimit);
    }
    let mut names = BTreeSet::new();
    let mut total = 0_usize;
    for shard in shards {
        shard.validate()?;
        if !names.insert(shard.name.as_str()) {
            return Err(ResearchSidecarError::InvalidSidecar {
                reason: "witness shard names are not unique",
            });
        }
        total = total
            .checked_add(shard.records.len())
            .ok_or(ResearchSidecarError::ResourceLimit)?;
    }
    if total > MAX_TOTAL_RECORDS {
        return Err(ResearchSidecarError::ResourceLimit);
    }
    Ok(())
}

fn validate_decision_shards(shards: &[DecisionIndexShard]) -> Result<(), ResearchSidecarError> {
    validate_decision_shard_shapes(shards)?;
    let mut decisions = BTreeSet::new();
    let mut prior = None;
    let mut total = 0_usize;
    for shard in shards {
        for record in &shard.records {
            let key = (record.cutoff.clone(), record.decision_id.clone());
            if prior
                .as_ref()
                .is_some_and(|value: &(AvailabilityCutoff, String)| value >= &key)
                || !decisions.insert(record.decision_id.as_str())
            {
                return Err(ResearchSidecarError::InvalidSidecar {
                    reason: "decision indexes are not globally unique and strictly ordered",
                });
            }
            prior = Some(key);
            total = total
                .checked_add(1)
                .ok_or(ResearchSidecarError::ResourceLimit)?;
        }
    }
    if total > MAX_TOTAL_RECORDS {
        return Err(ResearchSidecarError::ResourceLimit);
    }
    Ok(())
}

fn validate_decision_shard_shapes(
    shards: &[DecisionIndexShard],
) -> Result<(), ResearchSidecarError> {
    if shards.len() > MAX_SHARDS {
        return Err(ResearchSidecarError::ResourceLimit);
    }
    let mut names = BTreeSet::new();
    for shard in shards {
        shard.validate()?;
        if !names.insert(shard.name.as_str()) {
            return Err(ResearchSidecarError::InvalidSidecar {
                reason: "decision shard names are not unique",
            });
        }
    }
    Ok(())
}

fn validate_identifier(value: &str) -> Result<(), ResearchSidecarError> {
    if value.is_empty()
        || value.len() > MAX_IDENTIFIER_BYTES
        || value.trim() != value
        || value.chars().any(char::is_control)
    {
        return Err(ResearchSidecarError::InvalidSidecar {
            reason: "identifier is invalid",
        });
    }
    Ok(())
}

fn validate_component(value: &str) -> Result<(), ResearchSidecarError> {
    validate_identifier(value)?;
    if value == "." || value == ".." || value.contains('/') || value.contains('\\') {
        return Err(ResearchSidecarError::InvalidSidecar {
            reason: "file name must be one normal component",
        });
    }
    Ok(())
}

fn validate_digest(value: &str) -> Result<(), ResearchSidecarError> {
    let Some(hex) = value.strip_prefix("b3:") else {
        return Err(ResearchSidecarError::InvalidSidecar {
            reason: "digest lacks BLAKE3 prefix",
        });
    };
    if value.len() != DIGEST_BYTES
        || hex.len() != blake3::OUT_LEN * 2
        || !hex
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(ResearchSidecarError::InvalidSidecar {
            reason: "digest is not canonical lowercase BLAKE3",
        });
    }
    Ok(())
}

fn validate_references<'a>(
    references: impl IntoIterator<Item = &'a AvailabilitySourceReference>,
) -> Result<(), ResearchSidecarError> {
    let mut count = 0_usize;
    let mut prior: Option<&AvailabilitySourceReference> = None;
    for reference in references {
        count = count
            .checked_add(1)
            .ok_or(ResearchSidecarError::ResourceLimit)?;
        if count > MAX_REFS_PER_DECISION {
            return Err(ResearchSidecarError::ResourceLimit);
        }
        reference
            .validate()
            .map_err(|_| ResearchSidecarError::InvalidSidecar {
                reason: "raw source reference is invalid",
            })?;
        if prior.is_some_and(|previous| previous >= reference) {
            return Err(ResearchSidecarError::InvalidSidecar {
                reason: "raw source references are not strictly ordered",
            });
        }
        prior = Some(reference);
    }
    Ok(())
}

fn normalize_gaps(mut gaps: Vec<ExcludedGap>) -> Result<Vec<ExcludedGap>, ResearchSidecarError> {
    if gaps.len() > MAX_EXCLUDED_GAPS {
        return Err(ResearchSidecarError::ResourceLimit);
    }
    gaps.iter().try_for_each(ExcludedGap::validate)?;
    gaps.sort_by_key(|gap| (gap.range.start_ns, gap.range.end_ns, gap.reason));
    let mut normalized: Vec<ExcludedGap> = Vec::with_capacity(gaps.len());
    for gap in gaps {
        if let Some(previous) = normalized.last_mut() {
            if gap.range.start_ns < previous.range.end_ns {
                return Err(ResearchSidecarError::InvalidSidecar {
                    reason: "excluded gaps overlap",
                });
            }
            if gap.range.start_ns == previous.range.end_ns && gap.reason == previous.reason {
                previous.range.end_ns = gap.range.end_ns;
                continue;
            }
        }
        normalized.push(gap);
    }
    Ok(normalized)
}

#[derive(Debug)]
struct PreparedSidecar {
    manifest: SidecarManifest,
    payloads: Vec<(String, Vec<u8>)>,
}

fn prepare_sidecar(writer: ResearchSidecarWriter) -> Result<PreparedSidecar, ResearchSidecarError> {
    validate_witness_shards(&writer.witness_shards)?;
    validate_decision_shards(&writer.decision_shards)?;
    validate_cross_contract(&writer.witness_shards, &writer.decision_shards)?;
    let excluded_gaps = normalize_gaps(writer.excluded_gaps)?;
    let mut payloads = Vec::with_capacity(
        writer
            .witness_shards
            .len()
            .checked_add(writer.decision_shards.len())
            .ok_or(ResearchSidecarError::ResourceLimit)?,
    );
    let mut witness_shards = Vec::with_capacity(writer.witness_shards.len());
    for shard in writer.witness_shards {
        let file = format!("witness-{}.json", shard.name);
        validate_component(&file)?;
        let bytes = canonical_bytes(&shard)?;
        let record_count =
            u32::try_from(shard.records.len()).map_err(|_| ResearchSidecarError::ResourceLimit)?;
        witness_shards.push(ShardManifest {
            name: shard.name,
            file: file.clone(),
            digest: digest_bytes("trench.research.sidecar.witness-shard.v1", &bytes),
            record_count,
        });
        payloads.push((file, bytes));
    }
    let mut decision_shards = Vec::with_capacity(writer.decision_shards.len());
    for shard in writer.decision_shards {
        let file = format!("decision-{}.json", shard.name);
        validate_component(&file)?;
        let bytes = canonical_bytes(&shard)?;
        let record_count =
            u32::try_from(shard.records.len()).map_err(|_| ResearchSidecarError::ResourceLimit)?;
        decision_shards.push(ShardManifest {
            name: shard.name,
            file: file.clone(),
            digest: digest_bytes("trench.research.sidecar.decision-shard.v1", &bytes),
            record_count,
        });
        payloads.push((file, bytes));
    }
    payloads.sort_by(|left, right| left.0.cmp(&right.0));
    witness_shards.sort_by(|left, right| left.file.cmp(&right.file));
    decision_shards.sort_by(|left, right| left.file.cmp(&right.file));
    let mut total_bytes = payloads.iter().try_fold(0_u64, |total, (_, bytes)| {
        total
            .checked_add(
                u64::try_from(bytes.len()).map_err(|_| ResearchSidecarError::ResourceLimit)?,
            )
            .ok_or(ResearchSidecarError::ResourceLimit)
    })?;
    if total_bytes > MAX_TOTAL_BYTES
        || payloads.iter().any(|(_, bytes)| {
            u64::try_from(bytes.len())
                .ok()
                .is_none_or(|bytes| bytes > MAX_SHARD_BYTES)
        })
    {
        return Err(ResearchSidecarError::ResourceLimit);
    }
    let mut manifest = SidecarManifest {
        version: SIDECAR_VERSION,
        source_plan_digest: writer.source_plan_digest,
        member_set_digest: writer.member_set_digest,
        provenance: writer.provenance,
        availability_run_digest: writer.availability_run_digest,
        availability_run_records: writer.availability_run_records,
        witness_shards,
        decision_shards,
        excluded_gaps,
        sidecar_digest: String::new(),
    };
    manifest.sidecar_digest = sidecar_digest(&manifest)?;
    let manifest_bytes = canonical_bytes(&manifest)?;
    total_bytes = total_bytes
        .checked_add(
            u64::try_from(manifest_bytes.len()).map_err(|_| ResearchSidecarError::ResourceLimit)?,
        )
        .ok_or(ResearchSidecarError::ResourceLimit)?;
    if manifest_bytes.len() > usize::try_from(MAX_MANIFEST_BYTES).unwrap_or(usize::MAX)
        || total_bytes > MAX_TOTAL_BYTES
    {
        return Err(ResearchSidecarError::ResourceLimit);
    }
    payloads.push((SIDECAR_MANIFEST.to_owned(), manifest_bytes));
    Ok(PreparedSidecar { manifest, payloads })
}

fn canonical_bytes<T: Serialize>(value: &T) -> Result<Vec<u8>, ResearchSidecarError> {
    let bytes = serde_json::to_vec(value)?;
    if bytes.len() > usize::try_from(MAX_SHARD_BYTES).unwrap_or(usize::MAX) {
        return Err(ResearchSidecarError::ResourceLimit);
    }
    Ok(bytes)
}

fn digest_bytes(context: &str, bytes: &[u8]) -> String {
    let mut hasher = blake3::Hasher::new_derive_key(context);
    hasher.update(&(bytes.len() as u64).to_be_bytes());
    hasher.update(bytes);
    format!("b3:{}", hasher.finalize().to_hex())
}

fn sidecar_digest(manifest: &SidecarManifest) -> Result<String, ResearchSidecarError> {
    let wire = SidecarDigestWire {
        version: manifest.version,
        source_plan_digest: &manifest.source_plan_digest,
        member_set_digest: &manifest.member_set_digest,
        provenance: &manifest.provenance,
        availability_run_digest: &manifest.availability_run_digest,
        availability_run_records: manifest.availability_run_records,
        witness_shards: &manifest.witness_shards,
        decision_shards: &manifest.decision_shards,
        excluded_gaps: &manifest.excluded_gaps,
    };
    Ok(digest_bytes(
        "trench.research.sidecar.manifest.v1",
        &serde_json::to_vec(&wire)?,
    ))
}

#[derive(Serialize)]
struct SidecarDigestWire<'a> {
    version: u8,
    source_plan_digest: &'a str,
    member_set_digest: &'a str,
    provenance: &'a crate::parquet::DataProvenance,
    availability_run_digest: &'a str,
    availability_run_records: u64,
    witness_shards: &'a [ShardManifest],
    decision_shards: &'a [ShardManifest],
    excluded_gaps: &'a [ExcludedGap],
}

fn validate_cross_contract(
    witness_shards: &[WitnessShard],
    decision_shards: &[DecisionIndexShard],
) -> Result<(), ResearchSidecarError> {
    let mut witnesses = BTreeMap::new();
    for shard in witness_shards {
        for record in &shard.records {
            if witnesses
                .insert((shard.name.as_str(), record.record_id()), record)
                .is_some()
            {
                return Err(ResearchSidecarError::InvalidSidecar {
                    reason: "witness reference is ambiguous",
                });
            }
        }
    }
    for decision in decision_shards.iter().flat_map(|shard| &shard.records) {
        let mut required_inputs = BTreeSet::new();
        for (kind, reference) in [
            (WitnessKind::Recovery, &decision.recovery),
            (WitnessKind::Universe, &decision.universe),
            (WitnessKind::Feature, &decision.feature),
            (WitnessKind::Risk, &decision.risk),
        ] {
            let Some(record) =
                witnesses.get(&(reference.shard.as_str(), reference.record_id.as_str()))
            else {
                return Err(ResearchSidecarError::InvalidSidecar {
                    reason: "decision reference does not resolve",
                });
            };
            if record.kind() != kind
                || record.cutoff() > &decision.cutoff
                || record.source_range().start_ns < decision.source_range.start_ns
                || record.source_range().end_ns > decision.source_range.end_ns
                || record
                    .decision_id()
                    .is_some_and(|record_decision| record_decision != decision.decision_id)
            {
                return Err(ResearchSidecarError::InvalidSidecar {
                    reason: "decision references a wrong-kind or late witness",
                });
            }
            match record {
                WitnessRecord::Recovery(value) => {
                    required_inputs.insert(&value.anchor);
                    required_inputs.extend(value.backfill_references.iter());
                }
                WitnessRecord::Universe(value) => {
                    required_inputs.extend(value.candidate_references.iter());
                }
                WitnessRecord::Feature(value) => {
                    required_inputs.extend(value.input_references.iter());
                }
                WitnessRecord::Risk(value) => {
                    required_inputs.extend(value.venue_constraint_references.iter());
                    required_inputs.insert(&value.book_reference);
                    required_inputs.extend(value.impact_references.iter());
                    required_inputs.extend(value.funding_references.iter());
                }
            }
        }
        let declared = decision.input_references.iter().collect::<Vec<_>>();
        if declared != required_inputs.into_iter().collect::<Vec<_>>() {
            return Err(ResearchSidecarError::InvalidSidecar {
                reason: "decision input references do not exactly bind referenced raw inputs",
            });
        }
    }
    Ok(())
}

#[cfg(unix)]
fn publish_sidecar(
    writer: ResearchSidecarWriter,
    final_directory: &Path,
) -> Result<ResearchSidecar, ResearchSidecarError> {
    if !final_directory.is_absolute() {
        return Err(ResearchSidecarError::InvalidSidecar {
            reason: "final sidecar directory must be absolute",
        });
    }
    let prepared = prepare_sidecar(writer)?;
    let parent = final_directory
        .parent()
        .ok_or(ResearchSidecarError::InvalidSidecar {
            reason: "final sidecar requires a parent directory",
        })?;
    let final_name = private_component(final_directory)?;
    let parent_fd = open_private_directory_descriptor(parent)?;
    let stage_name = format!(
        ".{final_name}.stage-{}",
        &prepared.manifest.sidecar_digest[3..19]
    );
    mkdirat(
        &parent_fd,
        &stage_name,
        Mode::RUSR | Mode::WUSR | Mode::XUSR,
    )
    .map_err(|error| ResearchSidecarError::Io(error.into()))?;
    sync_directory(&parent_fd)?;
    let stage_fd = open_private_directory_at(&parent_fd, &stage_name)?;
    let result = (|| {
        for (name, bytes) in &prepared.payloads {
            let mut file = create_private_file_at(&stage_fd, name)?;
            file.write_all(bytes)?;
            file.sync_all()?;
        }
        let staged = open_sidecar_at(&stage_fd, &prepared.manifest, None)?;
        sync_directory(&stage_fd)?;
        match renameat_with(
            &parent_fd,
            &stage_name,
            &parent_fd,
            &final_name,
            RenameFlags::NOREPLACE,
        ) {
            Ok(()) => {
                sync_directory(&parent_fd)?;
                Ok(staged)
            }
            Err(error) if error == rustix::io::Errno::EXIST => {
                let existing = open_private_directory_at(&parent_fd, &final_name)?;
                if !directories_identical(&stage_fd, &existing)? {
                    return Err(ResearchSidecarError::ConflictingFinalSidecar);
                }
                remove_private_tree_at(&parent_fd, &stage_name)?;
                sync_directory(&parent_fd)?;
                Ok(staged)
            }
            Err(error) => Err(ResearchSidecarError::Io(error.into())),
        }
    })();
    if result.is_err() {
        let _ = remove_private_tree_at(&parent_fd, &stage_name);
        let _ = sync_directory(&parent_fd);
    }
    result
}

#[cfg(not(unix))]
fn publish_sidecar(
    _writer: ResearchSidecarWriter,
    _final_directory: &Path,
) -> Result<ResearchSidecar, ResearchSidecarError> {
    Err(ResearchSidecarError::UnsupportedPlatform)
}

fn open_sidecar(
    directory: &Path,
    source_plan: &VerifiedResearchSourcePlan,
) -> Result<ResearchSidecar, ResearchSidecarError> {
    if !directory.is_absolute() {
        return Err(ResearchSidecarError::InvalidSidecar {
            reason: "final sidecar directory must be absolute",
        });
    }
    let directory = open_private_directory_descriptor(directory)?;
    let manifest_bytes = read_private_file_at(&directory, SIDECAR_MANIFEST, MAX_MANIFEST_BYTES)?;
    let manifest = parse_manifest(&manifest_bytes)?;
    validate_plan_binding(&manifest, source_plan)?;
    let sidecar = open_sidecar_at(&directory, &manifest, Some(source_plan))?;
    source_plan
        .validate_source_references(&sidecar.raw_input_references()?)
        .map_err(|_| ResearchSidecarError::InvalidSidecar {
            reason: "sidecar raw witness references are not bound to the verified source plan",
        })?;
    Ok(sidecar)
}

fn open_sidecar_at(
    directory: &File,
    expected_manifest: &SidecarManifest,
    source_plan: Option<&VerifiedResearchSourcePlan>,
) -> Result<ResearchSidecar, ResearchSidecarError> {
    if let Some(source_plan) = source_plan {
        validate_plan_binding(expected_manifest, source_plan)?;
    }
    validate_manifest(expected_manifest)?;
    let mut expected_names = vec![SIDECAR_MANIFEST.to_owned()];
    expected_names.extend(
        expected_manifest
            .witness_shards
            .iter()
            .map(|shard| shard.file.clone()),
    );
    expected_names.extend(
        expected_manifest
            .decision_shards
            .iter()
            .map(|shard| shard.file.clone()),
    );
    expected_names.sort();
    if directory_names(directory)? != expected_names {
        return Err(ResearchSidecarError::InvalidSidecar {
            reason: "final sidecar directory is incomplete or has unexpected entries",
        });
    }
    let manifest_bytes = read_private_file_at(directory, SIDECAR_MANIFEST, MAX_MANIFEST_BYTES)?;
    let manifest = parse_manifest(&manifest_bytes)?;
    if manifest != *expected_manifest {
        return Err(ResearchSidecarError::InvalidSidecar {
            reason: "sidecar manifest changed during descriptor validation",
        });
    }
    let mut witness_shards = Vec::with_capacity(manifest.witness_shards.len());
    for shard_manifest in &manifest.witness_shards {
        let bytes = read_private_file_at(directory, &shard_manifest.file, MAX_SHARD_BYTES)?;
        if digest_bytes("trench.research.sidecar.witness-shard.v1", &bytes) != shard_manifest.digest
        {
            return Err(ResearchSidecarError::InvalidSidecar {
                reason: "witness shard digest disagrees with manifest",
            });
        }
        let shard = parse_canonical::<WitnessShard>(&bytes)?;
        if shard.name != shard_manifest.name
            || u32::try_from(shard.records.len()).ok() != Some(shard_manifest.record_count)
        {
            return Err(ResearchSidecarError::InvalidSidecar {
                reason: "witness shard metadata disagrees with manifest",
            });
        }
        shard.validate()?;
        witness_shards.push(shard);
    }
    let mut decision_shards = Vec::with_capacity(manifest.decision_shards.len());
    for shard_manifest in &manifest.decision_shards {
        let bytes = read_private_file_at(directory, &shard_manifest.file, MAX_SHARD_BYTES)?;
        if digest_bytes("trench.research.sidecar.decision-shard.v1", &bytes)
            != shard_manifest.digest
        {
            return Err(ResearchSidecarError::InvalidSidecar {
                reason: "decision shard digest disagrees with manifest",
            });
        }
        let shard = parse_canonical::<DecisionIndexShard>(&bytes)?;
        if shard.name != shard_manifest.name
            || u32::try_from(shard.records.len()).ok() != Some(shard_manifest.record_count)
        {
            return Err(ResearchSidecarError::InvalidSidecar {
                reason: "decision shard metadata disagrees with manifest",
            });
        }
        shard.validate()?;
        decision_shards.push(shard);
    }
    validate_witness_shards(&witness_shards)?;
    validate_decision_shards(&decision_shards)?;
    validate_cross_contract(&witness_shards, &decision_shards)?;
    if normalize_gaps(manifest.excluded_gaps.clone())? != manifest.excluded_gaps {
        return Err(ResearchSidecarError::InvalidSidecar {
            reason: "excluded gaps are not canonical",
        });
    }
    let mut witnesses = BTreeMap::new();
    for shard in witness_shards {
        for record in shard.records {
            let key = (shard.name.clone(), record.record_id().to_owned());
            if witnesses.insert(key, record).is_some() {
                return Err(ResearchSidecarError::InvalidSidecar {
                    reason: "witness identity is duplicated",
                });
            }
        }
    }
    let mut decisions = BTreeMap::new();
    for decision in decision_shards.into_iter().flat_map(|shard| shard.records) {
        if decisions
            .insert(decision.decision_id.clone(), decision)
            .is_some()
        {
            return Err(ResearchSidecarError::InvalidSidecar {
                reason: "decision index identity is duplicated",
            });
        }
    }
    Ok(ResearchSidecar {
        digest: manifest.sidecar_digest.clone(),
        source_plan_digest: manifest.source_plan_digest.clone(),
        decisions,
        witnesses,
        excluded_gaps: manifest.excluded_gaps.clone(),
    })
}

fn parse_manifest(bytes: &[u8]) -> Result<SidecarManifest, ResearchSidecarError> {
    let manifest = parse_canonical::<SidecarManifest>(bytes)?;
    validate_manifest(&manifest)?;
    Ok(manifest)
}

fn parse_canonical<T>(bytes: &[u8]) -> Result<T, ResearchSidecarError>
where
    T: serde::de::DeserializeOwned + Serialize,
{
    let value = serde_json::from_slice(bytes)?;
    if serde_json::to_vec(&value)? != bytes {
        return Err(ResearchSidecarError::InvalidSidecar {
            reason: "sidecar payload is not canonical JSON",
        });
    }
    Ok(value)
}

fn validate_manifest(manifest: &SidecarManifest) -> Result<(), ResearchSidecarError> {
    if manifest.version != SIDECAR_VERSION
        || manifest.availability_run_records == 0
        || manifest.witness_shards.len() > MAX_SHARDS
        || manifest.decision_shards.len() > MAX_SHARDS
    {
        return Err(ResearchSidecarError::InvalidSidecar {
            reason: "sidecar manifest fields are outside the fixed contract",
        });
    }
    validate_digest(&manifest.source_plan_digest)?;
    validate_digest(&manifest.member_set_digest)?;
    validate_digest(&manifest.availability_run_digest)?;
    validate_digest(&manifest.sidecar_digest)?;
    if manifest.sidecar_digest != sidecar_digest(manifest)? {
        return Err(ResearchSidecarError::InvalidSidecar {
            reason: "sidecar manifest digest is invalid",
        });
    }
    let mut names = BTreeSet::new();
    for shard in manifest
        .witness_shards
        .iter()
        .chain(&manifest.decision_shards)
    {
        validate_component(&shard.name)?;
        validate_component(&shard.file)?;
        validate_digest(&shard.digest)?;
        if shard.record_count == 0 || !names.insert(shard.file.as_str()) {
            return Err(ResearchSidecarError::InvalidSidecar {
                reason: "sidecar shard manifest is invalid",
            });
        }
    }
    if normalize_gaps(manifest.excluded_gaps.clone())? != manifest.excluded_gaps {
        return Err(ResearchSidecarError::InvalidSidecar {
            reason: "excluded gaps are not canonical",
        });
    }
    Ok(())
}

fn validate_plan_binding(
    manifest: &SidecarManifest,
    source_plan: &VerifiedResearchSourcePlan,
) -> Result<(), ResearchSidecarError> {
    if manifest.source_plan_digest != source_plan.source_plan_digest()
        || manifest.member_set_digest != source_plan.draft().member_set_digest()
        || manifest.provenance != *source_plan.provenance()
        || manifest.availability_run_digest != source_plan.availability_run().digest()
        || manifest.availability_run_records != source_plan.availability_run().record_count()
    {
        return Err(ResearchSidecarError::InvalidSidecar {
            reason: "sidecar is not bound to the supplied verified source plan",
        });
    }
    Ok(())
}

fn private_component(path: &Path) -> Result<String, ResearchSidecarError> {
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or(ResearchSidecarError::InvalidSidecar {
            reason: "final sidecar name must be one UTF-8 component",
        })?
        .to_owned();
    validate_component(&name)?;
    Ok(name)
}

#[cfg(unix)]
fn open_private_directory_descriptor(path: &Path) -> Result<File, ResearchSidecarError> {
    if !path.is_absolute() {
        return Err(ResearchSidecarError::InvalidSidecar {
            reason: "private directory path must be absolute",
        });
    }
    let mut directory = File::from(
        open(
            "/",
            OFlags::RDONLY
                | OFlags::DIRECTORY
                | OFlags::NOFOLLOW
                | OFlags::NONBLOCK
                | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(|error| ResearchSidecarError::Io(error.into()))?,
    );
    ensure_directory(&directory)?;
    for component in path.components() {
        let Component::Normal(component) = component else {
            if matches!(component, Component::RootDir) {
                continue;
            }
            return Err(ResearchSidecarError::InvalidSidecar {
                reason: "private directory path contains a non-normal component",
            });
        };
        directory = File::from(
            openat(
                &directory,
                component,
                OFlags::RDONLY
                    | OFlags::DIRECTORY
                    | OFlags::NOFOLLOW
                    | OFlags::NONBLOCK
                    | OFlags::CLOEXEC,
                Mode::empty(),
            )
            .map_err(|error| ResearchSidecarError::Io(error.into()))?,
        );
        ensure_directory(&directory)?;
    }
    ensure_private_directory(&directory)?;
    Ok(directory)
}

#[cfg(not(unix))]
fn open_private_directory_descriptor(_path: &Path) -> Result<File, ResearchSidecarError> {
    Err(ResearchSidecarError::UnsupportedPlatform)
}

#[cfg(unix)]
fn ensure_directory(directory: &File) -> Result<(), ResearchSidecarError> {
    let metadata = fstat(directory).map_err(|error| ResearchSidecarError::Io(error.into()))?;
    if !FileType::from_raw_mode(metadata.st_mode).is_dir() {
        return Err(ResearchSidecarError::InvalidSidecar {
            reason: "path contains a non-directory component",
        });
    }
    Ok(())
}

#[cfg(unix)]
fn ensure_private_directory(directory: &File) -> Result<(), ResearchSidecarError> {
    let metadata = fstat(directory).map_err(|error| ResearchSidecarError::Io(error.into()))?;
    if !FileType::from_raw_mode(metadata.st_mode).is_dir()
        || metadata.st_uid != rustix::process::geteuid().as_raw()
        || metadata.st_mode & 0o777 != 0o700
    {
        return Err(ResearchSidecarError::InvalidSidecar {
            reason: "directory must be effective-user owned mode 0700",
        });
    }
    Ok(())
}

#[cfg(unix)]
fn open_private_directory_at(parent: &File, name: &str) -> Result<File, ResearchSidecarError> {
    validate_component(name)?;
    let directory = File::from(
        openat(
            parent,
            name,
            OFlags::RDONLY
                | OFlags::DIRECTORY
                | OFlags::NOFOLLOW
                | OFlags::NONBLOCK
                | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(|error| ResearchSidecarError::Io(error.into()))?,
    );
    ensure_private_directory(&directory)?;
    Ok(directory)
}

#[cfg(unix)]
fn create_private_file_at(directory: &File, name: &str) -> Result<File, ResearchSidecarError> {
    validate_component(name)?;
    let file = File::from(
        openat(
            directory,
            name,
            OFlags::RDWR
                | OFlags::CREATE
                | OFlags::EXCL
                | OFlags::NOFOLLOW
                | OFlags::NONBLOCK
                | OFlags::CLOEXEC,
            Mode::RUSR | Mode::WUSR,
        )
        .map_err(|error| ResearchSidecarError::Io(error.into()))?,
    );
    rustix::fs::fchmod(&file, Mode::RUSR | Mode::WUSR)
        .map_err(|error| ResearchSidecarError::Io(error.into()))?;
    Ok(file)
}

#[cfg(unix)]
fn open_private_file_at(
    directory: &File,
    name: &str,
    limit: u64,
) -> Result<File, ResearchSidecarError> {
    validate_component(name)?;
    let file = File::from(
        openat(
            directory,
            name,
            OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(|error| ResearchSidecarError::Io(error.into()))?,
    );
    let metadata = fstat(&file).map_err(|error| ResearchSidecarError::Io(error.into()))?;
    let bytes = u64::try_from(metadata.st_size).map_err(|_| ResearchSidecarError::ResourceLimit)?;
    if !FileType::from_raw_mode(metadata.st_mode).is_file()
        || metadata.st_uid != rustix::process::geteuid().as_raw()
        || metadata.st_mode & 0o777 != 0o600
        || bytes > limit
    {
        return Err(ResearchSidecarError::InvalidSidecar {
            reason: "payload must be a bounded private regular file",
        });
    }
    Ok(file)
}

#[cfg(not(unix))]
fn open_private_file_at(
    _directory: &File,
    _name: &str,
    _limit: u64,
) -> Result<File, ResearchSidecarError> {
    Err(ResearchSidecarError::UnsupportedPlatform)
}

fn read_private_file_at(
    directory: &File,
    name: &str,
    limit: u64,
) -> Result<Vec<u8>, ResearchSidecarError> {
    let mut file = open_private_file_at(directory, name, limit)?;
    let length = file.metadata()?.len();
    let mut bytes = Vec::with_capacity(
        usize::try_from(length).map_err(|_| ResearchSidecarError::ResourceLimit)?,
    );
    file.read_to_end(&mut bytes)?;
    if u64::try_from(bytes.len()).ok() != Some(length) {
        return Err(ResearchSidecarError::InvalidSidecar {
            reason: "payload changed while being read",
        });
    }
    Ok(bytes)
}

#[cfg(unix)]
fn directory_names(directory: &File) -> Result<Vec<String>, ResearchSidecarError> {
    let path = format!("/proc/self/fd/{}", directory.as_raw_fd());
    let mut names = fs::read_dir(path)?
        .map(|entry| {
            entry?
                .file_name()
                .into_string()
                .map_err(|_| ResearchSidecarError::InvalidSidecar {
                    reason: "directory entry is not UTF-8",
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    names.sort();
    Ok(names)
}

#[cfg(not(unix))]
fn directory_names(_directory: &File) -> Result<Vec<String>, ResearchSidecarError> {
    Err(ResearchSidecarError::UnsupportedPlatform)
}

fn sync_directory(directory: &File) -> Result<(), ResearchSidecarError> {
    directory.sync_all()?;
    Ok(())
}

#[cfg(unix)]
fn directories_identical(left: &File, right: &File) -> Result<bool, ResearchSidecarError> {
    let left_names = directory_names(left)?;
    if left_names != directory_names(right)? {
        return Ok(false);
    }
    for name in left_names {
        let left_bytes =
            read_private_file_at(left, &name, MAX_SHARD_BYTES.max(MAX_MANIFEST_BYTES))?;
        let right_bytes =
            read_private_file_at(right, &name, MAX_SHARD_BYTES.max(MAX_MANIFEST_BYTES))?;
        if left_bytes != right_bytes {
            return Ok(false);
        }
    }
    Ok(true)
}

#[cfg(unix)]
fn remove_private_tree_at(parent: &File, name: &str) -> Result<(), ResearchSidecarError> {
    let directory = open_private_directory_at(parent, name)?;
    remove_private_directory_contents(&directory)?;
    unlinkat(parent, name, AtFlags::REMOVEDIR)
        .map_err(|error| ResearchSidecarError::Io(error.into()))
}

#[cfg(unix)]
fn remove_private_directory_contents(directory: &File) -> Result<(), ResearchSidecarError> {
    for name in directory_names(directory)? {
        let entry = File::from(
            openat(
                directory,
                &name,
                OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
                Mode::empty(),
            )
            .map_err(|error| ResearchSidecarError::Io(error.into()))?,
        );
        let metadata = fstat(&entry).map_err(|error| ResearchSidecarError::Io(error.into()))?;
        if FileType::from_raw_mode(metadata.st_mode).is_dir() {
            ensure_private_directory(&entry)?;
            remove_private_directory_contents(&entry)?;
            unlinkat(directory, &name, AtFlags::REMOVEDIR)
                .map_err(|error| ResearchSidecarError::Io(error.into()))?;
        } else {
            unlinkat(directory, &name, AtFlags::empty())
                .map_err(|error| ResearchSidecarError::Io(error.into()))?;
        }
    }
    Ok(())
}

/// Sidecar storage or validation failure.
#[derive(Debug, Error)]
pub enum ResearchSidecarError {
    #[error(transparent)]
    Event(#[from] trench_core::event::EventError),
    #[error(transparent)]
    TimeRange(#[from] trench_core::validation::ValidationError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error("invalid research sidecar: {reason}")]
    InvalidSidecar { reason: &'static str },
    #[error("existing final research sidecar conflicts with this staged sidecar")]
    ConflictingFinalSidecar,
    #[error("research sidecar resource limit exceeded")]
    ResourceLimit,
    #[error("research sidecars require a Unix private filesystem")]
    UnsupportedPlatform,
}
