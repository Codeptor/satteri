//! Immutable, descriptor-bound reconciled recovery outcomes for research.
//!
//! Recovery outcomes are system evidence rather than venue events. They are
//! published as private companion source members and can release a recovered
//! market only at their verified raw availability anchor.

use std::{
    collections::BTreeSet,
    fs::{self, File},
    io::{self, Read, Write},
    sync::Arc,
};

#[cfg(unix)]
use rustix::fs::{
    AtFlags, FileType, Mode, OFlags, RenameFlags, fstat, mkdirat, openat, renameat_with, unlinkat,
};
use serde::{Deserialize, Serialize};
#[cfg(unix)]
use std::os::unix::io::AsRawFd;
use thiserror::Error;
use trench_core::{
    domain::{EventId, Market},
    event::{MarketEvent, MarketEventKind, TimestampNs},
};

use crate::{
    parquet::{DataProvenance, ParquetError, ParquetStore},
    research_runs::AvailabilityKey,
};

const OUTCOME_VERSION: u8 = 1;
const OUTCOME_FILE: &str = "outcome.json";
const MANIFEST_FILE: &str = "manifest.json";
const MAX_OUTCOME_BYTES: u64 = 1_048_576;
const MAX_IDENTIFIER_BYTES: usize = 256;
const MAX_BACKFILL_REFS: usize = 8_192;
const MAX_OFFICIAL_CANDLE_REFS: usize = 5_000;
/// Fixed aggregate raw-proof budget shared by every selected source plan.
pub(crate) const MAX_TOTAL_RECOVERY_PROOF_REFERENCES: usize = 65_536;

/// The terminal reconciliation state of one recovery request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryOutcomeStatus {
    /// Local captured trades reconciled exactly against official candles.
    Reconciled,
    /// Recovery evidence was explicitly unavailable or conflicted.
    Unavailable,
}

/// The evidence class that produced a terminal recovery outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryOutcomeSource {
    /// Captured local trades and official candles produced a reconciled result.
    CapturedTrades,
    /// An official archive was L2-only and cannot reconstruct candles.
    ArchiveL2,
    /// No trustworthy evidence source was available.
    Unavailable,
}

/// One path-free raw source reference committed by a recovery companion member.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct RecoverySourceReference {
    member_manifest_digest: String,
    key: AvailabilityKey,
}

impl RecoverySourceReference {
    /// Binds one raw fact to its exact selected source member and full availability key.
    pub fn new(
        member_manifest_digest: impl Into<String>,
        key: AvailabilityKey,
    ) -> Result<Self, RecoveryOutcomeError> {
        let value = Self {
            member_manifest_digest: member_manifest_digest.into(),
            key,
        };
        value.validate()?;
        Ok(value)
    }

    /// Returns the manifest digest of the exact raw source member.
    #[must_use]
    pub fn member_manifest_digest(&self) -> &str {
        &self.member_manifest_digest
    }

    /// Returns the complete source availability key.
    #[must_use]
    pub const fn key(&self) -> &AvailabilityKey {
        &self.key
    }

    fn validate(&self) -> Result<(), RecoveryOutcomeError> {
        validate_digest(&self.member_manifest_digest)
    }
}

/// The request cursors needed to reproduce the recovery request boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveryRequestCursors {
    predecessor: Option<RecoverySourceReference>,
    trade_predecessor: Option<RecoverySourceReference>,
}

impl RecoveryRequestCursors {
    /// Creates the optional exact predecessor cursors captured with one request.
    pub fn new(
        predecessor: Option<RecoverySourceReference>,
        trade_predecessor: Option<RecoverySourceReference>,
    ) -> Result<Self, RecoveryOutcomeError> {
        let value = Self {
            predecessor,
            trade_predecessor,
        };
        value.validate()?;
        Ok(value)
    }

    fn validate(&self) -> Result<(), RecoveryOutcomeError> {
        if let Some(reference) = &self.predecessor {
            reference.validate()?;
        }
        if let Some(reference) = &self.trade_predecessor {
            reference.validate()?;
        }
        Ok(())
    }
}

/// Immutable recovery system evidence bound to an exact raw availability anchor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconciledRecoveryOutcome {
    request_id: String,
    generation: u64,
    market: Market,
    request_cursors: RecoveryRequestCursors,
    status: RecoveryOutcomeStatus,
    source: RecoveryOutcomeSource,
    completed_through: TimestampNs,
    recovery_anchor: RecoverySourceReference,
    backfill_references: Vec<RecoverySourceReference>,
    official_candle_references: Vec<RecoverySourceReference>,
    availability_anchor: RecoverySourceReference,
    result_digest: String,
}

impl ReconciledRecoveryOutcome {
    /// Creates one immutable terminal recovery outcome.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        request_id: impl Into<String>,
        generation: u64,
        market: Market,
        request_cursors: RecoveryRequestCursors,
        status: RecoveryOutcomeStatus,
        source: RecoveryOutcomeSource,
        completed_through: TimestampNs,
        recovery_anchor: RecoverySourceReference,
        backfill_references: Vec<RecoverySourceReference>,
        official_candle_references: Vec<RecoverySourceReference>,
        availability_anchor: RecoverySourceReference,
    ) -> Result<Self, RecoveryOutcomeError> {
        let mut value = Self {
            request_id: request_id.into(),
            generation,
            market,
            request_cursors,
            status,
            source,
            completed_through,
            recovery_anchor,
            backfill_references,
            official_candle_references,
            availability_anchor,
            result_digest: String::new(),
        };
        value.validate_structure()?;
        value.result_digest = value.expected_result_digest()?;
        Ok(value)
    }

    /// Returns the content-addressed immutable identity of this outcome payload.
    pub fn outcome_id(&self) -> Result<String, RecoveryOutcomeError> {
        payload_digest(&self.to_wire())
    }

    /// Returns the recovered market.
    #[must_use]
    pub const fn market(&self) -> &Market {
        &self.market
    }

    /// Returns the immutable recovery request identity.
    #[must_use]
    pub(crate) fn request_id(&self) -> &str {
        &self.request_id
    }

    /// Returns the completed historical recovery boundary, never a readiness clock.
    #[must_use]
    pub const fn completed_through(&self) -> TimestampNs {
        self.completed_through
    }

    /// Returns the exact raw source key at which this outcome becomes available.
    #[must_use]
    pub const fn availability_anchor(&self) -> &RecoverySourceReference {
        &self.availability_anchor
    }

    /// Returns the exact recovered-book source reference.
    #[must_use]
    pub(crate) const fn recovery_anchor(&self) -> &RecoverySourceReference {
        &self.recovery_anchor
    }

    /// Returns the captured-trade proof references in canonical order.
    #[must_use]
    pub(crate) fn backfill_references(&self) -> &[RecoverySourceReference] {
        &self.backfill_references
    }

    /// Returns the immutable result commitment bound to the complete raw proof.
    #[must_use]
    pub(crate) fn result_digest(&self) -> &str {
        &self.result_digest
    }

    /// Returns the typed reconciliation status.
    #[must_use]
    pub const fn status(&self) -> RecoveryOutcomeStatus {
        self.status
    }

    /// Returns the typed evidence source.
    #[must_use]
    pub const fn source(&self) -> RecoveryOutcomeSource {
        self.source
    }

    fn validate(&self) -> Result<(), RecoveryOutcomeError> {
        self.validate_structure()?;
        if self.result_digest != self.expected_result_digest()? {
            return Err(RecoveryOutcomeError::InvalidOutcome);
        }
        Ok(())
    }

    fn validate_structure(&self) -> Result<(), RecoveryOutcomeError> {
        if self.request_id.is_empty()
            || self.request_id.len() > MAX_IDENTIFIER_BYTES
            || self.request_id.trim() != self.request_id
            || self.request_id.chars().any(char::is_control)
            || self.generation == 0
            || self.backfill_references.len() > MAX_BACKFILL_REFS
            || self.official_candle_references.len() > MAX_OFFICIAL_CANDLE_REFS
        {
            return Err(RecoveryOutcomeError::InvalidOutcome);
        }
        self.request_cursors.validate()?;
        self.recovery_anchor.validate()?;
        self.availability_anchor.validate()?;
        validate_sorted_references(&self.backfill_references)?;
        validate_sorted_references(&self.official_candle_references)?;
        match (self.status, self.source) {
            (RecoveryOutcomeStatus::Reconciled, RecoveryOutcomeSource::CapturedTrades) => {}
            (RecoveryOutcomeStatus::Unavailable, RecoveryOutcomeSource::ArchiveL2)
            | (RecoveryOutcomeStatus::Unavailable, RecoveryOutcomeSource::Unavailable) => {
                if !self.backfill_references.is_empty()
                    || !self.official_candle_references.is_empty()
                {
                    return Err(RecoveryOutcomeError::InvalidOutcome);
                }
            }
            _ => return Err(RecoveryOutcomeError::InvalidOutcome),
        }
        let mut source_ids = BTreeSet::new();
        for reference in std::iter::once(&self.recovery_anchor)
            .chain(&self.backfill_references)
            .chain(&self.official_candle_references)
        {
            if !source_ids.insert(reference.key().event_id().clone()) {
                return Err(RecoveryOutcomeError::InvalidOutcome);
            }
        }
        let maximum_input = std::iter::once(&self.recovery_anchor)
            .chain(self.request_cursors.predecessor.iter())
            .chain(self.request_cursors.trade_predecessor.iter())
            .chain(&self.backfill_references)
            .chain(&self.official_candle_references)
            .max_by_key(|reference| reference.key());
        if maximum_input != Some(&self.availability_anchor) {
            return Err(RecoveryOutcomeError::InvalidOutcome);
        }
        Ok(())
    }

    pub(crate) fn expected_result_digest(&self) -> Result<String, RecoveryOutcomeError> {
        payload_digest(&RecoveryOutcomeResultWire {
            version: OUTCOME_VERSION,
            request_id: self.request_id.clone(),
            generation: self.generation,
            market: self.market.as_str().to_owned(),
            predecessor: self
                .request_cursors
                .predecessor
                .as_ref()
                .map(reference_wire),
            trade_predecessor: self
                .request_cursors
                .trade_predecessor
                .as_ref()
                .map(reference_wire),
            status: status_name(self.status).to_owned(),
            source: source_name(self.source).to_owned(),
            completed_through_ns: self.completed_through.value(),
            recovery_anchor: reference_wire(&self.recovery_anchor),
            backfill_references: self
                .backfill_references
                .iter()
                .map(reference_wire)
                .collect(),
            official_candle_references: self
                .official_candle_references
                .iter()
                .map(reference_wire)
                .collect(),
            availability_anchor: reference_wire(&self.availability_anchor),
        })
    }

    fn to_wire(&self) -> RecoveryOutcomeWire {
        RecoveryOutcomeWire {
            version: OUTCOME_VERSION,
            request_id: self.request_id.clone(),
            generation: self.generation,
            market: self.market.as_str().to_owned(),
            predecessor: self
                .request_cursors
                .predecessor
                .as_ref()
                .map(reference_wire),
            trade_predecessor: self
                .request_cursors
                .trade_predecessor
                .as_ref()
                .map(reference_wire),
            status: status_name(self.status).to_owned(),
            source: source_name(self.source).to_owned(),
            completed_through_ns: self.completed_through.value(),
            recovery_anchor: reference_wire(&self.recovery_anchor),
            backfill_references: self
                .backfill_references
                .iter()
                .map(reference_wire)
                .collect(),
            official_candle_references: self
                .official_candle_references
                .iter()
                .map(reference_wire)
                .collect(),
            availability_anchor: reference_wire(&self.availability_anchor),
            result_digest: self.result_digest.clone(),
        }
    }

    fn from_wire(wire: RecoveryOutcomeWire) -> Result<Self, RecoveryOutcomeError> {
        if wire.version != OUTCOME_VERSION {
            return Err(RecoveryOutcomeError::InvalidOutcome);
        }
        let value = Self {
            request_id: wire.request_id,
            generation: wire.generation,
            market: Market::new(wire.market).map_err(|_| RecoveryOutcomeError::InvalidOutcome)?,
            request_cursors: RecoveryRequestCursors::new(
                wire.predecessor.map(reference_from_wire).transpose()?,
                wire.trade_predecessor
                    .map(reference_from_wire)
                    .transpose()?,
            )?,
            status: parse_status(&wire.status)?,
            source: parse_source(&wire.source)?,
            completed_through: TimestampNs::new(i128::from(wire.completed_through_ns))
                .map_err(|_| RecoveryOutcomeError::InvalidOutcome)?,
            recovery_anchor: reference_from_wire(wire.recovery_anchor)?,
            backfill_references: wire
                .backfill_references
                .into_iter()
                .map(reference_from_wire)
                .collect::<Result<Vec<_>, _>>()?,
            official_candle_references: wire
                .official_candle_references
                .into_iter()
                .map(reference_from_wire)
                .collect::<Result<Vec<_>, _>>()?,
            availability_anchor: reference_from_wire(wire.availability_anchor)?,
            result_digest: wire.result_digest,
        };
        value.validate()?;
        Ok(value)
    }
}

/// Path-free locator for one content-addressed companion source member.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryOutcomeLocator {
    outcome_id: String,
    manifest_digest: String,
}

impl RecoveryOutcomeLocator {
    /// Returns the canonical companion payload identity.
    #[must_use]
    pub fn outcome_id(&self) -> &str {
        &self.outcome_id
    }

    /// Returns the canonical immutable companion manifest digest.
    #[must_use]
    pub fn manifest_digest(&self) -> &str {
        &self.manifest_digest
    }

    pub(crate) fn validate(&self) -> Result<(), RecoveryOutcomeError> {
        validate_digest(&self.outcome_id)?;
        validate_digest(&self.manifest_digest)
    }
}

impl ReconciledRecoveryOutcome {
    pub(crate) fn source_references(&self) -> impl Iterator<Item = &RecoverySourceReference> {
        std::iter::once(&self.recovery_anchor)
            .chain(self.request_cursors.predecessor.iter())
            .chain(self.request_cursors.trade_predecessor.iter())
            .chain(&self.backfill_references)
            .chain(&self.official_candle_references)
    }

    pub(crate) fn validate_raw_references(
        &self,
        observed: &std::collections::BTreeMap<RecoverySourceReference, MarketEvent>,
    ) -> Result<(), RecoveryOutcomeError> {
        let source = |reference: &RecoverySourceReference| {
            observed
                .get(reference)
                .ok_or(RecoveryOutcomeError::InvalidOutcome)
        };
        let anchor = source(&self.recovery_anchor)?;
        if anchor.market() != &self.market
            || !matches!(anchor.kind(), MarketEventKind::BookSnapshot(_))
            || anchor.event_time() > self.completed_through
        {
            return Err(RecoveryOutcomeError::InvalidOutcome);
        }
        for reference in self.request_cursors.predecessor.iter() {
            if source(reference)?.market() != &self.market
                || reference.key() >= self.recovery_anchor.key()
            {
                return Err(RecoveryOutcomeError::InvalidOutcome);
            }
        }
        for reference in self.request_cursors.trade_predecessor.iter() {
            let event = source(reference)?;
            if event.market() != &self.market
                || !matches!(event.kind(), MarketEventKind::Trade(_))
                || reference.key() >= self.recovery_anchor.key()
            {
                return Err(RecoveryOutcomeError::InvalidOutcome);
            }
        }
        for reference in &self.backfill_references {
            let event = source(reference)?;
            if event.market() != &self.market
                || !matches!(event.kind(), MarketEventKind::Trade(_))
                || event.event_time() > self.completed_through
            {
                return Err(RecoveryOutcomeError::InvalidOutcome);
            }
        }
        for reference in &self.official_candle_references {
            let event = source(reference)?;
            if event.market() != &self.market
                || !matches!(event.kind(), MarketEventKind::CompletedCandle(_))
                || event.event_time() > self.completed_through
            {
                return Err(RecoveryOutcomeError::InvalidOutcome);
            }
        }
        if self.status == RecoveryOutcomeStatus::Reconciled
            && self.official_candle_references.is_empty()
        {
            return Err(RecoveryOutcomeError::InvalidOutcome);
        }
        Ok(())
    }

    /// Recomputes the result commitment after checking every exact raw proof reference.
    pub(crate) fn verify_result_from_raw(
        &self,
        observed: &std::collections::BTreeMap<RecoverySourceReference, MarketEvent>,
    ) -> Result<(), RecoveryOutcomeError> {
        self.validate_raw_references(observed)?;
        if self.result_digest != self.expected_result_digest()? {
            return Err(RecoveryOutcomeError::InvalidOutcome);
        }
        Ok(())
    }
}

/// Private immutable companion-member store rooted by a verified Parquet store.
#[derive(Debug, Clone)]
pub struct RecoveryOutcomeStore {
    directory: Arc<File>,
    provenance: DataProvenance,
}

impl RecoveryOutcomeStore {
    /// Opens the trusted companion-member directory owned by one Parquet store.
    pub fn open(store: &ParquetStore) -> Result<Self, RecoveryOutcomeError> {
        Ok(Self {
            directory: Arc::new(store.recovery_outcomes_descriptor()?),
            provenance: store.provenance().clone(),
        })
    }

    /// Atomically publishes one immutable companion outcome and returns its path-free locator.
    pub fn publish(
        &self,
        outcome: &ReconciledRecoveryOutcome,
    ) -> Result<RecoveryOutcomeLocator, RecoveryOutcomeError> {
        outcome.validate()?;
        let outcome_wire = outcome.to_wire();
        let outcome_bytes = canonical_bytes(&outcome_wire)?;
        if u64::try_from(outcome_bytes.len()).map_err(|_| RecoveryOutcomeError::ResourceLimit)?
            > MAX_OUTCOME_BYTES
        {
            return Err(RecoveryOutcomeError::ResourceLimit);
        }
        let outcome_id = payload_digest(&outcome_wire)?;
        let manifest = RecoveryOutcomeManifest {
            version: OUTCOME_VERSION,
            outcome_id: outcome_id.clone(),
            payload_digest: bytes_digest(&outcome_bytes),
            provenance: self.provenance.clone(),
        };
        let manifest_bytes = canonical_bytes(&manifest)?;
        if u64::try_from(manifest_bytes.len()).map_err(|_| RecoveryOutcomeError::ResourceLimit)?
            > MAX_OUTCOME_BYTES
        {
            return Err(RecoveryOutcomeError::ResourceLimit);
        }
        let locator = RecoveryOutcomeLocator {
            outcome_id,
            manifest_digest: bytes_digest(&manifest_bytes),
        };
        #[cfg(not(unix))]
        {
            let _ = (outcome_bytes, manifest_bytes, locator);
            Err(RecoveryOutcomeError::UnsupportedPlatform)
        }
        #[cfg(unix)]
        {
            publish_outcome_at(&self.directory, &locator, &outcome_bytes, &manifest_bytes)?;
            Ok(locator)
        }
    }

    /// Reopens and revalidates only the exact immutable companion selected by its locator.
    pub fn open_member(
        &self,
        locator: &RecoveryOutcomeLocator,
    ) -> Result<ReconciledRecoveryOutcome, RecoveryOutcomeError> {
        locator.validate()?;
        #[cfg(not(unix))]
        {
            let _ = locator;
            Err(RecoveryOutcomeError::UnsupportedPlatform)
        }
        #[cfg(unix)]
        {
            open_outcome_at(&self.directory, locator, Some(&self.provenance))
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RecoveryOutcomeWire {
    version: u8,
    request_id: String,
    generation: u64,
    market: String,
    predecessor: Option<RecoverySourceReferenceWire>,
    trade_predecessor: Option<RecoverySourceReferenceWire>,
    status: String,
    source: String,
    completed_through_ns: i64,
    recovery_anchor: RecoverySourceReferenceWire,
    backfill_references: Vec<RecoverySourceReferenceWire>,
    official_candle_references: Vec<RecoverySourceReferenceWire>,
    availability_anchor: RecoverySourceReferenceWire,
    result_digest: String,
}

#[derive(Debug, Clone, Serialize)]
struct RecoveryOutcomeResultWire {
    version: u8,
    request_id: String,
    generation: u64,
    market: String,
    predecessor: Option<RecoverySourceReferenceWire>,
    trade_predecessor: Option<RecoverySourceReferenceWire>,
    status: String,
    source: String,
    completed_through_ns: i64,
    recovery_anchor: RecoverySourceReferenceWire,
    backfill_references: Vec<RecoverySourceReferenceWire>,
    official_candle_references: Vec<RecoverySourceReferenceWire>,
    availability_anchor: RecoverySourceReferenceWire,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RecoverySourceReferenceWire {
    member_manifest_digest: String,
    received_at_ns: i64,
    event_time_ns: i64,
    event_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RecoveryOutcomeManifest {
    version: u8,
    outcome_id: String,
    payload_digest: String,
    provenance: DataProvenance,
}

fn reference_wire(reference: &RecoverySourceReference) -> RecoverySourceReferenceWire {
    RecoverySourceReferenceWire {
        member_manifest_digest: reference.member_manifest_digest.clone(),
        received_at_ns: reference.key.received_at().value(),
        event_time_ns: reference.key.event_time().value(),
        event_id: reference.key.event_id().as_str().to_owned(),
    }
}

fn reference_from_wire(
    wire: RecoverySourceReferenceWire,
) -> Result<RecoverySourceReference, RecoveryOutcomeError> {
    RecoverySourceReference::new(
        wire.member_manifest_digest,
        AvailabilityKey::new(
            TimestampNs::new(i128::from(wire.received_at_ns))
                .map_err(|_| RecoveryOutcomeError::InvalidOutcome)?,
            TimestampNs::new(i128::from(wire.event_time_ns))
                .map_err(|_| RecoveryOutcomeError::InvalidOutcome)?,
            EventId::new(wire.event_id).map_err(|_| RecoveryOutcomeError::InvalidOutcome)?,
        )
        .map_err(|_| RecoveryOutcomeError::InvalidOutcome)?,
    )
}

fn validate_sorted_references(
    references: &[RecoverySourceReference],
) -> Result<(), RecoveryOutcomeError> {
    let mut previous = None;
    for reference in references {
        reference.validate()?;
        if previous
            .replace(reference.key.event_id())
            .is_some_and(|prior: &EventId| prior >= reference.key.event_id())
        {
            return Err(RecoveryOutcomeError::InvalidOutcome);
        }
    }
    Ok(())
}

const fn status_name(status: RecoveryOutcomeStatus) -> &'static str {
    match status {
        RecoveryOutcomeStatus::Reconciled => "reconciled",
        RecoveryOutcomeStatus::Unavailable => "unavailable",
    }
}

const fn source_name(source: RecoveryOutcomeSource) -> &'static str {
    match source {
        RecoveryOutcomeSource::CapturedTrades => "captured_trades",
        RecoveryOutcomeSource::ArchiveL2 => "archive_l2",
        RecoveryOutcomeSource::Unavailable => "unavailable",
    }
}

fn parse_status(value: &str) -> Result<RecoveryOutcomeStatus, RecoveryOutcomeError> {
    match value {
        "reconciled" => Ok(RecoveryOutcomeStatus::Reconciled),
        "unavailable" => Ok(RecoveryOutcomeStatus::Unavailable),
        _ => Err(RecoveryOutcomeError::InvalidOutcome),
    }
}

fn parse_source(value: &str) -> Result<RecoveryOutcomeSource, RecoveryOutcomeError> {
    match value {
        "captured_trades" => Ok(RecoveryOutcomeSource::CapturedTrades),
        "archive_l2" => Ok(RecoveryOutcomeSource::ArchiveL2),
        "unavailable" => Ok(RecoveryOutcomeSource::Unavailable),
        _ => Err(RecoveryOutcomeError::InvalidOutcome),
    }
}

fn payload_digest(value: &impl Serialize) -> Result<String, RecoveryOutcomeError> {
    Ok(bytes_digest(&canonical_bytes(value)?))
}

fn canonical_bytes<T: Serialize>(value: &T) -> Result<Vec<u8>, RecoveryOutcomeError> {
    Ok(serde_json::to_vec(value)?)
}

fn bytes_digest(bytes: &[u8]) -> String {
    format!("b3:{}", blake3::hash(bytes).to_hex())
}

fn validate_digest(value: &str) -> Result<(), RecoveryOutcomeError> {
    if is_digest(value) {
        Ok(())
    } else {
        Err(RecoveryOutcomeError::InvalidOutcome)
    }
}

fn is_digest(value: &str) -> bool {
    value.len() == 67
        && value.starts_with("b3:")
        && value[3..]
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[cfg(unix)]
fn publish_outcome_at(
    parent: &File,
    locator: &RecoveryOutcomeLocator,
    outcome_bytes: &[u8],
    manifest_bytes: &[u8],
) -> Result<(), RecoveryOutcomeError> {
    let final_name = outcome_directory_name(&locator.outcome_id)?;
    let stage_name = format!(".{final_name}.stage-{}", &locator.manifest_digest[3..19]);
    match mkdirat(parent, &stage_name, Mode::RUSR | Mode::WUSR | Mode::XUSR) {
        Ok(()) => {}
        Err(error) if error == rustix::io::Errno::EXIST => {
            remove_outcome_stage(parent, &stage_name)?;
            mkdirat(parent, &stage_name, Mode::RUSR | Mode::WUSR | Mode::XUSR)
                .map_err(|error| RecoveryOutcomeError::Io(error.into()))?;
        }
        Err(error) => return Err(RecoveryOutcomeError::Io(error.into())),
    }
    parent.sync_all()?;
    let stage = open_private_directory_at(parent, &stage_name)?;
    let result = (|| {
        write_private_file(&stage, OUTCOME_FILE, outcome_bytes)?;
        write_private_file(&stage, MANIFEST_FILE, manifest_bytes)?;
        let _ = outcome_from_bytes(locator, manifest_bytes, outcome_bytes, None)?;
        stage.sync_all()?;
        match renameat_with(
            parent,
            &stage_name,
            parent,
            &final_name,
            RenameFlags::NOREPLACE,
        ) {
            Ok(()) => {
                parent.sync_all()?;
                Ok(())
            }
            Err(error) if error == rustix::io::Errno::EXIST => {
                let existing = open_outcome_at(parent, locator, None)?;
                let staged = decode_outcome(outcome_bytes)?;
                if existing != staged {
                    return Err(RecoveryOutcomeError::ConflictingOutcome);
                }
                remove_outcome_stage(parent, &stage_name)?;
                parent.sync_all()?;
                Ok(())
            }
            Err(error) => Err(RecoveryOutcomeError::Io(error.into())),
        }
    })();
    if result.is_err() {
        let _ = remove_outcome_stage(parent, &stage_name);
        let _ = parent.sync_all();
    }
    result
}

#[cfg(unix)]
fn open_outcome_at(
    parent: &File,
    locator: &RecoveryOutcomeLocator,
    expected_provenance: Option<&DataProvenance>,
) -> Result<ReconciledRecoveryOutcome, RecoveryOutcomeError> {
    let directory =
        open_private_directory_at(parent, &outcome_directory_name(&locator.outcome_id)?)?;
    require_exact_outcome_entries(&directory)?;
    let manifest_bytes = read_private_file(&directory, MANIFEST_FILE)?;
    let outcome_bytes = read_private_file(&directory, OUTCOME_FILE)?;
    outcome_from_bytes(
        locator,
        &manifest_bytes,
        &outcome_bytes,
        expected_provenance,
    )
}

fn outcome_from_bytes(
    locator: &RecoveryOutcomeLocator,
    manifest_bytes: &[u8],
    outcome_bytes: &[u8],
    expected_provenance: Option<&DataProvenance>,
) -> Result<ReconciledRecoveryOutcome, RecoveryOutcomeError> {
    if bytes_digest(manifest_bytes) != locator.manifest_digest {
        return Err(RecoveryOutcomeError::InvalidOutcome);
    }
    let manifest = decode_manifest(manifest_bytes)?;
    if manifest.outcome_id != locator.outcome_id || manifest.version != OUTCOME_VERSION {
        return Err(RecoveryOutcomeError::InvalidOutcome);
    }
    if let Some(provenance) = expected_provenance
        && manifest.provenance != *provenance
    {
        return Err(RecoveryOutcomeError::InvalidOutcome);
    }
    if bytes_digest(outcome_bytes) != manifest.payload_digest {
        return Err(RecoveryOutcomeError::InvalidOutcome);
    }
    let outcome = decode_outcome(outcome_bytes)?;
    if outcome.outcome_id()? != locator.outcome_id {
        return Err(RecoveryOutcomeError::InvalidOutcome);
    }
    Ok(outcome)
}

fn decode_outcome(bytes: &[u8]) -> Result<ReconciledRecoveryOutcome, RecoveryOutcomeError> {
    let wire = serde_json::from_slice::<RecoveryOutcomeWire>(bytes)?;
    if canonical_bytes(&wire)? != bytes {
        return Err(RecoveryOutcomeError::InvalidOutcome);
    }
    ReconciledRecoveryOutcome::from_wire(wire)
}

fn decode_manifest(bytes: &[u8]) -> Result<RecoveryOutcomeManifest, RecoveryOutcomeError> {
    let manifest = serde_json::from_slice::<RecoveryOutcomeManifest>(bytes)?;
    if canonical_bytes(&manifest)? != bytes
        || manifest.version != OUTCOME_VERSION
        || !is_digest(&manifest.outcome_id)
        || !is_digest(&manifest.payload_digest)
    {
        return Err(RecoveryOutcomeError::InvalidOutcome);
    }
    Ok(manifest)
}

#[cfg(unix)]
fn outcome_directory_name(outcome_id: &str) -> Result<String, RecoveryOutcomeError> {
    validate_digest(outcome_id)?;
    Ok(format!("outcome-{outcome_id}.out"))
}

#[cfg(unix)]
fn open_private_directory_at(parent: &File, name: &str) -> Result<File, RecoveryOutcomeError> {
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
        .map_err(|error| RecoveryOutcomeError::Io(error.into()))?,
    );
    ensure_private_directory(&directory)?;
    Ok(directory)
}

#[cfg(unix)]
fn ensure_private_directory(directory: &File) -> Result<(), RecoveryOutcomeError> {
    let metadata = fstat(directory).map_err(|error| RecoveryOutcomeError::Io(error.into()))?;
    if !FileType::from_raw_mode(metadata.st_mode).is_dir()
        || metadata.st_uid != rustix::process::geteuid().as_raw()
        || metadata.st_mode & 0o777 != 0o700
    {
        return Err(RecoveryOutcomeError::InvalidOutcome);
    }
    Ok(())
}

#[cfg(unix)]
fn write_private_file(
    directory: &File,
    name: &str,
    bytes: &[u8],
) -> Result<(), RecoveryOutcomeError> {
    validate_component(name)?;
    let mut file = File::from(
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
        .map_err(|error| RecoveryOutcomeError::Io(error.into()))?,
    );
    rustix::fs::fchmod(&file, Mode::RUSR | Mode::WUSR)
        .map_err(|error| RecoveryOutcomeError::Io(error.into()))?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}

#[cfg(unix)]
fn read_private_file(directory: &File, name: &str) -> Result<Vec<u8>, RecoveryOutcomeError> {
    validate_component(name)?;
    let mut file = File::from(
        openat(
            directory,
            name,
            OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(|error| RecoveryOutcomeError::Io(error.into()))?,
    );
    let metadata = fstat(&file).map_err(|error| RecoveryOutcomeError::Io(error.into()))?;
    let length =
        u64::try_from(metadata.st_size).map_err(|_| RecoveryOutcomeError::ResourceLimit)?;
    if !FileType::from_raw_mode(metadata.st_mode).is_file()
        || metadata.st_uid != rustix::process::geteuid().as_raw()
        || metadata.st_mode & 0o777 != 0o600
        || length > MAX_OUTCOME_BYTES
    {
        return Err(RecoveryOutcomeError::InvalidOutcome);
    }
    let mut bytes = Vec::with_capacity(
        usize::try_from(length).map_err(|_| RecoveryOutcomeError::ResourceLimit)?,
    );
    file.read_to_end(&mut bytes)?;
    if u64::try_from(bytes.len()).ok() != Some(length) {
        return Err(RecoveryOutcomeError::InvalidOutcome);
    }
    Ok(bytes)
}

#[cfg(unix)]
fn directory_names(directory: &File) -> Result<Vec<std::ffi::OsString>, RecoveryOutcomeError> {
    let path = format!("/proc/self/fd/{}", directory.as_raw_fd());
    let mut entries = fs::read_dir(path)?
        .map(|entry| entry.map(|entry| entry.file_name()))
        .collect::<Result<Vec<_>, _>>()?;
    entries.sort();
    Ok(entries)
}

#[cfg(not(unix))]
fn directory_names(_directory: &File) -> Result<Vec<std::ffi::OsString>, RecoveryOutcomeError> {
    Err(RecoveryOutcomeError::UnsupportedPlatform)
}

fn require_exact_outcome_entries(directory: &File) -> Result<(), RecoveryOutcomeError> {
    let expected = [
        std::ffi::OsString::from(MANIFEST_FILE),
        std::ffi::OsString::from(OUTCOME_FILE),
    ];
    if directory_names(directory)? != expected {
        return Err(RecoveryOutcomeError::InvalidOutcome);
    }
    let _manifest = read_private_file(directory, MANIFEST_FILE)?;
    let _outcome = read_private_file(directory, OUTCOME_FILE)?;
    Ok(())
}

#[cfg(unix)]
fn remove_outcome_stage(parent: &File, stage_name: &str) -> Result<(), RecoveryOutcomeError> {
    let stage = open_private_directory_at(parent, stage_name)?;
    for name in [OUTCOME_FILE, MANIFEST_FILE] {
        match unlinkat(&stage, name, AtFlags::empty()) {
            Ok(()) => {}
            Err(error) if error == rustix::io::Errno::NOENT => {}
            Err(error) => return Err(RecoveryOutcomeError::Io(error.into())),
        }
    }
    unlinkat(parent, stage_name, AtFlags::REMOVEDIR)
        .map_err(|error| RecoveryOutcomeError::Io(error.into()))
}

#[cfg(unix)]
fn validate_component(value: &str) -> Result<(), RecoveryOutcomeError> {
    if value.is_empty()
        || value == "."
        || value == ".."
        || value.contains('/')
        || value.as_bytes().contains(&0)
    {
        return Err(RecoveryOutcomeError::InvalidOutcome);
    }
    Ok(())
}

/// Companion-member construction, persistence, or validation failure.
#[derive(Debug, Error)]
pub enum RecoveryOutcomeError {
    /// The owning source store failed validation or descriptor-relative access.
    #[error(transparent)]
    Storage(#[from] ParquetError),
    /// Canonical outcome JSON encoding or decoding failed.
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    /// Descriptor-relative companion I/O failed.
    #[error(transparent)]
    Io(#[from] io::Error),
    /// The typed recovery outcome was malformed or internally inconsistent.
    #[error("reconciled recovery outcome is invalid")]
    InvalidOutcome,
    /// A bounded companion payload exceeded its fixed resource limit.
    #[error("reconciled recovery outcome exceeded its fixed resource limit")]
    ResourceLimit,
    /// An existing immutable companion location contains different bytes.
    #[error("reconciled recovery outcome conflicts with an existing immutable member")]
    ConflictingOutcome,
    /// Descriptor-bound companion source members require Unix filesystem semantics.
    #[error("reconciled recovery outcome source members require Unix")]
    UnsupportedPlatform,
}
