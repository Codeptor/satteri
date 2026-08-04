//! Immutable, descriptor-bound reconciled recovery outcomes for research.
//!
//! Recovery outcomes are system evidence rather than venue events. They are
//! published as private companion source members and can release a recovered
//! market only at their verified raw availability anchor.

use std::{
    collections::{BTreeMap, BTreeSet},
    io,
};

#[cfg(unix)]
use rustix::fs::{
    AtFlags, FileType, Mode, OFlags, RenameFlags, fstat, mkdirat, openat, renameat_with, unlinkat,
};
use serde::{Deserialize, Serialize};
#[cfg(unix)]
use std::fs;
#[cfg(unix)]
use std::fs::File;
#[cfg(unix)]
use std::io::{Read, Write};
#[cfg(unix)]
use std::os::unix::io::AsRawFd;
#[cfg(unix)]
use std::sync::Arc;
use thiserror::Error;
use trench_core::{
    domain::{EventId, Market},
    event::{CandleInterval, MarketEvent, MarketEventKind, TimestampNs},
};
use trench_hyperliquid::{
    RecoveryResult as HyperliquidRecoveryResult, RecoverySource as HyperliquidRecoverySource,
    RecoveryStatus as HyperliquidRecoveryStatus, VerifiedRecoveryWitness,
};

use crate::{
    parquet::{DataProvenance, ParquetError, ParquetStore},
    research_runs::AvailabilityKey,
};

const OUTCOME_VERSION: u8 = 2;
#[cfg(unix)]
const OUTCOME_FILE: &str = "outcome.json";
#[cfg(unix)]
const MANIFEST_FILE: &str = "manifest.json";
const MAX_OUTCOME_BYTES: u64 = 1_048_576;
const MAX_IDENTIFIER_BYTES: usize = 256;
const MAX_BACKFILL_REFS: usize = 8_192;
const MAX_OFFICIAL_CANDLE_REFS: usize = 5_000;
const WITNESS_VERSION: u8 = 1;
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
    verified_result: Option<VerifiedRecoveryResultWire>,
    result_digest: String,
}

impl ReconciledRecoveryOutcome {
    /// Binds a reconciled companion to a result emitted by Hyperliquid's recovery coordinator.
    ///
    /// The coordinator owns the prior candle-aggregator state, so raw references alone can
    /// never establish reconciliation. This constructor is deliberately the only reconciled
    /// construction path and accepts the opaque, already-verified result instead of a caller
    /// supplied status or digest.
    #[expect(
        clippy::too_many_arguments,
        reason = "the opaque result and each independently selected source proof are explicit"
    )]
    pub(crate) fn from_verified_result(
        result: &HyperliquidRecoveryResult,
        predecessor: Option<RecoverySourceReference>,
        trade_predecessor: RecoverySourceReference,
        recovery_anchor: RecoverySourceReference,
        backfill_references: Vec<RecoverySourceReference>,
        official_candle_references: Vec<RecoverySourceReference>,
        availability_anchor: RecoverySourceReference,
        raw_proof: &BTreeMap<RecoverySourceReference, MarketEvent>,
    ) -> Result<Self, RecoveryOutcomeError> {
        let witness = result
            .verified_witness()
            .ok_or(RecoveryOutcomeError::InvalidOutcome)?;
        if result.source() != HyperliquidRecoverySource::LocalTradesAndOfficialCandles
            || !matches!(
                result.status(),
                HyperliquidRecoveryStatus::Reconciled { .. }
            )
            || result.request() != witness.request()
            || result.completed_through() != witness.completed_through()
        {
            return Err(RecoveryOutcomeError::InvalidOutcome);
        }
        let verified_result = VerifiedRecoveryResultWire::from_witness(witness)?;
        let request = result.request();
        let value = Self {
            request_id: format!("{}:{}", request.market().as_str(), request.generation()),
            generation: request.generation(),
            market: request.market().clone(),
            request_cursors: RecoveryRequestCursors::new(predecessor, Some(trade_predecessor))?,
            status: RecoveryOutcomeStatus::Reconciled,
            source: RecoveryOutcomeSource::CapturedTrades,
            completed_through: result.completed_through(),
            recovery_anchor,
            backfill_references,
            official_candle_references,
            availability_anchor,
            result_digest: witness.commitment().to_owned(),
            verified_result: Some(verified_result),
        };
        value.validate_structure()?;
        value.validate_verified_result(result, witness, raw_proof)?;
        Ok(value)
    }

    /// Creates an explicit unavailable outcome that can quarantine entries but never release them.
    #[expect(
        clippy::too_many_arguments,
        reason = "unavailable outcomes deliberately bind their independent immutable request facts"
    )]
    pub(crate) fn unavailable(
        request_id: impl Into<String>,
        generation: u64,
        market: Market,
        request_cursors: RecoveryRequestCursors,
        source: RecoveryOutcomeSource,
        completed_through: TimestampNs,
        recovery_anchor: RecoverySourceReference,
        availability_anchor: RecoverySourceReference,
    ) -> Result<Self, RecoveryOutcomeError> {
        let mut value = Self {
            request_id: request_id.into(),
            generation,
            market,
            request_cursors,
            status: RecoveryOutcomeStatus::Unavailable,
            source,
            completed_through,
            recovery_anchor,
            backfill_references: Vec::new(),
            official_candle_references: Vec::new(),
            availability_anchor,
            verified_result: None,
            result_digest: String::new(),
        };
        value.validate_structure()?;
        value.result_digest = value.unavailable_result_digest()?;
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

    /// Returns the opaque upstream recovery-witness commitment.
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
        match (&self.status, &self.verified_result) {
            (RecoveryOutcomeStatus::Reconciled, Some(result)) => {
                if self.result_digest != result.upstream_commitment {
                    return Err(RecoveryOutcomeError::InvalidOutcome);
                }
                self.validate_verified_result_shape(result)?;
            }
            (RecoveryOutcomeStatus::Unavailable, None) => {
                if self.result_digest != self.unavailable_result_digest()? {
                    return Err(RecoveryOutcomeError::InvalidOutcome);
                }
            }
            _ => return Err(RecoveryOutcomeError::InvalidOutcome),
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
            (RecoveryOutcomeStatus::Reconciled, RecoveryOutcomeSource::CapturedTrades) => {
                if self.request_cursors.trade_predecessor.is_none()
                    || self.official_candle_references.is_empty()
                {
                    return Err(RecoveryOutcomeError::InvalidOutcome);
                }
            }
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

    fn unavailable_result_digest(&self) -> Result<String, RecoveryOutcomeError> {
        payload_digest(&UnavailableRecoveryResultWire {
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
            verified_result: self.verified_result.clone(),
            result_digest: self.result_digest.clone(),
        }
    }

    #[cfg(unix)]
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
            verified_result: wire.verified_result,
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

    fn validate_raw_references(
        &self,
        observed: &BTreeMap<RecoverySourceReference, MarketEvent>,
    ) -> Result<(), RecoveryOutcomeError> {
        let source = |reference: &RecoverySourceReference| referenced_event(observed, reference);
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
        let trade_predecessor = self.request_cursors.trade_predecessor.as_ref();
        for reference in &self.backfill_references {
            let event = source(reference)?;
            if event.market() != &self.market
                || !matches!(event.kind(), MarketEventKind::Trade(_))
                || event.event_time() >= self.completed_through
                || trade_predecessor.is_some_and(|predecessor| reference.key() <= predecessor.key())
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
        Ok(())
    }

    /// Validates every exact raw source fact against the persisted opaque witness.
    pub(crate) fn verify_result_from_raw(
        &self,
        observed: &BTreeMap<RecoverySourceReference, MarketEvent>,
    ) -> Result<(), RecoveryOutcomeError> {
        self.validate_raw_references(observed)?;
        if let Some(verified_result) = &self.verified_result {
            self.validate_verified_result_raw(verified_result, observed)?;
        }
        Ok(())
    }

    fn validate_verified_result(
        &self,
        result: &HyperliquidRecoveryResult,
        witness: &VerifiedRecoveryWitness,
        observed: &BTreeMap<RecoverySourceReference, MarketEvent>,
    ) -> Result<(), RecoveryOutcomeError> {
        let verified_result = self
            .verified_result
            .as_ref()
            .ok_or(RecoveryOutcomeError::InvalidOutcome)?;
        if result.source() != HyperliquidRecoverySource::LocalTradesAndOfficialCandles
            || !matches!(
                result.status(),
                HyperliquidRecoveryStatus::Reconciled { .. }
            )
            || result.request() != witness.request()
            || result.completed_through() != witness.completed_through()
            || *verified_result != VerifiedRecoveryResultWire::from_witness(witness)?
            || self.result_digest != witness.commitment()
        {
            return Err(RecoveryOutcomeError::InvalidOutcome);
        }
        self.validate_raw_references(observed)?;
        self.validate_verified_result_raw(verified_result, observed)
    }

    fn validate_verified_result_shape(
        &self,
        result: &VerifiedRecoveryResultWire,
    ) -> Result<(), RecoveryOutcomeError> {
        let expected_request_id = format!("{}:{}", result.market, result.generation);
        if result.version != WITNESS_VERSION
            || self.request_id != expected_request_id
            || self.generation != result.generation
            || self.market.as_str() != result.market
            || self.completed_through.value() != result.completed_through_ns
            || self.status != RecoveryOutcomeStatus::Reconciled
            || self.source != RecoveryOutcomeSource::CapturedTrades
            || self.request_cursors.trade_predecessor.is_none()
            || result.local_trades.len() != self.backfill_references.len()
            || result.official_candles.len() != self.official_candle_references.len()
            || result.official_candles.is_empty()
        {
            return Err(RecoveryOutcomeError::InvalidOutcome);
        }
        validate_digest(&result.upstream_commitment)?;
        validate_digest(&result.snapshot_event_id)?;
        validate_digest(&result.trade_predecessor_event_id)?;
        if !is_gap_reason(&result.reason)
            || result.reconnect_attempt == 0
            || !result
                .local_trades
                .iter()
                .all(|trade| trade.market == result.market)
            || !result
                .official_candles
                .iter()
                .chain(&result.derived_candles)
                .all(|candle| candle.market == result.market && is_interval(&candle.interval))
        {
            return Err(RecoveryOutcomeError::InvalidOutcome);
        }
        Ok(())
    }

    fn validate_verified_result_raw(
        &self,
        result: &VerifiedRecoveryResultWire,
        observed: &BTreeMap<RecoverySourceReference, MarketEvent>,
    ) -> Result<(), RecoveryOutcomeError> {
        self.validate_verified_result_shape(result)?;
        let source = |reference: &RecoverySourceReference| referenced_event(observed, reference);
        let anchor = source(&self.recovery_anchor)?;
        if anchor.event_id().as_str() != result.snapshot_event_id
            || anchor.event_time().value() != result.snapshot_event_time_ns
            || anchor.received_at().value() != result.snapshot_received_at_ns
        {
            return Err(RecoveryOutcomeError::InvalidOutcome);
        }
        match (
            self.request_cursors.predecessor.as_ref(),
            result.predecessor_event_time_ns,
            result.predecessor_received_at_ns,
        ) {
            (None, None, None) => {}
            (Some(reference), Some(event_time), Some(received_at))
                if source(reference)?.event_time().value() == event_time
                    && source(reference)?.received_at().value() == received_at => {}
            _ => return Err(RecoveryOutcomeError::InvalidOutcome),
        }
        let trade_predecessor = self
            .request_cursors
            .trade_predecessor
            .as_ref()
            .ok_or(RecoveryOutcomeError::InvalidOutcome)?;
        let trade_event = source(trade_predecessor)?;
        if trade_event.event_id().as_str() != result.trade_predecessor_event_id
            || trade_event.event_time().value() != result.trade_predecessor_event_time_ns
            || trade_event.received_at().value() != result.trade_predecessor_received_at_ns
        {
            return Err(RecoveryOutcomeError::InvalidOutcome);
        }
        let observed_local_trades = self
            .backfill_references
            .iter()
            .map(|reference| {
                let trade = VerifiedTradeWire::from_event(source(reference)?)?;
                Ok((trade.event_id.clone(), trade))
            })
            .collect::<Result<BTreeMap<_, _>, RecoveryOutcomeError>>()?;
        let witness_local_trades = result
            .local_trades
            .iter()
            .cloned()
            .map(|trade| (trade.event_id.clone(), trade))
            .collect::<BTreeMap<_, _>>();
        if observed_local_trades.len() != self.backfill_references.len()
            || witness_local_trades.len() != result.local_trades.len()
            || observed_local_trades != witness_local_trades
        {
            return Err(RecoveryOutcomeError::InvalidOutcome);
        }
        let observed_official_candles = self
            .official_candle_references
            .iter()
            .map(|reference| {
                let candle = ReconciledCandleWire::from_completed_event(source(reference)?)?;
                Ok(((candle.interval.clone(), candle.open_time_ns), candle))
            })
            .collect::<Result<BTreeMap<_, _>, RecoveryOutcomeError>>()?;
        let witness_official_candles = result
            .official_candles
            .iter()
            .cloned()
            .map(|candle| ((candle.interval.clone(), candle.open_time_ns), candle))
            .collect::<BTreeMap<_, _>>();
        if observed_official_candles.len() != self.official_candle_references.len()
            || witness_official_candles.len() != result.official_candles.len()
            || observed_official_candles != witness_official_candles
        {
            return Err(RecoveryOutcomeError::InvalidOutcome);
        }
        let derived_candles = result
            .derived_candles
            .iter()
            .cloned()
            .map(|candle| ((candle.interval.clone(), candle.open_time_ns), candle))
            .collect::<BTreeMap<_, _>>();
        if derived_candles.len() != result.derived_candles.len()
            || !derived_candles
                .iter()
                .all(|(key, candle)| witness_official_candles.get(key) == Some(candle))
        {
            return Err(RecoveryOutcomeError::InvalidOutcome);
        }
        Ok(())
    }
}

/// Private immutable companion-member store rooted by a verified Parquet store.
#[derive(Debug, Clone)]
pub struct RecoveryOutcomeStore {
    #[cfg(unix)]
    directory: Arc<File>,
    provenance: DataProvenance,
}

impl RecoveryOutcomeStore {
    /// Opens the trusted companion-member directory owned by one Parquet store.
    pub fn open(store: &ParquetStore) -> Result<Self, RecoveryOutcomeError> {
        Ok(Self {
            #[cfg(unix)]
            directory: Arc::new(store.recovery_outcomes_descriptor()?),
            provenance: store.provenance().clone(),
        })
    }

    /// Binds and atomically publishes a coordinator-minted reconciled recovery witness.
    #[expect(
        clippy::too_many_arguments,
        reason = "publication keeps every external witness and raw-reference input explicit"
    )]
    pub fn publish_verified(
        &self,
        result: &HyperliquidRecoveryResult,
        predecessor: Option<RecoverySourceReference>,
        trade_predecessor: RecoverySourceReference,
        recovery_anchor: RecoverySourceReference,
        backfill_references: Vec<RecoverySourceReference>,
        official_candle_references: Vec<RecoverySourceReference>,
        availability_anchor: RecoverySourceReference,
        raw_proof: &BTreeMap<RecoverySourceReference, MarketEvent>,
    ) -> Result<RecoveryOutcomeLocator, RecoveryOutcomeError> {
        let outcome = ReconciledRecoveryOutcome::from_verified_result(
            result,
            predecessor,
            trade_predecessor,
            recovery_anchor,
            backfill_references,
            official_candle_references,
            availability_anchor,
            raw_proof,
        )?;
        self.publish(&outcome)
    }

    /// Atomically publishes an explicit unavailable outcome that cannot release entries.
    #[expect(
        clippy::too_many_arguments,
        reason = "unavailable publication keeps every immutable request fact explicit"
    )]
    pub fn publish_unavailable(
        &self,
        request_id: impl Into<String>,
        generation: u64,
        market: Market,
        request_cursors: RecoveryRequestCursors,
        source: RecoveryOutcomeSource,
        completed_through: TimestampNs,
        recovery_anchor: RecoverySourceReference,
        availability_anchor: RecoverySourceReference,
    ) -> Result<RecoveryOutcomeLocator, RecoveryOutcomeError> {
        let outcome = ReconciledRecoveryOutcome::unavailable(
            request_id,
            generation,
            market,
            request_cursors,
            source,
            completed_through,
            recovery_anchor,
            availability_anchor,
        )?;
        self.publish(&outcome)
    }

    fn publish(
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
    verified_result: Option<VerifiedRecoveryResultWire>,
    result_digest: String,
}

#[derive(Debug, Clone, Serialize)]
struct UnavailableRecoveryResultWire {
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
struct VerifiedRecoveryResultWire {
    version: u8,
    upstream_commitment: String,
    generation: u64,
    market: String,
    reason: String,
    reconnect_attempt: u32,
    predecessor_event_time_ns: Option<i64>,
    predecessor_received_at_ns: Option<i64>,
    trade_predecessor_event_time_ns: i64,
    trade_predecessor_received_at_ns: i64,
    trade_predecessor_event_id: String,
    snapshot_event_time_ns: i64,
    snapshot_received_at_ns: i64,
    snapshot_event_id: String,
    completed_through_ns: i64,
    local_trades: Vec<VerifiedTradeWire>,
    official_candles: Vec<ReconciledCandleWire>,
    derived_candles: Vec<ReconciledCandleWire>,
}

impl VerifiedRecoveryResultWire {
    fn from_witness(witness: &VerifiedRecoveryWitness) -> Result<Self, RecoveryOutcomeError> {
        let request = witness.request();
        let (
            Some(trade_predecessor_event_time),
            Some(trade_predecessor_received_at),
            Some(trade_predecessor_event_id),
        ) = (
            request.trade_predecessor_event_time(),
            request.trade_predecessor_received_at(),
            request.trade_predecessor_event_id(),
        )
        else {
            return Err(RecoveryOutcomeError::InvalidOutcome);
        };
        Ok(Self {
            version: WITNESS_VERSION,
            upstream_commitment: witness.commitment().to_owned(),
            generation: request.generation(),
            market: request.market().as_str().to_owned(),
            reason: gap_reason_name(request.reason()).to_owned(),
            reconnect_attempt: request.reconnect_attempt(),
            predecessor_event_time_ns: request.predecessor_event_time().map(TimestampNs::value),
            predecessor_received_at_ns: request.predecessor_received_at().map(TimestampNs::value),
            trade_predecessor_event_time_ns: trade_predecessor_event_time.value(),
            trade_predecessor_received_at_ns: trade_predecessor_received_at.value(),
            trade_predecessor_event_id: trade_predecessor_event_id.as_str().to_owned(),
            snapshot_event_time_ns: request.snapshot_event_time().value(),
            snapshot_received_at_ns: request.snapshot_received_at().value(),
            snapshot_event_id: request.snapshot_event_id().as_str().to_owned(),
            completed_through_ns: witness.completed_through().value(),
            local_trades: witness
                .local_trades()
                .iter()
                .map(VerifiedTradeWire::from_event)
                .collect::<Result<Vec<_>, _>>()?,
            official_candles: witness
                .official_candles()
                .iter()
                .map(ReconciledCandleWire::from_official)
                .collect::<Result<Vec<_>, _>>()?,
            derived_candles: witness
                .derived_candles()
                .iter()
                .map(ReconciledCandleWire::from_derived)
                .collect(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct VerifiedTradeWire {
    event_id: String,
    event_time_ns: i64,
    received_at_ns: i64,
    market: String,
    trade_id: u64,
    side: String,
    price: String,
    quantity: String,
}

impl VerifiedTradeWire {
    fn from_event(event: &MarketEvent) -> Result<Self, RecoveryOutcomeError> {
        let MarketEventKind::Trade(trade) = event.kind() else {
            return Err(RecoveryOutcomeError::InvalidOutcome);
        };
        Ok(Self {
            event_id: event.event_id().as_str().to_owned(),
            event_time_ns: event.event_time().value(),
            received_at_ns: event.received_at().value(),
            market: event.market().as_str().to_owned(),
            trade_id: trade.trade_id(),
            side: match trade.side() {
                trench_core::domain::Side::Buy => "buy".to_owned(),
                trench_core::domain::Side::Sell => "sell".to_owned(),
            },
            price: trade.price().value().to_string(),
            quantity: trade.quantity().value().to_string(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReconciledCandleWire {
    market: String,
    interval: String,
    open_time_ns: i64,
    open: String,
    high: String,
    low: String,
    close: String,
    volume: String,
    trade_count: u64,
}

impl ReconciledCandleWire {
    fn from_official(candle: &trench_hyperliquid::Candle) -> Result<Self, RecoveryOutcomeError> {
        let open_time_ns = candle
            .open_time_ms()
            .checked_mul(1_000_000)
            .ok_or(RecoveryOutcomeError::InvalidOutcome)?;
        Ok(Self {
            market: candle.market().as_str().to_owned(),
            interval: venue_interval_name(candle.interval()).to_owned(),
            open_time_ns,
            open: candle.open().value().to_string(),
            high: candle.high().value().to_string(),
            low: candle.low().value().to_string(),
            close: candle.close().value().to_string(),
            volume: candle.volume().value().to_string(),
            trade_count: candle.trade_count(),
        })
    }

    fn from_derived(candle: &trench_core::candle::Candle) -> Self {
        let market = candle.market().as_str().to_owned();
        let candle = candle.candle();
        Self {
            market,
            interval: core_interval_name(candle.interval()).to_owned(),
            open_time_ns: candle.open_time().value(),
            open: candle.open().value().to_string(),
            high: candle.high().value().to_string(),
            low: candle.low().value().to_string(),
            close: candle.close().value().to_string(),
            volume: candle.volume().value().to_string(),
            trade_count: candle.trade_count(),
        }
    }

    fn from_completed_event(event: &MarketEvent) -> Result<Self, RecoveryOutcomeError> {
        let MarketEventKind::CompletedCandle(candle) = event.kind() else {
            return Err(RecoveryOutcomeError::InvalidOutcome);
        };
        Ok(Self {
            market: event.market().as_str().to_owned(),
            interval: core_interval_name(candle.interval()).to_owned(),
            open_time_ns: candle.open_time().value(),
            open: candle.open().value().to_string(),
            high: candle.high().value().to_string(),
            low: candle.low().value().to_string(),
            close: candle.close().value().to_string(),
            volume: candle.volume().value().to_string(),
            trade_count: candle.trade_count(),
        })
    }
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

#[cfg(unix)]
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

fn referenced_event<'a>(
    observed: &'a BTreeMap<RecoverySourceReference, MarketEvent>,
    reference: &RecoverySourceReference,
) -> Result<&'a MarketEvent, RecoveryOutcomeError> {
    let event = observed
        .get(reference)
        .ok_or(RecoveryOutcomeError::InvalidOutcome)?;
    if AvailabilityKey::new(
        event.received_at(),
        event.event_time(),
        event.event_id().clone(),
    )
    .map_err(|_| RecoveryOutcomeError::InvalidOutcome)?
        != reference.key().clone()
    {
        return Err(RecoveryOutcomeError::InvalidOutcome);
    }
    Ok(event)
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

#[cfg(unix)]
fn parse_status(value: &str) -> Result<RecoveryOutcomeStatus, RecoveryOutcomeError> {
    match value {
        "reconciled" => Ok(RecoveryOutcomeStatus::Reconciled),
        "unavailable" => Ok(RecoveryOutcomeStatus::Unavailable),
        _ => Err(RecoveryOutcomeError::InvalidOutcome),
    }
}

#[cfg(unix)]
fn parse_source(value: &str) -> Result<RecoveryOutcomeSource, RecoveryOutcomeError> {
    match value {
        "captured_trades" => Ok(RecoveryOutcomeSource::CapturedTrades),
        "archive_l2" => Ok(RecoveryOutcomeSource::ArchiveL2),
        "unavailable" => Ok(RecoveryOutcomeSource::Unavailable),
        _ => Err(RecoveryOutcomeError::InvalidOutcome),
    }
}

const fn venue_interval_name(interval: trench_hyperliquid::CandleInterval) -> &'static str {
    match interval {
        trench_hyperliquid::CandleInterval::FifteenMinutes => "15m",
        trench_hyperliquid::CandleInterval::OneHour => "1h",
    }
}

const fn core_interval_name(interval: CandleInterval) -> &'static str {
    match interval {
        CandleInterval::FifteenMinutes => "15m",
        CandleInterval::OneHour => "1h",
    }
}

const fn gap_reason_name(reason: trench_hyperliquid::GapReason) -> &'static str {
    match reason {
        trench_hyperliquid::GapReason::TransportClosed => "transport_closed",
        trench_hyperliquid::GapReason::TransportError => "transport_error",
        trench_hyperliquid::GapReason::ReadTimeout => "read_timeout",
        trench_hyperliquid::GapReason::SnapshotRecoveryTimeout => "snapshot_recovery_timeout",
    }
}

fn is_gap_reason(reason: &str) -> bool {
    matches!(
        reason,
        "transport_closed" | "transport_error" | "read_timeout" | "snapshot_recovery_timeout"
    )
}

fn is_interval(interval: &str) -> bool {
    matches!(interval, "15m" | "1h")
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

#[cfg(unix)]
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

#[cfg(unix)]
fn decode_outcome(bytes: &[u8]) -> Result<ReconciledRecoveryOutcome, RecoveryOutcomeError> {
    let wire = serde_json::from_slice::<RecoveryOutcomeWire>(bytes)?;
    if canonical_bytes(&wire)? != bytes {
        return Err(RecoveryOutcomeError::InvalidOutcome);
    }
    ReconciledRecoveryOutcome::from_wire(wire)
}

#[cfg(unix)]
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

#[cfg(unix)]
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
