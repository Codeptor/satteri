//! Availability-ordered causal fencing for offline research evidence.
//!
//! This module deliberately consumes only a verified final availability run and
//! its descriptor-bound recovery companions. It never accepts recovery state
//! from a caller, opens SQLite, invokes an Engine, or makes a strategy decision.

use std::collections::{BTreeMap, BTreeSet};

use thiserror::Error;
use trench_core::{
    domain::{EventId, Market},
    event::{MarketEvent, MarketEventKind, TimestampNs},
    validation::TimeRange,
};

use crate::{
    recovery_outcomes::{
        MAX_TOTAL_RECOVERY_PROOF_REFERENCES, ReconciledRecoveryOutcome, RecoveryOutcomeError,
        RecoveryOutcomeStatus, RecoverySourceReference,
    },
    research_runs::{
        AvailabilityKey, AvailabilitySourceReference, ResearchRunError, VerifiedResearchSourcePlan,
    },
    research_sidecar::{
        AvailabilityCutoff, DecisionIndexShard, ExcludedGap, ExclusionReason, RecoverySource,
        RecoveryStatus, RecoveryWitness, ResearchSidecarError, ResearchSidecarWriter, WitnessShard,
    },
};

const MAX_RECOVERY_OUTCOMES: usize = 4_096;
const MAX_TRACKED_MARKETS: usize = 4_096;
const MAX_COMPILED_DECISIONS: usize = 65_536;
const MAX_COMPILED_EXCLUSIONS: usize = 16_384;
const MAX_COMPILED_RECOVERY_WITNESSES: usize = 65_536;
const MAX_EXECUTABLE_BOOK_AGE_NS: i64 = 1_000_000_000;

/// Closed reasons for a causal exclusion produced directly from verified facts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ResearchExclusionReason {
    /// A source fact arrived after the decision boundary it would otherwise affect.
    LateSource,
    /// A recovery companion has not released an executable post-recovery book.
    RecoveryFence,
}

/// One canonical source-time interval excluded before any later witness compiler runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResearchExclusion {
    range: TimeRange,
    reason: ResearchExclusionReason,
}

impl ResearchExclusion {
    /// Returns the exact half-open source-time interval that cannot be used.
    #[must_use]
    pub const fn range(&self) -> TimeRange {
        self.range
    }

    /// Returns the immutable machine-readable reason for the exclusion.
    #[must_use]
    pub const fn reason(&self) -> ResearchExclusionReason {
        self.reason
    }
}

/// A timely completed-candle boundary admitted for a later typed-witness pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CausalDecisionBoundary {
    decision_id: EventId,
    decision_at: TimestampNs,
    availability_cutoff: AvailabilityCutoff,
    source_event_ids: Vec<EventId>,
}

impl CausalDecisionBoundary {
    /// Returns the completed candle's immutable normalized identity.
    #[must_use]
    pub fn decision_id(&self) -> &EventId {
        &self.decision_id
    }

    /// Returns the exact completed-candle close, never a later receipt time.
    #[must_use]
    pub const fn decision_at(&self) -> TimestampNs {
        self.decision_at
    }

    /// Returns the complete availability coordinate observed at the decision boundary.
    #[must_use]
    pub const fn availability_cutoff(&self) -> &AvailabilityCutoff {
        &self.availability_cutoff
    }

    /// Returns the timely source identities admitted by this causal fence.
    #[must_use]
    pub fn source_event_ids(&self) -> &[EventId] {
        &self.source_event_ids
    }
}

/// Result of one bounded causal source pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResearchCompileResult {
    decisions: Vec<CausalDecisionBoundary>,
    excluded_gaps: Vec<ResearchExclusion>,
    recovery_witnesses: Vec<RecoveryWitness>,
}

impl ResearchCompileResult {
    /// Returns timely completed-candle boundaries in verified availability order.
    #[must_use]
    pub fn decisions(&self) -> &[CausalDecisionBoundary] {
        &self.decisions
    }

    /// Returns canonical merged causal exclusions.
    #[must_use]
    pub fn excluded_gaps(&self) -> &[ResearchExclusion] {
        &self.excluded_gaps
    }

    /// Returns the raw reconciled recovery contracts admitted during this pass.
    #[must_use]
    pub fn recovery_witnesses(&self) -> &[RecoveryWitness] {
        &self.recovery_witnesses
    }

    /// Converts an excluded-only result into the typed immutable sidecar writer.
    ///
    /// A result containing timely boundaries needs the later recovery, universe,
    /// feature, and risk witness compilers before it can become a sidecar. This
    /// method therefore fails closed rather than publishing an unbound decision.
    pub fn excluded_sidecar_writer(
        &self,
        source_plan: &VerifiedResearchSourcePlan,
    ) -> Result<ResearchSidecarWriter, ResearchCompileError> {
        if !self.decisions.is_empty() {
            return Err(ResearchCompileError::IncompleteWitnesses);
        }
        self.recovery_sidecar_writer(source_plan)
    }

    /// Builds an excluded-only sidecar writer when no partial decision exists.
    ///
    /// Later universe, feature, and risk passes must bind a complete four-witness
    /// decision before any recovery witness can be published alongside it.
    pub fn recovery_sidecar_writer(
        &self,
        source_plan: &VerifiedResearchSourcePlan,
    ) -> Result<ResearchSidecarWriter, ResearchCompileError> {
        if !self.decisions.is_empty() || !self.recovery_witnesses.is_empty() {
            return Err(ResearchCompileError::IncompleteWitnesses);
        }
        let gaps = self
            .excluded_gaps
            .iter()
            .map(|gap| {
                ExcludedGap::new(
                    gap.range,
                    match gap.reason {
                        ResearchExclusionReason::LateSource => ExclusionReason::LateSource,
                        ResearchExclusionReason::RecoveryFence => ExclusionReason::InvalidWitness,
                    },
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(ResearchSidecarWriter::new(source_plan)?.with_excluded_gaps(gaps)?)
    }
}

/// Offline compiler that establishes the first causal receipt-time fence.
#[derive(Debug, Default, Clone)]
pub struct ResearchEvidenceCompiler;

impl ResearchEvidenceCompiler {
    /// Creates the deterministic offline causal compiler.
    #[must_use]
    pub fn new() -> Self {
        Self
    }

    /// Streams the verified availability run exactly once.
    ///
    /// A completed candle has one immutable decision time: its close. If its
    /// receipt is later than that close, it becomes an excluded range at that
    /// original interval; it is never moved forward to its receipt time.
    ///
    /// Recovery outcomes activate only when the cursor reaches their exact
    /// descriptor-bound raw availability anchor. Unavailable outcomes remain
    /// auditable in the source plan but quarantine new entries instead of
    /// releasing them.
    pub fn compile(
        &self,
        source_plan: &VerifiedResearchSourcePlan,
    ) -> Result<ResearchCompileResult, ResearchCompileError> {
        let recovery_outcomes = ordered_recovery_outcomes(source_plan)?;
        let mut required_references = BTreeSet::new();
        for outcome in &recovery_outcomes {
            for reference in outcome.source_references() {
                required_references.insert(reference.clone());
                if required_references.len() > MAX_TOTAL_RECOVERY_PROOF_REFERENCES {
                    return Err(ResearchCompileError::ResourceLimit);
                }
            }
        }
        let mut releases =
            BTreeMap::<RecoverySourceReference, Vec<&ReconciledRecoveryOutcome>>::new();
        for outcome in &recovery_outcomes {
            releases
                .entry(outcome.availability_anchor().clone())
                .or_default()
                .push(*outcome);
        }

        let mut observed_references = BTreeMap::<RecoverySourceReference, MarketEvent>::new();
        let mut latest_books = BTreeMap::<Market, BookObservation>::new();
        let mut active_recoveries = BTreeMap::<Market, ActiveRecovery<'_>>::new();
        let mut decisions = Vec::new();
        let mut excluded_gaps = Vec::new();
        let mut recovery_witnesses = Vec::new();

        for record in source_plan.availability_run().records() {
            let record = record?;
            let event = record.event();
            let reference = RecoverySourceReference::new(
                record.member_manifest_digest().to_owned(),
                record.key(),
            )?;
            if required_references.contains(&reference)
                && observed_references
                    .insert(reference.clone(), event.clone())
                    .is_some()
            {
                return Err(ResearchCompileError::InvalidRecoveryOutcome);
            }
            if let Some(outcomes) = releases.get(&reference) {
                for outcome in outcomes {
                    let active = match outcome.status() {
                        RecoveryOutcomeStatus::Reconciled => ActiveRecovery::Reconciled(outcome),
                        RecoveryOutcomeStatus::Unavailable => ActiveRecovery::Quarantined,
                    };
                    active_recoveries.insert(outcome.market().clone(), active);
                }
            }
            if matches!(event.kind(), MarketEventKind::BookSnapshot(_)) {
                if !latest_books.contains_key(event.market())
                    && latest_books.len() >= MAX_TRACKED_MARKETS
                {
                    return Err(ResearchCompileError::ResourceLimit);
                }
                latest_books.insert(
                    event.market().clone(),
                    BookObservation::from_record(&record),
                );
                continue;
            }
            let MarketEventKind::CompletedCandle(candle) = event.kind() else {
                continue;
            };
            let decision_at = event.event_time();
            let candle_range = TimeRange::new(candle.open_time(), decision_at)?;
            if event.received_at() > decision_at {
                push_bounded(
                    &mut excluded_gaps,
                    ResearchExclusion {
                        range: candle_range,
                        reason: ResearchExclusionReason::LateSource,
                    },
                    MAX_COMPILED_EXCLUSIONS,
                )?;
                continue;
            }

            let recovery = active_recoveries.get(event.market());
            let has_executable_book = match recovery {
                Some(ActiveRecovery::Reconciled(outcome)) => {
                    latest_books.get(event.market()).is_some_and(|book| {
                        book.key > *outcome.availability_anchor().key()
                            && book.event_time <= decision_at
                            && book.received_at <= decision_at
                            && decision_at
                                .value()
                                .checked_sub(book.event_time.value())
                                .is_some_and(|age| age <= MAX_EXECUTABLE_BOOK_AGE_NS)
                    })
                }
                Some(ActiveRecovery::Quarantined) => false,
                None => true,
            };
            if !has_executable_book {
                push_bounded(
                    &mut excluded_gaps,
                    ResearchExclusion {
                        range: candle_range,
                        reason: ResearchExclusionReason::RecoveryFence,
                    },
                    MAX_COMPILED_EXCLUSIONS,
                )?;
                continue;
            }
            if let Some(ActiveRecovery::Reconciled(outcome)) = recovery {
                let recovery_anchor = outcome.recovery_anchor();
                let key = record.key();
                push_bounded(
                    &mut recovery_witnesses,
                    RecoveryWitness::new(
                        format!("recovery-{}", event.event_id().as_str()),
                        AvailabilityCutoff::new(
                            key.received_at(),
                            key.event_time(),
                            key.event_id().clone(),
                        )?,
                        candle_range,
                        outcome.request_id(),
                        RecoveryStatus::Complete,
                        RecoverySource::Captured,
                        outcome.completed_through(),
                        availability_source_reference(recovery_anchor)?,
                        outcome
                            .backfill_references()
                            .iter()
                            .map(availability_source_reference)
                            .collect::<Result<Vec<_>, _>>()?,
                        outcome.result_digest(),
                    )?,
                    MAX_COMPILED_RECOVERY_WITNESSES,
                )?;
            }
            let key = record.key();
            push_bounded(
                &mut decisions,
                CausalDecisionBoundary {
                    decision_id: event.event_id().clone(),
                    decision_at,
                    availability_cutoff: AvailabilityCutoff::new(
                        key.received_at(),
                        key.event_time(),
                        key.event_id().clone(),
                    )?,
                    source_event_ids: vec![event.event_id().clone()],
                },
                MAX_COMPILED_DECISIONS,
            )?;
        }
        for outcome in recovery_outcomes {
            outcome.verify_result_from_raw(&observed_references)?;
        }
        Ok(ResearchCompileResult {
            decisions,
            excluded_gaps: normalize_exclusions(excluded_gaps)?,
            recovery_witnesses,
        })
    }

    /// Completes the causal pass with a source-bound typed witness set.
    ///
    /// This is the only bridge from the availability compiler to sidecar
    /// publication. It does not derive or trust any witness output: callers
    /// must provide records produced by the typed universe/feature/risk
    /// recomputation APIs, and every decision boundary must be represented
    /// exactly once. Publishing remains an explicit caller action and this
    /// method never changes daemon readiness or strategy configuration.
    pub fn complete_sidecar_writer(
        &self,
        source_plan: &VerifiedResearchSourcePlan,
        result: &ResearchCompileResult,
        witness_shards: Vec<WitnessShard>,
        decision_shards: Vec<DecisionIndexShard>,
    ) -> Result<ResearchSidecarWriter, ResearchCompileError> {
        if result.decisions.is_empty() {
            return Err(ResearchCompileError::IncompleteWitnesses);
        }
        if witness_shards.is_empty() || decision_shards.is_empty() {
            return Err(ResearchCompileError::IncompleteWitnesses);
        }

        let expected = result
            .decisions
            .iter()
            .map(|decision| {
                (
                    decision.decision_id().as_str().to_owned(),
                    decision.availability_cutoff().clone(),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let mut observed = BTreeSet::new();
        for decision in decision_shards.iter().flat_map(DecisionIndexShard::records) {
            if expected
                .get(decision.decision_id())
                .is_none_or(|cutoff| cutoff != decision.cutoff())
                || !observed.insert(decision.decision_id().to_owned())
            {
                return Err(ResearchCompileError::InvalidWitnessSet);
            }
            source_plan.validate_source_references(decision.input_references())?;
        }
        if observed.len() != expected.len() {
            return Err(ResearchCompileError::InvalidWitnessSet);
        }

        Ok(ResearchSidecarWriter::new(source_plan)?
            .with_witness_shards(witness_shards)?
            .with_decision_index_shards(decision_shards)?)
    }
}

fn availability_source_reference(
    reference: &RecoverySourceReference,
) -> Result<AvailabilitySourceReference, ResearchRunError> {
    let key = reference.key();
    AvailabilitySourceReference::new(
        reference.member_manifest_digest(),
        key.received_at(),
        key.event_time(),
        key.event_id().clone(),
    )
}

fn ordered_recovery_outcomes(
    source_plan: &VerifiedResearchSourcePlan,
) -> Result<Vec<&ReconciledRecoveryOutcome>, ResearchCompileError> {
    if source_plan.recovery_outcomes().len() > MAX_RECOVERY_OUTCOMES {
        return Err(ResearchCompileError::ResourceLimit);
    }
    let mut outcomes = source_plan.recovery_outcomes().iter().collect::<Vec<_>>();
    outcomes.sort_by(|left, right| {
        (
            left.availability_anchor().key(),
            left.market(),
            left.completed_through(),
        )
            .cmp(&(
                right.availability_anchor().key(),
                right.market(),
                right.completed_through(),
            ))
    });
    let mut prior_by_market = BTreeMap::<Market, (AvailabilityKey, TimestampNs)>::new();
    for outcome in &outcomes {
        if let Some((prior_anchor, prior_completed)) = prior_by_market.get(outcome.market())
            && (prior_anchor >= outcome.availability_anchor().key()
                || *prior_completed >= outcome.completed_through())
        {
            return Err(ResearchCompileError::InvalidRecoveryOutcome);
        }
        prior_by_market.insert(
            outcome.market().clone(),
            (
                outcome.availability_anchor().key().clone(),
                outcome.completed_through(),
            ),
        );
    }
    Ok(outcomes)
}

#[derive(Debug, Clone, Copy)]
enum ActiveRecovery<'a> {
    Reconciled(&'a ReconciledRecoveryOutcome),
    Quarantined,
}

#[derive(Debug, Clone)]
struct BookObservation {
    key: AvailabilityKey,
    event_time: TimestampNs,
    received_at: TimestampNs,
}

impl BookObservation {
    fn from_record(record: &crate::research_runs::AvailabilityRecord) -> Self {
        Self {
            key: record.key(),
            event_time: record.event().event_time(),
            received_at: record.event().received_at(),
        }
    }
}

fn push_bounded<T>(
    values: &mut Vec<T>,
    value: T,
    limit: usize,
) -> Result<(), ResearchCompileError> {
    if values.len() >= limit {
        return Err(ResearchCompileError::ResourceLimit);
    }
    values.push(value);
    Ok(())
}

fn normalize_exclusions(
    mut gaps: Vec<ResearchExclusion>,
) -> Result<Vec<ResearchExclusion>, ResearchCompileError> {
    gaps.sort_by_key(|gap| (gap.range.start(), gap.range.end(), gap.reason));
    let mut normalized: Vec<ResearchExclusion> = Vec::with_capacity(gaps.len());
    for gap in gaps {
        if let Some(previous) = normalized.last_mut() {
            if gap.range.start() < previous.range.end() {
                return Err(ResearchCompileError::OverlappingExclusions);
            }
            if gap.range.start() == previous.range.end() && gap.reason == previous.reason {
                previous.range = TimeRange::new(previous.range.start(), gap.range.end())?;
                continue;
            }
        }
        normalized.push(gap);
    }
    Ok(normalized)
}

/// Causal source compiler failure.
#[derive(Debug, Error)]
pub enum ResearchCompileError {
    /// The immutable availability run failed while being streamed.
    #[error(transparent)]
    Run(#[from] ResearchRunError),
    /// A descriptor-bound recovery companion or its raw proof is invalid.
    #[error(transparent)]
    RecoveryOutcome(#[from] RecoveryOutcomeError),
    /// A source interval could not be represented as a valid half-open range.
    #[error(transparent)]
    Time(#[from] trench_core::validation::ValidationError),
    /// The typed sidecar writer rejected an excluded-only sidecar contract.
    #[error(transparent)]
    Sidecar(#[from] ResearchSidecarError),
    /// A timely decision cannot be published before all four typed witnesses exist.
    #[error("timely decision boundaries require recovery, universe, feature, and risk witnesses")]
    IncompleteWitnesses,
    /// Causal exclusions overlap before canonical normalization.
    #[error("causal exclusions overlap")]
    OverlappingExclusions,
    /// A source-plan companion has non-monotonic release or completion ordering.
    #[error("recovery outcome ordering is invalid")]
    InvalidRecoveryOutcome,
    /// A bounded causal input exceeded its fixed resource limit.
    #[error("research compiler resource limit exceeded")]
    ResourceLimit,
    /// The typed sidecar decision indexes do not exactly cover the causal pass.
    #[error("typed sidecar witnesses do not exactly cover the causal decisions")]
    InvalidWitnessSet,
}
