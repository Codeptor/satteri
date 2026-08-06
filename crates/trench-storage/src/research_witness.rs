//! Deterministic population of typed research witnesses from a verified source run.
//!
//! This module is the bridge between the availability-ordered source compiler and
//! the domain engines. It accepts only checked raw universe inputs, recomputes
//! features from normalized events, and reopens canonical risk-policy artifacts.
//! No derived snapshot is accepted from an unverified caller and no engine state
//! is mutated here.

use std::collections::{BTreeMap, BTreeSet};

use thiserror::Error;
use trench_core::{
    book::OrderBook,
    domain::{EventId, Market, Sleeve},
    event::{CandleInterval, DurationNs, MarketEvent, MarketEventKind, TimestampNs},
    features::common::{CommonFeatureEngine, FeatureSnapshot, LongHorizonFeatureHistory},
    risk::sizing::{RiskPolicy, RiskPolicyWireError},
    universe::{UniverseActivation, UniverseSelector, UniverseSnapshot},
};

use crate::{
    feature_replay::{FeatureInputWitness, FeatureReplayError},
    research_runs::{AvailabilitySourceReference, ResearchRunError, VerifiedResearchSourcePlan},
    research_sidecar::{
        AvailabilityCutoff, FeatureWitness, RawRiskWitness, UniverseCandidateInput, UniverseWitness,
    },
};
use trench_core::validation::TimeRange;

const MAX_WITNESS_EVENTS: usize = 1_000_000;
const MAX_UNIVERSE_CANDIDATES: usize = 4_096;

/// A raw universe candidate coupled to the exact availability facts used to derive it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UniverseCandidateEvidence {
    input: UniverseCandidateInput,
    source_references: Vec<AvailabilitySourceReference>,
}

impl UniverseCandidateEvidence {
    /// Creates one source-bound raw candidate contract.
    pub fn new(
        input: UniverseCandidateInput,
        mut source_references: Vec<AvailabilitySourceReference>,
    ) -> Result<Self, ResearchWitnessError> {
        source_references.sort();
        if source_references.is_empty() || source_references.len() > MAX_WITNESS_EVENTS {
            return Err(ResearchWitnessError::ResourceLimit);
        }
        ensure_strict_references(&source_references)?;
        Ok(Self {
            input,
            source_references,
        })
    }

    /// Returns the checked raw selector input.
    #[must_use]
    pub const fn input(&self) -> &UniverseCandidateInput {
        &self.input
    }

    /// Returns the exact source coordinates used by the candidate.
    #[must_use]
    pub fn source_references(&self) -> &[AvailabilitySourceReference] {
        &self.source_references
    }
}

/// Recomputed universe output and its source-bound candidate contracts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompiledUniverse {
    snapshot: UniverseSnapshot,
    activation: UniverseActivation,
    candidates: Vec<UniverseCandidateEvidence>,
}

impl CompiledUniverse {
    /// Returns the recomputed immutable hourly selector snapshot.
    #[must_use]
    pub const fn snapshot(&self) -> &UniverseSnapshot {
        &self.snapshot
    }

    /// Returns the selector-issued activation for the requested decision bar.
    #[must_use]
    pub const fn activation(&self) -> &UniverseActivation {
        &self.activation
    }

    /// Returns all raw candidate contracts used by the selector.
    #[must_use]
    pub fn candidates(&self) -> &[UniverseCandidateEvidence] {
        &self.candidates
    }

    /// Materializes the recomputed universe contract into a typed sidecar record.
    pub fn sidecar_witness(
        &self,
        record_id: impl Into<String>,
        cutoff: AvailabilityCutoff,
        source_range: TimeRange,
    ) -> Result<UniverseWitness, crate::research_sidecar::ResearchSidecarError> {
        let mut references = self
            .candidates
            .iter()
            .flat_map(|candidate| candidate.source_references().iter().cloned())
            .collect::<Vec<_>>();
        references.sort();
        references.dedup();
        let inputs = self
            .candidates
            .iter()
            .map(|candidate| candidate.input().clone())
            .collect::<Vec<_>>();
        UniverseWitness::new_with_inputs(
            record_id,
            self.snapshot.as_of_time(),
            cutoff,
            source_range,
            references,
            inputs,
            wire_digest(self.snapshot.digest())?,
            wire_digest(&self.activation.commitment_digest())?,
        )
    }
}

/// Recomputes one universe snapshot and activation from source-bound candidates.
pub fn compile_universe(
    source_plan: &VerifiedResearchSourcePlan,
    snapshot_time: TimestampNs,
    decision_time: TimestampNs,
    current: Option<&UniverseActivation>,
    candidates: Vec<UniverseCandidateEvidence>,
) -> Result<CompiledUniverse, ResearchWitnessError> {
    if candidates.is_empty() || candidates.len() > MAX_UNIVERSE_CANDIDATES {
        return Err(ResearchWitnessError::ResourceLimit);
    }

    let mut markets = BTreeSet::new();
    let mut checked_candidates = Vec::with_capacity(candidates.len());
    for candidate in candidates {
        let market = Market::new(candidate.input().market().to_owned())?;
        if !markets.insert(market.clone()) {
            return Err(ResearchWitnessError::DuplicateMarket { market });
        }
        source_plan.validate_source_references(candidate.source_references())?;
        verify_candidate_sources(source_plan, &candidate, snapshot_time)?;
        checked_candidates.push(candidate);
    }

    let domain_candidates = checked_candidates
        .iter()
        .map(|candidate| candidate.input().to_candidate())
        .collect::<Result<Vec<_>, _>>()?;
    let snapshot = UniverseSelector::select(snapshot_time, domain_candidates)?;
    let activation = UniverseSelector::activate(&snapshot, current, decision_time)?;
    Ok(CompiledUniverse {
        snapshot,
        activation,
        candidates: checked_candidates,
    })
}

fn verify_candidate_sources(
    source_plan: &VerifiedResearchSourcePlan,
    candidate: &UniverseCandidateEvidence,
    snapshot_time: TimestampNs,
) -> Result<(), ResearchWitnessError> {
    let mut found = BTreeMap::new();
    let mut has_metadata = false;
    let mut has_context = false;
    let mut has_book_or_bbo = false;
    let requested = candidate
        .source_references()
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    for record in source_plan.availability_run().records() {
        let record = record?;
        let reference = record.source_reference();
        if !requested.contains(&reference) {
            continue;
        }
        if record.event().received_at() > snapshot_time
            || record.event().event_time() > snapshot_time
        {
            return Err(ResearchWitnessError::LateSource);
        }
        if record.event().market().as_str() != candidate.input().market() {
            return Err(ResearchWitnessError::MarketMismatch);
        }
        match record.event().kind() {
            MarketEventKind::Metadata(_) => has_metadata = true,
            MarketEventKind::AssetContext(_) => has_context = true,
            MarketEventKind::BookSnapshot(_) | MarketEventKind::Bbo(_) => {
                has_book_or_bbo = true;
            }
            _ => {}
        }
        found.insert(reference, record.event().clone());
    }
    if found.len() != requested.len() {
        return Err(ResearchWitnessError::MissingSourceReference);
    }
    if !has_metadata || !has_context || !has_book_or_bbo {
        return Err(ResearchWitnessError::IncompleteUniverseSources);
    }
    Ok(())
}

/// Recomputed feature output and its source-bound witness.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompiledFeature {
    snapshot: FeatureSnapshot,
    long_history: LongHorizonFeatureHistory,
    witness: FeatureInputWitness,
}

impl CompiledFeature {
    /// Returns the recomputed common feature snapshot.
    #[must_use]
    pub const fn snapshot(&self) -> &FeatureSnapshot {
        &self.snapshot
    }

    /// Returns the recomputed long-horizon rules history.
    #[must_use]
    pub const fn long_history(&self) -> &LongHorizonFeatureHistory {
        &self.long_history
    }

    /// Returns the source-bound feature input witness.
    #[must_use]
    pub const fn witness(&self) -> &FeatureInputWitness {
        &self.witness
    }

    /// Materializes the recomputed feature contract into a typed sidecar record.
    pub fn sidecar_witness(
        &self,
        record_id: impl Into<String>,
        cutoff: AvailabilityCutoff,
        source_range: TimeRange,
    ) -> Result<FeatureWitness, crate::research_sidecar::ResearchSidecarError> {
        FeatureWitness::new_with_input_witness(
            record_id,
            EventId::new(self.witness.decision_event_id().to_owned()).map_err(|_| {
                crate::research_sidecar::ResearchSidecarError::InvalidSidecar {
                    reason: "feature decision identifier is invalid",
                }
            })?,
            TimestampNs::new(i128::from(self.witness.decision_at_ns())).map_err(|_| {
                crate::research_sidecar::ResearchSidecarError::InvalidSidecar {
                    reason: "feature decision timestamp is invalid",
                }
            })?,
            cutoff,
            source_range,
            self.witness.clone(),
            wire_digest(self.snapshot.snapshot_hash())?,
            wire_digest(self.long_history.input_digest())?,
        )
    }
}

/// Replays the verified source run through the production feature engine.
pub fn compile_feature(
    source_plan: &VerifiedResearchSourcePlan,
    decision_event_id: EventId,
    activation: &UniverseActivation,
) -> Result<CompiledFeature, ResearchWitnessError> {
    let records = source_plan
        .availability_run()
        .records()
        .collect::<Result<Vec<_>, _>>()?;
    if records.len() > MAX_WITNESS_EVENTS {
        return Err(ResearchWitnessError::ResourceLimit);
    }
    let decision = records
        .iter()
        .find(|record| record.event().event_id() == &decision_event_id)
        .ok_or(ResearchWitnessError::MissingDecision)?
        .event();
    let MarketEventKind::CompletedCandle(candle) = decision.kind() else {
        return Err(ResearchWitnessError::DecisionNotCandle);
    };
    let sleeve = match candle.interval() {
        CandleInterval::FifteenMinutes => Sleeve::FifteenMinute,
        CandleInterval::OneHour => Sleeve::OneHour,
    };

    let mut engine = CommonFeatureEngine::new();
    for record in &records {
        if record.event().received_at() <= decision.event_time()
            && record.event().event_time() <= decision.event_time()
        {
            engine.observe(record.event())?;
        }
    }
    let interval = candle.interval();
    let snapshot = engine
        .snapshots_at_with_activation(interval, decision.event_time(), Some(activation))
        .into_iter()
        .find(|snapshot| snapshot.market() == decision.market())
        .ok_or(ResearchWitnessError::MissingFeatureSnapshot)?;
    let long_history = engine.require_long_horizon_history_at(
        decision.market(),
        interval,
        decision.event_time(),
    )?;

    let mut input_references = records
        .iter()
        .filter(|record| {
            record.event().received_at() <= decision.event_time()
                && record.event().event_time() <= decision.event_time()
        })
        .map(|record| record.source_reference())
        .collect::<Vec<_>>();
    input_references.sort();
    input_references.dedup();
    let input_range_digest = snapshot
        .input_range()
        .map_or_else(digest_none, |range| range.digest().to_owned());
    let witness = FeatureInputWitness::new(
        decision_event_id,
        decision.market().clone(),
        sleeve,
        decision.event_time(),
        activation.commitment_digest(),
        snapshot.schema_hash(),
        input_range_digest,
        long_history.input_digest(),
        input_references,
    )?;
    Ok(CompiledFeature {
        snapshot,
        long_history,
        witness,
    })
}

fn digest_none() -> String {
    "0".repeat(64)
}

fn wire_digest(core_digest: &str) -> Result<String, crate::research_sidecar::ResearchSidecarError> {
    if core_digest.len() != blake3::OUT_LEN * 2
        || !core_digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(
            crate::research_sidecar::ResearchSidecarError::InvalidSidecar {
                reason: "core digest is not canonical lowercase BLAKE3",
            },
        );
    }
    Ok(format!("b3:{core_digest}"))
}

/// Reopened risk policy and the exact source references that made it executable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompiledRiskPolicy {
    policy: RiskPolicy,
    canonical_json: Vec<u8>,
    source_references: Vec<AvailabilitySourceReference>,
}

impl CompiledRiskPolicy {
    /// Returns the checked frozen policy.
    #[must_use]
    pub const fn policy(&self) -> &RiskPolicy {
        &self.policy
    }

    /// Returns the canonical policy artifact bytes.
    #[must_use]
    pub fn canonical_json(&self) -> &[u8] {
        &self.canonical_json
    }

    /// Returns the exact source coordinates used by the policy.
    #[must_use]
    pub fn source_references(&self) -> &[AvailabilitySourceReference] {
        &self.source_references
    }

    /// Materializes the frozen policy and its source partitions into a sidecar record.
    #[allow(clippy::too_many_arguments)]
    pub fn sidecar_witness(
        &self,
        record_id: impl Into<String>,
        decision_id: EventId,
        decision_at: TimestampNs,
        cutoff: AvailabilityCutoff,
        source_range: TimeRange,
        venue_constraint_references: Vec<AvailabilitySourceReference>,
        book_reference: AvailabilitySourceReference,
        impact_references: Vec<AvailabilitySourceReference>,
        funding_references: Vec<AvailabilitySourceReference>,
    ) -> Result<RawRiskWitness, crate::research_sidecar::ResearchSidecarError> {
        let mut references = venue_constraint_references.clone();
        references.push(book_reference.clone());
        references.extend(impact_references.iter().cloned());
        references.extend(funding_references.iter().cloned());
        references.sort();
        references.dedup();
        if references != self.source_references {
            return Err(
                crate::research_sidecar::ResearchSidecarError::InvalidSidecar {
                    reason: "risk witness references do not match the compiled source run",
                },
            );
        }
        let expected_policy_digest = self.policy.canonical_digest().map_err(|_| {
            crate::research_sidecar::ResearchSidecarError::InvalidSidecar {
                reason: "compiled risk policy digest is unavailable",
            }
        })?;
        RawRiskWitness::new(
            record_id,
            decision_id,
            decision_at,
            cutoff,
            source_range,
            venue_constraint_references,
            book_reference,
            impact_references,
            funding_references,
            format!("b3:{expected_policy_digest}"),
        )
    }
}

/// Reopens a canonical policy and proves its source book is timely and exact.
pub fn compile_risk_policy(
    source_plan: &VerifiedResearchSourcePlan,
    policy_json: &[u8],
    market: &Market,
    decision_time: TimestampNs,
    source_references: Vec<AvailabilitySourceReference>,
) -> Result<CompiledRiskPolicy, ResearchWitnessError> {
    let policy = RiskPolicy::from_canonical_json(policy_json)?;
    let mut source_references = source_references;
    source_references.sort();
    source_references.dedup();
    ensure_strict_references(&source_references)?;
    source_plan.validate_source_references(&source_references)?;
    let records = source_plan
        .availability_run()
        .records()
        .collect::<Result<Vec<_>, _>>()?;
    let mut books = Vec::<MarketEvent>::new();
    for record in records {
        if source_references
            .binary_search(&record.source_reference())
            .is_err()
        {
            continue;
        }
        if record.event().market() != market
            || record.event().received_at() > decision_time
            || record.event().event_time() > decision_time
        {
            return Err(ResearchWitnessError::InvalidRiskSource);
        }
        if matches!(record.event().kind(), MarketEventKind::BookSnapshot(_)) {
            books.push(record.event().clone());
        }
    }
    let book = books.last().ok_or(ResearchWitnessError::MissingRiskBook)?;
    let executable = OrderBook::apply_snapshot(
        None,
        book,
        DurationNs::new(i64::MAX as i128).map_err(|_| ResearchWitnessError::Arithmetic)?,
    )?;
    if !policy.matches_book_digest(&executable.commitment_digest()) {
        return Err(ResearchWitnessError::RiskBookMismatch);
    }
    Ok(CompiledRiskPolicy {
        policy,
        canonical_json: policy_json.to_vec(),
        source_references,
    })
}

fn ensure_strict_references(
    references: &[AvailabilitySourceReference],
) -> Result<(), ResearchWitnessError> {
    if references.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(ResearchWitnessError::InvalidReferences);
    }
    Ok(())
}

/// Automatic source-run witness population failure.
#[derive(Debug, Error)]
pub enum ResearchWitnessError {
    #[error(transparent)]
    ResearchRun(#[from] ResearchRunError),
    #[error(transparent)]
    Feature(#[from] trench_core::features::common::FeatureError),
    #[error(transparent)]
    FeatureReplay(#[from] FeatureReplayError),
    #[error(transparent)]
    UniverseInput(#[from] crate::research_sidecar::UniverseCandidateInputError),
    #[error(transparent)]
    Universe(#[from] trench_core::universe::UniverseError),
    #[error(transparent)]
    Domain(#[from] trench_core::domain::DomainError),
    #[error(transparent)]
    Event(#[from] trench_core::event::EventError),
    #[error(transparent)]
    Book(#[from] trench_core::book::BookError),
    #[error(transparent)]
    RiskPolicy(#[from] RiskPolicyWireError),
    #[error("research witness source references are not strictly ordered")]
    InvalidReferences,
    #[error("research witness resource limit exceeded")]
    ResourceLimit,
    #[error("duplicate universe market {market:?}")]
    DuplicateMarket { market: Market },
    #[error("research witness source arrived after its decision boundary")]
    LateSource,
    #[error("research witness source market does not match its candidate")]
    MarketMismatch,
    #[error("research witness source reference is missing from the verified run")]
    MissingSourceReference,
    #[error("universe candidate is missing metadata, context, or executable-book source facts")]
    IncompleteUniverseSources,
    #[error("decision event is missing from the verified run")]
    MissingDecision,
    #[error("decision event is not a completed candle")]
    DecisionNotCandle,
    #[error("feature snapshot is unavailable at the decision boundary")]
    MissingFeatureSnapshot,
    #[error("risk source is not a timely book for the requested market")]
    InvalidRiskSource,
    #[error("risk source does not contain a full-depth book")]
    MissingRiskBook,
    #[error("risk policy is bound to a different executable book")]
    RiskBookMismatch,
    #[error("checked timestamp arithmetic failed")]
    Arithmetic,
}
