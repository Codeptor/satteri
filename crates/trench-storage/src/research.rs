//! Authoritative rules research replay over a frozen normalized-event stream.
//!
//! This adapter deliberately owns neither an alternative execution model nor a
//! synthetic PnL calculator. It supplies the already-validated source facts to
//! `trench_core::engine::Engine` and derives every outcome from that engine's
//! immutable causal records.

use std::collections::{BTreeMap, BTreeSet};

use blake3::Hasher;
use rust_decimal::Decimal;
use trench_core::book::{BookError, OrderBook};
use trench_core::broker::{
    BrokerConfig, BrokerRecord, BrokerRunContext, BrokerState, ExitReason as BrokerExitReason,
    PaperBroker,
};
use trench_core::domain::{EventId, LedgerId, Market};
use trench_core::engine::{
    Engine, EngineContext, EngineEvent, EngineOutcome, EnginePersistenceKind, EngineRecord,
    EngineState, EntryCandidate, EventAdmission, SnapshotBindings, StrategyFingerprints,
};
use trench_core::event::{MarketEvent, MarketEventKind, TimestampNs};
use trench_core::features::common::{FeatureSnapshot, LongHorizonFeatureHistory};
use trench_core::ledger::LedgerState;
use trench_core::risk::sizing::RiskPolicy;
use trench_core::strategy::Strategy;
use trench_core::strategy::rules::{ExitReason as RuleExitReason, RulePosition, RulesStrategy};
use trench_core::universe::UniverseActivation;
use trench_core::validation::{
    EngineReplayOutcome, MissingReplayInput, ResearchProvenance, RuleGrid, RuleReplay,
    RuleReplayRequest, RulesArtifact, ValidationError,
};
use trench_hyperliquid::{RecoveryResult, RecoverySource, RecoveryStatus};

use crate::parquet::events_digest;
use crate::replay::DeterministicReplay;

const MAX_RESEARCH_RECOVERY_BOUNDARIES: usize = 100_000;
const MAX_RESEARCH_SOURCE_EVENTS: usize = 100_000;

/// Exact source facts that a replay sidecar consumed from one immutable
/// [`DeterministicReplay`]. The constructor verifies both identity and
/// canonical causal digest; an arbitrary list of IDs is not a usable sidecar.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResearchSourceEvidence {
    replay_digest: String,
    event_ids: Vec<EventId>,
    digest: String,
}

impl ResearchSourceEvidence {
    /// Binds a bounded, nonempty source set to one verified replay stream.
    pub fn from_replay(
        replay: &DeterministicReplay,
        event_ids: Vec<EventId>,
    ) -> Result<Self, ValidationError> {
        let event_ids = canonical_event_ids(event_ids)?;
        let events = source_events(replay, &event_ids)?;
        Ok(Self {
            replay_digest: replay.digest().to_owned(),
            event_ids,
            digest: events_digest(&events).map_err(engine_failure)?,
        })
    }

    fn verify(
        &self,
        replay: &DeterministicReplay,
        decision_at: Option<TimestampNs>,
    ) -> Result<Vec<MarketEvent>, ValidationError> {
        if self.replay_digest != replay.digest() {
            return Err(ValidationError::InvalidEngineOutcome);
        }
        let events = source_events(replay, &self.event_ids)?;
        if events_digest(&events).map_err(engine_failure)? != self.digest
            || decision_at.is_some_and(|at| {
                events
                    .iter()
                    .any(|event| event.event_time() > at || event.received_at() > at)
            })
        {
            return Err(ValidationError::InvalidEngineOutcome);
        }
        Ok(events)
    }

    fn contains(&self, event_id: &EventId) -> bool {
        self.event_ids.binary_search(event_id).is_ok()
    }
}

/// A persisted recovery completion derived only from an opaque reconciled
/// recovery result and evidence in the same immutable replay stream. A raw
/// source event can never unlock execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveryBoundary {
    event_id: EventId,
    at: TimestampNs,
    market: Market,
    generation: u64,
    evidence: ResearchSourceEvidence,
    snapshot_event_id: EventId,
}

impl RecoveryBoundary {
    /// Derives one execution boundary from a reconciled, queue-verified
    /// recovery result. `RecoveryResult` has no public constructor, so a
    /// caller cannot turn a selected snapshot into a recovery unlock.
    pub fn from_reconciled_recovery(
        replay: &DeterministicReplay,
        result: &RecoveryResult,
    ) -> Result<Self, ValidationError> {
        if !matches!(result.status(), RecoveryStatus::Reconciled { .. })
            || result.source() != RecoverySource::LocalTradesAndOfficialCandles
        {
            return Err(ValidationError::InvalidEngineOutcome);
        }
        let request = result.request();
        if request.generation() == 0 {
            return Err(ValidationError::InvalidEngineOutcome);
        }
        let mut evidence_ids = result
            .backfill_events()
            .iter()
            .map(|event| event.event_id().clone())
            .collect::<Vec<_>>();
        evidence_ids.push(request.snapshot_event_id().clone());
        let evidence = ResearchSourceEvidence::from_replay(replay, evidence_ids)?;
        let events = evidence.verify(replay, Some(result.completed_through()))?;
        let snapshot = events
            .iter()
            .find(|event| event.event_id() == request.snapshot_event_id())
            .ok_or(ValidationError::InvalidEngineOutcome)?;
        if snapshot.market() != request.market()
            || !matches!(snapshot.kind(), MarketEventKind::BookSnapshot(_))
        {
            return Err(ValidationError::InvalidEngineOutcome);
        }
        let event_id = recovery_event_id(
            request.market(),
            request.generation(),
            result.completed_through(),
            request.snapshot_event_id(),
        )?;
        Ok(Self {
            event_id,
            at: result.completed_through(),
            market: request.market().clone(),
            generation: request.generation(),
            evidence,
            snapshot_event_id: request.snapshot_event_id().clone(),
        })
    }

    /// Returns the source identity of this recovery completion.
    #[must_use]
    pub const fn event_id(&self) -> &EventId {
        &self.event_id
    }

    /// Returns the point after which a strictly later source book is executable.
    #[must_use]
    pub const fn at(&self) -> TimestampNs {
        self.at
    }

    /// Returns the recovered market.
    #[must_use]
    pub const fn market(&self) -> &Market {
        &self.market
    }
}

/// Immutable feature inputs for one completed-candle decision event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResearchFeatureFacts {
    snapshot: FeatureSnapshot,
    long_history: LongHorizonFeatureHistory,
    evidence: ResearchSourceEvidence,
}

impl ResearchFeatureFacts {
    /// Couples the exact common snapshot to its independently validated
    /// long-horizon history. Neither value is imputed by this adapter.
    #[must_use]
    pub const fn new(
        snapshot: FeatureSnapshot,
        long_history: LongHorizonFeatureHistory,
        evidence: ResearchSourceEvidence,
    ) -> Self {
        Self {
            snapshot,
            long_history,
            evidence,
        }
    }
}

/// A universe activation and its exact causal source evidence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResearchUniverseFacts {
    activation: UniverseActivation,
    evidence: ResearchSourceEvidence,
}

impl ResearchUniverseFacts {
    /// Couples a selector-issued activation to verified replay facts.
    #[must_use]
    pub const fn new(activation: UniverseActivation, evidence: ResearchSourceEvidence) -> Self {
        Self {
            activation,
            evidence,
        }
    }
}

/// Frozen risk policies and their config/source binding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResearchRiskPolicies {
    config_digest: String,
    policies: BTreeMap<Market, RiskPolicy>,
    evidence: ResearchSourceEvidence,
}

impl ResearchRiskPolicies {
    /// Couples market policies to one config commitment and verified replay
    /// facts. The adapter repeats both checks before an entry is admitted.
    pub fn new(
        config_digest: impl Into<String>,
        policies: BTreeMap<Market, RiskPolicy>,
        evidence: ResearchSourceEvidence,
    ) -> Self {
        Self {
            config_digest: config_digest.into(),
            policies,
            evidence,
        }
    }
}

/// Typed point-in-time facts required at a completed-candle decision boundary.
///
/// The maps are intentionally separate so callers cannot accidentally use a
/// feature, universe, or sizing policy from a different boundary. Missing maps
/// are reported in canonical protocol order before any engine transition runs.
#[derive(Debug, Default)]
pub struct ResearchFacts {
    features: BTreeMap<EventId, ResearchFeatureFacts>,
    universes: BTreeMap<EventId, ResearchUniverseFacts>,
    risk_policies: BTreeMap<EventId, ResearchRiskPolicies>,
}

impl ResearchFacts {
    /// Creates an empty fail-closed facts collection.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Inserts the exact feature snapshot and long-horizon history attached to
    /// one completed-candle event. Replacing a fact is rejected.
    pub fn insert_features(
        &mut self,
        event_id: EventId,
        facts: ResearchFeatureFacts,
    ) -> Result<(), ValidationError> {
        insert_once(
            &mut self.features,
            event_id,
            facts,
            MissingReplayInput::FeatureSnapshot,
        )
    }

    /// Inserts the exact activated universe attached to one decision event.
    pub fn insert_universe(
        &mut self,
        event_id: EventId,
        facts: ResearchUniverseFacts,
    ) -> Result<(), ValidationError> {
        insert_once(
            &mut self.universes,
            event_id,
            facts,
            MissingReplayInput::UniverseActivation,
        )
    }

    /// Inserts the frozen market sizing-policy set attached to one decision
    /// event. Policy/book consistency is checked against the exact raw book
    /// that the adapter derived from the replay stream.
    pub fn insert_risk_policies(
        &mut self,
        event_id: EventId,
        policies: ResearchRiskPolicies,
    ) -> Result<(), ValidationError> {
        insert_once(
            &mut self.risk_policies,
            event_id,
            policies,
            MissingReplayInput::RiskPolicies,
        )
    }
}

fn insert_once<T: PartialEq>(
    map: &mut BTreeMap<EventId, T>,
    event_id: EventId,
    value: T,
    input: MissingReplayInput,
) -> Result<(), ValidationError> {
    match map.entry(event_id) {
        std::collections::btree_map::Entry::Vacant(entry) => {
            entry.insert(value);
            Ok(())
        }
        std::collections::btree_map::Entry::Occupied(entry) if entry.get() == &value => Ok(()),
        std::collections::btree_map::Entry::Occupied(_) => Err(misaligned([input])),
    }
}

/// The reproducible paper-engine setup for each independent replay request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResearchExecutionSetup {
    ledger_id: LedgerId,
    broker_config: BrokerConfig,
    broker_context: BrokerRunContext,
    config_digest: String,
}

impl ResearchExecutionSetup {
    /// Creates an engine setup from the same paper broker configuration and run
    /// commitment used by runtime. No alternate broker may be supplied.
    pub fn new(
        ledger_id: LedgerId,
        broker_config: BrokerConfig,
        broker_context: BrokerRunContext,
        config_digest: impl Into<String>,
    ) -> Result<Self, ValidationError> {
        let config_digest = config_digest.into();
        if !is_prefixed_digest(&config_digest) {
            return Err(ValidationError::InvalidDigest);
        }
        Ok(Self {
            ledger_id,
            broker_config,
            broker_context,
            config_digest,
        })
    }

    fn state(
        &self,
        opened_at: TimestampNs,
        risk_policies: BTreeMap<Market, RiskPolicy>,
    ) -> Result<EngineState, ValidationError> {
        let ledger = LedgerState::new(self.ledger_id, opened_at).map_err(engine_failure)?;
        Ok(EngineState::new(
            ledger,
            PaperBroker::new(self.broker_config, self.broker_context.clone(), opened_at),
            risk_policies,
        ))
    }
}

/// The only production-engine implementation of [`RuleReplay`].
///
/// `artifacts` contains a fully immutable artifact for every declared grid
/// candidate. Selecting a candidate therefore never constructs rule values
/// from free-form parameters or an approximate research-only strategy.
#[derive(Debug)]
pub struct EngineRuleReplay {
    replay: DeterministicReplay,
    provenance: ResearchProvenance,
    artifacts: Vec<RulesArtifact>,
    facts: ResearchFacts,
    recovery_boundaries: Vec<RecoveryBoundary>,
    execution: ResearchExecutionSetup,
}

impl EngineRuleReplay {
    /// Validates the bounded replay manifest and every immutable sidecar before
    /// it can service a candidate/fold request.
    pub fn new(
        replay: DeterministicReplay,
        provenance: ResearchProvenance,
        artifacts: Vec<RulesArtifact>,
        facts: ResearchFacts,
        recovery_boundaries: Vec<RecoveryBoundary>,
        execution: ResearchExecutionSetup,
    ) -> Result<Self, ValidationError> {
        provenance.validate()?;
        validate_replay_provenance(&replay, &provenance)?;
        validate_artifacts(&artifacts, &provenance)?;
        validate_recovery_boundaries(&replay, &recovery_boundaries)?;
        validate_research_facts(&replay, &provenance, &facts)?;
        if execution.config_digest != provenance.config_digest {
            return Err(misaligned([MissingReplayInput::RiskPolicies]));
        }
        Ok(Self {
            replay,
            provenance,
            artifacts,
            facts,
            recovery_boundaries,
            execution,
        })
    }

    fn artifact_for(
        &self,
        config: trench_core::strategy::rules::RuleConfig,
    ) -> Result<&RulesArtifact, ValidationError> {
        self.artifacts
            .iter()
            .find(|artifact| artifact.config().ok() == Some(config))
            .ok_or(ValidationError::IncompleteGrid)
    }

    fn run(&self, request: RuleReplayRequest) -> Result<EngineReplayOutcome, ValidationError> {
        let artifact = self.artifact_for(request.config)?;
        let strategy = RulesStrategy::from_artifact(artifact)?;
        let source_start = request
            .training
            .map_or(request.evaluation.start(), |training| training.start());
        if source_start >= request.evaluation.end() {
            return Err(ValidationError::InvalidEngineOutcome);
        }
        let mut events = self
            .replay
            .events()
            .iter()
            .filter(|event| {
                event.event_time() >= source_start && event_as_of(event) < request.evaluation.end()
            })
            .collect::<Vec<_>>();
        events.sort_by(|left, right| causal_event_order(left, right));
        let mut state = self.execution.state(source_start, BTreeMap::new())?;
        let initial_equity = state.ledger().equity().value();
        let mut evidence = ReplayEvidence::new(self.replay.digest(), &self.provenance, request);
        let mut source_books = BTreeMap::<Market, OrderBook>::new();
        let mut executable_books = BTreeMap::<Market, OrderBook>::new();
        let mut recovery_index = 0_usize;
        let mut recovered = BTreeMap::<Market, TimestampNs>::new();
        let mut pending_rule_position = None::<RulePosition>;
        let mut rule_position = None::<RulePosition>;

        for event in events {
            let at = event_as_of(event);
            self.apply_recoveries_before(
                at,
                &mut recovery_index,
                &mut recovered,
                &mut state,
                &mut evidence,
            )?;
            match event.kind() {
                MarketEventKind::BookSnapshot(_) => {
                    let book = match OrderBook::apply_snapshot(
                        source_books.get(event.market()),
                        event,
                        self.execution.broker_config.maximum_book_age(),
                    ) {
                        Ok(book) => book,
                        Err(BookError::Stale { .. }) => {
                            return Err(misaligned([MissingReplayInput::ExecutableBooks]));
                        }
                        Err(BookError::NonMonotonicTime { .. }) => {
                            let was_open = state.ledger().position().is_some();
                            let prior = take_state(&self.execution, &mut state, at)?;
                            let outcome =
                                source_retained(event, at, prior).map_err(engine_failure)?;
                            state = evidence.consume(was_open, outcome)?;
                            sync_rule_position(
                                &state,
                                &mut rule_position,
                                &mut pending_rule_position,
                            );
                            continue;
                        }
                        Err(error) => return Err(engine_failure(error)),
                    };
                    let executable = recovered.get(event.market()).is_some_and(|recovered_at| {
                        book.event_time() > *recovered_at && book.received_at() > *recovered_at
                    });
                    source_books.insert(event.market().clone(), book.clone());
                    if executable {
                        executable_books.insert(event.market().clone(), book.clone());
                    }
                    let was_open = state.ledger().position().is_some();
                    let prior = take_state(&self.execution, &mut state, at)?;
                    let outcome = if executable {
                        Engine::apply(
                            EngineEvent::ExecutableBook {
                                event_id: event.event_id().clone(),
                                at,
                                book,
                            },
                            prior,
                            &EngineContext::passive(EventAdmission::New),
                        )
                    } else {
                        source_retained(event, at, prior)
                    }
                    .map_err(engine_failure)?;
                    state = evidence.consume(was_open, outcome)?;
                }
                MarketEventKind::AssetContext(context) => {
                    let was_open = state.ledger().position().is_some();
                    let prior = take_state(&self.execution, &mut state, at)?;
                    let exit = rule_position
                        .as_ref()
                        .and_then(|position| {
                            (position.market() == event.market()).then(|| {
                                strategy.exit_for(position, context.mark_price().value(), None, at)
                            })
                        })
                        .flatten();
                    let outcome = if recovered.contains_key(event.market())
                        && let Some(reason) = exit
                    {
                        Engine::apply(
                            EngineEvent::ExitRequested {
                                event_id: event.event_id().clone(),
                                at,
                                reason: broker_exit_reason(reason),
                                market: event.market().clone(),
                                price: context.mark_price(),
                                event_time: event.event_time(),
                                received_at: event.received_at(),
                            },
                            prior,
                            &EngineContext::passive(EventAdmission::New),
                        )
                    } else if recovered.contains_key(event.market()) {
                        Engine::apply(
                            EngineEvent::MarketMark {
                                event_id: event.event_id().clone(),
                                at,
                                market: event.market().clone(),
                                price: context.mark_price(),
                                event_time: event.event_time(),
                                received_at: event.received_at(),
                            },
                            prior,
                            &EngineContext::passive(EventAdmission::New),
                        )
                    } else {
                        source_retained(event, at, prior)
                    }
                    .map_err(engine_failure)?;
                    state = evidence.consume(was_open, outcome)?;
                }
                MarketEventKind::Funding(funding) => {
                    let was_open = state.ledger().position().is_some();
                    let prior = take_state(&self.execution, &mut state, at)?;
                    let outcome = if recovered.contains_key(event.market())
                        && let Some(mark_price) = funding.mark_price()
                    {
                        Engine::apply(
                            EngineEvent::FundingObserved {
                                event_id: event.event_id().clone(),
                                at,
                                market: event.market().clone(),
                                venue_at: event.event_time(),
                                received_at: event.received_at(),
                                rate: funding.rate(),
                                mark_price,
                            },
                            prior,
                            &EngineContext::passive(EventAdmission::New),
                        )
                    } else {
                        source_retained(event, at, prior)
                    }
                    .map_err(engine_failure)?;
                    state = evidence.consume(was_open, outcome)?;
                }
                MarketEventKind::CompletedCandle(candle)
                    if event.event_time() >= request.evaluation.start() =>
                {
                    let decision = self.decision_inputs(
                        event,
                        candle.interval(),
                        &executable_books,
                        &recovered,
                    )?;
                    let rule_decision = strategy.on_bar(decision.snapshot, decision.long_history);
                    evidence.observe_prediction(
                        event.event_id(),
                        rule_decision.explanation_json().as_bytes(),
                    );
                    let exit = rule_position
                        .as_ref()
                        .and_then(|position| {
                            (position.market() == event.market()).then(|| {
                                strategy.exit_for(
                                    position,
                                    candle.close().value(),
                                    rule_decision.composite(),
                                    at,
                                )
                            })
                        })
                        .flatten();
                    if let Some(reason) = exit {
                        let was_open = state.ledger().position().is_some();
                        let prior = take_state(&self.execution, &mut state, at)?;
                        let outcome = Engine::apply(
                            EngineEvent::ExitRequested {
                                event_id: event.event_id().clone(),
                                at,
                                reason: broker_exit_reason(reason),
                                market: event.market().clone(),
                                price: candle.close(),
                                event_time: event.event_time(),
                                received_at: event.received_at(),
                            },
                            prior,
                            &EngineContext::passive(EventAdmission::New),
                        )
                        .map_err(engine_failure)?;
                        state = evidence.consume(was_open, outcome)?;
                    } else if rule_position.is_none()
                        && pending_rule_position.is_none()
                        && let Some(candidate) = rule_decision.candidate()
                    {
                        if candidate.decision_time() != at {
                            return Err(misaligned([MissingReplayInput::FeatureSnapshot]));
                        }
                        let context = EngineContext::new(
                            EventAdmission::New,
                            SnapshotBindings::new(
                                decision.books.clone(),
                                decision.universe.clone(),
                            ),
                            StrategyFingerprints::new(
                                strategy.fingerprint(),
                                "0000000000000000000000000000000000000000000000000000000000000000",
                            ),
                        );
                        let was_open = state.ledger().position().is_some();
                        let prior =
                            take_state(&self.execution, &mut state, candidate.decision_time())?
                                .with_risk_policies(decision.risk_policies.clone())
                                .map_err(engine_failure)?;
                        let outcome = Engine::apply_verified_entry_arbitration(
                            event.event_id().clone(),
                            candidate.decision_time(),
                            vec![EntryCandidate::new(candidate.clone(), &strategy)],
                            prior,
                            &context,
                        )
                        .map_err(engine_failure)?;
                        state = evidence.consume(was_open, outcome)?;
                        if state.broker().state() == BrokerState::PendingEntry {
                            pending_rule_position = RulePosition::from_candidate(candidate);
                        }
                    } else {
                        let was_open = state.ledger().position().is_some();
                        let prior = take_state(&self.execution, &mut state, at)?;
                        let outcome = source_retained(event, at, prior).map_err(engine_failure)?;
                        state = evidence.consume(was_open, outcome)?;
                    }
                }
                _ => {
                    let was_open = state.ledger().position().is_some();
                    let prior = take_state(&self.execution, &mut state, at)?;
                    let outcome = source_retained(event, at, prior).map_err(engine_failure)?;
                    state = evidence.consume(was_open, outcome)?;
                }
            }
            sync_rule_position(&state, &mut rule_position, &mut pending_rule_position);
        }
        self.apply_recoveries_before(
            request.evaluation.end(),
            &mut recovery_index,
            &mut recovered,
            &mut state,
            &mut evidence,
        )?;
        let was_open = state.ledger().position().is_some();
        let prior = take_state(&self.execution, &mut state, request.evaluation.end())?;
        let terminal = Engine::apply(
            EngineEvent::EndOfData {
                event_id: end_of_data_event_id(self.replay.digest(), request)?,
                at: request.evaluation.end(),
            },
            prior,
            &EngineContext::passive(EventAdmission::New),
        )
        .map_err(engine_failure)?;
        state = evidence.consume(was_open, terminal)?;
        if state.ledger().position().is_some()
            || !matches!(
                state.broker().state(),
                BrokerState::Flat | BrokerState::Liquidated
            )
        {
            return Err(ValidationError::InvalidEngineOutcome);
        }
        let net_pnl = state
            .ledger()
            .equity()
            .value()
            .checked_sub(initial_equity)
            .ok_or(ValidationError::InvalidEngineOutcome)?;
        evidence.outcome(net_pnl)
    }

    fn apply_recoveries_before(
        &self,
        at: TimestampNs,
        index: &mut usize,
        recovered: &mut BTreeMap<Market, TimestampNs>,
        state: &mut EngineState,
        evidence: &mut ReplayEvidence,
    ) -> Result<(), ValidationError> {
        while let Some(boundary) = self.recovery_boundaries.get(*index)
            && boundary.at() < at
        {
            let was_open = state.ledger().position().is_some();
            let prior = take_state(&self.execution, state, boundary.at())?;
            let outcome = Engine::apply(
                EngineEvent::MarketRecovered {
                    event_id: boundary.event_id().clone(),
                    at: boundary.at(),
                    market: boundary.market().clone(),
                },
                prior,
                &EngineContext::passive(EventAdmission::New),
            )
            .map_err(engine_failure)?;
            *state = evidence.consume(was_open, outcome)?;
            recovered.insert(boundary.market().clone(), boundary.at());
            *index = index
                .checked_add(1)
                .ok_or(ValidationError::InvalidEngineOutcome)?;
        }
        Ok(())
    }

    fn decision_inputs<'a>(
        &'a self,
        event: &MarketEvent,
        sleeve: trench_core::event::CandleInterval,
        current_books: &'a BTreeMap<Market, OrderBook>,
        recovered: &BTreeMap<Market, TimestampNs>,
    ) -> Result<DecisionInputs<'a>, ValidationError> {
        let mut missing = Vec::new();
        let feature = self.facts.features.get(event.event_id());
        if feature.is_none() {
            missing.push(MissingReplayInput::FeatureSnapshot);
            missing.push(MissingReplayInput::LongHorizonHistory);
        }
        let universe = self.facts.universes.get(event.event_id());
        if universe.is_none() {
            missing.push(MissingReplayInput::UniverseActivation);
        }
        let risk_policies = self.facts.risk_policies.get(event.event_id());
        if risk_policies.is_none() {
            missing.push(MissingReplayInput::RiskPolicies);
        }
        if !recovered.contains_key(event.market()) {
            missing.push(MissingReplayInput::RecoveryBoundary);
        }
        if !current_books.contains_key(event.market()) {
            missing.push(MissingReplayInput::ExecutableBooks);
        }
        if !missing.is_empty() {
            return Err(missing_inputs(missing));
        }
        let (Some(feature), Some(universe), Some(risk_policies)) =
            (feature, universe, risk_policies)
        else {
            return Err(ValidationError::InvalidEngineOutcome);
        };
        let decision_at = event_as_of(event);
        let mut misaligned_inputs = Vec::new();
        if feature.snapshot.market() != event.market()
            || feature.snapshot.sleeve() != sleeve
            || feature.snapshot.as_of_time() != event.event_time()
            || feature.long_history.market() != event.market().as_str()
            || feature.long_history.as_of_time_ns() != event.event_time().value()
            || feature.long_history.verify().is_err()
            || feature.snapshot.schema_hash() != self.provenance.feature_schema_digest
            || feature.snapshot.input_range().is_none_or(|range| {
                range.universe_digest() != Some(self.provenance.universe_digest.as_str())
                    || !feature.evidence.contains(range.first_event_id())
                    || !feature.evidence.contains(range.last_event_id())
            })
            || !history_sources_are_declared(&feature.long_history, &feature.evidence)
            || feature
                .evidence
                .verify(&self.replay, Some(decision_at))
                .is_err()
            || !feature.evidence.contains(event.event_id())
        {
            misaligned_inputs.push(MissingReplayInput::FeatureSnapshot);
        }
        if !universe.activation.is_effective_for(event.event_time())
            || universe
                .activation
                .universe()
                .is_none_or(|tradeable| !tradeable.contains(event.market()))
            || universe
                .activation
                .universe()
                .is_none_or(|tradeable| tradeable.digest() != self.provenance.universe_digest)
            || universe
                .evidence
                .verify(&self.replay, Some(decision_at))
                .is_err()
        {
            misaligned_inputs.push(MissingReplayInput::UniverseActivation);
        }
        let Some(book) = current_books.get(event.market()) else {
            return Err(ValidationError::InvalidEngineOutcome);
        };
        let book_is_causal = book.event_time() <= decision_at
            && book.received_at() <= decision_at
            && decision_at
                .checked_duration_since(book.event_time())
                .is_ok_and(|age| age <= self.execution.broker_config.maximum_book_age());
        if !book_is_causal {
            misaligned_inputs.push(MissingReplayInput::ExecutableBooks);
        }
        if risk_policies.config_digest != self.provenance.config_digest
            || risk_policies
                .evidence
                .verify(&self.replay, Some(decision_at))
                .is_err()
            || !risk_policies.evidence.contains(book.event_id())
            || !risk_policies
                .policies
                .get(event.market())
                .is_some_and(|policy| policy.matches_book_digest(&book.commitment_digest()))
        {
            misaligned_inputs.push(MissingReplayInput::RiskPolicies);
        }
        if !misaligned_inputs.is_empty() {
            return Err(misaligned(misaligned_inputs));
        }
        Ok(DecisionInputs {
            snapshot: &feature.snapshot,
            long_history: &feature.long_history,
            universe: &universe.activation,
            books: current_books,
            risk_policies: &risk_policies.policies,
        })
    }
}

impl RuleReplay for EngineRuleReplay {
    fn replay(
        &mut self,
        request: RuleReplayRequest,
    ) -> Result<EngineReplayOutcome, ValidationError> {
        self.run(request)
    }
}

struct DecisionInputs<'a> {
    snapshot: &'a FeatureSnapshot,
    long_history: &'a LongHorizonFeatureHistory,
    universe: &'a UniverseActivation,
    books: &'a BTreeMap<Market, OrderBook>,
    risk_policies: &'a BTreeMap<Market, RiskPolicy>,
}

fn source_retained(
    event: &MarketEvent,
    at: TimestampNs,
    state: EngineState,
) -> Result<EngineOutcome, trench_core::engine::EngineError> {
    Engine::apply(
        EngineEvent::SourceRetained {
            event_id: event.event_id().clone(),
            at,
        },
        state,
        &EngineContext::passive(EventAdmission::New),
    )
}

fn take_state(
    execution: &ResearchExecutionSetup,
    state: &mut EngineState,
    at: TimestampNs,
) -> Result<EngineState, ValidationError> {
    let parked = execution.state(at, BTreeMap::new())?;
    Ok(std::mem::replace(state, parked))
}

fn event_as_of(event: &MarketEvent) -> TimestampNs {
    event.event_time().max(event.received_at())
}

fn causal_event_order(left: &MarketEvent, right: &MarketEvent) -> std::cmp::Ordering {
    event_as_of(left)
        .cmp(&event_as_of(right))
        .then_with(|| left.event_time().cmp(&right.event_time()))
        .then_with(|| left.event_id().cmp(right.event_id()))
}

fn canonical_event_ids(mut event_ids: Vec<EventId>) -> Result<Vec<EventId>, ValidationError> {
    if event_ids.is_empty() || event_ids.len() > MAX_RESEARCH_SOURCE_EVENTS {
        return Err(ValidationError::InvalidEngineOutcome);
    }
    event_ids.sort();
    event_ids.dedup();
    (event_ids.len() <= MAX_RESEARCH_SOURCE_EVENTS)
        .then_some(event_ids)
        .ok_or(ValidationError::InvalidEngineOutcome)
}

fn source_events(
    replay: &DeterministicReplay,
    event_ids: &[EventId],
) -> Result<Vec<MarketEvent>, ValidationError> {
    if event_ids.is_empty() || event_ids.len() > MAX_RESEARCH_SOURCE_EVENTS {
        return Err(ValidationError::InvalidEngineOutcome);
    }
    let by_id = replay
        .events()
        .iter()
        .map(|event| (event.event_id(), event))
        .collect::<BTreeMap<_, _>>();
    let mut events = event_ids
        .iter()
        .map(|event_id| {
            by_id
                .get(event_id)
                .cloned()
                .cloned()
                .ok_or(ValidationError::InvalidEngineOutcome)
        })
        .collect::<Result<Vec<_>, _>>()?;
    events.sort_by(causal_event_order);
    Ok(events)
}

fn recovery_event_id(
    market: &Market,
    generation: u64,
    at: TimestampNs,
    snapshot_event_id: &EventId,
) -> Result<EventId, ValidationError> {
    let mut hasher = Hasher::new_derive_key("trench.daemon-recovery-boundary.v1");
    for component in [
        market.as_str(),
        &generation.to_string(),
        &at.value().to_string(),
        snapshot_event_id.as_str(),
    ] {
        hasher.update(&(component.len() as u64).to_be_bytes());
        hasher.update(component.as_bytes());
    }
    EventId::new(format!("b3:{}", hasher.finalize().to_hex()))
        .map_err(|_| ValidationError::InvalidEngineOutcome)
}

fn end_of_data_event_id(
    replay_digest: &str,
    request: RuleReplayRequest,
) -> Result<EventId, ValidationError> {
    let mut hasher = Hasher::new_derive_key("trench.research.end-of-data.v1");
    for component in [
        replay_digest,
        &request.outer_fold.to_string(),
        &request.evaluation.start().value().to_string(),
        &request.evaluation.end().value().to_string(),
        &request.config.threshold().value().to_string(),
        &request.config.atr_floor().value().to_string(),
        &request.config.take_profit().value().to_string(),
    ] {
        hasher.update(&(component.len() as u64).to_be_bytes());
        hasher.update(component.as_bytes());
    }
    EventId::new(format!("b3:{}", hasher.finalize().to_hex()))
        .map_err(|_| ValidationError::InvalidEngineOutcome)
}

fn broker_exit_reason(reason: RuleExitReason) -> BrokerExitReason {
    match reason {
        RuleExitReason::Stop => BrokerExitReason::Stop,
        RuleExitReason::TakeProfit => BrokerExitReason::TakeProfit,
        RuleExitReason::OppositeSignal => BrokerExitReason::OppositeSignal,
        RuleExitReason::TimeLimit => BrokerExitReason::Time,
    }
}

fn sync_rule_position(
    state: &EngineState,
    rule_position: &mut Option<RulePosition>,
    pending_rule_position: &mut Option<RulePosition>,
) {
    if state.ledger().position().is_some() {
        if rule_position.is_none() {
            *rule_position = pending_rule_position.take();
        }
    } else if state.broker().state() != BrokerState::PendingEntry {
        *rule_position = None;
        *pending_rule_position = None;
    }
}

fn history_sources_are_declared(
    history: &LongHorizonFeatureHistory,
    evidence: &ResearchSourceEvidence,
) -> bool {
    history
        .hourly_realized_volatility_20_history()
        .iter()
        .chain(history.premium_history())
        .chain(history.open_interest_change_4_history())
        .chain(history.funding_history())
        .all(|sample| {
            EventId::new(sample.source_event_id().to_owned())
                .is_ok_and(|event_id| evidence.contains(&event_id))
        })
}

fn is_prefixed_digest(value: &str) -> bool {
    value.len() == 67
        && value.starts_with("b3:")
        && value[3..].bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn validate_replay_provenance(
    replay: &DeterministicReplay,
    provenance: &ResearchProvenance,
) -> Result<(), ValidationError> {
    if provenance.data_digest != replay.digest()
        || replay.manifests().iter().any(|manifest| {
            let source = manifest.provenance();
            source.config_digest() != provenance.config_digest
                || source.code_digest() != provenance.code_digest
        })
        || replay.events().iter().any(|event| {
            event.event_time() > provenance.data_cutoff
                || event.received_at() > provenance.data_cutoff
        })
    {
        return Err(ValidationError::MisalignedReplayInputs {
            inputs: vec![MissingReplayInput::FeatureSnapshot],
        });
    }
    Ok(())
}

fn validate_artifacts(
    artifacts: &[RulesArtifact],
    provenance: &ResearchProvenance,
) -> Result<(), ValidationError> {
    if artifacts.len() != RuleGrid::CANDIDATE_COUNT
        || artifacts
            .iter()
            .zip(RuleGrid::declared())
            .any(|(artifact, expected)| {
                artifact.config().ok() != Some(expected)
                    || artifact.verify_provenance(provenance).is_err()
            })
    {
        return Err(ValidationError::IncompleteGrid);
    }
    Ok(())
}

fn validate_research_facts(
    replay: &DeterministicReplay,
    provenance: &ResearchProvenance,
    facts: &ResearchFacts,
) -> Result<(), ValidationError> {
    let raw_ids = replay
        .events()
        .iter()
        .map(MarketEvent::event_id)
        .collect::<BTreeSet<_>>();
    for (event_id, feature) in &facts.features {
        if !raw_ids.contains(event_id)
            || !feature.evidence.contains(event_id)
            || feature.evidence.verify(replay, None).is_err()
            || feature.snapshot.schema_hash() != provenance.feature_schema_digest
            || feature.snapshot.input_range().is_none_or(|range| {
                range.universe_digest() != Some(provenance.universe_digest.as_str())
                    || !feature.evidence.contains(range.first_event_id())
                    || !feature.evidence.contains(range.last_event_id())
            })
            || !history_sources_are_declared(&feature.long_history, &feature.evidence)
        {
            return Err(misaligned([MissingReplayInput::FeatureSnapshot]));
        }
    }
    for (event_id, universe) in &facts.universes {
        if !raw_ids.contains(event_id)
            || universe.evidence.verify(replay, None).is_err()
            || universe
                .activation
                .universe()
                .is_none_or(|tradeable| tradeable.digest() != provenance.universe_digest)
        {
            return Err(misaligned([MissingReplayInput::UniverseActivation]));
        }
    }
    for (event_id, risk) in &facts.risk_policies {
        if !raw_ids.contains(event_id)
            || risk.config_digest != provenance.config_digest
            || risk.evidence.verify(replay, None).is_err()
        {
            return Err(misaligned([MissingReplayInput::RiskPolicies]));
        }
    }
    Ok(())
}

fn validate_recovery_boundaries(
    replay: &DeterministicReplay,
    boundaries: &[RecoveryBoundary],
) -> Result<(), ValidationError> {
    if boundaries.len() > MAX_RESEARCH_RECOVERY_BOUNDARIES {
        return Err(ValidationError::InvalidEngineOutcome);
    }
    let raw_ids = replay
        .events()
        .iter()
        .map(|event| event.event_id())
        .collect::<BTreeSet<_>>();
    let mut prior: Option<(TimestampNs, &Market, u64, &EventId)> = None;
    let mut generations = BTreeSet::new();
    let mut boundary_ids = BTreeSet::new();
    for boundary in boundaries {
        let evidence = boundary.evidence.verify(replay, Some(boundary.at()));
        let snapshot = evidence.as_ref().ok().and_then(|events| {
            events
                .iter()
                .find(|event| event.event_id() == &boundary.snapshot_event_id)
        });
        let expected_id = recovery_event_id(
            boundary.market(),
            boundary.generation,
            boundary.at(),
            &boundary.snapshot_event_id,
        );
        if boundary.generation == 0
            || raw_ids.contains(boundary.event_id())
            || !boundary_ids.insert(boundary.event_id())
            || !generations.insert((boundary.market(), boundary.generation))
            || snapshot.is_none_or(|event| {
                event.market() != boundary.market()
                    || !matches!(event.kind(), MarketEventKind::BookSnapshot(_))
            })
            || evidence.is_err()
            || evidence.is_ok_and(|events| {
                events
                    .iter()
                    .any(|event| event.market() != boundary.market())
            })
            || expected_id.as_ref().ok() != Some(boundary.event_id())
            || prior.is_some_and(|prior| {
                (
                    boundary.at(),
                    boundary.market(),
                    boundary.generation,
                    boundary.event_id(),
                ) <= prior
            })
        {
            return Err(misaligned([MissingReplayInput::RecoveryBoundary]));
        }
        prior = Some((
            boundary.at(),
            boundary.market(),
            boundary.generation,
            boundary.event_id(),
        ));
    }
    Ok(())
}

fn missing_inputs(inputs: impl IntoIterator<Item = MissingReplayInput>) -> ValidationError {
    ValidationError::MissingReplayInputs {
        inputs: canonical_inputs(inputs),
    }
}

fn misaligned(inputs: impl IntoIterator<Item = MissingReplayInput>) -> ValidationError {
    ValidationError::MisalignedReplayInputs {
        inputs: canonical_inputs(inputs),
    }
}

fn canonical_inputs(
    inputs: impl IntoIterator<Item = MissingReplayInput>,
) -> Vec<MissingReplayInput> {
    inputs
        .into_iter()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn engine_failure(error: impl std::fmt::Display) -> ValidationError {
    ValidationError::EngineReplayFailed {
        reason: error.to_string(),
    }
}

struct ReplayEvidence {
    prediction: Hasher,
    intent: Hasher,
    trade: Hasher,
    cost: Hasher,
    turnover: Decimal,
    closed_trades: u32,
}

impl ReplayEvidence {
    fn new(
        replay_digest: &str,
        provenance: &ResearchProvenance,
        request: RuleReplayRequest,
    ) -> Self {
        let mut evidence = Self {
            prediction: Hasher::new_derive_key("trench.rules-replay.prediction.v1"),
            intent: Hasher::new_derive_key("trench.rules-replay.intent.v1"),
            trade: Hasher::new_derive_key("trench.rules-replay.trade.v1"),
            cost: Hasher::new_derive_key("trench.rules-replay.cost.v1"),
            turnover: Decimal::ZERO,
            closed_trades: 0,
        };
        let header = format!(
            "{replay_digest}|{}|{}|{}|{}|{}|{}|{}|{}|{}",
            provenance.config_digest,
            provenance.code_digest,
            provenance.data_digest,
            request.outer_fold,
            request.evaluation.start().value(),
            request.evaluation.end().value(),
            request.config.threshold().value(),
            request.config.atr_floor().value(),
            request.config.take_profit().value(),
        );
        for stream in [
            &mut evidence.prediction,
            &mut evidence.intent,
            &mut evidence.trade,
            &mut evidence.cost,
        ] {
            stream.update(header.as_bytes());
        }
        evidence
    }

    fn observe_prediction(&mut self, event_id: &EventId, bytes: &[u8]) {
        update_stream(&mut self.prediction, event_id.as_str().as_bytes());
        update_stream(&mut self.prediction, bytes);
    }

    fn consume(
        &mut self,
        was_open: bool,
        outcome: EngineOutcome,
    ) -> Result<EngineState, ValidationError> {
        for record in outcome.persistence_batch().records() {
            let bytes = record.payload_json().as_bytes();
            match record.kind() {
                EnginePersistenceKind::Snapshot | EnginePersistenceKind::Signal => {
                    update_stream(&mut self.prediction, bytes);
                }
                EnginePersistenceKind::Intent | EnginePersistenceKind::Order => {
                    update_stream(&mut self.intent, bytes);
                }
                EnginePersistenceKind::Risk => update_stream(&mut self.cost, bytes),
                EnginePersistenceKind::Fill => {
                    update_stream(&mut self.trade, bytes);
                    update_stream(&mut self.cost, bytes);
                }
                EnginePersistenceKind::Ledger | EnginePersistenceKind::Breaker => {
                    update_stream(&mut self.trade, bytes);
                }
            }
        }
        for causal in outcome.batch().records() {
            if let EngineRecord::BrokerApplied { transition } = causal.record() {
                for record in transition.records() {
                    if let BrokerRecord::TakerFill { walk, .. } = record {
                        self.turnover = self
                            .turnover
                            .checked_add(walk.filled_notional().value())
                            .ok_or(ValidationError::InvalidEngineOutcome)?;
                    }
                }
            }
        }
        let (state, _) = outcome.into_parts();
        if was_open && state.ledger().position().is_none() {
            self.closed_trades = self
                .closed_trades
                .checked_add(1)
                .ok_or(ValidationError::InvalidEngineOutcome)?;
        }
        Ok(state)
    }

    fn outcome(self, net_pnl: Decimal) -> Result<EngineReplayOutcome, ValidationError> {
        EngineReplayOutcome::new(
            net_pnl,
            self.turnover,
            self.closed_trades,
            stream_digest(self.prediction),
            stream_digest(self.intent),
            stream_digest(self.trade),
            stream_digest(self.cost),
        )
    }
}

fn update_stream(stream: &mut Hasher, bytes: &[u8]) {
    stream.update(&(bytes.len() as u64).to_be_bytes());
    stream.update(bytes);
}

fn stream_digest(stream: Hasher) -> String {
    format!("b3:{}", stream.finalize().to_hex())
}

#[cfg(test)]
mod tests {
    use std::fs;

    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

    use rust_decimal_macros::dec;
    use tempfile::TempDir;
    use trench_core::broker::{BrokerConfig, BrokerRunContext};
    use trench_core::domain::{Price, Quantity, RunId, Usdc};
    use trench_core::engine::EngineRecord;
    use trench_core::event::{BookLevel, BookSnapshot, CandleInterval, CompletedCandle};
    use trench_core::validation::{ReplayPhase, RuleSelection, TimeRange};

    use super::*;
    use crate::parquet::{DataProvenance, ParquetStore};

    fn prefixed_digest(value: char) -> String {
        format!("b3:{}", value.to_string().repeat(64))
    }

    fn bare_digest(value: char) -> String {
        value.to_string().repeat(64)
    }

    fn timestamp(value: i64) -> TimestampNs {
        TimestampNs::new(i128::from(value)).expect("fixture timestamp must be valid")
    }

    fn market() -> Market {
        Market::new("BTC").expect("fixture market must be valid")
    }

    fn secure(directory: &TempDir) {
        #[cfg(unix)]
        fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700))
            .expect("fixture root must be private");
    }

    fn source_provenance() -> DataProvenance {
        DataProvenance::new(
            prefixed_digest('a'),
            prefixed_digest('b'),
            ParquetStore::schema_hash(),
        )
        .expect("fixture source provenance must be valid")
    }

    fn research_provenance(replay: &DeterministicReplay) -> ResearchProvenance {
        ResearchProvenance {
            config_digest: prefixed_digest('a'),
            code_digest: prefixed_digest('b'),
            data_digest: replay.digest().to_owned(),
            universe_digest: prefixed_digest('c'),
            feature_schema_digest: prefixed_digest('d'),
            data_cutoff: timestamp(2_000_000_000_000),
        }
    }

    fn artifacts(provenance: &ResearchProvenance) -> Vec<RulesArtifact> {
        RuleGrid::declared()
            .into_iter()
            .map(|config| {
                RulesArtifact::new(RuleSelection::from_config(config), provenance)
                    .expect("fixture artifact must be valid")
            })
            .collect()
    }

    fn execution() -> ResearchExecutionSetup {
        ResearchExecutionSetup::new(
            LedgerId::RulesOnly,
            BrokerConfig::new(
                Usdc::new(dec!(1)).expect("minimum notional"),
                trench_core::event::DurationNs::new(1_000).expect("book age"),
            )
            .expect("broker configuration"),
            BrokerRunContext::new(
                RunId::new("research-fixture").expect("run ID"),
                bare_digest('e'),
                bare_digest('f'),
            )
            .expect("broker run context"),
            prefixed_digest('a'),
        )
        .expect("config-bound execution setup")
    }

    fn request() -> RuleReplayRequest {
        RuleReplayRequest {
            config: RuleGrid::declared()[0],
            outer_fold: 0,
            phase: ReplayPhase::InnerValidation { inner_fold: 0 },
            training: None,
            evaluation: TimeRange::new(timestamp(0), timestamp(1_000_000_000_000))
                .expect("evaluation range"),
        }
    }

    fn replay(events: &[MarketEvent]) -> (TempDir, DeterministicReplay) {
        let directory = TempDir::new().expect("fixture directory must exist");
        secure(&directory);
        let store = ParquetStore::open(directory.path(), source_provenance())
            .expect("fixture store must open");
        store
            .write_events(events)
            .expect("fixture events must commit atomically");
        let replay = DeterministicReplay::open(directory.path(), source_provenance())
            .expect("fixture replay must verify");
        (directory, replay)
    }

    fn completed_candle_event() -> MarketEvent {
        MarketEvent::completed_candle(
            timestamp(900_000_000_000),
            timestamp(900_000_000_000),
            market(),
            CompletedCandle::new(
                CandleInterval::FifteenMinutes,
                timestamp(0),
                Price::new(dec!(100)).expect("open"),
                Price::new(dec!(101)).expect("high"),
                Price::new(dec!(99)).expect("low"),
                Price::new(dec!(100)).expect("close"),
                Quantity::new(dec!(1)).expect("volume"),
                1,
            )
            .expect("completed candle"),
        )
        .expect("normalized completed candle")
    }

    fn book_event(at: i64) -> MarketEvent {
        book_event_with_receipt(at, at)
    }

    fn book_event_with_receipt(event_at: i64, received_at: i64) -> MarketEvent {
        MarketEvent::book_snapshot(
            timestamp(event_at),
            timestamp(received_at),
            market(),
            BookSnapshot::new(
                1,
                vec![BookLevel::new(
                    Price::new(dec!(99)).expect("bid"),
                    Quantity::new(dec!(10)).expect("bid quantity"),
                )],
                vec![BookLevel::new(
                    Price::new(dec!(100)).expect("ask"),
                    Quantity::new(dec!(10)).expect("ask quantity"),
                )],
            ),
        )
        .expect("normalized book")
    }

    fn crossed_book_at(at: i64) -> MarketEvent {
        MarketEvent::book_snapshot(
            timestamp(at),
            timestamp(at),
            market(),
            BookSnapshot::new(
                1,
                vec![BookLevel::new(
                    Price::new(dec!(100)).expect("bid"),
                    Quantity::new(dec!(10)).expect("bid quantity"),
                )],
                vec![BookLevel::new(
                    Price::new(dec!(100)).expect("ask"),
                    Quantity::new(dec!(10)).expect("ask quantity"),
                )],
            ),
        )
        .expect("normalized crossed book is retained until execution")
    }

    fn recovery_boundary_fixture(
        replay: &DeterministicReplay,
        snapshot_event: &MarketEvent,
    ) -> RecoveryBoundary {
        let evidence =
            ResearchSourceEvidence::from_replay(replay, vec![snapshot_event.event_id().clone()])
                .expect("fixture source evidence");
        let at = snapshot_event.received_at();
        RecoveryBoundary {
            event_id: recovery_event_id(&market(), 1, at, snapshot_event.event_id())
                .expect("fixture recovery ID"),
            at,
            market: market(),
            generation: 1,
            evidence,
            snapshot_event_id: snapshot_event.event_id().clone(),
        }
    }

    #[test]
    fn missing_typed_decision_facts_are_canonical_ineligibility_not_a_zero_outcome() {
        let (_directory, replay) = replay(&[completed_candle_event()]);
        let provenance = research_provenance(&replay);
        let mut adapter = EngineRuleReplay::new(
            replay,
            provenance.clone(),
            artifacts(&provenance),
            ResearchFacts::new(),
            Vec::new(),
            execution(),
        )
        .expect("the empty sidecar is a valid fail-closed construction");

        assert_eq!(
            adapter.replay(request()),
            Err(ValidationError::MissingReplayInputs {
                inputs: vec![
                    MissingReplayInput::FeatureSnapshot,
                    MissingReplayInput::LongHorizonHistory,
                    MissingReplayInput::UniverseActivation,
                    MissingReplayInput::ExecutableBooks,
                    MissingReplayInput::RiskPolicies,
                    MissingReplayInput::RecoveryBoundary,
                ],
            })
        );
    }

    #[test]
    fn recovery_boundaries_reject_a_raw_source_identity_collision() {
        let book = book_event(2);
        let (_directory, replay) = replay(std::slice::from_ref(&book));
        let provenance = research_provenance(&replay);
        let mut forged = recovery_boundary_fixture(&replay, &book);
        forged.event_id = book.event_id().clone();
        let error = EngineRuleReplay::new(
            replay,
            provenance.clone(),
            artifacts(&provenance),
            ResearchFacts::new(),
            vec![forged],
            execution(),
        )
        .expect_err("a raw source identity cannot be reused as a recovery boundary");

        assert_eq!(
            error,
            ValidationError::MisalignedReplayInputs {
                inputs: vec![MissingReplayInput::RecoveryBoundary],
            }
        );
    }

    #[test]
    fn source_evidence_cannot_be_reused_across_replays_or_before_its_receipt() {
        let delayed = book_event_with_receipt(2, 20);
        let (_first_directory, first) = replay(std::slice::from_ref(&delayed));
        let evidence =
            ResearchSourceEvidence::from_replay(&first, vec![delayed.event_id().clone()])
                .expect("first replay proves its own source");
        let replacement = book_event(3);
        let (_second_directory, second) = replay(std::slice::from_ref(&replacement));

        assert!(evidence.verify(&first, Some(timestamp(19))).is_err());
        assert!(evidence.verify(&second, None).is_err());
    }

    #[test]
    fn source_observations_are_receipt_ordered_with_a_stable_tie_breaker() {
        let delayed_book = book_event_with_receipt(2, 20);
        let timely_book = book_event(3);
        let mut events = vec![&delayed_book, &timely_book];
        events.sort_by(|left, right| causal_event_order(left, right));

        assert_eq!(events, vec![&timely_book, &delayed_book]);
    }

    #[test]
    fn exclusive_evaluation_end_cannot_admit_an_invalid_source_book() {
        let at_end = crossed_book_at(request().evaluation.end().value());
        let (_directory, replay) = replay(&[at_end]);
        let provenance = research_provenance(&replay);
        let mut adapter = EngineRuleReplay::new(
            replay,
            provenance.clone(),
            artifacts(&provenance),
            ResearchFacts::new(),
            Vec::new(),
            execution(),
        )
        .expect("the replay boundary itself is valid");

        let outcome = adapter
            .replay(request())
            .expect("the exclusive-end book must not execute or fail the run");
        assert_eq!(outcome.net_pnl(), Decimal::ZERO);
    }

    #[test]
    fn broker_setup_must_match_the_frozen_config_commitment() {
        let (_directory, replay) = replay(&[book_event(2)]);
        let provenance = research_provenance(&replay);
        let setup = ResearchExecutionSetup::new(
            LedgerId::RulesOnly,
            BrokerConfig::new(
                Usdc::new(dec!(1)).expect("minimum notional"),
                trench_core::event::DurationNs::new(1_000).expect("book age"),
            )
            .expect("broker configuration"),
            BrokerRunContext::new(
                RunId::new("research-fixture-mismatch").expect("run ID"),
                bare_digest('e'),
                bare_digest('f'),
            )
            .expect("broker run context"),
            prefixed_digest('c'),
        )
        .expect("well-formed but wrong config binding");

        assert!(matches!(
            EngineRuleReplay::new(
                replay,
                provenance.clone(),
                artifacts(&provenance),
                ResearchFacts::new(),
                Vec::new(),
                setup,
            ),
            Err(ValidationError::MisalignedReplayInputs {
                inputs
            }) if inputs == vec![MissingReplayInput::RiskPolicies]
        ));
    }

    #[test]
    fn verified_recovery_and_book_take_the_real_engine_path_without_a_synthetic_fill() {
        let first_book = book_event(2);
        let second_book = book_event(3);
        let (_directory, replay) = replay(&[first_book.clone(), second_book]);
        let provenance = research_provenance(&replay);
        let boundary = recovery_boundary_fixture(&replay, &first_book);
        let mut adapter = EngineRuleReplay::new(
            replay,
            provenance.clone(),
            artifacts(&provenance),
            ResearchFacts::new(),
            vec![boundary],
            execution(),
        )
        .expect("recovery source is separate and ordered");

        let first = adapter
            .replay(request())
            .expect("flat real-engine replay must complete");
        let second = adapter
            .replay(request())
            .expect("replay must rebuild a deterministic engine state");

        assert_eq!(first, second);
        assert_eq!(first.net_pnl(), Decimal::ZERO);
        assert_eq!(first.turnover(), Decimal::ZERO);
        assert_eq!(first.closed_trades(), 0);
    }

    #[test]
    fn critical_lifecycle_fixture_is_generated_by_the_production_engine() {
        let fixtures = trench_core::engine::test_support::critical_lifecycle_fixtures();

        assert!(fixtures.iter().all(|fixture| {
            fixture.outcome().batch().records().iter().any(|record| {
                matches!(
                    record.record(),
                    EngineRecord::BrokerApplied { .. } | EngineRecord::LedgerApplied { .. }
                )
            })
        }));
    }
}
