//! Authoritative rules research replay over a frozen normalized-event stream.
//!
//! This adapter deliberately owns neither an alternative execution model nor a
//! synthetic PnL calculator. It supplies the already-validated source facts to
//! `trench_core::engine::Engine` and derives every outcome from that engine's
//! immutable causal records.

use std::collections::{BTreeMap, BTreeSet};

use blake3::Hasher;
use rust_decimal::Decimal;
use trench_core::book::OrderBook;
use trench_core::broker::{BrokerConfig, BrokerRecord, BrokerRunContext, BrokerState, PaperBroker};
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
use trench_core::strategy::rules::RulesStrategy;
use trench_core::universe::UniverseActivation;
use trench_core::validation::{
    EngineReplayOutcome, MissingReplayInput, ResearchProvenance, RuleGrid, RuleReplay,
    RuleReplayRequest, RulesArtifact, ValidationError,
};

use crate::replay::DeterministicReplay;

const MAX_RESEARCH_RECOVERY_BOUNDARIES: usize = 100_000;

/// A persisted recovery completion that may unlock a later full book for
/// engine execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveryBoundary {
    event_id: EventId,
    at: TimestampNs,
    market: Market,
}

impl RecoveryBoundary {
    /// Creates one explicit completed recovery boundary.
    #[must_use]
    pub const fn new(event_id: EventId, at: TimestampNs, market: Market) -> Self {
        Self {
            event_id,
            at,
            market,
        }
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
}

impl ResearchFeatureFacts {
    /// Couples the exact common snapshot to its independently validated
    /// long-horizon history. Neither value is imputed by this adapter.
    #[must_use]
    pub const fn new(snapshot: FeatureSnapshot, long_history: LongHorizonFeatureHistory) -> Self {
        Self {
            snapshot,
            long_history,
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
    universes: BTreeMap<EventId, UniverseActivation>,
    risk_policies: BTreeMap<EventId, BTreeMap<Market, RiskPolicy>>,
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
        activation: UniverseActivation,
    ) -> Result<(), ValidationError> {
        insert_once(
            &mut self.universes,
            event_id,
            activation,
            MissingReplayInput::UniverseActivation,
        )
    }

    /// Inserts the frozen market sizing-policy set attached to one decision
    /// event. Policy/book consistency is checked against the exact raw book
    /// that the adapter derived from the replay stream.
    pub fn insert_risk_policies(
        &mut self,
        event_id: EventId,
        policies: BTreeMap<Market, RiskPolicy>,
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
}

impl ResearchExecutionSetup {
    /// Creates an engine setup from the same paper broker configuration and run
    /// commitment used by runtime. No alternate broker may be supplied.
    #[must_use]
    pub const fn new(
        ledger_id: LedgerId,
        broker_config: BrokerConfig,
        broker_context: BrokerRunContext,
    ) -> Self {
        Self {
            ledger_id,
            broker_config,
            broker_context,
        }
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
        let events = self
            .replay
            .events()
            .iter()
            .filter(|event| {
                event.event_time() >= source_start
                    && event.event_time() < request.evaluation.end()
                    && event.received_at() <= request.evaluation.end()
            })
            .collect::<Vec<_>>();
        let opened_at = self
            .recovery_boundaries
            .iter()
            .map(RecoveryBoundary::at)
            .filter(|at| *at < request.evaluation.end())
            .min()
            .unwrap_or(source_start)
            .min(source_start);
        let mut state = self.execution.state(opened_at, BTreeMap::new())?;
        let initial_equity = state.ledger().equity().value();
        let mut evidence = ReplayEvidence::new(self.replay.digest(), &self.provenance, request);
        let mut source_books = BTreeMap::<Market, OrderBook>::new();
        let mut executable_books = BTreeMap::<Market, OrderBook>::new();
        let mut recovery_index = 0_usize;
        let mut recovered = BTreeMap::<Market, TimestampNs>::new();

        for event in events {
            let at = event_as_of(event);
            self.apply_recoveries_through(
                at,
                &mut recovery_index,
                &mut recovered,
                &mut state,
                &mut evidence,
            )?;
            match event.kind() {
                MarketEventKind::BookSnapshot(_) => {
                    let book = OrderBook::apply_snapshot(
                        source_books.get(event.market()),
                        event,
                        self.execution.broker_config.maximum_book_age(),
                    )
                    .map_err(engine_failure)?;
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
                    let outcome = if recovered.contains_key(event.market()) {
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
                    let outcome = if recovered.contains_key(event.market()) {
                        Engine::apply(
                            EngineEvent::FundingObserved {
                                event_id: event.event_id().clone(),
                                at,
                                market: event.market().clone(),
                                venue_at: event.event_time(),
                                received_at: event.received_at(),
                                rate: funding.rate(),
                                mark_price: funding.mark_price(),
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
                    if let Some(candidate) = rule_decision.candidate() {
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
        }
        self.apply_recoveries_through(
            request.evaluation.end(),
            &mut recovery_index,
            &mut recovered,
            &mut state,
            &mut evidence,
        )?;
        if state.ledger().position().is_some() || state.broker().state() != BrokerState::Flat {
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

    fn apply_recoveries_through(
        &self,
        at: TimestampNs,
        index: &mut usize,
        recovered: &mut BTreeMap<Market, TimestampNs>,
        state: &mut EngineState,
        evidence: &mut ReplayEvidence,
    ) -> Result<(), ValidationError> {
        while let Some(boundary) = self.recovery_boundaries.get(*index)
            && boundary.at() <= at
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
        let feature = feature.expect("checked feature facts are present");
        let universe = universe.expect("checked universe activation is present");
        let risk_policies = risk_policies.expect("checked risk policies are present");
        let mut misaligned_inputs = Vec::new();
        if feature.snapshot.market() != event.market()
            || feature.snapshot.sleeve() != sleeve
            || feature.snapshot.as_of_time() != event.event_time()
            || feature.long_history.market() != event.market().as_str()
            || feature.long_history.as_of_time_ns() != event.event_time().value()
            || feature.long_history.verify().is_err()
        {
            misaligned_inputs.push(MissingReplayInput::FeatureSnapshot);
        }
        if !universe.is_effective_for(event.event_time())
            || universe
                .universe()
                .is_none_or(|tradeable| !tradeable.contains(event.market()))
        {
            misaligned_inputs.push(MissingReplayInput::UniverseActivation);
        }
        let book = current_books
            .get(event.market())
            .expect("checked current executable book is present");
        if !risk_policies
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
            universe,
            books: current_books,
            risk_policies,
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
    let mut prior: Option<(TimestampNs, &Market, &EventId)> = None;
    for boundary in boundaries {
        if raw_ids.contains(boundary.event_id())
            || prior.is_some_and(|prior| {
                (boundary.at(), boundary.market(), boundary.event_id()) <= prior
            })
        {
            return Err(misaligned([MissingReplayInput::RecoveryBoundary]));
        }
        prior = Some((boundary.at(), boundary.market(), boundary.event_id()));
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
        )
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
        MarketEvent::book_snapshot(
            timestamp(at),
            timestamp(at),
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
    fn recovery_boundaries_cannot_reuse_verified_market_event_identity() {
        let book = book_event(2);
        let (_directory, replay) = replay(std::slice::from_ref(&book));
        let provenance = research_provenance(&replay);
        let error = EngineRuleReplay::new(
            replay,
            provenance.clone(),
            artifacts(&provenance),
            ResearchFacts::new(),
            vec![RecoveryBoundary::new(
                book.event_id().clone(),
                timestamp(1),
                market(),
            )],
            execution(),
        )
        .expect_err("a recovery must have its own causal identity");

        assert_eq!(
            error,
            ValidationError::MisalignedReplayInputs {
                inputs: vec![MissingReplayInput::RecoveryBoundary],
            }
        );
    }

    #[test]
    fn verified_recovery_and_book_take_the_real_engine_path_without_a_synthetic_fill() {
        let (_directory, replay) = replay(&[book_event(2)]);
        let provenance = research_provenance(&replay);
        let mut adapter = EngineRuleReplay::new(
            replay,
            provenance.clone(),
            artifacts(&provenance),
            ResearchFacts::new(),
            vec![RecoveryBoundary::new(
                EventId::new("research-recovery-btc").expect("recovery ID"),
                timestamp(1),
                market(),
            )],
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
