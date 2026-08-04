//! Pure causal arbitration between strategies, sealed risk, and paper execution.

use std::collections::BTreeMap;

use blake3::Hasher;
use rust_decimal::Decimal;
use thiserror::Error;

use crate::book::OrderBook;
use crate::broker::{
    BrokerError, BrokerPosition, BrokerState, BrokerTransition, ExecutableBook, ExecutableFunding,
    ExecutableMark, ExecutionRole, ExitReason, MarketExecutionReady, PaperBroker,
};
use crate::domain::{EventId, Market, Price};
use crate::event::{FundingRate, TimestampNs};
use crate::ledger::{
    BookFreshness, EntryFill, LedgerError, LedgerState, LedgerTransition, MarkCosts,
};
use crate::risk::sizing::{
    RiskEngine, RiskError, RiskInputError, RiskPolicy, RiskQuote, RiskRequest, RiskSnapshot,
};
use crate::strategy::{
    CostDecision, CostRejection, OrderIntent, QuoteId, SignalCandidate, Strategy, StrategyKind,
};
use crate::universe::UniverseActivation;

/// Immutable causal identity shared by every record emitted from one input event.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CausalityId(EventId);

impl CausalityId {
    fn from_event(event_id: EventId) -> Self {
        Self(event_id)
    }

    /// Returns the normalized input event that owns this causal batch.
    #[must_use]
    pub const fn event_id(&self) -> &EventId {
        &self.0
    }
}

/// Durable event-admission result supplied by the outer single-writer journal.
///
/// The engine does not keep a local event-ID cache: process-local memory cannot
/// establish replay safety across restarts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventAdmission {
    /// The journal admitted this event exactly once for normal processing.
    New,
    /// The journal already owns this event ID; emit no-op causal evidence only.
    Duplicate,
}

/// Immutable executable-book and activated-universe sources for one decision boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotBindings {
    books: BTreeMap<Market, OrderBook>,
    universe: UniverseActivation,
}

impl SnapshotBindings {
    /// Creates typed source commitments that are combined with engine-owned state.
    #[must_use]
    pub fn new(books: BTreeMap<Market, OrderBook>, universe: UniverseActivation) -> Self {
        Self { books, universe }
    }

    fn supports(
        &self,
        at: TimestampNs,
        maximum_book_age: crate::event::DurationNs,
        candidates: &[EntryCandidate<'_>],
    ) -> bool {
        let Some(universe) = self.universe.universe() else {
            return false;
        };
        self.universe.is_effective_for(at)
            && candidates.iter().all(|entry| {
                universe.contains(entry.candidate.market())
                    && entry.candidate.universe_digest() == universe.digest()
                    && self
                        .books
                        .get(entry.candidate.market())
                        .is_some_and(|book| {
                            book.market() == entry.candidate.market()
                                && book.event_time() <= at
                                && book.received_at() <= at
                                && at
                                    .checked_duration_since(book.event_time())
                                    .is_ok_and(|age| age <= maximum_book_age)
                        })
            })
    }

    fn book_digest(&self) -> String {
        let mut hasher = Hasher::new_derive_key("trench.engine-book-set.v1");
        for (market, book) in &self.books {
            let commitment = book.commitment_digest();
            for component in [market.as_str(), commitment.as_str()] {
                hasher.update(&(component.len() as u64).to_be_bytes());
                hasher.update(component.as_bytes());
            }
        }
        hasher.finalize().to_hex().to_string()
    }

    fn universe_digest(&self) -> Option<&str> {
        self.universe.universe().map(|universe| universe.digest())
    }

    fn book(&self, market: &Market) -> Option<&OrderBook> {
        self.books.get(market)
    }
}

/// Frozen strategy-version fingerprints for the two independent paper ledgers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StrategyFingerprints {
    rules_only: String,
    ml_champion: String,
}

impl StrategyFingerprints {
    /// Creates the active rules and ML artifact/version fingerprint contract.
    #[must_use]
    pub fn new(rules_only: impl Into<String>, ml_champion: impl Into<String>) -> Self {
        Self {
            rules_only: rules_only.into(),
            ml_champion: ml_champion.into(),
        }
    }

    fn for_strategy(&self, strategy: StrategyKind) -> &str {
        match strategy {
            StrategyKind::RulesOnly => &self.rules_only,
            StrategyKind::MlChampion => &self.ml_champion,
        }
    }
}

/// Verified immutable context passed by the outer event-admission path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EngineContext {
    admission: EventAdmission,
    bindings: SnapshotBindings,
    strategy_fingerprints: StrategyFingerprints,
}

impl EngineContext {
    /// Creates one explicit event-admission and replay-validation context.
    #[must_use]
    pub const fn new(
        admission: EventAdmission,
        bindings: SnapshotBindings,
        strategy_fingerprints: StrategyFingerprints,
    ) -> Self {
        Self {
            admission,
            bindings,
            strategy_fingerprints,
        }
    }
}

/// One candidate paired with only its owning public-cost strategy boundary.
pub struct EntryCandidate<'strategy> {
    candidate: SignalCandidate,
    strategy: &'strategy dyn Strategy,
}

impl<'strategy> EntryCandidate<'strategy> {
    /// Creates an un-sized candidate submitted for one common risk snapshot.
    #[must_use]
    pub fn new(candidate: SignalCandidate, strategy: &'strategy dyn Strategy) -> Self {
        Self {
            candidate,
            strategy,
        }
    }
}

/// One explicit input to the pure core engine.
pub enum EngineEvent<'strategy> {
    /// One completed decision boundary with every currently eligible candidate.
    EntryArbitration {
        /// Globally unique normalized source event.
        event_id: EventId,
        /// Explicit completed-bar boundary and queue time.
        at: TimestampNs,
        /// Untrusted snapshot assertion that must equal engine-derived commitments.
        snapshot: RiskSnapshot,
        /// Every candidate that reached the common arbitration boundary.
        candidates: Vec<EntryCandidate<'strategy>>,
    },
    /// A verified market-data recovery boundary after a detected stream gap.
    MarketRecovered {
        /// Globally unique normalized recovery-completion event.
        event_id: EventId,
        /// Explicit point after which a market's source events may execute.
        at: TimestampNs,
        /// Market whose gap-recovery process completed.
        market: Market,
    },
    /// One recovered, fresh full book eligible for a queued paper fill.
    ExecutableBook {
        /// Globally unique normalized source event.
        event_id: EventId,
        /// Explicit processing boundary for freshness and causal persistence.
        at: TimestampNs,
        /// Immutable normalized full-depth book.
        book: OrderBook,
    },
    /// A fresh normalized mark that may trigger broker exit priority.
    MarketMark {
        /// Globally unique normalized mark event.
        event_id: EventId,
        /// Explicit processing boundary for freshness and causal persistence.
        at: TimestampNs,
        /// Marked market.
        market: Market,
        /// Venue mark price.
        price: Price,
        /// Venue event time.
        event_time: TimestampNs,
        /// Local source receipt time.
        received_at: TimestampNs,
    },
    /// An explicit strategy or breaker exit request at a fresh normalized mark.
    ExitRequested {
        /// Globally unique normalized mark event that owns this request.
        event_id: EventId,
        /// Explicit processing boundary for freshness and causal persistence.
        at: TimestampNs,
        /// Requested broker exit priority.
        reason: ExitReason,
        /// Marked market.
        market: Market,
        /// Venue mark price.
        price: Price,
        /// Venue event time.
        event_time: TimestampNs,
        /// Local source receipt time.
        received_at: TimestampNs,
    },
    /// A fresh source-identified funding observation.
    FundingObserved {
        /// Globally unique normalized funding event.
        event_id: EventId,
        /// Explicit processing boundary for freshness and causal persistence.
        at: TimestampNs,
        /// Funded market.
        market: Market,
        /// Venue funding timestamp.
        venue_at: TimestampNs,
        /// Local source receipt time.
        received_at: TimestampNs,
        /// Signed venue funding rate.
        rate: FundingRate,
        /// Venue mark used for funding notional.
        mark_price: Price,
    },
    /// An explicit clock progression with no implied order fill.
    AdvanceTime {
        /// Globally unique normalized scheduler event.
        event_id: EventId,
        /// Explicit time boundary.
        at: TimestampNs,
    },
    /// Explicit source exhaustion; residual exposure remains unresolved.
    EndOfData {
        /// Globally unique normalized end-of-stream event.
        event_id: EventId,
        /// Explicit terminal data boundary.
        at: TimestampNs,
    },
}

impl<'strategy> EngineEvent<'strategy> {
    /// Creates a single-snapshot entry-arbitration event.
    #[must_use]
    pub fn entry_arbitration(
        event_id: EventId,
        at: TimestampNs,
        snapshot: RiskSnapshot,
        candidates: Vec<EntryCandidate<'strategy>>,
    ) -> Self {
        Self::EntryArbitration {
            event_id,
            at,
            snapshot,
            candidates,
        }
    }
}

/// Pure mutable state carried explicitly between engine applications.
///
/// It contains no clock, I/O handle, wallet, signer, or live-execution client.
#[derive(Debug)]
pub struct EngineState {
    ledger: LedgerState,
    broker: PaperBroker,
    risk: RiskEngine,
    risk_policies: BTreeMap<Market, RiskPolicy>,
    recovered_markets: BTreeMap<Market, TimestampNs>,
}

impl EngineState {
    /// Starts an engine state from independently constructed paper components.
    #[must_use]
    pub fn new(
        ledger: LedgerState,
        broker: PaperBroker,
        risk_policies: BTreeMap<Market, RiskPolicy>,
    ) -> Self {
        Self {
            ledger,
            broker,
            risk: RiskEngine::default(),
            risk_policies,
            recovered_markets: BTreeMap::new(),
        }
    }

    /// Returns the immutable isolated paper ledger successor.
    #[must_use]
    pub const fn ledger(&self) -> &LedgerState {
        &self.ledger
    }

    /// Returns the deterministic paper broker successor.
    #[must_use]
    pub const fn broker(&self) -> &PaperBroker {
        &self.broker
    }

    #[cfg(test)]
    fn outstanding_approvals(&self) -> usize {
        self.risk.outstanding_approvals()
    }

    fn supports_risk_policies(
        &self,
        bindings: &SnapshotBindings,
        candidates: &[EntryCandidate<'_>],
    ) -> bool {
        candidates.iter().all(|entry| {
            self.risk_policies
                .get(entry.candidate.market())
                .zip(bindings.book(entry.candidate.market()))
                .is_some_and(|(policy, book)| policy.matches_book_digest(&book.commitment_digest()))
        })
    }

    fn risk_policy(&self, market: &Market) -> Option<&RiskPolicy> {
        self.risk_policies.get(market)
    }

    fn recovered_at(&self, market: &Market) -> Option<TimestampNs> {
        self.recovered_markets.get(market).copied()
    }
}

/// The deterministic transition authority. It owns neither state nor I/O.
#[derive(Debug, Default)]
pub struct Engine;

/// One record retained in an atomic causal batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EngineRecord {
    /// The source event entered the pure engine.
    EventReceived,
    /// The risk request did not match the immutable point-in-time context.
    SnapshotRejected,
    /// A candidate named a strategy artifact/version other than the active one.
    StrategyContextRejected {
        /// Candidate that was rejected before its strategy could be called.
        candidate: SignalCandidate,
    },
    /// A candidate was evaluated by sealed risk against the common snapshot.
    RiskQuoted {
        /// Original un-sized strategy candidate.
        candidate: SignalCandidate,
        /// Public cost/rejection evidence only; it contains no sealed order data.
        quote: RiskQuote,
    },
    /// The owning strategy accepted public cost evidence and produced an intent.
    CostAccepted {
        /// Candidate bound to the accepted opaque quote.
        candidate: SignalCandidate,
        /// Conservative edge after complete public fractional cost.
        net_edge: Decimal,
    },
    /// The owning strategy rejected a feasible public cost quote.
    CostRejected {
        /// Candidate rejected after only public cost inspection.
        candidate: SignalCandidate,
        /// Stable public rejection reason.
        reason: CostRejection,
    },
    /// An approved but unselected quote was discarded before it could be reused.
    QuoteDiscarded {
        /// Candidate whose seal was discarded.
        candidate: SignalCandidate,
        /// Opaque quote ID removed from the private risk cache.
        quote_id: QuoteId,
    },
    /// The best accepted quote was consumed exactly once by risk.
    QuoteConsumed {
        /// Winning candidate.
        candidate: SignalCandidate,
        /// Opaque quote consumed by the private execution boundary.
        quote_id: QuoteId,
    },
    /// The sole paper broker received the selected sealed order.
    EntryQueued {
        /// Winning candidate that was risk-approved and queued.
        candidate: SignalCandidate,
        /// Append-only broker evidence for the queued paper order.
        transition: BrokerTransition,
    },
    /// A verified recovery boundary became the sole execution authority for one market.
    MarketRecovered {
        /// Market whose persisted recovery boundary advanced.
        market: Market,
    },
    /// A stale recovery event was rejected without weakening a newer boundary.
    RecoveryRejected {
        /// Market whose recovery boundary was preserved.
        market: Market,
    },
    /// A market, funding, or exit input was ignored after source exhaustion.
    TerminalInputRejected {
        /// Terminal broker state that preserves the unresolved paper ledger.
        broker_state: BrokerState,
    },
    /// The paper broker advanced from a validated market source.
    BrokerApplied {
        /// Complete append-only broker evidence.
        transition: BrokerTransition,
    },
    /// The isolated synthetic ledger advanced from broker-reported actuals.
    LedgerApplied {
        /// Complete immutable ledger evidence and successor state.
        transition: LedgerTransition,
    },
    /// Entry arbitration was skipped because the sole ledger/broker was not flat.
    EntryBlocked {
        /// Broker state that prevented another entry.
        broker_state: BrokerState,
    },
    /// A durably admitted duplicate was ignored without rerunning strategy or risk.
    DuplicateIgnored,
}

/// One engine record tagged with its source event causality.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CausalRecord {
    causality_id: CausalityId,
    record: EngineRecord,
}

impl CausalRecord {
    /// Returns the common causal ID for this record.
    #[must_use]
    pub const fn causality_id(&self) -> &CausalityId {
        &self.causality_id
    }

    /// Returns the typed immutable transition evidence.
    #[must_use]
    pub const fn record(&self) -> &EngineRecord {
        &self.record
    }
}

/// All records generated atomically from one source event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EngineBatch {
    causality_id: CausalityId,
    at: TimestampNs,
    selected_market: Option<Market>,
    records: Vec<CausalRecord>,
}

impl EngineBatch {
    fn new(causality_id: CausalityId, at: TimestampNs) -> Self {
        Self {
            causality_id,
            at,
            selected_market: None,
            records: Vec::new(),
        }
    }

    fn push(&mut self, record: EngineRecord) {
        self.records.push(CausalRecord {
            causality_id: self.causality_id.clone(),
            record,
        });
    }

    /// Returns the immutable causal identity for every record in this batch.
    #[must_use]
    pub const fn causality_id(&self) -> &CausalityId {
        &self.causality_id
    }

    /// Returns the explicit input boundary time.
    #[must_use]
    pub const fn at(&self) -> TimestampNs {
        self.at
    }

    /// Returns the winning market, if an entry was queued.
    #[must_use]
    pub fn selected_market(&self) -> Option<&str> {
        self.selected_market.as_ref().map(Market::as_str)
    }

    /// Returns every append-only causal record in deterministic order.
    #[must_use]
    pub fn records(&self) -> &[CausalRecord] {
        &self.records
    }
}

/// The pure successor state and one persistence-ready causal batch.
#[derive(Debug)]
pub struct EngineOutcome {
    state: EngineState,
    batch: EngineBatch,
}

impl EngineOutcome {
    /// Borrows the pure successor state.
    #[must_use]
    pub const fn state(&self) -> &EngineState {
        &self.state
    }

    /// Borrows the atomic causal batch that must be persisted together.
    #[must_use]
    pub const fn batch(&self) -> &EngineBatch {
        &self.batch
    }

    /// Consumes the outcome and returns its successor state and batch.
    #[must_use]
    pub fn into_parts(self) -> (EngineState, EngineBatch) {
        (self.state, self.batch)
    }
}

/// Rejection of an invalid engine-boundary transition.
#[derive(Debug, Error)]
pub enum EngineError {
    /// A candidate reached the engine from a different decision boundary.
    #[error("candidate {candidate_digest} does not match engine event boundary")]
    CandidateTimeMismatch {
        /// Immutable candidate digest retained for audit.
        candidate_digest: String,
    },
    /// A selected approval could not be discarded from the private risk cache.
    #[error("approved quote disappeared before deterministic arbitration completed")]
    MissingApproval,
    /// A strategy returned an intent not bound to the candidate and public quote it received.
    #[error("strategy intent does not match its candidate and public cost quote")]
    IntentBindingMismatch,
    /// Conservative net-edge subtraction overflowed.
    #[error("checked arithmetic failed while calculating conservative net edge")]
    NetEdgeArithmetic,
    /// The common one-and-a-half-times public-cost gate could not be represented.
    #[error("checked arithmetic failed while calculating the public cost gate")]
    CostGateArithmetic,
    /// Risk quote or sealed approval consumption failed.
    #[error(transparent)]
    Risk(#[from] RiskError),
    /// A canonical engine-owned risk snapshot could not be constructed.
    #[error(transparent)]
    RiskInput(#[from] RiskInputError),
    /// Typed source validation changed before the canonical snapshot could be built.
    #[error("verified book or active-universe source disappeared during engine application")]
    MissingVerifiedSource,
    /// Broker queueing failed after a sealed quote was consumed.
    #[error(transparent)]
    Broker(#[from] BrokerError),
    /// A broker-reported actual fill could not produce a valid isolated-ledger transition.
    #[error(transparent)]
    Ledger(#[from] LedgerError),
    /// A broker-reported visible fill had no executable VWAP.
    #[error(transparent)]
    Fill(#[from] crate::broker::fill::FillError),
    /// A broker fill requires its exact post-transition isolated position.
    #[error("broker reported an entry fill without an open paper position")]
    MissingBrokerPosition,
    /// An executable market event arrived before the engine-owned recovery boundary.
    #[error("market {market:?} has no verified persisted recovery boundary")]
    MissingMarketRecovery {
        /// Market whose recovery source was absent.
        market: Market,
    },
    /// A broker liquidation-loss record did not match the preceding actual fill.
    #[error("broker liquidation loss quantity did not match its immediately preceding fill")]
    LiquidationQuantityMismatch,
}

impl Engine {
    /// Applies one explicit input to explicit prior state without I/O or a clock.
    ///
    /// Every candidate is risk-quoted against one immutable request before any
    /// strategy receives its public cost quote. Exactly one deterministic winner
    /// may consume a still-fresh private approval and reach the paper broker.
    pub fn apply<'strategy>(
        event: EngineEvent<'strategy>,
        prior_state: EngineState,
        context: &EngineContext,
    ) -> Result<EngineOutcome, EngineError> {
        match event {
            EngineEvent::EntryArbitration {
                event_id,
                at,
                snapshot,
                candidates,
            } => Self::apply_entry_arbitration(
                event_id,
                at,
                snapshot,
                candidates,
                prior_state,
                context,
            ),
            EngineEvent::MarketRecovered {
                event_id,
                at,
                market,
            } => Self::apply_market_recovered(event_id, at, market, prior_state, context),
            EngineEvent::ExecutableBook { event_id, at, book } => {
                Self::apply_executable_book(event_id, at, book, prior_state, context)
            }
            EngineEvent::MarketMark {
                event_id,
                at,
                market,
                price,
                event_time,
                received_at,
            } => Self::apply_market_mark(
                event_id,
                at,
                market,
                price,
                event_time,
                received_at,
                prior_state,
                context,
            ),
            EngineEvent::ExitRequested {
                event_id,
                at,
                reason,
                market,
                price,
                event_time,
                received_at,
            } => Self::apply_exit_request(
                event_id,
                at,
                reason,
                market,
                price,
                event_time,
                received_at,
                prior_state,
                context,
            ),
            EngineEvent::FundingObserved {
                event_id,
                at,
                market,
                venue_at,
                received_at,
                rate,
                mark_price,
            } => Self::apply_funding(
                event_id,
                at,
                market,
                venue_at,
                received_at,
                rate,
                mark_price,
                prior_state,
                context,
            ),
            EngineEvent::AdvanceTime { event_id, at } => {
                Self::apply_advance_time(event_id, at, prior_state, context)
            }
            EngineEvent::EndOfData { event_id, at } => {
                Self::apply_end_of_data(event_id, at, prior_state, context)
            }
        }
    }

    fn apply_entry_arbitration<'strategy>(
        event_id: EventId,
        at: TimestampNs,
        supplied_snapshot: RiskSnapshot,
        candidates: Vec<EntryCandidate<'strategy>>,
        mut state: EngineState,
        context: &EngineContext,
    ) -> Result<EngineOutcome, EngineError> {
        let causality_id = CausalityId::from_event(event_id.clone());
        let mut batch = EngineBatch::new(causality_id, at);
        if context.admission == EventAdmission::Duplicate {
            batch.push(EngineRecord::DuplicateIgnored);
            return Ok(EngineOutcome { state, batch });
        }
        batch.push(EngineRecord::EventReceived);
        if !context
            .bindings
            .supports(at, state.broker.maximum_book_age(), &candidates)
            || !state.supports_risk_policies(&context.bindings, &candidates)
        {
            batch.push(EngineRecord::SnapshotRejected);
            return Ok(EngineOutcome { state, batch });
        }
        let snapshot = canonical_snapshot(&state, &event_id, at, &candidates, context)?;
        if supplied_snapshot != snapshot {
            batch.push(EngineRecord::SnapshotRejected);
            return Ok(EngineOutcome { state, batch });
        }
        if state.ledger.position().is_some() || state.broker.state() != BrokerState::Flat {
            batch.push(EngineRecord::EntryBlocked {
                broker_state: state.broker.state(),
            });
            return Ok(EngineOutcome { state, batch });
        }

        let mut accepted = Vec::new();
        for entry in candidates {
            if entry.candidate.decision_time() != at {
                return Err(EngineError::CandidateTimeMismatch {
                    candidate_digest: entry.candidate.digest().to_owned(),
                });
            }
            if entry.candidate.strategy_fingerprint()
                != context
                    .strategy_fingerprints
                    .for_strategy(entry.candidate.strategy())
                || entry.strategy.fingerprint() != entry.candidate.strategy_fingerprint()
            {
                batch.push(EngineRecord::StrategyContextRejected {
                    candidate: entry.candidate,
                });
                continue;
            }
            let policy = state
                .risk_policy(entry.candidate.market())
                .ok_or(EngineError::MissingVerifiedSource)?;
            let request = RiskRequest::from_policy(snapshot.clone(), policy);
            let quote = state.risk.quote_candidate(&entry.candidate, &request)?;
            batch.push(EngineRecord::RiskQuoted {
                candidate: entry.candidate.clone(),
                quote: quote.clone(),
            });
            if !quote.is_approved() {
                continue;
            }
            if !covers_complete_public_cost(
                entry.candidate.gross_edge(),
                quote.cost_quote().total_cost_fraction(),
            )? {
                if !state.risk.discard_quote(quote.cost_quote().quote_id()) {
                    return Err(EngineError::MissingApproval);
                }
                batch.push(EngineRecord::CostRejected {
                    candidate: entry.candidate.clone(),
                    reason: CostRejection::InsufficientGrossEdge,
                });
                batch.push(EngineRecord::QuoteDiscarded {
                    candidate: entry.candidate,
                    quote_id: quote.cost_quote().quote_id().clone(),
                });
                continue;
            }
            match entry
                .strategy
                .accept_cost(&entry.candidate, quote.cost_quote())
            {
                CostDecision::Accepted(intent) => {
                    if intent.candidate() != &entry.candidate
                        || intent.quote_id() != quote.cost_quote().quote_id()
                        || intent.total_cost_fraction() != quote.cost_quote().total_cost_fraction()
                    {
                        return Err(EngineError::IntentBindingMismatch);
                    }
                    let net_edge = entry
                        .candidate
                        .gross_edge()
                        .checked_sub(intent.total_cost_fraction())
                        .ok_or(EngineError::NetEdgeArithmetic)?;
                    batch.push(EngineRecord::CostAccepted {
                        candidate: entry.candidate.clone(),
                        net_edge,
                    });
                    accepted.push(AcceptedCandidate {
                        candidate: entry.candidate,
                        intent,
                        net_edge,
                    });
                }
                CostDecision::Rejected(reason) => {
                    if !state.risk.discard_quote(quote.cost_quote().quote_id()) {
                        return Err(EngineError::MissingApproval);
                    }
                    batch.push(EngineRecord::CostRejected {
                        candidate: entry.candidate.clone(),
                        reason,
                    });
                    batch.push(EngineRecord::QuoteDiscarded {
                        candidate: entry.candidate,
                        quote_id: quote.cost_quote().quote_id().clone(),
                    });
                }
            }
        }
        accepted.sort_by(|left, right| {
            right
                .net_edge
                .cmp(&left.net_edge)
                .then_with(|| left.candidate.digest().cmp(right.candidate.digest()))
        });
        let Some(_) = accepted.first() else {
            return Ok(EngineOutcome { state, batch });
        };
        for loser in accepted.iter().skip(1) {
            if !state.risk.discard_quote(loser.intent.quote_id()) {
                return Err(EngineError::MissingApproval);
            }
            batch.push(EngineRecord::QuoteDiscarded {
                candidate: loser.candidate.clone(),
                quote_id: loser.intent.quote_id().clone(),
            });
        }
        let winner = accepted.remove(0);
        let quote_id = winner.intent.quote_id().clone();
        let winner_policy = state
            .risk_policy(winner.candidate.market())
            .ok_or(EngineError::MissingVerifiedSource)?;
        let winner_request = RiskRequest::from_policy(snapshot, winner_policy);
        let approved = state
            .risk
            .consume_quote(&winner.intent, winner_request.snapshot(), at)?;
        batch.push(EngineRecord::QuoteConsumed {
            candidate: winner.candidate.clone(),
            quote_id,
        });
        let transition = state.broker.queue_entry(approved, at)?;
        batch.selected_market = Some(winner.candidate.market().clone());
        batch.push(EngineRecord::EntryQueued {
            candidate: winner.candidate,
            transition,
        });
        Ok(EngineOutcome { state, batch })
    }

    fn apply_market_recovered(
        event_id: EventId,
        at: TimestampNs,
        market: Market,
        mut state: EngineState,
        context: &EngineContext,
    ) -> Result<EngineOutcome, EngineError> {
        let causality_id = CausalityId::from_event(event_id);
        let mut batch = EngineBatch::new(causality_id, at);
        if context.admission == EventAdmission::Duplicate {
            batch.push(EngineRecord::DuplicateIgnored);
            return Ok(EngineOutcome { state, batch });
        }
        batch.push(EngineRecord::EventReceived);
        if state
            .recovered_at(&market)
            .is_some_and(|previous| at < previous)
        {
            batch.push(EngineRecord::RecoveryRejected { market });
            return Ok(EngineOutcome { state, batch });
        }
        state.recovered_markets.insert(market.clone(), at);
        batch.push(EngineRecord::MarketRecovered { market });
        Ok(EngineOutcome { state, batch })
    }

    fn apply_executable_book(
        event_id: EventId,
        at: TimestampNs,
        book: OrderBook,
        mut state: EngineState,
        context: &EngineContext,
    ) -> Result<EngineOutcome, EngineError> {
        let causality_id = CausalityId::from_event(event_id);
        let mut batch = EngineBatch::new(causality_id, at);
        if context.admission == EventAdmission::Duplicate {
            batch.push(EngineRecord::DuplicateIgnored);
            return Ok(EngineOutcome { state, batch });
        }
        batch.push(EngineRecord::EventReceived);
        if state.broker.state() == BrokerState::Unresolved {
            batch.push(EngineRecord::TerminalInputRejected {
                broker_state: state.broker.state(),
            });
            return Ok(EngineOutcome { state, batch });
        }

        let recovered_at = state.recovered_at(book.market()).ok_or_else(|| {
            EngineError::MissingMarketRecovery {
                market: book.market().clone(),
            }
        })?;
        let readiness = MarketExecutionReady::new(book.market().clone(), recovered_at);
        let executable =
            ExecutableBook::new(book, at, state.broker.maximum_book_age(), &readiness)?;
        let executable_book = executable.book();
        // An entry needs a fresh book-mark admission; an exit must settle only
        // from the broker's actual walk so a gap cannot fail early as a mark.
        if state.broker.state() == BrokerState::PendingEntry && state.ledger.position().is_none() {
            let transition = state.ledger.mark_to_book(
                executable_book.received_at(),
                Some(executable_book),
                BookFreshness::new(state.broker.maximum_book_age()),
                MarkCosts::none(),
            )?;
            apply_ledger_transition(&mut state, &mut batch, transition);
        }

        let Some(transition) = state.broker.on_executable_book(&executable)? else {
            return Ok(EngineOutcome { state, batch });
        };
        batch.push(EngineRecord::BrokerApplied {
            transition: transition.clone(),
        });
        apply_broker_transition(&mut state, &mut batch, &transition)?;
        Ok(EngineOutcome { state, batch })
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "normalized mark source is complete"
    )]
    fn apply_market_mark(
        event_id: EventId,
        at: TimestampNs,
        market: Market,
        price: Price,
        event_time: TimestampNs,
        received_at: TimestampNs,
        mut state: EngineState,
        context: &EngineContext,
    ) -> Result<EngineOutcome, EngineError> {
        let causality_id = CausalityId::from_event(event_id.clone());
        let mut batch = EngineBatch::new(causality_id, at);
        if context.admission == EventAdmission::Duplicate {
            batch.push(EngineRecord::DuplicateIgnored);
            return Ok(EngineOutcome { state, batch });
        }
        batch.push(EngineRecord::EventReceived);
        if state.broker.state() == BrokerState::Unresolved {
            batch.push(EngineRecord::TerminalInputRejected {
                broker_state: state.broker.state(),
            });
            return Ok(EngineOutcome { state, batch });
        }
        let readiness = market_readiness(&state, &market)?;
        let mark = ExecutableMark::new(
            market,
            event_id,
            price,
            event_time,
            received_at,
            at,
            state.broker.maximum_book_age(),
            &readiness,
        )?;
        if let Some(transition) = state.broker.observe_mark(&mark)? {
            batch.push(EngineRecord::BrokerApplied {
                transition: transition.clone(),
            });
            apply_broker_transition(&mut state, &mut batch, &transition)?;
        }
        Ok(EngineOutcome { state, batch })
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "normalized exit-request mark is complete"
    )]
    fn apply_exit_request(
        event_id: EventId,
        at: TimestampNs,
        reason: ExitReason,
        market: Market,
        price: Price,
        event_time: TimestampNs,
        received_at: TimestampNs,
        mut state: EngineState,
        context: &EngineContext,
    ) -> Result<EngineOutcome, EngineError> {
        let causality_id = CausalityId::from_event(event_id.clone());
        let mut batch = EngineBatch::new(causality_id, at);
        if context.admission == EventAdmission::Duplicate {
            batch.push(EngineRecord::DuplicateIgnored);
            return Ok(EngineOutcome { state, batch });
        }
        batch.push(EngineRecord::EventReceived);
        if state.broker.state() == BrokerState::Unresolved {
            batch.push(EngineRecord::TerminalInputRejected {
                broker_state: state.broker.state(),
            });
            return Ok(EngineOutcome { state, batch });
        }
        let readiness = market_readiness(&state, &market)?;
        let mark = ExecutableMark::new(
            market,
            event_id,
            price,
            event_time,
            received_at,
            at,
            state.broker.maximum_book_age(),
            &readiness,
        )?;
        let transition = state.broker.request_exit(reason, &mark)?;
        batch.push(EngineRecord::BrokerApplied {
            transition: transition.clone(),
        });
        apply_broker_transition(&mut state, &mut batch, &transition)?;
        Ok(EngineOutcome { state, batch })
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "normalized funding source is complete"
    )]
    fn apply_funding(
        event_id: EventId,
        at: TimestampNs,
        market: Market,
        venue_at: TimestampNs,
        received_at: TimestampNs,
        rate: FundingRate,
        mark_price: Price,
        mut state: EngineState,
        context: &EngineContext,
    ) -> Result<EngineOutcome, EngineError> {
        let causality_id = CausalityId::from_event(event_id.clone());
        let mut batch = EngineBatch::new(causality_id, at);
        if context.admission == EventAdmission::Duplicate {
            batch.push(EngineRecord::DuplicateIgnored);
            return Ok(EngineOutcome { state, batch });
        }
        batch.push(EngineRecord::EventReceived);
        if state.broker.state() == BrokerState::Unresolved {
            batch.push(EngineRecord::TerminalInputRejected {
                broker_state: state.broker.state(),
            });
            return Ok(EngineOutcome { state, batch });
        }
        let readiness = market_readiness(&state, &market)?;
        let funding = ExecutableFunding::new(
            market,
            event_id,
            venue_at,
            received_at,
            at,
            rate,
            mark_price,
            state.broker.maximum_book_age(),
            &readiness,
        )?;
        let transition = state.broker.apply_funding(&funding)?;
        batch.push(EngineRecord::BrokerApplied {
            transition: transition.clone(),
        });
        apply_broker_transition(&mut state, &mut batch, &transition)?;
        Ok(EngineOutcome { state, batch })
    }

    fn apply_advance_time(
        event_id: EventId,
        at: TimestampNs,
        mut state: EngineState,
        context: &EngineContext,
    ) -> Result<EngineOutcome, EngineError> {
        let causality_id = CausalityId::from_event(event_id);
        let mut batch = EngineBatch::new(causality_id, at);
        if context.admission == EventAdmission::Duplicate {
            batch.push(EngineRecord::DuplicateIgnored);
            return Ok(EngineOutcome { state, batch });
        }
        batch.push(EngineRecord::EventReceived);
        if let Some(transition) = state.broker.advance_time(at)? {
            batch.push(EngineRecord::BrokerApplied {
                transition: transition.clone(),
            });
            apply_broker_transition(&mut state, &mut batch, &transition)?;
        }
        Ok(EngineOutcome { state, batch })
    }

    fn apply_end_of_data(
        event_id: EventId,
        at: TimestampNs,
        mut state: EngineState,
        context: &EngineContext,
    ) -> Result<EngineOutcome, EngineError> {
        let causality_id = CausalityId::from_event(event_id);
        let mut batch = EngineBatch::new(causality_id, at);
        if context.admission == EventAdmission::Duplicate {
            batch.push(EngineRecord::DuplicateIgnored);
            return Ok(EngineOutcome { state, batch });
        }
        batch.push(EngineRecord::EventReceived);
        if let Some(transition) = state.broker.end_of_data(at)? {
            batch.push(EngineRecord::BrokerApplied {
                transition: transition.clone(),
            });
            apply_broker_transition(&mut state, &mut batch, &transition)?;
        }
        Ok(EngineOutcome { state, batch })
    }
}

fn market_readiness(
    state: &EngineState,
    market: &Market,
) -> Result<MarketExecutionReady, EngineError> {
    let recovered_at =
        state
            .recovered_at(market)
            .ok_or_else(|| EngineError::MissingMarketRecovery {
                market: market.clone(),
            })?;
    Ok(MarketExecutionReady::new(market.clone(), recovered_at))
}

fn apply_broker_transition(
    state: &mut EngineState,
    batch: &mut EngineBatch,
    transition: &BrokerTransition,
) -> Result<(), EngineError> {
    let mut paired_liquidation_losses = Vec::new();
    for (record_index, record) in transition.records().iter().enumerate() {
        match record {
            crate::broker::BrokerRecord::Funding { amount, .. } => {
                let transition = state.ledger.apply_funding(
                    transition.at(),
                    crate::ledger::FundingCashflow::from_signed(amount.value()),
                )?;
                apply_ledger_transition(state, batch, transition);
            }
            crate::broker::BrokerRecord::TakerFill {
                role: ExecutionRole::Entry,
                walk,
                fees,
                ..
            } => {
                let position = state
                    .broker
                    .position()
                    .ok_or(EngineError::MissingBrokerPosition)?;
                let entry = entry_fill_from_broker(position, walk, fees.total_fee())?;
                let transition =
                    state
                        .ledger
                        .open_position(transition.at(), entry, position.planned_loss())?;
                apply_ledger_transition(state, batch, transition);
            }
            crate::broker::BrokerRecord::TakerFill { walk, fees, .. } => {
                let liquidation = transition
                    .records()
                    .get(record_index + 1)
                    .and_then(|record| match record {
                        crate::broker::BrokerRecord::LiquidationLoss {
                            quantity,
                            forfeited_isolated_equity,
                            ..
                        } => Some((*quantity, *forfeited_isolated_equity)),
                        _ => None,
                    });
                let exit = crate::ledger::ExitFill::new(
                    walk.filled_quantity().value(),
                    walk.vwap()?,
                    fees.total_fee(),
                )?;
                let ledger_transition = if let Some((quantity, forfeited)) = liquidation {
                    if quantity != walk.filled_quantity() {
                        return Err(EngineError::LiquidationQuantityMismatch);
                    }
                    paired_liquidation_losses.push(record_index + 1);
                    state
                        .ledger
                        .settle_liquidated_exit(transition.at(), exit, forfeited)?
                } else {
                    state.ledger.reduce_position(transition.at(), exit)?
                };
                apply_ledger_transition(state, batch, ledger_transition);
            }
            _ => {}
        }
    }
    for (index, record) in transition.records().iter().enumerate() {
        if let crate::broker::BrokerRecord::LiquidationLoss {
            forfeited_isolated_equity,
            ..
        } = record
        {
            if paired_liquidation_losses.contains(&index) {
                continue;
            }
            let transition = state
                .ledger
                .settle_capped_liquidation(transition.at(), *forfeited_isolated_equity)?;
            apply_ledger_transition(state, batch, transition);
        }
    }
    Ok(())
}

fn entry_fill_from_broker(
    position: &BrokerPosition,
    walk: &crate::broker::fill::QuantityWalk,
    fee: crate::domain::Usdc,
) -> Result<EntryFill, EngineError> {
    Ok(EntryFill::new(
        position.market().clone(),
        position.side(),
        walk.filled_quantity().value(),
        walk.vwap()?,
        position.leverage(),
        fee,
    )?)
}

fn apply_ledger_transition(
    state: &mut EngineState,
    batch: &mut EngineBatch,
    transition: LedgerTransition,
) {
    state.ledger = transition.state().clone();
    batch.push(EngineRecord::LedgerApplied { transition });
}

struct AcceptedCandidate {
    candidate: SignalCandidate,
    intent: Box<OrderIntent>,
    net_edge: Decimal,
}

fn canonical_snapshot(
    state: &EngineState,
    event_id: &EventId,
    at: TimestampNs,
    candidates: &[EntryCandidate<'_>],
    context: &EngineContext,
) -> Result<RiskSnapshot, EngineError> {
    let mut candidate_digests = candidates
        .iter()
        .map(|entry| entry.candidate.digest())
        .collect::<Vec<_>>();
    candidate_digests.sort_unstable();
    let at_component = at.value().to_string();
    let mut hasher = Hasher::new_derive_key("trench.engine-entry-arbitration.v1");
    for component in std::iter::once(event_id.as_str())
        .chain(std::iter::once(at_component.as_str()))
        .chain(candidate_digests)
    {
        hasher.update(&(component.len() as u64).to_be_bytes());
        hasher.update(component.as_bytes());
    }
    RiskSnapshot::new(
        at,
        at,
        state.ledger.equity(),
        state.ledger.commitment_digest(),
        context.bindings.book_digest(),
        context
            .bindings
            .universe_digest()
            .ok_or(EngineError::MissingVerifiedSource)?,
        configuration_digest(state),
        hasher.finalize().to_hex().to_string(),
    )
    .map_err(EngineError::from)
}

fn configuration_digest(state: &EngineState) -> String {
    let mut hasher = Hasher::new_derive_key("trench.engine-configuration.v1");
    for component in [
        state.broker.context().run_digest(),
        state.broker.context().deployment_digest(),
    ] {
        hasher.update(&(component.len() as u64).to_be_bytes());
        hasher.update(component.as_bytes());
    }
    for (market, policy) in &state.risk_policies {
        let policy_digest = policy.commitment_digest();
        for component in [market.as_str(), policy_digest.as_str()] {
            hasher.update(&(component.len() as u64).to_be_bytes());
            hasher.update(component.as_bytes());
        }
    }
    hasher.finalize().to_hex().to_string()
}

fn covers_complete_public_cost(
    gross_edge: Decimal,
    total_cost_fraction: Decimal,
) -> Result<bool, EngineError> {
    let required_edge = total_cost_fraction
        .checked_mul(Decimal::new(15, 1))
        .ok_or(EngineError::CostGateArithmetic)?;
    Ok(gross_edge >= required_edge)
}

#[cfg(test)]
mod tests {
    use std::{cell::Cell, collections::BTreeMap};

    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use crate::book::OrderBook;
    use crate::broker::{BrokerConfig, BrokerRunContext, PaperBroker};
    use crate::domain::{
        Bps, EventId, LedgerId, Leverage, Market, Price, Quantity, RunId, Side, Sleeve, Usdc,
    };
    use crate::event::{BookLevel, BookSnapshot, DurationNs, MarketEvent, TimestampNs};
    use crate::ledger::LedgerState;
    use crate::risk::liquidation::{MaintenanceTier, MaintenanceTiers};
    use crate::risk::sizing::{
        ConservativeCosts, ImpactBand, ImpactCurve, RiskLimits, RiskRequest, RiskSnapshot,
        VenueConstraints,
    };
    use crate::strategy::{
        CandidateSpecification, CostDecision, CostQuote, OrderIntent, SignalCandidate, Strategy,
        StrategyKind,
    };
    use crate::universe::{
        DepthProfile, HistoryQuality, ListingState, MarketDataAvailability, SidedDepth,
        UniverseCandidate, UniverseLiquidity, UniverseSelector,
    };

    use super::{
        Engine, EngineContext, EngineEvent, EngineRecord, EngineState, EntryCandidate,
        EventAdmission, SnapshotBindings, StrategyFingerprints,
    };

    const RULES_FINGERPRINT: &str =
        "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";

    struct AcceptAll;

    impl Strategy for AcceptAll {
        fn fingerprint(&self) -> &str {
            RULES_FINGERPRINT
        }

        fn accept_cost(&self, candidate: &SignalCandidate, quote: &CostQuote) -> CostDecision {
            CostDecision::Accepted(Box::new(OrderIntent::new(candidate.clone(), quote)))
        }
    }

    struct CrossCandidateIntent {
        substituted: SignalCandidate,
    }

    impl Strategy for CrossCandidateIntent {
        fn fingerprint(&self) -> &str {
            RULES_FINGERPRINT
        }

        fn accept_cost(&self, _candidate: &SignalCandidate, quote: &CostQuote) -> CostDecision {
            CostDecision::Accepted(Box::new(OrderIntent::new(self.substituted.clone(), quote)))
        }
    }

    struct RejectAll;

    impl Strategy for RejectAll {
        fn fingerprint(&self) -> &str {
            RULES_FINGERPRINT
        }

        fn accept_cost(&self, _candidate: &SignalCandidate, _quote: &CostQuote) -> CostDecision {
            CostDecision::Rejected(crate::strategy::CostRejection::InsufficientGrossEdge)
        }
    }

    struct CountingStrategy {
        fingerprint: &'static str,
        calls: Cell<u8>,
    }

    impl CountingStrategy {
        fn new(fingerprint: &'static str) -> Self {
            Self {
                fingerprint,
                calls: Cell::new(0),
            }
        }

        fn calls(&self) -> u8 {
            self.calls.get()
        }
    }

    impl Strategy for CountingStrategy {
        fn fingerprint(&self) -> &str {
            self.fingerprint
        }

        fn accept_cost(&self, candidate: &SignalCandidate, quote: &CostQuote) -> CostDecision {
            self.calls.set(self.calls.get().saturating_add(1));
            CostDecision::Accepted(Box::new(OrderIntent::new(candidate.clone(), quote)))
        }
    }

    #[test]
    fn arbitration_quotes_every_candidate_discards_losers_and_queues_only_the_best_edge() {
        let at = timestamp(900_000_000_000);
        let strategy = AcceptAll;
        let entry_context = context(at, EventAdmission::New);
        let outcome = apply_entry(
            EventId::new("event-arbitration-1").expect("event ID"),
            at,
            vec![
                EntryCandidate::new(candidate("BTC", dec!(0.03), at), &strategy),
                EntryCandidate::new(candidate("ETH", dec!(0.04), at), &strategy),
            ],
            state(at),
            &entry_context,
        )
        .expect("sealed arbitration");

        assert_eq!(outcome.batch().records().len(), 8);
        assert!(
            outcome
                .batch()
                .records()
                .iter()
                .all(|record| record.causality_id() == outcome.batch().causality_id())
        );
        assert_eq!(
            outcome.state().broker().state(),
            crate::broker::BrokerState::PendingEntry
        );
        assert_eq!(outcome.state().outstanding_approvals(), 0);
        assert_eq!(outcome.batch().selected_market(), Some("ETH"));
    }

    #[test]
    fn cross_candidate_strategy_intent_fails_closed_before_ranking_or_consumption() {
        let at = timestamp(900_000_000_000);
        let malicious = CrossCandidateIntent {
            substituted: candidate("ETH", dec!(0.04), at),
        };
        let entry_context = context(at, EventAdmission::New);

        assert!(matches!(
            apply_entry(
                EventId::new("event-cross-candidate").expect("event ID"),
                at,
                vec![EntryCandidate::new(
                    candidate("BTC", dec!(0.03), at),
                    &malicious,
                )],
                state(at),
                &entry_context,
            ),
            Err(super::EngineError::IntentBindingMismatch)
        ));
    }

    #[test]
    fn rejected_quotes_are_discarded_so_repeated_rejections_cannot_starve_a_later_entry() {
        let at = timestamp(900_000_000_000);
        let reject_all = RejectAll;
        let context = context(at, EventAdmission::New);
        let mut state = state(at);

        for index in 0..65 {
            let event_id =
                EventId::new(format!("event-reject-{index}")).expect("distinct rejection event ID");
            let outcome = apply_entry(
                event_id,
                at,
                vec![EntryCandidate::new(
                    candidate("BTC", dec!(0.03), at),
                    &reject_all,
                )],
                state,
                &context,
            )
            .expect("rejected candidate must not retain a private approval");
            state = outcome.into_parts().0;
            assert_eq!(state.outstanding_approvals(), 0, "rejection {index}");
        }

        let accept_all = AcceptAll;
        let outcome = apply_entry(
            EventId::new("event-after-rejections").expect("event ID"),
            at,
            vec![EntryCandidate::new(
                candidate("ETH", dec!(0.04), at),
                &accept_all,
            )],
            state,
            &context,
        )
        .expect("later valid candidate must still be approved");

        assert_eq!(
            outcome.state().broker().state(),
            crate::broker::BrokerState::PendingEntry
        );
        assert_eq!(outcome.state().outstanding_approvals(), 0);
    }

    #[test]
    fn same_equity_with_attacker_supplied_matching_snapshot_digests_is_rejected() {
        let at = timestamp(900_000_000_000);
        let strategy = AcceptAll;
        let outcome = Engine::apply(
            EngineEvent::entry_arbitration(
                EventId::new("event-untrusted-snapshot").expect("event ID"),
                at,
                request(at).snapshot().clone(),
                vec![EntryCandidate::new(
                    candidate("BTC", dec!(0.03), at),
                    &strategy,
                )],
            ),
            state(at),
            &context(at, EventAdmission::New),
        )
        .expect("snapshot mismatch must be a deterministic no-op");

        assert!(
            outcome
                .batch()
                .records()
                .iter()
                .any(|record| matches!(record.record(), EngineRecord::SnapshotRejected))
        );
    }

    #[test]
    fn duplicate_admission_short_circuits_before_snapshot_validation_or_strategy_execution() {
        let at = timestamp(900_000_000_000);
        let strategy = CountingStrategy::new(RULES_FINGERPRINT);
        let initial = state(at);
        let outcome = Engine::apply(
            EngineEvent::entry_arbitration(
                EventId::new("event-duplicate").expect("event ID"),
                at,
                request(at).snapshot().clone(),
                vec![EntryCandidate::new(
                    candidate("BTC", dec!(0.03), at),
                    &strategy,
                )],
            ),
            initial,
            &context(at, EventAdmission::Duplicate),
        )
        .expect("duplicate must be a pure no-op");

        assert_eq!(strategy.calls(), 0);
        assert!(matches!(
            outcome.batch().records(),
            [record] if matches!(record.record(), EngineRecord::DuplicateIgnored)
        ));
    }

    #[test]
    fn stale_strategy_instance_cannot_pair_with_a_candidate_sealed_by_another_artifact() {
        let at = timestamp(900_000_000_000);
        let stale = CountingStrategy::new(
            "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
        );
        let context = context(at, EventAdmission::New);
        let outcome = apply_entry(
            EventId::new("event-stale-strategy").expect("event ID"),
            at,
            vec![EntryCandidate::new(
                candidate("BTC", dec!(0.03), at),
                &stale,
            )],
            state(at),
            &context,
        )
        .expect("artifact mismatch must be an auditable no-op");

        assert_eq!(stale.calls(), 0);
        assert!(outcome.batch().records().iter().any(|record| matches!(
            record.record(),
            EngineRecord::StrategyContextRejected { .. }
        )));
    }

    #[test]
    fn permissive_strategy_cannot_bypass_the_complete_public_cost_gate() {
        let at = timestamp(900_000_000_000);
        let strategy = AcceptAll;
        let context = context(at, EventAdmission::New);
        let outcome = apply_entry(
            EventId::new("event-permissive-cost").expect("event ID"),
            at,
            vec![EntryCandidate::new(
                candidate("BTC", dec!(0.001), at),
                &strategy,
            )],
            state(at),
            &context,
        )
        .expect("insufficient gross edge must be a deterministic no-op");

        assert_eq!(
            outcome.state().broker().state(),
            crate::broker::BrokerState::Flat
        );
        assert!(outcome.batch().records().iter().any(|record| matches!(
            record.record(),
            EngineRecord::CostRejected {
                reason: crate::strategy::CostRejection::InsufficientGrossEdge,
                ..
            }
        )));
    }

    #[test]
    fn stale_book_source_is_rejected_before_any_strategy_can_observe_costs() {
        let at = timestamp(900_000_000_000);
        let stale_at = timestamp(899_999_998_999);
        let strategy = CountingStrategy::new(RULES_FINGERPRINT);
        let outcome = Engine::apply(
            EngineEvent::entry_arbitration(
                EventId::new("event-stale-book").expect("event ID"),
                at,
                request(at).snapshot().clone(),
                vec![EntryCandidate::new(
                    candidate("BTC", dec!(0.03), at),
                    &strategy,
                )],
            ),
            state(at),
            &context_with_btc_book_time(at, EventAdmission::New, stale_at),
        )
        .expect("stale source must be an auditable no-op");

        assert_eq!(strategy.calls(), 0);
        assert!(
            outcome
                .batch()
                .records()
                .iter()
                .any(|record| matches!(record.record(), EngineRecord::SnapshotRejected))
        );
    }

    #[test]
    fn candidate_without_its_own_frozen_market_policy_is_rejected_before_quoting() {
        let at = timestamp(900_000_000_000);
        let strategy = CountingStrategy::new(RULES_FINGERPRINT);
        let state = EngineState::new(
            LedgerState::new(LedgerId::RulesOnly, at).expect("ledger"),
            broker(at),
            BTreeMap::from([(Market::new("BTC").expect("market"), policy_for("BTC", at))]),
        );
        let outcome = Engine::apply(
            EngineEvent::entry_arbitration(
                EventId::new("event-missing-eth-policy").expect("event ID"),
                at,
                request(at).snapshot().clone(),
                vec![
                    EntryCandidate::new(candidate("BTC", dec!(0.03), at), &strategy),
                    EntryCandidate::new(candidate("ETH", dec!(0.04), at), &strategy),
                ],
            ),
            state,
            &context(at, EventAdmission::New),
        )
        .expect("missing market policy must be a deterministic no-op");

        assert_eq!(strategy.calls(), 0);
        assert!(
            outcome
                .batch()
                .records()
                .iter()
                .any(|record| matches!(record.record(), EngineRecord::SnapshotRejected))
        );
    }

    #[test]
    fn fresh_same_market_book_cannot_reuse_a_policy_sized_from_another_depth_snapshot() {
        let at = timestamp(900_000_000_000);
        let strategy = CountingStrategy::new(RULES_FINGERPRINT);
        let outcome = Engine::apply(
            EngineEvent::entry_arbitration(
                EventId::new("event-book-policy-mismatch").expect("event ID"),
                at,
                request(at).snapshot().clone(),
                vec![EntryCandidate::new(
                    candidate("BTC", dec!(0.03), at),
                    &strategy,
                )],
            ),
            state(at),
            &context_with_btc_source(at, EventAdmission::New, at, 2),
        )
        .expect("book-policy mismatch must be a deterministic no-op");

        assert_eq!(strategy.calls(), 0);
        assert!(
            outcome
                .batch()
                .records()
                .iter()
                .any(|record| matches!(record.record(), EngineRecord::SnapshotRejected))
        );
    }

    #[test]
    fn executable_books_cannot_forge_or_cross_an_engine_owned_recovery_boundary() {
        let at = timestamp(900_000_000_000);
        let market = Market::new("BTC").expect("market");
        let context = context(at, EventAdmission::New);
        let no_recovery = Engine::apply(
            EngineEvent::ExecutableBook {
                event_id: EventId::new("event-no-recovery-book").expect("event ID"),
                at,
                book: book(market.clone(), at),
            },
            state(at),
            &context,
        );
        assert!(matches!(
            no_recovery,
            Err(super::EngineError::MissingMarketRecovery { .. })
        ));

        let recovered = Engine::apply(
            EngineEvent::MarketRecovered {
                event_id: EventId::new("event-btc-recovery").expect("event ID"),
                at,
                market: market.clone(),
            },
            state(at),
            &context,
        )
        .expect("recovery boundary")
        .into_parts()
        .0;
        let during_gap = Engine::apply(
            EngineEvent::ExecutableBook {
                event_id: EventId::new("event-during-gap-book").expect("event ID"),
                at,
                book: book(market, at),
            },
            recovered,
            &context,
        );
        assert!(matches!(
            during_gap,
            Err(super::EngineError::Broker(
                crate::broker::BrokerError::BookPredatesRecovery
            ))
        ));
    }

    #[test]
    fn stale_recovery_cannot_roll_back_a_newer_boundary() {
        let market = Market::new("BTC").expect("market");
        let context = context(timestamp(900_000_000_000), EventAdmission::New);
        let recovered_at = timestamp(100);
        let newest = Engine::apply(
            EngineEvent::MarketRecovered {
                event_id: EventId::new("event-recovery-100").expect("event ID"),
                at: recovered_at,
                market: market.clone(),
            },
            state(timestamp(0)),
            &context,
        )
        .expect("initial recovery")
        .into_parts()
        .0;
        let stale = Engine::apply(
            EngineEvent::MarketRecovered {
                event_id: EventId::new("event-recovery-50").expect("event ID"),
                at: timestamp(50),
                market: market.clone(),
            },
            newest,
            &context,
        )
        .expect("stale recovery must be an auditable no-op");
        assert!(
            stale
                .batch()
                .records()
                .iter()
                .any(|record| matches!(record.record(), EngineRecord::RecoveryRejected { .. }))
        );
        let before_boundary = Engine::apply(
            EngineEvent::ExecutableBook {
                event_id: EventId::new("event-book-90").expect("event ID"),
                at: timestamp(90),
                book: book(market, timestamp(90)),
            },
            stale.into_parts().0,
            &context,
        );
        assert!(matches!(
            before_boundary,
            Err(super::EngineError::Broker(
                crate::broker::BrokerError::BookPredatesRecovery
            ))
        ));
    }

    #[test]
    fn executable_entry_book_opens_the_isolated_ledger_from_the_actual_broker_fill() {
        let at = timestamp(900_000_000_000);
        let strategy = AcceptAll;
        let entry_context = context(at, EventAdmission::New);
        let entry = apply_entry(
            EventId::new("event-queue-entry").expect("event ID"),
            at,
            vec![EntryCandidate::new(
                candidate("BTC", dec!(0.03), at),
                &strategy,
            )],
            state(at),
            &entry_context,
        )
        .expect("entry queue");
        let (queued_state, _) = entry.into_parts();
        let recovery = Engine::apply(
            EngineEvent::MarketRecovered {
                event_id: EventId::new("event-btc-recovered").expect("event ID"),
                at,
                market: Market::new("BTC").expect("market"),
            },
            queued_state,
            &entry_context,
        )
        .expect("recovery boundary");
        let (state, _) = recovery.into_parts();
        let execution_at = timestamp(i128::from(at.value()) + 1);
        let outcome = Engine::apply(
            EngineEvent::ExecutableBook {
                event_id: EventId::new("event-entry-fill").expect("event ID"),
                at: execution_at,
                book: book(Market::new("BTC").expect("market"), execution_at),
            },
            state,
            &entry_context,
        )
        .expect("entry fill transition");

        assert_eq!(
            outcome.state().broker().state(),
            crate::broker::BrokerState::Open
        );
        assert_eq!(
            outcome
                .state()
                .ledger()
                .position()
                .expect("ledger position")
                .market()
                .as_str(),
            "BTC"
        );
        assert!(outcome.batch().records().iter().any(|record| matches!(
            record.record(),
            EngineRecord::LedgerApplied {
                transition,
            } if transition.kind() == crate::ledger::LedgerTransitionKind::PositionOpened
        )));
    }

    #[test]
    fn partial_exit_settles_only_the_broker_reported_visible_fill() {
        let at = timestamp(900_000_000_000);
        let context = context(at, EventAdmission::New);
        let market = Market::new("BTC").expect("market");
        let opened = opened_btc_state(at);
        let opening_quantity = opened
            .ledger()
            .position()
            .expect("opened ledger position")
            .quantity()
            .value();
        let request_at = timestamp(i128::from(at.value()) + 2);
        let exiting = Engine::apply(
            EngineEvent::ExitRequested {
                event_id: EventId::new("event-partial-exit-request").expect("event ID"),
                at: request_at,
                reason: crate::broker::ExitReason::Strategy,
                market: market.clone(),
                price: Price::new(dec!(100)).expect("mark"),
                event_time: request_at,
                received_at: request_at,
            },
            opened,
            &context,
        )
        .expect("exit request")
        .into_parts()
        .0;
        let execution_at = timestamp(i128::from(at.value()) + 3);
        let outcome = Engine::apply(
            EngineEvent::ExecutableBook {
                event_id: EventId::new("event-partial-exit-book").expect("event ID"),
                at: execution_at,
                book: book_with_quantity(market, execution_at, 2, dec!(0.1)),
            },
            exiting,
            &context,
        )
        .expect("partial visible fill");

        let remaining = outcome
            .state()
            .ledger()
            .position()
            .expect("residual ledger position")
            .quantity()
            .value();
        assert!(remaining > Decimal::ZERO && remaining < opening_quantity);
        assert_eq!(
            outcome.state().broker().state(),
            crate::broker::BrokerState::NormalExit
        );
        assert!(outcome.batch().records().iter().any(|record| matches!(
            record.record(),
            EngineRecord::LedgerApplied {
                transition,
            } if transition.kind() == crate::ledger::LedgerTransitionKind::PositionReduced
        )));
    }

    #[test]
    fn signed_broker_funding_maps_to_debit_and_credit_ledger_transitions() {
        let at = timestamp(900_000_000_000);
        let context = context(at, EventAdmission::New);
        let market = Market::new("BTC").expect("market");
        let debit_at = timestamp(i128::from(at.value()) + 2);
        let before = opened_btc_state(at);
        let debit = Engine::apply(
            EngineEvent::FundingObserved {
                event_id: EventId::new("event-funding-debit").expect("event ID"),
                at: debit_at,
                market: market.clone(),
                venue_at: debit_at,
                received_at: debit_at,
                rate: crate::event::FundingRate::new(dec!(0.001)),
                mark_price: Price::new(dec!(100)).expect("mark"),
            },
            before,
            &context,
        )
        .expect("funding debit");
        assert!(debit.state().ledger().funding_paid().value() > Decimal::ZERO);
        assert!(debit.batch().records().iter().any(|record| matches!(
            record.record(),
            EngineRecord::LedgerApplied {
                transition,
            } if transition.kind() == crate::ledger::LedgerTransitionKind::FundingApplied
        )));

        let credit_at = timestamp(i128::from(at.value()) + 3);
        let credit = Engine::apply(
            EngineEvent::FundingObserved {
                event_id: EventId::new("event-funding-credit").expect("event ID"),
                at: credit_at,
                market,
                venue_at: credit_at,
                received_at: credit_at,
                rate: crate::event::FundingRate::new(dec!(-0.001)),
                mark_price: Price::new(dec!(100)).expect("mark"),
            },
            debit.into_parts().0,
            &context,
        )
        .expect("funding credit");
        assert!(credit.state().ledger().funding_received().value() > Decimal::ZERO);
    }

    #[test]
    fn capped_liquidation_forfeits_only_live_isolated_collateral() {
        let at = timestamp(900_000_000_000);
        let context = context(at, EventAdmission::New);
        let market = Market::new("BTC").expect("market");
        let mark_at = timestamp(i128::from(at.value()) + 2);
        let triggered = Engine::apply(
            EngineEvent::MarketMark {
                event_id: EventId::new("event-liquidation-mark").expect("event ID"),
                at: mark_at,
                market: market.clone(),
                price: Price::new(dec!(80)).expect("mark"),
                event_time: mark_at,
                received_at: mark_at,
            },
            opened_btc_state(at),
            &context,
        )
        .expect("liquidation trigger")
        .into_parts()
        .0;
        assert_eq!(
            triggered.broker().state(),
            crate::broker::BrokerState::MandatoryExit
        );

        let execution_at = timestamp(i128::from(at.value()) + 3);
        let outcome = Engine::apply(
            EngineEvent::ExecutableBook {
                event_id: EventId::new("event-liquidation-gap-book").expect("event ID"),
                at: execution_at,
                book: book_with_prices(market, execution_at, 2, dec!(70), dec!(71), dec!(10)),
            },
            triggered,
            &context,
        )
        .expect("capped liquidation fill");

        assert_eq!(
            outcome.state().broker().state(),
            crate::broker::BrokerState::Liquidated
        );
        assert!(outcome.state().ledger().position().is_none());
        assert_eq!(
            outcome.state().ledger().isolated_collateral(),
            Decimal::ZERO
        );
        assert!(outcome.batch().records().iter().any(|record| matches!(
            record.record(),
            EngineRecord::LedgerApplied {
                transition,
            } if transition.kind() == crate::ledger::LedgerTransitionKind::PositionLiquidated
        )));
    }

    #[test]
    fn equal_sized_liquidation_fill_and_backstop_remain_distinct_ledger_transitions() {
        let at = timestamp(900_000_000_000);
        let context = context(at, EventAdmission::New);
        let market = Market::new("BTC").expect("market");
        let opened = opened_btc_state(at);
        let half_quantity = opened
            .ledger()
            .position()
            .expect("opened ledger position")
            .quantity()
            .value()
            .checked_div(Decimal::TWO)
            .expect("half quantity");
        let mark_at = timestamp(i128::from(at.value()) + 2);
        let triggered = Engine::apply(
            EngineEvent::MarketMark {
                event_id: EventId::new("event-equal-liquidation-mark").expect("event ID"),
                at: mark_at,
                market: market.clone(),
                price: Price::new(dec!(80)).expect("mark"),
                event_time: mark_at,
                received_at: mark_at,
            },
            opened,
            &context,
        )
        .expect("liquidation trigger")
        .into_parts()
        .0;
        let execution_at = timestamp(i128::from(at.value()) + 3);
        let outcome = Engine::apply(
            EngineEvent::ExecutableBook {
                event_id: EventId::new("event-equal-liquidation-book").expect("event ID"),
                at: execution_at,
                book: book_with_prices(market, execution_at, 2, dec!(70), dec!(71), half_quantity),
            },
            triggered,
            &context,
        )
        .expect("partial fill plus backstop");

        assert!(outcome.state().ledger().position().is_none());
        let ledger_kinds = outcome
            .batch()
            .records()
            .iter()
            .filter_map(|record| match record.record() {
                EngineRecord::LedgerApplied { transition } => Some(transition.kind()),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert!(ledger_kinds.contains(&crate::ledger::LedgerTransitionKind::PositionReduced));
        assert!(ledger_kinds.contains(&crate::ledger::LedgerTransitionKind::PositionLiquidated));
    }

    #[test]
    fn end_of_data_keeps_actual_ledger_exposure_unresolved() {
        let at = timestamp(900_000_000_000);
        let context = context(at, EventAdmission::New);
        let terminal_at = timestamp(i128::from(at.value()) + 2);
        let outcome = Engine::apply(
            EngineEvent::EndOfData {
                event_id: EventId::new("event-end-of-data").expect("event ID"),
                at: terminal_at,
            },
            opened_btc_state(at),
            &context,
        )
        .expect("end of data");

        assert_eq!(
            outcome.state().broker().state(),
            crate::broker::BrokerState::Unresolved
        );
        assert!(outcome.state().ledger().position().is_some());
        assert!(outcome.batch().records().iter().any(|record| matches!(
            record.record(),
            EngineRecord::BrokerApplied { transition }
                if transition.state() == crate::broker::BrokerState::Unresolved
        )));
    }

    #[test]
    fn end_of_data_rejects_later_funding_without_broker_or_ledger_mutation() {
        let at = timestamp(900_000_000_000);
        let context = context(at, EventAdmission::New);
        let terminal_at = timestamp(i128::from(at.value()) + 2);
        let unresolved = Engine::apply(
            EngineEvent::EndOfData {
                event_id: EventId::new("event-terminal-before-funding").expect("event ID"),
                at: terminal_at,
            },
            opened_btc_state(at),
            &context,
        )
        .expect("end of data")
        .into_parts()
        .0;
        let before = (
            unresolved.ledger().cash(),
            unresolved.ledger().isolated_collateral(),
            unresolved.ledger().funding_paid(),
            unresolved.ledger().funding_received(),
        );
        let funding_at = timestamp(i128::from(at.value()) + 3);
        let outcome = Engine::apply(
            EngineEvent::FundingObserved {
                event_id: EventId::new("event-terminal-funding").expect("event ID"),
                at: funding_at,
                market: Market::new("BTC").expect("market"),
                venue_at: funding_at,
                received_at: funding_at,
                rate: crate::event::FundingRate::new(dec!(0.001)),
                mark_price: Price::new(dec!(100)).expect("mark"),
            },
            unresolved,
            &context,
        )
        .expect("terminal funding rejection");

        assert_eq!(
            (
                outcome.state().ledger().cash(),
                outcome.state().ledger().isolated_collateral(),
                outcome.state().ledger().funding_paid(),
                outcome.state().ledger().funding_received(),
            ),
            before
        );
        assert_eq!(
            outcome.state().broker().state(),
            crate::broker::BrokerState::Unresolved
        );
        assert!(outcome.batch().records().iter().any(|record| matches!(
            record.record(),
            EngineRecord::TerminalInputRejected {
                broker_state: crate::broker::BrokerState::Unresolved
            }
        )));
        assert!(outcome.batch().records().iter().all(|record| !matches!(
            record.record(),
            EngineRecord::BrokerApplied { .. } | EngineRecord::LedgerApplied { .. }
        )));
    }

    fn state(at: TimestampNs) -> EngineState {
        let risk_policies = ["BTC", "ETH"]
            .into_iter()
            .map(|market| (Market::new(market).expect("market"), policy_for(market, at)))
            .collect();
        EngineState::new(
            LedgerState::new(LedgerId::RulesOnly, at).expect("ledger"),
            broker(at),
            risk_policies,
        )
    }

    fn opened_btc_state(at: TimestampNs) -> EngineState {
        let strategy = AcceptAll;
        let context = context(at, EventAdmission::New);
        let queued = apply_entry(
            EventId::new("event-open-helper-queue").expect("event ID"),
            at,
            vec![EntryCandidate::new(
                candidate("BTC", dec!(0.03), at),
                &strategy,
            )],
            state(at),
            &context,
        )
        .expect("queued entry")
        .into_parts()
        .0;
        let recovered = Engine::apply(
            EngineEvent::MarketRecovered {
                event_id: EventId::new("event-open-helper-recovery").expect("event ID"),
                at,
                market: Market::new("BTC").expect("market"),
            },
            queued,
            &context,
        )
        .expect("recovery")
        .into_parts()
        .0;
        let execution_at = timestamp(i128::from(at.value()) + 1);
        Engine::apply(
            EngineEvent::ExecutableBook {
                event_id: EventId::new("event-open-helper-book").expect("event ID"),
                at: execution_at,
                book: book(Market::new("BTC").expect("market"), execution_at),
            },
            recovered,
            &context,
        )
        .expect("entry fill")
        .into_parts()
        .0
    }

    fn apply_entry<'strategy>(
        event_id: EventId,
        at: TimestampNs,
        candidates: Vec<EntryCandidate<'strategy>>,
        state: EngineState,
        context: &EngineContext,
    ) -> Result<super::EngineOutcome, super::EngineError> {
        let snapshot = super::canonical_snapshot(&state, &event_id, at, &candidates, context)?;
        Engine::apply(
            EngineEvent::entry_arbitration(event_id, at, snapshot, candidates),
            state,
            context,
        )
    }

    fn broker(at: TimestampNs) -> PaperBroker {
        PaperBroker::new(
            BrokerConfig::new(usdc(dec!(1)), DurationNs::new(1_000).expect("duration"))
                .expect("broker config"),
            BrokerRunContext::new(
                RunId::new("paper-run").expect("run ID"),
                digest('a'),
                digest('b'),
            )
            .expect("broker context"),
            at,
        )
    }

    fn policy_for(market: &str, at: TimestampNs) -> crate::risk::sizing::RiskPolicy {
        let market = Market::new(market).expect("market");
        let snapshot = RiskSnapshot::new(
            at,
            at,
            usdc(dec!(100)),
            digest('a'),
            book(market, at).commitment_digest(),
            digest('c'),
            digest('d'),
            digest('e'),
        )
        .expect("risk snapshot");
        request(at).with_snapshot(snapshot).into_policy()
    }

    fn context(at: TimestampNs, admission: EventAdmission) -> EngineContext {
        context_with_btc_book_time(at, admission, at)
    }

    fn context_with_btc_book_time(
        at: TimestampNs,
        admission: EventAdmission,
        btc_book_time: TimestampNs,
    ) -> EngineContext {
        context_with_btc_source(at, admission, btc_book_time, 1)
    }

    fn context_with_btc_source(
        at: TimestampNs,
        admission: EventAdmission,
        btc_book_time: TimestampNs,
        btc_sequence: u64,
    ) -> EngineContext {
        let books = ["BTC", "ETH"]
            .into_iter()
            .map(|market| {
                let market = Market::new(market).expect("market");
                let book_time = if market.as_str() == "BTC" {
                    btc_book_time
                } else {
                    at
                };
                let sequence = if market.as_str() == "BTC" {
                    btc_sequence
                } else {
                    1
                };
                (
                    market.clone(),
                    book_with_sequence(market, book_time, sequence),
                )
            })
            .collect::<BTreeMap<_, _>>();
        EngineContext::new(
            admission,
            SnapshotBindings::new(books, active_universe(at)),
            StrategyFingerprints::new(digest('f'), digest('g')),
        )
    }

    fn book(market: Market, at: TimestampNs) -> OrderBook {
        book_with_sequence(market, at, 1)
    }

    fn book_with_sequence(market: Market, at: TimestampNs, sequence: u64) -> OrderBook {
        book_with_quantity(market, at, sequence, dec!(10))
    }

    fn book_with_quantity(
        market: Market,
        at: TimestampNs,
        sequence: u64,
        quantity: Decimal,
    ) -> OrderBook {
        book_with_prices(market, at, sequence, dec!(99), dec!(100), quantity)
    }

    fn book_with_prices(
        market: Market,
        at: TimestampNs,
        sequence: u64,
        bid: Decimal,
        ask: Decimal,
        quantity: Decimal,
    ) -> OrderBook {
        let event = MarketEvent::book_snapshot(
            at,
            at,
            market,
            BookSnapshot::new(
                sequence,
                vec![BookLevel::new(
                    Price::new(bid).expect("bid"),
                    Quantity::new(quantity).expect("bid quantity"),
                )],
                vec![BookLevel::new(
                    Price::new(ask).expect("ask"),
                    Quantity::new(quantity).expect("ask quantity"),
                )],
            ),
        )
        .expect("book event");
        OrderBook::apply_snapshot(None, &event, DurationNs::new(0).expect("duration"))
            .expect("order book")
    }

    fn active_universe(at: TimestampNs) -> crate::universe::UniverseActivation {
        let snapshot = UniverseSelector::select(
            timestamp(0),
            [universe_candidate("BTC"), universe_candidate("ETH")],
        )
        .expect("universe snapshot");
        UniverseSelector::activate(&snapshot, None, at).expect("active universe")
    }

    fn universe_candidate(market: &str) -> UniverseCandidate {
        let depth = SidedDepth::new(usdc(dec!(50_000)), usdc(dec!(60_000)), usdc(dec!(70_000)))
            .expect("depth");
        UniverseCandidate::new(
            Market::new(market).expect("market"),
            true,
            MarketDataAvailability::new(ListingState::Active, true, true, true, 20),
            HistoryQuality::new(30, dec!(0.995), true, dec!(1)).expect("history"),
            UniverseLiquidity::new(
                usdc(dec!(5_000_000)),
                usdc(dec!(1_000_000)),
                Bps::new(dec!(15)).expect("spread"),
                DepthProfile::new(depth.clone(), depth),
            ),
        )
    }

    fn request(at: TimestampNs) -> RiskRequest {
        let tiers = MaintenanceTiers::new(vec![
            MaintenanceTier::new(usdc(dec!(0)), None, dec!(0.025), usdc(dec!(0)))
                .expect("maintenance tier"),
        ])
        .expect("maintenance tiers");
        let snapshot = RiskSnapshot::new(
            at,
            at,
            usdc(dec!(100)),
            digest('a'),
            digest('b'),
            digest('c'),
            digest('d'),
            digest('e'),
        )
        .expect("risk snapshot");
        let constraints = VenueConstraints::new(
            3,
            usdc(dec!(1)),
            usdc(dec!(500)),
            usdc(dec!(500)),
            Leverage::new(20).expect("leverage"),
            tiers,
        )
        .expect("venue constraints");
        let costs = ConservativeCosts::new(
            dec!(0.00075),
            dec!(0.00075),
            ImpactCurve::new(vec![
                ImpactBand::new(None, dec!(0.0005), dec!(0.001)).expect("impact band"),
            ])
            .expect("impact curve"),
            dec!(0.0001),
            dec!(0.0002),
            4,
        )
        .expect("conservative costs");
        RiskRequest::new(
            snapshot,
            constraints,
            costs,
            RiskLimits::new(usdc(dec!(1)), dec!(0.25), dec!(2.5)).expect("risk limits"),
        )
    }

    fn candidate(market: &str, gross_edge: Decimal, at: TimestampNs) -> SignalCandidate {
        SignalCandidate::new(CandidateSpecification {
            strategy: StrategyKind::RulesOnly,
            market: Market::new(market).expect("market"),
            side: Side::Buy,
            sleeve: Sleeve::FifteenMinute,
            decision_time: at,
            gross_edge,
            reference_entry: Price::new(dec!(100)).expect("entry"),
            stop: Price::new(dec!(99)).expect("stop"),
            target: Price::new(dec!(102)).expect("target"),
            time_exit: TimestampNs::new(i128::from(at.value()) + 1_000).expect("time exit"),
            snapshot_digest: digest('a'),
            universe_digest: active_universe(at)
                .universe()
                .expect("tradeable universe")
                .digest()
                .to_owned(),
            history_digest: digest('d'),
            strategy_fingerprint: digest('f'),
            explanation_json: "{}".to_owned(),
        })
        .expect("candidate")
    }

    fn timestamp(value: i128) -> TimestampNs {
        TimestampNs::new(value).expect("timestamp")
    }

    fn usdc(value: Decimal) -> Usdc {
        Usdc::new(value).expect("USDC")
    }

    fn digest(value: char) -> String {
        std::iter::repeat_n(value, 64).collect()
    }
}
