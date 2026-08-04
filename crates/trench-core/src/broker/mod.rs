//! Deterministic primary-taker paper execution over sealed executable books.
//!
//! No order leaves this module. Entries require a crate-private sealed risk
//! approval; all book use requires an explicit fresh, post-recovery proof.

use rust_decimal::Decimal;
use thiserror::Error;

use crate::book::OrderBook;
use crate::domain::{
    Bps, DomainError, EventId, Leverage, Market, Price, Quantity, RunId, Side, Usdc,
};
use crate::event::{DurationNs, EventError, FundingRate, TimestampNs};
use crate::ledger::PositionSide;
use crate::risk::liquidation::{
    LiquidationInput, LiquidationResult, MaintenanceTier, MaintenanceTiers, calculate,
};
use crate::risk::sizing::ApprovedOrder;

use self::cost::{
    CostError, ExecutionCost, FeeBreakdown, SignedUsdc, TakerFeeSchedule, attribute_taker_execution,
};
use self::fill::{FillError, QuantityWalk, walk_visible_quantity, walk_visible_quantity_to_price};

pub mod cost;
pub mod fill;

const NORMAL_EXIT_BAND: Decimal = Decimal::from_parts(50, 0, 0, false, 0);
const MANDATORY_WIDENING: Decimal = Decimal::from_parts(25, 0, 0, false, 0);
const MANDATORY_CAP: Decimal = Decimal::from_parts(200, 0, 0, false, 0);
const NORMAL_EXIT_GRACE_NS: i64 = 5_000_000_000;

/// The exhaustive state machine for one isolated paper ledger.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BrokerState {
    /// A sealed order is waiting for the first strictly later executable book.
    PendingEntry,
    /// An actual visible-book fill created the only permitted position.
    Open,
    /// A normal reduce-only exit is retrying at 50 bps for up to five seconds.
    NormalExit,
    /// A stop, breaker, dust fill, or residual is reducing at widening bands.
    MandatoryExit,
    /// No pending order or exposure remains.
    Flat,
    /// A mark-triggered liquidation completed through book/backstop handling.
    Liquidated,
    /// The data stream ended with actual exposure still outstanding.
    Unresolved,
}

/// Why an exit transition was requested.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExitReason {
    /// Strategy discretionary exit.
    Strategy,
    /// Mark reached take-profit.
    TakeProfit,
    /// Frozen holding horizon elapsed.
    Time,
    /// A qualified opposite signal requested a close.
    OppositeSignal,
    /// Mark reached invalidation stop.
    Stop,
    /// Daily, weekly, or hard drawdown breaker forced reduction.
    Breaker,
    /// A partial entry exists below venue minimum and must be removed.
    Dust,
    /// Mark crossed the current tier-valid isolated liquidation threshold.
    Liquidation,
}

impl ExitReason {
    const fn mandatory(self) -> bool {
        matches!(
            self,
            Self::Stop | Self::Breaker | Self::Dust | Self::Liquidation
        )
    }
}

/// Purpose of one primary taker walk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionRole {
    /// Initial entry IOC.
    Entry,
    /// A normal strategy/target/time/opposite-signal IOC.
    NormalExit,
    /// A mandatory stop/breaker/dust/timeout IOC.
    MandatoryExit,
    /// A venue-like liquidation IOC after a mark breach.
    Liquidation,
}

/// Observed latency sample class, never a synthetic backtest delay.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LatencyKind {
    /// Pipeline completion to first selected entry book.
    DecisionToBook,
    /// Exit/mark trigger to first selected exit book.
    TriggerToBook,
}

/// Immutable binding attached to every measured latency sample.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerRunContext {
    run_id: RunId,
    run_digest: String,
    deployment_digest: String,
}

impl BrokerRunContext {
    /// Creates a complete source binding for append-only latency evidence.
    ///
    /// # Errors
    ///
    /// Rejects non-BLAKE3 digest bindings.
    pub fn new(
        run_id: RunId,
        run_digest: impl Into<String>,
        deployment_digest: impl Into<String>,
    ) -> Result<Self, BrokerInputError> {
        let run_digest = run_digest.into();
        let deployment_digest = deployment_digest.into();
        for (field, digest) in [
            ("run", run_digest.as_str()),
            ("deployment", deployment_digest.as_str()),
        ] {
            if !is_blake3_digest(digest) {
                return Err(BrokerInputError::InvalidDigest { field });
            }
        }
        Ok(Self {
            run_id,
            run_digest,
            deployment_digest,
        })
    }

    /// Returns the immutable run identifier.
    #[must_use]
    pub const fn run_id(&self) -> &RunId {
        &self.run_id
    }

    /// Returns the immutable run digest.
    #[must_use]
    pub fn run_digest(&self) -> &str {
        &self.run_digest
    }

    /// Returns the immutable deployment digest.
    #[must_use]
    pub fn deployment_digest(&self) -> &str {
        &self.deployment_digest
    }
}

/// Exact source evidence that a market completed recovery before execution.
///
/// Construction is crate-private so only the engine may translate its market
/// readiness/gap-recovery state into an execution proof.
#[allow(
    dead_code,
    reason = "Task 13 creates recovered execution proofs from market readiness"
)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MarketExecutionReady {
    market: Market,
    recovered_at: TimestampNs,
}

#[allow(
    dead_code,
    reason = "Task 13 creates recovered execution proofs from market readiness"
)]
impl MarketExecutionReady {
    pub(crate) const fn new(market: Market, recovered_at: TimestampNs) -> Self {
        Self {
            market,
            recovered_at,
        }
    }
}

/// A fresh immutable full book that is eligible for one broker execution attempt.
#[allow(
    dead_code,
    reason = "Task 13 is the sole production source of executable books"
)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ExecutableBook {
    book: OrderBook,
    as_of_time: TimestampNs,
}

#[allow(
    dead_code,
    reason = "Task 13 is the sole production source of executable books"
)]
impl ExecutableBook {
    /// Validates fresh event/receipt timing and a completed market-recovery proof.
    pub(crate) fn new(
        book: OrderBook,
        as_of_time: TimestampNs,
        maximum_age: DurationNs,
        readiness: &MarketExecutionReady,
    ) -> Result<Self, BrokerError> {
        if book.market() != &readiness.market {
            return Err(BrokerError::BookMarketMismatch);
        }
        if book.event_time() <= readiness.recovered_at
            || book.received_at() <= readiness.recovered_at
        {
            return Err(BrokerError::BookPredatesRecovery);
        }
        if book.event_time() > as_of_time || book.received_at() > as_of_time {
            return Err(BrokerError::BookFromFuture);
        }
        let age = as_of_time.checked_duration_since(book.event_time())?;
        if age > maximum_age {
            return Err(BrokerError::StaleExecutableBook { age, maximum_age });
        }
        Ok(Self { book, as_of_time })
    }

    fn book(&self) -> &OrderBook {
        &self.book
    }

    fn as_of_time(&self) -> TimestampNs {
        self.as_of_time
    }
}

/// A fresh, market-bound mark observation eligible to affect broker state.
#[allow(
    dead_code,
    reason = "Task 13 creates fresh market observations from normalized feeds"
)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ExecutableMark {
    market: Market,
    event_id: EventId,
    price: Price,
    event_time: TimestampNs,
    received_at: TimestampNs,
    as_of_time: TimestampNs,
}

#[allow(
    dead_code,
    reason = "Task 13 creates fresh market observations from normalized feeds"
)]
impl ExecutableMark {
    #[allow(
        clippy::too_many_arguments,
        reason = "the immutable mark source is complete"
    )]
    pub(crate) fn new(
        market: Market,
        event_id: EventId,
        price: Price,
        event_time: TimestampNs,
        received_at: TimestampNs,
        as_of_time: TimestampNs,
        maximum_age: DurationNs,
        readiness: &MarketExecutionReady,
    ) -> Result<Self, BrokerError> {
        if market != readiness.market {
            return Err(BrokerError::BookMarketMismatch);
        }
        if event_time <= readiness.recovered_at || received_at <= readiness.recovered_at {
            return Err(BrokerError::BookPredatesRecovery);
        }
        if event_time > received_at || received_at > as_of_time {
            return Err(BrokerError::BookFromFuture);
        }
        let age = as_of_time.checked_duration_since(event_time)?;
        if age > maximum_age {
            return Err(BrokerError::StaleExecutableBook { age, maximum_age });
        }
        Ok(Self {
            market,
            event_id,
            price,
            event_time,
            received_at,
            as_of_time,
        })
    }

    const fn price(&self) -> Price {
        self.price
    }
    const fn received_at(&self) -> TimestampNs {
        self.received_at
    }
    const fn event_time(&self) -> TimestampNs {
        self.event_time
    }
    const fn as_of_time(&self) -> TimestampNs {
        self.as_of_time
    }
    const fn market(&self) -> &Market {
        &self.market
    }
    const fn event_id(&self) -> &EventId {
        &self.event_id
    }
}

/// Stable ordering for distinct normalized market events at one venue instant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum MarketObservationKind {
    Mark,
    Funding,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct MarketObservationCursor {
    event_time: TimestampNs,
    kind: MarketObservationKind,
    event_id: EventId,
}

/// A fresh, source-identified funding observation eligible to mutate equity.
///
/// Funding carries its own venue identity and mark. It cannot be assembled
/// from an unrelated mark plus an arbitrary rate during replay.
#[allow(
    dead_code,
    reason = "Task 13 creates sealed funding observations from normalized feeds"
)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ExecutableFunding {
    market: Market,
    event_id: EventId,
    venue_at: TimestampNs,
    received_at: TimestampNs,
    as_of_time: TimestampNs,
    rate: FundingRate,
    mark_price: Price,
}

#[allow(
    dead_code,
    reason = "Task 13 creates sealed funding observations from normalized feeds"
)]
impl ExecutableFunding {
    #[allow(
        clippy::too_many_arguments,
        reason = "the immutable funding source is complete"
    )]
    pub(crate) fn new(
        market: Market,
        event_id: EventId,
        venue_at: TimestampNs,
        received_at: TimestampNs,
        as_of_time: TimestampNs,
        rate: FundingRate,
        mark_price: Price,
        maximum_age: DurationNs,
        readiness: &MarketExecutionReady,
    ) -> Result<Self, BrokerError> {
        if market != readiness.market {
            return Err(BrokerError::BookMarketMismatch);
        }
        if venue_at <= readiness.recovered_at || received_at <= readiness.recovered_at {
            return Err(BrokerError::BookPredatesRecovery);
        }
        if venue_at > received_at || received_at > as_of_time {
            return Err(BrokerError::BookFromFuture);
        }
        let age = as_of_time.checked_duration_since(venue_at)?;
        if age > maximum_age {
            return Err(BrokerError::StaleExecutableBook { age, maximum_age });
        }
        Ok(Self {
            market,
            event_id,
            venue_at,
            received_at,
            as_of_time,
            rate,
            mark_price,
        })
    }

    const fn venue_at(&self) -> TimestampNs {
        self.venue_at
    }

    const fn received_at(&self) -> TimestampNs {
        self.received_at
    }

    const fn as_of_time(&self) -> TimestampNs {
        self.as_of_time
    }

    const fn rate(&self) -> FundingRate {
        self.rate
    }

    const fn mark_price(&self) -> Price {
        self.mark_price
    }

    const fn market(&self) -> &Market {
        &self.market
    }

    const fn event_id(&self) -> &EventId {
        &self.event_id
    }
}

/// Frozen broker thresholds recorded with the run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BrokerConfig {
    minimum_notional: Usdc,
    maximum_book_age: DurationNs,
    normal_exit_band: Bps,
    normal_exit_grace: DurationNs,
    mandatory_widening: Bps,
    mandatory_cap: Bps,
}

#[allow(
    dead_code,
    reason = "state-machine consumption begins with Task 13 engine wiring"
)]
impl BrokerConfig {
    /// Creates a complete, finite paper-execution configuration.
    ///
    /// # Errors
    ///
    /// Rejects zero minimum notional and any unrepresentable fixed constraint.
    pub fn new(
        minimum_notional: Usdc,
        maximum_book_age: DurationNs,
    ) -> Result<Self, BrokerInputError> {
        if minimum_notional.value().is_zero() {
            return Err(BrokerInputError::ZeroMinimumNotional);
        }
        Ok(Self {
            minimum_notional,
            maximum_book_age,
            normal_exit_band: Bps::new(NORMAL_EXIT_BAND)
                .map_err(|_| BrokerInputError::InvalidFixedConstraint)?,
            normal_exit_grace: DurationNs::new(i128::from(NORMAL_EXIT_GRACE_NS))
                .map_err(|_| BrokerInputError::InvalidFixedConstraint)?,
            mandatory_widening: Bps::new(MANDATORY_WIDENING)
                .map_err(|_| BrokerInputError::InvalidFixedConstraint)?,
            mandatory_cap: Bps::new(MANDATORY_CAP)
                .map_err(|_| BrokerInputError::InvalidFixedConstraint)?,
        })
    }

    /// Returns the actual-fill dust boundary.
    #[must_use]
    pub const fn minimum_notional(self) -> Usdc {
        self.minimum_notional
    }

    /// Returns the maximum source-event age accepted at selection time.
    #[must_use]
    pub const fn maximum_book_age(self) -> DurationNs {
        self.maximum_book_age
    }

    const fn normal_exit_band(self) -> Bps {
        self.normal_exit_band
    }
    const fn normal_exit_grace(self) -> DurationNs {
        self.normal_exit_grace
    }
    const fn mandatory_widening(self) -> Bps {
        self.mandatory_widening
    }
    const fn mandatory_cap(self) -> Bps {
        self.mandatory_cap
    }
}

/// Exact actual position state exposed to the engine and journal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerPosition {
    market: Market,
    side: PositionSide,
    quantity: Quantity,
    entry_price: Price,
    stop: Price,
    target: Price,
    leverage: Leverage,
    liquidation: Option<LiquidationResult>,
    funding: SignedUsdc,
}

impl BrokerPosition {
    #[must_use]
    pub const fn market(&self) -> &Market {
        &self.market
    }
    #[must_use]
    pub const fn side(&self) -> PositionSide {
        self.side
    }
    #[must_use]
    pub const fn quantity(&self) -> Quantity {
        self.quantity
    }
    #[must_use]
    pub const fn entry_price(&self) -> Price {
        self.entry_price
    }
    #[must_use]
    pub const fn stop(&self) -> Price {
        self.stop
    }
    #[must_use]
    pub const fn target(&self) -> Price {
        self.target
    }
    #[must_use]
    pub const fn leverage(&self) -> Leverage {
        self.leverage
    }
    /// Returns the current revalued tier-valid liquidation result, if solvent.
    #[must_use]
    pub const fn liquidation(&self) -> Option<LiquidationResult> {
        self.liquidation
    }
    #[must_use]
    pub const fn funding(&self) -> SignedUsdc {
        self.funding
    }
}

/// One append-only state transition for atomic engine persistence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerTransition {
    at: TimestampNs,
    state: BrokerState,
    records: Vec<BrokerRecord>,
}

impl BrokerTransition {
    #[must_use]
    pub const fn at(&self) -> TimestampNs {
        self.at
    }
    #[must_use]
    pub const fn state(&self) -> BrokerState {
        self.state
    }
    #[must_use]
    pub fn records(&self) -> &[BrokerRecord] {
        &self.records
    }
}

/// Complete journal evidence from one broker transition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BrokerRecord {
    /// The sealed order is waiting for strictly later market data.
    EntryQueued {
        requested_quantity: Quantity,
        decision_complete_at: TimestampNs,
    },
    /// A primary taker-only visible-book execution; maker simulation is absent.
    TakerFill {
        role: ExecutionRole,
        side: Side,
        book_event_id: EventId,
        walk: QuantityWalk,
        fees: FeeBreakdown,
        cost: ExecutionCost,
    },
    /// Unfilled entry quantity was canceled and was never chased.
    EntryResidualCancelled { quantity: Quantity },
    /// Actual remaining exposure must await a later fresh execution book.
    ExitPending {
        reason: ExitReason,
        quantity: Quantity,
        next_limit_band: Option<Bps>,
    },
    /// Immutable observed latency for later empirical replay sampling.
    Latency(LatencySample),
    /// Funding was applied at a unique venue timestamp.
    Funding {
        venue_at: TimestampNs,
        source_event_id: EventId,
        rate: FundingRate,
        mark_price: Price,
        amount: SignedUsdc,
    },
    /// Remaining isolated equity was forfeited after the explicit maintenance backstop.
    LiquidationLoss {
        /// Quantity whose realized loss exhausted isolated collateral.
        quantity: Quantity,
        /// The isolated collateral actually forfeited to cover loss.
        forfeited_isolated_equity: Usdc,
        /// The adverse gap loss reported but never charged outside isolation.
        uncharged_gap_loss: Usdc,
    },
    /// Explicit durable state change.
    StateChanged {
        from: BrokerState,
        to: BrokerState,
        reason: Option<ExitReason>,
    },
}

/// Measured source latency bound to deployment and run digests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LatencySample {
    kind: LatencyKind,
    origin_at: TimestampNs,
    book_received_at: TimestampNs,
    observed: DurationNs,
    source_book_event_id: EventId,
    context: BrokerRunContext,
}

#[allow(
    dead_code,
    reason = "state-machine consumption begins with Task 13 engine wiring"
)]
impl LatencySample {
    fn new(
        kind: LatencyKind,
        origin_at: TimestampNs,
        book: &OrderBook,
        context: &BrokerRunContext,
    ) -> Result<Self, BrokerError> {
        Ok(Self {
            kind,
            origin_at,
            book_received_at: book.received_at(),
            observed: book.received_at().checked_duration_since(origin_at)?,
            source_book_event_id: book.event_id().clone(),
            context: context.clone(),
        })
    }

    #[must_use]
    pub const fn kind(&self) -> LatencyKind {
        self.kind
    }
    #[must_use]
    pub const fn origin_at(&self) -> TimestampNs {
        self.origin_at
    }
    #[must_use]
    pub const fn book_received_at(&self) -> TimestampNs {
        self.book_received_at
    }
    #[must_use]
    pub const fn observed(&self) -> DurationNs {
        self.observed
    }
    #[must_use]
    pub const fn source_book_event_id(&self) -> &EventId {
        &self.source_book_event_id
    }
    #[must_use]
    pub const fn context(&self) -> &BrokerRunContext {
        &self.context
    }
}

/// Full opposite-side visible-depth mark for ledger equity and breaker checks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutableExitQuote {
    side: Side,
    book_event_id: EventId,
    walk: QuantityWalk,
    fees: FeeBreakdown,
    cost: ExecutionCost,
}

impl ExecutableExitQuote {
    #[must_use]
    pub const fn side(&self) -> Side {
        self.side
    }
    #[must_use]
    pub const fn book_event_id(&self) -> &EventId {
        &self.book_event_id
    }
    #[must_use]
    pub const fn walk(&self) -> &QuantityWalk {
        &self.walk
    }
    #[must_use]
    pub const fn fees(&self) -> FeeBreakdown {
        self.fees
    }
    #[must_use]
    pub const fn cost(&self) -> ExecutionCost {
        self.cost
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PaperOrder {
    market: Market,
    side: PositionSide,
    quantity: Quantity,
    leverage: Leverage,
    reference_entry: Price,
    entry_slippage_limit: Bps,
    stop: Price,
    target: Price,
    tiers: MaintenanceTiers,
}

impl PaperOrder {
    #[allow(dead_code, reason = "Task 13 is the sealed approved-order consumer")]
    fn from_approved(approved: ApprovedOrder) -> Self {
        let candidate = approved.candidate();
        Self {
            market: candidate.market().clone(),
            side: match candidate.side() {
                Side::Buy => PositionSide::Long,
                Side::Sell => PositionSide::Short,
            },
            quantity: approved.quantity(),
            leverage: approved.leverage(),
            reference_entry: candidate.reference_entry(),
            entry_slippage_limit: approved.entry_slippage_limit(),
            stop: candidate.stop(),
            target: candidate.target(),
            tiers: approved.maintenance_tiers().clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PendingEntry {
    order: PaperOrder,
    decision_complete_at: TimestampNs,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ExitRequest {
    reason: ExitReason,
    trigger_at: TimestampNs,
    mark_price: Price,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MandatoryExit {
    request: ExitRequest,
    next_band: Option<Bps>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ActivePosition {
    view: BrokerPosition,
    collateral: Decimal,
    tiers: MaintenanceTiers,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct IsolatedLossCap {
    forfeited_isolated_equity: Usdc,
    uncharged_gap_loss: Usdc,
}

#[allow(
    dead_code,
    reason = "state-machine consumption begins with Task 13 engine wiring"
)]
impl ActivePosition {
    fn new(order: PaperOrder, walk: &QuantityWalk) -> Result<Self, BrokerError> {
        let entry_price = walk.vwap()?;
        let margin = walk
            .filled_notional()
            .value()
            .checked_div(Decimal::from(order.leverage.value()))
            .ok_or(BrokerError::Arithmetic {
                operation: "actual isolated margin",
            })?;
        let collateral = margin;
        if collateral <= Decimal::ZERO {
            return Err(BrokerError::NonPositiveIsolatedEquity);
        }
        Ok(Self {
            view: BrokerPosition {
                market: order.market,
                side: order.side,
                quantity: walk.filled_quantity(),
                entry_price,
                stop: order.stop,
                target: order.target,
                leverage: order.leverage,
                liquidation: None,
                funding: SignedUsdc::zero(),
            },
            collateral,
            tiers: order.tiers,
        })
    }

    fn reference_equity(&self, mark_price: Price) -> Result<Decimal, BrokerError> {
        let price_pnl = signed_position_pnl(
            self.view.side,
            self.view.entry_price,
            mark_price,
            self.view.quantity,
        )?;
        self.collateral
            .checked_add(price_pnl)
            .ok_or(BrokerError::Arithmetic {
                operation: "isolated reference equity",
            })
    }

    fn revalue_liquidation(&mut self, mark_price: Price) -> Result<LiquidationResult, BrokerError> {
        let equity = self.reference_equity(mark_price)?;
        if equity <= Decimal::ZERO {
            self.view.liquidation = None;
            return Err(BrokerError::NonPositiveIsolatedEquity);
        }
        let result = calculate(&LiquidationInput::new(
            self.view.quantity,
            self.view.side,
            mark_price,
            Usdc::new(equity)?,
            self.tiers.clone(),
        )?)?;
        self.view.liquidation = Some(result);
        Ok(result)
    }

    fn apply_funding(
        &mut self,
        mark_price: Price,
        rate: FundingRate,
    ) -> Result<SignedUsdc, BrokerError> {
        let notional = mark_price.checked_notional(self.view.quantity)?;
        let side_sign = match self.view.side {
            PositionSide::Long => Decimal::ONE,
            PositionSide::Short => Decimal::NEGATIVE_ONE,
        };
        let amount = notional
            .value()
            .checked_mul(rate.value())
            .and_then(|value| value.checked_mul(side_sign))
            .ok_or(BrokerError::Arithmetic {
                operation: "funding amount",
            })?;
        self.collateral = self
            .collateral
            .checked_sub(amount)
            .ok_or(BrokerError::Arithmetic {
                operation: "funding collateral debit",
            })?;
        self.view.funding = SignedUsdc::new(self.view.funding.value().checked_add(amount).ok_or(
            BrokerError::Arithmetic {
                operation: "funding accumulation",
            },
        )?);
        Ok(SignedUsdc::new(amount))
    }

    fn settle_partial_exit(
        &mut self,
        walk: &QuantityWalk,
    ) -> Result<Option<IsolatedLossCap>, BrokerError> {
        let filled = walk.filled_quantity();
        let before = self.view.quantity;
        if filled > before {
            return Err(BrokerError::Arithmetic {
                operation: "partial exit quantity",
            });
        }
        let allocated_collateral = self
            .collateral
            .checked_mul(filled.value())
            .and_then(|value| value.checked_div(before.value()))
            .ok_or(BrokerError::Arithmetic {
                operation: "allocated isolated collateral",
            })?;
        let realized =
            signed_position_pnl(self.view.side, self.view.entry_price, walk.vwap()?, filled)?;
        let settlement =
            allocated_collateral
                .checked_add(realized)
                .ok_or(BrokerError::Arithmetic {
                    operation: "partial exit settlement",
                })?;
        let residual_collateral =
            self.collateral
                .checked_sub(allocated_collateral)
                .ok_or(BrokerError::Arithmetic {
                    operation: "remaining isolated collateral",
                })?;
        let retained_loss = settlement.min(Decimal::ZERO);
        let residual_after_loss =
            residual_collateral
                .checked_add(retained_loss)
                .ok_or(BrokerError::Arithmetic {
                    operation: "remaining isolated collateral",
                })?;
        let loss_cap = if residual_after_loss < Decimal::ZERO {
            let uncharged_gap_loss =
                Usdc::new(Decimal::ZERO.checked_sub(residual_after_loss).ok_or(
                    BrokerError::Arithmetic {
                        operation: "uncharged isolated gap loss",
                    },
                )?)?;
            let forfeited_isolated_equity = Usdc::new(self.collateral)?;
            self.collateral = Decimal::ZERO;
            Some(IsolatedLossCap {
                forfeited_isolated_equity,
                uncharged_gap_loss,
            })
        } else {
            self.collateral = residual_after_loss;
            None
        };
        let remaining =
            before
                .value()
                .checked_sub(filled.value())
                .ok_or(BrokerError::Arithmetic {
                    operation: "remaining partial exit quantity",
                })?;
        self.view.quantity = Quantity::new(remaining)?;
        Ok(loss_cap)
    }

    fn maintenance_margin(&self, mark_price: Price) -> Result<Usdc, BrokerError> {
        let notional = mark_price.checked_notional(self.view.quantity)?;
        let tier =
            tier_for_notional(&self.tiers, notional).ok_or(BrokerError::NoMaintenanceTier)?;
        let value = notional
            .value()
            .checked_mul(tier.maintenance_rate())
            .and_then(|value| value.checked_sub(tier.maintenance_deduction().value()))
            .ok_or(BrokerError::Arithmetic {
                operation: "maintenance margin",
            })?
            .max(Decimal::ZERO);
        Ok(Usdc::new(value)?)
    }

    fn forfeitable_equity(&self, mark_price: Price) -> Result<Usdc, BrokerError> {
        Ok(Usdc::new(
            self.reference_equity(mark_price)?.max(Decimal::ZERO),
        )?)
    }
}

/// One sealed-order paper broker. It owns no clock, I/O, signer, or live action.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PaperBroker {
    config: BrokerConfig,
    context: BrokerRunContext,
    state: BrokerState,
    pending: Option<PendingEntry>,
    position: Option<ActivePosition>,
    normal_exit: Option<ExitRequest>,
    mandatory_exit: Option<MandatoryExit>,
    last_funding_at: Option<TimestampNs>,
    last_transition_at: TimestampNs,
    last_execution_book_received_at: Option<TimestampNs>,
    last_market_observation: Option<MarketObservationCursor>,
    latest_seen_as_of: TimestampNs,
}

#[allow(
    dead_code,
    reason = "Task 13 engine is the sole production state-machine caller"
)]
impl PaperBroker {
    /// Starts flat at an explicit event/as-of time.
    #[must_use]
    pub fn new(config: BrokerConfig, context: BrokerRunContext, opened_at: TimestampNs) -> Self {
        Self {
            config,
            context,
            state: BrokerState::Flat,
            pending: None,
            position: None,
            normal_exit: None,
            mandatory_exit: None,
            last_funding_at: None,
            last_transition_at: opened_at,
            last_execution_book_received_at: None,
            last_market_observation: None,
            latest_seen_as_of: opened_at,
        }
    }

    #[must_use]
    pub const fn state(&self) -> BrokerState {
        self.state
    }
    #[must_use]
    pub fn position(&self) -> Option<&BrokerPosition> {
        self.position.as_ref().map(|position| &position.view)
    }
    #[must_use]
    pub const fn context(&self) -> &BrokerRunContext {
        &self.context
    }

    /// Queues one risk-sealed order. No public API can construct this order.
    #[allow(
        dead_code,
        reason = "Task 13 is the sole sealed risk-to-broker integration"
    )]
    pub(crate) fn queue_entry(
        &mut self,
        approved: ApprovedOrder,
        decision_complete_at: TimestampNs,
    ) -> Result<BrokerTransition, BrokerError> {
        self.queue_order(PaperOrder::from_approved(approved), decision_complete_at)
    }

    fn queue_order(
        &mut self,
        order: PaperOrder,
        decision_complete_at: TimestampNs,
    ) -> Result<BrokerTransition, BrokerError> {
        self.ensure_not_backwards(decision_complete_at)?;
        if self.state != BrokerState::Flat {
            return Err(BrokerError::EntryBlocked { state: self.state });
        }
        if order.quantity.value().is_zero() {
            return Err(BrokerError::ZeroApprovedQuantity);
        }
        let quantity = order.quantity;
        self.pending = Some(PendingEntry {
            order,
            decision_complete_at,
        });
        self.state = BrokerState::PendingEntry;
        self.last_transition_at = decision_complete_at;
        self.observe_as_of(decision_complete_at);
        Ok(self.transition(
            decision_complete_at,
            vec![
                BrokerRecord::EntryQueued {
                    requested_quantity: quantity,
                    decision_complete_at,
                },
                state_change(BrokerState::Flat, BrokerState::PendingEntry, None),
            ],
        ))
    }

    /// Uses exactly one strictly later fresh/recovered book for an execution attempt.
    pub(crate) fn on_executable_book(
        &mut self,
        executable: &ExecutableBook,
    ) -> Result<Option<BrokerTransition>, BrokerError> {
        let book = executable.book();
        self.ensure_fresh_execution_book(executable)?;
        let transition = match self.state {
            BrokerState::PendingEntry => self.execute_entry(book).map(Some),
            BrokerState::NormalExit => self.execute_normal_exit(book).map(Some),
            BrokerState::MandatoryExit => self.execute_mandatory_exit(book).map(Some),
            BrokerState::Flat
            | BrokerState::Open
            | BrokerState::Liquidated
            | BrokerState::Unresolved => Ok(None),
        }?;
        self.observe_as_of(executable.as_of_time());
        Ok(transition)
    }

    /// Revalues the exact actual position at mark and escalates stop/liquidation priority.
    pub(crate) fn observe_mark(
        &mut self,
        observation: &ExecutableMark,
    ) -> Result<Option<BrokerTransition>, BrokerError> {
        let at = observation.received_at();
        let mark_price = observation.price();
        let active = matches!(
            self.state,
            BrokerState::Open | BrokerState::NormalExit | BrokerState::MandatoryExit
        );
        if active {
            let position = self.require_position()?;
            if observation.market() != &position.view.market {
                return Err(BrokerError::BookMarketMismatch);
            }
        }
        self.admit_market_observation(observation)?;
        if !active {
            return Ok(None);
        }
        let prior = self.state;
        let position = self.require_position_mut()?;
        let liquidation = position.revalue_liquidation(mark_price).ok();
        let reason = if liquidation.is_none_or(|result| {
            marked_at_or_beyond(position.view.side, mark_price, result.price())
        }) {
            Some(ExitReason::Liquidation)
        } else if marked_at_or_beyond(position.view.side, mark_price, position.view.stop) {
            Some(ExitReason::Stop)
        } else if prior == BrokerState::Open
            && marked_take_profit(position.view.side, mark_price, position.view.target)
        {
            Some(ExitReason::TakeProfit)
        } else {
            None
        };
        let Some(reason) = reason else {
            return Ok(None);
        };
        if reason.mandatory() {
            self.start_mandatory(ExitRequest {
                reason,
                trigger_at: at,
                mark_price,
            });
        } else {
            self.normal_exit = Some(ExitRequest {
                reason,
                trigger_at: at,
                mark_price,
            });
            self.state = BrokerState::NormalExit;
        }
        self.last_transition_at = at;
        Ok(Some(self.transition(
            at,
            vec![state_change(prior, self.state, Some(reason))],
        )))
    }

    /// Starts an explicit normal exit, unless the requested reason is mandatory.
    pub(crate) fn request_exit(
        &mut self,
        reason: ExitReason,
        observation: &ExecutableMark,
    ) -> Result<BrokerTransition, BrokerError> {
        let at = observation.received_at();
        let mark_price = observation.price();
        let position = self.require_position()?;
        if observation.market() != &position.view.market {
            return Err(BrokerError::BookMarketMismatch);
        }
        let previous = self.state;
        if !matches!(
            previous,
            BrokerState::Open | BrokerState::NormalExit | BrokerState::MandatoryExit
        ) {
            return Err(BrokerError::ExitUnavailable { state: previous });
        }
        self.admit_market_observation(observation)?;
        if reason.mandatory() {
            self.start_mandatory(ExitRequest {
                reason,
                trigger_at: at,
                mark_price,
            });
        } else if previous != BrokerState::MandatoryExit {
            self.normal_exit = Some(ExitRequest {
                reason,
                trigger_at: at,
                mark_price,
            });
            self.state = BrokerState::NormalExit;
        }
        self.last_transition_at = at;
        Ok(self.transition(at, vec![state_change(previous, self.state, Some(reason))]))
    }

    /// Turns an expired normal residual mandatory without inventing a fill.
    pub fn advance_time(
        &mut self,
        at: TimestampNs,
    ) -> Result<Option<BrokerTransition>, BrokerError> {
        self.ensure_not_backwards(at)?;
        if self.state != BrokerState::NormalExit {
            self.observe_as_of(at);
            return Ok(None);
        }
        let request = self.normal_exit.ok_or(BrokerError::MissingNormalExit)?;
        if at
            < request
                .trigger_at
                .checked_add(self.config.normal_exit_grace())?
        {
            self.observe_as_of(at);
            return Ok(None);
        }
        self.start_mandatory(request);
        self.last_transition_at = at;
        self.observe_as_of(at);
        Ok(Some(self.transition(
            at,
            vec![state_change(
                BrokerState::NormalExit,
                BrokerState::MandatoryExit,
                Some(request.reason),
            )],
        )))
    }

    /// Applies funding at a unique venue timestamp then recomputes exact liquidation.
    pub(crate) fn apply_funding(
        &mut self,
        observation: &ExecutableFunding,
    ) -> Result<BrokerTransition, BrokerError> {
        let venue_at = observation.venue_at();
        let observed_at = observation.received_at();
        let mark_price = observation.mark_price();
        if self
            .last_funding_at
            .is_some_and(|previous| venue_at <= previous)
        {
            return Err(BrokerError::DuplicateOrBackwardFunding { venue_at });
        }
        let prior = self.state;
        let position = self.require_position()?;
        if observation.market() != &position.view.market {
            return Err(BrokerError::BookMarketMismatch);
        }
        self.admit_funding_observation(observation)?;
        let position = self.require_position_mut()?;
        let amount = position.apply_funding(mark_price, observation.rate())?;
        let liquidated = match position.revalue_liquidation(mark_price) {
            Ok(result) => marked_at_or_beyond(position.view.side, mark_price, result.price()),
            Err(_) => true,
        };
        if liquidated {
            self.start_mandatory(ExitRequest {
                reason: ExitReason::Liquidation,
                trigger_at: observed_at,
                mark_price,
            });
        }
        self.last_funding_at = Some(venue_at);
        self.last_transition_at = observed_at;
        let mut records = vec![BrokerRecord::Funding {
            venue_at,
            source_event_id: observation.event_id().clone(),
            rate: observation.rate(),
            mark_price,
            amount,
        }];
        if prior != self.state {
            records.push(state_change(
                prior,
                self.state,
                Some(ExitReason::Liquidation),
            ));
        }
        Ok(self.transition(observed_at, records))
    }

    /// Produces a full opposite-side 200-bps executable quote, never a mid/mark fill.
    pub(crate) fn executable_full_exit(
        &self,
        executable: &ExecutableBook,
    ) -> Result<ExecutableExitQuote, BrokerError> {
        self.ensure_fresh_mark_book(executable)?;
        let position = self.require_position()?;
        self.ensure_book_market(executable.book(), &position.view.market)?;
        let side = exit_side(position.view.side);
        let walk = walk_visible_quantity(
            executable.book(),
            side,
            position.view.quantity,
            self.config.mandatory_cap(),
        )?;
        if walk.filled_quantity().value().is_zero() {
            return Err(BrokerError::NoExecutableExit);
        }
        let cost = attribute_taker_execution(
            TakerFeeSchedule::lowest_tier(),
            executable.book(),
            side,
            &walk,
            position.view.entry_price,
        )?
        .with_exit_alpha(
            position.view.side,
            position.view.entry_price,
            walk.vwap()?,
            walk.filled_quantity(),
        )?;
        Ok(ExecutableExitQuote {
            side,
            book_event_id: executable.book().event_id().clone(),
            fees: cost.fees(),
            walk,
            cost,
        })
    }

    /// Records end-of-data without declaring a residual closed.
    pub fn end_of_data(
        &mut self,
        at: TimestampNs,
    ) -> Result<Option<BrokerTransition>, BrokerError> {
        self.ensure_not_backwards(at)?;
        self.observe_as_of(at);
        let prior = self.state;
        let next = match prior {
            BrokerState::PendingEntry => BrokerState::Flat,
            BrokerState::Open | BrokerState::NormalExit | BrokerState::MandatoryExit => {
                BrokerState::Unresolved
            }
            BrokerState::Flat | BrokerState::Liquidated | BrokerState::Unresolved => {
                return Ok(None);
            }
        };
        self.state = next;
        if next == BrokerState::Flat {
            self.pending = None;
        }
        self.last_transition_at = at;
        Ok(Some(
            self.transition(at, vec![state_change(prior, next, None)]),
        ))
    }

    fn execute_entry(&mut self, book: &OrderBook) -> Result<BrokerTransition, BrokerError> {
        let pending = self
            .pending
            .clone()
            .ok_or(BrokerError::MissingPendingEntry)?;
        self.require_strictly_after(book, pending.decision_complete_at)?;
        self.ensure_book_market(book, &pending.order.market)?;
        let side = entry_side(pending.order.side);
        let entry_limit = adverse_price_limit(
            pending.order.reference_entry,
            pending.order.side,
            pending.order.entry_slippage_limit,
        )?;
        let walk = walk_visible_quantity_to_price(book, side, pending.order.quantity, entry_limit)?;
        let mut records = vec![BrokerRecord::Latency(LatencySample::new(
            LatencyKind::DecisionToBook,
            pending.decision_complete_at,
            book,
            &self.context,
        )?)];
        let previous = self.state;
        self.consume_execution_book(book);
        self.pending = None;
        if walk.filled_quantity().value().is_zero() {
            self.state = BrokerState::Flat;
            records.push(BrokerRecord::EntryResidualCancelled {
                quantity: walk.remaining_quantity(),
            });
            records.push(state_change(previous, self.state, None));
            return Ok(self.transition(book.received_at(), records));
        }
        let cost = attribute_taker_execution(
            TakerFeeSchedule::lowest_tier(),
            book,
            side,
            &walk,
            pending.order.reference_entry,
        )?;
        records.push(BrokerRecord::TakerFill {
            role: ExecutionRole::Entry,
            side,
            book_event_id: book.event_id().clone(),
            walk: walk.clone(),
            fees: cost.fees(),
            cost,
        });
        if !walk.is_complete() {
            records.push(BrokerRecord::EntryResidualCancelled {
                quantity: walk.remaining_quantity(),
            });
        }
        let mut position = ActivePosition::new(pending.order, &walk)?;
        let entry_mark = position.view.entry_price;
        let liquidated = match position.revalue_liquidation(entry_mark) {
            Ok(result) => marked_at_or_beyond(position.view.side, entry_mark, result.price()),
            Err(_) => true,
        };
        self.position = Some(position);
        if walk.filled_notional() < self.config.minimum_notional() {
            self.start_mandatory(ExitRequest {
                reason: ExitReason::Dust,
                trigger_at: book.received_at(),
                mark_price: entry_mark,
            });
            records.push(state_change(previous, self.state, Some(ExitReason::Dust)));
        } else if liquidated {
            self.start_mandatory(ExitRequest {
                reason: ExitReason::Liquidation,
                trigger_at: book.received_at(),
                mark_price: entry_mark,
            });
            records.push(state_change(
                previous,
                self.state,
                Some(ExitReason::Liquidation),
            ));
        } else {
            self.state = BrokerState::Open;
            records.push(state_change(previous, self.state, None));
        }
        Ok(self.transition(book.received_at(), records))
    }

    fn execute_normal_exit(&mut self, book: &OrderBook) -> Result<BrokerTransition, BrokerError> {
        let request = self.normal_exit.ok_or(BrokerError::MissingNormalExit)?;
        self.require_strictly_after(book, request.trigger_at)?;
        if book.received_at()
            >= request
                .trigger_at
                .checked_add(self.config.normal_exit_grace())?
        {
            self.start_mandatory(request);
            let mut transition = self.execute_mandatory_exit(book)?;
            transition.records.insert(
                0,
                state_change(
                    BrokerState::NormalExit,
                    BrokerState::MandatoryExit,
                    Some(request.reason),
                ),
            );
            return Ok(transition);
        }
        self.execute_exit(
            book,
            request,
            self.config.normal_exit_band(),
            ExecutionRole::NormalExit,
            false,
        )
    }

    fn execute_mandatory_exit(
        &mut self,
        book: &OrderBook,
    ) -> Result<BrokerTransition, BrokerError> {
        let mandatory = self
            .mandatory_exit
            .clone()
            .ok_or(BrokerError::MissingMandatoryExit)?;
        self.require_strictly_after(book, mandatory.request.trigger_at)?;
        let band = mandatory
            .next_band
            .map_or_else(|| mandatory_initial_band(book), Ok)?;
        let role = if mandatory.request.reason == ExitReason::Liquidation {
            ExecutionRole::Liquidation
        } else {
            ExecutionRole::MandatoryExit
        };
        self.execute_exit(book, mandatory.request, band, role, true)
    }

    fn execute_exit(
        &mut self,
        book: &OrderBook,
        request: ExitRequest,
        band: Bps,
        role: ExecutionRole,
        mandatory: bool,
    ) -> Result<BrokerTransition, BrokerError> {
        let active = self.require_position()?.clone();
        self.ensure_book_market(book, &active.view.market)?;
        let side = exit_side(active.view.side);
        let walk = walk_visible_quantity(book, side, active.view.quantity, band)?;
        let previous = self.state;
        let mut records = vec![BrokerRecord::Latency(LatencySample::new(
            LatencyKind::TriggerToBook,
            request.trigger_at,
            book,
            &self.context,
        )?)];
        self.consume_execution_book(book);
        let loss_cap = if !walk.filled_quantity().value().is_zero() {
            let cost = attribute_taker_execution(
                TakerFeeSchedule::lowest_tier(),
                book,
                side,
                &walk,
                request.mark_price,
            )?
            .with_exit_alpha(
                active.view.side,
                active.view.entry_price,
                walk.vwap()?,
                walk.filled_quantity(),
            )?;
            records.push(BrokerRecord::TakerFill {
                role,
                side,
                book_event_id: book.event_id().clone(),
                walk: walk.clone(),
                fees: cost.fees(),
                cost,
            });
            self.require_position_mut()?.settle_partial_exit(&walk)?
        } else {
            None
        };
        if let Some(loss_cap) = loss_cap {
            records.push(BrokerRecord::LiquidationLoss {
                quantity: walk.filled_quantity(),
                forfeited_isolated_equity: loss_cap.forfeited_isolated_equity,
                uncharged_gap_loss: loss_cap.uncharged_gap_loss,
            });
        }
        if self
            .position
            .as_ref()
            .is_some_and(|position| position.view.quantity.value().is_zero())
        {
            self.position = None;
            self.normal_exit = None;
            self.mandatory_exit = None;
            self.state = if request.reason == ExitReason::Liquidation || loss_cap.is_some() {
                BrokerState::Liquidated
            } else {
                BrokerState::Flat
            };
            let completion_reason = if loss_cap.is_some() {
                ExitReason::Liquidation
            } else {
                request.reason
            };
            records.push(state_change(previous, self.state, Some(completion_reason)));
            return Ok(self.transition(book.received_at(), records));
        }
        let liquidated = {
            let position = self.require_position_mut()?;
            match position.revalue_liquidation(request.mark_price) {
                Ok(result) => {
                    marked_at_or_beyond(position.view.side, request.mark_price, result.price())
                }
                Err(_) => true,
            }
        };
        if liquidated {
            self.start_mandatory(ExitRequest {
                reason: ExitReason::Liquidation,
                trigger_at: book.received_at(),
                mark_price: request.mark_price,
            });
        } else if mandatory {
            let next_band = widened_band(
                band,
                self.config.mandatory_widening(),
                self.config.mandatory_cap(),
            )?;
            self.normal_exit = None;
            self.mandatory_exit = Some(MandatoryExit {
                request,
                next_band: Some(next_band),
            });
            self.state = BrokerState::MandatoryExit;
        }
        let current_request = self
            .mandatory_exit
            .as_ref()
            .map_or(request, |mandatory| mandatory.request);
        let next_band = self
            .mandatory_exit
            .as_ref()
            .and_then(|mandatory| mandatory.next_band);
        let remaining = self.require_position()?.view.quantity;
        records.push(BrokerRecord::ExitPending {
            reason: current_request.reason,
            quantity: remaining,
            next_limit_band: next_band,
        });
        if role == ExecutionRole::Liquidation
            && current_request.reason == ExitReason::Liquidation
            && self.needs_backstop(request.mark_price)?
        {
            let forfeited = self
                .require_position()?
                .forfeitable_equity(request.mark_price)?;
            self.position = None;
            self.normal_exit = None;
            self.mandatory_exit = None;
            self.state = BrokerState::Liquidated;
            records.push(BrokerRecord::LiquidationLoss {
                quantity: remaining,
                forfeited_isolated_equity: forfeited,
                uncharged_gap_loss: Usdc::zero(),
            });
            records.push(state_change(
                previous,
                self.state,
                Some(ExitReason::Liquidation),
            ));
        } else if previous != self.state {
            records.push(state_change(
                previous,
                self.state,
                Some(current_request.reason),
            ));
        }
        Ok(self.transition(book.received_at(), records))
    }

    fn needs_backstop(&self, mark_price: Price) -> Result<bool, BrokerError> {
        let position = self.require_position()?;
        let equity = position.reference_equity(mark_price)?;
        let threshold = position
            .maintenance_margin(mark_price)?
            .value()
            .checked_mul(Decimal::TWO)
            .and_then(|value| value.checked_div(Decimal::from(3)))
            .ok_or(BrokerError::Arithmetic {
                operation: "two-thirds maintenance",
            })?;
        Ok(equity < threshold)
    }

    fn start_mandatory(&mut self, request: ExitRequest) {
        self.normal_exit = None;
        self.mandatory_exit = Some(MandatoryExit {
            request,
            next_band: None,
        });
        self.state = BrokerState::MandatoryExit;
    }

    fn ensure_fresh_execution_book(&self, executable: &ExecutableBook) -> Result<(), BrokerError> {
        self.ensure_fresh_mark_book(executable)?;
        if self
            .last_execution_book_received_at
            .is_some_and(|previous| executable.book().received_at() <= previous)
        {
            return Err(BrokerError::ReplayedOrStaleExecutionBook);
        }
        Ok(())
    }

    fn ensure_fresh_mark_book(&self, executable: &ExecutableBook) -> Result<(), BrokerError> {
        let age = executable
            .as_of_time()
            .checked_duration_since(executable.book().event_time())?;
        if age > self.config.maximum_book_age() {
            return Err(BrokerError::StaleExecutableBook {
                age,
                maximum_age: self.config.maximum_book_age(),
            });
        }
        if executable.as_of_time() < self.causal_boundary() {
            return Err(BrokerError::BackwardBookAsOf);
        }
        Ok(())
    }

    fn admit_market_observation(
        &mut self,
        observation: &ExecutableMark,
    ) -> Result<(), BrokerError> {
        let cursor = MarketObservationCursor {
            event_time: observation.event_time(),
            kind: MarketObservationKind::Mark,
            event_id: observation.event_id().clone(),
        };
        self.ensure_fresh_market_observation(
            &cursor,
            observation.received_at(),
            observation.as_of_time(),
        )?;
        self.consume_market_observation(cursor, observation.as_of_time());
        Ok(())
    }

    fn admit_funding_observation(
        &mut self,
        observation: &ExecutableFunding,
    ) -> Result<(), BrokerError> {
        let cursor = MarketObservationCursor {
            event_time: observation.venue_at(),
            kind: MarketObservationKind::Funding,
            event_id: observation.event_id().clone(),
        };
        self.ensure_fresh_market_observation(
            &cursor,
            observation.received_at(),
            observation.as_of_time(),
        )?;
        self.consume_market_observation(cursor, observation.as_of_time());
        Ok(())
    }

    fn ensure_fresh_market_observation(
        &self,
        cursor: &MarketObservationCursor,
        received_at: TimestampNs,
        as_of_time: TimestampNs,
    ) -> Result<(), BrokerError> {
        let age = as_of_time.checked_duration_since(cursor.event_time)?;
        if age > self.config.maximum_book_age() {
            return Err(BrokerError::StaleExecutableBook {
                age,
                maximum_age: self.config.maximum_book_age(),
            });
        }
        let boundary = self.causal_boundary();
        if as_of_time < boundary {
            return Err(BrokerError::BackwardBookAsOf);
        }
        if received_at < self.last_transition_at {
            return Err(BrokerError::ReplayedOrStaleObservation);
        }
        if self
            .last_market_observation
            .as_ref()
            .is_some_and(|previous| cursor <= previous)
        {
            return Err(BrokerError::ReplayedOrStaleObservation);
        }
        Ok(())
    }

    fn consume_market_observation(
        &mut self,
        cursor: MarketObservationCursor,
        as_of_time: TimestampNs,
    ) {
        self.last_market_observation = Some(cursor);
        self.observe_as_of(as_of_time);
    }

    fn consume_execution_book(&mut self, book: &OrderBook) {
        self.last_execution_book_received_at = Some(book.received_at());
        self.last_transition_at = book.received_at();
    }

    fn require_strictly_after(
        &self,
        book: &OrderBook,
        boundary: TimestampNs,
    ) -> Result<(), BrokerError> {
        if book.received_at() <= boundary {
            return Err(BrokerError::BookNotStrictlyAfterBoundary);
        }
        Ok(())
    }

    fn ensure_book_market(&self, book: &OrderBook, market: &Market) -> Result<(), BrokerError> {
        if book.market() != market {
            return Err(BrokerError::BookMarketMismatch);
        }
        Ok(())
    }

    fn ensure_not_backwards(&self, at: TimestampNs) -> Result<(), BrokerError> {
        let previous = self.causal_boundary();
        if at < previous {
            return Err(BrokerError::BackwardTime {
                previous,
                current: at,
            });
        }
        Ok(())
    }

    fn causal_boundary(&self) -> TimestampNs {
        self.last_transition_at.max(self.latest_seen_as_of)
    }

    fn observe_as_of(&mut self, as_of_time: TimestampNs) {
        self.latest_seen_as_of = self.latest_seen_as_of.max(as_of_time);
    }

    fn require_position(&self) -> Result<&ActivePosition, BrokerError> {
        self.position.as_ref().ok_or(BrokerError::NoOpenPosition)
    }
    fn require_position_mut(&mut self) -> Result<&mut ActivePosition, BrokerError> {
        self.position.as_mut().ok_or(BrokerError::NoOpenPosition)
    }
    fn transition(&self, at: TimestampNs, records: Vec<BrokerRecord>) -> BrokerTransition {
        BrokerTransition {
            at,
            state: self.state,
            records,
        }
    }
}

/// Construction failure before any paper state can be mutated.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum BrokerInputError {
    #[error("minimum executable notional must be positive")]
    ZeroMinimumNotional,
    #[error("{field} digest must be a 64-character hexadecimal BLAKE3 value")]
    InvalidDigest { field: &'static str },
    #[error("fixed broker constraint could not be represented")]
    InvalidFixedConstraint,
}

/// Deterministic paper-execution rejection or arithmetic failure.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum BrokerError {
    #[error("entry is blocked while broker state is {state:?}")]
    EntryBlocked { state: BrokerState },
    #[error("approved quantity must be positive")]
    ZeroApprovedQuantity,
    #[error("broker has no actual open position")]
    NoOpenPosition,
    #[error("exit is unavailable while broker state is {state:?}")]
    ExitUnavailable { state: BrokerState },
    #[error("pending-entry state had no sealed order")]
    MissingPendingEntry,
    #[error("normal-exit state had no request")]
    MissingNormalExit,
    #[error("mandatory-exit state had no request")]
    MissingMandatoryExit,
    #[error("book market does not match the active market")]
    BookMarketMismatch,
    #[error("book predates completed market recovery")]
    BookPredatesRecovery,
    #[error("book source timestamps are later than its explicit as-of time")]
    BookFromFuture,
    #[error("executable book age {age} exceeds {maximum_age}")]
    StaleExecutableBook {
        age: DurationNs,
        maximum_age: DurationNs,
    },
    #[error("book receipt must be strictly after the decision or trigger boundary")]
    BookNotStrictlyAfterBoundary,
    #[error("book source was already consumed or does not advance receipt time")]
    ReplayedOrStaleExecutionBook,
    #[error("market observation was replayed, stale, or out of source order")]
    ReplayedOrStaleObservation,
    #[error("book as-of time moved behind a committed broker transition")]
    BackwardBookAsOf,
    #[error("broker time moved backward from {previous} to {current}")]
    BackwardTime {
        previous: TimestampNs,
        current: TimestampNs,
    },
    #[error("funding venue timestamp {venue_at} is duplicate or out of order")]
    DuplicateOrBackwardFunding { venue_at: TimestampNs },
    #[error("actual isolated equity is no longer positive")]
    NonPositiveIsolatedEquity,
    #[error("no maintenance tier contains the current position notional")]
    NoMaintenanceTier,
    #[error("no visible full-exit quantity is executable")]
    NoExecutableExit,
    #[error("checked broker arithmetic failed while calculating {operation}")]
    Arithmetic { operation: &'static str },
    #[error(transparent)]
    Domain(#[from] DomainError),
    #[error(transparent)]
    Event(#[from] EventError),
    #[error(transparent)]
    Fill(#[from] FillError),
    #[error(transparent)]
    Cost(#[from] CostError),
    #[error(transparent)]
    Liquidation(#[from] crate::risk::liquidation::LiquidationError),
}

fn is_blake3_digest(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}
fn entry_side(side: PositionSide) -> Side {
    match side {
        PositionSide::Long => Side::Buy,
        PositionSide::Short => Side::Sell,
    }
}
fn exit_side(side: PositionSide) -> Side {
    match side {
        PositionSide::Long => Side::Sell,
        PositionSide::Short => Side::Buy,
    }
}
fn marked_at_or_beyond(side: PositionSide, mark: Price, threshold: Price) -> bool {
    match side {
        PositionSide::Long => mark <= threshold,
        PositionSide::Short => mark >= threshold,
    }
}
fn marked_take_profit(side: PositionSide, mark: Price, target: Price) -> bool {
    match side {
        PositionSide::Long => mark >= target,
        PositionSide::Short => mark <= target,
    }
}

fn adverse_price_limit(
    reference: Price,
    side: PositionSide,
    band: Bps,
) -> Result<Price, BrokerError> {
    let adjustment = reference
        .value()
        .checked_mul(band.value())
        .and_then(|value| value.checked_div(Decimal::from(10_000)))
        .ok_or(BrokerError::Arithmetic {
            operation: "sealed entry slippage",
        })?;
    let value = match side {
        PositionSide::Long => reference.value().checked_add(adjustment),
        PositionSide::Short => reference.value().checked_sub(adjustment),
    }
    .ok_or(BrokerError::Arithmetic {
        operation: "sealed entry price limit",
    })?;
    Ok(Price::new(value)?)
}

fn signed_position_pnl(
    side: PositionSide,
    entry: Price,
    exit: Price,
    quantity: Quantity,
) -> Result<Decimal, BrokerError> {
    let difference = match side {
        PositionSide::Long => exit.value().checked_sub(entry.value()),
        PositionSide::Short => entry.value().checked_sub(exit.value()),
    }
    .ok_or(BrokerError::Arithmetic {
        operation: "position price difference",
    })?;
    difference
        .checked_mul(quantity.value())
        .ok_or(BrokerError::Arithmetic {
            operation: "position pnl",
        })
}

fn tier_for_notional(tiers: &MaintenanceTiers, notional: Usdc) -> Option<MaintenanceTier> {
    tiers.as_slice().iter().copied().find(|tier| {
        notional >= tier.lower_notional()
            && tier.upper_notional().is_none_or(|upper| notional < upper)
    })
}

fn mandatory_initial_band(book: &OrderBook) -> Result<Bps, BrokerError> {
    let bid = book
        .bids()
        .first()
        .ok_or(BrokerError::NoExecutableExit)?
        .price();
    let ask = book
        .asks()
        .first()
        .ok_or(BrokerError::NoExecutableExit)?
        .price();
    let doubled_spread = ask
        .value()
        .checked_sub(bid.value())
        .and_then(|value| value.checked_mul(Decimal::from(10_000)))
        .and_then(|value| value.checked_div(bid.value()))
        .and_then(|value| value.checked_mul(Decimal::TWO))
        .ok_or(BrokerError::Arithmetic {
            operation: "mandatory doubled spread",
        })?;
    Ok(Bps::new(
        doubled_spread.max(NORMAL_EXIT_BAND).min(MANDATORY_CAP),
    )?)
}

fn widened_band(current: Bps, increment: Bps, cap: Bps) -> Result<Bps, BrokerError> {
    Ok(Bps::new(
        current
            .value()
            .checked_add(increment.value())
            .ok_or(BrokerError::Arithmetic {
                operation: "mandatory band widening",
            })?
            .min(cap.value()),
    )?)
}

fn state_change(from: BrokerState, to: BrokerState, reason: Option<ExitReason>) -> BrokerRecord {
    BrokerRecord::StateChanged { from, to, reason }
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use super::{
        BrokerConfig, BrokerError, BrokerRecord, BrokerRunContext, BrokerState, ExecutableBook,
        ExecutableFunding, ExecutionRole, ExitReason, MarketExecutionReady, PaperBroker,
        PaperOrder,
    };
    use crate::book::OrderBook;
    use crate::domain::{Bps, EventId, Leverage, Market, Price, Quantity, RunId, Side, Usdc};
    use crate::event::{
        BookLevel, BookSnapshot, DurationNs, FundingRate, MarketEvent, TimestampNs,
    };
    use crate::ledger::PositionSide;
    use crate::risk::liquidation::{
        LiquidationInput, MaintenanceTier, MaintenanceTiers, calculate,
    };

    #[test]
    fn entry_uses_only_first_strictly_post_decision_recovered_book_and_records_latency() {
        let mut broker = broker();
        broker
            .queue_order(long_order(), time(10))
            .expect("queue entry");

        let equal_boundary = executable(book(10, 10, 1, dec!(99), dec!(100), dec!(1)), 10, 0);
        assert!(matches!(
            broker.on_executable_book(&equal_boundary),
            Err(BrokerError::BookNotStrictlyAfterBoundary)
        ));

        let selected = executable(book(20, 20, 2, dec!(99), dec!(100.4), dec!(1)), 20, 0);
        let transition = broker
            .on_executable_book(&selected)
            .expect("fresh selected book")
            .expect("entry transition");
        assert_eq!(transition.state(), BrokerState::Open);
        assert_eq!(
            broker.position().expect("position").entry_price().value(),
            dec!(100.4)
        );
        assert!(transition.records().iter().any(|record| matches!(
            record,
            BrokerRecord::Latency(sample) if sample.observed().value() == 10
        )));
        let entry_cost = transition
            .records()
            .iter()
            .find_map(|record| match record {
                BrokerRecord::TakerFill {
                    role: ExecutionRole::Entry,
                    cost,
                    ..
                } => Some(*cost),
                _ => None,
            })
            .expect("entry cost attribution");
        assert_eq!(entry_cost.latency_loss().value(), dec!(-0.3));
    }

    #[test]
    fn replayed_partial_exit_book_cannot_reuse_visible_liquidity_and_stop_supersedes_normal_exit() {
        let mut broker = opened_long();
        broker
            .request_exit(ExitReason::Strategy, &mark(30, dec!(100), 0))
            .expect("normal exit");
        let partial = executable(book(31, 31, 3, dec!(99), dec!(100), dec!(0.4)), 31, 0);
        let transition = broker
            .on_executable_book(&partial)
            .expect("partial exit")
            .expect("transition");
        assert_eq!(transition.state(), BrokerState::NormalExit);
        assert_eq!(
            broker.position().expect("residual").quantity().value(),
            dec!(0.6)
        );
        assert!(matches!(
            broker.on_executable_book(&partial),
            Err(BrokerError::ReplayedOrStaleExecutionBook)
        ));

        let escalated = broker
            .observe_mark(&mark(32, dec!(95), 0))
            .expect("mark")
            .expect("stop transition");
        assert_eq!(escalated.state(), BrokerState::MandatoryExit);
        assert!(escalated.records().iter().any(|record| matches!(
            record,
            BrokerRecord::StateChanged {
                reason: Some(ExitReason::Stop),
                ..
            }
        )));
    }

    #[test]
    fn partial_entry_is_the_complete_position_and_subminimum_fill_becomes_dust_exit() {
        let mut partial = broker();
        partial.queue_order(long_order(), time(10)).expect("queue");
        let first = executable(book(20, 20, 10, dec!(99), dec!(100), dec!(0.4)), 20, 0);
        let transition = partial
            .on_executable_book(&first)
            .expect("partial entry")
            .expect("transition");
        assert_eq!(transition.state(), BrokerState::Open);
        assert_eq!(
            partial
                .position()
                .expect("actual position")
                .quantity()
                .value(),
            dec!(0.4)
        );
        assert!(transition.records().iter().any(|record| matches!(
            record,
            BrokerRecord::EntryResidualCancelled { quantity } if quantity.value() == dec!(0.6)
        )));

        let mut dust = broker();
        dust.queue_order(dust_order(), time(10))
            .expect("queue dust");
        let fill = executable(book(20, 20, 11, dec!(99), dec!(100), dec!(0.005)), 20, 0);
        let transition = dust
            .on_executable_book(&fill)
            .expect("dust entry")
            .expect("transition");
        assert_eq!(transition.state(), BrokerState::MandatoryExit);
        assert!(transition.records().iter().any(|record| matches!(
            record,
            BrokerRecord::StateChanged {
                reason: Some(ExitReason::Dust),
                ..
            }
        )));
    }

    #[test]
    fn take_profit_uses_mark_then_normal_exit_retries_before_mandatory_widening() {
        let mut broker = opened_long();
        let take_profit = broker
            .observe_mark(&mark(30, dec!(108), 0))
            .expect("take-profit mark")
            .expect("transition");
        assert_eq!(take_profit.state(), BrokerState::NormalExit);
        assert!(take_profit.records().iter().any(|record| matches!(
            record,
            BrokerRecord::StateChanged {
                reason: Some(ExitReason::TakeProfit),
                ..
            }
        )));

        let partial = executable(book(31, 31, 12, dec!(100), dec!(100.01), dec!(0.5)), 31, 0);
        broker
            .on_executable_book(&partial)
            .expect("normal partial exit")
            .expect("transition");
        let deadline = time(5_000_000_030);
        let escalation = broker
            .advance_time(deadline)
            .expect("grace expiry")
            .expect("mandatory transition");
        assert_eq!(escalation.state(), BrokerState::MandatoryExit);

        let mandatory = executable(
            book(
                5_000_000_031,
                5_000_000_031,
                13,
                dec!(100),
                dec!(100.01),
                dec!(0.1),
            ),
            5_000_000_031,
            0,
        );
        let transition = broker
            .on_executable_book(&mandatory)
            .expect("mandatory retry")
            .expect("transition");
        assert!(transition.records().iter().any(|record| matches!(
            record,
            BrokerRecord::ExitPending {
                next_limit_band: Some(band),
                ..
            } if band.value() == dec!(75)
        )));
    }

    #[test]
    fn actual_gap_fill_partial_exit_and_funding_revalue_tier_valid_liquidation() {
        let mut broker = broker();
        broker.queue_order(long_order(), time(10)).expect("queue");
        let entry = executable(book(20, 20, 2, dec!(100.3), dec!(100.4), dec!(1)), 20, 0);
        broker
            .on_executable_book(&entry)
            .expect("entry")
            .expect("transition");
        let after_gap_entry = broker
            .position()
            .expect("position")
            .liquidation()
            .expect("revalued liquidation");
        let tiers = tiers();
        let expected = calculate(
            &LiquidationInput::new(
                quantity(dec!(1)),
                PositionSide::Long,
                price(dec!(100.4)),
                Usdc::new(dec!(20.08)).expect("isolated equity"),
                tiers.clone(),
            )
            .expect("input"),
        )
        .expect("liquidation");
        assert_eq!(after_gap_entry, expected);

        broker
            .apply_funding(&funding(30, dec!(100.4), dec!(0.01), 0))
            .expect("funding");
        let after_funding = broker
            .position()
            .expect("position")
            .liquidation()
            .expect("revalued liquidation");
        assert!(after_funding.price() > after_gap_entry.price());

        broker
            .request_exit(ExitReason::Strategy, &mark(40, dec!(100), 0))
            .expect("normal exit");
        let partial = executable(book(41, 41, 3, dec!(100), dec!(101), dec!(0.5)), 41, 0);
        broker
            .on_executable_book(&partial)
            .expect("partial exit")
            .expect("transition");
        let actual = broker
            .position()
            .expect("residual")
            .liquidation()
            .expect("revalued residual liquidation");
        assert_eq!(
            broker.position().expect("residual").quantity().value(),
            dec!(0.5)
        );
        let expected_residual = calculate(
            &LiquidationInput::new(
                quantity(dec!(0.5)),
                PositionSide::Long,
                price(dec!(100)),
                Usdc::new(dec!(9.338)).expect("partial accounting equity"),
                tiers,
            )
            .expect("partial input"),
        )
        .expect("partial liquidation");
        assert_eq!(actual, expected_residual);
    }

    #[test]
    fn stale_or_pre_recovery_books_fail_closed_and_stream_end_keeps_residual_unresolved() {
        let recovery = MarketExecutionReady::new(market(), time(20));
        let old = book(20, 20, 1, dec!(99), dec!(100), dec!(1));
        assert!(matches!(
            ExecutableBook::new(old, time(20), duration(10), &recovery),
            Err(BrokerError::BookPredatesRecovery)
        ));
        let stale = book(21, 21, 2, dec!(99), dec!(100), dec!(1));
        assert!(matches!(
            ExecutableBook::new(stale, time(40), duration(10), &recovery),
            Err(BrokerError::StaleExecutableBook { .. })
        ));

        let mut broker = opened_long();
        let unresolved = broker
            .end_of_data(time(30))
            .expect("end")
            .expect("transition");
        assert_eq!(unresolved.state(), BrokerState::Unresolved);
        assert!(broker.position().is_some());
    }

    #[test]
    fn observation_admission_uses_broker_freshness_and_advances_all_causal_boundaries() {
        let mut broker = opened_long();
        let stale = mark_at(30, 30, 1_500, dec!(100), 2_000, 0, "stale-mark");
        assert!(matches!(
            broker.observe_mark(&stale),
            Err(BrokerError::StaleExecutableBook { .. })
        ));

        assert!(
            broker
                .observe_mark(&mark_at(30, 30, 30, dec!(100), 1_000, 0, "mark-30"))
                .expect("accepted no-op mark")
                .is_none()
        );
        let delayed = mark_at(25, 31, 31, dec!(95), 1_000, 0, "delayed-mark");
        assert!(matches!(
            broker.observe_mark(&delayed),
            Err(BrokerError::ReplayedOrStaleObservation)
        ));
        assert!(matches!(
            broker.end_of_data(time(25)),
            Err(BrokerError::BackwardTime { .. })
        ));

        let old_quote = executable(book(25, 25, 8, dec!(99), dec!(100), dec!(1)), 25, 0);
        assert!(matches!(
            broker.executable_full_exit(&old_quote),
            Err(BrokerError::BackwardBookAsOf)
        ));
        let current_quote = executable(book(31, 31, 14, dec!(99), dec!(101), dec!(1)), 31, 0);
        let quote = broker
            .executable_full_exit(&current_quote)
            .expect("fresh executable full exit");
        assert_eq!(quote.side(), Side::Sell);
        assert_eq!(quote.walk().vwap().expect("quote VWAP").value(), dec!(99));
    }

    #[test]
    fn funding_is_source_bound_and_orders_after_a_distinct_same_time_mark() {
        let mut broker = opened_long();
        assert!(
            broker
                .observe_mark(&mark_at(30, 30, 30, dec!(100), 1_000, 0, "mark-30"))
                .expect("mark")
                .is_none()
        );
        let source = funding_at(30, 30, 30, dec!(100), dec!(0.01), 1_000, 0, "funding-30");
        let transition = broker.apply_funding(&source).expect("funding");
        assert!(transition.records().iter().any(|record| matches!(
            record,
            BrokerRecord::Funding {
                venue_at,
                source_event_id,
                ..
            } if *venue_at == time(30) && source_event_id.as_str() == "funding-30"
        )));

        let duplicate = funding_at(
            30,
            31,
            31,
            dec!(100),
            dec!(0.01),
            1_000,
            0,
            "funding-30-replay",
        );
        assert!(matches!(
            broker.apply_funding(&duplicate),
            Err(BrokerError::DuplicateOrBackwardFunding { venue_at }) if venue_at == time(30)
        ));
    }

    #[test]
    fn full_gap_exit_forfeits_only_isolated_collateral_and_reports_uncharged_loss() {
        let mut broker = opened_long();
        broker
            .request_exit(ExitReason::Strategy, &mark(30, dec!(100), 0))
            .expect("exit request");
        let gap = executable(book(31, 31, 9, dec!(50), dec!(51), dec!(1)), 31, 0);
        let transition = broker
            .on_executable_book(&gap)
            .expect("gap execution")
            .expect("transition");

        assert_eq!(transition.state(), BrokerState::Liquidated);
        assert!(broker.position().is_none());
        assert!(transition.records().iter().any(|record| matches!(
            record,
            BrokerRecord::LiquidationLoss {
                quantity,
                forfeited_isolated_equity,
                uncharged_gap_loss,
            } if quantity.value() == dec!(1)
                && forfeited_isolated_equity.value() == dec!(20)
                && uncharged_gap_loss.value() == dec!(30)
        )));
    }

    #[test]
    fn sealed_entry_limit_rejects_a_tight_but_adversely_gapped_book() {
        let mut broker = broker();
        broker.queue_order(long_order(), time(10)).expect("queue");
        let gap = executable(book(20, 20, 2, dec!(119.9), dec!(120), dec!(1)), 20, 0);
        let transition = broker
            .on_executable_book(&gap)
            .expect("processed gap")
            .expect("transition");

        assert_eq!(transition.state(), BrokerState::Flat);
        assert!(broker.position().is_none());
        assert!(
            transition
                .records()
                .iter()
                .all(|record| !matches!(record, BrokerRecord::TakerFill { .. }))
        );
    }

    #[test]
    fn funding_or_partial_loss_beyond_current_liquidation_threshold_escalates_immediately() {
        let mut funded = opened_long();
        let funding = funded
            .apply_funding(&funding(30, dec!(100), dec!(0.20), 0))
            .expect("funding");
        assert_eq!(funding.state(), BrokerState::MandatoryExit);
        assert!(funding.records().iter().any(|record| matches!(
            record,
            BrokerRecord::StateChanged {
                reason: Some(ExitReason::Liquidation),
                ..
            }
        )));

        let mut partial = opened_long();
        partial
            .request_exit(ExitReason::Strategy, &mark(40, dec!(70), 0))
            .expect("normal exit");
        let loss_book = executable(book(41, 41, 3, dec!(70), dec!(71), dec!(0.5)), 41, 0);
        let transition = partial
            .on_executable_book(&loss_book)
            .expect("partial loss")
            .expect("transition");
        assert_eq!(transition.state(), BrokerState::MandatoryExit);
        assert!(transition.records().iter().any(|record| matches!(
            record,
            BrokerRecord::ExitPending {
                reason: ExitReason::Liquidation,
                ..
            }
        )));
    }

    #[test]
    fn liquidation_tries_book_before_forfeiting_remaining_isolated_equity() {
        let mut broker = opened_long();
        let trigger = broker
            .observe_mark(&mark(30, dec!(80), 0))
            .expect("liquidation mark")
            .expect("transition");
        assert_eq!(trigger.state(), BrokerState::MandatoryExit);
        let book = executable(book(31, 31, 3, dec!(70), dec!(71), dec!(0.1)), 31, 0);
        let transition = broker
            .on_executable_book(&book)
            .expect("book liquidation")
            .expect("transition");
        assert_eq!(transition.state(), BrokerState::Liquidated);
        let fill_index = transition
            .records()
            .iter()
            .position(|record| {
                matches!(
                    record,
                    BrokerRecord::TakerFill {
                        role: ExecutionRole::Liquidation,
                        ..
                    }
                )
            })
            .expect("book liquidation fill");
        let loss_index = transition
            .records()
            .iter()
            .position(|record| matches!(record, BrokerRecord::LiquidationLoss { forfeited_isolated_equity, .. } if forfeited_isolated_equity.value() >= Decimal::ZERO))
            .expect("monetary backstop loss");
        assert!(fill_index < loss_index);
    }

    fn broker() -> PaperBroker {
        PaperBroker::new(
            BrokerConfig::new(Usdc::new(dec!(1)).expect("minimum"), duration(1_000))
                .expect("config"),
            BrokerRunContext::new(
                RunId::new("paper-run").expect("run"),
                digest('a'),
                digest('b'),
            )
            .expect("context"),
            time(0),
        )
    }

    fn opened_long() -> PaperBroker {
        let mut broker = broker();
        broker.queue_order(long_order(), time(10)).expect("queue");
        let entry = executable(book(20, 20, 2, dec!(99), dec!(100), dec!(1)), 20, 0);
        broker
            .on_executable_book(&entry)
            .expect("entry")
            .expect("transition");
        broker
    }

    fn long_order() -> PaperOrder {
        PaperOrder {
            market: market(),
            side: PositionSide::Long,
            quantity: quantity(dec!(1)),
            leverage: Leverage::new(5).expect("leverage"),
            reference_entry: price(dec!(100)),
            entry_slippage_limit: Bps::new(dec!(50)).expect("entry limit"),
            stop: price(dec!(96)),
            target: price(dec!(108)),
            tiers: tiers(),
        }
    }

    fn dust_order() -> PaperOrder {
        PaperOrder {
            quantity: quantity(dec!(0.005)),
            ..long_order()
        }
    }

    fn tiers() -> MaintenanceTiers {
        MaintenanceTiers::new(vec![
            MaintenanceTier::new(Usdc::zero(), None, dec!(0.025), Usdc::zero()).expect("tier"),
        ])
        .expect("tiers")
    }

    fn executable(book: OrderBook, as_of: i128, recovered_at: i128) -> ExecutableBook {
        let readiness = MarketExecutionReady::new(market(), time(recovered_at));
        ExecutableBook::new(book, time(as_of), duration(1_000), &readiness)
            .expect("fresh executable book")
    }

    fn mark(received_at: i128, value: Decimal, recovered_at: i128) -> super::ExecutableMark {
        mark_at(
            received_at,
            received_at,
            received_at,
            value,
            1_000,
            recovered_at,
            &format!("mark-{received_at}"),
        )
    }

    fn mark_at(
        event_time: i128,
        received_at: i128,
        as_of_time: i128,
        value: Decimal,
        maximum_age: i128,
        recovered_at: i128,
        event_id: &str,
    ) -> super::ExecutableMark {
        let readiness = MarketExecutionReady::new(market(), time(recovered_at));
        super::ExecutableMark::new(
            market(),
            EventId::new(event_id).expect("mark event ID"),
            price(value),
            time(event_time),
            time(received_at),
            time(as_of_time),
            duration(maximum_age),
            &readiness,
        )
        .expect("fresh executable mark")
    }

    fn funding(
        venue_at: i128,
        mark_value: Decimal,
        rate: Decimal,
        recovered_at: i128,
    ) -> ExecutableFunding {
        funding_at(
            venue_at,
            venue_at,
            venue_at,
            mark_value,
            rate,
            1_000,
            recovered_at,
            &format!("funding-{venue_at}"),
        )
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "tests bind every funding source field"
    )]
    fn funding_at(
        venue_at: i128,
        received_at: i128,
        as_of_time: i128,
        mark_value: Decimal,
        rate: Decimal,
        maximum_age: i128,
        recovered_at: i128,
        event_id: &str,
    ) -> ExecutableFunding {
        let readiness = MarketExecutionReady::new(market(), time(recovered_at));
        ExecutableFunding::new(
            market(),
            EventId::new(event_id).expect("funding event ID"),
            time(venue_at),
            time(received_at),
            time(as_of_time),
            FundingRate::new(rate),
            price(mark_value),
            duration(maximum_age),
            &readiness,
        )
        .expect("fresh executable funding")
    }

    fn book(
        event_time: i128,
        received_at: i128,
        sequence: u64,
        bid: Decimal,
        ask: Decimal,
        size: Decimal,
    ) -> OrderBook {
        let event = MarketEvent::book_snapshot(
            time(event_time),
            time(received_at),
            market(),
            BookSnapshot::new(
                sequence,
                vec![BookLevel::new(price(bid), quantity(size))],
                vec![BookLevel::new(price(ask), quantity(size))],
            ),
        )
        .expect("event");
        OrderBook::apply_snapshot(None, &event, duration(1_000)).expect("book")
    }

    fn time(value: i128) -> TimestampNs {
        TimestampNs::new(value).expect("time")
    }
    fn duration(value: i128) -> DurationNs {
        DurationNs::new(value).expect("duration")
    }
    fn market() -> Market {
        Market::new("SOL").expect("market")
    }
    fn price(value: Decimal) -> Price {
        Price::new(value).expect("price")
    }
    fn quantity(value: Decimal) -> Quantity {
        Quantity::new(value).expect("quantity")
    }
    fn digest(character: char) -> String {
        std::iter::repeat_n(character, 64).collect()
    }
}
