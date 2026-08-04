//! Immutable isolated-margin ledger transitions for one paper experiment.

use blake3::Hasher;
use rust_decimal::Decimal;
use thiserror::Error;

use crate::book::OrderBook;
use crate::domain::{DomainError, LedgerId, Leverage, MarginMode, Market, Price, Quantity, Usdc};
use crate::event::{DurationNs, TimestampNs};
use crate::risk::breakers::{BreakerError, BreakerState};

const INITIAL_EQUITY: Decimal = Decimal::ONE_HUNDRED;
const MANDATORY_EXIT_BOUNDARY_FRACTION: Decimal = Decimal::from_parts(2, 0, 0, false, 2);

/// Direction of the one permitted isolated paper position.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PositionSide {
    /// A position that profits when the executable bid rises.
    Long,
    /// A position that profits when the executable ask falls.
    Short,
}

/// One accepted entry fill for an isolated position.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EntryFill {
    market: Market,
    side: PositionSide,
    quantity: Quantity,
    price: Price,
    leverage: Leverage,
    fee: Usdc,
}

impl EntryFill {
    /// Validates an entry fill without accepting a negative or zero size.
    ///
    /// # Errors
    ///
    /// Returns an error when its units are invalid or its size is zero.
    pub fn new(
        market: Market,
        side: PositionSide,
        quantity: Decimal,
        price: Price,
        leverage: Leverage,
        fee: Usdc,
    ) -> Result<Self, LedgerError> {
        let quantity = Quantity::new(quantity)?;
        if quantity.value().is_zero() {
            return Err(LedgerError::ZeroPositionSize);
        }
        Ok(Self {
            market,
            side,
            quantity,
            price,
            leverage,
            fee,
        })
    }
}

/// One actual reduce-only fill for the sole isolated position.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExitFill {
    quantity: Quantity,
    price: Price,
    fee: Usdc,
}

impl ExitFill {
    /// Creates an actual positive executed quantity from validated units.
    ///
    /// # Errors
    ///
    /// Rejects zero and negative executed quantity.
    pub fn new(quantity: Decimal, price: Price, fee: Usdc) -> Result<Self, LedgerError> {
        let quantity = Quantity::new(quantity)?;
        if quantity.value().is_zero() {
            return Err(LedgerError::ZeroPositionSize);
        }
        Ok(Self {
            quantity,
            price,
            fee,
        })
    }

    /// Returns the actual reduce-only executed quantity.
    #[must_use]
    pub const fn quantity(self) -> Quantity {
        self.quantity
    }

    /// Returns the actual execution price.
    #[must_use]
    pub const fn price(self) -> Price {
        self.price
    }

    /// Returns the fee charged on the actual fill only.
    #[must_use]
    pub const fn fee(self) -> Usdc {
        self.fee
    }
}

/// Exact signed funding cashflow. Positive values debit an isolated ledger;
/// negative values credit it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FundingCashflow(Decimal);

impl FundingCashflow {
    /// Creates an exact signed broker funding cashflow.
    ///
    /// Positive values debit isolated collateral; negative values credit it.
    #[must_use]
    pub const fn from_signed(value: Decimal) -> Self {
        Self(value)
    }

    /// Creates an exact funding debit.
    #[must_use]
    pub const fn debit(amount: Usdc) -> Self {
        Self(amount.value())
    }

    /// Creates an exact funding receipt.
    pub fn credit(amount: Usdc) -> Result<Self, LedgerError> {
        Decimal::ZERO
            .checked_sub(amount.value())
            .map(Self)
            .ok_or(LedgerError::Arithmetic {
                operation: "funding receipt sign",
            })
    }

    /// Returns the signed cashflow where positive is a debit.
    #[must_use]
    pub const fn value(self) -> Decimal {
        self.0
    }
}

/// Conservative charges reserved while marking an open position to executable depth.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MarkCosts {
    estimated_exit_fee: Usdc,
    estimated_exit_funding: Usdc,
}

impl MarkCosts {
    /// Creates conservative exit charges to subtract from executable equity.
    #[must_use]
    pub const fn new(estimated_exit_fee: Usdc, estimated_exit_funding: Usdc) -> Self {
        Self {
            estimated_exit_fee,
            estimated_exit_funding,
        }
    }

    /// Returns an exact zero-cost mark when no future exit cost is supplied.
    #[must_use]
    pub const fn none() -> Self {
        Self::new(Usdc::zero(), Usdc::zero())
    }
}

/// A finite, explicit upper bound for the age of an executable book source.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BookFreshness {
    max_age: DurationNs,
}

impl BookFreshness {
    /// Creates a typed finite maximum source-book age.
    #[must_use]
    pub const fn new(max_age: DurationNs) -> Self {
        Self { max_age }
    }

    /// Returns the maximum permitted age measured from source event time.
    #[must_use]
    pub const fn max_age(&self) -> DurationNs {
        self.max_age
    }
}

/// Source timestamps used to prove a book was available at a ledger transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BookSourceTimes {
    event_time: TimestampNs,
    received_at: TimestampNs,
}

impl BookSourceTimes {
    /// Returns the authoritative exchange event time for this book.
    #[must_use]
    pub const fn event_time(&self) -> TimestampNs {
        self.event_time
    }

    /// Returns the local receipt time for this book.
    #[must_use]
    pub const fn received_at(&self) -> TimestampNs {
        self.received_at
    }
}

/// Why a supplied book cannot supply executable valuation at the transition time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BookStaleReason {
    /// No source book was available for the mark.
    Missing,
    /// The source event itself occurred after the ledger transition.
    FutureEventTime,
    /// The book was not received until after the ledger transition.
    FutureReceiptTime,
    /// The source event age exceeded the explicit maximum.
    TooOld,
}

/// Freshness evidence retained by immutable ledger state after every book mark.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BookFreshnessStatus {
    /// No book mark has occurred yet.
    Unmarked,
    /// The source event and receipt were available within the explicit maximum age.
    Fresh {
        /// Event and receipt timestamps for the executable source.
        source: BookSourceTimes,
        /// Explicit maximum source age used for this decision.
        max_age: DurationNs,
    },
    /// The prior executable valuation was preserved because this source was unusable.
    Stale {
        /// The known source timestamps, absent only for a missing book.
        source: Option<BookSourceTimes>,
        /// Explicit maximum source age used for this decision.
        max_age: DurationNs,
        /// Deterministic reason this source cannot be used.
        reason: BookStaleReason,
    },
}

impl BookFreshnessStatus {
    const fn is_fresh(self) -> bool {
        matches!(self, Self::Fresh { .. })
    }

    fn is_fresh_at(self, at: TimestampNs) -> bool {
        let Self::Fresh { source, max_age } = self else {
            return false;
        };
        source.event_time <= at
            && source.received_at <= at
            && at
                .checked_duration_since(source.event_time)
                .is_ok_and(|age| age <= max_age)
    }
}

/// The sole open isolated position, when a ledger is not flat.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Position {
    market: Market,
    side: PositionSide,
    quantity: Quantity,
    entry_price: Price,
    leverage: Leverage,
    trade_cashflow: Decimal,
}

impl Position {
    /// Returns the position market.
    #[must_use]
    pub const fn market(&self) -> &Market {
        &self.market
    }

    /// Returns the long or short position direction.
    #[must_use]
    pub const fn side(&self) -> PositionSide {
        self.side
    }

    /// Returns remaining asset quantity.
    #[must_use]
    pub const fn quantity(&self) -> Quantity {
        self.quantity
    }

    /// Returns the complete-entry execution price.
    #[must_use]
    pub const fn entry_price(&self) -> Price {
        self.entry_price
    }

    /// Returns the fixed isolated leverage.
    #[must_use]
    pub const fn leverage(&self) -> Leverage {
        self.leverage
    }
}

/// Last full-exit valuation derived only from executable opposite-side depth.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutableMark {
    exit_value: Usdc,
    liquidity_incomplete: bool,
    estimated_exit_fee: Usdc,
    estimated_exit_funding: Usdc,
}

impl ExecutableMark {
    /// Returns the full remaining-position exit value after depth walking.
    #[must_use]
    pub const fn exit_value(&self) -> Usdc {
        self.exit_value
    }

    /// Returns whether absent visible depth was priced at the mandatory 200-bps boundary.
    #[must_use]
    pub const fn liquidity_incomplete(&self) -> bool {
        self.liquidity_incomplete
    }

    /// Returns the conservative estimated exit fee.
    #[must_use]
    pub const fn estimated_exit_fee(&self) -> Usdc {
        self.estimated_exit_fee
    }

    /// Returns the conservative estimated exit funding debit.
    #[must_use]
    pub const fn estimated_exit_funding(&self) -> Usdc {
        self.estimated_exit_funding
    }
}

/// A journalable immutable ledger transition kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LedgerTransitionKind {
    /// An executable book mark or a stale/missing-book preservation was recorded.
    BookMarked {
        /// Whether no fresh executable book was available.
        stale: bool,
        /// Whether the mark used the mandatory-exit boundary for missing depth.
        liquidity_incomplete: bool,
    },
    /// The sole isolated position was opened.
    PositionOpened,
    /// Signed funding was applied to cash and the isolated position.
    FundingApplied,
    /// The sole isolated position was partially reduced.
    PositionReduced,
    /// The sole isolated position was fully closed.
    PositionClosed,
    /// A broker-reported gap loss forfeited isolated collateral only.
    PositionLiquidated,
    /// UTC-period state was reset only after explicit reconciliation.
    Reconciled,
}

/// A journalable immutable ledger transition and its successor state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LedgerTransition {
    at: TimestampNs,
    kind: LedgerTransitionKind,
    state: LedgerState,
}

impl LedgerTransition {
    /// Returns the supplied explicit event time.
    #[must_use]
    pub const fn at(&self) -> TimestampNs {
        self.at
    }

    /// Returns the journalable mutation kind.
    #[must_use]
    pub const fn kind(&self) -> LedgerTransitionKind {
        self.kind
    }

    /// Borrows the immutable successor state.
    #[must_use]
    pub const fn state(&self) -> &LedgerState {
        &self.state
    }

    /// Consumes this record and returns the immutable successor state.
    #[must_use]
    pub fn into_state(self) -> LedgerState {
        self.state
    }
}

/// Errors raised before an invalid state transition can be committed.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum LedgerError {
    /// A checked domain value was invalid.
    #[error(transparent)]
    Domain(#[from] DomainError),
    /// A breaker rejected the transition.
    #[error(transparent)]
    Breaker(#[from] BreakerError),
    /// Position size must be strictly positive.
    #[error("position size must be greater than zero")]
    ZeroPositionSize,
    /// A ledger can own only one isolated position.
    #[error("ledger already has an open isolated position")]
    PositionAlreadyOpen,
    /// No position exists for a close or funding debit.
    #[error("ledger has no open isolated position")]
    NoOpenPosition,
    /// Planned trade risk exceeded the frozen 0.5-percent budget.
    #[error("planned loss exceeds the 0.5-percent trade budget")]
    TradeBudgetExceeded,
    /// A fresh executable book for the position market is required before entry.
    #[error("new entries require a fresh executable mark for their market")]
    StaleExecutableMark,
    /// A mark attempted to use a book for a different market.
    #[error("executable book market does not match the isolated position")]
    BookMarketMismatch,
    /// An entry, close, or debit would debit more cash than the ledger owns.
    #[error("isolated transition requires more cash than the ledger owns")]
    InsufficientCash,
    /// A mark would produce an invalid negative equity state.
    #[error("mark would produce negative synthetic equity")]
    NegativeEquity,
    /// A partial reduction exceeded the actual open quantity.
    #[error("reduce-only fill exceeds the isolated position quantity")]
    ReduceExceedsPosition,
    /// A broker liquidation forfeit exceeded live isolated collateral.
    #[error("broker liquidation forfeit exceeds live isolated collateral")]
    InvalidLiquidationForfeit,
    /// An actual exit exhausted collateral and must be paired with broker evidence.
    #[error("exit exhausted isolated collateral and requires a paired liquidation record")]
    CappedLiquidationRequired,
    /// Checked decimal arithmetic failed.
    #[error("checked arithmetic failed while calculating {operation}")]
    Arithmetic {
        /// The operation that failed.
        operation: &'static str,
    },
}

/// Immutable synthetic-USDC accounting for exactly one independent ledger.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LedgerState {
    ledger_id: LedgerId,
    cash: Usdc,
    isolated_collateral: Decimal,
    position: Option<Position>,
    realized_pnl: Decimal,
    unrealized_pnl: Decimal,
    fees_paid: Usdc,
    funding_paid: Usdc,
    funding_received: Usdc,
    equity: Usdc,
    breakers: BreakerState,
    last_executable_mark: Option<ExecutableMark>,
    fresh_book_market: Option<Market>,
    book_freshness: BookFreshnessStatus,
    liquidity_incomplete: bool,
}

impl LedgerState {
    /// Initializes one ledger with exactly 100 synthetic USDC and no position.
    ///
    /// # Errors
    ///
    /// Returns an error only if the fixed initial value cannot be represented.
    pub fn new(ledger_id: LedgerId, opened_at: TimestampNs) -> Result<Self, LedgerError> {
        let cash = Usdc::new(INITIAL_EQUITY)?;
        Ok(Self {
            ledger_id,
            cash,
            isolated_collateral: Decimal::ZERO,
            position: None,
            realized_pnl: Decimal::ZERO,
            unrealized_pnl: Decimal::ZERO,
            fees_paid: Usdc::zero(),
            funding_paid: Usdc::zero(),
            funding_received: Usdc::zero(),
            equity: cash,
            breakers: BreakerState::new(opened_at, cash)?,
            last_executable_mark: None,
            fresh_book_market: None,
            book_freshness: BookFreshnessStatus::Unmarked,
            liquidity_incomplete: false,
        })
    }

    pub(crate) fn commitment_digest(&self) -> String {
        let mut hasher = Hasher::new_derive_key("trench.ledger-state.v1");
        hash_component(
            &mut hasher,
            match self.ledger_id {
                LedgerId::RulesOnly => "rules_only",
                LedgerId::MlChampion => "ml_champion",
            },
        );
        for value in [
            self.cash.value(),
            self.isolated_collateral,
            self.realized_pnl,
            self.unrealized_pnl,
            self.fees_paid.value(),
            self.funding_paid.value(),
            self.funding_received.value(),
            self.equity.value(),
        ] {
            hash_component(&mut hasher, &value.to_string());
        }
        match &self.position {
            Some(position) => {
                hash_component(&mut hasher, "position");
                hash_component(&mut hasher, position.market.as_str());
                hash_component(
                    &mut hasher,
                    match position.side {
                        PositionSide::Long => "long",
                        PositionSide::Short => "short",
                    },
                );
                for value in [
                    position.quantity.value(),
                    position.entry_price.value(),
                    Decimal::from(position.leverage.value()),
                    position.trade_cashflow,
                ] {
                    hash_component(&mut hasher, &value.to_string());
                }
            }
            None => hash_component(&mut hasher, "flat"),
        }
        hash_breakers(&mut hasher, &self.breakers);
        match self.last_executable_mark {
            Some(mark) => {
                hash_component(&mut hasher, "mark");
                for value in [
                    mark.exit_value.value(),
                    mark.estimated_exit_fee.value(),
                    mark.estimated_exit_funding.value(),
                ] {
                    hash_component(&mut hasher, &value.to_string());
                }
                hash_component(
                    &mut hasher,
                    if mark.liquidity_incomplete { "1" } else { "0" },
                );
            }
            None => hash_component(&mut hasher, "no-mark"),
        }
        hash_component(
            &mut hasher,
            self.fresh_book_market
                .as_ref()
                .map_or("none", Market::as_str),
        );
        hash_book_freshness(&mut hasher, self.book_freshness);
        hash_component(
            &mut hasher,
            if self.liquidity_incomplete { "1" } else { "0" },
        );
        hasher.finalize().to_hex().to_string()
    }

    /// Returns the independent ledger identity.
    #[must_use]
    pub const fn ledger_id(&self) -> LedgerId {
        self.ledger_id
    }

    /// Returns the sole supported paper margin mode.
    #[must_use]
    pub const fn margin_mode(&self) -> MarginMode {
        MarginMode::Isolated
    }

    /// Returns freely available synthetic USDC.
    #[must_use]
    pub const fn cash(&self) -> Usdc {
        self.cash
    }

    /// Returns the live collateral held inside the isolated position.
    ///
    /// Funding and realized losses change this balance before they can affect
    /// free cash. A broker may temporarily carry a negative reference balance
    /// until its mandatory liquidation transition, so this is signed decimal
    /// accounting rather than a nonnegative [`Usdc`] amount.
    #[must_use]
    pub const fn isolated_collateral(&self) -> Decimal {
        self.isolated_collateral
    }

    /// Returns the sole open isolated position, if any.
    #[must_use]
    pub const fn position(&self) -> Option<&Position> {
        self.position.as_ref()
    }

    /// Returns closed price PnL before separately booked fees and funding debits.
    #[must_use]
    pub const fn realized_pnl(&self) -> Decimal {
        self.realized_pnl
    }

    /// Returns executable price PnL less reserved exit costs for an open position.
    #[must_use]
    pub const fn unrealized_pnl(&self) -> Decimal {
        self.unrealized_pnl
    }

    /// Returns all charged entry and exit fees.
    #[must_use]
    pub const fn fees_paid(&self) -> Usdc {
        self.fees_paid
    }

    /// Returns all charged funding debits.
    #[must_use]
    pub const fn funding_paid(&self) -> Usdc {
        self.funding_paid
    }

    /// Returns all funding receipts credited to isolated collateral.
    #[must_use]
    pub const fn funding_received(&self) -> Usdc {
        self.funding_received
    }

    /// Returns executable equity: free cash plus live collateral and marked adjustment.
    #[must_use]
    pub const fn equity(&self) -> Usdc {
        self.equity
    }

    /// Returns all frozen breaker accounting for this ledger only.
    #[must_use]
    pub const fn breakers(&self) -> &BreakerState {
        &self.breakers
    }

    /// Returns the all-time high-water executable equity.
    #[must_use]
    pub const fn high_water_equity(&self) -> Usdc {
        self.breakers.high_water_equity()
    }

    /// Returns the current reconciled UTC-day anchor.
    #[must_use]
    pub const fn daily_anchor(&self) -> TimestampNs {
        self.breakers.daily().anchor()
    }

    /// Returns the current reconciled UTC-week anchor.
    #[must_use]
    pub const fn weekly_anchor(&self) -> TimestampNs {
        self.breakers.weekly().anchor()
    }

    /// Returns accepted entries in the unreconciled UTC day.
    #[must_use]
    pub const fn entries_today(&self) -> u8 {
        self.breakers.entries_today()
    }

    /// Returns the current consecutive realized-loss count.
    #[must_use]
    pub const fn consecutive_losses(&self) -> u8 {
        self.breakers.consecutive_losses()
    }

    /// Returns the expiry of the active 12-hour entry cooldown, when any.
    #[must_use]
    pub const fn cooldown_until(&self) -> Option<TimestampNs> {
        self.breakers.cooldown_until()
    }

    /// Returns the last full-exit executable valuation, if a position was marked.
    #[must_use]
    pub const fn last_executable_mark(&self) -> Option<ExecutableMark> {
        self.last_executable_mark
    }

    /// Returns whether an executable book remains source-fresh at `at`.
    #[must_use]
    pub fn is_executable_mark_fresh_at(&self, at: TimestampNs) -> bool {
        self.book_freshness.is_fresh_at(at)
    }

    /// Returns whether no executable book remains source-fresh at `at`.
    #[must_use]
    pub fn is_executable_mark_stale_at(&self, at: TimestampNs) -> bool {
        !self.is_executable_mark_fresh_at(at)
    }

    /// Returns source-time evidence for the current executable-mark status.
    #[must_use]
    pub const fn book_freshness(&self) -> BookFreshnessStatus {
        self.book_freshness
    }

    /// Returns whether the current mark used the mandatory 200-bps depth fallback.
    #[must_use]
    pub const fn liquidity_incomplete(&self) -> bool {
        self.liquidity_incomplete
    }

    /// Applies a source-time-fresh full-exit valuation or preserves the last valuation.
    ///
    /// A long walks bids and a short walks asks for the full remaining asset
    /// quantity. When visible depth is insufficient, the residual is priced at
    /// the mandatory 200-bps boundary from the best executable quote.
    ///
    /// Future, missing, and too-old sources create a stale transition rather
    /// than a valuation. Freshness is measured from source event time under the
    /// caller-supplied typed bound, and a receipt after `at` is always rejected
    /// to prevent replay look-ahead.
    ///
    /// # Errors
    ///
    /// Rejects backward time, fresh-book market mismatch, arithmetic failure,
    /// and a mark that cannot remain a nonnegative synthetic-equity state.
    pub fn mark_to_book(
        &self,
        at: TimestampNs,
        book: Option<&OrderBook>,
        freshness: BookFreshness,
        costs: MarkCosts,
    ) -> Result<LedgerTransition, LedgerError> {
        let mut state = self.clone();
        let status = book_freshness_status(at, book, freshness);
        let Some(book) = book.filter(|_| status.is_fresh()) else {
            state.breakers = self.breakers.record_equity(at, self.equity)?;
            state.fresh_book_market = None;
            state.book_freshness = status;
            return Ok(Self::transition(
                at,
                LedgerTransitionKind::BookMarked {
                    stale: true,
                    liquidity_incomplete: self.liquidity_incomplete,
                },
                state,
            ));
        };

        if let Some(position) = &self.position {
            if position.market != *book.market() {
                return Err(LedgerError::BookMarketMismatch);
            }
            let mark = executable_mark(position, book, costs)?;
            state.unrealized_pnl = marked_pnl(position, mark.exit_value, costs)?;
            state.equity =
                equity_from(state.cash, state.isolated_collateral, state.unrealized_pnl)?;
            state.breakers = self.breakers.record_equity(at, state.equity)?;
            state.last_executable_mark = Some(mark);
            state.liquidity_incomplete = mark.liquidity_incomplete;
        } else {
            state.breakers = self.breakers.record_equity(at, state.equity)?;
            state.last_executable_mark = None;
            state.liquidity_incomplete = false;
        }
        state.fresh_book_market = Some(book.market().clone());
        state.book_freshness = status;
        Ok(Self::transition(
            at,
            LedgerTransitionKind::BookMarked {
                stale: false,
                liquidity_incomplete: state.liquidity_incomplete,
            },
            state,
        ))
    }

    /// Applies one complete isolated entry fill without mutating this state.
    ///
    /// # Errors
    ///
    /// Rejects stale books, overlapping positions, excess planned loss, active
    /// breakers, unaffordable margin, and checked-arithmetic failures.
    pub fn open_position(
        &self,
        at: TimestampNs,
        entry: EntryFill,
        planned_loss: Usdc,
    ) -> Result<LedgerTransition, LedgerError> {
        if self.position.is_some() {
            return Err(LedgerError::PositionAlreadyOpen);
        }
        if !self.is_executable_mark_fresh_at(at)
            || self.fresh_book_market.as_ref() != Some(&entry.market)
        {
            return Err(LedgerError::StaleExecutableMark);
        }
        if !self.breakers.allows_planned_loss(planned_loss, self.equity) {
            return Err(LedgerError::TradeBudgetExceeded);
        }
        if !self.breakers.entry_allowed(at) {
            return Err(LedgerError::Breaker(BreakerError::EntryBlocked));
        }

        let notional = entry.price.checked_notional(entry.quantity)?;
        let margin_value = notional
            .value()
            .checked_div(Decimal::from(entry.leverage.value()))
            .ok_or(LedgerError::Arithmetic {
                operation: "isolated margin",
            })?;
        let isolated_collateral = Usdc::new(margin_value)?;
        let required = checked_add_usdc(isolated_collateral, entry.fee, "entry cash requirement")?;
        let cash = checked_debit(self.cash, required)?;

        let mut state = self.clone();
        state.cash = cash;
        state.isolated_collateral = isolated_collateral.value();
        state.position = Some(Position {
            market: entry.market,
            side: entry.side,
            quantity: entry.quantity,
            entry_price: entry.price,
            leverage: entry.leverage,
            trade_cashflow: Decimal::ZERO.checked_sub(required.value()).ok_or(
                LedgerError::Arithmetic {
                    operation: "entry trade cashflow",
                },
            )?,
        });
        state.unrealized_pnl = Decimal::ZERO;
        state.fees_paid = checked_add_usdc(self.fees_paid, entry.fee, "entry fee total")?;
        state.equity = equity_from(state.cash, state.isolated_collateral, state.unrealized_pnl)?;
        state.breakers = self
            .breakers
            .record_entry(at)?
            .record_equity(at, state.equity)?;
        state.last_executable_mark = None;
        state.liquidity_incomplete = false;
        Ok(Self::transition(
            at,
            LedgerTransitionKind::PositionOpened,
            state,
        ))
    }

    /// Applies signed realized funding to the sole isolated collateral balance.
    ///
    /// # Errors
    ///
    /// Rejects a flat ledger, backward time, and checked arithmetic
    /// failures.
    pub fn apply_funding(
        &self,
        at: TimestampNs,
        cashflow: FundingCashflow,
    ) -> Result<LedgerTransition, LedgerError> {
        self.position.as_ref().ok_or(LedgerError::NoOpenPosition)?;
        let mut state = self.clone();
        let amount = cashflow.value();
        if amount >= Decimal::ZERO {
            let debit = Usdc::new(amount)?;
            state.funding_paid = checked_add_usdc(self.funding_paid, debit, "funding debit total")?;
        } else {
            let credit = Usdc::new(Decimal::ZERO.checked_sub(amount).ok_or(
                LedgerError::Arithmetic {
                    operation: "funding receipt",
                },
            )?)?;
            state.funding_received =
                checked_add_usdc(self.funding_received, credit, "funding receipt total")?;
        }
        state.isolated_collateral =
            self.isolated_collateral
                .checked_sub(amount)
                .ok_or(LedgerError::Arithmetic {
                    operation: "funding isolated collateral",
                })?;
        state.equity = equity_from(state.cash, state.isolated_collateral, state.unrealized_pnl)?;
        state.breakers = self.breakers.record_equity(at, state.equity)?;
        Ok(Self::transition(
            at,
            LedgerTransitionKind::FundingApplied,
            state,
        ))
    }

    /// Applies a nonnegative funding debit through the signed funding transition.
    ///
    /// This remains a narrow convenience for deterministic callers that have
    /// already established a debit direction.
    pub fn apply_funding_debit(
        &self,
        at: TimestampNs,
        amount: Usdc,
    ) -> Result<LedgerTransition, LedgerError> {
        self.apply_funding(at, FundingCashflow::debit(amount))
    }

    /// Applies one actual reduce-only fill without a broker liquidation cap.
    ///
    /// # Errors
    ///
    /// Rejects a flat ledger, an oversized reduction, a fill that requires a
    /// broker-reported collateral cap, insufficient fee cash, and checked
    /// arithmetic failures.
    pub fn reduce_position(
        &self,
        at: TimestampNs,
        exit: ExitFill,
    ) -> Result<LedgerTransition, LedgerError> {
        self.settle_exit(at, exit, None)
    }

    /// Atomically settles one actual exit fill with its broker-reported cap.
    ///
    /// A capped gap fill consumes the current isolated collateral, never free
    /// cash.  The forfeit must equal the live collateral immediately before the
    /// fill; this is the same quantity the broker reports in its paired
    /// `LiquidationLoss` record.
    pub fn settle_liquidated_exit(
        &self,
        at: TimestampNs,
        exit: ExitFill,
        forfeited_isolated_equity: Usdc,
    ) -> Result<LedgerTransition, LedgerError> {
        self.settle_exit(at, exit, Some(forfeited_isolated_equity))
    }

    fn settle_exit(
        &self,
        at: TimestampNs,
        exit: ExitFill,
        liquidation_forfeit: Option<Usdc>,
    ) -> Result<LedgerTransition, LedgerError> {
        let position = self.position.as_ref().ok_or(LedgerError::NoOpenPosition)?;
        if exit.quantity > position.quantity {
            return Err(LedgerError::ReduceExceedsPosition);
        }
        let exit_value = exit.price.checked_notional(exit.quantity)?;
        let gross_pnl = gross_pnl_for(position, exit_value, exit.quantity)?;
        let fraction = exit
            .quantity
            .value()
            .checked_div(position.quantity.value())
            .ok_or(LedgerError::Arithmetic {
                operation: "partial reduction fraction",
            })?;
        let allocated_collateral =
            self.isolated_collateral
                .checked_mul(fraction)
                .ok_or(LedgerError::Arithmetic {
                    operation: "allocated isolated collateral",
                })?;
        let settlement =
            allocated_collateral
                .checked_add(gross_pnl)
                .ok_or(LedgerError::Arithmetic {
                    operation: "partial exit settlement",
                })?;
        let residual_collateral = self
            .isolated_collateral
            .checked_sub(allocated_collateral)
            .and_then(|value| value.checked_add(settlement.min(Decimal::ZERO)))
            .ok_or(LedgerError::Arithmetic {
                operation: "remaining isolated collateral",
            })?;
        let requires_liquidation = residual_collateral < Decimal::ZERO;
        match (requires_liquidation, liquidation_forfeit) {
            (true, None) => return Err(LedgerError::CappedLiquidationRequired),
            (false, Some(_)) => return Err(LedgerError::InvalidLiquidationForfeit),
            (true, Some(forfeited)) => {
                let expected = Usdc::new(self.isolated_collateral.max(Decimal::ZERO))?;
                if forfeited != expected {
                    return Err(LedgerError::InvalidLiquidationForfeit);
                }
            }
            (false, None) => {}
        }
        let released_settlement = settlement.max(Decimal::ZERO);
        let cash_value = self
            .cash
            .value()
            .checked_add(released_settlement)
            .and_then(|value| value.checked_sub(exit.fee.value()))
            .filter(|value| *value >= Decimal::ZERO)
            .ok_or(LedgerError::InsufficientCash)?;
        let cash = Usdc::new(cash_value)?;
        let fees_paid = checked_add_usdc(self.fees_paid, exit.fee, "exit fee total")?;
        let trade_cashflow = position
            .trade_cashflow
            .checked_add(released_settlement)
            .and_then(|value| value.checked_sub(exit.fee.value()))
            .ok_or(LedgerError::Arithmetic {
                operation: "trade cashflow",
            })?;
        let remaining_quantity = Quantity::new(
            position
                .quantity
                .value()
                .checked_sub(exit.quantity.value())
                .ok_or(LedgerError::Arithmetic {
                    operation: "remaining position quantity",
                })?,
        )?;
        let mut state = self.clone();
        state.cash = cash;
        state.isolated_collateral = if requires_liquidation {
            Decimal::ZERO
        } else {
            residual_collateral
        };
        state.realized_pnl =
            self.realized_pnl
                .checked_add(gross_pnl)
                .ok_or(LedgerError::Arithmetic {
                    operation: "realized PnL total",
                })?;
        // The execution book has consumed its actual visible levels. Do not
        // reuse that liquidity to mark the residual; a later fresh book owns
        // the next executable valuation.
        state.unrealized_pnl = Decimal::ZERO;
        state.fees_paid = fees_paid;
        state.equity = equity_from(state.cash, state.isolated_collateral, state.unrealized_pnl)?;
        state.last_executable_mark = None;
        state.liquidity_incomplete = false;
        if remaining_quantity.value().is_zero() {
            state.isolated_collateral = Decimal::ZERO;
            state.position = None;
            state.equity = state.cash;
            state.breakers = self
                .breakers
                .record_closed_trade(at, trade_cashflow)?
                .record_equity(at, state.equity)?;
            Ok(Self::transition(
                at,
                if requires_liquidation {
                    LedgerTransitionKind::PositionLiquidated
                } else {
                    LedgerTransitionKind::PositionClosed
                },
                state,
            ))
        } else {
            state.position = Some(Position {
                quantity: remaining_quantity,
                trade_cashflow,
                ..position.clone()
            });
            state.fresh_book_market = None;
            state.book_freshness = BookFreshnessStatus::Stale {
                source: None,
                max_age: DurationNs::new(0).map_err(|_| LedgerError::Arithmetic {
                    operation: "residual mark freshness",
                })?,
                reason: BookStaleReason::Missing,
            };
            state.breakers = self.breakers.record_equity(at, state.equity)?;
            Ok(Self::transition(
                at,
                LedgerTransitionKind::PositionReduced,
                state,
            ))
        }
    }

    /// Applies a broker-reported capped liquidation without debiting free cash.
    ///
    /// The broker may report a gap loss beyond isolated collateral. That excess
    /// remains an auditable broker loss, never a synthetic-cash debit.
    pub fn settle_capped_liquidation(
        &self,
        at: TimestampNs,
        forfeited_isolated_equity: Usdc,
    ) -> Result<LedgerTransition, LedgerError> {
        let position = self.position.as_ref().ok_or(LedgerError::NoOpenPosition)?;
        let maximum_forfeit = Usdc::new(self.isolated_collateral.max(Decimal::ZERO))?;
        if forfeited_isolated_equity > maximum_forfeit {
            return Err(LedgerError::InvalidLiquidationForfeit);
        }
        let mut state = self.clone();
        state.isolated_collateral = Decimal::ZERO;
        state.position = None;
        state.unrealized_pnl = Decimal::ZERO;
        state.equity = state.cash;
        state.last_executable_mark = None;
        state.fresh_book_market = None;
        state.liquidity_incomplete = false;
        state.book_freshness = BookFreshnessStatus::Stale {
            source: None,
            max_age: DurationNs::new(0).map_err(|_| LedgerError::Arithmetic {
                operation: "liquidation mark freshness",
            })?,
            reason: BookStaleReason::Missing,
        };
        state.breakers = self
            .breakers
            .record_closed_trade(at, position.trade_cashflow)?
            .record_equity(at, state.equity)?;
        Ok(Self::transition(
            at,
            LedgerTransitionKind::PositionLiquidated,
            state,
        ))
    }

    /// Applies an actual complete exit through the reduction transition.
    pub fn close_position(
        &self,
        at: TimestampNs,
        exit: ExitFill,
    ) -> Result<LedgerTransition, LedgerError> {
        let position = self.position.as_ref().ok_or(LedgerError::NoOpenPosition)?;
        if exit.quantity != position.quantity {
            return Err(LedgerError::ReduceExceedsPosition);
        }
        self.reduce_position(at, exit)
    }

    /// Reconciles elapsed UTC day/week anchors without implicitly reading a clock.
    ///
    /// # Errors
    ///
    /// Rejects backward time and preserves hard-drawdown latching.
    pub fn reconcile(&self, at: TimestampNs) -> Result<LedgerTransition, LedgerError> {
        let mut state = self.clone();
        state.breakers = self.breakers.reconcile(at, self.equity)?;
        Ok(Self::transition(
            at,
            LedgerTransitionKind::Reconciled,
            state,
        ))
    }

    fn transition(at: TimestampNs, kind: LedgerTransitionKind, state: Self) -> LedgerTransition {
        LedgerTransition { at, kind, state }
    }
}

fn book_freshness_status(
    at: TimestampNs,
    book: Option<&OrderBook>,
    freshness: BookFreshness,
) -> BookFreshnessStatus {
    let Some(book) = book else {
        return BookFreshnessStatus::Stale {
            source: None,
            max_age: freshness.max_age,
            reason: BookStaleReason::Missing,
        };
    };
    let source = BookSourceTimes {
        event_time: book.event_time(),
        received_at: book.received_at(),
    };
    if source.event_time > at {
        return BookFreshnessStatus::Stale {
            source: Some(source),
            max_age: freshness.max_age,
            reason: BookStaleReason::FutureEventTime,
        };
    }
    if source.received_at > at {
        return BookFreshnessStatus::Stale {
            source: Some(source),
            max_age: freshness.max_age,
            reason: BookStaleReason::FutureReceiptTime,
        };
    }
    let Ok(age) = at.checked_duration_since(source.event_time) else {
        return BookFreshnessStatus::Stale {
            source: Some(source),
            max_age: freshness.max_age,
            reason: BookStaleReason::FutureEventTime,
        };
    };
    if age > freshness.max_age {
        return BookFreshnessStatus::Stale {
            source: Some(source),
            max_age: freshness.max_age,
            reason: BookStaleReason::TooOld,
        };
    }
    BookFreshnessStatus::Fresh {
        source,
        max_age: freshness.max_age,
    }
}

fn executable_mark(
    position: &Position,
    book: &OrderBook,
    costs: MarkCosts,
) -> Result<ExecutableMark, LedgerError> {
    let levels = match position.side {
        PositionSide::Long => book.bids(),
        PositionSide::Short => book.asks(),
    };
    let best = levels.first().ok_or(LedgerError::Arithmetic {
        operation: "best executable exit price",
    })?;
    let mut remaining = position.quantity.value();
    let mut exit_value = Decimal::ZERO;
    for level in levels {
        let fill = if level.quantity().value() <= remaining {
            level.quantity().value()
        } else {
            remaining
        };
        exit_value = exit_value
            .checked_add(level.price().value().checked_mul(fill).ok_or(
                LedgerError::Arithmetic {
                    operation: "visible executable exit value",
                },
            )?)
            .ok_or(LedgerError::Arithmetic {
                operation: "visible executable exit total",
            })?;
        remaining = remaining.checked_sub(fill).ok_or(LedgerError::Arithmetic {
            operation: "remaining executable exit quantity",
        })?;
        if remaining.is_zero() {
            break;
        }
    }
    let liquidity_incomplete = !remaining.is_zero();
    if liquidity_incomplete {
        let adjustment = best
            .price()
            .value()
            .checked_mul(MANDATORY_EXIT_BOUNDARY_FRACTION)
            .ok_or(LedgerError::Arithmetic {
                operation: "mandatory exit boundary adjustment",
            })?;
        let boundary = match position.side {
            PositionSide::Long => best.price().value().checked_sub(adjustment),
            PositionSide::Short => best.price().value().checked_add(adjustment),
        }
        .ok_or(LedgerError::Arithmetic {
            operation: "mandatory exit boundary price",
        })?;
        exit_value = exit_value
            .checked_add(
                boundary
                    .checked_mul(remaining)
                    .ok_or(LedgerError::Arithmetic {
                        operation: "mandatory exit boundary value",
                    })?,
            )
            .ok_or(LedgerError::Arithmetic {
                operation: "mandatory exit total",
            })?;
    }
    Ok(ExecutableMark {
        exit_value: Usdc::new(exit_value)?,
        liquidity_incomplete,
        estimated_exit_fee: costs.estimated_exit_fee,
        estimated_exit_funding: costs.estimated_exit_funding,
    })
}

fn marked_pnl(
    position: &Position,
    exit_value: Usdc,
    costs: MarkCosts,
) -> Result<Decimal, LedgerError> {
    let gross = gross_pnl(position, exit_value)?;
    gross
        .checked_sub(costs.estimated_exit_fee.value())
        .and_then(|value| value.checked_sub(costs.estimated_exit_funding.value()))
        .ok_or(LedgerError::Arithmetic {
            operation: "marked PnL after exit costs",
        })
}

fn gross_pnl(position: &Position, exit_value: Usdc) -> Result<Decimal, LedgerError> {
    gross_pnl_for(position, exit_value, position.quantity)
}

fn gross_pnl_for(
    position: &Position,
    exit_value: Usdc,
    quantity: Quantity,
) -> Result<Decimal, LedgerError> {
    let entry_value = position.entry_price.checked_notional(quantity)?;
    match position.side {
        PositionSide::Long => exit_value.value().checked_sub(entry_value.value()),
        PositionSide::Short => entry_value.value().checked_sub(exit_value.value()),
    }
    .ok_or(LedgerError::Arithmetic {
        operation: "gross position PnL",
    })
}

fn equity_from(
    cash: Usdc,
    isolated_collateral: Decimal,
    adjustment: Decimal,
) -> Result<Usdc, LedgerError> {
    cash.value()
        .checked_add(isolated_collateral)
        .and_then(|value| value.checked_add(adjustment))
        .filter(|value| *value >= Decimal::ZERO)
        .ok_or(LedgerError::NegativeEquity)
        .and_then(|value| Usdc::new(value).map_err(LedgerError::from))
}

fn hash_component(hasher: &mut Hasher, value: &str) {
    hasher.update(&(value.len() as u64).to_be_bytes());
    hasher.update(value.as_bytes());
}

fn hash_breakers(hasher: &mut Hasher, breakers: &BreakerState) {
    for budget in [breakers.daily(), breakers.weekly()] {
        for value in [
            budget.anchor().value().to_string(),
            budget.starting_equity().value().to_string(),
            budget.limit().value().to_string(),
            budget.used().value().to_string(),
        ] {
            hash_component(hasher, &value);
        }
        hash_component(hasher, if budget.tripped() { "1" } else { "0" });
    }
    for value in [
        breakers.high_water_equity().value().to_string(),
        breakers.entries_today().to_string(),
        breakers.consecutive_losses().to_string(),
        breakers.last_transition_at().value().to_string(),
    ] {
        hash_component(hasher, &value);
    }
    hash_component(
        hasher,
        if breakers.hard_drawdown_latched() {
            "1"
        } else {
            "0"
        },
    );
    hash_component(
        hasher,
        &breakers
            .cooldown_until()
            .map_or_else(|| "none".to_owned(), |at| at.value().to_string()),
    );
}

fn hash_book_freshness(hasher: &mut Hasher, status: BookFreshnessStatus) {
    match status {
        BookFreshnessStatus::Unmarked => hash_component(hasher, "unmarked"),
        BookFreshnessStatus::Fresh { source, max_age } => {
            hash_component(hasher, "fresh");
            hash_book_source(hasher, source);
            hash_component(hasher, &max_age.value().to_string());
        }
        BookFreshnessStatus::Stale {
            source,
            max_age,
            reason,
        } => {
            hash_component(hasher, "stale");
            match source {
                Some(source) => hash_book_source(hasher, source),
                None => hash_component(hasher, "no-source"),
            }
            hash_component(hasher, &max_age.value().to_string());
            hash_component(
                hasher,
                match reason {
                    BookStaleReason::Missing => "missing",
                    BookStaleReason::FutureEventTime => "future-event",
                    BookStaleReason::FutureReceiptTime => "future-receipt",
                    BookStaleReason::TooOld => "too-old",
                },
            );
        }
    }
}

fn hash_book_source(hasher: &mut Hasher, source: BookSourceTimes) {
    hash_component(hasher, &source.event_time().value().to_string());
    hash_component(hasher, &source.received_at().value().to_string());
}

fn checked_add_usdc(left: Usdc, right: Usdc, operation: &'static str) -> Result<Usdc, LedgerError> {
    left.value()
        .checked_add(right.value())
        .ok_or(LedgerError::Arithmetic { operation })
        .and_then(|value| Usdc::new(value).map_err(LedgerError::from))
}

fn checked_debit(cash: Usdc, debit: Usdc) -> Result<Usdc, LedgerError> {
    cash.value()
        .checked_sub(debit.value())
        .filter(|value| *value >= Decimal::ZERO)
        .ok_or(LedgerError::InsufficientCash)
        .and_then(|value| Usdc::new(value).map_err(LedgerError::from))
}
