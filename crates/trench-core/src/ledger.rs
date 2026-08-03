//! Immutable isolated-margin ledger transitions for one paper experiment.

use rust_decimal::Decimal;
use thiserror::Error;

use crate::book::OrderBook;
use crate::domain::{DomainError, LedgerId, Leverage, MarginMode, Market, Price, Quantity, Usdc};
use crate::event::TimestampNs;
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

/// One complete close fill for the sole isolated position.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExitFill {
    price: Price,
    fee: Usdc,
}

impl ExitFill {
    /// Creates a complete close fill from validated units.
    #[must_use]
    pub const fn new(price: Price, fee: Usdc) -> Self {
        Self { price, fee }
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

/// The sole open isolated position, when a ledger is not flat.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Position {
    market: Market,
    side: PositionSide,
    quantity: Quantity,
    entry_price: Price,
    leverage: Leverage,
    isolated_margin: Usdc,
    entry_fee: Usdc,
    funding_debits: Usdc,
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

    /// Returns the margin isolated to this position only.
    #[must_use]
    pub const fn isolated_margin(&self) -> Usdc {
        self.isolated_margin
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
    /// Funding was debited from cash and the isolated position.
    FundingDebited,
    /// The sole isolated position was completely closed.
    PositionClosed,
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
    isolated_margin: Usdc,
    position: Option<Position>,
    realized_pnl: Decimal,
    unrealized_pnl: Decimal,
    fees_paid: Usdc,
    funding_paid: Usdc,
    equity: Usdc,
    breakers: BreakerState,
    last_executable_mark: Option<ExecutableMark>,
    fresh_book_market: Option<Market>,
    executable_mark_stale: bool,
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
            isolated_margin: Usdc::zero(),
            position: None,
            realized_pnl: Decimal::ZERO,
            unrealized_pnl: Decimal::ZERO,
            fees_paid: Usdc::zero(),
            funding_paid: Usdc::zero(),
            equity: cash,
            breakers: BreakerState::new(opened_at, cash)?,
            last_executable_mark: None,
            fresh_book_market: None,
            executable_mark_stale: true,
            liquidity_incomplete: false,
        })
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

    /// Returns the margin reserved to the sole isolated position.
    #[must_use]
    pub const fn isolated_margin(&self) -> Usdc {
        self.isolated_margin
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

    /// Returns executable equity: cash plus isolated margin plus marked adjustment.
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

    /// Returns whether no fresh executable valuation is currently available.
    #[must_use]
    pub const fn executable_mark_stale(&self) -> bool {
        self.executable_mark_stale
    }

    /// Returns whether the current mark used the mandatory 200-bps depth fallback.
    #[must_use]
    pub const fn liquidity_incomplete(&self) -> bool {
        self.liquidity_incomplete
    }

    /// Applies a fresh full-exit book valuation or preserves the last valuation on missing data.
    ///
    /// A long walks bids and a short walks asks for the full remaining asset
    /// quantity. When visible depth is insufficient, the residual is priced at
    /// the mandatory 200-bps boundary from the best executable quote.
    ///
    /// # Errors
    ///
    /// Rejects backward time, market mismatch, arithmetic failure, and a mark
    /// that cannot remain a nonnegative synthetic-equity state.
    pub fn mark_to_book(
        &self,
        at: TimestampNs,
        book: Option<&OrderBook>,
        costs: MarkCosts,
    ) -> Result<LedgerTransition, LedgerError> {
        let mut state = self.clone();
        match book {
            None => {
                state.breakers = self.breakers.record_equity(at, self.equity)?;
                state.executable_mark_stale = true;
                Ok(Self::transition(
                    at,
                    LedgerTransitionKind::BookMarked {
                        stale: true,
                        liquidity_incomplete: self.liquidity_incomplete,
                    },
                    state,
                ))
            }
            Some(book) => {
                if let Some(position) = &self.position {
                    if position.market != *book.market() {
                        return Err(LedgerError::BookMarketMismatch);
                    }
                    let mark = executable_mark(position, book, costs)?;
                    state.unrealized_pnl = marked_pnl(position, mark.exit_value, costs)?;
                    state.equity =
                        equity_from(state.cash, state.isolated_margin, state.unrealized_pnl)?;
                    state.breakers = self.breakers.record_equity(at, state.equity)?;
                    state.last_executable_mark = Some(mark);
                    state.liquidity_incomplete = mark.liquidity_incomplete;
                } else {
                    state.breakers = self.breakers.record_equity(at, state.equity)?;
                    state.last_executable_mark = None;
                    state.liquidity_incomplete = false;
                }
                state.fresh_book_market = Some(book.market().clone());
                state.executable_mark_stale = false;
                Ok(Self::transition(
                    at,
                    LedgerTransitionKind::BookMarked {
                        stale: false,
                        liquidity_incomplete: state.liquidity_incomplete,
                    },
                    state,
                ))
            }
        }
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
        if self.executable_mark_stale || self.fresh_book_market.as_ref() != Some(&entry.market) {
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
        let isolated_margin = Usdc::new(margin_value)?;
        let required = checked_add_usdc(isolated_margin, entry.fee, "entry cash requirement")?;
        let cash = checked_debit(self.cash, required)?;

        let mut state = self.clone();
        state.cash = cash;
        state.isolated_margin = isolated_margin;
        state.position = Some(Position {
            market: entry.market,
            side: entry.side,
            quantity: entry.quantity,
            entry_price: entry.price,
            leverage: entry.leverage,
            isolated_margin,
            entry_fee: entry.fee,
            funding_debits: Usdc::zero(),
        });
        state.unrealized_pnl = Decimal::ZERO;
        state.fees_paid = checked_add_usdc(self.fees_paid, entry.fee, "entry fee total")?;
        state.equity = equity_from(state.cash, state.isolated_margin, state.unrealized_pnl)?;
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

    /// Debits realized funding from cash and the sole isolated position.
    ///
    /// # Errors
    ///
    /// Rejects a flat ledger, backward time, insufficient cash, and arithmetic
    /// failures.
    pub fn apply_funding_debit(
        &self,
        at: TimestampNs,
        amount: Usdc,
    ) -> Result<LedgerTransition, LedgerError> {
        let position = self.position.as_ref().ok_or(LedgerError::NoOpenPosition)?;
        let mut state = self.clone();
        state.cash = checked_debit(self.cash, amount)?;
        state.funding_paid = checked_add_usdc(self.funding_paid, amount, "funding total")?;
        let mut updated_position = position.clone();
        updated_position.funding_debits =
            checked_add_usdc(position.funding_debits, amount, "position funding total")?;
        state.position = Some(updated_position);
        state.equity = equity_from(state.cash, state.isolated_margin, state.unrealized_pnl)?;
        state.breakers = self.breakers.record_equity(at, state.equity)?;
        Ok(Self::transition(
            at,
            LedgerTransitionKind::FundingDebited,
            state,
        ))
    }

    /// Applies a complete close fill and returns all isolated margin to cash.
    ///
    /// # Errors
    ///
    /// Rejects a flat ledger, backward time, insufficient cash, and arithmetic
    /// failures. Partial closes are intentionally absent: this ledger owns at
    /// most one complete isolated position.
    pub fn close_position(
        &self,
        at: TimestampNs,
        exit: ExitFill,
    ) -> Result<LedgerTransition, LedgerError> {
        let position = self.position.as_ref().ok_or(LedgerError::NoOpenPosition)?;
        let exit_value = exit.price.checked_notional(position.quantity)?;
        let gross_pnl = gross_pnl(position, exit_value)?;
        let credited = self
            .cash
            .value()
            .checked_add(position.isolated_margin.value())
            .and_then(|value| value.checked_add(gross_pnl))
            .and_then(|value| value.checked_sub(exit.fee.value()))
            .filter(|value| *value >= Decimal::ZERO)
            .ok_or(LedgerError::InsufficientCash)?;
        let cash = Usdc::new(credited)?;
        let fees_paid = checked_add_usdc(self.fees_paid, exit.fee, "exit fee total")?;
        let net_trade_pnl = gross_pnl
            .checked_sub(position.entry_fee.value())
            .and_then(|value| value.checked_sub(position.funding_debits.value()))
            .and_then(|value| value.checked_sub(exit.fee.value()))
            .ok_or(LedgerError::Arithmetic {
                operation: "net realized trade PnL",
            })?;

        let mut state = self.clone();
        state.cash = cash;
        state.isolated_margin = Usdc::zero();
        state.position = None;
        state.realized_pnl =
            self.realized_pnl
                .checked_add(gross_pnl)
                .ok_or(LedgerError::Arithmetic {
                    operation: "realized PnL total",
                })?;
        state.unrealized_pnl = Decimal::ZERO;
        state.fees_paid = fees_paid;
        state.equity = cash;
        state.breakers = self
            .breakers
            .record_closed_trade(at, net_trade_pnl)?
            .record_equity(at, state.equity)?;
        state.last_executable_mark = None;
        state.liquidity_incomplete = false;
        Ok(Self::transition(
            at,
            LedgerTransitionKind::PositionClosed,
            state,
        ))
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
    let entry_value = position.entry_price.checked_notional(position.quantity)?;
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
    isolated_margin: Usdc,
    adjustment: Decimal,
) -> Result<Usdc, LedgerError> {
    cash.value()
        .checked_add(isolated_margin.value())
        .and_then(|value| value.checked_add(adjustment))
        .filter(|value| *value >= Decimal::ZERO)
        .ok_or(LedgerError::NegativeEquity)
        .and_then(|value| Usdc::new(value).map_err(LedgerError::from))
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
