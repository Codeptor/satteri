//! Frozen primary taker fees for deterministic paper execution.

use rust_decimal::Decimal;
use thiserror::Error;

use crate::{
    book::OrderBook,
    broker::fill::QuantityWalk,
    domain::{DomainError, Price, Quantity, Side, Usdc},
    ledger::PositionSide,
};

const PROTOCOL_TAKER_BPS: Decimal = Decimal::from_parts(45, 0, 0, false, 1);
const BUILDER_FEE_BPS: Decimal = Decimal::from_parts(30, 0, 0, false, 1);

/// The frozen 4.5-bps protocol plus 3.0-bps builder primary fee schedule.
///
/// It has no mutable public constructor: paper results cannot silently assume
/// discounts, maker rebates, staking, or referral tiers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TakerFeeSchedule {
    _private: (),
}

impl TakerFeeSchedule {
    /// Returns the approved 7.5-bps-per-filled-side paper schedule.
    #[must_use]
    pub const fn lowest_tier() -> Self {
        Self { _private: () }
    }

    /// Returns the frozen protocol fee in basis points.
    #[must_use]
    pub const fn protocol_bps(self) -> Decimal {
        PROTOCOL_TAKER_BPS
    }

    /// Returns the frozen builder-fee assumption in basis points.
    #[must_use]
    pub const fn builder_bps(self) -> Decimal {
        BUILDER_FEE_BPS
    }

    /// Calculates fees on actual filled notional only.
    ///
    /// # Errors
    ///
    /// Returns an error if exact decimal arithmetic cannot be represented.
    pub fn for_filled_notional(self, notional: Usdc) -> Result<FeeBreakdown, CostError> {
        let protocol_fee = fee_for_bps(notional, self.protocol_bps())?;
        let builder_fee = fee_for_bps(notional, self.builder_bps())?;
        let total_fee = protocol_fee.checked_add(builder_fee)?;
        Ok(FeeBreakdown {
            protocol_fee,
            builder_fee,
            total_fee,
        })
    }
}

fn fee_for_bps(notional: Usdc, bps: Decimal) -> Result<Usdc, CostError> {
    let value = notional
        .value()
        .checked_mul(bps)
        .and_then(|value| value.checked_div(Decimal::from(10_000)))
        .ok_or(DomainError::ArithmeticFailure {
            operation: "paper taker fee",
        })?;
    Ok(Usdc::new(value)?)
}

/// Exact protocol and builder fees paid by one actual primary taker fill.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FeeBreakdown {
    protocol_fee: Usdc,
    builder_fee: Usdc,
    total_fee: Usdc,
}

impl FeeBreakdown {
    /// Returns the protocol fee component.
    #[must_use]
    pub const fn protocol_fee(self) -> Usdc {
        self.protocol_fee
    }

    /// Returns the frozen builder-fee component.
    #[must_use]
    pub const fn builder_fee(self) -> Usdc {
        self.builder_fee
    }

    /// Returns the exact 7.5-bps-per-side total.
    #[must_use]
    pub const fn total_fee(self) -> Usdc {
        self.total_fee
    }
}

/// A signed synthetic-USDC attribution. Positive values are debits/losses;
/// negative values are funding receipts or favorable price movement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SignedUsdc(Decimal);

impl SignedUsdc {
    pub(crate) const fn new(value: Decimal) -> Self {
        Self(value)
    }

    /// Returns the exact signed synthetic-USDC attribution.
    #[must_use]
    pub const fn value(self) -> Decimal {
        self.0
    }

    /// Returns an exact zero attribution.
    #[must_use]
    pub const fn zero() -> Self {
        Self(Decimal::ZERO)
    }
}

/// Non-overlapping cost and alpha attribution for an actual taker fill.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutionCost {
    fees: FeeBreakdown,
    spread: Usdc,
    depth_slippage: Usdc,
    latency_loss: SignedUsdc,
    funding: SignedUsdc,
    gross_alpha: SignedUsdc,
}

#[allow(
    dead_code,
    reason = "Task 13 consumes complete broker execution records"
)]
impl ExecutionCost {
    /// Returns protocol and builder fees charged on actual filled notional.
    #[must_use]
    pub const fn fees(self) -> FeeBreakdown {
        self.fees
    }

    /// Returns immediate visible half-spread crossing cost.
    #[must_use]
    pub const fn spread(self) -> Usdc {
        self.spread
    }

    /// Returns additional visible-depth walk cost.
    #[must_use]
    pub const fn depth_slippage(self) -> Usdc {
        self.depth_slippage
    }

    /// Returns the signed decision-to-book gap, including adverse latency loss.
    #[must_use]
    pub const fn latency_loss(self) -> SignedUsdc {
        self.latency_loss
    }

    /// Returns signed funding allocated to this record, initially zero for fills.
    #[must_use]
    pub const fn funding(self) -> SignedUsdc {
        self.funding
    }

    /// Returns signed gross alpha before fees and execution costs.
    #[must_use]
    pub const fn gross_alpha(self) -> SignedUsdc {
        self.gross_alpha
    }

    pub(crate) fn with_exit_alpha(
        mut self,
        position_side: PositionSide,
        entry_price: Price,
        exit_vwap: Price,
        quantity: Quantity,
    ) -> Result<Self, CostError> {
        let price_change = match position_side {
            PositionSide::Long => exit_vwap.value().checked_sub(entry_price.value()),
            PositionSide::Short => entry_price.value().checked_sub(exit_vwap.value()),
        }
        .ok_or(CostError::Arithmetic {
            operation: "gross alpha price change",
        })?;
        self.gross_alpha = SignedUsdc::new(price_change.checked_mul(quantity.value()).ok_or(
            CostError::Arithmetic {
                operation: "gross alpha",
            },
        )?);
        Ok(self)
    }
}

/// Attributes a primary taker fill without using mid or mark as its fill price.
///
/// Spread, depth, and signed decision-to-book gap reconcile exactly to the
/// adverse difference between actual VWAP and the decision/trigger reference.
/// An in-band real gap may exceed planned risk and remains visible here; it is
/// never silently re-priced to the reference decision.
#[allow(
    dead_code,
    reason = "Task 13 consumes complete broker execution records"
)]
pub(crate) fn attribute_taker_execution(
    schedule: TakerFeeSchedule,
    book: &OrderBook,
    side: Side,
    walk: &QuantityWalk,
    reference_price: Price,
) -> Result<ExecutionCost, CostError> {
    let vwap = walk.vwap().map_err(|error| CostError::Fill {
        source: Box::new(error),
    })?;
    let best = match side {
        Side::Buy => book.asks(),
        Side::Sell => book.bids(),
    }
    .first()
    .ok_or(CostError::Arithmetic {
        operation: "best execution price",
    })?
    .price();
    let midpoint = midpoint(book)?;
    let quantity = walk.filled_quantity();
    Ok(ExecutionCost {
        fees: schedule.for_filled_notional(walk.filled_notional())?,
        spread: nonnegative_adverse_notional(side, best, midpoint, quantity)?,
        depth_slippage: nonnegative_adverse_notional(side, vwap, best, quantity)?,
        latency_loss: SignedUsdc::new(signed_adverse_notional(
            side,
            midpoint,
            reference_price,
            quantity,
        )?),
        funding: SignedUsdc::zero(),
        gross_alpha: SignedUsdc::zero(),
    })
}

fn midpoint(book: &OrderBook) -> Result<Price, CostError> {
    let bid = book.bids().first().ok_or(CostError::Arithmetic {
        operation: "best bid",
    })?;
    let ask = book.asks().first().ok_or(CostError::Arithmetic {
        operation: "best ask",
    })?;
    let value = bid
        .price()
        .value()
        .checked_add(ask.price().value())
        .and_then(|value| value.checked_div(Decimal::TWO))
        .ok_or(CostError::Arithmetic {
            operation: "book midpoint",
        })?;
    Ok(Price::new(value)?)
}

fn nonnegative_adverse_notional(
    side: Side,
    later_price: Price,
    earlier_price: Price,
    quantity: Quantity,
) -> Result<Usdc, CostError> {
    Ok(Usdc::new(
        signed_adverse_notional(side, later_price, earlier_price, quantity)?.max(Decimal::ZERO),
    )?)
}

fn signed_adverse_notional(
    side: Side,
    later_price: Price,
    earlier_price: Price,
    quantity: Quantity,
) -> Result<Decimal, CostError> {
    let difference = match side {
        Side::Buy => later_price.value().checked_sub(earlier_price.value()),
        Side::Sell => earlier_price.value().checked_sub(later_price.value()),
    }
    .ok_or(CostError::Arithmetic {
        operation: "adverse execution price difference",
    })?;
    difference
        .checked_mul(quantity.value())
        .ok_or(CostError::Arithmetic {
            operation: "adverse execution notional",
        })
}

/// A checked error from primary fee computation.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CostError {
    /// A checked synthetic-USDC computation failed.
    #[error(transparent)]
    Domain(#[from] DomainError),
    /// Exact execution arithmetic failed.
    #[error("checked arithmetic failed while calculating {operation}")]
    Arithmetic {
        /// Failed calculation.
        operation: &'static str,
    },
    /// A visible execution had no fill price to attribute.
    #[error("visible execution attribution requires a fill: {source}")]
    Fill {
        /// Underlying empty visible execution.
        source: Box<crate::broker::fill::FillError>,
    },
}

#[cfg(test)]
mod tests {
    use rust_decimal_macros::dec;

    use super::TakerFeeSchedule;
    use crate::domain::Usdc;

    #[test]
    fn lowest_tier_taker_fees_are_seven_point_five_bps_per_filled_side() {
        let costs = TakerFeeSchedule::lowest_tier();
        let breakdown = costs
            .for_filled_notional(Usdc::new(dec!(1_000)).expect("positive notional"))
            .expect("fee arithmetic");

        assert_eq!(breakdown.protocol_fee().value(), dec!(0.45));
        assert_eq!(breakdown.builder_fee().value(), dec!(0.30));
        assert_eq!(breakdown.total_fee().value(), dec!(0.75));
    }
}
