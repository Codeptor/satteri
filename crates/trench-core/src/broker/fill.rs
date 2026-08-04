//! Exact primary-taker visible-book quantity walks.

use rust_decimal::Decimal;
use thiserror::Error;

use crate::book::OrderBook;
use crate::domain::{Bps, DomainError, Price, Quantity, Side, Usdc};

const BPS_DENOMINATOR: u32 = 10_000;

/// One visible L2 level consumed by a primary taker order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TakerLevelFill {
    price: Price,
    quantity: Quantity,
    notional: Usdc,
}

impl TakerLevelFill {
    /// Returns the exact source level price.
    #[must_use]
    pub const fn price(self) -> Price {
        self.price
    }

    /// Returns filled base quantity at this level.
    #[must_use]
    pub const fn quantity(self) -> Quantity {
        self.quantity
    }

    /// Returns filled quote notional at this level.
    #[must_use]
    pub const fn notional(self) -> Usdc {
        self.notional
    }
}

/// Exact result of an IOC walk by base quantity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuantityWalk {
    levels: Vec<TakerLevelFill>,
    requested_quantity: Quantity,
    filled_quantity: Quantity,
    remaining_quantity: Quantity,
    filled_notional: Usdc,
}

impl QuantityWalk {
    /// Returns ordered visible levels consumed by this attempt.
    #[must_use]
    pub fn levels(&self) -> &[TakerLevelFill] {
        &self.levels
    }

    /// Returns immutable requested base quantity.
    #[must_use]
    pub const fn requested_quantity(&self) -> Quantity {
        self.requested_quantity
    }

    /// Returns actual filled base quantity, never over the request.
    #[must_use]
    pub const fn filled_quantity(&self) -> Quantity {
        self.filled_quantity
    }

    /// Returns the exact residual that was not executed.
    #[must_use]
    pub const fn remaining_quantity(&self) -> Quantity {
        self.remaining_quantity
    }

    /// Returns filled visible quote notional only.
    #[must_use]
    pub const fn filled_notional(&self) -> Usdc {
        self.filled_notional
    }

    /// Returns whether the requested base quantity fully executed.
    #[must_use]
    pub const fn is_complete(&self) -> bool {
        self.remaining_quantity.value().is_zero()
    }

    /// Returns exact actual fill VWAP when at least one level filled.
    ///
    /// # Errors
    ///
    /// Returns an error for an empty visible execution.
    pub fn vwap(&self) -> Result<Price, FillError> {
        if self.filled_quantity.value().is_zero() {
            return Err(FillError::NoFill);
        }
        let value = self
            .filled_notional
            .value()
            .checked_div(self.filled_quantity.value())
            .ok_or(FillError::Arithmetic {
                operation: "walk VWAP",
            })?;
        Ok(Price::new(value)?)
    }
}

/// A deterministic visible-depth-walk failure.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum FillError {
    /// A zero base quantity is not an executable order.
    #[error("requested base quantity must be positive")]
    ZeroRequestedQuantity,
    /// An IOC band cannot span a complete 100-percent price move.
    #[error("IOC limit band must be below 10000 bps")]
    InvalidLimitBand,
    /// No visible level filled, so no execution VWAP exists.
    #[error("the visible book produced no fill")]
    NoFill,
    /// Checked decimal arithmetic failed.
    #[error("checked arithmetic failed while calculating {operation}")]
    Arithmetic {
        /// Failed checked calculation.
        operation: &'static str,
    },
    /// A checked domain unit was invalid.
    #[error(transparent)]
    Domain(#[from] DomainError),
}

/// Walks immutable visible liquidity to fill at most an exact base quantity.
///
/// This deliberately differs from quote-notional walking: it cannot overfill a
/// reduce-only exit when price changes across levels. A missing/depth-limited
/// book returns its residual instead of manufacturing a close.
pub fn walk_visible_quantity(
    book: &OrderBook,
    side: Side,
    requested_quantity: Quantity,
    limit_band: Bps,
) -> Result<QuantityWalk, FillError> {
    if requested_quantity.value().is_zero() {
        return Err(FillError::ZeroRequestedQuantity);
    }
    if limit_band.value() >= Decimal::from(BPS_DENOMINATOR) {
        return Err(FillError::InvalidLimitBand);
    }
    let levels = match side {
        Side::Buy => book.asks(),
        Side::Sell => book.bids(),
    };
    let best = levels.first().ok_or(FillError::NoFill)?.price();
    let limit = limit_price(best, side, limit_band)?;
    walk_visible_quantity_to_price(book, side, requested_quantity, limit)
}

/// Walks visible liquidity against an explicit marketable-limit price.
///
/// This is used for a sealed entry limit derived from the risk-approved
/// reference price, so a tight post-gap book cannot bypass sizing slippage.
pub(crate) fn walk_visible_quantity_to_price(
    book: &OrderBook,
    side: Side,
    requested_quantity: Quantity,
    limit: Price,
) -> Result<QuantityWalk, FillError> {
    if requested_quantity.value().is_zero() {
        return Err(FillError::ZeroRequestedQuantity);
    }
    let levels = match side {
        Side::Buy => book.asks(),
        Side::Sell => book.bids(),
    };
    let mut remaining_quantity = requested_quantity;
    let mut filled_notional = Usdc::zero();
    let mut fills = Vec::new();

    for level in levels
        .iter()
        .take_while(|level| is_within_limit(level.price(), side, limit))
    {
        if remaining_quantity.value().is_zero() {
            break;
        }
        let quantity = level.quantity().min(remaining_quantity);
        let notional = level.price().checked_notional(quantity)?;
        remaining_quantity = Quantity::new(
            remaining_quantity
                .value()
                .checked_sub(quantity.value())
                .ok_or(FillError::Arithmetic {
                    operation: "remaining walk quantity",
                })?,
        )?;
        filled_notional = filled_notional.checked_add(notional)?;
        fills.push(TakerLevelFill {
            price: level.price(),
            quantity,
            notional,
        });
    }
    let filled_quantity = Quantity::new(
        requested_quantity
            .value()
            .checked_sub(remaining_quantity.value())
            .ok_or(FillError::Arithmetic {
                operation: "filled walk quantity",
            })?,
    )?;
    Ok(QuantityWalk {
        levels: fills,
        requested_quantity,
        filled_quantity,
        remaining_quantity,
        filled_notional,
    })
}

fn limit_price(best: Price, side: Side, band: Bps) -> Result<Price, FillError> {
    let adjustment = best
        .value()
        .checked_mul(band.value())
        .and_then(|value| value.checked_div(Decimal::from(BPS_DENOMINATOR)))
        .ok_or(FillError::Arithmetic {
            operation: "IOC limit adjustment",
        })?;
    let value = match side {
        Side::Buy => best.value().checked_add(adjustment),
        Side::Sell => best.value().checked_sub(adjustment),
    }
    .ok_or(FillError::Arithmetic {
        operation: "IOC limit price",
    })?;
    Ok(Price::new(value)?)
}

fn is_within_limit(price: Price, side: Side, limit: Price) -> bool {
    match side {
        Side::Buy => price <= limit,
        Side::Sell => price >= limit,
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal_macros::dec;

    use super::walk_visible_quantity;
    use crate::book::OrderBook;
    use crate::domain::{Bps, Market, Price, Quantity, Side};
    use crate::event::{BookLevel, BookSnapshot, DurationNs, MarketEvent, TimestampNs};

    #[test]
    fn exact_quantity_walk_never_overfills_at_worse_prices() {
        let book = book(
            vec![level(dec!(99), dec!(3)), level(dec!(98), dec!(3))],
            vec![level(dec!(101), dec!(3)), level(dec!(102), dec!(3))],
        );
        let walk = walk_visible_quantity(
            &book,
            Side::Sell,
            quantity(dec!(4)),
            Bps::new(dec!(200)).expect("band"),
        )
        .expect("visible walk");

        assert_eq!(walk.filled_quantity().value(), dec!(4));
        assert_eq!(walk.remaining_quantity().value(), dec!(0));
        assert_eq!(walk.filled_notional().value(), dec!(395));
        assert_eq!(walk.levels().len(), 2);
        assert_eq!(walk.levels()[1].quantity().value(), dec!(1));
    }

    fn book(bids: Vec<BookLevel>, asks: Vec<BookLevel>) -> OrderBook {
        let at = TimestampNs::new(1_000).expect("time");
        let event = MarketEvent::book_snapshot(
            at,
            at,
            Market::new("SOL").expect("market"),
            BookSnapshot::new(1, bids, asks),
        )
        .expect("book event");
        OrderBook::apply_snapshot(None, &event, DurationNs::new(0).expect("age"))
            .expect("validated book")
    }

    fn level(price: rust_decimal::Decimal, size: rust_decimal::Decimal) -> BookLevel {
        BookLevel::new(Price::new(price).expect("price"), quantity(size))
    }

    fn quantity(value: rust_decimal::Decimal) -> Quantity {
        Quantity::new(value).expect("quantity")
    }
}
