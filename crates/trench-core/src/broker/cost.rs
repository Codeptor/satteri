//! Frozen primary taker fees for deterministic paper execution.

use rust_decimal::Decimal;
use thiserror::Error;

use crate::domain::{DomainError, Usdc};

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

/// A checked error from primary fee computation.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CostError {
    /// A checked synthetic-USDC computation failed.
    #[error(transparent)]
    Domain(#[from] DomainError),
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
