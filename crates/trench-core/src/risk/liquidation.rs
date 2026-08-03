//! Piecewise isolated-margin liquidation calculations over frozen venue tiers.
//!
//! The calculation deliberately uses the reference-equity form from the paper
//! design.  A candidate is valid only when the resulting liquidation notional
//! belongs to the same maintenance tier used to solve it.

use rust_decimal::Decimal;
use thiserror::Error;

use crate::domain::{DomainError, Price, Quantity, Usdc};
use crate::ledger::PositionSide;

/// One point-in-time venue maintenance tier, represented as `[lower, upper)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaintenanceTier {
    lower_notional: Usdc,
    upper_notional: Option<Usdc>,
    maintenance_rate: Decimal,
    maintenance_deduction: Usdc,
}

impl MaintenanceTier {
    /// Creates one maintenance tier with a nonnegative deduction and rate below one.
    ///
    /// # Errors
    ///
    /// Rejects an empty range or a rate outside `[0, 1)`.
    pub fn new(
        lower_notional: Usdc,
        upper_notional: Option<Usdc>,
        maintenance_rate: Decimal,
        maintenance_deduction: Usdc,
    ) -> Result<Self, LiquidationError> {
        if upper_notional.is_some_and(|upper| upper <= lower_notional) {
            return Err(LiquidationError::EmptyTierRange);
        }
        if !(Decimal::ZERO..Decimal::ONE).contains(&maintenance_rate) {
            return Err(LiquidationError::InvalidMaintenanceRate { maintenance_rate });
        }
        Ok(Self {
            lower_notional,
            upper_notional,
            maintenance_rate,
            maintenance_deduction,
        })
    }

    /// Returns this tier's inclusive lower liquidation-notional boundary.
    #[must_use]
    pub const fn lower_notional(self) -> Usdc {
        self.lower_notional
    }

    /// Returns this tier's exclusive upper liquidation-notional boundary.
    #[must_use]
    pub const fn upper_notional(self) -> Option<Usdc> {
        self.upper_notional
    }

    /// Returns the exact maintenance-margin rate.
    #[must_use]
    pub const fn maintenance_rate(self) -> Decimal {
        self.maintenance_rate
    }

    /// Returns the cumulative maintenance deduction.
    #[must_use]
    pub const fn maintenance_deduction(self) -> Usdc {
        self.maintenance_deduction
    }

    fn contains(self, notional: Usdc) -> bool {
        notional >= self.lower_notional
            && self
                .upper_notional
                .is_none_or(|upper_notional| notional < upper_notional)
    }
}

/// A complete contiguous point-in-time venue maintenance table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaintenanceTiers(Vec<MaintenanceTier>);

impl MaintenanceTiers {
    /// Creates a contiguous, ascending maintenance table beginning at zero notional.
    ///
    /// # Errors
    ///
    /// Rejects empty, gapped, overlapping, or nonterminal tables.
    pub fn new(tiers: Vec<MaintenanceTier>) -> Result<Self, LiquidationError> {
        let Some(first) = tiers.first() else {
            return Err(LiquidationError::EmptyTierTable);
        };
        if first.lower_notional != Usdc::zero() {
            return Err(LiquidationError::TierTableMustStartAtZero);
        }
        for pair in tiers.windows(2) {
            let previous = pair[0];
            let next = pair[1];
            if previous.upper_notional != Some(next.lower_notional) {
                return Err(LiquidationError::NonContiguousTierTable);
            }
        }
        if tiers
            .get(..tiers.len().saturating_sub(1))
            .is_some_and(|nonterminal| nonterminal.iter().any(|tier| tier.upper_notional.is_none()))
        {
            return Err(LiquidationError::NonterminalOpenEndedTier);
        }
        Ok(Self(tiers))
    }

    /// Returns the frozen venue tiers in ascending notional order.
    #[must_use]
    pub fn as_slice(&self) -> &[MaintenanceTier] {
        &self.0
    }
}

/// Inputs to the reference-equity isolated-liquidation equation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiquidationInput {
    quantity: Quantity,
    side: PositionSide,
    reference_price: Price,
    isolated_equity: Usdc,
    tiers: MaintenanceTiers,
}

impl LiquidationInput {
    /// Creates a checked isolated-margin liquidation request.
    ///
    /// # Errors
    ///
    /// Rejects a zero position before tier solving.
    pub fn new(
        quantity: Quantity,
        side: PositionSide,
        reference_price: Price,
        isolated_equity: Usdc,
        tiers: MaintenanceTiers,
    ) -> Result<Self, LiquidationError> {
        if quantity.value().is_zero() {
            return Err(LiquidationError::ZeroQuantity);
        }
        Ok(Self {
            quantity,
            side,
            reference_price,
            isolated_equity,
            tiers,
        })
    }

    /// Returns the absolute open position quantity.
    #[must_use]
    pub const fn quantity(&self) -> Quantity {
        self.quantity
    }

    /// Returns the direction that determines adverse liquidation movement.
    #[must_use]
    pub const fn side(&self) -> PositionSide {
        self.side
    }

    /// Returns the point-in-time mark used for reference-equity maintenance.
    #[must_use]
    pub const fn reference_price(&self) -> Price {
        self.reference_price
    }

    /// Returns isolated equity after booked fees, funding, and unrealized PnL.
    #[must_use]
    pub const fn isolated_equity(&self) -> Usdc {
        self.isolated_equity
    }

    /// Returns the frozen point-in-time maintenance table.
    #[must_use]
    pub const fn tiers(&self) -> &MaintenanceTiers {
        &self.tiers
    }
}

/// A tier-valid liquidation result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LiquidationResult {
    price: Price,
    tier_index: usize,
    maintenance_rate: Decimal,
    maintenance_deduction: Usdc,
}

impl LiquidationResult {
    /// Returns the positive mark-price liquidation threshold.
    #[must_use]
    pub const fn price(self) -> Price {
        self.price
    }

    /// Returns the only maintenance-tier index consistent with this threshold.
    #[must_use]
    pub const fn tier_index(self) -> usize {
        self.tier_index
    }

    /// Returns the rate used by the tier-valid equation.
    #[must_use]
    pub const fn maintenance_rate(self) -> Decimal {
        self.maintenance_rate
    }

    /// Returns the maintenance deduction used by the tier-valid equation.
    #[must_use]
    pub const fn maintenance_deduction(self) -> Usdc {
        self.maintenance_deduction
    }
}

/// Deterministic failures before a tier-valid isolated liquidation can be produced.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum LiquidationError {
    /// A position without size has no liquidation threshold.
    #[error("liquidation quantity must be greater than zero")]
    ZeroQuantity,
    /// A maintenance tier's upper boundary did not exceed its lower boundary.
    #[error("maintenance tier has an empty notional range")]
    EmptyTierRange,
    /// The maintenance rate would make the long-side denominator zero or negative.
    #[error("maintenance rate must be in [0, 1), got {maintenance_rate}")]
    InvalidMaintenanceRate {
        /// Invalid exact venue rate.
        maintenance_rate: Decimal,
    },
    /// No venue tiers were supplied.
    #[error("maintenance tier table must not be empty")]
    EmptyTierTable,
    /// The first venue tier must begin at zero notional.
    #[error("maintenance tier table must begin at zero notional")]
    TierTableMustStartAtZero,
    /// A tier boundary leaves a gap or overlap in the table.
    #[error("maintenance tiers must be contiguous and ascending")]
    NonContiguousTierTable,
    /// An unbounded tier was followed by another tier.
    #[error("only the final maintenance tier may be open-ended")]
    NonterminalOpenEndedTier,
    /// No candidate tier was self-consistent at the solved liquidation notional.
    #[error("no maintenance tier contains the solved liquidation notional")]
    NoApplicableTier,
    /// The position has no positive-price liquidation threshold under supplied equity.
    #[error("position has no positive-price liquidation threshold")]
    NoPositiveLiquidationPrice,
    /// Exact decimal arithmetic could not represent a required operation.
    #[error("liquidation arithmetic failed while calculating {operation}")]
    Arithmetic {
        /// Failed operation label.
        operation: &'static str,
    },
    /// A computed decimal failed to satisfy its domain unit.
    #[error(transparent)]
    Domain(#[from] DomainError),
}

/// Solves the isolated reference-equity liquidation equation over venue tiers.
///
/// The result is accepted only when `quantity * liquidation_price` is inside
/// the tier that supplied its maintenance rate and deduction.  A tier boundary
/// therefore cannot be evaluated with a neighbouring tier's parameters.
///
/// # Errors
///
/// Returns an explicit error when no tier is self-consistent, the threshold is
/// nonpositive, or exact decimal arithmetic cannot represent the result.
pub fn calculate(input: &LiquidationInput) -> Result<LiquidationResult, LiquidationError> {
    for (tier_index, tier) in input.tiers.as_slice().iter().copied().enumerate() {
        let price = solve_tier(input, tier)?;
        let notional = checked_notional(input.quantity, price)?;
        if tier.contains(notional) {
            return Ok(LiquidationResult {
                price,
                tier_index,
                maintenance_rate: tier.maintenance_rate,
                maintenance_deduction: tier.maintenance_deduction,
            });
        }
    }
    Err(LiquidationError::NoApplicableTier)
}

fn solve_tier(input: &LiquidationInput, tier: MaintenanceTier) -> Result<Price, LiquidationError> {
    let quantity = input.quantity.value();
    let reference_price = input.reference_price.value();
    let reference_notional =
        quantity
            .checked_mul(reference_price)
            .ok_or(LiquidationError::Arithmetic {
                operation: "reference notional",
            })?;
    let maintenance_margin = reference_notional
        .checked_mul(tier.maintenance_rate)
        .and_then(|value| value.checked_sub(tier.maintenance_deduction.value()))
        .ok_or(LiquidationError::Arithmetic {
            operation: "reference maintenance margin",
        })?;
    let available_margin = input
        .isolated_equity
        .value()
        .checked_sub(maintenance_margin)
        .ok_or(LiquidationError::Arithmetic {
            operation: "available reference margin",
        })?;
    let side = match input.side {
        PositionSide::Long => Decimal::ONE,
        PositionSide::Short => -Decimal::ONE,
    };
    let denominator = Decimal::ONE
        .checked_sub(tier.maintenance_rate.checked_mul(side).ok_or(
            LiquidationError::Arithmetic {
                operation: "liquidation denominator rate",
            },
        )?)
        .ok_or(LiquidationError::Arithmetic {
            operation: "liquidation denominator",
        })?;
    let movement = available_margin
        .checked_div(quantity)
        .and_then(|value| value.checked_div(denominator))
        .ok_or(LiquidationError::Arithmetic {
            operation: "liquidation movement",
        })?;
    let signed_movement = side
        .checked_mul(movement)
        .ok_or(LiquidationError::Arithmetic {
            operation: "signed liquidation movement",
        })?;
    let liquidation_price =
        reference_price
            .checked_sub(signed_movement)
            .ok_or(LiquidationError::Arithmetic {
                operation: "liquidation price",
            })?;
    Price::new(liquidation_price).map_err(|error| match error {
        DomainError::NonPositivePrice => LiquidationError::NoPositiveLiquidationPrice,
        other => LiquidationError::Domain(other),
    })
}

fn checked_notional(quantity: Quantity, price: Price) -> Result<Usdc, LiquidationError> {
    quantity
        .value()
        .checked_mul(price.value())
        .ok_or(LiquidationError::Arithmetic {
            operation: "liquidation notional",
        })
        .and_then(|value| Usdc::new(value).map_err(LiquidationError::from))
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use super::{LiquidationError, LiquidationInput, MaintenanceTier, MaintenanceTiers, calculate};
    use crate::domain::{Price, Quantity, Usdc};
    use crate::ledger::PositionSide;

    fn usdc(value: Decimal) -> Usdc {
        Usdc::new(value).expect("valid synthetic USDC")
    }

    fn price(value: Decimal) -> Price {
        Price::new(value).expect("valid price")
    }

    fn quantity(value: Decimal) -> Quantity {
        Quantity::new(value).expect("valid quantity")
    }

    fn one_tier(rate: Decimal) -> MaintenanceTiers {
        MaintenanceTiers::new(vec![
            MaintenanceTier::new(usdc(dec!(0)), None, rate, usdc(dec!(0)))
                .expect("valid maintenance tier"),
        ])
        .expect("valid table")
    }

    #[test]
    fn reference_equity_equation_matches_long_and_short_golden_examples() {
        let common = |side| {
            LiquidationInput::new(
                quantity(dec!(1)),
                side,
                price(dec!(100)),
                usdc(dec!(5)),
                one_tier(dec!(0.025)),
            )
            .expect("valid input")
        };

        let long = calculate(&common(PositionSide::Long)).expect("long liquidation");
        let short = calculate(&common(PositionSide::Short)).expect("short liquidation");

        assert_eq!(long.price().value(), dec!(100) - dec!(2.5) / dec!(0.975));
        assert_eq!(short.price().value(), dec!(100) + dec!(2.5) / dec!(1.025));
    }

    #[test]
    fn solver_accepts_only_the_tier_containing_its_own_liquidation_notional() {
        let tiers = MaintenanceTiers::new(vec![
            MaintenanceTier::new(
                usdc(dec!(0)),
                Some(usdc(dec!(100))),
                dec!(0.025),
                usdc(dec!(0)),
            )
            .expect("lower tier"),
            MaintenanceTier::new(usdc(dec!(100)), None, dec!(0.05), usdc(dec!(0)))
                .expect("upper tier"),
        ])
        .expect("contiguous tiers");
        let input = LiquidationInput::new(
            quantity(dec!(1)),
            PositionSide::Long,
            price(dec!(120)),
            usdc(dec!(5)),
            tiers,
        )
        .expect("valid input");

        let result = calculate(&input).expect("upper tier is self-consistent");
        assert_eq!(result.tier_index(), 1);
        assert_eq!(result.maintenance_rate(), dec!(0.05));
        assert!(result.price().value() >= dec!(100));
    }

    #[test]
    fn booked_funding_moves_liquidation_adversely_for_both_sides() {
        let long_before = LiquidationInput::new(
            quantity(dec!(1)),
            PositionSide::Long,
            price(dec!(100)),
            usdc(dec!(5)),
            one_tier(dec!(0.025)),
        )
        .expect("input");
        let long_after = LiquidationInput::new(
            quantity(dec!(1)),
            PositionSide::Long,
            price(dec!(100)),
            usdc(dec!(4)),
            one_tier(dec!(0.025)),
        )
        .expect("input");
        let short_before = LiquidationInput::new(
            quantity(dec!(1)),
            PositionSide::Short,
            price(dec!(100)),
            usdc(dec!(5)),
            one_tier(dec!(0.025)),
        )
        .expect("input");
        let short_after = LiquidationInput::new(
            quantity(dec!(1)),
            PositionSide::Short,
            price(dec!(100)),
            usdc(dec!(4)),
            one_tier(dec!(0.025)),
        )
        .expect("input");

        assert!(
            calculate(&long_after).expect("long after").price()
                > calculate(&long_before).expect("long before").price()
        );
        assert!(
            calculate(&short_after).expect("short after").price()
                < calculate(&short_before).expect("short before").price()
        );
    }

    #[test]
    fn malformed_tier_tables_and_nonpositive_thresholds_fail_closed() {
        assert!(matches!(
            MaintenanceTiers::new(Vec::new()),
            Err(LiquidationError::EmptyTierTable)
        ));
        assert!(matches!(
            MaintenanceTier::new(usdc(dec!(0)), None, Decimal::ONE, usdc(dec!(0))),
            Err(LiquidationError::InvalidMaintenanceRate { .. })
        ));
        let high_equity = LiquidationInput::new(
            quantity(dec!(1)),
            PositionSide::Long,
            price(dec!(100)),
            usdc(dec!(200)),
            one_tier(dec!(0.025)),
        )
        .expect("input");
        assert!(matches!(
            calculate(&high_equity),
            Err(LiquidationError::NoPositiveLiquidationPrice)
        ));
    }
}
