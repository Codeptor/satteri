//! Checked units and identifiers used by deterministic paper-trading logic.

use std::str::FromStr;

use rust_decimal::Decimal;
use thiserror::Error;

/// A validation or checked-arithmetic failure for a domain value.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum DomainError {
    /// A price was zero or negative.
    #[error("price must be greater than zero")]
    NonPositivePrice,
    /// A price originated from a non-finite floating-point value.
    #[error("price must be finite")]
    NonFinitePrice,
    /// A finite floating-point price was outside the decimal range.
    #[error("price cannot be represented as a decimal")]
    PriceOutOfRange,
    /// A quantity was negative.
    #[error("quantity must not be negative")]
    NegativeQuantity,
    /// A quantity originated from a non-finite floating-point value.
    #[error("quantity must be finite")]
    NonFiniteQuantity,
    /// A finite floating-point quantity was outside the decimal range.
    #[error("quantity cannot be represented as a decimal")]
    QuantityOutOfRange,
    /// A USDC amount was negative.
    #[error("USDC amount must not be negative")]
    NegativeUsdc,
    /// A basis-point value was negative.
    #[error("basis points must not be negative")]
    NegativeBps,
    /// Leverage fell outside the paper engine's supported range.
    #[error("leverage must be between 5 and 20 inclusive, got {0}")]
    InvalidLeverage(u8),
    /// A required text identifier was empty, padded, or contained control bytes.
    #[error("{kind} must be nonempty, unpadded text without control characters")]
    InvalidIdentifier {
        /// The kind of identifier that failed validation.
        kind: &'static str,
    },
    /// A ledger name was not one of the two user-visible paper ledgers.
    #[error("unknown ledger name `{0}`")]
    UnknownLedger(String),
    /// Checked decimal arithmetic overflowed or produced an invalid unit value.
    #[error("checked arithmetic failed while calculating {operation}")]
    ArithmeticFailure {
        /// The calculation that could not be represented.
        operation: &'static str,
    },
}

/// A strictly positive decimal market price.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Price(Decimal);

impl Price {
    /// Creates a strictly positive price.
    ///
    /// # Errors
    ///
    /// Returns [`DomainError::NonPositivePrice`] for zero or negative input.
    pub fn new(value: Decimal) -> Result<Self, DomainError> {
        if value <= Decimal::ZERO {
            return Err(DomainError::NonPositivePrice);
        }
        Ok(Self(value))
    }

    /// Converts a finite `f64` to a checked decimal price explicitly.
    ///
    /// # Errors
    ///
    /// Rejects non-finite values, unrepresentable values, zero, and negatives.
    pub fn from_checked_f64(value: f64) -> Result<Self, DomainError> {
        if !value.is_finite() {
            return Err(DomainError::NonFinitePrice);
        }
        let decimal = Decimal::from_f64_retain(value).ok_or(DomainError::PriceOutOfRange)?;
        Self::new(decimal)
    }

    /// Returns the checked decimal value without discarding its price unit at construction.
    #[must_use]
    pub const fn value(self) -> Decimal {
        self.0
    }

    /// Multiplies this price by a quantity to produce USDC notional.
    ///
    /// # Errors
    ///
    /// Returns [`DomainError::ArithmeticFailure`] if the result cannot be represented.
    pub fn checked_notional(self, quantity: Quantity) -> Result<Usdc, DomainError> {
        self.0
            .checked_mul(quantity.0)
            .ok_or(DomainError::ArithmeticFailure {
                operation: "price notional",
            })
            .and_then(Usdc::new)
    }
}

/// A nonnegative decimal asset quantity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Quantity(Decimal);

impl Quantity {
    /// Creates a nonnegative quantity.
    ///
    /// # Errors
    ///
    /// Returns [`DomainError::NegativeQuantity`] for negative input.
    pub fn new(value: Decimal) -> Result<Self, DomainError> {
        if value < Decimal::ZERO {
            return Err(DomainError::NegativeQuantity);
        }
        Ok(Self(value))
    }

    /// Converts a finite `f64` to a checked decimal quantity explicitly.
    ///
    /// # Errors
    ///
    /// Rejects non-finite, unrepresentable, and negative values.
    pub fn from_checked_f64(value: f64) -> Result<Self, DomainError> {
        if !value.is_finite() {
            return Err(DomainError::NonFiniteQuantity);
        }
        let decimal = Decimal::from_f64_retain(value).ok_or(DomainError::QuantityOutOfRange)?;
        Self::new(decimal)
    }

    /// Returns the checked decimal quantity.
    #[must_use]
    pub const fn value(self) -> Decimal {
        self.0
    }
}

/// A nonnegative decimal amount of synthetic USDC.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Usdc(Decimal);

impl Usdc {
    /// Creates a nonnegative synthetic-USDC amount.
    ///
    /// # Errors
    ///
    /// Returns [`DomainError::NegativeUsdc`] for negative input.
    pub fn new(value: Decimal) -> Result<Self, DomainError> {
        if value < Decimal::ZERO {
            return Err(DomainError::NegativeUsdc);
        }
        Ok(Self(value))
    }

    /// Returns the checked decimal USDC amount.
    #[must_use]
    pub const fn value(self) -> Decimal {
        self.0
    }

    /// Adds two USDC amounts with overflow checking.
    ///
    /// # Errors
    ///
    /// Returns [`DomainError::ArithmeticFailure`] if the sum cannot be represented.
    pub fn checked_add(self, other: Self) -> Result<Self, DomainError> {
        self.0
            .checked_add(other.0)
            .ok_or(DomainError::ArithmeticFailure {
                operation: "USDC addition",
            })
            .and_then(Self::new)
    }

    /// Calculates the fee charged at an explicit basis-point rate.
    ///
    /// # Errors
    ///
    /// Returns [`DomainError::ArithmeticFailure`] if the fee cannot be represented.
    pub fn checked_fee(self, rate: Bps) -> Result<Self, DomainError> {
        let scaled = self
            .0
            .checked_mul(rate.0)
            .and_then(|value| value.checked_div(Decimal::from(10_000)))
            .ok_or(DomainError::ArithmeticFailure {
                operation: "USDC fee",
            })?;
        Self::new(scaled)
    }
}

/// A nonnegative decimal rate measured in basis points.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Bps(Decimal);

impl Bps {
    /// Creates a nonnegative basis-point rate.
    ///
    /// # Errors
    ///
    /// Returns [`DomainError::NegativeBps`] for negative input.
    pub fn new(value: Decimal) -> Result<Self, DomainError> {
        if value < Decimal::ZERO {
            return Err(DomainError::NegativeBps);
        }
        Ok(Self(value))
    }

    /// Returns the checked decimal basis-point value.
    #[must_use]
    pub const fn value(self) -> Decimal {
        self.0
    }
}

/// Integer isolated leverage constrained to the approved `5..=20` range.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Leverage(u8);

impl Leverage {
    /// Creates approved leverage between 5x and 20x inclusive.
    ///
    /// # Errors
    ///
    /// Returns [`DomainError::InvalidLeverage`] outside `5..=20`.
    pub fn new(value: u8) -> Result<Self, DomainError> {
        if !(5..=20).contains(&value) {
            return Err(DomainError::InvalidLeverage(value));
        }
        Ok(Self(value))
    }

    /// Returns the integer leverage multiplier.
    #[must_use]
    pub const fn value(self) -> u8 {
        self.0
    }
}

macro_rules! text_identifier {
    ($name:ident, $kind:literal, $docs:literal) => {
        #[doc = $docs]
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(String);

        impl $name {
            #[doc = concat!("Creates a checked ", $kind, ".")]
            ///
            /// # Errors
            ///
            /// Rejects empty, padded, or control-character-containing text.
            pub fn new(value: impl Into<String>) -> Result<Self, DomainError> {
                let value = value.into();
                if value.is_empty() || value.trim() != value || value.chars().any(char::is_control)
                {
                    return Err(DomainError::InvalidIdentifier { kind: $kind });
                }
                Ok(Self(value))
            }

            #[doc = concat!("Returns the checked ", $kind, " text.")]
            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }
    };
}

text_identifier!(
    Market,
    "market",
    "A checked native-perpetual market identifier."
);
text_identifier!(RunId, "run ID", "A checked deterministic run identifier.");
text_identifier!(
    EventId,
    "event ID",
    "A checked normalized-event identifier."
);

/// Aggressor direction of a market event or executable paper order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Side {
    /// Buy from visible asks.
    Buy,
    /// Sell into visible bids.
    Sell,
}

/// Decision cadence that owns a paper position.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Sleeve {
    /// Completed 15-minute bars.
    FifteenMinute,
    /// Completed one-hour bars.
    OneHour,
}

/// One of the two independently accounted user-visible paper ledgers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LedgerId {
    /// The deterministic rules-only ledger.
    RulesOnly,
    /// The validated ML-champion ledger.
    MlChampion,
}

impl FromStr for LedgerId {
    type Err = DomainError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "rules_only" => Ok(Self::RulesOnly),
            "ml_champion" => Ok(Self::MlChampion),
            unknown => Err(DomainError::UnknownLedger(unknown.to_owned())),
        }
    }
}

/// Whether rules are collecting data or using a validated frozen artifact.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RulesMode {
    /// Collect data without producing strategy decisions.
    CollectOnly,
    /// Run the validated active rules artifact.
    Active,
}

impl FromStr for RulesMode {
    type Err = DomainError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "collect_only" => Ok(Self::CollectOnly),
            "active" => Ok(Self::Active),
            _ => Err(DomainError::InvalidIdentifier { kind: "rules mode" }),
        }
    }
}

/// The only supported paper-position margin mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MarginMode {
    /// Margin isolated to a single paper position.
    Isolated,
}

impl FromStr for MarginMode {
    type Err = DomainError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "isolated" => Ok(Self::Isolated),
            _ => Err(DomainError::InvalidIdentifier {
                kind: "margin mode",
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use super::{
        Bps, DomainError, EventId, LedgerId, Leverage, Market, Price, Quantity, RunId, Usdc,
    };

    #[test]
    fn price_rejects_zero() {
        assert_eq!(
            Price::new(Decimal::ZERO),
            Err(DomainError::NonPositivePrice)
        );
    }

    #[test]
    fn price_rejects_negative_values() {
        assert_eq!(Price::new(dec!(-0.01)), Err(DomainError::NonPositivePrice));
    }

    #[test]
    fn checked_f64_price_rejects_non_finite_values() {
        for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            assert_eq!(
                Price::from_checked_f64(value),
                Err(DomainError::NonFinitePrice)
            );
        }
    }

    #[test]
    fn quantity_rejects_negative_values() {
        assert_eq!(
            Quantity::new(dec!(-0.01)),
            Err(DomainError::NegativeQuantity)
        );
    }

    #[test]
    fn ledger_id_rejects_unknown_names() {
        assert_eq!(
            LedgerId::from_str("shadow"),
            Err(DomainError::UnknownLedger("shadow".into()))
        );
    }

    #[test]
    fn leverage_rejects_values_below_five() {
        assert_eq!(Leverage::new(4), Err(DomainError::InvalidLeverage(4)));
    }

    #[test]
    fn leverage_rejects_values_above_twenty() {
        assert_eq!(Leverage::new(21), Err(DomainError::InvalidLeverage(21)));
    }

    #[test]
    fn price_times_quantity_preserves_usdc_units() -> Result<(), DomainError> {
        let notional = Price::new(dec!(12.5))?.checked_notional(Quantity::new(dec!(4))?)?;

        assert_eq!(notional, Usdc::new(dec!(50))?);
        Ok(())
    }

    #[test]
    fn fee_calculation_preserves_usdc_units() -> Result<(), DomainError> {
        let fee = Usdc::new(dec!(100))?.checked_fee(Bps::new(dec!(7.5))?)?;

        assert_eq!(fee, Usdc::new(dec!(0.075))?);
        Ok(())
    }

    #[test]
    fn checked_decimal_arithmetic_reports_overflow() -> Result<(), DomainError> {
        let max_usdc = Usdc::new(Decimal::MAX)?;

        assert_eq!(
            max_usdc.checked_add(Usdc::new(Decimal::ONE)?),
            Err(DomainError::ArithmeticFailure {
                operation: "USDC addition",
            })
        );
        assert_eq!(
            Price::new(Decimal::MAX)?.checked_notional(Quantity::new(dec!(2))?),
            Err(DomainError::ArithmeticFailure {
                operation: "price notional",
            })
        );
        assert_eq!(
            max_usdc.checked_fee(Bps::new(Decimal::MAX)?),
            Err(DomainError::ArithmeticFailure {
                operation: "USDC fee",
            })
        );
        Ok(())
    }

    #[test]
    fn checked_f64_quantity_rejects_non_finite_and_out_of_range_values() {
        for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            assert_eq!(
                Quantity::from_checked_f64(value),
                Err(DomainError::NonFiniteQuantity)
            );
        }
        assert_eq!(
            Quantity::from_checked_f64(f64::MAX),
            Err(DomainError::QuantityOutOfRange)
        );
    }

    #[test]
    fn money_and_rate_units_reject_negative_values() {
        assert_eq!(Usdc::new(dec!(-0.01)), Err(DomainError::NegativeUsdc));
        assert_eq!(Bps::new(dec!(-0.01)), Err(DomainError::NegativeBps));
    }

    #[test]
    fn identifiers_reject_empty_padded_and_control_text() {
        let cases = ["", " BTC", "BTC ", "BT\nC", "BT\u{7f}C"];

        for value in cases {
            assert_eq!(
                Market::new(value),
                Err(DomainError::InvalidIdentifier { kind: "market" })
            );
            assert_eq!(
                RunId::new(value),
                Err(DomainError::InvalidIdentifier { kind: "run ID" })
            );
            assert_eq!(
                EventId::new(value),
                Err(DomainError::InvalidIdentifier { kind: "event ID" })
            );
        }
    }
}
