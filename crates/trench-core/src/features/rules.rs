//! Auditable, deterministic rule-family scoring over immutable common features.

use rust_decimal::prelude::ToPrimitive;
use rust_decimal::{Decimal, RoundingStrategy};
use thiserror::Error;

const UNIT_DECIMAL_PLACES: u32 = 12;

/// A rule-scoring input could not be evaluated without weakening a frozen rule.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RuleScoreError {
    /// The requested nearest-rank percentile is outside the approved `(0, 1]` interval.
    #[error("percentile {percentile} must be in the open-closed interval (0, 1]")]
    InvalidPercentile {
        /// Rejected percentile fraction.
        percentile: Decimal,
    },
    /// A percentile requires at least one observed value.
    #[error("nearest-rank percentile requires at least one value")]
    EmptyPercentile,
    /// ATR percent or another positive volatility denominator was zero or negative.
    #[error("{field} must be strictly positive to scale a rules score, got {value}")]
    UnusableVolatilityScale {
        /// Immutable feature whose value cannot safely scale a score.
        field: &'static str,
        /// Rejected exact value.
        value: Decimal,
    },
    /// A feature advertised as a normalized fraction fell outside its declared range.
    #[error("{field} must be in [{minimum}, {maximum}], got {value}")]
    OutOfRange {
        /// Immutable feature whose checked domain was violated.
        field: &'static str,
        /// Lower inclusive bound.
        minimum: Decimal,
        /// Upper inclusive bound.
        maximum: Decimal,
        /// Rejected exact value.
        value: Decimal,
    },
    /// A decimal could not participate in the explicitly portable `libm::tanh` conversion.
    #[error("finite decimal could not be converted to portable tanh input")]
    TanhConversion,
    /// Decimal arithmetic failed while evaluating a frozen formula.
    #[error("decimal arithmetic failed while evaluating {operation}")]
    Arithmetic {
        /// Formula component whose checked arithmetic failed.
        operation: &'static str,
    },
}

/// Completed-hour inputs used for the frozen rules regime classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HourlyRuleInputs {
    ema_8: Decimal,
    ema_32: Decimal,
    atr_14: Decimal,
    adx_14: Decimal,
}

impl HourlyRuleInputs {
    /// Returns the hourly EMA(8).
    #[must_use]
    pub const fn ema_8(self) -> Decimal {
        self.ema_8
    }

    /// Returns the hourly EMA(32).
    #[must_use]
    pub const fn ema_32(self) -> Decimal {
        self.ema_32
    }

    /// Returns the hourly ATR(14).
    #[must_use]
    pub const fn atr_14(self) -> Decimal {
        self.atr_14
    }

    /// Returns the hourly ADX(14).
    #[must_use]
    pub const fn adx_14(self) -> Decimal {
        self.adx_14
    }
}

/// Scalar rule inputs already known at a completed strategy-bar boundary.
///
/// This internal representation is derived from [`FeatureSnapshot`] in the
/// strategy boundary; it has no source of truth apart from that immutable
/// snapshot and its checked long-horizon companion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuleInputs {
    close: Decimal,
    ema_8: Decimal,
    ema_32: Decimal,
    ema_8_slope_4: Decimal,
    atr_14: Decimal,
    atrp_14: Decimal,
    adx_14: Decimal,
    return_4: Decimal,
    return_16: Decimal,
    donchian_20_position: Decimal,
    volume_robust_z_20: Decimal,
    close_ema_20_residual_robust_z_20: Decimal,
    bid_depth_10bps: Decimal,
    ask_depth_10bps: Decimal,
    bid_depth_25bps: Decimal,
    ask_depth_25bps: Decimal,
    bid_depth_50bps: Decimal,
    ask_depth_50bps: Decimal,
    trade_imbalance_5m: Decimal,
    trade_imbalance_15m: Decimal,
    spread_bps: Decimal,
    premium: Decimal,
    open_interest_change_4: Decimal,
    funding_level: Decimal,
    cross_return_4_rank: Decimal,
    cross_return_16_rank: Decimal,
    low_10: Decimal,
    high_10: Decimal,
    hourly: HourlyRuleInputs,
}

impl RuleInputs {
    /// Returns the reference completed-bar close used for rule entry and exits.
    #[must_use]
    pub const fn close(&self) -> Decimal {
        self.close
    }

    /// Returns ATR(14) in price units.
    #[must_use]
    pub const fn atr_14(&self) -> Decimal {
        self.atr_14
    }

    /// Returns ATR(14) divided by close.
    #[must_use]
    pub const fn atrp_14(&self) -> Decimal {
        self.atrp_14
    }

    /// Returns the ten-bar completed low.
    #[must_use]
    pub const fn low_10(&self) -> Decimal {
        self.low_10
    }

    /// Returns the ten-bar completed high.
    #[must_use]
    pub const fn high_10(&self) -> Decimal {
        self.high_10
    }

    /// Returns the completed-hour regime inputs.
    #[must_use]
    pub const fn hourly(&self) -> HourlyRuleInputs {
        self.hourly
    }
}

/// Bounded historical inputs required by the frozen derivatives and volatility rules.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuleHistory {
    premium: Vec<Decimal>,
    open_interest_change_4: Vec<Decimal>,
    funding: Vec<Decimal>,
    hourly_realized_volatility_20: Vec<Decimal>,
    current_hourly_realized_volatility_20: Decimal,
}

/// One of the immutable rules regimes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Regime {
    /// Trend conditions are active.
    Trend,
    /// Range conditions are active.
    Range,
    /// Neither entry regime is active.
    Transition,
    /// The trailing realized-volatility distribution rejects new entries.
    ExtremeVolatility,
}

/// Regime result with its independent high-volatility threshold modifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RegimeAssessment {
    regime: Regime,
    high_volatility: bool,
    percentile_80: Decimal,
    percentile_95: Decimal,
}

impl RegimeAssessment {
    /// Returns the applicable structural regime.
    #[must_use]
    pub const fn regime(self) -> Regime {
        self.regime
    }

    /// Returns whether the frozen 0.10 entry-threshold surcharge applies.
    #[must_use]
    pub const fn high_volatility(self) -> bool {
        self.high_volatility
    }

    /// Returns the preceding realized-volatility 80th percentile.
    #[must_use]
    pub const fn percentile_80(self) -> Decimal {
        self.percentile_80
    }

    /// Returns the preceding realized-volatility 95th percentile.
    #[must_use]
    pub const fn percentile_95(self) -> Decimal {
        self.percentile_95
    }
}

/// All six clipped rule-family scores before regime weighting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuleScores {
    trend: Decimal,
    momentum: Decimal,
    mean_reversion: Decimal,
    microstructure: Decimal,
    derivatives: Decimal,
    cross_sectional: Decimal,
}

impl RuleScores {
    /// Returns the clipped trend score.
    #[must_use]
    pub const fn trend(self) -> Decimal {
        self.trend
    }

    /// Returns the clipped momentum/breakout score.
    #[must_use]
    pub const fn momentum(self) -> Decimal {
        self.momentum
    }

    /// Returns the clipped range-only mean-reversion score.
    #[must_use]
    pub const fn mean_reversion(self) -> Decimal {
        self.mean_reversion
    }

    /// Returns the clipped microstructure score.
    #[must_use]
    pub const fn microstructure(self) -> Decimal {
        self.microstructure
    }

    /// Returns the clipped derivatives/crowding score.
    #[must_use]
    pub const fn derivatives(self) -> Decimal {
        self.derivatives
    }

    /// Returns the clipped cross-sectional score.
    #[must_use]
    pub const fn cross_sectional(self) -> Decimal {
        self.cross_sectional
    }
}

/// Applies the frozen portable `unit(x) = tanh(x)` convention.
///
/// The calculation deliberately uses `libm::tanh`, then converts its finite result
/// back to a decimal rounded half-even to exactly twelve decimal places. This keeps
/// score fixtures byte-stable across supported platforms without introducing a
/// platform-specific C math dependency.
pub fn unit(value: Decimal) -> Result<Decimal, RuleScoreError> {
    let value = value.to_f64().ok_or(RuleScoreError::TanhConversion)?;
    let tanh = libm::tanh(value);
    if !tanh.is_finite() {
        return Err(RuleScoreError::TanhConversion);
    }
    Decimal::from_f64_retain(tanh)
        .ok_or(RuleScoreError::TanhConversion)
        .map(|value| {
            value.round_dp_with_strategy(UNIT_DECIMAL_PLACES, RoundingStrategy::MidpointNearestEven)
        })
}

/// Returns the frozen nearest-rank percentile using `ceil(p * n) - 1` indexing.
///
/// The input values are copied and sorted because all rule inputs are immutable.
/// In particular, `p = 0` must not underflow into an invalid index and `p > 1`
/// must not be silently clamped.
pub fn nearest_rank_percentile(
    values: &[Decimal],
    percentile: Decimal,
) -> Result<Decimal, RuleScoreError> {
    if percentile <= Decimal::ZERO || percentile > Decimal::ONE {
        return Err(RuleScoreError::InvalidPercentile { percentile });
    }
    let mut sorted = values.to_vec();
    sorted.sort();
    let count = Decimal::from(sorted.len());
    let rank = percentile
        .checked_mul(count)
        .ok_or(RuleScoreError::Arithmetic {
            operation: "nearest-rank percentile",
        })?
        .ceil()
        .to_usize()
        .ok_or(RuleScoreError::Arithmetic {
            operation: "nearest-rank percentile index",
        })?;
    let index = rank.checked_sub(1).ok_or(RuleScoreError::Arithmetic {
        operation: "nearest-rank percentile index",
    })?;
    sorted
        .get(index)
        .copied()
        .ok_or(RuleScoreError::EmptyPercentile)
}

/// Classifies the immutable completed-hour regime and volatility threshold modifier.
pub fn classify_regime(
    hourly: &HourlyRuleInputs,
    history: &RuleHistory,
) -> Result<RegimeAssessment, RuleScoreError> {
    ensure_positive("hourly_atr_14", hourly.atr_14)?;
    let percentile_80 =
        nearest_rank_percentile(&history.hourly_realized_volatility_20, Decimal::new(80, 2))?;
    let percentile_95 =
        nearest_rank_percentile(&history.hourly_realized_volatility_20, Decimal::new(95, 2))?;
    let current = history.current_hourly_realized_volatility_20;
    if current > percentile_95 {
        return Ok(RegimeAssessment {
            regime: Regime::ExtremeVolatility,
            high_volatility: false,
            percentile_80,
            percentile_95,
        });
    }

    let ema_distance = hourly
        .ema_8
        .checked_sub(hourly.ema_32)
        .ok_or(RuleScoreError::Arithmetic {
            operation: "hourly EMA distance",
        })?
        .abs()
        .checked_div(hourly.atr_14)
        .ok_or(RuleScoreError::Arithmetic {
            operation: "hourly trend scale",
        })?;
    let regime = if hourly.adx_14 >= Decimal::from(25) && ema_distance >= Decimal::new(35, 2) {
        Regime::Trend
    } else if hourly.adx_14 <= Decimal::from(20) {
        Regime::Range
    } else {
        Regime::Transition
    };
    Ok(RegimeAssessment {
        regime,
        high_volatility: current >= percentile_80 && current <= percentile_95,
        percentile_80,
        percentile_95,
    })
}

/// Calculates all six frozen rule families from one point-in-time input set.
pub fn score(inputs: &RuleInputs, history: &RuleHistory) -> Result<RuleScores, RuleScoreError> {
    ensure_positive("atr_14", inputs.atr_14)?;
    ensure_positive("atrp_14", inputs.atrp_14)?;
    validate_unit_interval("donchian_20_position", inputs.donchian_20_position)?;
    validate_unit_interval("cross_return_4_rank", inputs.cross_return_4_rank)?;
    validate_unit_interval("cross_return_16_rank", inputs.cross_return_16_rank)?;

    let trend_scale = clip(
        inputs
            .adx_14
            .checked_sub(Decimal::from(15))
            .ok_or(RuleScoreError::Arithmetic {
                operation: "trend ADX offset",
            })?
            .checked_div(Decimal::from(20))
            .ok_or(RuleScoreError::Arithmetic {
                operation: "trend ADX scale",
            })?,
    );
    let ema_gap = inputs
        .ema_8
        .checked_sub(inputs.ema_32)
        .ok_or(RuleScoreError::Arithmetic {
            operation: "trend EMA spread",
        })?
        .checked_div(inputs.atr_14)
        .ok_or(RuleScoreError::Arithmetic {
            operation: "trend EMA scale",
        })?;
    let slope =
        inputs
            .ema_8_slope_4
            .checked_div(inputs.atr_14)
            .ok_or(RuleScoreError::Arithmetic {
                operation: "trend EMA slope scale",
            })?;
    let trend = clip(
        average(&[unit(ema_gap)?, unit(slope)?])?
            .checked_mul(trend_scale)
            .ok_or(RuleScoreError::Arithmetic {
                operation: "trend confidence",
            })?,
    );

    let scaled_return_4 = scaled_return(inputs.return_4, inputs.atrp_14, Decimal::from(2))?;
    let scaled_return_16 = scaled_return(inputs.return_16, inputs.atrp_14, Decimal::from(4))?;
    let donchian_centered = inputs
        .donchian_20_position
        .checked_mul(Decimal::from(2))
        .and_then(|value| value.checked_sub(Decimal::ONE))
        .ok_or(RuleScoreError::Arithmetic {
            operation: "Donchian midpoint centering",
        })?;
    let signed_volume = inputs
        .volume_robust_z_20
        .checked_mul(sign(inputs.return_4))
        .ok_or(RuleScoreError::Arithmetic {
            operation: "signed volume robust z",
        })?;
    let momentum = clip(weighted_sum(&[
        (Decimal::new(35, 2), scaled_return_4),
        (Decimal::new(35, 2), scaled_return_16),
        (Decimal::new(20, 2), clip(donchian_centered)),
        (Decimal::new(10, 2), clip(signed_volume)),
    ])?);

    let mean_reversion = clip(
        Decimal::ZERO
            .checked_sub(inputs.close_ema_20_residual_robust_z_20)
            .ok_or(RuleScoreError::Arithmetic {
                operation: "mean-reversion robust z sign",
            })?,
    );

    let microstructure = clip(
        average(&[
            imbalance(inputs.bid_depth_10bps, inputs.ask_depth_10bps)?,
            imbalance(inputs.bid_depth_25bps, inputs.ask_depth_25bps)?,
            imbalance(inputs.bid_depth_50bps, inputs.ask_depth_50bps)?,
            clip(inputs.trade_imbalance_5m),
            clip(inputs.trade_imbalance_15m),
        ])?
        .checked_mul(
            Decimal::ONE
                .checked_sub(clip(
                    inputs.spread_bps.checked_div(Decimal::from(15)).ok_or(
                        RuleScoreError::Arithmetic {
                            operation: "microstructure spread scale",
                        },
                    )?,
                ))
                .ok_or(RuleScoreError::Arithmetic {
                    operation: "microstructure spread confidence",
                })?,
        )
        .ok_or(RuleScoreError::Arithmetic {
            operation: "microstructure confidence",
        })?,
    );

    let derivatives = clip(weighted_sum(&[
        (
            Decimal::new(50, 2),
            robust_z(inputs.premium, &history.premium)?,
        ),
        (
            Decimal::new(30, 2),
            robust_z(
                inputs.open_interest_change_4,
                &history.open_interest_change_4,
            )?
            .checked_mul(sign(inputs.return_4))
            .ok_or(RuleScoreError::Arithmetic {
                operation: "derivatives OI direction",
            })?,
        ),
        (
            Decimal::new(-20, 2),
            robust_z(inputs.funding_level, &history.funding)?,
        ),
    ])?);

    let cross_sectional = clip(average(&[
        centered_rank(inputs.cross_return_4_rank)?,
        centered_rank(inputs.cross_return_16_rank)?,
    ])?);

    Ok(RuleScores {
        trend,
        momentum,
        mean_reversion,
        microstructure,
        derivatives,
        cross_sectional,
    })
}

fn ensure_positive(field: &'static str, value: Decimal) -> Result<(), RuleScoreError> {
    if value <= Decimal::ZERO {
        return Err(RuleScoreError::UnusableVolatilityScale { field, value });
    }
    Ok(())
}

fn validate_unit_interval(field: &'static str, value: Decimal) -> Result<(), RuleScoreError> {
    if !(Decimal::ZERO..=Decimal::ONE).contains(&value) {
        return Err(RuleScoreError::OutOfRange {
            field,
            minimum: Decimal::ZERO,
            maximum: Decimal::ONE,
            value,
        });
    }
    Ok(())
}

fn scaled_return(
    value: Decimal,
    atrp_14: Decimal,
    horizon_sqrt: Decimal,
) -> Result<Decimal, RuleScoreError> {
    ensure_positive("atrp_14", atrp_14)?;
    let denominator = atrp_14
        .checked_mul(horizon_sqrt)
        .ok_or(RuleScoreError::Arithmetic {
            operation: "momentum volatility denominator",
        })?;
    ensure_positive("momentum volatility denominator", denominator)?;
    value
        .checked_div(denominator)
        .map(clip)
        .ok_or(RuleScoreError::Arithmetic {
            operation: "momentum scaled return",
        })
}

fn robust_z(current: Decimal, history: &[Decimal]) -> Result<Decimal, RuleScoreError> {
    let center = median(history)?;
    let deviations = history
        .iter()
        .map(|value| {
            value
                .checked_sub(center)
                .map(|difference| difference.abs())
                .ok_or(RuleScoreError::Arithmetic {
                    operation: "robust-z deviation",
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let denominator = median(&deviations)?
        .checked_mul(Decimal::new(14_826, 4))
        .and_then(|value| value.checked_add(Decimal::new(1, 12)))
        .ok_or(RuleScoreError::Arithmetic {
            operation: "robust-z denominator",
        })?;
    current
        .checked_sub(center)
        .and_then(|value| value.checked_div(denominator))
        .map(|value| clip_range(value, Decimal::from(-3), Decimal::from(3)))
        .and_then(|value| value.checked_div(Decimal::from(3)))
        .ok_or(RuleScoreError::Arithmetic {
            operation: "robust-z score",
        })
}

fn median(values: &[Decimal]) -> Result<Decimal, RuleScoreError> {
    if values.is_empty() {
        return Err(RuleScoreError::EmptyPercentile);
    }
    let mut sorted = values.to_vec();
    sorted.sort();
    let middle = sorted.len() / 2;
    if sorted.len().is_multiple_of(2) {
        sorted[middle - 1]
            .checked_add(sorted[middle])
            .and_then(|value| value.checked_div(Decimal::from(2)))
            .ok_or(RuleScoreError::Arithmetic {
                operation: "median",
            })
    } else {
        Ok(sorted[middle])
    }
}

fn imbalance(bid_notional: Decimal, ask_notional: Decimal) -> Result<Decimal, RuleScoreError> {
    let denominator = bid_notional
        .checked_add(ask_notional)
        .ok_or(RuleScoreError::Arithmetic {
            operation: "depth imbalance denominator",
        })?;
    if denominator.is_zero() {
        return Ok(Decimal::ZERO);
    }
    bid_notional
        .checked_sub(ask_notional)
        .and_then(|value| value.checked_div(denominator))
        .map(clip)
        .ok_or(RuleScoreError::Arithmetic {
            operation: "depth imbalance",
        })
}

fn centered_rank(rank: Decimal) -> Result<Decimal, RuleScoreError> {
    validate_unit_interval("cross-sectional rank", rank)?;
    rank.checked_mul(Decimal::from(2))
        .and_then(|value| value.checked_sub(Decimal::ONE))
        .ok_or(RuleScoreError::Arithmetic {
            operation: "cross-sectional rank centering",
        })
}

fn weighted_sum(values: &[(Decimal, Decimal)]) -> Result<Decimal, RuleScoreError> {
    values
        .iter()
        .try_fold(Decimal::ZERO, |total, (weight, value)| {
            total
                .checked_add(
                    weight
                        .checked_mul(*value)
                        .ok_or(RuleScoreError::Arithmetic {
                            operation: "weighted score product",
                        })?,
                )
                .ok_or(RuleScoreError::Arithmetic {
                    operation: "weighted score sum",
                })
        })
}

fn average(values: &[Decimal]) -> Result<Decimal, RuleScoreError> {
    if values.is_empty() {
        return Err(RuleScoreError::EmptyPercentile);
    }
    values
        .iter()
        .try_fold(Decimal::ZERO, |total, value| {
            total.checked_add(*value).ok_or(RuleScoreError::Arithmetic {
                operation: "score average sum",
            })
        })?
        .checked_div(Decimal::from(values.len()))
        .ok_or(RuleScoreError::Arithmetic {
            operation: "score average",
        })
}

fn sign(value: Decimal) -> Decimal {
    if value > Decimal::ZERO {
        Decimal::ONE
    } else if value < Decimal::ZERO {
        Decimal::NEGATIVE_ONE
    } else {
        Decimal::ZERO
    }
}

fn clip(value: Decimal) -> Decimal {
    clip_range(value, Decimal::NEGATIVE_ONE, Decimal::ONE)
}

fn clip_range(value: Decimal, minimum: Decimal, maximum: Decimal) -> Decimal {
    value.clamp(minimum, maximum)
}

#[cfg(test)]
mod tests {
    use rust_decimal_macros::dec;

    use super::{
        HourlyRuleInputs, Regime, RuleHistory, RuleInputs, RuleScoreError, classify_regime,
        nearest_rank_percentile, score, unit,
    };

    #[test]
    fn unit_uses_the_frozen_portable_tanh_convention() {
        assert_eq!(unit(dec!(0)).expect("zero is finite"), dec!(0));
    }

    #[test]
    fn percentile_rejects_values_outside_the_frozen_open_closed_interval() {
        assert!(matches!(
            nearest_rank_percentile(&[dec!(1), dec!(2)], dec!(0)),
            Err(RuleScoreError::InvalidPercentile { .. })
        ));
        assert!(matches!(
            nearest_rank_percentile(&[dec!(1), dec!(2)], dec!(1.01)),
            Err(RuleScoreError::InvalidPercentile { .. })
        ));
    }

    #[test]
    fn percentile_uses_frozen_nearest_rank_indexing() {
        let values = [dec!(1), dec!(2), dec!(3), dec!(4), dec!(5)];

        assert_eq!(
            nearest_rank_percentile(&values, dec!(0.80)).expect("valid percentile"),
            dec!(4)
        );
        assert_eq!(
            nearest_rank_percentile(&values, dec!(0.95)).expect("valid percentile"),
            dec!(5)
        );
    }

    #[test]
    fn all_six_family_scores_match_the_frozen_golden_formula() {
        let scores = score(&golden_inputs(), &golden_history()).expect("valid inputs");

        assert_eq!(scores.trend(), dec!(0.47948473809050));
        assert_eq!(scores.momentum(), dec!(0.95));
        assert_eq!(scores.mean_reversion(), dec!(-0.25));
        assert_eq!(scores.microstructure(), dec!(0.42));
        assert_eq!(scores.derivatives(), dec!(0.60));
        assert_eq!(scores.cross_sectional(), dec!(0.50));
    }

    #[test]
    fn momentum_and_derivatives_follow_return_direction() {
        let mut inputs = golden_inputs();
        inputs.return_4 = dec!(-0.20);
        inputs.return_16 = dec!(-0.40);
        inputs.volume_robust_z_20 = dec!(0.50);
        inputs.premium = dec!(-0.03);
        let mut history = golden_history();
        history.premium = vec![dec!(0), dec!(0), dec!(-0.03)];

        let scores = score(&inputs, &history).expect("valid inputs");

        assert_eq!(scores.momentum(), dec!(-0.55));
        assert_eq!(scores.derivatives(), dec!(-1));
    }

    #[test]
    fn flatline_atrp_is_a_typed_fail_closed_score_rejection() {
        let mut inputs = golden_inputs();
        inputs.atrp_14 = dec!(0);

        assert!(matches!(
            score(&inputs, &golden_history()),
            Err(RuleScoreError::UnusableVolatilityScale { .. })
        ));
    }

    #[test]
    fn regime_selection_applies_trend_range_transition_and_volatility_gates() {
        let history = golden_history();
        let trend = classify_regime(&golden_inputs().hourly, &history).expect("valid trend regime");
        assert_eq!(trend.regime(), Regime::Trend);
        assert!(!trend.high_volatility());

        let mut range = golden_inputs().hourly;
        range.adx_14 = dec!(20);
        let range = classify_regime(&range, &history).expect("valid range regime");
        assert_eq!(range.regime(), Regime::Range);

        let mut transition = golden_inputs().hourly;
        transition.adx_14 = dec!(22);
        let transition = classify_regime(&transition, &history).expect("valid transition regime");
        assert_eq!(transition.regime(), Regime::Transition);

        let mut extreme_history = history.clone();
        extreme_history.current_hourly_realized_volatility_20 = dec!(0.31);
        let extreme = classify_regime(&golden_inputs().hourly, &extreme_history)
            .expect("valid extreme-volatility regime");
        assert_eq!(extreme.regime(), Regime::ExtremeVolatility);

        let mut high_history = history;
        high_history.hourly_realized_volatility_20 =
            vec![dec!(0.10), dec!(0.10), dec!(0.10), dec!(0.20), dec!(0.30)];
        high_history.current_hourly_realized_volatility_20 = dec!(0.20);
        let high = classify_regime(&golden_inputs().hourly, &high_history)
            .expect("valid high-volatility regime");
        assert_eq!(high.regime(), Regime::Trend);
        assert!(high.high_volatility());
    }

    fn golden_inputs() -> RuleInputs {
        RuleInputs {
            close: dec!(100),
            ema_8: dec!(110),
            ema_32: dec!(100),
            ema_8_slope_4: dec!(2),
            atr_14: dec!(10),
            atrp_14: dec!(0.10),
            adx_14: dec!(35),
            return_4: dec!(0.20),
            return_16: dec!(0.40),
            donchian_20_position: dec!(1),
            volume_robust_z_20: dec!(0.50),
            close_ema_20_residual_robust_z_20: dec!(0.25),
            bid_depth_10bps: dec!(3),
            ask_depth_10bps: dec!(1),
            bid_depth_25bps: dec!(3),
            ask_depth_25bps: dec!(1),
            bid_depth_50bps: dec!(3),
            ask_depth_50bps: dec!(1),
            trade_imbalance_5m: dec!(0.20),
            trade_imbalance_15m: dec!(0.40),
            spread_bps: dec!(0),
            premium: dec!(0.03),
            open_interest_change_4: dec!(0.02),
            funding_level: dec!(0.01),
            cross_return_4_rank: dec!(0.75),
            cross_return_16_rank: dec!(0.75),
            low_10: dec!(95),
            high_10: dec!(105),
            hourly: HourlyRuleInputs {
                ema_8: dec!(110),
                ema_32: dec!(100),
                atr_14: dec!(10),
                adx_14: dec!(35),
            },
        }
    }

    fn golden_history() -> RuleHistory {
        RuleHistory {
            premium: vec![dec!(0), dec!(0), dec!(0.03)],
            open_interest_change_4: vec![dec!(0), dec!(0), dec!(0.02)],
            funding: vec![dec!(0), dec!(0), dec!(0.01)],
            hourly_realized_volatility_20: vec![dec!(0.10), dec!(0.20), dec!(0.30)],
            current_hourly_realized_volatility_20: dec!(0.10),
        }
    }
}
