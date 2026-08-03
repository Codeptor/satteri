//! The interpretable rules-only strategy over immutable feature inputs.

use rust_decimal::Decimal;
use serde::Serialize;

use crate::domain::{Market, Price, Side, Sleeve};
use crate::event::{CandleInterval, DurationNs, TimestampNs};
use crate::features::common::{FeatureSnapshot, LongHorizonFeatureHistory};
use crate::features::rules::{
    Regime, RegimeAssessment, RuleFeatureFrame, RuleHistory, RuleInputs, RuleScoreError,
    RuleScores, classify_regime, score,
};
use crate::strategy::{
    CandidateSpecification, CostDecision, CostQuote, CostRejection, OrderIntent, SignalCandidate,
    Strategy, StrategyKind,
};

const HIGH_VOLATILITY_THRESHOLD_SURCHARGE: Decimal = Decimal::from_parts(10, 0, 0, false, 2);
const MINIMUM_AGREEMENT_MAGNITUDE: Decimal = Decimal::from_parts(15, 0, 0, false, 2);
const OPPOSITE_EXIT_MAGNITUDE: Decimal = Decimal::from_parts(25, 0, 0, false, 2);

/// The only threshold values eligible for frozen rules training.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryThreshold {
    /// Absolute composite threshold 0.55.
    P55,
    /// Absolute composite threshold 0.60, the approved default.
    P60,
    /// Absolute composite threshold 0.65.
    P65,
}

impl EntryThreshold {
    /// Returns the exact selected threshold fraction.
    #[must_use]
    pub fn value(self) -> Decimal {
        match self {
            Self::P55 => Decimal::from_parts(55, 0, 0, false, 2),
            Self::P60 => Decimal::from_parts(60, 0, 0, false, 2),
            Self::P65 => Decimal::from_parts(65, 0, 0, false, 2),
        }
    }
}

/// The only ATR-floor values eligible for frozen rules training.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AtrFloor {
    /// 1.25 ATR.
    OnePointTwoFive,
    /// 1.50 ATR, the approved default.
    OnePointFive,
}

impl AtrFloor {
    /// Returns the exact selected ATR multiple.
    #[must_use]
    pub fn value(self) -> Decimal {
        match self {
            Self::OnePointTwoFive => Decimal::from_parts(125, 0, 0, false, 2),
            Self::OnePointFive => Decimal::from_parts(150, 0, 0, false, 2),
        }
    }
}

/// The only take-profit values eligible for frozen rules training.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TakeProfitMultiple {
    /// 1.5R.
    OnePointFiveR,
    /// 2.0R, the approved default.
    TwoR,
}

impl TakeProfitMultiple {
    /// Returns the exact selected R multiple.
    #[must_use]
    pub fn value(self) -> Decimal {
        match self {
            Self::OnePointFiveR => Decimal::from_parts(15, 0, 0, false, 1),
            Self::TwoR => Decimal::from_parts(2, 0, 0, false, 0),
        }
    }
}

/// Fully frozen rule configuration. No family weight or regime parameter is configurable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuleConfig {
    threshold: EntryThreshold,
    atr_floor: AtrFloor,
    take_profit: TakeProfitMultiple,
}

impl RuleConfig {
    /// Creates one of the twelve approved frozen training-grid configurations.
    #[must_use]
    pub const fn new(
        threshold: EntryThreshold,
        atr_floor: AtrFloor,
        take_profit: TakeProfitMultiple,
    ) -> Self {
        Self {
            threshold,
            atr_floor,
            take_profit,
        }
    }

    /// Returns the selected base threshold.
    #[must_use]
    pub const fn threshold(self) -> EntryThreshold {
        self.threshold
    }

    /// Returns the selected ATR floor.
    #[must_use]
    pub const fn atr_floor(self) -> AtrFloor {
        self.atr_floor
    }

    /// Returns the selected take-profit R multiple.
    #[must_use]
    pub const fn take_profit(self) -> TakeProfitMultiple {
        self.take_profit
    }
}

impl Default for RuleConfig {
    fn default() -> Self {
        // User-approved production baseline; research can only select one of
        // the explicitly represented values above before freezing an artifact.
        Self::new(
            EntryThreshold::P60,
            AtrFloor::OnePointFive,
            TakeProfitMultiple::TwoR,
        )
    }
}

/// Immutable provenance that accompanies an already-derived rules input set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RuleDecisionSource {
    market: Market,
    sleeve: Sleeve,
    as_of_time: TimestampNs,
    snapshot_digest: String,
    universe_digest: String,
    history_digest: String,
}

impl RuleDecisionSource {
    pub(crate) fn new(
        market: Market,
        sleeve: Sleeve,
        as_of_time: TimestampNs,
        snapshot_digest: impl Into<String>,
        universe_digest: impl Into<String>,
        history_digest: impl Into<String>,
    ) -> Self {
        Self {
            market,
            sleeve,
            as_of_time,
            snapshot_digest: snapshot_digest.into(),
            universe_digest: universe_digest.into(),
            history_digest: history_digest.into(),
        }
    }
}

/// Why an evaluated completed bar did not produce an entry candidate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuleRejection {
    /// ATR percent or a required volatility scale was non-positive.
    UnusableVolatilityScale,
    /// The completed-hour regime permits no new entry.
    TransitionRegime,
    /// The realized-volatility extreme gate permits no new entry.
    ExtremeVolatility,
    /// Absolute composite did not meet its fixed threshold.
    Threshold,
    /// Fewer than three positive-weight families agreed at the frozen magnitude.
    Agreement,
    /// A mathematically valid score could not form positive stop/target prices.
    InvalidExitPlan,
    /// A checked score calculation failed without a fallback.
    ScoreCalculation,
    /// Snapshot/history provenance did not form one complete immutable rules frame.
    FeatureInput,
}

impl RuleRejection {
    const fn code(self) -> &'static str {
        match self {
            Self::UnusableVolatilityScale => "unusable_volatility_scale",
            Self::TransitionRegime => "transition_regime",
            Self::ExtremeVolatility => "extreme_volatility",
            Self::Threshold => "threshold",
            Self::Agreement => "agreement",
            Self::InvalidExitPlan => "invalid_exit_plan",
            Self::ScoreCalculation => "score_calculation",
            Self::FeatureInput => "feature_input",
        }
    }
}

/// One auditable bar evaluation with zero or one un-sized candidate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuleDecision {
    candidate: Option<SignalCandidate>,
    rejections: Vec<RuleRejection>,
    explanation_json: String,
}

impl RuleDecision {
    /// Returns the un-sized candidate when this completed bar passed every entry gate.
    #[must_use]
    pub const fn candidate(&self) -> Option<&SignalCandidate> {
        self.candidate.as_ref()
    }

    /// Checks for a stable machine-readable rejection code in this decision.
    #[must_use]
    pub fn has_rejection(&self, code: &str) -> bool {
        self.rejections
            .iter()
            .any(|rejection| rejection.code() == code)
    }

    /// Returns the full byte-stable JSON audit explanation.
    #[must_use]
    pub fn explanation_json(&self) -> &str {
        &self.explanation_json
    }
}

/// Stateless rules strategy using only frozen feature and long-horizon inputs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RulesStrategy {
    config: RuleConfig,
}

impl RulesStrategy {
    /// Creates a rules strategy from one frozen allowed configuration.
    #[must_use]
    pub const fn new(config: RuleConfig) -> Self {
        Self { config }
    }

    /// Returns the immutable selected configuration.
    #[must_use]
    pub const fn config(self) -> RuleConfig {
        self.config
    }

    /// Evaluates one immutable snapshot and exact long-horizon history at its completed-bar boundary.
    ///
    /// No direct universe membership is accepted here: the only acceptable universe
    /// evidence is the digest already sealed into the common snapshot provenance.
    #[must_use]
    pub fn on_bar(
        &self,
        snapshot: &FeatureSnapshot,
        long_history: &LongHorizonFeatureHistory,
    ) -> RuleDecision {
        match RuleFeatureFrame::from_snapshot(snapshot, long_history) {
            Ok(frame) => self.evaluate_parts(
                RuleDecisionSource::new(
                    frame.market().clone(),
                    frame.sleeve(),
                    frame.as_of_time(),
                    frame.snapshot_digest(),
                    frame.universe_digest(),
                    frame.history_digest(),
                ),
                frame.inputs(),
                frame.history(),
            ),
            Err(_) => self.no_signal(
                RuleDecisionSource::new(
                    snapshot.market().clone(),
                    match snapshot.sleeve() {
                        CandleInterval::FifteenMinutes => Sleeve::FifteenMinute,
                        CandleInterval::OneHour => Sleeve::OneHour,
                    },
                    snapshot.as_of_time(),
                    snapshot.snapshot_hash(),
                    snapshot
                        .input_range()
                        .and_then(|range| range.universe_digest())
                        .unwrap_or("unavailable"),
                    long_history.input_digest(),
                ),
                None,
                None,
                Vec::from([RuleRejection::FeatureInput]),
            ),
        }
    }

    fn evaluate_parts(
        &self,
        source: RuleDecisionSource,
        inputs: &RuleInputs,
        history: &RuleHistory,
    ) -> RuleDecision {
        let scores = match score(inputs, history) {
            Ok(scores) => scores,
            Err(RuleScoreError::UnusableVolatilityScale { .. }) => {
                return self.no_signal(
                    source,
                    None,
                    None,
                    Vec::from([RuleRejection::UnusableVolatilityScale]),
                );
            }
            Err(_) => {
                return self.no_signal(
                    source,
                    None,
                    None,
                    Vec::from([RuleRejection::ScoreCalculation]),
                );
            }
        };
        let regime = match classify_regime(&inputs.hourly, history) {
            Ok(regime) => regime,
            Err(RuleScoreError::UnusableVolatilityScale { .. }) => {
                return self.no_signal(
                    source,
                    Some(scores),
                    None,
                    Vec::from([RuleRejection::UnusableVolatilityScale]),
                );
            }
            Err(_) => {
                return self.no_signal(
                    source,
                    Some(scores),
                    None,
                    Vec::from([RuleRejection::ScoreCalculation]),
                );
            }
        };
        match regime.regime() {
            Regime::Transition => {
                return self.no_signal(
                    source,
                    Some(scores),
                    Some(regime),
                    Vec::from([RuleRejection::TransitionRegime]),
                );
            }
            Regime::ExtremeVolatility => {
                return self.no_signal(
                    source,
                    Some(scores),
                    Some(regime),
                    Vec::from([RuleRejection::ExtremeVolatility]),
                );
            }
            Regime::Trend | Regime::Range => {}
        }

        let weights = fixed_weights(regime.regime());
        let composite = weighted_composite(scores, weights);
        let threshold = self.config.threshold.value()
            + if regime.high_volatility() {
                HIGH_VOLATILITY_THRESHOLD_SURCHARGE
            } else {
                Decimal::ZERO
            };
        let mut rejections = Vec::new();
        if composite.abs() < threshold {
            rejections.push(RuleRejection::Threshold);
        }
        let direction = direction(composite);
        if agreeing_families(scores, weights, direction) < 3 {
            rejections.push(RuleRejection::Agreement);
        }
        let gross_edge = composite
            .abs()
            .checked_mul(inputs.atrp_14)
            .and_then(|value| value.checked_mul(Decimal::from(2)));
        let Some(gross_edge) = gross_edge else {
            rejections.push(RuleRejection::ScoreCalculation);
            return self.no_signal(source, Some(scores), Some(regime), rejections);
        };
        if !rejections.is_empty() {
            return self.no_signal(source, Some(scores), Some(regime), rejections);
        }
        let Some(side) = direction else {
            return self.no_signal(
                source,
                Some(scores),
                Some(regime),
                Vec::from([RuleRejection::Threshold]),
            );
        };
        let exit_plan =
            build_exit_plan(inputs, side, self.config, source.sleeve, source.as_of_time);
        let Ok((reference_entry, stop, target, time_exit)) = exit_plan else {
            return self.no_signal(
                source,
                Some(scores),
                Some(regime),
                Vec::from([RuleRejection::InvalidExitPlan]),
            );
        };
        let explanation_json = explanation_json(
            &source,
            Some(scores),
            Some(regime),
            weights,
            threshold,
            Some(composite),
            Some(gross_edge),
            &[],
        );
        let fallback_source = source.clone();
        let candidate = SignalCandidate::new(CandidateSpecification {
            strategy: StrategyKind::RulesOnly,
            market: source.market,
            side,
            sleeve: source.sleeve,
            decision_time: source.as_of_time,
            gross_edge,
            reference_entry,
            stop,
            target,
            time_exit,
            snapshot_digest: source.snapshot_digest,
            universe_digest: source.universe_digest,
            history_digest: source.history_digest,
            explanation_json: explanation_json.clone(),
        });
        match candidate {
            Ok(candidate) => RuleDecision {
                candidate: Some(candidate),
                rejections: Vec::new(),
                explanation_json,
            },
            Err(_) => self.no_signal(
                fallback_source,
                Some(scores),
                Some(regime),
                Vec::from([RuleRejection::InvalidExitPlan]),
            ),
        }
    }

    fn no_signal(
        &self,
        source: RuleDecisionSource,
        scores: Option<RuleScores>,
        regime: Option<RegimeAssessment>,
        rejections: Vec<RuleRejection>,
    ) -> RuleDecision {
        let weights = regime.map_or(FIXED_TREND_WEIGHTS, |regime| fixed_weights(regime.regime()));
        let threshold = self.config.threshold.value()
            + if regime.is_some_and(RegimeAssessment::high_volatility) {
                HIGH_VOLATILITY_THRESHOLD_SURCHARGE
            } else {
                Decimal::ZERO
            };
        let composite = scores.map(|scores| weighted_composite(scores, weights));
        let explanation_json = explanation_json(
            &source,
            scores,
            regime,
            weights,
            threshold,
            composite,
            None,
            &rejections,
        );
        RuleDecision {
            candidate: None,
            rejections,
            explanation_json,
        }
    }

    /// Evaluates exit priority from an explicit executable price and completed-bar composite.
    #[must_use]
    pub fn exit_for_composite(
        &self,
        position: &RulePosition,
        executable_price: Decimal,
        composite: Decimal,
        at: TimestampNs,
    ) -> Option<ExitReason> {
        let candidate = &position.candidate;
        let hit_stop = match candidate.side() {
            Side::Buy => executable_price <= candidate.stop().value(),
            Side::Sell => executable_price >= candidate.stop().value(),
        };
        if hit_stop {
            return Some(ExitReason::Stop);
        }
        let hit_target = match candidate.side() {
            Side::Buy => executable_price >= candidate.target().value(),
            Side::Sell => executable_price <= candidate.target().value(),
        };
        if hit_target {
            return Some(ExitReason::TakeProfit);
        }
        let opposite = match candidate.side() {
            Side::Buy => composite <= -OPPOSITE_EXIT_MAGNITUDE,
            Side::Sell => composite >= OPPOSITE_EXIT_MAGNITUDE,
        };
        if opposite {
            return Some(ExitReason::OppositeSignal);
        }
        (at >= candidate.time_exit()).then_some(ExitReason::TimeLimit)
    }
}

impl Strategy for RulesStrategy {
    fn accept_cost(&self, candidate: &SignalCandidate, quote: &CostQuote) -> CostDecision {
        if candidate.strategy() != StrategyKind::RulesOnly
            || candidate.market() != quote.market()
            || candidate.digest() != quote.candidate_digest()
        {
            return CostDecision::Rejected(CostRejection::Mismatch);
        }
        if !quote.is_fresh_at(candidate.decision_time()) {
            return CostDecision::Rejected(CostRejection::Stale);
        }
        if !quote.is_feasible() {
            return CostDecision::Rejected(CostRejection::Infeasible);
        }
        let Some(required_cost_cover) =
            quote.total_cost_fraction().checked_mul(Decimal::new(15, 1))
        else {
            return CostDecision::Rejected(CostRejection::InsufficientGrossEdge);
        };
        if candidate.gross_edge() < required_cost_cover {
            return CostDecision::Rejected(CostRejection::InsufficientGrossEdge);
        }
        CostDecision::Accepted(Box::new(OrderIntent::new(candidate.clone(), quote)))
    }
}

/// Stateless open-position rule state needed only for deterministic exit evaluation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RulePosition {
    candidate: SignalCandidate,
}

impl RulePosition {
    /// Captures a rules-only candidate's market-owned exit plan without risk sizing.
    ///
    /// A rules position cannot be created from an ML candidate, preserving the
    /// independent-strategy boundary even for shared downstream exit plumbing.
    #[must_use]
    pub fn from_candidate(candidate: &SignalCandidate) -> Option<Self> {
        (candidate.strategy() == StrategyKind::RulesOnly).then(|| Self {
            candidate: candidate.clone(),
        })
    }
}

/// Ordered non-risk exit causes owned by the rules sleeve.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExitReason {
    /// Stop is hit at the executable price.
    Stop,
    /// Take-profit is hit after stop priority.
    TakeProfit,
    /// Opposite composite crossed the frozen 0.25 magnitude.
    OppositeSignal,
    /// Four completed sleeve bars elapsed after entry.
    TimeLimit,
}

const FIXED_TREND_WEIGHTS: [Decimal; 6] = [
    Decimal::from_parts(30, 0, 0, false, 2),
    Decimal::from_parts(25, 0, 0, false, 2),
    Decimal::ZERO,
    Decimal::from_parts(20, 0, 0, false, 2),
    Decimal::from_parts(10, 0, 0, false, 2),
    Decimal::from_parts(15, 0, 0, false, 2),
];
const FIXED_RANGE_WEIGHTS: [Decimal; 6] = [
    Decimal::ZERO,
    Decimal::from_parts(10, 0, 0, false, 2),
    Decimal::from_parts(35, 0, 0, false, 2),
    Decimal::from_parts(25, 0, 0, false, 2),
    Decimal::from_parts(20, 0, 0, false, 2),
    Decimal::from_parts(10, 0, 0, false, 2),
];

fn fixed_weights(regime: Regime) -> [Decimal; 6] {
    match regime {
        Regime::Trend => FIXED_TREND_WEIGHTS,
        Regime::Range => FIXED_RANGE_WEIGHTS,
        Regime::Transition | Regime::ExtremeVolatility => [Decimal::ZERO; 6],
    }
}

fn family_values(scores: RuleScores) -> [Decimal; 6] {
    [
        scores.trend(),
        scores.momentum(),
        scores.mean_reversion(),
        scores.microstructure(),
        scores.derivatives(),
        scores.cross_sectional(),
    ]
}

fn weighted_composite(scores: RuleScores, weights: [Decimal; 6]) -> Decimal {
    family_values(scores)
        .into_iter()
        .zip(weights)
        .fold(Decimal::ZERO, |total, (score, weight)| {
            total + score * weight
        })
}

fn direction(composite: Decimal) -> Option<Side> {
    if composite > Decimal::ZERO {
        Some(Side::Buy)
    } else if composite < Decimal::ZERO {
        Some(Side::Sell)
    } else {
        None
    }
}

fn agreeing_families(scores: RuleScores, weights: [Decimal; 6], direction: Option<Side>) -> usize {
    let Some(direction) = direction else {
        return 0;
    };
    family_values(scores)
        .into_iter()
        .zip(weights)
        .filter(|(score, weight)| {
            let agrees = match direction {
                Side::Buy => *score > Decimal::ZERO,
                Side::Sell => *score < Decimal::ZERO,
            };
            *weight > Decimal::ZERO && score.abs() >= MINIMUM_AGREEMENT_MAGNITUDE && agrees
        })
        .count()
}

fn build_exit_plan(
    inputs: &RuleInputs,
    side: Side,
    config: RuleConfig,
    sleeve: Sleeve,
    as_of_time: TimestampNs,
) -> Result<(Price, Price, Price, TimestampNs), ()> {
    let reference_entry = Price::new(inputs.close).map_err(|_| ())?;
    let atr = inputs.atr_14;
    let atr_floor = config.atr_floor.value().checked_mul(atr).ok_or(())?;
    let maximum = Decimal::new(25, 1).checked_mul(atr).ok_or(())?;
    let fallback = Decimal::new(15, 1).checked_mul(atr).ok_or(())?;
    let adverse_swing = match side {
        Side::Buy if inputs.low_10 < inputs.close => inputs.close.checked_sub(inputs.low_10),
        Side::Sell if inputs.high_10 > inputs.close => inputs.high_10.checked_sub(inputs.close),
        Side::Buy | Side::Sell => None,
    };
    let risk_distance = adverse_swing.map_or(fallback, |swing| swing.max(atr_floor).min(maximum));
    if risk_distance <= Decimal::ZERO {
        return Err(());
    }
    let stop_value = match side {
        Side::Buy => inputs.close.checked_sub(risk_distance),
        Side::Sell => inputs.close.checked_add(risk_distance),
    }
    .ok_or(())?;
    let reward_distance = risk_distance
        .checked_mul(config.take_profit.value())
        .ok_or(())?;
    let target_value = match side {
        Side::Buy => inputs.close.checked_add(reward_distance),
        Side::Sell => inputs.close.checked_sub(reward_distance),
    }
    .ok_or(())?;
    let stop = Price::new(stop_value).map_err(|_| ())?;
    let target = Price::new(target_value).map_err(|_| ())?;
    let duration = sleeve_duration(sleeve).checked_mul(4).ok_or(())?;
    let time_exit = as_of_time
        .checked_add(DurationNs::new(i128::from(duration)).map_err(|_| ())?)
        .map_err(|_| ())?;
    Ok((reference_entry, stop, target, time_exit))
}

const fn sleeve_duration(sleeve: Sleeve) -> i64 {
    match sleeve {
        Sleeve::FifteenMinute => 900_000_000_000,
        Sleeve::OneHour => 3_600_000_000_000,
    }
}

#[derive(Serialize)]
struct Explanation<'a> {
    version: &'static str,
    market: &'a str,
    sleeve: &'static str,
    as_of_time_ns: i64,
    snapshot_digest: &'a str,
    universe_digest: &'a str,
    history_digest: &'a str,
    scores: Option<ScoreExplanation>,
    regime: Option<RegimeExplanation>,
    weights: [Decimal; 6],
    threshold: Decimal,
    composite: Option<Decimal>,
    gross_edge: Option<Decimal>,
    cost_estimate: Option<Decimal>,
    rejections: Vec<&'static str>,
}

#[derive(Serialize)]
struct ScoreExplanation {
    trend: Decimal,
    momentum: Decimal,
    mean_reversion: Decimal,
    microstructure: Decimal,
    derivatives: Decimal,
    cross_sectional: Decimal,
}

#[derive(Serialize)]
struct RegimeExplanation {
    kind: &'static str,
    high_volatility: bool,
    percentile_80: Decimal,
    percentile_95: Decimal,
}

#[expect(
    clippy::too_many_arguments,
    reason = "auditable explanation has a fixed schema"
)]
fn explanation_json(
    source: &RuleDecisionSource,
    scores: Option<RuleScores>,
    regime: Option<RegimeAssessment>,
    weights: [Decimal; 6],
    threshold: Decimal,
    composite: Option<Decimal>,
    gross_edge: Option<Decimal>,
    rejections: &[RuleRejection],
) -> String {
    let explanation = Explanation {
        version: "trench.rules.explanation.v1",
        market: source.market.as_str(),
        sleeve: sleeve_name(source.sleeve),
        as_of_time_ns: source.as_of_time.value(),
        snapshot_digest: &source.snapshot_digest,
        universe_digest: &source.universe_digest,
        history_digest: &source.history_digest,
        scores: scores.map(|scores| ScoreExplanation {
            trend: scores.trend(),
            momentum: scores.momentum(),
            mean_reversion: scores.mean_reversion(),
            microstructure: scores.microstructure(),
            derivatives: scores.derivatives(),
            cross_sectional: scores.cross_sectional(),
        }),
        regime: regime.map(|regime| RegimeExplanation {
            kind: regime_name(regime.regime()),
            high_volatility: regime.high_volatility(),
            percentile_80: regime.percentile_80(),
            percentile_95: regime.percentile_95(),
        }),
        weights,
        threshold,
        composite,
        gross_edge,
        cost_estimate: None,
        rejections: rejections
            .iter()
            .map(|rejection| rejection.code())
            .collect(),
    };
    serde_json::to_string(&explanation).expect("fixed audit explanation must serialize")
}

const fn sleeve_name(sleeve: Sleeve) -> &'static str {
    match sleeve {
        Sleeve::FifteenMinute => "15m",
        Sleeve::OneHour => "1h",
    }
}

const fn regime_name(regime: Regime) -> &'static str {
    match regime {
        Regime::Trend => "trend",
        Regime::Range => "range",
        Regime::Transition => "transition",
        Regime::ExtremeVolatility => "extreme_volatility",
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal_macros::dec;

    use crate::domain::{Market, Price, Side, Sleeve};
    use crate::event::TimestampNs;
    use crate::features::rules::{HourlyRuleInputs, RuleHistory, RuleInputs};
    use crate::strategy::{
        CandidateSpecification, CostAttribution, CostDecision, CostFeasibilityReason, CostQuote,
        CostQuoteFreshness, CostRejection, CostSourceDigests, QuoteId, SignalCandidate, Strategy,
        StrategyKind,
    };

    use super::{
        AtrFloor, EntryThreshold, ExitReason, RuleConfig, RuleDecisionSource, RulePosition,
        RulesStrategy, TakeProfitMultiple,
    };

    #[test]
    fn frozen_default_emits_an_un_sized_long_candidate_with_bounded_stop_target_and_time_exit() {
        let strategy = RulesStrategy::new(RuleConfig::default());
        let decision = strategy.evaluate_parts(source(), &golden_inputs(), &golden_history());
        let candidate = decision
            .candidate()
            .expect("golden score should cross the default threshold");

        assert_eq!(candidate.sleeve(), Sleeve::FifteenMinute);
        assert_eq!(candidate.gross_edge(), dec!(0.120069084285430000));
        assert_eq!(candidate.reference_entry().value(), dec!(100));
        assert_eq!(candidate.stop().value(), dec!(85));
        assert_eq!(candidate.target().value(), dec!(130));
        assert_eq!(candidate.time_exit().value(), 4_500_000_000_000);
    }

    #[test]
    fn no_adverse_swing_uses_fixed_one_point_five_atr_even_with_one_point_two_five_floor() {
        let strategy = RulesStrategy::new(RuleConfig::new(
            EntryThreshold::P60,
            AtrFloor::OnePointTwoFive,
            TakeProfitMultiple::TwoR,
        ));
        let mut inputs = golden_inputs();
        inputs.low_10 = inputs.close;

        let decision = strategy.evaluate_parts(source(), &inputs, &golden_history());
        let candidate = decision
            .candidate()
            .expect("golden score should cross the selected threshold");

        assert_eq!(candidate.stop().value(), dec!(85));
        assert_eq!(candidate.target().value(), dec!(130));
    }

    #[test]
    fn frozen_configuration_allows_only_approved_training_grid_values() {
        assert_eq!(RuleConfig::default().threshold(), EntryThreshold::P60);
        assert_eq!(RuleConfig::default().atr_floor(), AtrFloor::OnePointFive);
        assert_eq!(
            RuleConfig::default().take_profit(),
            TakeProfitMultiple::TwoR
        );
    }

    #[test]
    fn trend_and_range_weights_are_the_fixed_unoptimizable_table() {
        assert_eq!(
            super::fixed_weights(crate::features::rules::Regime::Trend),
            [
                dec!(0.30),
                dec!(0.25),
                dec!(0),
                dec!(0.20),
                dec!(0.10),
                dec!(0.15),
            ]
        );
        assert_eq!(
            super::fixed_weights(crate::features::rules::Regime::Range),
            [
                dec!(0),
                dec!(0.10),
                dec!(0.35),
                dec!(0.25),
                dec!(0.20),
                dec!(0.10),
            ]
        );
    }

    #[test]
    fn candidate_requires_three_positive_weight_families_to_agree() {
        let strategy = RulesStrategy::new(RuleConfig::new(
            EntryThreshold::P55,
            AtrFloor::OnePointFive,
            TakeProfitMultiple::TwoR,
        ));
        let mut inputs = golden_inputs();
        inputs.ema_8 = dec!(200);
        inputs.ema_8_slope_4 = dec!(100);
        inputs.return_4 = dec!(1);
        inputs.return_16 = dec!(4);
        inputs.volume_robust_z_20 = dec!(1);
        inputs.bid_depth_10bps = dec!(1);
        inputs.ask_depth_10bps = dec!(1);
        inputs.bid_depth_25bps = dec!(1);
        inputs.ask_depth_25bps = dec!(1);
        inputs.bid_depth_50bps = dec!(1);
        inputs.ask_depth_50bps = dec!(1);
        inputs.trade_imbalance_5m = dec!(0);
        inputs.trade_imbalance_15m = dec!(0);
        inputs.premium = dec!(0);
        inputs.open_interest_change_4 = dec!(0);
        inputs.funding_level = dec!(0);
        inputs.cross_return_4_rank = dec!(0.5);
        inputs.cross_return_16_rank = dec!(0.5);
        let mut history = golden_history();
        history.premium = vec![dec!(0), dec!(0), dec!(0)];
        history.open_interest_change_4 = vec![dec!(0), dec!(0), dec!(0)];
        history.funding = vec![dec!(0), dec!(0), dec!(0)];

        let decision = strategy.evaluate_parts(source(), &inputs, &history);

        assert!(decision.candidate().is_none());
        assert!(decision.has_rejection("agreement"));
    }

    #[test]
    fn flatline_atrp_is_an_auditable_no_signal_not_a_division_fallback() {
        let strategy = RulesStrategy::new(RuleConfig::default());
        let mut inputs = golden_inputs();
        inputs.atrp_14 = dec!(0);

        let decision = strategy.evaluate_parts(source(), &inputs, &golden_history());

        assert!(decision.candidate().is_none());
        assert!(decision.has_rejection("unusable_volatility_scale"));
        assert!(
            decision
                .explanation_json()
                .contains("unusable_volatility_scale")
        );
    }

    #[test]
    fn high_volatility_adds_the_frozen_ten_point_threshold_surcharge() {
        let strategy = RulesStrategy::new(RuleConfig::default());
        let mut history = golden_history();
        history.hourly_realized_volatility_20 =
            vec![dec!(0.10), dec!(0.10), dec!(0.10), dec!(0.20), dec!(0.30)];
        history.current_hourly_realized_volatility_20 = dec!(0.20);

        let decision = strategy.evaluate_parts(source(), &golden_inputs(), &history);

        assert!(decision.candidate().is_none());
        assert!(decision.has_rejection("threshold"));
        assert!(decision.explanation_json().contains("0.70"));
    }

    #[test]
    fn stop_target_opposite_and_four_bar_timeout_obey_frozen_priority() {
        let strategy = RulesStrategy::new(RuleConfig::default());
        let decision = strategy.evaluate_parts(source(), &golden_inputs(), &golden_history());
        let candidate = decision.candidate().expect("candidate");
        let position = RulePosition::from_candidate(candidate).expect("rules position");

        assert_eq!(
            strategy.exit_for_composite(&position, dec!(80), dec!(-1), timestamp(1)),
            Some(ExitReason::Stop)
        );
        assert_eq!(
            strategy.exit_for_composite(&position, dec!(130), dec!(-1), timestamp(1)),
            Some(ExitReason::TakeProfit)
        );
        assert_eq!(
            strategy.exit_for_composite(&position, dec!(100), dec!(-0.25), timestamp(1)),
            Some(ExitReason::OppositeSignal)
        );
        assert_eq!(
            strategy.exit_for_composite(
                &position,
                dec!(100),
                dec!(0),
                timestamp(4_500_000_000_000)
            ),
            Some(ExitReason::TimeLimit)
        );
    }

    #[test]
    fn rules_exit_state_refuses_ml_candidates() {
        assert!(RulePosition::from_candidate(&candidate_for(StrategyKind::MlChampion)).is_none());
    }

    #[test]
    fn public_cost_gate_accepts_exact_equality_and_rejects_stale_mismatched_or_infeasible_quotes() {
        let strategy = RulesStrategy::new(RuleConfig::default());
        let candidate = exact_candidate();
        let accepted = quote_for(&candidate, timestamp(900_000_000_000), Vec::new());

        assert!(matches!(
            strategy.accept_cost(&candidate, &accepted),
            CostDecision::Accepted(_)
        ));

        let stale = quote_for(&candidate, timestamp(899_999_999_999), Vec::new());
        assert!(matches!(
            strategy.accept_cost(&candidate, &stale),
            CostDecision::Rejected(CostRejection::Stale)
        ));

        let mismatch = CostQuote::new(
            QuoteId::new("quote-mismatch").expect("quote ID"),
            Market::new("BTC").expect("market"),
            "different-candidate",
            CostQuoteFreshness::new(timestamp(900_000_000_000), timestamp(900_000_000_000))
                .expect("freshness"),
            CostSourceDigests::new("book", "risk"),
            dec!(0.08),
            vec![CostAttribution::entry_fee(dec!(0.08))],
            Vec::new(),
        )
        .expect("quote");
        assert!(matches!(
            strategy.accept_cost(&candidate, &mismatch),
            CostDecision::Rejected(CostRejection::Mismatch)
        ));

        let infeasible = quote_for(
            &candidate,
            timestamp(900_000_000_000),
            vec![CostFeasibilityReason::RiskBlocked],
        );
        assert!(matches!(
            strategy.accept_cost(&candidate, &infeasible),
            CostDecision::Rejected(CostRejection::Infeasible)
        ));
    }

    #[test]
    fn explanation_is_byte_stable_and_contains_scores_weights_threshold_regime_cost_and_rejections()
    {
        let strategy = RulesStrategy::new(RuleConfig::default());
        let first = strategy.evaluate_parts(source(), &golden_inputs(), &golden_history());
        let second = strategy.evaluate_parts(source(), &golden_inputs(), &golden_history());

        assert_eq!(first.explanation_json(), second.explanation_json());
        for field in [
            "trend",
            "momentum",
            "mean_reversion",
            "microstructure",
            "derivatives",
            "cross_sectional",
            "weights",
            "threshold",
            "regime",
            "cost_estimate",
            "rejections",
        ] {
            assert!(first.explanation_json().contains(field), "missing {field}");
        }
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(first.explanation_json())
                .expect("canonical explanation JSON")["cost_estimate"],
            serde_json::Value::Null
        );
    }

    fn exact_candidate() -> SignalCandidate {
        candidate_for(StrategyKind::RulesOnly)
    }

    fn candidate_for(strategy: StrategyKind) -> SignalCandidate {
        SignalCandidate::new(CandidateSpecification {
            strategy,
            market: Market::new("BTC").expect("market"),
            side: Side::Buy,
            sleeve: Sleeve::FifteenMinute,
            decision_time: timestamp(900_000_000_000),
            gross_edge: dec!(0.12),
            reference_entry: Price::new(dec!(100)).expect("entry"),
            stop: Price::new(dec!(90)).expect("stop"),
            target: Price::new(dec!(120)).expect("target"),
            time_exit: timestamp(4_500_000_000_000),
            snapshot_digest: "snapshot".to_owned(),
            universe_digest: "universe".to_owned(),
            history_digest: "history".to_owned(),
            explanation_json: "{}".to_owned(),
        })
        .expect("candidate")
    }

    fn quote_for(
        candidate: &SignalCandidate,
        valid_through: TimestampNs,
        infeasibility_reasons: Vec<CostFeasibilityReason>,
    ) -> CostQuote {
        CostQuote::new(
            QuoteId::new("quote-equality").expect("quote ID"),
            candidate.market().clone(),
            candidate.digest(),
            CostQuoteFreshness::new(timestamp(899_000_000_000), valid_through).expect("freshness"),
            CostSourceDigests::new("book", "risk"),
            dec!(0.08),
            vec![CostAttribution::entry_fee(dec!(0.08))],
            infeasibility_reasons,
        )
        .expect("quote")
    }

    fn source() -> RuleDecisionSource {
        RuleDecisionSource::new(
            Market::new("BTC").expect("market"),
            Sleeve::FifteenMinute,
            timestamp(900_000_000_000),
            "snapshot-digest",
            "universe-digest",
            "history-digest",
        )
    }

    fn timestamp(value: i128) -> TimestampNs {
        TimestampNs::new(value).expect("timestamp")
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
