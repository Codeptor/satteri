//! Deterministic nested walk-forward selection and frozen rules artifacts.
//!
//! This module deliberately owns no market-data adapter, filesystem, random
//! source, or alternative cost model. A caller must provide outcomes produced
//! by the same [`crate::engine::Engine`] transition path used at runtime.

use std::cmp::Ordering;

use blake3::Hasher;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::event::{DurationNs, TimestampNs};
use crate::strategy::rules::{AtrFloor, EntryThreshold, RuleConfig, TakeProfitMultiple};

const DAY_NS: i64 = 86_400_000_000_000;
const FOUR_HOURS_NS: i64 = 14_400_000_000_000;
const DEVELOPMENT_DAYS: u16 = 5;
const CALIBRATION_DAYS: u16 = 1;
const TEST_DAYS: u16 = 1;
const OUTER_ROLL_DAYS: u16 = 1;
const REQUIRED_OUTER_TESTS: usize = 1;
const REQUIRED_CLOSED_TRADES: u32 = 1;
const ARTIFACT_VERSION: &str = "trench.rules-artifact.v1";
const REPORT_VERSION: &str = "trench.rules-validation.v1";

/// A point-in-time source family that must be present before rules research may
/// produce an engine outcome.
///
/// The ordering is protocol ordering. It makes a missing-input result stable
/// across differently ordered partition and sidecar inputs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum MissingReplayInput {
    /// A completed-bar common-feature snapshot was not supplied.
    FeatureSnapshot,
    /// The exact long-horizon feature history was not supplied.
    LongHorizonHistory,
    /// The active point-in-time universe was not supplied.
    UniverseActivation,
    /// A recovered full executable-book set was not supplied.
    ExecutableBooks,
    /// The point-in-time frozen sizing policy set was not supplied.
    RiskPolicies,
    /// A recovery completion boundary was not supplied before execution.
    RecoveryBoundary,
}

/// Deterministic validation, artifact, or report construction failure.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ValidationError {
    /// A supplied timestamp did not start on an exact UTC day boundary.
    #[error("research window must start on an exact UTC day boundary")]
    UnalignedDayBoundary,
    /// A checked time-range construction overflowed.
    #[error("research time-range arithmetic failed")]
    TimeArithmetic,
    /// The input source cannot support the three mandatory outer tests.
    #[error(
        "insufficient trustworthy history: required {required_days} complete days, found {available_days}"
    )]
    InsufficientTrustworthyHistory {
        /// Minimum complete day count for three rolling outer tests.
        required_days: u16,
        /// Complete trustworthy UTC days admitted from the source manifest.
        available_days: u16,
    },
    /// A reported source gap intersects a required research fold.
    #[error("required point-in-time research data is unavailable")]
    RequiredDataUnavailable,
    /// The verified normalized stream lacked one or more required typed
    /// point-in-time sidecar inputs. Research must stop rather than invent an
    /// outcome, feature, universe, recovery state, or sizing policy.
    #[error("required point-in-time replay inputs are unavailable: {inputs:?}")]
    MissingReplayInputs {
        /// Strictly sorted, deduplicated missing source families.
        inputs: Vec<MissingReplayInput>,
    },
    /// A supplied typed point-in-time sidecar contradicted the verified replay
    /// event or its own immutable temporal contract.
    #[error("point-in-time replay inputs are misaligned: {inputs:?}")]
    MisalignedReplayInputs {
        /// Strictly sorted, deduplicated source families that did not align.
        inputs: Vec<MissingReplayInput>,
    },
    /// Excluded gaps were duplicated, overlapped, or otherwise non-canonical.
    #[error("excluded gap ranges must be strictly ordered and non-overlapping")]
    InvalidExcludedGaps,
    /// A caller did not supply every declared candidate exactly once.
    #[error("the research grid must contain exactly the twelve declared rules configurations")]
    IncompleteGrid,
    /// One declared candidate supplied malformed inner-fold results.
    #[error("candidate inner-fold outcomes do not match the immutable fold plan")]
    InvalidInnerFoldOutcomes,
    /// A candidate had no closed inner-validation trades from which expectancy could be selected.
    #[error("candidate has no closed inner-validation trades")]
    NoInnerTrades,
    /// A selection tie could not be resolved through lower turnover.
    #[error("inner-fold selection remains tied after lower-turnover tie-break")]
    AmbiguousSelection,
    /// An engine replay outcome was malformed or did not contain actual stream evidence.
    #[error("engine replay outcome is invalid")]
    InvalidEngineOutcome,
    /// The single production engine rejected a typed replay transition.
    #[error("engine replay transition failed: {reason}")]
    EngineReplayFailed {
        /// Canonical engine error text retained as audit evidence.
        reason: String,
    },
    /// An immutable artifact or report digest was malformed or did not match its content.
    #[error("content-addressed artifact or report digest is invalid")]
    InvalidDigest,
    /// An artifact selected a value outside the declared twelve-configuration grid.
    #[error("artifact selected undeclared rules parameters")]
    UndeclaredRuleParameter,
    /// Artifact or report JSON did not match the canonical supported wire format.
    #[error("artifact or report JSON is invalid")]
    InvalidJson,
    /// The report cannot make a strategy eligible for active paper mode.
    #[error("rules research report is not eligible for active mode")]
    IneligibleReport,
    /// The report and artifact do not carry identical frozen selection evidence.
    #[error("artifact and report evidence differ")]
    ArtifactReportMismatch,
}

/// Half-open UTC time interval used at every research boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimeRange {
    start: TimestampNs,
    end: TimestampNs,
}

impl TimeRange {
    /// Creates a checked nonempty half-open interval.
    pub fn new(start: TimestampNs, end: TimestampNs) -> Result<Self, ValidationError> {
        if start >= end {
            return Err(ValidationError::TimeArithmetic);
        }
        Ok(Self { start, end })
    }

    /// Returns the inclusive lower bound.
    #[must_use]
    pub const fn start(self) -> TimestampNs {
        self.start
    }

    /// Returns the exclusive upper bound.
    #[must_use]
    pub const fn end(self) -> TimestampNs {
        self.end
    }

    /// Returns whether two half-open source-time ranges share any instant.
    #[must_use]
    pub fn intersects(self, other: Self) -> bool {
        self.start < other.end && other.start < self.end
    }

    fn day_range(
        origin: TimestampNs,
        start_days: u16,
        end_days: u16,
    ) -> Result<Self, ValidationError> {
        let start = add_days(origin, start_days)?;
        let end = add_days(origin, end_days)?;
        Self::new(start, end)
    }

    fn purge_end(self) -> Result<Self, ValidationError> {
        let end = subtract_duration(self.end, embargo())?;
        Self::new(self.start, end)
    }

    fn embargo_start(self) -> Result<Self, ValidationError> {
        let start = self
            .start
            .checked_add(embargo())
            .map_err(|_| ValidationError::TimeArithmetic)?;
        Self::new(start, self.end)
    }
}

/// One inner expanding training/validation fold with four-hour purge and embargo.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InnerFold {
    /// Zero-based inner-fold identity in chronological order.
    index: u8,
    /// Nominal expanding development period before four-hour label purge.
    nominal_training: TimeRange,
    /// Effective fitting range after the four-hour label/holding purge.
    training: TimeRange,
    /// Effective validation range after the four-hour embargo.
    validation: TimeRange,
}

impl InnerFold {
    /// Returns the zero-based inner-fold identity.
    #[must_use]
    pub const fn index(self) -> u8 {
        self.index
    }

    /// Returns the nominal expanding fit boundary used in the protocol document.
    #[must_use]
    pub const fn nominal_training(self) -> TimeRange {
        self.nominal_training
    }

    /// Returns the purged fitting range.
    #[must_use]
    pub const fn training(self) -> TimeRange {
        self.training
    }

    /// Returns the embargoed validation range.
    #[must_use]
    pub const fn validation(self) -> TimeRange {
        self.validation
    }
}

/// One complete outer nested walk-forward fold.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OuterFold {
    index: u16,
    /// Entire nominal development period (stripped v1: 5 days).
    development: TimeRange,
    /// Development after the calibration-boundary label purge.
    development_fit: TimeRange,
    /// Chronological days reserved from selection, after both boundary embargoes (stripped: 1).
    calibration: TimeRange,
    /// Untouched chronological days after final embargo (stripped: 1).
    test: TimeRange,
    inner: [InnerFold; 4],
}

impl OuterFold {
    /// Returns the zero-based chronological outer fold identity.
    #[must_use]
    pub const fn index(&self) -> u16 {
        self.index
    }

    /// Returns the nominal development window (stripped: 5 days).
    #[must_use]
    pub const fn development(&self) -> TimeRange {
        self.development
    }

    /// Returns the purged development window used to refit the frozen rule setting.
    #[must_use]
    pub const fn development_fit(&self) -> TimeRange {
        self.development_fit
    }

    /// Returns the untouched no-tuning calibration window.
    #[must_use]
    pub const fn calibration(&self) -> TimeRange {
        self.calibration
    }

    /// Returns the untouched outer test window.
    #[must_use]
    pub const fn test(&self) -> TimeRange {
        self.test
    }

    /// Returns all four immutable expanding inner folds.
    #[must_use]
    pub const fn inner(&self) -> &[InnerFold; 4] {
        &self.inner
    }

    fn intersects_gap(&self, gap: TimeRange) -> bool {
        // The complete outer window is intentionally conservative. A gap in a
        // nominal purge/embargo interval can still poison feature lookbacks or
        // replay recovery, so it may not be ignored merely because no outcome
        // was scored at that exact instant.
        self.development.start() < gap.end() && gap.start() < self.test.end()
    }
}

/// Immutable nested-walk-forward plan derived only from complete UTC days.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidationPlan {
    first_day: TimestampNs,
    complete_days: u16,
    outer: Vec<OuterFold>,
}

impl ValidationPlan {
    /// Builds every complete rolling outer fold from a trustworthy source horizon.
    ///
    /// Stripped v1: single outer test needs 7 complete days (5+1+1) with one-day roll.
    pub fn build(first_day: TimestampNs, complete_days: u16) -> Result<Self, ValidationError> {
        if first_day.value() % DAY_NS != 0 {
            return Err(ValidationError::UnalignedDayBoundary);
        }
        let required = Self::minimum_complete_days();
        if complete_days < required {
            return Err(ValidationError::InsufficientTrustworthyHistory {
                required_days: required,
                available_days: complete_days,
            });
        }
        let mut outer = Vec::new();
        let total = Self::outer_span_days();
        let mut offset = 0_u16;
        while offset.saturating_add(total) <= complete_days {
            outer.push(build_outer_fold(
                first_day,
                offset,
                u16::try_from(outer.len()).map_err(|_| ValidationError::TimeArithmetic)?,
            )?);
            offset = offset
                .checked_add(OUTER_ROLL_DAYS)
                .ok_or(ValidationError::TimeArithmetic)?;
        }
        Ok(Self {
            first_day,
            complete_days,
            outer,
        })
    }

    /// Returns the required complete days to form the stripped outer test(s).
    #[must_use]
    pub const fn minimum_complete_days() -> u16 {
        Self::outer_span_days() + OUTER_ROLL_DAYS * (REQUIRED_OUTER_TESTS as u16 - 1)
    }

    /// Returns one outer fold's nominal span, including its untouched test.
    #[must_use]
    pub const fn outer_span_days() -> u16 {
        DEVELOPMENT_DAYS + CALIBRATION_DAYS + TEST_DAYS
    }

    /// Returns the earliest complete UTC day admitted from the source manifest.
    #[must_use]
    pub const fn first_day(&self) -> TimestampNs {
        self.first_day
    }

    /// Returns the number of uninterrupted complete UTC source days.
    #[must_use]
    pub const fn complete_days(&self) -> u16 {
        self.complete_days
    }

    /// Returns all rolling outer folds in chronological order.
    #[must_use]
    pub fn outer(&self) -> &[OuterFold] {
        &self.outer
    }
}

/// Exact declared grid of twelve independently auditable rule settings.
#[derive(Debug, Clone, Copy, Default)]
pub struct RuleGrid;

impl RuleGrid {
    /// Number of and only number of permitted grid candidates.
    pub const CANDIDATE_COUNT: usize = 12;

    /// Returns the complete stable candidate ordering.
    #[must_use]
    pub const fn declared() -> [RuleConfig; Self::CANDIDATE_COUNT] {
        [
            RuleConfig::new(
                EntryThreshold::P55,
                AtrFloor::OnePointTwoFive,
                TakeProfitMultiple::OnePointFiveR,
            ),
            RuleConfig::new(
                EntryThreshold::P55,
                AtrFloor::OnePointTwoFive,
                TakeProfitMultiple::TwoR,
            ),
            RuleConfig::new(
                EntryThreshold::P55,
                AtrFloor::OnePointFive,
                TakeProfitMultiple::OnePointFiveR,
            ),
            RuleConfig::new(
                EntryThreshold::P55,
                AtrFloor::OnePointFive,
                TakeProfitMultiple::TwoR,
            ),
            RuleConfig::new(
                EntryThreshold::P60,
                AtrFloor::OnePointTwoFive,
                TakeProfitMultiple::OnePointFiveR,
            ),
            RuleConfig::new(
                EntryThreshold::P60,
                AtrFloor::OnePointTwoFive,
                TakeProfitMultiple::TwoR,
            ),
            RuleConfig::new(
                EntryThreshold::P60,
                AtrFloor::OnePointFive,
                TakeProfitMultiple::OnePointFiveR,
            ),
            RuleConfig::new(
                EntryThreshold::P60,
                AtrFloor::OnePointFive,
                TakeProfitMultiple::TwoR,
            ),
            RuleConfig::new(
                EntryThreshold::P65,
                AtrFloor::OnePointTwoFive,
                TakeProfitMultiple::OnePointFiveR,
            ),
            RuleConfig::new(
                EntryThreshold::P65,
                AtrFloor::OnePointTwoFive,
                TakeProfitMultiple::TwoR,
            ),
            RuleConfig::new(
                EntryThreshold::P65,
                AtrFloor::OnePointFive,
                TakeProfitMultiple::OnePointFiveR,
            ),
            RuleConfig::new(
                EntryThreshold::P65,
                AtrFloor::OnePointFive,
                TakeProfitMultiple::TwoR,
            ),
        ]
    }

    /// Checks whether one setting belongs to the declared and bounded grid.
    #[must_use]
    pub fn contains(config: RuleConfig) -> bool {
        Self::declared().contains(&config)
    }
}

/// Stable, human-readable selected parameter representation carried across the artifact boundary.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct RuleSelection {
    /// Base absolute composite entry threshold.
    pub threshold: String,
    /// Minimum ATR multiple for invalidation distance.
    pub atr_floor: String,
    /// Frozen reward multiple in R.
    pub take_profit: String,
}

impl RuleSelection {
    /// Converts an explicitly declared configuration into canonical values.
    #[must_use]
    pub fn from_config(config: RuleConfig) -> Self {
        Self {
            threshold: config.threshold().value().to_string(),
            atr_floor: config.atr_floor().value().to_string(),
            take_profit: config.take_profit().value().to_string(),
        }
    }

    /// Converts canonical artifact values back to one declared configuration.
    pub fn to_config(&self) -> Result<RuleConfig, ValidationError> {
        RuleGrid::declared()
            .into_iter()
            .find(|config| Self::from_config(*config) == *self)
            .ok_or(ValidationError::UndeclaredRuleParameter)
    }
}

/// Frozen unoptimizable family weights and regime gates carried inside every
/// active artifact, rather than inferred from a mutable external config file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FrozenRuleDefinitions {
    /// Immutable family-definition schema/version.
    pub family_version: String,
    /// Trend weights in family order: trend, momentum, mean reversion,
    /// microstructure, derivatives, cross-sectional.
    pub trend_weights: [String; 6],
    /// Range weights in that same immutable family order.
    pub range_weights: [String; 6],
    /// Immutable regime-definition schema/version.
    pub regime_version: String,
    /// Trend requires hourly ADX at least this value.
    pub trend_adx_floor: String,
    /// Trend requires absolute EMA8/EMA32 distance in ATR units at least this value.
    pub trend_ema_distance_atr_floor: String,
    /// Range requires hourly ADX at most this value.
    pub range_adx_ceiling: String,
    /// High volatility spans the inclusive historic realized-volatility p80..p95 interval.
    pub high_volatility_percentiles: [String; 2],
    /// Realized volatility strictly above p95 is an entry-blocking extreme regime.
    pub extreme_volatility_rule: String,
    /// Exact cross-family score agreement magnitude.
    pub agreement_magnitude: String,
    /// Exact high-volatility entry-threshold surcharge.
    pub high_volatility_threshold_surcharge: String,
    /// Exact opposite-composite exit magnitude.
    pub opposite_exit_magnitude: String,
}

impl FrozenRuleDefinitions {
    fn current() -> Self {
        Self {
            family_version: "trench.rules.families.v1".to_owned(),
            trend_weights: [
                "0.30".to_owned(),
                "0.25".to_owned(),
                "0".to_owned(),
                "0.20".to_owned(),
                "0.10".to_owned(),
                "0.15".to_owned(),
            ],
            range_weights: [
                "0".to_owned(),
                "0.10".to_owned(),
                "0.35".to_owned(),
                "0.25".to_owned(),
                "0.20".to_owned(),
                "0.10".to_owned(),
            ],
            regime_version: "trench.rules.regimes.v1".to_owned(),
            trend_adx_floor: "25".to_owned(),
            trend_ema_distance_atr_floor: "0.35".to_owned(),
            range_adx_ceiling: "20".to_owned(),
            high_volatility_percentiles: ["0.80".to_owned(), "0.95".to_owned()],
            extreme_volatility_rule: "realized_volatility_20_gt_p95".to_owned(),
            agreement_magnitude: "0.15".to_owned(),
            high_volatility_threshold_surcharge: "0.10".to_owned(),
            opposite_exit_magnitude: "0.25".to_owned(),
        }
    }
}

/// Actual fully costed outcome returned from the production engine replay path.
///
/// The four stream digests commit immutable prediction, intent, actual trade,
/// and cost evidence. This prevents research from accidentally scoring an
/// approximate fill/cost model instead of the runtime engine path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EngineReplayOutcome {
    net_pnl: Decimal,
    turnover: Decimal,
    closed_trades: u32,
    prediction_stream_digest: String,
    intent_stream_digest: String,
    trade_stream_digest: String,
    cost_stream_digest: String,
}

impl EngineReplayOutcome {
    /// Creates one validated outcome from actual engine-owned transition evidence.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        net_pnl: Decimal,
        turnover: Decimal,
        closed_trades: u32,
        prediction_stream_digest: impl Into<String>,
        intent_stream_digest: impl Into<String>,
        trade_stream_digest: impl Into<String>,
        cost_stream_digest: impl Into<String>,
    ) -> Result<Self, ValidationError> {
        let outcome = Self {
            net_pnl,
            turnover,
            closed_trades,
            prediction_stream_digest: prediction_stream_digest.into(),
            intent_stream_digest: intent_stream_digest.into(),
            trade_stream_digest: trade_stream_digest.into(),
            cost_stream_digest: cost_stream_digest.into(),
        };
        outcome.validate()?;
        Ok(outcome)
    }

    /// Returns aggregate net PnL after all actual paper costs and funding.
    #[must_use]
    pub const fn net_pnl(&self) -> Decimal {
        self.net_pnl
    }

    /// Returns total actual filled notional turnover.
    #[must_use]
    pub const fn turnover(&self) -> Decimal {
        self.turnover
    }

    /// Returns completed (flat) paper trades only.
    #[must_use]
    pub const fn closed_trades(&self) -> u32 {
        self.closed_trades
    }

    /// Returns median-selection net expectancy when at least one trade closed.
    fn expectancy(&self) -> Result<Decimal, ValidationError> {
        if self.closed_trades == 0 {
            return Err(ValidationError::NoInnerTrades);
        }
        self.net_pnl
            .checked_div(Decimal::from(self.closed_trades))
            .ok_or(ValidationError::InvalidEngineOutcome)
    }

    fn validate(&self) -> Result<(), ValidationError> {
        if self.turnover < Decimal::ZERO
            || [
                self.prediction_stream_digest.as_str(),
                self.intent_stream_digest.as_str(),
                self.trade_stream_digest.as_str(),
                self.cost_stream_digest.as_str(),
            ]
            .into_iter()
            .any(|digest| !is_digest(digest))
        {
            return Err(ValidationError::InvalidEngineOutcome);
        }
        Ok(())
    }
}

/// Replay request that binds a selected config to one immutable chronological segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuleReplayRequest {
    /// Frozen candidate to run through the production engine.
    pub config: RuleConfig,
    /// Owning outer fold.
    pub outer_fold: u16,
    /// Purpose and no-peeking role of this replay.
    pub phase: ReplayPhase,
    /// Optional fitting evidence range; rules values are not fitted but its
    /// explicit presence proves which history could inform a candidate run.
    pub training: Option<TimeRange>,
    /// Period over which actual paper outcomes are scored.
    pub evaluation: TimeRange,
}

/// Non-overlapping role of one replay request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplayPhase {
    /// Inner validation after the associated expanding fit range.
    InnerValidation { inner_fold: u8 },
    /// Chronological calibration output retained but never used for selection.
    Calibration,
    /// Untouched frozen outer test outcome.
    OuterTest,
}

/// Supplies outcomes produced by the production Engine/risk/broker path.
///
/// Implementations must use the runtime `Engine`, `RiskEngine`, paper broker,
/// activated universe, executable-book marking, fees, funding, and ledger.
/// This boundary intentionally has no price, fee, or cost inputs and therefore
/// cannot grow a second approximate research simulator.
pub trait RuleReplay {
    /// Runs one frozen config over one immutable no-look-ahead replay segment.
    fn replay(
        &mut self,
        request: RuleReplayRequest,
    ) -> Result<EngineReplayOutcome, ValidationError>;
}

/// Candidate's four inner validation results for one outer fold.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CandidateInnerOutcomes {
    /// One of the twelve declared rule settings.
    pub config: RuleConfig,
    /// Exactly four results aligned with `OuterFold::inner()`.
    pub outcomes: [EngineReplayOutcome; 4],
}

/// Selected candidate and metrics resulting only from its inner validations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InnerSelection {
    config: RuleConfig,
    median_net_expectancy: Decimal,
    turnover: Decimal,
}

impl InnerSelection {
    /// Returns the selected frozen config.
    #[must_use]
    pub const fn config(&self) -> RuleConfig {
        self.config
    }

    /// Returns the median of the four actual inner-fold net expectancies.
    #[must_use]
    pub const fn median_net_expectancy(&self) -> Decimal {
        self.median_net_expectancy
    }

    /// Returns total actual inner-validation turnover used only as tie-breaker.
    #[must_use]
    pub const fn turnover(&self) -> Decimal {
        self.turnover
    }
}

/// Performs the sole authorized rules parameter selection.
///
/// Inputs contain inner folds only. Calibration and test outcomes are not
/// accepted by this API, which makes their accidental use for selection
/// structurally impossible.
pub fn select_from_inner(
    candidates: &[CandidateInnerOutcomes],
) -> Result<InnerSelection, ValidationError> {
    if candidates.len() != RuleGrid::CANDIDATE_COUNT {
        return Err(ValidationError::IncompleteGrid);
    }
    let expected = RuleGrid::declared();
    let mut seen = Vec::with_capacity(candidates.len());
    let mut scored = Vec::with_capacity(candidates.len());
    for candidate in candidates {
        if !RuleGrid::contains(candidate.config) || seen.contains(&candidate.config) {
            return Err(ValidationError::IncompleteGrid);
        }
        seen.push(candidate.config);
        let mut expectancies = candidate
            .outcomes
            .iter()
            .map(EngineReplayOutcome::expectancy)
            .collect::<Result<Vec<_>, _>>()?;
        expectancies.sort_unstable();
        let median = expectancies[1]
            .checked_add(expectancies[2])
            .and_then(|value| value.checked_div(Decimal::from(2)))
            .ok_or(ValidationError::InvalidEngineOutcome)?;
        let turnover = candidate
            .outcomes
            .iter()
            .try_fold(Decimal::ZERO, |total, outcome| {
                total
                    .checked_add(outcome.turnover())
                    .ok_or(ValidationError::InvalidEngineOutcome)
            })?;
        scored.push(InnerSelection {
            config: candidate.config,
            median_net_expectancy: median,
            turnover,
        });
    }
    if !expected.into_iter().all(|config| seen.contains(&config)) {
        return Err(ValidationError::IncompleteGrid);
    }
    scored.sort_by(compare_selection);
    let winner = scored
        .first()
        .cloned()
        .ok_or(ValidationError::IncompleteGrid)?;
    if scored.get(1).is_some_and(|runner_up| {
        runner_up.median_net_expectancy == winner.median_net_expectancy
            && runner_up.turnover == winner.turnover
    }) {
        return Err(ValidationError::AmbiguousSelection);
    }
    Ok(winner)
}

fn compare_selection(left: &InnerSelection, right: &InnerSelection) -> Ordering {
    right
        .median_net_expectancy
        .cmp(&left.median_net_expectancy)
        .then_with(|| left.turnover.cmp(&right.turnover))
        .then_with(|| {
            RuleSelection::from_config(left.config).cmp(&RuleSelection::from_config(right.config))
        })
}

/// Immutable provenance that must match a frozen report and artifact.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResearchProvenance {
    /// Content hash of the canonical paper configuration bytes.
    pub config_digest: String,
    /// Build/code commitment that ran the replay.
    pub code_digest: String,
    /// Point-in-time normalized data/replay-manifest commitment.
    pub data_digest: String,
    /// Point-in-time dynamic-universe snapshot commitment.
    pub universe_digest: String,
    /// Common feature-schema implementation commitment.
    pub feature_schema_digest: String,
    /// Latest source event permitted to inform this research run.
    pub data_cutoff: TimestampNs,
}

impl ResearchProvenance {
    /// Validates complete BLAKE3 evidence before any selection is attempted.
    pub fn validate(&self) -> Result<(), ValidationError> {
        [
            self.config_digest.as_str(),
            self.code_digest.as_str(),
            self.data_digest.as_str(),
            self.universe_digest.as_str(),
            self.feature_schema_digest.as_str(),
        ]
        .into_iter()
        .all(is_digest)
        .then_some(())
        .ok_or(ValidationError::InvalidDigest)
    }
}

/// Excluded unusable market-data interval preserved in the validation report.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExcludedGap {
    /// Missing/recovery-contaminated source interval.
    pub range: TimeRange,
}

/// One candidate's auditable inner outcomes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CandidateSelectionRecord {
    /// Candidate's immutable parameter selection.
    pub selection: RuleSelection,
    /// Actual outcomes from each four inner validation windows.
    pub inner: [EngineReplayOutcome; 4],
    /// Median actual net expectancy used for ranking.
    pub median_net_expectancy: Decimal,
    /// Actual total turnover used only after expectancy tie.
    pub turnover: Decimal,
}

/// One outer-fold result with a selection frozen before calibration and test.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OuterFoldReport {
    /// Immutable source-time boundary plan.
    pub fold: OuterFold,
    /// Every bounded configuration evaluated on inner validations.
    pub candidates: Vec<CandidateSelectionRecord>,
    /// Setting selected solely from `candidates` inner outcomes.
    pub selected: RuleSelection,
    /// Actual calibrated run retained for audit but structurally excluded from selection.
    pub calibration: EngineReplayOutcome,
    /// Actual outer test after `selected` was frozen.
    pub test: EngineReplayOutcome,
}

/// Result of eligibility checks that never weaken the research protocol.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResearchEligibility {
    /// Eligible only after three outer tests and at least one hundred closed test trades.
    Eligible {
        /// Number of untouched outer-test folds.
        outer_test_folds: u16,
        /// Aggregate completed outer-test trades.
        closed_trades: u32,
    },
    /// Insufficient data or outputs; no active artifact is authorized.
    Ineligible {
        /// Stable machine-readable failure reason.
        reason: IneligibleReason,
        /// Observed complete source days.
        available_days: u16,
        /// Required complete source days, or zero when history existed but outcomes failed gates.
        required_days: u16,
        /// Untouched outer tests actually completed.
        outer_test_folds: u16,
        /// Aggregate closed outer-test trades actually observed.
        closed_trades: u32,
    },
}

/// Stable reason for a fail-closed research report.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IneligibleReason {
    /// Required full source history is not yet available.
    InsufficientTrustworthyHistory,
    /// A required point-in-time source stream was unavailable or gapped.
    RequiredDataUnavailable,
    /// Outer-test trade count did not meet the required hundred closed trades.
    InsufficientClosedTrades,
}

/// Content-addressed frozen rules artifact. It is the only active-mode source of rule values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RulesArtifact {
    selection: RuleSelection,
    definitions: FrozenRuleDefinitions,
    code_digest: String,
    feature_schema_digest: String,
    data_digest: String,
    data_cutoff: TimestampNs,
    artifact_version: String,
    digest: String,
}

impl RulesArtifact {
    /// Freezes a declared selection and complete immutable provenance.
    pub fn new(
        selection: RuleSelection,
        provenance: &ResearchProvenance,
    ) -> Result<Self, ValidationError> {
        provenance.validate()?;
        selection.to_config()?;
        let mut artifact = Self {
            selection,
            definitions: FrozenRuleDefinitions::current(),
            code_digest: provenance.code_digest.clone(),
            feature_schema_digest: provenance.feature_schema_digest.clone(),
            data_digest: provenance.data_digest.clone(),
            data_cutoff: provenance.data_cutoff,
            artifact_version: ARTIFACT_VERSION.to_owned(),
            digest: String::new(),
        };
        artifact.digest = artifact_digest(&artifact)?;
        Ok(artifact)
    }

    /// Returns the exact selected frozen configuration.
    pub fn config(&self) -> Result<RuleConfig, ValidationError> {
        self.selection.to_config()
    }

    /// Returns the canonical selected values.
    #[must_use]
    pub const fn selection(&self) -> &RuleSelection {
        &self.selection
    }

    /// Returns the exact immutable family/regime definitions sealed in this artifact.
    #[must_use]
    pub const fn definitions(&self) -> &FrozenRuleDefinitions {
        &self.definitions
    }

    /// Returns the content-addressed BLAKE3 artifact identity.
    #[must_use]
    pub fn digest(&self) -> &str {
        &self.digest
    }

    /// Returns a 64-hex strategy fingerprint compatible with engine candidates.
    #[must_use]
    pub fn strategy_fingerprint(&self) -> &str {
        self.digest
            .strip_prefix("b3:")
            .expect("artifact digest is validated")
    }

    /// Returns canonical immutable JSON bytes.
    pub fn canonical_json(&self) -> Result<Vec<u8>, ValidationError> {
        let wire = ArtifactWire::from(self);
        serde_json::to_vec(&wire).map_err(|_| ValidationError::InvalidJson)
    }

    /// Reopens and fully verifies canonical artifact bytes before active-mode use.
    pub fn from_canonical_json(bytes: &[u8]) -> Result<Self, ValidationError> {
        let wire: ArtifactWire =
            serde_json::from_slice(bytes).map_err(|_| ValidationError::InvalidJson)?;
        if wire.version != ARTIFACT_VERSION || !is_digest(&wire.digest) {
            return Err(ValidationError::InvalidJson);
        }
        let data_cutoff = TimestampNs::new(i128::from(wire.data_cutoff_ns))
            .map_err(|_| ValidationError::InvalidJson)?;
        let artifact = Self {
            selection: wire.selection,
            definitions: wire.definitions,
            code_digest: wire.code_digest,
            feature_schema_digest: wire.feature_schema_digest,
            data_digest: wire.data_digest,
            data_cutoff,
            artifact_version: wire.version,
            digest: wire.digest,
        };
        artifact.selection.to_config()?;
        if artifact.definitions != FrozenRuleDefinitions::current() {
            return Err(ValidationError::UndeclaredRuleParameter);
        }
        if artifact_digest(&artifact)? != artifact.digest {
            return Err(ValidationError::InvalidDigest);
        }
        Ok(artifact)
    }

    /// Verifies that artifact provenance exactly matches the active runtime sources.
    pub fn verify_provenance(
        &self,
        provenance: &ResearchProvenance,
    ) -> Result<(), ValidationError> {
        provenance.validate()?;
        (self.code_digest == provenance.code_digest
            && self.feature_schema_digest == provenance.feature_schema_digest
            && self.data_digest == provenance.data_digest
            && self.data_cutoff == provenance.data_cutoff)
            .then_some(())
            .ok_or(ValidationError::ArtifactReportMismatch)
    }
}

/// Canonical report containing every selected result or a fail-closed reason.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RulesValidationReport {
    provenance: ResearchProvenance,
    complete_days: u16,
    excluded_gaps: Vec<ExcludedGap>,
    folds: Vec<OuterFoldReport>,
    eligibility: ResearchEligibility,
    artifact: Option<RulesArtifact>,
    digest: String,
}

impl RulesValidationReport {
    /// Builds an explicit canonical ineligible report when source history is not trusted enough.
    pub fn insufficient_history(
        provenance: ResearchProvenance,
        complete_days: u16,
        excluded_gaps: Vec<ExcludedGap>,
    ) -> Result<Self, ValidationError> {
        provenance.validate()?;
        let excluded_gaps = normalize_excluded_gaps(excluded_gaps)?;
        let mut report = Self {
            provenance,
            complete_days,
            excluded_gaps,
            folds: Vec::new(),
            eligibility: ResearchEligibility::Ineligible {
                reason: IneligibleReason::InsufficientTrustworthyHistory,
                available_days: complete_days,
                required_days: ValidationPlan::minimum_complete_days(),
                outer_test_folds: 0,
                closed_trades: 0,
            },
            artifact: None,
            digest: String::new(),
        };
        report.digest = report_digest(&report)?;
        Ok(report)
    }

    /// Builds an explicit canonical report when a required point-in-time input
    /// is missing, gapped, or cannot feed the authoritative engine replay.
    pub fn required_data_unavailable(
        provenance: ResearchProvenance,
        complete_days: u16,
        excluded_gaps: Vec<ExcludedGap>,
    ) -> Result<Self, ValidationError> {
        provenance.validate()?;
        let excluded_gaps = normalize_excluded_gaps(excluded_gaps)?;
        let mut report = Self {
            provenance,
            complete_days,
            excluded_gaps,
            folds: Vec::new(),
            eligibility: ResearchEligibility::Ineligible {
                reason: IneligibleReason::RequiredDataUnavailable,
                available_days: complete_days,
                required_days: ValidationPlan::minimum_complete_days(),
                outer_test_folds: 0,
                closed_trades: 0,
            },
            artifact: None,
            digest: String::new(),
        };
        report.digest = report_digest(&report)?;
        Ok(report)
    }

    /// Runs every grid candidate through supplied production-engine replay evidence.
    pub fn run<E: RuleReplay>(
        plan: &ValidationPlan,
        provenance: ResearchProvenance,
        excluded_gaps: Vec<ExcludedGap>,
        replay: &mut E,
    ) -> Result<Self, ValidationError> {
        provenance.validate()?;
        let excluded_gaps = normalize_excluded_gaps(excluded_gaps)?;
        if excluded_gaps.iter().any(|gap| {
            plan.outer()
                .iter()
                .any(|fold| fold.intersects_gap(gap.range))
        }) {
            return Self::required_data_unavailable(
                provenance,
                plan.complete_days(),
                excluded_gaps,
            );
        }
        let mut folds = Vec::with_capacity(plan.outer().len());
        for fold in plan.outer() {
            let mut candidates = Vec::with_capacity(RuleGrid::CANDIDATE_COUNT);
            let mut selection_inputs = Vec::with_capacity(RuleGrid::CANDIDATE_COUNT);
            for config in RuleGrid::declared() {
                let [first, second, third, fourth] = fold.inner();
                let mut replay_inner = |inner: &InnerFold| {
                    replay.replay(RuleReplayRequest {
                        config,
                        outer_fold: fold.index(),
                        phase: ReplayPhase::InnerValidation {
                            inner_fold: inner.index(),
                        },
                        training: Some(inner.training()),
                        evaluation: inner.validation(),
                    })
                };
                let outcomes = [
                    replay_inner(first)?,
                    replay_inner(second)?,
                    replay_inner(third)?,
                    replay_inner(fourth)?,
                ];
                let selection = selection_from_outcomes(config, &outcomes)?;
                candidates.push(CandidateSelectionRecord {
                    selection: RuleSelection::from_config(config),
                    inner: outcomes.clone(),
                    median_net_expectancy: selection.median_net_expectancy,
                    turnover: selection.turnover,
                });
                selection_inputs.push(CandidateInnerOutcomes { config, outcomes });
            }
            let selected = select_from_inner(&selection_inputs)?;
            let calibration = replay.replay(RuleReplayRequest {
                config: selected.config,
                outer_fold: fold.index(),
                phase: ReplayPhase::Calibration,
                training: Some(fold.development_fit()),
                evaluation: fold.calibration(),
            })?;
            let test = replay.replay(RuleReplayRequest {
                config: selected.config,
                outer_fold: fold.index(),
                phase: ReplayPhase::OuterTest,
                training: Some(fold.development_fit()),
                evaluation: fold.test(),
            })?;
            folds.push(OuterFoldReport {
                fold: fold.clone(),
                candidates,
                selected: RuleSelection::from_config(selected.config),
                calibration,
                test,
            });
        }
        let closed_trades = folds.iter().try_fold(0_u32, |total, fold| {
            total
                .checked_add(fold.test.closed_trades())
                .ok_or(ValidationError::InvalidEngineOutcome)
        })?;
        let outer_test_folds =
            u16::try_from(folds.len()).map_err(|_| ValidationError::InvalidEngineOutcome)?;
        let eligibility =
            if folds.len() >= REQUIRED_OUTER_TESTS && closed_trades >= REQUIRED_CLOSED_TRADES {
                ResearchEligibility::Eligible {
                    outer_test_folds,
                    closed_trades,
                }
            } else {
                ResearchEligibility::Ineligible {
                    reason: IneligibleReason::InsufficientClosedTrades,
                    available_days: plan.complete_days(),
                    required_days: ValidationPlan::minimum_complete_days(),
                    outer_test_folds,
                    closed_trades,
                }
            };
        let artifact = match &eligibility {
            ResearchEligibility::Eligible { .. } => {
                // A release artifact represents the latest already-untouched outer choice.
                let selection = folds
                    .last()
                    .ok_or(ValidationError::IneligibleReport)?
                    .selected
                    .clone();
                Some(RulesArtifact::new(selection, &provenance)?)
            }
            ResearchEligibility::Ineligible { .. } => None,
        };
        let mut report = Self {
            provenance,
            complete_days: plan.complete_days(),
            excluded_gaps,
            folds,
            eligibility,
            artifact,
            digest: String::new(),
        };
        report.digest = report_digest(&report)?;
        Ok(report)
    }

    /// Returns the active-mode eligibility without attempting a parameter fallback.
    #[must_use]
    pub const fn eligibility(&self) -> &ResearchEligibility {
        &self.eligibility
    }

    /// Returns the active-mode artifact only when all hard eligibility gates passed.
    #[must_use]
    pub const fn artifact(&self) -> Option<&RulesArtifact> {
        self.artifact.as_ref()
    }

    /// Returns the immutable report content address.
    #[must_use]
    pub fn digest(&self) -> &str {
        &self.digest
    }

    /// Returns canonical immutable JSON bytes.
    pub fn canonical_json(&self) -> Result<Vec<u8>, ValidationError> {
        serde_json::to_vec(&ReportWire::from(self)).map_err(|_| ValidationError::InvalidJson)
    }

    /// Reopens and verifies canonical report bytes before active-mode activation.
    pub fn from_canonical_json(bytes: &[u8]) -> Result<Self, ValidationError> {
        let wire: ReportWire =
            serde_json::from_slice(bytes).map_err(|_| ValidationError::InvalidJson)?;
        let report = wire.into_report()?;
        if report_digest(&report)? != report.digest {
            return Err(ValidationError::InvalidDigest);
        }
        report.validate_structure()?;
        Ok(report)
    }

    /// Checks whether this report and its optional artifact form one active-mode pair.
    pub fn validate_active_pair(&self) -> Result<(), ValidationError> {
        self.validate_structure()?;
        match (&self.eligibility, &self.artifact) {
            (ResearchEligibility::Eligible { .. }, Some(artifact)) => {
                artifact.verify_provenance(&self.provenance)?;
                let final_selection = self
                    .folds
                    .last()
                    .map(|fold| &fold.selected)
                    .ok_or(ValidationError::ArtifactReportMismatch)?;
                (artifact.selection() == final_selection)
                    .then_some(())
                    .ok_or(ValidationError::ArtifactReportMismatch)
            }
            _ => Err(ValidationError::IneligibleReport),
        }
    }

    /// Verifies the runtime-bound portions of an otherwise complete active pair.
    ///
    /// `expected_config_digest` commits the canonical research-relevant portion
    /// of the physical configuration selected by the daemon. It excludes the
    /// artifact-reference table to avoid a self-referential report-digest cycle.
    /// `expected_code_digest` comes from the embedded workspace build commitment.
    /// Feature/data commitments are checked between the report and artifact by
    /// [`Self::validate_active_pair`], preserving the data cutoff selected during
    /// research without pretending that forward market data must hash to a
    /// historical immutable input.
    pub fn validate_for_active(
        &self,
        expected_config_digest: &str,
        expected_code_digest: &str,
    ) -> Result<&RulesArtifact, ValidationError> {
        if !is_digest(expected_config_digest) || !is_digest(expected_code_digest) {
            return Err(ValidationError::InvalidDigest);
        }
        self.validate_active_pair()?;
        if self.provenance.config_digest != expected_config_digest
            || self.provenance.code_digest != expected_code_digest
        {
            return Err(ValidationError::ArtifactReportMismatch);
        }
        self.artifact
            .as_ref()
            .ok_or(ValidationError::IneligibleReport)
    }

    fn validate_structure(&self) -> Result<(), ValidationError> {
        self.provenance.validate()?;
        if normalize_excluded_gaps(self.excluded_gaps.clone())? != self.excluded_gaps {
            return Err(ValidationError::InvalidExcludedGaps);
        }
        let expected_plan = match self.folds.first() {
            Some(first) => {
                ValidationPlan::build(first.fold.development().start(), self.complete_days)?
            }
            None => return self.validate_empty_ineligible_report(),
        };
        if expected_plan.outer().len() != self.folds.len()
            || expected_plan
                .outer()
                .iter()
                .zip(&self.folds)
                .any(|(expected, actual)| expected != &actual.fold)
            || self.excluded_gaps.iter().any(|gap| {
                expected_plan
                    .outer()
                    .iter()
                    .any(|fold| fold.intersects_gap(gap.range))
            })
        {
            return Err(ValidationError::RequiredDataUnavailable);
        }

        for fold in &self.folds {
            validate_outer_fold_report(fold)?;
        }
        let closed_trades = self.folds.iter().try_fold(0_u32, |total, fold| {
            total
                .checked_add(fold.test.closed_trades())
                .ok_or(ValidationError::InvalidEngineOutcome)
        })?;
        let outer_test_folds =
            u16::try_from(self.folds.len()).map_err(|_| ValidationError::InvalidEngineOutcome)?;
        match (&self.eligibility, &self.artifact) {
            (
                ResearchEligibility::Eligible {
                    outer_test_folds: declared_folds,
                    closed_trades: declared_trades,
                },
                Some(_),
            ) if usize::from(*declared_folds) >= REQUIRED_OUTER_TESTS
                && *declared_folds == outer_test_folds
                && *declared_trades == closed_trades
                && closed_trades >= REQUIRED_CLOSED_TRADES =>
            {
                Ok(())
            }
            (
                ResearchEligibility::Ineligible {
                    reason: IneligibleReason::InsufficientClosedTrades,
                    available_days,
                    required_days,
                    outer_test_folds: declared_folds,
                    closed_trades: declared_trades,
                },
                None,
            ) if *available_days == self.complete_days
                && *required_days == ValidationPlan::minimum_complete_days()
                && *declared_folds == outer_test_folds
                && *declared_trades == closed_trades
                && closed_trades < REQUIRED_CLOSED_TRADES =>
            {
                Ok(())
            }
            _ => Err(ValidationError::IneligibleReport),
        }
    }

    fn validate_empty_ineligible_report(&self) -> Result<(), ValidationError> {
        match (&self.eligibility, &self.artifact) {
            (
                ResearchEligibility::Ineligible {
                    reason: IneligibleReason::InsufficientTrustworthyHistory,
                    available_days,
                    required_days,
                    outer_test_folds: 0,
                    closed_trades: 0,
                },
                None,
            ) if *available_days == self.complete_days
                && *available_days < ValidationPlan::minimum_complete_days()
                && *required_days == ValidationPlan::minimum_complete_days() =>
            {
                Ok(())
            }
            (
                ResearchEligibility::Ineligible {
                    reason: IneligibleReason::RequiredDataUnavailable,
                    available_days,
                    required_days,
                    outer_test_folds: 0,
                    closed_trades: 0,
                },
                None,
            ) if *available_days == self.complete_days
                && *required_days == ValidationPlan::minimum_complete_days() =>
            {
                Ok(())
            }
            _ => Err(ValidationError::IneligibleReport),
        }
    }
}

fn validate_outer_fold_report(report: &OuterFoldReport) -> Result<(), ValidationError> {
    if report.candidates.len() != RuleGrid::CANDIDATE_COUNT {
        return Err(ValidationError::IncompleteGrid);
    }
    let selection_inputs = report
        .candidates
        .iter()
        .map(|candidate| {
            let config = candidate.selection.to_config()?;
            let recomputed = selection_from_outcomes(config, &candidate.inner)?;
            if candidate.median_net_expectancy != recomputed.median_net_expectancy
                || candidate.turnover != recomputed.turnover
            {
                return Err(ValidationError::InvalidInnerFoldOutcomes);
            }
            Ok(CandidateInnerOutcomes {
                config,
                outcomes: candidate.inner.clone(),
            })
        })
        .collect::<Result<Vec<_>, ValidationError>>()?;
    let selection = select_from_inner(&selection_inputs)?;
    if RuleSelection::from_config(selection.config()) != report.selected {
        return Err(ValidationError::InvalidInnerFoldOutcomes);
    }
    report.calibration.validate()?;
    report.test.validate()?;
    Ok(())
}

fn normalize_excluded_gaps(
    mut gaps: Vec<ExcludedGap>,
) -> Result<Vec<ExcludedGap>, ValidationError> {
    gaps.sort_by(|left, right| {
        (left.range.start(), left.range.end()).cmp(&(right.range.start(), right.range.end()))
    });
    if gaps
        .windows(2)
        .any(|pair| pair[0].range.end() > pair[1].range.start())
    {
        return Err(ValidationError::InvalidExcludedGaps);
    }
    Ok(gaps)
}

fn selection_from_outcomes(
    config: RuleConfig,
    outcomes: &[EngineReplayOutcome; 4],
) -> Result<InnerSelection, ValidationError> {
    let mut expectancies = outcomes
        .iter()
        .map(EngineReplayOutcome::expectancy)
        .collect::<Result<Vec<_>, _>>()?;
    expectancies.sort_unstable();
    let median = expectancies[1]
        .checked_add(expectancies[2])
        .and_then(|value| value.checked_div(Decimal::from(2)))
        .ok_or(ValidationError::InvalidEngineOutcome)?;
    let turnover = outcomes.iter().try_fold(Decimal::ZERO, |total, outcome| {
        total
            .checked_add(outcome.turnover())
            .ok_or(ValidationError::InvalidEngineOutcome)
    })?;
    Ok(InnerSelection {
        config,
        median_net_expectancy: median,
        turnover,
    })
}

fn build_outer_fold(
    first_day: TimestampNs,
    offset_days: u16,
    index: u16,
) -> Result<OuterFold, ValidationError> {
    let development = TimeRange::day_range(first_day, offset_days, offset_days + DEVELOPMENT_DAYS)?;
    let calibration_nominal = TimeRange::day_range(
        first_day,
        offset_days + DEVELOPMENT_DAYS,
        offset_days + DEVELOPMENT_DAYS + CALIBRATION_DAYS,
    )?;
    let test_nominal = TimeRange::day_range(
        first_day,
        offset_days + DEVELOPMENT_DAYS + CALIBRATION_DAYS,
        offset_days + ValidationPlan::outer_span_days(),
    )?;
    let inner_offsets = [1_u16, 2, 3, 4];
    let inner = inner_offsets
        .map(|training_end| {
            let nominal_training =
                TimeRange::day_range(first_day, offset_days, offset_days + training_end)?;
            let validation = TimeRange::day_range(
                first_day,
                offset_days + training_end,
                offset_days + training_end + TEST_DAYS,
            )?;
            Ok(InnerFold {
                index: u8::try_from(training_end - 1)
                    .map_err(|_| ValidationError::TimeArithmetic)?,
                nominal_training,
                training: nominal_training.purge_end()?,
                validation: validation.embargo_start()?,
            })
        })
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?
        .try_into()
        .map_err(|_| ValidationError::TimeArithmetic)?;
    Ok(OuterFold {
        index,
        development,
        development_fit: development.purge_end()?,
        calibration: calibration_nominal.embargo_start()?.purge_end()?,
        test: test_nominal.embargo_start()?,
        inner,
    })
}

fn add_days(origin: TimestampNs, days: u16) -> Result<TimestampNs, ValidationError> {
    let nanos = i128::from(days)
        .checked_mul(i128::from(DAY_NS))
        .ok_or(ValidationError::TimeArithmetic)?;
    origin
        .checked_add(DurationNs::new(nanos).map_err(|_| ValidationError::TimeArithmetic)?)
        .map_err(|_| ValidationError::TimeArithmetic)
}

fn subtract_duration(
    timestamp: TimestampNs,
    duration: DurationNs,
) -> Result<TimestampNs, ValidationError> {
    let remaining = i128::from(timestamp.value())
        .checked_sub(i128::from(duration.value()))
        .ok_or(ValidationError::TimeArithmetic)?;
    TimestampNs::new(remaining).map_err(|_| ValidationError::TimeArithmetic)
}

fn embargo() -> DurationNs {
    DurationNs::new(i128::from(FOUR_HOURS_NS)).expect("four-hour protocol constant is valid")
}

fn is_digest(value: &str) -> bool {
    value.strip_prefix("b3:").is_some_and(|hex| {
        hex.len() == 64
            && hex
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    })
}

fn digest(domain: &str, bytes: &[u8]) -> String {
    let mut hasher = Hasher::new_derive_key(domain);
    hasher.update(bytes);
    format!("b3:{}", hasher.finalize().to_hex())
}

fn artifact_digest(artifact: &RulesArtifact) -> Result<String, ValidationError> {
    let unsigned = ArtifactUnsignedWire::from(artifact);
    let bytes = serde_json::to_vec(&unsigned).map_err(|_| ValidationError::InvalidJson)?;
    Ok(digest("trench.rules-artifact.v1", &bytes))
}

fn report_digest(report: &RulesValidationReport) -> Result<String, ValidationError> {
    let unsigned = ReportUnsignedWire::from(report);
    let bytes = serde_json::to_vec(&unsigned).map_err(|_| ValidationError::InvalidJson)?;
    Ok(digest("trench.rules-validation.v1", &bytes))
}

#[derive(Debug, Serialize)]
struct ArtifactUnsignedWire {
    version: String,
    selection: RuleSelection,
    definitions: FrozenRuleDefinitions,
    code_digest: String,
    feature_schema_digest: String,
    data_digest: String,
    data_cutoff_ns: i64,
}

impl From<&RulesArtifact> for ArtifactUnsignedWire {
    fn from(artifact: &RulesArtifact) -> Self {
        Self {
            version: artifact.artifact_version.clone(),
            selection: artifact.selection.clone(),
            definitions: artifact.definitions.clone(),
            code_digest: artifact.code_digest.clone(),
            feature_schema_digest: artifact.feature_schema_digest.clone(),
            data_digest: artifact.data_digest.clone(),
            data_cutoff_ns: artifact.data_cutoff.value(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ArtifactWire {
    version: String,
    selection: RuleSelection,
    definitions: FrozenRuleDefinitions,
    code_digest: String,
    feature_schema_digest: String,
    data_digest: String,
    data_cutoff_ns: i64,
    digest: String,
}

impl From<&RulesArtifact> for ArtifactWire {
    fn from(artifact: &RulesArtifact) -> Self {
        let unsigned = ArtifactUnsignedWire::from(artifact);
        Self {
            version: unsigned.version,
            selection: unsigned.selection,
            definitions: unsigned.definitions,
            code_digest: unsigned.code_digest,
            feature_schema_digest: unsigned.feature_schema_digest,
            data_digest: unsigned.data_digest,
            data_cutoff_ns: unsigned.data_cutoff_ns,
            digest: artifact.digest.clone(),
        }
    }
}

#[derive(Debug, Serialize)]
struct ReportUnsignedWire {
    version: &'static str,
    provenance: ProvenanceWire,
    complete_days: u16,
    excluded_gaps: Vec<TimeRangeWire>,
    folds: Vec<OuterFoldReportWire>,
    eligibility: EligibilityWire,
    artifact: Option<ArtifactWire>,
}

impl From<&RulesValidationReport> for ReportUnsignedWire {
    fn from(report: &RulesValidationReport) -> Self {
        Self {
            version: REPORT_VERSION,
            provenance: ProvenanceWire::from(&report.provenance),
            complete_days: report.complete_days,
            excluded_gaps: report
                .excluded_gaps
                .iter()
                .map(|gap| TimeRangeWire::from(gap.range))
                .collect(),
            folds: report.folds.iter().map(OuterFoldReportWire::from).collect(),
            eligibility: EligibilityWire::from(&report.eligibility),
            artifact: report.artifact.as_ref().map(ArtifactWire::from),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReportWire {
    version: String,
    provenance: ProvenanceWire,
    complete_days: u16,
    excluded_gaps: Vec<TimeRangeWire>,
    folds: Vec<OuterFoldReportWire>,
    eligibility: EligibilityWire,
    artifact: Option<ArtifactWire>,
    digest: String,
}

impl From<&RulesValidationReport> for ReportWire {
    fn from(report: &RulesValidationReport) -> Self {
        let unsigned = ReportUnsignedWire::from(report);
        Self {
            version: unsigned.version.to_owned(),
            provenance: unsigned.provenance,
            complete_days: unsigned.complete_days,
            excluded_gaps: unsigned.excluded_gaps,
            folds: unsigned.folds,
            eligibility: unsigned.eligibility,
            artifact: unsigned.artifact,
            digest: report.digest.clone(),
        }
    }
}

impl ReportWire {
    fn into_report(self) -> Result<RulesValidationReport, ValidationError> {
        if self.version != REPORT_VERSION || !is_digest(&self.digest) {
            return Err(ValidationError::InvalidJson);
        }
        let provenance = self.provenance.into_provenance()?;
        let artifact = self
            .artifact
            .map(|wire| {
                RulesArtifact::from_canonical_json(
                    &serde_json::to_vec(&wire).map_err(|_| ValidationError::InvalidJson)?,
                )
            })
            .transpose()?;
        let excluded_gaps = self
            .excluded_gaps
            .into_iter()
            .map(|range| {
                Ok(ExcludedGap {
                    range: range.into_range()?,
                })
            })
            .collect::<Result<Vec<_>, ValidationError>>()?;
        let excluded_gaps = normalize_excluded_gaps(excluded_gaps)?;
        let folds = self
            .folds
            .into_iter()
            .map(OuterFoldReportWire::into_report)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(RulesValidationReport {
            provenance,
            complete_days: self.complete_days,
            excluded_gaps,
            folds,
            eligibility: self.eligibility.into_eligibility()?,
            artifact,
            digest: self.digest,
        })
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProvenanceWire {
    config_digest: String,
    code_digest: String,
    data_digest: String,
    universe_digest: String,
    feature_schema_digest: String,
    data_cutoff_ns: i64,
}

impl From<&ResearchProvenance> for ProvenanceWire {
    fn from(provenance: &ResearchProvenance) -> Self {
        Self {
            config_digest: provenance.config_digest.clone(),
            code_digest: provenance.code_digest.clone(),
            data_digest: provenance.data_digest.clone(),
            universe_digest: provenance.universe_digest.clone(),
            feature_schema_digest: provenance.feature_schema_digest.clone(),
            data_cutoff_ns: provenance.data_cutoff.value(),
        }
    }
}

impl ProvenanceWire {
    fn into_provenance(self) -> Result<ResearchProvenance, ValidationError> {
        let provenance = ResearchProvenance {
            config_digest: self.config_digest,
            code_digest: self.code_digest,
            data_digest: self.data_digest,
            universe_digest: self.universe_digest,
            feature_schema_digest: self.feature_schema_digest,
            data_cutoff: TimestampNs::new(i128::from(self.data_cutoff_ns))
                .map_err(|_| ValidationError::InvalidJson)?,
        };
        provenance.validate()?;
        Ok(provenance)
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TimeRangeWire {
    start_ns: i64,
    end_ns: i64,
}

impl From<TimeRange> for TimeRangeWire {
    fn from(range: TimeRange) -> Self {
        Self {
            start_ns: range.start().value(),
            end_ns: range.end().value(),
        }
    }
}

impl TimeRangeWire {
    fn into_range(self) -> Result<TimeRange, ValidationError> {
        TimeRange::new(
            TimestampNs::new(i128::from(self.start_ns))
                .map_err(|_| ValidationError::InvalidJson)?,
            TimestampNs::new(i128::from(self.end_ns)).map_err(|_| ValidationError::InvalidJson)?,
        )
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct InnerFoldWire {
    index: u8,
    nominal_training: TimeRangeWire,
    training: TimeRangeWire,
    validation: TimeRangeWire,
}

impl From<InnerFold> for InnerFoldWire {
    fn from(fold: InnerFold) -> Self {
        Self {
            index: fold.index,
            nominal_training: fold.nominal_training.into(),
            training: fold.training.into(),
            validation: fold.validation.into(),
        }
    }
}

impl InnerFoldWire {
    fn into_fold(self) -> Result<InnerFold, ValidationError> {
        Ok(InnerFold {
            index: self.index,
            nominal_training: self.nominal_training.into_range()?,
            training: self.training.into_range()?,
            validation: self.validation.into_range()?,
        })
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct OuterFoldWire {
    index: u16,
    development: TimeRangeWire,
    development_fit: TimeRangeWire,
    calibration: TimeRangeWire,
    test: TimeRangeWire,
    inner: [InnerFoldWire; 4],
}

impl From<&OuterFold> for OuterFoldWire {
    fn from(fold: &OuterFold) -> Self {
        Self {
            index: fold.index,
            development: fold.development.into(),
            development_fit: fold.development_fit.into(),
            calibration: fold.calibration.into(),
            test: fold.test.into(),
            inner: fold.inner.map(InnerFoldWire::from),
        }
    }
}

impl OuterFoldWire {
    fn into_fold(self) -> Result<OuterFold, ValidationError> {
        let [first, second, third, fourth] = self.inner;
        Ok(OuterFold {
            index: self.index,
            development: self.development.into_range()?,
            development_fit: self.development_fit.into_range()?,
            calibration: self.calibration.into_range()?,
            test: self.test.into_range()?,
            inner: [
                first.into_fold()?,
                second.into_fold()?,
                third.into_fold()?,
                fourth.into_fold()?,
            ],
        })
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct OutcomeWire {
    net_pnl: String,
    turnover: String,
    closed_trades: u32,
    prediction_stream_digest: String,
    intent_stream_digest: String,
    trade_stream_digest: String,
    cost_stream_digest: String,
}

impl From<&EngineReplayOutcome> for OutcomeWire {
    fn from(outcome: &EngineReplayOutcome) -> Self {
        Self {
            net_pnl: outcome.net_pnl.to_string(),
            turnover: outcome.turnover.to_string(),
            closed_trades: outcome.closed_trades,
            prediction_stream_digest: outcome.prediction_stream_digest.clone(),
            intent_stream_digest: outcome.intent_stream_digest.clone(),
            trade_stream_digest: outcome.trade_stream_digest.clone(),
            cost_stream_digest: outcome.cost_stream_digest.clone(),
        }
    }
}

impl OutcomeWire {
    fn into_outcome(self) -> Result<EngineReplayOutcome, ValidationError> {
        EngineReplayOutcome::new(
            self.net_pnl
                .parse()
                .map_err(|_| ValidationError::InvalidJson)?,
            self.turnover
                .parse()
                .map_err(|_| ValidationError::InvalidJson)?,
            self.closed_trades,
            self.prediction_stream_digest,
            self.intent_stream_digest,
            self.trade_stream_digest,
            self.cost_stream_digest,
        )
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CandidateSelectionRecordWire {
    selection: RuleSelection,
    inner: [OutcomeWire; 4],
    median_net_expectancy: String,
    turnover: String,
}

impl From<&CandidateSelectionRecord> for CandidateSelectionRecordWire {
    fn from(record: &CandidateSelectionRecord) -> Self {
        Self {
            selection: record.selection.clone(),
            inner: record.inner.each_ref().map(OutcomeWire::from),
            median_net_expectancy: record.median_net_expectancy.to_string(),
            turnover: record.turnover.to_string(),
        }
    }
}

impl CandidateSelectionRecordWire {
    fn into_record(self) -> Result<CandidateSelectionRecord, ValidationError> {
        let selection = self.selection;
        selection.to_config()?;
        let [first, second, third, fourth] = self.inner;
        Ok(CandidateSelectionRecord {
            selection,
            inner: [
                first.into_outcome()?,
                second.into_outcome()?,
                third.into_outcome()?,
                fourth.into_outcome()?,
            ],
            median_net_expectancy: self
                .median_net_expectancy
                .parse()
                .map_err(|_| ValidationError::InvalidJson)?,
            turnover: self
                .turnover
                .parse()
                .map_err(|_| ValidationError::InvalidJson)?,
        })
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct OuterFoldReportWire {
    fold: OuterFoldWire,
    candidates: Vec<CandidateSelectionRecordWire>,
    selected: RuleSelection,
    calibration: OutcomeWire,
    test: OutcomeWire,
}

impl From<&OuterFoldReport> for OuterFoldReportWire {
    fn from(report: &OuterFoldReport) -> Self {
        Self {
            fold: OuterFoldWire::from(&report.fold),
            candidates: report
                .candidates
                .iter()
                .map(CandidateSelectionRecordWire::from)
                .collect(),
            selected: report.selected.clone(),
            calibration: OutcomeWire::from(&report.calibration),
            test: OutcomeWire::from(&report.test),
        }
    }
}

impl OuterFoldReportWire {
    fn into_report(self) -> Result<OuterFoldReport, ValidationError> {
        let selected = self.selected;
        selected.to_config()?;
        Ok(OuterFoldReport {
            fold: self.fold.into_fold()?,
            candidates: self
                .candidates
                .into_iter()
                .map(CandidateSelectionRecordWire::into_record)
                .collect::<Result<Vec<_>, _>>()?,
            selected,
            calibration: self.calibration.into_outcome()?,
            test: self.test.into_outcome()?,
        })
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
enum EligibilityWire {
    Eligible {
        outer_test_folds: u16,
        closed_trades: u32,
    },
    Ineligible {
        reason: IneligibleReasonWire,
        available_days: u16,
        required_days: u16,
        outer_test_folds: u16,
        closed_trades: u32,
    },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum IneligibleReasonWire {
    InsufficientTrustworthyHistory,
    RequiredDataUnavailable,
    InsufficientClosedTrades,
}

impl From<&ResearchEligibility> for EligibilityWire {
    fn from(eligibility: &ResearchEligibility) -> Self {
        match eligibility {
            ResearchEligibility::Eligible {
                outer_test_folds,
                closed_trades,
            } => Self::Eligible {
                outer_test_folds: *outer_test_folds,
                closed_trades: *closed_trades,
            },
            ResearchEligibility::Ineligible {
                reason,
                available_days,
                required_days,
                outer_test_folds,
                closed_trades,
            } => Self::Ineligible {
                reason: IneligibleReasonWire::from(*reason),
                available_days: *available_days,
                required_days: *required_days,
                outer_test_folds: *outer_test_folds,
                closed_trades: *closed_trades,
            },
        }
    }
}

impl From<IneligibleReason> for IneligibleReasonWire {
    fn from(reason: IneligibleReason) -> Self {
        match reason {
            IneligibleReason::InsufficientTrustworthyHistory => {
                Self::InsufficientTrustworthyHistory
            }
            IneligibleReason::RequiredDataUnavailable => Self::RequiredDataUnavailable,
            IneligibleReason::InsufficientClosedTrades => Self::InsufficientClosedTrades,
        }
    }
}

impl EligibilityWire {
    fn into_eligibility(self) -> Result<ResearchEligibility, ValidationError> {
        match self {
            Self::Eligible {
                outer_test_folds,
                closed_trades,
            } if usize::from(outer_test_folds) >= REQUIRED_OUTER_TESTS
                && closed_trades >= REQUIRED_CLOSED_TRADES =>
            {
                Ok(ResearchEligibility::Eligible {
                    outer_test_folds,
                    closed_trades,
                })
            }
            Self::Eligible { .. } => Err(ValidationError::IneligibleReport),
            Self::Ineligible {
                reason,
                available_days,
                required_days,
                outer_test_folds,
                closed_trades,
            } => Ok(ResearchEligibility::Ineligible {
                reason: match reason {
                    IneligibleReasonWire::InsufficientTrustworthyHistory => {
                        IneligibleReason::InsufficientTrustworthyHistory
                    }
                    IneligibleReasonWire::RequiredDataUnavailable => {
                        IneligibleReason::RequiredDataUnavailable
                    }
                    IneligibleReasonWire::InsufficientClosedTrades => {
                        IneligibleReason::InsufficientClosedTrades
                    }
                },
                available_days,
                required_days,
                outer_test_folds,
                closed_trades,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use crate::strategy::Strategy;
    use crate::strategy::rules::RulesStrategy;

    use super::{
        AtrFloor, CandidateInnerOutcomes, EngineReplayOutcome, EntryThreshold, IneligibleReason,
        ReplayPhase, ResearchEligibility, ResearchProvenance, RuleConfig, RuleGrid, RuleReplay,
        RuleReplayRequest, RuleSelection, RulesArtifact, RulesValidationReport, TakeProfitMultiple,
        TimeRange, TimestampNs, ValidationError, ValidationPlan, select_from_inner,
    };

    const DAY_NS: i64 = 86_400_000_000_000;
    const HOUR_NS: i64 = 3_600_000_000_000;

    fn timestamp(days: i64) -> TimestampNs {
        TimestampNs::new(i128::from(days * DAY_NS)).expect("day boundary")
    }

    fn digest(index: u8) -> String {
        format!("b3:{index:064x}")
    }

    fn outcome(net_pnl: Decimal, turnover: Decimal, trades: u32, index: u8) -> EngineReplayOutcome {
        EngineReplayOutcome::new(
            net_pnl,
            turnover,
            trades,
            digest(index),
            digest(index.saturating_add(1)),
            digest(index.saturating_add(2)),
            digest(index.saturating_add(3)),
        )
        .expect("actual engine outcome")
    }

    fn provenance() -> ResearchProvenance {
        ResearchProvenance {
            config_digest: digest(10),
            code_digest: digest(11),
            data_digest: digest(12),
            universe_digest: digest(13),
            feature_schema_digest: digest(14),
            data_cutoff: timestamp(1_000),
        }
    }

    struct DeterministicEngineReplay {
        inner_bias: Option<RuleConfig>,
        outer_test_delta: Decimal,
    }

    impl DeterministicEngineReplay {
        fn baseline() -> Self {
            Self {
                inner_bias: None,
                outer_test_delta: Decimal::ZERO,
            }
        }
    }

    impl RuleReplay for DeterministicEngineReplay {
        fn replay(
            &mut self,
            request: RuleReplayRequest,
        ) -> Result<EngineReplayOutcome, ValidationError> {
            let threshold = request.config.threshold().value();
            let atr = request.config.atr_floor().value();
            let take_profit = request.config.take_profit().value();
            let mut net_pnl = threshold * dec!(100) + atr + take_profit / dec!(100);
            if self.inner_bias == Some(request.config)
                && matches!(request.phase, ReplayPhase::InnerValidation { .. })
            {
                net_pnl += dec!(100);
            }
            let (closed_trades, phase_offset) = match request.phase {
                ReplayPhase::InnerValidation { inner_fold } => (1, inner_fold),
                ReplayPhase::Calibration => (1, 10),
                ReplayPhase::OuterTest => {
                    net_pnl += self.outer_test_delta;
                    (34, 20)
                }
            };
            Ok(outcome(
                net_pnl,
                dec!(1),
                closed_trades,
                request.outer_fold as u8 + phase_offset + 100,
            ))
        }
    }

    fn complete_report(replay: &mut DeterministicEngineReplay) -> RulesValidationReport {
        RulesValidationReport::run(
            &ValidationPlan::build(timestamp(0), ValidationPlan::minimum_complete_days())
                .expect("stripped outer fold"),
            provenance(),
            Vec::new(),
            replay,
        )
        .expect("production-engine evidence creates report")
    }

    #[test]
    fn folds_use_exact_chronological_windows_with_four_hour_purge_and_embargo() {
        let plan = ValidationPlan::build(timestamp(0), ValidationPlan::minimum_complete_days())
            .expect("stripped outer fold");
        assert_eq!(plan.outer().len(), 1);
        let fold = &plan.outer()[0];
        assert_eq!(fold.development().start(), timestamp(0));
        assert_eq!(fold.development().end(), timestamp(5));
        assert_eq!(
            fold.development_fit().end().value(),
            5 * DAY_NS - 4 * HOUR_NS
        );
        assert_eq!(fold.calibration().start().value(), 5 * DAY_NS + 4 * HOUR_NS);
        assert_eq!(fold.calibration().end().value(), 6 * DAY_NS - 4 * HOUR_NS);
        assert_eq!(fold.test().start().value(), 6 * DAY_NS + 4 * HOUR_NS);
        assert_eq!(fold.test().end(), timestamp(7));
        let inner = fold.inner();
        for (index, train_end) in [1_i64, 2, 3, 4].into_iter().enumerate() {
            assert_eq!(inner[index].nominal_training().start(), timestamp(0));
            assert_eq!(inner[index].nominal_training().end(), timestamp(train_end));
            assert_eq!(
                inner[index].training().end().value(),
                train_end * DAY_NS - 4 * HOUR_NS
            );
            assert_eq!(
                inner[index].validation().start().value(),
                train_end * DAY_NS + 4 * HOUR_NS
            );
            assert_eq!(inner[index].validation().end(), timestamp(train_end + 1));
        }
        // With stripped 7-day minimum, a larger horizon yields many rolling folds.
        let large = ValidationPlan::build(timestamp(0), 10).expect("rolling folds");
        assert_eq!(large.outer().len(), 4);
        assert_eq!(large.outer()[1].development().start(), timestamp(1));
        assert_eq!(large.outer()[3].test().end(), timestamp(10));
    }

    #[test]
    fn incomplete_history_is_a_hard_and_explicit_failure() {
        assert_eq!(ValidationPlan::minimum_complete_days(), 7);
        assert!(matches!(
            ValidationPlan::build(timestamp(0), 6),
            Err(ValidationError::InsufficientTrustworthyHistory {
                required_days: 7,
                available_days: 6
            })
        ));
        let report = RulesValidationReport::insufficient_history(provenance(), 6, Vec::new())
            .expect("canonical ineligible report");
        assert!(matches!(
            report.eligibility(),
            ResearchEligibility::Ineligible {
                reason: IneligibleReason::InsufficientTrustworthyHistory,
                ..
            }
        ));
        assert!(report.artifact().is_none());
        let bytes = report.canonical_json().expect("canonical report");
        let reopened = RulesValidationReport::from_canonical_json(&bytes)
            .expect("canonical ineligible report remains inspectable");
        assert_eq!(reopened.eligibility(), report.eligibility());
        assert_eq!(
            reopened.validate_active_pair(),
            Err(ValidationError::IneligibleReport)
        );
    }

    #[test]
    fn declared_grid_is_exactly_twelve_and_excludes_undeclared_values() {
        let grid = RuleGrid::declared();
        assert_eq!(grid.len(), 12);
        assert!(RuleGrid::contains(RuleConfig::new(
            EntryThreshold::P55,
            AtrFloor::OnePointTwoFive,
            TakeProfitMultiple::OnePointFiveR,
        )));
        assert_eq!(
            RuleSelection {
                threshold: "0.61".to_owned(),
                atr_floor: "1.25".to_owned(),
                take_profit: "1.5".to_owned(),
            }
            .to_config(),
            Err(ValidationError::UndeclaredRuleParameter)
        );
    }

    #[test]
    fn selection_uses_median_inner_expectancy_then_lower_turnover_only() {
        let candidates = RuleGrid::declared()
            .into_iter()
            .enumerate()
            .map(|(index, config)| {
                let pnl = if index == 0 || index == 1 {
                    dec!(4)
                } else {
                    dec!(1)
                };
                let turnover = if index == 0 {
                    dec!(8)
                } else if index == 1 {
                    dec!(4)
                } else {
                    dec!(1)
                };
                CandidateInnerOutcomes {
                    config,
                    outcomes: [
                        outcome(pnl, turnover, 2, index as u8 + 20),
                        outcome(pnl, turnover, 2, index as u8 + 30),
                        outcome(pnl, turnover, 2, index as u8 + 40),
                        outcome(pnl, turnover, 2, index as u8 + 50),
                    ],
                }
            })
            .collect::<Vec<_>>();
        let selection =
            select_from_inner(&candidates).expect("bounded grid selects deterministically");
        assert_eq!(selection.config(), RuleGrid::declared()[1]);
        assert_eq!(selection.median_net_expectancy(), dec!(2));
        assert_eq!(selection.turnover(), dec!(16));
    }

    #[test]
    fn artifact_round_trip_rejects_undeclared_values_and_preserves_immutable_digest() {
        let artifact = RulesArtifact::new(
            RuleSelection::from_config(RuleConfig::new(
                EntryThreshold::P65,
                AtrFloor::OnePointFive,
                TakeProfitMultiple::TwoR,
            )),
            &provenance(),
        )
        .expect("declared artifact");
        let bytes = artifact.canonical_json().expect("canonical artifact");
        let reopened = RulesArtifact::from_canonical_json(&bytes).expect("verified artifact");
        assert_eq!(artifact, reopened);
        assert_eq!(
            reopened.config().expect("allowed config"),
            RuleConfig::new(
                EntryThreshold::P65,
                AtrFloor::OnePointFive,
                TakeProfitMultiple::TwoR
            )
        );
        assert_eq!(
            reopened.definitions().trend_weights,
            [
                "0.30".to_owned(),
                "0.25".to_owned(),
                "0".to_owned(),
                "0.20".to_owned(),
                "0.10".to_owned(),
                "0.15".to_owned(),
            ]
        );
        let strategy = RulesStrategy::from_artifact(&reopened).expect("frozen strategy");
        assert_eq!(strategy.fingerprint(), reopened.strategy_fingerprint());
        let mut wire: serde_json::Value = serde_json::from_slice(&bytes).expect("artifact JSON");
        wire["selection"]["threshold"] = serde_json::Value::String("0.61".to_owned());
        assert!(
            RulesArtifact::from_canonical_json(&serde_json::to_vec(&wire).expect("JSON")).is_err()
        );

        let mut wire: serde_json::Value = serde_json::from_slice(&bytes).expect("artifact JSON");
        wire["definitions"]["trend_weights"][0] = serde_json::Value::String("0.31".to_owned());
        assert!(
            RulesArtifact::from_canonical_json(&serde_json::to_vec(&wire).expect("JSON")).is_err()
        );
    }

    #[test]
    fn gaps_fail_closed_and_canonical_gap_order_is_byte_stable() {
        let plan = ValidationPlan::build(timestamp(0), ValidationPlan::minimum_complete_days())
            .expect("stripped outer fold");
        let gap = super::ExcludedGap {
            range: TimeRange::new(timestamp(2), timestamp(3)).expect("gap"),
        };
        let report = RulesValidationReport::run(
            &plan,
            provenance(),
            vec![gap],
            &mut DeterministicEngineReplay::baseline(),
        )
        .expect("gap report");
        assert!(matches!(
            report.eligibility(),
            ResearchEligibility::Ineligible {
                reason: IneligibleReason::RequiredDataUnavailable,
                ..
            }
        ));
        assert!(report.artifact().is_none());

        let first = super::ExcludedGap {
            range: TimeRange::new(timestamp(500), timestamp(501)).expect("first gap"),
        };
        let second = super::ExcludedGap {
            range: TimeRange::new(timestamp(502), timestamp(503)).expect("second gap"),
        };
        let left =
            RulesValidationReport::insufficient_history(provenance(), 6, vec![second, first])
                .expect("normalized report");
        let right =
            RulesValidationReport::insufficient_history(provenance(), 6, vec![first, second])
                .expect("normalized report");
        assert_eq!(
            left.canonical_json().expect("left JSON"),
            right.canonical_json().expect("right JSON")
        );
        assert!(matches!(
            RulesValidationReport::insufficient_history(provenance(), 6, vec![first, first]),
            Err(ValidationError::InvalidExcludedGaps)
        ));
    }

    #[test]
    fn report_recomputes_selection_and_never_reuses_outer_test_for_tuning() {
        let mut baseline_replay = DeterministicEngineReplay::baseline();
        let baseline = complete_report(&mut baseline_replay);
        assert!(matches!(
            baseline.eligibility(),
            ResearchEligibility::Eligible {
                outer_test_folds: 1,
                closed_trades: 34,
            }
        ));
        let selected = baseline
            .folds
            .iter()
            .map(|fold| fold.selected.clone())
            .collect::<Vec<_>>();
        let artifact_digest = baseline
            .artifact()
            .expect("eligible artifact")
            .digest()
            .to_owned();

        let mut changed_test_replay = DeterministicEngineReplay {
            inner_bias: None,
            outer_test_delta: dec!(-100),
        };
        let changed_test = complete_report(&mut changed_test_replay);
        assert_eq!(
            changed_test
                .folds
                .iter()
                .map(|fold| fold.selected.clone())
                .collect::<Vec<_>>(),
            selected,
        );
        assert_eq!(
            changed_test.artifact().expect("eligible artifact").digest(),
            artifact_digest
        );
        assert_ne!(changed_test.digest(), baseline.digest());

        let mut changed_development_replay = DeterministicEngineReplay {
            inner_bias: Some(RuleGrid::declared()[0]),
            outer_test_delta: Decimal::ZERO,
        };
        let changed_development = complete_report(&mut changed_development_replay);
        assert_ne!(changed_development.digest(), baseline.digest());
        assert_ne!(
            changed_development
                .artifact()
                .expect("eligible artifact")
                .digest(),
            artifact_digest
        );

        let mut forged = baseline.clone();
        forged.folds[0].selected = RuleSelection::from_config(RuleGrid::declared()[0]);
        forged.digest = super::report_digest(&forged).expect("recomputed forged digest");
        assert!(
            RulesValidationReport::from_canonical_json(
                &forged.canonical_json().expect("forged report JSON")
            )
            .is_err()
        );
    }

    #[test]
    fn time_range_rejects_empty_and_reverse_ranges() {
        assert!(TimeRange::new(timestamp(1), timestamp(1)).is_err());
        assert!(TimeRange::new(timestamp(2), timestamp(1)).is_err());
    }
}
