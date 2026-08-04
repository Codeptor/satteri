//! Deterministic isolated-margin sizing and one-time sealed risk approvals.
//!
//! A strategy receives only [`CostQuote`].  Size, margin, leverage, and the
//! approval seal stay inside risk until the core engine consumes a matching
//! [`OrderIntent`] exactly once against the same immutable input snapshot.

use std::collections::BTreeMap;

use blake3::Hasher;
use rust_decimal::{
    Decimal, RoundingStrategy,
    prelude::{FromPrimitive, ToPrimitive},
};
use thiserror::Error;

use crate::domain::{Bps, DomainError, Leverage, Price, Quantity, Side, Usdc};
use crate::event::TimestampNs;
use crate::ledger::PositionSide;
use crate::risk::liquidation::{
    LiquidationError, LiquidationInput, LiquidationResult, MaintenanceTiers, calculate,
};
use crate::strategy::{
    CostAttribution, CostFeasibilityReason, CostQuote, CostQuoteError, CostQuoteFreshness,
    CostSourceDigests, OrderIntent, QuoteId, SignalCandidate,
};

const MAX_MARGIN_FRACTION: Decimal = Decimal::from_parts(25, 0, 0, false, 2);
const MIN_LIQUIDATION_STOP_MULTIPLE: Decimal = Decimal::from_parts(25, 0, 0, false, 1);
const MAX_TRADE_RISK_FRACTION: Decimal = Decimal::from_parts(5, 0, 0, false, 3);
const MAX_OUTSTANDING_APPROVALS: usize = 64;
const APPROVED_ENTRY_SLIPPAGE_BPS: Decimal = Decimal::from_parts(50, 0, 0, false, 0);
const RISK_QUOTE_DOMAIN: &str = "trench.risk-quote.v1";

/// Frozen venue limits used by the deterministic isolated sizing solver.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VenueConstraints {
    quantity_decimals: u8,
    minimum_notional: Usdc,
    maximum_notional: Usdc,
    maximum_executable_notional: Usdc,
    maximum_leverage: Leverage,
    maintenance_tiers: MaintenanceTiers,
}

impl VenueConstraints {
    /// Creates bounded point-in-time venue constraints.
    ///
    /// # Errors
    ///
    /// Rejects precision above Rust decimal capacity, zero minima, and empty
    /// notional/depth ranges before a candidate can enter the solver.
    pub fn new(
        quantity_decimals: u8,
        minimum_notional: Usdc,
        maximum_notional: Usdc,
        maximum_executable_notional: Usdc,
        maximum_leverage: Leverage,
        maintenance_tiers: MaintenanceTiers,
    ) -> Result<Self, RiskInputError> {
        if quantity_decimals > 28 {
            return Err(RiskInputError::UnsupportedQuantityPrecision { quantity_decimals });
        }
        if minimum_notional.value().is_zero() {
            return Err(RiskInputError::ZeroMinimumNotional);
        }
        if maximum_notional < minimum_notional {
            return Err(RiskInputError::InvalidNotionalRange);
        }
        if maximum_executable_notional.value().is_zero() {
            return Err(RiskInputError::ZeroExecutableDepth);
        }
        Ok(Self {
            quantity_decimals,
            minimum_notional,
            maximum_notional,
            maximum_executable_notional,
            maximum_leverage,
            maintenance_tiers,
        })
    }

    /// Returns venue quantity precision used for floor rounding.
    #[must_use]
    pub const fn quantity_decimals(&self) -> u8 {
        self.quantity_decimals
    }

    /// Returns the venue minimum tradeable quote notional.
    #[must_use]
    pub const fn minimum_notional(&self) -> Usdc {
        self.minimum_notional
    }

    /// Returns the point-in-time asset notional cap.
    #[must_use]
    pub const fn maximum_notional(&self) -> Usdc {
        self.maximum_notional
    }

    /// Returns the executable visible-depth notional cap.
    #[must_use]
    pub const fn maximum_executable_notional(&self) -> Usdc {
        self.maximum_executable_notional
    }

    /// Returns the point-in-time asset leverage maximum.
    #[must_use]
    pub const fn maximum_leverage(&self) -> Leverage {
        self.maximum_leverage
    }

    /// Returns the frozen point-in-time maintenance tiers.
    #[must_use]
    pub const fn maintenance_tiers(&self) -> &MaintenanceTiers {
        &self.maintenance_tiers
    }
}

/// One bounded notional band of observed current and trailing-p99 impact.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ImpactBand {
    upper_notional: Option<Usdc>,
    current_fraction: Decimal,
    trailing_p99_fraction: Decimal,
}

impl ImpactBand {
    /// Creates one right-closed impact band, or the final open-ended band.
    pub fn new(
        upper_notional: Option<Usdc>,
        current_fraction: Decimal,
        trailing_p99_fraction: Decimal,
    ) -> Result<Self, RiskInputError> {
        for (field, value) in [
            ("current impact", current_fraction),
            ("trailing p99 impact", trailing_p99_fraction),
        ] {
            if value < Decimal::ZERO {
                return Err(RiskInputError::NegativeFraction { field, value });
            }
        }
        let stressed = current_fraction
            .checked_mul(Decimal::TWO)
            .map(|doubled| doubled.max(trailing_p99_fraction))
            .ok_or(RiskInputError::ImpactArithmetic)?;
        if stressed >= Decimal::ONE {
            return Err(RiskInputError::UnsafeImpactFraction { stressed });
        }
        Ok(Self {
            upper_notional,
            current_fraction,
            trailing_p99_fraction,
        })
    }

    fn contains(self, notional: Usdc) -> bool {
        self.upper_notional.is_none_or(|upper| notional <= upper)
    }

    fn stressed_fraction(self) -> Result<Decimal, RiskError> {
        self.current_fraction
            .checked_mul(Decimal::TWO)
            .map(|doubled| doubled.max(self.trailing_p99_fraction))
            .ok_or(RiskError::Arithmetic {
                operation: "banded stressed impact",
            })
    }
}

/// Complete ascending impact ladder, frozen with the executable book snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImpactCurve(Vec<ImpactBand>);

impl ImpactCurve {
    /// Creates a nonempty ascending ladder with exactly one final open band.
    pub fn new(bands: Vec<ImpactBand>) -> Result<Self, RiskInputError> {
        let Some(last) = bands.last() else {
            return Err(RiskInputError::EmptyImpactCurve);
        };
        if last.upper_notional.is_some() {
            return Err(RiskInputError::ImpactCurveMustBeOpenEnded);
        }
        for pair in bands.windows(2) {
            let Some(left) = pair[0].upper_notional else {
                return Err(RiskInputError::NonterminalOpenImpactBand);
            };
            let Some(right) = pair[1].upper_notional else {
                continue;
            };
            if right <= left {
                return Err(RiskInputError::NonAscendingImpactBands);
            }
        }
        let mut previous_stressed = None;
        for band in &bands {
            let stressed = band
                .current_fraction
                .checked_mul(Decimal::TWO)
                .map(|doubled| doubled.max(band.trailing_p99_fraction))
                .ok_or(RiskInputError::ImpactArithmetic)?;
            if previous_stressed.is_some_and(|previous| stressed < previous) {
                return Err(RiskInputError::DecreasingImpactBands);
            }
            previous_stressed = Some(stressed);
        }
        Ok(Self(bands))
    }

    fn stressed_fraction(&self, notional: Usdc) -> Result<Decimal, RiskError> {
        self.0
            .iter()
            .copied()
            .find(|band| band.contains(notional))
            .ok_or(RiskError::Arithmetic {
                operation: "impact ladder lookup",
            })?
            .stressed_fraction()
    }
}

/// Conservative costs and scheduled-funding reserve bound to a frozen impact ladder.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConservativeCosts {
    entry_fee_fraction: Decimal,
    exit_fee_fraction: Decimal,
    impact_curve: ImpactCurve,
    current_funding_fraction: Decimal,
    trailing_p99_funding_fraction: Decimal,
    funding_timestamps: u16,
}

impl ConservativeCosts {
    /// Creates checked nonnegative fee/funding inputs and a complete impact ladder.
    pub fn new(
        entry_fee_fraction: Decimal,
        exit_fee_fraction: Decimal,
        impact_curve: ImpactCurve,
        current_funding_fraction: Decimal,
        trailing_p99_funding_fraction: Decimal,
        funding_timestamps: u16,
    ) -> Result<Self, RiskInputError> {
        for (field, value) in [
            ("entry fee", entry_fee_fraction),
            ("exit fee", exit_fee_fraction),
            ("current funding", current_funding_fraction),
            ("trailing p99 funding", trailing_p99_funding_fraction),
        ] {
            if value < Decimal::ZERO {
                return Err(RiskInputError::NegativeFraction { field, value });
            }
        }
        Ok(Self {
            entry_fee_fraction,
            exit_fee_fraction,
            impact_curve,
            current_funding_fraction,
            trailing_p99_funding_fraction,
            funding_timestamps,
        })
    }

    /// Returns the fixed entry-fee fraction.
    #[must_use]
    pub const fn entry_fee_fraction(&self) -> Decimal {
        self.entry_fee_fraction
    }

    /// Returns the fixed exit-fee fraction.
    #[must_use]
    pub const fn exit_fee_fraction(&self) -> Decimal {
        self.exit_fee_fraction
    }

    fn stressed_impact_fraction(&self, notional: Usdc) -> Result<Decimal, RiskError> {
        self.impact_curve.stressed_fraction(notional)
    }

    /// Returns the full-horizon adverse funding reserve fraction.
    pub fn funding_reserve_fraction(&self) -> Result<Decimal, RiskError> {
        self.current_funding_fraction
            .max(self.trailing_p99_funding_fraction)
            .checked_mul(Decimal::from(self.funding_timestamps))
            .ok_or(RiskError::Arithmetic {
                operation: "funding reserve",
            })
    }
}

/// Point-in-time scalar limits for one immutable ledger snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RiskLimits {
    risk_budget: Usdc,
    margin_cap_fraction: Decimal,
    liquidation_stop_multiple: Decimal,
}

impl RiskLimits {
    /// Creates checked, nonzero risk limits.
    pub fn new(
        risk_budget: Usdc,
        margin_cap_fraction: Decimal,
        liquidation_stop_multiple: Decimal,
    ) -> Result<Self, RiskInputError> {
        if risk_budget.value().is_zero() {
            return Err(RiskInputError::ZeroRiskBudget);
        }
        if !(Decimal::ZERO..=MAX_MARGIN_FRACTION).contains(&margin_cap_fraction)
            || margin_cap_fraction.is_zero()
        {
            return Err(RiskInputError::InvalidMarginCap {
                margin_cap_fraction,
            });
        }
        if liquidation_stop_multiple < MIN_LIQUIDATION_STOP_MULTIPLE {
            return Err(RiskInputError::UnsafeLiquidationMultiple {
                liquidation_stop_multiple,
            });
        }
        Ok(Self {
            risk_budget,
            margin_cap_fraction,
            liquidation_stop_multiple,
        })
    }

    /// Returns the per-entry loss budget.
    #[must_use]
    pub const fn risk_budget(self) -> Usdc {
        self.risk_budget
    }

    /// Returns the cap applied to isolated margin plus reserved entry fee.
    #[must_use]
    pub const fn margin_cap_fraction(self) -> Decimal {
        self.margin_cap_fraction
    }

    /// Returns the required liquidation-to-stop distance multiple.
    #[must_use]
    pub const fn liquidation_stop_multiple(self) -> Decimal {
        self.liquidation_stop_multiple
    }
}

/// Immutable, digested state supplied to one risk quote request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RiskSnapshot {
    as_of_time: TimestampNs,
    valid_through: TimestampNs,
    equity: Usdc,
    ledger_digest: String,
    book_digest: String,
    universe_digest: String,
    config_digest: String,
    event_digest: String,
}

impl RiskSnapshot {
    /// Creates a complete point-in-time risk snapshot with no optional bindings.
    #[expect(
        clippy::too_many_arguments,
        reason = "every digest is a required approval binding"
    )]
    pub fn new(
        as_of_time: TimestampNs,
        valid_through: TimestampNs,
        equity: Usdc,
        ledger_digest: impl Into<String>,
        book_digest: impl Into<String>,
        universe_digest: impl Into<String>,
        config_digest: impl Into<String>,
        event_digest: impl Into<String>,
    ) -> Result<Self, RiskInputError> {
        if valid_through < as_of_time {
            return Err(RiskInputError::BackwardValidity {
                as_of_time,
                valid_through,
            });
        }
        if equity.value().is_zero() {
            return Err(RiskInputError::ZeroEquity);
        }
        let snapshot = Self {
            as_of_time,
            valid_through,
            equity,
            ledger_digest: ledger_digest.into(),
            book_digest: book_digest.into(),
            universe_digest: universe_digest.into(),
            config_digest: config_digest.into(),
            event_digest: event_digest.into(),
        };
        for (field, digest) in [
            ("ledger", snapshot.ledger_digest.as_str()),
            ("book", snapshot.book_digest.as_str()),
            ("universe", snapshot.universe_digest.as_str()),
            ("config", snapshot.config_digest.as_str()),
            ("event", snapshot.event_digest.as_str()),
        ] {
            if !is_blake3_digest(digest) {
                return Err(RiskInputError::InvalidDigest { field });
            }
        }
        Ok(snapshot)
    }

    /// Returns the explicit quote boundary.
    #[must_use]
    pub const fn as_of_time(&self) -> TimestampNs {
        self.as_of_time
    }

    /// Returns the inclusive explicit expiry boundary.
    #[must_use]
    pub const fn valid_through(&self) -> TimestampNs {
        self.valid_through
    }

    /// Returns snapshot synthetic equity.
    #[must_use]
    pub const fn equity(&self) -> Usdc {
        self.equity
    }

    /// Returns the executable-book digest.
    #[must_use]
    pub fn book_digest(&self) -> &str {
        &self.book_digest
    }

    /// Returns the exact isolated-ledger digest bound to this quote request.
    #[must_use]
    pub fn ledger_digest(&self) -> &str {
        &self.ledger_digest
    }

    /// Returns the exact active-universe digest bound to this quote request.
    #[must_use]
    pub fn universe_digest(&self) -> &str {
        &self.universe_digest
    }

    /// Returns the immutable run/configuration digest bound to this quote request.
    #[must_use]
    pub fn config_digest(&self) -> &str {
        &self.config_digest
    }

    /// Returns the exact causal event digest bound to this quote request.
    #[must_use]
    pub fn event_digest(&self) -> &str {
        &self.event_digest
    }

    fn digest(&self) -> String {
        let mut hasher = Hasher::new_derive_key(RISK_QUOTE_DOMAIN);
        for value in [
            self.as_of_time.value().to_string(),
            self.valid_through.value().to_string(),
            self.equity.value().to_string(),
            self.ledger_digest.clone(),
            self.book_digest.clone(),
            self.universe_digest.clone(),
            self.config_digest.clone(),
            self.event_digest.clone(),
        ] {
            let length = value.len() as u64;
            hasher.update(&length.to_be_bytes());
            hasher.update(value.as_bytes());
        }
        hasher.finalize().to_hex().to_string()
    }
}

fn is_blake3_digest(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

/// Fully frozen inputs for one risk quote calculation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RiskRequest {
    snapshot: RiskSnapshot,
    constraints: VenueConstraints,
    costs: ConservativeCosts,
    limits: RiskLimits,
}

/// Frozen sizing policy owned by an engine run rather than an individual event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RiskPolicy {
    book_digest: String,
    constraints: VenueConstraints,
    costs: ConservativeCosts,
    limits: RiskLimits,
}

impl RiskPolicy {
    fn from_request(request: RiskRequest) -> Self {
        Self {
            book_digest: request.snapshot.book_digest,
            constraints: request.constraints,
            costs: request.costs,
            limits: request.limits,
        }
    }

    fn request(&self, snapshot: RiskSnapshot) -> RiskRequest {
        RiskRequest {
            snapshot,
            constraints: self.constraints.clone(),
            costs: self.costs.clone(),
            limits: self.limits,
        }
    }

    /// Returns the deterministic commitment for the frozen venue constraints,
    /// costs, limits, and executable-book binding.
    #[must_use]
    pub fn commitment_digest(&self) -> String {
        let mut hasher = Hasher::new_derive_key("trench.risk-policy.v1");
        for value in [
            self.book_digest.clone(),
            self.constraints.quantity_decimals.to_string(),
            self.constraints.minimum_notional.value().to_string(),
            self.constraints.maximum_notional.value().to_string(),
            self.constraints
                .maximum_executable_notional
                .value()
                .to_string(),
            self.constraints.maximum_leverage.value().to_string(),
            self.costs.entry_fee_fraction.to_string(),
            self.costs.exit_fee_fraction.to_string(),
            self.costs.current_funding_fraction.to_string(),
            self.costs.trailing_p99_funding_fraction.to_string(),
            self.costs.funding_timestamps.to_string(),
            self.limits.risk_budget.value().to_string(),
            self.limits.margin_cap_fraction.to_string(),
            self.limits.liquidation_stop_multiple.to_string(),
        ] {
            hasher.update(&(value.len() as u64).to_be_bytes());
            hasher.update(value.as_bytes());
        }
        for tier in self.constraints.maintenance_tiers.as_slice() {
            for value in [
                tier.lower_notional().value().to_string(),
                tier.upper_notional()
                    .map_or_else(|| "none".to_owned(), |upper| upper.value().to_string()),
                tier.maintenance_rate().to_string(),
                tier.maintenance_deduction().value().to_string(),
            ] {
                hasher.update(&(value.len() as u64).to_be_bytes());
                hasher.update(value.as_bytes());
            }
        }
        for band in &self.costs.impact_curve.0 {
            for value in [
                band.upper_notional
                    .map_or_else(|| "none".to_owned(), |upper| upper.value().to_string()),
                band.current_fraction.to_string(),
                band.trailing_p99_fraction.to_string(),
            ] {
                hasher.update(&(value.len() as u64).to_be_bytes());
                hasher.update(value.as_bytes());
            }
        }
        hasher.finalize().to_hex().to_string()
    }

    /// Returns whether this frozen policy was derived from the exact immutable
    /// full-depth book supplied at the engine boundary.
    ///
    /// Research adapters use this only to fail closed before a replay starts;
    /// the engine repeats the check before every sealed quote.
    #[must_use]
    pub fn matches_book_digest(&self, book_digest: &str) -> bool {
        self.book_digest == book_digest
    }
}

impl RiskRequest {
    /// Creates a deterministic risk request from explicit point-in-time inputs.
    #[must_use]
    pub const fn new(
        snapshot: RiskSnapshot,
        constraints: VenueConstraints,
        costs: ConservativeCosts,
        limits: RiskLimits,
    ) -> Self {
        Self {
            snapshot,
            constraints,
            costs,
            limits,
        }
    }

    /// Returns the immutable state bindings used by the request.
    #[must_use]
    pub const fn snapshot(&self) -> &RiskSnapshot {
        &self.snapshot
    }

    /// Freezes this request's sizing inputs for the lifetime of one engine run.
    #[must_use]
    pub fn into_policy(self) -> RiskPolicy {
        RiskPolicy::from_request(self)
    }

    pub(crate) fn from_policy(snapshot: RiskSnapshot, policy: &RiskPolicy) -> Self {
        policy.request(snapshot)
    }

    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn with_snapshot(&self, snapshot: RiskSnapshot) -> Self {
        Self {
            snapshot,
            constraints: self.constraints.clone(),
            costs: self.costs.clone(),
            limits: self.limits,
        }
    }
}

/// One fixed-leverage survivability counterfactual retained for audit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LeverageCounterfactual {
    leverage: Leverage,
    feasible: bool,
    isolated_margin: Option<Usdc>,
    liquidation: Option<LiquidationResult>,
}

/// Nonpublic size, margin, and seal released only to the pure core engine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ApprovedOrder {
    candidate: SignalCandidate,
    quantity: Quantity,
    entry_notional: Usdc,
    leverage: Leverage,
    isolated_margin: Usdc,
    planned_loss: Usdc,
    liquidation: LiquidationResult,
    maintenance_tiers: MaintenanceTiers,
    entry_slippage_limit: Bps,
    snapshot_digest: String,
    freshness: CostQuoteFreshness,
    counterfactuals: [LeverageCounterfactual; 4],
}

#[allow(
    dead_code,
    reason = "Task 13's pure engine is the sole production consumer of sealed approval data"
)]
impl ApprovedOrder {
    /// Returns the immutable candidate sealed by this risk approval.
    #[must_use]
    pub(crate) const fn candidate(&self) -> &SignalCandidate {
        &self.candidate
    }

    /// Returns the sealed quantity to the core execution transition only.
    #[must_use]
    pub(crate) const fn quantity(&self) -> Quantity {
        self.quantity
    }

    /// Returns the sealed notional to the core execution transition only.
    #[must_use]
    pub(crate) const fn entry_notional(&self) -> Usdc {
        self.entry_notional
    }

    /// Returns the selected lowest safe leverage.
    #[must_use]
    pub(crate) const fn leverage(&self) -> Leverage {
        self.leverage
    }

    /// Returns isolated margin including the reserved entry cost.
    #[must_use]
    pub(crate) const fn isolated_margin(&self) -> Usdc {
        self.isolated_margin
    }

    /// Returns the bounded modeled entry-to-stressed-stop loss.
    #[must_use]
    pub(crate) const fn planned_loss(&self) -> Usdc {
        self.planned_loss
    }

    /// Returns the tier-valid pre-entry liquidation threshold.
    #[must_use]
    pub(crate) const fn liquidation(&self) -> LiquidationResult {
        self.liquidation
    }

    /// Returns the frozen complete maintenance table for actual-fill revaluation.
    #[must_use]
    pub(crate) const fn maintenance_tiers(&self) -> &MaintenanceTiers {
        &self.maintenance_tiers
    }

    /// Returns the sealed maximum adverse entry movement from reference price.
    #[must_use]
    pub(crate) const fn entry_slippage_limit(&self) -> Bps {
        self.entry_slippage_limit
    }

    /// Returns the four fixed leverage counterfactuals for the journal.
    #[must_use]
    pub(crate) const fn counterfactuals(&self) -> &[LeverageCounterfactual; 4] {
        &self.counterfactuals
    }
}

/// A public result exposing only cost evidence and machine-readable rejections.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RiskQuote {
    cost_quote: CostQuote,
    rejections: Vec<RiskRejection>,
}

impl RiskQuote {
    /// Returns the public strategy-visible cost evidence.
    #[must_use]
    pub const fn cost_quote(&self) -> &CostQuote {
        &self.cost_quote
    }

    /// Returns exhaustive deterministic risk rejection reasons.
    #[must_use]
    pub fn rejections(&self) -> &[RiskRejection] {
        &self.rejections
    }

    /// Returns whether a sealed order exists behind this cost quote.
    #[must_use]
    pub fn is_approved(&self) -> bool {
        self.rejections.is_empty() && self.cost_quote.is_feasible()
    }
}

/// Deterministic rejection emitted by the authoritative risk boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RiskRejection {
    /// Candidate/snapshot boundary timestamps do not match.
    CandidateTimeMismatch,
    /// Candidate and current universe provenance differ.
    UniverseDigestMismatch,
    /// No venue-rounded size met the complete planned-loss budget.
    RiskBudget,
    /// Venue minimum, maximum, precision, or executable depth prevented entry.
    VenueConstraint,
    /// No integer leverage met the isolated-margin cap.
    MarginCap,
    /// No leverage produced sufficient liquidation distance from the stop.
    LiquidationDistance,
    /// The point-in-time maintenance table had no valid solution.
    Liquidation,
    /// The candidate's stressed stop could not remain positive and executable.
    PriceBand,
    /// The bounded approval cache already contains unconsumed current quotes.
    ApprovalCapacity,
}

/// Checked invalid input before quoting begins.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RiskInputError {
    /// Venue quantity precision exceeded fixed decimal capacity.
    #[error("venue quantity precision {quantity_decimals} exceeds decimal capacity")]
    UnsupportedQuantityPrecision { quantity_decimals: u8 },
    /// A venue cannot trade a zero quote minimum.
    #[error("venue minimum notional must be positive")]
    ZeroMinimumNotional,
    /// Asset maximum notional fell below the minimum.
    #[error("venue maximum notional must not be below the minimum")]
    InvalidNotionalRange,
    /// No visible depth cap was supplied.
    #[error("executable depth cap must be positive")]
    ZeroExecutableDepth,
    /// A cost fraction was negative.
    #[error("{field} fraction must be nonnegative, got {value}")]
    NegativeFraction { field: &'static str, value: Decimal },
    /// A quote cannot start with zero risk budget.
    #[error("risk budget must be positive")]
    ZeroRiskBudget,
    /// Margin cap must be in the approved open `(0, 25%]` interval.
    #[error("margin cap must be in (0, 0.25], got {margin_cap_fraction}")]
    InvalidMarginCap { margin_cap_fraction: Decimal },
    /// The liquidation safety multiplier weakened the approved minimum.
    #[error("liquidation-stop multiple must be at least 2.5, got {liquidation_stop_multiple}")]
    UnsafeLiquidationMultiple { liquidation_stop_multiple: Decimal },
    /// Validity cannot run backward.
    #[error("risk snapshot valid-through {valid_through} precedes {as_of_time}")]
    BackwardValidity {
        /// Construction boundary.
        as_of_time: TimestampNs,
        /// Rejected earlier expiry.
        valid_through: TimestampNs,
    },
    /// Zero equity cannot support an isolated position.
    #[error("risk snapshot equity must be positive")]
    ZeroEquity,
    /// Every approved-order binding digest is mandatory.
    #[error("risk snapshot requires nonempty ledger, book, universe, config, and event digests")]
    MissingDigest,
    /// A source digest must be a fixed-width hexadecimal BLAKE3 value.
    #[error("{field} digest must be a 64-character hexadecimal BLAKE3 digest")]
    InvalidDigest { field: &'static str },
    /// Every impact ladder needs at least one band.
    #[error("impact curve must contain at least one band")]
    EmptyImpactCurve,
    /// The final impact band must cover every greater notional.
    #[error("the final impact band must be open-ended")]
    ImpactCurveMustBeOpenEnded,
    /// An open-ended impact band may only appear last.
    #[error("only the final impact band may be open-ended")]
    NonterminalOpenImpactBand,
    /// Finite impact bands must increase strictly by notional.
    #[error("impact bands must have strictly increasing upper notionals")]
    NonAscendingImpactBands,
    /// Stressed impact must not fall as requested notional increases.
    #[error("stressed impact fractions must not decrease across notional bands")]
    DecreasingImpactBands,
    /// Exact impact multiplication could not be represented.
    #[error("impact arithmetic could not be represented")]
    ImpactArithmetic,
    /// An adverse impact cannot consume a full price or more.
    #[error("stressed impact fraction must be below one, got {stressed}")]
    UnsafeImpactFraction { stressed: Decimal },
}

/// Risk quote or one-time approval consumption failed.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RiskError {
    /// One checked decimal operation could not be represented.
    #[error("risk arithmetic failed while calculating {operation}")]
    Arithmetic { operation: &'static str },
    /// A candidate reached risk with a stale or mismatched snapshot boundary.
    #[error("candidate does not match the immutable risk snapshot")]
    CandidateSnapshotMismatch,
    /// The intent did not name a currently sealed quote.
    #[error("risk quote is missing, stale, or was already consumed")]
    UnknownOrConsumedQuote,
    /// A post-quote snapshot changed before consumption.
    #[error("risk quote input snapshot changed before consumption")]
    SnapshotChanged,
    /// The intent candidate did not match the sealed candidate.
    #[error("order intent candidate does not match the sealed risk approval")]
    CandidateChanged,
    /// A checked domain unit could not be created.
    #[error(transparent)]
    Domain(#[from] DomainError),
    /// Liquidation could not be solved from frozen tiers.
    #[error(transparent)]
    Liquidation(#[from] LiquidationError),
    /// The public cost quote could not preserve complete attribution.
    #[error(transparent)]
    CostQuote(#[from] CostQuoteError),
}

/// Sole authority that mints and consumes sealed paper orders.
#[derive(Debug, Default)]
pub struct RiskEngine {
    next_quote: u64,
    approvals: BTreeMap<QuoteId, ApprovedOrder>,
}

impl RiskEngine {
    /// Quotes one un-sized candidate against an immutable point-in-time request.
    ///
    /// A rejected quote still contains complete public fractional cost evidence
    /// and explicit reasons, but never an approved size or opaque order data.
    pub fn quote_candidate(
        &mut self,
        candidate: &SignalCandidate,
        request: &RiskRequest,
    ) -> Result<RiskQuote, RiskError> {
        let snapshot = &request.snapshot;
        self.prune_expired(snapshot.as_of_time);
        let source_digest = snapshot.digest();
        let quote_id = self.next_quote_id(&source_digest)?;
        let freshness = CostQuoteFreshness::new(snapshot.as_of_time, snapshot.valid_through)?;
        let source_digests = CostSourceDigests::new(snapshot.book_digest.clone(), source_digest);
        let (attributions, total_cost) =
            cost_attributions(&request.costs, request.constraints.minimum_notional)?;

        if self.approvals.len() >= MAX_OUTSTANDING_APPROVALS {
            return self.rejected_quote(
                quote_id,
                candidate,
                freshness,
                source_digests,
                total_cost,
                attributions,
                Vec::from([RiskRejection::ApprovalCapacity]),
            );
        }

        let mut rejections = Vec::new();
        if candidate.decision_time() != snapshot.as_of_time {
            rejections.push(RiskRejection::CandidateTimeMismatch);
        }
        if candidate.universe_digest() != snapshot.universe_digest {
            rejections.push(RiskRejection::UniverseDigestMismatch);
        }
        if !rejections.is_empty() {
            return self.rejected_quote(
                quote_id,
                candidate,
                freshness,
                source_digests,
                total_cost,
                attributions,
                rejections,
            );
        }

        let some_notional = find_largest_safe_notional(candidate, request)?;
        let Some(entry_notional) = some_notional else {
            let reason = if maximum_quote_notional(request)? < request.constraints.minimum_notional
            {
                RiskRejection::VenueConstraint
            } else {
                RiskRejection::RiskBudget
            };
            return self.rejected_quote(
                quote_id,
                candidate,
                freshness,
                source_digests,
                total_cost,
                attributions,
                Vec::from([reason]),
            );
        };
        if entry_notional < request.constraints.minimum_notional {
            return self.rejected_quote(
                quote_id,
                candidate,
                freshness,
                source_digests,
                total_cost,
                attributions,
                Vec::from([RiskRejection::VenueConstraint]),
            );
        }

        let quantity = rounded_quantity(
            entry_notional,
            candidate.reference_entry(),
            request.constraints.quantity_decimals,
        )?;
        if quantity.value().is_zero() {
            return self.rejected_quote(
                quote_id,
                candidate,
                freshness,
                source_digests,
                total_cost,
                attributions,
                Vec::from([RiskRejection::VenueConstraint]),
            );
        }
        let rounded_notional = Usdc::new(
            quantity
                .value()
                .checked_mul(candidate.reference_entry().value())
                .ok_or(RiskError::Arithmetic {
                    operation: "rounded entry notional",
                })?,
        )?;
        if rounded_notional < request.constraints.minimum_notional {
            return self.rejected_quote(
                quote_id,
                candidate,
                freshness,
                source_digests,
                total_cost,
                attributions,
                Vec::from([RiskRejection::VenueConstraint]),
            );
        }
        let planned_loss = planned_loss(candidate, rounded_notional, &request.costs)?;
        if planned_loss > effective_risk_budget(request)? {
            return self.rejected_quote(
                quote_id,
                candidate,
                freshness,
                source_digests,
                total_cost,
                attributions,
                Vec::from([RiskRejection::RiskBudget]),
            );
        }

        let counterfactuals = counterfactuals(candidate, rounded_notional, quantity, request)?;
        let Some((leverage, isolated_margin, liquidation)) =
            select_lowest_safe_leverage(candidate, rounded_notional, quantity, request)?
        else {
            let reason = infer_leverage_rejection(&counterfactuals, request)?;
            return self.rejected_quote(
                quote_id,
                candidate,
                freshness,
                source_digests,
                total_cost,
                attributions,
                Vec::from([reason]),
            );
        };

        let approved = ApprovedOrder {
            candidate: candidate.clone(),
            quantity,
            entry_notional: rounded_notional,
            leverage,
            isolated_margin,
            planned_loss,
            liquidation,
            maintenance_tiers: request.constraints.maintenance_tiers.clone(),
            entry_slippage_limit: approved_entry_slippage_limit()?,
            snapshot_digest: snapshot.digest(),
            freshness,
            counterfactuals,
        };
        let (attributions, total_cost) = cost_attributions(&request.costs, rounded_notional)?;
        let quote = CostQuote::new(
            quote_id.clone(),
            candidate.market().clone(),
            candidate.digest(),
            freshness,
            source_digests,
            total_cost,
            attributions,
            Vec::new(),
        )?;
        self.approvals.insert(quote_id, approved);
        Ok(RiskQuote {
            cost_quote: quote,
            rejections: Vec::new(),
        })
    }

    /// Consumes exactly one still-fresh quote after strategy cost acceptance.
    ///
    /// The caller must supply its current immutable state binding.  A changed
    /// ledger, book, universe, config, or event digest invalidates the seal.
    #[allow(
        dead_code,
        reason = "Task 13's pure engine is the only production quote consumer"
    )]
    pub(crate) fn consume_quote(
        &mut self,
        intent: &OrderIntent,
        current_snapshot: &RiskSnapshot,
        at: TimestampNs,
    ) -> Result<ApprovedOrder, RiskError> {
        let quote_id = intent.quote_id();
        let approval = self
            .approvals
            .get(quote_id)
            .ok_or(RiskError::UnknownOrConsumedQuote)?;
        if approval.candidate.digest() != intent.candidate().digest() {
            return Err(RiskError::CandidateChanged);
        }
        if approval.snapshot_digest != current_snapshot.digest() {
            return Err(RiskError::SnapshotChanged);
        }
        if !approval.freshness.is_fresh_at(at) {
            self.approvals.remove(quote_id);
            return Err(RiskError::UnknownOrConsumedQuote);
        }
        self.approvals
            .remove(quote_id)
            .ok_or(RiskError::UnknownOrConsumedQuote)
    }

    /// Removes an unselected approval before it can become stale or consume
    /// bounded approval-cache capacity. Only the core engine owns this path.
    pub(crate) fn discard_quote(&mut self, quote_id: &QuoteId) -> bool {
        self.approvals.remove(quote_id).is_some()
    }

    /// Returns the sealed approvals still retained by the private risk boundary.
    ///
    /// This remains crate-private; the engine uses it solely to fence a
    /// point-in-time policy replacement between complete transitions.
    pub(crate) fn outstanding_approvals(&self) -> usize {
        self.approvals.len()
    }

    fn next_quote_id(&mut self, source_digest: &str) -> Result<QuoteId, RiskError> {
        let index = self.next_quote;
        self.next_quote = self
            .next_quote
            .checked_add(1)
            .ok_or(RiskError::Arithmetic {
                operation: "quote sequence",
            })?;
        QuoteId::new(format!("risk-{index}-{source_digest}")).map_err(RiskError::from)
    }

    fn prune_expired(&mut self, at: TimestampNs) {
        self.approvals
            .retain(|_, approval| approval.freshness.valid_through() >= at);
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "preserves the exact public cost boundary for every rejection"
    )]
    fn rejected_quote(
        &self,
        quote_id: QuoteId,
        candidate: &SignalCandidate,
        freshness: CostQuoteFreshness,
        source_digests: CostSourceDigests,
        total_cost: Decimal,
        attributions: Vec<CostAttribution>,
        rejections: Vec<RiskRejection>,
    ) -> Result<RiskQuote, RiskError> {
        let infeasibility_reasons = rejections
            .iter()
            .copied()
            .map(cost_feasibility_reason)
            .collect();
        Ok(RiskQuote {
            cost_quote: CostQuote::new(
                quote_id,
                candidate.market().clone(),
                candidate.digest(),
                freshness,
                source_digests,
                total_cost,
                attributions,
                infeasibility_reasons,
            )?,
            rejections,
        })
    }
}

fn approved_entry_slippage_limit() -> Result<Bps, RiskError> {
    Ok(Bps::new(APPROVED_ENTRY_SLIPPAGE_BPS)?)
}

fn cost_feasibility_reason(rejection: RiskRejection) -> CostFeasibilityReason {
    match rejection {
        RiskRejection::RiskBudget => CostFeasibilityReason::RiskBlocked,
        RiskRejection::VenueConstraint | RiskRejection::MarginCap | RiskRejection::Liquidation => {
            CostFeasibilityReason::VenueConstraint
        }
        RiskRejection::LiquidationDistance | RiskRejection::PriceBand => {
            CostFeasibilityReason::PriceBand
        }
        RiskRejection::CandidateTimeMismatch
        | RiskRejection::UniverseDigestMismatch
        | RiskRejection::ApprovalCapacity => CostFeasibilityReason::RiskBlocked,
    }
}

fn cost_attributions(
    costs: &ConservativeCosts,
    notional: Usdc,
) -> Result<(Vec<CostAttribution>, Decimal), RiskError> {
    let funding = costs.funding_reserve_fraction()?;
    let impact = costs.stressed_impact_fraction(notional)?;
    let attributions = Vec::from([
        CostAttribution::entry_fee(costs.entry_fee_fraction),
        CostAttribution::exit_fee(costs.exit_fee_fraction),
        CostAttribution::FundingReserve(funding),
        CostAttribution::MarketImpact(impact),
    ]);
    let total = attributions
        .iter()
        .try_fold(Decimal::ZERO, |total, attribution| {
            total
                .checked_add(attribution.fraction())
                .ok_or(RiskError::Arithmetic {
                    operation: "public cost attribution total",
                })
        })?;
    Ok((attributions, total))
}

fn find_largest_safe_notional(
    candidate: &SignalCandidate,
    request: &RiskRequest,
) -> Result<Option<Usdc>, RiskError> {
    let cap = maximum_quote_notional(request)?;
    let unit_notional = quantity_quantum_notional(
        candidate.reference_entry(),
        request.constraints.quantity_decimals,
    )?;
    let max_units = floor_units(cap, unit_notional)?;
    if max_units == 0 {
        return Ok(None);
    }
    let mut low = 0_u128;
    let mut high = max_units;
    while low < high {
        let upper_mid = low
            .checked_add(high)
            .and_then(|sum| sum.checked_add(1))
            .and_then(|sum| sum.checked_div(2))
            .ok_or(RiskError::Arithmetic {
                operation: "notional bisection midpoint",
            })?;
        let notional = units_to_notional(upper_mid, unit_notional)?;
        if planned_loss(candidate, notional, &request.costs)? <= effective_risk_budget(request)? {
            low = upper_mid;
        } else {
            high = upper_mid.checked_sub(1).ok_or(RiskError::Arithmetic {
                operation: "notional bisection upper bound",
            })?;
        }
    }
    (low > 0)
        .then(|| units_to_notional(low, unit_notional))
        .transpose()
}

fn maximum_quote_notional(request: &RiskRequest) -> Result<Usdc, RiskError> {
    Ok(request
        .constraints
        .maximum_notional
        .min(request.constraints.maximum_executable_notional)
        .min(maximum_margin_notional(request)?))
}

fn maximum_margin_notional(request: &RiskRequest) -> Result<Usdc, RiskError> {
    let margin_cap = request
        .snapshot
        .equity
        .value()
        .checked_mul(request.limits.margin_cap_fraction)
        .ok_or(RiskError::Arithmetic {
            operation: "maximum isolated margin",
        })?;
    let initial_margin_fraction = Decimal::ONE
        .checked_div(Decimal::from(request.constraints.maximum_leverage.value()))
        .ok_or(RiskError::Arithmetic {
            operation: "maximum leverage margin fraction",
        })?;
    let per_notional_margin = initial_margin_fraction
        .checked_add(request.costs.entry_fee_fraction)
        .ok_or(RiskError::Arithmetic {
            operation: "entry reserve margin fraction",
        })?;
    Usdc::new(
        margin_cap
            .checked_div(per_notional_margin)
            .ok_or(RiskError::Arithmetic {
                operation: "margin-limited notional",
            })?,
    )
    .map_err(RiskError::from)
}

fn effective_risk_budget(request: &RiskRequest) -> Result<Usdc, RiskError> {
    let hard_cap = request
        .snapshot
        .equity
        .value()
        .checked_mul(MAX_TRADE_RISK_FRACTION)
        .ok_or(RiskError::Arithmetic {
            operation: "hard trade risk budget",
        })?;
    Ok(request.limits.risk_budget.min(Usdc::new(hard_cap)?))
}

fn quantity_quantum_notional(entry: Price, quantity_decimals: u8) -> Result<Usdc, RiskError> {
    let quantum = Decimal::new(1, quantity_decimals.into());
    Usdc::new(
        entry
            .value()
            .checked_mul(quantum)
            .ok_or(RiskError::Arithmetic {
                operation: "quantity quantum notional",
            })?,
    )
    .map_err(RiskError::from)
}

fn floor_units(cap: Usdc, unit_notional: Usdc) -> Result<u128, RiskError> {
    if unit_notional.value().is_zero() {
        return Err(RiskError::Arithmetic {
            operation: "zero notional quantum",
        });
    }
    cap.value()
        .checked_div(unit_notional.value())
        .map(|ratio| ratio.floor().to_u128().unwrap_or_default())
        .ok_or(RiskError::Arithmetic {
            operation: "notional unit count",
        })
}

fn units_to_notional(units: u128, unit_notional: Usdc) -> Result<Usdc, RiskError> {
    let units = Decimal::from_u128(units).ok_or(RiskError::Arithmetic {
        operation: "notional unit conversion",
    })?;
    Usdc::new(
        units
            .checked_mul(unit_notional.value())
            .ok_or(RiskError::Arithmetic {
                operation: "rounded notional",
            })?,
    )
    .map_err(RiskError::from)
}

fn rounded_quantity(
    notional: Usdc,
    entry: Price,
    quantity_decimals: u8,
) -> Result<Quantity, RiskError> {
    let raw = notional
        .value()
        .checked_div(entry.value())
        .ok_or(RiskError::Arithmetic {
            operation: "entry quantity",
        })?;
    let rounded = raw.round_dp_with_strategy(quantity_decimals.into(), RoundingStrategy::ToZero);
    Quantity::new(rounded).map_err(RiskError::from)
}

fn planned_loss(
    candidate: &SignalCandidate,
    notional: Usdc,
    costs: &ConservativeCosts,
) -> Result<Usdc, RiskError> {
    let stressed_stop = stressed_stop(candidate, costs.stressed_impact_fraction(notional)?)?;
    let stop_distance = match candidate.side() {
        Side::Buy => candidate
            .reference_entry()
            .value()
            .checked_sub(stressed_stop.value()),
        Side::Sell => stressed_stop
            .value()
            .checked_sub(candidate.reference_entry().value()),
    }
    .ok_or(RiskError::Arithmetic {
        operation: "stressed stop distance",
    })?;
    let price_loss_fraction = stop_distance
        .checked_div(candidate.reference_entry().value())
        .ok_or(RiskError::Arithmetic {
            operation: "stressed stop loss fraction",
        })?;
    let fixed_cost_fraction = costs
        .entry_fee_fraction
        .checked_add(costs.exit_fee_fraction)
        .and_then(|total| total.checked_add(costs.funding_reserve_fraction().ok()?))
        .ok_or(RiskError::Arithmetic {
            operation: "planned fixed cost fraction",
        })?;
    let total_fraction =
        price_loss_fraction
            .checked_add(fixed_cost_fraction)
            .ok_or(RiskError::Arithmetic {
                operation: "planned loss fraction",
            })?;
    Usdc::new(
        notional
            .value()
            .checked_mul(total_fraction)
            .ok_or(RiskError::Arithmetic {
                operation: "planned loss",
            })?,
    )
    .map_err(RiskError::from)
}

fn stressed_stop(
    candidate: &SignalCandidate,
    impact_fraction: Decimal,
) -> Result<Price, RiskError> {
    let multiplier = match candidate.side() {
        Side::Buy => Decimal::ONE.checked_sub(impact_fraction),
        Side::Sell => Decimal::ONE.checked_add(impact_fraction),
    }
    .ok_or(RiskError::Arithmetic {
        operation: "stressed stop multiplier",
    })?;
    Price::new(
        candidate
            .stop()
            .value()
            .checked_mul(multiplier)
            .ok_or(RiskError::Arithmetic {
                operation: "stressed stop price",
            })?,
    )
    .map_err(|error| match error {
        DomainError::NonPositivePrice => RiskError::Arithmetic {
            operation: "nonpositive stressed stop",
        },
        other => RiskError::Domain(other),
    })
}

fn counterfactuals(
    candidate: &SignalCandidate,
    notional: Usdc,
    quantity: Quantity,
    request: &RiskRequest,
) -> Result<[LeverageCounterfactual; 4], RiskError> {
    let mut results = Vec::with_capacity(4);
    for value in [5_u8, 10, 15, 20] {
        let leverage = Leverage::new(value)?;
        let calculation = leverage_calculation(candidate, notional, quantity, leverage, request);
        results.push(match calculation {
            Ok((margin, liquidation)) => LeverageCounterfactual {
                leverage,
                feasible: leverage <= request.constraints.maximum_leverage
                    && margin_within_cap(margin, request)?
                    && liquidation_is_safe(candidate, liquidation, request.limits)?,
                isolated_margin: Some(margin),
                liquidation: Some(liquidation),
            },
            Err(_) => LeverageCounterfactual {
                leverage,
                feasible: false,
                isolated_margin: None,
                liquidation: None,
            },
        });
    }
    results.try_into().map_err(|_| RiskError::Arithmetic {
        operation: "fixed leverage counterfactual array",
    })
}

fn select_lowest_safe_leverage(
    candidate: &SignalCandidate,
    notional: Usdc,
    quantity: Quantity,
    request: &RiskRequest,
) -> Result<Option<(Leverage, Usdc, LiquidationResult)>, RiskError> {
    for value in 5..=request.constraints.maximum_leverage.value() {
        let leverage = Leverage::new(value)?;
        let (margin, liquidation) =
            match leverage_calculation(candidate, notional, quantity, leverage, request) {
                Ok(value) => value,
                Err(_) => continue,
            };
        if !margin_within_cap(margin, request)? {
            continue;
        }
        if !liquidation_is_safe(candidate, liquidation, request.limits)? {
            continue;
        }
        return Ok(Some((leverage, margin, liquidation)));
    }
    Ok(None)
}

fn leverage_calculation(
    candidate: &SignalCandidate,
    notional: Usdc,
    quantity: Quantity,
    leverage: Leverage,
    request: &RiskRequest,
) -> Result<(Usdc, LiquidationResult), RiskError> {
    let initial_margin = notional
        .value()
        .checked_div(Decimal::from(leverage.value()))
        .ok_or(RiskError::Arithmetic {
            operation: "initial isolated margin",
        })?;
    let entry_fee = notional
        .value()
        .checked_mul(request.costs.entry_fee_fraction)
        .ok_or(RiskError::Arithmetic {
            operation: "entry fee reserve",
        })?;
    let margin = Usdc::new(initial_margin.checked_add(entry_fee).ok_or(
        RiskError::Arithmetic {
            operation: "isolated margin plus entry reserve",
        },
    )?)?;
    let funding_reserve = notional
        .value()
        .checked_mul(request.costs.funding_reserve_fraction()?)
        .ok_or(RiskError::Arithmetic {
            operation: "funding debit reserve",
        })?;
    let liquidation_equity = margin
        .value()
        .checked_sub(entry_fee)
        .and_then(|value| value.checked_sub(funding_reserve))
        .ok_or(RiskError::Arithmetic {
            operation: "liquidation reference equity",
        })?;
    let liquidation = calculate(&LiquidationInput::new(
        quantity,
        position_side(candidate.side()),
        candidate.reference_entry(),
        Usdc::new(liquidation_equity)?,
        request.constraints.maintenance_tiers.clone(),
    )?)?;
    Ok((margin, liquidation))
}

fn margin_within_cap(margin: Usdc, request: &RiskRequest) -> Result<bool, RiskError> {
    let cap = request
        .snapshot
        .equity
        .value()
        .checked_mul(request.limits.margin_cap_fraction)
        .ok_or(RiskError::Arithmetic {
            operation: "isolated margin cap",
        })?;
    Ok(margin.value() <= cap)
}

fn liquidation_is_safe(
    candidate: &SignalCandidate,
    liquidation: LiquidationResult,
    limits: RiskLimits,
) -> Result<bool, RiskError> {
    let has_adverse_direction = match candidate.side() {
        Side::Buy => liquidation.price() < candidate.reference_entry(),
        Side::Sell => liquidation.price() > candidate.reference_entry(),
    };
    if !has_adverse_direction {
        return Ok(false);
    }
    let stop_distance = absolute_difference(candidate.reference_entry(), candidate.stop())?;
    let liquidation_distance =
        absolute_difference(candidate.reference_entry(), liquidation.price())?;
    let required = stop_distance
        .checked_mul(limits.liquidation_stop_multiple)
        .ok_or(RiskError::Arithmetic {
            operation: "liquidation safety distance",
        })?;
    Ok(liquidation_distance >= required)
}

fn absolute_difference(left: Price, right: Price) -> Result<Decimal, RiskError> {
    if left >= right {
        left.value()
            .checked_sub(right.value())
            .ok_or(RiskError::Arithmetic {
                operation: "price distance",
            })
    } else {
        right
            .value()
            .checked_sub(left.value())
            .ok_or(RiskError::Arithmetic {
                operation: "price distance",
            })
    }
}

fn position_side(side: Side) -> PositionSide {
    match side {
        Side::Buy => PositionSide::Long,
        Side::Sell => PositionSide::Short,
    }
}

fn infer_leverage_rejection(
    counterfactuals: &[LeverageCounterfactual; 4],
    request: &RiskRequest,
) -> Result<RiskRejection, RiskError> {
    let margin_cap = request
        .snapshot
        .equity
        .value()
        .checked_mul(request.limits.margin_cap_fraction)
        .ok_or(RiskError::Arithmetic {
            operation: "leverage rejection margin cap",
        })?;
    if counterfactuals.iter().any(|result| {
        result.leverage <= request.constraints.maximum_leverage
            && result
                .isolated_margin
                .is_some_and(|margin| margin.value() <= margin_cap)
            && result.liquidation.is_some()
    }) {
        Ok(RiskRejection::LiquidationDistance)
    } else if counterfactuals.iter().any(|result| {
        result.leverage <= request.constraints.maximum_leverage && result.isolated_margin.is_some()
    }) {
        Ok(RiskRejection::MarginCap)
    } else {
        Ok(RiskRejection::Liquidation)
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal_macros::dec;

    use super::{
        ConservativeCosts, ImpactBand, ImpactCurve, RiskEngine, RiskInputError, RiskLimits,
        RiskRejection, RiskRequest, RiskSnapshot, VenueConstraints,
    };
    use crate::domain::{Leverage, Market, Price, Side, Sleeve, Usdc};
    use crate::event::TimestampNs;
    use crate::risk::liquidation::{MaintenanceTier, MaintenanceTiers};
    use crate::strategy::rules::{RuleConfig, RulesStrategy};
    use crate::strategy::{
        CandidateSpecification, CostDecision, SignalCandidate, Strategy, StrategyKind,
    };

    fn usdc(value: rust_decimal::Decimal) -> Usdc {
        Usdc::new(value).expect("nonnegative synthetic USDC")
    }

    fn digest(value: char) -> String {
        value.to_string().repeat(64)
    }

    fn impact_curve(
        current_fraction: rust_decimal::Decimal,
        trailing_p99_fraction: rust_decimal::Decimal,
    ) -> ImpactCurve {
        ImpactCurve::new(vec![
            ImpactBand::new(None, current_fraction, trailing_p99_fraction)
                .expect("valid open impact band"),
        ])
        .expect("complete impact curve")
    }

    fn candidate(side: Side) -> SignalCandidate {
        SignalCandidate::new(CandidateSpecification {
            strategy: StrategyKind::RulesOnly,
            market: Market::new("BTC").expect("market"),
            side,
            sleeve: Sleeve::FifteenMinute,
            decision_time: TimestampNs::new(1_000).expect("timestamp"),
            gross_edge: dec!(0.03),
            reference_entry: Price::new(dec!(100)).expect("entry"),
            stop: Price::new(match side {
                Side::Buy => dec!(99),
                Side::Sell => dec!(101),
            })
            .expect("stop"),
            target: Price::new(match side {
                Side::Buy => dec!(102),
                Side::Sell => dec!(98),
            })
            .expect("target"),
            time_exit: TimestampNs::new(2_000).expect("timestamp"),
            snapshot_digest: digest('a'),
            universe_digest: digest('c'),
            history_digest: digest('d'),
            strategy_fingerprint: digest('e'),
            explanation_json: "{}".into(),
        })
        .expect("candidate")
    }

    fn request(costs: ConservativeCosts, budget: rust_decimal::Decimal) -> RiskRequest {
        let tiers = MaintenanceTiers::new(vec![
            MaintenanceTier::new(usdc(dec!(0)), None, dec!(0.025), usdc(dec!(0))).expect("tier"),
        ])
        .expect("tiers");
        RiskRequest::new(
            RiskSnapshot::new(
                TimestampNs::new(1_000).expect("timestamp"),
                TimestampNs::new(1_000).expect("timestamp"),
                usdc(dec!(100)),
                digest('a'),
                digest('b'),
                digest('c'),
                digest('d'),
                digest('e'),
            )
            .expect("snapshot"),
            VenueConstraints::new(
                3,
                usdc(dec!(1)),
                usdc(dec!(500)),
                usdc(dec!(500)),
                Leverage::new(20).expect("leverage"),
                tiers,
            )
            .expect("constraints"),
            costs,
            RiskLimits::new(usdc(budget), dec!(0.25), dec!(2.5)).expect("limits"),
        )
    }

    fn costs() -> ConservativeCosts {
        ConservativeCosts::new(
            dec!(0.00075),
            dec!(0.00075),
            impact_curve(dec!(0.0005), dec!(0.001)),
            dec!(0.0001),
            dec!(0.0002),
            4,
        )
        .expect("costs")
    }

    #[test]
    fn impact_costs_are_selected_from_the_proposed_notional_band() {
        let costs = ConservativeCosts::new(
            dec!(0.00075),
            dec!(0.00075),
            ImpactCurve::new(vec![
                ImpactBand::new(Some(usdc(dec!(10))), dec!(0.0001), dec!(0.0002))
                    .expect("small band"),
                ImpactBand::new(None, dec!(0.005), dec!(0.006)).expect("large band"),
            ])
            .expect("complete curve"),
            dec!(0),
            dec!(0),
            0,
        )
        .expect("costs");
        let (_, low_cost) = super::cost_attributions(&costs, usdc(dec!(10))).expect("low cost");
        let (_, high_cost) =
            super::cost_attributions(&costs, usdc(dec!(10.001))).expect("high cost");

        assert!(high_cost > low_cost);
    }

    #[test]
    fn decreasing_impact_ladder_is_rejected_before_bisection() {
        let curve = ImpactCurve::new(vec![
            ImpactBand::new(Some(usdc(dec!(10))), dec!(0.10), dec!(0.20))
                .expect("expensive small band"),
            ImpactBand::new(None, dec!(0.001), dec!(0.002)).expect("cheap large band"),
        ]);

        assert_eq!(curve, Err(RiskInputError::DecreasingImpactBands));
    }

    #[test]
    fn snapshot_digest_bindings_reject_controls_and_non_hex_text() {
        let invalid = RiskSnapshot::new(
            TimestampNs::new(1_000).expect("timestamp"),
            TimestampNs::new(1_000).expect("timestamp"),
            usdc(dec!(100)),
            "a\0b",
            digest('b'),
            digest('c'),
            digest('d'),
            digest('e'),
        );
        assert!(matches!(
            invalid,
            Err(RiskInputError::InvalidDigest { field: "ledger" })
        ));
    }

    #[test]
    fn largest_venue_rounded_size_obeys_half_percent_loss_and_margin_cap() {
        let mut engine = RiskEngine::default();
        let request = request(costs(), dec!(1));
        let quote = engine
            .quote_candidate(&candidate(Side::Buy), &request)
            .expect("risk quote");

        assert!(quote.is_approved());
        assert!(quote.cost_quote().is_feasible());
        assert_eq!(quote.cost_quote().attributions().len(), 4);
    }

    #[test]
    fn increasing_conservative_cost_cannot_make_an_unsafe_candidate_approved() {
        let mut low_engine = RiskEngine::default();
        let mut high_engine = RiskEngine::default();
        let low = low_engine
            .quote_candidate(&candidate(Side::Buy), &request(costs(), dec!(0.5)))
            .expect("low cost quote");
        let high_costs = ConservativeCosts::new(
            dec!(0.003),
            dec!(0.003),
            impact_curve(dec!(0.004), dec!(0.004)),
            dec!(0.001),
            dec!(0.001),
            4,
        )
        .expect("higher costs");
        let high = high_engine
            .quote_candidate(&candidate(Side::Buy), &request(high_costs, dec!(0.5)))
            .expect("high cost quote");

        assert!(low.cost_quote().total_cost_fraction() < high.cost_quote().total_cost_fraction());
        assert!(!high.is_approved() || low.is_approved());
    }

    #[test]
    fn provenance_or_time_mismatch_returns_a_public_infeasible_quote() {
        let mut engine = RiskEngine::default();
        let specification = CandidateSpecification {
            strategy: StrategyKind::RulesOnly,
            market: Market::new("BTC").expect("market"),
            side: Side::Buy,
            sleeve: Sleeve::FifteenMinute,
            decision_time: TimestampNs::new(1_001).expect("timestamp"),
            gross_edge: dec!(0.03),
            reference_entry: Price::new(dec!(100)).expect("entry"),
            stop: Price::new(dec!(99)).expect("stop"),
            target: Price::new(dec!(102)).expect("target"),
            time_exit: TimestampNs::new(2_000).expect("timestamp"),
            snapshot_digest: digest('a'),
            universe_digest: digest('c'),
            history_digest: digest('d'),
            strategy_fingerprint: digest('e'),
            explanation_json: "{}".into(),
        };
        let mismatched = SignalCandidate::new(specification).expect("candidate");
        let quote = engine
            .quote_candidate(&mismatched, &request(costs(), dec!(0.5)))
            .expect("rejected public quote");

        assert_eq!(quote.rejections(), &[RiskRejection::CandidateTimeMismatch]);
        assert!(!quote.cost_quote().is_feasible());
    }

    #[test]
    fn unsafe_impact_ladder_is_rejected_before_quote() {
        assert!(matches!(
            ImpactBand::new(None, dec!(0.75), dec!(0.75)),
            Err(RiskInputError::UnsafeImpactFraction { .. })
        ));
    }

    #[test]
    fn policy_commitment_is_stable_for_frozen_inputs() {
        let commitment = request(costs(), dec!(0.5))
            .into_policy()
            .commitment_digest();

        assert_eq!(
            commitment,
            "ee42c395675ce042201689e6d17b22161b5bbef7bdcecd84b2142cf8a234ecb6"
        );
    }

    #[test]
    fn long_and_short_use_the_same_safety_contract() {
        for side in [Side::Buy, Side::Sell] {
            let mut engine = RiskEngine::default();
            let quote = engine
                .quote_candidate(&candidate(side), &request(costs(), dec!(0.5)))
                .expect("quote");
            assert!(quote.is_approved());
        }
    }

    #[test]
    fn zero_size_rounding_is_not_approved() {
        let mut engine = RiskEngine::default();
        let tiers = MaintenanceTiers::new(vec![
            MaintenanceTier::new(usdc(dec!(0)), None, dec!(0.025), usdc(dec!(0))).expect("tier"),
        ])
        .expect("tiers");
        let tiny = RiskRequest::new(
            request(costs(), dec!(0.5)).snapshot().clone(),
            VenueConstraints::new(
                0,
                usdc(dec!(1)),
                usdc(dec!(1)),
                usdc(dec!(1)),
                Leverage::new(20).expect("leverage"),
                tiers,
            )
            .expect("constraints"),
            costs(),
            RiskLimits::new(usdc(dec!(0.5)), dec!(0.25), dec!(2.5)).expect("limits"),
        );
        let quote = engine
            .quote_candidate(&candidate(Side::Buy), &tiny)
            .expect("quote");
        assert!(!quote.is_approved());
    }

    #[test]
    fn approval_is_digest_bound_and_consumed_exactly_once() {
        let mut engine = RiskEngine::default();
        let request = request(costs(), dec!(0.5));
        let candidate = candidate(Side::Buy);
        let quote = engine
            .quote_candidate(&candidate, &request)
            .expect("approved quote");
        let CostDecision::Accepted(intent) =
            RulesStrategy::new(RuleConfig::default()).accept_cost(&candidate, quote.cost_quote())
        else {
            panic!("cost gate must accept a safe candidate");
        };

        let changed = RiskSnapshot::new(
            request.snapshot().as_of_time(),
            request.snapshot().valid_through(),
            request.snapshot().equity(),
            digest('f'),
            digest('b'),
            digest('c'),
            digest('d'),
            digest('e'),
        )
        .expect("changed snapshot");
        assert!(matches!(
            engine.consume_quote(&intent, &changed, request.snapshot().as_of_time()),
            Err(super::RiskError::SnapshotChanged)
        ));

        let order = engine
            .consume_quote(&intent, request.snapshot(), request.snapshot().as_of_time())
            .expect("matching seal consumes once");
        assert!(order.quantity().value() > dec!(0));
        assert!(order.entry_notional().value() > dec!(0));
        assert_eq!(order.leverage().value(), 5);
        assert!(order.isolated_margin().value() <= dec!(25));
        assert!(order.planned_loss().value() <= dec!(0.5));
        assert!(order.liquidation().price().value() < dec!(97.5));
        let first_counterfactual = &order.counterfactuals()[0];
        assert_eq!(first_counterfactual.leverage.value(), 5);
        assert!(first_counterfactual.feasible);
        assert!(first_counterfactual.isolated_margin.is_some());
        assert!(first_counterfactual.liquidation.is_some());
        assert!(matches!(
            engine.consume_quote(&intent, request.snapshot(), request.snapshot().as_of_time()),
            Err(super::RiskError::UnknownOrConsumedQuote)
        ));
    }

    #[test]
    fn expired_or_excess_unselected_approvals_fail_closed_and_stay_bounded() {
        let mut engine = RiskEngine::default();
        let request = request(costs(), dec!(0.5));
        let candidate = candidate(Side::Buy);
        let quote = engine
            .quote_candidate(&candidate, &request)
            .expect("approved quote");
        let CostDecision::Accepted(intent) =
            RulesStrategy::new(RuleConfig::default()).accept_cost(&candidate, quote.cost_quote())
        else {
            panic!("cost gate must accept a safe candidate");
        };
        assert!(matches!(
            engine.consume_quote(
                &intent,
                request.snapshot(),
                TimestampNs::new(1_001).expect("expired timestamp"),
            ),
            Err(super::RiskError::UnknownOrConsumedQuote)
        ));

        for _ in 0..super::MAX_OUTSTANDING_APPROVALS {
            assert!(
                engine
                    .quote_candidate(&candidate, &request)
                    .expect("bounded quote")
                    .is_approved()
            );
        }
        let overflow = engine
            .quote_candidate(&candidate, &request)
            .expect("capacity rejection quote");
        assert_eq!(overflow.rejections(), &[RiskRejection::ApprovalCapacity]);
        assert!(!overflow.is_approved());
    }
}
