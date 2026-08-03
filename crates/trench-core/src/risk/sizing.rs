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

use crate::domain::{DomainError, Leverage, Price, Quantity, Side, Usdc};
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

/// Conservative per-notional costs and scheduled-funding reserve inputs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConservativeCosts {
    entry_fee_fraction: Decimal,
    exit_fee_fraction: Decimal,
    current_impact_fraction: Decimal,
    trailing_p99_impact_fraction: Decimal,
    current_funding_fraction: Decimal,
    trailing_p99_funding_fraction: Decimal,
    funding_timestamps: u16,
}

impl ConservativeCosts {
    /// Creates checked nonnegative fractional cost inputs.
    pub fn new(
        entry_fee_fraction: Decimal,
        exit_fee_fraction: Decimal,
        current_impact_fraction: Decimal,
        trailing_p99_impact_fraction: Decimal,
        current_funding_fraction: Decimal,
        trailing_p99_funding_fraction: Decimal,
        funding_timestamps: u16,
    ) -> Result<Self, RiskInputError> {
        for (field, value) in [
            ("entry fee", entry_fee_fraction),
            ("exit fee", exit_fee_fraction),
            ("current impact", current_impact_fraction),
            ("trailing p99 impact", trailing_p99_impact_fraction),
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
            current_impact_fraction,
            trailing_p99_impact_fraction,
            current_funding_fraction,
            trailing_p99_funding_fraction,
            funding_timestamps,
        })
    }

    /// Returns the fixed entry-fee fraction.
    #[must_use]
    pub const fn entry_fee_fraction(self) -> Decimal {
        self.entry_fee_fraction
    }

    /// Returns the fixed exit-fee fraction.
    #[must_use]
    pub const fn exit_fee_fraction(self) -> Decimal {
        self.exit_fee_fraction
    }

    /// Returns `max(2 * current impact, trailing 30-day p99 impact)`.
    pub fn stressed_impact_fraction(self) -> Result<Decimal, RiskError> {
        self.current_impact_fraction
            .checked_mul(Decimal::TWO)
            .map(|doubled| doubled.max(self.trailing_p99_impact_fraction))
            .ok_or(RiskError::Arithmetic {
                operation: "stressed impact",
            })
    }

    /// Returns the full-horizon adverse funding reserve fraction.
    pub fn funding_reserve_fraction(self) -> Result<Decimal, RiskError> {
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
        if [
            snapshot.ledger_digest.as_str(),
            snapshot.book_digest.as_str(),
            snapshot.universe_digest.as_str(),
            snapshot.config_digest.as_str(),
            snapshot.event_digest.as_str(),
        ]
        .iter()
        .any(|digest| digest.is_empty())
        {
            return Err(RiskInputError::MissingDigest);
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
            hasher.update(value.as_bytes());
            hasher.update(&[0]);
        }
        hasher.finalize().to_hex().to_string()
    }
}

/// Fully frozen inputs for one risk quote calculation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RiskRequest {
    snapshot: RiskSnapshot,
    constraints: VenueConstraints,
    costs: ConservativeCosts,
    limits: RiskLimits,
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
    snapshot_digest: String,
    counterfactuals: [LeverageCounterfactual; 4],
}

#[allow(
    dead_code,
    reason = "Task 13's pure engine is the sole production consumer of sealed approval data"
)]
impl ApprovedOrder {
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
        let source_digest = snapshot.digest();
        let quote_id = self.next_quote_id(&source_digest)?;
        let freshness = CostQuoteFreshness::new(snapshot.as_of_time, snapshot.valid_through)?;
        let source_digests = CostSourceDigests::new(snapshot.book_digest.clone(), source_digest);
        let (attributions, total_cost) = cost_attributions(request.costs)?;

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

        if stressed_stop(candidate, request.costs.stressed_impact_fraction()?).is_err() {
            return self.rejected_quote(
                quote_id,
                candidate,
                freshness,
                source_digests,
                total_cost,
                attributions,
                Vec::from([RiskRejection::PriceBand]),
            );
        }
        let some_notional = find_largest_safe_notional(candidate, request)?;
        let Some(entry_notional) = some_notional else {
            return self.rejected_quote(
                quote_id,
                candidate,
                freshness,
                source_digests,
                total_cost,
                attributions,
                Vec::from([RiskRejection::RiskBudget]),
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
        let planned_loss = planned_loss(candidate, rounded_notional, request.costs)?;
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
            snapshot_digest: snapshot.digest(),
            counterfactuals,
        };
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
        self.approvals
            .remove(quote_id)
            .ok_or(RiskError::UnknownOrConsumedQuote)
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

fn cost_feasibility_reason(rejection: RiskRejection) -> CostFeasibilityReason {
    match rejection {
        RiskRejection::RiskBudget => CostFeasibilityReason::RiskBlocked,
        RiskRejection::VenueConstraint | RiskRejection::MarginCap | RiskRejection::Liquidation => {
            CostFeasibilityReason::VenueConstraint
        }
        RiskRejection::LiquidationDistance | RiskRejection::PriceBand => {
            CostFeasibilityReason::PriceBand
        }
        RiskRejection::CandidateTimeMismatch | RiskRejection::UniverseDigestMismatch => {
            CostFeasibilityReason::RiskBlocked
        }
    }
}

fn cost_attributions(
    costs: ConservativeCosts,
) -> Result<(Vec<CostAttribution>, Decimal), RiskError> {
    let funding = costs.funding_reserve_fraction()?;
    let impact = costs.stressed_impact_fraction()?;
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
    let venue_cap = request
        .constraints
        .maximum_notional
        .min(request.constraints.maximum_executable_notional);
    let cap = venue_cap.min(maximum_margin_notional(request)?);
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
        if planned_loss(candidate, notional, request.costs)? <= effective_risk_budget(request)? {
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
    costs: ConservativeCosts,
) -> Result<Usdc, RiskError> {
    let stressed_stop = stressed_stop(candidate, costs.stressed_impact_fraction()?)?;
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
        ConservativeCosts, RiskEngine, RiskLimits, RiskRejection, RiskRequest, RiskSnapshot,
        VenueConstraints,
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
            snapshot_digest: "snapshot".into(),
            universe_digest: "universe".into(),
            history_digest: "history".into(),
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
                "ledger",
                "book",
                "universe",
                "config",
                "event",
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
            dec!(0.0005),
            dec!(0.001),
            dec!(0.0001),
            dec!(0.0002),
            4,
        )
        .expect("costs")
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
            dec!(0.004),
            dec!(0.004),
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
            snapshot_digest: "snapshot".into(),
            universe_digest: "universe".into(),
            history_digest: "history".into(),
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
    fn impossible_stop_impact_fails_closed_without_a_size() {
        let mut engine = RiskEngine::default();
        let impact = ConservativeCosts::new(
            dec!(0.00075),
            dec!(0.00075),
            dec!(0.75),
            dec!(0.75),
            dec!(0),
            dec!(0),
            0,
        )
        .expect("costs");
        let quote = engine
            .quote_candidate(&candidate(Side::Buy), &request(impact, dec!(0.5)))
            .expect("public rejected quote");
        assert_eq!(quote.rejections(), &[RiskRejection::PriceBand]);
        assert!(!quote.is_approved());
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
            "changed-ledger",
            "book",
            "universe",
            "config",
            "event",
        )
        .expect("changed snapshot");
        assert!(matches!(
            engine.consume_quote(&intent, &changed),
            Err(super::RiskError::SnapshotChanged)
        ));

        let order = engine
            .consume_quote(&intent, request.snapshot())
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
            engine.consume_quote(&intent, request.snapshot()),
            Err(super::RiskError::UnknownOrConsumedQuote)
        ));
    }
}
