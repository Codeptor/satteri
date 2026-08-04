//! Strategy boundary types that intentionally exclude risk sizing and live execution.

use blake3::Hasher;
use rust_decimal::Decimal;
use thiserror::Error;

use crate::domain::{Market, Price, Side, Sleeve};
use crate::event::TimestampNs;

pub mod rules;

/// Strategy identity for independent paper ledgers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StrategyKind {
    /// The auditable, interpretable rules ensemble.
    RulesOnly,
    /// The separately validated ML challenger.
    MlChampion,
}

/// Immutable, un-sized signal sent from a strategy to the sealed risk engine.
///
/// It intentionally contains no quantity, margin, leverage, PnL, account,
/// ledger, or executable-order fields. The risk engine is the sole owner of
/// those later decisions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignalCandidate {
    strategy: StrategyKind,
    market: Market,
    side: Side,
    sleeve: Sleeve,
    decision_time: TimestampNs,
    gross_edge: Decimal,
    reference_entry: Price,
    stop: Price,
    target: Price,
    time_exit: TimestampNs,
    snapshot_digest: String,
    universe_digest: String,
    history_digest: String,
    strategy_fingerprint: String,
    explanation_json: String,
    digest: String,
}

impl SignalCandidate {
    /// Builds a checked un-sized candidate inside the deterministic strategy boundary.
    pub(crate) fn new(specification: CandidateSpecification) -> Result<Self, CandidateError> {
        if specification.gross_edge <= Decimal::ZERO {
            return Err(CandidateError::NonPositiveGrossEdge {
                gross_edge: specification.gross_edge,
            });
        }
        let entry = specification.reference_entry;
        let exits_are_ordered = match specification.side {
            Side::Buy => specification.stop < entry && specification.target > entry,
            Side::Sell => specification.stop > entry && specification.target < entry,
        };
        if !exits_are_ordered {
            return Err(CandidateError::InvalidExitOrdering {
                side: specification.side,
                reference_entry: entry,
                stop: specification.stop,
                target: specification.target,
            });
        }
        if specification.time_exit <= specification.decision_time {
            return Err(CandidateError::TimeExitNotAfterDecision {
                decision_time: specification.decision_time,
                time_exit: specification.time_exit,
            });
        }
        if specification.snapshot_digest.is_empty()
            || specification.universe_digest.is_empty()
            || specification.history_digest.is_empty()
        {
            return Err(CandidateError::MissingProvenance);
        }
        if !is_blake3_digest(&specification.strategy_fingerprint) {
            return Err(CandidateError::InvalidStrategyFingerprint);
        }
        let mut candidate = Self {
            strategy: specification.strategy,
            market: specification.market,
            side: specification.side,
            sleeve: specification.sleeve,
            decision_time: specification.decision_time,
            gross_edge: specification.gross_edge,
            reference_entry: specification.reference_entry,
            stop: specification.stop,
            target: specification.target,
            time_exit: specification.time_exit,
            snapshot_digest: specification.snapshot_digest,
            universe_digest: specification.universe_digest,
            history_digest: specification.history_digest,
            strategy_fingerprint: specification.strategy_fingerprint,
            explanation_json: specification.explanation_json,
            digest: String::new(),
        };
        candidate.digest = candidate_digest(&candidate);
        Ok(candidate)
    }

    /// Returns the independent strategy that emitted this candidate.
    #[must_use]
    pub const fn strategy(&self) -> StrategyKind {
        self.strategy
    }

    /// Returns the eligible native-perpetual market.
    #[must_use]
    pub const fn market(&self) -> &Market {
        &self.market
    }

    /// Returns the directional paper-order side, without any size.
    #[must_use]
    pub const fn side(&self) -> Side {
        self.side
    }

    /// Returns the completed-bar sleeve that owns this prospective position.
    #[must_use]
    pub const fn sleeve(&self) -> Sleeve {
        self.sleeve
    }

    /// Returns the explicit completed-bar boundary where this candidate was evaluated.
    #[must_use]
    pub const fn decision_time(&self) -> TimestampNs {
        self.decision_time
    }

    /// Returns the positive conservative gross-edge fraction before risk-sized costs.
    #[must_use]
    pub const fn gross_edge(&self) -> Decimal {
        self.gross_edge
    }

    /// Returns the immutable completed-bar close used as the reference entry.
    #[must_use]
    pub const fn reference_entry(&self) -> Price {
        self.reference_entry
    }

    /// Returns the market-invalidation stop, independent of leverage.
    #[must_use]
    pub const fn stop(&self) -> Price {
        self.stop
    }

    /// Returns the take-profit price derived from the frozen R multiple.
    #[must_use]
    pub const fn target(&self) -> Price {
        self.target
    }

    /// Returns the explicit completed-bar time-exit boundary.
    #[must_use]
    pub const fn time_exit(&self) -> TimestampNs {
        self.time_exit
    }

    /// Returns the exact immutable common-feature snapshot digest.
    #[must_use]
    pub fn snapshot_digest(&self) -> &str {
        &self.snapshot_digest
    }

    /// Returns the universe digest embedded by feature provenance.
    #[must_use]
    pub fn universe_digest(&self) -> &str {
        &self.universe_digest
    }

    /// Returns the exact long-horizon source digest.
    #[must_use]
    pub fn history_digest(&self) -> &str {
        &self.history_digest
    }

    /// Returns the validated frozen strategy artifact/version fingerprint.
    #[must_use]
    pub fn strategy_fingerprint(&self) -> &str {
        &self.strategy_fingerprint
    }

    /// Returns the byte-stable auditable rules explanation JSON.
    #[must_use]
    pub fn explanation_json(&self) -> &str {
        &self.explanation_json
    }

    /// Returns the stable digest binding this exact candidate to a cost quote.
    #[must_use]
    pub fn digest(&self) -> &str {
        &self.digest
    }
}

/// Complete internal input to an un-sized candidate construction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CandidateSpecification {
    pub(crate) strategy: StrategyKind,
    pub(crate) market: Market,
    pub(crate) side: Side,
    pub(crate) sleeve: Sleeve,
    pub(crate) decision_time: TimestampNs,
    pub(crate) gross_edge: Decimal,
    pub(crate) reference_entry: Price,
    pub(crate) stop: Price,
    pub(crate) target: Price,
    pub(crate) time_exit: TimestampNs,
    pub(crate) snapshot_digest: String,
    pub(crate) universe_digest: String,
    pub(crate) history_digest: String,
    pub(crate) strategy_fingerprint: String,
    pub(crate) explanation_json: String,
}

/// Candidate construction rejected an invalid strategy output before it reached risk.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CandidateError {
    /// Gross edge must cover a positive expected move before cost acceptance.
    #[error("candidate gross edge must be positive, got {gross_edge}")]
    NonPositiveGrossEdge {
        /// Rejected fraction.
        gross_edge: Decimal,
    },
    /// Side-aware invalidation and profit prices did not straddle the entry.
    #[error(
        "candidate {side:?} exits must straddle entry {reference_entry:?}, got stop {stop:?} and target {target:?}"
    )]
    InvalidExitOrdering {
        /// Candidate direction governing the required ordering.
        side: Side,
        /// Immutable completed-bar entry reference.
        reference_entry: Price,
        /// Proposed stop price.
        stop: Price,
        /// Proposed target price.
        target: Price,
    },
    /// A time exit must give the candidate at least one instant after its decision boundary.
    #[error("candidate time exit {time_exit} must be later than decision time {decision_time}")]
    TimeExitNotAfterDecision {
        /// Immutable strategy decision boundary.
        decision_time: TimestampNs,
        /// Rejected non-future exit boundary.
        time_exit: TimestampNs,
    },
    /// A candidate cannot be reconstructed without all three input digests.
    #[error("candidate requires snapshot, universe, and long-horizon provenance digests")]
    MissingProvenance,
    /// A candidate must seal a canonical BLAKE3 strategy artifact/version fingerprint.
    #[error("candidate strategy fingerprint must be a 64-character hexadecimal BLAKE3 digest")]
    InvalidStrategyFingerprint,
}

fn candidate_digest(candidate: &SignalCandidate) -> String {
    let mut hasher = Hasher::new_derive_key("trench.signal-candidate.v1");
    hasher.update(match candidate.strategy {
        StrategyKind::RulesOnly => b"rules_only",
        StrategyKind::MlChampion => b"ml_champion",
    });
    hasher.update(&[0]);
    hasher.update(candidate.market.as_str().as_bytes());
    hasher.update(&[0]);
    hasher.update(&[match candidate.side {
        Side::Buy => 0,
        Side::Sell => 1,
    }]);
    hasher.update(&[match candidate.sleeve {
        Sleeve::FifteenMinute => 0,
        Sleeve::OneHour => 1,
    }]);
    hasher.update(&candidate.decision_time.value().to_be_bytes());
    for value in [
        candidate.gross_edge,
        candidate.reference_entry.value(),
        candidate.stop.value(),
        candidate.target.value(),
    ] {
        hasher.update(value.to_string().as_bytes());
        hasher.update(&[0]);
    }
    hasher.update(&candidate.time_exit.value().to_be_bytes());
    for digest in [
        &candidate.snapshot_digest,
        &candidate.universe_digest,
        &candidate.history_digest,
        &candidate.strategy_fingerprint,
        &candidate.explanation_json,
    ] {
        hasher.update(digest.as_bytes());
        hasher.update(&[0]);
    }
    hasher.finalize().to_hex().to_string()
}

fn is_blake3_digest(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

/// Opaque identifier assigned to a sealed risk-sized cost quote.
///
/// External code can inspect a quote ID but cannot manufacture one, its source
/// digests, or a feasible quote. The risk authority owns that construction
/// boundary.
///
/// ```compile_fail
/// use rust_decimal::Decimal;
/// use trench_core::domain::Market;
/// use trench_core::event::TimestampNs;
/// use trench_core::strategy::{
///     CostQuote, CostQuoteFreshness, CostSourceDigests, QuoteId,
/// };
///
/// let at = TimestampNs::new(0).unwrap();
/// let _ = CostQuote::new(
///     QuoteId::new("untrusted").unwrap(),
///     Market::new("BTC").unwrap(),
///     "candidate",
///     CostQuoteFreshness::new(at, at).unwrap(),
///     CostSourceDigests::new("book", "risk"),
///     Decimal::ZERO,
///     Vec::new(),
///     Vec::new(),
/// );
/// ```
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct QuoteId(String);

impl QuoteId {
    /// Creates a checked opaque quote identifier for the crate-private risk boundary.
    ///
    /// # Errors
    ///
    /// Rejects empty, padded, or control-character-containing IDs.
    #[allow(
        dead_code,
        reason = "Task 11's risk engine is the sole production constructor; strategy tests exercise the boundary first."
    )]
    pub(crate) fn new(value: impl Into<String>) -> Result<Self, CostQuoteError> {
        let value = value.into();
        if value.is_empty() || value.trim() != value || value.chars().any(char::is_control) {
            return Err(CostQuoteError::InvalidQuoteId);
        }
        Ok(Self(value))
    }
}

/// Explicit validity interval for one risk-sized public cost quote.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CostQuoteFreshness {
    as_of_time: TimestampNs,
    valid_through: TimestampNs,
}

impl CostQuoteFreshness {
    /// Creates a checked inclusive quote-validity interval.
    pub fn new(
        as_of_time: TimestampNs,
        valid_through: TimestampNs,
    ) -> Result<Self, CostQuoteError> {
        if valid_through < as_of_time {
            return Err(CostQuoteError::BackwardFreshness {
                as_of_time,
                valid_through,
            });
        }
        Ok(Self {
            as_of_time,
            valid_through,
        })
    }

    /// Returns the explicit quote construction boundary.
    #[must_use]
    pub const fn as_of_time(self) -> TimestampNs {
        self.as_of_time
    }

    /// Returns the last inclusive decision time for this quote.
    #[must_use]
    pub const fn valid_through(self) -> TimestampNs {
        self.valid_through
    }

    /// Returns whether the quote can be used at the supplied explicit decision time.
    #[must_use]
    pub fn is_fresh_at(self, at: TimestampNs) -> bool {
        at >= self.as_of_time && at <= self.valid_through
    }
}

/// Public digests proving the source inputs of a cost quote, without its sealed order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CostSourceDigests {
    book: String,
    risk: String,
}

impl CostSourceDigests {
    /// Captures immutable public book and risk-quote source digests inside risk.
    #[must_use]
    #[allow(
        dead_code,
        reason = "Task 11's risk engine is the sole production constructor; strategy tests exercise the boundary first."
    )]
    pub(crate) fn new(book: impl Into<String>, risk: impl Into<String>) -> Self {
        Self {
            book: book.into(),
            risk: risk.into(),
        }
    }

    /// Returns the executable-book input digest.
    #[must_use]
    pub fn book(&self) -> &str {
        &self.book
    }

    /// Returns the opaque risk-quote source digest.
    #[must_use]
    pub fn risk(&self) -> &str {
        &self.risk
    }
}

/// One public component of complete cost as a fraction of notional.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CostAttribution {
    /// Expected entry fee fraction.
    EntryFee(Decimal),
    /// Expected exit fee fraction.
    ExitFee(Decimal),
    /// Maximum-horizon funding reserve fraction.
    FundingReserve(Decimal),
    /// Conservative book-impact fraction.
    MarketImpact(Decimal),
}

impl CostAttribution {
    /// Creates an entry-fee attribution.
    #[must_use]
    pub const fn entry_fee(fraction: Decimal) -> Self {
        Self::EntryFee(fraction)
    }

    /// Creates an exit-fee attribution.
    #[must_use]
    pub const fn exit_fee(fraction: Decimal) -> Self {
        Self::ExitFee(fraction)
    }

    /// Returns the nonnegative public cost fraction.
    #[must_use]
    pub const fn fraction(self) -> Decimal {
        match self {
            Self::EntryFee(fraction)
            | Self::ExitFee(fraction)
            | Self::FundingReserve(fraction)
            | Self::MarketImpact(fraction) => fraction,
        }
    }
}

/// Machine-readable cause a sealed risk quote cannot become an order intent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CostFeasibilityReason {
    /// Visible executable depth is insufficient at the sealed size.
    InsufficientDepth,
    /// The sealed entry or stop would breach its price band.
    PriceBand,
    /// Venue constraints reject the sealed isolated-margin proposal.
    VenueConstraint,
    /// A global or ledger-local risk breaker disallows a new entry.
    RiskBlocked,
}

/// Public, un-sized cost evidence returned by the sealed risk engine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CostQuote {
    quote_id: QuoteId,
    market: Market,
    candidate_digest: String,
    freshness: CostQuoteFreshness,
    source_digests: CostSourceDigests,
    total_cost_fraction: Decimal,
    attributions: Vec<CostAttribution>,
    infeasibility_reasons: Vec<CostFeasibilityReason>,
}

impl CostQuote {
    /// Creates a checked public cost quote inside risk without revealing sealed sizing detail.
    #[expect(
        clippy::too_many_arguments,
        reason = "fixed cost-quote boundary schema"
    )]
    #[allow(
        dead_code,
        reason = "Task 11's risk engine is the sole production constructor; strategy tests exercise the boundary first."
    )]
    pub(crate) fn new(
        quote_id: QuoteId,
        market: Market,
        candidate_digest: impl Into<String>,
        freshness: CostQuoteFreshness,
        source_digests: CostSourceDigests,
        total_cost_fraction: Decimal,
        attributions: Vec<CostAttribution>,
        infeasibility_reasons: Vec<CostFeasibilityReason>,
    ) -> Result<Self, CostQuoteError> {
        if total_cost_fraction < Decimal::ZERO {
            return Err(CostQuoteError::NegativeFraction {
                field: "total cost",
                value: total_cost_fraction,
            });
        }
        let attributed_total =
            attributions
                .iter()
                .try_fold(Decimal::ZERO, |total, attribution| {
                    let fraction = attribution.fraction();
                    if fraction < Decimal::ZERO {
                        return Err(CostQuoteError::NegativeFraction {
                            field: "attributed cost",
                            value: fraction,
                        });
                    }
                    total
                        .checked_add(fraction)
                        .ok_or(CostQuoteError::Arithmetic)
                })?;
        if attributed_total != total_cost_fraction {
            return Err(CostQuoteError::AttributionTotalMismatch {
                total_cost_fraction,
                attributed_total,
            });
        }
        let candidate_digest = candidate_digest.into();
        if candidate_digest.is_empty()
            || source_digests.book.is_empty()
            || source_digests.risk.is_empty()
        {
            return Err(CostQuoteError::MissingBinding);
        }
        Ok(Self {
            quote_id,
            market,
            candidate_digest,
            freshness,
            source_digests,
            total_cost_fraction,
            attributions,
            infeasibility_reasons,
        })
    }

    /// Returns the opaque quote ID only for binding an accepted intent.
    #[must_use]
    pub const fn quote_id(&self) -> &QuoteId {
        &self.quote_id
    }

    /// Returns the market this quote was risk-sized for.
    #[must_use]
    pub const fn market(&self) -> &Market {
        &self.market
    }

    /// Returns the immutable candidate digest this quote is bound to.
    #[must_use]
    pub fn candidate_digest(&self) -> &str {
        &self.candidate_digest
    }

    /// Returns the only public cost total: a fraction of notional.
    #[must_use]
    pub const fn total_cost_fraction(&self) -> Decimal {
        self.total_cost_fraction
    }

    /// Returns every public attributed fractional cost component.
    #[must_use]
    pub fn attributions(&self) -> &[CostAttribution] {
        &self.attributions
    }

    /// Returns explicit quote freshness evidence.
    #[must_use]
    pub const fn freshness(&self) -> CostQuoteFreshness {
        self.freshness
    }

    /// Returns source digests without exposing the sealed approved order.
    #[must_use]
    pub const fn source_digests(&self) -> &CostSourceDigests {
        &self.source_digests
    }

    /// Returns every non-size feasibility blocker, if any.
    #[must_use]
    pub fn infeasibility_reasons(&self) -> &[CostFeasibilityReason] {
        &self.infeasibility_reasons
    }

    /// Returns whether no public feasibility reason blocks this quote.
    #[must_use]
    pub fn is_feasible(&self) -> bool {
        self.infeasibility_reasons.is_empty()
    }

    /// Returns whether this quote is valid at an explicit strategy decision boundary.
    #[must_use]
    pub fn is_fresh_at(&self, at: TimestampNs) -> bool {
        self.freshness.is_fresh_at(at)
    }
}

/// Cost-quote construction failed without exposing a sealed order.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CostQuoteError {
    /// A quote ID did not meet the opaque identifier contract.
    #[error("quote ID must be nonempty, unpadded, and free of controls")]
    InvalidQuoteId,
    /// Quote expiry was earlier than its own source boundary.
    #[error("quote valid-through {valid_through} precedes quote boundary {as_of_time}")]
    BackwardFreshness {
        /// Explicit quote source boundary.
        as_of_time: TimestampNs,
        /// Rejected earlier expiry.
        valid_through: TimestampNs,
    },
    /// A public cost fraction cannot be negative.
    #[error("{field} fraction cannot be negative, got {value}")]
    NegativeFraction {
        /// Failing fraction family.
        field: &'static str,
        /// Rejected exact fraction.
        value: Decimal,
    },
    /// Total cost must equal the complete public attribution sum.
    #[error("total cost {total_cost_fraction} does not equal attributed total {attributed_total}")]
    AttributionTotalMismatch {
        /// Public declared total.
        total_cost_fraction: Decimal,
        /// Checked sum of public components.
        attributed_total: Decimal,
    },
    /// Candidate or source binding evidence was omitted.
    #[error("cost quote requires candidate and source digest bindings")]
    MissingBinding,
    /// Exact decimal attribution accumulation overflowed.
    #[error("cost attribution arithmetic failed")]
    Arithmetic,
}

/// Rejection reason from the public post-risk cost acceptance gate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CostRejection {
    /// Quote belongs to a different candidate or market.
    Mismatch,
    /// Quote was not fresh at the candidate's explicit decision boundary.
    Stale,
    /// The sealed quote exposed a public feasibility blocker.
    Infeasible,
    /// Gross edge did not cover at least 1.5 times complete cost.
    InsufficientGrossEdge,
}

/// An accepted, still-un-sized intent bound only to an opaque quote ID.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderIntent {
    candidate: SignalCandidate,
    quote_id: QuoteId,
    total_cost_fraction: Decimal,
}

impl OrderIntent {
    /// Creates an intent only after the common public-cost gate accepted it.
    #[must_use]
    pub(crate) fn new(candidate: SignalCandidate, quote: &CostQuote) -> Self {
        Self {
            candidate,
            quote_id: quote.quote_id.clone(),
            total_cost_fraction: quote.total_cost_fraction,
        }
    }

    /// Returns the original un-sized candidate.
    #[must_use]
    pub const fn candidate(&self) -> &SignalCandidate {
        &self.candidate
    }

    /// Returns the opaque quote binding without revealing any sealed order detail.
    #[must_use]
    pub const fn quote_id(&self) -> &QuoteId {
        &self.quote_id
    }

    /// Returns the accepted public complete fractional cost.
    #[must_use]
    pub const fn total_cost_fraction(&self) -> Decimal {
        self.total_cost_fraction
    }
}

/// Result of the common post-risk public-cost acceptance gate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CostDecision {
    /// Candidate covers complete public cost and is bound to its quote.
    Accepted(Box<OrderIntent>),
    /// Candidate was rejected with a machine-readable public reason.
    Rejected(CostRejection),
}

/// Common strategy boundary for public post-risk cost acceptance only.
///
/// Strategies receive no sealed quantity, margin, leverage, PnL, or order.
pub trait Strategy {
    /// Returns the immutable artifact/version fingerprint that produced candidates.
    fn fingerprint(&self) -> &str;

    /// Applies the exact `gross_edge >= 1.5 * total_cost` acceptance gate.
    fn accept_cost(&self, candidate: &SignalCandidate, quote: &CostQuote) -> CostDecision;
}

#[cfg(test)]
mod tests {
    use rust_decimal_macros::dec;

    use crate::domain::{Market, Price, Side, Sleeve};
    use crate::event::TimestampNs;

    use super::{
        CandidateError, CandidateSpecification, CostAttribution, CostQuote, CostQuoteError,
        CostQuoteFreshness, CostSourceDigests, QuoteId, SignalCandidate, StrategyKind,
    };

    #[test]
    fn candidate_rejects_nonpositive_gross_edge() {
        let mut specification = candidate_specification(Side::Buy);
        specification.gross_edge = dec!(0);

        assert!(matches!(
            SignalCandidate::new(specification),
            Err(CandidateError::NonPositiveGrossEdge { gross_edge }) if gross_edge == dec!(0)
        ));
    }

    #[test]
    fn candidate_rejects_a_noncanonical_strategy_artifact_fingerprint() {
        let mut specification = candidate_specification(Side::Buy);
        specification.strategy_fingerprint = "not-a-blake3-digest".to_owned();

        assert!(matches!(
            SignalCandidate::new(specification),
            Err(CandidateError::InvalidStrategyFingerprint)
        ));
    }

    #[test]
    fn candidate_rejects_side_aware_inverted_exit_ordering() {
        let mut long = candidate_specification(Side::Buy);
        long.stop = Price::new(dec!(100)).expect("price");
        assert!(matches!(
            SignalCandidate::new(long),
            Err(CandidateError::InvalidExitOrdering {
                side: Side::Buy,
                ..
            })
        ));

        let mut long = candidate_specification(Side::Buy);
        long.target = Price::new(dec!(100)).expect("price");
        assert!(matches!(
            SignalCandidate::new(long),
            Err(CandidateError::InvalidExitOrdering {
                side: Side::Buy,
                ..
            })
        ));

        let mut short = candidate_specification(Side::Sell);
        short.stop = Price::new(dec!(100)).expect("price");
        assert!(matches!(
            SignalCandidate::new(short),
            Err(CandidateError::InvalidExitOrdering {
                side: Side::Sell,
                ..
            })
        ));

        let mut short = candidate_specification(Side::Sell);
        short.target = Price::new(dec!(100)).expect("price");
        assert!(matches!(
            SignalCandidate::new(short),
            Err(CandidateError::InvalidExitOrdering {
                side: Side::Sell,
                ..
            })
        ));
    }

    #[test]
    fn candidate_rejects_time_exit_at_or_before_decision_time() {
        let mut specification = candidate_specification(Side::Buy);
        specification.time_exit = specification.decision_time;

        assert!(matches!(
            SignalCandidate::new(specification),
            Err(CandidateError::TimeExitNotAfterDecision { .. })
        ));

        let mut specification = candidate_specification(Side::Buy);
        specification.time_exit = timestamp(9);

        assert!(matches!(
            SignalCandidate::new(specification),
            Err(CandidateError::TimeExitNotAfterDecision { .. })
        ));
    }

    #[test]
    fn public_cost_quote_exposes_only_complete_fractional_cost_audit() {
        let quote = CostQuote::new(
            QuoteId::new("quote-1").expect("quote ID"),
            Market::new("BTC").expect("market"),
            "candidate-digest",
            CostQuoteFreshness::new(timestamp(10), timestamp(20)).expect("freshness"),
            CostSourceDigests::new("book-digest", "risk-digest"),
            dec!(0.01),
            vec![
                CostAttribution::entry_fee(dec!(0.004)),
                CostAttribution::exit_fee(dec!(0.006)),
            ],
            Vec::new(),
        )
        .expect("complete quote");

        assert_eq!(quote.total_cost_fraction(), dec!(0.01));
        assert_eq!(quote.attributions().len(), 2);
        assert!(quote.is_fresh_at(timestamp(20)));
    }

    #[test]
    fn cost_quote_rejects_incomplete_attribution() {
        assert!(matches!(
            CostQuote::new(
                QuoteId::new("quote-1").expect("quote ID"),
                Market::new("BTC").expect("market"),
                "candidate-digest",
                CostQuoteFreshness::new(timestamp(10), timestamp(20)).expect("freshness"),
                CostSourceDigests::new("book-digest", "risk-digest"),
                dec!(0.01),
                vec![CostAttribution::entry_fee(dec!(0.004))],
                Vec::new(),
            ),
            Err(CostQuoteError::AttributionTotalMismatch { .. })
        ));
    }

    fn timestamp(value: i128) -> TimestampNs {
        TimestampNs::new(value).expect("timestamp")
    }

    fn candidate_specification(side: Side) -> CandidateSpecification {
        let (stop, target) = match side {
            Side::Buy => (dec!(90), dec!(120)),
            Side::Sell => (dec!(110), dec!(80)),
        };
        CandidateSpecification {
            strategy: StrategyKind::MlChampion,
            market: Market::new("BTC").expect("market"),
            side,
            sleeve: Sleeve::FifteenMinute,
            decision_time: timestamp(10),
            gross_edge: dec!(0.12),
            reference_entry: Price::new(dec!(100)).expect("price"),
            stop: Price::new(stop).expect("price"),
            target: Price::new(target).expect("price"),
            time_exit: timestamp(11),
            snapshot_digest: "snapshot".to_owned(),
            universe_digest: "universe".to_owned(),
            history_digest: "history".to_owned(),
            strategy_fingerprint: "a".repeat(64),
            explanation_json: "{}".to_owned(),
        }
    }
}
