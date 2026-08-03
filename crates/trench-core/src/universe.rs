//! Immutable tradeable-universe contracts shared by point-in-time feature consumers.

use std::{
    collections::{BTreeMap, BTreeSet},
    str::FromStr,
};

use blake3::Hasher;
use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use thiserror::Error;

use crate::domain::{Bps, Market, Usdc};
use crate::event::TimestampNs;

/// Maximum markets that may be tradeable in one frozen universe snapshot.
pub const MAX_TRADEABLE_MARKETS: usize = 20;
const HOUR_NS: i64 = 3_600_000_000_000;
const STRATEGY_BAR_NS: i64 = 900_000_000_000;
const REQUIRED_HISTORY_DAYS: u16 = 30;
const REQUIRED_BAR_COVERAGE: Decimal = Decimal::from_parts(995, 0, 0, false, 3);
const MINIMUM_VENUE_LEVERAGE: u16 = 5;
const MAX_EFFECTIVE_SPREAD_BPS: Decimal = Decimal::from_parts(15, 0, 0, false, 0);
const MINIMUM_DAILY_NOTIONAL_USDC: Decimal = Decimal::from_parts(5_000_000, 0, 0, false, 0);
const FIXED_DEPTH_PROBE_USDC: Decimal = Decimal::from_parts(500, 0, 0, false, 0);
const MINIMUM_DEPTH_MULTIPLE: Decimal = Decimal::from_parts(100, 0, 0, false, 0);
const WARM_BUFFER_MARKETS: usize = 10;
const VOLUME_WEIGHT: Decimal = Decimal::from_parts(30, 0, 0, false, 2);
const OPEN_INTEREST_WEIGHT: Decimal = Decimal::from_parts(20, 0, 0, false, 2);
const INVERSE_SPREAD_WEIGHT: Decimal = Decimal::from_parts(30, 0, 0, false, 2);
const DEPTH_WEIGHT: Decimal = Decimal::from_parts(15, 0, 0, false, 2);
const CONTINUITY_WEIGHT: Decimal = Decimal::from_parts(5, 0, 0, false, 2);

/// Invalid immutable tradeable-universe input.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum UniverseError {
    /// A frozen universe must name at least one eligible market.
    #[error("tradeable universe must contain at least one market")]
    Empty,
    /// A caller attempted to provide more members than the fixed tradeable rank range.
    #[error("tradeable universe has {actual} markets, exceeding the fixed limit {limit}")]
    TooManyMarkets {
        /// Number of supplied unique markets.
        actual: usize,
        /// Maximum fixed tradeable membership.
        limit: usize,
    },
    /// Serialized membership did not match its deterministic content digest.
    #[error("universe digest does not match its serialized contents")]
    DigestMismatch,
    /// Serialized snapshot inputs or derived fields differed from canonical selector output.
    #[error("universe snapshot contents do not match canonical selector output")]
    SnapshotContentMismatch,
    /// A snapshot field was malformed or not encoded canonically.
    #[error("invalid universe snapshot wire field `{field}`: {message}")]
    InvalidSnapshotWire {
        /// The rejected field name.
        field: &'static str,
        /// The validation failure.
        message: String,
    },
    /// A universe must freeze at a completed UTC hour.
    #[error("tradeable universe boundary {as_of_time} is not a completed UTC hour")]
    NotCompletedHour {
        /// Rejected immutable snapshot boundary.
        as_of_time: TimestampNs,
    },
    /// A universe input named one market more than once at a single boundary.
    #[error("duplicate market `{market:?}` in one universe snapshot")]
    DuplicateMarket {
        /// The ambiguous market identifier.
        market: Market,
    },
    /// A fractional quality metric was outside the inclusive unit interval.
    #[error("universe metric `{field}` must be between zero and one inclusive")]
    InvalidFraction {
        /// The invalid metric field.
        field: &'static str,
    },
    /// A depth profile decreased as its executable price band widened.
    #[error("{side} depth must not decrease from 10 to 25 to 50 basis points")]
    NonMonotonicDepth {
        /// The directional side whose profile was malformed.
        side: &'static str,
    },
    /// A decision activation time was not a completed fifteen-minute strategy bar.
    #[error("strategy activation boundary {decision_time} is not a completed fifteen-minute bar")]
    NotCompletedStrategyBar {
        /// The invalid decision boundary.
        decision_time: TimestampNs,
    },
    /// A strategy attempted to use a snapshot before its explicit hourly boundary.
    #[error("strategy decision {decision_time} precedes universe snapshot {snapshot_time}")]
    DecisionBeforeSnapshot {
        /// The invalid strategy decision boundary.
        decision_time: TimestampNs,
        /// The later snapshot boundary.
        snapshot_time: TimestampNs,
    },
    /// A completed hourly snapshot was no longer the latest one available to a decision.
    #[error(
        "strategy decision {decision_time} requires a newer universe than snapshot {snapshot_time}"
    )]
    SnapshotNotCurrent {
        /// The stale hourly universe boundary.
        snapshot_time: TimestampNs,
        /// The strategy decision that must use a newer completed hour.
        decision_time: TimestampNs,
    },
    /// A transition attempted to use selector output from any hour other than the exact prior one.
    #[error(
        "prior selector snapshot {actual_snapshot_time} must be the exact hour before {snapshot_time} ({expected_snapshot_time})"
    )]
    PriorSnapshotBoundaryMismatch {
        /// Hourly boundary being activated.
        snapshot_time: TimestampNs,
        /// Required source boundary for the prior selector output.
        expected_snapshot_time: TimestampNs,
        /// Actual source boundary carried by the supplied selector output.
        actual_snapshot_time: TimestampNs,
    },
    /// A prior selector output no longer agrees with the digest of its frozen snapshot.
    #[error("prior selector output has invalid snapshot provenance at {snapshot_time}")]
    PriorSnapshotDigestMismatch {
        /// Source boundary whose immutable selection digest was invalid.
        snapshot_time: TimestampNs,
    },
    /// A prior selector output carried membership from a different decision boundary.
    #[error(
        "prior selector output membership boundary {membership_time} must equal its snapshot boundary {snapshot_time}"
    )]
    PriorMembershipBoundaryMismatch {
        /// Source snapshot boundary carried by the selector output.
        snapshot_time: TimestampNs,
        /// Actual membership boundary carried by the selector output.
        membership_time: TimestampNs,
    },
    /// Checked arithmetic could not preserve an exact deterministic selector value.
    #[error("checked arithmetic failed while calculating {operation}")]
    Arithmetic {
        /// The failed calculation.
        operation: &'static str,
    },
}

/// Venue listing lifecycle used by the native-perpetual structural gate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ListingState {
    /// The market is currently listed and accepting normal venue activity.
    Active,
    /// The venue has delisted the market.
    Delisted,
    /// The venue has paused the market.
    Paused,
}

/// Explicit availability inputs from one point-in-time metadata/context sample.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MarketDataAvailability {
    listing_state: ListingState,
    live_mid: bool,
    live_mark: bool,
    live_metadata: bool,
    venue_max_leverage: u16,
}

impl MarketDataAvailability {
    /// Constructs point-in-time venue-state inputs for one native-perpetual candidate.
    #[must_use]
    pub const fn new(
        listing_state: ListingState,
        live_mid: bool,
        live_mark: bool,
        live_metadata: bool,
        venue_max_leverage: u16,
    ) -> Self {
        Self {
            listing_state,
            live_mid,
            live_mark,
            live_metadata,
            venue_max_leverage,
        }
    }

    /// Returns the market lifecycle state observed at the snapshot boundary.
    #[must_use]
    pub const fn listing_state(&self) -> ListingState {
        self.listing_state
    }

    /// Returns whether a live midpoint was available at the snapshot boundary.
    #[must_use]
    pub const fn has_live_mid(&self) -> bool {
        self.live_mid
    }

    /// Returns whether a live venue mark was available at the snapshot boundary.
    #[must_use]
    pub const fn has_live_mark(&self) -> bool {
        self.live_mark
    }

    /// Returns whether current venue metadata was available at the snapshot boundary.
    #[must_use]
    pub const fn has_live_metadata(&self) -> bool {
        self.live_metadata
    }

    /// Returns the venue-advertised maximum leverage.
    #[must_use]
    pub const fn venue_max_leverage(&self) -> u16 {
        self.venue_max_leverage
    }
}

/// Point-in-time local-history and detailed-feed quality inputs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HistoryQuality {
    usable_calendar_days: u16,
    trailing_seven_day_coverage: Decimal,
    detailed_feed_fresh: bool,
    feed_continuity: Decimal,
}

impl HistoryQuality {
    /// Creates checked local-history and feed-quality observations.
    ///
    /// # Errors
    ///
    /// Rejects coverage or continuity outside the inclusive unit interval.
    pub fn new(
        usable_calendar_days: u16,
        trailing_seven_day_coverage: Decimal,
        detailed_feed_fresh: bool,
        feed_continuity: Decimal,
    ) -> Result<Self, UniverseError> {
        validate_fraction(trailing_seven_day_coverage, "trailing_seven_day_coverage")?;
        validate_fraction(feed_continuity, "feed_continuity")?;
        Ok(Self {
            usable_calendar_days,
            trailing_seven_day_coverage,
            detailed_feed_fresh,
            feed_continuity,
        })
    }

    /// Returns complete local fifteen-minute history in calendar days.
    #[must_use]
    pub const fn usable_calendar_days(&self) -> u16 {
        self.usable_calendar_days
    }

    /// Returns exact trailing-seven-day required-bar coverage.
    #[must_use]
    pub const fn trailing_seven_day_coverage(&self) -> Decimal {
        self.trailing_seven_day_coverage
    }

    /// Returns whether the detailed market feed was fresh during the scoring window.
    #[must_use]
    pub const fn detailed_feed_fresh(&self) -> bool {
        self.detailed_feed_fresh
    }

    /// Returns the normalized feed-continuity metric used only in frozen ranking.
    #[must_use]
    pub const fn feed_continuity(&self) -> Decimal {
        self.feed_continuity
    }
}

/// Directional executable notional available inside 10, 25, and 50 basis points.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SidedDepth {
    at_10_bps: Usdc,
    at_25_bps: Usdc,
    at_50_bps: Usdc,
}

impl SidedDepth {
    /// Creates one monotonic directional executable-depth profile.
    ///
    /// # Errors
    ///
    /// Rejects a profile that loses visible executable notional as the permitted
    /// price band widens.
    pub fn new(at_10_bps: Usdc, at_25_bps: Usdc, at_50_bps: Usdc) -> Result<Self, UniverseError> {
        if at_10_bps > at_25_bps || at_25_bps > at_50_bps {
            return Err(UniverseError::NonMonotonicDepth {
                side: "directional",
            });
        }
        Ok(Self {
            at_10_bps,
            at_25_bps,
            at_50_bps,
        })
    }

    /// Returns executable notional inside 10 basis points.
    #[must_use]
    pub const fn at_10_bps(&self) -> Usdc {
        self.at_10_bps
    }

    /// Returns executable notional inside 25 basis points.
    #[must_use]
    pub const fn at_25_bps(&self) -> Usdc {
        self.at_25_bps
    }

    /// Returns executable notional inside 50 basis points.
    #[must_use]
    pub const fn at_50_bps(&self) -> Usdc {
        self.at_50_bps
    }
}

/// Bid and ask executable-depth profiles at one hourly boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DepthProfile {
    bid: SidedDepth,
    ask: SidedDepth,
}

impl DepthProfile {
    /// Combines already validated bid and ask depth profiles.
    #[must_use]
    pub const fn new(bid: SidedDepth, ask: SidedDepth) -> Self {
        Self { bid, ask }
    }

    /// Returns the sell-side executable profile.
    #[must_use]
    pub const fn bid(&self) -> &SidedDepth {
        &self.bid
    }

    /// Returns the buy-side executable profile.
    #[must_use]
    pub const fn ask(&self) -> &SidedDepth {
        &self.ask
    }

    fn balanced_depth_at_10_bps(&self) -> Usdc {
        let bid = self.bid.at_10_bps();
        let ask = self.ask.at_10_bps();
        if bid <= ask { bid } else { ask }
    }

    fn balanced_depth_at_25_bps(&self) -> Usdc {
        let bid = self.bid.at_25_bps();
        let ask = self.ask.at_25_bps();
        if bid <= ask { bid } else { ask }
    }

    fn balanced_depth_at_50_bps(&self) -> Usdc {
        let bid = self.bid.at_50_bps();
        let ask = self.ask.at_50_bps();
        if bid <= ask { bid } else { ask }
    }

    fn minimum_executable_50_bps(&self) -> Usdc {
        let bid = self.bid.at_50_bps();
        let ask = self.ask.at_50_bps();
        if bid <= ask { bid } else { ask }
    }
}

/// Point-in-time liquidity metrics used by the structurally independent selector.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UniverseLiquidity {
    trailing_day_notional: Usdc,
    open_interest_notional: Usdc,
    effective_spread: Bps,
    depth: DepthProfile,
}

impl UniverseLiquidity {
    /// Creates immutable hourly liquidity inputs.
    #[must_use]
    pub const fn new(
        trailing_day_notional: Usdc,
        open_interest_notional: Usdc,
        effective_spread: Bps,
        depth: DepthProfile,
    ) -> Self {
        Self {
            trailing_day_notional,
            open_interest_notional,
            effective_spread,
            depth,
        }
    }

    /// Returns trailing 24-hour native-perp notional volume.
    #[must_use]
    pub const fn trailing_day_notional(&self) -> Usdc {
        self.trailing_day_notional
    }

    /// Returns mark-notionalized open interest for cross-market comparison.
    #[must_use]
    pub const fn open_interest_notional(&self) -> Usdc {
        self.open_interest_notional
    }

    /// Returns the median effective spread measured in basis points.
    #[must_use]
    pub const fn effective_spread(&self) -> Bps {
        self.effective_spread
    }

    /// Returns both directional visible-depth profiles.
    #[must_use]
    pub const fn depth(&self) -> &DepthProfile {
        &self.depth
    }
}

/// All point-in-time inputs required to evaluate one discovered market.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UniverseCandidate {
    market: Market,
    native_perpetual: bool,
    availability: MarketDataAvailability,
    history: HistoryQuality,
    liquidity: UniverseLiquidity,
}

impl UniverseCandidate {
    /// Creates one immutable selector input. No strategy, position, or risk state is accepted.
    #[must_use]
    pub const fn new(
        market: Market,
        native_perpetual: bool,
        availability: MarketDataAvailability,
        history: HistoryQuality,
        liquidity: UniverseLiquidity,
    ) -> Self {
        Self {
            market,
            native_perpetual,
            availability,
            history,
            liquidity,
        }
    }

    /// Returns the discovered market identifier.
    #[must_use]
    pub const fn market(&self) -> &Market {
        &self.market
    }

    /// Returns whether discovery classified this as a native perpetual.
    #[must_use]
    pub const fn is_native_perpetual(&self) -> bool {
        self.native_perpetual
    }

    /// Returns current venue and feed availability inputs.
    #[must_use]
    pub const fn availability(&self) -> &MarketDataAvailability {
        &self.availability
    }

    /// Returns local history and feed-continuity inputs.
    #[must_use]
    pub const fn history(&self) -> &HistoryQuality {
        &self.history
    }

    /// Returns immutable hourly liquidity inputs.
    #[must_use]
    pub const fn liquidity(&self) -> &UniverseLiquidity {
        &self.liquidity
    }
}

/// Machine-readable reason a market is structurally excluded at one hourly boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum UniverseExclusionReason {
    /// Market is not a native perpetual.
    NotNativePerpetual,
    /// Market has been delisted by the venue.
    Delisted,
    /// Market is paused by the venue.
    Paused,
    /// No live midpoint was available.
    MissingLiveMid,
    /// No live mark was available.
    MissingLiveMark,
    /// No live metadata was available.
    MissingLiveMetadata,
    /// Venue maximum leverage is below the frozen five-times minimum.
    VenueMaxLeverageBelowMinimum,
    /// Local usable fifteen-minute history is under thirty calendar days.
    InsufficientLocalHistory,
    /// Trailing required-bar coverage is under 99.5 percent.
    InsufficientRequiredBarCoverage,
    /// Detailed market feed was not fresh during the scoring window.
    StaleDetailedFeed,
    /// Trailing 24-hour notional is under five million USDC.
    InsufficientDailyNotional,
    /// Median effective spread exceeds fifteen basis points.
    ExcessiveEffectiveSpread,
    /// One direction has less than 100 times the fixed 500-USDC probe inside 50 bps.
    InsufficientExecutableDepth,
}

impl UniverseExclusionReason {
    const fn tag(self) -> u8 {
        match self {
            Self::NotNativePerpetual => 0,
            Self::Delisted => 1,
            Self::Paused => 2,
            Self::MissingLiveMid => 3,
            Self::MissingLiveMark => 4,
            Self::MissingLiveMetadata => 5,
            Self::VenueMaxLeverageBelowMinimum => 6,
            Self::InsufficientLocalHistory => 7,
            Self::InsufficientRequiredBarCoverage => 8,
            Self::StaleDetailedFeed => 9,
            Self::InsufficientDailyNotional => 10,
            Self::ExcessiveEffectiveSpread => 11,
            Self::InsufficientExecutableDepth => 12,
        }
    }
}

/// A market's frozen membership at one completed hourly universe snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Membership {
    /// Ranks one through twenty; eligible for strategy cross-sections after activation.
    Tradeable,
    /// Ranks twenty-one through thirty; detailed feeds stay warm but strategies cannot trade it.
    Warm,
    /// Structurally excluded or ranked below the warm buffer.
    Absent,
}

impl Membership {
    const fn tag(self) -> u8 {
        match self {
            Self::Tradeable => 0,
            Self::Warm => 1,
            Self::Absent => 2,
        }
    }
}

/// Frozen score components for one structurally eligible market.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LiquidityScore {
    volume_percentile: Decimal,
    open_interest_percentile: Decimal,
    inverse_spread_percentile: Decimal,
    depth_percentile: Decimal,
    continuity_percentile: Decimal,
    total: Decimal,
}

impl LiquidityScore {
    /// Returns the frozen total robust-liquidity score.
    #[must_use]
    pub const fn total(&self) -> Decimal {
        self.total
    }

    /// Returns the trailing-notional cross-sectional percentile.
    #[must_use]
    pub const fn volume_percentile(&self) -> Decimal {
        self.volume_percentile
    }

    /// Returns the open-interest cross-sectional percentile.
    #[must_use]
    pub const fn open_interest_percentile(&self) -> Decimal {
        self.open_interest_percentile
    }

    /// Returns the lower-spread-is-better cross-sectional percentile.
    #[must_use]
    pub const fn inverse_spread_percentile(&self) -> Decimal {
        self.inverse_spread_percentile
    }

    /// Returns the balanced executable-depth cross-sectional percentile.
    #[must_use]
    pub const fn depth_percentile(&self) -> Decimal {
        self.depth_percentile
    }

    /// Returns the detailed-feed continuity cross-sectional percentile.
    #[must_use]
    pub const fn continuity_percentile(&self) -> Decimal {
        self.continuity_percentile
    }
}

/// Complete point-in-time selection result for one discovered market.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UniverseEntry {
    candidate: UniverseCandidate,
    exclusion_reasons: Vec<UniverseExclusionReason>,
    score: Option<LiquidityScore>,
    rank: Option<u64>,
    membership: Membership,
}

impl UniverseEntry {
    /// Returns all observed metric inputs, including inputs that failed a hard gate.
    #[must_use]
    pub const fn candidate(&self) -> &UniverseCandidate {
        &self.candidate
    }

    /// Returns the market identifier.
    #[must_use]
    pub const fn market(&self) -> &Market {
        self.candidate.market()
    }

    /// Returns every independent hard-gate exclusion at this hour.
    #[must_use]
    pub fn exclusion_reasons(&self) -> &[UniverseExclusionReason] {
        &self.exclusion_reasons
    }

    /// Returns frozen percentile inputs and total score for an eligible market.
    #[must_use]
    pub const fn liquidity_score(&self) -> Option<LiquidityScore> {
        self.score
    }

    /// Returns the robust-liquidity score when structural gates passed.
    #[must_use]
    pub const fn score(&self) -> Option<Decimal> {
        match self.score {
            Some(score) => Some(score.total()),
            None => None,
        }
    }

    /// Returns one-based liquidity rank, including rank above the warm buffer.
    #[must_use]
    pub const fn rank(&self) -> Option<u64> {
        self.rank
    }

    /// Returns strategy membership at this completed hourly snapshot.
    #[must_use]
    pub const fn membership(&self) -> Membership {
        self.membership
    }

    fn is_structurally_eligible(&self) -> bool {
        self.exclusion_reasons.is_empty()
    }
}

/// Complete immutable discovery result at one explicit completed UTC hour.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UniverseSnapshot {
    as_of_time: TimestampNs,
    entries: BTreeMap<Market, UniverseEntry>,
    digest: String,
}

/// Opaque selector transition output bound to its complete frozen source snapshot.
///
/// This proof object is deliberately constructed only by [`UniverseSelector::activate`].
/// Replays persist and validate `UniverseSnapshot` values, then reconstruct this short-lived
/// transition state in order; a detached `TradeableUniverse` cannot be supplied as prior state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UniverseActivation {
    snapshot: UniverseSnapshot,
    snapshot_digest: String,
    universe: Option<TradeableUniverse>,
}

impl UniverseActivation {
    fn new(snapshot: UniverseSnapshot, universe: Option<TradeableUniverse>) -> Self {
        Self {
            snapshot_digest: snapshot.digest().to_owned(),
            snapshot,
            universe,
        }
    }

    /// Returns the frozen source snapshot boundary that produced this transition output.
    #[must_use]
    pub const fn snapshot_time(&self) -> TimestampNs {
        self.snapshot.as_of_time()
    }

    /// Returns the frozen source snapshot digest used to prove transition provenance.
    #[must_use]
    pub fn snapshot_digest(&self) -> &str {
        &self.snapshot_digest
    }

    /// Returns the active rank-1-to-20 membership, if any remains after hard removals.
    #[must_use]
    pub const fn universe(&self) -> Option<&TradeableUniverse> {
        self.universe.as_ref()
    }

    fn validate_provenance(&self) -> Result<(), UniverseError> {
        if self.snapshot_digest != self.snapshot.digest()
            || self.snapshot_digest
                != snapshot_digest(self.snapshot.as_of_time(), &self.snapshot.entries)
        {
            return Err(UniverseError::PriorSnapshotDigestMismatch {
                snapshot_time: self.snapshot.as_of_time(),
            });
        }
        if let Some(universe) = &self.universe
            && universe.as_of_time() != self.snapshot.as_of_time()
        {
            return Err(UniverseError::PriorMembershipBoundaryMismatch {
                snapshot_time: self.snapshot.as_of_time(),
                membership_time: universe.as_of_time(),
            });
        }
        Ok(())
    }
}

impl UniverseSnapshot {
    /// Returns the exact completed hourly boundary used for all inputs and ranks.
    #[must_use]
    pub const fn as_of_time(&self) -> TimestampNs {
        self.as_of_time
    }

    /// Returns all entries in deterministic market-symbol order.
    pub fn entries(&self) -> impl Iterator<Item = &UniverseEntry> {
        self.entries.values()
    }

    /// Looks up a complete selection record by market symbol.
    #[must_use]
    pub fn entry(&self, market: &Market) -> Option<&UniverseEntry> {
        self.entries.get(market)
    }

    /// Returns the frozen content digest over all inputs, exclusions, scores, and memberships.
    #[must_use]
    pub fn digest(&self) -> &str {
        &self.digest
    }

    fn to_wire(&self) -> UniverseSnapshotWire {
        UniverseSnapshotWire {
            as_of_time_ns: self.as_of_time.value(),
            entries: self
                .entries
                .values()
                .map(UniverseEntryWire::from_entry)
                .collect(),
            digest: self.digest.clone(),
        }
    }

    fn from_wire(wire: UniverseSnapshotWire) -> Result<Self, UniverseError> {
        let as_of_time = TimestampNs::new(i128::from(wire.as_of_time_ns)).map_err(|error| {
            UniverseError::InvalidSnapshotWire {
                field: "as_of_time_ns",
                message: error.to_string(),
            }
        })?;
        let candidates = wire
            .entries
            .iter()
            .map(UniverseEntryWire::to_candidate)
            .collect::<Result<Vec<_>, _>>()?;
        let snapshot = UniverseSelector::select(as_of_time, candidates)?;
        if snapshot.digest != wire.digest {
            return Err(UniverseError::DigestMismatch);
        }
        let canonical = snapshot.to_wire();
        if canonical.entries != wire.entries {
            return Err(UniverseError::SnapshotContentMismatch);
        }
        Ok(snapshot)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct UniverseSnapshotWire {
    as_of_time_ns: i64,
    entries: Vec<UniverseEntryWire>,
    digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct UniverseEntryWire {
    market: String,
    native_perpetual: bool,
    listing_state: ListingStateWire,
    live_mid: bool,
    live_mark: bool,
    live_metadata: bool,
    venue_max_leverage: u16,
    usable_calendar_days: u16,
    trailing_seven_day_coverage: String,
    detailed_feed_fresh: bool,
    feed_continuity: String,
    trailing_day_notional_usdc: String,
    open_interest_notional_usdc: String,
    effective_spread_bps: String,
    bid_depth: SidedDepthWire,
    ask_depth: SidedDepthWire,
    exclusion_reasons: Vec<UniverseExclusionReasonWire>,
    score: Option<LiquidityScoreWire>,
    rank: Option<u64>,
    membership: MembershipWire,
}

impl UniverseEntryWire {
    fn from_entry(entry: &UniverseEntry) -> Self {
        let candidate = entry.candidate();
        let availability = candidate.availability();
        let history = candidate.history();
        let liquidity = candidate.liquidity();
        Self {
            market: candidate.market().as_str().to_owned(),
            native_perpetual: candidate.is_native_perpetual(),
            listing_state: availability.listing_state().into(),
            live_mid: availability.has_live_mid(),
            live_mark: availability.has_live_mark(),
            live_metadata: availability.has_live_metadata(),
            venue_max_leverage: availability.venue_max_leverage(),
            usable_calendar_days: history.usable_calendar_days(),
            trailing_seven_day_coverage: canonical_decimal(history.trailing_seven_day_coverage()),
            detailed_feed_fresh: history.detailed_feed_fresh(),
            feed_continuity: canonical_decimal(history.feed_continuity()),
            trailing_day_notional_usdc: canonical_decimal(
                liquidity.trailing_day_notional().value(),
            ),
            open_interest_notional_usdc: canonical_decimal(
                liquidity.open_interest_notional().value(),
            ),
            effective_spread_bps: canonical_decimal(liquidity.effective_spread().value()),
            bid_depth: SidedDepthWire::from_depth(liquidity.depth().bid()),
            ask_depth: SidedDepthWire::from_depth(liquidity.depth().ask()),
            exclusion_reasons: entry
                .exclusion_reasons()
                .iter()
                .copied()
                .map(Into::into)
                .collect(),
            score: entry.liquidity_score().map(LiquidityScoreWire::from_score),
            rank: entry.rank(),
            membership: entry.membership().into(),
        }
    }

    fn to_candidate(&self) -> Result<UniverseCandidate, UniverseError> {
        let market = Market::new(self.market.clone())
            .map_err(|error| invalid_snapshot_wire("market", error))?;
        let availability = MarketDataAvailability::new(
            self.listing_state.into(),
            self.live_mid,
            self.live_mark,
            self.live_metadata,
            self.venue_max_leverage,
        );
        let history = HistoryQuality::new(
            self.usable_calendar_days,
            parse_canonical_decimal(
                &self.trailing_seven_day_coverage,
                "trailing_seven_day_coverage",
            )?,
            self.detailed_feed_fresh,
            parse_canonical_decimal(&self.feed_continuity, "feed_continuity")?,
        )?;
        let liquidity = UniverseLiquidity::new(
            Usdc::new(parse_canonical_decimal(
                &self.trailing_day_notional_usdc,
                "trailing_day_notional_usdc",
            )?)
            .map_err(|error| invalid_snapshot_wire("trailing_day_notional_usdc", error))?,
            Usdc::new(parse_canonical_decimal(
                &self.open_interest_notional_usdc,
                "open_interest_notional_usdc",
            )?)
            .map_err(|error| invalid_snapshot_wire("open_interest_notional_usdc", error))?,
            Bps::new(parse_canonical_decimal(
                &self.effective_spread_bps,
                "effective_spread_bps",
            )?)
            .map_err(|error| invalid_snapshot_wire("effective_spread_bps", error))?,
            DepthProfile::new(
                self.bid_depth.to_depth("bid_depth")?,
                self.ask_depth.to_depth("ask_depth")?,
            ),
        );
        Ok(UniverseCandidate::new(
            market,
            self.native_perpetual,
            availability,
            history,
            liquidity,
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SidedDepthWire {
    at_10_bps_usdc: String,
    at_25_bps_usdc: String,
    at_50_bps_usdc: String,
}

impl SidedDepthWire {
    fn from_depth(depth: &SidedDepth) -> Self {
        Self {
            at_10_bps_usdc: canonical_decimal(depth.at_10_bps().value()),
            at_25_bps_usdc: canonical_decimal(depth.at_25_bps().value()),
            at_50_bps_usdc: canonical_decimal(depth.at_50_bps().value()),
        }
    }

    fn to_depth(&self, field: &'static str) -> Result<SidedDepth, UniverseError> {
        SidedDepth::new(
            Usdc::new(parse_canonical_decimal(&self.at_10_bps_usdc, field)?)
                .map_err(|error| invalid_snapshot_wire(field, error))?,
            Usdc::new(parse_canonical_decimal(&self.at_25_bps_usdc, field)?)
                .map_err(|error| invalid_snapshot_wire(field, error))?,
            Usdc::new(parse_canonical_decimal(&self.at_50_bps_usdc, field)?)
                .map_err(|error| invalid_snapshot_wire(field, error))?,
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ListingStateWire {
    Active,
    Delisted,
    Paused,
}

impl From<ListingState> for ListingStateWire {
    fn from(value: ListingState) -> Self {
        match value {
            ListingState::Active => Self::Active,
            ListingState::Delisted => Self::Delisted,
            ListingState::Paused => Self::Paused,
        }
    }
}

impl From<ListingStateWire> for ListingState {
    fn from(value: ListingStateWire) -> Self {
        match value {
            ListingStateWire::Active => Self::Active,
            ListingStateWire::Delisted => Self::Delisted,
            ListingStateWire::Paused => Self::Paused,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum UniverseExclusionReasonWire {
    NotNativePerpetual,
    Delisted,
    Paused,
    MissingLiveMid,
    MissingLiveMark,
    MissingLiveMetadata,
    VenueMaxLeverageBelowMinimum,
    InsufficientLocalHistory,
    InsufficientRequiredBarCoverage,
    StaleDetailedFeed,
    InsufficientDailyNotional,
    ExcessiveEffectiveSpread,
    InsufficientExecutableDepth,
}

impl From<UniverseExclusionReason> for UniverseExclusionReasonWire {
    fn from(value: UniverseExclusionReason) -> Self {
        match value {
            UniverseExclusionReason::NotNativePerpetual => Self::NotNativePerpetual,
            UniverseExclusionReason::Delisted => Self::Delisted,
            UniverseExclusionReason::Paused => Self::Paused,
            UniverseExclusionReason::MissingLiveMid => Self::MissingLiveMid,
            UniverseExclusionReason::MissingLiveMark => Self::MissingLiveMark,
            UniverseExclusionReason::MissingLiveMetadata => Self::MissingLiveMetadata,
            UniverseExclusionReason::VenueMaxLeverageBelowMinimum => {
                Self::VenueMaxLeverageBelowMinimum
            }
            UniverseExclusionReason::InsufficientLocalHistory => Self::InsufficientLocalHistory,
            UniverseExclusionReason::InsufficientRequiredBarCoverage => {
                Self::InsufficientRequiredBarCoverage
            }
            UniverseExclusionReason::StaleDetailedFeed => Self::StaleDetailedFeed,
            UniverseExclusionReason::InsufficientDailyNotional => Self::InsufficientDailyNotional,
            UniverseExclusionReason::ExcessiveEffectiveSpread => Self::ExcessiveEffectiveSpread,
            UniverseExclusionReason::InsufficientExecutableDepth => {
                Self::InsufficientExecutableDepth
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum MembershipWire {
    Tradeable,
    Warm,
    Absent,
}

impl From<Membership> for MembershipWire {
    fn from(value: Membership) -> Self {
        match value {
            Membership::Tradeable => Self::Tradeable,
            Membership::Warm => Self::Warm,
            Membership::Absent => Self::Absent,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LiquidityScoreWire {
    volume_percentile: String,
    open_interest_percentile: String,
    inverse_spread_percentile: String,
    depth_percentile: String,
    continuity_percentile: String,
    total: String,
}

impl LiquidityScoreWire {
    fn from_score(score: LiquidityScore) -> Self {
        Self {
            volume_percentile: canonical_decimal(score.volume_percentile()),
            open_interest_percentile: canonical_decimal(score.open_interest_percentile()),
            inverse_spread_percentile: canonical_decimal(score.inverse_spread_percentile()),
            depth_percentile: canonical_decimal(score.depth_percentile()),
            continuity_percentile: canonical_decimal(score.continuity_percentile()),
            total: canonical_decimal(score.total()),
        }
    }
}

fn canonical_decimal(value: Decimal) -> String {
    value.normalize().to_string()
}

fn parse_canonical_decimal(value: &str, field: &'static str) -> Result<Decimal, UniverseError> {
    let decimal = Decimal::from_str(value).map_err(|error| invalid_snapshot_wire(field, error))?;
    if canonical_decimal(decimal) != value {
        return Err(UniverseError::InvalidSnapshotWire {
            field,
            message: "decimal must use canonical normalized syntax".to_owned(),
        });
    }
    Ok(decimal)
}

fn invalid_snapshot_wire(field: &'static str, error: impl std::fmt::Display) -> UniverseError {
    UniverseError::InvalidSnapshotWire {
        field,
        message: error.to_string(),
    }
}

impl Serialize for UniverseSnapshot {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.to_wire().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for UniverseSnapshot {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = UniverseSnapshotWire::deserialize(deserializer)?;
        Self::from_wire(wire).map_err(serde::de::Error::custom)
    }
}

/// Stateless deterministic selector for the dynamically ranked native-perp universe.
#[derive(Debug, Default, Clone, Copy)]
pub struct UniverseSelector;

impl UniverseSelector {
    /// Freezes all structural eligibility, percentile inputs, ranks, and memberships at one hour.
    ///
    /// # Errors
    ///
    /// Rejects non-hourly decision boundaries and duplicate market inputs.
    pub fn select(
        as_of_time: TimestampNs,
        candidates: impl IntoIterator<Item = UniverseCandidate>,
    ) -> Result<UniverseSnapshot, UniverseError> {
        if as_of_time.value() % HOUR_NS != 0 {
            return Err(UniverseError::NotCompletedHour { as_of_time });
        }
        let mut candidates_by_market = BTreeMap::new();
        for candidate in candidates {
            let market = candidate.market().clone();
            if candidates_by_market
                .insert(market.clone(), candidate)
                .is_some()
            {
                return Err(UniverseError::DuplicateMarket { market });
            }
        }

        let exclusions = candidates_by_market
            .iter()
            .map(|(market, candidate)| Ok((market.clone(), exclusion_reasons(candidate)?)))
            .collect::<Result<BTreeMap<_, _>, UniverseError>>()?;
        let eligible = candidates_by_market
            .values()
            .filter(|candidate| {
                exclusions
                    .get(candidate.market())
                    .is_some_and(Vec::is_empty)
            })
            .collect::<Vec<_>>();
        let scores = score_eligible(&eligible)?;
        let mut ranked = scores
            .iter()
            .map(|(market, score)| (market.clone(), *score))
            .collect::<Vec<_>>();
        ranked.sort_by(|left, right| {
            right
                .1
                .total()
                .cmp(&left.1.total())
                .then_with(|| left.0.cmp(&right.0))
        });
        let mut ranks = BTreeMap::new();
        for (index, (market, _)) in ranked.iter().enumerate() {
            let rank = u64::try_from(index)
                .ok()
                .and_then(|index| index.checked_add(1))
                .ok_or(UniverseError::Arithmetic {
                    operation: "universe rank",
                })?;
            ranks.insert(market.clone(), rank);
        }

        let entries = candidates_by_market
            .into_iter()
            .map(|(market, candidate)| {
                let exclusion_reasons = exclusions
                    .get(&market)
                    .cloned()
                    .expect("candidate exclusions are collected from the same map");
                let score = scores.get(&market).copied();
                let rank = ranks.get(&market).copied();
                let membership = rank.map_or(Membership::Absent, membership_for_rank);
                (
                    market,
                    UniverseEntry {
                        candidate,
                        exclusion_reasons,
                        score,
                        rank,
                        membership,
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        let digest = snapshot_digest(as_of_time, &entries);
        Ok(UniverseSnapshot {
            as_of_time,
            entries,
            digest,
        })
    }

    /// Activates a frozen hourly snapshot for one completed fifteen-minute strategy bar.
    ///
    /// At the snapshot hour, structural failures remove current members immediately.
    /// Rank additions and removals activate only at the next completed fifteen-minute
    /// bar, so strategy decisions never use a partially refreshed cross-section.
    ///
    /// # Errors
    ///
    /// Rejects non-bar boundaries, stale snapshots, and prior state that lacks exact selector
    /// snapshot provenance for the preceding hour.
    pub fn activate(
        snapshot: &UniverseSnapshot,
        current: Option<&UniverseActivation>,
        decision_time: TimestampNs,
    ) -> Result<UniverseActivation, UniverseError> {
        if decision_time.value() % STRATEGY_BAR_NS != 0 {
            return Err(UniverseError::NotCompletedStrategyBar { decision_time });
        }
        if decision_time < snapshot.as_of_time() {
            return Err(UniverseError::DecisionBeforeSnapshot {
                decision_time,
                snapshot_time: snapshot.as_of_time(),
            });
        }
        let next_hour = snapshot.as_of_time().value().checked_add(HOUR_NS).ok_or(
            UniverseError::Arithmetic {
                operation: "next universe refresh boundary",
            },
        )?;
        if decision_time.value() >= next_hour {
            return Err(UniverseError::SnapshotNotCurrent {
                snapshot_time: snapshot.as_of_time(),
                decision_time,
            });
        }
        if let Some(current) = current {
            let expected_prior_snapshot_time = exact_prior_hour(snapshot.as_of_time())?;
            current.validate_provenance()?;
            if current.snapshot_time() != expected_prior_snapshot_time {
                return Err(UniverseError::PriorSnapshotBoundaryMismatch {
                    snapshot_time: snapshot.as_of_time(),
                    expected_snapshot_time: expected_prior_snapshot_time,
                    actual_snapshot_time: current.snapshot_time(),
                });
            }
        }
        let next_activation = snapshot
            .as_of_time()
            .value()
            .checked_add(STRATEGY_BAR_NS)
            .ok_or(UniverseError::Arithmetic {
                operation: "next universe activation boundary",
            })?;
        let universe = if decision_time.value() < next_activation {
            let retained = current
                .into_iter()
                .flat_map(|activation| activation.universe().into_iter())
                .flat_map(TradeableUniverse::markets)
                .filter(|market| {
                    snapshot
                        .entry(market)
                        .is_some_and(UniverseEntry::is_structurally_eligible)
                })
                .cloned()
                .collect::<Vec<_>>();
            frozen_membership(snapshot.as_of_time(), retained)?
        } else {
            let ranked = snapshot
                .entries()
                .filter(|entry| entry.membership() == Membership::Tradeable)
                .map(|entry| entry.market().clone())
                .collect::<Vec<_>>();
            frozen_membership(snapshot.as_of_time(), ranked)?
        };
        Ok(UniverseActivation::new(snapshot.clone(), universe))
    }
}

fn exact_prior_hour(snapshot_time: TimestampNs) -> Result<TimestampNs, UniverseError> {
    TimestampNs::new(i128::from(snapshot_time.value()) - i128::from(HOUR_NS)).map_err(|_| {
        UniverseError::Arithmetic {
            operation: "prior universe refresh boundary",
        }
    })
}

fn validate_fraction(value: Decimal, field: &'static str) -> Result<(), UniverseError> {
    if !(Decimal::ZERO..=Decimal::ONE).contains(&value) {
        return Err(UniverseError::InvalidFraction { field });
    }
    Ok(())
}

fn exclusion_reasons(
    candidate: &UniverseCandidate,
) -> Result<Vec<UniverseExclusionReason>, UniverseError> {
    let mut reasons = Vec::new();
    if !candidate.is_native_perpetual() {
        reasons.push(UniverseExclusionReason::NotNativePerpetual);
    }
    match candidate.availability().listing_state() {
        ListingState::Active => {}
        ListingState::Delisted => reasons.push(UniverseExclusionReason::Delisted),
        ListingState::Paused => reasons.push(UniverseExclusionReason::Paused),
    }
    if !candidate.availability().has_live_mid() {
        reasons.push(UniverseExclusionReason::MissingLiveMid);
    }
    if !candidate.availability().has_live_mark() {
        reasons.push(UniverseExclusionReason::MissingLiveMark);
    }
    if !candidate.availability().has_live_metadata() {
        reasons.push(UniverseExclusionReason::MissingLiveMetadata);
    }
    if candidate.availability().venue_max_leverage() < MINIMUM_VENUE_LEVERAGE {
        reasons.push(UniverseExclusionReason::VenueMaxLeverageBelowMinimum);
    }
    if candidate.history().usable_calendar_days() < REQUIRED_HISTORY_DAYS {
        reasons.push(UniverseExclusionReason::InsufficientLocalHistory);
    }
    if candidate.history().trailing_seven_day_coverage() < REQUIRED_BAR_COVERAGE {
        reasons.push(UniverseExclusionReason::InsufficientRequiredBarCoverage);
    }
    if !candidate.history().detailed_feed_fresh() {
        reasons.push(UniverseExclusionReason::StaleDetailedFeed);
    }
    if candidate.liquidity().trailing_day_notional().value() < MINIMUM_DAILY_NOTIONAL_USDC {
        reasons.push(UniverseExclusionReason::InsufficientDailyNotional);
    }
    if candidate.liquidity().effective_spread().value() > MAX_EFFECTIVE_SPREAD_BPS {
        reasons.push(UniverseExclusionReason::ExcessiveEffectiveSpread);
    }
    if candidate
        .liquidity()
        .depth()
        .minimum_executable_50_bps()
        .value()
        < minimum_executable_depth_usdc()?
    {
        reasons.push(UniverseExclusionReason::InsufficientExecutableDepth);
    }
    Ok(reasons)
}

fn minimum_executable_depth_usdc() -> Result<Decimal, UniverseError> {
    FIXED_DEPTH_PROBE_USDC
        .checked_mul(MINIMUM_DEPTH_MULTIPLE)
        .ok_or(UniverseError::Arithmetic {
            operation: "minimum executable depth threshold",
        })
}

fn score_eligible(
    eligible: &[&UniverseCandidate],
) -> Result<BTreeMap<Market, LiquidityScore>, UniverseError> {
    let volumes = eligible
        .iter()
        .map(|candidate| candidate.liquidity().trailing_day_notional().value())
        .collect::<Vec<_>>();
    let open_interest = eligible
        .iter()
        .map(|candidate| candidate.liquidity().open_interest_notional().value())
        .collect::<Vec<_>>();
    let spreads = eligible
        .iter()
        .map(|candidate| candidate.liquidity().effective_spread().value())
        .collect::<Vec<_>>();
    let depth_at_10_bps = eligible
        .iter()
        .map(|candidate| {
            candidate
                .liquidity()
                .depth()
                .balanced_depth_at_10_bps()
                .value()
        })
        .collect::<Vec<_>>();
    let depth_at_25_bps = eligible
        .iter()
        .map(|candidate| {
            candidate
                .liquidity()
                .depth()
                .balanced_depth_at_25_bps()
                .value()
        })
        .collect::<Vec<_>>();
    let depth_at_50_bps = eligible
        .iter()
        .map(|candidate| {
            candidate
                .liquidity()
                .depth()
                .balanced_depth_at_50_bps()
                .value()
        })
        .collect::<Vec<_>>();
    let continuity = eligible
        .iter()
        .map(|candidate| candidate.history().feed_continuity())
        .collect::<Vec<_>>();
    eligible
        .iter()
        .map(|candidate| {
            let volume_percentile = percentile(
                &volumes,
                candidate.liquidity().trailing_day_notional().value(),
                true,
            );
            let open_interest_percentile = percentile(
                &open_interest,
                candidate.liquidity().open_interest_notional().value(),
                true,
            );
            let inverse_spread_percentile = percentile(
                &spreads,
                candidate.liquidity().effective_spread().value(),
                false,
            );
            let depth_percentile = mean_percentiles([
                percentile(
                    &depth_at_10_bps,
                    candidate
                        .liquidity()
                        .depth()
                        .balanced_depth_at_10_bps()
                        .value(),
                    true,
                ),
                percentile(
                    &depth_at_25_bps,
                    candidate
                        .liquidity()
                        .depth()
                        .balanced_depth_at_25_bps()
                        .value(),
                    true,
                ),
                percentile(
                    &depth_at_50_bps,
                    candidate
                        .liquidity()
                        .depth()
                        .balanced_depth_at_50_bps()
                        .value(),
                    true,
                ),
            ])?;
            let continuity_percentile =
                percentile(&continuity, candidate.history().feed_continuity(), true);
            let total = weighted_total([
                (VOLUME_WEIGHT, volume_percentile),
                (OPEN_INTEREST_WEIGHT, open_interest_percentile),
                (INVERSE_SPREAD_WEIGHT, inverse_spread_percentile),
                (DEPTH_WEIGHT, depth_percentile),
                (CONTINUITY_WEIGHT, continuity_percentile),
            ])?;
            Ok((
                candidate.market().clone(),
                LiquidityScore {
                    volume_percentile,
                    open_interest_percentile,
                    inverse_spread_percentile,
                    depth_percentile,
                    continuity_percentile,
                    total,
                },
            ))
        })
        .collect()
}

fn percentile(values: &[Decimal], value: Decimal, higher_is_better: bool) -> Decimal {
    debug_assert!(!values.is_empty());
    let mut ordered = values.to_vec();
    ordered.sort_unstable();
    let tie_start = ordered.partition_point(|candidate| *candidate < value);
    let tie_end = ordered.partition_point(|candidate| *candidate <= value);
    let count = ordered.len();
    let numerator = if higher_is_better {
        tie_start + tie_end + 1
    } else {
        count
            .checked_mul(2)
            .and_then(|twice_count| twice_count.checked_sub(tie_start + tie_end))
            .and_then(|value| value.checked_add(1))
            .expect("eligible cross-section size is bounded by addressable memory")
    };
    Decimal::from(numerator) / Decimal::from(count * 2)
}

fn weighted_total(parts: [(Decimal, Decimal); 5]) -> Result<Decimal, UniverseError> {
    parts
        .into_iter()
        .try_fold(Decimal::ZERO, |total, (weight, percentile)| {
            let weighted = weight
                .checked_mul(percentile)
                .ok_or(UniverseError::Arithmetic {
                    operation: "liquidity score weight",
                })?;
            total
                .checked_add(weighted)
                .ok_or(UniverseError::Arithmetic {
                    operation: "liquidity score total",
                })
        })
}

fn mean_percentiles(percentiles: [Decimal; 3]) -> Result<Decimal, UniverseError> {
    let sum = percentiles
        .into_iter()
        .try_fold(Decimal::ZERO, |total, value| {
            total.checked_add(value).ok_or(UniverseError::Arithmetic {
                operation: "depth percentile mean",
            })
        })?;
    sum.checked_div(Decimal::from(3))
        .ok_or(UniverseError::Arithmetic {
            operation: "depth percentile mean",
        })
}

fn membership_for_rank(rank: u64) -> Membership {
    if rank <= MAX_TRADEABLE_MARKETS as u64 {
        Membership::Tradeable
    } else if rank <= (MAX_TRADEABLE_MARKETS + WARM_BUFFER_MARKETS) as u64 {
        Membership::Warm
    } else {
        Membership::Absent
    }
}

fn frozen_membership(
    as_of_time: TimestampNs,
    markets: impl IntoIterator<Item = Market>,
) -> Result<Option<TradeableUniverse>, UniverseError> {
    let markets = markets.into_iter().collect::<BTreeSet<_>>();
    if markets.is_empty() {
        return Ok(None);
    }
    TradeableUniverse::new(as_of_time, markets).map(Some)
}

fn snapshot_digest(as_of_time: TimestampNs, entries: &BTreeMap<Market, UniverseEntry>) -> String {
    let mut hasher = Hasher::new_derive_key("trench.universe-snapshot.v1");
    hasher.update(&as_of_time.value().to_be_bytes());
    hasher.update(&FIXED_DEPTH_PROBE_USDC.serialize());
    hasher.update(&MINIMUM_DEPTH_MULTIPLE.serialize());
    for entry in entries.values() {
        let candidate = entry.candidate();
        hasher.update(candidate.market().as_str().as_bytes());
        hasher.update(&[0]);
        hasher.update(&[u8::from(candidate.is_native_perpetual())]);
        hasher.update(&[match candidate.availability().listing_state() {
            ListingState::Active => 0,
            ListingState::Delisted => 1,
            ListingState::Paused => 2,
        }]);
        hasher.update(&[
            u8::from(candidate.availability().has_live_mid()),
            u8::from(candidate.availability().has_live_mark()),
            u8::from(candidate.availability().has_live_metadata()),
        ]);
        hasher.update(&candidate.availability().venue_max_leverage().to_be_bytes());
        hasher.update(&candidate.history().usable_calendar_days().to_be_bytes());
        hash_decimal(
            &mut hasher,
            candidate.history().trailing_seven_day_coverage(),
        );
        hasher.update(&[u8::from(candidate.history().detailed_feed_fresh())]);
        hash_decimal(&mut hasher, candidate.history().feed_continuity());
        hash_decimal(
            &mut hasher,
            candidate.liquidity().trailing_day_notional().value(),
        );
        hash_decimal(
            &mut hasher,
            candidate.liquidity().open_interest_notional().value(),
        );
        hash_decimal(
            &mut hasher,
            candidate.liquidity().effective_spread().value(),
        );
        for depth in [
            candidate.liquidity().depth().bid(),
            candidate.liquidity().depth().ask(),
        ] {
            hash_decimal(&mut hasher, depth.at_10_bps().value());
            hash_decimal(&mut hasher, depth.at_25_bps().value());
            hash_decimal(&mut hasher, depth.at_50_bps().value());
        }
        for reason in entry.exclusion_reasons() {
            hasher.update(&[reason.tag()]);
        }
        hasher.update(&[255]);
        match entry.liquidity_score() {
            Some(score) => {
                hasher.update(&[1]);
                for value in [
                    score.volume_percentile(),
                    score.open_interest_percentile(),
                    score.inverse_spread_percentile(),
                    score.depth_percentile(),
                    score.continuity_percentile(),
                    score.total(),
                ] {
                    hash_decimal(&mut hasher, value);
                }
            }
            None => {
                hasher.update(&[0]);
            }
        }
        hasher.update(&entry.rank().unwrap_or(0).to_be_bytes());
        hasher.update(&[entry.membership().tag()]);
    }
    hasher.finalize().to_hex().to_string()
}

fn hash_decimal(hasher: &mut Hasher, value: Decimal) {
    hasher.update(value.normalize().to_string().as_bytes());
    hasher.update(&[0]);
}

/// Immutable, serializable membership at one explicit UTC decision boundary.
///
/// It intentionally carries only the rank-1-to-20 tradeable set; warm-only or
/// absent markets cannot leak into a strategy cross-section through this type.
/// A membership alone is never valid prior state for [`UniverseSelector::activate`]; that
/// transition requires its opaque [`UniverseActivation`] provenance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TradeableUniverse {
    as_of_time: TimestampNs,
    markets: BTreeSet<Market>,
    digest: String,
}

impl TradeableUniverse {
    /// Creates a checked frozen tradeable universe.
    ///
    /// # Errors
    ///
    /// Rejects empty membership and more than the fixed rank-1-to-20 range.
    pub fn new(
        as_of_time: TimestampNs,
        markets: impl IntoIterator<Item = Market>,
    ) -> Result<Self, UniverseError> {
        if as_of_time.value() % HOUR_NS != 0 {
            return Err(UniverseError::NotCompletedHour { as_of_time });
        }
        let markets = markets.into_iter().collect::<BTreeSet<_>>();
        if markets.is_empty() {
            return Err(UniverseError::Empty);
        }
        if markets.len() > MAX_TRADEABLE_MARKETS {
            return Err(UniverseError::TooManyMarkets {
                actual: markets.len(),
                limit: MAX_TRADEABLE_MARKETS,
            });
        }
        Ok(Self {
            as_of_time,
            digest: universe_digest(as_of_time, &markets),
            markets,
        })
    }

    /// Returns the explicit completed boundary at which this membership froze.
    #[must_use]
    pub const fn as_of_time(&self) -> TimestampNs {
        self.as_of_time
    }

    /// Returns the ordered immutable rank-1-to-20 market membership.
    #[must_use]
    pub const fn markets(&self) -> &BTreeSet<Market> {
        &self.markets
    }

    /// Returns whether a market is eligible for cross-sectional ranks.
    #[must_use]
    pub fn contains(&self, market: &Market) -> bool {
        self.markets.contains(market)
    }

    /// Returns whether this is the sole valid completed-hour membership for a decision time.
    ///
    /// A Task 8 selector freezes exactly once per completed UTC hour. A 15-minute
    /// decision uses that snapshot through the next hourly refresh, never a
    /// future membership and never a snapshot older than the immediately
    /// preceding completed hour.
    #[must_use]
    pub const fn is_current_for(&self, decision_time: TimestampNs) -> bool {
        let delta = decision_time.value() - self.as_of_time.value();
        delta >= 0 && delta < HOUR_NS
    }

    /// Returns a stable content digest used in feature provenance and snapshot hashes.
    #[must_use]
    pub fn digest(&self) -> &str {
        &self.digest
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TradeableUniverseWire {
    as_of_time_ns: i64,
    markets: Vec<String>,
    digest: String,
}

impl Serialize for TradeableUniverse {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        TradeableUniverseWire {
            as_of_time_ns: self.as_of_time.value(),
            markets: self
                .markets
                .iter()
                .map(|market| market.as_str().to_owned())
                .collect(),
            digest: self.digest.clone(),
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for TradeableUniverse {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = TradeableUniverseWire::deserialize(deserializer)?;
        let as_of_time =
            TimestampNs::new(i128::from(wire.as_of_time_ns)).map_err(serde::de::Error::custom)?;
        let markets = wire
            .markets
            .into_iter()
            .map(Market::new)
            .collect::<Result<Vec<_>, _>>()
            .map_err(serde::de::Error::custom)?;
        let universe = Self::new(as_of_time, markets).map_err(serde::de::Error::custom)?;
        if universe.digest != wire.digest {
            return Err(serde::de::Error::custom(UniverseError::DigestMismatch));
        }
        Ok(universe)
    }
}

fn universe_digest(as_of_time: TimestampNs, markets: &BTreeSet<Market>) -> String {
    let mut hasher = Hasher::new_derive_key("trench.tradeable-universe.v1");
    hasher.update(&as_of_time.value().to_be_bytes());
    for market in markets {
        hasher.update(market.as_str().as_bytes());
        hasher.update(&[0]);
    }
    hasher.finalize().to_hex().to_string()
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;
    use serde_json::Value;

    use crate::domain::{Bps, Market, Usdc};
    use crate::event::TimestampNs;

    use super::{
        DepthProfile, HistoryQuality, ListingState, MarketDataAvailability, Membership, SidedDepth,
        TradeableUniverse, UniverseCandidate, UniverseError, UniverseExclusionReason,
        UniverseLiquidity, UniverseSelector,
    };

    const HOUR_NS: i128 = 3_600_000_000_000;
    const FIFTEEN_MINUTES_NS: i128 = 900_000_000_000;

    fn timestamp(value: i128) -> TimestampNs {
        TimestampNs::new(value).expect("timestamp must be valid")
    }

    fn decimal(value: u64) -> Decimal {
        Decimal::from(value)
    }

    fn state() -> MarketDataAvailability {
        MarketDataAvailability::new(ListingState::Active, true, true, true, 20)
    }

    fn history() -> HistoryQuality {
        HistoryQuality::new(30, dec!(0.995), true, dec!(1)).expect("history must be valid")
    }

    fn liquidity(index: u64) -> UniverseLiquidity {
        UniverseLiquidity::new(
            Usdc::new(decimal(5_000_000 + index)).expect("volume must be valid"),
            Usdc::new(decimal(1_000_000 + index)).expect("open interest must be valid"),
            Bps::new(dec!(15)).expect("spread must be valid"),
            DepthProfile::new(
                SidedDepth::new(
                    Usdc::new(decimal(50_000 + index)).expect("depth must be valid"),
                    Usdc::new(decimal(60_000 + index)).expect("depth must be valid"),
                    Usdc::new(decimal(70_000 + index)).expect("depth must be valid"),
                )
                .expect("buy depth must be valid"),
                SidedDepth::new(
                    Usdc::new(decimal(50_000 + index)).expect("depth must be valid"),
                    Usdc::new(decimal(60_000 + index)).expect("depth must be valid"),
                    Usdc::new(decimal(70_000 + index)).expect("depth must be valid"),
                )
                .expect("sell depth must be valid"),
            ),
        )
    }

    fn candidate(market: &str, index: u64) -> UniverseCandidate {
        UniverseCandidate::new(
            Market::new(market).expect("market must be valid"),
            true,
            state(),
            history(),
            liquidity(index),
        )
    }

    fn candidate_with_depth(
        market: &str,
        at_10_bps: u64,
        at_25_bps: u64,
        at_50_bps: u64,
    ) -> UniverseCandidate {
        UniverseCandidate::new(
            Market::new(market).expect("market must be valid"),
            true,
            state(),
            history(),
            UniverseLiquidity::new(
                Usdc::new(dec!(5000000)).expect("volume must be valid"),
                Usdc::new(dec!(1000000)).expect("open interest must be valid"),
                Bps::new(dec!(15)).expect("spread must be valid"),
                DepthProfile::new(
                    SidedDepth::new(
                        Usdc::new(decimal(at_10_bps)).expect("depth must be valid"),
                        Usdc::new(decimal(at_25_bps)).expect("depth must be valid"),
                        Usdc::new(decimal(at_50_bps)).expect("depth must be valid"),
                    )
                    .expect("bid depth must be valid"),
                    SidedDepth::new(
                        Usdc::new(decimal(at_10_bps)).expect("depth must be valid"),
                        Usdc::new(decimal(at_25_bps)).expect("depth must be valid"),
                        Usdc::new(decimal(at_50_bps)).expect("depth must be valid"),
                    )
                    .expect("ask depth must be valid"),
                ),
            ),
        )
    }

    #[test]
    fn round_trip_preserves_checked_membership_and_digest() {
        let universe = TradeableUniverse::new(
            TimestampNs::new(3_600_000_000_000).expect("timestamp must be valid"),
            [
                Market::new("ETH").expect("market must be valid"),
                Market::new("BTC").expect("market must be valid"),
            ],
        )
        .expect("membership must be valid");

        let encoded = serde_json::to_string(&universe).expect("universe must serialize");
        let decoded: TradeableUniverse =
            serde_json::from_str(&encoded).expect("universe must deserialize");
        assert_eq!(decoded, universe);
    }

    #[test]
    fn serialized_universe_rejects_unknown_wire_fields() {
        let universe = TradeableUniverse::new(
            TimestampNs::new(3_600_000_000_000).expect("timestamp must be valid"),
            [Market::new("BTC").expect("market must be valid")],
        )
        .expect("membership must be valid");
        let mut encoded = serde_json::to_value(universe).expect("universe must serialize");
        encoded["unexpected"] = serde_json::Value::Bool(true);

        assert!(
            serde_json::from_value::<TradeableUniverse>(encoded).is_err(),
            "unknown universe wire fields must fail closed"
        );
    }

    #[test]
    fn snapshot_round_trip_preserves_all_scored_and_excluded_inputs() {
        let snapshot = UniverseSelector::select(
            timestamp(HOUR_NS),
            [
                candidate("BTC", 1),
                UniverseCandidate::new(
                    Market::new("PAUSED").expect("market must be valid"),
                    true,
                    MarketDataAvailability::new(ListingState::Paused, true, true, true, 20),
                    history(),
                    liquidity(2),
                ),
            ],
        )
        .expect("selection must be valid");

        let encoded = serde_json::to_string(&snapshot).expect("snapshot must serialize");
        let decoded: super::UniverseSnapshot =
            serde_json::from_str(&encoded).expect("snapshot must deserialize");

        assert_eq!(decoded, snapshot);
        assert_eq!(
            serde_json::to_string(&decoded).expect("decoded snapshot must serialize"),
            encoded,
            "snapshot wire data must be canonical for deterministic replay"
        );
    }

    #[test]
    fn serialized_snapshot_rejects_tampered_inputs_and_unknown_wire_fields() {
        let snapshot = UniverseSelector::select(timestamp(HOUR_NS), [candidate("BTC", 0)])
            .expect("selection must be valid");
        let encoded = serde_json::to_value(snapshot).expect("snapshot must serialize");

        let mut tampered = encoded.clone();
        tampered["entries"][0]["trailing_day_notional_usdc"] = Value::String("5000001".into());
        assert!(
            serde_json::from_value::<super::UniverseSnapshot>(tampered).is_err(),
            "any causally relevant metric mutation must fail the snapshot digest"
        );

        let mut unknown = encoded;
        unknown["unexpected"] = Value::Bool(true);
        assert!(
            serde_json::from_value::<super::UniverseSnapshot>(unknown).is_err(),
            "snapshot wire data must reject unknown fields"
        );

        let snapshot = UniverseSelector::select(timestamp(HOUR_NS), [candidate("ETH", 0)])
            .expect("selection must be valid");
        let mut nested_unknown = serde_json::to_value(snapshot).expect("snapshot must serialize");
        nested_unknown["entries"][0]["unexpected"] = Value::Bool(true);
        assert!(
            serde_json::from_value::<super::UniverseSnapshot>(nested_unknown).is_err(),
            "nested snapshot wire data must reject unknown fields"
        );
    }

    #[test]
    fn completed_hour_membership_is_current_for_each_following_fifteen_minute_decision() {
        let hour = TimestampNs::new(3_600_000_000_000).expect("timestamp must be valid");
        let universe =
            TradeableUniverse::new(hour, [Market::new("BTC").expect("market must be valid")])
                .expect("membership must be valid");

        for offset in [
            0_i128,
            900_000_000_000,
            1_800_000_000_000,
            2_700_000_000_000,
        ] {
            assert!(
                universe.is_current_for(
                    TimestampNs::new(i128::from(hour.value()) + offset)
                        .expect("decision timestamp must be valid")
                )
            );
        }
    }

    #[test]
    fn completed_hour_membership_rejects_future_and_stale_decisions() {
        let hour = TimestampNs::new(3_600_000_000_000).expect("timestamp must be valid");
        let universe =
            TradeableUniverse::new(hour, [Market::new("BTC").expect("market must be valid")])
                .expect("membership must be valid");

        assert!(
            !universe.is_current_for(
                TimestampNs::new(i128::from(hour.value()) - 900_000_000_000)
                    .expect("decision timestamp must be valid")
            )
        );
        assert!(
            !universe.is_current_for(
                TimestampNs::new(i128::from(hour.value()) + 3_600_000_000_000)
                    .expect("decision timestamp must be valid")
            )
        );
    }

    #[test]
    fn selector_uses_symbol_ties_and_exact_rank_boundaries() {
        let candidates = (1..=31)
            .map(|index| candidate(&format!("M{index:02}"), 0))
            .collect::<Vec<_>>();
        let snapshot = UniverseSelector::select(timestamp(HOUR_NS), candidates)
            .expect("selection must be valid");

        let ranked = snapshot
            .entries()
            .filter(|entry| entry.rank().is_some())
            .collect::<Vec<_>>();
        assert_eq!(ranked.len(), 31);
        assert_eq!(ranked[0].market().as_str(), "M01");
        assert_eq!(ranked[0].rank(), Some(1));
        assert_eq!(ranked[19].market().as_str(), "M20");
        assert_eq!(ranked[19].membership(), Membership::Tradeable);
        assert_eq!(ranked[20].market().as_str(), "M21");
        assert_eq!(ranked[20].membership(), Membership::Warm);
        assert_eq!(ranked[29].market().as_str(), "M30");
        assert_eq!(ranked[29].membership(), Membership::Warm);
        assert_eq!(ranked[30].market().as_str(), "M31");
        assert_eq!(ranked[30].rank(), Some(31));
        assert_eq!(ranked[30].membership(), Membership::Absent);
        assert_eq!(ranked[0].score(), ranked[30].score());
    }

    #[test]
    fn selector_scores_executable_depth_across_ten_twenty_five_and_fifty_bps() {
        let snapshot = UniverseSelector::select(
            timestamp(HOUR_NS),
            [
                candidate_with_depth("AAA", 100_000, 100_000, 300_000),
                candidate_with_depth("BBB", 50_000, 200_000, 200_000),
            ],
        )
        .expect("selection must be valid");

        let aaa = snapshot
            .entry(&Market::new("AAA").expect("market must be valid"))
            .expect("AAA must be ranked");
        let bbb = snapshot
            .entry(&Market::new("BBB").expect("market must be valid"))
            .expect("BBB must be ranked");
        assert!(
            aaa.score().expect("AAA score") > bbb.score().expect("BBB score"),
            "the frozen depth component must use all three stated execution bands"
        );
    }

    #[test]
    fn depth_ranking_uses_ten_and_twenty_five_bps_when_fifty_bps_is_equal() {
        let snapshot = UniverseSelector::select(
            timestamp(HOUR_NS),
            [
                candidate_with_depth("NARROW", 100_000, 200_000, 300_000),
                candidate_with_depth("SHALLOW", 50_000, 60_000, 300_000),
            ],
        )
        .expect("selection must be valid");

        let narrow = snapshot
            .entry(&Market::new("NARROW").expect("market must be valid"))
            .expect("NARROW must be ranked");
        let shallow = snapshot
            .entry(&Market::new("SHALLOW").expect("market must be valid"))
            .expect("SHALLOW must be ranked");
        assert!(
            narrow
                .liquidity_score()
                .expect("NARROW score")
                .depth_percentile()
                > shallow
                    .liquidity_score()
                    .expect("SHALLOW score")
                    .depth_percentile(),
            "10/25-bps executable depth must influence the score independently of 50-bps depth"
        );
    }

    #[test]
    fn depth_ranking_uses_fifty_bps_when_near_bands_are_equal() {
        let snapshot = UniverseSelector::select(
            timestamp(HOUR_NS),
            [
                candidate_with_depth("DEEP", 100_000, 200_000, 400_000),
                candidate_with_depth("THIN", 100_000, 200_000, 300_000),
            ],
        )
        .expect("selection must be valid");

        let deep = snapshot
            .entry(&Market::new("DEEP").expect("market must be valid"))
            .expect("DEEP must be ranked");
        let thin = snapshot
            .entry(&Market::new("THIN").expect("market must be valid"))
            .expect("THIN must be ranked");
        assert!(
            deep.liquidity_score()
                .expect("DEEP score")
                .depth_percentile()
                > thin
                    .liquidity_score()
                    .expect("THIN score")
                    .depth_percentile(),
            "50-bps executable depth must influence the score independently of the near bands"
        );
    }

    #[test]
    fn selector_records_every_structural_exclusion() {
        let mut cases = vec![
            (
                UniverseCandidate::new(
                    Market::new("NONNATIVE").expect("market must be valid"),
                    false,
                    state(),
                    history(),
                    liquidity(0),
                ),
                UniverseExclusionReason::NotNativePerpetual,
            ),
            (
                UniverseCandidate::new(
                    Market::new("DELISTED").expect("market must be valid"),
                    true,
                    MarketDataAvailability::new(ListingState::Delisted, true, true, true, 20),
                    history(),
                    liquidity(0),
                ),
                UniverseExclusionReason::Delisted,
            ),
            (
                UniverseCandidate::new(
                    Market::new("PAUSED").expect("market must be valid"),
                    true,
                    MarketDataAvailability::new(ListingState::Paused, true, true, true, 20),
                    history(),
                    liquidity(0),
                ),
                UniverseExclusionReason::Paused,
            ),
            (
                UniverseCandidate::new(
                    Market::new("NOMID").expect("market must be valid"),
                    true,
                    MarketDataAvailability::new(ListingState::Active, false, true, true, 20),
                    history(),
                    liquidity(0),
                ),
                UniverseExclusionReason::MissingLiveMid,
            ),
            (
                UniverseCandidate::new(
                    Market::new("NOMARK").expect("market must be valid"),
                    true,
                    MarketDataAvailability::new(ListingState::Active, true, false, true, 20),
                    history(),
                    liquidity(0),
                ),
                UniverseExclusionReason::MissingLiveMark,
            ),
            (
                UniverseCandidate::new(
                    Market::new("NOMETA").expect("market must be valid"),
                    true,
                    MarketDataAvailability::new(ListingState::Active, true, true, false, 20),
                    history(),
                    liquidity(0),
                ),
                UniverseExclusionReason::MissingLiveMetadata,
            ),
            (
                UniverseCandidate::new(
                    Market::new("LOWLEV").expect("market must be valid"),
                    true,
                    MarketDataAvailability::new(ListingState::Active, true, true, true, 4),
                    history(),
                    liquidity(0),
                ),
                UniverseExclusionReason::VenueMaxLeverageBelowMinimum,
            ),
            (
                UniverseCandidate::new(
                    Market::new("HISTORY").expect("market must be valid"),
                    true,
                    state(),
                    HistoryQuality::new(29, dec!(0.995), true, dec!(1))
                        .expect("history must be valid"),
                    liquidity(0),
                ),
                UniverseExclusionReason::InsufficientLocalHistory,
            ),
            (
                UniverseCandidate::new(
                    Market::new("COVERAGE").expect("market must be valid"),
                    true,
                    state(),
                    HistoryQuality::new(30, dec!(0.9949), true, dec!(1))
                        .expect("history must be valid"),
                    liquidity(0),
                ),
                UniverseExclusionReason::InsufficientRequiredBarCoverage,
            ),
            (
                UniverseCandidate::new(
                    Market::new("STALE").expect("market must be valid"),
                    true,
                    state(),
                    HistoryQuality::new(30, dec!(0.995), false, dec!(1))
                        .expect("history must be valid"),
                    liquidity(0),
                ),
                UniverseExclusionReason::StaleDetailedFeed,
            ),
            (
                UniverseCandidate::new(
                    Market::new("VOLUME").expect("market must be valid"),
                    true,
                    state(),
                    history(),
                    UniverseLiquidity::new(
                        Usdc::new(dec!(4999999.999999)).expect("volume must be valid"),
                        Usdc::new(dec!(1000000)).expect("open interest must be valid"),
                        Bps::new(dec!(15)).expect("spread must be valid"),
                        liquidity(0).depth().clone(),
                    ),
                ),
                UniverseExclusionReason::InsufficientDailyNotional,
            ),
            (
                UniverseCandidate::new(
                    Market::new("SPREAD").expect("market must be valid"),
                    true,
                    state(),
                    history(),
                    UniverseLiquidity::new(
                        Usdc::new(dec!(5000000)).expect("volume must be valid"),
                        Usdc::new(dec!(1000000)).expect("open interest must be valid"),
                        Bps::new(dec!(15.000001)).expect("spread must be valid"),
                        liquidity(0).depth().clone(),
                    ),
                ),
                UniverseExclusionReason::ExcessiveEffectiveSpread,
            ),
            (
                UniverseCandidate::new(
                    Market::new("DEPTH").expect("market must be valid"),
                    true,
                    state(),
                    history(),
                    UniverseLiquidity::new(
                        Usdc::new(dec!(5000000)).expect("volume must be valid"),
                        Usdc::new(dec!(1000000)).expect("open interest must be valid"),
                        Bps::new(dec!(15)).expect("spread must be valid"),
                        DepthProfile::new(
                            SidedDepth::new(
                                Usdc::new(dec!(40000)).expect("depth must be valid"),
                                Usdc::new(dec!(45000)).expect("depth must be valid"),
                                Usdc::new(dec!(49999.999999)).expect("depth must be valid"),
                            )
                            .expect("depth must be valid"),
                            SidedDepth::new(
                                Usdc::new(dec!(50000)).expect("depth must be valid"),
                                Usdc::new(dec!(60000)).expect("depth must be valid"),
                                Usdc::new(dec!(70000)).expect("depth must be valid"),
                            )
                            .expect("depth must be valid"),
                        ),
                    ),
                ),
                UniverseExclusionReason::InsufficientExecutableDepth,
            ),
        ];
        let snapshot = UniverseSelector::select(
            timestamp(HOUR_NS),
            cases.iter().map(|(candidate, _)| candidate.clone()),
        )
        .expect("selection must be valid");

        for (candidate, reason) in cases.drain(..) {
            let entry = snapshot
                .entry(candidate.market())
                .expect("every candidate must be represented");
            assert_eq!(entry.membership(), Membership::Absent);
            assert!(entry.exclusion_reasons().contains(&reason));
        }
    }

    #[test]
    fn exact_prior_selector_output_removes_hard_failures_at_hour_and_applies_ranks_at_next_bar() {
        let previous_hour = timestamp(HOUR_NS);
        let previous_snapshot =
            UniverseSelector::select(previous_hour, [candidate("AAA", 0), candidate("BBB", 1)])
                .expect("previous selection must be valid");
        let previous = UniverseSelector::activate(
            &previous_snapshot,
            None,
            timestamp(HOUR_NS + FIFTEEN_MINUTES_NS),
        )
        .expect("previous selector output must activate at its next bar");
        let refresh_hour = timestamp(HOUR_NS * 2);
        let candidates = std::iter::once(UniverseCandidate::new(
            Market::new("AAA").expect("market must be valid"),
            true,
            MarketDataAvailability::new(ListingState::Paused, true, true, true, 20),
            history(),
            liquidity(0),
        ))
        .chain(std::iter::once(candidate("BBB", 0)))
        .chain((1..=20).map(|index| candidate(&format!("NEW{index:02}"), 100 + index)))
        .collect::<Vec<_>>();
        let snapshot =
            UniverseSelector::select(refresh_hour, candidates).expect("selection must be valid");

        let before_next_bar_activation =
            UniverseSelector::activate(&snapshot, Some(&previous), refresh_hour)
                .expect("exact prior selector output must activate");
        let before_next_bar = before_next_bar_activation
            .universe()
            .expect("BBB must remain active");
        assert!(!before_next_bar.contains(&Market::new("AAA").expect("market must be valid")));
        assert!(before_next_bar.contains(&Market::new("BBB").expect("market must be valid")));
        assert!(
            !before_next_bar.contains(&Market::new("NEW01").expect("market must be valid")),
            "rank-only activation must wait for the next completed strategy bar"
        );

        let after_next_bar_activation = UniverseSelector::activate(
            &snapshot,
            Some(&previous),
            timestamp(HOUR_NS * 2 + FIFTEEN_MINUTES_NS),
        )
        .expect("exact prior selector output must activate");
        let after_next_bar = after_next_bar_activation
            .universe()
            .expect("ranked universe must activate");
        assert!(after_next_bar.contains(&Market::new("NEW01").expect("market must be valid")));
        assert!(!after_next_bar.contains(&Market::new("AAA").expect("market must be valid")));
    }

    #[test]
    fn activation_rejects_manual_same_hour_membership_without_prior_provenance() {
        let snapshot = UniverseSelector::select(timestamp(HOUR_NS * 2), [candidate("BTC", 0)])
            .expect("selection must be valid");
        let same_hour = super::UniverseActivation {
            snapshot: snapshot.clone(),
            snapshot_digest: snapshot.digest().to_owned(),
            universe: Some(
                TradeableUniverse::new(
                    snapshot.as_of_time(),
                    [Market::new("ETH").expect("market must be valid")],
                )
                .expect("manual membership shape must be valid"),
            ),
        };

        assert!(matches!(
            UniverseSelector::activate(&snapshot, Some(&same_hour), snapshot.as_of_time()),
            Err(UniverseError::PriorSnapshotBoundaryMismatch {
                snapshot_time,
                expected_snapshot_time,
                actual_snapshot_time,
            }) if snapshot_time == snapshot.as_of_time()
                && expected_snapshot_time == timestamp(HOUR_NS)
                && actual_snapshot_time == snapshot.as_of_time()
        ));
    }

    #[test]
    fn activation_rejects_stale_selector_output_as_prior_provenance() {
        let stale_snapshot = UniverseSelector::select(timestamp(HOUR_NS), [candidate("BTC", 0)])
            .expect("stale selection must be valid");
        let stale = UniverseSelector::activate(
            &stale_snapshot,
            None,
            timestamp(HOUR_NS + FIFTEEN_MINUTES_NS),
        )
        .expect("stale selector output must be valid itself");
        let snapshot = UniverseSelector::select(timestamp(HOUR_NS * 3), [candidate("ETH", 0)])
            .expect("current selection must be valid");

        assert!(matches!(
            UniverseSelector::activate(&snapshot, Some(&stale), snapshot.as_of_time()),
            Err(UniverseError::PriorSnapshotBoundaryMismatch {
                snapshot_time,
                expected_snapshot_time,
                actual_snapshot_time,
            }) if snapshot_time == snapshot.as_of_time()
                && expected_snapshot_time == timestamp(HOUR_NS * 2)
                && actual_snapshot_time == stale_snapshot.as_of_time()
        ));
    }

    #[test]
    fn activation_rejects_a_snapshot_at_its_next_hour_boundary_or_later() {
        let snapshot = UniverseSelector::select(timestamp(HOUR_NS), [candidate("BTC", 0)])
            .expect("selection must be valid");

        for decision_time in [timestamp(HOUR_NS * 2), timestamp(HOUR_NS * 3)] {
            assert!(matches!(
                UniverseSelector::activate(&snapshot, None, decision_time),
                Err(UniverseError::SnapshotNotCurrent {
                    snapshot_time,
                    decision_time: rejected_time,
                }) if snapshot_time == snapshot.as_of_time() && rejected_time == decision_time
            ));
        }
    }

    #[test]
    fn activation_rejects_future_selector_output_as_prior_provenance() {
        let snapshot = UniverseSelector::select(timestamp(HOUR_NS * 2), [candidate("BTC", 0)])
            .expect("selection must be valid");
        let future_snapshot =
            UniverseSelector::select(timestamp(HOUR_NS * 3), [candidate("ETH", 0)])
                .expect("future selection must be valid");
        let future = UniverseSelector::activate(
            &future_snapshot,
            None,
            timestamp(HOUR_NS * 3 + FIFTEEN_MINUTES_NS),
        )
        .expect("future selector output must be valid itself");
        let decision_time = timestamp(HOUR_NS * 2 + FIFTEEN_MINUTES_NS);

        assert!(matches!(
            UniverseSelector::activate(&snapshot, Some(&future), decision_time),
            Err(UniverseError::PriorSnapshotBoundaryMismatch {
                snapshot_time,
                expected_snapshot_time,
                actual_snapshot_time,
            }) if snapshot_time == snapshot.as_of_time()
                && expected_snapshot_time == timestamp(HOUR_NS)
                && actual_snapshot_time == future_snapshot.as_of_time()
        ));
    }

    #[test]
    fn activation_rejects_tampered_prior_snapshot_digest() {
        let prior_snapshot = UniverseSelector::select(timestamp(HOUR_NS), [candidate("BTC", 0)])
            .expect("prior selection must be valid");
        let mut prior = UniverseSelector::activate(
            &prior_snapshot,
            None,
            timestamp(HOUR_NS + FIFTEEN_MINUTES_NS),
        )
        .expect("prior selector output must be valid");
        prior.snapshot_digest = "tampered".to_owned();
        let snapshot = UniverseSelector::select(timestamp(HOUR_NS * 2), [candidate("ETH", 0)])
            .expect("current selection must be valid");

        assert!(matches!(
            UniverseSelector::activate(&snapshot, Some(&prior), snapshot.as_of_time()),
            Err(UniverseError::PriorSnapshotDigestMismatch { snapshot_time })
                if snapshot_time == prior_snapshot.as_of_time()
        ));
    }

    #[test]
    fn native_perp_fixture_treats_btc_eth_and_sol_with_identical_gate_logic() {
        let fixture: Value = serde_json::from_str(include_str!(
            "../../../tests/fixtures/meta/native-perps.json"
        ))
        .expect("fixture must remain valid JSON");
        let universe = fixture[0]["universe"].as_array().expect("fixture universe");
        let candidates = universe
            .iter()
            .map(|entry| {
                let market = entry["name"].as_str().expect("fixture market name");
                let max_leverage =
                    entry["maxLeverage"].as_u64().expect("fixture max leverage") as u16;
                UniverseCandidate::new(
                    Market::new(market).expect("fixture market must be valid"),
                    market != "OLD",
                    MarketDataAvailability::new(
                        ListingState::Active,
                        true,
                        true,
                        true,
                        max_leverage,
                    ),
                    history(),
                    liquidity(0),
                )
            })
            .collect::<Vec<_>>();
        let snapshot = UniverseSelector::select(timestamp(HOUR_NS), candidates)
            .expect("selection must be valid");

        for market in ["BTC", "ETH", "SOL"] {
            let entry = snapshot
                .entry(&Market::new(market).expect("market must be valid"))
                .expect("fixture market must be represented");
            assert!(
                entry.exclusion_reasons().is_empty(),
                "{market} must use normal gates"
            );
        }
        assert!(
            snapshot
                .entry(&Market::new("OLD").expect("market must be valid"))
                .expect("old market must be represented")
                .exclusion_reasons()
                .contains(&UniverseExclusionReason::NotNativePerpetual)
        );
    }
}
