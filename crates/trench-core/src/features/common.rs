//! Finite, immutable common market features at explicit completed-bar boundaries.

use std::collections::{BTreeMap, BTreeSet};

use blake3::Hasher;
use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer, Serialize};
use thiserror::Error;

use crate::candle::Candle;
use crate::domain::{EventId, Market};
use crate::event::{
    AssetContext, Bbo, BookSnapshot, CandleInterval, Funding, MarketEvent, MarketEventKind,
    TimestampNs,
};
use crate::universe::TradeableUniverse;

const FEATURE_SCHEMA: &str = concat!(
    "trench.common-features.v3\n",
    "warmup.bars=97;context_observations=30:canonical-boundary;funding_observations=30\n",
    "rule_history=derivatives:30d;hourly_realized_volatility_20:90d\n",
    "returns=1,2,4,8,16,32,96\n",
    "ema=8,20,32;ema_slope=8:4;close_ema_20_residual;high_low=10\n",
    "rsi=14;atr=14;adx=14;realized_volatility=8,20,64\n",
    "donchian=20;volume_robust_z=20\n",
    "premium;open_interest_change=1,4,16;funding=level,percentile:30\n",
    "spread_bps;depth=sided:10,25,50;trade_imbalance=5m,15m\n",
    "cross_return_rank=liquidity_adjusted:4,16,96;hourly_regime\n"
);
const MAX_BAR_LOOKBACK: usize = 97;
const FIFTEEN_MINUTE_BARS_PER_DAY: usize = 24 * 4;
const HOURLY_BARS_PER_DAY: usize = 24;
const DERIVATIVE_HISTORY_DAYS: usize = 30;
const HOURLY_REGIME_HISTORY_DAYS: usize = 90;
const MAX_OPEN_INTEREST_LOOKBACK: usize = 16;
const HOURLY_REALIZED_VOLATILITY_WINDOW: usize = 20;
const DERIVATIVE_15_MINUTE_BARS: usize = DERIVATIVE_HISTORY_DAYS * FIFTEEN_MINUTE_BARS_PER_DAY;
const DERIVATIVE_HOURLY_BARS: usize = DERIVATIVE_HISTORY_DAYS * HOURLY_BARS_PER_DAY;
const HOURLY_REALIZED_VOLATILITY_HISTORY: usize = HOURLY_REGIME_HISTORY_DAYS * HOURLY_BARS_PER_DAY;
// Retain enough completed 15-minute bars to sample every 30-day derivative
// input at a bar boundary and still calculate OI change over its largest
// declared lookback.
const MAX_CANDLE_HISTORY: usize = DERIVATIVE_15_MINUTE_BARS + MAX_OPEN_INTEREST_LOOKBACK;
// A current 20-bar hourly realized-volatility value and its preceding 90-day
// distribution require 20 warmup bars, 2,160 historical observations, and
// one current completed bar.
const MAX_HOURLY_CANDLE_HISTORY: usize =
    HOURLY_REALIZED_VOLATILITY_WINDOW + HOURLY_REALIZED_VOLATILITY_HISTORY + 1;
const CONTEXT_WINDOW: usize = 30;
const MAX_MARKETS: usize = 128;
const POINT_EVENT_HISTORY: usize = 64;
// Raw source events are retained only long enough to attach observations to
// newly completed bars. Thirty-day strategy state lives in canonical boundary
// samples below, never in an event-count window vulnerable to feed bursts.
const SOURCE_EVENT_HISTORY: usize = 512;
// Raw trades are retained only for bounded identity replay. Feature windows
// use receipt-aware aggregate buckets below, so deep markets cannot make a
// valid decision depend on this event-count cache.
const TRADE_EVENT_HISTORY: usize = 128;
const TRADE_AGGREGATE_HISTORY: usize = 1_024;
/// Number of source identities retained after their active feature history is pruned.
///
/// Exact replays remain idempotent within this fixed-capacity horizon. Once it
/// has filled, a replay that falls before an active source history fails with
/// [`FeatureError::EventReplayOutsideHorizon`].
pub const FINALIZED_EVENT_ID_HORIZON: usize = 4_096;
/// Number of completed-candle identities retained after their active warmup history is pruned.
///
/// Exact replays remain idempotent within this fixed-capacity horizon. Once it
/// has filled, a replay that predates an active candle history fails with
/// [`FeatureError::CandleReplayOutsideHorizon`].
pub const FINALIZED_CANDLE_ID_HORIZON: usize = 4_096;
const MICRO_5_MINUTES_NS: i64 = 300_000_000_000;
const MICRO_15_MINUTES_NS: i64 = 900_000_000_000;
/// Maximum authoritative age of a BBO or L2 snapshot at a decision boundary.
///
/// This frozen one-second policy matches paper execution's executable-book
/// contract and is evaluated solely against explicit event and decision times.
pub const MAX_BBO_BOOK_AGE_NS: i64 = 1_000_000_000;

/// A dependency family included in an immutable feature-input range.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum FeatureInputKind {
    /// One completed fifteen-minute candle represented by its contributing-trade span.
    FifteenMinuteCandle,
    /// One completed one-hour candle represented by its contributing-trade span.
    OneHourCandle,
    /// One point-in-time asset-context observation.
    AssetContext,
    /// One funding observation.
    Funding,
    /// The current best-bid/best-offer observation.
    Bbo,
    /// The current order-book snapshot.
    Book,
    /// One public trade used by the microstructure windows.
    MicrostructureTrade,
}

impl FeatureInputKind {
    const fn identity_tag(self) -> u8 {
        match self {
            Self::FifteenMinuteCandle => 0,
            Self::OneHourCandle => 1,
            Self::AssetContext => 3,
            Self::Funding => 4,
            Self::Bbo => 5,
            Self::Book => 6,
            Self::MicrostructureTrade => 7,
        }
    }
}

/// Exact immutable event span for one feature dependency.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct InputEventSpan {
    kind: FeatureInputKind,
    first_event_id: EventId,
    last_event_id: EventId,
    first_event_time: TimestampNs,
    last_event_time: TimestampNs,
    first_received_at: TimestampNs,
    last_received_at: TimestampNs,
    available_at: TimestampNs,
    sampled_at: Option<TimestampNs>,
    aggregate_digest: Option<String>,
}

impl InputEventSpan {
    /// Returns the dependency family represented by this span.
    #[must_use]
    pub const fn kind(&self) -> FeatureInputKind {
        self.kind
    }

    /// Returns the first canonical identity in this dependency span.
    #[must_use]
    pub const fn first_event_id(&self) -> &EventId {
        &self.first_event_id
    }

    /// Returns the final canonical identity in this dependency span.
    #[must_use]
    pub const fn last_event_id(&self) -> &EventId {
        &self.last_event_id
    }

    /// Returns the earliest authoritative event time in this dependency span.
    #[must_use]
    pub const fn first_event_time(&self) -> TimestampNs {
        self.first_event_time
    }

    /// Returns the latest authoritative event time in this dependency span.
    #[must_use]
    pub const fn last_event_time(&self) -> TimestampNs {
        self.last_event_time
    }

    /// Returns the latest source receipt time required to use this span.
    #[must_use]
    pub const fn available_at(&self) -> TimestampNs {
        self.available_at
    }

    fn event(kind: FeatureInputKind, event: &MarketEvent) -> Self {
        Self {
            kind,
            first_event_id: event.event_id().clone(),
            last_event_id: event.event_id().clone(),
            first_event_time: event.event_time(),
            last_event_time: event.event_time(),
            first_received_at: event.received_at(),
            last_received_at: event.received_at(),
            available_at: event.received_at(),
            sampled_at: None,
            aggregate_digest: None,
        }
    }

    fn candle(kind: FeatureInputKind, candle: &Candle) -> Self {
        Self {
            kind,
            first_event_id: candle.first_event_id().clone(),
            last_event_id: candle.last_event_id().clone(),
            first_event_time: candle.first_event_time(),
            last_event_time: candle.last_event_time(),
            first_received_at: candle.first_received_at(),
            last_received_at: candle.last_received_at(),
            available_at: candle.source_available_at(),
            sampled_at: None,
            aggregate_digest: None,
        }
    }

    fn sampled_event(kind: FeatureInputKind, event: &MarketEvent, sampled_at: TimestampNs) -> Self {
        Self {
            sampled_at: Some(sampled_at),
            ..Self::event(kind, event)
        }
    }

    fn aggregate(kind: FeatureInputKind, aggregate: &AggressiveTradeAggregate) -> Self {
        Self {
            kind,
            first_event_id: aggregate.first_event_id.clone(),
            last_event_id: aggregate.last_event_id.clone(),
            first_event_time: aggregate.first_event_time,
            last_event_time: aggregate.last_event_time,
            first_received_at: aggregate.first_received_at,
            last_received_at: aggregate.last_received_at,
            available_at: aggregate.available_at,
            sampled_at: None,
            aggregate_digest: Some(aggregate.digest()),
        }
    }
}

/// Exact input event boundaries and spans represented by an immutable feature snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InputEventRange {
    first_event_id: EventId,
    last_event_id: EventId,
    first_event_time: TimestampNs,
    last_event_time: TimestampNs,
    spans: Vec<InputEventSpan>,
    universe_digest: Option<String>,
    digest: String,
}

impl InputEventRange {
    /// Returns the first canonical event identity used by this snapshot.
    #[must_use]
    pub const fn first_event_id(&self) -> &EventId {
        &self.first_event_id
    }

    /// Returns the final canonical event identity used by this snapshot.
    #[must_use]
    pub const fn last_event_id(&self) -> &EventId {
        &self.last_event_id
    }

    /// Returns the earliest authoritative input event time.
    #[must_use]
    pub const fn first_event_time(&self) -> TimestampNs {
        self.first_event_time
    }

    /// Returns the latest authoritative input event time.
    #[must_use]
    pub const fn last_event_time(&self) -> TimestampNs {
        self.last_event_time
    }

    /// Returns every exact dependency span in deterministic canonical order.
    #[must_use]
    pub fn spans(&self) -> &[InputEventSpan] {
        &self.spans
    }

    /// Returns the stable BLAKE3 digest of every exact dependency span.
    #[must_use]
    pub fn digest(&self) -> &str {
        &self.digest
    }

    /// Returns the frozen tradeable-universe content digest when ranks used it.
    #[must_use]
    pub fn universe_digest(&self) -> Option<&str> {
        self.universe_digest.as_deref()
    }

    fn from_spans(mut spans: Vec<InputEventSpan>) -> Option<Self> {
        spans.sort();
        spans.dedup();
        let first = spans.iter().min_by(|left, right| {
            left.first_event_time
                .cmp(&right.first_event_time)
                .then_with(|| left.first_received_at.cmp(&right.first_received_at))
                .then_with(|| left.first_event_id.cmp(&right.first_event_id))
        })?;
        let last = spans.iter().max_by(|left, right| {
            left.last_event_time
                .cmp(&right.last_event_time)
                .then_with(|| left.last_received_at.cmp(&right.last_received_at))
                .then_with(|| left.last_event_id.cmp(&right.last_event_id))
        })?;
        Some(Self {
            first_event_id: first.first_event_id.clone(),
            last_event_id: last.last_event_id.clone(),
            first_event_time: first.first_event_time,
            last_event_time: last.last_event_time,
            digest: input_digest(&spans, None),
            spans,
            universe_digest: None,
        })
    }

    fn with_additional_spans(&self, additional: &[InputEventSpan]) -> Option<Self> {
        let mut spans = self.spans.clone();
        spans.extend_from_slice(additional);
        let mut result = Self::from_spans(spans)?;
        result.universe_digest = self.universe_digest.clone();
        result.digest = input_digest(&result.spans, result.universe_digest.as_deref());
        Some(result)
    }

    fn with_universe(&self, universe: &TradeableUniverse) -> Self {
        let mut result = self.clone();
        result.universe_digest = Some(universe.digest().to_owned());
        result.digest = input_digest(&result.spans, result.universe_digest.as_deref());
        result
    }
}

fn input_digest(spans: &[InputEventSpan], universe_digest: Option<&str>) -> String {
    let mut hasher = Hasher::new_derive_key("trench.feature-input-range.v1");
    for span in spans {
        hasher.update(&[span.kind.identity_tag()]);
        hasher.update(&span.first_event_time.value().to_be_bytes());
        hasher.update(&span.first_received_at.value().to_be_bytes());
        hasher.update(span.first_event_id.as_str().as_bytes());
        hasher.update(&[0]);
        hasher.update(&span.last_event_time.value().to_be_bytes());
        hasher.update(&span.last_received_at.value().to_be_bytes());
        hasher.update(span.last_event_id.as_str().as_bytes());
        hasher.update(&[0]);
        hasher.update(&span.available_at.value().to_be_bytes());
        match span.sampled_at {
            Some(sampled_at) => {
                hasher.update(&[1]);
                hasher.update(&sampled_at.value().to_be_bytes());
            }
            None => {
                hasher.update(&[0]);
            }
        }
        match &span.aggregate_digest {
            Some(digest) => {
                hasher.update(&[1]);
                hasher.update(digest.as_bytes());
            }
            None => {
                hasher.update(&[0]);
            }
        }
    }
    if let Some(universe_digest) = universe_digest {
        hasher.update(b"universe\0");
        hasher.update(universe_digest.as_bytes());
    }
    hasher.finalize().to_hex().to_string()
}

/// Completeness of the point-in-time source families needed for common features.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct FeatureCompleteness {
    candles: bool,
    context: bool,
    funding: bool,
    microstructure: bool,
    regime: bool,
    calculations: bool,
    cross_section: bool,
}

impl FeatureCompleteness {
    /// Returns whether the completed-bar history is contiguous and fully warmed.
    #[must_use]
    pub const fn candles(self) -> bool {
        self.candles
    }

    /// Returns whether complete point-in-time mark/oracle/OI context is available.
    #[must_use]
    pub const fn context(self) -> bool {
        self.context
    }

    /// Returns whether the funding window is complete.
    #[must_use]
    pub const fn funding(self) -> bool {
        self.funding
    }

    /// Returns whether BBO, depth, and aggressive-trade inputs are complete.
    #[must_use]
    pub const fn microstructure(self) -> bool {
        self.microstructure
    }

    /// Returns whether the preceding completed hourly regime inputs are complete.
    #[must_use]
    pub const fn regime(self) -> bool {
        self.regime
    }

    /// Returns whether every declared feature calculation completed without arithmetic failure.
    #[must_use]
    pub const fn calculations(self) -> bool {
        self.calculations
    }

    /// Returns whether a contemporaneous multi-market cross-section is available.
    #[must_use]
    pub const fn cross_section(self) -> bool {
        self.cross_section
    }

    /// Returns whether every declared common-feature input family is complete.
    #[must_use]
    pub const fn is_ready(self) -> bool {
        self.candles
            && self.context
            && self.funding
            && self.microstructure
            && self.regime
            && self.calculations
            && self.cross_section
    }

    const fn primary_inputs_ready(self) -> bool {
        self.candles
            && self.context
            && self.funding
            && self.microstructure
            && self.regime
            && self.calculations
    }
}

/// Machine-readable reason a snapshot cannot be used as strategy or model input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FeatureUnreadyReason {
    /// A source dependency was evicted beyond the explicit retained horizon.
    HistoryPruned,
    /// The completed-bar history is absent, discontinuous, or not warm.
    CandleHistory,
    /// The point-in-time asset-context window is incomplete.
    ContextHistory,
    /// The point-in-time funding window is incomplete.
    FundingHistory,
    /// BBO, order-book, or aggressive-trade inputs are incomplete.
    Microstructure,
    /// A BBO or L2 book was available but older than the frozen executable-data bound.
    StaleBboOrBook,
    /// The preceding completed hourly regime inputs are incomplete.
    HourlyRegime,
    /// A declared feature calculation could not be represented from complete inputs.
    CalculationFailed,
    /// A contemporaneous multi-market cross-section is unavailable.
    CrossSection,
    /// Exact immutable provenance could not be derived for every dependency.
    InputProvenance,
}

/// Completed one-hour inputs used by either decision sleeve's regime evaluation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RegimeInputs {
    ema_8: Decimal,
    ema_32: Decimal,
    atr_14: Decimal,
    adx_14: Decimal,
    realized_volatility_20: Decimal,
}

impl RegimeInputs {
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

    /// Returns hourly 20-bar realized volatility.
    #[must_use]
    pub const fn realized_volatility_20(self) -> Decimal {
        self.realized_volatility_20
    }
}

/// One exact scalar input sampled at an explicit completed-bar boundary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TimedFeatureValue {
    as_of_time_ns: i64,
    value: Decimal,
    source_event_id: String,
    source_event_time_ns: i64,
    source_received_at_ns: i64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TimedFeatureValueWire {
    as_of_time_ns: i64,
    value: Decimal,
    source_event_id: String,
    source_event_time_ns: i64,
    source_received_at_ns: i64,
}

impl<'de> Deserialize<'de> for TimedFeatureValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = TimedFeatureValueWire::deserialize(deserializer)?;
        let value = Self {
            as_of_time_ns: wire.as_of_time_ns,
            value: wire.value,
            source_event_id: wire.source_event_id,
            source_event_time_ns: wire.source_event_time_ns,
            source_received_at_ns: wire.source_received_at_ns,
        };
        value
            .verify()
            .map_err(|error| serde::de::Error::custom(error.to_string()))?;
        Ok(value)
    }
}

impl TimedFeatureValue {
    /// Returns the completed-bar UTC boundary at which this value was known.
    #[must_use]
    pub const fn as_of_time_ns(&self) -> i64 {
        self.as_of_time_ns
    }

    /// Returns the exact decimal input value.
    #[must_use]
    pub const fn value(&self) -> Decimal {
        self.value
    }

    /// Returns the canonical normalized source identity used at this boundary.
    #[must_use]
    pub fn source_event_id(&self) -> &str {
        &self.source_event_id
    }

    /// Returns the authoritative source event time retained for replay audit.
    #[must_use]
    pub const fn source_event_time_ns(&self) -> i64 {
        self.source_event_time_ns
    }

    /// Returns the source receipt time that made this value available.
    #[must_use]
    pub const fn source_received_at_ns(&self) -> i64 {
        self.source_received_at_ns
    }

    fn verify(&self) -> Result<(), LongHorizonHistoryError> {
        checked_history_timestamp(self.as_of_time_ns, "sample boundary")?;
        checked_history_timestamp(self.source_event_time_ns, "sample event time")?;
        checked_history_timestamp(self.source_received_at_ns, "sample receipt time")?;
        EventId::new(self.source_event_id.clone()).map_err(|_| {
            LongHorizonHistoryError::InvalidEventId {
                field: "sample source event ID",
            }
        })?;
        if self.source_event_time_ns > self.source_received_at_ns
            || self.source_received_at_ns > self.as_of_time_ns
        {
            return Err(LongHorizonHistoryError::InvalidSampleProvenance);
        }
        Ok(())
    }
}

/// Completeness of all bounded histories required by long-horizon rules.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct LongHorizonFeatureCompleteness {
    primary_candles: bool,
    derivative_context: bool,
    funding: bool,
    hourly_volatility: bool,
}

impl LongHorizonFeatureCompleteness {
    /// Returns whether every long-horizon dependency is explicitly complete.
    #[must_use]
    pub const fn is_complete(self) -> bool {
        self.primary_candles && self.derivative_context && self.funding && self.hourly_volatility
    }
}

/// Bounded, serializable long-horizon inputs for the future rules sleeve.
///
/// Every value is constructed only from data available at `as_of_time_ns`.
/// The API returns `None` when the retained state cannot reconstruct an exact
/// complete horizon rather than filling a gap or substituting a newer input.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LongHorizonFeatureHistory {
    market: String,
    sleeve: String,
    as_of_time_ns: i64,
    primary_candle_provenance: Vec<LongHorizonCandleProvenance>,
    hourly_candle_provenance: Vec<LongHorizonCandleProvenance>,
    hourly_realized_volatility_20_history: Vec<TimedFeatureValue>,
    current_hourly_realized_volatility_20: Decimal,
    premium_history: Vec<TimedFeatureValue>,
    open_interest_change_4_history: Vec<TimedFeatureValue>,
    funding_history: Vec<TimedFeatureValue>,
    completeness: LongHorizonFeatureCompleteness,
    input_digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LongHorizonCandleProvenance {
    market: String,
    interval_tag: u8,
    open_time_ns: i64,
    first_event_id: String,
    last_event_id: String,
    first_event_time_ns: i64,
    last_event_time_ns: i64,
    first_received_at_ns: i64,
    last_received_at_ns: i64,
    source_available_at_ns: i64,
}

impl LongHorizonCandleProvenance {
    fn from_candle(candle: &Candle) -> Self {
        Self {
            market: candle.market().as_str().to_owned(),
            interval_tag: candle_interval_tag(candle.candle().interval()),
            open_time_ns: candle.candle().open_time().value(),
            first_event_id: candle.first_event_id().as_str().to_owned(),
            last_event_id: candle.last_event_id().as_str().to_owned(),
            first_event_time_ns: candle.first_event_time().value(),
            last_event_time_ns: candle.last_event_time().value(),
            first_received_at_ns: candle.first_received_at().value(),
            last_received_at_ns: candle.last_received_at().value(),
            source_available_at_ns: candle.source_available_at().value(),
        }
    }
}

/// A failed integrity check while restoring serialized long-horizon features.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum LongHorizonHistoryError {
    /// The serialized market did not satisfy the checked domain identifier contract.
    #[error("long-horizon history market is invalid")]
    InvalidMarket,
    /// The serialized sleeve was not one of the supported completed-bar cadences.
    #[error("long-horizon history sleeve {sleeve:?} is invalid")]
    InvalidSleeve {
        /// Rejected serialized sleeve text.
        sleeve: String,
    },
    /// A timestamp fell outside the supported nonnegative nanosecond range.
    #[error("long-horizon history {field} timestamp is invalid")]
    InvalidTimestamp {
        /// Name of the invalid timestamp field.
        field: &'static str,
    },
    /// A serialized normalized-event identifier was malformed.
    #[error("long-horizon history {field} is invalid")]
    InvalidEventId {
        /// Name of the invalid identifier field.
        field: &'static str,
    },
    /// One required long-horizon source family was marked incomplete.
    #[error("long-horizon history completeness is not complete")]
    Incomplete,
    /// A bounded history does not contain its exact required number of entries.
    #[error("long-horizon history {field} count {actual} does not equal required {expected}")]
    Count {
        /// Name of the bounded history.
        field: &'static str,
        /// Required number of entries.
        expected: usize,
        /// Serialized number of entries.
        actual: usize,
    },
    /// A serialized scalar history skipped or reordered an explicit boundary.
    #[error("long-horizon history {field} is not contiguous")]
    NonContiguous {
        /// Name of the non-contiguous source history.
        field: &'static str,
    },
    /// A scalar source timestamp was not causally available at its sample boundary.
    #[error("long-horizon history sample provenance is invalid")]
    InvalidSampleProvenance,
    /// A serialized candle span did not retain canonical source provenance.
    #[error("long-horizon history {field} candle provenance is invalid")]
    InvalidCandleProvenance {
        /// Name of the invalid candle history.
        field: &'static str,
    },
    /// Checked arithmetic for a declared long-horizon cadence overflowed.
    #[error("long-horizon history time arithmetic failed")]
    TimeArithmetic,
    /// The digest recomputed from serialized inputs did not match the payload.
    #[error("long-horizon history digest mismatch")]
    DigestMismatch,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LongHorizonFeatureHistoryWire {
    market: String,
    sleeve: String,
    as_of_time_ns: i64,
    primary_candle_provenance: Vec<LongHorizonCandleProvenance>,
    hourly_candle_provenance: Vec<LongHorizonCandleProvenance>,
    hourly_realized_volatility_20_history: Vec<TimedFeatureValue>,
    current_hourly_realized_volatility_20: Decimal,
    premium_history: Vec<TimedFeatureValue>,
    open_interest_change_4_history: Vec<TimedFeatureValue>,
    funding_history: Vec<TimedFeatureValue>,
    completeness: LongHorizonFeatureCompleteness,
    input_digest: String,
}

impl<'de> Deserialize<'de> for LongHorizonFeatureHistory {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = LongHorizonFeatureHistoryWire::deserialize(deserializer)?;
        let history = Self {
            market: wire.market,
            sleeve: wire.sleeve,
            as_of_time_ns: wire.as_of_time_ns,
            primary_candle_provenance: wire.primary_candle_provenance,
            hourly_candle_provenance: wire.hourly_candle_provenance,
            hourly_realized_volatility_20_history: wire.hourly_realized_volatility_20_history,
            current_hourly_realized_volatility_20: wire.current_hourly_realized_volatility_20,
            premium_history: wire.premium_history,
            open_interest_change_4_history: wire.open_interest_change_4_history,
            funding_history: wire.funding_history,
            completeness: wire.completeness,
            input_digest: wire.input_digest,
        };
        history
            .verify()
            .map_err(|error| serde::de::Error::custom(error.to_string()))?;
        Ok(history)
    }
}

impl LongHorizonFeatureHistory {
    /// Returns the checked market identifier as canonical text.
    #[must_use]
    pub fn market(&self) -> &str {
        &self.market
    }

    /// Returns the decision sleeve as `"15m"` or `"1h"`.
    #[must_use]
    pub fn sleeve(&self) -> &str {
        &self.sleeve
    }

    /// Returns the explicit UTC snapshot boundary.
    #[must_use]
    pub const fn as_of_time_ns(&self) -> i64 {
        self.as_of_time_ns
    }

    /// Returns the preceding 90-day hourly RV(20) distribution in chronological order.
    #[must_use]
    pub fn hourly_realized_volatility_20_history(&self) -> &[TimedFeatureValue] {
        &self.hourly_realized_volatility_20_history
    }

    /// Returns the current completed-hour RV(20), excluded from the preceding distribution.
    #[must_use]
    pub const fn current_hourly_realized_volatility_20(&self) -> Decimal {
        self.current_hourly_realized_volatility_20
    }

    /// Returns 30 days of completed-bar premium observations in chronological order.
    #[must_use]
    pub fn premium_history(&self) -> &[TimedFeatureValue] {
        &self.premium_history
    }

    /// Returns 30 days of 4-bar OI-change observations in chronological order.
    #[must_use]
    pub fn open_interest_change_4_history(&self) -> &[TimedFeatureValue] {
        &self.open_interest_change_4_history
    }

    /// Returns 30 days of completed-bar funding observations in chronological order.
    #[must_use]
    pub fn funding_history(&self) -> &[TimedFeatureValue] {
        &self.funding_history
    }

    /// Returns explicit availability for every long-horizon source family.
    #[must_use]
    pub const fn completeness(&self) -> LongHorizonFeatureCompleteness {
        self.completeness
    }

    /// Returns a stable digest of all samples and their receipt-aware provenance.
    #[must_use]
    pub fn input_digest(&self) -> &str {
        &self.input_digest
    }

    /// Verifies the bounded counts, source provenance, temporal continuity, and digest.
    ///
    /// This is also enforced by [`Deserialize`], so serialized payloads cannot
    /// reintroduce an incomplete or causally invalid model input.
    ///
    /// # Errors
    ///
    /// Returns a typed integrity error when any serialized invariant is invalid.
    pub fn verify(&self) -> Result<(), LongHorizonHistoryError> {
        let market =
            Market::new(self.market.clone()).map_err(|_| LongHorizonHistoryError::InvalidMarket)?;
        let sleeve = parse_history_sleeve(&self.sleeve)?;
        let as_of_time = checked_history_timestamp(self.as_of_time_ns, "snapshot boundary")?;
        if !self.completeness.is_complete() {
            return Err(LongHorizonHistoryError::Incomplete);
        }

        let derivative_bars = derivative_history_bars(sleeve);
        let primary_count = derivative_bars
            .checked_add(4)
            .ok_or(LongHorizonHistoryError::TimeArithmetic)?;
        verify_candle_provenance_history(
            &self.primary_candle_provenance,
            &market,
            sleeve,
            as_of_time,
            primary_count,
            "primary",
        )?;
        verify_candle_provenance_history(
            &self.hourly_candle_provenance,
            &market,
            CandleInterval::OneHour,
            as_of_time,
            MAX_HOURLY_CANDLE_HISTORY,
            "hourly",
        )?;

        verify_timed_history(
            &self.hourly_realized_volatility_20_history,
            HOURLY_REALIZED_VOLATILITY_HISTORY,
            CandleInterval::OneHour.duration().value(),
            as_of_time
                .value()
                .checked_sub(CandleInterval::OneHour.duration().value())
                .ok_or(LongHorizonHistoryError::TimeArithmetic)?,
            "hourly realized volatility history",
        )?;
        for (field, values) in [
            ("premium history", &self.premium_history),
            (
                "open-interest change history",
                &self.open_interest_change_4_history,
            ),
            ("funding history", &self.funding_history),
        ] {
            verify_timed_history(
                values,
                derivative_bars,
                sleeve.duration().value(),
                as_of_time.value(),
                field,
            )?;
        }

        let expected_digest = long_horizon_input_digest(&LongHorizonDigestInputs {
            market: &market,
            sleeve,
            as_of_time,
            primary_candle_provenance: &self.primary_candle_provenance,
            hourly_candle_provenance: &self.hourly_candle_provenance,
            hourly_volatility: &self.hourly_realized_volatility_20_history,
            current_hourly_volatility: self.current_hourly_realized_volatility_20,
            premium: &self.premium_history,
            open_interest_change: &self.open_interest_change_4_history,
            funding: &self.funding_history,
        });
        if self.input_digest != expected_digest {
            return Err(LongHorizonHistoryError::DigestMismatch);
        }
        Ok(())
    }
}

/// Immutable, point-in-time common-feature snapshot for one market and sleeve.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeatureSnapshot {
    market: Market,
    sleeve: CandleInterval,
    as_of_time: TimestampNs,
    input_range: Option<InputEventRange>,
    completeness: FeatureCompleteness,
    unready_reason: Option<FeatureUnreadyReason>,
    schema_hash: String,
    snapshot_hash: String,
    values: BTreeMap<String, Decimal>,
    regime: Option<RegimeInputs>,
}

impl FeatureSnapshot {
    /// Returns the market represented by this snapshot.
    #[must_use]
    pub const fn market(&self) -> &Market {
        &self.market
    }

    /// Returns the completed-bar sleeve that caused this evaluation.
    #[must_use]
    pub const fn sleeve(&self) -> CandleInterval {
        self.sleeve
    }

    /// Returns the exact UTC completed-bar boundary for this snapshot.
    #[must_use]
    pub const fn as_of_time(&self) -> TimestampNs {
        self.as_of_time
    }

    /// Returns the source input range when at least one completed candle exists.
    #[must_use]
    pub const fn input_range(&self) -> Option<&InputEventRange> {
        self.input_range.as_ref()
    }

    /// Returns which required point-in-time source families were complete.
    #[must_use]
    pub const fn completeness(&self) -> FeatureCompleteness {
        self.completeness
    }

    /// Returns the deterministic reason this snapshot is not strategy-ready.
    #[must_use]
    pub const fn unready_reason(&self) -> Option<FeatureUnreadyReason> {
        self.unready_reason
    }

    /// Returns whether the snapshot is complete enough for strategy or model input.
    #[must_use]
    pub const fn is_ready(&self) -> bool {
        self.completeness.is_ready() && self.unready_reason.is_none()
    }

    /// Returns the stable BLAKE3 hash of the declared feature schema.
    #[must_use]
    pub fn schema_hash(&self) -> &str {
        &self.schema_hash
    }

    /// Returns a stable digest of every immutable value and input contract.
    #[must_use]
    pub fn snapshot_hash(&self) -> &str {
        &self.snapshot_hash
    }

    /// Returns complete finite decimal feature values keyed by stable schema name.
    #[must_use]
    pub const fn values(&self) -> &BTreeMap<String, Decimal> {
        &self.values
    }

    /// Returns preceding completed-hour regime inputs, including for a 15-minute snapshot.
    #[must_use]
    pub const fn regime(&self) -> Option<RegimeInputs> {
        self.regime
    }
}

/// Feature-ingestion failure.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum FeatureError {
    /// A canonical normalized event identity was reused with a different payload.
    #[error("conflicting normalized event identity {event_id:?}")]
    ConflictingEvent {
        /// Reused canonical event identity.
        event_id: EventId,
    },
    /// A canonical normalized event identity changed receipt time.
    #[error(
        "normalized event identity {event_id:?} changed receipt time from {existing_received_at} to {received_at}"
    )]
    ConflictingEventReceiptTime {
        /// Reused canonical event identity.
        event_id: EventId,
        /// Receipt time already bound to the identity.
        existing_received_at: TimestampNs,
        /// Receipt time supplied by the conflicting replay.
        received_at: TimestampNs,
    },
    /// A completed-candle identity was reused with a different immutable value.
    #[error("conflicting completed candle for market {market:?} at {open_time}")]
    ConflictingCandle {
        /// Candle market.
        market: Market,
        /// Candle interval open time.
        open_time: TimestampNs,
    },
    /// A new market would exceed the engine's finite hot-state capacity.
    #[error("feature state market capacity {limit} reached")]
    MarketCapacity {
        /// Maximum distinct markets retained by one engine.
        limit: usize,
    },
    /// An event fell before the retained ordered source horizon.
    #[error("event for market {market:?} at {event_time} is outside retained source history")]
    EventOutsideRetention {
        /// Event market.
        market: Market,
        /// Rejected event's authoritative time.
        event_time: TimestampNs,
    },
    /// A source replay fell outside the bounded finalized-identity horizon.
    #[error("event identity {event_id:?} is outside retained replay horizon {limit}")]
    EventReplayOutsideHorizon {
        /// Replayed canonical event identity.
        event_id: EventId,
        /// Number of finalized identities retained for idempotent replay.
        limit: usize,
    },
    /// A candle fell before the retained completed-bar horizon.
    #[error("candle for market {market:?} at {open_time} is outside retained history")]
    CandleOutsideRetention {
        /// Candle market.
        market: Market,
        /// Rejected candle's interval open time.
        open_time: TimestampNs,
    },
    /// A completed-candle replay fell outside the bounded finalized-identity horizon.
    #[error(
        "completed candle for market {market:?} at {open_time} is outside retained replay horizon {limit}"
    )]
    CandleReplayOutsideHorizon {
        /// Candle market.
        market: Market,
        /// Candle interval open time.
        open_time: TimestampNs,
        /// Number of finalized candle identities retained for idempotent replay.
        limit: usize,
    },
    /// The bounded state cannot reconstruct every exact input required by the long-horizon rules.
    #[error(
        "long-horizon feature history for market {market:?}, sleeve {sleeve:?}, and boundary {as_of_time} is unavailable"
    )]
    LongHorizonHistoryUnavailable {
        /// Market whose rules inputs are unavailable.
        market: Market,
        /// Requested decision sleeve.
        sleeve: CandleInterval,
        /// Explicit decision boundary.
        as_of_time: TimestampNs,
    },
    /// A completed candle contained invalid timestamp arithmetic.
    #[error(transparent)]
    Event(#[from] crate::event::EventError),
    /// Checked decimal arithmetic failed while aggregating bounded trade inputs.
    #[error("checked decimal arithmetic failed while calculating {operation}")]
    Arithmetic {
        /// Failed calculation.
        operation: &'static str,
    },
}

type EventKey = (TimestampNs, TimestampNs, EventId);
type EventHistory = BTreeMap<EventKey, MarketEvent>;
type CompletedCandleId = (Market, CandleInterval, TimestampNs);
type CompletedCandleOrder = (TimestampNs, Market, CandleInterval);
type BoundarySourceSamples = BTreeMap<(Market, CandleInterval), BTreeMap<TimestampNs, MarketEvent>>;
type TradeAggregateKey = (TimestampNs, TimestampNs);

#[derive(Debug, Clone, PartialEq, Eq)]
struct AggressiveTradeAggregate {
    first_event_id: EventId,
    last_event_id: EventId,
    first_event_time: TimestampNs,
    last_event_time: TimestampNs,
    first_received_at: TimestampNs,
    last_received_at: TimestampNs,
    available_at: TimestampNs,
    buy_notional: Decimal,
    sell_notional: Decimal,
}

impl AggressiveTradeAggregate {
    fn from_trade(event: &MarketEvent) -> Result<Self, FeatureError> {
        let MarketEventKind::Trade(trade) = event.kind() else {
            return Err(FeatureError::Arithmetic {
                operation: "trade aggregate input kind",
            });
        };
        let notional = trade
            .price()
            .value()
            .checked_mul(trade.quantity().value())
            .ok_or(FeatureError::Arithmetic {
                operation: "trade aggregate notional",
            })?;
        let (buy_notional, sell_notional) = match trade.side() {
            crate::domain::Side::Buy => (notional, Decimal::ZERO),
            crate::domain::Side::Sell => (Decimal::ZERO, notional),
        };
        Ok(Self {
            first_event_id: event.event_id().clone(),
            last_event_id: event.event_id().clone(),
            first_event_time: event.event_time(),
            last_event_time: event.event_time(),
            first_received_at: event.received_at(),
            last_received_at: event.received_at(),
            available_at: event.received_at(),
            buy_notional,
            sell_notional,
        })
    }

    fn add_trade(&mut self, event: &MarketEvent) -> Result<(), FeatureError> {
        let MarketEventKind::Trade(trade) = event.kind() else {
            return Err(FeatureError::Arithmetic {
                operation: "trade aggregate input kind",
            });
        };
        let notional = trade
            .price()
            .value()
            .checked_mul(trade.quantity().value())
            .ok_or(FeatureError::Arithmetic {
                operation: "trade aggregate notional",
            })?;
        let candidate_key = (event.event_time(), event.received_at(), event.event_id());
        let first_key = (
            self.first_event_time,
            self.first_received_at,
            &self.first_event_id,
        );
        if candidate_key < first_key {
            self.first_event_id = event.event_id().clone();
            self.first_event_time = event.event_time();
            self.first_received_at = event.received_at();
        }
        let last_key = (
            self.last_event_time,
            self.last_received_at,
            &self.last_event_id,
        );
        if candidate_key > last_key {
            self.last_event_id = event.event_id().clone();
            self.last_event_time = event.event_time();
            self.last_received_at = event.received_at();
        }
        self.available_at = self.available_at.max(event.received_at());
        match trade.side() {
            crate::domain::Side::Buy => {
                self.buy_notional =
                    self.buy_notional
                        .checked_add(notional)
                        .ok_or(FeatureError::Arithmetic {
                            operation: "trade aggregate buy notional",
                        })?;
            }
            crate::domain::Side::Sell => {
                self.sell_notional =
                    self.sell_notional
                        .checked_add(notional)
                        .ok_or(FeatureError::Arithmetic {
                            operation: "trade aggregate sell notional",
                        })?;
            }
        }
        Ok(())
    }

    fn digest(&self) -> String {
        let mut hasher = Hasher::new_derive_key("trench.trade-window-aggregate.v1");
        for (event_time, received_at, event_id) in [
            (
                self.first_event_time,
                self.first_received_at,
                &self.first_event_id,
            ),
            (
                self.last_event_time,
                self.last_received_at,
                &self.last_event_id,
            ),
        ] {
            hasher.update(&event_time.value().to_be_bytes());
            hasher.update(&received_at.value().to_be_bytes());
            hasher.update(event_id.as_str().as_bytes());
            hasher.update(&[0]);
        }
        hasher.update(&self.available_at.value().to_be_bytes());
        hasher.update(self.buy_notional.to_string().as_bytes());
        hasher.update(&[0]);
        hasher.update(self.sell_notional.to_string().as_bytes());
        hasher.finalize().to_hex().to_string()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AggressiveTradeWindow {
    buy_notional: Decimal,
    sell_notional: Decimal,
    imbalance: Decimal,
    spans: Vec<InputEventSpan>,
}

impl AggressiveTradeWindow {
    const fn imbalance(&self) -> Decimal {
        self.imbalance
    }

    fn calculate_imbalance(&self) -> Option<Decimal> {
        self.buy_notional
            .checked_sub(self.sell_notional)?
            .checked_div(self.buy_notional.checked_add(self.sell_notional)?)
    }
}

/// Bounded, source-specific point-in-time histories for one market.
#[derive(Debug, Default)]
struct MarketEventHistory {
    contexts: EventHistory,
    fundings: EventHistory,
    bbo: EventHistory,
    books: EventHistory,
    trades: EventHistory,
    other: EventHistory,
}

impl MarketEventHistory {
    fn accepts(&self, event: &MarketEvent) -> bool {
        let source = self.source(event.kind());
        let key = (
            event.event_time(),
            event.received_at(),
            event.event_id().clone(),
        );
        source.len() < source_limit(event.kind())
            || source
                .first_key_value()
                .is_some_and(|(first, _)| key > *first)
    }

    fn insert(&mut self, event: MarketEvent) -> Vec<MarketEvent> {
        let mut pruned = Vec::new();
        {
            let limit = source_limit(event.kind());
            let source = self.source_mut(event.kind());
            source.insert(
                (
                    event.event_time(),
                    event.received_at(),
                    event.event_id().clone(),
                ),
                event,
            );
            while source.len() > limit {
                if let Some((_, discarded)) = source.pop_first() {
                    pruned.push(discarded);
                }
            }
        }
        pruned
    }

    fn source(&self, kind: &MarketEventKind) -> &EventHistory {
        match kind {
            MarketEventKind::AssetContext(_) => &self.contexts,
            MarketEventKind::Funding(_) => &self.fundings,
            MarketEventKind::Bbo(_) => &self.bbo,
            MarketEventKind::BookSnapshot(_) => &self.books,
            MarketEventKind::Trade(_) => &self.trades,
            MarketEventKind::Metadata(_) | MarketEventKind::CompletedCandle(_) => &self.other,
        }
    }

    fn source_mut(&mut self, kind: &MarketEventKind) -> &mut EventHistory {
        match kind {
            MarketEventKind::AssetContext(_) => &mut self.contexts,
            MarketEventKind::Funding(_) => &mut self.fundings,
            MarketEventKind::Bbo(_) => &mut self.bbo,
            MarketEventKind::BookSnapshot(_) => &mut self.books,
            MarketEventKind::Trade(_) => &mut self.trades,
            MarketEventKind::Metadata(_) | MarketEventKind::CompletedCandle(_) => &mut self.other,
        }
    }
}

const fn source_limit(kind: &MarketEventKind) -> usize {
    match kind {
        MarketEventKind::Trade(_) => TRADE_EVENT_HISTORY,
        MarketEventKind::AssetContext(_) | MarketEventKind::Funding(_) => SOURCE_EVENT_HISTORY,
        MarketEventKind::Metadata(_)
        | MarketEventKind::BookSnapshot(_)
        | MarketEventKind::Bbo(_)
        | MarketEventKind::CompletedCandle(_) => POINT_EVENT_HISTORY,
    }
}

const fn candle_history_limit(interval: CandleInterval) -> usize {
    match interval {
        CandleInterval::FifteenMinutes => MAX_CANDLE_HISTORY,
        CandleInterval::OneHour => MAX_HOURLY_CANDLE_HISTORY,
    }
}

const fn derivative_history_bars(interval: CandleInterval) -> usize {
    match interval {
        CandleInterval::FifteenMinutes => DERIVATIVE_15_MINUTE_BARS,
        CandleInterval::OneHour => DERIVATIVE_HOURLY_BARS,
    }
}

const fn sleeve_name(interval: CandleInterval) -> &'static str {
    match interval {
        CandleInterval::FifteenMinutes => "15m",
        CandleInterval::OneHour => "1h",
    }
}

/// Deterministic per-market, per-sleeve common-feature state.
///
/// Active source and candle windows are bounded. Identities evicted from those
/// windows remain idempotent only for [`FINALIZED_EVENT_ID_HORIZON`] or
/// [`FINALIZED_CANDLE_ID_HORIZON`] canonical ordering positions, respectively.
#[derive(Debug, Default)]
pub struct CommonFeatureEngine {
    seen_events: BTreeMap<EventId, MarketEvent>,
    finalized_events: BTreeMap<EventId, MarketEvent>,
    finalized_event_order: BTreeMap<EventKey, EventId>,
    finalized_candles: BTreeMap<CompletedCandleId, Candle>,
    finalized_candle_order: BTreeMap<CompletedCandleOrder, CompletedCandleId>,
    markets: BTreeSet<Market>,
    events: BTreeMap<Market, MarketEventHistory>,
    trade_aggregates:
        BTreeMap<(Market, CandleInterval), BTreeMap<TradeAggregateKey, AggressiveTradeAggregate>>,
    pruned_trade_aggregate_through: BTreeMap<(Market, CandleInterval), TimestampNs>,
    candles: BTreeMap<(Market, CandleInterval), BTreeMap<TimestampNs, Candle>>,
    context_samples: BoundarySourceSamples,
    funding_samples: BoundarySourceSamples,
    pruned_candle_through: BTreeMap<(Market, CandleInterval), TimestampNs>,
}

impl CommonFeatureEngine {
    /// Creates empty warmup state for every market and sleeve.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Records a normalized public input without reading wall-clock time.
    ///
    /// Exact duplicates are idempotent while their identity is retained by an
    /// active source history or [`FINALIZED_EVENT_ID_HORIZON`]. Conflicting
    /// reuse of a canonical event identity fails closed, so replay order
    /// cannot choose a feature value.
    ///
    /// # Errors
    ///
    /// Returns [`FeatureError::ConflictingEvent`] for a reused identity with a
    /// nonidentical immutable payload, or rejects input before its retained
    /// source horizon instead of silently changing a bounded feature window.
    pub fn observe(&mut self, event: &MarketEvent) -> Result<(), FeatureError> {
        if let Some(existing) = self.seen_events.get(event.event_id()) {
            return event_duplicate_result(existing, event);
        }
        if let Some(existing) = self.finalized_events.get(event.event_id()) {
            return event_duplicate_result(existing, event);
        }
        self.ensure_market_capacity(event.market())?;
        let accepts = self
            .events
            .get(event.market())
            .is_none_or(|history| history.accepts(event));
        if !accepts {
            if self.finalized_events.len() == FINALIZED_EVENT_ID_HORIZON {
                return Err(FeatureError::EventReplayOutsideHorizon {
                    event_id: event.event_id().clone(),
                    limit: FINALIZED_EVENT_ID_HORIZON,
                });
            }
            return Err(FeatureError::EventOutsideRetention {
                market: event.market().clone(),
                event_time: event.event_time(),
            });
        }
        if matches!(event.kind(), MarketEventKind::Trade(_)) {
            self.record_trade_aggregates(event)?;
        }
        let history = self.events.entry(event.market().clone()).or_default();
        let pruned = history.insert(event.clone());
        self.seen_events
            .insert(event.event_id().clone(), event.clone());
        for event in pruned {
            self.seen_events.remove(event.event_id());
            self.record_finalized_event(event);
        }
        self.refresh_boundary_samples(event);
        self.markets.insert(event.market().clone());
        Ok(())
    }

    /// Adds one immutable completed candle to its independent `(market, sleeve)` warmup state.
    ///
    /// # Errors
    ///
    /// Exact duplicates are idempotent while their identity is retained by an
    /// active warmup history or [`FINALIZED_CANDLE_ID_HORIZON`]. Returns
    /// [`FeatureError::ConflictingCandle`] when a completed-candle identity is
    /// replayed with a different immutable value.
    pub fn ingest_candle(&mut self, candle: Candle) -> Result<(), FeatureError> {
        let key = (candle.market().clone(), candle.candle().interval());
        let open_time = candle.candle().open_time();
        let candle_id = (key.0.clone(), key.1, open_time);
        if let Some(existing) = self.finalized_candles.get(&candle_id) {
            return candle_duplicate_result(existing, &candle);
        }
        self.ensure_market_capacity(candle.market())?;
        let mut pruned = Vec::new();
        {
            let candles = self.candles.entry(key.clone()).or_default();
            if let Some(existing) = candles.get(&open_time) {
                return if existing == &candle {
                    Ok(())
                } else {
                    Err(FeatureError::ConflictingCandle {
                        market: candle.market().clone(),
                        open_time,
                    })
                };
            }
            let limit = candle_history_limit(key.1);
            if candles.len() == limit
                && candles
                    .first_key_value()
                    .is_some_and(|(first, _)| open_time <= *first)
            {
                if self.finalized_candles.len() == FINALIZED_CANDLE_ID_HORIZON {
                    return Err(FeatureError::CandleReplayOutsideHorizon {
                        market: candle.market().clone(),
                        open_time,
                        limit: FINALIZED_CANDLE_ID_HORIZON,
                    });
                }
                return Err(FeatureError::CandleOutsideRetention {
                    market: candle.market().clone(),
                    open_time,
                });
            }
            candles.insert(open_time, candle.clone());
            while candles.len() > limit {
                if let Some((_, discarded)) = candles.pop_first() {
                    pruned.push(discarded);
                }
            }
        }
        for discarded in pruned {
            let close_time = discarded.close_time()?;
            self.pruned_candle_through
                .entry(key.clone())
                .and_modify(|current| *current = (*current).max(close_time))
                .or_insert(close_time);
            self.record_finalized_candle(discarded);
        }
        self.record_boundary_samples(&key, &candle);
        self.markets.insert(key.0.clone());
        Ok(())
    }

    /// Builds immutable snapshots at one explicit completed-bar boundary.
    ///
    /// Inputs after `as_of_time` are excluded from every calculation. A market
    /// with a completed candle at the requested sleeve/boundary receives an
    /// unready snapshot when any source family is missing; no value is imputed.
    #[must_use]
    pub fn snapshots_at(
        &self,
        sleeve: CandleInterval,
        as_of_time: TimestampNs,
    ) -> Vec<FeatureSnapshot> {
        self.snapshots_at_with_universe(sleeve, as_of_time, None)
    }

    /// Builds immutable snapshots using one explicit frozen tradeable universe.
    ///
    /// A missing, stale, incomplete, or mismatched membership never falls back
    /// to all locally warm markets: it leaves cross-sectional ranks unavailable
    /// and every affected snapshot fails closed.
    #[must_use]
    pub fn snapshots_at_with_universe(
        &self,
        sleeve: CandleInterval,
        as_of_time: TimestampNs,
        universe: Option<&TradeableUniverse>,
    ) -> Vec<FeatureSnapshot> {
        let mut bases = self
            .markets_at(sleeve, as_of_time)
            .into_iter()
            .map(|market| self.base_snapshot(market, sleeve, as_of_time))
            .collect::<Vec<_>>();

        let complete = universe
            .filter(|universe| universe.is_current_for(as_of_time))
            .and_then(|universe| {
                let members = universe.markets().iter().cloned().collect::<Vec<_>>();
                (members.len() >= 2
                    && members.iter().all(|market| {
                        bases.iter().any(|base| {
                            base.market == *market && base.completeness.primary_inputs_ready()
                        })
                    }))
                .then_some((universe, members))
            });
        if let Some((universe, members)) = complete {
            add_cross_sectional_ranks(&mut bases, &members, sleeve, universe);
        }

        bases
            .into_iter()
            .map(|base| {
                let unready_reason = base.unready_reason.or_else(|| {
                    (!base.completeness.cross_section).then_some(FeatureUnreadyReason::CrossSection)
                });
                let is_ready = base.completeness.is_ready() && unready_reason.is_none();
                let schema_hash = schema_hash();
                let values = if is_ready {
                    base.values
                } else {
                    BTreeMap::new()
                };
                let mut snapshot = FeatureSnapshot {
                    market: base.market,
                    sleeve,
                    as_of_time,
                    input_range: base.input_range,
                    completeness: base.completeness,
                    unready_reason,
                    schema_hash,
                    snapshot_hash: String::new(),
                    values,
                    regime: base.regime,
                };
                snapshot.snapshot_hash = feature_snapshot_hash(&snapshot);
                snapshot
            })
            .collect()
    }

    /// Returns exact, bounded long-horizon state for the future rules sleeve.
    ///
    /// The result contains a preceding 90-day distribution of completed-hour
    /// RV(20), the current RV(20), and 30-day derivatives histories sampled at
    /// the requested sleeve's completed-bar boundaries. It deliberately returns
    /// `None` if any required history is absent, discontinuous, unavailable at
    /// `as_of_time`, or outside the retained horizon.
    #[must_use]
    pub fn long_horizon_history_at(
        &self,
        market: &Market,
        sleeve: CandleInterval,
        as_of_time: TimestampNs,
    ) -> Option<LongHorizonFeatureHistory> {
        self.require_long_horizon_history_at(market, sleeve, as_of_time)
            .ok()
    }

    /// Returns complete long-horizon state or a typed fail-closed readiness error.
    ///
    /// # Errors
    ///
    /// Returns [`FeatureError::LongHorizonHistoryUnavailable`] rather than
    /// substituting incomplete, future, or pruned source data.
    pub fn require_long_horizon_history_at(
        &self,
        market: &Market,
        sleeve: CandleInterval,
        as_of_time: TimestampNs,
    ) -> Result<LongHorizonFeatureHistory, FeatureError> {
        self.build_long_horizon_history_at(market, sleeve, as_of_time)
            .ok_or_else(|| FeatureError::LongHorizonHistoryUnavailable {
                market: market.clone(),
                sleeve,
                as_of_time,
            })
    }

    fn build_long_horizon_history_at(
        &self,
        market: &Market,
        sleeve: CandleInterval,
        as_of_time: TimestampNs,
    ) -> Option<LongHorizonFeatureHistory> {
        let derivative_bars = derivative_history_bars(sleeve);
        let required_primary = derivative_bars.checked_add(4)?;
        let primary_history = self.candle_history(market, sleeve, as_of_time);
        let primary_history = contiguous_tail(&primary_history, required_primary, sleeve)?;
        (primary_history.last()?.close_time().ok()? == as_of_time).then_some(())?;
        let sampled_contexts =
            sampled_sources(&self.context_samples, market, sleeve, primary_history)?;
        let premium_history = sampled_contexts
            .iter()
            .skip(4)
            .map(|(boundary, event)| {
                let MarketEventKind::AssetContext(context) = event.kind() else {
                    return None;
                };
                Some(TimedFeatureValue {
                    as_of_time_ns: boundary.value(),
                    value: premium(context)?,
                    source_event_id: event.event_id().as_str().to_owned(),
                    source_event_time_ns: event.event_time().value(),
                    source_received_at_ns: event.received_at().value(),
                })
            })
            .collect::<Option<Vec<_>>>()?;
        let open_interest_change_4_history = sampled_contexts
            .windows(5)
            .map(|contexts| {
                let (boundary, current_event) = contexts[4];
                let MarketEventKind::AssetContext(current) = current_event.kind() else {
                    return None;
                };
                let MarketEventKind::AssetContext(prior) = contexts[0].1.kind() else {
                    return None;
                };
                current
                    .open_interest()
                    .value()
                    .checked_div(prior.open_interest().value())
                    .and_then(|ratio| ratio.checked_sub(Decimal::ONE))
                    .map(|value| TimedFeatureValue {
                        as_of_time_ns: boundary.value(),
                        value,
                        source_event_id: current_event.event_id().as_str().to_owned(),
                        source_event_time_ns: current_event.event_time().value(),
                        source_received_at_ns: current_event.received_at().value(),
                    })
            })
            .collect::<Option<Vec<_>>>()?;
        let sampled_fundings =
            sampled_sources(&self.funding_samples, market, sleeve, primary_history)?;
        let funding_history = sampled_fundings
            .iter()
            .skip(4)
            .map(|(boundary, event)| {
                let MarketEventKind::Funding(funding) = event.kind() else {
                    return None;
                };
                Some(TimedFeatureValue {
                    as_of_time_ns: boundary.value(),
                    value: funding.rate().value(),
                    source_event_id: event.event_id().as_str().to_owned(),
                    source_event_time_ns: event.event_time().value(),
                    source_received_at_ns: event.received_at().value(),
                })
            })
            .collect::<Option<Vec<_>>>()?;

        let hourly_history = self.candle_history(market, CandleInterval::OneHour, as_of_time);
        let hourly_history = contiguous_tail(
            &hourly_history,
            MAX_HOURLY_CANDLE_HISTORY,
            CandleInterval::OneHour,
        )?;
        let current_hourly_realized_volatility_20 =
            realized_volatility(hourly_history, HOURLY_REALIZED_VOLATILITY_WINDOW)?;
        let hourly_realized_volatility_20_history = (HOURLY_REALIZED_VOLATILITY_WINDOW
            ..hourly_history.len().checked_sub(1)?)
            .map(|end| {
                let history = hourly_history.get(end - HOURLY_REALIZED_VOLATILITY_WINDOW..=end)?;
                let candle = history.last()?;
                let boundary = candle.close_time().ok()?;
                Some(TimedFeatureValue {
                    as_of_time_ns: boundary.value(),
                    value: realized_volatility(history, HOURLY_REALIZED_VOLATILITY_WINDOW)?,
                    source_event_id: candle.last_event_id().as_str().to_owned(),
                    source_event_time_ns: candle.last_event_time().value(),
                    source_received_at_ns: candle.source_available_at().value(),
                })
            })
            .collect::<Option<Vec<_>>>()?;
        if hourly_realized_volatility_20_history.len() != HOURLY_REALIZED_VOLATILITY_HISTORY
            || premium_history.len() != derivative_bars
            || open_interest_change_4_history.len() != derivative_bars
            || funding_history.len() != derivative_bars
        {
            return None;
        }

        let primary_candle_provenance = primary_history
            .iter()
            .map(|candle| LongHorizonCandleProvenance::from_candle(candle))
            .collect::<Vec<_>>();
        let hourly_candle_provenance = hourly_history
            .iter()
            .map(|candle| LongHorizonCandleProvenance::from_candle(candle))
            .collect::<Vec<_>>();

        let input_digest = long_horizon_input_digest(&LongHorizonDigestInputs {
            market,
            sleeve,
            as_of_time,
            primary_candle_provenance: &primary_candle_provenance,
            hourly_candle_provenance: &hourly_candle_provenance,
            hourly_volatility: &hourly_realized_volatility_20_history,
            current_hourly_volatility: current_hourly_realized_volatility_20,
            premium: &premium_history,
            open_interest_change: &open_interest_change_4_history,
            funding: &funding_history,
        });
        Some(LongHorizonFeatureHistory {
            market: market.as_str().to_owned(),
            sleeve: sleeve_name(sleeve).to_owned(),
            as_of_time_ns: as_of_time.value(),
            primary_candle_provenance,
            hourly_candle_provenance,
            hourly_realized_volatility_20_history,
            current_hourly_realized_volatility_20,
            premium_history,
            open_interest_change_4_history,
            funding_history,
            completeness: LongHorizonFeatureCompleteness {
                primary_candles: true,
                derivative_context: true,
                funding: true,
                hourly_volatility: true,
            },
            input_digest,
        })
    }

    fn markets_at(&self, sleeve: CandleInterval, as_of_time: TimestampNs) -> Vec<Market> {
        self.markets
            .iter()
            .filter(|market| {
                let key = ((*market).clone(), sleeve);
                self.candles.get(&key).is_some_and(|candles| {
                    candles.values().any(|candle| {
                        candle.close_time().is_ok_and(|close| close == as_of_time)
                            && candle.source_available_at() <= as_of_time
                    })
                }) || self
                    .pruned_candle_through
                    .get(&key)
                    .is_some_and(|through| as_of_time <= *through)
            })
            .cloned()
            .collect()
    }

    fn base_snapshot(
        &self,
        market: Market,
        sleeve: CandleInterval,
        as_of_time: TimestampNs,
    ) -> BaseSnapshot {
        let history = self.candle_history(&market, sleeve, as_of_time);
        let feature_history = contiguous_tail(&history, MAX_BAR_LOOKBACK, sleeve);
        let primary_history_pruned = feature_history.is_none()
            && self
                .pruned_candle_through
                .contains_key(&(market.clone(), sleeve));
        let context_events = self.canonical_context_samples(&market, sleeve, as_of_time);
        let context_history = context_events
            .as_ref()
            .map(|events| {
                events
                    .iter()
                    .filter_map(|(_, event)| match event.kind() {
                        MarketEventKind::AssetContext(context) => Some(context),
                        _ => None,
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let funding_events = self.canonical_funding_samples(&market, sleeve, as_of_time);
        let funding_history = funding_events
            .as_ref()
            .map(|samples| {
                samples
                    .iter()
                    .filter_map(|(_, event)| match event.kind() {
                        MarketEventKind::Funding(funding) => Some(funding),
                        _ => None,
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let latest_context = context_history.last().copied();
        let latest_funding = funding_history.last().copied();
        let bbo_event = self.latest_bbo(&market, as_of_time);
        let bbo = bbo_event.and_then(|event| match event.kind() {
            MarketEventKind::Bbo(bbo) => Some(bbo),
            _ => None,
        });
        let book_event = self.latest_book(&market, as_of_time);
        let book = book_event.and_then(|event| match event.kind() {
            MarketEventKind::BookSnapshot(book) => Some(book),
            _ => None,
        });
        let hourly_history = self.candle_history(&market, CandleInterval::OneHour, as_of_time);
        let hourly_history = contiguous_tail(&hourly_history, 32, CandleInterval::OneHour);
        let hourly_history_pruned = hourly_history.is_none()
            && self
                .pruned_candle_through
                .contains_key(&(market.clone(), CandleInterval::OneHour));
        let regime = hourly_history.and_then(hourly_regime);
        let trade_window_5m = self.trade_window(&market, sleeve, as_of_time, MICRO_5_MINUTES_NS);
        let trade_window_15m = self.trade_window(&market, sleeve, as_of_time, MICRO_15_MINUTES_NS);
        let microstructure_history_pruned =
            microstructure_window_start(as_of_time, MICRO_15_MINUTES_NS).map_or(true, |start| {
                self.pruned_trade_aggregate_through
                    .get(&(market.clone(), sleeve))
                    .is_some_and(|through| *through > start)
            });
        let stale_bbo_or_book = bbo_event
            .is_some_and(|event| !is_fresh_book_input(event, as_of_time))
            || book_event.is_some_and(|event| !is_fresh_book_input(event, as_of_time));
        let microstructure = bbo.is_some()
            && book.is_some()
            && !stale_bbo_or_book
            && !microstructure_history_pruned
            && trade_window_5m.is_some()
            && trade_window_15m.is_some();
        let mut completeness = FeatureCompleteness {
            candles: feature_history.is_some(),
            context: context_events.is_some() && latest_context.is_some(),
            funding: funding_events.is_some() && latest_funding.is_some(),
            microstructure,
            regime: regime.is_some(),
            calculations: false,
            cross_section: false,
        };
        let mut values = BTreeMap::new();
        let mut source_range = None;
        let mut unready_reason =
            (primary_history_pruned || hourly_history_pruned || microstructure_history_pruned)
                .then_some(FeatureUnreadyReason::HistoryPruned)
                .or_else(|| stale_bbo_or_book.then_some(FeatureUnreadyReason::StaleBboOrBook))
                .or_else(|| incomplete_source_reason(completeness));
        if unready_reason.is_none() {
            if let (
                Some(feature_history),
                Some(hourly_history),
                Some(context_events),
                Some(funding_events),
                Some(bbo_event),
                Some(book_event),
                Some(trade_window_5m),
                Some(trade_window_15m),
            ) = (
                feature_history,
                hourly_history,
                context_events,
                funding_events,
                bbo_event,
                book_event,
                trade_window_5m.as_ref(),
                trade_window_15m.as_ref(),
            ) {
                let microstructure_spans = trade_window_5m
                    .spans
                    .iter()
                    .chain(&trade_window_15m.spans)
                    .cloned()
                    .collect::<Vec<_>>();
                let range = input_range(
                    feature_history,
                    hourly_history,
                    &context_events,
                    &funding_events,
                    bbo_event,
                    book_event,
                    &microstructure_spans,
                );
                if let Some(range) = range {
                    source_range = Some(range);
                    let inputs = SnapshotInputs {
                        history: feature_history,
                        latest_context,
                        contexts: &context_history,
                        latest_funding,
                        fundings: &funding_history,
                        bbo,
                        book,
                        trade_imbalance_5m: trade_window_5m.imbalance(),
                        trade_imbalance_15m: trade_window_15m.imbalance(),
                    };
                    if let Some(built_values) = build_values(&inputs) {
                        completeness.calculations = true;
                        values = built_values;
                    } else {
                        unready_reason = Some(FeatureUnreadyReason::CalculationFailed);
                    }
                } else {
                    unready_reason = Some(FeatureUnreadyReason::InputProvenance);
                }
            } else {
                unready_reason = Some(FeatureUnreadyReason::InputProvenance);
            }
        }
        BaseSnapshot {
            market,
            input_range: source_range,
            completeness,
            unready_reason,
            values,
            regime,
        }
    }

    fn candle_history(
        &self,
        market: &Market,
        sleeve: CandleInterval,
        as_of_time: TimestampNs,
    ) -> Vec<&Candle> {
        self.candles
            .get(&(market.clone(), sleeve))
            .map(|candles| {
                candles
                    .values()
                    .filter(|candle| {
                        candle.close_time().is_ok_and(|close| close <= as_of_time)
                            && candle.source_available_at() <= as_of_time
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    fn canonical_context_samples(
        &self,
        market: &Market,
        sleeve: CandleInterval,
        as_of_time: TimestampNs,
    ) -> Option<Vec<(TimestampNs, &MarketEvent)>> {
        let count = CONTEXT_WINDOW;
        let interval_ns = sleeve.duration().value();
        let span_ns =
            i128::from(interval_ns).checked_mul(i128::try_from(count.checked_sub(1)?).ok()?)?;
        let first = i128::from(as_of_time.value()).checked_sub(span_ns)?;
        let by_boundary = self.context_samples.get(&(market.clone(), sleeve))?;
        (0..count)
            .map(|offset| {
                let boundary_ns = first.checked_add(
                    i128::from(interval_ns).checked_mul(i128::try_from(offset).ok()?)?,
                )?;
                let boundary = TimestampNs::new(boundary_ns).ok()?;
                let event = by_boundary.get(&boundary)?;
                matches!(event.kind(), MarketEventKind::AssetContext(_)).then_some(())?;
                event_is_available(event, boundary).then_some((boundary, event))
            })
            .collect()
    }

    fn canonical_funding_samples(
        &self,
        market: &Market,
        sleeve: CandleInterval,
        as_of_time: TimestampNs,
    ) -> Option<Vec<(TimestampNs, &MarketEvent)>> {
        let count = derivative_history_bars(sleeve);
        let interval_ns = sleeve.duration().value();
        let span_ns =
            i128::from(interval_ns).checked_mul(i128::try_from(count.checked_sub(1)?).ok()?)?;
        let first = i128::from(as_of_time.value()).checked_sub(span_ns)?;
        let by_boundary = self.funding_samples.get(&(market.clone(), sleeve))?;
        (0..count)
            .map(|offset| {
                let boundary_ns = first.checked_add(
                    i128::from(interval_ns).checked_mul(i128::try_from(offset).ok()?)?,
                )?;
                let boundary = TimestampNs::new(boundary_ns).ok()?;
                let event = by_boundary.get(&boundary)?;
                matches!(event.kind(), MarketEventKind::Funding(_)).then_some(())?;
                event_is_available(event, boundary).then_some((boundary, event))
            })
            .collect()
    }

    fn latest_bbo(&self, market: &Market, as_of_time: TimestampNs) -> Option<&MarketEvent> {
        self.events.get(market).and_then(|history| {
            history
                .bbo
                .values()
                .rev()
                .find(|event| event_is_available(event, as_of_time))
        })
    }

    fn latest_book(&self, market: &Market, as_of_time: TimestampNs) -> Option<&MarketEvent> {
        self.events.get(market).and_then(|history| {
            history
                .books
                .values()
                .rev()
                .find(|event| event_is_available(event, as_of_time))
        })
    }

    fn ensure_market_capacity(&self, market: &Market) -> Result<(), FeatureError> {
        if self.markets.contains(market) {
            return Ok(());
        }
        if self.markets.len() == MAX_MARKETS {
            return Err(FeatureError::MarketCapacity { limit: MAX_MARKETS });
        }
        Ok(())
    }

    fn record_finalized_event(&mut self, event: MarketEvent) {
        let order_key = (
            event.event_time(),
            event.received_at(),
            event.event_id().clone(),
        );
        self.finalized_event_order
            .insert(order_key, event.event_id().clone());
        self.finalized_events
            .insert(event.event_id().clone(), event);
        while self.finalized_events.len() > FINALIZED_EVENT_ID_HORIZON {
            if let Some((_, event_id)) = self.finalized_event_order.pop_first() {
                self.finalized_events.remove(&event_id);
            }
        }
    }

    fn record_finalized_candle(&mut self, candle: Candle) {
        let candle_id = (
            candle.market().clone(),
            candle.candle().interval(),
            candle.candle().open_time(),
        );
        let order_key = (
            candle.candle().open_time(),
            candle.market().clone(),
            candle.candle().interval(),
        );
        self.finalized_candle_order
            .insert(order_key, candle_id.clone());
        self.finalized_candles.insert(candle_id, candle);
        while self.finalized_candles.len() > FINALIZED_CANDLE_ID_HORIZON {
            if let Some((_, candle_id)) = self.finalized_candle_order.pop_first() {
                self.finalized_candles.remove(&candle_id);
            }
        }
    }

    fn record_boundary_samples(&mut self, key: &(Market, CandleInterval), candle: &Candle) {
        let Ok(boundary) = candle.close_time() else {
            return;
        };
        let context = self.latest_context_event(&key.0, boundary);
        let funding = self.latest_funding_event(&key.0, boundary);
        if let Some(context) = context {
            insert_boundary_sample(&mut self.context_samples, key, boundary, context);
        }
        if let Some(funding) = funding {
            insert_boundary_sample(&mut self.funding_samples, key, boundary, funding);
        }
    }

    fn refresh_boundary_samples(&mut self, event: &MarketEvent) {
        match event.kind() {
            MarketEventKind::AssetContext(_) => {
                refresh_boundary_samples(&mut self.context_samples, event);
            }
            MarketEventKind::Funding(_) => {
                refresh_boundary_samples(&mut self.funding_samples, event);
            }
            MarketEventKind::Metadata(_)
            | MarketEventKind::BookSnapshot(_)
            | MarketEventKind::Bbo(_)
            | MarketEventKind::Trade(_)
            | MarketEventKind::CompletedCandle(_) => {}
        }
    }

    fn latest_context_event(
        &self,
        market: &Market,
        as_of_time: TimestampNs,
    ) -> Option<MarketEvent> {
        self.events.get(market).and_then(|history| {
            history
                .contexts
                .values()
                .rev()
                .find(|event| event_is_available(event, as_of_time))
                .cloned()
        })
    }

    fn latest_funding_event(
        &self,
        market: &Market,
        as_of_time: TimestampNs,
    ) -> Option<MarketEvent> {
        self.events.get(market).and_then(|history| {
            history
                .fundings
                .values()
                .rev()
                .find(|event| event_is_available(event, as_of_time))
                .cloned()
        })
    }

    fn record_trade_aggregates(&mut self, event: &MarketEvent) -> Result<(), FeatureError> {
        for sleeve in [CandleInterval::FifteenMinutes, CandleInterval::OneHour] {
            let key = (event.market().clone(), sleeve);
            let event_bucket_close = bucket_close(event.event_time(), MICRO_5_MINUTES_NS).ok_or(
                FeatureError::Arithmetic {
                    operation: "trade aggregate bucket close",
                },
            )?;
            let available_at = bucket_close(event.received_at(), sleeve.duration().value()).ok_or(
                FeatureError::Arithmetic {
                    operation: "trade aggregate availability boundary",
                },
            )?;
            let aggregate_key = (event_bucket_close, available_at);
            let aggregates = self.trade_aggregates.entry(key.clone()).or_default();
            if let Some(existing) = aggregates.get_mut(&aggregate_key) {
                existing.add_trade(event)?;
                continue;
            }
            if aggregates.len() == TRADE_AGGREGATE_HISTORY
                && aggregates
                    .first_key_value()
                    .is_some_and(|(first, _)| aggregate_key <= *first)
            {
                return Err(FeatureError::EventOutsideRetention {
                    market: event.market().clone(),
                    event_time: event.event_time(),
                });
            }
            aggregates.insert(aggregate_key, AggressiveTradeAggregate::from_trade(event)?);
            while aggregates.len() > TRADE_AGGREGATE_HISTORY {
                if let Some(((discarded_bucket_close, _), _)) = aggregates.pop_first() {
                    self.pruned_trade_aggregate_through
                        .entry(key.clone())
                        .and_modify(|through| *through = (*through).max(discarded_bucket_close))
                        .or_insert(discarded_bucket_close);
                }
            }
        }
        Ok(())
    }

    fn trade_window(
        &self,
        market: &Market,
        sleeve: CandleInterval,
        as_of_time: TimestampNs,
        window_ns: i64,
    ) -> Option<AggressiveTradeWindow> {
        let start = microstructure_window_start(as_of_time, window_ns)?;
        let key = (market.clone(), sleeve);
        if self
            .pruned_trade_aggregate_through
            .get(&key)
            .is_some_and(|through| *through > start)
        {
            return None;
        }
        let aggregates = self.trade_aggregates.get(&key)?;
        let mut window = AggressiveTradeWindow {
            buy_notional: Decimal::ZERO,
            sell_notional: Decimal::ZERO,
            imbalance: Decimal::ZERO,
            spans: Vec::new(),
        };
        for ((bucket_close, available_at), aggregate) in aggregates {
            if *bucket_close <= start || *bucket_close > as_of_time || *available_at > as_of_time {
                continue;
            }
            window.buy_notional = window.buy_notional.checked_add(aggregate.buy_notional)?;
            window.sell_notional = window.sell_notional.checked_add(aggregate.sell_notional)?;
            window.spans.push(InputEventSpan::aggregate(
                FeatureInputKind::MicrostructureTrade,
                aggregate,
            ));
        }
        window.imbalance = window.calculate_imbalance()?;
        Some(window)
    }
}

fn bucket_close(timestamp: TimestampNs, bucket_ns: i64) -> Option<TimestampNs> {
    let remainder = timestamp.value().checked_rem(bucket_ns)?;
    let adjustment = bucket_ns.checked_sub(remainder)?;
    TimestampNs::new(i128::from(timestamp.value()).checked_add(i128::from(adjustment))?).ok()
}

fn microstructure_window_start(as_of_time: TimestampNs, window_ns: i64) -> Option<TimestampNs> {
    TimestampNs::new(i128::from(as_of_time.value()).checked_sub(i128::from(window_ns))?).ok()
}

fn boundary_sample_limit(interval: CandleInterval) -> usize {
    derivative_history_bars(interval) + 4
}

fn insert_boundary_sample(
    samples: &mut BoundarySourceSamples,
    key: &(Market, CandleInterval),
    boundary: TimestampNs,
    event: MarketEvent,
) {
    if !event_is_available(&event, boundary) {
        return;
    }
    let limit = boundary_sample_limit(key.1);
    let by_boundary = samples.entry(key.clone()).or_default();
    if let Some(existing) = by_boundary.get(&boundary)
        && !source_event_is_later(&event, existing)
    {
        return;
    }
    if !by_boundary.contains_key(&boundary)
        && by_boundary.len() == limit
        && by_boundary
            .first_key_value()
            .is_some_and(|(first, _)| boundary <= *first)
    {
        return;
    }
    by_boundary.insert(boundary, event);
    while by_boundary.len() > limit {
        let _ = by_boundary.pop_first();
    }
}

fn refresh_boundary_samples(samples: &mut BoundarySourceSamples, event: &MarketEvent) {
    let available_at = event.received_at();
    for interval in [CandleInterval::FifteenMinutes, CandleInterval::OneHour] {
        let key = (event.market().clone(), interval);
        let Some(by_boundary) = samples.get_mut(&key) else {
            continue;
        };
        let boundaries = by_boundary
            .range(available_at..)
            .filter_map(|(boundary, existing)| {
                (event_is_available(event, *boundary) && source_event_is_later(event, existing))
                    .then_some(*boundary)
            })
            .collect::<Vec<_>>();
        for boundary in boundaries {
            by_boundary.insert(boundary, event.clone());
        }
    }
}

fn source_event_is_later(candidate: &MarketEvent, existing: &MarketEvent) -> bool {
    (
        candidate.event_time(),
        candidate.received_at(),
        candidate.event_id(),
    ) > (
        existing.event_time(),
        existing.received_at(),
        existing.event_id(),
    )
}

fn event_duplicate_result(
    existing: &MarketEvent,
    incoming: &MarketEvent,
) -> Result<(), FeatureError> {
    if existing == incoming {
        return Ok(());
    }
    if existing.received_at() != incoming.received_at() {
        return Err(FeatureError::ConflictingEventReceiptTime {
            event_id: incoming.event_id().clone(),
            existing_received_at: existing.received_at(),
            received_at: incoming.received_at(),
        });
    }
    Err(FeatureError::ConflictingEvent {
        event_id: incoming.event_id().clone(),
    })
}

fn candle_duplicate_result(existing: &Candle, incoming: &Candle) -> Result<(), FeatureError> {
    if existing == incoming {
        return Ok(());
    }
    Err(FeatureError::ConflictingCandle {
        market: incoming.market().clone(),
        open_time: incoming.candle().open_time(),
    })
}

fn event_is_available(event: &MarketEvent, as_of_time: TimestampNs) -> bool {
    event.event_time() <= as_of_time && event.received_at() <= as_of_time
}

fn is_fresh_book_input(event: &MarketEvent, as_of_time: TimestampNs) -> bool {
    event_is_available(event, as_of_time)
        && as_of_time
            .value()
            .checked_sub(event.event_time().value())
            .is_some_and(|age| age <= MAX_BBO_BOOK_AGE_NS)
}

#[derive(Debug)]
struct BaseSnapshot {
    market: Market,
    input_range: Option<InputEventRange>,
    completeness: FeatureCompleteness,
    unready_reason: Option<FeatureUnreadyReason>,
    values: BTreeMap<String, Decimal>,
    regime: Option<RegimeInputs>,
}

fn hourly_regime(history: &[&Candle]) -> Option<RegimeInputs> {
    Some(RegimeInputs {
        ema_8: ema(&closes(history), 8)?,
        ema_32: ema(&closes(history), 32)?,
        atr_14: atr(history, 14)?,
        adx_14: adx(history, 14)?,
        realized_volatility_20: realized_volatility(history, 20)?,
    })
}

fn incomplete_source_reason(completeness: FeatureCompleteness) -> Option<FeatureUnreadyReason> {
    if !completeness.candles {
        return Some(FeatureUnreadyReason::CandleHistory);
    }
    if !completeness.context {
        return Some(FeatureUnreadyReason::ContextHistory);
    }
    if !completeness.funding {
        return Some(FeatureUnreadyReason::FundingHistory);
    }
    if !completeness.microstructure {
        return Some(FeatureUnreadyReason::Microstructure);
    }
    if !completeness.regime {
        return Some(FeatureUnreadyReason::HourlyRegime);
    }
    None
}

fn add_cross_sectional_ranks(
    bases: &mut [BaseSnapshot],
    complete: &[Market],
    sleeve: CandleInterval,
    universe: &TradeableUniverse,
) {
    for lookback in [4_usize, 16, 96] {
        let name = format!("liquidity_adjusted_return_{lookback}");
        let rank_name = format!("cross_return_{lookback}_rank");
        let mut ranked = complete
            .iter()
            .filter_map(|base| {
                bases
                    .iter()
                    .find(|candidate| candidate.market == *base)?
                    .values
                    .get(&name)
                    .copied()
                    .map(|value| (base.clone(), value))
            })
            .collect::<Vec<_>>();
        ranked.sort_by(|left, right| left.1.cmp(&right.1).then_with(|| left.0.cmp(&right.0)));
        let denominator = Decimal::from(ranked.len());
        let mut tie_start = 0;
        while tie_start < ranked.len() {
            let tie_end = ranked[tie_start..]
                .iter()
                .position(|(_, value)| *value != ranked[tie_start].1)
                .map_or(ranked.len(), |offset| tie_start + offset);
            let rank = Decimal::from(tie_start + tie_end + 1) / Decimal::from(2) / denominator;
            for (market, _) in &ranked[tie_start..tie_end] {
                if let Some(base) = bases.iter_mut().find(|base| base.market == *market) {
                    base.values.insert(rank_name.clone(), rank);
                    base.completeness.cross_section = true;
                }
            }
            tie_start = tie_end;
        }
    }

    let primary_candle_kind = candle_input_kind(sleeve);
    let peer_causal_spans = complete
        .iter()
        .filter_map(|market| {
            let range = bases
                .iter()
                .find(|base| base.market == *market)?
                .input_range
                .as_ref()?;
            Some((
                market.clone(),
                range
                    .spans()
                    .iter()
                    .filter(|span| {
                        matches!(
                            span.kind(),
                            kind if kind == primary_candle_kind
                                || kind == FeatureInputKind::Bbo
                                || kind == FeatureInputKind::Book
                        )
                    })
                    .cloned()
                    .collect::<Vec<_>>(),
            ))
        })
        .collect::<Vec<_>>();
    for base in bases
        .iter_mut()
        .filter(|base| complete.contains(&base.market))
    {
        let additional = peer_causal_spans
            .iter()
            .filter(|(market, _)| *market != base.market)
            .flat_map(|(_, spans)| spans.iter().cloned())
            .collect::<Vec<_>>();
        if let Some(range) = &base.input_range {
            base.input_range = range
                .with_additional_spans(&additional)
                .map(|range| range.with_universe(universe));
        }
    }
}

struct SnapshotInputs<'a> {
    history: &'a [&'a Candle],
    latest_context: Option<&'a AssetContext>,
    contexts: &'a [&'a AssetContext],
    latest_funding: Option<&'a Funding>,
    fundings: &'a [&'a Funding],
    bbo: Option<&'a Bbo>,
    book: Option<&'a BookSnapshot>,
    trade_imbalance_5m: Decimal,
    trade_imbalance_15m: Decimal,
}

fn build_values(inputs: &SnapshotInputs<'_>) -> Option<BTreeMap<String, Decimal>> {
    let mut values = BTreeMap::new();
    let history = inputs.history;
    let history = contiguous_tail(
        history,
        MAX_BAR_LOOKBACK,
        history.first()?.candle().interval(),
    )?;
    let close_values = closes(history);
    for lookback in [1_usize, 2, 4, 8, 16, 32, 96] {
        values.insert(
            format!("return_{lookback}"),
            return_over(&close_values, lookback)?,
        );
    }
    let ema_8 = ema(&close_values, 8)?;
    let ema_20 = ema(&close_values, 20)?;
    let ema_32 = ema(&close_values, 32)?;
    let close = *close_values.last()?;
    values.insert("close".to_owned(), close);
    values.insert("ema_8".to_owned(), ema_8);
    values.insert("ema_20".to_owned(), ema_20);
    values.insert("ema_32".to_owned(), ema_32);
    values.insert(
        "close_ema_20_residual".to_owned(),
        close.checked_sub(ema_20)?,
    );
    values.insert("ema_8_32_ratio".to_owned(), ema_8.checked_div(ema_32)?);
    values.insert(
        "ema_8_slope_4".to_owned(),
        ema_8.checked_sub(ema(&close_values[..close_values.len().checked_sub(4)?], 8)?)?,
    );
    values.insert("rsi_14".to_owned(), rsi(&close_values, 14)?);
    values.insert("atr_14".to_owned(), atr(history, 14)?);
    values.insert(
        "atrp_14".to_owned(),
        atr(history, 14)?.checked_div(*close_values.last()?)?,
    );
    values.insert("adx_14".to_owned(), adx(history, 14)?);
    for window in [8_usize, 20, 64] {
        values.insert(
            format!("realized_volatility_{window}"),
            realized_volatility(history, window)?,
        );
    }
    values.insert(
        "donchian_20_position".to_owned(),
        donchian_position(history, 20)?,
    );
    let adverse_window = history.get(history.len().checked_sub(10)?..)?;
    values.insert(
        "high_10".to_owned(),
        adverse_window
            .iter()
            .map(|candle| candle.candle().high().value())
            .max()?,
    );
    values.insert(
        "low_10".to_owned(),
        adverse_window
            .iter()
            .map(|candle| candle.candle().low().value())
            .min()?,
    );
    values.insert(
        "volume_robust_z_20".to_owned(),
        robust_z(
            &history
                .iter()
                .rev()
                .take(20)
                .rev()
                .map(|candle| candle.candle().volume().value())
                .collect::<Vec<_>>(),
        )?,
    );

    let context = inputs.latest_context?;
    values.insert("premium".to_owned(), premium(context)?);
    for lookback in [1_usize, 4, 16] {
        values.insert(
            format!("open_interest_change_{lookback}"),
            open_interest_change(inputs.contexts, lookback)?,
        );
    }
    let funding = inputs.latest_funding?;
    values.insert("funding_level".to_owned(), funding.rate().value());
    values.insert(
        "funding_percentile_30".to_owned(),
        percentile(
            &inputs
                .fundings
                .iter()
                .map(|funding| funding.rate().value())
                .collect::<Vec<_>>(),
            funding.rate().value(),
        )?,
    );

    let bbo = inputs.bbo?;
    let book = inputs.book?;
    values.insert("spread_bps".to_owned(), spread_bps(bbo)?);
    for bps in [10_u32, 25, 50] {
        let bid_depth = depth(book, bbo, bps, BookSide::Bid)?;
        let ask_depth = depth(book, bbo, bps, BookSide::Ask)?;
        values.insert(format!("bid_depth_{bps}bps"), bid_depth);
        values.insert(format!("ask_depth_{bps}bps"), ask_depth);
        values.insert(format!("depth_{bps}bps"), bid_depth.checked_add(ask_depth)?);
    }
    let liquidity_factor = values
        .get("depth_50bps")?
        .checked_div(values.get("depth_50bps")?.checked_add(Decimal::from(500))?)?;
    for lookback in [4_usize, 16, 96] {
        values.insert(
            format!("liquidity_adjusted_return_{lookback}"),
            values
                .get(&format!("return_{lookback}"))?
                .checked_mul(liquidity_factor)?,
        );
    }
    values.insert("trade_imbalance_5m".to_owned(), inputs.trade_imbalance_5m);
    values.insert("trade_imbalance_15m".to_owned(), inputs.trade_imbalance_15m);
    Some(values)
}

fn input_range(
    feature_history: &[&Candle],
    hourly_history: &[&Candle],
    context_events: &[(TimestampNs, &MarketEvent)],
    funding_events: &[(TimestampNs, &MarketEvent)],
    bbo_event: &MarketEvent,
    book_event: &MarketEvent,
    microstructure_spans: &[InputEventSpan],
) -> Option<InputEventRange> {
    let spans = feature_history
        .iter()
        .map(|candle| InputEventSpan::candle(candle_input_kind(candle.candle().interval()), candle))
        .chain(hourly_history.iter().map(|candle| {
            InputEventSpan::candle(candle_input_kind(candle.candle().interval()), candle)
        }))
        .chain(context_events.iter().map(|(boundary, event)| {
            InputEventSpan::sampled_event(FeatureInputKind::AssetContext, event, *boundary)
        }))
        .chain(funding_events.iter().map(|(boundary, event)| {
            InputEventSpan::sampled_event(FeatureInputKind::Funding, event, *boundary)
        }))
        .chain(std::iter::once(InputEventSpan::event(
            FeatureInputKind::Bbo,
            bbo_event,
        )))
        .chain(std::iter::once(InputEventSpan::event(
            FeatureInputKind::Book,
            book_event,
        )))
        .chain(microstructure_spans.iter().cloned())
        .collect::<Vec<_>>();
    InputEventRange::from_spans(spans)
}

const fn candle_input_kind(interval: CandleInterval) -> FeatureInputKind {
    match interval {
        CandleInterval::FifteenMinutes => FeatureInputKind::FifteenMinuteCandle,
        CandleInterval::OneHour => FeatureInputKind::OneHourCandle,
    }
}

fn contiguous_tail<'a>(
    history: &'a [&'a Candle],
    count: usize,
    interval: CandleInterval,
) -> Option<&'a [&'a Candle]> {
    if history.len() < count {
        return None;
    }
    let tail = &history[history.len() - count..];
    for pair in tail.windows(2) {
        let expected = pair[0]
            .candle()
            .open_time()
            .checked_add(interval.duration())
            .ok()?;
        if pair[1].candle().open_time() != expected {
            return None;
        }
    }
    Some(tail)
}

fn sampled_sources<'a>(
    samples: &'a BoundarySourceSamples,
    market: &Market,
    sleeve: CandleInterval,
    candles: &[&Candle],
) -> Option<Vec<(TimestampNs, &'a MarketEvent)>> {
    let by_boundary = samples.get(&(market.clone(), sleeve))?;
    candles
        .iter()
        .map(|candle| {
            let boundary = candle.close_time().ok()?;
            let event = by_boundary.get(&boundary)?;
            event_is_available(event, boundary).then_some((boundary, event))
        })
        .collect()
}

fn parse_history_sleeve(sleeve: &str) -> Result<CandleInterval, LongHorizonHistoryError> {
    match sleeve {
        "15m" => Ok(CandleInterval::FifteenMinutes),
        "1h" => Ok(CandleInterval::OneHour),
        _ => Err(LongHorizonHistoryError::InvalidSleeve {
            sleeve: sleeve.to_owned(),
        }),
    }
}

fn checked_history_timestamp(
    value: i64,
    field: &'static str,
) -> Result<TimestampNs, LongHorizonHistoryError> {
    TimestampNs::new(i128::from(value))
        .map_err(|_| LongHorizonHistoryError::InvalidTimestamp { field })
}

fn verify_timed_history(
    values: &[TimedFeatureValue],
    expected_count: usize,
    interval_ns: i64,
    final_boundary_ns: i64,
    field: &'static str,
) -> Result<(), LongHorizonHistoryError> {
    if values.len() != expected_count {
        return Err(LongHorizonHistoryError::Count {
            field,
            expected: expected_count,
            actual: values.len(),
        });
    }
    let offsets = i64::try_from(
        values
            .len()
            .checked_sub(1)
            .ok_or(LongHorizonHistoryError::TimeArithmetic)?,
    )
    .map_err(|_| LongHorizonHistoryError::TimeArithmetic)?;
    let start_boundary = final_boundary_ns
        .checked_sub(
            interval_ns
                .checked_mul(offsets)
                .ok_or(LongHorizonHistoryError::TimeArithmetic)?,
        )
        .ok_or(LongHorizonHistoryError::TimeArithmetic)?;
    for (index, value) in values.iter().enumerate() {
        value.verify()?;
        let offset = i64::try_from(index).map_err(|_| LongHorizonHistoryError::TimeArithmetic)?;
        let expected_boundary = start_boundary
            .checked_add(
                interval_ns
                    .checked_mul(offset)
                    .ok_or(LongHorizonHistoryError::TimeArithmetic)?,
            )
            .ok_or(LongHorizonHistoryError::TimeArithmetic)?;
        if value.as_of_time_ns != expected_boundary {
            return Err(LongHorizonHistoryError::NonContiguous { field });
        }
    }
    Ok(())
}

fn verify_candle_provenance_history(
    provenance: &[LongHorizonCandleProvenance],
    market: &Market,
    interval: CandleInterval,
    as_of_time: TimestampNs,
    expected_count: usize,
    field: &'static str,
) -> Result<(), LongHorizonHistoryError> {
    if provenance.len() != expected_count {
        return Err(LongHorizonHistoryError::Count {
            field,
            expected: expected_count,
            actual: provenance.len(),
        });
    }
    let interval_ns = interval.duration().value();
    let last_open = as_of_time
        .value()
        .checked_sub(interval_ns)
        .ok_or(LongHorizonHistoryError::TimeArithmetic)?;
    let offsets = i64::try_from(
        provenance
            .len()
            .checked_sub(1)
            .ok_or(LongHorizonHistoryError::TimeArithmetic)?,
    )
    .map_err(|_| LongHorizonHistoryError::TimeArithmetic)?;
    let first_open = last_open
        .checked_sub(
            interval_ns
                .checked_mul(offsets)
                .ok_or(LongHorizonHistoryError::TimeArithmetic)?,
        )
        .ok_or(LongHorizonHistoryError::TimeArithmetic)?;

    for (index, candle) in provenance.iter().enumerate() {
        if Market::new(candle.market.clone()).as_ref() != Ok(market)
            || candle.interval_tag != candle_interval_tag(interval)
        {
            return Err(LongHorizonHistoryError::InvalidCandleProvenance { field });
        }
        let offset = i64::try_from(index).map_err(|_| LongHorizonHistoryError::TimeArithmetic)?;
        let expected_open = first_open
            .checked_add(
                interval_ns
                    .checked_mul(offset)
                    .ok_or(LongHorizonHistoryError::TimeArithmetic)?,
            )
            .ok_or(LongHorizonHistoryError::TimeArithmetic)?;
        if candle.open_time_ns != expected_open {
            return Err(LongHorizonHistoryError::NonContiguous { field });
        }
        for (timestamp, timestamp_field) in [
            (candle.open_time_ns, "candle open time"),
            (candle.first_event_time_ns, "candle first event time"),
            (candle.last_event_time_ns, "candle last event time"),
            (candle.first_received_at_ns, "candle first receipt time"),
            (candle.last_received_at_ns, "candle last receipt time"),
            (candle.source_available_at_ns, "candle availability time"),
        ] {
            checked_history_timestamp(timestamp, timestamp_field)?;
        }
        EventId::new(candle.first_event_id.clone()).map_err(|_| {
            LongHorizonHistoryError::InvalidEventId {
                field: "candle first event ID",
            }
        })?;
        EventId::new(candle.last_event_id.clone()).map_err(|_| {
            LongHorizonHistoryError::InvalidEventId {
                field: "candle last event ID",
            }
        })?;
        let first = (
            candle.first_event_time_ns,
            candle.first_received_at_ns,
            candle.first_event_id.as_str(),
        );
        let last = (
            candle.last_event_time_ns,
            candle.last_received_at_ns,
            candle.last_event_id.as_str(),
        );
        if candle.first_event_time_ns > candle.first_received_at_ns
            || candle.last_event_time_ns > candle.last_received_at_ns
            || first > last
            || candle.source_available_at_ns < candle.first_received_at_ns
            || candle.source_available_at_ns < candle.last_received_at_ns
            || candle.source_available_at_ns > as_of_time.value()
        {
            return Err(LongHorizonHistoryError::InvalidCandleProvenance { field });
        }
    }
    Ok(())
}

struct LongHorizonDigestInputs<'a> {
    market: &'a Market,
    sleeve: CandleInterval,
    as_of_time: TimestampNs,
    primary_candle_provenance: &'a [LongHorizonCandleProvenance],
    hourly_candle_provenance: &'a [LongHorizonCandleProvenance],
    hourly_volatility: &'a [TimedFeatureValue],
    current_hourly_volatility: Decimal,
    premium: &'a [TimedFeatureValue],
    open_interest_change: &'a [TimedFeatureValue],
    funding: &'a [TimedFeatureValue],
}

fn long_horizon_input_digest(inputs: &LongHorizonDigestInputs<'_>) -> String {
    let mut hasher = Hasher::new_derive_key("trench.long-horizon-input.v1");
    hasher.update(inputs.market.as_str().as_bytes());
    hasher.update(&[0, candle_interval_tag(inputs.sleeve)]);
    hasher.update(&inputs.as_of_time.value().to_be_bytes());
    for candle in inputs
        .primary_candle_provenance
        .iter()
        .chain(inputs.hourly_candle_provenance)
    {
        hasher.update(candle.market.as_bytes());
        hasher.update(&[candle.interval_tag]);
        hasher.update(&candle.open_time_ns.to_be_bytes());
        hasher.update(candle.first_event_id.as_bytes());
        hasher.update(&[0]);
        hasher.update(candle.last_event_id.as_bytes());
        hasher.update(&candle.first_event_time_ns.to_be_bytes());
        hasher.update(&candle.last_event_time_ns.to_be_bytes());
        hasher.update(&candle.first_received_at_ns.to_be_bytes());
        hasher.update(&candle.last_received_at_ns.to_be_bytes());
        hasher.update(&candle.source_available_at_ns.to_be_bytes());
    }
    hasher.update(inputs.current_hourly_volatility.to_string().as_bytes());
    for value in inputs
        .hourly_volatility
        .iter()
        .chain(inputs.premium)
        .chain(inputs.open_interest_change)
        .chain(inputs.funding)
    {
        hasher.update(&value.as_of_time_ns.to_be_bytes());
        hasher.update(value.value.to_string().as_bytes());
        hasher.update(&[0]);
        hasher.update(value.source_event_id.as_bytes());
        hasher.update(&[0]);
        hasher.update(&value.source_event_time_ns.to_be_bytes());
        hasher.update(&value.source_received_at_ns.to_be_bytes());
    }
    hasher.finalize().to_hex().to_string()
}

fn closes(history: &[&Candle]) -> Vec<Decimal> {
    history
        .iter()
        .map(|candle| candle.candle().close().value())
        .collect()
}

fn return_over(values: &[Decimal], lookback: usize) -> Option<Decimal> {
    let current = *values.last()?;
    let prior = *values.get(values.len().checked_sub(lookback + 1)?)?;
    current.checked_div(prior)?.checked_sub(Decimal::ONE)
}

fn average(values: &[Decimal]) -> Option<Decimal> {
    let total = values
        .iter()
        .try_fold(Decimal::ZERO, |total, value| total.checked_add(*value))?;
    total.checked_div(Decimal::from(values.len()))
}

fn ema(values: &[Decimal], period: usize) -> Option<Decimal> {
    if values.len() < period {
        return None;
    }
    let alpha = Decimal::from(2).checked_div(Decimal::from(period + 1))?;
    values[period..]
        .iter()
        .try_fold(average(&values[..period])?, |prior, value| {
            value
                .checked_sub(prior)
                .and_then(|difference| difference.checked_mul(alpha))
                .and_then(|adjustment| prior.checked_add(adjustment))
        })
}

fn rma_series(values: &[Decimal], period: usize) -> Option<Vec<Decimal>> {
    if values.len() < period {
        return None;
    }
    let denominator = Decimal::from(period);
    let mut result = Vec::with_capacity(values.len() - period + 1);
    let mut prior = average(&values[..period])?;
    result.push(prior);
    for value in &values[period..] {
        prior = prior
            .checked_mul(Decimal::from(period - 1))?
            .checked_add(*value)?
            .checked_div(denominator)?;
        result.push(prior);
    }
    Some(result)
}

fn atr(history: &[&Candle], period: usize) -> Option<Decimal> {
    let values = true_ranges(history)?;
    rma_series(&values, period)?.last().copied()
}

fn true_ranges(history: &[&Candle]) -> Option<Vec<Decimal>> {
    if history.len() < 2 {
        return None;
    }
    history
        .windows(2)
        .map(|pair| {
            let previous = pair[0].candle().close().value();
            let current = pair[1].candle();
            let high_low = current.high().value().checked_sub(current.low().value())?;
            let high_close = current.high().value().checked_sub(previous)?.abs();
            let low_close = current.low().value().checked_sub(previous)?.abs();
            Some(high_low.max(high_close).max(low_close))
        })
        .collect()
}

fn rsi(values: &[Decimal], period: usize) -> Option<Decimal> {
    let deltas = values
        .windows(2)
        .map(|pair| pair[1].checked_sub(pair[0]))
        .collect::<Option<Vec<_>>>()?;
    let gains = deltas
        .iter()
        .map(|delta| (*delta).max(Decimal::ZERO))
        .collect::<Vec<_>>();
    let losses = deltas
        .iter()
        .map(|delta| (-*delta).max(Decimal::ZERO))
        .collect::<Vec<_>>();
    let gain = rma_series(&gains, period)?.last().copied()?;
    let loss = rma_series(&losses, period)?.last().copied()?;
    if loss.is_zero() {
        return if gain.is_zero() {
            Some(Decimal::from(50))
        } else {
            Some(Decimal::from(100))
        };
    }
    Decimal::from(100).checked_sub(
        Decimal::from(100).checked_div(Decimal::ONE.checked_add(gain.checked_div(loss)?)?)?,
    )
}

fn adx(history: &[&Candle], period: usize) -> Option<Decimal> {
    if history.len() < period.checked_mul(2)?.checked_add(1)? {
        return None;
    }
    let ranges = true_ranges(history)?;
    let movements = history
        .windows(2)
        .map(|pair| {
            let up = pair[1]
                .candle()
                .high()
                .value()
                .checked_sub(pair[0].candle().high().value())?;
            let down = pair[0]
                .candle()
                .low()
                .value()
                .checked_sub(pair[1].candle().low().value())?;
            let plus = if up > Decimal::ZERO && up > down {
                up
            } else {
                Decimal::ZERO
            };
            let minus = if down > Decimal::ZERO && down > up {
                down
            } else {
                Decimal::ZERO
            };
            Some((plus, minus))
        })
        .collect::<Option<Vec<_>>>()?;
    let plus = movements.iter().map(|(plus, _)| *plus).collect::<Vec<_>>();
    let minus = movements
        .iter()
        .map(|(_, minus)| *minus)
        .collect::<Vec<_>>();
    let atrs = rma_series(&ranges, period)?;
    let pluses = rma_series(&plus, period)?;
    let minuses = rma_series(&minus, period)?;
    let dx = atrs
        .iter()
        .zip(pluses.iter().zip(minuses.iter()))
        .map(|(atr, (plus, minus))| {
            if atr.is_zero() {
                return Some(Decimal::ZERO);
            }
            let plus_di = plus.checked_mul(Decimal::from(100))?.checked_div(*atr)?;
            let minus_di = minus.checked_mul(Decimal::from(100))?.checked_div(*atr)?;
            let total = plus_di.checked_add(minus_di)?;
            if total.is_zero() {
                Some(Decimal::ZERO)
            } else {
                plus_di
                    .checked_sub(minus_di)?
                    .abs()
                    .checked_div(total)
                    .and_then(|value| value.checked_mul(Decimal::from(100)))
            }
        })
        .collect::<Option<Vec<_>>>()?;
    rma_series(&dx, period)?.last().copied()
}

fn realized_volatility(history: &[&Candle], window: usize) -> Option<Decimal> {
    if history.len() < window.checked_add(1)? {
        return None;
    }
    let returns = closes(&history[history.len() - window - 1..])
        .windows(2)
        .map(|pair| pair[1].checked_div(pair[0])?.checked_sub(Decimal::ONE))
        .collect::<Option<Vec<_>>>()?;
    let square_mean = returns
        .iter()
        .try_fold(Decimal::ZERO, |total, value| {
            value
                .checked_mul(*value)
                .and_then(|squared| total.checked_add(squared))
        })?
        .checked_div(Decimal::from(window))?;
    decimal_sqrt(square_mean)
}

fn decimal_sqrt(value: Decimal) -> Option<Decimal> {
    if value < Decimal::ZERO {
        return None;
    }
    if value.is_zero() {
        return Some(Decimal::ZERO);
    }
    let mut estimate = if value > Decimal::ONE {
        value
    } else {
        Decimal::ONE
    };
    for _ in 0..32 {
        estimate = estimate
            .checked_add(value.checked_div(estimate)?)?
            .checked_div(Decimal::from(2))?;
    }
    Some(estimate)
}

fn donchian_position(history: &[&Candle], window: usize) -> Option<Decimal> {
    let candles = history.get(history.len().checked_sub(window)?..)?;
    let high = candles
        .iter()
        .map(|candle| candle.candle().high().value())
        .max()?;
    let low = candles
        .iter()
        .map(|candle| candle.candle().low().value())
        .min()?;
    let span = high.checked_sub(low)?;
    if span.is_zero() {
        Some(Decimal::ZERO)
    } else {
        candles
            .last()?
            .candle()
            .close()
            .value()
            .checked_sub(low)?
            .checked_div(span)
    }
}

fn robust_z(values: &[Decimal]) -> Option<Decimal> {
    let current = *values.last()?;
    let center = median(values)?;
    let deviations = values
        .iter()
        .map(|value| value.checked_sub(center).map(|difference| difference.abs()))
        .collect::<Option<Vec<_>>>()?;
    let denominator = median(&deviations)?
        .checked_mul(Decimal::new(14_826, 4))?
        .checked_add(Decimal::new(1, 12))?;
    current
        .checked_sub(center)?
        .checked_div(denominator)
        .map(|value| value.clamp(Decimal::from(-3), Decimal::from(3)))
        .and_then(|value| value.checked_div(Decimal::from(3)))
}

fn median(values: &[Decimal]) -> Option<Decimal> {
    let mut sorted = values.to_vec();
    sorted.sort();
    let middle = sorted.len().checked_div(2)?;
    if sorted.len().is_multiple_of(2) {
        sorted[middle - 1]
            .checked_add(sorted[middle])?
            .checked_div(Decimal::from(2))
    } else {
        sorted.get(middle).copied()
    }
}

fn premium(context: &AssetContext) -> Option<Decimal> {
    context
        .mark_price()
        .value()
        .checked_sub(context.oracle_price().value())?
        .checked_div(context.oracle_price().value())
}

fn open_interest_change(contexts: &[&AssetContext], lookback: usize) -> Option<Decimal> {
    let current = contexts.last()?.open_interest().value();
    let prior = contexts
        .get(contexts.len().checked_sub(lookback + 1)?)?
        .open_interest()
        .value();
    current.checked_div(prior)?.checked_sub(Decimal::ONE)
}

fn percentile(values: &[Decimal], current: Decimal) -> Option<Decimal> {
    let count = values.iter().filter(|value| **value <= current).count();
    Decimal::from(count).checked_div(Decimal::from(values.len()))
}

fn spread_bps(bbo: &Bbo) -> Option<Decimal> {
    let bid = bbo.bid().price().value();
    let ask = bbo.ask().price().value();
    ask.checked_sub(bid)?
        .checked_div(ask.checked_add(bid)?.checked_div(Decimal::from(2))?)?
        .checked_mul(Decimal::from(10_000))
}

#[derive(Clone, Copy)]
enum BookSide {
    Bid,
    Ask,
}

fn depth(book: &BookSnapshot, bbo: &Bbo, bps: u32, side: BookSide) -> Option<Decimal> {
    let bid = bbo.bid().price().value();
    let ask = bbo.ask().price().value();
    let mid = bid.checked_add(ask)?.checked_div(Decimal::from(2))?;
    let band = mid
        .checked_mul(Decimal::from(bps))?
        .checked_div(Decimal::from(10_000))?;
    let lower = mid.checked_sub(band)?;
    let upper = mid.checked_add(band)?;
    let levels = match side {
        BookSide::Bid => book.bids(),
        BookSide::Ask => book.asks(),
    };
    levels
        .iter()
        .filter(|level| match side {
            BookSide::Bid => level.price().value() >= lower && level.price().value() <= mid,
            BookSide::Ask => level.price().value() >= mid && level.price().value() <= upper,
        })
        .try_fold(Decimal::ZERO, |total, level| {
            level
                .price()
                .value()
                .checked_mul(level.quantity().value())
                .and_then(|notional| total.checked_add(notional))
        })
}

fn schema_hash() -> String {
    let mut hasher = Hasher::new_derive_key("trench.feature-schema.v1");
    hasher.update(FEATURE_SCHEMA.as_bytes());
    hasher.finalize().to_hex().to_string()
}

fn feature_snapshot_hash(snapshot: &FeatureSnapshot) -> String {
    let mut hasher = Hasher::new_derive_key("trench.feature-snapshot.v1");
    hasher.update(snapshot.market.as_str().as_bytes());
    hasher.update(&[0, candle_interval_tag(snapshot.sleeve)]);
    hasher.update(&snapshot.as_of_time.value().to_be_bytes());
    hasher.update(snapshot.schema_hash.as_bytes());
    if let Some(input_range) = &snapshot.input_range {
        hasher.update(input_range.digest().as_bytes());
    } else {
        hasher.update(b"none");
    }
    hasher.update(&[
        u8::from(snapshot.completeness.candles),
        u8::from(snapshot.completeness.context),
        u8::from(snapshot.completeness.funding),
        u8::from(snapshot.completeness.microstructure),
        u8::from(snapshot.completeness.regime),
        u8::from(snapshot.completeness.calculations),
        u8::from(snapshot.completeness.cross_section),
        unready_reason_tag(snapshot.unready_reason),
    ]);
    for (name, value) in &snapshot.values {
        hasher.update(name.as_bytes());
        hasher.update(&[0]);
        hasher.update(value.to_string().as_bytes());
        hasher.update(&[0]);
    }
    if let Some(regime) = snapshot.regime {
        for value in [
            regime.ema_8,
            regime.ema_32,
            regime.atr_14,
            regime.adx_14,
            regime.realized_volatility_20,
        ] {
            hasher.update(value.to_string().as_bytes());
            hasher.update(&[0]);
        }
    }
    hasher.finalize().to_hex().to_string()
}

const fn candle_interval_tag(interval: CandleInterval) -> u8 {
    match interval {
        CandleInterval::FifteenMinutes => 0,
        CandleInterval::OneHour => 1,
    }
}

const fn unready_reason_tag(reason: Option<FeatureUnreadyReason>) -> u8 {
    match reason {
        None => 0,
        Some(FeatureUnreadyReason::HistoryPruned) => 1,
        Some(FeatureUnreadyReason::CandleHistory) => 2,
        Some(FeatureUnreadyReason::ContextHistory) => 3,
        Some(FeatureUnreadyReason::FundingHistory) => 4,
        Some(FeatureUnreadyReason::Microstructure) => 5,
        Some(FeatureUnreadyReason::StaleBboOrBook) => 6,
        Some(FeatureUnreadyReason::HourlyRegime) => 7,
        Some(FeatureUnreadyReason::CalculationFailed) => 8,
        Some(FeatureUnreadyReason::CrossSection) => 9,
        Some(FeatureUnreadyReason::InputProvenance) => 10,
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use super::{
        CommonFeatureEngine, FeatureError, FeatureInputKind, FeatureSnapshot, FeatureUnreadyReason,
        MAX_BBO_BOOK_AGE_NS,
    };
    use crate::candle::CandleAggregator;
    use crate::domain::{Market, Price, Quantity, Side, Usdc};
    use crate::event::{
        AssetContext, Bbo, BookLevel, BookSnapshot, Funding, FundingRate, MarketEvent, TimestampNs,
        Trade,
    };
    use crate::universe::TradeableUniverse;

    const FIFTEEN_MINUTES_NS: i128 = 900_000_000_000;
    const THIRTY_DAYS_NS: i128 = 30 * 24 * 60 * 60 * 1_000_000_000;

    fn timestamp(value: i128) -> TimestampNs {
        absolute_timestamp(
            value
                .checked_add(THIRTY_DAYS_NS)
                .expect("test timestamp offset must fit"),
        )
    }

    fn absolute_timestamp(value: i128) -> TimestampNs {
        TimestampNs::new(value).expect("test timestamp must be valid")
    }

    fn price(value: Decimal) -> Price {
        Price::new(value).expect("test price must be valid")
    }

    fn quantity(value: Decimal) -> Quantity {
        Quantity::new(value).expect("test quantity must be valid")
    }

    fn market(value: &str) -> Market {
        Market::new(value).expect("test market must be valid")
    }

    fn bbo_at(market: Market, received_at: i128, sequence: u64, bid: Decimal) -> MarketEvent {
        bbo_at_time(market, 10, received_at, sequence, bid)
    }

    fn bbo_at_time(
        market: Market,
        event_time: i128,
        received_at: i128,
        sequence: u64,
        bid: Decimal,
    ) -> MarketEvent {
        MarketEvent::bbo(
            timestamp(event_time),
            timestamp(received_at),
            market,
            Bbo::new(
                sequence,
                BookLevel::new(price(bid), quantity(dec!(1))),
                BookLevel::new(price(bid + dec!(1)), quantity(dec!(1))),
            )
            .expect("test BBO must be valid"),
        )
        .expect("test BBO event must be valid")
    }

    fn book_at(market: Market, received_at: i128, sequence: u64, bid: Decimal) -> MarketEvent {
        book_at_time(market, 10, received_at, sequence, bid)
    }

    fn book_at_time(
        market: Market,
        event_time: i128,
        received_at: i128,
        sequence: u64,
        bid: Decimal,
    ) -> MarketEvent {
        MarketEvent::book_snapshot(
            timestamp(event_time),
            timestamp(received_at),
            market,
            BookSnapshot::new(
                sequence,
                vec![BookLevel::new(price(bid), quantity(dec!(1)))],
                vec![BookLevel::new(price(bid + dec!(1)), quantity(dec!(1)))],
            ),
        )
        .expect("test book event must be valid")
    }

    fn bbo_at_absolute(
        market: Market,
        event_time: i128,
        received_at: i128,
        sequence: u64,
        bid: Decimal,
    ) -> MarketEvent {
        MarketEvent::bbo(
            absolute_timestamp(event_time),
            absolute_timestamp(received_at),
            market,
            Bbo::new(
                sequence,
                BookLevel::new(price(bid), quantity(dec!(1))),
                BookLevel::new(price(bid + dec!(1)), quantity(dec!(1))),
            )
            .expect("test BBO must be valid"),
        )
        .expect("test BBO event must be valid")
    }

    fn book_at_absolute(
        market: Market,
        event_time: i128,
        received_at: i128,
        sequence: u64,
        bid: Decimal,
    ) -> MarketEvent {
        MarketEvent::book_snapshot(
            absolute_timestamp(event_time),
            absolute_timestamp(received_at),
            market,
            BookSnapshot::new(
                sequence,
                vec![BookLevel::new(price(bid), quantity(dec!(1)))],
                vec![BookLevel::new(price(bid + dec!(1)), quantity(dec!(1)))],
            ),
        )
        .expect("test book event must be valid")
    }

    #[derive(Clone, Copy)]
    struct PopulateRange {
        start: u64,
        count: u64,
        offset: Decimal,
        step: Decimal,
        volume_spike: Option<(u64, Decimal)>,
    }

    #[derive(Clone, Copy, Default)]
    struct ReceiptDelays {
        direct_at: Option<u64>,
        trade_at: Option<u64>,
    }

    fn populate(
        engine: &mut CommonFeatureEngine,
        aggregator: &mut CandleAggregator,
        market: Market,
        start: u64,
        count: u64,
        offset: Decimal,
        step: Decimal,
    ) {
        populate_with_open_interest(
            engine,
            aggregator,
            market,
            PopulateRange {
                start,
                count,
                offset,
                step,
                volume_spike: None,
            },
            None,
        );
    }

    fn populate_with_open_interest(
        engine: &mut CommonFeatureEngine,
        aggregator: &mut CandleAggregator,
        market: Market,
        range: PopulateRange,
        zero_open_interest_at: Option<u64>,
    ) {
        populate_with_receipt_delays(
            engine,
            aggregator,
            market,
            range,
            zero_open_interest_at,
            ReceiptDelays::default(),
        );
    }

    fn populate_with_receipt_delays(
        engine: &mut CommonFeatureEngine,
        aggregator: &mut CandleAggregator,
        market: Market,
        range: PopulateRange,
        zero_open_interest_at: Option<u64>,
        receipt_delays: ReceiptDelays,
    ) {
        for index in range.start..range.start + range.count {
            let open = i128::from(index) * FIFTEEN_MINUTES_NS;
            let close = open + FIFTEEN_MINUTES_NS;
            let value = range.offset + Decimal::from(index) * range.step;
            let bid = price(value - dec!(0.5));
            let ask = price(value + dec!(0.5));
            let trade_received_at = if receipt_delays.trade_at == Some(index) {
                close + 1
            } else {
                close - 1
            };
            let direct_received_at = if receipt_delays.direct_at == Some(index) {
                close + 1
            } else {
                close
            };
            let volume = range
                .volume_spike
                .filter(|(spike_at, _)| *spike_at == index)
                .map_or(dec!(1), |(_, volume)| volume);
            let trade = MarketEvent::trade(
                timestamp(close - 1),
                timestamp(trade_received_at),
                market.clone(),
                Trade::new(index + 1, Side::Buy, price(value), quantity(volume))
                    .expect("test trade must be valid"),
            )
            .expect("test trade event must be valid");
            aggregator.ingest(&trade).expect("trade must be accepted");
            engine
                .observe(&trade)
                .expect("trade observation must be accepted");
            engine
                .observe(&trade)
                .expect("duplicate trade observation must be idempotent");

            let context = MarketEvent::asset_context(
                timestamp(close),
                timestamp(direct_received_at),
                market.clone(),
                AssetContext::new(
                    price(value),
                    price(value - dec!(0.25)),
                    Some(price(value)),
                    quantity(if zero_open_interest_at == Some(index) {
                        Decimal::ZERO
                    } else {
                        Decimal::from(index + 1)
                    }),
                    Usdc::new(Decimal::from(1_000_000_u64 + index))
                        .expect("test USDC must be valid"),
                    FundingRate::new(Decimal::from(index) / dec!(10000)),
                ),
            )
            .expect("test context must be valid");
            let bbo = MarketEvent::bbo(
                timestamp(close),
                timestamp(direct_received_at),
                market.clone(),
                Bbo::new(
                    index + 1,
                    BookLevel::new(bid, quantity(dec!(10))),
                    BookLevel::new(ask, quantity(dec!(10))),
                )
                .expect("test BBO must be valid"),
            )
            .expect("test BBO event must be valid");
            let book = MarketEvent::book_snapshot(
                timestamp(close),
                timestamp(direct_received_at),
                market.clone(),
                BookSnapshot::new(
                    index + 1,
                    vec![
                        BookLevel::new(bid, quantity(dec!(10))),
                        BookLevel::new(price(value - dec!(1)), quantity(dec!(10))),
                    ],
                    vec![
                        BookLevel::new(ask, quantity(dec!(10))),
                        BookLevel::new(price(value + dec!(1)), quantity(dec!(10))),
                    ],
                ),
            )
            .expect("test book event must be valid");
            let funding = MarketEvent::funding(
                timestamp(close),
                timestamp(direct_received_at),
                market.clone(),
                Funding::new(
                    FundingRate::new(Decimal::from(index) / dec!(10000)),
                    price(value),
                ),
            )
            .expect("test funding event must be valid");
            for event in [&context, &bbo, &book, &funding] {
                engine.observe(event).expect("observation must be accepted");
                engine
                    .observe(event)
                    .expect("duplicate observation must be idempotent");
            }
        }
    }

    fn complete(
        engine: &mut CommonFeatureEngine,
        aggregator: &mut CandleAggregator,
        close: TimestampNs,
    ) {
        for candle in aggregator
            .complete_through(close)
            .expect("watermark must be valid")
        {
            engine
                .ingest_candle(candle.clone())
                .expect("candle must be accepted");
            engine
                .ingest_candle(candle)
                .expect("duplicate candle must be idempotent");
        }
    }

    fn complete_with_canonical_funding_history(
        engine: &mut CommonFeatureEngine,
        aggregator: &mut CandleAggregator,
        close: TimestampNs,
    ) {
        complete(engine, aggregator, close);
        let markets = engine.markets.iter().cloned().collect::<Vec<_>>();
        for market in markets {
            for sleeve in [
                crate::event::CandleInterval::FifteenMinutes,
                crate::event::CandleInterval::OneHour,
            ] {
                let count = super::derivative_history_bars(sleeve);
                let interval = i128::from(sleeve.duration().value());
                let start = i128::from(close.value())
                    .checked_sub(
                        interval
                            .checked_mul(
                                i128::try_from(count - 1).expect("fixed history length must fit"),
                            )
                            .expect("fixed history span must fit"),
                    )
                    .expect("test epoch must cover canonical funding history");
                let samples = engine
                    .funding_samples
                    .entry((market.clone(), sleeve))
                    .or_default();
                for offset in 0..count {
                    let boundary = absolute_timestamp(
                        start
                            .checked_add(
                                interval
                                    .checked_mul(
                                        i128::try_from(offset)
                                            .expect("fixed history index must fit"),
                                    )
                                    .expect("fixed sample offset must fit"),
                            )
                            .expect("fixed sample boundary must fit"),
                    );
                    let event = MarketEvent::funding(
                        boundary,
                        boundary,
                        market.clone(),
                        Funding::new(FundingRate::new(Decimal::from(offset)), price(dec!(100))),
                    )
                    .expect("canonical funding sample must be valid");
                    samples.insert(boundary, event);
                }
            }
        }
    }

    fn tradeable_universe(
        decision: TimestampNs,
        markets: impl IntoIterator<Item = Market>,
    ) -> TradeableUniverse {
        let completed_hour = absolute_timestamp(
            i128::from(decision.value() / 3_600_000_000_000) * 3_600_000_000_000,
        );
        TradeableUniverse::new(completed_hour, markets).expect("test universe must be valid")
    }

    fn frozen_snapshots(
        engine: &CommonFeatureEngine,
        sleeve: crate::event::CandleInterval,
        decision: TimestampNs,
    ) -> Vec<FeatureSnapshot> {
        let universe = tradeable_universe(decision, engine.markets_at(sleeve, decision));
        engine.snapshots_at_with_universe(sleeve, decision, Some(&universe))
    }

    fn populate_long_history(
        engine: &mut CommonFeatureEngine,
        aggregator: &mut CandleAggregator,
        market: Market,
        receipt_delays: ReceiptDelays,
    ) -> TimestampNs {
        const BARS: u64 = 90 * 24 * 4 + 20 * 4 + 4;

        for start in (0..BARS).step_by(512) {
            let count = (BARS - start).min(512);
            populate_with_receipt_delays(
                engine,
                aggregator,
                market.clone(),
                PopulateRange {
                    start,
                    count,
                    offset: dec!(100),
                    step: dec!(1),
                    volume_spike: None,
                },
                None,
                receipt_delays,
            );
            complete(
                engine,
                aggregator,
                timestamp(i128::from(start + count) * FIFTEEN_MINUTES_NS),
            );
        }
        timestamp(i128::from(BARS) * FIFTEEN_MINUTES_NS)
    }

    #[test]
    fn same_exchange_time_uses_receipt_time_before_identity_for_latest_bbo_and_book() {
        let market = market("BTC");
        let (early_bbo, late_bbo) = (1_u64..64)
            .flat_map(|early_sequence| {
                ((early_sequence + 1)..64).map(move |late_sequence| (early_sequence, late_sequence))
            })
            .find_map(|(early_sequence, late_sequence)| {
                let early = bbo_at(market.clone(), 20, early_sequence, dec!(100));
                let late = bbo_at(market.clone(), 30, late_sequence, dec!(200));
                (early.event_id() > late.event_id()).then_some((early, late))
            })
            .expect("test BBO identities must include an order opposite to receipt time");
        let (early_book, late_book) = (1_u64..64)
            .flat_map(|early_sequence| {
                ((early_sequence + 1)..64).map(move |late_sequence| (early_sequence, late_sequence))
            })
            .find_map(|(early_sequence, late_sequence)| {
                let early = book_at(market.clone(), 20, early_sequence, dec!(100));
                let late = book_at(market.clone(), 30, late_sequence, dec!(200));
                (early.event_id() > late.event_id()).then_some((early, late))
            })
            .expect("test book identities must include an order opposite to receipt time");
        assert!(early_bbo.event_id() > late_bbo.event_id());
        assert!(early_book.event_id() > late_book.event_id());

        let mut engine = CommonFeatureEngine::new();
        for event in [&late_bbo, &early_bbo, &late_book, &early_book] {
            engine.observe(event).expect("event must be accepted");
        }

        assert_eq!(
            engine
                .latest_bbo(&market, timestamp(30))
                .expect("latest BBO must exist")
                .event_id(),
            late_bbo.event_id()
        );
        assert_eq!(
            engine
                .latest_book(&market, timestamp(30))
                .expect("latest book must exist")
                .event_id(),
            late_book.event_id()
        );
    }

    #[test]
    fn input_range_boundaries_follow_event_time_receipt_and_identity() {
        let market = market("BTC");
        let (earlier, later) = (1_u64..64)
            .flat_map(|early_sequence| {
                ((early_sequence + 1)..64).map(move |late_sequence| (early_sequence, late_sequence))
            })
            .find_map(|(early_sequence, late_sequence)| {
                let earlier = bbo_at_time(market.clone(), 10, 20, early_sequence, dec!(100));
                let later = bbo_at_time(market.clone(), 10, 30, late_sequence, dec!(200));
                (earlier.event_id() > later.event_id()).then_some((earlier, later))
            })
            .expect("test identities must oppose receipt ordering");

        let range = super::InputEventRange::from_spans(vec![
            super::InputEventSpan::event(FeatureInputKind::Bbo, &later),
            super::InputEventSpan::event(FeatureInputKind::Bbo, &earlier),
        ])
        .expect("nonempty input range must build");

        assert_eq!(range.first_event_id(), earlier.event_id());
        assert_eq!(range.last_event_id(), later.event_id());
    }

    #[test]
    fn source_pruning_preserves_exact_event_replays_within_the_finalized_horizon() {
        let market = market("BTC");
        let events = (1..=super::POINT_EVENT_HISTORY + 1)
            .map(|sequence| bbo_at(market.clone(), 20, sequence as u64, dec!(100)))
            .collect::<Vec<_>>();
        let pruned = events
            .iter()
            .min_by_key(|event| event.event_id())
            .expect("events must not be empty")
            .clone();
        let mut engine = CommonFeatureEngine::new();
        for event in &events {
            engine.observe(event).expect("event must be accepted");
        }

        assert_eq!(engine.observe(&pruned), Ok(()));
    }

    #[test]
    fn source_pruning_keeps_changed_receipts_as_conflicts_within_the_finalized_horizon() {
        let market = market("BTC");
        let events = (1..=super::POINT_EVENT_HISTORY + 1)
            .map(|sequence| bbo_at(market.clone(), 20, sequence as u64, dec!(100)))
            .collect::<Vec<_>>();
        let pruned = events
            .iter()
            .min_by_key(|event| event.event_id())
            .expect("events must not be empty");
        let changed_receipt = match pruned.kind() {
            crate::event::MarketEventKind::Bbo(bbo) => {
                bbo_at(market.clone(), 21, bbo.sequence(), dec!(100))
            }
            _ => panic!("test event must be a BBO"),
        };
        let mut engine = CommonFeatureEngine::new();
        for event in &events {
            engine.observe(event).expect("event must be accepted");
        }

        assert!(matches!(
            engine.observe(&changed_receipt),
            Err(FeatureError::ConflictingEventReceiptTime { .. })
        ));
    }

    #[test]
    fn replay_beyond_the_finalized_event_horizon_is_not_reported_as_source_input() {
        let market = market("BTC");
        let events = (1..=super::POINT_EVENT_HISTORY + super::FINALIZED_EVENT_ID_HORIZON + 1)
            .map(|sequence| {
                bbo_at_time(
                    market.clone(),
                    sequence as i128,
                    sequence as i128,
                    sequence as u64,
                    dec!(100),
                )
            })
            .collect::<Vec<_>>();
        let expired = events.first().expect("events must not be empty").clone();
        let mut engine = CommonFeatureEngine::new();
        for event in &events {
            engine.observe(event).expect("event must be accepted");
        }
        assert_eq!(
            engine.finalized_events.len(),
            super::FINALIZED_EVENT_ID_HORIZON
        );
        assert_eq!(
            engine.finalized_event_order.len(),
            super::FINALIZED_EVENT_ID_HORIZON
        );

        let error = engine
            .observe(&expired)
            .expect_err("expired event identity must not be accepted");
        assert!(matches!(
            error,
            FeatureError::EventReplayOutsideHorizon {
                limit: super::FINALIZED_EVENT_ID_HORIZON,
                ..
            }
        ));
    }

    #[test]
    fn pruned_completed_candle_replays_remain_idempotent_within_the_finalized_horizon() {
        let market = market("BTC");
        let mut source_engine = CommonFeatureEngine::new();
        let mut aggregator = CandleAggregator::new();
        populate(
            &mut source_engine,
            &mut aggregator,
            market.clone(),
            0,
            99,
            dec!(100),
            dec!(1),
        );
        let candles = aggregator
            .complete_through(timestamp(99 * FIFTEEN_MINUTES_NS))
            .expect("watermark must finalize all completed candles");
        let pruned = candles
            .iter()
            .find(|candle| {
                candle.market() == &market
                    && candle.candle().interval() == crate::event::CandleInterval::FifteenMinutes
                    && candle.candle().open_time() == timestamp(0)
            })
            .expect("first completed fifteen-minute candle must exist")
            .clone();
        let mut engine = CommonFeatureEngine::new();
        for candle in candles {
            engine
                .ingest_candle(candle)
                .expect("completed candle must be accepted");
        }

        assert_eq!(engine.ingest_candle(pruned), Ok(()));
    }

    #[test]
    fn future_bars_cannot_change_prior_snapshots() {
        let mut engine = CommonFeatureEngine::new();
        let mut aggregator = CandleAggregator::new();
        populate(
            &mut engine,
            &mut aggregator,
            market("BTC"),
            0,
            128,
            dec!(100),
            dec!(1),
        );
        populate(
            &mut engine,
            &mut aggregator,
            market("ETH"),
            0,
            128,
            dec!(100),
            dec!(1),
        );
        let decision = timestamp(128 * FIFTEEN_MINUTES_NS);
        complete_with_canonical_funding_history(&mut engine, &mut aggregator, decision);
        let before = frozen_snapshots(
            &engine,
            crate::event::CandleInterval::FifteenMinutes,
            decision,
        );
        assert!(
            before.iter().all(|snapshot| snapshot.is_ready()),
            "unexpected unready snapshots: {before:#?}"
        );

        populate(
            &mut engine,
            &mut aggregator,
            market("BTC"),
            128,
            1,
            dec!(100),
            dec!(1),
        );
        complete(
            &mut engine,
            &mut aggregator,
            timestamp(129 * FIFTEEN_MINUTES_NS),
        );
        assert_eq!(
            before,
            frozen_snapshots(
                &engine,
                crate::event::CandleInterval::FifteenMinutes,
                decision
            )
        );
    }

    #[test]
    fn delayed_direct_events_are_excluded_from_decision_provenance() {
        let mut engine = CommonFeatureEngine::new();
        let mut aggregator = CandleAggregator::new();
        let receipt_delays = ReceiptDelays {
            direct_at: Some(127),
            trade_at: None,
        };
        for (market, offset) in [(market("BTC"), dec!(100)), (market("ETH"), dec!(200))] {
            populate_with_receipt_delays(
                &mut engine,
                &mut aggregator,
                market,
                PopulateRange {
                    start: 0,
                    count: 128,
                    offset,
                    step: dec!(1),
                    volume_spike: None,
                },
                None,
                receipt_delays,
            );
        }
        let decision = timestamp(128 * FIFTEEN_MINUTES_NS);
        complete(&mut engine, &mut aggregator, decision);

        for snapshot in engine.snapshots_at(crate::event::CandleInterval::FifteenMinutes, decision)
        {
            assert_eq!(
                snapshot.unready_reason(),
                Some(FeatureUnreadyReason::StaleBboOrBook)
            );
            assert!(snapshot.values().is_empty());
        }
    }

    #[test]
    fn delayed_candle_is_unavailable_at_its_completed_bar_boundary() {
        let mut engine = CommonFeatureEngine::new();
        let mut aggregator = CandleAggregator::new();
        populate_with_receipt_delays(
            &mut engine,
            &mut aggregator,
            market("BTC"),
            PopulateRange {
                start: 0,
                count: 128,
                offset: dec!(100),
                step: dec!(1),
                volume_spike: None,
            },
            None,
            ReceiptDelays {
                direct_at: None,
                trade_at: Some(127),
            },
        );
        let decision = timestamp(128 * FIFTEEN_MINUTES_NS);
        complete(&mut engine, &mut aggregator, decision);

        assert!(
            engine
                .snapshots_at(crate::event::CandleInterval::FifteenMinutes, decision)
                .is_empty()
        );
    }

    #[test]
    fn snapshots_have_complete_finite_common_schema_and_hourly_regime_inputs() {
        let mut engine = CommonFeatureEngine::new();
        let mut aggregator = CandleAggregator::new();
        populate(
            &mut engine,
            &mut aggregator,
            market("BTC"),
            0,
            128,
            dec!(100),
            dec!(1),
        );
        populate(
            &mut engine,
            &mut aggregator,
            market("ETH"),
            0,
            128,
            dec!(200),
            dec!(1),
        );
        let decision = timestamp(128 * FIFTEEN_MINUTES_NS);
        complete_with_canonical_funding_history(&mut engine, &mut aggregator, decision);

        let snapshots = frozen_snapshots(
            &engine,
            crate::event::CandleInterval::FifteenMinutes,
            decision,
        );
        assert_eq!(snapshots.len(), 2);
        for snapshot in snapshots {
            assert!(snapshot.is_ready(), "unexpected snapshot: {snapshot:#?}");
            assert_eq!(snapshot.as_of_time(), decision);
            assert!(snapshot.input_range().is_some());
            assert!(snapshot.regime().is_some());
            for name in [
                "ema_8",
                "atr_14",
                "rsi_14",
                "adx_14",
                "realized_volatility_64",
                "donchian_20_position",
                "volume_robust_z_20",
                "premium",
                "open_interest_change_16",
                "funding_percentile_30",
                "spread_bps",
                "depth_10bps",
                "trade_imbalance_15m",
                "cross_return_4_rank",
                "cross_return_16_rank",
                "cross_return_96_rank",
            ] {
                assert!(
                    snapshot.values().contains_key(name),
                    "missing {name}: {:#?}",
                    snapshot.values()
                );
            }
        }
    }

    #[test]
    fn rules_inputs_require_a_frozen_tradeable_universe_and_retain_exact_contract_fields() {
        let mut engine = CommonFeatureEngine::new();
        let mut aggregator = CandleAggregator::new();
        let markets = [market("BTC"), market("ETH")];
        for (market, offset) in [
            (markets[0].clone(), dec!(100)),
            (markets[1].clone(), dec!(200)),
        ] {
            populate(
                &mut engine,
                &mut aggregator,
                market,
                0,
                128,
                offset,
                dec!(1),
            );
        }
        let decision = timestamp(128 * FIFTEEN_MINUTES_NS);
        complete_with_canonical_funding_history(&mut engine, &mut aggregator, decision);

        let without_membership = engine.snapshots_at_with_universe(
            crate::event::CandleInterval::FifteenMinutes,
            decision,
            None,
        );
        assert!(
            without_membership
                .iter()
                .all(|snapshot| !snapshot.is_ready())
        );

        let universe = tradeable_universe(decision, markets.clone());
        let snapshots = engine.snapshots_at_with_universe(
            crate::event::CandleInterval::FifteenMinutes,
            decision,
            Some(&universe),
        );
        for snapshot in snapshots {
            assert!(snapshot.is_ready(), "unexpected snapshot: {snapshot:#?}");
            for name in [
                "close",
                "ema_20",
                "close_ema_20_residual",
                "high_10",
                "low_10",
                "bid_depth_10bps",
                "ask_depth_10bps",
                "bid_depth_25bps",
                "ask_depth_25bps",
                "bid_depth_50bps",
                "ask_depth_50bps",
            ] {
                assert!(snapshot.values().contains_key(name), "missing {name}");
            }
            assert_eq!(
                snapshot
                    .input_range()
                    .and_then(|range| range.universe_digest()),
                Some(universe.digest())
            );
            assert!(!snapshot.snapshot_hash().is_empty());
        }
    }

    #[test]
    fn latest_completed_hour_universe_remains_current_at_the_fifteen_minute_boundary() {
        let mut engine = CommonFeatureEngine::new();
        let mut aggregator = CandleAggregator::new();
        let markets = [market("BTC"), market("ETH")];
        for (market, offset) in [
            (markets[0].clone(), dec!(100)),
            (markets[1].clone(), dec!(200)),
        ] {
            populate(
                &mut engine,
                &mut aggregator,
                market,
                0,
                129,
                offset,
                dec!(1),
            );
        }
        let hour = timestamp(128 * FIFTEEN_MINUTES_NS);
        let decision = timestamp(129 * FIFTEEN_MINUTES_NS);
        complete_with_canonical_funding_history(&mut engine, &mut aggregator, decision);
        let universe = tradeable_universe(hour, markets);

        let snapshots = engine.snapshots_at_with_universe(
            crate::event::CandleInterval::FifteenMinutes,
            decision,
            Some(&universe),
        );

        assert!(
            snapshots
                .iter()
                .all(|snapshot| snapshot.completeness().cross_section())
        );
    }

    #[test]
    fn bbo_and_book_freshness_is_receipt_aware_and_bounded_at_decision_time() {
        let mut engine = CommonFeatureEngine::new();
        let mut aggregator = CandleAggregator::new();
        for (market, offset) in [(market("BTC"), dec!(100)), (market("ETH"), dec!(200))] {
            populate(
                &mut engine,
                &mut aggregator,
                market,
                0,
                128,
                offset,
                dec!(1),
            );
        }
        let decision = timestamp(128 * FIFTEEN_MINUTES_NS);
        complete_with_canonical_funding_history(&mut engine, &mut aggregator, decision);
        let btc = market("BTC");
        let reset_market = |engine: &mut CommonFeatureEngine| {
            let sources = engine.events.get_mut(&btc).expect("BTC sources must exist");
            sources.bbo.clear();
            sources.books.clear();
        };
        let observe_pair = |engine: &mut CommonFeatureEngine,
                            event_time: i128,
                            received_at: i128| {
            let bbo = bbo_at_absolute(btc.clone(), event_time, received_at, 10_000, dec!(227));
            let book = book_at_absolute(btc.clone(), event_time, received_at, 10_000, dec!(227));
            engine.observe(&bbo).expect("BBO must be observed");
            engine.observe(&book).expect("book must be observed");
        };
        let universe = tradeable_universe(decision, [btc.clone(), market("ETH")]);

        reset_market(&mut engine);
        observe_pair(
            &mut engine,
            i128::from(decision.value() - MAX_BBO_BOOK_AGE_NS),
            i128::from(decision.value() - MAX_BBO_BOOK_AGE_NS),
        );
        let inclusive = engine
            .snapshots_at_with_universe(
                crate::event::CandleInterval::FifteenMinutes,
                decision,
                Some(&universe),
            )
            .into_iter()
            .find(|snapshot| snapshot.market() == &btc)
            .expect("BTC snapshot must exist");
        assert!(inclusive.completeness().microstructure());
        assert!(
            inclusive
                .input_range()
                .expect("fresh snapshot retains provenance")
                .spans()
                .iter()
                .any(|span| span.kind() == FeatureInputKind::Bbo)
        );

        reset_market(&mut engine);
        observe_pair(
            &mut engine,
            i128::from(decision.value() - MAX_BBO_BOOK_AGE_NS - 1),
            i128::from(decision.value() - MAX_BBO_BOOK_AGE_NS - 1),
        );
        let stale = engine
            .snapshots_at_with_universe(
                crate::event::CandleInterval::FifteenMinutes,
                decision,
                Some(&universe),
            )
            .into_iter()
            .find(|snapshot| snapshot.market() == &btc)
            .expect("BTC snapshot must exist");
        assert!(!stale.completeness().microstructure());
        assert_eq!(
            stale.unready_reason(),
            Some(FeatureUnreadyReason::StaleBboOrBook)
        );

        reset_market(&mut engine);
        observe_pair(
            &mut engine,
            i128::from(decision.value()),
            i128::from(decision.value()) + 1,
        );
        let future_receipt = engine
            .snapshots_at_with_universe(
                crate::event::CandleInterval::FifteenMinutes,
                decision,
                Some(&universe),
            )
            .into_iter()
            .find(|snapshot| snapshot.market() == &btc)
            .expect("BTC snapshot must exist");
        assert!(!future_receipt.completeness().microstructure());
    }

    #[test]
    fn funding_percentile_requires_thirty_calendar_days_of_canonical_boundary_samples() {
        let mut engine = CommonFeatureEngine::new();
        let mut aggregator = CandleAggregator::new();
        let btc = market("BTC");
        populate(
            &mut engine,
            &mut aggregator,
            btc.clone(),
            0,
            128,
            dec!(100),
            dec!(1),
        );
        let decision = timestamp(128 * FIFTEEN_MINUTES_NS);
        for sequence in 0..super::SOURCE_EVENT_HISTORY {
            let source_time = i128::from(decision.value())
                - i128::try_from(super::SOURCE_EVENT_HISTORY).expect("bounded source count fits")
                + i128::try_from(sequence).expect("bounded source index fits");
            let funding = MarketEvent::funding(
                timestamp(source_time),
                timestamp(source_time),
                btc.clone(),
                Funding::new(FundingRate::new(Decimal::from(sequence)), price(dec!(227))),
            )
            .expect("funding event must be valid");
            engine.observe(&funding).expect("funding must be observed");
        }
        complete(&mut engine, &mut aggregator, decision);

        let snapshot = engine
            .snapshots_at(crate::event::CandleInterval::FifteenMinutes, decision)
            .into_iter()
            .find(|snapshot| snapshot.market() == &btc)
            .expect("BTC snapshot must exist");

        assert!(!snapshot.completeness().funding());
        assert_eq!(
            snapshot.unready_reason(),
            Some(FeatureUnreadyReason::FundingHistory)
        );
    }

    #[test]
    fn rising_ema_has_a_positive_four_bar_slope() {
        let mut engine = CommonFeatureEngine::new();
        let mut aggregator = CandleAggregator::new();
        for (market, offset) in [(market("BTC"), dec!(100)), (market("ETH"), dec!(200))] {
            populate(
                &mut engine,
                &mut aggregator,
                market,
                0,
                128,
                offset,
                dec!(1),
            );
        }
        let decision = timestamp(128 * FIFTEEN_MINUTES_NS);
        complete_with_canonical_funding_history(&mut engine, &mut aggregator, decision);

        for snapshot in frozen_snapshots(
            &engine,
            crate::event::CandleInterval::FifteenMinutes,
            decision,
        ) {
            assert!(
                snapshot.values()["ema_8_slope_4"] > Decimal::ZERO,
                "rising EMA must have a positive slope"
            );
        }
    }

    #[test]
    fn current_volume_spike_has_a_positive_robust_z_score() {
        let mut engine = CommonFeatureEngine::new();
        let mut aggregator = CandleAggregator::new();
        for (market, offset, volume_spike) in [
            (market("BTC"), dec!(100), Some((127, dec!(100)))),
            (market("ETH"), dec!(200), None),
        ] {
            populate_with_open_interest(
                &mut engine,
                &mut aggregator,
                market,
                PopulateRange {
                    start: 0,
                    count: 128,
                    offset,
                    step: dec!(1),
                    volume_spike,
                },
                None,
            );
        }
        let decision = timestamp(128 * FIFTEEN_MINUTES_NS);
        complete_with_canonical_funding_history(&mut engine, &mut aggregator, decision);

        let snapshot = frozen_snapshots(
            &engine,
            crate::event::CandleInterval::FifteenMinutes,
            decision,
        )
        .into_iter()
        .find(|snapshot| snapshot.market() == &market("BTC"))
        .expect("BTC snapshot must exist");
        assert_eq!(snapshot.values()["volume_robust_z_20"], Decimal::ONE);
    }

    #[test]
    fn long_horizon_history_retains_exact_rule_inputs_and_round_trips_as_json() {
        let market = market("BTC");
        let mut engine = CommonFeatureEngine::new();
        let mut aggregator = CandleAggregator::new();
        let decision = populate_long_history(
            &mut engine,
            &mut aggregator,
            market.clone(),
            ReceiptDelays::default(),
        );

        let history = engine
            .long_horizon_history_at(
                &market,
                crate::event::CandleInterval::FifteenMinutes,
                decision,
            )
            .expect("fully retained 30-day and 90-day inputs must be available");
        assert_eq!(
            history.hourly_realized_volatility_20_history().len(),
            90 * 24
        );
        assert!(history.current_hourly_realized_volatility_20() >= Decimal::ZERO);
        assert_eq!(history.premium_history().len(), 30 * 24 * 4);
        assert_eq!(history.open_interest_change_4_history().len(), 30 * 24 * 4);
        assert_eq!(history.funding_history().len(), 30 * 24 * 4);
        assert!(history.completeness().is_complete());
        assert!(!history.input_digest().is_empty());
        assert!(
            history
                .premium_history()
                .iter()
                .all(|sample| !sample.source_event_id().is_empty()
                    && sample.source_received_at_ns() <= sample.as_of_time_ns())
        );
        let encoded = serde_json::to_string(&history).expect("history must serialize");
        let decoded: super::LongHorizonFeatureHistory =
            serde_json::from_str(&encoded).expect("history must deserialize");
        assert_eq!(decoded, history);
    }

    #[test]
    fn long_horizon_history_json_rejects_forged_digest_and_sample_ordering() {
        let market = market("BTC");
        let mut engine = CommonFeatureEngine::new();
        let mut aggregator = CandleAggregator::new();
        let decision = populate_long_history(
            &mut engine,
            &mut aggregator,
            market.clone(),
            ReceiptDelays::default(),
        );
        let history = engine
            .long_horizon_history_at(
                &market,
                crate::event::CandleInterval::FifteenMinutes,
                decision,
            )
            .expect("fully retained long-horizon history must be available");

        let mut forged_digest = serde_json::to_value(&history).expect("history must serialize");
        forged_digest["input_digest"] = serde_json::Value::String("forged".to_owned());
        let digest_error =
            match serde_json::from_value::<super::LongHorizonFeatureHistory>(forged_digest) {
                Err(error) => error,
                Ok(_) => panic!("a forged digest must not deserialize"),
            };
        assert!(
            digest_error
                .to_string()
                .contains("long-horizon history digest mismatch")
        );

        let mut forged_ordering = serde_json::to_value(&history).expect("history must serialize");
        let sample = forged_ordering["funding_history"]
            .get_mut(1)
            .expect("second funding sample must exist");
        let as_of = sample["as_of_time_ns"]
            .as_i64()
            .expect("sample boundary must be a signed timestamp");
        sample["as_of_time_ns"] = serde_json::Value::from(as_of + 1);
        let ordering_error =
            match serde_json::from_value::<super::LongHorizonFeatureHistory>(forged_ordering) {
                Err(error) => error,
                Ok(_) => panic!("a non-contiguous history must not deserialize"),
            };
        assert!(
            ordering_error
                .to_string()
                .contains("long-horizon history funding history is not contiguous")
        );

        let mut forged_source = serde_json::to_value(&history).expect("history must serialize");
        forged_source["premium_history"][0]["source_event_id"] =
            serde_json::Value::String(" invalid-source ".to_owned());
        let source_error =
            match serde_json::from_value::<super::LongHorizonFeatureHistory>(forged_source) {
                Err(error) => error,
                Ok(_) => panic!("an invalid scalar provenance identity must not deserialize"),
            };
        assert!(
            source_error
                .to_string()
                .contains("long-horizon history sample source event ID is invalid")
        );
    }

    #[test]
    fn long_horizon_history_rejects_a_primary_sleeve_without_a_close_at_the_requested_boundary() {
        let market = market("BTC");
        let mut engine = CommonFeatureEngine::new();
        let mut aggregator = CandleAggregator::new();
        let decision = populate_long_history(
            &mut engine,
            &mut aggregator,
            market.clone(),
            ReceiptDelays::default(),
        );
        let stale_boundary = timestamp(i128::from(decision.value()) + FIFTEEN_MINUTES_NS);

        assert!(matches!(
            engine.require_long_horizon_history_at(
                &market,
                crate::event::CandleInterval::FifteenMinutes,
                stale_boundary,
            ),
            Err(FeatureError::LongHorizonHistoryUnavailable { .. })
        ));
    }

    #[test]
    fn retained_boundary_samples_survive_context_and_funding_bursts_after_thirty_days() {
        let market = market("BTC");
        let mut engine = CommonFeatureEngine::new();
        let mut aggregator = CandleAggregator::new();
        let decision = populate_long_history(
            &mut engine,
            &mut aggregator,
            market.clone(),
            ReceiptDelays::default(),
        );
        let before = engine
            .long_horizon_history_at(
                &market,
                crate::event::CandleInterval::FifteenMinutes,
                decision,
            )
            .expect("continuous sampled history must be available");

        for sequence in 0..super::SOURCE_EVENT_HISTORY {
            let value = Decimal::from(10_000_u64 + sequence as u64);
            let received_at =
                timestamp(i128::from(decision.value()) + i128::from(sequence as u64) + 1);
            let burst = MarketEvent::asset_context(
                received_at,
                received_at,
                market.clone(),
                AssetContext::new(
                    price(value),
                    price(value - dec!(1)),
                    Some(price(value)),
                    quantity(value),
                    Usdc::new(Decimal::from(1_000_000_u64)).expect("USDC must be valid"),
                    FundingRate::new(Decimal::ZERO),
                ),
            )
            .expect("burst context must be valid");
            engine
                .observe(&burst)
                .expect("burst context must be accepted");
            let funding = MarketEvent::funding(
                received_at,
                received_at,
                market.clone(),
                Funding::new(
                    FundingRate::new(Decimal::from(sequence as u64)),
                    price(value),
                ),
            )
            .expect("burst funding must be valid");
            engine
                .observe(&funding)
                .expect("burst funding must be accepted");
        }

        let after = engine
            .long_horizon_history_at(
                &market,
                crate::event::CandleInterval::FifteenMinutes,
                decision,
            )
            .expect("canonical boundary samples must outlive raw context bursts");
        assert_eq!(after, before);
    }

    #[test]
    fn long_horizon_history_does_not_retime_a_delayed_final_source_to_a_non_boundary() {
        const BARS: u64 = 90 * 24 * 4 + 20 * 4 + 4;

        let market = market("BTC");
        let mut immediate_engine = CommonFeatureEngine::new();
        let mut immediate_aggregator = CandleAggregator::new();
        let decision = populate_long_history(
            &mut immediate_engine,
            &mut immediate_aggregator,
            market.clone(),
            ReceiptDelays::default(),
        );
        let immediate = immediate_engine
            .long_horizon_history_at(
                &market,
                crate::event::CandleInterval::FifteenMinutes,
                decision,
            )
            .expect("immediate source must be available");

        let mut delayed_engine = CommonFeatureEngine::new();
        let mut delayed_aggregator = CandleAggregator::new();
        assert_eq!(
            populate_long_history(
                &mut delayed_engine,
                &mut delayed_aggregator,
                market.clone(),
                ReceiptDelays {
                    direct_at: Some(BARS - 1),
                    trade_at: None,
                },
            ),
            decision
        );
        let before_receipt = delayed_engine
            .long_horizon_history_at(
                &market,
                crate::event::CandleInterval::FifteenMinutes,
                decision,
            )
            .expect("older source state remains exact at the bar close");
        assert_ne!(before_receipt, immediate);
        assert!(
            delayed_engine
                .long_horizon_history_at(
                    &market,
                    crate::event::CandleInterval::FifteenMinutes,
                    timestamp(i128::from(decision.value() + 1)),
                )
                .is_none()
        );
    }

    #[test]
    fn long_horizon_history_does_not_fill_missing_30_day_inputs() {
        let market = market("BTC");
        let mut engine = CommonFeatureEngine::new();
        let mut aggregator = CandleAggregator::new();
        populate(
            &mut engine,
            &mut aggregator,
            market.clone(),
            0,
            128,
            dec!(100),
            dec!(1),
        );
        let decision = timestamp(128 * FIFTEEN_MINUTES_NS);
        complete(&mut engine, &mut aggregator, decision);

        assert!(
            engine
                .long_horizon_history_at(
                    &market,
                    crate::event::CandleInterval::FifteenMinutes,
                    decision,
                )
                .is_none()
        );
        assert!(matches!(
            engine.require_long_horizon_history_at(
                &market,
                crate::event::CandleInterval::FifteenMinutes,
                decision,
            ),
            Err(FeatureError::LongHorizonHistoryUnavailable { .. })
        ));
    }

    #[test]
    fn equal_tradeable_universe_returns_receive_the_same_midrank() {
        let mut engine = CommonFeatureEngine::new();
        let mut aggregator = CandleAggregator::new();
        for market in [market("ETH"), market("BTC")] {
            populate(
                &mut engine,
                &mut aggregator,
                market,
                0,
                128,
                dec!(100),
                dec!(1),
            );
        }
        let decision = timestamp(128 * FIFTEEN_MINUTES_NS);
        complete_with_canonical_funding_history(&mut engine, &mut aggregator, decision);

        for snapshot in frozen_snapshots(
            &engine,
            crate::event::CandleInterval::FifteenMinutes,
            decision,
        ) {
            assert_eq!(
                snapshot.values().get("cross_return_4_rank"),
                Some(&dec!(0.75))
            );
            assert_eq!(
                snapshot.values().get("cross_return_16_rank"),
                Some(&dec!(0.75))
            );
        }
    }

    #[test]
    fn missing_cross_section_leaves_only_that_market_unready() {
        let mut engine = CommonFeatureEngine::new();
        let mut aggregator = CandleAggregator::new();
        populate(
            &mut engine,
            &mut aggregator,
            market("BTC"),
            0,
            128,
            dec!(100),
            dec!(1),
        );
        let decision = timestamp(128 * FIFTEEN_MINUTES_NS);
        complete(&mut engine, &mut aggregator, decision);

        let snapshot = engine
            .snapshots_at(crate::event::CandleInterval::OneHour, decision)
            .into_iter()
            .next()
            .expect("market must have a snapshot");
        assert!(!snapshot.is_ready());
        assert!(!snapshot.completeness().cross_section());
    }

    #[test]
    fn a_gap_in_the_contiguous_candle_warmup_stays_unready_without_imputation() {
        let mut engine = CommonFeatureEngine::new();
        let mut aggregator = CandleAggregator::new();
        populate(
            &mut engine,
            &mut aggregator,
            market("BTC"),
            0,
            128,
            dec!(100),
            dec!(1),
        );
        let decision = timestamp(128 * FIFTEEN_MINUTES_NS);
        for candle in aggregator
            .complete_through(decision)
            .expect("watermark must be valid")
        {
            let is_missing_fifteen_minute = candle.candle().interval()
                == crate::event::CandleInterval::FifteenMinutes
                && candle.candle().open_time() == timestamp(64 * FIFTEEN_MINUTES_NS);
            if !is_missing_fifteen_minute {
                engine
                    .ingest_candle(candle)
                    .expect("candle must be accepted");
            }
        }

        let snapshot = engine
            .snapshots_at(crate::event::CandleInterval::FifteenMinutes, decision)
            .into_iter()
            .next()
            .expect("market must have a snapshot");
        assert!(!snapshot.completeness().candles());
        assert!(!snapshot.is_ready());
        assert!(snapshot.values().is_empty());
    }

    #[test]
    fn complete_flat_markets_produce_finite_zero_volatility_features() {
        let mut engine = CommonFeatureEngine::new();
        let mut aggregator = CandleAggregator::new();
        populate(
            &mut engine,
            &mut aggregator,
            market("BTC"),
            0,
            128,
            dec!(100),
            dec!(0),
        );
        populate(
            &mut engine,
            &mut aggregator,
            market("ETH"),
            0,
            128,
            dec!(200),
            dec!(0),
        );
        let decision = timestamp(128 * FIFTEEN_MINUTES_NS);
        complete_with_canonical_funding_history(&mut engine, &mut aggregator, decision);

        for snapshot in frozen_snapshots(
            &engine,
            crate::event::CandleInterval::FifteenMinutes,
            decision,
        ) {
            assert!(snapshot.is_ready());
            assert_eq!(snapshot.values()["adx_14"], Decimal::ZERO);
            assert_eq!(snapshot.values()["realized_volatility_64"], Decimal::ZERO);
        }
    }

    #[test]
    fn calculation_failure_is_explicitly_unready_and_exposes_no_strategy_values() {
        let mut engine = CommonFeatureEngine::new();
        let mut aggregator = CandleAggregator::new();
        populate_with_open_interest(
            &mut engine,
            &mut aggregator,
            market("BTC"),
            PopulateRange {
                start: 0,
                count: 128,
                offset: dec!(100),
                step: dec!(1),
                volume_spike: None,
            },
            Some(111),
        );
        populate(
            &mut engine,
            &mut aggregator,
            market("ETH"),
            0,
            128,
            dec!(200),
            dec!(1),
        );
        let decision = timestamp(128 * FIFTEEN_MINUTES_NS);
        complete_with_canonical_funding_history(&mut engine, &mut aggregator, decision);

        let snapshot = engine
            .snapshots_at(crate::event::CandleInterval::FifteenMinutes, decision)
            .into_iter()
            .find(|snapshot| snapshot.market() == &market("BTC"))
            .expect("BTC snapshot must exist");
        assert!(!snapshot.completeness().calculations());
        assert_eq!(
            snapshot.unready_reason(),
            Some(FeatureUnreadyReason::CalculationFailed)
        );
        assert!(snapshot.input_range().is_some());
        assert!(!snapshot.is_ready());
        assert!(snapshot.values().is_empty());
    }

    #[test]
    fn provenance_covers_every_feature_dependency_and_ignores_future_events() {
        let mut engine = CommonFeatureEngine::new();
        let mut aggregator = CandleAggregator::new();
        populate(
            &mut engine,
            &mut aggregator,
            market("BTC"),
            0,
            128,
            dec!(100),
            dec!(1),
        );
        populate(
            &mut engine,
            &mut aggregator,
            market("ETH"),
            0,
            128,
            dec!(200),
            dec!(1),
        );
        let decision = timestamp(128 * FIFTEEN_MINUTES_NS);
        complete_with_canonical_funding_history(&mut engine, &mut aggregator, decision);

        let before = frozen_snapshots(
            &engine,
            crate::event::CandleInterval::FifteenMinutes,
            decision,
        )
        .into_iter()
        .find(|snapshot| snapshot.market() == &market("BTC"))
        .expect("BTC snapshot must exist");
        let range = before
            .input_range()
            .expect("ready snapshot must retain provenance");
        let count = |kind| {
            range
                .spans()
                .iter()
                .filter(|span| span.kind() == kind)
                .count()
        };
        assert_eq!(count(FeatureInputKind::FifteenMinuteCandle), 194);
        assert_eq!(count(FeatureInputKind::OneHourCandle), 32);
        assert_eq!(count(FeatureInputKind::AssetContext), 30);
        assert_eq!(count(FeatureInputKind::Funding), 30 * 24 * 4);
        assert_eq!(count(FeatureInputKind::Bbo), 2);
        assert_eq!(count(FeatureInputKind::Book), 2);
        assert_eq!(count(FeatureInputKind::MicrostructureTrade), 1);
        assert!(!range.digest().is_empty());

        populate(
            &mut engine,
            &mut aggregator,
            market("BTC"),
            128,
            1,
            dec!(100),
            dec!(1),
        );
        complete(
            &mut engine,
            &mut aggregator,
            timestamp(129 * FIFTEEN_MINUTES_NS),
        );
        let after = frozen_snapshots(
            &engine,
            crate::event::CandleInterval::FifteenMinutes,
            decision,
        )
        .into_iter()
        .find(|snapshot| snapshot.market() == &market("BTC"))
        .expect("BTC snapshot must still exist");
        assert_eq!(after.input_range(), before.input_range());
    }

    #[test]
    fn one_hour_snapshot_provenance_labels_primary_and_peer_candles_as_one_hour() {
        let mut engine = CommonFeatureEngine::new();
        let mut aggregator = CandleAggregator::new();
        for (market, offset) in [(market("BTC"), dec!(100)), (market("ETH"), dec!(200))] {
            populate(
                &mut engine,
                &mut aggregator,
                market,
                0,
                388,
                offset,
                dec!(1),
            );
        }
        let decision = timestamp(388 * FIFTEEN_MINUTES_NS);
        complete_with_canonical_funding_history(&mut engine, &mut aggregator, decision);

        for snapshot in frozen_snapshots(&engine, crate::event::CandleInterval::OneHour, decision) {
            let candle_kinds = snapshot
                .input_range()
                .expect("ready snapshot must retain provenance")
                .spans()
                .iter()
                .map(|span| span.kind())
                .filter(|kind| {
                    matches!(
                        kind,
                        FeatureInputKind::FifteenMinuteCandle | FeatureInputKind::OneHourCandle
                    )
                })
                .collect::<Vec<_>>();
            assert_eq!(
                candle_kinds,
                vec![FeatureInputKind::OneHourCandle; 194],
                "1h primary and peer return inputs must retain their source interval"
            );
        }
    }

    #[test]
    fn retained_history_is_capped_and_matches_a_fresh_replay_of_its_window() {
        let market = market("BTC");
        let mut retained_engine = CommonFeatureEngine::new();
        let mut retained_aggregator = CandleAggregator::new();
        populate(
            &mut retained_engine,
            &mut retained_aggregator,
            market.clone(),
            0,
            225,
            dec!(100),
            dec!(1),
        );
        let decision = timestamp(225 * FIFTEEN_MINUTES_NS);
        complete(&mut retained_engine, &mut retained_aggregator, decision);

        let mut replay_engine = CommonFeatureEngine::new();
        let mut replay_aggregator = CandleAggregator::new();
        populate(
            &mut replay_engine,
            &mut replay_aggregator,
            market.clone(),
            96,
            129,
            dec!(100),
            dec!(1),
        );
        complete(&mut replay_engine, &mut replay_aggregator, decision);

        assert_eq!(
            frozen_snapshots(
                &retained_engine,
                crate::event::CandleInterval::FifteenMinutes,
                decision
            ),
            frozen_snapshots(
                &replay_engine,
                crate::event::CandleInterval::FifteenMinutes,
                decision
            )
        );
        assert!(
            retained_engine
                .candles
                .get(&(market.clone(), crate::event::CandleInterval::FifteenMinutes))
                .expect("fifteen-minute state must exist")
                .len()
                <= super::MAX_CANDLE_HISTORY
        );
        let sources = retained_engine
            .events
            .get(&market)
            .expect("market source state must exist");
        assert!(sources.contexts.len() <= super::SOURCE_EVENT_HISTORY);
        assert!(sources.fundings.len() <= super::SOURCE_EVENT_HISTORY);
        assert!(sources.bbo.len() <= super::POINT_EVENT_HISTORY);
        assert!(sources.books.len() <= super::POINT_EVENT_HISTORY);
        assert!(sources.trades.len() <= super::TRADE_EVENT_HISTORY);
        assert!(
            retained_engine
                .context_samples
                .get(&(market.clone(), crate::event::CandleInterval::FifteenMinutes))
                .expect("fifteen-minute context samples must exist")
                .len()
                <= super::boundary_sample_limit(crate::event::CandleInterval::FifteenMinutes)
        );
        assert!(
            retained_engine
                .funding_samples
                .get(&(market.clone(), crate::event::CandleInterval::FifteenMinutes))
                .expect("fifteen-minute funding samples must exist")
                .len()
                <= super::boundary_sample_limit(crate::event::CandleInterval::FifteenMinutes)
        );
    }

    #[test]
    fn high_rate_trade_windows_remain_exact_beyond_the_raw_event_cache() {
        let btc = market("BTC");
        let eth = market("ETH");
        let mut engine = CommonFeatureEngine::new();
        let mut aggregator = CandleAggregator::new();
        populate(
            &mut engine,
            &mut aggregator,
            btc.clone(),
            0,
            128,
            dec!(100),
            dec!(1),
        );
        populate(
            &mut engine,
            &mut aggregator,
            eth.clone(),
            0,
            128,
            dec!(200),
            dec!(1),
        );
        let decision = timestamp(128 * FIFTEEN_MINUTES_NS);
        complete_with_canonical_funding_history(&mut engine, &mut aggregator, decision);

        for trade_id in 0..=super::TRADE_EVENT_HISTORY {
            let event = MarketEvent::trade(
                timestamp(128 * FIFTEEN_MINUTES_NS - 1),
                timestamp(128 * FIFTEEN_MINUTES_NS - 1),
                btc.clone(),
                Trade::new(
                    10_000 + trade_id as u64,
                    Side::Buy,
                    price(dec!(228)),
                    quantity(dec!(1)),
                )
                .expect("test trade must be valid"),
            )
            .expect("test trade event must be valid");
            engine.observe(&event).expect("trade must be accepted");
        }

        let snapshot = engine
            .snapshots_at_with_universe(
                crate::event::CandleInterval::FifteenMinutes,
                decision,
                Some(&tradeable_universe(decision, [btc.clone(), eth])),
            )
            .into_iter()
            .find(|snapshot| snapshot.market() == &btc)
            .expect("the retained decision candle must produce a snapshot");
        assert!(snapshot.is_ready(), "unexpected snapshot: {snapshot:#?}");
        assert_eq!(
            snapshot.values().get("trade_imbalance_15m"),
            Some(&Decimal::ONE)
        );
    }

    #[test]
    fn exact_decision_boundary_trade_belongs_to_the_next_microstructure_bucket() {
        let btc = market("BTC");
        let eth = market("ETH");
        let mut engine = CommonFeatureEngine::new();
        let mut aggregator = CandleAggregator::new();
        for (market, offset) in [(btc.clone(), dec!(100)), (eth, dec!(200))] {
            populate(
                &mut engine,
                &mut aggregator,
                market,
                0,
                128,
                offset,
                dec!(1),
            );
        }
        let decision = timestamp(128 * FIFTEEN_MINUTES_NS);
        complete_with_canonical_funding_history(&mut engine, &mut aggregator, decision);
        let boundary_trade = MarketEvent::trade(
            decision,
            decision,
            btc.clone(),
            Trade::new(10_000, Side::Sell, price(dec!(227)), quantity(dec!(1)))
                .expect("boundary trade must be valid"),
        )
        .expect("boundary trade event must be valid");
        engine
            .observe(&boundary_trade)
            .expect("boundary trade must be observed");

        for window in [super::MICRO_5_MINUTES_NS, super::MICRO_15_MINUTES_NS] {
            assert_eq!(
                engine
                    .trade_window(
                        &btc,
                        crate::event::CandleInterval::FifteenMinutes,
                        decision,
                        window,
                    )
                    .expect("completed microstructure window must exist")
                    .imbalance(),
                Decimal::ONE
            );
        }
    }

    #[test]
    fn microstructure_window_start_fails_closed_before_the_supported_epoch() {
        assert_eq!(
            super::microstructure_window_start(absolute_timestamp(0), super::MICRO_15_MINUTES_NS,),
            None
        );
    }

    #[test]
    fn open_interest_lookbacks_use_receipt_aware_boundary_samples_not_raw_context_bursts() {
        let btc = market("BTC");
        let eth = market("ETH");
        let mut engine = CommonFeatureEngine::new();
        let mut aggregator = CandleAggregator::new();
        for (market, offset) in [(btc.clone(), dec!(100)), (eth.clone(), dec!(200))] {
            populate(
                &mut engine,
                &mut aggregator,
                market,
                0,
                128,
                offset,
                dec!(1),
            );
        }
        let decision = timestamp(128 * FIFTEEN_MINUTES_NS);
        complete_with_canonical_funding_history(&mut engine, &mut aggregator, decision);
        let universe = tradeable_universe(decision, [btc.clone(), eth]);
        let before = engine
            .snapshots_at_with_universe(
                crate::event::CandleInterval::FifteenMinutes,
                decision,
                Some(&universe),
            )
            .into_iter()
            .find(|snapshot| snapshot.market() == &btc)
            .expect("BTC snapshot must exist");

        for sequence in 0..(super::SOURCE_EVENT_HISTORY - 128) {
            let rate = FundingRate::new(Decimal::from(sequence));
            let source_time = TimestampNs::new(
                i128::from(decision.value()) - i128::try_from(sequence).expect("sequence fits") - 1,
            )
            .expect("burst source time must be valid");
            let burst = MarketEvent::asset_context(
                source_time,
                source_time,
                btc.clone(),
                AssetContext::new(
                    price(dec!(10_000) + Decimal::from(sequence)),
                    price(dec!(9_999) + Decimal::from(sequence)),
                    Some(price(dec!(10_000) + Decimal::from(sequence))),
                    quantity(dec!(128)),
                    Usdc::new(dec!(1_000_000)).expect("test USDC must be valid"),
                    rate,
                ),
            )
            .expect("burst context must be valid");
            engine
                .observe(&burst)
                .expect("burst context must be accepted");
        }

        let after = engine
            .snapshots_at_with_universe(
                crate::event::CandleInterval::FifteenMinutes,
                decision,
                Some(&universe),
            )
            .into_iter()
            .find(|snapshot| snapshot.market() == &btc)
            .expect("BTC snapshot must exist");

        for lookback in [1, 4, 16] {
            let name = format!("open_interest_change_{lookback}");
            assert_eq!(after.values().get(&name), before.values().get(&name));
        }
        assert!(
            after
                .input_range()
                .expect("ready snapshot must retain provenance")
                .spans()
                .iter()
                .filter(|span| span.kind() == FeatureInputKind::AssetContext)
                .all(|span| span.sampled_at.is_some())
        );
    }
}
