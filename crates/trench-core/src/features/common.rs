//! Finite, immutable common market features at explicit completed-bar boundaries.

use std::collections::BTreeMap;

use blake3::Hasher;
use rust_decimal::Decimal;
use thiserror::Error;

use crate::candle::Candle;
use crate::domain::{EventId, Market};
use crate::event::{
    AssetContext, Bbo, BookSnapshot, CandleInterval, Funding, MarketEvent, MarketEventKind,
    TimestampNs,
};

const FEATURE_SCHEMA: &str = concat!(
    "trench.common-features.v1\n",
    "warmup.bars=97;context_observations=30;funding_observations=30\n",
    "returns=1,2,4,8,16,32,96\n",
    "ema=8,32;ema_slope=8:4\n",
    "rsi=14;atr=14;adx=14;realized_volatility=8,20,64\n",
    "donchian=20;volume_robust_z=20\n",
    "premium;open_interest_change=1,4,16;funding=level,percentile:30\n",
    "spread_bps;depth=10,25,50;trade_imbalance=5m,15m\n",
    "cross_return_rank=4,16,96;hourly_regime\n"
);
const MAX_BAR_LOOKBACK: usize = 97;
const CONTEXT_WINDOW: usize = 30;
const FUNDING_WINDOW: usize = 30;
const MICRO_5_MINUTES_NS: i64 = 300_000_000_000;
const MICRO_15_MINUTES_NS: i64 = 900_000_000_000;

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
    available_at: TimestampNs,
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
            available_at: event.received_at(),
        }
    }

    fn candle(kind: FeatureInputKind, candle: &Candle) -> Self {
        Self {
            kind,
            first_event_id: candle.first_event_id().clone(),
            last_event_id: candle.last_event_id().clone(),
            first_event_time: candle.first_event_time(),
            last_event_time: candle.last_event_time(),
            available_at: candle.source_available_at(),
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

    fn from_spans(mut spans: Vec<InputEventSpan>) -> Option<Self> {
        spans.sort();
        spans.dedup();
        let first = spans.iter().min_by(|left, right| {
            left.first_event_time
                .cmp(&right.first_event_time)
                .then_with(|| left.first_event_id.cmp(&right.first_event_id))
        })?;
        let last = spans.iter().max_by(|left, right| {
            left.last_event_time
                .cmp(&right.last_event_time)
                .then_with(|| left.last_event_id.cmp(&right.last_event_id))
        })?;
        Some(Self {
            first_event_id: first.first_event_id.clone(),
            last_event_id: last.last_event_id.clone(),
            first_event_time: first.first_event_time,
            last_event_time: last.last_event_time,
            digest: input_digest(&spans),
            spans,
        })
    }

    fn with_additional_spans(&self, additional: &[InputEventSpan]) -> Option<Self> {
        let mut spans = self.spans.clone();
        spans.extend_from_slice(additional);
        Self::from_spans(spans)
    }
}

fn input_digest(spans: &[InputEventSpan]) -> String {
    let mut hasher = Hasher::new_derive_key("trench.feature-input-range.v1");
    for span in spans {
        hasher.update(&[span.kind.identity_tag()]);
        hasher.update(&span.first_event_time.value().to_be_bytes());
        hasher.update(span.first_event_id.as_str().as_bytes());
        hasher.update(&[0]);
        hasher.update(&span.last_event_time.value().to_be_bytes());
        hasher.update(span.last_event_id.as_str().as_bytes());
        hasher.update(&[0]);
        hasher.update(&span.available_at.value().to_be_bytes());
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
    /// The completed-bar history is absent, discontinuous, or not warm.
    CandleHistory,
    /// The point-in-time asset-context window is incomplete.
    ContextHistory,
    /// The point-in-time funding window is incomplete.
    FundingHistory,
    /// BBO, order-book, or aggressive-trade inputs are incomplete.
    Microstructure,
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
    /// A completed-candle identity was reused with a different immutable value.
    #[error("conflicting completed candle for market {market:?} at {open_time}")]
    ConflictingCandle {
        /// Candle market.
        market: Market,
        /// Candle interval open time.
        open_time: TimestampNs,
    },
}

/// Deterministic per-market, per-sleeve common-feature state.
#[derive(Debug, Default)]
pub struct CommonFeatureEngine {
    seen_events: BTreeMap<EventId, MarketEvent>,
    events: BTreeMap<Market, BTreeMap<(TimestampNs, EventId), MarketEvent>>,
    candles: BTreeMap<(Market, CandleInterval), BTreeMap<TimestampNs, Candle>>,
}

impl CommonFeatureEngine {
    /// Creates empty warmup state for every market and sleeve.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Records a normalized public input without reading wall-clock time.
    ///
    /// Exact duplicates are idempotent. Conflicting reuse of a canonical event
    /// identity fails closed, so replay order cannot choose a feature value.
    ///
    /// # Errors
    ///
    /// Returns [`FeatureError::ConflictingEvent`] for a reused identity with a
    /// nonidentical immutable payload.
    pub fn observe(&mut self, event: &MarketEvent) -> Result<(), FeatureError> {
        if let Some(existing) = self.seen_events.get(event.event_id()) {
            return if existing == event {
                Ok(())
            } else {
                Err(FeatureError::ConflictingEvent {
                    event_id: event.event_id().clone(),
                })
            };
        }
        self.seen_events
            .insert(event.event_id().clone(), event.clone());
        self.events
            .entry(event.market().clone())
            .or_default()
            .insert(
                (event.event_time(), event.event_id().clone()),
                event.clone(),
            );
        Ok(())
    }

    /// Adds one immutable completed candle to its independent `(market, sleeve)` warmup state.
    ///
    /// # Errors
    ///
    /// Returns [`FeatureError::ConflictingCandle`] when a completed-candle
    /// identity is replayed with a different value.
    pub fn ingest_candle(&mut self, candle: Candle) -> Result<(), FeatureError> {
        let key = (candle.market().clone(), candle.candle().interval());
        let open_time = candle.candle().open_time();
        let candles = self.candles.entry(key).or_default();
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
        candles.insert(open_time, candle);
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
        let mut bases = self
            .markets_at(sleeve, as_of_time)
            .into_iter()
            .map(|market| self.base_snapshot(market, sleeve, as_of_time))
            .collect::<Vec<_>>();

        let complete = bases
            .iter()
            .filter(|base| base.completeness.primary_inputs_ready())
            .map(|base| base.market.clone())
            .collect::<Vec<_>>();
        if complete.len() >= 2 {
            add_cross_sectional_ranks(&mut bases, &complete, sleeve);
        }

        bases
            .into_iter()
            .map(|base| {
                let unready_reason = base.unready_reason.or_else(|| {
                    (!base.completeness.cross_section).then_some(FeatureUnreadyReason::CrossSection)
                });
                let is_ready = base.completeness.is_ready() && unready_reason.is_none();
                FeatureSnapshot {
                    market: base.market,
                    sleeve,
                    as_of_time,
                    input_range: base.input_range,
                    completeness: base.completeness,
                    unready_reason,
                    schema_hash: schema_hash(),
                    values: if is_ready {
                        base.values
                    } else {
                        BTreeMap::new()
                    },
                    regime: base.regime,
                }
            })
            .collect()
    }

    fn markets_at(&self, sleeve: CandleInterval, as_of_time: TimestampNs) -> Vec<Market> {
        self.candles
            .iter()
            .filter(|((_, candidate_sleeve), candles)| {
                *candidate_sleeve == sleeve
                    && candles.values().any(|candle| {
                        candle.close_time().is_ok_and(|close| close == as_of_time)
                            && candle.source_available_at() <= as_of_time
                    })
            })
            .map(|((market, _), _)| market.clone())
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
        let context_events = self.context_events(&market, as_of_time);
        let context_events = trailing_window(&context_events, CONTEXT_WINDOW);
        let context_history = context_events
            .map(|events| {
                events
                    .iter()
                    .filter_map(|event| match event.kind() {
                        MarketEventKind::AssetContext(context) => Some(context),
                        _ => None,
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let funding_events = self.funding_events(&market, as_of_time);
        let funding_events = trailing_window(&funding_events, FUNDING_WINDOW);
        let funding_history = funding_events
            .map(|events| {
                events
                    .iter()
                    .filter_map(|event| match event.kind() {
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
        let regime = hourly_history.and_then(hourly_regime);
        let trades = self.trade_events(&market, as_of_time);
        let microstructure = bbo.is_some()
            && book.is_some()
            && trade_imbalance(&trades, as_of_time, MICRO_5_MINUTES_NS).is_some()
            && trade_imbalance(&trades, as_of_time, MICRO_15_MINUTES_NS).is_some();
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
        let mut unready_reason = incomplete_source_reason(completeness);
        if unready_reason.is_none() {
            if let (
                Some(feature_history),
                Some(hourly_history),
                Some(context_events),
                Some(funding_events),
                Some(bbo_event),
                Some(book_event),
            ) = (
                feature_history,
                hourly_history,
                context_events,
                funding_events,
                bbo_event,
                book_event,
            ) {
                let range = input_range(
                    feature_history,
                    hourly_history,
                    context_events,
                    funding_events,
                    bbo_event,
                    book_event,
                    &trades,
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
                        trades: &trades,
                        as_of_time,
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

    fn context_events(&self, market: &Market, as_of_time: TimestampNs) -> Vec<&MarketEvent> {
        self.events_before(market, as_of_time)
            .filter(|event| matches!(event.kind(), MarketEventKind::AssetContext(_)))
            .collect()
    }

    fn funding_events(&self, market: &Market, as_of_time: TimestampNs) -> Vec<&MarketEvent> {
        self.events_before(market, as_of_time)
            .filter(|event| matches!(event.kind(), MarketEventKind::Funding(_)))
            .collect()
    }

    fn latest_bbo(&self, market: &Market, as_of_time: TimestampNs) -> Option<&MarketEvent> {
        self.events_before(market, as_of_time)
            .rev()
            .find(|event| matches!(event.kind(), MarketEventKind::Bbo(_)))
    }

    fn latest_book(&self, market: &Market, as_of_time: TimestampNs) -> Option<&MarketEvent> {
        self.events_before(market, as_of_time)
            .rev()
            .find(|event| matches!(event.kind(), MarketEventKind::BookSnapshot(_)))
    }

    fn trade_events(&self, market: &Market, as_of_time: TimestampNs) -> Vec<&MarketEvent> {
        let microstructure_start = as_of_time.value().saturating_sub(MICRO_15_MINUTES_NS);
        self.events_before(market, as_of_time)
            .filter(|event| {
                event.event_time().value() > microstructure_start
                    && matches!(event.kind(), MarketEventKind::Trade(_))
            })
            .collect()
    }

    fn events_before(
        &self,
        market: &Market,
        as_of_time: TimestampNs,
    ) -> impl DoubleEndedIterator<Item = &MarketEvent> {
        self.events
            .get(market)
            .into_iter()
            .flat_map(|events| events.values())
            .filter(move |event| {
                event.event_time() <= as_of_time && event.received_at() <= as_of_time
            })
    }
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

fn trailing_window<T>(values: &[T], count: usize) -> Option<&[T]> {
    values
        .len()
        .checked_sub(count)
        .and_then(|start| values.get(start..))
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
) {
    for lookback in [4_usize, 16, 96] {
        let name = format!("return_{lookback}");
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
        for (index, (market, _)) in ranked.into_iter().enumerate() {
            if let Some(base) = bases.iter_mut().find(|base| base.market == market) {
                let rank = Decimal::from(index + 1) / denominator;
                base.values.insert(rank_name.clone(), rank);
                base.completeness.cross_section = true;
            }
        }
    }

    let primary_candle_kind = candle_input_kind(sleeve);
    let peer_candle_spans = complete
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
                    .filter(|span| span.kind() == primary_candle_kind)
                    .cloned()
                    .collect::<Vec<_>>(),
            ))
        })
        .collect::<Vec<_>>();
    for base in bases
        .iter_mut()
        .filter(|base| complete.contains(&base.market))
    {
        let additional = peer_candle_spans
            .iter()
            .filter(|(market, _)| *market != base.market)
            .flat_map(|(_, spans)| spans.iter().cloned())
            .collect::<Vec<_>>();
        if let Some(range) = &base.input_range {
            base.input_range = range.with_additional_spans(&additional);
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
    trades: &'a [&'a MarketEvent],
    as_of_time: TimestampNs,
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
    let ema_32 = ema(&close_values, 32)?;
    values.insert("ema_8".to_owned(), ema_8);
    values.insert("ema_32".to_owned(), ema_32);
    values.insert("ema_8_32_ratio".to_owned(), ema_8.checked_div(ema_32)?);
    values.insert(
        "ema_8_slope_4".to_owned(),
        ema(&close_values[..close_values.len().checked_sub(4)?], 8)?.checked_sub(ema_8)?,
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
    values.insert(
        "volume_robust_z_20".to_owned(),
        robust_z(
            &history
                .iter()
                .rev()
                .take(20)
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
                .rev()
                .take(FUNDING_WINDOW)
                .map(|funding| funding.rate().value())
                .collect::<Vec<_>>(),
            funding.rate().value(),
        )?,
    );

    let bbo = inputs.bbo?;
    let book = inputs.book?;
    values.insert("spread_bps".to_owned(), spread_bps(bbo)?);
    for bps in [10_u32, 25, 50] {
        values.insert(format!("depth_{bps}bps"), depth(book, bbo, bps)?);
    }
    values.insert(
        "trade_imbalance_5m".to_owned(),
        trade_imbalance(inputs.trades, inputs.as_of_time, MICRO_5_MINUTES_NS)?,
    );
    values.insert(
        "trade_imbalance_15m".to_owned(),
        trade_imbalance(inputs.trades, inputs.as_of_time, MICRO_15_MINUTES_NS)?,
    );
    Some(values)
}

fn input_range(
    feature_history: &[&Candle],
    hourly_history: &[&Candle],
    context_events: &[&MarketEvent],
    funding_events: &[&MarketEvent],
    bbo_event: &MarketEvent,
    book_event: &MarketEvent,
    trades: &[&MarketEvent],
) -> Option<InputEventRange> {
    let spans = feature_history
        .iter()
        .map(|candle| InputEventSpan::candle(candle_input_kind(candle.candle().interval()), candle))
        .chain(hourly_history.iter().map(|candle| {
            InputEventSpan::candle(candle_input_kind(candle.candle().interval()), candle)
        }))
        .chain(
            context_events
                .iter()
                .map(|event| InputEventSpan::event(FeatureInputKind::AssetContext, event)),
        )
        .chain(
            funding_events
                .iter()
                .map(|event| InputEventSpan::event(FeatureInputKind::Funding, event)),
        )
        .chain(std::iter::once(InputEventSpan::event(
            FeatureInputKind::Bbo,
            bbo_event,
        )))
        .chain(std::iter::once(InputEventSpan::event(
            FeatureInputKind::Book,
            book_event,
        )))
        .chain(
            trades
                .iter()
                .map(|event| InputEventSpan::event(FeatureInputKind::MicrostructureTrade, event)),
        )
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

fn depth(book: &BookSnapshot, bbo: &Bbo, bps: u32) -> Option<Decimal> {
    let bid = bbo.bid().price().value();
    let ask = bbo.ask().price().value();
    let mid = bid.checked_add(ask)?.checked_div(Decimal::from(2))?;
    let band = mid
        .checked_mul(Decimal::from(bps))?
        .checked_div(Decimal::from(10_000))?;
    let lower = mid.checked_sub(band)?;
    let upper = mid.checked_add(band)?;
    book.bids()
        .iter()
        .chain(book.asks())
        .filter(|level| match level.price().value() >= mid {
            true => level.price().value() <= upper,
            false => level.price().value() >= lower,
        })
        .try_fold(Decimal::ZERO, |total, level| {
            level
                .price()
                .value()
                .checked_mul(level.quantity().value())
                .and_then(|notional| total.checked_add(notional))
        })
}

fn trade_imbalance(
    events: &[&MarketEvent],
    as_of_time: TimestampNs,
    window_ns: i64,
) -> Option<Decimal> {
    let start = as_of_time.value().checked_sub(window_ns)?;
    let (buy, sell) =
        events
            .iter()
            .try_fold((Decimal::ZERO, Decimal::ZERO), |(buy, sell), event| {
                if event.event_time().value() <= start {
                    return Some((buy, sell));
                }
                let MarketEventKind::Trade(trade) = event.kind() else {
                    return Some((buy, sell));
                };
                let notional = trade
                    .price()
                    .value()
                    .checked_mul(trade.quantity().value())?;
                match trade.side() {
                    crate::domain::Side::Buy => buy.checked_add(notional).map(|next| (next, sell)),
                    crate::domain::Side::Sell => sell.checked_add(notional).map(|next| (buy, next)),
                }
            })?;
    buy.checked_sub(sell)?.checked_div(buy.checked_add(sell)?)
}

fn schema_hash() -> String {
    let mut hasher = Hasher::new_derive_key("trench.feature-schema.v1");
    hasher.update(FEATURE_SCHEMA.as_bytes());
    hasher.finalize().to_hex().to_string()
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use super::{CommonFeatureEngine, FeatureInputKind, FeatureUnreadyReason};
    use crate::candle::CandleAggregator;
    use crate::domain::{Market, Price, Quantity, Side, Usdc};
    use crate::event::{
        AssetContext, Bbo, BookLevel, BookSnapshot, Funding, FundingRate, MarketEvent, TimestampNs,
        Trade,
    };

    const FIFTEEN_MINUTES_NS: i128 = 900_000_000_000;

    fn timestamp(value: i128) -> TimestampNs {
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

    #[derive(Clone, Copy)]
    struct PopulateRange {
        start: u64,
        count: u64,
        offset: Decimal,
        step: Decimal,
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
            let trade = MarketEvent::trade(
                timestamp(close - 1),
                timestamp(trade_received_at),
                market.clone(),
                Trade::new(index + 1, Side::Buy, price(value), quantity(dec!(1)))
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
        complete(&mut engine, &mut aggregator, decision);
        let before = engine.snapshots_at(crate::event::CandleInterval::FifteenMinutes, decision);
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
            engine.snapshots_at(crate::event::CandleInterval::FifteenMinutes, decision)
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
                },
                None,
                receipt_delays,
            );
        }
        let decision = timestamp(128 * FIFTEEN_MINUTES_NS);
        complete(&mut engine, &mut aggregator, decision);

        for snapshot in engine.snapshots_at(crate::event::CandleInterval::FifteenMinutes, decision)
        {
            assert!(snapshot.is_ready(), "unexpected snapshot: {snapshot:#?}");
            assert!(
                snapshot
                    .input_range()
                    .expect("ready snapshot must retain provenance")
                    .spans()
                    .iter()
                    .all(|span| span.available_at() <= decision)
            );
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
        complete(&mut engine, &mut aggregator, decision);

        let snapshots = engine.snapshots_at(crate::event::CandleInterval::FifteenMinutes, decision);
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
        complete(&mut engine, &mut aggregator, decision);

        for snapshot in engine.snapshots_at(crate::event::CandleInterval::FifteenMinutes, decision)
        {
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
        complete(&mut engine, &mut aggregator, decision);

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
        complete(&mut engine, &mut aggregator, decision);

        let before = engine
            .snapshots_at(crate::event::CandleInterval::FifteenMinutes, decision)
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
        assert_eq!(count(FeatureInputKind::Funding), 30);
        assert_eq!(count(FeatureInputKind::Bbo), 1);
        assert_eq!(count(FeatureInputKind::Book), 1);
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
        let after = engine
            .snapshots_at(crate::event::CandleInterval::FifteenMinutes, decision)
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
        complete(&mut engine, &mut aggregator, decision);

        for snapshot in engine.snapshots_at(crate::event::CandleInterval::OneHour, decision) {
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
}
