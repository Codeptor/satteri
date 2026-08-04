//! Deterministic normalized market events.

use std::cmp::Ordering;
use std::fmt;

use blake3::Hasher;
use rust_decimal::Decimal;
use serde::Serialize;
use thiserror::Error;

use crate::domain::{DomainError, EventId, Market, Price, Quantity, Side, Usdc};

const ID_PREFIX: &str = "b3:";

/// A normalized-event construction or timestamp failure.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum EventError {
    /// A Unix timestamp did not fit the supported nonnegative `i64` nanosecond range.
    #[error("timestamp nanoseconds must be in 0..=i64::MAX, got {value}")]
    TimestampOutOfRange {
        /// The rejected integer value.
        value: i128,
    },
    /// A duration did not fit the supported nonnegative `i64` nanosecond range.
    #[error("duration nanoseconds must be in 0..=i64::MAX, got {value}")]
    DurationOutOfRange {
        /// The rejected integer value.
        value: i128,
    },
    /// Checked timestamp arithmetic failed.
    #[error("checked timestamp arithmetic failed")]
    TimestampArithmetic,
    /// Exchange time was later than local receipt time.
    #[error("event time {event_time} is later than receipt time {received_at}")]
    EventAfterReceipt {
        /// Authoritative exchange timestamp.
        event_time: TimestampNs,
        /// Local receipt timestamp supplied by the caller.
        received_at: TimestampNs,
    },
    /// A BBO bid exposed no executable quantity.
    #[error("BBO bid quantity must be greater than zero")]
    ZeroBboBidQuantity,
    /// A BBO ask exposed no executable quantity.
    #[error("BBO ask quantity must be greater than zero")]
    ZeroBboAskQuantity,
    /// A BBO bid was equal to or above its ask.
    #[error("BBO is crossed or locked: bid {best_bid:?}, ask {best_ask:?}")]
    CrossedOrLockedBbo {
        /// Rejected best bid.
        best_bid: Price,
        /// Rejected best ask.
        best_ask: Price,
    },
    /// A normalized public trade had no executed quantity.
    #[error("trade quantity must be greater than zero")]
    ZeroTradeQuantity,
    /// A completed candle did not close exactly one declared interval after its open.
    #[error("completed candle close {actual} must equal expected close {expected}")]
    InvalidCandleClose {
        /// Close derived from the candle open and interval.
        expected: TimestampNs,
        /// Event time supplied for the completed candle.
        actual: TimestampNs,
    },
    /// A candle high/low did not contain its open and close.
    #[error("completed candle OHLC bounds are inconsistent")]
    InvalidCandleBounds,
    /// Candle volume and trade-count activity disagreed.
    #[error("candle volume {volume:?} is inconsistent with trade count {trade_count}")]
    InconsistentCandleActivity {
        /// Rejected base-asset volume.
        volume: Quantity,
        /// Rejected contributing trade count.
        trade_count: u64,
    },
    /// A candle without trades contained price movement.
    #[error("zero-trade candle OHLC values must be flat")]
    NonFlatZeroTradeCandle,
    /// A generated checked identifier failed domain validation.
    #[error(transparent)]
    Domain(#[from] DomainError),
}

/// Nonnegative Unix time in nanoseconds bounded to SQLite-compatible `i64`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub struct TimestampNs(i64);

impl TimestampNs {
    /// Creates a bounded nonnegative Unix timestamp in nanoseconds.
    ///
    /// # Errors
    ///
    /// Returns [`EventError::TimestampOutOfRange`] outside `0..=i64::MAX`.
    pub fn new(value: i128) -> Result<Self, EventError> {
        let value = i64::try_from(value).map_err(|_| EventError::TimestampOutOfRange { value })?;
        if value < 0 {
            return Err(EventError::TimestampOutOfRange {
                value: i128::from(value),
            });
        }
        Ok(Self(value))
    }

    /// Returns Unix nanoseconds.
    #[must_use]
    pub const fn value(self) -> i64 {
        self.0
    }

    /// Adds a bounded duration with overflow checking.
    ///
    /// # Errors
    ///
    /// Returns [`EventError::TimestampArithmetic`] when the sum exceeds `i64::MAX`.
    pub fn checked_add(self, duration: DurationNs) -> Result<Self, EventError> {
        self.0
            .checked_add(duration.0)
            .map(Self)
            .ok_or(EventError::TimestampArithmetic)
    }

    /// Calculates a nonnegative elapsed duration from an earlier timestamp.
    ///
    /// # Errors
    ///
    /// Returns [`EventError::TimestampArithmetic`] when `earlier` is later.
    pub fn checked_duration_since(self, earlier: Self) -> Result<DurationNs, EventError> {
        let duration = self
            .0
            .checked_sub(earlier.0)
            .filter(|duration| *duration >= 0)
            .ok_or(EventError::TimestampArithmetic)?;
        Ok(DurationNs(duration))
    }
}

impl fmt::Display for TimestampNs {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Nonnegative duration in nanoseconds bounded to `i64::MAX`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub struct DurationNs(i64);

impl DurationNs {
    /// Creates a bounded nonnegative nanosecond duration.
    ///
    /// # Errors
    ///
    /// Returns [`EventError::DurationOutOfRange`] outside `0..=i64::MAX`.
    pub fn new(value: i128) -> Result<Self, EventError> {
        let value = i64::try_from(value).map_err(|_| EventError::DurationOutOfRange { value })?;
        if value < 0 {
            return Err(EventError::DurationOutOfRange {
                value: i128::from(value),
            });
        }
        Ok(Self(value))
    }

    /// Returns duration nanoseconds.
    #[must_use]
    pub const fn value(self) -> i64 {
        self.0
    }
}

impl fmt::Display for DurationNs {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Supported completed-candle intervals.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum CandleInterval {
    /// Fifteen-minute bars.
    FifteenMinutes,
    /// One-hour bars.
    OneHour,
}

impl CandleInterval {
    /// Returns the exact interval duration in nanoseconds.
    #[must_use]
    pub const fn duration(self) -> DurationNs {
        match self {
            Self::FifteenMinutes => DurationNs(900_000_000_000),
            Self::OneHour => DurationNs(3_600_000_000_000),
        }
    }

    const fn identity_tag(self) -> u8 {
        match self {
            Self::FifteenMinutes => 0,
            Self::OneHour => 1,
        }
    }
}

/// Point-in-time native-perpetual metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Metadata {
    size_decimals: u8,
    venue_max_leverage: u16,
    active: bool,
}

impl Metadata {
    /// Creates a typed metadata payload.
    #[must_use]
    pub const fn new(size_decimals: u8, venue_max_leverage: u16, active: bool) -> Self {
        Self {
            size_decimals,
            venue_max_leverage,
            active,
        }
    }

    /// Returns venue quantity precision.
    #[must_use]
    pub const fn size_decimals(&self) -> u8 {
        self.size_decimals
    }

    /// Returns venue maximum leverage.
    #[must_use]
    pub const fn venue_max_leverage(&self) -> u16 {
        self.venue_max_leverage
    }

    /// Returns whether the market was active at this event time.
    #[must_use]
    pub const fn is_active(&self) -> bool {
        self.active
    }
}

/// Signed decimal funding rate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FundingRate(Decimal);

impl FundingRate {
    /// Creates an exact signed funding rate.
    #[must_use]
    pub const fn new(value: Decimal) -> Self {
        Self(value)
    }

    /// Returns the exact decimal rate.
    #[must_use]
    pub const fn value(self) -> Decimal {
        self.0
    }
}

/// Point-in-time market context used by liquidity and derivatives features.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AssetContext {
    mark_price: Price,
    oracle_price: Price,
    mid_price: Option<Price>,
    open_interest: Quantity,
    day_notional_volume: Usdc,
    funding_rate: FundingRate,
}

impl AssetContext {
    /// Creates a typed asset-context payload.
    #[must_use]
    pub const fn new(
        mark_price: Price,
        oracle_price: Price,
        mid_price: Option<Price>,
        open_interest: Quantity,
        day_notional_volume: Usdc,
        funding_rate: FundingRate,
    ) -> Self {
        Self {
            mark_price,
            oracle_price,
            mid_price,
            open_interest,
            day_notional_volume,
            funding_rate,
        }
    }

    /// Returns the venue mark price.
    #[must_use]
    pub const fn mark_price(&self) -> Price {
        self.mark_price
    }

    /// Returns the venue oracle price.
    #[must_use]
    pub const fn oracle_price(&self) -> Price {
        self.oracle_price
    }

    /// Returns the live midpoint when provided by the source.
    #[must_use]
    pub const fn mid_price(&self) -> Option<Price> {
        self.mid_price
    }

    /// Returns open interest in asset quantity.
    #[must_use]
    pub const fn open_interest(&self) -> Quantity {
        self.open_interest
    }

    /// Returns trailing-day notional volume.
    #[must_use]
    pub const fn day_notional_volume(&self) -> Usdc {
        self.day_notional_volume
    }

    /// Returns the current funding rate.
    #[must_use]
    pub const fn funding_rate(&self) -> FundingRate {
        self.funding_rate
    }
}

/// One visible order-book level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BookLevel {
    price: Price,
    quantity: Quantity,
}

impl BookLevel {
    /// Creates a level for later order-book validation.
    #[must_use]
    pub const fn new(price: Price, quantity: Quantity) -> Self {
        Self { price, quantity }
    }

    /// Returns the level price.
    #[must_use]
    pub const fn price(&self) -> Price {
        self.price
    }

    /// Returns visible asset quantity.
    #[must_use]
    pub const fn quantity(&self) -> Quantity {
        self.quantity
    }
}

/// Full visible L2 snapshot in source order.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BookSnapshot {
    sequence: u64,
    bids: Vec<BookLevel>,
    asks: Vec<BookLevel>,
}

impl BookSnapshot {
    /// Creates an immutable snapshot payload for order-book validation.
    #[must_use]
    pub fn new(sequence: u64, bids: Vec<BookLevel>, asks: Vec<BookLevel>) -> Self {
        Self {
            sequence,
            bids,
            asks,
        }
    }

    /// Returns the exchange/source sequence.
    #[must_use]
    pub const fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Returns bids in source order.
    #[must_use]
    pub fn bids(&self) -> &[BookLevel] {
        &self.bids
    }

    /// Returns asks in source order.
    #[must_use]
    pub fn asks(&self) -> &[BookLevel] {
        &self.asks
    }
}

/// Best-bid/best-ask update.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Bbo {
    sequence: u64,
    bid: BookLevel,
    ask: BookLevel,
}

impl Bbo {
    /// Creates a validated typed BBO payload.
    ///
    /// # Errors
    ///
    /// Rejects zero bid/ask quantity and crossed or locked prices.
    pub fn new(sequence: u64, bid: BookLevel, ask: BookLevel) -> Result<Self, EventError> {
        let bbo = Self { sequence, bid, ask };
        bbo.validate()?;
        Ok(bbo)
    }

    fn validate(&self) -> Result<(), EventError> {
        if self.bid.quantity().value().is_zero() {
            return Err(EventError::ZeroBboBidQuantity);
        }
        if self.ask.quantity().value().is_zero() {
            return Err(EventError::ZeroBboAskQuantity);
        }
        if self.bid.price() >= self.ask.price() {
            return Err(EventError::CrossedOrLockedBbo {
                best_bid: self.bid.price(),
                best_ask: self.ask.price(),
            });
        }
        Ok(())
    }

    /// Returns the exchange/source sequence.
    #[must_use]
    pub const fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Returns the best bid.
    #[must_use]
    pub const fn bid(&self) -> BookLevel {
        self.bid
    }

    /// Returns the best ask.
    #[must_use]
    pub const fn ask(&self) -> BookLevel {
        self.ask
    }
}

/// Normalized public trade payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Trade {
    trade_id: u64,
    side: Side,
    price: Price,
    quantity: Quantity,
}

impl Trade {
    /// Creates a typed trade payload with strictly positive executed quantity.
    ///
    /// # Errors
    ///
    /// Returns [`EventError::ZeroTradeQuantity`] for zero quantity.
    pub fn new(
        trade_id: u64,
        side: Side,
        price: Price,
        quantity: Quantity,
    ) -> Result<Self, EventError> {
        let trade = Self {
            trade_id,
            side,
            price,
            quantity,
        };
        trade.validate()?;
        Ok(trade)
    }

    fn validate(&self) -> Result<(), EventError> {
        if self.quantity.value().is_zero() {
            return Err(EventError::ZeroTradeQuantity);
        }
        Ok(())
    }

    /// Returns the exchange trade ID.
    #[must_use]
    pub const fn trade_id(&self) -> u64 {
        self.trade_id
    }

    /// Returns aggressor side.
    #[must_use]
    pub const fn side(&self) -> Side {
        self.side
    }

    /// Returns execution price.
    #[must_use]
    pub const fn price(&self) -> Price {
        self.price
    }

    /// Returns execution quantity.
    #[must_use]
    pub const fn quantity(&self) -> Quantity {
        self.quantity
    }
}

/// Funding observation at a venue timestamp.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Funding {
    rate: FundingRate,
    mark_price: Option<Price>,
}

impl Funding {
    /// Creates a funding observation with the contemporaneous venue mark.
    #[must_use]
    pub const fn with_mark(rate: FundingRate, mark_price: Price) -> Self {
        Self {
            rate,
            mark_price: Some(mark_price),
        }
    }

    /// Creates a historical funding observation for which the venue did not
    /// provide a contemporaneous mark.
    ///
    /// This fact is valid feature provenance, but must never be used to book a
    /// paper-broker funding cashflow.
    #[must_use]
    pub const fn historical(rate: FundingRate) -> Self {
        Self {
            rate,
            mark_price: None,
        }
    }

    /// Returns the signed funding rate.
    #[must_use]
    pub const fn rate(&self) -> FundingRate {
        self.rate
    }

    /// Returns the contemporaneous mark price when the source supplied one.
    #[must_use]
    pub const fn mark_price(&self) -> Option<Price> {
        self.mark_price
    }
}

/// Fully completed OHLCV candle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CompletedCandle {
    interval: CandleInterval,
    open_time: TimestampNs,
    open: Price,
    high: Price,
    low: Price,
    close: Price,
    volume: Quantity,
    trade_count: u64,
}

impl CompletedCandle {
    /// Creates a candle whose high/low contain its open and close.
    ///
    /// # Errors
    ///
    /// Rejects inconsistent OHLC bounds, mismatched volume/trade activity, and
    /// non-flat zero-trade candles.
    #[expect(
        clippy::too_many_arguments,
        reason = "OHLCV candle fields are a fixed wire schema"
    )]
    pub fn new(
        interval: CandleInterval,
        open_time: TimestampNs,
        open: Price,
        high: Price,
        low: Price,
        close: Price,
        volume: Quantity,
        trade_count: u64,
    ) -> Result<Self, EventError> {
        let candle = Self {
            interval,
            open_time,
            open,
            high,
            low,
            close,
            volume,
            trade_count,
        };
        candle.validate()?;
        Ok(candle)
    }

    fn validate(&self) -> Result<(), EventError> {
        if self.high < self.open
            || self.high < self.close
            || self.high < self.low
            || self.low > self.open
            || self.low > self.close
        {
            return Err(EventError::InvalidCandleBounds);
        }
        let has_volume = !self.volume.value().is_zero();
        let has_trades = self.trade_count > 0;
        if has_volume != has_trades {
            return Err(EventError::InconsistentCandleActivity {
                volume: self.volume,
                trade_count: self.trade_count,
            });
        }
        if !has_trades
            && (self.open != self.high || self.open != self.low || self.open != self.close)
        {
            return Err(EventError::NonFlatZeroTradeCandle);
        }
        Ok(())
    }

    /// Returns the candle interval.
    #[must_use]
    pub const fn interval(&self) -> CandleInterval {
        self.interval
    }

    /// Returns the inclusive candle open time.
    #[must_use]
    pub const fn open_time(&self) -> TimestampNs {
        self.open_time
    }

    /// Returns the open price.
    #[must_use]
    pub const fn open(&self) -> Price {
        self.open
    }

    /// Returns the high price.
    #[must_use]
    pub const fn high(&self) -> Price {
        self.high
    }

    /// Returns the low price.
    #[must_use]
    pub const fn low(&self) -> Price {
        self.low
    }

    /// Returns the close price.
    #[must_use]
    pub const fn close(&self) -> Price {
        self.close
    }

    /// Returns base-asset volume.
    #[must_use]
    pub const fn volume(&self) -> Quantity {
        self.volume
    }

    /// Returns contributing trade count.
    #[must_use]
    pub const fn trade_count(&self) -> u64 {
        self.trade_count
    }
}

/// Closed set of normalized market event payloads.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum MarketEventKind {
    /// Point-in-time perpetual metadata.
    Metadata(Metadata),
    /// Point-in-time prices, funding, volume, and open interest.
    AssetContext(AssetContext),
    /// Full visible L2 snapshot.
    BookSnapshot(BookSnapshot),
    /// Best-bid/best-ask update.
    Bbo(Bbo),
    /// Public trade.
    Trade(Trade),
    /// Funding observation.
    Funding(Funding),
    /// Fully completed OHLCV candle.
    CompletedCandle(CompletedCandle),
}

impl MarketEventKind {
    const fn order(&self) -> u8 {
        match self {
            Self::Metadata(_) => 0,
            Self::AssetContext(_) => 1,
            Self::BookSnapshot(_) => 2,
            Self::Bbo(_) => 3,
            Self::Trade(_) => 4,
            Self::Funding(_) => 5,
            Self::CompletedCandle(_) => 6,
        }
    }
}

/// One immutable normalized exchange fact with explicit exchange and receipt time.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MarketEvent {
    event_id: EventId,
    event_time: TimestampNs,
    received_at: TimestampNs,
    market: Market,
    kind: MarketEventKind,
}

impl MarketEvent {
    /// Creates a point-in-time metadata event.
    ///
    /// # Errors
    ///
    /// Rejects exchange time later than receipt time or identifier failures.
    pub fn metadata(
        event_time: TimestampNs,
        received_at: TimestampNs,
        market: Market,
        metadata: Metadata,
    ) -> Result<Self, EventError> {
        let event_id = timestamp_market_id("trench.event.metadata.v1", event_time, &market)?;
        Self::build(
            event_id,
            event_time,
            received_at,
            market,
            MarketEventKind::Metadata(metadata),
        )
    }

    /// Creates a point-in-time asset-context event.
    ///
    /// # Errors
    ///
    /// Rejects exchange time later than receipt time or identifier failures.
    pub fn asset_context(
        event_time: TimestampNs,
        received_at: TimestampNs,
        market: Market,
        context: AssetContext,
    ) -> Result<Self, EventError> {
        let event_id = timestamp_market_id("trench.event.asset-context.v1", event_time, &market)?;
        Self::build(
            event_id,
            event_time,
            received_at,
            market,
            MarketEventKind::AssetContext(context),
        )
    }

    /// Creates a full L2 snapshot event.
    ///
    /// Snapshot price/quantity quality is validated by the immutable order-book transition.
    ///
    /// # Errors
    ///
    /// Rejects exchange time later than receipt time or identifier failures.
    pub fn book_snapshot(
        event_time: TimestampNs,
        received_at: TimestampNs,
        market: Market,
        snapshot: BookSnapshot,
    ) -> Result<Self, EventError> {
        let time = event_time.value().to_be_bytes();
        let sequence = snapshot.sequence().to_be_bytes();
        let event_id = canonical_event_id(
            "trench.event.book-snapshot.v1",
            &[&time, market.as_str().as_bytes(), &sequence],
        )?;
        Self::build(
            event_id,
            event_time,
            received_at,
            market,
            MarketEventKind::BookSnapshot(snapshot),
        )
    }

    /// Creates a best-bid/best-ask event.
    ///
    /// # Errors
    ///
    /// Rejects exchange time later than receipt time or identifier failures.
    pub fn bbo(
        event_time: TimestampNs,
        received_at: TimestampNs,
        market: Market,
        bbo: Bbo,
    ) -> Result<Self, EventError> {
        bbo.validate()?;
        let time = event_time.value().to_be_bytes();
        let sequence = bbo.sequence().to_be_bytes();
        let event_id = canonical_event_id(
            "trench.event.bbo.v1",
            &[&time, market.as_str().as_bytes(), &sequence],
        )?;
        Self::build(
            event_id,
            event_time,
            received_at,
            market,
            MarketEventKind::Bbo(bbo),
        )
    }

    /// Creates a public trade whose identity is exactly `(block_time, coin, tid)`.
    ///
    /// # Errors
    ///
    /// Rejects exchange time later than receipt time or identifier failures.
    pub fn trade(
        block_time: TimestampNs,
        received_at: TimestampNs,
        market: Market,
        trade: Trade,
    ) -> Result<Self, EventError> {
        trade.validate()?;
        let time = block_time.value().to_be_bytes();
        let trade_id = trade.trade_id().to_be_bytes();
        let event_id = canonical_event_id(
            "trench.event.trade.v1",
            &[&time, market.as_str().as_bytes(), &trade_id],
        )?;
        Self::build(
            event_id,
            block_time,
            received_at,
            market,
            MarketEventKind::Trade(trade),
        )
    }

    /// Creates a funding observation event.
    ///
    /// # Errors
    ///
    /// Rejects exchange time later than receipt time or identifier failures.
    pub fn funding(
        event_time: TimestampNs,
        received_at: TimestampNs,
        market: Market,
        funding: Funding,
    ) -> Result<Self, EventError> {
        let event_id = timestamp_market_id("trench.event.funding.v1", event_time, &market)?;
        Self::build(
            event_id,
            event_time,
            received_at,
            market,
            MarketEventKind::Funding(funding),
        )
    }

    /// Creates a completed candle identified by `(coin, interval, open_time)`.
    ///
    /// # Errors
    ///
    /// Rejects a mismatched close time, exchange time later than receipt time,
    /// timestamp overflow, or identifier failure.
    pub fn completed_candle(
        event_time: TimestampNs,
        received_at: TimestampNs,
        market: Market,
        candle: CompletedCandle,
    ) -> Result<Self, EventError> {
        candle.validate()?;
        let expected = candle
            .open_time()
            .checked_add(candle.interval().duration())?;
        if event_time != expected {
            return Err(EventError::InvalidCandleClose {
                expected,
                actual: event_time,
            });
        }
        let interval = [candle.interval().identity_tag()];
        let open_time = candle.open_time().value().to_be_bytes();
        let event_id = canonical_event_id(
            "trench.event.completed-candle.v1",
            &[market.as_str().as_bytes(), &interval, &open_time],
        )?;
        Self::build(
            event_id,
            event_time,
            received_at,
            market,
            MarketEventKind::CompletedCandle(candle),
        )
    }

    fn build(
        event_id: EventId,
        event_time: TimestampNs,
        received_at: TimestampNs,
        market: Market,
        kind: MarketEventKind,
    ) -> Result<Self, EventError> {
        if event_time > received_at {
            return Err(EventError::EventAfterReceipt {
                event_time,
                received_at,
            });
        }
        Ok(Self {
            event_id,
            event_time,
            received_at,
            market,
            kind,
        })
    }

    /// Returns the deterministic canonical exchange identity.
    #[must_use]
    pub const fn event_id(&self) -> &EventId {
        &self.event_id
    }

    /// Returns authoritative exchange time.
    #[must_use]
    pub const fn event_time(&self) -> TimestampNs {
        self.event_time
    }

    /// Returns local receipt time used only for latency and deterministic ordering.
    #[must_use]
    pub const fn received_at(&self) -> TimestampNs {
        self.received_at
    }

    /// Returns the native-perpetual market.
    #[must_use]
    pub const fn market(&self) -> &Market {
        &self.market
    }

    /// Returns the closed typed payload.
    #[must_use]
    pub const fn kind(&self) -> &MarketEventKind {
        &self.kind
    }
}

impl Ord for MarketEvent {
    fn cmp(&self, other: &Self) -> Ordering {
        self.event_time
            .cmp(&other.event_time)
            .then_with(|| self.received_at.cmp(&other.received_at))
            .then_with(|| self.kind.order().cmp(&other.kind.order()))
            .then_with(|| self.event_id.cmp(&other.event_id))
            .then_with(|| self.market.cmp(&other.market))
            .then_with(|| self.kind.cmp(&other.kind))
    }
}

impl PartialOrd for MarketEvent {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

fn timestamp_market_id(
    domain: &'static str,
    event_time: TimestampNs,
    market: &Market,
) -> Result<EventId, EventError> {
    let time = event_time.value().to_be_bytes();
    canonical_event_id(domain, &[&time, market.as_str().as_bytes()])
}

fn canonical_event_id(domain: &'static str, fields: &[&[u8]]) -> Result<EventId, EventError> {
    let mut hasher = Hasher::new_derive_key(domain);
    hasher.update(&(fields.len() as u64).to_be_bytes());
    fields.iter().for_each(|field| {
        hasher.update(&(field.len() as u64).to_be_bytes());
        hasher.update(field);
    });
    EventId::new(format!("{ID_PREFIX}{}", hasher.finalize().to_hex())).map_err(EventError::from)
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use super::{
        AssetContext, Bbo, BookLevel, BookSnapshot, CandleInterval, CompletedCandle, DurationNs,
        EventError, Funding, FundingRate, MarketEvent, MarketEventKind, Metadata, TimestampNs,
        Trade,
    };
    use crate::domain::{Market, Price, Quantity, Side, Usdc};

    const FIFTEEN_MINUTES_NS: i128 = 900_000_000_000;

    fn timestamp(value: i128) -> TimestampNs {
        TimestampNs::new(value).expect("test timestamp must be valid")
    }

    fn market(value: &str) -> Market {
        Market::new(value).expect("test market must be valid")
    }

    fn price(value: Decimal) -> Price {
        Price::new(value).expect("test price must be valid")
    }

    fn quantity(value: Decimal) -> Quantity {
        Quantity::new(value).expect("test quantity must be valid")
    }

    fn trade_payload(trade_id: u64, side: Side, price: Price, quantity: Quantity) -> Trade {
        Trade::new(trade_id, side, price, quantity).expect("test trade must be valid")
    }

    fn usdc(value: Decimal) -> Usdc {
        Usdc::new(value).expect("test USDC must be valid")
    }

    fn metadata_payload() -> Metadata {
        Metadata::new(3, 50, true)
    }

    fn context_payload() -> AssetContext {
        AssetContext::new(
            price(dec!(100)),
            price(dec!(99.9)),
            Some(price(dec!(100.1))),
            quantity(dec!(42)),
            usdc(dec!(5_000_000)),
            FundingRate::new(dec!(0.0001)),
        )
    }

    fn snapshot_payload(sequence: u64) -> BookSnapshot {
        BookSnapshot::new(
            sequence,
            vec![BookLevel::new(price(dec!(99)), quantity(dec!(2)))],
            vec![BookLevel::new(price(dec!(100)), quantity(dec!(3)))],
        )
    }

    fn bbo_payload(sequence: u64) -> Bbo {
        Bbo::new(
            sequence,
            BookLevel::new(price(dec!(99)), quantity(dec!(2))),
            BookLevel::new(price(dec!(100)), quantity(dec!(3))),
        )
        .expect("test BBO must be valid")
    }

    fn candle_payload(open_time: TimestampNs) -> CompletedCandle {
        CompletedCandle::new(
            CandleInterval::FifteenMinutes,
            open_time,
            price(dec!(100)),
            price(dec!(104)),
            price(dec!(98)),
            price(dec!(102)),
            quantity(dec!(250)),
            17,
        )
        .expect("test candle must be valid")
    }

    fn unchecked_candle(
        open: Decimal,
        high: Decimal,
        low: Decimal,
        close: Decimal,
        volume: Decimal,
        trade_count: u64,
    ) -> CompletedCandle {
        CompletedCandle {
            interval: CandleInterval::FifteenMinutes,
            open_time: timestamp(1_000),
            open: price(open),
            high: price(high),
            low: price(low),
            close: price(close),
            volume: quantity(volume),
            trade_count,
        }
    }

    fn normalize_unchecked_candle(candle: CompletedCandle) -> Result<MarketEvent, EventError> {
        let close_time = timestamp(1_000 + FIFTEEN_MINUTES_NS);
        MarketEvent::completed_candle(close_time, close_time, market("BTC"), candle)
    }

    fn every_event_kind(received_at: TimestampNs) -> Vec<MarketEvent> {
        let event_time = timestamp(FIFTEEN_MINUTES_NS + 1_000);
        let open_time = timestamp(1_000);

        vec![
            MarketEvent::metadata(event_time, received_at, market("BTC"), metadata_payload())
                .expect("metadata event must be valid"),
            MarketEvent::asset_context(event_time, received_at, market("BTC"), context_payload())
                .expect("context event must be valid"),
            MarketEvent::book_snapshot(event_time, received_at, market("BTC"), snapshot_payload(7))
                .expect("book event must be valid"),
            MarketEvent::bbo(event_time, received_at, market("BTC"), bbo_payload(7))
                .expect("BBO event must be valid"),
            MarketEvent::trade(
                event_time,
                received_at,
                market("BTC"),
                trade_payload(7, Side::Buy, price(dec!(100)), quantity(dec!(1))),
            )
            .expect("trade event must be valid"),
            MarketEvent::funding(
                event_time,
                received_at,
                market("BTC"),
                Funding::with_mark(FundingRate::new(dec!(0.0001)), price(dec!(100))),
            )
            .expect("funding event must be valid"),
            MarketEvent::completed_candle(
                event_time,
                received_at,
                market("BTC"),
                candle_payload(open_time),
            )
            .expect("candle event must be valid"),
        ]
    }

    #[test]
    fn duplicate_trade_identity_is_stable_across_receipt_and_payload_changes() {
        let event_time = timestamp(1_000);
        let first = MarketEvent::trade(
            event_time,
            timestamp(1_100),
            market("BTC"),
            trade_payload(42, Side::Buy, price(dec!(100)), quantity(dec!(1))),
        )
        .expect("first trade must be valid");
        let duplicate = MarketEvent::trade(
            event_time,
            timestamp(1_200),
            market("BTC"),
            trade_payload(42, Side::Sell, price(dec!(101)), quantity(dec!(2))),
        )
        .expect("duplicate trade must be valid");

        assert_eq!(first.event_id(), duplicate.event_id());
    }

    #[test]
    fn trade_identity_changes_when_any_exchange_identity_component_changes() {
        let baseline = MarketEvent::trade(
            timestamp(1_000),
            timestamp(2_000),
            market("BTC"),
            trade_payload(42, Side::Buy, price(dec!(100)), quantity(dec!(1))),
        )
        .expect("baseline trade must be valid");
        let cases = [
            MarketEvent::trade(
                timestamp(1_001),
                timestamp(2_000),
                market("BTC"),
                trade_payload(42, Side::Buy, price(dec!(100)), quantity(dec!(1))),
            )
            .expect("time variant must be valid"),
            MarketEvent::trade(
                timestamp(1_000),
                timestamp(2_000),
                market("ETH"),
                trade_payload(42, Side::Buy, price(dec!(100)), quantity(dec!(1))),
            )
            .expect("market variant must be valid"),
            MarketEvent::trade(
                timestamp(1_000),
                timestamp(2_000),
                market("BTC"),
                trade_payload(43, Side::Buy, price(dec!(100)), quantity(dec!(1))),
            )
            .expect("trade ID variant must be valid"),
        ];

        assert!(
            cases
                .iter()
                .all(|event| event.event_id() != baseline.event_id())
        );
    }

    #[test]
    fn binary_identity_encoding_prevents_concatenation_collisions() {
        let left = MarketEvent::trade(
            timestamp(1),
            timestamp(20),
            market("23"),
            trade_payload(4, Side::Buy, price(dec!(1)), quantity(dec!(1))),
        )
        .expect("left trade must be valid");
        let right = MarketEvent::trade(
            timestamp(12),
            timestamp(20),
            market("3"),
            trade_payload(4, Side::Buy, price(dec!(1)), quantity(dec!(1))),
        )
        .expect("right trade must be valid");

        assert_eq!("1234", format!("{}{}{}", 1, "23", 4));
        assert_eq!("1234", format!("{}{}{}", 12, "3", 4));
        assert_ne!(left.event_id(), right.event_id());
    }

    #[test]
    fn every_event_kind_has_a_stable_domain_separated_identity() {
        let first = every_event_kind(timestamp(FIFTEEN_MINUTES_NS + 2_000));
        let repeated = every_event_kind(timestamp(FIFTEEN_MINUTES_NS + 3_000));
        let unique: HashSet<_> = first.iter().map(MarketEvent::event_id).collect();

        assert_eq!(first.len(), unique.len());
        assert!(
            first
                .iter()
                .zip(&repeated)
                .all(|(left, right)| left.event_id() == right.event_id())
        );
        assert!(first.iter().all(|event| {
            event.event_id().as_str().starts_with("b3:") && event.event_id().as_str().len() == 67
        }));
    }

    #[test]
    fn market_event_exposes_exact_common_fields_and_typed_kind() {
        let event = MarketEvent::trade(
            timestamp(100),
            timestamp(120),
            market("SOL"),
            trade_payload(9, Side::Sell, price(dec!(150)), quantity(dec!(2))),
        )
        .expect("trade must be valid");

        assert_eq!(event.event_time(), timestamp(100));
        assert_eq!(event.received_at(), timestamp(120));
        assert_eq!(event.market().as_str(), "SOL");
        assert!(matches!(event.kind(), MarketEventKind::Trade(trade) if
            trade.trade_id() == 9
                && trade.side() == Side::Sell
                && trade.price() == price(dec!(150))
                && trade.quantity() == quantity(dec!(2))));
    }

    #[test]
    fn historical_funding_keeps_the_absence_of_a_contemporaneous_mark() {
        let event = MarketEvent::funding(
            timestamp(100),
            timestamp(120),
            market("SOL"),
            Funding::historical(FundingRate::new(dec!(-0.00001))),
        )
        .expect("historical funding fact must be valid");

        assert!(matches!(event.kind(), MarketEventKind::Funding(funding)
            if funding.rate() == FundingRate::new(dec!(-0.00001))
                && funding.mark_price().is_none()));
    }

    #[test]
    fn ordering_uses_event_time_before_receipt_time() {
        let earlier_exchange = MarketEvent::funding(
            timestamp(100),
            timestamp(300),
            market("BTC"),
            Funding::with_mark(FundingRate::new(dec!(0.001)), price(dec!(100))),
        )
        .expect("earlier exchange event must be valid");
        let later_exchange = MarketEvent::funding(
            timestamp(200),
            timestamp(201),
            market("BTC"),
            Funding::with_mark(FundingRate::new(dec!(0.001)), price(dec!(100))),
        )
        .expect("later exchange event must be valid");
        let mut events = [later_exchange.clone(), earlier_exchange.clone()];

        events.sort();

        assert_eq!(events, [earlier_exchange, later_exchange]);
    }

    #[test]
    fn ordering_uses_receipt_time_then_a_deterministic_tie_breaker() {
        let event_time = timestamp(100);
        let early_receipt = MarketEvent::trade(
            event_time,
            timestamp(110),
            market("BTC"),
            trade_payload(3, Side::Buy, price(dec!(100)), quantity(dec!(1))),
        )
        .expect("early receipt must be valid");
        let tied_a = MarketEvent::trade(
            event_time,
            timestamp(120),
            market("BTC"),
            trade_payload(1, Side::Buy, price(dec!(100)), quantity(dec!(1))),
        )
        .expect("first tie must be valid");
        let tied_b = MarketEvent::trade(
            event_time,
            timestamp(120),
            market("BTC"),
            trade_payload(2, Side::Buy, price(dec!(100)), quantity(dec!(1))),
        )
        .expect("second tie must be valid");
        let mut forward = vec![tied_a.clone(), tied_b.clone(), early_receipt.clone()];
        let mut reversed = vec![tied_b, tied_a, early_receipt.clone()];

        forward.sort();
        reversed.sort();

        assert_eq!(forward, reversed);
        assert_eq!(forward.first(), Some(&early_receipt));
    }

    #[test]
    fn timestamp_and_duration_types_reject_out_of_range_nanoseconds() {
        assert_eq!(
            TimestampNs::new(-1),
            Err(EventError::TimestampOutOfRange { value: -1 })
        );
        assert_eq!(
            TimestampNs::new(i128::from(i64::MAX) + 1),
            Err(EventError::TimestampOutOfRange {
                value: i128::from(i64::MAX) + 1,
            })
        );
        assert_eq!(
            DurationNs::new(i128::from(i64::MAX) + 1),
            Err(EventError::DurationOutOfRange {
                value: i128::from(i64::MAX) + 1,
            })
        );
    }

    #[test]
    fn market_event_rejects_exchange_time_after_receipt_time() {
        let error = MarketEvent::metadata(
            timestamp(101),
            timestamp(100),
            market("BTC"),
            metadata_payload(),
        )
        .expect_err("future exchange time must fail");

        assert_eq!(
            error,
            EventError::EventAfterReceipt {
                event_time: timestamp(101),
                received_at: timestamp(100),
            }
        );
    }

    #[test]
    fn bbo_rejects_zero_bid_quantity() {
        assert_eq!(
            Bbo::new(
                1,
                BookLevel::new(price(dec!(99)), quantity(Decimal::ZERO)),
                BookLevel::new(price(dec!(100)), quantity(dec!(1))),
            ),
            Err(EventError::ZeroBboBidQuantity)
        );
    }

    #[test]
    fn bbo_rejects_zero_ask_quantity() {
        assert_eq!(
            Bbo::new(
                1,
                BookLevel::new(price(dec!(99)), quantity(dec!(1))),
                BookLevel::new(price(dec!(100)), quantity(Decimal::ZERO)),
            ),
            Err(EventError::ZeroBboAskQuantity)
        );
    }

    #[test]
    fn bbo_rejects_locked_prices() {
        assert_eq!(
            Bbo::new(
                1,
                BookLevel::new(price(dec!(100)), quantity(dec!(1))),
                BookLevel::new(price(dec!(100)), quantity(dec!(1))),
            ),
            Err(EventError::CrossedOrLockedBbo {
                best_bid: price(dec!(100)),
                best_ask: price(dec!(100)),
            })
        );
    }

    #[test]
    fn bbo_rejects_crossed_prices() {
        assert_eq!(
            Bbo::new(
                1,
                BookLevel::new(price(dec!(101)), quantity(dec!(1))),
                BookLevel::new(price(dec!(100)), quantity(dec!(1))),
            ),
            Err(EventError::CrossedOrLockedBbo {
                best_bid: price(dec!(101)),
                best_ask: price(dec!(100)),
            })
        );
    }

    #[test]
    fn market_event_bbo_revalidates_its_payload() {
        let invalid = Bbo {
            sequence: 1,
            bid: BookLevel::new(price(dec!(100)), quantity(dec!(1))),
            ask: BookLevel::new(price(dec!(100)), quantity(dec!(1))),
        };

        let error = MarketEvent::bbo(timestamp(100), timestamp(100), market("BTC"), invalid)
            .expect_err("invalid BBO payload must not enter normalized event state");

        assert_eq!(
            error,
            EventError::CrossedOrLockedBbo {
                best_bid: price(dec!(100)),
                best_ask: price(dec!(100)),
            }
        );
    }

    #[test]
    fn trade_rejects_zero_quantity() {
        assert_eq!(
            Trade::new(1, Side::Buy, price(dec!(100)), quantity(Decimal::ZERO),),
            Err(EventError::ZeroTradeQuantity)
        );
    }

    #[test]
    fn market_event_trade_revalidates_its_payload() {
        let invalid = Trade {
            trade_id: 1,
            side: Side::Buy,
            price: price(dec!(100)),
            quantity: quantity(Decimal::ZERO),
        };

        let error = MarketEvent::trade(timestamp(100), timestamp(100), market("BTC"), invalid)
            .expect_err("zero-quantity trade must not enter normalized event state");

        assert_eq!(error, EventError::ZeroTradeQuantity);
    }

    #[test]
    fn market_event_candle_rejects_positive_volume_with_zero_trades() {
        let candle = unchecked_candle(dec!(100), dec!(100), dec!(100), dec!(100), dec!(1), 0);

        let error = normalize_unchecked_candle(candle)
            .expect_err("event path must reject volume without trades");

        assert_eq!(
            error,
            EventError::InconsistentCandleActivity {
                volume: quantity(dec!(1)),
                trade_count: 0,
            }
        );
    }

    #[test]
    fn market_event_candle_rejects_zero_volume_with_trades() {
        let candle = unchecked_candle(dec!(100), dec!(100), dec!(100), dec!(100), Decimal::ZERO, 1);

        let error = normalize_unchecked_candle(candle)
            .expect_err("event path must reject trades without volume");

        assert_eq!(
            error,
            EventError::InconsistentCandleActivity {
                volume: quantity(Decimal::ZERO),
                trade_count: 1,
            }
        );
    }

    #[test]
    fn market_event_candle_rejects_nonflat_zero_trade_ohlc() {
        let candle = unchecked_candle(dec!(100), dec!(101), dec!(100), dec!(100), Decimal::ZERO, 0);

        let error = normalize_unchecked_candle(candle)
            .expect_err("event path must reject non-flat zero-trade candle");

        assert_eq!(error, EventError::NonFlatZeroTradeCandle);
    }

    #[test]
    fn market_event_candle_rejects_invalid_ohlc_bounds() {
        let candle = unchecked_candle(dec!(100), dec!(99), dec!(98), dec!(102), dec!(1), 1);

        let error = normalize_unchecked_candle(candle)
            .expect_err("event path must reject invalid OHLC bounds");

        assert_eq!(error, EventError::InvalidCandleBounds);
    }

    #[test]
    fn completed_candle_rejects_positive_volume_with_zero_trades() {
        let error = CompletedCandle::new(
            CandleInterval::FifteenMinutes,
            timestamp(1_000),
            price(dec!(100)),
            price(dec!(100)),
            price(dec!(100)),
            price(dec!(100)),
            quantity(dec!(1)),
            0,
        )
        .expect_err("positive volume with no trades must fail");

        assert_eq!(
            error,
            EventError::InconsistentCandleActivity {
                volume: quantity(dec!(1)),
                trade_count: 0,
            }
        );
    }

    #[test]
    fn completed_candle_rejects_zero_volume_with_trades() {
        let error = CompletedCandle::new(
            CandleInterval::FifteenMinutes,
            timestamp(1_000),
            price(dec!(100)),
            price(dec!(100)),
            price(dec!(100)),
            price(dec!(100)),
            quantity(Decimal::ZERO),
            1,
        )
        .expect_err("zero volume with a positive trade count must fail");

        assert_eq!(
            error,
            EventError::InconsistentCandleActivity {
                volume: quantity(Decimal::ZERO),
                trade_count: 1,
            }
        );
    }

    #[test]
    fn completed_candle_rejects_nonflat_zero_trade_ohlc() {
        let error = CompletedCandle::new(
            CandleInterval::FifteenMinutes,
            timestamp(1_000),
            price(dec!(100)),
            price(dec!(101)),
            price(dec!(100)),
            price(dec!(100)),
            quantity(Decimal::ZERO),
            0,
        )
        .expect_err("zero-trade candle must be flat");

        assert_eq!(error, EventError::NonFlatZeroTradeCandle);
    }

    #[test]
    fn completed_candle_accepts_consistent_empty_and_active_activity() {
        let empty = CompletedCandle::new(
            CandleInterval::FifteenMinutes,
            timestamp(1_000),
            price(dec!(100)),
            price(dec!(100)),
            price(dec!(100)),
            price(dec!(100)),
            quantity(Decimal::ZERO),
            0,
        );
        let active = CompletedCandle::new(
            CandleInterval::FifteenMinutes,
            timestamp(1_000),
            price(dec!(100)),
            price(dec!(104)),
            price(dec!(98)),
            price(dec!(102)),
            quantity(dec!(250)),
            17,
        );

        assert!(empty.is_ok());
        assert!(active.is_ok());
    }

    #[test]
    fn completed_candle_requires_exact_close_time_and_valid_ohlc_bounds() {
        let open_time = timestamp(1_000);
        let wrong_close = MarketEvent::completed_candle(
            timestamp(1_000 + FIFTEEN_MINUTES_NS + 1),
            timestamp(1_000 + FIFTEEN_MINUTES_NS + 1),
            market("BTC"),
            candle_payload(open_time),
        )
        .expect_err("wrong close must fail");
        let invalid_bounds = CompletedCandle::new(
            CandleInterval::FifteenMinutes,
            open_time,
            price(dec!(100)),
            price(dec!(99)),
            price(dec!(98)),
            price(dec!(102)),
            quantity(dec!(1)),
            1,
        )
        .expect_err("invalid OHLC bounds must fail");

        assert!(matches!(wrong_close, EventError::InvalidCandleClose { .. }));
        assert_eq!(invalid_bounds, EventError::InvalidCandleBounds);
    }
}
