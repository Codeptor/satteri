use std::collections::{BTreeMap, BTreeSet};

use rust_decimal::Decimal;
use serde::Deserialize;
use trench_core::domain::{Market, Price, Quantity, Usdc};

use crate::info::{CandleInterval, InfoError, TimeRange};

/// A checked, positive venue leverage limit independent of paper leverage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct VenueMaxLeverage(u32);

impl VenueMaxLeverage {
    /// Returns the venue-reported integer leverage multiplier.
    #[must_use]
    pub const fn value(self) -> u32 {
        self.0
    }
}

/// An exact signed decimal funding or premium rate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SignedRate(Decimal);

impl SignedRate {
    /// Returns the exact signed decimal rate.
    #[must_use]
    pub const fn value(self) -> Decimal {
        self.0
    }
}

/// Point-in-time public context for one native perpetual market.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct AssetContext {
    day_notional_volume: Usdc,
    funding_rate: SignedRate,
    impact_prices: Option<[Price; 2]>,
    mark_price: Price,
    mid_price: Option<Price>,
    open_interest: Quantity,
    oracle_price: Price,
    premium: Option<SignedRate>,
    previous_day_price: Price,
}

impl AssetContext {
    /// Returns trailing day notional volume in USDC.
    #[must_use]
    pub const fn day_notional_volume(&self) -> Usdc {
        self.day_notional_volume
    }

    /// Returns the exact signed funding rate.
    #[must_use]
    pub const fn funding_rate(&self) -> SignedRate {
        self.funding_rate
    }

    /// Returns bid- and ask-side impact prices when the venue reports them.
    #[must_use]
    pub const fn impact_prices(&self) -> Option<[Price; 2]> {
        self.impact_prices
    }

    /// Returns the mark price.
    #[must_use]
    pub const fn mark_price(&self) -> Price {
        self.mark_price
    }

    /// Returns the mid price when the venue reports one.
    #[must_use]
    pub const fn mid_price(&self) -> Option<Price> {
        self.mid_price
    }

    /// Returns native-asset open interest.
    #[must_use]
    pub const fn open_interest(&self) -> Quantity {
        self.open_interest
    }

    /// Returns the oracle price.
    #[must_use]
    pub const fn oracle_price(&self) -> Price {
        self.oracle_price
    }

    /// Returns the exact signed premium rate when the venue reports one.
    #[must_use]
    pub const fn premium(&self) -> Option<SignedRate> {
        self.premium
    }

    /// Returns the previous-day price.
    #[must_use]
    pub const fn previous_day_price(&self) -> Price {
        self.previous_day_price
    }
}

/// Native-perpetual metadata paired with its point-in-time asset context.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct PerpAsset {
    market: Market,
    size_decimals: u8,
    max_leverage: VenueMaxLeverage,
    only_isolated: bool,
    margin_table_id: Option<u32>,
    context: AssetContext,
}

impl PerpAsset {
    /// Returns the native perpetual symbol.
    #[must_use]
    pub const fn market(&self) -> &Market {
        &self.market
    }

    /// Returns the point-in-time size precision.
    #[must_use]
    pub const fn size_decimals(&self) -> u8 {
        self.size_decimals
    }

    /// Returns the venue leverage limit without applying the paper cap.
    #[must_use]
    pub const fn max_leverage(&self) -> VenueMaxLeverage {
        self.max_leverage
    }

    /// Returns whether the venue marks the asset isolated-only.
    #[must_use]
    pub const fn only_isolated(&self) -> bool {
        self.only_isolated
    }

    /// Returns the optional venue margin-table identifier.
    #[must_use]
    pub const fn margin_table_id(&self) -> Option<u32> {
        self.margin_table_id
    }

    /// Returns the aligned point-in-time asset context.
    #[must_use]
    pub const fn context(&self) -> &AssetContext {
        &self.context
    }
}

/// Order-preserving native-perpetual metadata/context snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetaAndAssetContexts {
    assets: Vec<PerpAsset>,
}

impl MetaAndAssetContexts {
    /// Returns assets in the exchange's aligned universe order.
    #[must_use]
    pub fn assets(&self) -> &[PerpAsset] {
        &self.assets
    }
}

/// One positive L2 price level.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct BookLevel {
    price: Price,
    quantity: Quantity,
    order_count: u32,
}

impl BookLevel {
    /// Returns the level price.
    #[must_use]
    pub const fn price(&self) -> Price {
        self.price
    }

    /// Returns the positive visible quantity.
    #[must_use]
    pub const fn quantity(&self) -> Quantity {
        self.quantity
    }

    /// Returns the positive number of contributing orders.
    #[must_use]
    pub const fn order_count(&self) -> u32 {
        self.order_count
    }
}

/// One point-in-time native-perpetual L2 snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct L2Book {
    market: Market,
    time_ms: i64,
    bids: Vec<BookLevel>,
    asks: Vec<BookLevel>,
}

impl L2Book {
    /// Returns the native perpetual symbol.
    #[must_use]
    pub const fn market(&self) -> &Market {
        &self.market
    }

    /// Returns the snapshot epoch-millisecond timestamp.
    #[must_use]
    pub const fn time_ms(&self) -> i64 {
        self.time_ms
    }

    /// Returns bid levels in exchange response order.
    #[must_use]
    pub fn bids(&self) -> &[BookLevel] {
        &self.bids
    }

    /// Returns ask levels in exchange response order.
    #[must_use]
    pub fn asks(&self) -> &[BookLevel] {
        &self.asks
    }
}

/// One normalized candle from a requested native-perpetual interval.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Candle {
    open_time_ms: i64,
    close_time_ms: i64,
    market: Market,
    interval: CandleInterval,
    open: Price,
    close: Price,
    high: Price,
    low: Price,
    volume: Quantity,
    trade_count: u64,
}

impl Candle {
    /// Returns the candle-open epoch-millisecond timestamp.
    #[must_use]
    pub const fn open_time_ms(&self) -> i64 {
        self.open_time_ms
    }

    /// Returns the candle-close epoch-millisecond timestamp.
    #[must_use]
    pub const fn close_time_ms(&self) -> i64 {
        self.close_time_ms
    }

    /// Returns the native perpetual symbol.
    #[must_use]
    pub const fn market(&self) -> &Market {
        &self.market
    }

    /// Returns the requested closed interval.
    #[must_use]
    pub const fn interval(&self) -> CandleInterval {
        self.interval
    }

    /// Returns the candle-open price.
    #[must_use]
    pub const fn open(&self) -> Price {
        self.open
    }

    /// Returns the candle-close price.
    #[must_use]
    pub const fn close(&self) -> Price {
        self.close
    }

    /// Returns the candle-high price.
    #[must_use]
    pub const fn high(&self) -> Price {
        self.high
    }

    /// Returns the candle-low price.
    #[must_use]
    pub const fn low(&self) -> Price {
        self.low
    }

    /// Returns native-asset volume.
    #[must_use]
    pub const fn volume(&self) -> Quantity {
        self.volume
    }

    /// Returns the nonnegative trade count.
    #[must_use]
    pub const fn trade_count(&self) -> u64 {
        self.trade_count
    }
}

/// One normalized historical funding record.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct FundingRecord {
    market: Market,
    funding_rate: SignedRate,
    premium: SignedRate,
    time_ms: i64,
}

impl FundingRecord {
    /// Returns the native perpetual symbol.
    #[must_use]
    pub const fn market(&self) -> &Market {
        &self.market
    }

    /// Returns the exact signed funding rate.
    #[must_use]
    pub const fn funding_rate(&self) -> SignedRate {
        self.funding_rate
    }

    /// Returns the exact signed premium rate.
    #[must_use]
    pub const fn premium(&self) -> SignedRate {
        self.premium
    }

    /// Returns the funding epoch-millisecond timestamp.
    #[must_use]
    pub const fn time_ms(&self) -> i64 {
        self.time_ms
    }
}

#[derive(Deserialize)]
struct RawMeta {
    universe: Vec<RawUniverseAsset>,
}

#[derive(Deserialize)]
struct RawUniverseAsset {
    name: String,
    #[serde(rename = "szDecimals")]
    size_decimals: u8,
    #[serde(rename = "maxLeverage")]
    max_leverage: u32,
    #[serde(rename = "onlyIsolated", default)]
    only_isolated: bool,
    #[serde(rename = "marginTableId")]
    margin_table_id: Option<u32>,
}

#[derive(Deserialize)]
struct RawAssetContext {
    #[serde(rename = "dayNtlVlm")]
    day_notional_volume: String,
    funding: String,
    #[serde(rename = "impactPxs")]
    impact_prices: RequiredNullable<[String; 2]>,
    #[serde(rename = "markPx")]
    mark_price: String,
    #[serde(rename = "midPx")]
    mid_price: RequiredNullable<String>,
    #[serde(rename = "openInterest")]
    open_interest: String,
    #[serde(rename = "oraclePx")]
    oracle_price: String,
    premium: RequiredNullable<String>,
    #[serde(rename = "prevDayPx")]
    previous_day_price: String,
}

#[derive(Deserialize)]
struct RequiredNullable<T>(Option<T>);

#[derive(Deserialize)]
struct RawL2Book {
    coin: String,
    time: i64,
    levels: [Vec<RawBookLevel>; 2],
}

#[derive(Deserialize)]
struct RawBookLevel {
    px: String,
    sz: String,
    n: i64,
}

#[derive(Deserialize)]
struct RawCandle {
    t: i64,
    #[serde(rename = "T")]
    close_time: i64,
    s: String,
    i: String,
    o: String,
    c: String,
    h: String,
    l: String,
    v: String,
    n: i64,
}

#[derive(Deserialize)]
struct RawFundingRecord {
    coin: String,
    #[serde(rename = "fundingRate")]
    funding_rate: String,
    premium: String,
    time: i64,
}

pub(crate) fn decode_meta(body: &[u8]) -> Result<MetaAndAssetContexts, InfoError> {
    let (meta, contexts): (RawMeta, Vec<RawAssetContext>) = decode(body)?;
    if meta.universe.len() != contexts.len() {
        return Err(invalid_response(
            "metaAndAssetCtxs",
            "universe and context arrays must be index-aligned",
        ));
    }

    let mut markets = BTreeSet::new();
    let assets = meta
        .universe
        .into_iter()
        .zip(contexts)
        .map(|(asset, context)| {
            let market = parse_native_market(&asset.name, "universe.name")?;
            if !markets.insert(market.clone()) {
                return Err(invalid_response(
                    "universe.name",
                    "native market names must be unique",
                ));
            }
            if asset.max_leverage == 0 {
                return Err(invalid_response("universe.maxLeverage", "must be positive"));
            }
            Ok(PerpAsset {
                market,
                size_decimals: asset.size_decimals,
                max_leverage: VenueMaxLeverage(asset.max_leverage),
                only_isolated: asset.only_isolated,
                margin_table_id: asset.margin_table_id,
                context: normalize_context(context)?,
            })
        })
        .collect::<Result<Vec<_>, InfoError>>()?;

    Ok(MetaAndAssetContexts { assets })
}

pub(crate) fn decode_all_mids(body: &[u8]) -> Result<BTreeMap<Market, Price>, InfoError> {
    let raw: BTreeMap<String, String> = decode(body)?;
    raw.into_iter()
        .filter(|(name, _)| is_native_market_candidate(name))
        .map(|(name, value)| {
            let market = parse_native_market(&name, "allMids.coin")?;
            let price = parse_price(&value, "allMids.price")?;
            Ok((market, price))
        })
        .collect()
}

pub(crate) fn decode_l2_book(body: &[u8], requested: &Market) -> Result<L2Book, InfoError> {
    let raw: RawL2Book = decode(body)?;
    let market = matching_market(&raw.coin, requested, "l2Book.coin")?;
    require_positive_time(raw.time, "l2Book.time")?;
    let [raw_bids, raw_asks] = raw.levels;
    if raw_bids.len() > 20 || raw_asks.len() > 20 {
        return Err(invalid_response(
            "l2Book.levels",
            "must contain at most 20 levels per side",
        ));
    }
    let bids = normalize_levels(raw_bids)?;
    let asks = normalize_levels(raw_asks)?;
    Ok(L2Book {
        market,
        time_ms: raw.time,
        bids,
        asks,
    })
}

pub(crate) fn decode_candles(
    body: &[u8],
    requested: &Market,
    interval: CandleInterval,
    range: TimeRange,
) -> Result<Vec<Candle>, InfoError> {
    let raw: Vec<RawCandle> = decode(body)?;
    if raw.len() > 5_000 {
        return Err(InfoError::InvalidResponse {
            field: "candleSnapshot",
            requirement: "must contain at most 5000 candles",
        });
    }

    raw.into_iter()
        .map(|candle| normalize_candle(candle, requested, interval, range))
        .collect()
}

pub(crate) fn decode_funding(
    body: &[u8],
    requested: &Market,
    range: TimeRange,
) -> Result<Vec<FundingRecord>, InfoError> {
    let raw: Vec<RawFundingRecord> = decode(body)?;
    raw.into_iter()
        .map(|record| {
            let market = matching_market(&record.coin, requested, "fundingHistory.coin")?;
            require_positive_time(record.time, "fundingHistory.time")?;
            if !range.contains(record.time) {
                return Err(invalid_response(
                    "fundingHistory.time",
                    "must fall within the requested inclusive range",
                ));
            }
            Ok(FundingRecord {
                market,
                funding_rate: parse_signed_rate(
                    &record.funding_rate,
                    "fundingHistory.fundingRate",
                )?,
                premium: parse_signed_rate(&record.premium, "fundingHistory.premium")?,
                time_ms: record.time,
            })
        })
        .collect()
}

fn normalize_context(raw: RawAssetContext) -> Result<AssetContext, InfoError> {
    let impact_prices = raw
        .impact_prices
        .0
        .map(|[bid_impact, ask_impact]| {
            Ok([
                parse_price(&bid_impact, "assetCtx.impactPxs")?,
                parse_price(&ask_impact, "assetCtx.impactPxs")?,
            ])
        })
        .transpose()?;
    Ok(AssetContext {
        day_notional_volume: parse_usdc(&raw.day_notional_volume, "assetCtx.dayNtlVlm")?,
        funding_rate: parse_signed_rate(&raw.funding, "assetCtx.funding")?,
        impact_prices,
        mark_price: parse_price(&raw.mark_price, "assetCtx.markPx")?,
        mid_price: raw
            .mid_price
            .0
            .as_deref()
            .map(|value| parse_price(value, "assetCtx.midPx"))
            .transpose()?,
        open_interest: parse_quantity(&raw.open_interest, "assetCtx.openInterest")?,
        oracle_price: parse_price(&raw.oracle_price, "assetCtx.oraclePx")?,
        premium: raw
            .premium
            .0
            .as_deref()
            .map(|value| parse_signed_rate(value, "assetCtx.premium"))
            .transpose()?,
        previous_day_price: parse_price(&raw.previous_day_price, "assetCtx.prevDayPx")?,
    })
}

fn normalize_levels(raw: Vec<RawBookLevel>) -> Result<Vec<BookLevel>, InfoError> {
    raw.into_iter()
        .map(|level| {
            let quantity = parse_quantity(&level.sz, "l2Book.levels.sz")?;
            if quantity.value() == Decimal::ZERO {
                return Err(invalid_response("l2Book.levels.sz", "must be positive"));
            }
            let order_count = u32::try_from(level.n)
                .map_err(|_| invalid_response("l2Book.levels.n", "must be a positive integer"))?;
            if order_count == 0 {
                return Err(invalid_response(
                    "l2Book.levels.n",
                    "must be a positive integer",
                ));
            }
            Ok(BookLevel {
                price: parse_price(&level.px, "l2Book.levels.px")?,
                quantity,
                order_count,
            })
        })
        .collect()
}

fn normalize_candle(
    raw: RawCandle,
    requested: &Market,
    interval: CandleInterval,
    range: TimeRange,
) -> Result<Candle, InfoError> {
    let market = matching_market(&raw.s, requested, "candleSnapshot.s")?;
    if raw.i != interval.as_wire() {
        return Err(invalid_response(
            "candleSnapshot.i",
            "must match the requested interval",
        ));
    }
    require_positive_time(raw.t, "candleSnapshot.t")?;
    require_positive_time(raw.close_time, "candleSnapshot.T")?;
    let expected_close_time = raw
        .t
        .checked_add(interval.duration_ms())
        .and_then(|end_time| end_time.checked_sub(1))
        .ok_or_else(|| {
            invalid_response(
                "candleSnapshot.T",
                "declared interval close must not overflow",
            )
        })?;
    if raw.close_time != expected_close_time {
        return Err(invalid_response(
            "candleSnapshot.T",
            "must equal the declared interval close",
        ));
    }
    if raw.t > range.end_ms() || raw.close_time < range.start_ms() {
        return Err(invalid_response(
            "candleSnapshot.time",
            "must overlap the requested inclusive range",
        ));
    }

    let open = parse_price(&raw.o, "candleSnapshot.o")?;
    let close = parse_price(&raw.c, "candleSnapshot.c")?;
    let high = parse_price(&raw.h, "candleSnapshot.h")?;
    let low = parse_price(&raw.l, "candleSnapshot.l")?;
    if high < open || high < close || low > open || low > close || high < low {
        return Err(invalid_response(
            "candleSnapshot.ohlc",
            "high and low must bound open and close",
        ));
    }
    let trade_count = u64::try_from(raw.n)
        .map_err(|_| invalid_response("candleSnapshot.n", "must be a nonnegative integer"))?;
    let volume = parse_quantity(&raw.v, "candleSnapshot.v")?;
    let has_volume = volume.value() != Decimal::ZERO;
    let has_trades = trade_count > 0;
    if has_volume != has_trades {
        return Err(invalid_response(
            "candleSnapshot.activity",
            "volume must be zero if and only if trade count is zero",
        ));
    }
    if !has_trades && (open != close || open != high || open != low) {
        return Err(invalid_response(
            "candleSnapshot.ohlc",
            "zero-trade candles must be flat",
        ));
    }
    Ok(Candle {
        open_time_ms: raw.t,
        close_time_ms: raw.close_time,
        market,
        interval,
        open,
        close,
        high,
        low,
        volume,
        trade_count,
    })
}

fn decode<T: for<'de> Deserialize<'de>>(body: &[u8]) -> Result<T, InfoError> {
    serde_json::from_slice(body).map_err(|_| InfoError::Decode)
}

fn parse_decimal(value: &str, field: &'static str) -> Result<Decimal, InfoError> {
    Decimal::from_str_exact(value)
        .map_err(|_| invalid_response(field, "must be an exact decimal string"))
}

fn parse_price(value: &str, field: &'static str) -> Result<Price, InfoError> {
    let decimal = parse_decimal(value, field)?;
    Price::new(decimal).map_err(|_| invalid_response(field, "must be a positive decimal string"))
}

fn parse_quantity(value: &str, field: &'static str) -> Result<Quantity, InfoError> {
    let decimal = parse_decimal(value, field)?;
    Quantity::new(decimal)
        .map_err(|_| invalid_response(field, "must be a nonnegative decimal string"))
}

fn parse_usdc(value: &str, field: &'static str) -> Result<Usdc, InfoError> {
    let decimal = parse_decimal(value, field)?;
    Usdc::new(decimal).map_err(|_| invalid_response(field, "must be a nonnegative decimal string"))
}

fn parse_signed_rate(value: &str, field: &'static str) -> Result<SignedRate, InfoError> {
    parse_decimal(value, field).map(SignedRate)
}

fn parse_native_market(value: &str, field: &'static str) -> Result<Market, InfoError> {
    if !is_native_market_candidate(value) {
        return Err(invalid_response(field, "must be a native perpetual symbol"));
    }
    Market::new(value).map_err(|_| invalid_response(field, "must be a native perpetual symbol"))
}

fn matching_market(
    value: &str,
    requested: &Market,
    field: &'static str,
) -> Result<Market, InfoError> {
    let market = parse_native_market(value, field)?;
    if &market != requested {
        return Err(invalid_response(field, "must match the requested coin"));
    }
    Ok(market)
}

fn require_positive_time(value: i64, field: &'static str) -> Result<(), InfoError> {
    if value <= 0 {
        return Err(invalid_response(
            field,
            "must be positive epoch milliseconds",
        ));
    }
    Ok(())
}

fn is_native_market_candidate(value: &str) -> bool {
    !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_alphanumeric())
}

fn invalid_response(field: &'static str, requirement: &'static str) -> InfoError {
    InfoError::InvalidResponse { field, requirement }
}
