//! Bounded capture of public point-in-time context into normalized facts.
//!
//! The venue leaves `metaAndAssetCtxs` without an exchange timestamp. Those
//! metadata and context facts therefore use the explicitly injected receipt
//! timestamp as both their event and availability time. The current funding
//! field is retained without a mark as a non-settlement feature/risk fact:
//! only exchange-timestamped funding records can carry settlement semantics.
//! Every exchange-timestamped book, funding-history record, and completed
//! candle retains its source timestamp separately from that receipt timestamp.

use std::collections::{BTreeSet, HashSet};
use std::future::Future;
use std::sync::Arc;

use blake3::Hasher;
use futures_util::{StreamExt, TryStreamExt, stream};
use thiserror::Error;
use tokio::sync::Semaphore;
use trench_core::domain::{EventId, Market};
use trench_core::event::{
    AssetContext as CoreAssetContext, Bbo, BookLevel as CoreBookLevel, BookSnapshot,
    CandleInterval as CoreCandleInterval, CompletedCandle, DurationNs, EventError, Funding,
    FundingRate, MarketEvent, Metadata, TimestampNs,
};

use crate::{
    Candle, CandleInterval, FundingRecord, InfoClient, InfoError, L2Book, L2BookPrecision,
    PerpAsset, TimeRange,
};

/// Maximum native perpetual rows accepted from one metadata response.
pub const MAX_CONTEXT_MARKETS: usize = 128;
/// Maximum detailed markets captured in one bounded public context batch.
pub const MAX_DETAILED_CONTEXT_MARKETS: usize = 30;
/// Maximum public REST operations in flight for one context batch.
pub const MAX_CONTEXT_REQUEST_CONCURRENCY: usize = 8;
/// Maximum historical funding observations retained for one detailed market.
pub const MAX_CONTEXT_FUNDING_RECORDS: usize = 4_096;
/// Maximum normalized facts produced by one bounded context batch.
pub const MAX_CONTEXT_EVENTS: usize = 65_536;
const MAX_CANDLES_PER_SERIES: usize = 5_000;
const MILLISECOND_NS: i128 = 1_000_000;

/// Shared permit pool for every public operation in one context batch.
#[derive(Debug, Clone)]
struct RequestLimiter {
    permits: Arc<Semaphore>,
    #[cfg(test)]
    observed: Arc<ObservedConcurrency>,
}

impl RequestLimiter {
    fn new() -> Self {
        Self {
            permits: Arc::new(Semaphore::new(MAX_CONTEXT_REQUEST_CONCURRENCY)),
            #[cfg(test)]
            observed: Arc::new(ObservedConcurrency::default()),
        }
    }

    #[cfg(test)]
    fn maximum_observed(&self) -> usize {
        self.observed
            .maximum
            .load(std::sync::atomic::Ordering::Relaxed)
    }
}

#[cfg(test)]
#[derive(Debug, Default)]
struct ObservedConcurrency {
    active: std::sync::atomic::AtomicUsize,
    maximum: std::sync::atomic::AtomicUsize,
}

#[cfg(test)]
impl ObservedConcurrency {
    fn request_started(&self) {
        let active = self
            .active
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            + 1;
        let mut maximum = self.maximum.load(std::sync::atomic::Ordering::Relaxed);
        while active > maximum {
            match self.maximum.compare_exchange_weak(
                maximum,
                active,
                std::sync::atomic::Ordering::Relaxed,
                std::sync::atomic::Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(observed) => maximum = observed,
            }
        }
    }

    fn request_finished(&self) {
        self.active
            .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
    }
}

/// Supplies an explicit local receipt timestamp to the I/O adapter.
///
/// The core crate never calls this trait. Production wiring may use an NTP-
/// monitored source; deterministic replay tests can provide fixed timestamps.
pub trait ReceiptClock: Send + Sync {
    /// Returns the time at which the public response became available locally.
    fn receipt_time(&self) -> Option<TimestampNs>;
}

/// Explicit ranges and detailed-market scope for one public context capture.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextCaptureRequest {
    detailed_markets: Vec<Market>,
    fifteen_minute_range: TimeRange,
    hourly_range: TimeRange,
    funding_range: TimeRange,
}

impl ContextCaptureRequest {
    /// Creates a bounded request with exact completed-candle ranges.
    ///
    /// Detailed requests are deliberately independent from metadata discovery:
    /// callers can observe every public native perpetual cheaply, then schedule
    /// bounded detailed captures for the dynamic universe and warm buffer.
    pub fn new(
        mut detailed_markets: Vec<Market>,
        fifteen_minute_range: TimeRange,
        hourly_range: TimeRange,
        funding_range: TimeRange,
    ) -> Result<Self, ContextCaptureError> {
        if detailed_markets.len() > MAX_DETAILED_CONTEXT_MARKETS {
            return Err(ContextCaptureError::TooManyDetailedMarkets {
                actual: detailed_markets.len(),
                limit: MAX_DETAILED_CONTEXT_MARKETS,
            });
        }
        detailed_markets.sort();
        if detailed_markets.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(ContextCaptureError::DuplicateDetailedMarket);
        }
        validate_candle_range(fifteen_minute_range, CandleInterval::FifteenMinutes)?;
        validate_candle_range(hourly_range, CandleInterval::OneHour)?;
        let estimated_events = detailed_markets
            .len()
            .checked_mul(
                expected_candle_count(fifteen_minute_range, CandleInterval::FifteenMinutes)?
                    .checked_add(expected_candle_count(
                        hourly_range,
                        CandleInterval::OneHour,
                    )?)
                    .and_then(|count| count.checked_add(MAX_CONTEXT_FUNDING_RECORDS))
                    .and_then(|count| count.checked_add(2))
                    .ok_or(ContextCaptureError::EventBudgetExceeded {
                        limit: MAX_CONTEXT_EVENTS,
                    })?,
            )
            .and_then(|count| count.checked_add(MAX_CONTEXT_MARKETS * 3))
            .ok_or(ContextCaptureError::EventBudgetExceeded {
                limit: MAX_CONTEXT_EVENTS,
            })?;
        if estimated_events > MAX_CONTEXT_EVENTS {
            return Err(ContextCaptureError::EventBudgetExceeded {
                limit: MAX_CONTEXT_EVENTS,
            });
        }
        Ok(Self {
            detailed_markets,
            fifteen_minute_range,
            hourly_range,
            funding_range,
        })
    }

    /// Returns detailed markets in canonical native-symbol order.
    #[must_use]
    pub fn detailed_markets(&self) -> &[Market] {
        &self.detailed_markets
    }
}

/// One immutable, complete public context capture.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextCaptureBatch {
    events: Vec<MarketEvent>,
    captured_at: TimestampNs,
    source_digest: String,
}

impl ContextCaptureBatch {
    fn new(mut events: Vec<MarketEvent>) -> Result<Self, ContextCaptureError> {
        if events.len() > MAX_CONTEXT_EVENTS {
            return Err(ContextCaptureError::EventBudgetExceeded {
                limit: MAX_CONTEXT_EVENTS,
            });
        }
        events.sort();
        let mut source_ids = HashSet::with_capacity(events.len());
        for event in &events {
            if !source_ids.insert(event.event_id().clone()) {
                return Err(ContextCaptureError::DuplicateSourceId {
                    source_id: event.event_id().clone(),
                });
            }
        }
        let captured_at = events
            .iter()
            .map(MarketEvent::received_at)
            .max()
            .ok_or(ContextCaptureError::EmptyCapture)?;
        let source_digest = source_digest(&events);
        Ok(Self {
            events,
            captured_at,
            source_digest,
        })
    }

    /// Returns every normalized source fact in deterministic event order.
    #[must_use]
    pub fn events(&self) -> &[MarketEvent] {
        &self.events
    }

    /// Returns the latest explicit local availability time in this batch.
    #[must_use]
    pub const fn captured_at(&self) -> TimestampNs {
        self.captured_at
    }

    /// Returns the BLAKE3 commitment to the exact ordered source identities.
    #[must_use]
    pub fn source_digest(&self) -> &str {
        &self.source_digest
    }

    /// Iterates the immutable normalized source IDs retained in this batch.
    pub fn source_ids(&self) -> impl Iterator<Item = &EventId> {
        self.events.iter().map(MarketEvent::event_id)
    }
}

/// Read-only, bounded Hyperliquid public-context capture adapter.
#[derive(Debug, Clone)]
pub struct ContextCapture {
    client: InfoClient,
}

impl ContextCapture {
    /// Creates a context capture adapter from the constrained public client.
    #[must_use]
    pub fn new(client: InfoClient) -> Self {
        Self { client }
    }

    /// Captures one complete public context batch.
    ///
    /// Any failed sub-request, unknown detailed market, malformed source fact,
    /// missing completed candle, duplicate source identity, or unavailable
    /// receipt timestamp rejects the whole batch. No response is silently
    /// dropped or filled with a later/current value.
    pub async fn capture<C: ReceiptClock + ?Sized>(
        &self,
        request: &ContextCaptureRequest,
        clock: &C,
    ) -> Result<ContextCaptureBatch, ContextCaptureError> {
        self.capture_with_limiter(request, clock, &RequestLimiter::new())
            .await
    }

    async fn capture_with_limiter<C: ReceiptClock + ?Sized>(
        &self,
        request: &ContextCaptureRequest,
        clock: &C,
        limiter: &RequestLimiter,
    ) -> Result<ContextCaptureBatch, ContextCaptureError> {
        let (metadata, metadata_received_at) = observed(
            limiter,
            self.client.meta_and_asset_contexts(),
            clock,
            CaptureOperation::Metadata,
            None,
        )
        .await?;
        if metadata.assets().len() > MAX_CONTEXT_MARKETS {
            return Err(ContextCaptureError::TooManyMetadataMarkets {
                actual: metadata.assets().len(),
                limit: MAX_CONTEXT_MARKETS,
            });
        }
        let discovered = metadata
            .assets()
            .iter()
            .filter(|asset| !asset.is_delisted())
            .map(|asset| asset.market().clone())
            .collect::<BTreeSet<_>>();
        if request
            .detailed_markets()
            .iter()
            .any(|market| !discovered.contains(market))
        {
            let market = request
                .detailed_markets()
                .iter()
                .find(|market| !discovered.contains(*market))
                .cloned()
                .ok_or(ContextCaptureError::EmptyCapture)?;
            return Err(ContextCaptureError::DetailedMarketNotDiscovered { market });
        }

        let metadata_events = metadata
            .assets()
            .iter()
            .map(|asset| metadata_events(asset, metadata_received_at))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        let detailed_events = stream::iter(request.detailed_markets().iter().cloned())
            .map(|market| self.capture_detailed_market(market, request, clock, limiter))
            .buffer_unordered(MAX_CONTEXT_REQUEST_CONCURRENCY)
            .try_collect::<Vec<_>>()
            .await?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();

        let mut events = metadata_events;
        events.extend(detailed_events);
        ContextCaptureBatch::new(events)
    }

    async fn capture_detailed_market<C: ReceiptClock + ?Sized>(
        &self,
        market: Market,
        request: &ContextCaptureRequest,
        clock: &C,
        limiter: &RequestLimiter,
    ) -> Result<Vec<MarketEvent>, ContextCaptureError> {
        let (book, fifteen_minute_candles, hourly_candles, funding) = tokio::try_join!(
            observed(
                limiter,
                self.client.l2_book(&market, L2BookPrecision::Full),
                clock,
                CaptureOperation::Book,
                Some(market.clone()),
            ),
            observed(
                limiter,
                self.client.candle_snapshot(
                    &market,
                    CandleInterval::FifteenMinutes,
                    request.fifteen_minute_range,
                ),
                clock,
                CaptureOperation::FifteenMinuteCandles,
                Some(market.clone()),
            ),
            observed(
                limiter,
                self.client
                    .candle_snapshot(&market, CandleInterval::OneHour, request.hourly_range,),
                clock,
                CaptureOperation::HourlyCandles,
                Some(market.clone()),
            ),
            observed(
                limiter,
                self.client.funding_history(&market, request.funding_range),
                clock,
                CaptureOperation::FundingHistory,
                Some(market.clone()),
            ),
        )?;
        let (book, book_received_at) = book;
        let (fifteen_minute_candles, fifteen_minute_received_at) = fifteen_minute_candles;
        let (hourly_candles, hourly_received_at) = hourly_candles;
        let (funding, funding_received_at) = funding;

        let mut events = book_events(book, book_received_at)?.to_vec();
        events.extend(candle_events(
            &market,
            fifteen_minute_candles,
            fifteen_minute_received_at,
            request.fifteen_minute_range,
            CandleInterval::FifteenMinutes,
        )?);
        events.extend(candle_events(
            &market,
            hourly_candles,
            hourly_received_at,
            request.hourly_range,
            CandleInterval::OneHour,
        )?);
        events.extend(funding_events(
            &market,
            funding,
            funding_received_at,
            request.funding_range,
        )?);
        Ok(events)
    }
}

/// A stable reason one public context batch was rejected.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum ContextCaptureError {
    /// The requested detailed scope exceeded the fixed per-batch bound.
    #[error("detailed market count {actual} exceeds {limit}")]
    TooManyDetailedMarkets {
        /// Actual request size.
        actual: usize,
        /// Fixed maximum.
        limit: usize,
    },
    /// The same detailed market was requested more than once.
    #[error("detailed market request contains a duplicate")]
    DuplicateDetailedMarket,
    /// A requested completed-candle range was not exact for its interval.
    #[error("{interval:?} candle range is not an exact sequence of completed bars")]
    UnalignedCandleRange {
        /// Requested interval.
        interval: CandleInterval,
    },
    /// A public context batch could exceed its fixed normalized-event bound.
    #[error("public context event budget exceeds {limit}")]
    EventBudgetExceeded {
        /// Fixed maximum normalized event count.
        limit: usize,
    },
    /// The metadata response exceeded the bounded native-perpetual universe.
    #[error("metadata market count {actual} exceeds {limit}")]
    TooManyMetadataMarkets {
        /// Actual response size.
        actual: usize,
        /// Fixed maximum.
        limit: usize,
    },
    /// A detailed request was not a current native perpetual in metadata.
    #[error("detailed market {market:?} was absent from metadata discovery")]
    DetailedMarketNotDiscovered {
        /// Unknown requested market.
        market: Market,
    },
    /// The explicit receipt clock was unavailable.
    #[error("public context receipt clock is unavailable")]
    ReceiptClockUnavailable,
    /// One constrained public request failed.
    #[error("public context {operation:?} request failed")]
    Info {
        /// Source family that failed.
        operation: CaptureOperation,
        /// Market when this was a detailed request.
        market: Option<Market>,
        /// Stable public-client failure.
        source: InfoError,
    },
    /// The authority-local public-operation limiter became unavailable.
    #[error("public context request limiter became unavailable")]
    RequestLimiterUnavailable,
    /// A normalized core fact rejected source timestamps or payload values.
    #[error("public context source normalization failed")]
    Event(#[from] EventError),
    /// A metadata leverage value did not fit the core event representation.
    #[error("venue leverage for {market:?} cannot fit the normalized event")]
    VenueLeverageOutOfRange {
        /// Affected market.
        market: Market,
    },
    /// A candle response did not contain an exact complete sequence.
    #[error("{interval:?} candle response for {market:?} was incomplete")]
    IncompleteCandles {
        /// Affected market.
        market: Market,
        /// Requested interval.
        interval: CandleInterval,
    },
    /// A funding-history response exceeded its retained resource ceiling.
    #[error("funding history for {market:?} exceeds {limit} records")]
    TooManyFundingRecords {
        /// Affected market.
        market: Market,
        /// Fixed maximum record count.
        limit: usize,
    },
    /// A funding-history response did not have strictly increasing timestamps.
    #[error("funding history for {market:?} was not strictly time ordered")]
    UnorderedFunding {
        /// Affected market.
        market: Market,
    },
    /// A purported full L2 snapshot omitted one executable side.
    #[error("full L2 snapshot for {market:?} omitted its {side} side")]
    MissingBookSide {
        /// Affected market.
        market: Market,
        /// Missing side name.
        side: &'static str,
    },
    /// A purported full L2 snapshot did not preserve strict price priority.
    #[error("full L2 snapshot for {market:?} has non-monotonic {side} prices")]
    NonMonotonicBookSide {
        /// Affected market.
        market: Market,
        /// Side with invalid price priority.
        side: &'static str,
    },
    /// Two independently decoded source facts produced one normalized identity.
    #[error("duplicate normalized source identity {source_id:?}")]
    DuplicateSourceId {
        /// Canonical normalized event identity.
        source_id: EventId,
    },
    /// An internal invariant unexpectedly produced no source facts.
    #[error("public context capture contained no source facts")]
    EmptyCapture,
}

/// Public source family used in stable capture errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CaptureOperation {
    /// Whole native-perpetual metadata and context discovery.
    Metadata,
    /// One detailed full L2 snapshot.
    Book,
    /// One completed fifteen-minute candle sequence.
    FifteenMinuteCandles,
    /// One completed one-hour candle sequence.
    HourlyCandles,
    /// One bounded historical funding sequence.
    FundingHistory,
}

async fn observed<T, F, C>(
    limiter: &RequestLimiter,
    request: F,
    clock: &C,
    operation: CaptureOperation,
    market: Option<Market>,
) -> Result<(T, TimestampNs), ContextCaptureError>
where
    F: Future<Output = Result<T, InfoError>>,
    C: ReceiptClock + ?Sized,
{
    let permit = limiter
        .permits
        .clone()
        .acquire_owned()
        .await
        .map_err(|_| ContextCaptureError::RequestLimiterUnavailable)?;
    #[cfg(test)]
    limiter.observed.request_started();
    let result = request.await;
    #[cfg(test)]
    limiter.observed.request_finished();
    drop(permit);
    let value = result.map_err(|source| ContextCaptureError::Info {
        operation,
        market,
        source,
    })?;
    let received_at = clock
        .receipt_time()
        .ok_or(ContextCaptureError::ReceiptClockUnavailable)?;
    Ok((value, received_at))
}

fn metadata_events(
    asset: &PerpAsset,
    received_at: TimestampNs,
) -> Result<[MarketEvent; 3], ContextCaptureError> {
    let market = asset.market().clone();
    let leverage = u16::try_from(asset.max_leverage().value()).map_err(|_| {
        ContextCaptureError::VenueLeverageOutOfRange {
            market: market.clone(),
        }
    })?;
    let context = asset.context();
    Ok([
        MarketEvent::metadata(
            received_at,
            received_at,
            market.clone(),
            Metadata::new(asset.size_decimals(), leverage, !asset.is_delisted()),
        )?,
        MarketEvent::asset_context(
            received_at,
            received_at,
            market.clone(),
            CoreAssetContext::new(
                context.mark_price(),
                context.oracle_price(),
                context.mid_price(),
                context.open_interest(),
                context.day_notional_volume(),
                FundingRate::new(context.funding_rate().value()),
            ),
        )?,
        MarketEvent::funding(
            received_at,
            received_at,
            market,
            Funding::historical(FundingRate::new(context.funding_rate().value())),
        )?,
    ])
}

fn book_events(
    book: L2Book,
    received_at: TimestampNs,
) -> Result<[MarketEvent; 2], ContextCaptureError> {
    let market = book.market().clone();
    let event_time = timestamp_from_ms(book.time_ms())?;
    let sequence = u64::try_from(book.time_ms()).map_err(|_| EventError::TimestampOutOfRange {
        value: i128::from(book.time_ms()),
    })?;
    let bids = book
        .bids()
        .iter()
        .map(|level| CoreBookLevel::new(level.price(), level.quantity()))
        .collect::<Vec<_>>();
    let asks = book
        .asks()
        .iter()
        .map(|level| CoreBookLevel::new(level.price(), level.quantity()))
        .collect::<Vec<_>>();
    if bids
        .windows(2)
        .any(|pair| pair[0].price() <= pair[1].price())
    {
        return Err(ContextCaptureError::NonMonotonicBookSide {
            market,
            side: "bid",
        });
    }
    if asks
        .windows(2)
        .any(|pair| pair[0].price() >= pair[1].price())
    {
        return Err(ContextCaptureError::NonMonotonicBookSide {
            market,
            side: "ask",
        });
    }
    let bid = bids
        .first()
        .copied()
        .ok_or(ContextCaptureError::MissingBookSide {
            market: market.clone(),
            side: "bid",
        })?;
    let ask = asks
        .first()
        .copied()
        .ok_or(ContextCaptureError::MissingBookSide {
            market: market.clone(),
            side: "ask",
        })?;
    let snapshot = BookSnapshot::new(sequence, bids, asks);
    let bbo = Bbo::new(sequence, bid, ask)?;
    Ok([
        MarketEvent::book_snapshot(event_time, received_at, market.clone(), snapshot)?,
        MarketEvent::bbo(event_time, received_at, market, bbo)?,
    ])
}

fn candle_events(
    market: &Market,
    mut candles: Vec<Candle>,
    received_at: TimestampNs,
    range: TimeRange,
    interval: CandleInterval,
) -> Result<Vec<MarketEvent>, ContextCaptureError> {
    validate_candle_response(market, &mut candles, range, interval)?;
    candles
        .into_iter()
        .map(|candle| {
            let open_time = timestamp_from_ms(candle.open_time_ms())?;
            let event_time = timestamp_from_ms(candle.close_time_ms())?
                .checked_add(DurationNs::new(MILLISECOND_NS)?)?;
            let core_interval = core_interval(interval);
            MarketEvent::completed_candle(
                event_time,
                received_at,
                market.clone(),
                CompletedCandle::new(
                    core_interval,
                    open_time,
                    candle.open(),
                    candle.high(),
                    candle.low(),
                    candle.close(),
                    candle.volume(),
                    candle.trade_count(),
                )?,
            )
            .map_err(ContextCaptureError::from)
        })
        .collect()
}

fn funding_events(
    market: &Market,
    funding: Vec<FundingRecord>,
    received_at: TimestampNs,
    range: TimeRange,
) -> Result<Vec<MarketEvent>, ContextCaptureError> {
    if funding.len() > MAX_CONTEXT_FUNDING_RECORDS {
        return Err(ContextCaptureError::TooManyFundingRecords {
            market: market.clone(),
            limit: MAX_CONTEXT_FUNDING_RECORDS,
        });
    }
    if funding
        .windows(2)
        .any(|pair| pair[0].time_ms() >= pair[1].time_ms())
        || funding.iter().any(|record| {
            record.market() != market
                || record.time_ms() < range.start_ms()
                || record.time_ms() > range.end_ms()
        })
    {
        return Err(ContextCaptureError::UnorderedFunding {
            market: market.clone(),
        });
    }
    funding
        .into_iter()
        .map(|record| {
            MarketEvent::funding(
                timestamp_from_ms(record.time_ms())?,
                received_at,
                market.clone(),
                Funding::historical(FundingRate::new(record.funding_rate().value())),
            )
            .map_err(ContextCaptureError::from)
        })
        .collect()
}

fn validate_candle_range(
    range: TimeRange,
    interval: CandleInterval,
) -> Result<(), ContextCaptureError> {
    let duration = interval.duration_ms();
    let end_exclusive = range
        .end_ms()
        .checked_add(1)
        .ok_or(ContextCaptureError::UnalignedCandleRange { interval })?;
    if range.start_ms() % duration != 0
        || end_exclusive % duration != 0
        || expected_candle_count(range, interval)? > MAX_CANDLES_PER_SERIES
    {
        return Err(ContextCaptureError::UnalignedCandleRange { interval });
    }
    Ok(())
}

fn expected_candle_count(
    range: TimeRange,
    interval: CandleInterval,
) -> Result<usize, ContextCaptureError> {
    let end_exclusive = range
        .end_ms()
        .checked_add(1)
        .ok_or(ContextCaptureError::UnalignedCandleRange { interval })?;
    let span = end_exclusive
        .checked_sub(range.start_ms())
        .ok_or(ContextCaptureError::UnalignedCandleRange { interval })?;
    usize::try_from(span / interval.duration_ms()).map_err(|_| {
        ContextCaptureError::EventBudgetExceeded {
            limit: MAX_CONTEXT_EVENTS,
        }
    })
}

fn validate_candle_response(
    market: &Market,
    candles: &mut [Candle],
    range: TimeRange,
    interval: CandleInterval,
) -> Result<(), ContextCaptureError> {
    candles.sort_by_key(Candle::open_time_ms);
    let mut expected_open = range.start_ms();
    for candle in candles.iter() {
        if candle.market() != market
            || candle.interval() != interval
            || candle.open_time_ms() != expected_open
        {
            return Err(ContextCaptureError::IncompleteCandles {
                market: market.clone(),
                interval,
            });
        }
        expected_open = expected_open.checked_add(interval.duration_ms()).ok_or(
            ContextCaptureError::IncompleteCandles {
                market: market.clone(),
                interval,
            },
        )?;
    }
    let expected_end =
        range
            .end_ms()
            .checked_add(1)
            .ok_or(ContextCaptureError::IncompleteCandles {
                market: market.clone(),
                interval,
            })?;
    if expected_open != expected_end || candles.len() != expected_candle_count(range, interval)? {
        return Err(ContextCaptureError::IncompleteCandles {
            market: market.clone(),
            interval,
        });
    }
    Ok(())
}

fn core_interval(interval: CandleInterval) -> CoreCandleInterval {
    match interval {
        CandleInterval::FifteenMinutes => CoreCandleInterval::FifteenMinutes,
        CandleInterval::OneHour => CoreCandleInterval::OneHour,
    }
}

fn timestamp_from_ms(milliseconds: i64) -> Result<TimestampNs, EventError> {
    TimestampNs::new(i128::from(milliseconds).checked_mul(MILLISECOND_NS).ok_or(
        EventError::TimestampOutOfRange {
            value: i128::from(milliseconds),
        },
    )?)
}

fn source_digest(events: &[MarketEvent]) -> String {
    let mut hasher = Hasher::new_derive_key("trench.public-context-capture.v1");
    for event in events {
        hasher.update(&(event.event_id().as_str().len() as u64).to_be_bytes());
        hasher.update(event.event_id().as_str().as_bytes());
        hasher.update(&event.event_time().value().to_be_bytes());
        hasher.update(&event.received_at().value().to_be_bytes());
    }
    format!("b3:{}", hasher.finalize().to_hex())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::time::Duration;

    use serde_json::{Value, json};
    use trench_core::domain::Market;
    use trench_core::event::{MarketEventKind, TimestampNs};
    use wiremock::matchers::{body_json, method, path};
    use wiremock::{Mock, MockServer, Request, ResponseTemplate};

    use super::{
        ContextCapture, ContextCaptureError, ContextCaptureRequest,
        MAX_CONTEXT_REQUEST_CONCURRENCY, MAX_DETAILED_CONTEXT_MARKETS, ReceiptClock,
        RequestLimiter,
    };
    use crate::{InfoClient, TimeRange};

    const HOUR_START_MS: i64 = 1_700_002_800_000;
    const HOUR_END_MS: i64 = HOUR_START_MS + 3_600_000 - 1;
    const RECEIPT_NS: i128 = (HOUR_END_MS as i128 + 60_000) * 1_000_000;
    const META_FIXTURE: &str = include_str!("../../../tests/fixtures/meta/native-perps.json");

    #[derive(Debug)]
    struct FixedClock;

    impl ReceiptClock for FixedClock {
        fn receipt_time(&self) -> Option<TimestampNs> {
            TimestampNs::new(RECEIPT_NS).ok()
        }
    }

    fn market(value: &str) -> Market {
        Market::new(value).expect("fixture market")
    }

    fn range() -> TimeRange {
        TimeRange::new(HOUR_START_MS, HOUR_END_MS).expect("fixture range")
    }

    fn request() -> ContextCaptureRequest {
        ContextCaptureRequest::new(vec![market("BTC")], range(), range(), range())
            .expect("bounded fixture request")
    }

    fn l2_body() -> Value {
        json!({
            "coin": "BTC",
            "time": HOUR_START_MS,
            "levels": [
                [{"px": "64120.5", "sz": "1.5", "n": 2}],
                [{"px": "64121.0", "sz": "0.75", "n": 1}]
            ]
        })
    }

    fn l2_body_for(symbol: &str) -> Value {
        let mut body = l2_body();
        body["coin"] = json!(symbol);
        body
    }

    fn candle_body(interval: &str, step_ms: i64, count: usize) -> Value {
        Value::Array(
            (0..count)
                .map(|index| {
                    let open = HOUR_START_MS + i64::try_from(index).expect("small index") * step_ms;
                    json!({
                        "t": open,
                        "T": open + step_ms - 1,
                        "s": "BTC",
                        "i": interval,
                        "o": "64120.0",
                        "c": "64121.0",
                        "h": "64122.0",
                        "l": "64119.0",
                        "v": "10.0",
                        "n": 3
                    })
                })
                .collect(),
        )
    }

    fn candle_body_for(symbol: &str, interval: &str, step_ms: i64, count: usize) -> Value {
        let mut body = candle_body(interval, step_ms, count);
        for candle in body.as_array_mut().expect("fixture candle array") {
            candle["s"] = json!(symbol);
        }
        body
    }

    fn three_market_metadata() -> Value {
        let mut metadata =
            serde_json::from_str::<Value>(META_FIXTURE).expect("fixture metadata must parse");
        metadata[0]["universe"]
            .as_array_mut()
            .expect("fixture universe")
            .truncate(3);
        metadata[1]
            .as_array_mut()
            .expect("fixture contexts")
            .truncate(3);
        metadata
    }

    async fn mounted_capture(
        partial_fifteen_minutes: bool,
        expected_calls: u64,
    ) -> (ContextCapture, MockServer) {
        let server = MockServer::start().await;
        let client = InfoClient::new_loopback_for_test(&format!("{}/info", server.uri()))
            .expect("loopback client");
        Mock::given(method("POST"))
            .and(path("/info"))
            .and(body_json(json!({"type": "metaAndAssetCtxs"})))
            .respond_with(ResponseTemplate::new(200).set_body_raw(META_FIXTURE, "application/json"))
            .expect(expected_calls)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/info"))
            .and(body_json(json!({"type": "l2Book", "coin": "BTC"})))
            .respond_with(ResponseTemplate::new(200).set_body_json(l2_body()))
            .expect(expected_calls)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/info"))
            .and(body_json(json!({
                "type": "candleSnapshot",
                "req": {
                    "coin": "BTC",
                    "interval": "15m",
                    "startTime": HOUR_START_MS,
                    "endTime": HOUR_END_MS,
                }
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(candle_body(
                "15m",
                900_000,
                if partial_fifteen_minutes { 3 } else { 4 },
            )))
            .expect(expected_calls)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/info"))
            .and(body_json(json!({
                "type": "candleSnapshot",
                "req": {
                    "coin": "BTC",
                    "interval": "1h",
                    "startTime": HOUR_START_MS,
                    "endTime": HOUR_END_MS,
                }
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(candle_body("1h", 3_600_000, 1)))
            .expect(expected_calls)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/info"))
            .and(body_json(json!({
                "type": "fundingHistory",
                "coin": "BTC",
                "startTime": HOUR_START_MS,
                "endTime": HOUR_END_MS,
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!([{
                "coin": "BTC",
                "fundingRate": "0.00001",
                "premium": "0.00002",
                "time": HOUR_START_MS,
            }])))
            .expect(expected_calls)
            .mount(&server)
            .await;
        (ContextCapture::new(client), server)
    }

    async fn mounted_metadata_capture(metadata: Value) -> (ContextCapture, MockServer) {
        let server = MockServer::start().await;
        let client = InfoClient::new_loopback_for_test(&format!("{}/info", server.uri()))
            .expect("loopback client");
        Mock::given(method("POST"))
            .and(path("/info"))
            .and(body_json(json!({"type": "metaAndAssetCtxs"})))
            .respond_with(ResponseTemplate::new(200).set_body_json(metadata))
            .expect(1)
            .mount(&server)
            .await;
        (ContextCapture::new(client), server)
    }

    #[tokio::test]
    async fn capture_materializes_complete_explicit_time_public_context() {
        let (capture, _server) = mounted_capture(false, 1).await;
        let batch = capture
            .capture(&request(), &FixedClock)
            .await
            .expect("complete public context");

        assert_eq!(batch.events().len(), 20);
        assert_eq!(batch.captured_at().value(), RECEIPT_NS as i64);
        assert!(batch.source_digest().starts_with("b3:"));
        assert!(batch.events().windows(2).all(|pair| pair[0] <= pair[1]));
        assert!(
            batch
                .events()
                .iter()
                .all(|event| event.event_time() <= event.received_at())
        );
        assert_eq!(
            batch.source_ids().collect::<BTreeSet<_>>().len(),
            batch.events().len()
        );
        assert!(batch.events().iter().any(|event| {
            matches!(event.kind(), MarketEventKind::BookSnapshot(_))
                && event.market() == &market("BTC")
        }));
        assert!(batch.events().iter().any(|event| {
            matches!(event.kind(), MarketEventKind::Bbo(_)) && event.market() == &market("BTC")
        }));
        assert!(batch.events().iter().any(|event| {
            matches!(event.kind(), MarketEventKind::Funding(funding)
                if funding.mark_price().is_none()
                    && event.event_time().value() == HOUR_START_MS * 1_000_000)
        }));
        assert!(batch.events().iter().any(|event| {
            matches!(event.kind(), MarketEventKind::Funding(funding)
                if funding.mark_price().is_none()
                    && event.event_time().value() == RECEIPT_NS as i64)
        }));
        assert_eq!(
            batch
                .events()
                .iter()
                .filter(|event| matches!(event.kind(), MarketEventKind::CompletedCandle(_)))
                .count(),
            5
        );
    }

    #[tokio::test]
    async fn capture_rejects_partial_candle_series_without_returning_facts() {
        let (capture, _server) = mounted_capture(true, 1).await;
        assert_eq!(
            capture.capture(&request(), &FixedClock).await,
            Err(ContextCaptureError::IncompleteCandles {
                market: market("BTC"),
                interval: crate::CandleInterval::FifteenMinutes,
            })
        );
    }

    #[tokio::test]
    async fn repeated_current_context_captures_never_materialize_settlement_funding() {
        let (capture, _server) = mounted_capture(false, 2).await;
        for _ in 0..2 {
            let batch = capture
                .capture(&request(), &FixedClock)
                .await
                .expect("complete public context");
            assert!(batch.events().iter().all(|event| {
                !matches!(event.kind(), MarketEventKind::Funding(funding)
                    if event.event_time().value() == RECEIPT_NS as i64
                        && funding.mark_price().is_some())
            }));
        }
    }

    #[tokio::test]
    async fn capture_excludes_delisted_markets_from_detailed_discovery() {
        let mut metadata =
            serde_json::from_str::<Value>(META_FIXTURE).expect("fixture metadata must parse");
        metadata[0]["universe"][3]["isDelisted"] = json!(true);
        let (capture, _server) = mounted_metadata_capture(metadata).await;
        let request = ContextCaptureRequest::new(vec![market("OLD")], range(), range(), range())
            .expect("bounded detailed request");

        assert_eq!(
            capture.capture(&request, &FixedClock).await,
            Err(ContextCaptureError::DetailedMarketNotDiscovered {
                market: market("OLD"),
            })
        );
    }

    #[tokio::test]
    async fn detailed_capture_never_exceeds_the_global_individual_request_cap() {
        const DELAY: Duration = Duration::from_millis(30);
        let server = MockServer::start().await;
        let client = InfoClient::new_loopback_for_test(&format!("{}/info", server.uri()))
            .expect("loopback client");
        let metadata = three_market_metadata();
        Mock::given(method("POST"))
            .and(path("/info"))
            .respond_with(move |request: &Request| {
                let body: Value = serde_json::from_slice(&request.body)
                    .expect("capture request body must remain JSON");
                let response = match body["type"].as_str() {
                    Some("metaAndAssetCtxs") => metadata.clone(),
                    Some("l2Book") => l2_body_for(
                        body["coin"]
                            .as_str()
                            .expect("book request coin must remain present"),
                    ),
                    Some("candleSnapshot") => {
                        let request = &body["req"];
                        let symbol = request["coin"]
                            .as_str()
                            .expect("candle request coin must remain present");
                        match request["interval"].as_str() {
                            Some("15m") => candle_body_for(symbol, "15m", 900_000, 4),
                            Some("1h") => candle_body_for(symbol, "1h", 3_600_000, 1),
                            _ => Value::Null,
                        }
                    }
                    Some("fundingHistory") => json!([{
                        "coin": body["coin"]
                            .as_str()
                            .expect("funding request coin must remain present"),
                        "fundingRate": "0.00001",
                        "premium": "0.00002",
                        "time": HOUR_START_MS,
                    }]),
                    _ => Value::Null,
                };
                ResponseTemplate::new(200)
                    .set_body_json(response)
                    .set_delay(DELAY)
            })
            .expect(13)
            .mount(&server)
            .await;
        let capture = ContextCapture::new(client);
        let request = ContextCaptureRequest::new(
            vec![market("BTC"), market("ETH"), market("SOL")],
            range(),
            range(),
            range(),
        )
        .expect("bounded detailed request");
        let limiter = RequestLimiter::new();

        capture
            .capture_with_limiter(&request, &FixedClock, &limiter)
            .await
            .expect("complete bounded context capture");

        assert_eq!(
            limiter.maximum_observed(),
            MAX_CONTEXT_REQUEST_CONCURRENCY,
            "the test must saturate but never exceed the global operation cap"
        );
    }

    #[test]
    fn request_rejects_duplicate_scope_and_unbounded_detail() {
        assert_eq!(
            ContextCaptureRequest::new(
                vec![market("BTC"), market("BTC")],
                range(),
                range(),
                range(),
            ),
            Err(ContextCaptureError::DuplicateDetailedMarket)
        );
        let markets = (0..=MAX_DETAILED_CONTEXT_MARKETS)
            .map(|index| market(&format!("M{index}")))
            .collect::<Vec<_>>();
        assert!(matches!(
            ContextCaptureRequest::new(markets, range(), range(), range()),
            Err(ContextCaptureError::TooManyDetailedMarkets { .. })
        ));
    }
}
