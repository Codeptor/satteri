//! Deterministic completed candles derived from normalized public trades.

use std::collections::{BTreeMap, BTreeSet};

use rust_decimal::Decimal;
use thiserror::Error;

use crate::domain::{EventId, Market, Price, Quantity, Side};
use crate::event::{
    CandleInterval, CompletedCandle, EventError, MarketEvent, MarketEventKind, TimestampNs,
};

/// Number of fully finalized trade identities retained for idempotent replay.
///
/// Once this fixed-capacity horizon has filled, an unseen trade for an already
/// finalized interval is rejected as [`CandleError::FinalizedReplayOutsideHorizon`].
pub const FINALIZED_TRADE_ID_HORIZON: usize = 4_096;

/// Maximum concurrently pending markets in one deterministic aggregator.
pub const MAX_PENDING_MARKETS: usize = 128;
/// Maximum unfinalized normalized trades retained for any one market.
///
/// The bound covers the longest open one-hour bucket. It is deliberately
/// provisioned independently per market so a deep market cannot consume the
/// entire universe's deduplication budget before the watermark advances.
pub const MAX_PENDING_TRADES_PER_MARKET: usize = 16_384;

/// One immutable completed candle with its exact normalized-trade input range.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Candle {
    market: Market,
    candle: CompletedCandle,
    first_event_id: EventId,
    last_event_id: EventId,
    first_event_time: TimestampNs,
    last_event_time: TimestampNs,
    source_available_at: TimestampNs,
    buy_notional: Decimal,
    sell_notional: Decimal,
}

impl Candle {
    /// Returns the market whose trades formed this candle.
    #[must_use]
    pub const fn market(&self) -> &Market {
        &self.market
    }

    /// Returns the immutable OHLCV candle.
    #[must_use]
    pub const fn candle(&self) -> &CompletedCandle {
        &self.candle
    }

    /// Returns the first canonical normalized-trade identity included.
    #[must_use]
    pub const fn first_event_id(&self) -> &EventId {
        &self.first_event_id
    }

    /// Returns the final canonical normalized-trade identity included.
    #[must_use]
    pub const fn last_event_id(&self) -> &EventId {
        &self.last_event_id
    }

    /// Returns the first contributing authoritative exchange time.
    #[must_use]
    pub const fn first_event_time(&self) -> TimestampNs {
        self.first_event_time
    }

    /// Returns the final contributing authoritative exchange time.
    #[must_use]
    pub const fn last_event_time(&self) -> TimestampNs {
        self.last_event_time
    }

    /// Returns the latest receipt time among this candle's contributing trades.
    #[must_use]
    pub const fn source_available_at(&self) -> TimestampNs {
        self.source_available_at
    }

    /// Returns aggressive-buy quote notional from the completed interval.
    #[must_use]
    pub const fn buy_notional(&self) -> Decimal {
        self.buy_notional
    }

    /// Returns aggressive-sell quote notional from the completed interval.
    #[must_use]
    pub const fn sell_notional(&self) -> Decimal {
        self.sell_notional
    }

    /// Returns the close time used as the point-in-time decision boundary.
    ///
    /// # Errors
    ///
    /// Returns an event error only if the bounded timestamp cannot advance by
    /// this candle's declared interval.
    pub fn close_time(&self) -> Result<TimestampNs, EventError> {
        self.candle
            .open_time()
            .checked_add(self.candle.interval().duration())
    }
}

/// Candle aggregation failure.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CandleError {
    /// A non-trade event was supplied to the trade-only aggregator.
    #[error("candle aggregation requires a normalized trade")]
    ExpectedTrade,
    /// A distinct payload attempted to reuse a canonical normalized-trade ID.
    #[error("conflicting duplicate trade identity {event_id:?}")]
    ConflictingDuplicate {
        /// Reused canonical trade identity.
        event_id: EventId,
    },
    /// A duplicate trade identity changed its receipt time, which would change
    /// point-in-time availability and feature provenance.
    #[error(
        "duplicate trade identity {event_id:?} changed receipt time from {existing_received_at} to {received_at}"
    )]
    ConflictingDuplicateReceiptTime {
        /// Reused canonical trade identity.
        event_id: EventId,
        /// Receipt time already bound to the identity.
        existing_received_at: TimestampNs,
        /// Receipt time supplied by the conflicting replay.
        received_at: TimestampNs,
    },
    /// One market exhausted its explicitly provisioned pending-trade buffer.
    #[error("pending trade capacity {limit} reached for market {market:?} before finalization")]
    PendingTradeCapacity {
        /// Market whose independent capacity was exhausted.
        market: Market,
        /// Maximum unique pending trades for that market across both sleeves.
        limit: usize,
    },
    /// A new market would exceed the finite pending-market capacity.
    #[error("pending market capacity {limit} reached before finalization")]
    PendingMarketCapacity {
        /// Maximum concurrently pending markets.
        limit: usize,
    },
    /// A replay targeted a finalized interval after its bounded identity horizon expired.
    #[error("finalized trade identity {event_id:?} is outside retained replay horizon {limit}")]
    FinalizedReplayOutsideHorizon {
        /// Replayed canonical trade identity.
        event_id: EventId,
        /// Number of finalized identities retained for idempotent replay.
        limit: usize,
    },
    /// A trade arrived after the caller finalized its enclosing interval.
    #[error("trade at {event_time} is older than finalized watermark {watermark}")]
    LateTrade {
        /// Rejected authoritative trade time.
        event_time: TimestampNs,
        /// Last successful finalization watermark.
        watermark: TimestampNs,
    },
    /// A caller attempted to move the exchange-time finalization watermark backwards.
    #[error("watermark {current} is before prior watermark {previous}")]
    BackwardWatermark {
        /// Previous successful watermark.
        previous: TimestampNs,
        /// Requested earlier watermark.
        current: TimestampNs,
    },
    /// Exact decimal aggregation overflowed.
    #[error("checked decimal arithmetic failed while calculating {operation}")]
    Arithmetic {
        /// Failed calculation.
        operation: &'static str,
    },
    /// An internal buffered-candle invariant was violated.
    #[error("candle aggregation invariant failed: {reason}")]
    Invariant {
        /// Failed invariant description.
        reason: &'static str,
    },
    /// Derived candle validation or timestamp arithmetic failed.
    #[error(transparent)]
    Event(#[from] EventError),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TradePoint {
    event_id: EventId,
    event_time: TimestampNs,
    received_at: TimestampNs,
    market: Market,
    side: Side,
    price: Price,
    quantity: Quantity,
}

impl Ord for TradePoint {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.event_time
            .cmp(&other.event_time)
            .then_with(|| self.received_at.cmp(&other.received_at))
            .then_with(|| self.event_id.cmp(&other.event_id))
    }
}

impl PartialOrd for TradePoint {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct BucketKey {
    market: Market,
    interval: CandleInterval,
    open_time: TimestampNs,
}

/// Running exact aggregate for one nonempty candle bucket.
///
/// Only the two order-defining trades remain as full records. Every other
/// trade is represented by the bounded global identity map until its one-hour
/// bucket finalizes, avoiding a second full copy in each sleeve bucket.
#[derive(Debug, Clone, PartialEq, Eq)]
struct PendingCandle {
    first: TradePoint,
    last: TradePoint,
    high: Price,
    low: Price,
    source_available_at: TimestampNs,
    volume: Decimal,
    buy_notional: Decimal,
    sell_notional: Decimal,
    trade_count: u64,
    arithmetic_error: Option<&'static str>,
}

impl PendingCandle {
    fn from_trade(trade: &TradePoint) -> Self {
        let notional = trade.price.value().checked_mul(trade.quantity.value());
        let (buy_notional, sell_notional) = match (trade.side, notional) {
            (Side::Buy, Some(notional)) => (notional, Decimal::ZERO),
            (Side::Sell, Some(notional)) => (Decimal::ZERO, notional),
            (_, None) => (Decimal::ZERO, Decimal::ZERO),
        };
        Self {
            first: trade.clone(),
            last: trade.clone(),
            high: trade.price,
            low: trade.price,
            source_available_at: trade.received_at,
            volume: trade.quantity.value(),
            buy_notional,
            sell_notional,
            trade_count: 1,
            arithmetic_error: notional.is_none().then_some("candle trade notional"),
        }
    }

    fn with_trade(&self, trade: &TradePoint) -> Self {
        let mut next = Self {
            first: self.first.clone().min(trade.clone()),
            last: self.last.clone().max(trade.clone()),
            high: self.high.max(trade.price),
            low: self.low.min(trade.price),
            source_available_at: self.source_available_at.max(trade.received_at),
            volume: self.volume,
            buy_notional: self.buy_notional,
            sell_notional: self.sell_notional,
            trade_count: self.trade_count,
            arithmetic_error: self.arithmetic_error,
        };
        if next.arithmetic_error.is_some() {
            return next;
        }
        let Some(notional) = trade.price.value().checked_mul(trade.quantity.value()) else {
            next.arithmetic_error = Some("candle trade notional");
            return next;
        };
        let Some(volume) = next.volume.checked_add(trade.quantity.value()) else {
            next.arithmetic_error = Some("candle volume");
            return next;
        };
        let (buy_notional, sell_notional) = match trade.side {
            Side::Buy => (
                next.buy_notional.checked_add(notional),
                Some(next.sell_notional),
            ),
            Side::Sell => (
                Some(next.buy_notional),
                next.sell_notional.checked_add(notional),
            ),
        };
        let Some(buy_notional) = buy_notional else {
            next.arithmetic_error = Some("candle buy notional");
            return next;
        };
        let Some(sell_notional) = sell_notional else {
            next.arithmetic_error = Some("candle sell notional");
            return next;
        };
        let Some(trade_count) = next.trade_count.checked_add(1) else {
            next.arithmetic_error = Some("candle trade count");
            return next;
        };
        next.volume = volume;
        next.buy_notional = buy_notional;
        next.sell_notional = sell_notional;
        next.trade_count = trade_count;
        next
    }

    fn candle(&self, key: &BucketKey) -> Result<Candle, CandleError> {
        if let Some(operation) = self.arithmetic_error {
            return Err(CandleError::Arithmetic { operation });
        }
        let candle = CompletedCandle::new(
            key.interval,
            key.open_time,
            self.first.price,
            self.high,
            self.low,
            self.last.price,
            Quantity::new(self.volume).map_err(EventError::from)?,
            self.trade_count,
        )?;
        Ok(Candle {
            market: key.market.clone(),
            candle,
            first_event_id: self.first.event_id.clone(),
            last_event_id: self.last.event_id.clone(),
            first_event_time: self.first.event_time,
            last_event_time: self.last.event_time,
            source_available_at: self.source_available_at,
            buy_notional: self.buy_notional,
            sell_notional: self.sell_notional,
        })
    }
}

/// Stateful deterministic trade-to-candle aggregation for the two supported sleeves.
///
/// Pending identities remain idempotent until both candle sleeves close. Fully
/// finalized identities remain idempotent for
/// [`FINALIZED_TRADE_ID_HORIZON`] canonical ordering positions; a replay after
/// that finite horizon fails closed instead of being accepted as a new late trade.
#[derive(Debug, Default)]
pub struct CandleAggregator {
    seen: BTreeMap<EventId, TradePoint>,
    pending_counts: BTreeMap<Market, usize>,
    finalized: BTreeMap<EventId, TradePoint>,
    finalized_order: BTreeMap<(TimestampNs, TimestampNs, EventId), EventId>,
    pending: BTreeMap<BucketKey, PendingCandle>,
    hourly_trade_ids: BTreeMap<BucketKey, BTreeSet<EventId>>,
    watermark: Option<TimestampNs>,
}

impl CandleAggregator {
    /// Creates an empty aggregator with independent market and sleeve state.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Buffers one canonical trade for both 15-minute and one-hour intervals.
    ///
    /// Duplicate events are an idempotent no-op while their identity is pending
    /// or within [`FINALIZED_TRADE_ID_HORIZON`]. A conflicting reuse of a
    /// canonical identity or a trade after its interval was finalized fails
    /// closed instead of mutating an immutable candle.
    ///
    /// # Errors
    ///
    /// Returns [`CandleError`] for non-trades, invalid arrival order, duplicate
    /// conflicts, or checked arithmetic failures.
    pub fn ingest(&mut self, event: &MarketEvent) -> Result<(), CandleError> {
        let MarketEventKind::Trade(trade) = event.kind() else {
            return Err(CandleError::ExpectedTrade);
        };
        let point = TradePoint {
            event_id: event.event_id().clone(),
            event_time: event.event_time(),
            received_at: event.received_at(),
            market: event.market().clone(),
            side: trade.side(),
            price: trade.price(),
            quantity: trade.quantity(),
        };

        if let Some(existing) = self.seen.get(&point.event_id) {
            return duplicate_result(existing, &point);
        }
        if let Some(existing) = self.finalized.get(&point.event_id) {
            return duplicate_result(existing, &point);
        }

        let pending_for_market = self
            .pending_counts
            .get(&point.market)
            .copied()
            .unwrap_or_default();
        if pending_for_market == 0 && self.pending_counts.len() == MAX_PENDING_MARKETS {
            return Err(CandleError::PendingMarketCapacity {
                limit: MAX_PENDING_MARKETS,
            });
        }
        if pending_for_market == MAX_PENDING_TRADES_PER_MARKET {
            return Err(CandleError::PendingTradeCapacity {
                market: point.market.clone(),
                limit: MAX_PENDING_TRADES_PER_MARKET,
            });
        }

        let mut keys = Vec::with_capacity(2);
        for interval in [CandleInterval::FifteenMinutes, CandleInterval::OneHour] {
            keys.push(BucketKey {
                market: point.market.clone(),
                interval,
                open_time: bucket_open(point.event_time, interval)?,
            });
        }
        if let Some(watermark) = self.watermark {
            for key in &keys {
                let close = key
                    .open_time
                    .checked_add(key.interval.duration())
                    .map_err(CandleError::from)?;
                if close <= watermark {
                    if self.finalized.len() == FINALIZED_TRADE_ID_HORIZON {
                        return Err(CandleError::FinalizedReplayOutsideHorizon {
                            event_id: point.event_id,
                            limit: FINALIZED_TRADE_ID_HORIZON,
                        });
                    }
                    return Err(CandleError::LateTrade {
                        event_time: point.event_time,
                        watermark,
                    });
                }
            }
        }

        let accumulated = keys
            .iter()
            .map(|key| match self.pending.get(key) {
                Some(existing) => existing.with_trade(&point),
                None => PendingCandle::from_trade(&point),
            })
            .collect::<Vec<_>>();
        self.seen.insert(point.event_id.clone(), point.clone());
        self.pending_counts
            .entry(point.market.clone())
            .and_modify(|count| *count += 1)
            .or_insert(1);
        for (key, candle) in keys.into_iter().zip(accumulated) {
            if key.interval == CandleInterval::OneHour {
                self.hourly_trade_ids
                    .entry(key.clone())
                    .or_default()
                    .insert(point.event_id.clone());
            }
            self.pending.insert(key, candle);
        }
        Ok(())
    }

    /// Closes every interval whose exchange-time close is at or before `watermark`.
    ///
    /// Only nonempty intervals emit a candle, but elapsed empty intervals are
    /// closed too: a later trade for either kind of interval is rejected.
    ///
    /// The caller owns this explicit watermark. This makes replays deterministic:
    /// any ordering of a complete batch can be ingested before advancing it.
    ///
    /// # Errors
    ///
    /// Returns [`CandleError`] when the watermark moves backwards or exact
    /// candle aggregation cannot be represented.
    pub fn complete_through(&mut self, watermark: TimestampNs) -> Result<Vec<Candle>, CandleError> {
        if let Some(previous) = self.watermark
            && watermark < previous
        {
            return Err(CandleError::BackwardWatermark {
                previous,
                current: watermark,
            });
        }

        let complete = self
            .pending
            .keys()
            .filter(|key| {
                key.open_time
                    .checked_add(key.interval.duration())
                    .is_ok_and(|close| close <= watermark)
            })
            .cloned()
            .collect::<Vec<_>>();
        let candles = complete
            .iter()
            .map(|key| {
                let pending = self.pending.get(key).ok_or(CandleError::Invariant {
                    reason: "selected pending bucket must remain present",
                })?;
                pending.candle(key)
            })
            .collect::<Result<Vec<_>, _>>()?;
        for key in complete {
            self.pending.remove(&key).ok_or(CandleError::Invariant {
                reason: "selected pending bucket must remain present",
            })?;
            if key.interval == CandleInterval::OneHour {
                let trade_ids =
                    self.hourly_trade_ids
                        .remove(&key)
                        .ok_or(CandleError::Invariant {
                            reason: "selected hourly bucket must retain its trade identities",
                        })?;
                for event_id in trade_ids {
                    let trade = self.seen.remove(&event_id).ok_or(CandleError::Invariant {
                        reason: "pending hourly trade identity must remain present",
                    })?;
                    let remove_market = {
                        let count = self.pending_counts.get_mut(&trade.market).ok_or(
                            CandleError::Invariant {
                                reason: "pending trade market count must remain present",
                            },
                        )?;
                        *count = count.checked_sub(1).ok_or(CandleError::Invariant {
                            reason: "pending trade market count must not underflow",
                        })?;
                        *count == 0
                    };
                    if remove_market {
                        self.pending_counts.remove(&trade.market);
                    }
                    self.record_finalized(trade);
                }
            }
        }
        self.watermark = Some(watermark);
        Ok(candles)
    }

    fn record_finalized(&mut self, trade: TradePoint) {
        let order_key = (trade.event_time, trade.received_at, trade.event_id.clone());
        self.finalized_order
            .insert(order_key, trade.event_id.clone());
        self.finalized.insert(trade.event_id.clone(), trade);
        while self.finalized.len() > FINALIZED_TRADE_ID_HORIZON {
            if let Some((_, event_id)) = self.finalized_order.pop_first() {
                self.finalized.remove(&event_id);
            }
        }
    }
}

fn duplicate_result(existing: &TradePoint, incoming: &TradePoint) -> Result<(), CandleError> {
    if existing == incoming {
        return Ok(());
    }
    if existing.received_at != incoming.received_at {
        return Err(CandleError::ConflictingDuplicateReceiptTime {
            event_id: incoming.event_id.clone(),
            existing_received_at: existing.received_at,
            received_at: incoming.received_at,
        });
    }
    Err(CandleError::ConflictingDuplicate {
        event_id: incoming.event_id.clone(),
    })
}

fn bucket_open(time: TimestampNs, interval: CandleInterval) -> Result<TimestampNs, CandleError> {
    let duration = interval.duration().value();
    TimestampNs::new(i128::from(time.value() / duration * duration)).map_err(CandleError::from)
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use rust_decimal_macros::dec;

    use crate::domain::{Market, Price, Quantity, Side};
    use crate::event::{CandleInterval, MarketEvent, TimestampNs, Trade};

    use super::{CandleAggregator, CandleError};

    fn timestamp(value: i128) -> TimestampNs {
        TimestampNs::new(value).expect("test timestamp must be valid")
    }

    fn trade(time: i128, trade_id: u64, price: rust_decimal::Decimal) -> MarketEvent {
        trade_with_receipt(time, time, trade_id, price)
    }

    fn trade_with_receipt(
        event_time: i128,
        received_at: i128,
        trade_id: u64,
        price: rust_decimal::Decimal,
    ) -> MarketEvent {
        MarketEvent::trade(
            timestamp(event_time),
            timestamp(received_at),
            Market::new("BTC").expect("test market must be valid"),
            Trade::new(
                trade_id,
                Side::Buy,
                Price::new(price).expect("test price must be valid"),
                Quantity::new(dec!(1)).expect("test quantity must be valid"),
            )
            .expect("test trade must be valid"),
        )
        .expect("test event must be valid")
    }

    #[test]
    fn earlier_candle_emits_once_only_after_its_close() {
        let mut aggregator = CandleAggregator::new();
        aggregator
            .ingest(&trade(1, 1, dec!(100)))
            .expect("trade must be accepted");
        aggregator
            .ingest(&trade(900_000_000_001, 2, dec!(101)))
            .expect("next interval trade must be accepted");

        assert!(
            aggregator
                .complete_through(timestamp(899_999_999_999))
                .expect("watermark must be valid")
                .is_empty()
        );

        let candles = aggregator
            .complete_through(timestamp(900_000_000_000))
            .expect("watermark must be valid");
        assert_eq!(candles.len(), 1, "only the earlier interval may close");
        assert_eq!(candles[0].candle().open().value(), dec!(100));
        assert!(
            aggregator
                .complete_through(timestamp(900_000_000_000))
                .expect("watermark must be valid")
                .is_empty()
        );
    }

    #[test]
    fn reordered_duplicate_trades_produce_identical_candles() {
        let trades = [
            trade(2, 2, dec!(101)),
            trade(1, 1, dec!(100)),
            trade(3, 3, dec!(99)),
            trade(2, 2, dec!(101)),
        ];
        let mut first = CandleAggregator::new();
        let mut second = CandleAggregator::new();

        for event in &trades {
            first.ingest(event).expect("trade must be accepted");
        }
        for event in trades.iter().rev() {
            second.ingest(event).expect("trade must be accepted");
        }

        let close = timestamp(900_000_000_000);
        assert_eq!(
            first
                .complete_through(close)
                .expect("watermark must be valid"),
            second
                .complete_through(close)
                .expect("watermark must be valid")
        );
    }

    #[test]
    fn rejects_a_late_trade_for_an_empty_interval_closed_by_the_watermark() {
        let mut aggregator = CandleAggregator::new();
        aggregator
            .ingest(&trade(1, 1, dec!(100)))
            .expect("initial trade must be accepted");
        aggregator
            .complete_through(timestamp(1_800_000_000_000))
            .expect("watermark must finalize elapsed intervals");

        let late_trade = trade(900_000_000_001, 2, dec!(101));
        assert!(matches!(
            aggregator.ingest(&late_trade),
            Err(CandleError::LateTrade { .. })
        ));
        assert!(
            aggregator
                .complete_through(timestamp(1_800_000_000_000))
                .expect("unchanged watermark must be valid")
                .is_empty(),
            "a rejected late trade must not produce a retroactive candle"
        );
    }

    #[test]
    fn failed_finalization_leaves_every_pending_bucket_available_for_retry() {
        let mut aggregator = CandleAggregator::new();
        aggregator
            .ingest(&trade(1, 1, dec!(100)))
            .expect("normal trade must be accepted");
        aggregator
            .ingest(&trade(900_000_000_001, 2, rust_decimal::Decimal::MAX))
            .expect("first maximal trade must be accepted");
        aggregator
            .ingest(&trade(900_000_000_002, 3, rust_decimal::Decimal::MAX))
            .expect("second maximal trade must be accepted");
        let pending_before = aggregator.pending.clone();
        let seen_before = aggregator.seen.clone();

        assert_eq!(
            aggregator.complete_through(timestamp(1_800_000_000_000)),
            Err(CandleError::Arithmetic {
                operation: "candle buy notional"
            })
        );
        assert_eq!(aggregator.pending, pending_before);
        assert_eq!(aggregator.seen, seen_before);
        assert_eq!(aggregator.watermark, None);
    }

    #[test]
    fn completed_candle_records_the_latest_contributing_receipt_time() {
        let mut aggregator = CandleAggregator::new();
        aggregator
            .ingest(&trade_with_receipt(1, 500, 1, dec!(100)))
            .expect("trade must be accepted");

        let candle = aggregator
            .complete_through(timestamp(900_000_000_000))
            .expect("watermark must finalize the candle")
            .into_iter()
            .find(|candle| candle.candle().interval() == CandleInterval::FifteenMinutes)
            .expect("fifteen-minute candle must exist");
        assert_eq!(candle.source_available_at(), timestamp(500));
    }

    #[test]
    fn same_exchange_time_uses_receipt_time_before_trade_identity_for_ohlc_order() {
        let (earlier_receipt, later_receipt) = (1_u64..64)
            .flat_map(|early_id| ((early_id + 1)..64).map(move |late_id| (early_id, late_id)))
            .find_map(|(early_id, late_id)| {
                let early = trade_with_receipt(1, 10, early_id, dec!(100));
                let late = trade_with_receipt(1, 20, late_id, dec!(200));
                (early.event_id() > late.event_id()).then_some((early, late))
            })
            .expect("test identities must include an order opposite to receipt time");
        assert!(earlier_receipt.event_id() > later_receipt.event_id());

        let mut aggregator = CandleAggregator::new();
        aggregator
            .ingest(&later_receipt)
            .expect("later trade must be accepted");
        aggregator
            .ingest(&earlier_receipt)
            .expect("earlier trade must be accepted");

        let candle = aggregator
            .complete_through(timestamp(900_000_000_000))
            .expect("watermark must finalize the candle")
            .into_iter()
            .find(|candle| candle.candle().interval() == CandleInterval::FifteenMinutes)
            .expect("fifteen-minute candle must exist");
        assert_eq!(candle.candle().open().value(), dec!(100));
        assert_eq!(candle.candle().close().value(), dec!(200));
    }

    #[test]
    fn rejects_a_duplicate_identity_with_a_different_receipt_time() {
        let mut aggregator = CandleAggregator::new();
        let first = trade_with_receipt(1, 1, 1, dec!(100));
        aggregator
            .ingest(&first)
            .expect("initial trade must be accepted");

        assert!(matches!(
            aggregator.ingest(&trade_with_receipt(1, 2, 1, dec!(100))),
            Err(CandleError::ConflictingDuplicateReceiptTime {
                event_id,
                existing_received_at,
                received_at,
            }) if event_id == *first.event_id()
                && existing_received_at == timestamp(1)
                && received_at == timestamp(2)
        ));
    }

    #[test]
    fn finalization_moves_deduplication_to_the_bounded_finalized_horizon() {
        let mut aggregator = CandleAggregator::new();
        aggregator
            .ingest(&trade(1, 1, dec!(100)))
            .expect("trade must be accepted");
        aggregator
            .complete_through(timestamp(3_600_000_000_000))
            .expect("watermark must finalize both candle intervals");

        assert!(aggregator.seen.is_empty());
        assert_eq!(aggregator.finalized.len(), 1);
    }

    #[test]
    fn finalized_trade_identity_is_idempotent_within_the_declared_horizon() {
        let mut aggregator = CandleAggregator::new();
        let event = trade(1, 1, dec!(100));
        aggregator.ingest(&event).expect("trade must be accepted");
        aggregator
            .complete_through(timestamp(3_600_000_000_000))
            .expect("watermark must finalize the trade");

        assert_eq!(aggregator.ingest(&event), Ok(()));
    }

    #[test]
    fn finalized_trade_identity_rejects_a_changed_receipt_time_within_the_horizon() {
        let mut aggregator = CandleAggregator::new();
        let event = trade_with_receipt(1, 1, 1, dec!(100));
        aggregator.ingest(&event).expect("trade must be accepted");
        aggregator
            .complete_through(timestamp(3_600_000_000_000))
            .expect("watermark must finalize the trade");

        assert!(matches!(
            aggregator.ingest(&trade_with_receipt(1, 2, 1, dec!(100))),
            Err(CandleError::ConflictingDuplicateReceiptTime { .. })
        ));
    }

    #[test]
    fn replay_beyond_the_finalized_identity_horizon_is_not_reported_as_late() {
        let mut aggregator = CandleAggregator::new();
        let first_batch = (1..=super::FINALIZED_TRADE_ID_HORIZON)
            .map(|trade_id| trade(1, trade_id as u64, dec!(100)))
            .collect::<Vec<_>>();
        let expired = first_batch
            .iter()
            .min_by_key(|event| event.event_id())
            .expect("first batch must not be empty")
            .clone();
        for event in &first_batch {
            aggregator.ingest(event).expect("trade must be accepted");
        }
        aggregator
            .complete_through(timestamp(3_600_000_000_000))
            .expect("watermark must finalize the first batch");
        let next = trade(
            3_600_000_000_001,
            super::FINALIZED_TRADE_ID_HORIZON as u64 + 1,
            dec!(100),
        );
        aggregator
            .ingest(&next)
            .expect("next trade must be accepted");
        aggregator
            .complete_through(timestamp(7_200_000_000_000))
            .expect("watermark must finalize the next trade");
        assert_eq!(
            aggregator.finalized.len(),
            super::FINALIZED_TRADE_ID_HORIZON
        );
        assert_eq!(
            aggregator.finalized_order.len(),
            super::FINALIZED_TRADE_ID_HORIZON
        );

        let error = aggregator
            .ingest(&expired)
            .expect_err("expired finalized identity must not be accepted");
        assert!(matches!(
            error,
            CandleError::FinalizedReplayOutsideHorizon {
                limit: super::FINALIZED_TRADE_ID_HORIZON,
                ..
            }
        ));
    }

    #[test]
    fn refuses_new_trades_when_the_unfinalized_buffer_reaches_its_capacity() {
        let mut aggregator = CandleAggregator::new();
        for trade_id in 0..super::MAX_PENDING_TRADES_PER_MARKET {
            aggregator
                .ingest(&trade(1, trade_id as u64 + 1, dec!(100)))
                .expect("trade within the pending capacity must be accepted");
        }

        assert_eq!(
            aggregator.ingest(&trade(
                1,
                super::MAX_PENDING_TRADES_PER_MARKET as u64 + 1,
                dec!(100),
            )),
            Err(CandleError::PendingTradeCapacity {
                market: Market::new("BTC").expect("test market must be valid"),
                limit: super::MAX_PENDING_TRADES_PER_MARKET,
            })
        );
    }

    #[test]
    fn twenty_deep_markets_do_not_share_a_single_pending_trade_budget() {
        let mut aggregator = CandleAggregator::new();
        let per_market = super::MAX_PENDING_TRADES_PER_MARKET.min(4_097);

        for market_index in 0..20_u64 {
            let market =
                Market::new(format!("M{market_index}")).expect("test market must be valid");
            for trade_id in 0..per_market {
                let event = MarketEvent::trade(
                    timestamp(1),
                    timestamp(1),
                    market.clone(),
                    Trade::new(
                        trade_id as u64 + 1,
                        Side::Buy,
                        Price::new(dec!(100)).expect("test price must be valid"),
                        Quantity::new(dec!(1)).expect("test quantity must be valid"),
                    )
                    .expect("test trade must be valid"),
                )
                .expect("test event must be valid");
                aggregator
                    .ingest(&event)
                    .expect("each market receives its independently provisioned pending budget");
            }
        }

        let candles = aggregator
            .complete_through(timestamp(900_000_000_000))
            .expect("watermark must be valid");
        assert_eq!(
            candles
                .iter()
                .filter(|candle| candle.candle().interval() == CandleInterval::FifteenMinutes)
                .count(),
            20
        );
    }

    proptest! {
        #[test]
        fn arbitrary_duplicate_trade_replay_is_order_independent(
            points in prop::collection::vec((0_i64..900_000_000_000_i64, 1_i64..10_000_i64), 1..32)
        ) {
            let events = points
                .into_iter()
                .enumerate()
                .map(|(index, (time, price))| trade(i128::from(time), index as u64 + 1, rust_decimal::Decimal::from(price)))
                .collect::<Vec<_>>();
            let mut forward = CandleAggregator::new();
            let mut reverse_with_duplicates = CandleAggregator::new();

            for event in &events {
                forward.ingest(event).expect("generated trade must be accepted");
            }
            for event in events.iter().rev() {
                reverse_with_duplicates.ingest(event).expect("generated trade must be accepted");
                reverse_with_duplicates.ingest(event).expect("duplicate must be idempotent");
            }

            let close = timestamp(3_600_000_000_000);
            prop_assert_eq!(
                forward.complete_through(close).expect("watermark must be valid"),
                reverse_with_duplicates.complete_through(close).expect("watermark must be valid")
            );
        }
    }
}
