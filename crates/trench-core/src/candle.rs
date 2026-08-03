//! Deterministic completed candles derived from normalized public trades.

use std::collections::{BTreeMap, BTreeSet};

use rust_decimal::Decimal;
use thiserror::Error;

use crate::domain::{EventId, Market, Price, Quantity, Side};
use crate::event::{
    CandleInterval, CompletedCandle, EventError, MarketEvent, MarketEventKind, TimestampNs,
};

/// One immutable completed candle with its exact normalized-trade input range.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Candle {
    market: Market,
    candle: CompletedCandle,
    first_event_id: EventId,
    last_event_id: EventId,
    first_event_time: TimestampNs,
    last_event_time: TimestampNs,
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
    market: Market,
    side: Side,
    price: Price,
    quantity: Quantity,
}

impl Ord for TradePoint {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.event_time
            .cmp(&other.event_time)
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

/// Stateful deterministic trade-to-candle aggregation for the two supported sleeves.
#[derive(Debug, Default)]
pub struct CandleAggregator {
    seen: BTreeMap<EventId, TradePoint>,
    pending: BTreeMap<BucketKey, Vec<TradePoint>>,
    finalized: BTreeSet<BucketKey>,
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
    /// Duplicate events are an idempotent no-op. A conflicting reuse of a
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
            market: event.market().clone(),
            side: trade.side(),
            price: trade.price(),
            quantity: trade.quantity(),
        };

        if let Some(existing) = self.seen.get(&point.event_id) {
            return if existing == &point {
                Ok(())
            } else {
                Err(CandleError::ConflictingDuplicate {
                    event_id: point.event_id,
                })
            };
        }

        let mut keys = Vec::with_capacity(2);
        for interval in [CandleInterval::FifteenMinutes, CandleInterval::OneHour] {
            keys.push(BucketKey {
                market: point.market.clone(),
                interval,
                open_time: bucket_open(point.event_time, interval)?,
            });
        }
        if let Some(watermark) = self.watermark
            && keys.iter().any(|key| self.finalized.contains(key))
        {
            return Err(CandleError::LateTrade {
                event_time: point.event_time,
                watermark,
            });
        }

        self.seen.insert(point.event_id.clone(), point.clone());
        keys.into_iter().for_each(|key| {
            self.pending.entry(key).or_default().push(point.clone());
        });
        Ok(())
    }

    /// Finalizes each buffered interval whose exchange-time close is at or before `watermark`.
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
        let mut candles = Vec::with_capacity(complete.len());
        for key in complete {
            let trades = self.pending.remove(&key).ok_or(CandleError::Invariant {
                reason: "selected pending bucket must remain present",
            })?;
            let candle = aggregate(key.clone(), trades)?;
            self.finalized.insert(key);
            candles.push(candle);
        }
        self.watermark = Some(watermark);
        Ok(candles)
    }
}

fn bucket_open(time: TimestampNs, interval: CandleInterval) -> Result<TimestampNs, CandleError> {
    let duration = interval.duration().value();
    TimestampNs::new(i128::from(time.value() / duration * duration)).map_err(CandleError::from)
}

fn aggregate(key: BucketKey, mut trades: Vec<TradePoint>) -> Result<Candle, CandleError> {
    trades.sort();
    let first = trades.first().ok_or(CandleError::Invariant {
        reason: "pending candle must have at least one trade",
    })?;
    let last = trades.last().ok_or(CandleError::Invariant {
        reason: "pending candle must have at least one trade",
    })?;
    let mut high = first.price;
    let mut low = first.price;
    let mut volume = Decimal::ZERO;
    let mut buy_notional = Decimal::ZERO;
    let mut sell_notional = Decimal::ZERO;

    for trade in &trades {
        high = high.max(trade.price);
        low = low.min(trade.price);
        volume = volume
            .checked_add(trade.quantity.value())
            .ok_or(CandleError::Arithmetic {
                operation: "candle volume",
            })?;
        let notional = trade
            .price
            .value()
            .checked_mul(trade.quantity.value())
            .ok_or(CandleError::Arithmetic {
                operation: "candle trade notional",
            })?;
        match trade.side {
            Side::Buy => {
                buy_notional =
                    buy_notional
                        .checked_add(notional)
                        .ok_or(CandleError::Arithmetic {
                            operation: "candle buy notional",
                        })?;
            }
            Side::Sell => {
                sell_notional =
                    sell_notional
                        .checked_add(notional)
                        .ok_or(CandleError::Arithmetic {
                            operation: "candle sell notional",
                        })?;
            }
        }
    }
    let volume = Quantity::new(volume).map_err(EventError::from)?;
    let trade_count = u64::try_from(trades.len()).map_err(|_| CandleError::Arithmetic {
        operation: "candle trade count",
    })?;
    let candle = CompletedCandle::new(
        key.interval,
        key.open_time,
        first.price,
        high,
        low,
        last.price,
        volume,
        trade_count,
    )?;
    Ok(Candle {
        market: key.market,
        candle,
        first_event_id: first.event_id.clone(),
        last_event_id: last.event_id.clone(),
        first_event_time: first.event_time,
        last_event_time: last.event_time,
        buy_notional,
        sell_notional,
    })
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use rust_decimal_macros::dec;

    use crate::domain::{Market, Price, Quantity, Side};
    use crate::event::{MarketEvent, TimestampNs, Trade};

    use super::CandleAggregator;

    fn timestamp(value: i128) -> TimestampNs {
        TimestampNs::new(value).expect("test timestamp must be valid")
    }

    fn trade(time: i128, trade_id: u64, price: rust_decimal::Decimal) -> MarketEvent {
        MarketEvent::trade(
            timestamp(time),
            timestamp(time),
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
