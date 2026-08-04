//! Bounded public-data evidence production for queued market-data gaps.
//!
//! [`RecoveryEvidenceProducer`] owns no storage writer, readiness state, or
//! execution path. The authority loop retains a normalized trade only after
//! its source fact is durably accepted, queues the immutable request emitted
//! by the WebSocket, and routes the resulting [`RecoveryResult`] back through
//! its single writer path. A reconnect or fresh L2 snapshot is never recovery
//! evidence by itself.

use std::collections::BTreeMap;

use thiserror::Error;
use tokio_util::sync::CancellationToken;
use trench_core::candle::CandleAggregator;
use trench_core::domain::{EventId, Market};
use trench_core::event::{CandleInterval as CoreCandleInterval, MarketEvent, MarketEventKind};

use crate::info::{CandleInterval, InfoClient, TimeRange};
use crate::recovery::{
    GapRecovery, GapRecoveryRequest, MAX_RECOVERY_LOCAL_TRADES, MAX_RECOVERY_OFFICIAL_CANDLES,
    RecoveryError, RecoveryEvidence, RecoveryResult, RecoveryUnavailable,
};

const NANOS_PER_MILLISECOND: i64 = 1_000_000;

/// Maximum durably admitted normalized trades retained for one market before
/// the authority must start a new source-replay epoch rather than drop facts.
pub const MAX_RETAINED_RECOVERY_TRADES_PER_MARKET: usize = MAX_RECOVERY_LOCAL_TRADES;

/// Produces explicit FIFO recovery results from durable local trade facts and
/// the documented public candle endpoint.
#[derive(Debug)]
pub struct RecoveryEvidenceProducer {
    info: InfoClient,
    recovery: GapRecovery,
    retained_trades: BTreeMap<Market, Vec<MarketEvent>>,
}

impl RecoveryEvidenceProducer {
    /// Creates a bounded recovery producer over one immutable read-only info
    /// client.
    #[must_use]
    pub fn new(info: InfoClient) -> Self {
        Self {
            info,
            recovery: GapRecovery::new(),
            retained_trades: BTreeMap::new(),
        }
    }

    /// Returns the number of queued gap requests awaiting a final result.
    #[must_use]
    pub fn pending_len(&self) -> usize {
        self.recovery.pending_len()
    }

    /// Retains one source event already committed by the authority writer.
    ///
    /// Only normalized public trades are relevant to candle reconciliation;
    /// every other market fact is ignored. The caller must invoke this only
    /// after its single writer has durably accepted the original source fact.
    /// The producer never accepts an evicted subset as evidence: the fixed
    /// per-market bound fails closed before dropping any retained trade.
    ///
    /// # Errors
    ///
    /// Returns [`RecoveryProducerError`] when a canonical trade identity is
    /// reused with different immutable data or the bounded retained window is
    /// full.
    pub fn retain_committed_source_event(
        &mut self,
        event: &MarketEvent,
    ) -> Result<(), RecoveryProducerError> {
        if !matches!(event.kind(), MarketEventKind::Trade(_)) {
            return Ok(());
        }
        let market = event.market().clone();
        let events = self.retained_trades.entry(market.clone()).or_default();
        if let Some(existing) = events
            .iter()
            .find(|existing| existing.event_id() == event.event_id())
        {
            return if existing == event {
                Ok(())
            } else {
                Err(RecoveryProducerError::ConflictingRetainedTrade {
                    event_id: event.event_id().clone(),
                })
            };
        }
        if events.len() == MAX_RETAINED_RECOVERY_TRADES_PER_MARKET {
            return Err(RecoveryProducerError::RetainedTradeCapacity {
                market,
                limit: MAX_RETAINED_RECOVERY_TRADES_PER_MARKET,
            });
        }
        events.push(event.clone());
        Ok(())
    }

    /// Queues one WebSocket-delivered recovery request in strict FIFO order.
    ///
    /// # Errors
    ///
    /// Returns [`RecoveryError`] for an invalid, duplicated, nonmonotonic, or
    /// over-capacity request. The request is not retained on failure.
    pub fn enqueue(&mut self, request: GapRecoveryRequest) -> Result<(), RecoveryError> {
        self.recovery.enqueue(request)
    }

    /// Produces a result for only the oldest pending request.
    ///
    /// The two official candle reads are scoped to the exact closed interval
    /// implied by the request. A network, decoding, capacity, or time-range
    /// failure becomes a conservative unavailable result; it never grants
    /// continuity. Cancellation leaves the queue head and candle state
    /// untouched so the caller can retry the same request in order.
    ///
    /// # Errors
    ///
    /// Returns [`RecoveryProducerError::Cancelled`] without dequeuing work, or
    /// propagates a core reconciliation failure without mutating queue order.
    pub async fn process_next(
        &mut self,
        candles: &mut CandleAggregator,
        cancellation: &CancellationToken,
    ) -> Result<Option<RecoveryResult>, RecoveryProducerError> {
        if cancellation.is_cancelled() {
            return Err(RecoveryProducerError::Cancelled);
        }
        let Some(request) = self.recovery.next_request().cloned() else {
            return Ok(None);
        };
        let local_trades = self.local_trades_for(&request);
        let Some(ranges) = CandleRanges::from_request(&request) else {
            return self
                .recovery
                .process_next(
                    RecoveryEvidence::Reconciled {
                        local_trades: &local_trades,
                        official_candles: &[],
                    },
                    candles,
                )
                .map_err(Into::into);
        };
        let official_candles = match self
            .fetch_official_candles(&request, ranges, cancellation)
            .await
        {
            Ok(candles) => candles,
            Err(RecoveryProducerError::Cancelled) => return Err(RecoveryProducerError::Cancelled),
            Err(RecoveryProducerError::OfficialCandleEvidenceUnavailable) => {
                return self
                    .recovery
                    .process_next(
                        RecoveryEvidence::Unavailable {
                            reason: RecoveryUnavailable::OfficialCandleEvidenceUnavailable,
                        },
                        candles,
                    )
                    .map_err(Into::into);
            }
            Err(error) => return Err(error),
        };
        self.recovery
            .process_next(
                RecoveryEvidence::Reconciled {
                    local_trades: &local_trades,
                    official_candles: &official_candles,
                },
                candles,
            )
            .map_err(Into::into)
    }

    fn local_trades_for(&self, request: &GapRecoveryRequest) -> Vec<MarketEvent> {
        let mut trades = self
            .retained_trades
            .get(request.market())
            .into_iter()
            .flatten()
            .filter(|event| is_within_request_trade_window(request, event))
            .cloned()
            .collect::<Vec<_>>();
        trades.sort_by(canonical_trade_order);
        trades
    }

    async fn fetch_official_candles(
        &self,
        request: &GapRecoveryRequest,
        ranges: CandleRanges,
        cancellation: &CancellationToken,
    ) -> Result<Vec<crate::normalize::Candle>, RecoveryProducerError> {
        let fetch = async {
            let fifteen_minutes = self.info.candle_snapshot(
                request.market(),
                CandleInterval::FifteenMinutes,
                ranges.fifteen_minutes,
            );
            let one_hour = self.info.candle_snapshot(
                request.market(),
                CandleInterval::OneHour,
                ranges.one_hour,
            );
            let (mut fifteen_minutes, one_hour) = tokio::try_join!(fifteen_minutes, one_hour)
                .map_err(|error| {
                    tracing::warn!(
                        market = %request.market().as_str(),
                        generation = request.generation(),
                        error = %error,
                        "official candle evidence unavailable for gap recovery"
                    );
                    RecoveryProducerError::OfficialCandleEvidenceUnavailable
                })?;
            fifteen_minutes.extend(one_hour);
            if fifteen_minutes.len() > MAX_RECOVERY_OFFICIAL_CANDLES {
                return Err(RecoveryProducerError::OfficialCandleEvidenceUnavailable);
            }
            Ok(fifteen_minutes)
        };
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => Err(RecoveryProducerError::Cancelled),
            result = fetch => result,
        }
    }
}

/// Failure while retaining or producing conservative recovery evidence.
#[derive(Debug, Error)]
pub enum RecoveryProducerError {
    /// A canonical trade identity was supplied with conflicting immutable data.
    #[error("retained recovery trade {event_id:?} conflicts with earlier committed evidence")]
    ConflictingRetainedTrade {
        /// Canonical conflicting trade identity.
        event_id: EventId,
    },
    /// Retained local evidence reached its explicit per-market capacity.
    #[error("retained recovery trade capacity {limit} reached for market {market:?}")]
    RetainedTradeCapacity {
        /// Market whose fixed retained source window is full.
        market: Market,
        /// Maximum source events retained for that market.
        limit: usize,
    },
    /// The caller cancelled before public evidence could be obtained.
    #[error("recovery evidence production cancelled before processing the queue head")]
    Cancelled,
    /// The documented public endpoint could not produce the required candle
    /// comparison facts. The queued request is consumed as explicitly
    /// unavailable rather than replayed from a partial response.
    #[error("official public candle evidence is unavailable")]
    OfficialCandleEvidenceUnavailable,
    /// The underlying bounded recovery coordinator rejected an immutable
    /// request, supplied evidence, or candle transition.
    #[error(transparent)]
    Recovery(#[from] RecoveryError),
}

#[derive(Debug, Clone, Copy)]
struct CandleRanges {
    fifteen_minutes: TimeRange,
    one_hour: TimeRange,
}

impl CandleRanges {
    fn from_request(request: &GapRecoveryRequest) -> Option<Self> {
        let predecessor = request.trade_predecessor_event_time()?;
        let snapshot = request.snapshot_event_time();
        if snapshot
            .value()
            .rem_euclid(CoreCandleInterval::OneHour.duration().value())
            != 0
        {
            return None;
        }
        let snapshot_ms = milliseconds(snapshot.value())?;
        let end_ms = snapshot_ms.checked_sub(1)?;
        let fifteen_minutes = range_for_interval(
            predecessor.value(),
            end_ms,
            CoreCandleInterval::FifteenMinutes,
        )?;
        let one_hour =
            range_for_interval(predecessor.value(), end_ms, CoreCandleInterval::OneHour)?;
        let fifteen_count = candle_count(fifteen_minutes, CoreCandleInterval::FifteenMinutes)?;
        let one_hour_count = candle_count(one_hour, CoreCandleInterval::OneHour)?;
        fifteen_count
            .checked_add(one_hour_count)
            .filter(|count| *count <= MAX_RECOVERY_OFFICIAL_CANDLES)
            .map(|_| Self {
                fifteen_minutes,
                one_hour,
            })
    }
}

fn range_for_interval(
    predecessor_ns: i64,
    end_ms: i64,
    interval: CoreCandleInterval,
) -> Option<TimeRange> {
    let interval_ms = interval.duration().value() / NANOS_PER_MILLISECOND;
    let predecessor_ms = milliseconds(predecessor_ns)?;
    let start_ms = predecessor_ms.checked_sub(predecessor_ms.rem_euclid(interval_ms))?;
    TimeRange::new(start_ms, end_ms).ok()
}

fn candle_count(range: TimeRange, interval: CoreCandleInterval) -> Option<usize> {
    let interval_ms = interval.duration().value() / NANOS_PER_MILLISECOND;
    let span = range
        .end_ms()
        .checked_sub(range.start_ms())?
        .checked_add(1)?;
    usize::try_from(span.checked_div(interval_ms)?).ok()
}

fn milliseconds(timestamp_ns: i64) -> Option<i64> {
    timestamp_ns.checked_div(NANOS_PER_MILLISECOND)
}

fn is_within_request_trade_window(request: &GapRecoveryRequest, event: &MarketEvent) -> bool {
    if !matches!(event.kind(), MarketEventKind::Trade(_)) || event.market() != request.market() {
        return false;
    }
    let after_predecessor = request
        .trade_predecessor_event_time()
        .zip(request.trade_predecessor_received_at())
        .zip(request.trade_predecessor_event_id())
        .is_none_or(|((event_time, received_at), event_id)| {
            event
                .event_time()
                .cmp(&event_time)
                .then_with(|| event.received_at().cmp(&received_at))
                .then_with(|| event.event_id().cmp(event_id))
                .is_gt()
        });
    after_predecessor && event.event_time() < request.snapshot_event_time()
}

fn canonical_trade_order(left: &MarketEvent, right: &MarketEvent) -> std::cmp::Ordering {
    left.event_time()
        .cmp(&right.event_time())
        .then_with(|| left.received_at().cmp(&right.received_at()))
        .then_with(|| left.event_id().cmp(right.event_id()))
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;
    use serde_json::json;
    use tokio_util::sync::CancellationToken;
    use trench_core::candle::CandleAggregator;
    use trench_core::domain::{Market, Price, Quantity, Side};
    use trench_core::event::{MarketEvent, TimestampNs, Trade};
    use wiremock::matchers::{body_json, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::{
        MAX_RETAINED_RECOVERY_TRADES_PER_MARKET, RecoveryEvidenceProducer, RecoveryProducerError,
    };
    use crate::info::InfoClient;
    use crate::recovery::{
        RecoverySource, RecoveryStatus, RecoveryUnavailable, recovery_request_for_test,
    };

    const BASE_MS: i64 = 1_800_000_000;
    const BASE_NS: i64 = BASE_MS * 1_000_000;
    const HOUR_NS: i64 = 3_600_000_000_000;

    fn market(symbol: &str) -> Market {
        Market::new(symbol).expect("test market must be valid")
    }

    fn timestamp(value: i64) -> TimestampNs {
        TimestampNs::new(i128::from(value)).expect("test timestamp must be valid")
    }

    fn request(symbol: &str, generation: u64) -> crate::GapRecoveryRequest {
        recovery_request_for_test(
            market(symbol),
            generation,
            Some(timestamp(BASE_NS)),
            timestamp(BASE_NS + HOUR_NS),
        )
    }

    fn trade(symbol: &str, at: i64, id: u64) -> MarketEvent {
        MarketEvent::trade(
            timestamp(at),
            timestamp(at),
            market(symbol),
            Trade::new(
                id,
                Side::Buy,
                Price::new(Decimal::from(100)).expect("test price must be valid"),
                Quantity::new(Decimal::ONE).expect("test quantity must be valid"),
            )
            .expect("test trade must be valid"),
        )
        .expect("test event must be valid")
    }

    fn candle(open: i64, interval_ms: i64, symbol: &str, trades: u64) -> serde_json::Value {
        let active = trades > 0;
        json!({
            "t": open,
            "T": open + interval_ms - 1,
            "s": symbol,
            "i": if interval_ms == 900_000 { "15m" } else { "1h" },
            "o": "100",
            "c": "100",
            "h": "100",
            "l": "100",
            "v": if active { "1" } else { "0" },
            "n": trades,
        })
    }

    async fn client(server: &MockServer) -> InfoClient {
        InfoClient::new_loopback_for_test(&format!("{}/info", server.uri()))
            .expect("test info client must be valid")
    }

    #[tokio::test]
    async fn reconciles_only_after_exact_local_trades_and_official_candles() {
        let server = MockServer::start().await;
        let end_ms = BASE_MS + 3_600_000 - 1;
        Mock::given(method("POST"))
            .and(path("/info"))
            .and(body_json(json!({
                "type": "candleSnapshot",
                "req": {
                    "coin": "BTC",
                    "interval": "15m",
                    "startTime": BASE_MS,
                    "endTime": end_ms,
                }
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(vec![
                candle(BASE_MS, 900_000, "BTC", 1),
                candle(BASE_MS + 900_000, 900_000, "BTC", 0),
                candle(BASE_MS + 1_800_000, 900_000, "BTC", 0),
                candle(BASE_MS + 2_700_000, 900_000, "BTC", 0),
            ]))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/info"))
            .and(body_json(json!({
                "type": "candleSnapshot",
                "req": {
                    "coin": "BTC",
                    "interval": "1h",
                    "startTime": BASE_MS,
                    "endTime": end_ms,
                }
            })))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(vec![candle(BASE_MS, 3_600_000, "BTC", 1)]),
            )
            .expect(1)
            .mount(&server)
            .await;

        let request = request("BTC", 1);
        let retained = trade("BTC", BASE_NS + 1, 1);
        let mut producer = RecoveryEvidenceProducer::new(client(&server).await);
        producer
            .retain_committed_source_event(&retained)
            .expect("committed local trade must retain");
        producer
            .enqueue(request.clone())
            .expect("request must queue");

        let result = producer
            .process_next(&mut CandleAggregator::new(), &CancellationToken::new())
            .await
            .expect("matching evidence must process")
            .expect("queued request must produce a result");
        assert_eq!(result.request(), &request);
        assert_eq!(
            result.source(),
            RecoverySource::LocalTradesAndOfficialCandles
        );
        assert!(matches!(result.status(), RecoveryStatus::Reconciled { .. }));
        assert_eq!(result.backfill_events(), std::slice::from_ref(&retained));
        assert_eq!(producer.pending_len(), 0);
    }

    #[tokio::test]
    async fn unavailable_public_candles_do_not_turn_a_snapshot_into_recovery() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(503))
            .mount(&server)
            .await;
        let mut producer = RecoveryEvidenceProducer::new(client(&server).await);
        producer
            .enqueue(request("BTC", 1))
            .expect("request must queue");

        let result = producer
            .process_next(&mut CandleAggregator::new(), &CancellationToken::new())
            .await
            .expect("unavailable evidence must be recorded")
            .expect("request must resolve explicitly");
        assert!(matches!(
            result.status(),
            RecoveryStatus::Unavailable {
                reason: RecoveryUnavailable::OfficialCandleEvidenceUnavailable
            }
        ));
        assert!(result.backfill_events().is_empty());
        assert_eq!(producer.pending_len(), 0);
    }

    #[tokio::test]
    async fn cancellation_keeps_the_fifo_head_unmodified() {
        let server = MockServer::start().await;
        let mut producer = RecoveryEvidenceProducer::new(client(&server).await);
        producer
            .enqueue(request("BTC", 1))
            .expect("request must queue");
        let cancellation = CancellationToken::new();
        cancellation.cancel();

        assert!(matches!(
            producer
                .process_next(&mut CandleAggregator::new(), &cancellation)
                .await,
            Err(RecoveryProducerError::Cancelled)
        ));
        assert_eq!(producer.pending_len(), 1);
    }

    #[tokio::test]
    async fn processes_only_the_oldest_request_before_later_markets() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(503))
            .mount(&server)
            .await;
        let btc = request("BTC", 1);
        let eth = request("ETH", 1);
        let mut producer = RecoveryEvidenceProducer::new(client(&server).await);
        producer
            .enqueue(btc.clone())
            .expect("first request must queue");
        producer
            .enqueue(eth.clone())
            .expect("second request must queue");
        let mut candles = CandleAggregator::new();

        let first = producer
            .process_next(&mut candles, &CancellationToken::new())
            .await
            .expect("first unavailable result must process")
            .expect("first result must exist");
        let second = producer
            .process_next(&mut candles, &CancellationToken::new())
            .await
            .expect("second unavailable result must process")
            .expect("second result must exist");
        assert_eq!(first.request(), &btc);
        assert_eq!(second.request(), &eth);
    }

    #[tokio::test]
    async fn retained_trade_capacity_fails_before_a_source_fact_is_dropped() {
        let first = trade("BTC", BASE_NS + 1, 1);
        let second = trade("BTC", BASE_NS + 2, 2);
        let server = MockServer::start().await;
        let mut producer = RecoveryEvidenceProducer::new(client(&server).await);
        producer.retained_trades.insert(
            market("BTC"),
            vec![first; MAX_RETAINED_RECOVERY_TRADES_PER_MARKET],
        );

        assert!(matches!(
            producer.retain_committed_source_event(&second),
            Err(RecoveryProducerError::RetainedTradeCapacity { .. })
        ));
    }
}
