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
use trench_core::candle::{CandleAggregator, MAX_PENDING_MARKETS};
use trench_core::domain::{EventId, Market};
use trench_core::event::{CandleInterval as CoreCandleInterval, MarketEvent, MarketEventKind};

use crate::info::{CandleInterval, InfoClient, TimeRange};
use crate::recovery::{
    GapRecovery, GapRecoveryRequest, MAX_OUTSTANDING_RECOVERY_REQUESTS, MAX_RECOVERY_LOCAL_TRADES,
    MAX_RECOVERY_OFFICIAL_CANDLES, RecoveryError, RecoveryEvidence, RecoveryResult,
    RecoveryUnavailable,
};

const NANOS_PER_MILLISECOND: i64 = 1_000_000;

/// Maximum durably admitted normalized trades retained for one market before
/// a queued recovery must terminate as explicitly unavailable.
pub const MAX_RETAINED_RECOVERY_TRADES_PER_MARKET: usize = MAX_RECOVERY_LOCAL_TRADES;
/// Maximum markets with outstanding recovery source evidence.
///
/// This exactly follows the bounded recovery queue and the candle aggregator's
/// pending-market capacity, so normal healthy-market collection retains no
/// per-market evidence at all.
pub const MAX_RETAINED_RECOVERY_MARKETS: usize = MAX_OUTSTANDING_RECOVERY_REQUESTS;

const _: () = assert!(MAX_RETAINED_RECOVERY_MARKETS <= MAX_PENDING_MARKETS);

#[derive(Debug, Default)]
struct MarketEvidence {
    trades: Vec<MarketEvent>,
    observed_through: Option<trench_core::event::TimestampNs>,
    capacity_exhausted: bool,
}

/// Produces explicit FIFO recovery results from durable local trade facts and
/// the documented public candle endpoint.
#[derive(Debug)]
pub struct RecoveryEvidenceProducer {
    info: InfoClient,
    recovery: GapRecovery,
    evidence: BTreeMap<Market, MarketEvidence>,
}

impl RecoveryEvidenceProducer {
    /// Creates a bounded recovery producer over one immutable read-only info
    /// client.
    #[must_use]
    pub fn new(info: InfoClient) -> Self {
        Self {
            info,
            recovery: GapRecovery::new(),
            evidence: BTreeMap::new(),
        }
    }

    /// Returns the number of queued gap requests awaiting a final result.
    #[must_use]
    pub fn pending_len(&self) -> usize {
        self.recovery.pending_len()
    }

    /// Retains one source event already committed by the authority writer.
    ///
    /// The caller must invoke this only after its single writer has durably
    /// accepted the original source fact. Evidence exists only while that
    /// market has queued recovery work. Every source fact advances its explicit
    /// observed-time watermark; trades are retained only when they can still
    /// be needed by an outstanding request.
    ///
    /// # Errors
    ///
    /// Returns [`RecoveryProducerError`] only for a conflicting canonical
    /// trade identity. A full active evidence window is recorded as an
    /// explicit terminal unavailable outcome on the next FIFO processing step,
    /// never by silently evicting potentially required facts.
    pub fn retain_committed_source_event(
        &mut self,
        event: &MarketEvent,
    ) -> Result<(), RecoveryProducerError> {
        let market = event.market();
        if !self.recovery.has_pending_market(market) {
            return Ok(());
        }
        let needed_by_pending_request = self.event_needed_by_any_pending_request(event);
        let evidence = self.evidence.get_mut(market).ok_or_else(|| {
            RecoveryProducerError::MissingActiveEvidence {
                market: market.clone(),
            }
        })?;
        evidence.observed_through = Some(
            evidence
                .observed_through
                .map_or(event.event_time(), |at| at.max(event.event_time())),
        );
        if !matches!(event.kind(), MarketEventKind::Trade(_)) {
            return Ok(());
        }
        if !needed_by_pending_request {
            return Ok(());
        }
        if let Some(existing) = evidence
            .trades
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
        if evidence.capacity_exhausted {
            return Ok(());
        }
        if evidence.trades.len() == MAX_RETAINED_RECOVERY_TRADES_PER_MARKET {
            evidence.capacity_exhausted = true;
            return Ok(());
        }
        evidence.trades.push(event.clone());
        Ok(())
    }

    /// Queues one WebSocket-delivered recovery request in strict FIFO order.
    ///
    /// # Errors
    ///
    /// Returns [`RecoveryError`] for an invalid, duplicated, nonmonotonic, or
    /// over-capacity request. The request is not retained on failure.
    pub fn enqueue(&mut self, request: GapRecoveryRequest) -> Result<(), RecoveryError> {
        let market = request.market().clone();
        let snapshot = request.snapshot_event_time();
        self.recovery.enqueue(request)?;
        if !self.evidence.contains_key(&market) {
            if self.evidence.len() == MAX_RETAINED_RECOVERY_MARKETS {
                return Err(RecoveryError::Invariant {
                    reason: "recovery queue accepted more active markets than evidence capacity",
                });
            }
            self.evidence.insert(
                market,
                MarketEvidence {
                    observed_through: Some(snapshot),
                    ..MarketEvidence::default()
                },
            );
        } else if let Some(evidence) = self.evidence.get_mut(&market) {
            evidence.observed_through = Some(
                evidence
                    .observed_through
                    .map_or(snapshot, |known| known.max(snapshot)),
            );
        }
        Ok(())
    }

    /// Advances an active market's explicit source-time watermark without
    /// inventing a market event. The authority may use this from its bounded
    /// timer when the public feed is quiet, so a mid-bar recovery cannot wait
    /// indefinitely for an unrelated trade.
    pub fn advance_time(&mut self, market: &Market, at: trench_core::event::TimestampNs) {
        if let Some(evidence) = self.evidence.get_mut(market) {
            evidence.observed_through =
                Some(evidence.observed_through.map_or(at, |known| known.max(at)));
        }
    }

    /// Produces a result for only the oldest pending request.
    ///
    /// The producer keeps a mid-bar request pending until the next completed
    /// common 15-minute/one-hour boundary is reached by an explicit source-time
    /// watermark. It retains the original fresh L2 snapshot as the recovery
    /// anchor while gathering local and official evidence through that later
    /// boundary. A network, decoding, capacity, or time-range failure becomes
    /// a conservative unavailable result; it never grants continuity.
    /// Cancellation leaves the queue head and candle state untouched so the
    /// caller can retry the same request in order.
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
        let (completed_through, capacity_exhausted) = self
            .evidence
            .get(request.market())
            .map(|evidence| (evidence.observed_through, evidence.capacity_exhausted))
            .ok_or_else(|| RecoveryProducerError::MissingActiveEvidence {
                market: request.market().clone(),
            })?;
        if request.trade_predecessor_event_time().is_none() {
            return self.complete_unavailable(
                RecoveryUnavailable::MissingTradePredecessor,
                request.snapshot_event_time(),
                candles,
                cancellation,
            );
        }
        let Some(boundary) = next_completion_boundary(request.snapshot_event_time()) else {
            return self.complete_unavailable(
                RecoveryUnavailable::CandleCoverageUnavailable,
                request.snapshot_event_time(),
                candles,
                cancellation,
            );
        };
        let Some(completed_through) = completed_through else {
            return Ok(None);
        };
        if completed_through < boundary {
            return Ok(None);
        }
        if capacity_exhausted {
            return self.complete_unavailable(
                RecoveryUnavailable::LocalTradeEvidenceCapacity,
                boundary,
                candles,
                cancellation,
            );
        }
        let local_trades = self.local_trades_for(&request, boundary);
        let Some(ranges) = CandleRanges::from_request(&request, boundary) else {
            return self.complete_unavailable(
                RecoveryUnavailable::CandleCoverageUnavailable,
                boundary,
                candles,
                cancellation,
            );
        };
        let official_candles = match self
            .fetch_official_candles(&request, ranges, cancellation)
            .await
        {
            Ok(candles) => candles,
            Err(RecoveryProducerError::Cancelled) => return Err(RecoveryProducerError::Cancelled),
            Err(RecoveryProducerError::OfficialCandleEvidenceUnavailable) => {
                return self.complete_unavailable(
                    RecoveryUnavailable::OfficialCandleEvidenceUnavailable,
                    boundary,
                    candles,
                    cancellation,
                );
            }
            Err(error) => return Err(error),
        };
        self.complete_reconciled(
            &local_trades,
            &official_candles,
            boundary,
            candles,
            cancellation,
        )
    }

    fn complete_unavailable(
        &mut self,
        reason: RecoveryUnavailable,
        completed_through: trench_core::event::TimestampNs,
        candles: &mut CandleAggregator,
        cancellation: &CancellationToken,
    ) -> Result<Option<RecoveryResult>, RecoveryProducerError> {
        self.ensure_not_cancelled(cancellation)?;
        let result = self
            .recovery
            .process_next_through(
                RecoveryEvidence::Unavailable { reason },
                completed_through,
                candles,
            )
            .map_err(RecoveryProducerError::from)?;
        self.cleanup_after_terminal_result(&result);
        Ok(result)
    }

    fn complete_reconciled(
        &mut self,
        local_trades: &[MarketEvent],
        official_candles: &[crate::normalize::Candle],
        completed_through: trench_core::event::TimestampNs,
        candles: &mut CandleAggregator,
        cancellation: &CancellationToken,
    ) -> Result<Option<RecoveryResult>, RecoveryProducerError> {
        self.ensure_not_cancelled(cancellation)?;
        let result = self
            .recovery
            .process_next_through(
                RecoveryEvidence::Reconciled {
                    local_trades,
                    official_candles,
                },
                completed_through,
                candles,
            )
            .map_err(RecoveryProducerError::from)?;
        self.cleanup_after_terminal_result(&result);
        Ok(result)
    }

    fn ensure_not_cancelled(
        &self,
        cancellation: &CancellationToken,
    ) -> Result<(), RecoveryProducerError> {
        (!cancellation.is_cancelled())
            .then_some(())
            .ok_or(RecoveryProducerError::Cancelled)
    }

    fn event_needed_by_any_pending_request(&self, event: &MarketEvent) -> bool {
        self.recovery
            .pending_requests_for_market(event.market())
            .any(|request| is_after_request_predecessor(request, event))
    }

    fn cleanup_after_terminal_result(&mut self, result: &Option<RecoveryResult>) {
        let Some(result) = result else {
            return;
        };
        let market = result.request().market().clone();
        let Some(mut evidence) = self.evidence.remove(&market) else {
            return;
        };
        if !self.recovery.has_pending_market(&market) {
            return;
        }
        evidence
            .trades
            .retain(|event| self.event_needed_by_any_pending_request(event));
        self.evidence.insert(market, evidence);
    }

    fn local_trades_for(
        &self,
        request: &GapRecoveryRequest,
        completed_through: trench_core::event::TimestampNs,
    ) -> Vec<MarketEvent> {
        let mut trades = self
            .evidence
            .get(request.market())
            .into_iter()
            .flat_map(|evidence| evidence.trades.iter())
            .filter(|event| is_within_request_trade_window(request, event, completed_through))
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
        let mut fifteen_minutes = self
            .fetch_candle_interval(
                request,
                CandleInterval::FifteenMinutes,
                ranges.fifteen_minutes,
                cancellation,
            )
            .await?;
        self.ensure_not_cancelled(cancellation)?;
        let one_hour = self
            .fetch_candle_interval(
                request,
                CandleInterval::OneHour,
                ranges.one_hour,
                cancellation,
            )
            .await?;
        self.ensure_not_cancelled(cancellation)?;
        fifteen_minutes.extend(one_hour);
        if fifteen_minutes.len() > MAX_RECOVERY_OFFICIAL_CANDLES {
            return Err(RecoveryProducerError::OfficialCandleEvidenceUnavailable);
        }
        Ok(fifteen_minutes)
    }

    async fn fetch_candle_interval(
        &self,
        request: &GapRecoveryRequest,
        interval: CandleInterval,
        range: TimeRange,
        cancellation: &CancellationToken,
    ) -> Result<Vec<crate::normalize::Candle>, RecoveryProducerError> {
        let response = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Err(RecoveryProducerError::Cancelled),
            response = self.info.candle_snapshot(request.market(), interval, range) => response,
        };
        self.ensure_not_cancelled(cancellation)?;
        response.map_err(|error| {
            tracing::warn!(
                market = %request.market().as_str(),
                generation = request.generation(),
                error = %error,
                "official candle evidence unavailable for gap recovery"
            );
            RecoveryProducerError::OfficialCandleEvidenceUnavailable
        })
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
    /// A queued request was missing its internal active evidence state.
    #[error("queued recovery request for market {market:?} has no active evidence state")]
    MissingActiveEvidence {
        /// Affected market.
        market: Market,
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
    fn from_request(
        request: &GapRecoveryRequest,
        completed_through: trench_core::event::TimestampNs,
    ) -> Option<Self> {
        let predecessor = request.trade_predecessor_event_time()?;
        let completed_ms = milliseconds(completed_through.value())?;
        let end_ms = completed_ms.checked_sub(1)?;
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

fn next_completion_boundary(
    snapshot: trench_core::event::TimestampNs,
) -> Option<trench_core::event::TimestampNs> {
    let duration = CoreCandleInterval::OneHour.duration().value();
    let quotient = snapshot.value().checked_div(duration)?;
    let next = quotient
        .checked_add(i64::from(snapshot.value().rem_euclid(duration) != 0))?
        .checked_mul(duration)?;
    trench_core::event::TimestampNs::new(i128::from(next)).ok()
}

fn is_after_request_predecessor(request: &GapRecoveryRequest, event: &MarketEvent) -> bool {
    if !matches!(event.kind(), MarketEventKind::Trade(_)) || event.market() != request.market() {
        return false;
    }
    request
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
        })
}

fn is_within_request_trade_window(
    request: &GapRecoveryRequest,
    event: &MarketEvent,
    completed_through: trench_core::event::TimestampNs,
) -> bool {
    is_after_request_predecessor(request, event) && event.event_time() < completed_through
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
        request_at(symbol, generation, BASE_NS + HOUR_NS)
    }

    fn request_at(symbol: &str, generation: u64, snapshot: i64) -> crate::GapRecoveryRequest {
        recovery_request_for_test(
            market(symbol),
            generation,
            Some(timestamp(BASE_NS)),
            timestamp(snapshot),
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
        json!({
            "t": open,
            "T": open + interval_ms - 1,
            "s": symbol,
            "i": if interval_ms == 900_000 { "15m" } else { "1h" },
            "o": "100",
            "c": "100",
            "h": "100",
            "l": "100",
            "v": trades.to_string(),
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
            .enqueue(request.clone())
            .expect("request must queue");
        producer
            .retain_committed_source_event(&retained)
            .expect("committed local trade must retain");

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
    async fn mid_bar_snapshot_waits_for_a_completed_boundary_without_replacing_its_anchor() {
        let server = MockServer::start().await;
        let snapshot = BASE_NS + 300_000_000_000;
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
                candle(BASE_MS, 900_000, "BTC", 2),
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
                    .set_body_json(vec![candle(BASE_MS, 3_600_000, "BTC", 2)]),
            )
            .expect(1)
            .mount(&server)
            .await;

        let request = request_at("BTC", 1, snapshot);
        let backfill = trade("BTC", BASE_NS + 1, 1);
        let post_snapshot = trade("BTC", snapshot + 1, 2);
        let mut producer = RecoveryEvidenceProducer::new(client(&server).await);
        producer
            .enqueue(request.clone())
            .expect("request must queue");
        producer
            .retain_committed_source_event(&backfill)
            .expect("gap evidence must retain");
        producer
            .retain_committed_source_event(&post_snapshot)
            .expect("post-snapshot source evidence must retain");

        assert!(
            producer
                .process_next(&mut CandleAggregator::new(), &CancellationToken::new())
                .await
                .expect("mid-bar request must remain pending")
                .is_none()
        );
        assert_eq!(producer.pending_len(), 1);

        producer.advance_time(&market("BTC"), timestamp(BASE_NS + HOUR_NS));
        let result = producer
            .process_next(&mut CandleAggregator::new(), &CancellationToken::new())
            .await
            .expect("completed-boundary evidence must process")
            .expect("request must resolve at the next completed boundary");
        assert_eq!(result.request(), &request);
        assert_eq!(result.request().snapshot_event_time(), timestamp(snapshot));
        assert_eq!(result.completed_through(), timestamp(BASE_NS + HOUR_NS));
        assert!(matches!(result.status(), RecoveryStatus::Reconciled { .. }));
        assert_eq!(result.backfill_events(), std::slice::from_ref(&backfill));
    }

    #[tokio::test]
    async fn cancellation_after_a_public_response_cannot_dequeue_the_head() {
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
        let cancellation = CancellationToken::new();
        let cancel_after_response = cancellation.clone();
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
            .respond_with(move |_: &wiremock::Request| {
                cancel_after_response.cancel();
                ResponseTemplate::new(200).set_body_json(vec![candle(BASE_MS, 3_600_000, "BTC", 1)])
            })
            .expect(1)
            .mount(&server)
            .await;
        let request = request("BTC", 1);
        let mut producer = RecoveryEvidenceProducer::new(client(&server).await);
        producer.enqueue(request).expect("request must queue");
        producer
            .retain_committed_source_event(&trade("BTC", BASE_NS + 1, 1))
            .expect("local evidence must retain");
        let mut candles = CandleAggregator::new();

        assert!(matches!(
            producer.process_next(&mut candles, &cancellation).await,
            Err(RecoveryProducerError::Cancelled)
        ));
        assert_eq!(producer.pending_len(), 1);
        assert!(candles.unavailable_gaps().is_empty());
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
    async fn capacity_exhaustion_is_terminal_then_releases_market_evidence() {
        let server = MockServer::start().await;
        let first = request("BTC", 1);
        let mut producer = RecoveryEvidenceProducer::new(client(&server).await);
        producer.enqueue(first).expect("request must queue");
        producer
            .evidence
            .get_mut(&market("BTC"))
            .expect("active evidence")
            .trades = (1..=MAX_RETAINED_RECOVERY_TRADES_PER_MARKET)
            .map(|id| trade("BTC", BASE_NS + 1, id as u64))
            .collect();
        producer
            .retain_committed_source_event(&trade(
                "BTC",
                BASE_NS + 2,
                (MAX_RETAINED_RECOVERY_TRADES_PER_MARKET + 1) as u64,
            ))
            .expect("capacity is recorded as an explicit terminal outcome");
        producer.advance_time(&market("BTC"), timestamp(BASE_NS + HOUR_NS));

        let result = producer
            .process_next(&mut CandleAggregator::new(), &CancellationToken::new())
            .await
            .expect("capacity outcome must process")
            .expect("capacity outcome must resolve the request");
        assert!(matches!(
            result.status(),
            RecoveryStatus::Unavailable {
                reason: RecoveryUnavailable::LocalTradeEvidenceCapacity
            }
        ));
        assert!(!producer.evidence.contains_key(&market("BTC")));

        producer
            .enqueue(request("BTC", 2))
            .expect("next recovery generation must start a fresh evidence epoch");
        producer
            .retain_committed_source_event(&trade("BTC", BASE_NS + HOUR_NS + 1, 99_999))
            .expect("new recovery epoch must retain evidence again");
        assert_eq!(
            producer
                .evidence
                .get(&market("BTC"))
                .expect("new active evidence")
                .trades
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn inactive_markets_do_not_consume_recovery_evidence_capacity() {
        let server = MockServer::start().await;
        let mut producer = RecoveryEvidenceProducer::new(client(&server).await);
        for index in 0..(super::MAX_RETAINED_RECOVERY_MARKETS + 32) {
            let symbol = format!("X{index:03}");
            producer
                .retain_committed_source_event(&trade(&symbol, BASE_NS + 1, index as u64 + 1))
                .expect("healthy source data must not allocate inactive recovery state");
        }
        assert!(producer.evidence.is_empty());

        for index in 0..super::MAX_RETAINED_RECOVERY_MARKETS {
            let symbol = format!("X{index:03}");
            producer
                .enqueue(request(&symbol, 1))
                .expect("bounded active recovery market must queue");
        }
        assert_eq!(
            producer.evidence.len(),
            super::MAX_RETAINED_RECOVERY_MARKETS
        );
        assert!(matches!(
            producer.enqueue(request("X999", 1)),
            Err(crate::recovery::RecoveryError::QueueCapacity { .. })
        ));
    }
}
