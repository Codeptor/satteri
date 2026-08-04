//! Pure, bounded reconciliation of public market-data interruption evidence.
//!
//! This module never fetches, writes storage, changes readiness, or fabricates
//! market facts. WebSocket ingestion emits a [`GapRecoveryRequest`] only after
//! the fresh L2 snapshot that anchors it. A downstream owner may then feed the
//! request, explicit evidence, and prior candle state through this synchronous
//! queue in receive order.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use thiserror::Error;
use trench_core::candle::{
    Candle as DerivedCandle, CandleAggregator, CandleError, CandleGap,
    MAX_PENDING_TRADES_PER_MARKET,
};
use trench_core::domain::{EventId, Market};
use trench_core::event::{
    CandleInterval as CoreCandleInterval, MarketEvent, MarketEventKind, TimestampNs,
};

use crate::archive::ArchiveBatch;
use crate::normalize::Candle;
use crate::ws::{GapOpened, GapReason};

/// Maximum recovery requests retained before an upstream durable replay epoch
/// must take over.
pub const MAX_OUTSTANDING_RECOVERY_REQUESTS: usize = 128;
/// Maximum completed request identities retained by one recovery epoch.
pub const MAX_PROCESSED_RECOVERY_REQUESTS: usize = 4_096;
/// Maximum explicit local trade facts accepted for one recovered gap.
pub const MAX_RECOVERY_LOCAL_TRADES: usize = MAX_PENDING_TRADES_PER_MARKET;
/// Maximum official candles accepted as explicit evidence for one recovered gap.
pub const MAX_RECOVERY_OFFICIAL_CANDLES: usize = 5_000;

/// Immutable evidence linking one interrupted market stream to its first fresh
/// post-gap L2 snapshot.
///
/// Construction is crate-scoped: external consumers can inspect and enqueue a
/// request delivered by [`crate::WsOutput`], but cannot forge a readiness-like
/// recovery record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GapRecoveryRequest {
    generation: u64,
    market: Market,
    reason: GapReason,
    predecessor_event_time: Option<TimestampNs>,
    predecessor_received_at: Option<TimestampNs>,
    trade_predecessor_event_time: Option<TimestampNs>,
    trade_predecessor_received_at: Option<TimestampNs>,
    trade_predecessor_event_id: Option<EventId>,
    snapshot_event_id: EventId,
    snapshot_event_time: TimestampNs,
    snapshot_received_at: TimestampNs,
    reconnect_attempt: u32,
}

impl GapRecoveryRequest {
    pub(crate) fn from_gap_snapshot(
        opened: &GapOpened,
        snapshot: &MarketEvent,
        reconnect_attempt: u32,
    ) -> Self {
        Self {
            generation: opened.generation(),
            market: opened.market().clone(),
            reason: opened.reason(),
            predecessor_event_time: opened.last_event_time(),
            predecessor_received_at: opened.last_received_at(),
            trade_predecessor_event_time: opened.last_trade_event_time(),
            trade_predecessor_received_at: opened.last_trade_received_at(),
            trade_predecessor_event_id: opened.last_trade_event_id().cloned(),
            snapshot_event_id: snapshot.event_id().clone(),
            snapshot_event_time: snapshot.event_time(),
            snapshot_received_at: snapshot.received_at(),
            reconnect_attempt,
        }
    }

    /// Returns the interrupted stream generation.
    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    /// Returns the affected market.
    #[must_use]
    pub const fn market(&self) -> &Market {
        &self.market
    }

    /// Returns the original interruption cause.
    #[must_use]
    pub const fn reason(&self) -> GapReason {
        self.reason
    }

    /// Returns the last accepted exchange event time before the interruption.
    #[must_use]
    pub const fn predecessor_event_time(&self) -> Option<TimestampNs> {
        self.predecessor_event_time
    }

    /// Returns the final pre-gap local receipt cursor, when known.
    #[must_use]
    pub const fn predecessor_received_at(&self) -> Option<TimestampNs> {
        self.predecessor_received_at
    }

    /// Returns the last accepted trade exchange-time cursor before the gap.
    ///
    /// Candles reconcile against this trade-specific cursor, never a later BBO
    /// or L2 timestamp from a different market-data channel.
    #[must_use]
    pub const fn trade_predecessor_event_time(&self) -> Option<TimestampNs> {
        self.trade_predecessor_event_time
    }

    /// Returns the local receipt cursor of the final pre-gap trade, when known.
    #[must_use]
    pub const fn trade_predecessor_received_at(&self) -> Option<TimestampNs> {
        self.trade_predecessor_received_at
    }

    /// Returns the final pre-gap trade identity, completing its canonical
    /// `(event_time, received_at, event_id)` ordering cursor when present.
    #[must_use]
    pub const fn trade_predecessor_event_id(&self) -> Option<&EventId> {
        self.trade_predecessor_event_id.as_ref()
    }

    /// Returns the canonical identity of the fresh full-L2 recovery point.
    #[must_use]
    pub const fn snapshot_event_id(&self) -> &EventId {
        &self.snapshot_event_id
    }

    /// Returns the fresh recovery snapshot's exchange event time.
    #[must_use]
    pub const fn snapshot_event_time(&self) -> TimestampNs {
        self.snapshot_event_time
    }

    /// Returns the fresh recovery snapshot's local receipt time.
    #[must_use]
    pub const fn snapshot_received_at(&self) -> TimestampNs {
        self.snapshot_received_at
    }

    /// Returns the reconnect attempt that supplied the fresh snapshot.
    #[must_use]
    pub const fn reconnect_attempt(&self) -> u32 {
        self.reconnect_attempt
    }

    fn key(&self) -> RecoveryKey {
        RecoveryKey {
            generation: self.generation,
            market: self.market.clone(),
        }
    }

    fn valid_cursors(&self) -> bool {
        let complete_trade_cursor = matches!(
            (
                self.trade_predecessor_event_time,
                self.trade_predecessor_received_at,
                self.trade_predecessor_event_id.as_ref(),
            ),
            (None, None, None) | (Some(_), Some(_), Some(_))
        );
        self.reconnect_attempt > 0
            && complete_trade_cursor
            && self
                .predecessor_received_at
                .is_none_or(|cursor| cursor <= self.snapshot_received_at)
            && self
                .trade_predecessor_event_time
                .is_none_or(|cursor| cursor < self.snapshot_event_time)
            && self
                .trade_predecessor_received_at
                .is_none_or(|cursor| cursor <= self.snapshot_received_at)
    }

    fn unavailable_gap(&self) -> Result<CandleGap, CandleError> {
        CandleGap::new(
            self.market.clone(),
            self.trade_predecessor_event_time,
            self.snapshot_event_time,
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct RecoveryKey {
    generation: u64,
    market: Market,
}

/// Explicit evidence available to one queued recovery request.
///
/// Archive input is deliberately distinct: the documented archive contains L2
/// snapshots only, so it can establish neither trades nor candles.
#[derive(Debug, Clone, Copy)]
pub enum RecoveryEvidence<'a> {
    /// Locally retained normalized trades and separately supplied official
    /// candle snapshots for exact reconciliation.
    Reconciled {
        /// Trades from the interrupted interval, in any order.
        local_trades: &'a [MarketEvent],
        /// Official candle snapshots for the requested sleeves/range.
        official_candles: &'a [Candle],
    },
    /// Verified historical archive input. It is L2-only and therefore can
    /// never supply the trade evidence required to build a candle.
    ArchiveL2(&'a ArchiveBatch),
    /// The required evidence source was explicitly unavailable for the
    /// interval. This is a completed conservative recovery outcome, never an
    /// inferred continuation.
    Unavailable {
        /// Stable reason the required evidence could not be obtained.
        reason: RecoveryUnavailable,
    },
}

/// The final, non-readiness result of one recovery request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveryResult {
    request: GapRecoveryRequest,
    completed_through: TimestampNs,
    status: RecoveryStatus,
    source: RecoverySource,
    backfill_events: Vec<MarketEvent>,
}

impl RecoveryResult {
    /// Returns the immutable request that produced this result.
    #[must_use]
    pub const fn request(&self) -> &GapRecoveryRequest {
        &self.request
    }

    /// Returns the completed common candle boundary through which evidence was
    /// evaluated. The request's fresh L2 snapshot remains its immutable
    /// recovery anchor even when evidence had to wait for a later bar close.
    #[must_use]
    pub const fn completed_through(&self) -> TimestampNs {
        self.completed_through
    }

    /// Returns whether independently supplied trade and candle evidence
    /// reconciled, or an explicit unavailable/conflict outcome was recorded.
    #[must_use]
    pub const fn status(&self) -> &RecoveryStatus {
        &self.status
    }

    /// Returns the explicit evidence class used for this result.
    #[must_use]
    pub const fn source(&self) -> RecoverySource {
        self.source
    }

    /// Returns accepted normalized trade backfill facts in caller-supplied
    /// order for durable replay. Unavailable outcomes never fabricate events.
    #[must_use]
    pub fn backfill_events(&self) -> &[MarketEvent] {
        &self.backfill_events
    }
}

/// The evidence class that produced one complete recovery result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoverySource {
    /// Local normalized trade facts matched independently supplied official candles.
    LocalTradesAndOfficialCandles,
    /// A verified official archive was L2-only and could not reconstruct candles.
    ArchiveL2Only,
    /// No trustworthy recovery evidence was available.
    Unavailable,
}

/// A recovery outcome. Neither variant represents strategy readiness.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoveryStatus {
    /// Exact local trade aggregation matched the supplied official candles.
    Reconciled {
        /// Newly completed deterministic core candles.
        candles: Vec<DerivedCandle>,
    },
    /// Candles remain explicitly unavailable until a later replay epoch can
    /// supply complete trustworthy evidence.
    Unavailable {
        /// Why trustworthy candle reconstruction was impossible.
        reason: RecoveryUnavailable,
    },
}

/// Stable explanations for an explicitly unavailable recovery span.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryUnavailable {
    /// No pre-gap trade cursor exists, so the unknown candle history has no
    /// bounded lower edge for trustworthy reconstruction.
    MissingTradePredecessor,
    /// The fresh snapshot landed inside an active 15-minute or one-hour bar,
    /// so no complete two-sleeve candle comparison is possible yet.
    IncompleteSleeves,
    /// Official candles did not provide one valid, unique 15-minute and
    /// one-hour record for every completed bucket touching the recovered span.
    CandleCoverageUnavailable,
    /// The documented archive supplied only L2 snapshots, not trade facts.
    ArchiveL2Only,
    /// No independent local trade evidence was supplied.
    MissingTradeEvidence,
    /// The documented public candle endpoint could not provide the requested
    /// verified comparison facts.
    OfficialCandleEvidenceUnavailable,
    /// The bounded local source-evidence window filled before the pending gap
    /// could reach its completed reconciliation boundary.
    LocalTradeEvidenceCapacity,
    /// Local trade aggregation did not exactly match the supplied official
    /// candle snapshots.
    CandleConflict,
}

/// One bounded FIFO recovery coordinator.
///
/// The queue is deliberately synchronous and owns neither a network client nor
/// storage writer. It processes only its head, preserving the preceding
/// WebSocket output order (fresh L2 market event, then recovery request).
#[derive(Debug, Default)]
pub struct GapRecovery {
    queue: VecDeque<GapRecoveryRequest>,
    queued: BTreeMap<RecoveryKey, GapRecoveryRequest>,
    last_generation: BTreeMap<Market, u64>,
    processed: BTreeSet<RecoveryKey>,
}

impl GapRecovery {
    /// Creates an empty bounded recovery queue.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the number of requests awaiting explicit evidence.
    #[must_use]
    pub fn pending_len(&self) -> usize {
        self.queue.len()
    }

    /// Returns the oldest request awaiting evidence without removing it.
    ///
    /// A producer must build evidence only for this head request, then call
    /// [`Self::process_next`] with that evidence. This preserves WebSocket
    /// output order across asynchronous public-data fetches.
    #[must_use]
    pub fn next_request(&self) -> Option<&GapRecoveryRequest> {
        self.queue.front()
    }

    /// Returns whether one or more queued requests still need source evidence
    /// for `market`.
    #[must_use]
    pub fn has_pending_market(&self, market: &Market) -> bool {
        self.queue.iter().any(|request| request.market() == market)
    }

    /// Borrows queued requests for one market in FIFO order.
    pub(crate) fn pending_requests_for_market(
        &self,
        market: &Market,
    ) -> impl Iterator<Item = &GapRecoveryRequest> {
        self.queue
            .iter()
            .filter(move |request| request.market() == market)
    }

    /// Returns only verified archived L2 facts for durable import.
    ///
    /// This explicit recovery boundary preserves the archive's documented
    /// L2-only limitation: it never treats archived books as trades, BBOs, or
    /// synthesized candles. A later queued gap request can consume the same
    /// archive through [`RecoveryEvidence::ArchiveL2`] and records its candle
    /// span as unavailable.
    pub fn archive_l2_events(batch: &ArchiveBatch) -> Result<&[MarketEvent], RecoveryError> {
        for event in batch.events() {
            if !matches!(event.kind(), MarketEventKind::BookSnapshot(_)) {
                return Err(RecoveryError::ArchiveNotL2 {
                    event_id: event.event_id().clone(),
                });
            }
        }
        Ok(batch.events())
    }

    /// Adds one WebSocket-delivered request in per-market generation order.
    ///
    /// # Errors
    ///
    /// Rejects forged cursor relationships, duplicate/conflicting identities,
    /// generation rollback, and attempts to exceed the fixed queue bound.
    pub fn enqueue(&mut self, request: GapRecoveryRequest) -> Result<(), RecoveryError> {
        if !request.valid_cursors() {
            return Err(RecoveryError::InvalidCursor {
                market: request.market.clone(),
            });
        }
        let key = request.key();
        if let Some(existing) = self.queued.get(&key) {
            return if existing == &request {
                Err(RecoveryError::DuplicateRequest { market: key.market })
            } else {
                Err(RecoveryError::ConflictingRequest { market: key.market })
            };
        }
        if self.processed.contains(&key) {
            return Err(RecoveryError::DuplicateRequest { market: key.market });
        }
        if self.processed.len() == MAX_PROCESSED_RECOVERY_REQUESTS {
            return Err(RecoveryError::ProcessedHistoryCapacity {
                limit: MAX_PROCESSED_RECOVERY_REQUESTS,
            });
        }
        if self
            .last_generation
            .get(&request.market)
            .is_some_and(|generation| request.generation <= *generation)
        {
            return Err(RecoveryError::OutOfOrderGeneration {
                market: request.market.clone(),
                generation: request.generation,
            });
        }
        if self.queue.len() == MAX_OUTSTANDING_RECOVERY_REQUESTS {
            return Err(RecoveryError::QueueCapacity {
                limit: MAX_OUTSTANDING_RECOVERY_REQUESTS,
            });
        }
        self.last_generation
            .insert(request.market.clone(), request.generation);
        self.queued.insert(key, request.clone());
        self.queue.push_back(request);
        Ok(())
    }

    /// Processes the oldest request with explicitly supplied evidence.
    ///
    /// A rejected evidence payload remains at the head for deterministic retry
    /// or durable escalation. An unavailable/conflict result is successful
    /// processing because it records a conservative candle span and then lets
    /// the next existing dependency proceed.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid evidence or a candle-state failure without
    /// removing the head request.
    pub fn process_next(
        &mut self,
        evidence: RecoveryEvidence<'_>,
        candles: &mut CandleAggregator,
    ) -> Result<Option<RecoveryResult>, RecoveryError> {
        let Some(request) = self.queue.front().cloned() else {
            return Ok(None);
        };
        self.process_next_through(evidence, request.snapshot_event_time, candles)
    }

    /// Processes the oldest request using evidence complete through an
    /// explicit common candle boundary.
    ///
    /// The request remains anchored to its original fresh L2 snapshot. A
    /// producer may wait for a later completed boundary when that snapshot
    /// landed mid-bar, but it may never substitute another snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error without dequeuing when the supplied boundary predates
    /// the immutable snapshot or the evidence cannot be reconciled.
    pub fn process_next_through(
        &mut self,
        evidence: RecoveryEvidence<'_>,
        completed_through: TimestampNs,
        candles: &mut CandleAggregator,
    ) -> Result<Option<RecoveryResult>, RecoveryError> {
        let Some(request) = self.queue.front().cloned() else {
            return Ok(None);
        };
        if completed_through < request.snapshot_event_time() {
            return Err(RecoveryError::EvidenceBeforeSnapshot {
                snapshot: request.snapshot_event_time(),
                completed_through,
            });
        }
        let result = reconcile(&request, evidence, completed_through, candles)?;
        let popped = self.queue.pop_front().ok_or(RecoveryError::Invariant {
            reason: "queued recovery head must remain present",
        })?;
        let key = popped.key();
        self.queued.remove(&key).ok_or(RecoveryError::Invariant {
            reason: "queued recovery index must retain its head",
        })?;
        self.processed.insert(key);
        Ok(Some(result))
    }
}

/// A bounded recovery request or explicit evidence failure.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RecoveryError {
    /// The request cursor did not prove forward recovery from its predecessor.
    #[error("recovery request for market {market:?} has invalid predecessor or snapshot cursors")]
    InvalidCursor {
        /// Affected market.
        market: Market,
    },
    /// A request repeated an already queued or processed exact identity.
    #[error("duplicate recovery request for market {market:?}")]
    DuplicateRequest {
        /// Affected market.
        market: Market,
    },
    /// The same market/generation was replayed with different immutable facts.
    #[error("conflicting recovery request for market {market:?}")]
    ConflictingRequest {
        /// Affected market.
        market: Market,
    },
    /// A market's interruption generation regressed after a later request.
    #[error("out-of-order recovery generation {generation} for market {market:?}")]
    OutOfOrderGeneration {
        /// Affected market.
        market: Market,
        /// Rejected nonmonotonic generation.
        generation: u64,
    },
    /// The fixed recovery queue cannot retain another unresolved request.
    #[error("recovery queue capacity {limit} reached")]
    QueueCapacity {
        /// Maximum outstanding work items.
        limit: usize,
    },
    /// The exact completed-request journal reached its recovery-epoch bound.
    #[error("processed recovery identity capacity {limit} reached; start a new replay epoch")]
    ProcessedHistoryCapacity {
        /// Maximum retained completed request identities.
        limit: usize,
    },
    /// The explicit evidence boundary predates the immutable fresh L2
    /// snapshot that anchors this recovery request.
    #[error("recovery evidence boundary {completed_through} predates fresh snapshot {snapshot}")]
    EvidenceBeforeSnapshot {
        /// Immutable fresh L2 recovery-point time.
        snapshot: TimestampNs,
        /// Caller-supplied completed candle boundary.
        completed_through: TimestampNs,
    },
    /// Caller-supplied synchronous evidence exceeded its fixed processing bound.
    #[error("recovery evidence field `{field}` has {count} records, exceeding {limit}")]
    EvidenceCapacity {
        /// Bounded evidence field.
        field: &'static str,
        /// Supplied record count.
        count: usize,
        /// Maximum accepted records.
        limit: usize,
    },
    /// Evidence contained a non-trade fact where local trade history was required.
    #[error("recovery evidence contains a non-trade market event {event_id:?}")]
    ExpectedTrade {
        /// Canonical identity of the rejected event.
        event_id: EventId,
    },
    /// Evidence named a market other than the queued recovery request.
    #[error("recovery evidence market {actual:?} does not match requested market {expected:?}")]
    ForeignMarket {
        /// Request market.
        expected: Market,
        /// Evidence market.
        actual: Market,
    },
    /// A local trade was not strictly between the exact predecessor and fresh snapshot cursors.
    #[error("recovery trade {event_id:?} is outside the requested market-time gap")]
    TradeOutsideGap {
        /// Canonical identity of the rejected trade.
        event_id: EventId,
    },
    /// A verified archive violated its documented L2-only contract.
    #[error("verified archive contained a non-L2 event {event_id:?}")]
    ArchiveNotL2 {
        /// Canonical identity of the rejected archive event.
        event_id: EventId,
    },
    /// A queue-index invariant was violated.
    #[error("gap recovery invariant failed: {reason}")]
    Invariant {
        /// Failed invariant description.
        reason: &'static str,
    },
    /// Core candle aggregation rejected otherwise explicit recovery data.
    #[error(transparent)]
    Candle(#[from] CandleError),
}

fn reconcile(
    request: &GapRecoveryRequest,
    evidence: RecoveryEvidence<'_>,
    completed_through: TimestampNs,
    candles: &mut CandleAggregator,
) -> Result<RecoveryResult, RecoveryError> {
    let (status, source, backfill_events) = match evidence {
        RecoveryEvidence::Reconciled {
            local_trades,
            official_candles,
        } => {
            let (status, backfill_events) = reconcile_trades(
                request,
                local_trades,
                official_candles,
                completed_through,
                candles,
            )?;
            (
                status,
                RecoverySource::LocalTradesAndOfficialCandles,
                backfill_events,
            )
        }
        RecoveryEvidence::ArchiveL2(batch) => {
            for event in batch.events() {
                if !matches!(event.kind(), MarketEventKind::BookSnapshot(_)) {
                    return Err(RecoveryError::ArchiveNotL2 {
                        event_id: event.event_id().clone(),
                    });
                }
            }
            (
                mark_unavailable(request, candles, RecoveryUnavailable::ArchiveL2Only)?,
                RecoverySource::ArchiveL2Only,
                Vec::new(),
            )
        }
        RecoveryEvidence::Unavailable { reason } => (
            mark_unavailable(request, candles, reason)?,
            RecoverySource::Unavailable,
            Vec::new(),
        ),
    };
    Ok(RecoveryResult {
        request: request.clone(),
        completed_through,
        status,
        source,
        backfill_events,
    })
}

fn reconcile_trades(
    request: &GapRecoveryRequest,
    local_trades: &[MarketEvent],
    official_candles: &[Candle],
    completed_through: TimestampNs,
    candles: &mut CandleAggregator,
) -> Result<(RecoveryStatus, Vec<MarketEvent>), RecoveryError> {
    if local_trades.len() > MAX_RECOVERY_LOCAL_TRADES {
        return Err(RecoveryError::EvidenceCapacity {
            field: "local_trades",
            count: local_trades.len(),
            limit: MAX_RECOVERY_LOCAL_TRADES,
        });
    }
    if official_candles.len() > MAX_RECOVERY_OFFICIAL_CANDLES {
        return Err(RecoveryError::EvidenceCapacity {
            field: "official_candles",
            count: official_candles.len(),
            limit: MAX_RECOVERY_OFFICIAL_CANDLES,
        });
    }
    for event in local_trades {
        validate_trade(request, event, completed_through)?;
    }
    for candle in official_candles {
        if candle.market() != request.market() {
            return Err(RecoveryError::ForeignMarket {
                expected: request.market.clone(),
                actual: candle.market().clone(),
            });
        }
    }

    let unavailable_reason = if request.trade_predecessor_event_time.is_none() {
        Some(RecoveryUnavailable::MissingTradePredecessor)
    } else if !all_required_sleeves_closed(completed_through) {
        Some(RecoveryUnavailable::IncompleteSleeves)
    } else {
        None
    };
    if let Some(reason) = unavailable_reason {
        return mark_unavailable(request, candles, reason).map(|status| (status, Vec::new()));
    }

    let Some(expected_candle_keys) = required_candle_keys(request, completed_through) else {
        return mark_unavailable(
            request,
            candles,
            RecoveryUnavailable::CandleCoverageUnavailable,
        )
        .map(|status| (status, Vec::new()));
    };
    if !has_exact_candle_coverage(&expected_candle_keys, official_candles) {
        return mark_unavailable(
            request,
            candles,
            RecoveryUnavailable::CandleCoverageUnavailable,
        )
        .map(|status| (status, Vec::new()));
    }

    let mut candidate = candles.clone();
    for event in local_trades {
        candidate.ingest(event)?;
    }
    let completed = candidate.complete_market_through(request.market(), completed_through)?;
    if candles_match(&expected_candle_keys, &completed, official_candles) {
        *candles = candidate;
        return Ok((
            RecoveryStatus::Reconciled { candles: completed },
            local_trades
                .iter()
                .filter(|event| event.event_time() < request.snapshot_event_time())
                .cloned()
                .collect(),
        ));
    }
    mark_unavailable(request, candles, RecoveryUnavailable::CandleConflict)
        .map(|status| (status, Vec::new()))
}

fn validate_trade(
    request: &GapRecoveryRequest,
    event: &MarketEvent,
    completed_through: TimestampNs,
) -> Result<(), RecoveryError> {
    if !matches!(event.kind(), MarketEventKind::Trade(_)) {
        return Err(RecoveryError::ExpectedTrade {
            event_id: event.event_id().clone(),
        });
    }
    if event.market() != request.market() {
        return Err(RecoveryError::ForeignMarket {
            expected: request.market.clone(),
            actual: event.market().clone(),
        });
    }
    let after_predecessor = request
        .trade_predecessor_event_time
        .zip(request.trade_predecessor_received_at)
        .zip(request.trade_predecessor_event_id.as_ref())
        .is_none_or(|((event_time, received_at), event_id)| {
            event
                .event_time()
                .cmp(&event_time)
                .then_with(|| event.received_at().cmp(&received_at))
                .then_with(|| event.event_id().cmp(event_id))
                .is_gt()
        });
    if !after_predecessor || event.event_time() >= completed_through {
        return Err(RecoveryError::TradeOutsideGap {
            event_id: event.event_id().clone(),
        });
    }
    Ok(())
}

fn all_required_sleeves_closed(completed_through: TimestampNs) -> bool {
    [
        CoreCandleInterval::FifteenMinutes,
        CoreCandleInterval::OneHour,
    ]
    .into_iter()
    .all(|interval| {
        completed_through
            .value()
            .rem_euclid(interval.duration().value())
            == 0
    })
}

type CandleKey = (CoreCandleInterval, TimestampNs);

fn required_candle_keys(
    request: &GapRecoveryRequest,
    completed_through: TimestampNs,
) -> Option<BTreeSet<CandleKey>> {
    let start = request.trade_predecessor_event_time?;
    let mut expected = BTreeSet::new();
    for interval in [
        CoreCandleInterval::FifteenMinutes,
        CoreCandleInterval::OneHour,
    ] {
        let duration = interval.duration();
        let first_open = start
            .value()
            .checked_sub(start.value().rem_euclid(duration.value()))?;
        let mut open = TimestampNs::new(i128::from(first_open)).ok()?;
        while open < completed_through {
            let close = open.checked_add(duration).ok()?;
            if close > start && close <= completed_through {
                if expected.len() == MAX_RECOVERY_OFFICIAL_CANDLES {
                    return None;
                }
                expected.insert((interval, open));
            }
            open = close;
        }
    }
    Some(expected)
}

fn mark_unavailable(
    request: &GapRecoveryRequest,
    candles: &mut CandleAggregator,
    reason: RecoveryUnavailable,
) -> Result<RecoveryStatus, RecoveryError> {
    candles.mark_gap_unavailable(request.unavailable_gap()?)?;
    Ok(RecoveryStatus::Unavailable { reason })
}

fn has_exact_candle_coverage(expected: &BTreeSet<CandleKey>, official: &[Candle]) -> bool {
    let Some(observed) = official_candles_by_key(official) else {
        return false;
    };
    observed.len() == expected.len() && observed.keys().all(|key| expected.contains(key))
}

fn candles_match(
    expected: &BTreeSet<CandleKey>,
    derived: &[DerivedCandle],
    official: &[Candle],
) -> bool {
    let Some(observed) = official_candles_by_key(official) else {
        return false;
    };
    let mut derived_by_key = BTreeMap::new();
    for candle in derived {
        let key = (candle.candle().interval(), candle.candle().open_time());
        if derived_by_key.insert(key, candle).is_some() {
            return false;
        }
    }
    expected.iter().all(|key| {
        let Some(official) = observed.get(key) else {
            return false;
        };
        match derived_by_key.get(key) {
            Some(derived) => candle_matches(derived, official),
            None => official.trade_count() == 0,
        }
    }) && derived_by_key.keys().all(|key| expected.contains(key))
}

fn official_candles_by_key(official: &[Candle]) -> Option<BTreeMap<CandleKey, &Candle>> {
    let mut observed = BTreeMap::new();
    for candle in official {
        let key = venue_candle_key(candle)?;
        observed.insert(key, candle).is_none().then_some(())?;
    }
    Some(observed)
}

fn candle_matches(derived: &DerivedCandle, official: &Candle) -> bool {
    let Some((interval, open)) = venue_candle_key(official) else {
        return false;
    };
    let Ok(close) = open.checked_add(interval.duration()) else {
        return false;
    };
    derived.market() == official.market()
        && derived.candle().interval() == interval
        && derived.candle().open_time() == open
        && derived
            .close_time()
            .is_ok_and(|derived_close| derived_close == close)
        && derived.candle().open() == official.open()
        && derived.candle().close() == official.close()
        && derived.candle().high() == official.high()
        && derived.candle().low() == official.low()
        && derived.candle().volume() == official.volume()
        && derived.candle().trade_count() == official.trade_count()
}

fn venue_candle_key(candle: &Candle) -> Option<(CoreCandleInterval, TimestampNs)> {
    let interval = match candle.interval() {
        crate::info::CandleInterval::FifteenMinutes => CoreCandleInterval::FifteenMinutes,
        crate::info::CandleInterval::OneHour => CoreCandleInterval::OneHour,
    };
    let open = TimestampNs::new(i128::from(candle.open_time_ms()) * 1_000_000).ok()?;
    let close = TimestampNs::new(
        i128::from(candle.close_time_ms())
            .checked_add(1)?
            .checked_mul(1_000_000)?,
    )
    .ok()?;
    (open.checked_add(interval.duration()).ok()? == close).then_some((interval, open))
}

#[cfg(test)]
pub(crate) fn recovery_request_for_test(
    market: Market,
    generation: u64,
    trade_predecessor: Option<TimestampNs>,
    snapshot: TimestampNs,
) -> GapRecoveryRequest {
    let market_name = market.as_str().to_owned();
    GapRecoveryRequest {
        generation,
        market,
        reason: GapReason::TransportClosed,
        predecessor_event_time: trade_predecessor,
        predecessor_received_at: trade_predecessor,
        trade_predecessor_event_time: trade_predecessor,
        trade_predecessor_received_at: trade_predecessor,
        trade_predecessor_event_id: trade_predecessor.map(|time| {
            EventId::new(format!(
                "recovery-test-predecessor-{market_name}-{generation}-{}",
                time.value()
            ))
            .expect("test recovery predecessor ID must be valid")
        }),
        snapshot_event_id: EventId::new(format!(
            "recovery-test-snapshot-{market_name}-{generation}-{}",
            snapshot.value()
        ))
        .expect("test recovery snapshot ID must be valid"),
        snapshot_event_time: snapshot,
        snapshot_received_at: snapshot,
        reconnect_attempt: 1,
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;

    use rust_decimal::Decimal;
    use serde_json::json;
    use tempfile::TempDir;
    use trench_core::domain::{Market, Price, Quantity, Side};
    use trench_core::event::{MarketEvent, TimestampNs, Trade};

    use crate::archive::{
        ArchiveDataKind, ArchiveDigest, ArchiveManifest, ArchiveReader, ArchiveRequirement,
        ArchiveSource, ArchiveSpan,
    };
    use crate::info::{CandleInterval, TimeRange};
    use crate::normalize::decode_candles;

    use super::{
        GapReason, GapRecovery, GapRecoveryRequest, MAX_OUTSTANDING_RECOVERY_REQUESTS,
        MAX_PROCESSED_RECOVERY_REQUESTS, MAX_RECOVERY_LOCAL_TRADES, RecoveryError,
        RecoveryEvidence, RecoverySource, RecoveryStatus, RecoveryUnavailable,
    };
    use trench_core::candle::CandleAggregator;

    const BASE_NS: i128 = 1_800_000_000_000_000;
    const BASE_MS: i64 = 1_800_000_000;
    const L2_FIXTURE: &[u8] = include_bytes!("../../../tests/fixtures/archive/l2-sample.lz4");

    fn timestamp(value: i128) -> TimestampNs {
        TimestampNs::new(value).expect("test timestamp must be valid")
    }

    fn market(symbol: &str) -> Market {
        Market::new(symbol).expect("test market must be valid")
    }

    fn request(
        symbol: &str,
        generation: u64,
        predecessor_event_time: Option<i128>,
        snapshot_event_time: i128,
        reconnect_attempt: u32,
    ) -> GapRecoveryRequest {
        GapRecoveryRequest {
            generation,
            market: market(symbol),
            reason: GapReason::TransportClosed,
            predecessor_event_time: predecessor_event_time.map(timestamp),
            predecessor_received_at: predecessor_event_time.map(timestamp),
            trade_predecessor_event_time: predecessor_event_time.map(timestamp),
            trade_predecessor_received_at: predecessor_event_time.map(timestamp),
            trade_predecessor_event_id: predecessor_event_time.map(|event_time| {
                trench_core::domain::EventId::new(format!(
                    "trade-predecessor-{symbol}-{generation}-{event_time}"
                ))
                .expect("test event ID must be valid")
            }),
            snapshot_event_id: trench_core::domain::EventId::new(format!(
                "snapshot-{symbol}-{generation}-{snapshot_event_time}"
            ))
            .expect("test event ID must be valid"),
            snapshot_event_time: timestamp(snapshot_event_time),
            snapshot_received_at: timestamp(snapshot_event_time),
            reconnect_attempt,
        }
    }

    fn trade(
        symbol: &str,
        event_time: i128,
        trade_id: u64,
        price: rust_decimal::Decimal,
    ) -> MarketEvent {
        MarketEvent::trade(
            timestamp(event_time),
            timestamp(event_time),
            market(symbol),
            Trade::new(
                trade_id,
                Side::Buy,
                Price::new(price).expect("test price must be valid"),
                Quantity::new(Decimal::ONE).expect("test quantity must be valid"),
            )
            .expect("test trade must be valid"),
        )
        .expect("test trade event must be valid")
    }

    fn official_closed_candles(
        symbol: &str,
        fifteen_minute_close: &str,
        one_hour_close: &str,
    ) -> Vec<crate::normalize::Candle> {
        let fifteen_minute_rows = (0_i64..4)
            .map(|index| {
                let open_time = BASE_MS + (index * 900_000);
                if index == 0 {
                    json!({
                        "t": open_time,
                        "T": open_time + 899_999,
                        "s": symbol,
                        "i": "15m",
                        "o": "100",
                        "c": fifteen_minute_close,
                        "h": fifteen_minute_close,
                        "l": "100",
                        "v": "1",
                        "n": 1,
                    })
                } else {
                    json!({
                        "t": open_time,
                        "T": open_time + 899_999,
                        "s": symbol,
                        "i": "15m",
                        "o": "100",
                        "c": "100",
                        "h": "100",
                        "l": "100",
                        "v": "0",
                        "n": 0,
                    })
                }
            })
            .collect::<Vec<_>>();
        let body = json!(fifteen_minute_rows).to_string();
        let mut candles = decode_candles(
            body.as_bytes(),
            &market(symbol),
            CandleInterval::FifteenMinutes,
            TimeRange::new(BASE_MS, BASE_MS + 3_600_000).expect("test range must be valid"),
        )
        .expect("test fifteen-minute candle must decode");
        let body = json!([{
            "t": BASE_MS,
            "T": BASE_MS + 3_599_999,
            "s": symbol,
            "i": "1h",
            "o": "100",
            "c": one_hour_close,
            "h": one_hour_close,
            "l": "100",
            "v": "1",
            "n": 1,
        }])
        .to_string();
        candles.extend(
            decode_candles(
                body.as_bytes(),
                &market(symbol),
                CandleInterval::OneHour,
                TimeRange::new(BASE_MS, BASE_MS + 3_600_000).expect("test range must be valid"),
            )
            .expect("test one-hour candle must decode"),
        );
        candles
    }

    fn l2_batch() -> crate::archive::ArchiveBatch {
        let root = TempDir::new().expect("create archive root");
        let relative_path = PathBuf::from("market_data/20230916/9/l2Book/SOL.lz4");
        let destination = root.path().join(&relative_path);
        fs::create_dir_all(
            destination
                .parent()
                .expect("fixture archive path must have a parent"),
        )
        .expect("create fixture archive directories");
        fs::write(&destination, L2_FIXTURE).expect("write immutable L2 fixture");
        let span = ArchiveSpan::new(
            market("SOL"),
            ArchiveDataKind::L2Book,
            1_694_854_800_000,
            1_694_858_400_000,
        )
        .expect("fixture span must be valid");
        let source = ArchiveSource::new(
            span.clone(),
            relative_path,
            u64::try_from(L2_FIXTURE.len()).expect("fixture length fits u64"),
            ArchiveDigest::of_bytes(L2_FIXTURE),
        );
        let manifest = ArchiveManifest::new(
            1_694_862_000_000,
            [ArchiveRequirement::required(span)],
            [source],
        )
        .expect("fixture manifest must be valid");
        ArchiveReader::open(root.path(), manifest)
            .expect("fixture archive must open")
            .read_all()
            .expect("fixture archive must decode")
    }

    #[test]
    fn reconciled_evidence_is_fifo_and_does_not_create_readiness() {
        let btc = request("BTC", 1, Some(BASE_NS), BASE_NS + 3_600_000_000_000, 1);
        let eth = request("ETH", 1, Some(BASE_NS), BASE_NS + 3_600_000_000_000, 1);
        let local_trades = [trade("BTC", BASE_NS + 1, 1, Decimal::from(100))];
        let official = official_closed_candles("BTC", "100", "100");
        let mut recovery = GapRecovery::new();
        let mut candles = CandleAggregator::new();

        recovery
            .enqueue(btc.clone())
            .expect("first request must queue");
        recovery
            .enqueue(eth.clone())
            .expect("independent market must queue");
        let result = recovery
            .process_next(
                RecoveryEvidence::Reconciled {
                    local_trades: &local_trades,
                    official_candles: &official,
                },
                &mut candles,
            )
            .expect("matching evidence must process")
            .expect("first request must produce a result");

        assert_eq!(result.request(), &btc);
        assert!(
            matches!(
                result.status(),
                RecoveryStatus::Reconciled { candles } if candles.len() == 2
            ),
            "unexpected recovery result: {result:#?}"
        );
        assert_eq!(
            result.source(),
            RecoverySource::LocalTradesAndOfficialCandles
        );
        assert_eq!(result.backfill_events(), local_trades);
        assert_eq!(recovery.pending_len(), 1);
        assert!(candles.unavailable_gaps().is_empty());

        let second = recovery
            .process_next(
                RecoveryEvidence::Unavailable {
                    reason: RecoveryUnavailable::MissingTradeEvidence,
                },
                &mut candles,
            )
            .expect("unavailable evidence is an explicit result")
            .expect("second request must produce a result");
        assert_eq!(second.request(), &eth);
        assert!(matches!(
            second.status(),
            RecoveryStatus::Unavailable {
                reason: RecoveryUnavailable::MissingTradeEvidence
            }
        ));
        assert_eq!(recovery.pending_len(), 0);
    }

    #[test]
    fn conflict_and_archive_l2_only_mark_unavailable_without_synthesizing_candles() {
        let btc = request("BTC", 1, Some(BASE_NS), BASE_NS + 3_600_000_000_000, 1);
        let local_trade = trade("BTC", BASE_NS + 1, 1, Decimal::from(100));
        let conflict = official_closed_candles("BTC", "101", "100");
        let mut recovery = GapRecovery::new();
        let mut candles = CandleAggregator::new();
        recovery.enqueue(btc).expect("request must queue");

        let conflict = recovery
            .process_next(
                RecoveryEvidence::Reconciled {
                    local_trades: &[local_trade],
                    official_candles: &conflict,
                },
                &mut candles,
            )
            .expect("mismatch must become an explicit unavailable result")
            .expect("queued request must resolve");
        assert!(matches!(
            conflict.status(),
            RecoveryStatus::Unavailable {
                reason: RecoveryUnavailable::CandleConflict
            }
        ));
        assert_eq!(candles.unavailable_gaps().len(), 1);

        let sol = request(
            "SOL",
            2,
            Some(BASE_NS + 3_600_000_000_000),
            BASE_NS + 7_200_000_000_000,
            1,
        );
        recovery.enqueue(sol).expect("second request must queue");
        let batch = l2_batch();
        let archive = recovery
            .process_next(RecoveryEvidence::ArchiveL2(&batch), &mut candles)
            .expect("L2-only archive must remain processable")
            .expect("archive request must resolve");
        assert!(matches!(
            archive.status(),
            RecoveryStatus::Unavailable {
                reason: RecoveryUnavailable::ArchiveL2Only
            }
        ));
        assert_eq!(candles.unavailable_gaps().len(), 2);
    }

    #[test]
    fn mid_interval_recovery_is_explicitly_unavailable_not_falsely_reconciled() {
        let request = request("BTC", 1, Some(BASE_NS), BASE_NS + 1, 1);
        let mut recovery = GapRecovery::new();
        let mut candles = CandleAggregator::new();
        recovery.enqueue(request).expect("request must queue");

        let result = recovery
            .process_next(
                RecoveryEvidence::Reconciled {
                    local_trades: &[],
                    official_candles: &[],
                },
                &mut candles,
            )
            .expect("incomplete sleeves must produce an explicit result")
            .expect("request must resolve conservatively");
        assert!(matches!(
            result.status(),
            RecoveryStatus::Unavailable {
                reason: RecoveryUnavailable::IncompleteSleeves
            }
        ));
        assert!(result.backfill_events().is_empty());
        assert_eq!(candles.unavailable_gaps().len(), 1);
        assert!(matches!(
            candles.ingest(&trade("BTC", BASE_NS, 99, Decimal::from(100))),
            Err(trench_core::candle::CandleError::TradeWithinUnavailableGap { .. })
        ));
    }

    #[test]
    fn missing_trade_predecessor_is_unavailable_even_at_an_exact_hour_boundary() {
        let request = request("BTC", 1, None, BASE_NS + 3_600_000_000_000, 1);
        let mut recovery = GapRecovery::new();
        let mut candles = CandleAggregator::new();
        recovery.enqueue(request).expect("request must queue");

        let result = recovery
            .process_next(
                RecoveryEvidence::Reconciled {
                    local_trades: &[],
                    official_candles: &[],
                },
                &mut candles,
            )
            .expect("unbounded history must produce an explicit result")
            .expect("request must resolve conservatively");
        assert!(matches!(
            result.status(),
            RecoveryStatus::Unavailable {
                reason: RecoveryUnavailable::MissingTradePredecessor
            }
        ));
        assert_eq!(candles.unavailable_gaps()[0].start(), None);
    }

    #[test]
    fn incomplete_official_candle_coverage_is_unavailable_at_an_exact_boundary() {
        let request = request("BTC", 1, Some(BASE_NS), BASE_NS + 3_600_000_000_000, 1);
        let mut recovery = GapRecovery::new();
        let mut candles = CandleAggregator::new();
        recovery.enqueue(request).expect("request must queue");

        let result = recovery
            .process_next(
                RecoveryEvidence::Reconciled {
                    local_trades: &[],
                    official_candles: &[],
                },
                &mut candles,
            )
            .expect("incomplete official evidence must produce an explicit result")
            .expect("request must resolve conservatively");
        assert!(matches!(
            result.status(),
            RecoveryStatus::Unavailable {
                reason: RecoveryUnavailable::CandleCoverageUnavailable
            }
        ));
        assert!(result.backfill_events().is_empty());
        assert_eq!(candles.unavailable_gaps().len(), 1);
    }

    #[test]
    fn recovery_accepts_a_distinct_trade_at_the_predecessors_exchange_timestamp() {
        let mut candidates = (1..=3)
            .map(|trade_id| trade("BTC", BASE_NS, trade_id, Decimal::from(100)))
            .collect::<Vec<_>>();
        candidates.sort_by(|left, right| left.event_id().cmp(right.event_id()));
        let predecessor = candidates.remove(0);
        let recovered = candidates.remove(0);
        let mut request = request("BTC", 1, Some(BASE_NS), BASE_NS + 3_600_000_000_000, 1);
        request.trade_predecessor_event_id = Some(predecessor.event_id().clone());
        let official = official_closed_candles("BTC", "100", "100");
        let mut recovery = GapRecovery::new();
        let mut candles = CandleAggregator::new();
        recovery.enqueue(request).expect("request must queue");

        let result = recovery
            .process_next(
                RecoveryEvidence::Reconciled {
                    local_trades: std::slice::from_ref(&recovered),
                    official_candles: &official,
                },
                &mut candles,
            )
            .expect("trade after the full predecessor cursor must be accepted")
            .expect("request must resolve");
        assert!(matches!(result.status(), RecoveryStatus::Reconciled { .. }));
        assert_eq!(result.backfill_events(), std::slice::from_ref(&recovered));
    }

    #[test]
    fn trade_recovery_uses_the_trade_cursor_not_a_later_generic_feed_cursor() {
        let mut request = request("BTC", 1, Some(BASE_NS), BASE_NS + 3_600_000_000_000, 1);
        request.predecessor_event_time = Some(timestamp(BASE_NS + 1_800_000_000_000));
        request.predecessor_received_at = Some(timestamp(BASE_NS + 1_800_000_000_000));
        let trades = [trade("BTC", BASE_NS + 1, 1, Decimal::from(100))];
        let official = official_closed_candles("BTC", "100", "100");
        let mut recovery = GapRecovery::new();
        let mut candles = CandleAggregator::new();
        recovery
            .enqueue(request)
            .expect("generic feed cursor must not reject a valid trade gap");

        let result = recovery
            .process_next(
                RecoveryEvidence::Reconciled {
                    local_trades: &trades,
                    official_candles: &official,
                },
                &mut candles,
            )
            .expect("trade cursor must delimit the recovered evidence")
            .expect("request must resolve");
        assert!(matches!(result.status(), RecoveryStatus::Reconciled { .. }));
    }

    #[test]
    fn rejects_forged_duplicate_and_out_of_order_recovery_requests() {
        let valid = request("BTC", 2, Some(BASE_NS), BASE_NS + 1, 1);
        let mut recovery = GapRecovery::new();
        recovery
            .enqueue(valid.clone())
            .expect("valid request must queue");
        assert!(matches!(
            recovery.enqueue(valid),
            Err(RecoveryError::DuplicateRequest { .. })
        ));
        assert!(matches!(
            recovery.enqueue(request("BTC", 1, Some(BASE_NS), BASE_NS + 2, 1)),
            Err(RecoveryError::OutOfOrderGeneration { .. })
        ));
        assert!(matches!(
            GapRecovery::new().enqueue(request("ETH", 1, Some(BASE_NS + 2), BASE_NS + 1, 1)),
            Err(RecoveryError::InvalidCursor { .. })
        ));
        assert!(matches!(
            GapRecovery::new().enqueue(request("SOL", 1, Some(BASE_NS), BASE_NS + 1, 0)),
            Err(RecoveryError::InvalidCursor { .. })
        ));
    }

    #[test]
    fn outstanding_recovery_work_has_a_hard_capacity() {
        let mut recovery = GapRecovery::new();
        for index in 0..MAX_OUTSTANDING_RECOVERY_REQUESTS {
            recovery
                .enqueue(request(
                    &format!("M{index}"),
                    1,
                    Some(BASE_NS),
                    BASE_NS + 1,
                    1,
                ))
                .expect("request below capacity must queue");
        }
        assert_eq!(recovery.pending_len(), MAX_OUTSTANDING_RECOVERY_REQUESTS);
        assert!(matches!(
            recovery.enqueue(request("OVERFLOW", 1, Some(BASE_NS), BASE_NS + 1, 1)),
            Err(RecoveryError::QueueCapacity { .. })
        ));
    }

    #[test]
    fn evidence_outside_the_exact_gap_is_rejected_without_popping_work() {
        let request = request("BTC", 1, Some(BASE_NS), BASE_NS + 900_000_000_000, 1);
        let too_early = trade("BTC", BASE_NS, 1, Decimal::from(100));
        let mut recovery = GapRecovery::new();
        let mut candles = CandleAggregator::new();
        recovery.enqueue(request).expect("request must queue");

        assert!(matches!(
            recovery.process_next(
                RecoveryEvidence::Reconciled {
                    local_trades: &[too_early],
                    official_candles: &[],
                },
                &mut candles,
            ),
            Err(RecoveryError::TradeOutsideGap { .. })
        ));
        assert_eq!(
            recovery.pending_len(),
            1,
            "invalid evidence must not skip FIFO work"
        );
    }

    #[test]
    fn recovery_history_and_evidence_have_hard_bounds() {
        let mut recovery = GapRecovery::new();
        let mut candles = CandleAggregator::new();
        for index in 0..MAX_PROCESSED_RECOVERY_REQUESTS {
            let request = request(&format!("P{index}"), 1, Some(BASE_NS), BASE_NS + 1, 1);
            recovery
                .enqueue(request)
                .expect("request below processed cap must queue");
            recovery
                .process_next(
                    RecoveryEvidence::Unavailable {
                        reason: RecoveryUnavailable::MissingTradeEvidence,
                    },
                    &mut candles,
                )
                .expect("unavailable work below processed cap must process");
        }
        assert!(matches!(
            recovery.enqueue(request("P_OVERFLOW", 1, Some(BASE_NS), BASE_NS + 1, 1)),
            Err(RecoveryError::ProcessedHistoryCapacity { .. })
        ));

        let request = request("BTC", 1, Some(BASE_NS), BASE_NS + 3_600_000_000_000, 1);
        let oversized =
            vec![trade("BTC", BASE_NS + 1, 1, Decimal::from(100)); MAX_RECOVERY_LOCAL_TRADES + 1];
        let mut bounded = GapRecovery::new();
        bounded.enqueue(request).expect("request must queue");
        assert!(matches!(
            bounded.process_next(
                RecoveryEvidence::Reconciled {
                    local_trades: &oversized,
                    official_candles: &[],
                },
                &mut CandleAggregator::new(),
            ),
            Err(RecoveryError::EvidenceCapacity {
                field: "local_trades",
                ..
            })
        ));
    }
}
