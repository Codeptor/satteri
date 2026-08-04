//! Typed market-data routing for the daemon authority loop.

use std::collections::BTreeMap;

use blake3::Hasher;
use thiserror::Error;
use trench_core::book::{BookError, OrderBook};
use trench_core::domain::{EventId, Market, Price};
use trench_core::engine::EngineEvent;
use trench_core::event::{DurationNs, FundingRate, MarketEvent, MarketEventKind, TimestampNs};
use trench_hyperliquid::{
    GapEvent, GapRecoveryRequest, RecoveryResult, RecoveryStatus, RecoveryUnavailable,
};

/// Why a source fact cannot reach an executable engine path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ExecutionBlocker {
    /// No independently verified recovery boundary exists for the market.
    RecoveryUnverified,
    /// A bounded recovery recorded a conservative unavailable result.
    RecoveryUnavailable(RecoveryUnavailable),
}

/// One normalized source fact's executable routing result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum MarketRoute {
    /// One or more typed core inputs, in strict authority order.
    Engine(Vec<TypedEngineEvent>),
    /// A source fact retained in Parquet but barred from execution.
    Blocked {
        /// Affected native-perpetual market.
        market: Market,
        /// Exact fail-closed reason.
        reason: ExecutionBlocker,
    },
}

impl MarketRoute {
    /// Borrows the typed engine inputs when execution was authorized.
    #[must_use]
    #[cfg(test)]
    pub(crate) fn events(&self) -> Option<&[TypedEngineEvent]> {
        match self {
            Self::Engine(events) => Some(events),
            Self::Blocked { .. } => None,
        }
    }
}

/// One owned non-entry input for the pure engine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TypedEngineEvent {
    /// A source fact that advances only the explicit broker clock.
    AdvanceTime {
        /// Immutable normalized source fact.
        source: MarketEvent,
    },
    /// A durable record that one fresh L2 snapshot opened a recovery fence.
    ///
    /// This is deliberately only a source-clock transition. It cannot restore
    /// execution; only independently reconciled evidence may produce
    /// [`Self::MarketRecovered`].
    RecoveryRequested {
        /// Deterministic recovery-request identity.
        event_id: EventId,
        /// Fresh snapshot time at which the request was emitted.
        at: TimestampNs,
        /// Affected market.
        market: Market,
        /// Monotonic gap generation scoped to the market.
        generation: u64,
        /// Immutable identity of the fresh L2 snapshot that anchors recovery.
        snapshot_event_id: EventId,
    },
    /// A verified recovery boundary completed for one market.
    MarketRecovered {
        /// Deterministic recovery completion identity.
        event_id: EventId,
        /// Boundary before which execution remains forbidden.
        at: TimestampNs,
        /// Market whose source continuity was reconciled.
        market: Market,
        /// Fresh snapshot that anchors this completion.
        snapshot_event_id: EventId,
    },
    /// A recovered, validated full-depth execution book.
    ExecutableBook {
        /// Immutable normalized source fact.
        source: MarketEvent,
        /// Validated visible book derived directly from that source fact.
        book: OrderBook,
    },
    /// A verified venue mark from one asset-context observation.
    MarketMark {
        /// Immutable normalized source fact.
        source: MarketEvent,
        /// Venue mark price from the source context.
        price: Price,
    },
    /// A verified signed funding observation.
    FundingObserved {
        /// Immutable normalized source fact.
        source: MarketEvent,
        /// Signed venue funding rate.
        rate: FundingRate,
        /// Contemporaneous venue mark used for funding notional.
        mark_price: Price,
    },
}

impl TypedEngineEvent {
    /// Returns the immutable source/recovery identity used by SQLite admission.
    #[must_use]
    pub(crate) fn event_id(&self) -> &EventId {
        match self {
            Self::AdvanceTime { source }
            | Self::ExecutableBook { source, .. }
            | Self::MarketMark { source, .. }
            | Self::FundingObserved { source, .. } => source.event_id(),
            Self::RecoveryRequested { event_id, .. } | Self::MarketRecovered { event_id, .. } => {
                event_id
            }
        }
    }

    /// Returns the explicit engine as-of time.
    #[must_use]
    pub(crate) fn at(&self) -> TimestampNs {
        match self {
            Self::AdvanceTime { source }
            | Self::ExecutableBook { source, .. }
            | Self::MarketMark { source, .. }
            | Self::FundingObserved { source, .. } => source.event_time(),
            Self::RecoveryRequested { at, .. } | Self::MarketRecovered { at, .. } => *at,
        }
    }

    /// Returns the market scoped by this source or recovery transition.
    #[must_use]
    pub(crate) fn market(&self) -> &Market {
        match self {
            Self::AdvanceTime { source }
            | Self::ExecutableBook { source, .. }
            | Self::MarketMark { source, .. }
            | Self::FundingObserved { source, .. } => source.market(),
            Self::RecoveryRequested { market, .. } | Self::MarketRecovered { market, .. } => market,
        }
    }

    /// Returns the stable source category retained beside the engine batch.
    #[must_use]
    pub(crate) const fn source_kind(&self) -> &'static str {
        match self {
            Self::AdvanceTime { .. } => "source_clock",
            Self::RecoveryRequested { .. } => "recovery_request",
            Self::MarketRecovered { .. } => "market_recovered",
            Self::ExecutableBook { .. } => "executable_book",
            Self::MarketMark { .. } => "market_mark",
            Self::FundingObserved { .. } => "funding_observed",
        }
    }

    /// Returns compact, canonical, secret-free source evidence.
    pub(crate) fn source_payload_json(&self) -> Result<String, RoutingError> {
        let payload = match self {
            Self::AdvanceTime { source } => serde_json::json!({
                "schema_version": 1,
                "event_id": source.event_id().as_str(),
                "market": source.market().as_str(),
                "event_time_ns": source.event_time().value(),
                "received_at_ns": source.received_at().value(),
                "kind": "source_clock",
            }),
            Self::RecoveryRequested {
                event_id,
                at,
                market,
                generation,
                snapshot_event_id,
            } => serde_json::json!({
                "schema_version": 1,
                "event_id": event_id.as_str(),
                "market": market.as_str(),
                "event_time_ns": at.value(),
                "kind": "recovery_request",
                "generation": generation,
                "snapshot_event_id": snapshot_event_id.as_str(),
            }),
            Self::MarketRecovered {
                event_id,
                at,
                market,
                snapshot_event_id,
            } => serde_json::json!({
                "schema_version": 1,
                "event_id": event_id.as_str(),
                "market": market.as_str(),
                "event_time_ns": at.value(),
                "kind": "market_recovered",
                "snapshot_event_id": snapshot_event_id.as_str(),
            }),
            Self::ExecutableBook { source, book } => serde_json::json!({
                "schema_version": 1,
                "event_id": source.event_id().as_str(),
                "market": source.market().as_str(),
                "event_time_ns": source.event_time().value(),
                "received_at_ns": source.received_at().value(),
                "kind": "executable_book",
                "book_digest": book.commitment_digest(),
            }),
            Self::MarketMark { source, price } => serde_json::json!({
                "schema_version": 1,
                "event_id": source.event_id().as_str(),
                "market": source.market().as_str(),
                "event_time_ns": source.event_time().value(),
                "received_at_ns": source.received_at().value(),
                "kind": "market_mark",
                "price": price.value().to_string(),
            }),
            Self::FundingObserved {
                source,
                rate,
                mark_price,
            } => serde_json::json!({
                "schema_version": 1,
                "event_id": source.event_id().as_str(),
                "market": source.market().as_str(),
                "event_time_ns": source.event_time().value(),
                "received_at_ns": source.received_at().value(),
                "kind": "funding_observed",
                "rate": rate.value().to_string(),
                "mark_price": mark_price.value().to_string(),
            }),
        };
        serde_json::to_string(&payload).map_err(RoutingError::Json)
    }

    /// Converts the owned source route to the matching pure engine input.
    #[must_use]
    pub(crate) fn into_engine_event(self) -> EngineEvent<'static> {
        match self {
            Self::AdvanceTime { source } => EngineEvent::AdvanceTime {
                event_id: source.event_id().clone(),
                at: source.event_time(),
            },
            Self::RecoveryRequested { event_id, at, .. } => {
                EngineEvent::AdvanceTime { event_id, at }
            }
            Self::MarketRecovered {
                event_id,
                at,
                market,
                ..
            } => EngineEvent::MarketRecovered {
                event_id,
                at,
                market,
            },
            Self::ExecutableBook { source, book } => EngineEvent::ExecutableBook {
                event_id: source.event_id().clone(),
                at: source.event_time(),
                book,
            },
            Self::MarketMark { source, price } => EngineEvent::MarketMark {
                event_id: source.event_id().clone(),
                at: source.event_time(),
                market: source.market().clone(),
                price,
                event_time: source.event_time(),
                received_at: source.received_at(),
            },
            Self::FundingObserved {
                source,
                rate,
                mark_price,
            } => EngineEvent::FundingObserved {
                event_id: source.event_id().clone(),
                at: source.event_time(),
                market: source.market().clone(),
                venue_at: source.event_time(),
                received_at: source.received_at(),
                rate,
                mark_price,
            },
        }
    }

    /// Builds the only daemon-owned durable request record for a WebSocket
    /// recovery handoff. The request itself remains execution-fenced.
    #[must_use]
    pub(crate) fn recovery_requested(request: &GapRecoveryRequest) -> Self {
        let event_id = recovery_request_event_id(request);
        Self::RecoveryRequested {
            event_id,
            at: request.snapshot_event_time(),
            market: request.market().clone(),
            generation: request.generation(),
            snapshot_event_id: request.snapshot_event_id().clone(),
        }
    }
}

/// Verified recovery evidence reduced to the daemon's execution boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RecoveryCompletion {
    market: Market,
    generation: u64,
    boundary_at: TimestampNs,
    snapshot_event_id: EventId,
    backfill_events: Vec<MarketEvent>,
}

impl RecoveryCompletion {
    fn from_result(result: &RecoveryResult) -> Result<Self, RoutingError> {
        let RecoveryStatus::Reconciled { .. } = result.status() else {
            return Err(RoutingError::RecoveryUnavailable {
                market: result.request().market().clone(),
                reason: match result.status() {
                    RecoveryStatus::Unavailable { reason } => *reason,
                    RecoveryStatus::Reconciled { .. } => unreachable!("matched above"),
                },
            });
        };
        let request = result.request();
        Ok(Self {
            market: request.market().clone(),
            generation: request.generation(),
            boundary_at: result.completed_through(),
            snapshot_event_id: request.snapshot_event_id().clone(),
            backfill_events: result.backfill_events().to_vec(),
        })
    }

    #[cfg(test)]
    fn fixture(market: Market, boundary_at: TimestampNs, snapshot_event_id: EventId) -> Self {
        Self {
            market,
            generation: 1,
            boundary_at,
            snapshot_event_id,
            backfill_events: Vec::new(),
        }
    }
}

/// Stateful typed router owned only by the daemon authority loop.
#[derive(Debug, Clone)]
pub(crate) struct TypedMarketRouter {
    maximum_book_age: DurationNs,
    books: BTreeMap<Market, OrderBook>,
    deferred_books: BTreeMap<Market, (MarketEvent, OrderBook)>,
    gap_generations: BTreeMap<Market, u64>,
    recovered_at: BTreeMap<Market, TimestampNs>,
    unavailable: BTreeMap<Market, RecoveryUnavailable>,
}

fn recovery_request_event_id(request: &GapRecoveryRequest) -> EventId {
    let mut hasher = Hasher::new_derive_key("trench.daemon-recovery-request.v1");
    for component in [
        request.market().as_str(),
        &request.generation().to_string(),
        request.snapshot_event_id().as_str(),
    ] {
        hasher.update(&(component.len() as u64).to_be_bytes());
        hasher.update(component.as_bytes());
    }
    EventId::new(format!("b3:{}", hasher.finalize().to_hex()))
        .expect("BLAKE3 recovery-request identity is a valid event ID")
}

impl TypedMarketRouter {
    /// Creates an empty authority-local router using the broker's fixed freshness bound.
    #[must_use]
    pub(crate) fn new(maximum_book_age: DurationNs) -> Self {
        Self {
            maximum_book_age,
            books: BTreeMap::new(),
            deferred_books: BTreeMap::new(),
            gap_generations: BTreeMap::new(),
            recovered_at: BTreeMap::new(),
            unavailable: BTreeMap::new(),
        }
    }

    /// Opens a source-continuity gap and immediately fences executable routing.
    pub(crate) fn open_gap(&mut self, gap: &GapEvent) {
        let (market, generation) = match gap {
            GapEvent::Opened(opened) => (opened.market(), opened.generation()),
            GapEvent::ReconnectExhausted(exhausted) => (exhausted.market(), exhausted.generation()),
        };
        self.gap_generations.insert(market.clone(), generation);
        self.recovered_at.remove(market);
        self.unavailable.remove(market);
        self.deferred_books.remove(market);
    }

    #[cfg(test)]
    pub(crate) fn open_gap_for_test(&mut self, market: Market) {
        self.gap_generations.insert(market.clone(), 1);
        self.recovered_at.remove(&market);
        self.unavailable.remove(&market);
        self.deferred_books.remove(&market);
    }

    /// Routes exactly one normalized source fact without inventing execution semantics.
    pub(crate) fn route_market_event(
        &mut self,
        source: MarketEvent,
        open_position_market: Option<&Market>,
    ) -> Result<MarketRoute, RoutingError> {
        match source.kind() {
            MarketEventKind::BookSnapshot(_) => self.route_book(source),
            MarketEventKind::AssetContext(context) => {
                if let Some(reason) = self.execution_blocker(source.market())
                    && open_position_market != Some(source.market())
                {
                    return Ok(MarketRoute::Blocked {
                        market: source.market().clone(),
                        reason,
                    });
                }
                Ok(MarketRoute::Engine(vec![TypedEngineEvent::MarketMark {
                    price: context.mark_price(),
                    source,
                }]))
            }
            MarketEventKind::Funding(funding) => {
                if let Some(reason) = self.execution_blocker(source.market()) {
                    return Ok(MarketRoute::Blocked {
                        market: source.market().clone(),
                        reason,
                    });
                }
                Ok(MarketRoute::Engine(vec![
                    TypedEngineEvent::FundingObserved {
                        rate: funding.rate(),
                        mark_price: funding.mark_price(),
                        source,
                    },
                ]))
            }
            MarketEventKind::Metadata(_)
            | MarketEventKind::Bbo(_)
            | MarketEventKind::Trade(_)
            | MarketEventKind::CompletedCandle(_) => {
                Ok(MarketRoute::Engine(vec![TypedEngineEvent::AdvanceTime {
                    source,
                }]))
            }
        }
    }

    /// Turns independently reconciled recovery evidence into the only recovery boundary.
    pub(crate) fn route_recovery_result(
        &mut self,
        result: &RecoveryResult,
    ) -> Result<MarketRoute, RoutingError> {
        match RecoveryCompletion::from_result(result) {
            Ok(completion) => Ok(MarketRoute::Engine(self.complete_recovery(completion)?)),
            Err(RoutingError::RecoveryUnavailable { market, reason }) => {
                self.unavailable.insert(market.clone(), reason);
                self.recovered_at.remove(&market);
                self.deferred_books.remove(&market);
                Ok(MarketRoute::Blocked {
                    market,
                    reason: ExecutionBlocker::RecoveryUnavailable(reason),
                })
            }
            Err(error) => Err(error),
        }
    }

    fn route_book(&mut self, source: MarketEvent) -> Result<MarketRoute, RoutingError> {
        let market = source.market().clone();
        let book =
            OrderBook::apply_snapshot(self.books.get(&market), &source, self.maximum_book_age)?;
        if let Some(reason) = self.execution_blocker(&market) {
            // A recovery request is anchored to the first fresh L2 snapshot
            // emitted after its gap. Later books cannot replace that evidence:
            // a mandatory exit must use the first verified executable price.
            self.deferred_books
                .entry(market.clone())
                .or_insert((source, book));
            return Ok(MarketRoute::Blocked { market, reason });
        }
        self.books.insert(market, book.clone());
        Ok(MarketRoute::Engine(vec![
            TypedEngineEvent::ExecutableBook { source, book },
        ]))
    }

    fn complete_recovery(
        &mut self,
        completion: RecoveryCompletion,
    ) -> Result<Vec<TypedEngineEvent>, RoutingError> {
        let market = completion.market.clone();
        if self.gap_generations.get(&market) != Some(&completion.generation) {
            return Err(RoutingError::RecoveryGenerationMismatch { market });
        }
        let Some((source, _)) = self.deferred_books.get(&market) else {
            return Err(RoutingError::MissingRecoverySnapshot { market });
        };
        if source.event_id() != &completion.snapshot_event_id {
            return Err(RoutingError::RecoverySnapshotMismatch {
                market,
                expected: completion.snapshot_event_id,
                actual: source.event_id().clone(),
            });
        }
        self.deferred_books
            .remove(&market)
            .ok_or(RoutingError::MissingRecoverySnapshot {
                market: market.clone(),
            })?;
        let event_id = recovery_event_id(&completion);
        let RecoveryCompletion {
            market,
            generation: _,
            boundary_at,
            snapshot_event_id,
            backfill_events,
        } = completion;
        let mut events = backfill_events
            .into_iter()
            .map(|source| TypedEngineEvent::AdvanceTime { source })
            .collect::<Vec<_>>();
        events.push(TypedEngineEvent::MarketRecovered {
            event_id,
            at: boundary_at,
            market: market.clone(),
            snapshot_event_id,
        });
        // The immutable snapshot proves this recovery request, but evidence
        // may complete at a later UTC boundary. It must never become a book
        // for execution when it predates that boundary; wait for a new full L2
        // source fact after `MarketRecovered` instead.
        self.books.remove(&market);
        self.recovered_at.insert(market.clone(), boundary_at);
        self.gap_generations.remove(&market);
        self.unavailable.remove(&market);
        Ok(events)
    }

    fn execution_blocker(&self, market: &Market) -> Option<ExecutionBlocker> {
        if self.recovered_at.contains_key(market) {
            None
        } else if let Some(reason) = self.unavailable.get(market) {
            Some(ExecutionBlocker::RecoveryUnavailable(*reason))
        } else {
            Some(ExecutionBlocker::RecoveryUnverified)
        }
    }
}

fn recovery_event_id(completion: &RecoveryCompletion) -> EventId {
    let mut hasher = Hasher::new_derive_key("trench.daemon-recovery-boundary.v1");
    for component in [
        completion.market.as_str(),
        &completion.generation.to_string(),
        &completion.boundary_at.value().to_string(),
        completion.snapshot_event_id.as_str(),
    ] {
        hasher.update(&(component.len() as u64).to_be_bytes());
        hasher.update(component.as_bytes());
    }
    EventId::new(format!("b3:{}", hasher.finalize().to_hex()))
        .expect("BLAKE3 recovery identity is a valid event ID")
}

/// Typed source-routing failure.
#[derive(Debug, Error)]
pub(crate) enum RoutingError {
    /// A normalized full book failed deterministic validation.
    #[error(transparent)]
    Book(#[from] BookError),
    /// Recovery did not produce reconciled source evidence.
    #[error("market {market:?} recovery is unavailable: {reason:?}")]
    #[allow(
        dead_code,
        reason = "only an independently produced RecoveryResult may construct this unavailable outcome"
    )]
    RecoveryUnavailable {
        /// Affected market.
        market: Market,
        /// Exact conservative recovery conclusion.
        reason: RecoveryUnavailable,
    },
    /// Recovery evidence belonged to a stale or unknown gap generation.
    #[error("market {market:?} recovery generation does not match the pending gap")]
    RecoveryGenerationMismatch {
        /// Affected market.
        market: Market,
    },
    /// Recovery completed before the requested fresh L2 snapshot was retained.
    #[error("market {market:?} recovery has no retained fresh L2 snapshot")]
    MissingRecoverySnapshot {
        /// Affected market.
        market: Market,
    },
    /// Recovery evidence did not match the exact fresh snapshot it claimed to reconcile.
    #[error("market {market:?} recovery snapshot identity did not match the retained L2 source")]
    RecoverySnapshotMismatch {
        /// Affected market.
        market: Market,
        /// Expected request snapshot identity.
        expected: EventId,
        /// Retained source snapshot identity.
        actual: EventId,
    },
    /// Compact source evidence could not be serialized.
    #[error("typed source evidence could not be serialized")]
    Json(#[source] serde_json::Error),
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use rust_decimal::Decimal;
    use trench_core::book::OrderBook;
    use trench_core::broker::{BrokerConfig, BrokerRunContext, PaperBroker};
    use trench_core::domain::{LedgerId, Market, Price, Quantity, RunId, Usdc};
    use trench_core::engine::{Engine, EngineContext, EngineEvent, EngineState};
    use trench_core::event::{
        AssetContext, BookLevel, BookSnapshot, DurationNs, Funding, FundingRate, MarketEvent,
        TimestampNs,
    };
    use trench_core::ledger::LedgerState;

    use super::{MarketRoute, RecoveryCompletion, TypedEngineEvent, TypedMarketRouter};

    fn timestamp(value: i64) -> TimestampNs {
        TimestampNs::new(i128::from(value)).expect("fixture timestamp")
    }

    fn market() -> Market {
        Market::new("SOL").expect("fixture market")
    }

    fn price(value: i64) -> Price {
        Price::new(Decimal::from(value)).expect("fixture price")
    }

    fn quantity(value: i64) -> Quantity {
        Quantity::new(Decimal::from(value)).expect("fixture quantity")
    }

    fn snapshot(at: i64, sequence: u64) -> MarketEvent {
        snapshot_for(market(), at, sequence)
    }

    fn snapshot_for(market: Market, at: i64, sequence: u64) -> MarketEvent {
        MarketEvent::book_snapshot(
            timestamp(at),
            timestamp(at),
            market,
            BookSnapshot::new(
                sequence,
                vec![BookLevel::new(price(99), quantity(10))],
                vec![BookLevel::new(price(101), quantity(10))],
            ),
        )
        .expect("fixture book")
    }

    fn initial_state(at: TimestampNs) -> EngineState {
        let ledger = LedgerState::new(LedgerId::RulesOnly, at).expect("fixture ledger");
        let broker = PaperBroker::new(
            BrokerConfig::new(
                Usdc::new(Decimal::ONE).expect("fixture USDC"),
                DurationNs::new(1_000_000_000).expect("fixture duration"),
            )
            .expect("fixture broker config"),
            BrokerRunContext::new(
                RunId::new("run-router-test").expect("fixture run"),
                "a".repeat(64),
                "b".repeat(64),
            )
            .expect("fixture broker context"),
            at,
        );
        EngineState::new(ledger, broker, BTreeMap::new())
    }

    #[test]
    fn book_is_fenced_until_recovery_then_requires_a_post_boundary_snapshot() {
        let mut router =
            TypedMarketRouter::new(DurationNs::new(1_000_000_000).expect("fixture maximum age"));
        let book = snapshot(20, 1);

        router.open_gap_for_test(market());
        assert!(
            router
                .route_market_event(book.clone(), None)
                .expect("typed source route")
                .events()
                .is_none()
        );

        let events = router
            .complete_recovery(RecoveryCompletion::fixture(
                market(),
                timestamp(10),
                book.event_id().clone(),
            ))
            .expect("recovery completion")
            .into_iter()
            .collect::<Vec<_>>();
        assert!(matches!(
            events[0],
            super::TypedEngineEvent::MarketRecovered { .. }
        ));
        assert!(matches!(
            router
                .route_market_event(snapshot(21, 2), None)
                .expect("post-recovery source route")
                .events()
                .expect("post-recovery book route"),
            [super::TypedEngineEvent::ExecutableBook { .. }]
        ));
    }

    #[test]
    fn recovery_preserves_its_first_snapshot_as_proof_but_never_executes_it() {
        let mut router =
            TypedMarketRouter::new(DurationNs::new(1_000_000_000).expect("fixture maximum age"));
        let anchor = snapshot(20, 1);
        let later = snapshot(21, 2);
        let post_boundary = snapshot(30, 3);

        router.open_gap_for_test(market());
        let _ = router
            .route_market_event(anchor.clone(), None)
            .expect("anchor snapshot must fence");
        let _ = router
            .route_market_event(later, None)
            .expect("later snapshot must stay fenced");

        let events = router
            .complete_recovery(RecoveryCompletion::fixture(
                market(),
                timestamp(10),
                anchor.event_id().clone(),
            ))
            .expect("anchored recovery completion")
            .into_iter()
            .collect::<Vec<_>>();
        assert!(matches!(
            events.as_slice(),
            [TypedEngineEvent::MarketRecovered { .. }]
        ));
        let route = router
            .route_market_event(post_boundary.clone(), None)
            .expect("post-boundary book route");
        let Some([TypedEngineEvent::ExecutableBook { source, .. }]) = route.events() else {
            panic!("new post-boundary snapshot must be the only executable book");
        };
        assert_eq!(source.event_id(), post_boundary.event_id());
        assert_ne!(source.event_id(), anchor.event_id());
    }

    #[test]
    fn snapshot_mismatch_keeps_the_anchor_available_for_a_retry() {
        let mut router =
            TypedMarketRouter::new(DurationNs::new(1_000_000_000).expect("fixture maximum age"));
        let anchor = snapshot(20, 1);
        let wrong = snapshot(21, 2);

        router.open_gap_for_test(market());
        let _ = router
            .route_market_event(anchor.clone(), None)
            .expect("anchor snapshot must fence");
        assert!(matches!(
            router.complete_recovery(RecoveryCompletion::fixture(
                market(),
                timestamp(10),
                wrong.event_id().clone(),
            )),
            Err(super::RoutingError::RecoverySnapshotMismatch { .. })
        ));
        assert!(
            router
                .complete_recovery(RecoveryCompletion::fixture(
                    market(),
                    timestamp(10),
                    anchor.event_id().clone(),
                ))
                .is_ok()
        );
    }

    #[test]
    fn context_and_funding_take_typed_engine_paths_after_recovery() {
        let mut router =
            TypedMarketRouter::new(DurationNs::new(1_000_000_000).expect("fixture maximum age"));
        router.open_gap_for_test(market());
        let book = snapshot(20, 1);
        let _ = router
            .route_market_event(book.clone(), None)
            .expect("deferred book");
        let _ = router
            .complete_recovery(RecoveryCompletion::fixture(
                market(),
                timestamp(10),
                book.event_id().clone(),
            ))
            .expect("recovery completion");

        let context = MarketEvent::asset_context(
            timestamp(21),
            timestamp(21),
            market(),
            AssetContext::new(
                price(100),
                price(100),
                Some(price(100)),
                quantity(1),
                Usdc::new(Decimal::ONE).expect("fixture notional"),
                FundingRate::new(Decimal::ZERO),
            ),
        )
        .expect("fixture context");
        let funding = MarketEvent::funding(
            timestamp(22),
            timestamp(22),
            market(),
            Funding::new(FundingRate::new(Decimal::ZERO), price(100)),
        )
        .expect("fixture funding");

        assert!(matches!(
            router
                .route_market_event(context, None)
                .expect("context route")
                .events()
                .expect("typed engine route"),
            [super::TypedEngineEvent::MarketMark { .. }]
        ));
        assert!(matches!(
            router
                .route_market_event(funding, None)
                .expect("funding route")
                .events()
                .expect("typed engine route"),
            [super::TypedEngineEvent::FundingObserved { .. }]
        ));
    }

    #[test]
    fn recovery_fenced_mark_requires_a_matching_open_position() {
        let mut router =
            TypedMarketRouter::new(DurationNs::new(1_000_000_000).expect("fixture maximum age"));
        let protected_market = market();
        let different_market = Market::new("BTC").expect("different fixture market");
        router.open_gap_for_test(protected_market.clone());
        let source = MarketEvent::asset_context(
            timestamp(20),
            timestamp(20),
            protected_market.clone(),
            AssetContext::new(
                price(98),
                price(98),
                Some(price(98)),
                quantity(1),
                Usdc::new(Decimal::ONE).expect("fixture notional"),
                FundingRate::new(Decimal::ZERO),
            ),
        )
        .expect("fixture mark");

        assert!(matches!(
            router
                .route_market_event(source.clone(), Some(&different_market))
                .expect("unexposed mark route"),
            MarketRoute::Blocked {
                reason: super::ExecutionBlocker::RecoveryUnverified,
                ..
            }
        ));
        assert!(matches!(
            router
                .route_market_event(source, Some(&protected_market))
                .expect("protective mark route")
                .events(),
            Some([TypedEngineEvent::MarketMark { .. }])
        ));
    }

    #[test]
    fn gap_stop_mark_waits_for_verified_recovery_before_its_fresh_book_fills() {
        let opened_at = timestamp(900_000_000_000);
        let market = Market::new("BTC").expect("fixture market");
        let mut state = trench_core::engine::test_support::opened_btc_state(opened_at);
        let mut router =
            TypedMarketRouter::new(DurationNs::new(1_000_000_000).expect("fixture maximum age"));

        router.open_gap_for_test(market.clone());
        let stop_at = timestamp(900_000_000_010);
        let stop_mark = MarketEvent::asset_context(
            stop_at,
            stop_at,
            market.clone(),
            AssetContext::new(
                price(98),
                price(98),
                Some(price(98)),
                quantity(1),
                Usdc::new(Decimal::ONE).expect("fixture notional"),
                FundingRate::new(Decimal::ZERO),
            ),
        )
        .expect("fixture stop mark");
        let stop_route = router
            .route_market_event(stop_mark, Some(&market))
            .expect("open position routes its protective mark");
        let MarketRoute::Engine(events) = stop_route else {
            panic!("recovery-fenced position mark must remain protective");
        };
        assert!(matches!(
            events.as_slice(),
            [TypedEngineEvent::MarketMark { .. }]
        ));
        for event in events {
            state = Engine::apply(
                event.into_engine_event(),
                state,
                &EngineContext::passive(trench_core::engine::EventAdmission::New),
            )
            .expect("protective mark must apply")
            .into_parts()
            .0;
        }
        assert_eq!(
            state.broker().state(),
            trench_core::broker::BrokerState::MandatoryExit
        );
        assert!(state.ledger().position().is_some());

        let book = snapshot_for(market.clone(), 900_000_000_012, 2);
        assert!(
            router
                .route_market_event(book.clone(), Some(&market))
                .expect("fresh book must be deferred")
                .events()
                .is_none()
        );
        assert_eq!(
            state.broker().state(),
            trench_core::broker::BrokerState::MandatoryExit
        );

        let recovery_events = router
            .complete_recovery(RecoveryCompletion::fixture(
                market,
                timestamp(900_000_000_011),
                book.event_id().clone(),
            ))
            .expect("verified recovery completion");
        assert!(matches!(
            recovery_events.as_slice(),
            [TypedEngineEvent::MarketRecovered { .. }]
        ));
        for event in recovery_events {
            state = Engine::apply(
                event.into_engine_event(),
                state,
                &EngineContext::passive(trench_core::engine::EventAdmission::New),
            )
            .expect("recovery route must apply")
            .into_parts()
            .0;
        }
        assert_eq!(
            state.broker().state(),
            trench_core::broker::BrokerState::MandatoryExit
        );
        let post_recovery_book = snapshot_for(
            Market::new("BTC").expect("fixture market"),
            900_000_000_013,
            3,
        );
        let MarketRoute::Engine(events) = router
            .route_market_event(
                post_recovery_book,
                Some(&Market::new("BTC").expect("fixture market")),
            )
            .expect("post-recovery book route")
        else {
            panic!("post-recovery book must become executable");
        };
        for event in events {
            state = Engine::apply(
                event.into_engine_event(),
                state,
                &EngineContext::passive(trench_core::engine::EventAdmission::New),
            )
            .expect("post-recovery book must settle the mandatory exit")
            .into_parts()
            .0;
        }
        assert_eq!(
            state.broker().state(),
            trench_core::broker::BrokerState::Flat
        );
        assert!(state.ledger().position().is_none());
    }

    #[test]
    fn recovered_book_reaches_the_passive_engine_path_while_entries_are_unavailable() {
        let mut state = initial_state(timestamp(0));
        let recovered = Engine::apply(
            EngineEvent::MarketRecovered {
                event_id: trench_core::domain::EventId::new("recovery-sol").expect("event"),
                at: timestamp(10),
                market: market(),
            },
            state,
            &EngineContext::passive(trench_core::engine::EventAdmission::New),
        )
        .expect("recovery event");
        state = recovered.into_parts().0;
        let book = OrderBook::apply_snapshot(
            None,
            &snapshot(20, 1),
            DurationNs::new(1_000_000_000).expect("fixture maximum age"),
        )
        .expect("validated book");
        let outcome = Engine::apply(
            EngineEvent::ExecutableBook {
                event_id: book.event_id().clone(),
                at: timestamp(20),
                book,
            },
            state,
            &EngineContext::passive(trench_core::engine::EventAdmission::New),
        )
        .expect("passive executable-book path");
        assert!(outcome.batch().records().iter().any(|record| matches!(
            record.record(),
            trench_core::engine::EngineRecord::EventReceived
        )));
        let mut readiness = crate::readiness::Readiness::default();
        readiness.register_market(market());
        let gates = readiness
            .market_gates_mut(&market())
            .expect("registered market gates");
        gates.set_recovered(true);
        gates.set_executable_book(true);
        assert!(!readiness.rules_entry_ready(&market()));
        assert!(readiness.mandatory_exit_ready(&market()));
    }
}
