//! Bounded daemon lifecycle and startup recovery orchestration.

use std::collections::{BTreeMap, BTreeSet};
use std::future;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use thiserror::Error;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use trench_core::broker::{BrokerConfig, BrokerRunContext, PaperBroker};
use trench_core::candle::CandleAggregator;
use trench_core::config::PaperConfig;
use trench_core::domain::{LedgerId, Market, RunId, Usdc};
use trench_core::engine::{Engine, EngineContext, EngineState};
use trench_core::event::{DurationNs, MarketEvent, TimestampNs};
use trench_core::ledger::LedgerState;
use trench_hyperliquid::{
    GapEvent, GapRecoveryRequest, InfoClient, InfoError, RecoveryEvidenceProducer,
    RecoveryProducerError, RecoveryResult, WsClient, WsConfig, WsOutput, WsStream,
};
use trench_storage::parquet::{DataProvenance, ParquetError, ParquetStore};
use trench_storage::replay::{DeterministicReplay, ReplayError, ReplayPlan};

use crate::admin::{
    AdminError, AdminServer, AuthorityRequest, DaemonMode, DaemonStatus, authority_channel,
};
use crate::commands::RulesStartup;
use crate::execution::{MarketRoute, RoutingError, TypedEngineEvent, TypedMarketRouter};
use crate::readiness::Readiness;
use crate::writer::{EngineWriter, SourceEvent, WriterError};

const SOURCE_CHANNEL_CAPACITY: usize = 128;
const RECOVERY_CHANNEL_CAPACITY: usize = 16;
const MAXIMUM_BOOK_AGE_NS: i64 = 1_000_000_000;

/// One requested daemon lifecycle mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeMode {
    /// Persist/observe public market data only; no entries may be evaluated.
    Collect,
    /// Start the long-lived observation daemon without an active strategy reactor.
    ///
    /// This mode persists typed non-entry transitions but cannot produce a
    /// rules entry until a verified artifact, point-in-time universe, feature,
    /// and recovery inputs are activated together.
    Run,
}

impl From<RuntimeMode> for DaemonMode {
    fn from(value: RuntimeMode) -> Self {
        match value {
            RuntimeMode::Collect | RuntimeMode::Run => Self::CollectionOnly,
        }
    }
}

/// Finite verified deterministic source-replay report.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayReport {
    /// Count of normalized source facts in replay order.
    pub event_count: usize,
    /// Exact content digest over the source replay order.
    pub digest: String,
}

struct RecoveredSource {
    report: ReplayReport,
    events: Vec<MarketEvent>,
}

struct AuthorityState {
    engine_state: Option<EngineState>,
    router: TypedMarketRouter,
    readiness: Readiness,
    reconciled: bool,
    recovery_markets: BTreeSet<Market>,
    recovery_worker_available: bool,
}

/// One authority-approved handoff to the read-only evidence producer.
#[derive(Debug)]
enum RecoveryInput {
    /// A normalized source fact accepted by the sole SQLite writer.
    CommittedSource(MarketEvent),
    /// An immutable WebSocket gap request, already recorded and fenced.
    Request(GapRecoveryRequest),
    /// An explicit source-time watermark for a quiet recovering market.
    AdvanceTime { market: Market, at: TimestampNs },
}

/// One bounded recovery-worker outcome returned to the authority loop.
#[derive(Debug)]
enum RecoveryOutput {
    /// A final reconciled or unavailable recovery conclusion.
    Result(RecoveryResult),
    /// The producer could not safely retain or reconcile its queue head.
    Failed {
        /// Market that remains execution-fenced.
        market: Market,
        /// Exact conservative producer failure.
        error: RecoveryProducerError,
    },
}

/// Starts the paper-only daemon and waits for a bounded duration or Ctrl-C.
///
/// Recovery validates the atomic Parquet source stream before any public feed
/// would be allowed to subscribe. It never parses a debugging checkpoint as
/// executable state and never recreates a prior decision from partial evidence.
pub async fn run(
    physical_config_path: &Path,
    config_bytes: &[u8],
    config: &PaperConfig,
    rules_startup: RulesStartup,
    mode: RuntimeMode,
    duration: Option<Duration>,
) -> Result<(), AppError> {
    let sqlite_path = configured_path(physical_config_path, config.storage().sqlite_path())?;
    let parquet_path = configured_path(physical_config_path, config.storage().parquet_path())?;
    let admin_socket = PathBuf::from(config.runtime().admin_socket_path());
    let provenance = provenance(config_bytes)?;
    let started_at_ns = current_time_ns()?;
    let run_id = run_id(started_at_ns, provenance.config_digest());
    let mut writer = EngineWriter::open(&sqlite_path, run_id, started_at_ns).await?;

    let recovery = recover_source_stream(&parquet_path, provenance.clone())?;
    let initial_at_ns = recovery
        .as_ref()
        .and_then(|recovery| recovery.events.first())
        .map_or(started_at_ns, |event| {
            event.event_time().value().min(started_at_ns)
        });
    let mut authority = AuthorityState {
        engine_state: Some(initial_engine_state(writer.run_id(), initial_at_ns)?),
        router: TypedMarketRouter::new(maximum_book_age()?),
        readiness: Readiness::default(),
        reconciled: false,
        recovery_markets: BTreeSet::new(),
        recovery_worker_available: true,
    };
    authority.readiness.set_storage_writable(true);
    authority
        .readiness
        .set_ntp_synchronized(local_ntp_synchronized());
    authority.readiness.set_rules_configuration_valid(false);
    authority.readiness.set_rules_sleeve_warm(false);
    if let Some(error) = rules_startup.error() {
        tracing::warn!(
            reason = %error,
            "rules are unready; keeping collection and mandatory-exit paths available"
        );
    }
    tracing::info!(
        mode = ?mode,
        "entry reactor is sealed collection-only; missing recovery evidence keeps execution fenced"
    );
    let cancellation = CancellationToken::new();
    let recovery_client = InfoClient::new(config.endpoints().info_url())?;
    let (recovery_sender, recovery_receiver) = mpsc::channel(RECOVERY_CHANNEL_CAPACITY);
    let (recovery_result_sender, mut recovery_result_receiver) =
        mpsc::channel(RECOVERY_CHANNEL_CAPACITY);
    let recovery_task = tokio::spawn(recovery_worker(
        recovery_client,
        recovery_receiver,
        recovery_result_sender,
        cancellation.clone(),
    ));
    let (source_sender, mut source_receiver) = mpsc::channel(SOURCE_CHANNEL_CAPACITY);
    let replay_task = tokio::spawn(replay_producer(
        recovery
            .as_ref()
            .map_or_else(Vec::new, |recovery| recovery.events.clone()),
        source_sender,
        cancellation.clone(),
    ));
    while let Some(event) = source_receiver.recv().await {
        admit_market_event(&mut writer, &mut authority, event, Some(&recovery_sender)).await?;
    }
    replay_task.await.map_err(AppError::ReplayProducerJoin)?;
    authority.reconciled = true;
    authority.readiness.set_sqlite_reconciled(true);
    if let Some(recovery) = &recovery {
        tracing::info!(
            source_events = recovery.report.event_count,
            source_digest = %recovery.report.digest,
            "verified deterministic source progression before subscription"
        );
    } else {
        tracing::info!("fresh SQLite journal has no source partitions to reconstruct");
    }

    let parquet_store = ParquetStore::open(&parquet_path, provenance)?;
    let mut live_stream = open_live_stream(config, &mut authority.readiness).await;
    let server = AdminServer::bind(&admin_socket).await?;
    let (authority_sender, mut authority_receiver) = authority_channel();
    let admin_cancellation = cancellation.clone();
    let admin_task = tokio::spawn(server.serve(authority_sender, admin_cancellation));

    let stop = stop_signal(duration);
    tokio::pin!(stop);
    let mut recovery_results_open = true;
    let mut recovery_clock = tokio::time::interval_at(
        tokio::time::Instant::now() + Duration::from_secs(1),
        Duration::from_secs(1),
    );
    recovery_clock.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            result = &mut stop => {
                result?;
                break;
            }
            request = authority_receiver.recv() => {
                let Some(request) = request else {
                    return Err(AppError::AuthorityChannelClosed);
                };
                match request {
                    AuthorityRequest::Status { respond_to } => {
                        let _ = respond_to.send(DaemonStatus {
                            run_id: writer.run_id().to_owned(),
                            reconciled: authority.reconciled,
                            mode: mode.into(),
                            execution_enabled: false,
                            readiness: authority.readiness.snapshot(),
                        });
                    }
                }
            }
            output = receive_live(&mut live_stream) => {
                match output {
                    Some(WsOutput::MarketEvent(event)) => {
                        parquet_store.write_events(std::slice::from_ref(&event))?;
                        admit_market_event(
                            &mut writer,
                            &mut authority,
                            event,
                            Some(&recovery_sender),
                        ).await?;
                    }
                    Some(WsOutput::Gap(gap)) => {
                        authority.router.open_gap(&gap);
                        let market = gap_market(&gap).clone();
                        mark_market_execution_blocked(&mut authority.readiness, market);
                    }
                    Some(WsOutput::RecoveryRequest(request)) => {
                        let market = request.market().clone();
                        mark_market_execution_blocked(&mut authority.readiness, market);
                        admit_typed_engine_event(
                            &mut writer,
                            &mut authority,
                            TypedEngineEvent::recovery_requested(&request),
                        ).await?;
                        authority.recovery_markets.insert(request.market().clone());
                        authority.recovery_worker_available = submit_recovery_request(
                            &recovery_sender,
                            &mut authority.readiness,
                            request,
                        ).await;
                    }
                    Some(WsOutput::Rejected(_)) => {}
                    Some(WsOutput::Terminal(_)) | None => {
                        authority.readiness.set_stream_connected(false);
                        live_stream = None;
                    }
                }
            }
            output = recovery_result_receiver.recv(), if recovery_results_open => {
                match output {
                    Some(output) => {
                        admit_recovery_output(
                            &mut writer,
                            &mut authority,
                            &parquet_store,
                            output,
                        ).await?;
                    }
                    None => {
                        recovery_results_open = false;
                        authority.recovery_worker_available = false;
                        tracing::error!(
                            "recovery producer stopped; subsequent markets remain execution-fenced"
                        );
                    }
                }
            }
            _ = recovery_clock.tick(), if authority.recovery_worker_available && !authority.recovery_markets.is_empty() => {
                let at = TimestampNs::new(i128::from(current_time_ns()?))
                    .map_err(|_| AppError::SystemTime)?;
                for market in authority.recovery_markets.clone() {
                    if !advance_recovery_clock(&recovery_sender, market, at).await {
                        authority.recovery_worker_available = false;
                        break;
                    }
                }
            }
        }
    }

    cancellation.cancel();
    drop(recovery_sender);
    if let Some(stream) = live_stream {
        stream.shutdown().await;
    }
    recovery_task.await.map_err(AppError::RecoveryTaskJoin)?;
    while let Ok(output) = recovery_result_receiver.try_recv() {
        admit_recovery_output(&mut writer, &mut authority, &parquet_store, output).await?;
    }
    admin_task.await.map_err(AppError::AdminTaskJoin)??;
    let counts = writer.journal_counts().await?;
    tracing::info!(
        run_id = writer.run_id(),
        events = counts.events,
        admissions = counts.admissions,
        checkpoints = counts.checkpoints,
        "daemon shutdown completed after durable journal checkpoint"
    );
    Ok(())
}

/// Opens one explicit Task-14 replay plan without SQLite, network, or mutation.
pub fn replay(
    physical_config_path: &Path,
    config_bytes: &[u8],
    config: &PaperConfig,
    manifest: &Path,
) -> Result<ReplayReport, AppError> {
    let expected = provenance(config_bytes)?;
    let plan = ReplayPlan::read_from(manifest)?;
    if plan.provenance() != &expected {
        return Err(AppError::ReplayProvenanceMismatch);
    }
    let parquet_path = configured_path(physical_config_path, config.storage().parquet_path())?;
    let replay = DeterministicReplay::open_plan(parquet_path, plan)?;
    Ok(ReplayReport {
        event_count: replay.events().len(),
        digest: replay.digest().to_owned(),
    })
}

fn recover_source_stream(
    parquet_path: &Path,
    provenance: DataProvenance,
) -> Result<Option<RecoveredSource>, AppError> {
    let store = ParquetStore::open(parquet_path, provenance.clone())?;
    if store.partitions()?.is_empty() {
        return Ok(None);
    }
    let replay = DeterministicReplay::open(parquet_path, provenance)?;
    Ok(Some(RecoveredSource {
        report: ReplayReport {
            event_count: replay.events().len(),
            digest: replay.digest().to_owned(),
        },
        events: replay.events().to_vec(),
    }))
}

async fn replay_producer(
    events: Vec<MarketEvent>,
    sender: mpsc::Sender<MarketEvent>,
    cancellation: CancellationToken,
) {
    for event in events {
        tokio::select! {
            _ = cancellation.cancelled() => return,
            result = sender.send(event) => {
                if result.is_err() {
                    return;
                }
            }
        }
    }
}

/// Runs the bounded, read-only recovery producer outside the authority loop.
///
/// It owns no SQLite handle, Parquet store, router, or readiness state. Every
/// input is supplied by the authority after durable admission, and every final
/// result is returned over a bounded channel for that same authority to route.
async fn recovery_worker(
    client: InfoClient,
    mut input: mpsc::Receiver<RecoveryInput>,
    output: mpsc::Sender<RecoveryOutput>,
    cancellation: CancellationToken,
) {
    let mut producer = RecoveryEvidenceProducer::new(client);
    let mut candles = CandleAggregator::new();
    loop {
        let next = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return,
            next = input.recv() => next,
        };
        let Some(next) = next else {
            return;
        };

        let market = match &next {
            RecoveryInput::CommittedSource(event) => event.market().clone(),
            RecoveryInput::Request(request) => request.market().clone(),
            RecoveryInput::AdvanceTime { market, .. } => market.clone(),
        };
        let input_result = match next {
            RecoveryInput::CommittedSource(event) => producer.retain_committed_source_event(&event),
            RecoveryInput::Request(request) => producer.enqueue(request).map_err(Into::into),
            RecoveryInput::AdvanceTime { market, at } => {
                producer.advance_time(&market, at);
                Ok(())
            }
        };
        let (result, terminal) = match input_result {
            Err(error) => (RecoveryOutput::Failed { market, error }, true),
            Ok(()) => match producer.process_next(&mut candles, &cancellation).await {
                Ok(Some(result)) => (RecoveryOutput::Result(result), false),
                Ok(None) => continue,
                Err(RecoveryProducerError::Cancelled) => return,
                Err(error) => (RecoveryOutput::Failed { market, error }, true),
            },
        };
        let delivered = tokio::select! {
            biased;
            _ = cancellation.cancelled() => false,
            delivered = output.send(result) => delivered.is_ok(),
        };
        if terminal || !delivered {
            return;
        }
    }
}

/// Sends a source fact only after the authority's atomic engine append.
async fn retain_recovery_source(
    sender: &mpsc::Sender<RecoveryInput>,
    readiness: &mut Readiness,
    event: MarketEvent,
) -> bool {
    let market = event.market().clone();
    if sender
        .send(RecoveryInput::CommittedSource(event))
        .await
        .is_err()
    {
        mark_market_execution_blocked(readiness, market.clone());
        tracing::error!(
            market = market.as_str(),
            "recovery producer is unavailable; retained source evidence cannot be extended"
        );
        return false;
    }
    true
}

/// Hands an already-recorded recovery request to the bounded evidence worker.
async fn submit_recovery_request(
    sender: &mpsc::Sender<RecoveryInput>,
    readiness: &mut Readiness,
    request: GapRecoveryRequest,
) -> bool {
    let market = request.market().clone();
    let generation = request.generation();
    if sender.send(RecoveryInput::Request(request)).await.is_err() {
        mark_market_execution_blocked(readiness, market.clone());
        tracing::error!(
            market = market.as_str(),
            generation,
            "recovery producer is unavailable; market remains execution-fenced"
        );
        return false;
    }
    true
}

/// Advances only recovery evidence watermarks from the daemon's explicit UTC
/// clock. It never reaches the engine, broker, SQLite, or Parquet directly.
async fn advance_recovery_clock(
    sender: &mpsc::Sender<RecoveryInput>,
    market: Market,
    at: TimestampNs,
) -> bool {
    sender
        .send(RecoveryInput::AdvanceTime { market, at })
        .await
        .is_ok()
}

/// Applies only authority-owned recovery output through Parquet, routing, and
/// the sole SQLite writer.
async fn admit_recovery_output(
    writer: &mut EngineWriter,
    authority: &mut AuthorityState,
    parquet_store: &ParquetStore,
    output: RecoveryOutput,
) -> Result<(), AppError> {
    match output {
        RecoveryOutput::Result(result) => {
            let market = result.request().market().clone();
            admit_recovery_result(writer, authority, parquet_store, result).await?;
            authority.recovery_markets.remove(&market);
            Ok(())
        }
        RecoveryOutput::Failed { market, error } => {
            mark_market_execution_blocked(&mut authority.readiness, market.clone());
            authority.recovery_markets.remove(&market);
            authority.recovery_worker_available = false;
            tracing::error!(
                market = market.as_str(),
                error = %error,
                "recovery producer failed; market remains execution-fenced"
            );
            Ok(())
        }
    }
}

/// Persists replayable recovery facts before accepting typed recovery routes.
async fn admit_recovery_result(
    writer: &mut EngineWriter,
    authority: &mut AuthorityState,
    parquet_store: &ParquetStore,
    result: RecoveryResult,
) -> Result<(), AppError> {
    parquet_store.write_events(result.backfill_events())?;
    let mut candidate_router = authority.router.clone();
    match candidate_router.route_recovery_result(&result)? {
        MarketRoute::Engine(events) => {
            for event in events {
                admit_typed_engine_event(writer, authority, event).await?;
            }
        }
        MarketRoute::Blocked { market, reason } => {
            mark_market_execution_blocked(&mut authority.readiness, market.clone());
            tracing::warn!(
                market = market.as_str(),
                reason = ?reason,
                "recovery result remains execution-fenced"
            );
        }
    }
    authority.router = candidate_router;
    Ok(())
}

async fn receive_live(stream: &mut Option<WsStream>) -> Option<WsOutput> {
    match stream {
        Some(stream) => stream.recv().await,
        None => future::pending::<Option<WsOutput>>().await,
    }
}

async fn open_live_stream(config: &PaperConfig, readiness: &mut Readiness) -> Option<WsStream> {
    let client = match InfoClient::new(config.endpoints().info_url()) {
        Ok(client) => client,
        Err(error) => {
            tracing::warn!(error = %error, "public metadata client is unavailable; stream remains unready");
            return None;
        }
    };
    let metadata = match client.meta_and_asset_contexts().await {
        Ok(metadata) => metadata,
        Err(error) => {
            tracing::warn!(error = %error, "public metadata fetch failed; stream remains unready");
            return None;
        }
    };
    let limit = usize::from(config.feed().tradeable_market_count())
        .checked_add(usize::from(config.feed().warm_buffer_market_count()))?;
    let mut markets = metadata
        .assets()
        .iter()
        .filter(|asset| {
            asset.only_isolated()
                && asset.max_leverage().value()
                    >= u32::from(config.risk().minimum_leverage().value())
                && asset.context().day_notional_volume() >= config.feed().minimum_daily_notional()
        })
        .map(|asset| {
            (
                asset.market().clone(),
                asset.context().day_notional_volume().value(),
            )
        })
        .collect::<Vec<_>>();
    markets.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
    let markets = markets
        .into_iter()
        .take(limit)
        .map(|(market, _)| market)
        .collect::<Vec<_>>();
    if markets.is_empty() {
        tracing::warn!("no market passed the collector's conservative public liquidity prefilter");
        return None;
    }
    let fresh_book_markets = markets.iter().cloned().collect();
    for market in &markets {
        readiness.register_market(market.clone());
    }
    readiness.set_fresh_book_markets(fresh_book_markets);
    readiness.set_metadata_current(true);
    match WsConfig::new(markets) {
        Ok(config) => Some(WsClient::new(config).start()),
        Err(error) => {
            tracing::warn!(error = %error, "public WebSocket configuration rejected dynamic universe");
            None
        }
    }
}

fn mark_market_execution_blocked(readiness: &mut Readiness, market: trench_core::domain::Market) {
    readiness.register_market(market.clone());
    if let Some(gates) = readiness.market_gates_mut(&market) {
        gates.set_data_quality_valid(true);
        gates.set_common_features_warm(false);
        gates.set_recovered(false);
        gates.set_executable_book(false);
    }
}

fn update_readiness_from_typed_event(readiness: &mut Readiness, event: &TypedEngineEvent) {
    readiness.set_stream_connected(true);
    readiness.register_market(event.market().clone());
    if let Some(gates) = readiness.market_gates_mut(event.market()) {
        gates.set_data_quality_valid(true);
        gates.set_common_features_warm(false);
        match event {
            TypedEngineEvent::RecoveryRequested { .. } => {
                gates.set_recovered(false);
                gates.set_executable_book(false);
            }
            TypedEngineEvent::MarketRecovered { .. } => {
                gates.set_recovered(true);
                gates.set_executable_book(false);
            }
            TypedEngineEvent::ExecutableBook { .. } => gates.set_executable_book(true),
            TypedEngineEvent::AdvanceTime { .. }
            | TypedEngineEvent::MarketMark { .. }
            | TypedEngineEvent::FundingObserved { .. } => {}
        }
    }
}

fn gap_market(gap: &GapEvent) -> &trench_core::domain::Market {
    match gap {
        GapEvent::Opened(opened) => opened.market(),
        GapEvent::ReconnectExhausted(exhausted) => exhausted.market(),
    }
}

/// Routes one normalized source fact through a typed execution fence.
///
/// Facts that do not carry executable semantics retain an explicit source-clock
/// transition. Funding and books require a verified recovery boundary. A fresh
/// mark may still escalate an already-open position to a mandatory exit, but
/// cannot fill it until a post-recovery book arrives.
async fn admit_market_event(
    writer: &mut EngineWriter,
    authority: &mut AuthorityState,
    event: MarketEvent,
    recovery_sender: Option<&mpsc::Sender<RecoveryInput>>,
) -> Result<(), AppError> {
    let committed_source = event.clone();
    let open_position_market = authority.engine_state.as_ref().and_then(|state| {
        state
            .broker()
            .position()
            .map(|position| position.market().clone())
    });
    match authority
        .router
        .route_market_event(event, open_position_market.as_ref())?
    {
        MarketRoute::Engine(events) => {
            for event in events {
                admit_typed_engine_event(writer, authority, event).await?;
            }
        }
        MarketRoute::Blocked { market, reason } => {
            admit_typed_engine_event(
                writer,
                authority,
                TypedEngineEvent::AdvanceTime {
                    source: committed_source.clone(),
                },
            )
            .await?;
            mark_market_execution_blocked(&mut authority.readiness, market.clone());
            tracing::debug!(
                market = market.as_str(),
                reason = ?reason,
                "normalized source fact is retained but execution-fenced"
            );
        }
    }
    if let Some(sender) = recovery_sender
        && authority.recovery_worker_available
    {
        authority.recovery_worker_available =
            retain_recovery_source(sender, &mut authority.readiness, committed_source).await;
    }
    Ok(())
}

/// Admits one already-typed source transition through the sole SQLite writer.
async fn admit_typed_engine_event(
    writer: &mut EngineWriter,
    authority: &mut AuthorityState,
    event: TypedEngineEvent,
) -> Result<(), AppError> {
    let source = SourceEvent::new(
        writer.run_id(),
        event.event_id().as_str(),
        event.at().value(),
        event.source_kind(),
        event.source_payload_json()?,
    );
    let prior = authority
        .engine_state
        .take()
        .ok_or(AppError::MissingEngineState)?;
    let readiness_event = event.clone();
    let outcome = writer
        .admit_apply_append(LedgerId::RulesOnly, &source, move |admission| {
            Engine::apply(
                event.into_engine_event(),
                prior,
                &EngineContext::passive(admission),
            )
        })
        .await?;
    authority.engine_state = Some(outcome.into_parts().0);
    update_readiness_from_typed_event(&mut authority.readiness, &readiness_event);
    Ok(())
}

fn initial_engine_state(run_id: &str, opened_at_ns: i64) -> Result<EngineState, AppError> {
    let opened_at =
        TimestampNs::new(i128::from(opened_at_ns)).map_err(|_| AppError::InitialEngineState)?;
    let ledger = LedgerState::new(LedgerId::RulesOnly, opened_at)
        .map_err(|_| AppError::InitialEngineState)?;
    let broker = PaperBroker::new(
        BrokerConfig::new(
            Usdc::new(rust_decimal::Decimal::ONE).map_err(|_| AppError::InitialEngineState)?,
            maximum_book_age()?,
        )
        .map_err(|_| AppError::InitialEngineState)?,
        BrokerRunContext::new(
            RunId::new(run_id).map_err(|_| AppError::InitialEngineState)?,
            blake3::hash(run_id.as_bytes()).to_hex().to_string(),
            "0".repeat(64),
        )
        .map_err(|_| AppError::InitialEngineState)?,
        opened_at,
    );
    Ok(EngineState::new(ledger, broker, BTreeMap::new()))
}

fn maximum_book_age() -> Result<DurationNs, AppError> {
    DurationNs::new(i128::from(MAXIMUM_BOOK_AGE_NS)).map_err(|_| AppError::InitialEngineState)
}

fn provenance(config_bytes: &[u8]) -> Result<DataProvenance, AppError> {
    let code_digest =
        option_env!("TRENCH_WORKSPACE_BUILD_DIGEST").ok_or(AppError::MissingBuildDigest)?;
    DataProvenance::new(
        format!("b3:{}", blake3::hash(config_bytes).to_hex()),
        code_digest,
        ParquetStore::schema_hash(),
    )
    .map_err(Into::into)
}

pub(crate) fn configured_path(
    physical_config_path: &Path,
    value: &str,
) -> Result<PathBuf, AppError> {
    let config_path = absolute_path(physical_config_path)?;
    let value = Path::new(value);
    if value.is_absolute() {
        Ok(value.to_owned())
    } else {
        config_path
            .parent()
            .map(|parent| parent.join(value))
            .ok_or(AppError::ConfigPath)
    }
}

fn absolute_path(path: &Path) -> Result<PathBuf, AppError> {
    if path.is_absolute() {
        Ok(path.to_owned())
    } else {
        std::env::current_dir()
            .map(|directory| directory.join(path))
            .map_err(AppError::CurrentDirectory)
    }
}

fn current_time_ns() -> Result<i64, AppError> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| AppError::SystemTime)?;
    let seconds = i64::try_from(elapsed.as_secs()).map_err(|_| AppError::SystemTime)?;
    seconds
        .checked_mul(1_000_000_000)
        .and_then(|seconds| seconds.checked_add(i64::from(elapsed.subsec_nanos())))
        .ok_or(AppError::SystemTime)
}

fn local_ntp_synchronized() -> bool {
    #[cfg(target_os = "linux")]
    {
        std::fs::metadata("/run/systemd/timesync/synchronized")
            .map(|metadata| metadata.is_file())
            .unwrap_or(false)
    }
    #[cfg(not(target_os = "linux"))]
    {
        false
    }
}

fn run_id(started_at_ns: i64, config_digest: &str) -> String {
    let suffix = config_digest.get(3..15).unwrap_or("unknown");
    format!("run-{started_at_ns}-{suffix}")
}

async fn stop_signal(duration: Option<Duration>) -> Result<(), AppError> {
    match duration {
        Some(duration) => tokio::time::sleep(duration).await,
        None => {
            tokio::signal::ctrl_c().await.map_err(AppError::Signal)?;
        }
    }
    Ok(())
}

/// A startup, source-replay, lifecycle, or bounded authority-loop failure.
#[derive(Debug, Error)]
pub enum AppError {
    /// The embedded build commitment is unavailable.
    #[error("immutable workspace build digest was not embedded")]
    MissingBuildDigest,
    /// The config path could not be made absolute.
    #[error("could not determine the current directory")]
    CurrentDirectory(#[source] std::io::Error),
    /// The configuration path had no parent directory.
    #[error("configuration path has no usable parent")]
    ConfigPath,
    /// The local system clock was outside the representable SQLite range.
    #[error("system clock could not produce a valid UTC nanosecond run boundary")]
    SystemTime,
    /// The Ctrl-C lifecycle signal could not be installed.
    #[error("daemon shutdown signal could not be installed")]
    Signal(#[source] std::io::Error),
    /// The bounded source-replay producer task did not complete normally.
    #[error("source replay producer task join failed")]
    ReplayProducerJoin(#[source] tokio::task::JoinError),
    /// The read-only public recovery client could not be constructed.
    #[error(transparent)]
    Info(#[from] InfoError),
    /// The bounded public recovery worker did not complete normally.
    #[error("recovery producer task join failed")]
    RecoveryTaskJoin(#[source] tokio::task::JoinError),
    /// Engine state was consumed by a failed authority transition.
    #[error("authority engine state is unavailable after a failed transition")]
    MissingEngineState,
    /// The minimal no-entry engine state could not be initialized safely.
    #[error("paper engine initial state could not be constructed")]
    InitialEngineState,
    /// A normalized source fact could not take a typed, fail-closed route.
    #[error(transparent)]
    Routing(#[from] RoutingError),
    /// SQLite admission/write ownership could not initialize or drain.
    #[error(transparent)]
    Writer(#[from] WriterError),
    /// Atomic market-data persistence/recovery rejected a local path or state.
    #[error(transparent)]
    Storage(#[from] ParquetError),
    /// The frozen source replay plan or facts could not be verified.
    #[error(transparent)]
    Replay(#[from] ReplayError),
    /// A supplied replay plan was frozen to another config/code/schema run.
    #[error("replay plan provenance does not match the supplied config and code")]
    ReplayProvenanceMismatch,
    /// Local admin listener setup or protocol servicing failed.
    #[error(transparent)]
    Admin(#[from] AdminError),
    /// The admin task panicked or was cancelled outside the daemon shutdown path.
    #[error("admin task join failed")]
    AdminTaskJoin(#[source] tokio::task::JoinError),
    /// The only authority request channel was closed before shutdown.
    #[error("admin authority channel closed unexpectedly")]
    AuthorityChannelClosed,
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use std::time::Duration;

    use rust_decimal::Decimal;
    use serde_json::json;
    use tokio::sync::mpsc;
    use tokio::time::timeout;
    use tokio_util::sync::CancellationToken;
    use trench_core::event::{BookLevel, BookSnapshot};
    use trench_hyperliquid::{
        InfoClient, RecoveryStatus, RecoveryUnavailable, recovery_request_from_events_for_test,
    };
    use wiremock::matchers::{body_json, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::{
        AuthorityState, RecoveryInput, RecoveryOutput, admit_market_event, admit_recovery_output,
        configured_path, initial_engine_state, maximum_book_age, recover_source_stream,
        recovery_worker,
    };
    use crate::readiness::Readiness;
    use crate::writer::EngineWriter;
    use trench_core::domain::{Market, Price, Quantity, Side};
    use trench_core::event::{MarketEvent, TimestampNs, Trade};
    use trench_storage::parquet::{DataProvenance, ParquetStore};

    const BASE_MS: i64 = 1_800_000_000;
    const BASE_NS: i64 = BASE_MS * 1_000_000;
    const HOUR_NS: i64 = 3_600_000_000_000;

    fn timestamp(value: i64) -> TimestampNs {
        TimestampNs::new(i128::from(value)).expect("fixture timestamp")
    }

    fn btc() -> Market {
        Market::new("BTC").expect("fixture market")
    }

    fn price(value: i64) -> Price {
        Price::new(Decimal::from(value)).expect("fixture price")
    }

    fn quantity(value: i64) -> Quantity {
        Quantity::new(Decimal::from(value)).expect("fixture quantity")
    }

    fn predecessor() -> MarketEvent {
        MarketEvent::trade(
            timestamp(BASE_NS),
            timestamp(BASE_NS),
            btc(),
            Trade::new(1, Side::Buy, price(100), quantity(1)).expect("fixture trade"),
        )
        .expect("fixture event")
    }

    fn snapshot(at: i64, sequence: u64) -> MarketEvent {
        MarketEvent::book_snapshot(
            timestamp(at),
            timestamp(at),
            btc(),
            BookSnapshot::new(
                sequence,
                vec![BookLevel::new(price(99), quantity(10))],
                vec![BookLevel::new(price(101), quantity(10))],
            ),
        )
        .expect("fixture book")
    }

    fn provenance() -> DataProvenance {
        DataProvenance::new(
            format!("b3:{}", "a".repeat(64)),
            format!("b3:{}", "b".repeat(64)),
            ParquetStore::schema_hash(),
        )
        .expect("fixture provenance")
    }

    fn authority(run_id: &str) -> AuthorityState {
        AuthorityState {
            engine_state: Some(initial_engine_state(run_id, BASE_NS).expect("fixture state")),
            router: crate::execution::TypedMarketRouter::new(
                maximum_book_age().expect("fixture maximum book age"),
            ),
            readiness: Readiness::default(),
            reconciled: false,
            recovery_markets: BTreeSet::new(),
            recovery_worker_available: true,
        }
    }

    async fn next_recovery_output(receiver: &mut mpsc::Receiver<RecoveryOutput>) -> RecoveryOutput {
        timeout(Duration::from_secs(1), receiver.recv())
            .await
            .expect("recovery worker must respond before timeout")
            .expect("recovery worker result channel must remain open")
    }

    fn zero_candle(open: i64, interval_ms: i64) -> serde_json::Value {
        json!({
            "t": open,
            "T": open + interval_ms - 1,
            "s": "BTC",
            "i": if interval_ms == 900_000 { "15m" } else { "1h" },
            "o": "100",
            "c": "100",
            "h": "100",
            "l": "100",
            "v": "0",
            "n": 0,
        })
    }

    #[test]
    fn configured_relative_storage_is_anchored_to_the_config_not_the_process() {
        let config = std::path::Path::new("/srv/trench/config/paper.toml");
        assert_eq!(
            configured_path(config, "state/trench.sqlite").expect("configured path"),
            std::path::Path::new("/srv/trench/config/state/trench.sqlite")
        );
    }

    #[test]
    fn startup_recovery_accepts_an_empty_verified_task_fourteen_store() {
        let root = tempfile::tempdir().expect("fixture root");
        fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700)).expect("private root");
        let provenance = DataProvenance::new(
            format!("b3:{}", "a".repeat(64)),
            format!("b3:{}", "b".repeat(64)),
            ParquetStore::schema_hash(),
        )
        .expect("provenance");
        ParquetStore::open(root.path(), provenance.clone()).expect("empty store");
        assert!(
            recover_source_stream(root.path(), provenance)
                .expect("empty recovery")
                .is_none()
        );
    }

    #[tokio::test]
    async fn historical_source_replay_remains_monotonic_through_the_authority_writer() {
        let root = tempfile::tempdir().expect("fixture root");
        fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700)).expect("private root");
        let provenance = DataProvenance::new(
            format!("b3:{}", "a".repeat(64)),
            format!("b3:{}", "b".repeat(64)),
            ParquetStore::schema_hash(),
        )
        .expect("provenance");
        let store = ParquetStore::open(root.path(), provenance.clone()).expect("store");
        let at = TimestampNs::new(1).expect("timestamp");
        let event = MarketEvent::trade(
            at,
            at,
            Market::new("SOL").expect("market"),
            Trade::new(
                1,
                Side::Buy,
                Price::new(Decimal::ONE).expect("price"),
                Quantity::new(Decimal::ONE).expect("quantity"),
            )
            .expect("trade"),
        )
        .expect("market event");
        store.write_events(&[event]).expect("partition write");
        let recovered = recover_source_stream(root.path(), provenance)
            .expect("recovery")
            .expect("source facts");
        let mut writer = EngineWriter::open(root.path().join("trench.sqlite"), "run-replay", 100)
            .await
            .expect("writer");
        let initial_at = recovered
            .events
            .first()
            .expect("source fact")
            .event_time()
            .value()
            .min(100);
        let mut authority = AuthorityState {
            engine_state: Some(initial_engine_state("run-replay", initial_at).expect("state")),
            router: crate::execution::TypedMarketRouter::new(
                maximum_book_age().expect("fixture maximum book age"),
            ),
            readiness: Readiness::default(),
            reconciled: false,
            recovery_markets: BTreeSet::new(),
            recovery_worker_available: false,
        };
        for event in recovered.events {
            admit_market_event(&mut writer, &mut authority, event, None)
                .await
                .expect("authority admission");
        }
        assert_eq!(
            writer
                .journal_counts()
                .await
                .expect("journal counts")
                .events,
            1
        );
        assert!(authority.engine_state.is_some());
        assert!(!authority.reconciled);
    }

    #[tokio::test]
    async fn unavailable_recovery_evidence_stays_fenced_after_the_authority_drains_it() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(503))
            .mount(&server)
            .await;
        let directory = tempfile::tempdir().expect("fixture root");
        fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700))
            .expect("private fixture root");
        let store = ParquetStore::open(directory.path(), provenance()).expect("fixture store");
        let mut writer = EngineWriter::open(
            directory.path().join("trench.sqlite"),
            "run-unavailable",
            BASE_NS,
        )
        .await
        .expect("fixture writer");
        let mut authority = authority("run-unavailable");
        let predecessor = predecessor();
        let anchor = snapshot(BASE_NS + HOUR_NS, 1);
        let request = recovery_request_from_events_for_test(1, Some(&predecessor), &anchor);

        store
            .write_events(std::slice::from_ref(&predecessor))
            .expect("persisted predecessor");
        admit_market_event(&mut writer, &mut authority, predecessor, None)
            .await
            .expect("durable predecessor admission");
        authority.router.open_gap_for_test(btc());
        store
            .write_events(std::slice::from_ref(&anchor))
            .expect("persisted recovery snapshot");
        admit_market_event(&mut writer, &mut authority, anchor, None)
            .await
            .expect("durable fenced snapshot admission");
        authority.recovery_markets.insert(btc());
        super::admit_typed_engine_event(
            &mut writer,
            &mut authority,
            crate::execution::TypedEngineEvent::recovery_requested(&request),
        )
        .await
        .expect("durable recovery request record");
        let count_before_result = writer
            .journal_counts()
            .await
            .expect("journal counts")
            .events;

        let client = InfoClient::new_loopback_for_test(&format!("{}/info", server.uri()))
            .expect("loopback recovery client");
        let cancellation = CancellationToken::new();
        let (input_sender, input_receiver) = mpsc::channel(4);
        let (output_sender, mut output_receiver) = mpsc::channel(4);
        let task = tokio::spawn(recovery_worker(
            client,
            input_receiver,
            output_sender,
            cancellation.clone(),
        ));
        input_sender
            .send(RecoveryInput::Request(request))
            .await
            .expect("recovery request handoff");
        let output = next_recovery_output(&mut output_receiver).await;
        assert!(matches!(
            &output,
            RecoveryOutput::Result(result)
                if matches!(
                    result.status(),
                    RecoveryStatus::Unavailable {
                        reason: RecoveryUnavailable::OfficialCandleEvidenceUnavailable
                    }
                )
        ));
        admit_recovery_output(&mut writer, &mut authority, &store, output)
            .await
            .expect("authority must record the fenced result");
        assert_eq!(
            writer
                .journal_counts()
                .await
                .expect("journal counts")
                .events,
            count_before_result,
            "an unavailable result must not forge a market recovery transition"
        );
        assert!(!authority.readiness.mandatory_exit_ready(&btc()));
        assert!(
            authority
                .readiness
                .market_blockers(&btc())
                .contains(&crate::readiness::MarketBlocker::Recovery)
        );

        cancellation.cancel();
        drop(input_sender);
        task.await.expect("recovery worker shutdown");
    }

    #[tokio::test]
    async fn reconciled_recovery_requires_a_new_post_completion_book_before_execution() {
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
                zero_candle(BASE_MS, 900_000),
                zero_candle(BASE_MS + 900_000, 900_000),
                zero_candle(BASE_MS + 1_800_000, 900_000),
                zero_candle(BASE_MS + 2_700_000, 900_000),
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
                ResponseTemplate::new(200).set_body_json(vec![zero_candle(BASE_MS, 3_600_000)]),
            )
            .expect(1)
            .mount(&server)
            .await;
        let directory = tempfile::tempdir().expect("fixture root");
        fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700))
            .expect("private fixture root");
        let store = ParquetStore::open(directory.path(), provenance()).expect("fixture store");
        let mut writer = EngineWriter::open(
            directory.path().join("trench.sqlite"),
            "run-reconciled",
            BASE_NS,
        )
        .await
        .expect("fixture writer");
        let mut authority = authority("run-reconciled");
        let predecessor = predecessor();
        let anchor = snapshot(BASE_NS + HOUR_NS, 1);
        let request = recovery_request_from_events_for_test(1, Some(&predecessor), &anchor);

        store
            .write_events(std::slice::from_ref(&predecessor))
            .expect("persisted predecessor");
        admit_market_event(&mut writer, &mut authority, predecessor, None)
            .await
            .expect("durable predecessor admission");
        authority.router.open_gap_for_test(btc());
        store
            .write_events(std::slice::from_ref(&anchor))
            .expect("persisted recovery snapshot");
        admit_market_event(&mut writer, &mut authority, anchor.clone(), None)
            .await
            .expect("durable fenced snapshot admission");
        authority.recovery_markets.insert(btc());
        super::admit_typed_engine_event(
            &mut writer,
            &mut authority,
            crate::execution::TypedEngineEvent::recovery_requested(&request),
        )
        .await
        .expect("durable recovery request record");

        let client = InfoClient::new_loopback_for_test(&format!("{}/info", server.uri()))
            .expect("loopback recovery client");
        let cancellation = CancellationToken::new();
        let (input_sender, input_receiver) = mpsc::channel(4);
        let (output_sender, mut output_receiver) = mpsc::channel(4);
        let task = tokio::spawn(recovery_worker(
            client,
            input_receiver,
            output_sender,
            cancellation.clone(),
        ));
        input_sender
            .send(RecoveryInput::Request(request))
            .await
            .expect("recovery request handoff");
        let output = next_recovery_output(&mut output_receiver).await;
        assert!(matches!(
            &output,
            RecoveryOutput::Result(result) if matches!(result.status(), RecoveryStatus::Reconciled { .. })
        ));
        admit_recovery_output(&mut writer, &mut authority, &store, output)
            .await
            .expect("authority must route reconciled recovery");
        assert!(
            authority
                .readiness
                .market_blockers(&btc())
                .contains(&crate::readiness::MarketBlocker::ExecutableBook)
        );
        assert!(!authority.readiness.mandatory_exit_ready(&btc()));

        let post_completion = snapshot(BASE_NS + HOUR_NS + 1, 2);
        store
            .write_events(std::slice::from_ref(&post_completion))
            .expect("persisted post-completion book");
        admit_market_event(&mut writer, &mut authority, post_completion, None)
            .await
            .expect("post-completion book admission");
        assert!(authority.readiness.mandatory_exit_ready(&btc()));

        cancellation.cancel();
        drop(input_sender);
        task.await.expect("recovery worker shutdown");
    }
}
