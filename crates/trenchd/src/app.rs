//! Bounded daemon lifecycle and startup recovery orchestration.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::future;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use sd_notify::{NotifyState, notify, watchdog_enabled};
use serde::Deserialize;
use thiserror::Error;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use trench_core::broker::{BrokerConfig, BrokerRunContext, PaperBroker};
use trench_core::candle::CandleAggregator;
use trench_core::config::PaperConfig;
use trench_core::domain::{EventId, LedgerId, Market, RunId, Usdc};
use trench_core::engine::{Engine, EngineContext, EngineError, EnginePersistenceKind, EngineState};
use trench_core::event::{DurationNs, MarketEvent, MarketEventKind, TimestampNs};
use trench_core::ledger::LedgerState;
use trench_hyperliquid::{
    ContextCapture, ContextCaptureBatch, ContextCaptureError, ContextCaptureRequest, GapEvent,
    GapRecoveryRequest, InfoClient, InfoError, ReceiptClock, RecoveryEvidenceProducer,
    RecoveryProducerError, RecoveryResult, WsClient, WsConfig, WsOutput, WsStream,
};
use trench_storage::parquet::{DataProvenance, ParquetError, ParquetStore};
use trench_storage::replay::{DeterministicReplay, ReplayError, ReplayPlan};
use trench_storage::sqlite::{EngineJournalHistory, JournalEvent};

use crate::admin::{
    AdminError, AdminServer, AuthorityRequest, DaemonMode, DaemonStatus, authority_channel,
};
use crate::capture_scheduler::{CaptureScheduler, cadence};
use crate::commands::RulesStartup;
use crate::execution::{MarketRoute, RoutingError, TypedEngineEvent, TypedMarketRouter};
use crate::readiness::Readiness;
use crate::writer::{EngineWriter, SourceEvent, WriterError};

const SOURCE_CHANNEL_CAPACITY: usize = 128;
const CAPTURE_CHANNEL_CAPACITY: usize = 1;
const CAPTURE_RESULT_CHANNEL_CAPACITY: usize = 1;
const RECOVERY_CHANNEL_CAPACITY: usize = 16;
const RECOVERY_PENDING_CAPACITY: usize = 128;
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

/// Fully reconstructed authority state plus only the source facts that were
/// durably retained in Parquet but never atomically admitted to SQLite before
/// the prior daemon stopped.
#[derive(Debug)]
struct RestoredAuthority {
    authority: AuthorityState,
    fresh_events: Vec<MarketEvent>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RecoveryRequestJournal {
    schema_version: u8,
    event_id: String,
    market: String,
    event_time_ns: i64,
    kind: String,
    generation: u64,
    snapshot_event_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RecoveryCompletionJournal {
    schema_version: u8,
    event_id: String,
    market: String,
    event_time_ns: i64,
    kind: String,
    snapshot_event_id: String,
}

struct AuthorityState {
    engine_state: Option<EngineState>,
    router: TypedMarketRouter,
    historical_sources: BTreeMap<EventId, MarketEvent>,
    readiness: Readiness,
    live: LiveSubscription,
    reconciled: bool,
    recovery_markets: BTreeSet<Market>,
    recovery_pending: VecDeque<RecoveryInput>,
    recovery_worker_available: bool,
}

impl std::fmt::Debug for AuthorityState {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AuthorityState")
            .finish_non_exhaustive()
    }
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
    Result(Box<RecoveryResult>),
    /// The producer could not safely retain or reconcile its queue head.
    Failed {
        /// Market that remains execution-fenced.
        market: Market,
        /// Exact conservative producer failure.
        error: RecoveryProducerError,
    },
}

/// One complete all-or-nothing public context outcome returned to the authority.
#[derive(Debug)]
enum CaptureOutput {
    /// The bounded public adapter produced one immutable complete source batch.
    Captured(ContextCaptureBatch),
    /// No source facts are available because the batch failed atomically.
    Rejected(ContextCaptureError),
}

/// Whether a complete capture can cross the durable authority boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CaptureSourcePreflight {
    /// Every capture fact is new and the full batch can be persisted atomically.
    AllNew,
    /// Every capture fact is already verified immutable history, so this retry
    /// must leave all daemon state unchanged.
    HistoricalNoOp,
}

/// Explicit UTC receipt-time source used only by the read-only I/O adapter.
#[derive(Debug, Clone, Copy)]
struct SystemReceiptClock;

impl ReceiptClock for SystemReceiptClock {
    fn receipt_time(&self) -> Option<TimestampNs> {
        current_time_ns()
            .ok()
            .and_then(|value| TimestampNs::new(i128::from(value)).ok())
    }
}

const STREAM_RESTART_MIN_DELAY: Duration = Duration::from_secs(1);
const STREAM_RESTART_MAX_DELAY: Duration = Duration::from_secs(30);

/// One daemon-owned public-stream lifecycle. Its scope is replaced only from
/// a complete capture after that capture has crossed the durable authority
/// path. A stream epoch is not healthy until every scoped market has supplied
/// a live L2 snapshot.
struct LiveSubscription {
    stream: Option<WsStream>,
    scope: Vec<Market>,
    observed_l2: BTreeSet<Market>,
    active_epoch: bool,
    epoch: u64,
    restart_attempt: u8,
    restart_at: Option<tokio::time::Instant>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum StreamScopeAction {
    None,
    Stop,
    Start {
        scope: Vec<Market>,
        epoch: u64,
        shutdown_prior: bool,
    },
}

impl LiveSubscription {
    fn new() -> Self {
        Self {
            stream: None,
            scope: Vec::new(),
            observed_l2: BTreeSet::new(),
            active_epoch: false,
            epoch: 0,
            restart_attempt: 0,
            restart_at: None,
        }
    }

    /// Applies only a scope derived from a successfully persisted capture.
    fn replace_persisted_scope(&mut self, mut scope: Vec<Market>) -> StreamScopeAction {
        scope.sort();
        scope.dedup();
        if scope.is_empty() {
            let running = self.active_epoch;
            self.scope.clear();
            self.observed_l2.clear();
            self.active_epoch = false;
            self.restart_attempt = 0;
            self.restart_at = None;
            return if running {
                StreamScopeAction::Stop
            } else {
                StreamScopeAction::None
            };
        }

        if self.scope == scope && self.active_epoch {
            return StreamScopeAction::None;
        }

        let shutdown_prior = self.active_epoch;
        self.scope = scope.clone();
        self.observed_l2.clear();
        self.restart_attempt = 0;
        self.restart_at = None;
        self.epoch = self.epoch.saturating_add(1);
        StreamScopeAction::Start {
            scope,
            epoch: self.epoch,
            shutdown_prior,
        }
    }

    /// Ends one terminal epoch and returns a bounded restart delay for the
    /// most recently persisted nonempty scope.
    fn end_epoch(&mut self) -> Option<Duration> {
        self.observed_l2.clear();
        self.active_epoch = false;
        if self.scope.is_empty() {
            self.restart_at = None;
            return None;
        }
        self.restart_attempt = self.restart_attempt.saturating_add(1).min(6);
        let shift = u32::from(self.restart_attempt.saturating_sub(1));
        let seconds = STREAM_RESTART_MIN_DELAY
            .as_secs()
            .checked_shl(shift)
            .unwrap_or(u64::MAX);
        Some(Duration::from_secs(
            seconds.min(STREAM_RESTART_MAX_DELAY.as_secs()),
        ))
    }

    /// Opens a fresh epoch after the terminal delay elapsed.
    fn restart_due(&mut self) -> StreamScopeAction {
        if self.scope.is_empty() || self.active_epoch {
            return StreamScopeAction::None;
        }
        self.restart_at = None;
        self.observed_l2.clear();
        self.epoch = self.epoch.saturating_add(1);
        StreamScopeAction::Start {
            scope: self.scope.clone(),
            epoch: self.epoch,
            shutdown_prior: false,
        }
    }

    /// Records an L2 fact only from the current stream epoch and returns
    /// whether the complete subscribed scope has now produced L2.
    fn observe_l2(&mut self, market: &Market) -> bool {
        if !self.active_epoch || !self.scope.contains(market) {
            return false;
        }
        self.observed_l2.insert(market.clone());
        self.observed_l2.len() == self.scope.len()
    }

    fn mark_epoch_started(&mut self) {
        self.active_epoch = true;
    }

    fn mark_epoch_start_failed(&mut self) {
        self.active_epoch = false;
    }
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
    let recovery = recover_source_stream(&parquet_path, provenance.clone())?;
    let history = EngineWriter::inspect_history(&sqlite_path).await?;
    let RestoredAuthority {
        mut authority,
        fresh_events,
    } = reconstruct_authority(recovery.as_ref(), &history, &run_id, started_at_ns)?;
    let mut writer =
        EngineWriter::open_after_reconstruction(&sqlite_path, run_id, started_at_ns, &history)
            .await?;
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
        fresh_events,
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
    // The initial capture is intentionally metadata-only. A WebSocket scope
    // may be constructed only after that capture is durable and admitted.
    let capture_client = ContextCapture::new(InfoClient::new(config.endpoints().info_url())?);
    let (capture_sender, capture_receiver) = mpsc::channel(CAPTURE_CHANNEL_CAPACITY);
    let (capture_result_sender, mut capture_result_receiver) =
        mpsc::channel(CAPTURE_RESULT_CHANNEL_CAPACITY);
    let capture_task = tokio::spawn(capture_worker(
        capture_client,
        capture_receiver,
        capture_result_sender,
        cancellation.clone(),
    ));
    let mut capture_scheduler = CaptureScheduler::new(Vec::new());
    authority.readiness.set_context_capture_current(false);
    tracing::info!("public context capture is bootstrapping the dynamic universe");
    let server = AdminServer::bind(&admin_socket).await?;
    let (authority_sender, mut authority_receiver) = authority_channel();
    let admin_cancellation = cancellation.clone();
    let admin_task = tokio::spawn(server.serve(authority_sender, admin_cancellation));
    let watchdog_task = tokio::spawn(systemd_watchdog(cancellation.clone()));
    notify_systemd(&[
        NotifyState::Ready,
        NotifyState::Status("collection-only authority ready"),
    ]);

    let stop = stop_signal(duration);
    tokio::pin!(stop);
    let mut recovery_results_open = true;
    let mut capture_results_open = true;
    let mut recovery_clock = tokio::time::interval_at(
        tokio::time::Instant::now() + Duration::from_secs(1),
        Duration::from_secs(1),
    );
    recovery_clock.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut capture_clock = tokio::time::interval_at(
        tokio::time::Instant::now(),
        cadence(config.feed().universe_refresh_seconds()),
    );
    capture_clock.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        flush_recovery_inputs(&recovery_sender, &mut authority);
        let restart_at = authority.live.restart_at;
        tokio::select! {
            biased;
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
            output = capture_result_receiver.recv(), if capture_results_open => {
                match output {
                    Some(output) => {
                        let stream_action = admit_capture_output(
                            &parquet_store,
                            &mut writer,
                            &mut authority,
                            output,
                            &mut capture_scheduler,
                            config,
                            Some(&recovery_sender),
                        ).await?;
                        apply_stream_scope_action(
                            &mut authority.live,
                            stream_action,
                            &mut authority.readiness,
                        ).await;
                    }
                    None => {
                        capture_results_open = false;
                        capture_scheduler.complete(false);
                        authority.readiness.set_context_capture_current(false);
                        tracing::error!(
                            "public context capture worker stopped; entries remain fail-closed"
                        );
                    }
                }
            }
            output = receive_live(&mut authority.live.stream) => {
                match output {
                    Some(WsOutput::MarketEvent(event)) => {
                        let admitted = persist_live_market_event(
                            &parquet_store,
                            &mut writer,
                            &mut authority,
                            event.clone(),
                            Some(&recovery_sender),
                        )
                        .await?;
                        if admitted && matches!(event.kind(), MarketEventKind::BookSnapshot(_))
                            && authority.live.observe_l2(event.market())
                        {
                            authority.readiness.set_stream_connected(true);
                            authority.live.restart_attempt = 0;
                        }
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
                        submit_recovery_request(
                            &recovery_sender,
                            &mut authority,
                            request,
                        );
                    }
                    Some(WsOutput::Rejected(_)) => {}
                    Some(WsOutput::Terminal(_)) | None => {
                        end_live_epoch(&mut authority.live, &mut authority.readiness).await;
                    }
                }
            }
            _ = wait_for_stream_restart(restart_at), if restart_at.is_some() => {
                let stream_action = authority.live.restart_due();
                apply_stream_scope_action(
                    &mut authority.live,
                    stream_action,
                    &mut authority.readiness,
                ).await;
            }
            output = recovery_result_receiver.recv(), if recovery_results_open => {
                match output {
                    Some(output) => {
                        admit_recovery_output(
                            &mut writer,
                            &mut authority,
                            output,
                        ).await?;
                    }
                    None => {
                        recovery_results_open = false;
                        fence_recovery_worker(&mut authority, None);
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
                    advance_recovery_clock(&recovery_sender, &mut authority, market, at);
                    if !authority.recovery_worker_available {
                        break;
                    }
                }
            }
            _ = capture_clock.tick(), if capture_results_open => {
                match TimestampNs::new(i128::from(current_time_ns()?)) {
                    Ok(scheduled_at) => match capture_scheduler.dispatch(scheduled_at) {
                        Ok(Some(request)) => match capture_sender.try_send(request) {
                            Ok(()) => {}
                            Err(mpsc::error::TrySendError::Full(_)) => {
                                capture_scheduler.complete(false);
                                authority.readiness.set_context_capture_current(false);
                                tracing::error!(
                                    "public context capture scheduler became contended; entries remain fail-closed"
                                );
                            }
                            Err(mpsc::error::TrySendError::Closed(_)) => {
                                capture_results_open = false;
                                capture_scheduler.complete(false);
                                authority.readiness.set_context_capture_current(false);
                                tracing::error!(
                                    "public context capture worker is unavailable; entries remain fail-closed"
                                );
                            }
                        },
                        Ok(None) => {}
                        Err(error) => {
                            authority.readiness.set_context_capture_current(false);
                            tracing::warn!(
                                error = %error,
                                "public context capture schedule is invalid; entries remain fail-closed"
                            );
                        }
                    },
                    Err(_) => {
                        authority.readiness.set_context_capture_current(false);
                        tracing::warn!(
                            "public context capture UTC clock is invalid; entries remain fail-closed"
                        );
                    }
                }
            }
        }
    }

    cancellation.cancel();
    notify_systemd(&[NotifyState::Stopping]);
    drop(capture_sender);
    drop(recovery_sender);
    if let Some(stream) = authority.live.stream.take() {
        stream.shutdown().await;
    }
    capture_task.await.map_err(AppError::CaptureTaskJoin)?;
    recovery_task.await.map_err(AppError::RecoveryTaskJoin)?;
    while let Ok(output) = recovery_result_receiver.try_recv() {
        admit_recovery_output(&mut writer, &mut authority, output).await?;
    }
    admin_task.await.map_err(AppError::AdminTaskJoin)??;
    watchdog_task.await.map_err(AppError::WatchdogTaskJoin)?;
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

fn notify_systemd(states: &[NotifyState<'_>]) {
    if let Err(error) = notify(states) {
        tracing::warn!(error = %error, "systemd notification failed");
    }
}

async fn systemd_watchdog(cancellation: CancellationToken) {
    let Some(period) = watchdog_enabled() else {
        cancellation.cancelled().await;
        return;
    };
    let interval = period
        .checked_div(2)
        .unwrap_or(Duration::from_secs(1))
        .max(Duration::from_secs(1));
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = cancellation.cancelled() => return,
            _ = ticker.tick() => notify_systemd(&[NotifyState::Watchdog]),
        }
    }
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

/// Reconstructs authority-local executable state before a new SQLite run is
/// created. The journal supplies comparison witnesses only: every resulting
/// transition is recalculated through the same typed router and pure engine
/// from immutable normalized Parquet source facts.
fn reconstruct_authority(
    recovery: Option<&RecoveredSource>,
    history: &EngineJournalHistory,
    new_run_id: &str,
    started_at_ns: i64,
) -> Result<RestoredAuthority, AppError> {
    let source_events = recovery.map_or(&[][..], |recovered| recovered.events.as_slice());
    if history.is_empty() {
        let initial_at_ns = source_events.first().map_or(started_at_ns, |event| {
            event.event_time().value().min(started_at_ns)
        });
        return Ok(RestoredAuthority {
            authority: new_authority(new_run_id, initial_at_ns)?,
            fresh_events: source_events.to_vec(),
        });
    }
    let Some(first_journal_event) = history.events().first() else {
        return Err(AppError::HistoryParity {
            reason: "nonempty journal had no first transition",
        });
    };
    let Some(first_source_event) = source_events.first() else {
        return Err(AppError::MissingHistoricalSourceEvidence);
    };
    let source_replay_initial_at_ns = first_source_event
        .event_time()
        .value()
        .min(first_journal_event.run_started_at_ns());
    let source_by_id = source_events
        .iter()
        .map(|event| (event.event_id().as_str(), event))
        .collect::<BTreeMap<_, _>>();
    let initial_candidates = [
        first_journal_event.run_started_at_ns(),
        source_replay_initial_at_ns,
    ];
    let mut last_candidate_error = None;
    let mut previous_candidate = None;
    for initial_at_ns in initial_candidates {
        if previous_candidate == Some(initial_at_ns) {
            continue;
        }
        previous_candidate = Some(initial_at_ns);
        let mut authority = new_authority(first_journal_event.run_id(), initial_at_ns)?;
        match reconstruct_historical_journal(&mut authority, history, &source_by_id) {
            Ok(historical_sources) => {
                if authority.router.has_pending_recovery() {
                    return Err(AppError::IncompleteRecoveryEvidence);
                }
                let causal_cursor = authority
                    .engine_state
                    .as_ref()
                    .ok_or(AppError::MissingEngineState)?
                    .broker()
                    .causal_boundary();
                let fresh_events = source_events
                    .iter()
                    .filter(|event| !historical_sources.contains_key(event.event_id()))
                    .map(|event| {
                        if event.received_at() <= causal_cursor {
                            Err(AppError::UncoveredSourceBeforeCursor)
                        } else {
                            Ok(event.clone())
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                authority.historical_sources = historical_sources;
                return Ok(RestoredAuthority {
                    authority,
                    fresh_events,
                });
            }
            Err(error @ AppError::HistoryParity { .. }) | Err(error @ AppError::Engine(_)) => {
                last_candidate_error = Some(error);
            }
            Err(error) => return Err(error),
        }
    }
    Err(last_candidate_error.unwrap_or(AppError::HistoryParity {
        reason: "no valid initial engine boundary could reproduce durable history",
    }))
}

fn reconstruct_historical_journal(
    authority: &mut AuthorityState,
    history: &EngineJournalHistory,
    source_by_id: &BTreeMap<&str, &MarketEvent>,
) -> Result<BTreeMap<EventId, MarketEvent>, AppError> {
    let mut historical_sources = BTreeMap::new();
    for journal_event in history.events() {
        if let Some(source) = source_by_id.get(journal_event.event_id()) {
            if historical_sources
                .insert(source.event_id().clone(), (*source).clone())
                .is_some()
            {
                return Err(AppError::HistoryParity {
                    reason: "source fact appeared more than once in SQLite history",
                });
            }
            reconstruct_market_event(authority, (*source).clone(), journal_event)?;
        } else {
            reconstruct_recovery_event(authority, journal_event)?;
        }
    }
    Ok(historical_sources)
}

fn new_authority(run_id: &str, initial_at_ns: i64) -> Result<AuthorityState, AppError> {
    Ok(AuthorityState {
        engine_state: Some(initial_engine_state(run_id, initial_at_ns)?),
        router: TypedMarketRouter::new(maximum_book_age()?),
        historical_sources: BTreeMap::new(),
        readiness: Readiness::default(),
        live: LiveSubscription::new(),
        reconciled: false,
        recovery_markets: BTreeSet::new(),
        recovery_pending: VecDeque::new(),
        recovery_worker_available: true,
    })
}

fn reconstruct_market_event(
    authority: &mut AuthorityState,
    source: MarketEvent,
    journal_event: &JournalEvent,
) -> Result<(), AppError> {
    let retained_source = source.clone();
    if source_is_late(authority, &retained_source)? {
        return reconstruct_typed_event(
            authority,
            TypedEngineEvent::SourceRetained {
                source: retained_source,
            },
            journal_event,
        );
    }
    let open_position_market = authority.engine_state.as_ref().and_then(|state| {
        state
            .broker()
            .position()
            .map(|position| position.market().clone())
    });
    match authority
        .router
        .route_market_event(source, open_position_market.as_ref())?
    {
        MarketRoute::Engine(events) => events
            .into_iter()
            .try_for_each(|event| reconstruct_typed_event(authority, event, journal_event)),
        MarketRoute::Blocked { .. } => reconstruct_typed_event(
            authority,
            TypedEngineEvent::AdvanceTime {
                source: retained_source,
            },
            journal_event,
        ),
    }
}

fn reconstruct_recovery_event(
    authority: &mut AuthorityState,
    journal_event: &JournalEvent,
) -> Result<(), AppError> {
    match journal_event.kind() {
        "recovery_request" => {
            let evidence =
                serde_json::from_str::<RecoveryRequestJournal>(journal_event.payload_json())
                    .map_err(|_| AppError::InvalidRecoveryEvidence)?;
            if evidence.schema_version != 1
                || evidence.kind != "recovery_request"
                || evidence.event_id != journal_event.event_id()
                || evidence.event_time_ns != journal_event.event_time_ns()
            {
                return Err(AppError::InvalidRecoveryEvidence);
            }
            let market =
                Market::new(evidence.market).map_err(|_| AppError::InvalidRecoveryEvidence)?;
            let event_id =
                EventId::new(evidence.event_id).map_err(|_| AppError::InvalidRecoveryEvidence)?;
            let snapshot_event_id = EventId::new(evidence.snapshot_event_id)
                .map_err(|_| AppError::InvalidRecoveryEvidence)?;
            let at = TimestampNs::new(i128::from(evidence.event_time_ns))
                .map_err(|_| AppError::InvalidRecoveryEvidence)?;
            authority.router.restore_recovery_request(
                market.clone(),
                evidence.generation,
                snapshot_event_id.clone(),
            )?;
            reconstruct_typed_event(
                authority,
                TypedEngineEvent::RecoveryRequested {
                    event_id,
                    at,
                    market,
                    generation: evidence.generation,
                    snapshot_event_id,
                },
                journal_event,
            )
        }
        "market_recovered" => {
            let evidence =
                serde_json::from_str::<RecoveryCompletionJournal>(journal_event.payload_json())
                    .map_err(|_| AppError::InvalidRecoveryEvidence)?;
            if evidence.schema_version != 1
                || evidence.kind != "market_recovered"
                || evidence.event_id != journal_event.event_id()
                || evidence.event_time_ns != journal_event.event_time_ns()
            {
                return Err(AppError::InvalidRecoveryEvidence);
            }
            let market =
                Market::new(evidence.market).map_err(|_| AppError::InvalidRecoveryEvidence)?;
            let event_id =
                EventId::new(evidence.event_id).map_err(|_| AppError::InvalidRecoveryEvidence)?;
            let snapshot_event_id = EventId::new(evidence.snapshot_event_id)
                .map_err(|_| AppError::InvalidRecoveryEvidence)?;
            let at = TimestampNs::new(i128::from(evidence.event_time_ns))
                .map_err(|_| AppError::InvalidRecoveryEvidence)?;
            authority.router.restore_recovery_completion(
                &event_id,
                at,
                market.clone(),
                snapshot_event_id.clone(),
            )?;
            reconstruct_typed_event(
                authority,
                TypedEngineEvent::MarketRecovered {
                    event_id,
                    at,
                    market,
                    snapshot_event_id,
                },
                journal_event,
            )
        }
        _ => Err(AppError::MissingHistoricalSourceEvidence),
    }
}

fn reconstruct_typed_event(
    authority: &mut AuthorityState,
    event: TypedEngineEvent,
    journal_event: &JournalEvent,
) -> Result<(), AppError> {
    let payload_json = event.source_payload_json()?;
    if journal_event.event_id() != event.event_id().as_str()
        || journal_event.event_time_ns() != event.at().value()
        || journal_event.kind() != event.source_kind()
        || journal_event.payload_json() != payload_json
    {
        return Err(AppError::HistoryParity {
            reason: "typed source evidence differs from immutable SQLite journal",
        });
    }
    let prior = authority
        .engine_state
        .take()
        .ok_or(AppError::MissingEngineState)?;
    let readiness_event = event.clone();
    let outcome = Engine::apply(
        event.into_engine_event(),
        prior,
        &EngineContext::passive(trench_core::engine::EventAdmission::New),
    )?;
    verify_historical_outcome(&outcome, journal_event)?;
    authority.engine_state = Some(outcome.into_parts().0);
    update_readiness_from_typed_event(&mut authority.readiness, &readiness_event);
    Ok(())
}

fn verify_historical_outcome(
    outcome: &trench_core::engine::EngineOutcome,
    journal_event: &JournalEvent,
) -> Result<(), AppError> {
    let projection = outcome.persistence_batch();
    let checkpoint = projection.checkpoint();
    if projection.event_id() != journal_event.event_id()
        || projection.at().value() != journal_event.checkpoint_at_ns()
        || checkpoint.checkpoint_id() != journal_event.checkpoint_id()
        || checkpoint.state_digest() != journal_event.state_digest()
        || projection.records().len() != journal_event.records().len()
        || projection
            .records()
            .iter()
            .zip(journal_event.records())
            .any(|(actual, expected)| {
                persistence_kind_name(actual.kind()) != expected.kind()
                    || actual.payload_json() != expected.payload_json()
            })
    {
        return Err(AppError::HistoryParity {
            reason: "recomputed core transition differs from immutable SQLite evidence",
        });
    }
    Ok(())
}

const fn persistence_kind_name(kind: EnginePersistenceKind) -> &'static str {
    match kind {
        EnginePersistenceKind::Snapshot => "snapshot",
        EnginePersistenceKind::Signal => "signal",
        EnginePersistenceKind::Intent => "intent",
        EnginePersistenceKind::Risk => "risk",
        EnginePersistenceKind::Order => "order",
        EnginePersistenceKind::Fill => "fill",
        EnginePersistenceKind::Ledger => "ledger",
        EnginePersistenceKind::Breaker => "breaker",
    }
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

/// Runs one read-only complete-context request at a time outside the authority.
///
/// The worker has no persistence, engine, or readiness access. Cancellation
/// drops an in-progress public request instead of publishing a partial batch.
async fn capture_worker(
    capture: ContextCapture,
    mut input: mpsc::Receiver<ContextCaptureRequest>,
    output: mpsc::Sender<CaptureOutput>,
    cancellation: CancellationToken,
) {
    loop {
        let request = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return,
            request = input.recv() => request,
        };
        let Some(request) = request else {
            return;
        };
        let outcome = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return,
            result = capture.capture(&request, &SystemReceiptClock) => {
                match result {
                    Ok(batch) => CaptureOutput::Captured(batch),
                    Err(error) => CaptureOutput::Rejected(error),
                }
            }
        };
        let delivered = tokio::select! {
            biased;
            _ = cancellation.cancelled() => false,
            delivered = output.send(outcome) => delivered.is_ok(),
        };
        if !delivered {
            return;
        }
    }
}

/// Persists a complete capture before every individual source fact traverses
/// the authority writer. Rejected captures have no facts to persist or route.
async fn admit_capture_output(
    parquet_store: &ParquetStore,
    writer: &mut EngineWriter,
    authority: &mut AuthorityState,
    output: CaptureOutput,
    scheduler: &mut CaptureScheduler,
    config: &PaperConfig,
    recovery_sender: Option<&mpsc::Sender<RecoveryInput>>,
) -> Result<StreamScopeAction, AppError> {
    match output {
        CaptureOutput::Captured(batch) => {
            if preflight_capture_sources(authority, batch.events())?
                == CaptureSourcePreflight::HistoricalNoOp
            {
                tracing::debug!(
                    events = batch.events().len(),
                    captured_at_ns = batch.captured_at().value(),
                    source_digest = %batch.source_digest(),
                    "dropping complete historical context-capture retry before durable mutation"
                );
                return Ok(StreamScopeAction::None);
            }
            parquet_store.write_capture_batch(batch.events())?;
            for event in batch.events().iter().cloned() {
                admit_market_event(writer, authority, event, recovery_sender).await?;
            }
            scheduler.complete(true);
            let markets = select_dynamic_markets_from_capture(config, batch.events());
            scheduler.replace_markets(markets.clone());
            for market in &markets {
                authority.readiness.register_market(market.clone());
            }
            authority
                .readiness
                .set_fresh_book_markets(markets.iter().cloned().collect());
            authority.readiness.set_metadata_current(true);
            authority.readiness.set_context_capture_current(true);
            let stream_action = authority.live.replace_persisted_scope(markets);
            tracing::info!(
                events = batch.events().len(),
                captured_at_ns = batch.captured_at().value(),
                source_digest = %batch.source_digest(),
                "persisted and admitted complete public context capture"
            );
            Ok(stream_action)
        }
        CaptureOutput::Rejected(error) => {
            scheduler.complete(false);
            authority.readiness.set_context_capture_current(false);
            tracing::warn!(
                error = %error,
                "public context capture rejected atomically; entries remain fail-closed"
            );
            Ok(StreamScopeAction::None)
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
                Ok(Some(result)) => (RecoveryOutput::Result(Box::new(result)), false),
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

/// Retains a source fact only after the authority's atomic engine append.
///
/// The input is never awaited from the authority loop: if the worker is busy,
/// it stays in the authority-owned FIFO until a later loop turn can flush it.
fn retain_recovery_source(
    sender: &mpsc::Sender<RecoveryInput>,
    authority: &mut AuthorityState,
    event: MarketEvent,
) {
    enqueue_recovery_input(sender, authority, RecoveryInput::CommittedSource(event));
}

/// Hands an already-recorded recovery request to the bounded evidence worker.
fn submit_recovery_request(
    sender: &mpsc::Sender<RecoveryInput>,
    authority: &mut AuthorityState,
    request: GapRecoveryRequest,
) {
    enqueue_recovery_input(sender, authority, RecoveryInput::Request(request));
}

/// Advances only recovery evidence watermarks from the daemon's explicit UTC
/// clock. It never reaches the engine, broker, SQLite, or Parquet directly.
fn advance_recovery_clock(
    sender: &mpsc::Sender<RecoveryInput>,
    authority: &mut AuthorityState,
    market: Market,
    at: TimestampNs,
) {
    enqueue_recovery_input(sender, authority, RecoveryInput::AdvanceTime { market, at });
}

fn enqueue_recovery_input(
    sender: &mpsc::Sender<RecoveryInput>,
    authority: &mut AuthorityState,
    input: RecoveryInput,
) {
    let market = recovery_input_market(&input).clone();
    if !authority.recovery_worker_available {
        mark_market_execution_blocked(&mut authority.readiness, market);
        return;
    }
    if authority.recovery_pending.is_empty() {
        match sender.try_send(input) {
            Ok(()) => return,
            Err(mpsc::error::TrySendError::Full(input)) => {
                authority.recovery_pending.push_back(input);
                return;
            }
            Err(mpsc::error::TrySendError::Closed(input)) => {
                fence_recovery_worker(authority, Some(recovery_input_market(&input).clone()));
                return;
            }
        }
    }
    if authority.recovery_pending.len() == RECOVERY_PENDING_CAPACITY {
        fence_recovery_worker(authority, Some(market));
        tracing::error!(
            limit = RECOVERY_PENDING_CAPACITY,
            "authority recovery input queue filled; recovery remains execution-fenced"
        );
        return;
    }
    authority.recovery_pending.push_back(input);
}

fn flush_recovery_inputs(sender: &mpsc::Sender<RecoveryInput>, authority: &mut AuthorityState) {
    while authority.recovery_worker_available {
        let Some(input) = authority.recovery_pending.pop_front() else {
            return;
        };
        match sender.try_send(input) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(input)) => {
                authority.recovery_pending.push_front(input);
                return;
            }
            Err(mpsc::error::TrySendError::Closed(input)) => {
                fence_recovery_worker(authority, Some(recovery_input_market(&input).clone()));
                return;
            }
        }
    }
}

fn fence_recovery_worker(authority: &mut AuthorityState, additional_market: Option<Market>) {
    authority.recovery_worker_available = false;
    let mut markets = authority.recovery_markets.clone();
    if let Some(market) = additional_market {
        markets.insert(market);
    }
    for input in authority.recovery_pending.drain(..) {
        markets.insert(recovery_input_market(&input).clone());
    }
    for market in markets {
        mark_market_execution_blocked(&mut authority.readiness, market);
    }
}

fn recovery_input_market(input: &RecoveryInput) -> &Market {
    match input {
        RecoveryInput::CommittedSource(event) => event.market(),
        RecoveryInput::Request(request) => request.market(),
        RecoveryInput::AdvanceTime { market, .. } => market,
    }
}

/// Applies only authority-owned recovery output through routing and the sole
/// SQLite writer.
async fn admit_recovery_output(
    writer: &mut EngineWriter,
    authority: &mut AuthorityState,
    output: RecoveryOutput,
) -> Result<(), AppError> {
    match output {
        RecoveryOutput::Result(result) => {
            let market = result.request().market().clone();
            if !authority.recovery_worker_available {
                authority.recovery_markets.remove(&market);
                mark_market_execution_blocked(&mut authority.readiness, market.clone());
                tracing::warn!(
                    market = market.as_str(),
                    "discarding recovery result after authority recovery fencing"
                );
                return Ok(());
            }
            admit_recovery_result(writer, authority, *result).await?;
            authority.recovery_markets.remove(&market);
            Ok(())
        }
        RecoveryOutput::Failed { market, error } => {
            authority.recovery_markets.remove(&market);
            fence_recovery_worker(authority, Some(market.clone()));
            tracing::error!(
                market = market.as_str(),
                error = %error,
                "recovery producer failed; market remains execution-fenced"
            );
            Ok(())
        }
    }
}

/// Accepts a completed recovery only after all of its evidence has already
/// crossed the authority's immutable source-persistence path.
async fn admit_recovery_result(
    writer: &mut EngineWriter,
    authority: &mut AuthorityState,
    result: RecoveryResult,
) -> Result<(), AppError> {
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

/// Realizes a scope action only after [`admit_capture_output`] accepted the
/// complete capture. Replacing a scope always joins the old task before a new
/// subscription epoch is started, so stale receiver output cannot bleed into
/// the newly persisted universe.
async fn apply_stream_scope_action(
    live: &mut LiveSubscription,
    action: StreamScopeAction,
    readiness: &mut Readiness,
) {
    match action {
        StreamScopeAction::None => {}
        StreamScopeAction::Stop => {
            readiness.set_stream_connected(false);
            live.mark_epoch_start_failed();
            if let Some(stream) = live.stream.take() {
                stream.shutdown().await;
            }
        }
        StreamScopeAction::Start {
            scope,
            epoch,
            shutdown_prior,
        } => {
            readiness.set_stream_connected(false);
            if shutdown_prior && let Some(stream) = live.stream.take() {
                stream.shutdown().await;
            }
            match WsConfig::new(scope) {
                Ok(config) => {
                    tracing::info!(
                        epoch,
                        markets = ?config.markets(),
                        "starting public WebSocket epoch from persisted capture scope"
                    );
                    live.stream = Some(WsClient::new(config).start());
                    live.mark_epoch_started();
                }
                Err(error) => {
                    // This is unreachable for a validated native-perpetual
                    // capture selection, but retaining no stream is safer
                    // than constructing any substituted scope.
                    live.stream = None;
                    live.mark_epoch_start_failed();
                    tracing::error!(
                        error = %error,
                        epoch,
                        "persisted capture scope cannot form a WebSocket epoch; stream remains unready"
                    );
                }
            }
        }
    }
}

/// Terminates the active receiver before the next bounded retry. The retry
/// uses only the last successfully persisted capture scope.
async fn end_live_epoch(live: &mut LiveSubscription, readiness: &mut Readiness) {
    readiness.set_stream_connected(false);
    if let Some(stream) = live.stream.take() {
        stream.shutdown().await;
    }
    live.restart_at = live
        .end_epoch()
        .and_then(|delay| tokio::time::Instant::now().checked_add(delay));
    if let Some(restart_at) = live.restart_at {
        tracing::warn!(
            epoch = live.epoch,
            scope = ?live.scope,
            delay_ms = restart_at
                .saturating_duration_since(tokio::time::Instant::now())
                .as_millis(),
            "public WebSocket epoch ended; scheduling a fresh bounded retry"
        );
    }
}

async fn wait_for_stream_restart(restart_at: Option<tokio::time::Instant>) {
    match restart_at {
        Some(restart_at) => tokio::time::sleep_until(restart_at).await,
        None => future::pending::<()>().await,
    }
}

/// Selects the next detailed scope from an already-persisted complete capture.
///
/// The authority applies this only after every normalized fact in the batch
/// was durable and admitted, so a failed or partial public response cannot
/// alter the rotating universe.
fn select_dynamic_markets_from_capture(
    config: &PaperConfig,
    events: &[MarketEvent],
) -> Vec<Market> {
    let mut metadata = BTreeMap::new();
    let mut contexts = BTreeMap::new();
    for event in events {
        match event.kind() {
            MarketEventKind::Metadata(value) => {
                metadata.insert(event.market().clone(), *value);
            }
            MarketEventKind::AssetContext(value) => {
                contexts.insert(event.market().clone(), *value);
            }
            MarketEventKind::BookSnapshot(_)
            | MarketEventKind::Bbo(_)
            | MarketEventKind::Trade(_)
            | MarketEventKind::CompletedCandle(_)
            | MarketEventKind::Funding(_) => {}
        }
    }
    let limit = usize::from(config.feed().tradeable_market_count())
        + usize::from(config.feed().warm_buffer_market_count());
    let mut markets = metadata
        .into_iter()
        .filter_map(|(market, metadata)| {
            let context = contexts.get(&market)?;
            (metadata.is_active()
                && metadata.venue_max_leverage()
                    >= u16::from(config.risk().minimum_leverage().value())
                && context.day_notional_volume() >= config.feed().minimum_daily_notional())
            .then_some((market, context.day_notional_volume().value()))
        })
        .collect::<Vec<_>>();
    markets.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
    markets
        .into_iter()
        .take(limit)
        .map(|(market, _)| market)
        .collect()
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
            | TypedEngineEvent::SourceRetained { .. }
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
    if is_verified_historical_source_retry(authority, &event)? {
        return Ok(());
    }
    let committed_source = event.clone();
    if source_is_late(authority, &committed_source)? {
        admit_typed_engine_event(
            writer,
            authority,
            TypedEngineEvent::SourceRetained {
                source: committed_source.clone(),
            },
        )
        .await?;
        if let Some(sender) = recovery_sender
            && authority.recovery_worker_available
        {
            retain_recovery_source(sender, authority, committed_source);
        }
        return Ok(());
    }
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
        retain_recovery_source(sender, authority, committed_source);
    }
    Ok(())
}

/// Filters a source retry against immutable evidence verified during restart.
///
/// This must run before a public source fact reaches either durable store. An
/// exact retry is already durable; a reused identity with changed evidence is
/// an integrity failure and never becomes a new source fact.
fn is_verified_historical_source_retry(
    authority: &AuthorityState,
    event: &MarketEvent,
) -> Result<bool, AppError> {
    let Some(historical) = authority.historical_sources.get(event.event_id()) else {
        return Ok(false);
    };
    if historical == event {
        tracing::debug!(
            event_id = event.event_id().as_str(),
            "dropping exact source retry already verified during restart reconstruction"
        );
        return Ok(true);
    }
    Err(AppError::HistoricalSourceConflict {
        event_id: event.event_id().as_str().to_owned(),
    })
}

/// Preflights a complete context capture without mutating authority state.
///
/// A capture is indivisible: accepting a subset would bind its dynamic scope
/// to facts that were never admitted in the same durable capture boundary.
fn preflight_capture_sources(
    authority: &AuthorityState,
    events: &[MarketEvent],
) -> Result<CaptureSourcePreflight, AppError> {
    let historical_count = events.iter().try_fold(0_usize, |count, event| {
        Ok::<_, AppError>(
            count + usize::from(is_verified_historical_source_retry(authority, event)?),
        )
    })?;
    if historical_count == 0 {
        return Ok(CaptureSourcePreflight::AllNew);
    }
    if historical_count == events.len() {
        return Ok(CaptureSourcePreflight::HistoricalNoOp);
    }
    Err(AppError::MixedHistoricalCapture)
}

/// Persists and routes one live source fact after the restart-evidence fence.
///
/// `false` means a verified historical retry was dropped without mutating
/// Parquet, SQLite, the engine, or recovery input state.
async fn persist_live_market_event(
    parquet_store: &ParquetStore,
    writer: &mut EngineWriter,
    authority: &mut AuthorityState,
    event: MarketEvent,
    recovery_sender: Option<&mpsc::Sender<RecoveryInput>>,
) -> Result<bool, AppError> {
    if is_verified_historical_source_retry(authority, &event)? {
        return Ok(false);
    }
    parquet_store.write_events(std::slice::from_ref(&event))?;
    admit_market_event(writer, authority, event, recovery_sender).await?;
    Ok(true)
}

fn source_is_late(authority: &AuthorityState, source: &MarketEvent) -> Result<bool, AppError> {
    let broker_boundary = authority
        .engine_state
        .as_ref()
        .ok_or(AppError::MissingEngineState)?
        .broker()
        .causal_boundary();
    let recovery_boundary = authority.router.recovery_boundary(source.market());
    Ok(source.event_time() < broker_boundary
        || recovery_boundary.is_some_and(|boundary| {
            source.event_time() <= boundary || source.received_at() <= boundary
        }))
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
            #[cfg(unix)]
            {
                let mut terminate =
                    tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                        .map_err(AppError::Signal)?;
                tokio::select! {
                    result = tokio::signal::ctrl_c() => result.map_err(AppError::Signal)?,
                    _ = terminate.recv() => {},
                }
            }
            #[cfg(not(unix))]
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
    /// The bounded public context-capture worker did not complete normally.
    #[error("public context capture task join failed")]
    CaptureTaskJoin(#[source] tokio::task::JoinError),
    /// Engine state was consumed by a failed authority transition.
    #[error("authority engine state is unavailable after a failed transition")]
    MissingEngineState,
    /// Reapplying verified historical evidence produced a different typed
    /// transition, core record sequence, or successor-state commitment.
    #[error("deterministic restart history parity failed: {reason}")]
    HistoryParity { reason: &'static str },
    /// A SQLite history transition claimed a raw source fact absent from the
    /// immutable committed Parquet stream.
    #[error("historical engine journal referenced source evidence missing from committed Parquet")]
    MissingHistoricalSourceEvidence,
    /// A Parquet fact not present in SQLite was at or before the reconstructed
    /// causal cursor, so it cannot safely be admitted as a fresh source input.
    #[error("uncommitted Parquet source evidence preceded the reconstructed causal cursor")]
    UncoveredSourceBeforeCursor,
    /// An incoming source event reused a verified historical identity with
    /// changed immutable source or receipt evidence.
    #[error("incoming source evidence conflicted with reconstructed history for event {event_id}")]
    HistoricalSourceConflict {
        /// Reused normalized source identity.
        event_id: String,
    },
    /// A complete context capture combined exact historical retries with new
    /// facts, so no atomic source/scope transition can be proved.
    #[error("context capture mixed historical retries with new source facts")]
    MixedHistoricalCapture,
    /// Persisted recovery control evidence was malformed, edited, or did not
    /// bind to its anchored raw L2 source fact.
    #[error("persisted recovery evidence was invalid")]
    InvalidRecoveryEvidence,
    /// Prior durable history ended behind an unresolved recovery fence. The
    /// ephemeral worker result cannot be recreated safely after restart.
    #[error("historical recovery was incomplete; startup remains fail-closed")]
    IncompleteRecoveryEvidence,
    /// The minimal no-entry engine state could not be initialized safely.
    #[error("paper engine initial state could not be constructed")]
    InitialEngineState,
    /// A normalized source fact could not take a typed, fail-closed route.
    #[error(transparent)]
    Routing(#[from] RoutingError),
    /// SQLite admission/write ownership could not initialize or drain.
    #[error(transparent)]
    Writer(#[from] WriterError),
    /// The pure engine rejected a reconstructed historical typed transition.
    #[error(transparent)]
    Engine(#[from] EngineError),
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
    /// The systemd watchdog task panicked or was cancelled outside shutdown.
    #[error("systemd watchdog task join failed")]
    WatchdogTaskJoin(#[source] tokio::task::JoinError),
    /// The only authority request channel was closed before shutdown.
    #[error("admin authority channel closed unexpectedly")]
    AuthorityChannelClosed,
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet, VecDeque};
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use std::time::Duration;

    use rust_decimal::Decimal;
    use serde_json::{Value, json};
    use sqlx::{Connection, SqliteConnection};
    use tokio::sync::mpsc;
    use tokio::time::timeout;
    use tokio_util::sync::CancellationToken;
    use trench_core::config::PaperConfig;
    use trench_core::event::{BookLevel, BookSnapshot};
    use trench_hyperliquid::{
        ContextCapture, ContextCaptureError, InfoClient, RecoveryStatus, RecoveryUnavailable,
        recovery_request_from_events_for_test,
    };
    use wiremock::matchers::{body_json, method, path};
    use wiremock::{Mock, MockServer, Request, ResponseTemplate};

    use super::{
        AppError, AuthorityState, CaptureOutput, LiveSubscription, RecoveryInput, RecoveryOutput,
        StreamScopeAction, SystemReceiptClock, admit_capture_output, admit_market_event,
        admit_recovery_output, admit_typed_engine_event, capture_worker, configured_path,
        current_time_ns, initial_engine_state, maximum_book_age, persist_live_market_event,
        reconstruct_authority, recover_source_stream, recovery_worker,
    };
    use crate::capture_scheduler::CaptureScheduler;
    use crate::readiness::Readiness;
    use crate::writer::EngineWriter;
    use trench_core::domain::{Market, Price, Quantity, Side};
    use trench_core::event::{MarketEvent, TimestampNs, Trade};
    use trench_storage::parquet::{DataProvenance, ParquetStore};

    const BASE_MS: i64 = 1_800_000_000;
    const BASE_NS: i64 = BASE_MS * 1_000_000;
    const HOUR_NS: i64 = 3_600_000_000_000;
    const META_FIXTURE: &str = include_str!("../../../tests/fixtures/meta/native-perps.json");
    const PAPER_CONFIG: &str = include_str!("../../../config/paper.example.toml");

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

    fn late_trade(event_at: i64, received_at: i64) -> MarketEvent {
        MarketEvent::trade(
            timestamp(event_at),
            timestamp(received_at),
            btc(),
            Trade::new(2, Side::Buy, price(100), quantity(1)).expect("fixture trade"),
        )
        .expect("fixture event")
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
            historical_sources: BTreeMap::new(),
            readiness: Readiness::default(),
            live: LiveSubscription::new(),
            reconciled: false,
            recovery_markets: BTreeSet::new(),
            recovery_pending: VecDeque::new(),
            recovery_worker_available: true,
        }
    }

    fn current_timestamp() -> TimestampNs {
        TimestampNs::new(i128::from(current_time_ns().expect("UTC clock"))).expect("UTC timestamp")
    }

    #[test]
    fn initial_empty_scope_opens_only_after_a_persisted_capture_scope() {
        let mut live = LiveSubscription::new();

        assert_eq!(
            live.replace_persisted_scope(Vec::new()),
            StreamScopeAction::None
        );
        assert_eq!(live.epoch, 0);
        assert!(live.scope.is_empty());

        assert_eq!(
            live.replace_persisted_scope(vec![btc()]),
            StreamScopeAction::Start {
                scope: vec![btc()],
                epoch: 1,
                shutdown_prior: false,
            }
        );
        assert!(
            !live.active_epoch,
            "the actual stream task starts after this action"
        );
        assert!(
            !live.observe_l2(&btc()),
            "an action is not an L2 observation"
        );
    }

    #[test]
    fn changed_persisted_scope_replaces_the_prior_subscription_epoch() {
        let mut live = LiveSubscription::new();
        let first = live.replace_persisted_scope(vec![btc()]);
        assert!(matches!(first, StreamScopeAction::Start { epoch: 1, .. }));
        live.mark_epoch_started();

        let eth = Market::new("ETH").expect("fixture market");
        assert_eq!(
            live.replace_persisted_scope(vec![eth.clone()]),
            StreamScopeAction::Start {
                scope: vec![eth],
                epoch: 2,
                shutdown_prior: true,
            },
            "a changed persisted universe must join the old task before resubscribing"
        );
        assert!(live.observed_l2.is_empty());
        assert!(
            !live.observe_l2(&btc()),
            "prior-scope L2 cannot ready the new epoch"
        );
    }

    #[test]
    fn terminal_epoch_restarts_from_the_latest_persisted_scope_with_bounded_backoff() {
        let mut live = LiveSubscription::new();
        let _ = live.replace_persisted_scope(vec![btc()]);
        live.mark_epoch_started();

        assert_eq!(live.end_epoch(), Some(Duration::from_secs(1)));
        assert!(!live.active_epoch);
        assert_eq!(
            live.restart_due(),
            StreamScopeAction::Start {
                scope: vec![btc()],
                epoch: 2,
                shutdown_prior: false,
            }
        );
        live.mark_epoch_started();
        assert_eq!(live.end_epoch(), Some(Duration::from_secs(2)));
        for _ in 0..8 {
            let _ = live.restart_due();
            live.mark_epoch_started();
            assert!(
                live.end_epoch()
                    .is_some_and(|delay| delay <= Duration::from_secs(30))
            );
        }
    }

    async fn mounted_context_capture(
        delay: Option<Duration>,
    ) -> (
        ContextCapture,
        trench_hyperliquid::ContextCaptureRequest,
        MockServer,
    ) {
        let server = MockServer::start().await;
        let metadata =
            serde_json::from_str::<Value>(META_FIXTURE).expect("fixture metadata must parse");
        let book_time_ms = current_time_ns().expect("UTC clock") / 1_000_000;
        Mock::given(method("POST"))
            .and(path("/info"))
            .respond_with(move |request: &Request| {
                let request: Value = serde_json::from_slice(&request.body)
                    .expect("capture request must remain JSON");
                let body = match request["type"].as_str() {
                    Some("metaAndAssetCtxs") => metadata.clone(),
                    Some("l2Book") => json!({
                        "coin": request["coin"].as_str().expect("book market"),
                        "time": book_time_ms,
                        "levels": [
                            [{"px": "99", "sz": "10", "n": 1}],
                            [{"px": "101", "sz": "10", "n": 1}]
                        ]
                    }),
                    Some("candleSnapshot") => {
                        let source = &request["req"];
                        let interval = source["interval"].as_str().expect("candle interval");
                        let step_ms = match interval {
                            "15m" => 900_000_i64,
                            "1h" => 3_600_000_i64,
                            _ => panic!("unexpected candle interval"),
                        };
                        let start = source["startTime"].as_i64().expect("candle start");
                        let end = source["endTime"].as_i64().expect("candle end");
                        let symbol = source["coin"].as_str().expect("candle market");
                        let mut candles = Vec::new();
                        let mut open = start;
                        while open <= end {
                            candles.push(json!({
                                "t": open,
                                "T": open + step_ms - 1,
                                "s": symbol,
                                "i": interval,
                                "o": "100",
                                "c": "100",
                                "h": "101",
                                "l": "99",
                                "v": "1",
                                "n": 1,
                            }));
                            open += step_ms;
                        }
                        Value::Array(candles)
                    }
                    Some("fundingHistory") => json!([{
                        "coin": request["coin"].as_str().expect("funding market"),
                        "fundingRate": "0.00001",
                        "premium": "0.00001",
                        "time": request["startTime"].as_i64().expect("funding start"),
                    }]),
                    _ => panic!("unexpected public context operation"),
                };
                let response = ResponseTemplate::new(200).set_body_json(body);
                match delay {
                    Some(delay) => response.set_delay(delay),
                    None => response,
                }
            })
            .mount(&server)
            .await;
        let mut scheduler = CaptureScheduler::new(vec![btc()]);
        let request = scheduler
            .dispatch(current_timestamp())
            .expect("capture schedule")
            .expect("first capture schedule");
        let client = InfoClient::new_loopback_for_test(&format!("{}/info", server.uri()))
            .expect("loopback capture client");
        (ContextCapture::new(client), request, server)
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

    fn one_trade_candle(open: i64, interval_ms: i64) -> serde_json::Value {
        json!({
            "t": open,
            "T": open + interval_ms - 1,
            "s": "BTC",
            "i": if interval_ms == 900_000 { "15m" } else { "1h" },
            "o": "100",
            "c": "100",
            "h": "100",
            "l": "100",
            "v": "1",
            "n": 1,
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
            historical_sources: BTreeMap::new(),
            readiness: Readiness::default(),
            live: LiveSubscription::new(),
            reconciled: false,
            recovery_markets: BTreeSet::new(),
            recovery_pending: VecDeque::new(),
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
    async fn cancelled_context_capture_does_not_publish_a_partial_batch() {
        let (capture, request, _server) =
            mounted_context_capture(Some(Duration::from_secs(2))).await;
        let cancellation = CancellationToken::new();
        let (input_sender, input_receiver) = mpsc::channel(1);
        let (output_sender, mut output_receiver) = mpsc::channel(1);
        let task = tokio::spawn(capture_worker(
            capture,
            input_receiver,
            output_sender,
            cancellation.clone(),
        ));
        input_sender
            .send(request)
            .await
            .expect("capture request handoff");
        tokio::time::sleep(Duration::from_millis(25)).await;
        cancellation.cancel();

        timeout(Duration::from_secs(1), task)
            .await
            .expect("capture worker must observe cancellation")
            .expect("capture worker must not panic");
        assert!(
            output_receiver.recv().await.is_none(),
            "cancellation must not publish an incomplete capture"
        );
    }

    #[tokio::test]
    async fn rejected_context_capture_degrades_readiness_without_persisting_facts() {
        let directory = tempfile::tempdir().expect("fixture root");
        fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700))
            .expect("private fixture root");
        let store = ParquetStore::open(directory.path(), provenance()).expect("fixture store");
        let mut writer = EngineWriter::open(
            directory.path().join("trench.sqlite"),
            "run-capture-rejected",
            current_time_ns().expect("UTC clock"),
        )
        .await
        .expect("fixture writer");
        let mut authority = authority("run-capture-rejected");
        let mut scheduler = CaptureScheduler::new(vec![btc()]);
        let config = PaperConfig::from_toml(PAPER_CONFIG).expect("fixture config");
        let _ = scheduler
            .dispatch(current_timestamp())
            .expect("capture schedule")
            .expect("in-flight capture");

        let stream_action = admit_capture_output(
            &store,
            &mut writer,
            &mut authority,
            CaptureOutput::Rejected(ContextCaptureError::IncompleteCandles {
                market: btc(),
                interval: trench_hyperliquid::CandleInterval::FifteenMinutes,
            }),
            &mut scheduler,
            &config,
            None,
        )
        .await
        .expect("rejected capture is a handled readiness transition");

        assert!(!scheduler.in_flight());
        assert_eq!(stream_action, StreamScopeAction::None);
        assert!(
            authority
                .readiness
                .global_blockers()
                .contains(&crate::readiness::GlobalBlocker::ContextCapture)
        );
        assert!(store.partitions().expect("source partitions").is_empty());
        assert_eq!(
            writer
                .journal_counts()
                .await
                .expect("journal counts")
                .events,
            0
        );
    }

    #[tokio::test]
    async fn complete_historical_capture_is_a_no_op_before_every_mutation() {
        let (capture, request, _server) = mounted_context_capture(None).await;
        let batch = capture
            .capture(&request, &SystemReceiptClock)
            .await
            .expect("complete public context batch");
        let directory = tempfile::tempdir().expect("fixture root");
        fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700))
            .expect("private fixture root");
        let store = ParquetStore::open(directory.path(), provenance()).expect("fixture store");
        let mut writer = EngineWriter::open(
            directory.path().join("trench.sqlite"),
            "run-capture-historical-noop",
            current_time_ns().expect("UTC clock"),
        )
        .await
        .expect("fixture writer");
        let mut authority = authority("run-capture-historical-noop");
        authority.historical_sources = batch
            .events()
            .iter()
            .map(|event| (event.event_id().clone(), event.clone()))
            .collect();
        let _ = authority.live.replace_persisted_scope(vec![btc()]);
        let scope_before = authority.live.scope.clone();
        let epoch_before = authority.live.epoch;
        let readiness_before = authority.readiness.snapshot();
        let mut scheduler = CaptureScheduler::new(vec![btc()]);
        let _ = scheduler
            .dispatch(current_timestamp())
            .expect("capture schedule")
            .expect("in-flight capture");
        let config = PaperConfig::from_toml(PAPER_CONFIG).expect("fixture config");

        let action = admit_capture_output(
            &store,
            &mut writer,
            &mut authority,
            CaptureOutput::Captured(batch),
            &mut scheduler,
            &config,
            None,
        )
        .await
        .expect("complete historical capture is a safe no-op");

        assert_eq!(action, StreamScopeAction::None);
        assert!(
            scheduler.in_flight(),
            "the retry must not complete the schedule"
        );
        assert!(store.partitions().expect("source partitions").is_empty());
        assert_eq!(
            writer
                .journal_counts()
                .await
                .expect("journal counts")
                .events,
            0
        );
        assert_eq!(authority.live.scope, scope_before);
        assert_eq!(authority.live.epoch, epoch_before);
        assert_eq!(authority.readiness.snapshot(), readiness_before);
    }

    #[tokio::test]
    async fn mixed_historical_capture_fails_closed_before_every_mutation() {
        let (capture, request, _server) = mounted_context_capture(None).await;
        let batch = capture
            .capture(&request, &SystemReceiptClock)
            .await
            .expect("complete public context batch");
        let historical = batch
            .events()
            .first()
            .expect("complete capture has source facts")
            .clone();
        assert!(
            batch.events().len() > 1,
            "fixture must include both historical and new source facts"
        );
        let directory = tempfile::tempdir().expect("fixture root");
        fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700))
            .expect("private fixture root");
        let store = ParquetStore::open(directory.path(), provenance()).expect("fixture store");
        let mut writer = EngineWriter::open(
            directory.path().join("trench.sqlite"),
            "run-capture-mixed-history",
            current_time_ns().expect("UTC clock"),
        )
        .await
        .expect("fixture writer");
        let mut authority = authority("run-capture-mixed-history");
        authority
            .historical_sources
            .insert(historical.event_id().clone(), historical);
        let _ = authority.live.replace_persisted_scope(vec![btc()]);
        let scope_before = authority.live.scope.clone();
        let epoch_before = authority.live.epoch;
        let readiness_before = authority.readiness.snapshot();
        let mut scheduler = CaptureScheduler::new(vec![btc()]);
        let _ = scheduler
            .dispatch(current_timestamp())
            .expect("capture schedule")
            .expect("in-flight capture");
        let config = PaperConfig::from_toml(PAPER_CONFIG).expect("fixture config");

        let error = admit_capture_output(
            &store,
            &mut writer,
            &mut authority,
            CaptureOutput::Captured(batch),
            &mut scheduler,
            &config,
            None,
        )
        .await
        .expect_err("a mixed historical capture cannot cross an atomic boundary");

        assert!(matches!(error, AppError::MixedHistoricalCapture));
        assert!(
            scheduler.in_flight(),
            "the failed batch must not complete the schedule"
        );
        assert!(store.partitions().expect("source partitions").is_empty());
        assert_eq!(
            writer
                .journal_counts()
                .await
                .expect("journal counts")
                .events,
            0
        );
        assert_eq!(authority.live.scope, scope_before);
        assert_eq!(authority.live.epoch, epoch_before);
        assert_eq!(authority.readiness.snapshot(), readiness_before);
    }

    #[tokio::test]
    async fn complete_context_capture_persists_before_authority_admission() {
        let (capture, request, _server) = mounted_context_capture(None).await;
        let batch = capture
            .capture(&request, &SystemReceiptClock)
            .await
            .expect("complete public context batch");
        let source_ids = batch.source_ids().cloned().collect::<BTreeSet<_>>();
        let directory = tempfile::tempdir().expect("fixture root");
        fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700))
            .expect("private fixture root");
        let store = ParquetStore::open(directory.path(), provenance()).expect("fixture store");
        let mut writer = EngineWriter::open(
            directory.path().join("trench.sqlite"),
            "run-capture-ordering",
            current_time_ns().expect("UTC clock"),
        )
        .await
        .expect("fixture writer");
        let mut authority = authority("run-capture-ordering");
        authority.engine_state = None;
        let mut scheduler = CaptureScheduler::new(vec![btc()]);
        let config = PaperConfig::from_toml(PAPER_CONFIG).expect("fixture config");
        let _ = scheduler
            .dispatch(current_timestamp())
            .expect("capture schedule")
            .expect("in-flight capture");

        let error = admit_capture_output(
            &store,
            &mut writer,
            &mut authority,
            CaptureOutput::Captured(batch),
            &mut scheduler,
            &config,
            None,
        )
        .await
        .expect_err("missing authority state must reject the first engine admission");
        assert!(matches!(error, super::AppError::MissingEngineState));

        let persisted_ids = store
            .partitions()
            .expect("source partitions")
            .into_iter()
            .flat_map(|manifest| store.read_partition(&manifest).expect("source partition"))
            .map(|event| event.event_id().clone())
            .collect::<BTreeSet<_>>();
        assert_eq!(
            persisted_ids, source_ids,
            "the complete batch must reach atomic Parquet before any authority admission"
        );
        assert_eq!(
            writer
                .journal_counts()
                .await
                .expect("journal counts")
                .events,
            0,
            "the induced authority failure occurs after source persistence"
        );
    }

    #[tokio::test]
    async fn restart_live_boundary_drops_exact_historical_retries_before_durable_mutation() {
        let root = tempfile::tempdir().expect("fixture root");
        fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700))
            .expect("private fixture root");
        let provenance = provenance();
        let store = ParquetStore::open(root.path(), provenance.clone()).expect("fixture store");
        let database = root.path().join("trench.sqlite");
        let committed = predecessor();
        store
            .write_events(std::slice::from_ref(&committed))
            .expect("committed source evidence");

        let mut initial_writer = EngineWriter::open(&database, "run-initial", BASE_NS)
            .await
            .expect("fresh initial writer");
        let mut initial_authority = authority("run-initial");
        admit_market_event(
            &mut initial_writer,
            &mut initial_authority,
            committed.clone(),
            None,
        )
        .await
        .expect("initial atomic admission");
        assert_eq!(
            initial_writer
                .journal_counts()
                .await
                .expect("initial counts")
                .events,
            1
        );
        drop(initial_writer);

        let fresh = late_trade(BASE_NS + 1, BASE_NS + 1);
        store
            .write_events(std::slice::from_ref(&fresh))
            .expect("new source retained before restart");
        let history = EngineWriter::inspect_history(&database)
            .await
            .expect("complete immutable history");
        let recovered = recover_source_stream(root.path(), provenance)
            .expect("verified source replay")
            .expect("source evidence");
        let mut restored =
            reconstruct_authority(Some(&recovered), &history, "run-restarted", BASE_NS + 100)
                .expect("history must reconstruct before opening a writer");
        assert_eq!(restored.fresh_events, vec![fresh.clone()]);
        assert_eq!(
            restored
                .authority
                .engine_state
                .as_ref()
                .expect("reconstructed state")
                .broker()
                .causal_boundary(),
            initial_authority
                .engine_state
                .as_ref()
                .expect("initial state")
                .broker()
                .causal_boundary(),
            "reconstruction must exactly preserve the executed causal state"
        );
        assert_eq!(
            EngineWriter::inspect_history(&database)
                .await
                .expect("read-only restart verification"),
            history,
            "reconstruction itself must not duplicate a SQLite source or checkpoint"
        );

        let mut restarted_writer = EngineWriter::open_after_reconstruction(
            &database,
            "run-restarted",
            BASE_NS + 100,
            &history,
        )
        .await
        .expect("writer opens only after successful reconstruction");
        let reconstructed_boundary = restored
            .authority
            .engine_state
            .as_ref()
            .expect("reconstructed state")
            .broker()
            .causal_boundary();
        let source_events_before = store
            .partitions()
            .expect("source partitions before live retry")
            .into_iter()
            .flat_map(|manifest| store.read_partition(&manifest).expect("source partition"))
            .collect::<Vec<_>>();
        let admitted = persist_live_market_event(
            &store,
            &mut restarted_writer,
            &mut restored.authority,
            committed.clone(),
            None,
        )
        .await
        .expect("exact historic live retry is dropped before Parquet or SQLite admission");
        assert!(
            !admitted,
            "the live intake boundary must report an exact historical retry as dropped"
        );
        let source_events_after_retry = store
            .partitions()
            .expect("source partitions after live retry")
            .into_iter()
            .flat_map(|manifest| store.read_partition(&manifest).expect("source partition"))
            .collect::<Vec<_>>();
        assert_eq!(
            source_events_after_retry, source_events_before,
            "an exact historical live retry must not mutate Parquet"
        );
        assert_eq!(
            restarted_writer
                .journal_counts()
                .await
                .expect("restarted counts after retry")
                .events,
            0,
            "a historic retry must not duplicate source or checkpoint persistence"
        );
        assert_eq!(
            restored
                .authority
                .engine_state
                .as_ref()
                .expect("state after retry")
                .broker()
                .causal_boundary(),
            reconstructed_boundary,
            "dropping a historic retry must not alter source-clock or late-source state"
        );
        let conflicting_receipt = MarketEvent::trade(
            timestamp(BASE_NS),
            timestamp(BASE_NS + 1),
            btc(),
            Trade::new(1, Side::Buy, price(100), quantity(1)).expect("fixture trade"),
        )
        .expect("same source identity with altered receipt evidence");
        assert_eq!(conflicting_receipt.event_id(), committed.event_id());
        assert_ne!(conflicting_receipt, committed);
        let conflict = persist_live_market_event(
            &store,
            &mut restarted_writer,
            &mut restored.authority,
            conflicting_receipt,
            None,
        )
        .await
        .expect_err("changed receipt evidence must fail closed rather than be dropped");
        assert!(matches!(
            conflict,
            AppError::HistoricalSourceConflict { .. }
        ));
        assert_eq!(
            restarted_writer
                .journal_counts()
                .await
                .expect("restarted counts after conflict")
                .events,
            0
        );
        let source_events_after_conflict = store
            .partitions()
            .expect("source partitions after rejected conflict")
            .into_iter()
            .flat_map(|manifest| store.read_partition(&manifest).expect("source partition"))
            .collect::<Vec<_>>();
        assert_eq!(
            source_events_after_conflict, source_events_before,
            "conflicting historical evidence must not mutate Parquet"
        );
        admit_market_event(&mut restarted_writer, &mut restored.authority, fresh, None)
            .await
            .expect("only the fresh source fact is appended");
        assert_eq!(
            restarted_writer
                .journal_counts()
                .await
                .expect("restarted run counts")
                .events,
            1,
            "the restarted run contains only newly admitted source evidence"
        );
        drop(restarted_writer);
        let complete_history = EngineWriter::inspect_history(&database)
            .await
            .expect("complete history after fresh append");
        assert_eq!(complete_history.events().len(), 2);
        assert_eq!(
            complete_history.events()[0].event_id(),
            committed.event_id().as_str()
        );
        assert_ne!(
            complete_history.events()[1].event_id(),
            committed.event_id().as_str(),
            "the new run must append the one previously uncommitted source fact"
        );
        assert_eq!(complete_history.events()[1].run_id(), "run-restarted");
    }

    #[tokio::test]
    async fn restart_reuses_the_original_live_open_boundary_when_late_history_sorts_first() {
        let root = tempfile::tempdir().expect("fixture root");
        fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700))
            .expect("private fixture root");
        let provenance = provenance();
        let store = ParquetStore::open(root.path(), provenance.clone()).expect("fixture store");
        let database = root.path().join("trench.sqlite");
        let first = MarketEvent::trade(
            timestamp(BASE_NS + 100),
            timestamp(BASE_NS + 100),
            btc(),
            Trade::new(1, Side::Buy, price(100), quantity(1)).expect("fixture trade"),
        )
        .expect("fixture source");
        let late = late_trade(BASE_NS + 50, BASE_NS + 101);
        let mut writer = EngineWriter::open(&database, "run-live-boundary", BASE_NS)
            .await
            .expect("fresh writer");
        let mut initial_authority = authority("run-live-boundary");
        for source in [first, late] {
            store
                .write_events(std::slice::from_ref(&source))
                .expect("committed source evidence");
            admit_market_event(&mut writer, &mut initial_authority, source, None)
                .await
                .expect("initial source admission");
        }
        let expected_boundary = initial_authority
            .engine_state
            .as_ref()
            .expect("initial state")
            .broker()
            .causal_boundary();
        drop(writer);

        let history = EngineWriter::inspect_history(&database)
            .await
            .expect("history");
        let recovered = recover_source_stream(root.path(), provenance)
            .expect("verified raw source replay")
            .expect("raw source evidence");
        assert!(
            recovered.events[0].event_time() < recovered.events[1].event_time(),
            "the deterministic Parquet order deliberately differs from original live admission"
        );
        let restored = reconstruct_authority(
            Some(&recovered),
            &history,
            "run-restarted",
            BASE_NS + 200,
        )
        .expect("the original live opening boundary must be tried before replay-derived fallback");
        assert!(restored.fresh_events.is_empty());
        assert_eq!(
            restored
                .authority
                .engine_state
                .as_ref()
                .expect("restored state")
                .broker()
                .causal_boundary(),
            expected_boundary
        );
    }

    #[tokio::test]
    async fn failed_atomic_capture_publication_cannot_activate_a_stream_scope() {
        let (capture, request, _server) = mounted_context_capture(None).await;
        let batch = capture
            .capture(&request, &SystemReceiptClock)
            .await
            .expect("complete public context batch");
        let directory = tempfile::tempdir().expect("fixture root");
        fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700))
            .expect("private fixture root");
        let store = ParquetStore::open(directory.path(), provenance()).expect("fixture store");
        // An already-published ordinary source fact makes this distinct atomic
        // capture fail its duplicate fence before any batch commit marker can
        // become visible.
        store
            .write_events(std::slice::from_ref(&batch.events()[0]))
            .expect("fixture conflicting source fact");
        let mut writer = EngineWriter::open(
            directory.path().join("trench.sqlite"),
            "run-capture-publication-failure",
            current_time_ns().expect("UTC clock"),
        )
        .await
        .expect("fixture writer");
        let mut authority = authority("run-capture-publication-failure");
        let mut scheduler = CaptureScheduler::new(Vec::new());
        let config = PaperConfig::from_toml(PAPER_CONFIG).expect("fixture config");
        let _ = scheduler
            .dispatch(current_timestamp())
            .expect("capture schedule")
            .expect("metadata-only capture schedule");

        let error = admit_capture_output(
            &store,
            &mut writer,
            &mut authority,
            CaptureOutput::Captured(batch),
            &mut scheduler,
            &config,
            None,
        )
        .await
        .expect_err("atomic publication conflict must reject the complete capture");

        assert!(matches!(error, super::AppError::Storage(_)));
        assert!(authority.live.scope.is_empty());
        assert!(!authority.live.active_epoch);
        assert!(scheduler.in_flight());
        assert_eq!(
            writer
                .journal_counts()
                .await
                .expect("journal counts")
                .events,
            0,
            "no capture fact may reach the writer after atomic publication failure"
        );
    }

    #[tokio::test]
    async fn complete_context_capture_routes_every_persisted_fact_through_the_writer() {
        let (capture, request, _server) = mounted_context_capture(None).await;
        let batch = capture
            .capture(&request, &SystemReceiptClock)
            .await
            .expect("complete public context batch");
        let event_count = batch.events().len();
        let directory = tempfile::tempdir().expect("fixture root");
        fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700))
            .expect("private fixture root");
        let store = ParquetStore::open(directory.path(), provenance()).expect("fixture store");
        let mut writer = EngineWriter::open(
            directory.path().join("trench.sqlite"),
            "run-capture-complete",
            current_time_ns().expect("UTC clock"),
        )
        .await
        .expect("fixture writer");
        let mut authority = authority("run-capture-complete");
        let mut scheduler = CaptureScheduler::new(vec![btc()]);
        let config = PaperConfig::from_toml(PAPER_CONFIG).expect("fixture config");
        let _ = scheduler
            .dispatch(current_timestamp())
            .expect("capture schedule")
            .expect("in-flight capture");

        let stream_action = admit_capture_output(
            &store,
            &mut writer,
            &mut authority,
            CaptureOutput::Captured(batch),
            &mut scheduler,
            &config,
            None,
        )
        .await
        .expect("complete batch must persist and route");

        assert!(!scheduler.in_flight());
        assert!(matches!(
            stream_action,
            StreamScopeAction::Start {
                shutdown_prior: false,
                ..
            }
        ));
        assert!(
            !authority.live.active_epoch,
            "only the authority loop starts the returned persisted-capture epoch"
        );
        assert!(
            !authority.live.scope.is_empty(),
            "the capture-derived dynamic scope is retained for the fresh epoch"
        );
        assert!(
            !authority
                .readiness
                .global_blockers()
                .contains(&crate::readiness::GlobalBlocker::ContextCapture)
        );
        assert_eq!(
            writer
                .journal_counts()
                .await
                .expect("journal counts")
                .events,
            i64::try_from(event_count).expect("bounded capture event count"),
            "every persisted normalized fact must use the sole authority writer"
        );
    }

    #[tokio::test]
    async fn restart_rejects_missing_or_tampered_source_evidence() {
        let root = tempfile::tempdir().expect("fixture root");
        fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700))
            .expect("private fixture root");
        let provenance = provenance();
        let database = root.path().join("trench.sqlite");
        let source = predecessor();
        let mut writer = EngineWriter::open(&database, "run-corrupt", BASE_NS)
            .await
            .expect("fresh writer");
        let mut initial_authority = authority("run-corrupt");
        admit_market_event(&mut writer, &mut initial_authority, source, None)
            .await
            .expect("durable source transition");
        drop(writer);

        let missing = reconstruct_authority(
            None,
            &EngineWriter::inspect_history(&database)
                .await
                .expect("history"),
            "run-next",
            BASE_NS + 1,
        )
        .expect_err("SQLite history without committed Parquet source must fail closed");
        assert!(matches!(missing, AppError::MissingHistoricalSourceEvidence));

        let store = ParquetStore::open(root.path(), provenance.clone()).expect("fixture store");
        let source = predecessor();
        store
            .write_events(std::slice::from_ref(&source))
            .expect("restore source evidence");
        let mut connection =
            SqliteConnection::connect(&format!("sqlite://{}", database.to_string_lossy()))
                .await
                .expect("fixture database connection");
        sqlx::query("UPDATE events SET payload_json = '{}' WHERE event_id = ?1")
            .bind(source.event_id().as_str())
            .execute(&mut connection)
            .await
            .expect("fixture corruption");
        connection.close().await.expect("fixture connection closes");

        let tampered_history = EngineWriter::inspect_history(&database)
            .await
            .expect("history remains structurally complete");
        let recovered = recover_source_stream(root.path(), provenance)
            .expect("verified raw source evidence")
            .expect("raw source evidence");
        let tampered =
            reconstruct_authority(Some(&recovered), &tampered_history, "run-next", BASE_NS + 1)
                .expect_err("edited SQLite source evidence must fail parity");
        assert!(matches!(tampered, AppError::HistoryParity { .. }));
    }

    #[tokio::test]
    async fn restart_rejects_incomplete_recovery_evidence() {
        let root = tempfile::tempdir().expect("fixture root");
        fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700))
            .expect("private fixture root");
        let provenance = provenance();
        let store = ParquetStore::open(root.path(), provenance.clone()).expect("fixture store");
        let database = root.path().join("trench.sqlite");
        let predecessor = predecessor();
        let anchor = snapshot(BASE_NS + 1, 1);
        let request = recovery_request_from_events_for_test(1, Some(&predecessor), &anchor);
        let mut writer = EngineWriter::open(&database, "run-incomplete-recovery", BASE_NS)
            .await
            .expect("fresh writer");
        let mut initial_authority = authority("run-incomplete-recovery");
        for source in [predecessor.clone(), anchor.clone()] {
            store
                .write_events(std::slice::from_ref(&source))
                .expect("committed source evidence");
            admit_market_event(&mut writer, &mut initial_authority, source, None)
                .await
                .expect("source admission");
        }
        admit_typed_engine_event(
            &mut writer,
            &mut initial_authority,
            crate::execution::TypedEngineEvent::recovery_requested(&request),
        )
        .await
        .expect("durable recovery request evidence");
        drop(writer);

        let history = EngineWriter::inspect_history(&database)
            .await
            .expect("complete journal rows");
        let recovered = recover_source_stream(root.path(), provenance)
            .expect("verified raw source replay")
            .expect("raw source evidence");
        let error = reconstruct_authority(Some(&recovered), &history, "run-next", BASE_NS + 10)
            .expect_err("a pending recovery worker result cannot be recreated after restart");
        assert!(matches!(error, AppError::IncompleteRecoveryEvidence));
    }

    #[tokio::test]
    async fn receipt_causal_boundary_retains_late_source_without_broker_rollback() {
        let directory = tempfile::tempdir().expect("fixture root");
        fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700))
            .expect("private fixture root");
        let mut writer = EngineWriter::open(
            directory.path().join("trench.sqlite"),
            "run-receipt-boundary",
            BASE_NS,
        )
        .await
        .expect("fixture writer");
        let mut authority = authority("run-receipt-boundary");
        let first = late_trade(BASE_NS + 100, BASE_NS + 1_000);
        admit_market_event(&mut writer, &mut authority, first, None)
            .await
            .expect("received-at boundary advances the broker clock");
        assert_eq!(
            authority
                .engine_state
                .as_ref()
                .expect("engine state")
                .broker()
                .causal_boundary(),
            timestamp(BASE_NS + 1_000)
        );

        let delayed = late_trade(BASE_NS + 200, BASE_NS + 1_001);
        assert!(
            super::source_is_late(&authority, &delayed).expect("late source classification"),
            "an older exchange timestamp must not be replayed through the newer broker clock"
        );
        admit_market_event(&mut writer, &mut authority, delayed, None)
            .await
            .expect("late source remains durably retained");
        assert_eq!(
            authority
                .engine_state
                .as_ref()
                .expect("engine state")
                .broker()
                .causal_boundary(),
            timestamp(BASE_NS + 1_000)
        );
        assert_eq!(
            writer
                .journal_counts()
                .await
                .expect("source-only journal count")
                .events,
            2,
            "the retained late source remains durably auditable"
        );
    }

    #[tokio::test]
    async fn authority_drains_over_seventeen_completed_recoveries_without_send_deadlock() {
        let (input_sender, mut input_receiver) = mpsc::channel(super::RECOVERY_CHANNEL_CAPACITY);
        let (completed_sender, mut completed_receiver) =
            mpsc::channel(super::RECOVERY_CHANNEL_CAPACITY);
        let worker = tokio::spawn(async move {
            while let Some(input) = input_receiver.recv().await {
                if completed_sender.send(input).await.is_err() {
                    return;
                }
            }
        });
        let mut authority = authority("run-recovery-backpressure");
        let input_count = super::RECOVERY_CHANNEL_CAPACITY
            .checked_mul(2)
            .and_then(|count| count.checked_add(17))
            .expect("bounded fixture input count");
        for offset in 0..input_count {
            super::enqueue_recovery_input(
                &input_sender,
                &mut authority,
                RecoveryInput::AdvanceTime {
                    market: btc(),
                    at: timestamp(BASE_NS + i64::try_from(offset).expect("fixture offset")),
                },
            );
        }
        assert!(
            !authority.recovery_pending.is_empty(),
            "rapid inputs must remain authority-owned while worker output is bounded"
        );

        for _ in 0..input_count {
            timeout(Duration::from_secs(1), completed_receiver.recv())
                .await
                .expect("completed recovery work must continue draining")
                .expect("worker completion channel stays open");
            super::flush_recovery_inputs(&input_sender, &mut authority);
        }
        assert!(authority.recovery_pending.is_empty());
        drop(input_sender);
        timeout(Duration::from_secs(1), worker)
            .await
            .expect("worker must shut down after the authority closes its input")
            .expect("worker task must not panic");
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
        admit_recovery_output(&mut writer, &mut authority, output)
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
                one_trade_candle(BASE_MS, 900_000),
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
                ResponseTemplate::new(200)
                    .set_body_json(vec![one_trade_candle(BASE_MS, 3_600_000)]),
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
        let anchor_at = BASE_NS + 300_000_000_000;
        let anchor = snapshot(anchor_at, 1);
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
        let late_backfill = late_trade(BASE_NS + 1, anchor_at + 1);
        store
            .write_events(std::slice::from_ref(&late_backfill))
            .expect("late source remains durably auditable before recovery");
        admit_market_event(&mut writer, &mut authority, late_backfill.clone(), None)
            .await
            .expect("late source must not move the broker clock backward");
        assert_eq!(
            authority
                .engine_state
                .as_ref()
                .expect("engine state")
                .broker()
                .causal_boundary(),
            timestamp(anchor_at)
        );

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
        input_sender
            .send(RecoveryInput::CommittedSource(late_backfill.clone()))
            .await
            .expect("late source remains available to the bounded recovery worker");
        input_sender
            .send(RecoveryInput::AdvanceTime {
                market: btc(),
                at: timestamp(BASE_NS + HOUR_NS),
            })
            .await
            .expect("explicit recovery watermark");
        let output = next_recovery_output(&mut output_receiver).await;
        assert!(matches!(
            &output,
            RecoveryOutput::Result(result)
                if matches!(result.status(), RecoveryStatus::Reconciled { .. })
                    && result.backfill_events() == std::slice::from_ref(&late_backfill)
        ));
        admit_recovery_output(&mut writer, &mut authority, output)
            .await
            .expect("authority must route reconciled recovery");
        let recovered_events = store
            .partitions()
            .expect("source partitions")
            .into_iter()
            .flat_map(|manifest| store.read_partition(&manifest).expect("source partition"))
            .collect::<Vec<_>>();
        assert_eq!(
            recovered_events
                .iter()
                .filter(|event| event.event_id() == late_backfill.event_id())
                .count(),
            1,
            "recovery result must not write its already-retained source twice"
        );
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
