//! Bounded daemon lifecycle and startup recovery orchestration.

use std::collections::BTreeMap;
use std::future;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use thiserror::Error;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use trench_core::broker::{BrokerConfig, BrokerRunContext, PaperBroker};
use trench_core::config::PaperConfig;
use trench_core::domain::{LedgerId, RulesMode, RunId, Usdc};
use trench_core::engine::{
    Engine, EngineContext, EngineEvent, EngineState, SnapshotBindings, StrategyFingerprints,
};
use trench_core::event::{DurationNs, MarketEvent, MarketEventKind, TimestampNs};
use trench_core::ledger::LedgerState;
use trench_core::universe::UniverseSelector;
use trench_hyperliquid::{GapEvent, InfoClient, WsClient, WsConfig, WsOutput, WsStream};
use trench_storage::parquet::{DataProvenance, ParquetError, ParquetStore};
use trench_storage::replay::{DeterministicReplay, ReplayError, ReplayPlan};

use crate::admin::{
    AdminError, AdminServer, AuthorityRequest, DaemonMode, DaemonStatus, authority_channel,
};
use crate::readiness::Readiness;
use crate::writer::{EngineWriter, SourceEvent, WriterError};

const SOURCE_CHANNEL_CAPACITY: usize = 128;

/// One requested daemon lifecycle mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeMode {
    /// Persist/observe public market data only; no entries may be evaluated.
    Collect,
    /// Start the long-lived observation daemon without an active strategy reactor.
    ///
    /// This mode exposes readiness for a later strategy activation, but it is
    /// deliberately non-executing until typed recovery and strategy adapters
    /// are installed as one atomic authority path.
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
    readiness: Readiness,
    reconciled: bool,
}

/// Starts the paper-only daemon and waits for a bounded duration or Ctrl-C.
///
/// Recovery validates the atomic Parquet source stream before any public feed
/// would be allowed to subscribe. It never parses a debugging checkpoint as
/// executable state and never recreates a prior decision from partial evidence.
pub async fn run(
    config_path: &Path,
    config_bytes: &[u8],
    config: &PaperConfig,
    mode: RuntimeMode,
    duration: Option<Duration>,
) -> Result<(), AppError> {
    require_collection_only(config)?;
    let sqlite_path = configured_path(config_path, config.storage().sqlite_path())?;
    let parquet_path = configured_path(config_path, config.storage().parquet_path())?;
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
        readiness: Readiness::default(),
        reconciled: false,
    };
    authority.readiness.set_storage_writable(true);
    authority
        .readiness
        .set_ntp_synchronized(local_ntp_synchronized());
    authority.readiness.set_rules_configuration_valid(false);
    authority.readiness.set_rules_sleeve_warm(false);
    tracing::info!(
        mode = ?mode,
        "strategy/recovery reactors are not active; daemon is sealed collection-only"
    );
    let cancellation = CancellationToken::new();
    let (source_sender, mut source_receiver) = mpsc::channel(SOURCE_CHANNEL_CAPACITY);
    let replay_task = tokio::spawn(replay_producer(
        recovery
            .as_ref()
            .map_or_else(Vec::new, |recovery| recovery.events.clone()),
        source_sender,
        cancellation.clone(),
    ));
    while let Some(event) = source_receiver.recv().await {
        admit_source_event(&mut writer, &mut authority, event).await?;
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
                        update_readiness_from_market_event(&mut authority.readiness, &event);
                        admit_source_event(&mut writer, &mut authority, event).await?;
                    }
                    Some(WsOutput::Gap(gap)) => {
                        let market = gap_market(&gap).clone();
                        if let Some(gates) = authority.readiness.market_gates_mut(&market) {
                            gates.set_recovered(false);
                            gates.set_executable_book(false);
                        }
                    }
                    Some(WsOutput::RecoveryRequest(request)) => {
                        let market = request.market().clone();
                        if let Some(gates) = authority.readiness.market_gates_mut(&market) {
                            gates.set_recovered(false);
                            gates.set_executable_book(false);
                        }
                    }
                    Some(WsOutput::Rejected(_)) => {}
                    Some(WsOutput::Terminal(_)) | None => {
                        authority.readiness.set_stream_connected(false);
                        live_stream = None;
                    }
                }
            }
        }
    }

    cancellation.cancel();
    if let Some(stream) = live_stream {
        stream.shutdown().await;
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
    config_path: &Path,
    config_bytes: &[u8],
    config: &PaperConfig,
    manifest: &Path,
) -> Result<ReplayReport, AppError> {
    let expected = provenance(config_bytes)?;
    let plan = ReplayPlan::read_from(manifest)?;
    if plan.provenance() != &expected {
        return Err(AppError::ReplayProvenanceMismatch);
    }
    let parquet_path = configured_path(config_path, config.storage().parquet_path())?;
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

fn update_readiness_from_market_event(readiness: &mut Readiness, event: &MarketEvent) {
    readiness.set_stream_connected(true);
    readiness.register_market(event.market().clone());
    if let Some(gates) = readiness.market_gates_mut(event.market()) {
        gates.set_data_quality_valid(true);
        gates.set_common_features_warm(false);
        if matches!(event.kind(), MarketEventKind::BookSnapshot(_)) {
            // An initial WebSocket snapshot has not yet completed a full gap
            // recovery/backfill cycle, so it is deliberately not `recovered`.
            gates.set_executable_book(true);
        }
    }
}

fn gap_market(gap: &GapEvent) -> &trench_core::domain::Market {
    match gap {
        GapEvent::Opened(opened) => opened.market(),
        GapEvent::ReconnectExhausted(exhausted) => exhausted.market(),
    }
}

/// Durably records source-clock progression without interpreting an event as a
/// trade, mark, book, funding, or recovery transition.
///
/// A typed market/recovery adapter must be introduced together with strategy
/// activation. Until then this boundary is intentionally collection-only: it
/// cannot create positions, submit entry candidates, or route exits.
async fn admit_source_event(
    writer: &mut EngineWriter,
    authority: &mut AuthorityState,
    event: MarketEvent,
) -> Result<(), AppError> {
    let source = SourceEvent::new(
        writer.run_id(),
        event.event_id().as_str(),
        event.event_time().value(),
        market_event_kind_code(event.kind()),
        market_event_payload(&event)?,
    );
    let prior = authority
        .engine_state
        .take()
        .ok_or(AppError::MissingEngineState)?;
    let event_id = event.event_id().clone();
    let at = event.event_time();
    let outcome = writer
        .admit_apply_append(LedgerId::RulesOnly, &source, move |admission| {
            Engine::apply(
                EngineEvent::AdvanceTime { event_id, at },
                prior,
                &engine_context(admission)?,
            )
        })
        .await?;
    authority.engine_state = Some(outcome.into_parts().0);
    Ok(())
}

fn market_event_kind_code(kind: &MarketEventKind) -> &'static str {
    match kind {
        MarketEventKind::Metadata(_) => "metadata",
        MarketEventKind::AssetContext(_) => "asset_context",
        MarketEventKind::BookSnapshot(_) => "book_snapshot",
        MarketEventKind::Bbo(_) => "bbo",
        MarketEventKind::Trade(_) => "trade",
        MarketEventKind::Funding(_) => "funding",
        MarketEventKind::CompletedCandle(_) => "completed_candle",
    }
}

fn market_event_payload(event: &MarketEvent) -> Result<String, AppError> {
    serde_json::to_string(&serde_json::json!({
        "schema_version": 1,
        "market": event.market().as_str(),
        "event_time_ns": event.event_time().value(),
        "received_at_ns": event.received_at().value(),
        "kind": market_event_kind_code(event.kind()),
    }))
    .map_err(AppError::Json)
}

fn initial_engine_state(run_id: &str, opened_at_ns: i64) -> Result<EngineState, AppError> {
    let opened_at =
        TimestampNs::new(i128::from(opened_at_ns)).map_err(|_| AppError::InitialEngineState)?;
    let ledger = LedgerState::new(LedgerId::RulesOnly, opened_at)
        .map_err(|_| AppError::InitialEngineState)?;
    let broker = PaperBroker::new(
        BrokerConfig::new(
            Usdc::new(rust_decimal::Decimal::ONE).map_err(|_| AppError::InitialEngineState)?,
            DurationNs::new(1_000_000_000).map_err(|_| AppError::InitialEngineState)?,
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

fn engine_context(
    admission: trench_core::engine::EventAdmission,
) -> Result<EngineContext, trench_core::engine::EngineError> {
    let initial =
        TimestampNs::new(0).map_err(|_| trench_core::engine::EngineError::MissingVerifiedSource)?;
    let snapshot = UniverseSelector::select(initial, Vec::new())
        .map_err(|_| trench_core::engine::EngineError::MissingVerifiedSource)?;
    let activation = UniverseSelector::activate(&snapshot, None, initial)
        .map_err(|_| trench_core::engine::EngineError::MissingVerifiedSource)?;
    Ok(EngineContext::new(
        admission,
        SnapshotBindings::new(BTreeMap::new(), activation),
        StrategyFingerprints::new("rules-unavailable", "ml-unavailable"),
    ))
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

fn require_collection_only(config: &PaperConfig) -> Result<(), AppError> {
    if config.rules().mode() == RulesMode::Active {
        return Err(AppError::ActiveRulesUnavailable);
    }
    Ok(())
}

pub(crate) fn configured_path(config_path: &Path, value: &str) -> Result<PathBuf, AppError> {
    let config_path = absolute_path(config_path)?;
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
    /// Engine state was consumed by a failed authority transition.
    #[error("authority engine state is unavailable after a failed transition")]
    MissingEngineState,
    /// The minimal no-entry engine state could not be initialized safely.
    #[error("paper engine initial state could not be constructed")]
    InitialEngineState,
    /// An active rules artifact cannot run before its typed execution reactor.
    #[error(
        "active rules require the typed strategy/recovery execution reactor; this daemon is collection-only"
    )]
    ActiveRulesUnavailable,
    /// Canonical source evidence could not be serialized.
    #[error("normalized source evidence could not be serialized")]
    Json(#[source] serde_json::Error),
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
    use std::fs;
    use std::os::unix::fs::PermissionsExt;

    use rust_decimal::Decimal;

    use super::{
        AuthorityState, admit_source_event, configured_path, initial_engine_state,
        recover_source_stream, require_collection_only,
    };
    use crate::readiness::Readiness;
    use crate::writer::EngineWriter;
    use trench_core::domain::{Market, Price, Quantity, Side};
    use trench_core::event::{MarketEvent, TimestampNs, Trade};
    use trench_storage::parquet::{DataProvenance, ParquetStore};

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

    #[test]
    fn active_rules_are_refused_until_the_typed_execution_reactor_exists() {
        const DIGEST: &str = "b3:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        let active = include_str!("../../../config/paper.example.toml").replacen(
            "mode = \"collect_only\"",
            &format!(
                "mode = \"active\"\nartifact_file = \"rules.toml\"\nartifact_digest = \"{DIGEST}\"\nvalidation_report_file = \"validation.json\"\nvalidation_report_digest = \"{DIGEST}\""
            ),
            1,
        );
        let config = trench_core::config::PaperConfig::from_toml(&active).expect("active fixture");
        assert!(matches!(
            require_collection_only(&config),
            Err(super::AppError::ActiveRulesUnavailable)
        ));
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
            readiness: Readiness::default(),
            reconciled: false,
        };
        for event in recovered.events {
            admit_source_event(&mut writer, &mut authority, event)
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
}
