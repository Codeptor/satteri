//! Bounded, public-only Hyperliquid WebSocket market-data configuration.
//!
//! The runtime is added alongside the wire decoder. This module already keeps
//! its configuration constrained to the documented public connection budgets.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_util::{SinkExt, StreamExt};
use rand::random;
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::Value;
#[cfg(test)]
use std::sync::Arc;
use thiserror::Error;
#[cfg(test)]
use tokio::sync::Notify;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{Instant, timeout};
use tokio_tungstenite::tungstenite::{Message, protocol::WebSocketConfig};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async_with_config};
use tokio_util::sync::CancellationToken;
use trench_core::domain::{Market, Price, Quantity, Side};
use trench_core::event::{Bbo, BookLevel, BookSnapshot, MarketEvent, TimestampNs, Trade};

// Three subscriptions per market and a four-second reconnect floor bound a
// pathological reconnect storm to 1,584 subscription frames per minute.
const MAX_MARKETS: usize = 33;
const MAX_RECONNECT_ATTEMPTS: u32 = 20;
const MIN_RECONNECT_DELAY: Duration = Duration::from_secs(4);
const MAX_INBOUND_MESSAGE_BYTES: usize = 1_048_576;
const MAX_OUTPUT_CHANNEL_CAPACITY: usize = 4_096;
const OFFICIAL_WS_URL: &str = "wss://api.hyperliquid.xyz/ws";
const WEBSOCKET_WRITE_BUFFER_BYTES: usize = 8 * 1024;
const WEBSOCKET_MAX_WRITE_BUFFER_BYTES: usize = 16 * 1024;
const MAX_L2_LEVELS_PER_SIDE: usize = 20;
const DEFAULT_MAX_TRADE_IDENTITIES: usize = 100_000;
const MAX_TRADE_IDENTITIES: usize = 1_000_000;

/// Validated, finite limits for one public market-data WebSocket connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WsLimits {
    connect_timeout: Duration,
    read_timeout: Duration,
    snapshot_recovery_timeout: Duration,
    heartbeat_interval: Duration,
    reconnect_min_delay: Duration,
    reconnect_max_delay: Duration,
    max_reconnect_attempts: u32,
    max_inbound_message_bytes: usize,
    output_channel_capacity: usize,
    max_trade_identities: usize,
}

impl WsLimits {
    /// Builds fixed limits that remain below Hyperliquid's public connection,
    /// subscription, and message-rate budgets.
    ///
    /// The reconnect floor of four seconds permits at most sixteen new
    /// connections in a rolling minute, including a boundary connection. The
    /// caller must still use [`WsConfig`], which limits a connection to 99
    /// subscriptions (33 markets times three feeds). Together this bounds a
    /// reconnect storm to 1,584 subscription frames per minute, leaving room
    /// below the 2,000-message public budget for heartbeats.
    ///
    /// # Errors
    ///
    /// Returns [`WsError::InvalidConfig`] when a duration, byte bound, retry
    /// cap, or channel capacity cannot provide a bounded safe runtime.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        connect_timeout: Duration,
        read_timeout: Duration,
        snapshot_recovery_timeout: Duration,
        heartbeat_interval: Duration,
        reconnect_min_delay: Duration,
        reconnect_max_delay: Duration,
        max_reconnect_attempts: u32,
        max_inbound_message_bytes: usize,
        output_channel_capacity: usize,
        max_trade_identities: usize,
    ) -> Result<Self, WsError> {
        if connect_timeout.is_zero() {
            return Err(invalid_config("connect_timeout", "must be nonzero"));
        }
        if read_timeout.is_zero() {
            return Err(invalid_config("read_timeout", "must be nonzero"));
        }
        if snapshot_recovery_timeout.is_zero() {
            return Err(invalid_config(
                "snapshot_recovery_timeout",
                "must be nonzero",
            ));
        }
        if snapshot_recovery_timeout > read_timeout {
            return Err(invalid_config(
                "snapshot_recovery_timeout",
                "must not exceed read_timeout",
            ));
        }
        if heartbeat_interval.is_zero() {
            return Err(invalid_config("heartbeat_interval", "must be nonzero"));
        }
        if heartbeat_interval >= Duration::from_secs(60) {
            return Err(invalid_config(
                "heartbeat_interval",
                "must be shorter than 60 seconds",
            ));
        }
        if heartbeat_interval >= read_timeout {
            return Err(invalid_config(
                "heartbeat_interval",
                "must be shorter than read_timeout",
            ));
        }
        if reconnect_min_delay < MIN_RECONNECT_DELAY {
            return Err(invalid_config(
                "reconnect_min_delay",
                "must be at least 4 seconds",
            ));
        }
        if reconnect_max_delay < reconnect_min_delay {
            return Err(invalid_config(
                "reconnect_max_delay",
                "must be at least reconnect_min_delay",
            ));
        }
        if max_reconnect_attempts == 0 || max_reconnect_attempts > MAX_RECONNECT_ATTEMPTS {
            return Err(invalid_config(
                "max_reconnect_attempts",
                "must be between 1 and 20",
            ));
        }
        if !(1..=MAX_INBOUND_MESSAGE_BYTES).contains(&max_inbound_message_bytes) {
            return Err(invalid_config(
                "max_inbound_message_bytes",
                "must be between 1 and 1048576",
            ));
        }
        if !(1..=MAX_OUTPUT_CHANNEL_CAPACITY).contains(&output_channel_capacity) {
            return Err(invalid_config(
                "output_channel_capacity",
                "must be between 1 and 4096",
            ));
        }
        if !(1..=MAX_TRADE_IDENTITIES).contains(&max_trade_identities) {
            return Err(invalid_config(
                "max_trade_identities",
                "must be between 1 and 1000000",
            ));
        }

        Ok(Self {
            connect_timeout,
            read_timeout,
            snapshot_recovery_timeout,
            heartbeat_interval,
            reconnect_min_delay,
            reconnect_max_delay,
            max_reconnect_attempts,
            max_inbound_message_bytes,
            output_channel_capacity,
            max_trade_identities,
        })
    }

    /// Returns the bounded TLS connection deadline.
    #[must_use]
    pub const fn connect_timeout(self) -> Duration {
        self.connect_timeout
    }

    /// Returns the maximum interval without receiving a server frame.
    #[must_use]
    pub const fn read_timeout(self) -> Duration {
        self.read_timeout
    }

    /// Returns the finite deadline for every connection to receive all selected L2 snapshots.
    #[must_use]
    pub const fn snapshot_recovery_timeout(self) -> Duration {
        self.snapshot_recovery_timeout
    }

    /// Returns the interval between official JSON heartbeat messages.
    #[must_use]
    pub const fn heartbeat_interval(self) -> Duration {
        self.heartbeat_interval
    }

    /// Returns the lower bound for reconnect backoff.
    #[must_use]
    pub const fn reconnect_min_delay(self) -> Duration {
        self.reconnect_min_delay
    }

    /// Returns the upper bound for reconnect backoff.
    #[must_use]
    pub const fn reconnect_max_delay(self) -> Duration {
        self.reconnect_max_delay
    }

    /// Returns the finite reconnect-attempt cap for a run.
    #[must_use]
    pub const fn max_reconnect_attempts(self) -> u32 {
        self.max_reconnect_attempts
    }

    /// Returns the maximum accepted inbound WebSocket message length.
    #[must_use]
    pub const fn max_inbound_message_bytes(self) -> usize {
        self.max_inbound_message_bytes
    }

    /// Returns the capacity of the downstream Tokio channel.
    #[must_use]
    pub const fn output_channel_capacity(self) -> usize {
        self.output_channel_capacity
    }

    /// Returns the exact trade-identity capacity of this stream epoch.
    #[must_use]
    pub const fn max_trade_identities(self) -> usize {
        self.max_trade_identities
    }

    #[cfg(test)]
    fn fast_for_test() -> Self {
        Self {
            connect_timeout: Duration::from_millis(100),
            read_timeout: Duration::from_millis(200),
            snapshot_recovery_timeout: Duration::from_millis(100),
            heartbeat_interval: Duration::from_millis(20),
            reconnect_min_delay: Duration::from_millis(5),
            reconnect_max_delay: Duration::from_millis(20),
            max_reconnect_attempts: 2,
            max_inbound_message_bytes: 4 * 1024,
            output_channel_capacity: 8,
            max_trade_identities: 8_192,
        }
    }

    #[cfg(test)]
    fn fast_with_output_capacity_for_test(output_channel_capacity: usize) -> Self {
        Self {
            output_channel_capacity,
            ..Self::fast_for_test()
        }
    }

    #[cfg(test)]
    fn fast_with_trade_identity_limit_for_test(max_trade_identities: usize) -> Self {
        Self {
            max_trade_identities,
            ..Self::fast_for_test()
        }
    }
}

impl Default for WsLimits {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(5),
            read_timeout: Duration::from_secs(45),
            snapshot_recovery_timeout: Duration::from_secs(15),
            heartbeat_interval: Duration::from_secs(25),
            reconnect_min_delay: MIN_RECONNECT_DELAY,
            reconnect_max_delay: Duration::from_secs(30),
            max_reconnect_attempts: 20,
            max_inbound_message_bytes: 64 * 1024,
            output_channel_capacity: 128,
            max_trade_identities: DEFAULT_MAX_TRADE_IDENTITIES,
        }
    }
}

/// A fixed native-perpetual universe and bounded runtime limits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WsConfig {
    markets: Vec<Market>,
    limits: WsLimits,
}

impl WsConfig {
    /// Builds a configuration using production-safe default limits.
    ///
    /// # Errors
    ///
    /// Rejects an empty, duplicate, non-native, or over-budget market universe.
    pub fn new(markets: Vec<Market>) -> Result<Self, WsError> {
        Self::with_limits(markets, WsLimits::default())
    }

    /// Builds a configuration from a native-perpetual universe and explicit limits.
    ///
    /// # Errors
    ///
    /// Rejects an empty, duplicate, non-native, or over-budget market universe.
    pub fn with_limits(markets: Vec<Market>, limits: WsLimits) -> Result<Self, WsError> {
        if markets.is_empty() {
            return Err(WsError::EmptyUniverse);
        }
        if markets.len() > MAX_MARKETS {
            return Err(WsError::TooManyMarkets {
                max_markets: MAX_MARKETS,
            });
        }

        let mut unique = BTreeSet::new();
        for market in &markets {
            if !is_native_perpetual(market) {
                return Err(WsError::NonNativeMarket {
                    market: market.clone(),
                });
            }
            if !unique.insert(market.clone()) {
                return Err(WsError::DuplicateMarket {
                    market: market.clone(),
                });
            }
        }

        Ok(Self { markets, limits })
    }

    /// Returns the selected native-perpetual markets in subscription order.
    #[must_use]
    pub fn markets(&self) -> &[Market] {
        &self.markets
    }

    /// Returns the finite limits that govern this connection.
    #[must_use]
    pub const fn limits(&self) -> WsLimits {
        self.limits
    }
}

/// Public-only WebSocket client constrained to Hyperliquid mainnet `/ws`.
#[derive(Debug, Clone)]
pub struct WsClient {
    config: WsConfig,
    endpoint: WsEndpoint,
    #[cfg(test)]
    write_stall: TestWriteStall,
}

impl WsClient {
    /// Creates a client that can connect only to `wss://api.hyperliquid.xyz/ws`.
    #[must_use]
    pub const fn new(config: WsConfig) -> Self {
        Self {
            config,
            endpoint: WsEndpoint::production(),
            #[cfg(test)]
            write_stall: None,
        }
    }

    /// Returns the selected native-perpetual markets.
    #[must_use]
    pub fn markets(&self) -> &[Market] {
        self.config.markets()
    }

    /// Returns the bounded connection configuration.
    #[must_use]
    pub const fn config(&self) -> &WsConfig {
        &self.config
    }

    /// Starts one bounded task and returns its bounded normalized-output stream.
    ///
    /// The task connects only to the fixed public mainnet WebSocket endpoint.
    /// It never exposes a write/action API; its only outbound messages are the
    /// three fixed subscriptions for each selected market and `{"method":"ping"}`.
    #[must_use]
    pub fn start(&self) -> WsStream {
        let (sender, receiver) = mpsc::channel(self.config.limits.output_channel_capacity());
        let cancellation = CancellationToken::new();
        let task_cancellation = cancellation.clone();
        let config = self.config.clone();
        let endpoint = self.endpoint.clone();
        #[cfg(test)]
        let write_stall = self.write_stall.clone();
        #[cfg(not(test))]
        let write_stall = ();
        let task = tokio::spawn(async move {
            run_client(config, endpoint, sender, task_cancellation, write_stall).await;
        });
        WsStream {
            receiver,
            cancellation,
            task,
        }
    }

    #[cfg(test)]
    fn new_for_test(config: WsConfig, endpoint: String) -> Self {
        Self {
            config,
            endpoint: WsEndpoint::test_loopback(endpoint),
            write_stall: None,
        }
    }

    #[cfg(test)]
    fn new_for_stalled_writes_test(
        config: WsConfig,
        endpoint: String,
        write_started: Arc<Notify>,
    ) -> Self {
        Self {
            config,
            endpoint: WsEndpoint::test_loopback(endpoint),
            write_stall: Some(write_started),
        }
    }
}

/// A bounded stream of normalized public market facts and control records.
///
/// Dropping the stream or calling [`WsStream::cancel`] stops the single
/// associated connection task. The receiver is bounded by [`WsLimits`]. Trade
/// identities are retained exactly until [`WsLimits::max_trade_identities`] is
/// reached. The stream then emits [`WsTerminal::TradeIdentityLimit`] and
/// stops before a new identity could be emitted without durable retention. A
/// supervising durable journal must persist identities and start a fresh
/// stream epoch after that terminal record.
pub struct WsStream {
    receiver: mpsc::Receiver<WsOutput>,
    cancellation: CancellationToken,
    task: JoinHandle<()>,
}

impl WsStream {
    /// Receives the next normalized market fact or observability record.
    pub async fn recv(&mut self) -> Option<WsOutput> {
        self.receiver.recv().await
    }

    /// Signals the WebSocket task to close without sending any venue action.
    pub fn cancel(&self) {
        self.cancellation.cancel();
    }

    /// Cancels the task and waits for its bounded shutdown path to finish.
    pub async fn shutdown(mut self) {
        self.cancellation.cancel();
        let _ = (&mut self.task).await;
    }
}

impl Drop for WsStream {
    fn drop(&mut self) {
        self.cancellation.cancel();
    }
}

/// A record emitted by the public WebSocket boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WsOutput {
    /// A normalized immutable market fact suitable for deterministic core input.
    MarketEvent(MarketEvent),
    /// An append-only transport-gap record that gates execution readiness.
    Gap(GapEvent),
    /// A rejected wire update retained for observability without its raw body.
    Rejected(RejectedUpdate),
    /// A non-recoverable bounded-stream condition that ends this epoch.
    Terminal(WsTerminal),
}

/// A typed, non-recoverable reason that ends one bounded stream epoch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WsTerminal {
    /// Recording another exact trade identity would exceed the configured cap.
    TradeIdentityLimit(TradeIdentityLimit),
}

/// Evidence that exact trade-identity retention reached its configured bound.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TradeIdentityLimit {
    max_identities: usize,
}

impl TradeIdentityLimit {
    /// Returns the exact identity capacity reached by the terminated epoch.
    #[must_use]
    pub const fn max_identities(&self) -> usize {
        self.max_identities
    }
}

/// One append-only interruption record, kept separate from market facts.
///
/// A [`GapEvent::Opened`] never asserts trade continuity. Consumers must gate
/// paper execution for the affected market until the matching
/// [`GapEvent::Closed`] arrives after a newly received valid full L2 snapshot.
/// [`GapEvent::ReconnectExhausted`] leaves that gate closed and marks the
/// bounded stream as terminal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GapEvent {
    /// A connection failure made one selected market's continuity unknown.
    Opened(GapOpened),
    /// A new full L2 snapshot arrived after a recorded gap.
    Closed(GapClosed),
    /// The bounded reconnect budget ended while this gap remained open.
    ReconnectExhausted(GapExhausted),
}

/// Why a market-data continuity gap was opened.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GapReason {
    /// The remote peer cleanly closed its public connection.
    TransportClosed,
    /// The WebSocket connection or frame I/O failed.
    TransportError,
    /// The configured finite read deadline elapsed.
    ReadTimeout,
    /// Not every selected market supplied a fresh full L2 snapshot in time.
    SnapshotRecoveryTimeout,
}

/// Evidence retained when continuity first becomes unknown for one market.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GapOpened {
    generation: u64,
    market: Market,
    reason: GapReason,
    last_event_time: Option<TimestampNs>,
    last_received_at: Option<TimestampNs>,
}

impl GapOpened {
    /// Returns the monotonically increasing interruption generation.
    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    /// Returns the market whose continuity is unknown.
    #[must_use]
    pub const fn market(&self) -> &Market {
        &self.market
    }

    /// Returns the transport condition that opened the gap.
    #[must_use]
    pub const fn reason(&self) -> GapReason {
        self.reason
    }

    /// Returns the last authoritative exchange event time, if one was accepted.
    #[must_use]
    pub const fn last_event_time(&self) -> Option<TimestampNs> {
        self.last_event_time
    }

    /// Returns the local receipt of the last accepted market fact, when known.
    #[must_use]
    pub const fn last_received_at(&self) -> Option<TimestampNs> {
        self.last_received_at
    }
}

/// Evidence that a previously-opened gap has a fresh L2 recovery point.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GapClosed {
    generation: u64,
    market: Market,
    reason: GapReason,
    last_event_time: Option<TimestampNs>,
    last_received_at: Option<TimestampNs>,
    reconnect_received_at: TimestampNs,
    reconnect_attempt: u32,
}

impl GapClosed {
    /// Returns the interruption generation being closed.
    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    /// Returns the market whose fresh L2 snapshot recovered this gap.
    #[must_use]
    pub const fn market(&self) -> &Market {
        &self.market
    }

    /// Returns the reason that originally opened the gap.
    #[must_use]
    pub const fn reason(&self) -> GapReason {
        self.reason
    }

    /// Returns the last authoritative exchange event time before the interruption.
    #[must_use]
    pub const fn last_event_time(&self) -> Option<TimestampNs> {
        self.last_event_time
    }

    /// Returns the receipt timestamp of the final pre-gap market fact.
    #[must_use]
    pub const fn last_received_at(&self) -> Option<TimestampNs> {
        self.last_received_at
    }

    /// Returns the receipt timestamp of the fresh post-reconnect L2 snapshot.
    #[must_use]
    pub const fn reconnect_received_at(&self) -> TimestampNs {
        self.reconnect_received_at
    }

    /// Returns the bounded reconnect attempt that obtained the fresh snapshot.
    #[must_use]
    pub const fn reconnect_attempt(&self) -> u32 {
        self.reconnect_attempt
    }
}

/// Evidence that a gap could not recover within the finite reconnect budget.
///
/// This is terminal for the associated stream. It does not close the original
/// interruption or restore execution readiness.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GapExhausted {
    generation: u64,
    market: Market,
    reason: GapReason,
    last_event_time: Option<TimestampNs>,
    last_received_at: Option<TimestampNs>,
    reconnect_attempts: u32,
}

impl GapExhausted {
    /// Returns the still-open interruption generation.
    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    /// Returns the market that did not recover before the stream ended.
    #[must_use]
    pub const fn market(&self) -> &Market {
        &self.market
    }

    /// Returns the reason that originally opened the interruption.
    #[must_use]
    pub const fn reason(&self) -> GapReason {
        self.reason
    }

    /// Returns the last authoritative exchange event time before interruption.
    #[must_use]
    pub const fn last_event_time(&self) -> Option<TimestampNs> {
        self.last_event_time
    }

    /// Returns the receipt timestamp of the final pre-gap market fact.
    #[must_use]
    pub const fn last_received_at(&self) -> Option<TimestampNs> {
        self.last_received_at
    }

    /// Returns the successful reconnect connections observed without an L2 recovery.
    #[must_use]
    pub const fn reconnect_attempts(&self) -> u32 {
        self.reconnect_attempts
    }
}

/// A public wire update rejected before it reached the deterministic core.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RejectedUpdate {
    received_at: TimestampNs,
    reason: RejectionReason,
}

impl RejectedUpdate {
    /// Returns the explicit local receipt time for the rejected update.
    #[must_use]
    pub const fn received_at(&self) -> TimestampNs {
        self.received_at
    }

    /// Returns the stable category that explains why the update was rejected.
    #[must_use]
    pub const fn reason(&self) -> RejectionReason {
        self.reason
    }
}

/// Stable categories for rejected public WebSocket updates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RejectionReason {
    /// The frame was not valid JSON or missed a required typed field.
    Malformed,
    /// The channel or subscription acknowledgement is outside the fixed allowlist.
    UnsupportedChannel,
    /// The update targeted a valid native symbol outside the selected universe.
    ForeignMarket,
    /// The update targeted a non-native market identifier.
    NonNativeMarket,
    /// A price or quantity could not be parsed as an exact positive decimal.
    InvalidDecimal,
    /// A timestamp was nonpositive, overflowed nanoseconds, or exceeded receipt time.
    InvalidTimestamp,
    /// A trade aggressor side was not the official `B` or `A` value.
    InvalidSide,
    /// An L2 or BBO update omitted one side of visible liquidity.
    MissingLiquidity,
    /// A BBO was locked, crossed, or had nonpositive visible quantity.
    InvalidBbo,
    /// An L2 snapshot was empty, unsorted, duplicated, or crossed.
    InvalidBook,
    /// An L2 snapshot did not advance strictly beyond the last accepted snapshot.
    NonMonotonicBook,
    /// The server used a binary frame, which this JSON-only client declines.
    NonTextFrame,
    /// The server sent a frame larger than the configured hard byte limit.
    MessageTooLarge,
    /// Exact trade-identity retention reached its configured stream bound.
    TradeIdentityLimit,
}

#[derive(Debug, Clone)]
struct WsEndpoint(String);

impl WsEndpoint {
    const fn production() -> Self {
        Self(String::new())
    }

    fn as_str(&self) -> &str {
        if self.0.is_empty() {
            OFFICIAL_WS_URL
        } else {
            &self.0
        }
    }

    #[cfg(test)]
    fn test_loopback(endpoint: String) -> Self {
        Self(endpoint)
    }
}

type ClientSocket = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

#[cfg(test)]
type TestWriteStall = Option<Arc<Notify>>;

#[cfg(not(test))]
type TestWriteStall = ();

enum TimedIo<T> {
    Completed(T),
    TimedOut,
    Cancelled,
}

struct ConnectionIo<'a> {
    cancellation: &'a CancellationToken,
    test_write_stall: &'a TestWriteStall,
}

async fn cancellable_timeout<T>(
    duration: Duration,
    cancellation: &CancellationToken,
    operation: impl Future<Output = T>,
) -> TimedIo<T> {
    tokio::select! {
        _ = cancellation.cancelled() => TimedIo::Cancelled,
        result = timeout(duration, operation) => match result {
            Ok(value) => TimedIo::Completed(value),
            Err(_) => TimedIo::TimedOut,
        },
    }
}

async fn send_socket_message(
    sink: &mut futures_util::stream::SplitSink<ClientSocket, Message>,
    message: Message,
    duration: Duration,
    io: &ConnectionIo<'_>,
) -> TimedIo<Result<(), tokio_tungstenite::tungstenite::Error>> {
    #[cfg(test)]
    if let Some(write_started) = io.test_write_stall {
        write_started.notify_one();
        return cancellable_timeout(
            duration,
            io.cancellation,
            std::future::pending::<Result<(), tokio_tungstenite::tungstenite::Error>>(),
        )
        .await;
    }

    #[cfg(not(test))]
    let _ = io.test_write_stall;

    cancellable_timeout(duration, io.cancellation, sink.send(message)).await
}

async fn run_client(
    config: WsConfig,
    endpoint: WsEndpoint,
    output: mpsc::Sender<WsOutput>,
    cancellation: CancellationToken,
    test_write_stall: TestWriteStall,
) {
    let mut retry = 0_u32;
    let mut backoff = ReconnectBackoff::new(random());
    let mut state = StreamState::new(config.markets().to_vec());
    let mut decoder = Decoder::with_trade_identity_limit(
        config.markets().iter().cloned(),
        config.limits.max_trade_identities(),
    );
    loop {
        if cancellation.is_cancelled() {
            return;
        }
        let socket_config = WebSocketConfig::default()
            .read_buffer_size(config.limits.max_inbound_message_bytes())
            .write_buffer_size(WEBSOCKET_WRITE_BUFFER_BYTES)
            .max_write_buffer_size(WEBSOCKET_MAX_WRITE_BUFFER_BYTES)
            .max_message_size(Some(config.limits.max_inbound_message_bytes()))
            .max_frame_size(Some(config.limits.max_inbound_message_bytes()));
        let connected = cancellable_timeout(
            config.limits.connect_timeout(),
            &cancellation,
            connect_async_with_config(endpoint.as_str(), Some(socket_config), false),
        )
        .await;
        let socket = match connected {
            TimedIo::Completed(Ok((socket, _))) => socket,
            TimedIo::Completed(Err(error)) => {
                tracing::warn!(error = %error, "public WebSocket connection failed");
                if !emit_gaps(
                    &mut state,
                    GapReason::TransportError,
                    &output,
                    &cancellation,
                )
                .await
                    || !schedule_reconnect(
                        &config,
                        &cancellation,
                        &mut retry,
                        &mut backoff,
                        &state,
                        &output,
                    )
                    .await
                {
                    return;
                }
                continue;
            }
            TimedIo::TimedOut => {
                tracing::warn!("public WebSocket connection timed out");
                if !emit_gaps(
                    &mut state,
                    GapReason::TransportError,
                    &output,
                    &cancellation,
                )
                .await
                    || !schedule_reconnect(
                        &config,
                        &cancellation,
                        &mut retry,
                        &mut backoff,
                        &state,
                        &output,
                    )
                    .await
                {
                    return;
                }
                continue;
            }
            TimedIo::Cancelled => return,
        };
        state.record_reconnect_connection();
        let connection = run_connection(
            socket,
            &config,
            &output,
            &cancellation,
            &mut state,
            &mut decoder,
            &test_write_stall,
        )
        .await;
        if connection.healthy {
            retry = 0;
            backoff = ReconnectBackoff::new(random());
        }
        match connection.end {
            ConnectionEnd::Cancelled
            | ConnectionEnd::OutputClosed
            | ConnectionEnd::TradeIdentityLimit => return,
            end @ (ConnectionEnd::Closed
            | ConnectionEnd::ReadTimeout
            | ConnectionEnd::SnapshotRecoveryTimeout
            | ConnectionEnd::TransportError) => {
                let reason = match end {
                    ConnectionEnd::Closed => GapReason::TransportClosed,
                    ConnectionEnd::ReadTimeout => GapReason::ReadTimeout,
                    ConnectionEnd::SnapshotRecoveryTimeout => GapReason::SnapshotRecoveryTimeout,
                    ConnectionEnd::TransportError => GapReason::TransportError,
                    ConnectionEnd::Cancelled
                    | ConnectionEnd::OutputClosed
                    | ConnectionEnd::TradeIdentityLimit => return,
                };
                if !emit_gaps(&mut state, reason, &output, &cancellation).await
                    || !schedule_reconnect(
                        &config,
                        &cancellation,
                        &mut retry,
                        &mut backoff,
                        &state,
                        &output,
                    )
                    .await
                {
                    return;
                }
            }
        }
    }
}

async fn emit_gaps(
    state: &mut StreamState,
    reason: GapReason,
    output: &mpsc::Sender<WsOutput>,
    cancellation: &CancellationToken,
) -> bool {
    for gap in state.open_gaps(reason) {
        if !send_output(output, cancellation, WsOutput::Gap(gap)).await {
            return false;
        }
    }
    true
}

async fn emit_reconnect_exhausted_gaps(
    state: &StreamState,
    output: &mpsc::Sender<WsOutput>,
    cancellation: &CancellationToken,
) -> bool {
    for gap in state.reconnect_exhausted() {
        if !send_output(output, cancellation, WsOutput::Gap(gap)).await {
            return false;
        }
    }
    true
}

async fn schedule_reconnect(
    config: &WsConfig,
    cancellation: &CancellationToken,
    retry: &mut u32,
    backoff: &mut ReconnectBackoff,
    state: &StreamState,
    output: &mpsc::Sender<WsOutput>,
) -> bool {
    match sleep_for_reconnect(config, cancellation, retry, backoff).await {
        ReconnectOutcome::Ready => true,
        ReconnectOutcome::Cancelled => false,
        ReconnectOutcome::Exhausted => {
            let _ = emit_reconnect_exhausted_gaps(state, output, cancellation).await;
            false
        }
    }
}

async fn sleep_for_reconnect(
    config: &WsConfig,
    cancellation: &CancellationToken,
    retry: &mut u32,
    backoff: &mut ReconnectBackoff,
) -> ReconnectOutcome {
    *retry = retry.saturating_add(1);
    if *retry > config.limits.max_reconnect_attempts() {
        tracing::error!(
            attempts = *retry,
            "public WebSocket reconnect budget exhausted"
        );
        return ReconnectOutcome::Exhausted;
    }
    let delay = backoff.delay(*retry, config.limits);
    tracing::warn!(
        attempt = *retry,
        delay_ms = delay.as_millis(),
        "reconnecting public WebSocket"
    );
    tokio::select! {
        _ = cancellation.cancelled() => ReconnectOutcome::Cancelled,
        _ = tokio::time::sleep(delay) => ReconnectOutcome::Ready,
    }
}

#[derive(Debug, Clone, Copy)]
enum ReconnectOutcome {
    Ready,
    Cancelled,
    Exhausted,
}

async fn run_connection(
    socket: ClientSocket,
    config: &WsConfig,
    output: &mpsc::Sender<WsOutput>,
    cancellation: &CancellationToken,
    state: &mut StreamState,
    decoder: &mut Decoder,
    test_write_stall: &TestWriteStall,
) -> ConnectionOutcome {
    let (mut sink, mut source) = socket.split();
    let io = ConnectionIo {
        cancellation,
        test_write_stall,
    };
    for market in config.markets() {
        for kind in ["l2Book", "trades", "bbo"] {
            let subscription = serde_json::json!({
                "method": "subscribe",
                "subscription": {"type": kind, "coin": market.as_str()}
            })
            .to_string();
            match send_socket_message(
                &mut sink,
                Message::Text(subscription.into()),
                config.limits.connect_timeout(),
                &io,
            )
            .await
            {
                TimedIo::Completed(Ok(())) => {}
                TimedIo::Completed(Err(error)) => {
                    tracing::warn!(error = %error, "public WebSocket subscription write failed");
                    return ConnectionOutcome::unhealthy(ConnectionEnd::TransportError);
                }
                TimedIo::TimedOut => {
                    tracing::warn!("public WebSocket subscription write timed out");
                    return ConnectionOutcome::unhealthy(ConnectionEnd::TransportError);
                }
                TimedIo::Cancelled => {
                    return ConnectionOutcome::unhealthy(ConnectionEnd::Cancelled);
                }
            }
        }
    }

    decoder.begin_connection();
    let mut healthy = false;
    let mut recovered_markets = BTreeSet::new();
    let mut heartbeat_due = Box::pin(tokio::time::sleep(config.limits.heartbeat_interval()));
    let mut read_deadline = Box::pin(tokio::time::sleep(config.limits.read_timeout()));
    let mut snapshot_recovery_deadline = Box::pin(tokio::time::sleep(
        config.limits.snapshot_recovery_timeout(),
    ));
    loop {
        tokio::select! {
            _ = cancellation.cancelled() => {
                return ConnectionOutcome { end: ConnectionEnd::Cancelled, healthy };
            }
            _ = &mut heartbeat_due => {
                match send_socket_message(
                    &mut sink,
                    Message::Text("{\"method\":\"ping\"}".into()),
                    config.limits.connect_timeout(),
                    &io,
                ).await {
                    TimedIo::Completed(Ok(())) => {
                        heartbeat_due.as_mut().reset(Instant::now() + config.limits.heartbeat_interval());
                    }
                    TimedIo::Completed(Err(error)) => {
                        tracing::warn!(error = %error, "public WebSocket heartbeat write failed");
                        return ConnectionOutcome { end: ConnectionEnd::TransportError, healthy };
                    }
                    TimedIo::TimedOut => {
                        tracing::warn!("public WebSocket heartbeat write timed out");
                        return ConnectionOutcome { end: ConnectionEnd::TransportError, healthy };
                    }
                    TimedIo::Cancelled => {
                        return ConnectionOutcome { end: ConnectionEnd::Cancelled, healthy };
                    }
                }
            }
            _ = &mut read_deadline => {
                tracing::warn!("public WebSocket read deadline elapsed");
                return ConnectionOutcome { end: ConnectionEnd::ReadTimeout, healthy };
            }
            _ = &mut snapshot_recovery_deadline, if !healthy => {
                tracing::warn!("public WebSocket snapshot recovery deadline elapsed");
                return ConnectionOutcome { end: ConnectionEnd::SnapshotRecoveryTimeout, healthy };
            }
            frame = source.next() => {
                read_deadline.as_mut().reset(Instant::now() + config.limits.read_timeout());
                let Some(frame) = frame else {
                    return ConnectionOutcome { end: ConnectionEnd::Closed, healthy };
                };
                let frame = match frame {
                    Ok(frame) => frame,
                    Err(error) => {
                        tracing::warn!(error = %error, "public WebSocket transport failed");
                        return ConnectionOutcome { end: ConnectionEnd::TransportError, healthy };
                    }
                };
                match handle_frame(
                    frame,
                    decoder,
                    &mut sink,
                    config,
                    output,
                    state,
                    &io,
                ).await {
                    FrameOutcome::Continue => {}
                    FrameOutcome::FreshL2(market) => {
                        recovered_markets.insert(market);
                        healthy = recovered_markets.len() == config.markets().len();
                    }
                    FrameOutcome::Closed => {
                        return ConnectionOutcome { end: ConnectionEnd::Closed, healthy };
                    }
                    FrameOutcome::TransportError => {
                        return ConnectionOutcome { end: ConnectionEnd::TransportError, healthy };
                    }
                    FrameOutcome::OutputClosed => {
                        return ConnectionOutcome { end: ConnectionEnd::OutputClosed, healthy };
                    }
                    FrameOutcome::Terminal => {
                        return ConnectionOutcome {
                            end: ConnectionEnd::TradeIdentityLimit,
                            healthy,
                        };
                    }
                    FrameOutcome::Cancelled => {
                        return ConnectionOutcome { end: ConnectionEnd::Cancelled, healthy };
                    }
                }
            }
        }
    }
}

async fn handle_frame(
    frame: Message,
    decoder: &mut Decoder,
    sink: &mut futures_util::stream::SplitSink<ClientSocket, Message>,
    config: &WsConfig,
    output: &mpsc::Sender<WsOutput>,
    state: &mut StreamState,
    io: &ConnectionIo<'_>,
) -> FrameOutcome {
    let received_at = match now_timestamp() {
        Some(timestamp) => timestamp,
        None => return FrameOutcome::OutputClosed,
    };
    match frame {
        Message::Text(frame) => {
            if frame.len() > config.limits.max_inbound_message_bytes() {
                return if emit_rejection(
                    output,
                    io.cancellation,
                    received_at,
                    RejectionReason::MessageTooLarge,
                )
                .await
                {
                    FrameOutcome::Continue
                } else {
                    FrameOutcome::OutputClosed
                };
            }
            match decoder.decode(&frame, received_at) {
                Ok(DecodedFrame::MarketEvents(events)) => {
                    let recovered_market = events.iter().find_map(|event| {
                        matches!(
                            event.kind(),
                            trench_core::event::MarketEventKind::BookSnapshot(_)
                        )
                        .then(|| event.market().clone())
                    });
                    for event in events {
                        let gap = state.record_event(&event);
                        if !send_output(output, io.cancellation, WsOutput::MarketEvent(event)).await
                        {
                            return FrameOutcome::OutputClosed;
                        }
                        if let Some(gap) = gap
                            && !send_output(output, io.cancellation, WsOutput::Gap(gap)).await
                        {
                            return FrameOutcome::OutputClosed;
                        }
                    }
                    if let Some(market) = recovered_market {
                        FrameOutcome::FreshL2(market)
                    } else {
                        FrameOutcome::Continue
                    }
                }
                Ok(DecodedFrame::SubscriptionAck(ack)) => {
                    tracing::debug!(market = ack.market().as_str(), kind = ?ack.kind(), "public WebSocket subscription acknowledged");
                    FrameOutcome::Continue
                }
                Ok(DecodedFrame::Pong) => FrameOutcome::Continue,
                Err(DecodeError::TradeIdentityLimit { max_identities }) => {
                    if send_output(
                        output,
                        io.cancellation,
                        WsOutput::Terminal(WsTerminal::TradeIdentityLimit(TradeIdentityLimit {
                            max_identities,
                        })),
                    )
                    .await
                    {
                        FrameOutcome::Terminal
                    } else {
                        FrameOutcome::OutputClosed
                    }
                }
                Err(error) => {
                    tracing::warn!(reason = ?error, "public WebSocket update rejected");
                    if emit_rejection(output, io.cancellation, received_at, error.into()).await {
                        FrameOutcome::Continue
                    } else {
                        FrameOutcome::OutputClosed
                    }
                }
            }
        }
        Message::Ping(payload) => {
            match send_socket_message(
                sink,
                Message::Pong(payload),
                config.limits.connect_timeout(),
                io,
            )
            .await
            {
                TimedIo::Completed(Ok(())) => FrameOutcome::Continue,
                TimedIo::Completed(Err(error)) => {
                    tracing::warn!(error = %error, "public WebSocket pong write failed");
                    FrameOutcome::TransportError
                }
                TimedIo::TimedOut => {
                    tracing::warn!("public WebSocket pong write timed out");
                    FrameOutcome::TransportError
                }
                TimedIo::Cancelled => FrameOutcome::Cancelled,
            }
        }
        Message::Pong(_) => FrameOutcome::Continue,
        Message::Close(_) => FrameOutcome::Closed,
        Message::Binary(_) | Message::Frame(_) => {
            if emit_rejection(
                output,
                io.cancellation,
                received_at,
                RejectionReason::NonTextFrame,
            )
            .await
            {
                FrameOutcome::Continue
            } else {
                FrameOutcome::OutputClosed
            }
        }
    }
}

async fn emit_rejection(
    output: &mpsc::Sender<WsOutput>,
    cancellation: &CancellationToken,
    received_at: TimestampNs,
    reason: RejectionReason,
) -> bool {
    send_output(
        output,
        cancellation,
        WsOutput::Rejected(RejectedUpdate {
            received_at,
            reason,
        }),
    )
    .await
}

async fn send_output(
    output: &mpsc::Sender<WsOutput>,
    cancellation: &CancellationToken,
    item: WsOutput,
) -> bool {
    if cancellation.is_cancelled() {
        return false;
    }
    tokio::select! {
        _ = cancellation.cancelled() => false,
        result = output.send(item) => result.is_ok(),
    }
}

fn now_timestamp() -> Option<TimestampNs> {
    let elapsed = SystemTime::now().duration_since(UNIX_EPOCH).ok()?;
    let nanoseconds = i128::try_from(elapsed.as_nanos()).ok()?;
    TimestampNs::new(nanoseconds).ok()
}

#[derive(Debug, Clone, Copy)]
enum ConnectionEnd {
    Cancelled,
    Closed,
    ReadTimeout,
    SnapshotRecoveryTimeout,
    TransportError,
    TradeIdentityLimit,
    OutputClosed,
}

struct ConnectionOutcome {
    end: ConnectionEnd,
    healthy: bool,
}

impl ConnectionOutcome {
    const fn unhealthy(end: ConnectionEnd) -> Self {
        Self {
            end,
            healthy: false,
        }
    }
}

#[derive(Debug)]
enum FrameOutcome {
    Continue,
    FreshL2(Market),
    Closed,
    TransportError,
    Terminal,
    OutputClosed,
    Cancelled,
}

struct StreamState {
    markets: Vec<Market>,
    generation: u64,
    pending_gaps: BTreeMap<Market, PendingGap>,
    last_event_times: BTreeMap<Market, TimestampNs>,
    last_received_at: BTreeMap<Market, TimestampNs>,
}

struct PendingGap {
    opened: GapOpened,
    reconnect_attempt: u32,
}

impl StreamState {
    fn new(markets: Vec<Market>) -> Self {
        Self {
            markets,
            generation: 0,
            pending_gaps: BTreeMap::new(),
            last_event_times: BTreeMap::new(),
            last_received_at: BTreeMap::new(),
        }
    }

    fn open_gaps(&mut self, reason: GapReason) -> Vec<GapEvent> {
        let affected = self
            .markets
            .iter()
            .filter(|market| !self.pending_gaps.contains_key(*market))
            .cloned()
            .collect::<Vec<_>>();
        if affected.is_empty() {
            return Vec::new();
        }
        self.generation = self.generation.saturating_add(1);
        affected
            .into_iter()
            .map(|market| {
                let opened = GapOpened {
                    generation: self.generation,
                    last_event_time: self.last_event_times.get(&market).copied(),
                    last_received_at: self.last_received_at.get(&market).copied(),
                    market: market.clone(),
                    reason,
                };
                self.pending_gaps.insert(
                    market,
                    PendingGap {
                        opened: opened.clone(),
                        reconnect_attempt: 0,
                    },
                );
                GapEvent::Opened(opened)
            })
            .collect()
    }

    fn record_reconnect_connection(&mut self) {
        for pending in self.pending_gaps.values_mut() {
            pending.reconnect_attempt = pending.reconnect_attempt.saturating_add(1);
        }
    }

    fn reconnect_exhausted(&self) -> Vec<GapEvent> {
        self.pending_gaps
            .iter()
            .map(|(market, pending)| {
                let opened = &pending.opened;
                GapEvent::ReconnectExhausted(GapExhausted {
                    generation: opened.generation,
                    market: market.clone(),
                    reason: opened.reason,
                    last_event_time: opened.last_event_time,
                    last_received_at: opened.last_received_at,
                    reconnect_attempts: pending.reconnect_attempt,
                })
            })
            .collect()
    }

    fn record_event(&mut self, event: &MarketEvent) -> Option<GapEvent> {
        let market = event.market().clone();
        self.last_event_times
            .insert(market.clone(), event.event_time());
        self.last_received_at
            .insert(market.clone(), event.received_at());
        if !matches!(
            event.kind(),
            trench_core::event::MarketEventKind::BookSnapshot(_)
        ) {
            return None;
        }
        self.pending_gaps.remove(&market).map(|pending| {
            let opened = pending.opened;
            GapEvent::Closed(GapClosed {
                generation: opened.generation,
                market,
                reason: opened.reason,
                last_event_time: opened.last_event_time,
                last_received_at: opened.last_received_at,
                reconnect_received_at: event.received_at(),
                reconnect_attempt: pending.reconnect_attempt,
            })
        })
    }
}

struct ReconnectBackoff {
    state: u64,
}

impl ReconnectBackoff {
    const fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn delay(&mut self, attempt: u32, limits: WsLimits) -> Duration {
        let exponent = attempt.saturating_sub(1).min(20);
        let multiplier = 1_u128 << exponent;
        let minimum = limits.reconnect_min_delay().as_nanos();
        let maximum = limits.reconnect_max_delay().as_nanos();
        let capped = minimum.saturating_mul(multiplier).min(maximum);
        let span = capped.saturating_sub(minimum);
        let offset = if span == 0 {
            0
        } else {
            u128::from(self.next_u64()) % (span + 1)
        };
        let nanoseconds = minimum.saturating_add(offset);
        let nanoseconds = u64::try_from(nanoseconds).map_or(u64::MAX, |value| value);
        Duration::from_nanos(nanoseconds)
    }

    fn next_u64(&mut self) -> u64 {
        self.state ^= self.state << 13;
        self.state ^= self.state >> 7;
        self.state ^= self.state << 17;
        self.state
    }
}

impl From<DecodeError> for RejectionReason {
    fn from(value: DecodeError) -> Self {
        match value {
            DecodeError::Malformed => Self::Malformed,
            DecodeError::UnsupportedChannel => Self::UnsupportedChannel,
            DecodeError::ForeignMarket => Self::ForeignMarket,
            DecodeError::NonNativeMarket => Self::NonNativeMarket,
            DecodeError::InvalidDecimal => Self::InvalidDecimal,
            DecodeError::InvalidTimestamp => Self::InvalidTimestamp,
            DecodeError::InvalidSide => Self::InvalidSide,
            DecodeError::MissingLiquidity => Self::MissingLiquidity,
            DecodeError::InvalidBbo => Self::InvalidBbo,
            DecodeError::InvalidBook => Self::InvalidBook,
            DecodeError::NonMonotonicBook => Self::NonMonotonicBook,
            DecodeError::TradeIdentityLimit { .. } => Self::TradeIdentityLimit,
        }
    }
}

/// Public WebSocket configuration and invariant failures.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum WsError {
    /// No market was selected for the public feeds.
    #[error("the WebSocket market universe must not be empty")]
    EmptyUniverse,
    /// A market appeared more than once in the selected universe.
    #[error("duplicate WebSocket market `{market:?}`")]
    DuplicateMarket {
        /// The duplicate market.
        market: Market,
    },
    /// A selected market was not an unqualified native perpetual symbol.
    #[error("WebSocket market `{market:?}` must be a native perpetual symbol")]
    NonNativeMarket {
        /// The rejected market.
        market: Market,
    },
    /// The universe would exceed the safe subscription budget.
    #[error("WebSocket market universe exceeds the {max_markets}-market limit")]
    TooManyMarkets {
        /// Maximum selected markets, yielding 99 subscriptions.
        max_markets: usize,
    },
    /// A finite runtime setting violated a required bound.
    #[error("invalid WebSocket configuration `{field}`: {requirement}")]
    InvalidConfig {
        /// The invalid field.
        field: &'static str,
        /// The required invariant.
        requirement: &'static str,
    },
}

fn invalid_config(field: &'static str, requirement: &'static str) -> WsError {
    WsError::InvalidConfig { field, requirement }
}

fn is_native_perpetual(market: &Market) -> bool {
    market
        .as_str()
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric())
}

#[derive(Debug, PartialEq, Eq)]
enum DecodedFrame {
    MarketEvents(Vec<MarketEvent>),
    SubscriptionAck(SubscriptionAck),
    Pong,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SubscriptionKind {
    L2Book,
    Trades,
    Bbo,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SubscriptionAck {
    kind: SubscriptionKind,
    market: Market,
}

impl SubscriptionAck {
    fn kind(&self) -> SubscriptionKind {
        self.kind
    }

    fn market(&self) -> &Market {
        &self.market
    }
}

struct Decoder {
    markets: BTreeSet<Market>,
    /// Exact identities retained for this bounded stream epoch so reconnect
    /// replays cannot become duplicate market facts.
    trades: BTreeSet<TradeIdentity>,
    max_trade_identities: usize,
    last_book_times: BTreeMap<Market, TimestampNs>,
}

impl Decoder {
    #[cfg(test)]
    fn new(markets: impl IntoIterator<Item = Market>) -> Self {
        Self::with_trade_identity_limit(markets, DEFAULT_MAX_TRADE_IDENTITIES)
    }

    fn with_trade_identity_limit(
        markets: impl IntoIterator<Item = Market>,
        max_trade_identities: usize,
    ) -> Self {
        Self {
            markets: markets.into_iter().collect(),
            trades: BTreeSet::new(),
            max_trade_identities,
            last_book_times: BTreeMap::new(),
        }
    }

    fn begin_connection(&mut self) {
        self.last_book_times.clear();
    }

    fn decode(
        &mut self,
        frame: &str,
        received_at: TimestampNs,
    ) -> Result<DecodedFrame, DecodeError> {
        let envelope: RawEnvelope =
            serde_json::from_str(frame).map_err(|_| DecodeError::Malformed)?;
        match envelope.channel.as_str() {
            "l2Book" => self.decode_book(envelope.data.ok_or(DecodeError::Malformed)?, received_at),
            "trades" => {
                self.decode_trades(envelope.data.ok_or(DecodeError::Malformed)?, received_at)
            }
            "bbo" => self.decode_bbo(envelope.data.ok_or(DecodeError::Malformed)?, received_at),
            "subscriptionResponse" => {
                self.decode_subscription_ack(envelope.data.ok_or(DecodeError::Malformed)?)
            }
            "pong" => Ok(DecodedFrame::Pong),
            _ => Err(DecodeError::UnsupportedChannel),
        }
    }

    fn decode_book(
        &mut self,
        data: Value,
        received_at: TimestampNs,
    ) -> Result<DecodedFrame, DecodeError> {
        let event = normalize_l2_book_wire(data, &self.markets, received_at)?;
        let market = event.market().clone();
        let event_time = event.event_time();
        if self
            .last_book_times
            .get(&market)
            .is_some_and(|previous| event_time <= *previous)
        {
            return Err(DecodeError::NonMonotonicBook);
        }
        self.last_book_times.insert(market.clone(), event_time);
        Ok(DecodedFrame::MarketEvents(vec![event]))
    }

    fn decode_trades(
        &mut self,
        data: Value,
        received_at: TimestampNs,
    ) -> Result<DecodedFrame, DecodeError> {
        let trades: Vec<RawWsTrade> =
            serde_json::from_value(data).map_err(|_| DecodeError::Malformed)?;
        let mut staged_identities = BTreeSet::new();
        let mut staged = Vec::with_capacity(trades.len());
        for raw in trades {
            let market = parse_selected_market(&raw.coin, &self.markets)?;
            let event_time = timestamp_from_millis(raw.time)?;
            let identity = TradeIdentity {
                block_time: event_time,
                market: market.clone(),
                trade_id: raw.tid,
            };
            let side = match raw.side.as_str() {
                "B" => Side::Buy,
                "A" => Side::Sell,
                _ => return Err(DecodeError::InvalidSide),
            };
            let price = decode_price(&raw.px)?;
            let quantity = decode_quantity(&raw.sz)?;
            let trade = Trade::new(raw.tid, side, price, quantity)
                .map_err(|_| DecodeError::InvalidDecimal)?;
            let event = MarketEvent::trade(event_time, received_at, market, trade)
                .map_err(|_| DecodeError::InvalidTimestamp)?;
            if self.trades.contains(&identity) || !staged_identities.insert(identity.clone()) {
                continue;
            }
            staged.push((identity, event));
        }
        if self.trades.len().saturating_add(staged.len()) > self.max_trade_identities {
            return Err(DecodeError::TradeIdentityLimit {
                max_identities: self.max_trade_identities,
            });
        }
        self.trades
            .extend(staged.iter().map(|(identity, _)| identity.clone()));
        let events = staged.into_iter().map(|(_, event)| event).collect();
        Ok(DecodedFrame::MarketEvents(events))
    }

    fn decode_bbo(
        &self,
        data: Value,
        received_at: TimestampNs,
    ) -> Result<DecodedFrame, DecodeError> {
        let event = normalize_bbo_wire(data, &self.markets, received_at)?;
        Ok(DecodedFrame::MarketEvents(vec![event]))
    }

    fn decode_subscription_ack(&self, data: Value) -> Result<DecodedFrame, DecodeError> {
        let response: RawSubscriptionResponse =
            serde_json::from_value(data).map_err(|_| DecodeError::Malformed)?;
        if response.method != "subscribe" {
            return Err(DecodeError::Malformed);
        }
        let market = parse_selected_market(&response.subscription.coin, &self.markets)?;
        let kind = match response.subscription.kind.as_str() {
            "l2Book" => SubscriptionKind::L2Book,
            "trades" => SubscriptionKind::Trades,
            "bbo" => SubscriptionKind::Bbo,
            _ => return Err(DecodeError::UnsupportedChannel),
        };
        Ok(DecodedFrame::SubscriptionAck(SubscriptionAck {
            kind,
            market,
        }))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DecodeError {
    Malformed,
    UnsupportedChannel,
    ForeignMarket,
    NonNativeMarket,
    InvalidDecimal,
    InvalidTimestamp,
    InvalidSide,
    MissingLiquidity,
    InvalidBbo,
    InvalidBook,
    NonMonotonicBook,
    TradeIdentityLimit { max_identities: usize },
}

#[derive(Deserialize)]
struct RawEnvelope {
    channel: String,
    data: Option<Value>,
}

#[derive(Deserialize)]
struct RawWsBook {
    coin: String,
    time: i64,
    levels: [Vec<RawWsLevel>; 2],
}

#[derive(Deserialize)]
struct RawWsLevel {
    px: String,
    sz: String,
    #[serde(rename = "n")]
    order_count: u32,
}

#[derive(Deserialize)]
struct RawWsTrade {
    coin: String,
    side: String,
    px: String,
    sz: String,
    time: i64,
    tid: u64,
}

#[derive(Deserialize)]
struct RawWsBbo {
    coin: String,
    time: i64,
    bbo: [Option<RawWsLevel>; 2],
}

#[derive(Deserialize)]
struct RawSubscriptionResponse {
    method: String,
    subscription: RawSubscription,
}

#[derive(Deserialize)]
struct RawSubscription {
    #[serde(rename = "type")]
    kind: String,
    coin: String,
}

pub(crate) fn normalize_l2_book_wire(
    data: Value,
    markets: &BTreeSet<Market>,
    received_at: TimestampNs,
) -> Result<MarketEvent, DecodeError> {
    let book: RawWsBook = serde_json::from_value(data).map_err(|_| DecodeError::Malformed)?;
    let market = parse_selected_market(&book.coin, markets)?;
    normalize_l2_book(book, market, received_at)
}

pub(crate) fn normalize_l2_book_wire_for_market(
    data: Value,
    expected_market: &Market,
    received_at: TimestampNs,
) -> Result<MarketEvent, DecodeError> {
    let book: RawWsBook = serde_json::from_value(data).map_err(|_| DecodeError::Malformed)?;
    let market = parse_expected_market(&book.coin, expected_market)?;
    normalize_l2_book(book, market, received_at)
}

fn normalize_l2_book(
    book: RawWsBook,
    market: Market,
    received_at: TimestampNs,
) -> Result<MarketEvent, DecodeError> {
    let event_time = timestamp_from_millis(book.time)?;
    let sequence = u64::try_from(book.time).map_err(|_| DecodeError::InvalidTimestamp)?;
    let [bids, asks] = book.levels;
    if bids.len() > MAX_L2_LEVELS_PER_SIDE || asks.len() > MAX_L2_LEVELS_PER_SIDE {
        return Err(DecodeError::InvalidBook);
    }
    let bids = decode_book_levels(bids)?;
    let asks = decode_book_levels(asks)?;
    validate_l2_book(&bids, &asks)?;
    MarketEvent::book_snapshot(
        event_time,
        received_at,
        market,
        BookSnapshot::new(sequence, bids, asks),
    )
    .map_err(|_| DecodeError::InvalidTimestamp)
}

pub(crate) fn normalize_bbo_wire(
    data: Value,
    markets: &BTreeSet<Market>,
    received_at: TimestampNs,
) -> Result<MarketEvent, DecodeError> {
    let bbo: RawWsBbo = serde_json::from_value(data).map_err(|_| DecodeError::Malformed)?;
    let market = parse_selected_market(&bbo.coin, markets)?;
    normalize_bbo(bbo, market, received_at)
}

pub(crate) fn normalize_bbo_wire_for_market(
    data: Value,
    expected_market: &Market,
    received_at: TimestampNs,
) -> Result<MarketEvent, DecodeError> {
    let bbo: RawWsBbo = serde_json::from_value(data).map_err(|_| DecodeError::Malformed)?;
    let market = parse_expected_market(&bbo.coin, expected_market)?;
    normalize_bbo(bbo, market, received_at)
}

fn normalize_bbo(
    bbo: RawWsBbo,
    market: Market,
    received_at: TimestampNs,
) -> Result<MarketEvent, DecodeError> {
    let event_time = timestamp_from_millis(bbo.time)?;
    let sequence = u64::try_from(bbo.time).map_err(|_| DecodeError::InvalidTimestamp)?;
    let [raw_bid, raw_ask] = bbo.bbo;
    let bid = decode_book_level(raw_bid.ok_or(DecodeError::MissingLiquidity)?)?;
    let ask = decode_book_level(raw_ask.ok_or(DecodeError::MissingLiquidity)?)?;
    let payload = Bbo::new(sequence, bid, ask).map_err(|_| DecodeError::InvalidBbo)?;
    MarketEvent::bbo(event_time, received_at, market, payload)
        .map_err(|_| DecodeError::InvalidTimestamp)
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct TradeIdentity {
    block_time: TimestampNs,
    market: Market,
    trade_id: u64,
}

fn parse_selected_market(value: &str, markets: &BTreeSet<Market>) -> Result<Market, DecodeError> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_alphanumeric()) {
        return Err(DecodeError::NonNativeMarket);
    }
    let market = Market::new(value).map_err(|_| DecodeError::ForeignMarket)?;
    if !markets.contains(&market) {
        return Err(DecodeError::ForeignMarket);
    }
    Ok(market)
}

fn parse_expected_market(value: &str, expected_market: &Market) -> Result<Market, DecodeError> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_alphanumeric()) {
        return Err(DecodeError::NonNativeMarket);
    }
    let market = Market::new(value).map_err(|_| DecodeError::ForeignMarket)?;
    if &market != expected_market {
        return Err(DecodeError::ForeignMarket);
    }
    Ok(market)
}

fn timestamp_from_millis(value: i64) -> Result<TimestampNs, DecodeError> {
    let nanoseconds = i128::from(value)
        .checked_mul(1_000_000)
        .ok_or(DecodeError::InvalidTimestamp)?;
    TimestampNs::new(nanoseconds).map_err(|_| DecodeError::InvalidTimestamp)
}

fn decode_book_levels(levels: Vec<RawWsLevel>) -> Result<Vec<BookLevel>, DecodeError> {
    levels.into_iter().map(decode_book_level).collect()
}

fn decode_book_level(level: RawWsLevel) -> Result<BookLevel, DecodeError> {
    if level.order_count == 0 {
        return Err(DecodeError::InvalidBook);
    }
    let price = decode_price(&level.px)?;
    let quantity = decode_quantity(&level.sz)?;
    Ok(BookLevel::new(price, quantity))
}

fn validate_l2_book(bids: &[BookLevel], asks: &[BookLevel]) -> Result<(), DecodeError> {
    if bids.is_empty() || asks.is_empty() {
        return Err(DecodeError::MissingLiquidity);
    }
    validate_l2_side(bids, true)?;
    validate_l2_side(asks, false)?;
    if bids[0].price() >= asks[0].price() {
        return Err(DecodeError::InvalidBook);
    }
    Ok(())
}

fn validate_l2_side(levels: &[BookLevel], descending: bool) -> Result<(), DecodeError> {
    if levels
        .iter()
        .any(|level| level.quantity().value().is_zero())
    {
        return Err(DecodeError::InvalidBook);
    }
    if levels.windows(2).any(|pair| {
        if descending {
            pair[0].price() <= pair[1].price()
        } else {
            pair[0].price() >= pair[1].price()
        }
    }) {
        return Err(DecodeError::InvalidBook);
    }
    Ok(())
}

fn decode_price(value: &str) -> Result<Price, DecodeError> {
    Price::new(parse_plain_decimal(value)?).map_err(|_| DecodeError::InvalidDecimal)
}

fn decode_quantity(value: &str) -> Result<Quantity, DecodeError> {
    Quantity::new(parse_plain_decimal(value)?).map_err(|_| DecodeError::InvalidDecimal)
}

fn parse_plain_decimal(value: &str) -> Result<Decimal, DecodeError> {
    if !is_plain_decimal(value) {
        return Err(DecodeError::InvalidDecimal);
    }
    Decimal::from_str_exact(value).map_err(|_| DecodeError::InvalidDecimal)
}

fn is_plain_decimal(value: &str) -> bool {
    let unsigned = match value.strip_prefix('-') {
        Some(value) => value,
        None => value,
    };
    let mut parts = unsigned.split('.');
    let Some(integer) = parts.next() else {
        return false;
    };
    if integer.is_empty() || !integer.bytes().all(|byte| byte.is_ascii_digit()) {
        return false;
    }
    match (parts.next(), parts.next()) {
        (None, None) => true,
        (Some(fraction), None) => {
            !fraction.is_empty() && fraction.bytes().all(|byte| byte.is_ascii_digit())
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use futures_util::{SinkExt, StreamExt};
    use serde_json::json;
    use tokio::io::AsyncReadExt;
    use tokio::net::TcpListener;
    use tokio::sync::{Notify, oneshot};
    use tokio::time::{Duration, timeout};
    use tokio_tungstenite::{accept_async, tungstenite::Message};
    use trench_core::event::{MarketEventKind, TimestampNs};

    use super::{
        DecodeError, DecodedFrame, Decoder, GapEvent, GapReason, Market, SubscriptionKind,
        WsClient, WsConfig, WsLimits, WsOutput,
    };

    fn market(value: &str) -> Market {
        Market::new(value).expect("valid native perpetual market")
    }

    #[test]
    fn decoder_maps_an_exact_l2_book_envelope_to_a_core_snapshot() {
        let mut decoder = Decoder::new([market("BTC")]);
        let received_at = TimestampNs::new(1_700_000_000_001_000_000)
            .expect("receipt timestamp fits nanoseconds");
        let frame = json!({
            "channel": "l2Book",
            "data": {
                "coin": "BTC",
                "time": 1_700_000_000_000_i64,
                "levels": [
                    [{"px": "64120.5", "sz": "1.5", "n": 2}],
                    [{"px": "64121.0", "sz": "0.75", "n": 1}]
                ]
            }
        })
        .to_string();

        let DecodedFrame::MarketEvents(events) = decoder
            .decode(&frame, received_at)
            .expect("well-formed selected L2 frame must decode")
        else {
            panic!("L2 frame must produce a normalized market event");
        };
        assert_eq!(events.len(), 1);
        let event = &events[0];
        assert_eq!(event.market(), &market("BTC"));
        assert_eq!(event.event_time().value(), 1_700_000_000_000_000_000);
        let MarketEventKind::BookSnapshot(snapshot) = event.kind() else {
            panic!("L2 frame must produce a book snapshot");
        };
        assert_eq!(snapshot.bids().len(), 1);
        assert_eq!(snapshot.asks().len(), 1);
    }

    #[test]
    fn decoder_maps_trades_and_drops_duplicate_exchange_identities() {
        let mut decoder = Decoder::new([market("BTC")]);
        let received_at = TimestampNs::new(1_700_000_000_002_000_000)
            .expect("receipt timestamp fits nanoseconds");
        let frame = json!({
            "channel": "trades",
            "data": [
                {
                    "coin": "BTC",
                    "side": "B",
                    "px": "64120.5",
                    "sz": "1.5",
                    "time": 1_700_000_000_000_i64,
                    "tid": 41
                },
                {
                    "coin": "BTC",
                    "side": "A",
                    "px": "64121.0",
                    "sz": "0.25",
                    "time": 1_700_000_000_001_i64,
                    "tid": 42
                }
            ]
        })
        .to_string();

        let DecodedFrame::MarketEvents(events) = decoder
            .decode(&frame, received_at)
            .expect("well-formed selected trades must decode")
        else {
            panic!("trade frame must produce normalized market events");
        };
        assert_eq!(events.len(), 2);
        let MarketEventKind::Trade(first) = events[0].kind() else {
            panic!("trade frame must produce trades");
        };
        assert_eq!(first.trade_id(), 41);

        let DecodedFrame::MarketEvents(duplicates) = decoder
            .decode(&frame, received_at)
            .expect("duplicate trade frame remains a valid frame")
        else {
            panic!("duplicate trade frame must remain a market frame");
        };
        assert!(duplicates.is_empty());
    }

    #[test]
    fn decoder_suppresses_duplicate_identities_within_one_valid_trade_batch() {
        let received_at = TimestampNs::new(1_700_000_000_002_000_000)
            .expect("receipt timestamp fits nanoseconds");
        let mut decoder = Decoder::new([market("BTC")]);
        let frame = json!({
            "channel": "trades",
            "data": [
                {
                    "coin": "BTC", "side": "B", "px": "1", "sz": "1",
                    "time": 1_700_000_000_000_i64, "tid": 41
                },
                {
                    "coin": "BTC", "side": "A", "px": "2", "sz": "2",
                    "time": 1_700_000_000_000_i64, "tid": 41
                }
            ]
        })
        .to_string();

        let DecodedFrame::MarketEvents(events) = decoder
            .decode(&frame, received_at)
            .expect("a valid batch with duplicate identities must decode")
        else {
            panic!("trade frame must produce market events");
        };
        assert_eq!(events.len(), 1);
        assert_eq!(decoder.trades.len(), 1);
    }

    #[tokio::test]
    async fn malformed_trade_batch_does_not_commit_earlier_staged_identities() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback WebSocket server");
        let endpoint = format!(
            "ws://{}",
            listener
                .local_addr()
                .expect("loopback address is available")
        );
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.expect("accept client");
            let mut socket = accept_async(socket)
                .await
                .expect("accept WebSocket handshake");
            for _ in 0..3 {
                let Some(Ok(Message::Text(_))) = socket.next().await else {
                    panic!("client must subscribe before trade data arrives");
                };
            }
            socket
                .send(Message::Text(
                    json!({
                        "channel": "trades",
                        "data": [
                            {
                                "coin": "BTC", "side": "B", "px": "1", "sz": "1",
                                "time": 1_700_000_000_000_i64, "tid": 41
                            },
                            {
                                "coin": "BTC", "side": "A", "px": "bad", "sz": "1",
                                "time": 1_700_000_000_001_i64, "tid": 42
                            }
                        ]
                    })
                    .to_string()
                    .into(),
                ))
                .await
                .expect("send malformed trade batch");
            socket
                .send(Message::Text(
                    json!({
                        "channel": "trades",
                        "data": [{
                            "coin": "BTC", "side": "B", "px": "1", "sz": "1",
                            "time": 1_700_000_000_000_i64, "tid": 41
                        }]
                    })
                    .to_string()
                    .into(),
                ))
                .await
                .expect("replay the formerly staged trade");
        });

        let config = WsConfig::with_limits(vec![market("BTC")], WsLimits::fast_for_test())
            .expect("test configuration is valid");
        let mut stream = WsClient::new_for_test(config, endpoint).start();
        let rejected = timeout(Duration::from_secs(1), stream.recv())
            .await
            .expect("malformed batch rejection before timeout")
            .expect("stream remains open");
        assert!(matches!(
            rejected,
            WsOutput::Rejected(ref update) if update.reason() == super::RejectionReason::InvalidDecimal
        ));
        let replay = timeout(Duration::from_secs(1), stream.recv())
            .await
            .expect("replayed valid trade before timeout")
            .expect("stream remains open");
        let WsOutput::MarketEvent(event) = replay else {
            panic!("the staged identity must not survive a rejected batch");
        };
        let MarketEventKind::Trade(trade) = event.kind() else {
            panic!("replayed trade must remain a trade event");
        };
        assert_eq!(trade.trade_id(), 41);
        stream.cancel();
        server.await.expect("server task must complete");
    }

    #[test]
    fn decoder_maps_a_bbo_envelope_without_synthesizing_levels() {
        let mut decoder = Decoder::new([market("BTC")]);
        let received_at = TimestampNs::new(1_700_000_000_003_000_000)
            .expect("receipt timestamp fits nanoseconds");
        let frame = json!({
            "channel": "bbo",
            "data": {
                "coin": "BTC",
                "time": 1_700_000_000_002_i64,
                "bbo": [
                    {"px": "64120.5", "sz": "1.5", "n": 2},
                    {"px": "64121.0", "sz": "0.75", "n": 1}
                ]
            }
        })
        .to_string();

        let DecodedFrame::MarketEvents(events) = decoder
            .decode(&frame, received_at)
            .expect("well-formed selected BBO must decode")
        else {
            panic!("BBO frame must produce normalized market events");
        };
        assert_eq!(events.len(), 1);
        let MarketEventKind::Bbo(bbo) = events[0].kind() else {
            panic!("BBO frame must produce a BBO event");
        };
        assert_eq!(bbo.bid().price().value().to_string(), "64120.5");
        assert_eq!(bbo.ask().quantity().value().to_string(), "0.75");
    }

    #[test]
    fn decoder_consumes_exact_subscription_acks_and_pongs() {
        let mut decoder = Decoder::new([market("BTC")]);
        let received_at = TimestampNs::new(1_700_000_000_004_000_000)
            .expect("receipt timestamp fits nanoseconds");
        let ack = json!({
            "channel": "subscriptionResponse",
            "data": {
                "method": "subscribe",
                "subscription": {"type": "trades", "coin": "BTC"}
            }
        })
        .to_string();

        let DecodedFrame::SubscriptionAck(subscription) = decoder
            .decode(&ack, received_at)
            .expect("exact subscription acknowledgement must decode")
        else {
            panic!("subscription acknowledgement must not become a market event");
        };
        assert_eq!(subscription.kind(), SubscriptionKind::Trades);
        assert_eq!(subscription.market(), &market("BTC"));

        assert_eq!(
            decoder.decode(r#"{"channel":"pong"}"#, received_at),
            Ok(DecodedFrame::Pong)
        );
    }

    #[test]
    fn decoder_rejects_malformed_foreign_non_native_and_invalid_market_updates() {
        let received_at = TimestampNs::new(1_700_000_000_005_000_000)
            .expect("receipt timestamp fits nanoseconds");
        let mut decoder = Decoder::new([market("BTC")]);
        assert_eq!(
            decoder.decode("not json", received_at),
            Err(DecodeError::Malformed)
        );
        assert_eq!(
            decoder.decode(
                &json!({
                    "channel": "trades",
                    "data": [{
                        "coin": "ETH", "side": "B", "px": "1", "sz": "1",
                        "time": 1_700_000_000_000_i64, "tid": 1
                    }]
                })
                .to_string(),
                received_at,
            ),
            Err(DecodeError::ForeignMarket)
        );
        assert_eq!(
            decoder.decode(
                &json!({
                    "channel": "trades",
                    "data": [{
                        "coin": "@107", "side": "B", "px": "1", "sz": "1",
                        "time": 1_700_000_000_000_i64, "tid": 1
                    }]
                })
                .to_string(),
                received_at,
            ),
            Err(DecodeError::NonNativeMarket)
        );
        assert_eq!(
            decoder.decode(
                &json!({
                    "channel": "bbo",
                    "data": {
                        "coin": "BTC", "time": 1_700_000_000_000_i64,
                        "bbo": [null, {"px": "2", "sz": "1", "n": 1}]
                    }
                })
                .to_string(),
                received_at,
            ),
            Err(DecodeError::MissingLiquidity)
        );
        assert_eq!(
            decoder.decode(
                &json!({
                    "channel": "trades",
                    "data": [{
                        "coin": "BTC", "side": "B", "px": "not-a-decimal", "sz": "1",
                        "time": 1_700_000_000_000_i64, "tid": 1
                    }]
                })
                .to_string(),
                received_at,
            ),
            Err(DecodeError::InvalidDecimal)
        );
    }

    #[test]
    fn wire_decimals_require_plain_finite_base_ten_lexemes() {
        for value in [
            "1e-3",
            "1E3",
            "+1",
            " 1",
            "1 ",
            "NaN",
            "nan",
            "Infinity",
            "inf",
            "-Infinity",
            "-inf",
            "",
            ".",
            "1.2.3",
        ] {
            assert_eq!(
                super::decode_price(value),
                Err(DecodeError::InvalidDecimal),
                "price `{value}` must be rejected before decimal parsing"
            );
            assert_eq!(
                super::decode_quantity(value),
                Err(DecodeError::InvalidDecimal),
                "quantity `{value}` must be rejected before decimal parsing"
            );
        }

        for value in ["1", "1.0", "0.000001", "64120.5", "0.75"] {
            assert!(
                super::decode_price(value).is_ok(),
                "plain exchange price `{value}` must be accepted"
            );
            assert!(
                super::decode_quantity(value).is_ok(),
                "plain exchange quantity `{value}` must be accepted"
            );
        }
    }

    #[test]
    fn wire_decimals_reject_overprecision_instead_of_rounding() {
        let overprecision = "0.12345678901234567890123456789";
        assert_eq!(
            super::decode_price(overprecision),
            Err(DecodeError::InvalidDecimal),
            "over-precision prices must not silently normalize"
        );
        assert_eq!(
            super::decode_quantity(overprecision),
            Err(DecodeError::InvalidDecimal),
            "over-precision quantities must not silently normalize"
        );

        let representable = "0.1234567890123456789012345678";
        assert!(
            super::decode_price(representable).is_ok(),
            "a high but exactly representable price must remain valid"
        );
        assert!(
            super::decode_quantity(representable).is_ok(),
            "a high but exactly representable quantity must remain valid"
        );
    }

    #[test]
    fn decoder_rejects_timestamp_overflow_and_out_of_order_books() {
        let received_at =
            TimestampNs::new(i128::from(i64::MAX)).expect("maximum receipt timestamp is valid");
        let mut decoder = Decoder::new([market("BTC")]);
        let book = |time| {
            json!({
                "channel": "l2Book",
                "data": {
                    "coin": "BTC", "time": time,
                    "levels": [
                        [{"px": "1", "sz": "1", "n": 1}],
                        [{"px": "2", "sz": "1", "n": 1}]
                    ]
                }
            })
            .to_string()
        };

        assert_eq!(
            decoder.decode(&book(i64::MAX), received_at),
            Err(DecodeError::InvalidTimestamp)
        );
        decoder
            .decode(&book(1_700_000_000_000_i64), received_at)
            .expect("first book must decode");
        assert_eq!(
            decoder.decode(&book(1_699_999_999_999_i64), received_at),
            Err(DecodeError::NonMonotonicBook)
        );
    }

    #[test]
    fn decoder_rejects_invalid_l2_without_advancing_its_snapshot_state() {
        let received_at =
            TimestampNs::new(i128::from(i64::MAX)).expect("maximum receipt timestamp is valid");
        let mut decoder = Decoder::new([market("BTC")]);
        let invalid = json!({
            "channel": "l2Book",
            "data": {
                "coin": "BTC", "time": 1_700_000_000_000_i64,
                "levels": [
                    [{"px": "2", "sz": "1", "n": 1}],
                    [{"px": "1", "sz": "1", "n": 1}]
                ]
            }
        })
        .to_string();
        assert_eq!(
            decoder.decode(&invalid, received_at),
            Err(DecodeError::InvalidBook)
        );

        let valid_same_time = json!({
            "channel": "l2Book",
            "data": {
                "coin": "BTC", "time": 1_700_000_000_000_i64,
                "levels": [
                    [{"px": "1", "sz": "1", "n": 1}],
                    [{"px": "2", "sz": "1", "n": 1}]
                ]
            }
        })
        .to_string();
        assert!(decoder.decode(&valid_same_time, received_at).is_ok());
    }

    #[test]
    fn decoder_rejects_l2_frames_above_the_official_visible_depth_bound() {
        let received_at =
            TimestampNs::new(i128::from(i64::MAX)).expect("maximum receipt timestamp is valid");
        let mut decoder = Decoder::new([market("BTC")]);
        let levels = vec![json!({"px": "1", "sz": "1", "n": 1}); 21];
        let frame = json!({
            "channel": "l2Book",
            "data": {
                "coin": "BTC", "time": 1_700_000_000_000_i64,
                "levels": [levels, [{"px": "2", "sz": "1", "n": 1}]]
            }
        })
        .to_string();
        assert_eq!(
            decoder.decode(&frame, received_at),
            Err(DecodeError::InvalidBook)
        );
    }

    #[test]
    fn decoder_keeps_trade_deduplication_across_connections_but_resets_book_ordering() {
        let received_at =
            TimestampNs::new(i128::from(i64::MAX)).expect("maximum receipt timestamp is valid");
        let mut decoder = Decoder::new([market("BTC")]);
        let trade = json!({
            "channel": "trades",
            "data": [{
                "coin": "BTC", "side": "B", "px": "1", "sz": "1",
                "time": 1_700_000_000_000_i64, "tid": 7
            }]
        })
        .to_string();
        let book = json!({
            "channel": "l2Book",
            "data": {
                "coin": "BTC", "time": 1_700_000_000_000_i64,
                "levels": [
                    [{"px": "1", "sz": "1", "n": 1}],
                    [{"px": "2", "sz": "1", "n": 1}]
                ]
            }
        })
        .to_string();

        assert!(decoder.decode(&trade, received_at).is_ok());
        assert!(decoder.decode(&book, received_at).is_ok());
        decoder.begin_connection();
        let DecodedFrame::MarketEvents(duplicates) = decoder
            .decode(&trade, received_at)
            .expect("duplicate trade frame remains valid")
        else {
            panic!("trade frame must stay a market frame");
        };
        assert!(duplicates.is_empty());
        assert!(decoder.decode(&book, received_at).is_ok());
    }

    #[test]
    fn decoder_never_reemits_a_trade_identity_past_the_legacy_cache_capacity() {
        const LEGACY_CACHE_CAPACITY: usize = 4_096;

        let received_at =
            TimestampNs::new(i128::from(i64::MAX)).expect("maximum receipt timestamp is valid");
        let mut decoder = Decoder::new([market("BTC")]);
        let initial = json!({
            "channel": "trades",
            "data": (0..=LEGACY_CACHE_CAPACITY)
                .map(|trade_id| json!({
                    "coin": "BTC", "side": "B", "px": "1", "sz": "1",
                    "time": 1_700_000_000_000_i64, "tid": trade_id
                }))
                .collect::<Vec<_>>(),
        })
        .to_string();
        let DecodedFrame::MarketEvents(events) = decoder
            .decode(&initial, received_at)
            .expect("unique trade identities must decode")
        else {
            panic!("trade frame must produce normalized market events");
        };
        assert_eq!(events.len(), LEGACY_CACHE_CAPACITY + 1);

        decoder.begin_connection();
        let replay = json!({
            "channel": "trades",
            "data": [{
                "coin": "BTC", "side": "B", "px": "1", "sz": "1",
                "time": 1_700_000_000_000_i64, "tid": 0
            }]
        })
        .to_string();
        let DecodedFrame::MarketEvents(events) = decoder
            .decode(&replay, received_at)
            .expect("a replayed trade remains a valid wire frame")
        else {
            panic!("trade frame must remain a market frame");
        };
        assert!(
            events.is_empty(),
            "a trade identity cannot be re-emitted after reconnect"
        );
    }

    #[test]
    fn decoder_stops_an_exact_trade_identity_epoch_before_overflow() {
        let received_at =
            TimestampNs::new(i128::from(i64::MAX)).expect("maximum receipt timestamp is valid");
        let mut decoder = Decoder::with_trade_identity_limit([market("BTC")], 2);
        let at_capacity = json!({
            "channel": "trades",
            "data": [
                {
                    "coin": "BTC", "side": "B", "px": "1", "sz": "1",
                    "time": 1_700_000_000_000_i64, "tid": 1
                },
                {
                    "coin": "BTC", "side": "A", "px": "2", "sz": "1",
                    "time": 1_700_000_000_001_i64, "tid": 2
                }
            ]
        })
        .to_string();
        let DecodedFrame::MarketEvents(events) = decoder
            .decode(&at_capacity, received_at)
            .expect("the exact identity cap must remain usable")
        else {
            panic!("trade frame must produce market events");
        };
        assert_eq!(events.len(), 2);
        assert_eq!(decoder.trades.len(), 2);

        let overflow = json!({
            "channel": "trades",
            "data": [{
                "coin": "BTC", "side": "B", "px": "3", "sz": "1",
                "time": 1_700_000_000_002_i64, "tid": 3
            }]
        })
        .to_string();
        assert_eq!(
            decoder.decode(&overflow, received_at),
            Err(DecodeError::TradeIdentityLimit { max_identities: 2 })
        );
        assert_eq!(decoder.trades.len(), 2);

        let replay = json!({
            "channel": "trades",
            "data": [{
                "coin": "BTC", "side": "B", "px": "1", "sz": "1",
                "time": 1_700_000_000_000_i64, "tid": 1
            }]
        })
        .to_string();
        let DecodedFrame::MarketEvents(events) = decoder
            .decode(&replay, received_at)
            .expect("already-recorded identities remain valid")
        else {
            panic!("trade frame must remain a market frame");
        };
        assert!(events.is_empty());
    }

    #[test]
    fn repeated_reconnect_failures_preserve_the_original_pending_gap_generation() {
        let mut state = super::StreamState::new(vec![market("BTC")]);
        let first = state.open_gaps(GapReason::TransportClosed);
        assert_eq!(first.len(), 1);
        assert_eq!(state.generation, 1);
        assert!(state.open_gaps(GapReason::TransportError).is_empty());
        assert_eq!(state.generation, 1);
        assert_eq!(
            state
                .pending_gaps
                .get(&market("BTC"))
                .expect("the first gap remains pending")
                .opened
                .reason(),
            GapReason::TransportClosed
        );
    }

    #[test]
    fn reconnect_exhaustion_retains_the_original_gap_evidence() {
        let mut state = super::StreamState::new(vec![market("BTC")]);
        let _ = state.open_gaps(GapReason::TransportClosed);
        state.record_reconnect_connection();
        state.record_reconnect_connection();

        let exhausted = state.reconnect_exhausted();
        let [GapEvent::ReconnectExhausted(exhausted)] = exhausted.as_slice() else {
            panic!("reconnect exhaustion must be a typed terminal gap record");
        };
        assert_eq!(exhausted.generation(), 1);
        assert_eq!(exhausted.market(), &market("BTC"));
        assert_eq!(exhausted.reason(), GapReason::TransportClosed);
        assert_eq!(exhausted.reconnect_attempts(), 2);
    }

    #[test]
    fn reconnect_backoff_is_seeded_bounded_and_deterministic_for_tests() {
        let limits = WsLimits::default();
        let mut first = super::ReconnectBackoff::new(7);
        let mut second = super::ReconnectBackoff::new(7);
        for attempt in 1..=20 {
            let delay = first.delay(attempt, limits);
            assert_eq!(delay, second.delay(attempt, limits));
            assert!(delay >= limits.reconnect_min_delay());
            assert!(delay <= limits.reconnect_max_delay());
        }
    }

    #[tokio::test]
    async fn client_sends_exact_subscriptions_decodes_events_and_heartbeats() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback WebSocket server");
        let endpoint = format!(
            "ws://{}",
            listener
                .local_addr()
                .expect("loopback address is available")
        );
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.expect("accept client");
            let mut socket = accept_async(socket)
                .await
                .expect("accept WebSocket handshake");
            for expected in [
                json!({"method":"subscribe","subscription":{"type":"l2Book","coin":"BTC"}}),
                json!({"method":"subscribe","subscription":{"type":"trades","coin":"BTC"}}),
                json!({"method":"subscribe","subscription":{"type":"bbo","coin":"BTC"}}),
            ] {
                let Some(Ok(Message::Text(message))) = socket.next().await else {
                    panic!("client must send a text subscription");
                };
                assert_eq!(
                    serde_json::from_str::<serde_json::Value>(&message)
                        .expect("subscription must be JSON"),
                    expected
                );
            }
            socket
                .send(Message::Ping(b"challenge".to_vec().into()))
                .await
                .expect("send WebSocket ping");
            let mut saw_heartbeat = false;
            loop {
                let Some(Ok(message)) = socket.next().await else {
                    panic!("client must answer server pings with a pong");
                };
                match message {
                    Message::Pong(payload) => {
                        assert_eq!(payload.as_ref(), b"challenge");
                        break;
                    }
                    Message::Text(message) => {
                        assert_eq!(
                            serde_json::from_str::<serde_json::Value>(&message)
                                .expect("heartbeat must be JSON"),
                            json!({"method":"ping"})
                        );
                        saw_heartbeat = true;
                    }
                    _ => panic!("client must only send its heartbeat or matching pong"),
                }
            }
            socket
                .send(Message::Text(
                    json!({
                        "channel": "l2Book",
                        "data": {
                            "coin": "BTC", "time": 1_700_000_000_000_i64,
                            "levels": [
                                [{"px": "1", "sz": "1", "n": 1}],
                                [{"px": "2", "sz": "1", "n": 1}]
                            ]
                        }
                    })
                    .to_string()
                    .into(),
                ))
                .await
                .expect("send L2 message");
            if !saw_heartbeat {
                let Some(Ok(Message::Text(message))) = socket.next().await else {
                    panic!("client must send an official JSON heartbeat");
                };
                assert_eq!(
                    serde_json::from_str::<serde_json::Value>(&message)
                        .expect("heartbeat must be JSON"),
                    json!({"method":"ping"})
                );
            }
        });

        let limits = WsLimits::fast_for_test();
        let config =
            WsConfig::with_limits(vec![market("BTC")], limits).expect("test limits are valid");
        let client = WsClient::new_for_test(config, endpoint);
        let mut stream = client.start();
        let output = timeout(Duration::from_secs(1), stream.recv())
            .await
            .expect("market event must arrive before timeout")
            .expect("stream must remain open");
        assert!(matches!(output, WsOutput::MarketEvent(_)));
        server.await.expect("server task must complete");
        stream.cancel();
    }

    #[tokio::test]
    async fn disconnect_creates_append_only_gap_and_requires_a_fresh_l2_to_close() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback WebSocket server");
        let endpoint = format!(
            "ws://{}",
            listener
                .local_addr()
                .expect("loopback address is available")
        );
        let server = tokio::spawn(async move {
            for time in [1_700_000_000_000_i64, 1_700_000_000_001_i64] {
                let (socket, _) = listener.accept().await.expect("accept reconnect");
                let mut socket = accept_async(socket)
                    .await
                    .expect("accept WebSocket handshake");
                for _ in 0..3 {
                    let Some(Ok(Message::Text(_))) = socket.next().await else {
                        panic!("every connection must resubscribe exactly once per feed");
                    };
                }
                socket
                    .send(Message::Text(
                        json!({
                            "channel": "l2Book",
                            "data": {
                                "coin": "BTC", "time": time,
                                "levels": [
                                    [{"px": "1", "sz": "1", "n": 1}],
                                    [{"px": "2", "sz": "1", "n": 1}]
                                ]
                            }
                        })
                        .to_string()
                        .into(),
                    ))
                    .await
                    .expect("send fresh L2 snapshot");
                socket
                    .send(Message::Close(None))
                    .await
                    .expect("force reconnect");
                let _ = timeout(Duration::from_millis(20), socket.next()).await;
            }
        });

        let config = WsConfig::with_limits(vec![market("BTC")], WsLimits::fast_for_test())
            .expect("test configuration is valid");
        let mut stream = WsClient::new_for_test(config, endpoint).start();
        let first = timeout(Duration::from_secs(1), stream.recv())
            .await
            .expect("first market event before timeout")
            .expect("stream remains open");
        assert!(matches!(first, WsOutput::MarketEvent(_)));
        let gap_opened = timeout(Duration::from_secs(1), stream.recv())
            .await
            .expect("gap opening before timeout")
            .expect("stream remains open");
        let WsOutput::Gap(GapEvent::Opened(opened)) = gap_opened else {
            panic!("disconnect must be observed as a typed gap opening");
        };
        assert_eq!(opened.reason(), GapReason::TransportClosed);
        assert_eq!(opened.generation(), 1);
        let second = timeout(Duration::from_secs(1), stream.recv())
            .await
            .expect("fresh snapshot before timeout")
            .expect("stream remains open");
        assert!(matches!(second, WsOutput::MarketEvent(_)));
        let gap_closed = timeout(Duration::from_secs(1), stream.recv())
            .await
            .expect("gap closure before timeout")
            .expect("stream remains open");
        let WsOutput::Gap(GapEvent::Closed(closed)) = gap_closed else {
            panic!("only a fresh L2 snapshot can close a typed gap");
        };
        assert_eq!(closed.generation(), 1);
        assert_eq!(closed.reconnect_attempt(), 1);
        stream.cancel();
        server.await.expect("server task must complete");
    }

    #[tokio::test]
    async fn reconnect_budget_exhaustion_emits_a_terminal_typed_gap_record() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback WebSocket server");
        let endpoint = format!(
            "ws://{}",
            listener
                .local_addr()
                .expect("loopback address is available")
        );
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.expect("accept client");
            let mut socket = accept_async(socket)
                .await
                .expect("accept WebSocket handshake");
            for _ in 0..3 {
                let Some(Ok(Message::Text(_))) = socket.next().await else {
                    panic!("client must subscribe before the forced disconnect");
                };
            }
            socket
                .send(Message::Close(None))
                .await
                .expect("force reconnect exhaustion");
        });

        let config = WsConfig::with_limits(vec![market("BTC")], WsLimits::fast_for_test())
            .expect("test configuration is valid");
        let mut stream = WsClient::new_for_test(config, endpoint).start();
        let opened = timeout(Duration::from_secs(1), stream.recv())
            .await
            .expect("gap opening before timeout")
            .expect("stream remains open before reconnect exhaustion");
        assert!(matches!(opened, WsOutput::Gap(GapEvent::Opened(_))));
        let terminal = timeout(Duration::from_secs(1), stream.recv())
            .await
            .expect("terminal gap record before timeout")
            .expect("exhaustion record must be sent before stream termination");
        let WsOutput::Gap(GapEvent::ReconnectExhausted(exhausted)) = terminal else {
            panic!("reconnect exhaustion must remain an append-only typed gap record");
        };
        assert_eq!(exhausted.generation(), 1);
        assert_eq!(exhausted.reconnect_attempts(), 0);
        assert!(
            timeout(Duration::from_secs(1), stream.recv())
                .await
                .expect("stream must terminate after its terminal gap record")
                .is_none()
        );
        server.await.expect("server task must complete");
    }

    #[tokio::test]
    async fn acknowledged_connections_without_l2_exhaust_the_reconnect_budget() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback WebSocket server");
        let endpoint = format!(
            "ws://{}",
            listener
                .local_addr()
                .expect("loopback address is available")
        );
        let server = tokio::spawn(async move {
            for _ in 0..3 {
                let (socket, _) = listener.accept().await.expect("accept reconnect");
                let mut socket = accept_async(socket)
                    .await
                    .expect("accept WebSocket handshake");
                for kind in ["l2Book", "trades", "bbo"] {
                    let Some(Ok(Message::Text(subscription))) = socket.next().await else {
                        panic!("client must send every intended subscription");
                    };
                    assert_eq!(
                        serde_json::from_str::<serde_json::Value>(&subscription)
                            .expect("subscription must be JSON"),
                        json!({"method":"subscribe","subscription":{"type":kind,"coin":"BTC"}})
                    );
                    socket
                        .send(Message::Text(
                            json!({
                                "channel": "subscriptionResponse",
                                "data": {
                                    "method": "subscribe",
                                    "subscription": {"type": kind, "coin": "BTC"}
                                }
                            })
                            .to_string()
                            .into(),
                        ))
                        .await
                        .expect("acknowledge subscription");
                }
                socket
                    .send(Message::Close(None))
                    .await
                    .expect("close acknowledged but unhealthy connection");
            }
            assert!(
                timeout(Duration::from_millis(100), listener.accept())
                    .await
                    .is_err(),
                "an acknowledged connection without a valid L2 must not renew the reconnect budget"
            );
        });

        let config = WsConfig::with_limits(vec![market("BTC")], WsLimits::fast_for_test())
            .expect("test configuration is valid");
        let mut stream = WsClient::new_for_test(config, endpoint).start();
        assert!(matches!(
            timeout(Duration::from_secs(1), stream.recv())
                .await
                .expect("initial gap must arrive"),
            Some(WsOutput::Gap(GapEvent::Opened(_)))
        ));
        let terminal = timeout(Duration::from_secs(1), stream.recv())
            .await
            .expect("reconnect budget must be exhausted")
            .expect("terminal record must precede stream closure");
        let WsOutput::Gap(GapEvent::ReconnectExhausted(exhausted)) = terminal else {
            panic!("unhealthy acknowledged connections must terminate with a typed gap");
        };
        assert_eq!(exhausted.reconnect_attempts(), 2);
        assert!(
            timeout(Duration::from_secs(1), stream.recv())
                .await
                .expect("stream must terminate after exhaustion")
                .is_none()
        );
        server
            .await
            .expect("server task must not observe a fourth reconnect");
    }

    #[tokio::test]
    async fn initial_connection_without_l2_times_out_despite_acks_pongs_and_trades() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback WebSocket server");
        let endpoint = format!(
            "ws://{}",
            listener
                .local_addr()
                .expect("loopback address is available")
        );
        let server = tokio::spawn(async move {
            for connection in 0..3_u64 {
                let (socket, _) = listener.accept().await.expect("accept reconnect");
                let mut socket = accept_async(socket)
                    .await
                    .expect("accept WebSocket handshake");
                for kind in ["l2Book", "trades", "bbo"] {
                    let Some(Ok(Message::Text(subscription))) = socket.next().await else {
                        panic!("client must subscribe before the readiness deadline");
                    };
                    assert_eq!(
                        serde_json::from_str::<serde_json::Value>(&subscription)
                            .expect("subscription must be JSON"),
                        json!({"method":"subscribe","subscription":{"type":kind,"coin":"BTC"}})
                    );
                    socket
                        .send(Message::Text(
                            json!({
                                "channel": "subscriptionResponse",
                                "data": {
                                    "method": "subscribe",
                                    "subscription": {"type": kind, "coin": "BTC"}
                                }
                            })
                            .to_string()
                            .into(),
                        ))
                        .await
                        .expect("acknowledge subscription");
                }
                for tick in 0..20_u64 {
                    if socket
                        .send(Message::Text(r#"{"channel":"pong"}"#.into()))
                        .await
                        .is_err()
                    {
                        break;
                    }
                    if socket
                        .send(Message::Text(
                            json!({
                                "channel": "trades",
                                "data": [{
                                    "coin": "BTC", "side": "B", "px": "1", "sz": "1",
                                    "time": 1_700_000_000_000_i64 + i64::try_from(tick).expect("tick fits i64"),
                                    "tid": connection * 100 + tick
                                }]
                            })
                            .to_string()
                            .into(),
                        ))
                        .await
                        .is_err()
                    {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }
        });

        let config = WsConfig::with_limits(vec![market("BTC")], WsLimits::fast_for_test())
            .expect("test configuration is valid");
        let mut stream = WsClient::new_for_test(config, endpoint).start();
        let opened = loop {
            let output = timeout(Duration::from_secs(1), stream.recv())
                .await
                .expect("readiness gap before timeout")
                .expect("stream must remain open before exhaustion");
            if let WsOutput::Gap(GapEvent::Opened(opened)) = output {
                break opened;
            }
        };
        assert_eq!(opened.reason(), GapReason::SnapshotRecoveryTimeout);
        let exhausted = loop {
            let output = timeout(Duration::from_secs(1), stream.recv())
                .await
                .expect("terminal gap before timeout")
                .expect("exhaustion record must precede stream closure");
            if let WsOutput::Gap(GapEvent::ReconnectExhausted(exhausted)) = output {
                break exhausted;
            }
        };
        assert_eq!(exhausted.reason(), GapReason::SnapshotRecoveryTimeout);
        assert_eq!(exhausted.reconnect_attempts(), 2);
        assert!(
            timeout(Duration::from_secs(1), stream.recv())
                .await
                .expect("stream must stop after reconnect exhaustion")
                .is_none()
        );
        server
            .await
            .expect("server must observe the bounded reconnect sequence");
    }

    #[tokio::test]
    async fn initial_complete_l2_snapshots_keep_the_connection_healthy() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback WebSocket server");
        let endpoint = format!(
            "ws://{}",
            listener
                .local_addr()
                .expect("loopback address is available")
        );
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.expect("accept client");
            let mut socket = accept_async(socket)
                .await
                .expect("accept WebSocket handshake");
            for (coin, kind) in [
                ("BTC", "l2Book"),
                ("BTC", "trades"),
                ("BTC", "bbo"),
                ("ETH", "l2Book"),
                ("ETH", "trades"),
                ("ETH", "bbo"),
            ] {
                let Some(Ok(Message::Text(subscription))) = socket.next().await else {
                    panic!("client must subscribe before L2 readiness");
                };
                assert_eq!(
                    serde_json::from_str::<serde_json::Value>(&subscription)
                        .expect("subscription must be JSON"),
                    json!({"method":"subscribe","subscription":{"type":kind,"coin":coin}})
                );
            }
            for coin in ["BTC", "ETH"] {
                socket
                    .send(Message::Text(
                        json!({
                            "channel": "l2Book",
                            "data": {
                                "coin": coin, "time": 1_700_000_000_000_i64,
                                "levels": [
                                    [{"px": "1", "sz": "1", "n": 1}],
                                    [{"px": "2", "sz": "1", "n": 1}]
                                ]
                            }
                        })
                        .to_string()
                        .into(),
                    ))
                    .await
                    .expect("send initial L2 snapshot");
            }
            for _ in 0..20 {
                if socket
                    .send(Message::Text(r#"{"channel":"pong"}"#.into()))
                    .await
                    .is_err()
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        });

        let config = WsConfig::with_limits(
            vec![market("BTC"), market("ETH")],
            WsLimits::fast_for_test(),
        )
        .expect("test configuration is valid");
        let mut stream = WsClient::new_for_test(config, endpoint).start();
        for _ in 0..2 {
            assert!(matches!(
                timeout(Duration::from_secs(1), stream.recv())
                    .await
                    .expect("initial L2 before timeout"),
                Some(WsOutput::MarketEvent(_))
            ));
        }
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(
            timeout(Duration::from_millis(20), stream.recv())
                .await
                .is_err(),
            "all selected initial L2 snapshots must prevent a readiness-timeout gap"
        );
        stream.cancel();
        server
            .await
            .expect("server task must complete after cancellation");
    }

    #[tokio::test]
    async fn all_selected_l2_snapshots_renew_the_reconnect_budget() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback WebSocket server");
        let endpoint = format!(
            "ws://{}",
            listener
                .local_addr()
                .expect("loopback address is available")
        );
        let server = tokio::spawn(async move {
            for connection in 0..4 {
                let (socket, _) = listener.accept().await.expect("accept reconnect");
                let mut socket = accept_async(socket)
                    .await
                    .expect("accept WebSocket handshake");
                for (coin, kind) in [
                    ("BTC", "l2Book"),
                    ("BTC", "trades"),
                    ("BTC", "bbo"),
                    ("ETH", "l2Book"),
                    ("ETH", "trades"),
                    ("ETH", "bbo"),
                ] {
                    let Some(Ok(Message::Text(subscription))) = socket.next().await else {
                        panic!("client must send every intended subscription");
                    };
                    assert_eq!(
                        serde_json::from_str::<serde_json::Value>(&subscription)
                            .expect("subscription must be JSON"),
                        json!({"method":"subscribe","subscription":{"type":kind,"coin":coin}})
                    );
                }
                if connection == 1 {
                    for coin in ["BTC", "ETH"] {
                        socket
                            .send(Message::Text(
                                json!({
                                    "channel": "l2Book",
                                    "data": {
                                        "coin": coin, "time": 1_700_000_000_000_i64,
                                        "levels": [
                                            [{"px": "1", "sz": "1", "n": 1}],
                                            [{"px": "2", "sz": "1", "n": 1}]
                                        ]
                                    }
                                })
                                .to_string()
                                .into(),
                            ))
                            .await
                            .expect("send healthy fresh L2");
                    }
                }
                socket
                    .send(Message::Close(None))
                    .await
                    .expect("force reconnect");
            }
            assert!(
                timeout(Duration::from_millis(100), listener.accept())
                    .await
                    .is_err(),
                "all selected healthy L2 snapshots must renew exactly one full reconnect budget"
            );
        });

        let config = WsConfig::with_limits(
            vec![market("BTC"), market("ETH")],
            WsLimits::fast_with_output_capacity_for_test(16),
        )
        .expect("test configuration is valid");
        let mut stream = WsClient::new_for_test(config, endpoint).start();
        let mut fresh_l2s = 0;
        let mut recovered_gaps = 0;
        let mut exhausted = Vec::new();
        while let Some(output) = timeout(Duration::from_secs(1), stream.recv())
            .await
            .expect("expected output before timeout")
        {
            match output {
                WsOutput::MarketEvent(_) => fresh_l2s += 1,
                WsOutput::Gap(GapEvent::Closed(_)) => recovered_gaps += 1,
                WsOutput::Gap(GapEvent::ReconnectExhausted(gap)) => exhausted.push(gap),
                WsOutput::Gap(GapEvent::Opened(_)) | WsOutput::Rejected(_) => {}
                WsOutput::Terminal(_) => panic!("identity cap must not end this recovery test"),
            }
        }
        assert_eq!(fresh_l2s, 2);
        assert_eq!(recovered_gaps, 2);
        assert_eq!(exhausted.len(), 2);
        assert!(exhausted.iter().all(|gap| gap.reconnect_attempts() == 2));
        server
            .await
            .expect("server must observe the renewed reconnect budget");
    }

    #[tokio::test]
    async fn incomplete_multi_market_recovery_cannot_be_kept_alive_by_acks_pongs_or_trades() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback WebSocket server");
        let endpoint = format!(
            "ws://{}",
            listener
                .local_addr()
                .expect("loopback address is available")
        );
        let server = tokio::spawn(async move {
            for connection in 0..3 {
                let (socket, _) = listener.accept().await.expect("accept reconnect");
                let mut socket = accept_async(socket)
                    .await
                    .expect("accept WebSocket handshake");
                for expected in [
                    ("BTC", "l2Book"),
                    ("BTC", "trades"),
                    ("BTC", "bbo"),
                    ("ETH", "l2Book"),
                    ("ETH", "trades"),
                    ("ETH", "bbo"),
                ] {
                    let Some(Ok(Message::Text(subscription))) = socket.next().await else {
                        panic!(
                            "client must subscribe to every selected market feed on connection {connection} for {expected:?}"
                        );
                    };
                    assert_eq!(
                        serde_json::from_str::<serde_json::Value>(&subscription)
                            .expect("subscription must be JSON"),
                        json!({
                            "method": "subscribe",
                            "subscription": {"type": expected.1, "coin": expected.0}
                        })
                    );
                    socket
                        .send(Message::Text(
                            json!({
                                "channel": "subscriptionResponse",
                                "data": {
                                    "method": "subscribe",
                                    "subscription": {"type": expected.1, "coin": expected.0}
                                }
                            })
                            .to_string()
                            .into(),
                        ))
                        .await
                        .expect("acknowledge subscription");
                }
                let books: &[&str] = if connection == 0 {
                    &["BTC", "ETH"]
                } else {
                    &["BTC"]
                };
                for coin in books {
                    socket
                        .send(Message::Text(
                            json!({
                                "channel": "l2Book",
                                "data": {
                                    "coin": coin,
                                    "time": 1_700_000_000_000_i64 + i64::from(connection),
                                    "levels": [
                                        [{"px": "1", "sz": "1", "n": 1}],
                                        [{"px": "2", "sz": "1", "n": 1}]
                                    ]
                                }
                            })
                            .to_string()
                            .into(),
                        ))
                        .await
                        .expect("send fresh L2 snapshot");
                }
                if connection == 0 {
                    socket
                        .send(Message::Close(None))
                        .await
                        .expect("open recovery gaps");
                    continue;
                }
                for _ in 0..20 {
                    if socket
                        .send(Message::Text(r#"{"channel":"pong"}"#.into()))
                        .await
                        .is_err()
                    {
                        break;
                    }
                    if socket
                        .send(Message::Text(
                            json!({
                                "channel": "trades",
                                "data": [{
                                    "coin": "BTC", "side": "B", "px": "1", "sz": "1",
                                    "time": 1_700_000_000_010_i64, "tid": 99
                                }]
                            })
                            .to_string()
                            .into(),
                        ))
                        .await
                        .is_err()
                    {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }
            assert!(
                timeout(Duration::from_millis(100), listener.accept())
                    .await
                    .is_err(),
                "BTC-only recovery must exhaust retries instead of opening a fourth connection"
            );
        });

        let limits = WsLimits::fast_with_output_capacity_for_test(16);
        let config = WsConfig::with_limits(vec![market("BTC"), market("ETH")], limits)
            .expect("test configuration is valid");
        let mut stream = WsClient::new_for_test(config, endpoint).start();
        server
            .await
            .expect("server must not observe a reconnect after the recovery budget exhausts");

        let mut closed_markets = Vec::new();
        let mut exhausted_markets = Vec::new();
        while let Some(output) = timeout(Duration::from_secs(1), stream.recv())
            .await
            .expect("typed output before timeout")
        {
            match output {
                WsOutput::Gap(GapEvent::Closed(closed)) => {
                    closed_markets.push(closed.market().clone());
                }
                WsOutput::Gap(GapEvent::ReconnectExhausted(exhausted)) => {
                    exhausted_markets.push(exhausted.market().clone());
                }
                WsOutput::MarketEvent(_)
                | WsOutput::Gap(GapEvent::Opened(_))
                | WsOutput::Rejected(_) => {}
                WsOutput::Terminal(_) => panic!("identity cap must not end this recovery test"),
            }
        }
        assert_eq!(closed_markets, vec![market("BTC"), market("BTC")]);
        assert_eq!(exhausted_markets, vec![market("BTC"), market("ETH")]);
    }

    #[tokio::test]
    async fn downstream_market_channel_never_exceeds_its_configured_capacity() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback WebSocket server");
        let endpoint = format!(
            "ws://{}",
            listener
                .local_addr()
                .expect("loopback address is available")
        );
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.expect("accept client");
            let mut socket = accept_async(socket)
                .await
                .expect("accept WebSocket handshake");
            for _ in 0..3 {
                let Some(Ok(Message::Text(_))) = socket.next().await else {
                    panic!("client must subscribe before market data arrives");
                };
            }
            for time in [1_700_000_000_000_i64, 1_700_000_000_001_i64] {
                socket
                    .send(Message::Text(
                        json!({
                            "channel": "trades",
                            "data": [{
                                "coin": "BTC", "side": "B", "px": "1", "sz": "1",
                                "time": time, "tid": time
                            }]
                        })
                        .to_string()
                        .into(),
                    ))
                    .await
                    .expect("send public trade frame");
            }
        });

        let limits = WsLimits::fast_with_output_capacity_for_test(1);
        let config = WsConfig::with_limits(vec![market("BTC")], limits)
            .expect("test configuration is valid");
        let stream = WsClient::new_for_test(config, endpoint).start();
        timeout(Duration::from_secs(1), async {
            while stream.receiver.len() != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("first event must enter the bounded channel");
        assert_eq!(stream.receiver.len(), 1);
        stream.cancel();
        let _ = timeout(Duration::from_secs(1), server)
            .await
            .expect("server must not be backpressured by local output");
    }

    #[tokio::test]
    async fn shutdown_completes_while_a_full_output_channel_blocks_an_enqueue() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback WebSocket server");
        let endpoint = format!(
            "ws://{}",
            listener
                .local_addr()
                .expect("loopback address is available")
        );
        let (second_frame_sent, second_frame_observed) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let mut second_frame_sent = Some(second_frame_sent);
            let (socket, _) = listener.accept().await.expect("accept client");
            let mut socket = accept_async(socket)
                .await
                .expect("accept WebSocket handshake");
            for _ in 0..3 {
                let Some(Ok(Message::Text(_))) = socket.next().await else {
                    panic!("client must subscribe before market data arrives");
                };
            }
            for time in [1_700_000_000_000_i64, 1_700_000_000_001_i64] {
                socket
                    .send(Message::Text(
                        json!({
                            "channel": "trades",
                            "data": [{
                                "coin": "BTC", "side": "B", "px": "1", "sz": "1",
                                "time": time, "tid": time
                            }]
                        })
                        .to_string()
                        .into(),
                    ))
                    .await
                    .expect("send public trade frame");
                if time == 1_700_000_000_001_i64
                    && let Some(sender) = second_frame_sent.take()
                {
                    let _ = sender.send(());
                }
            }
        });

        let limits = WsLimits::fast_with_output_capacity_for_test(1);
        let config = WsConfig::with_limits(vec![market("BTC")], limits)
            .expect("test configuration is valid");
        let stream = WsClient::new_for_test(config, endpoint).start();
        second_frame_observed
            .await
            .expect("server must send the second frame");
        timeout(Duration::from_secs(1), async {
            while stream.receiver.len() != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the first event must fill the bounded receiver");
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(
            timeout(Duration::from_millis(100), stream.shutdown())
                .await
                .is_ok(),
            "shutdown must cancel an output enqueue without draining the receiver"
        );
        server.await.expect("server task must complete");
    }

    #[tokio::test]
    async fn shutdown_cancels_a_stalled_websocket_handshake() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback TCP server");
        let endpoint = format!(
            "ws://{}",
            listener
                .local_addr()
                .expect("loopback address is available")
        );
        let (handshake_received, handshake_observed) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept TCP client");
            let mut request = [0_u8; 1_024];
            let bytes = socket
                .read(&mut request)
                .await
                .expect("read WebSocket handshake request");
            assert!(bytes > 0, "client must begin the WebSocket handshake");
            let _ = handshake_received.send(());
            let bytes = timeout(Duration::from_secs(1), socket.read(&mut request))
                .await
                .expect("client must close the stalled handshake connection")
                .expect("read connection closure");
            assert_eq!(bytes, 0, "cancellation must close the handshake connection");
        });

        let config = WsConfig::with_limits(vec![market("BTC")], WsLimits::fast_for_test())
            .expect("test configuration is valid");
        let stream = WsClient::new_for_test(config, endpoint).start();
        handshake_observed
            .await
            .expect("server must observe the pending handshake");
        assert!(
            timeout(Duration::from_millis(50), stream.shutdown())
                .await
                .is_ok(),
            "shutdown must preempt the configured handshake timeout"
        );
        server
            .await
            .expect("server task must finish after handshake cancellation");
    }

    #[tokio::test]
    async fn shutdown_cancels_a_stalled_websocket_write() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback WebSocket server");
        let endpoint = format!(
            "ws://{}",
            listener
                .local_addr()
                .expect("loopback address is available")
        );
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.expect("accept client");
            let mut socket = accept_async(socket)
                .await
                .expect("accept WebSocket handshake");
            let _ = timeout(Duration::from_secs(1), socket.next()).await;
        });

        let write_started = Arc::new(Notify::new());
        let config = WsConfig::with_limits(vec![market("BTC")], WsLimits::fast_for_test())
            .expect("test configuration is valid");
        let stream =
            WsClient::new_for_stalled_writes_test(config, endpoint, write_started.clone()).start();
        timeout(Duration::from_secs(1), write_started.notified())
            .await
            .expect("client must enter a pending WebSocket write");
        assert!(
            timeout(Duration::from_millis(50), stream.shutdown())
                .await
                .is_ok(),
            "shutdown must preempt the configured WebSocket write timeout"
        );
        server
            .await
            .expect("server task must finish after write cancellation");
    }

    #[tokio::test]
    async fn trade_identity_limit_emits_a_terminal_record_before_overflow() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback WebSocket server");
        let endpoint = format!(
            "ws://{}",
            listener
                .local_addr()
                .expect("loopback address is available")
        );
        let (client_terminated, await_client_termination) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.expect("accept client");
            let mut socket = accept_async(socket)
                .await
                .expect("accept WebSocket handshake");
            for _ in 0..3 {
                let Some(Ok(Message::Text(_))) = socket.next().await else {
                    panic!("client must subscribe before trade data arrives");
                };
            }
            for (time, trade_id) in [
                (1_700_000_000_000_i64, 1_u64),
                (1_700_000_000_001_i64, 2_u64),
            ] {
                socket
                    .send(Message::Text(
                        json!({
                            "channel": "trades",
                            "data": [{
                                "coin": "BTC", "side": "B", "px": "1", "sz": "1",
                                "time": time, "tid": trade_id
                            }]
                        })
                        .to_string()
                        .into(),
                    ))
                    .await
                    .expect("send public trade frame");
            }
            timeout(Duration::from_secs(1), await_client_termination)
                .await
                .expect("client must terminate after the identity-cap record")
                .expect("test client completion signal must be delivered");
        });

        let limits = WsLimits::fast_with_trade_identity_limit_for_test(1);
        let config = WsConfig::with_limits(vec![market("BTC")], limits)
            .expect("test configuration is valid");
        let mut stream = WsClient::new_for_test(config, endpoint).start();
        assert!(matches!(
            timeout(Duration::from_secs(1), stream.recv())
                .await
                .expect("first trade before timeout"),
            Some(WsOutput::MarketEvent(_))
        ));
        let terminal = timeout(Duration::from_secs(1), stream.recv())
            .await
            .expect("identity terminal record before timeout")
            .expect("terminal record must precede stream closure");
        assert!(matches!(
            terminal,
            WsOutput::Terminal(super::WsTerminal::TradeIdentityLimit(limit))
                if limit.max_identities() == 1
        ));
        assert!(
            timeout(Duration::from_secs(1), stream.recv())
                .await
                .expect("stream must stop at its exact identity cap")
                .is_none()
        );
        client_terminated
            .send(())
            .expect("server must wait for client termination");
        server.await.expect("server task must complete");
    }

    #[tokio::test]
    async fn invalid_l2_is_observable_and_cannot_close_or_replace_market_state() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback WebSocket server");
        let endpoint = format!(
            "ws://{}",
            listener
                .local_addr()
                .expect("loopback address is available")
        );
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.expect("accept client");
            let mut socket = accept_async(socket)
                .await
                .expect("accept WebSocket handshake");
            for _ in 0..3 {
                let Some(Ok(Message::Text(_))) = socket.next().await else {
                    panic!("client must subscribe before market data arrives");
                };
            }
            for levels in [
                json!([
                    [{"px": "2", "sz": "1", "n": 1}],
                    [{"px": "1", "sz": "1", "n": 1}]
                ]),
                json!([
                    [{"px": "1", "sz": "1", "n": 1}],
                    [{"px": "2", "sz": "1", "n": 1}]
                ]),
            ] {
                socket
                    .send(Message::Text(
                        json!({
                            "channel": "l2Book",
                            "data": {
                                "coin": "BTC", "time": 1_700_000_000_000_i64, "levels": levels
                            }
                        })
                        .to_string()
                        .into(),
                    ))
                    .await
                    .expect("send L2 frame");
            }
        });

        let config = WsConfig::with_limits(vec![market("BTC")], WsLimits::fast_for_test())
            .expect("test configuration is valid");
        let mut stream = WsClient::new_for_test(config, endpoint).start();
        let rejected = timeout(Duration::from_secs(1), stream.recv())
            .await
            .expect("rejection before timeout")
            .expect("stream remains open");
        assert!(matches!(
            rejected,
            WsOutput::Rejected(ref update) if update.reason() == super::RejectionReason::InvalidBook
        ));
        let valid = timeout(Duration::from_secs(1), stream.recv())
            .await
            .expect("valid same-time book before timeout")
            .expect("stream remains open");
        assert!(matches!(valid, WsOutput::MarketEvent(_)));
        stream.cancel();
        server.await.expect("server task must complete");
    }

    #[tokio::test]
    async fn failed_reconnect_keeps_the_original_gap_until_a_later_l2_snapshot() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback WebSocket server");
        let endpoint = format!(
            "ws://{}",
            listener
                .local_addr()
                .expect("loopback address is available")
        );
        let server = tokio::spawn(async move {
            for outcome in [
                Some(1_700_000_000_000_i64),
                None,
                Some(1_700_000_000_001_i64),
            ] {
                let (socket, _) = listener.accept().await.expect("accept reconnect");
                let mut socket = accept_async(socket)
                    .await
                    .expect("accept WebSocket handshake");
                for _ in 0..3 {
                    let Some(Ok(Message::Text(_))) = socket.next().await else {
                        panic!("each connection must resubscribe");
                    };
                }
                if let Some(time) = outcome {
                    socket
                        .send(Message::Text(
                            json!({
                                "channel": "l2Book",
                                "data": {
                                    "coin": "BTC", "time": time,
                                    "levels": [
                                        [{"px": "1", "sz": "1", "n": 1}],
                                        [{"px": "2", "sz": "1", "n": 1}]
                                    ]
                                }
                            })
                            .to_string()
                            .into(),
                        ))
                        .await
                        .expect("send fresh L2 snapshot");
                }
                socket
                    .send(Message::Close(None))
                    .await
                    .expect("force reconnect");
            }
        });

        let config = WsConfig::with_limits(vec![market("BTC")], WsLimits::fast_for_test())
            .expect("test configuration is valid");
        let mut stream = WsClient::new_for_test(config, endpoint).start();
        assert!(matches!(
            timeout(Duration::from_secs(1), stream.recv())
                .await
                .expect("first event before timeout"),
            Some(WsOutput::MarketEvent(_))
        ));
        let first_gap = timeout(Duration::from_secs(1), stream.recv())
            .await
            .expect("gap opening before timeout")
            .expect("stream remains open");
        let WsOutput::Gap(GapEvent::Opened(opened)) = first_gap else {
            panic!("first disconnect must open a gap");
        };
        assert_eq!(opened.generation(), 1);
        let recovered = timeout(Duration::from_secs(1), stream.recv())
            .await
            .expect("later fresh L2 before timeout")
            .expect("stream remains open");
        assert!(matches!(recovered, WsOutput::MarketEvent(_)));
        let closed = timeout(Duration::from_secs(1), stream.recv())
            .await
            .expect("matching gap closure before timeout")
            .expect("stream remains open");
        let WsOutput::Gap(GapEvent::Closed(closed)) = closed else {
            panic!("later L2 must close the original gap generation");
        };
        assert_eq!(closed.generation(), 1);
        assert_eq!(closed.reconnect_attempt(), 2);
        stream.cancel();
        server.await.expect("server task must complete");
    }
}
