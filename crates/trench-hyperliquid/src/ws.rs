//! Bounded, public-only Hyperliquid WebSocket market-data configuration.
//!
//! The runtime is added alongside the wire decoder. This module already keeps
//! its configuration constrained to the documented public connection budgets.

use std::collections::BTreeSet;
use std::time::Duration;

use thiserror::Error;
use trench_core::domain::Market;

const MAX_MARKETS: usize = 333;
const MAX_RECONNECT_ATTEMPTS: u32 = 20;
const MIN_RECONNECT_DELAY: Duration = Duration::from_secs(3);
const MAX_INBOUND_MESSAGE_BYTES: usize = 1_048_576;
const MAX_OUTPUT_CHANNEL_CAPACITY: usize = 4_096;

/// Validated, finite limits for one public market-data WebSocket connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WsLimits {
    connect_timeout: Duration,
    read_timeout: Duration,
    heartbeat_interval: Duration,
    reconnect_min_delay: Duration,
    reconnect_max_delay: Duration,
    max_reconnect_attempts: u32,
    max_inbound_message_bytes: usize,
    output_channel_capacity: usize,
}

impl WsLimits {
    /// Builds fixed limits that remain below Hyperliquid's public connection,
    /// subscription, and message-rate budgets.
    ///
    /// The reconnect floor of three seconds permits at most twenty-one new
    /// connections in a minute, including the initial connection. The caller
    /// must still use [`WsConfig`], which limits a connection to 999
    /// subscriptions (333 markets times three feeds).
    ///
    /// # Errors
    ///
    /// Returns [`WsError::InvalidConfig`] when a duration, byte bound, retry
    /// cap, or channel capacity cannot provide a bounded safe runtime.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        connect_timeout: Duration,
        read_timeout: Duration,
        heartbeat_interval: Duration,
        reconnect_min_delay: Duration,
        reconnect_max_delay: Duration,
        max_reconnect_attempts: u32,
        max_inbound_message_bytes: usize,
        output_channel_capacity: usize,
    ) -> Result<Self, WsError> {
        if connect_timeout.is_zero() {
            return Err(invalid_config("connect_timeout", "must be nonzero"));
        }
        if read_timeout.is_zero() {
            return Err(invalid_config("read_timeout", "must be nonzero"));
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
                "must be at least 3 seconds",
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

        Ok(Self {
            connect_timeout,
            read_timeout,
            heartbeat_interval,
            reconnect_min_delay,
            reconnect_max_delay,
            max_reconnect_attempts,
            max_inbound_message_bytes,
            output_channel_capacity,
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
}

impl Default for WsLimits {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(5),
            read_timeout: Duration::from_secs(45),
            heartbeat_interval: Duration::from_secs(25),
            reconnect_min_delay: MIN_RECONNECT_DELAY,
            reconnect_max_delay: Duration::from_secs(30),
            max_reconnect_attempts: 20,
            max_inbound_message_bytes: 64 * 1024,
            output_channel_capacity: 128,
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
}

impl WsClient {
    /// Creates a client that can connect only to `wss://api.hyperliquid.xyz/ws`.
    #[must_use]
    pub const fn new(config: WsConfig) -> Self {
        Self { config }
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
        /// Maximum selected markets, yielding 999 subscriptions.
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
