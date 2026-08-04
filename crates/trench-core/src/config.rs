//! Strict, fully validated configuration for the paper-only engine.

use std::path::{Component, Path};
use std::str::FromStr;

use blake3::Hasher;
use rust_decimal::Decimal;
use serde::Deserialize;
use thiserror::Error;
use url::Url;

pub use crate::domain::RulesMode;
use crate::domain::{Bps, DomainError, Leverage, MarginMode, Usdc};

const REQUIRED_HISTORY_DAYS: u16 = 30;
const REQUIRED_COVERAGE: Decimal = Decimal::from_parts(995, 0, 0, false, 3);
const MAX_SPREAD_BPS: Decimal = Decimal::from_parts(15, 0, 0, false, 0);
const MINIMUM_NOTIONAL_USDC: Decimal = Decimal::from_parts(5_000_000, 0, 0, false, 0);
const DEPTH_PROBE_USDC: Decimal = Decimal::from_parts(500, 0, 0, false, 0);
const MINIMUM_DEPTH_MULTIPLE: Decimal = Decimal::from_parts(100, 0, 0, false, 0);
const INITIAL_EQUITY_USDC: Decimal = Decimal::from_parts(100, 0, 0, false, 0);
const MAX_PLANNED_LOSS: Decimal = Decimal::from_parts(5, 0, 0, false, 3);
const MAX_DAILY_BREAKER: Decimal = Decimal::from_parts(15, 0, 0, false, 3);
const MAX_WEEKLY_BREAKER: Decimal = Decimal::from_parts(4, 0, 0, false, 2);
const MAX_HARD_DRAWDOWN: Decimal = Decimal::from_parts(8, 0, 0, false, 2);
const MAX_MARGIN_FRACTION: Decimal = Decimal::from_parts(25, 0, 0, false, 2);
const MINIMUM_FEE_BPS: Decimal = Decimal::from_parts(75, 0, 0, false, 1);
const INFO_URL: &str = "https://api.hyperliquid.xyz/info";
const WEBSOCKET_URL: &str = "wss://api.hyperliquid.xyz/ws";
const ARCHIVE_URL: &str = "https://hyperliquid-archive.s3.amazonaws.com/";

/// A TOML decoding, domain conversion, or frozen-gate validation error.
#[derive(Debug, Error)]
pub enum ConfigError {
    /// The TOML shape or a strict field type was invalid.
    ///
    /// Decoder details are intentionally discarded because `toml` errors retain
    /// the complete input and can expose secret-bearing unknown fields through
    /// their display, debug, and source representations.
    #[error("invalid paper configuration TOML")]
    Toml,
    /// A checked domain value was invalid.
    #[error(transparent)]
    Domain(#[from] DomainError),
    /// A quoted decimal field was malformed.
    #[error("configuration field `{field}` must be an exact decimal string")]
    InvalidDecimal {
        /// The invalid TOML field.
        field: &'static str,
    },
    /// A value weakened or contradicted a frozen paper-engine gate.
    #[error("configuration field `{field}` {requirement}")]
    InvalidValue {
        /// The invalid TOML field.
        field: &'static str,
        /// The invariant required by the approved design.
        requirement: &'static str,
    },
}

/// Validated public market-data endpoints.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct EndpointsConfig {
    info_url: String,
    websocket_url: String,
    archive_url: String,
}

impl EndpointsConfig {
    /// Returns the public HTTPS info endpoint.
    #[must_use]
    pub fn info_url(&self) -> &str {
        &self.info_url
    }

    /// Returns the public WSS market-feed endpoint.
    #[must_use]
    pub fn websocket_url(&self) -> &str {
        &self.websocket_url
    }

    /// Returns the public HTTPS archive endpoint.
    #[must_use]
    pub fn archive_url(&self) -> &str {
        &self.archive_url
    }
}

/// Validated local persistence paths, without performing filesystem access.
///
/// Relative paths resolve from the process working directory. Unix absolute
/// paths are also accepted. Parent traversal, control characters, URL-like
/// paths, and Windows path syntax are rejected on every platform.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct StorageConfig {
    sqlite_path: String,
    parquet_path: String,
}

/// Validated local daemon control endpoint.
///
/// This is a Unix-domain socket path only; it never describes a public TCP
/// listener or an account/action endpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct RuntimeConfig {
    admin_socket_path: String,
}

impl RuntimeConfig {
    /// Returns the configured local Unix admin-socket path.
    #[must_use]
    pub fn admin_socket_path(&self) -> &str {
        &self.admin_socket_path
    }
}

impl StorageConfig {
    /// Returns the configured local SQLite path.
    ///
    /// Relative paths resolve from the process working directory.
    #[must_use]
    pub fn sqlite_path(&self) -> &str {
        &self.sqlite_path
    }

    /// Returns the configured local Parquet path.
    ///
    /// Relative paths resolve from the process working directory.
    #[must_use]
    pub fn parquet_path(&self) -> &str {
        &self.parquet_path
    }
}

/// Frozen feed-quality and dynamic-universe gates.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct FeedConfig {
    universe_refresh_seconds: u32,
    required_history_days: u16,
    required_bar_coverage: Decimal,
    coverage_window_days: u16,
    max_effective_spread: Bps,
    minimum_daily_notional: Usdc,
    depth_probe_notional: Usdc,
    minimum_depth_multiple: Decimal,
    tradeable_market_count: u16,
    warm_buffer_market_count: u16,
}

impl FeedConfig {
    /// Returns the universe refresh cadence in seconds.
    #[must_use]
    pub const fn universe_refresh_seconds(&self) -> u32 {
        self.universe_refresh_seconds
    }

    /// Returns the required history in calendar days.
    #[must_use]
    pub const fn required_history_days(&self) -> u16 {
        self.required_history_days
    }

    /// Returns the exact required-bar coverage fraction.
    #[must_use]
    pub const fn required_bar_coverage(&self) -> Decimal {
        self.required_bar_coverage
    }

    /// Returns the trailing coverage window in calendar days.
    #[must_use]
    pub const fn coverage_window_days(&self) -> u16 {
        self.coverage_window_days
    }

    /// Returns the maximum effective spread.
    #[must_use]
    pub const fn max_effective_spread(&self) -> Bps {
        self.max_effective_spread
    }

    /// Returns the minimum trailing 24-hour notional.
    #[must_use]
    pub const fn minimum_daily_notional(&self) -> Usdc {
        self.minimum_daily_notional
    }

    /// Returns the fixed depth-probe notional.
    #[must_use]
    pub const fn depth_probe_notional(&self) -> Usdc {
        self.depth_probe_notional
    }

    /// Returns the required executable-depth multiple.
    #[must_use]
    pub const fn minimum_depth_multiple(&self) -> Decimal {
        self.minimum_depth_multiple
    }

    /// Returns the exact number of tradeable markets.
    #[must_use]
    pub const fn tradeable_market_count(&self) -> u16 {
        self.tradeable_market_count
    }

    /// Returns the exact number of warm-buffer markets.
    #[must_use]
    pub const fn warm_buffer_market_count(&self) -> u16 {
        self.warm_buffer_market_count
    }
}

/// Validated risk limits for each independent paper ledger.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct RiskConfig {
    initial_equity: Usdc,
    max_planned_loss_fraction: Decimal,
    daily_loss_breaker_fraction: Decimal,
    weekly_loss_breaker_fraction: Decimal,
    hard_drawdown_fraction: Decimal,
    consecutive_loss_limit: u8,
    cooldown_hours: u16,
    max_entries_per_day: u8,
    max_open_positions: u8,
    minimum_leverage: Leverage,
    max_leverage: Leverage,
    max_margin_fraction: Decimal,
    fixed_fee_per_side: Bps,
}

impl RiskConfig {
    /// Returns the synthetic starting equity, fixed at 100 USDC.
    #[must_use]
    pub const fn initial_equity(&self) -> Usdc {
        self.initial_equity
    }

    /// Returns the maximum planned-loss fraction per trade.
    #[must_use]
    pub const fn max_planned_loss_fraction(&self) -> Decimal {
        self.max_planned_loss_fraction
    }

    /// Returns the daily loss-breaker fraction.
    #[must_use]
    pub const fn daily_loss_breaker_fraction(&self) -> Decimal {
        self.daily_loss_breaker_fraction
    }

    /// Returns the weekly loss-breaker fraction.
    #[must_use]
    pub const fn weekly_loss_breaker_fraction(&self) -> Decimal {
        self.weekly_loss_breaker_fraction
    }

    /// Returns the hard drawdown fraction.
    #[must_use]
    pub const fn hard_drawdown_fraction(&self) -> Decimal {
        self.hard_drawdown_fraction
    }

    /// Returns the number of consecutive losses that triggers cooldown.
    #[must_use]
    pub const fn consecutive_loss_limit(&self) -> u8 {
        self.consecutive_loss_limit
    }

    /// Returns the cooldown duration in hours.
    #[must_use]
    pub const fn cooldown_hours(&self) -> u16 {
        self.cooldown_hours
    }

    /// Returns the entry limit per UTC day.
    #[must_use]
    pub const fn max_entries_per_day(&self) -> u8 {
        self.max_entries_per_day
    }

    /// Returns the exact open-position limit for each ledger.
    #[must_use]
    pub const fn max_open_positions(&self) -> u8 {
        self.max_open_positions
    }

    /// Returns the fixed minimum leverage.
    #[must_use]
    pub const fn minimum_leverage(&self) -> Leverage {
        self.minimum_leverage
    }

    /// Returns the highest permitted leverage, capped at 20x.
    #[must_use]
    pub const fn max_leverage(&self) -> Leverage {
        self.max_leverage
    }

    /// Returns the maximum margin fraction.
    #[must_use]
    pub const fn max_margin_fraction(&self) -> Decimal {
        self.max_margin_fraction
    }

    /// Returns the fixed fee charged per filled side.
    #[must_use]
    pub const fn fixed_fee_per_side(&self) -> Bps {
        self.fixed_fee_per_side
    }
}

/// Validated rules-mode state.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum RulesConfig {
    /// Collect data without artifact-driven decisions.
    CollectOnly,
    /// Run a complete validated artifact and its validation report.
    Active(ActiveRulesConfig),
}

impl RulesConfig {
    /// Returns whether this is collect-only or active state.
    #[must_use]
    pub const fn mode(&self) -> RulesMode {
        match self {
            Self::CollectOnly => RulesMode::CollectOnly,
            Self::Active(_) => RulesMode::Active,
        }
    }

    /// Returns the complete artifact state when active.
    #[must_use]
    pub const fn active(&self) -> Option<&ActiveRulesConfig> {
        match self {
            Self::CollectOnly => None,
            Self::Active(active) => Some(active),
        }
    }
}

/// Complete artifact references required by active rules mode.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct ActiveRulesConfig {
    artifact_file: String,
    artifact_digest: String,
    validation_report_file: String,
    validation_report_digest: String,
}

impl ActiveRulesConfig {
    /// Returns the active artifact filename.
    #[must_use]
    pub fn artifact_file(&self) -> &str {
        &self.artifact_file
    }

    /// Returns the declared lowercase BLAKE3 artifact digest.
    #[must_use]
    pub fn artifact_digest(&self) -> &str {
        &self.artifact_digest
    }

    /// Returns the validation-report filename.
    #[must_use]
    pub fn validation_report_file(&self) -> &str {
        &self.validation_report_file
    }

    /// Returns the declared lowercase BLAKE3 report digest.
    #[must_use]
    pub fn validation_report_digest(&self) -> &str {
        &self.validation_report_digest
    }
}

/// Fully validated paper-engine configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct PaperConfig {
    endpoints: EndpointsConfig,
    storage: StorageConfig,
    runtime: RuntimeConfig,
    feed: FeedConfig,
    risk: RiskConfig,
    margin_mode: MarginMode,
    rules: RulesConfig,
}

impl PaperConfig {
    /// Returns the approved public read-only endpoints.
    #[must_use]
    pub const fn endpoints(&self) -> &EndpointsConfig {
        &self.endpoints
    }

    /// Returns the validated local persistence paths.
    #[must_use]
    pub const fn storage(&self) -> &StorageConfig {
        &self.storage
    }

    /// Returns the validated daemon-local runtime endpoint.
    #[must_use]
    pub const fn runtime(&self) -> &RuntimeConfig {
        &self.runtime
    }

    /// Returns the frozen feed and universe gates.
    #[must_use]
    pub const fn feed(&self) -> &FeedConfig {
        &self.feed
    }

    /// Returns the per-ledger risk gates.
    #[must_use]
    pub const fn risk(&self) -> &RiskConfig {
        &self.risk
    }

    /// Returns the only supported paper margin mode.
    #[must_use]
    pub const fn margin_mode(&self) -> MarginMode {
        self.margin_mode
    }

    /// Returns the validated rules state.
    #[must_use]
    pub const fn rules(&self) -> &RulesConfig {
        &self.rules
    }

    /// Parses strict nested TOML and returns only fully validated state.
    ///
    /// Decimal and fractional fields must be quoted so their base-10 values are
    /// parsed without a binary floating-point intermediate.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError`] for malformed TOML, unknown fields, invalid
    /// domain values, unsafe endpoints or paths, and weakened frozen gates.
    pub fn from_toml(input: &str) -> Result<Self, ConfigError> {
        let raw: RawPaperConfig = toml::from_str(input).map_err(|_| ConfigError::Toml)?;
        let config = Self::try_from(raw)?;
        config.validate()?;
        Ok(config)
    }

    /// Returns the canonical digest of the paper-engine configuration excluding
    /// the entire rules reference table.
    ///
    /// The rule selection is immutable artifact data, while the active TOML
    /// table only names the artifact/report that prove it. Excluding that
    /// table prevents a self-referential hash cycle between a report digest and
    /// the config field that names it, and ensures collect-only and active
    /// configurations with the same frozen engine gates share research
    /// provenance.
    pub fn research_digest(input: &str) -> Result<String, ConfigError> {
        Self::from_toml(input)?;
        let mut document: toml::Table = toml::from_str(input).map_err(|_| ConfigError::Toml)?;
        document.remove("rules");
        let canonical = toml::to_string(&document).map_err(|_| ConfigError::Toml)?;
        let mut hasher = Hasher::new_derive_key("trench.paper-research-config.v1");
        hasher.update(canonical.as_bytes());
        Ok(format!("b3:{}", hasher.finalize().to_hex()))
    }

    /// Revalidates all frozen gates represented by this configuration.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError`] if any endpoint, path, feed, risk, margin, or
    /// rules-artifact invariant is violated.
    pub fn validate(&self) -> Result<(), ConfigError> {
        validate_approved_url(&self.endpoints.info_url, INFO_URL, "endpoints.info_url")?;
        validate_approved_url(
            &self.endpoints.websocket_url,
            WEBSOCKET_URL,
            "endpoints.websocket_url",
        )?;
        validate_approved_url(
            &self.endpoints.archive_url,
            ARCHIVE_URL,
            "endpoints.archive_url",
        )?;
        validate_local_path(&self.storage.sqlite_path, "storage.sqlite_path")?;
        validate_local_path(&self.storage.parquet_path, "storage.parquet_path")?;
        validate_unix_socket_path(&self.runtime.admin_socket_path, "runtime.admin_socket_path")?;
        validate_feed(&self.feed)?;
        validate_risk(&self.risk)?;
        validate_rules(&self.rules)?;
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawPaperConfig {
    endpoints: RawEndpointsConfig,
    storage: RawStorageConfig,
    runtime: RawRuntimeConfig,
    feeds: RawFeedConfig,
    risk: RawRiskConfig,
    rules: RawRulesConfig,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawEndpointsConfig {
    info_url: String,
    websocket_url: String,
    archive_url: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawStorageConfig {
    sqlite_path: String,
    parquet_path: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawRuntimeConfig {
    admin_socket_path: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawFeedConfig {
    universe_refresh_seconds: u32,
    required_history_days: u16,
    required_bar_coverage: String,
    coverage_window_days: u16,
    max_effective_spread_bps: String,
    minimum_daily_notional_usdc: String,
    depth_probe_notional_usdc: String,
    minimum_depth_multiple: String,
    tradeable_market_count: u16,
    warm_buffer_market_count: u16,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawRiskConfig {
    initial_equity_usdc: String,
    max_planned_loss_fraction: String,
    daily_loss_breaker_fraction: String,
    weekly_loss_breaker_fraction: String,
    hard_drawdown_fraction: String,
    consecutive_loss_limit: u8,
    cooldown_hours: u16,
    max_entries_per_day: u8,
    max_open_positions: u8,
    margin_mode: String,
    minimum_leverage: u8,
    max_leverage: u8,
    max_margin_fraction: String,
    fixed_fee_bps_per_side: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawRulesConfig {
    mode: String,
    artifact_file: Option<String>,
    artifact_digest: Option<String>,
    validation_report_file: Option<String>,
    validation_report_digest: Option<String>,
}

impl TryFrom<RawPaperConfig> for PaperConfig {
    type Error = ConfigError;

    fn try_from(raw: RawPaperConfig) -> Result<Self, Self::Error> {
        let required_bar_coverage = parse_decimal(
            &raw.feeds.required_bar_coverage,
            "feeds.required_bar_coverage",
        )?;
        let max_effective_spread = Bps::new(parse_decimal(
            &raw.feeds.max_effective_spread_bps,
            "feeds.max_effective_spread_bps",
        )?)?;
        let minimum_daily_notional = Usdc::new(parse_decimal(
            &raw.feeds.minimum_daily_notional_usdc,
            "feeds.minimum_daily_notional_usdc",
        )?)?;
        let depth_probe_notional = Usdc::new(parse_decimal(
            &raw.feeds.depth_probe_notional_usdc,
            "feeds.depth_probe_notional_usdc",
        )?)?;
        let minimum_depth_multiple = parse_decimal(
            &raw.feeds.minimum_depth_multiple,
            "feeds.minimum_depth_multiple",
        )?;

        let initial_equity = Usdc::new(parse_decimal(
            &raw.risk.initial_equity_usdc,
            "risk.initial_equity_usdc",
        )?)?;
        let max_planned_loss_fraction = parse_decimal(
            &raw.risk.max_planned_loss_fraction,
            "risk.max_planned_loss_fraction",
        )?;
        let daily_loss_breaker_fraction = parse_decimal(
            &raw.risk.daily_loss_breaker_fraction,
            "risk.daily_loss_breaker_fraction",
        )?;
        let weekly_loss_breaker_fraction = parse_decimal(
            &raw.risk.weekly_loss_breaker_fraction,
            "risk.weekly_loss_breaker_fraction",
        )?;
        let hard_drawdown_fraction = parse_decimal(
            &raw.risk.hard_drawdown_fraction,
            "risk.hard_drawdown_fraction",
        )?;
        let max_margin_fraction =
            parse_decimal(&raw.risk.max_margin_fraction, "risk.max_margin_fraction")?;
        let fixed_fee_per_side = Bps::new(parse_decimal(
            &raw.risk.fixed_fee_bps_per_side,
            "risk.fixed_fee_bps_per_side",
        )?)?;
        let rules = RulesConfig::try_from(raw.rules)?;

        Ok(Self {
            endpoints: EndpointsConfig {
                info_url: raw.endpoints.info_url,
                websocket_url: raw.endpoints.websocket_url,
                archive_url: raw.endpoints.archive_url,
            },
            storage: StorageConfig {
                sqlite_path: raw.storage.sqlite_path,
                parquet_path: raw.storage.parquet_path,
            },
            runtime: RuntimeConfig {
                admin_socket_path: raw.runtime.admin_socket_path,
            },
            feed: FeedConfig {
                universe_refresh_seconds: raw.feeds.universe_refresh_seconds,
                required_history_days: raw.feeds.required_history_days,
                required_bar_coverage,
                coverage_window_days: raw.feeds.coverage_window_days,
                max_effective_spread,
                minimum_daily_notional,
                depth_probe_notional,
                minimum_depth_multiple,
                tradeable_market_count: raw.feeds.tradeable_market_count,
                warm_buffer_market_count: raw.feeds.warm_buffer_market_count,
            },
            risk: RiskConfig {
                initial_equity,
                max_planned_loss_fraction,
                daily_loss_breaker_fraction,
                weekly_loss_breaker_fraction,
                hard_drawdown_fraction,
                consecutive_loss_limit: raw.risk.consecutive_loss_limit,
                cooldown_hours: raw.risk.cooldown_hours,
                max_entries_per_day: raw.risk.max_entries_per_day,
                max_open_positions: raw.risk.max_open_positions,
                minimum_leverage: Leverage::new(raw.risk.minimum_leverage)?,
                max_leverage: Leverage::new(raw.risk.max_leverage)?,
                max_margin_fraction,
                fixed_fee_per_side,
            },
            margin_mode: MarginMode::from_str(&raw.risk.margin_mode)?,
            rules,
        })
    }
}

impl TryFrom<RawRulesConfig> for RulesConfig {
    type Error = ConfigError;

    fn try_from(raw: RawRulesConfig) -> Result<Self, Self::Error> {
        let RawRulesConfig {
            mode,
            artifact_file,
            artifact_digest,
            validation_report_file,
            validation_report_digest,
        } = raw;

        match RulesMode::from_str(&mode)? {
            RulesMode::CollectOnly => {
                if [
                    artifact_file.as_ref(),
                    artifact_digest.as_ref(),
                    validation_report_file.as_ref(),
                    validation_report_digest.as_ref(),
                ]
                .iter()
                .any(|value| value.is_some())
                {
                    return Err(invalid(
                        "rules",
                        "must omit all artifact fields in collect_only mode",
                    ));
                }
                Ok(Self::CollectOnly)
            }
            RulesMode::Active => {
                let active = ActiveRulesConfig {
                    artifact_file: required_owned(artifact_file, "rules.artifact_file")?,
                    artifact_digest: required_owned(artifact_digest, "rules.artifact_digest")?,
                    validation_report_file: required_owned(
                        validation_report_file,
                        "rules.validation_report_file",
                    )?,
                    validation_report_digest: required_owned(
                        validation_report_digest,
                        "rules.validation_report_digest",
                    )?,
                };
                validate_active_rules(&active)?;
                Ok(Self::Active(active))
            }
        }
    }
}

fn parse_decimal(value: &str, field: &'static str) -> Result<Decimal, ConfigError> {
    Decimal::from_str_exact(value).map_err(|_| ConfigError::InvalidDecimal { field })
}

fn invalid(field: &'static str, requirement: &'static str) -> ConfigError {
    ConfigError::InvalidValue { field, requirement }
}

fn validate_feed(feed: &FeedConfig) -> Result<(), ConfigError> {
    require_equal(
        feed.universe_refresh_seconds,
        3_600,
        "feeds.universe_refresh_seconds",
        "must equal 3600",
    )?;
    require_equal(
        feed.required_history_days,
        REQUIRED_HISTORY_DAYS,
        "feeds.required_history_days",
        "must equal 30 calendar days",
    )?;
    require_equal(
        feed.required_bar_coverage,
        REQUIRED_COVERAGE,
        "feeds.required_bar_coverage",
        "must equal 0.995",
    )?;
    require_equal(
        feed.coverage_window_days,
        7,
        "feeds.coverage_window_days",
        "must equal 7 calendar days",
    )?;
    require_equal(
        feed.max_effective_spread.value(),
        MAX_SPREAD_BPS,
        "feeds.max_effective_spread_bps",
        "must equal 15 bps",
    )?;
    require_equal(
        feed.minimum_daily_notional.value(),
        MINIMUM_NOTIONAL_USDC,
        "feeds.minimum_daily_notional_usdc",
        "must equal 5000000 USDC",
    )?;
    require_equal(
        feed.depth_probe_notional.value(),
        DEPTH_PROBE_USDC,
        "feeds.depth_probe_notional_usdc",
        "must equal 500 USDC",
    )?;
    require_equal(
        feed.minimum_depth_multiple,
        MINIMUM_DEPTH_MULTIPLE,
        "feeds.minimum_depth_multiple",
        "must equal 100",
    )?;
    require_equal(
        feed.tradeable_market_count,
        20,
        "feeds.tradeable_market_count",
        "must equal 20",
    )?;
    require_equal(
        feed.warm_buffer_market_count,
        10,
        "feeds.warm_buffer_market_count",
        "must equal 10",
    )
}

fn validate_risk(risk: &RiskConfig) -> Result<(), ConfigError> {
    require_equal(
        risk.initial_equity.value(),
        INITIAL_EQUITY_USDC,
        "risk.initial_equity_usdc",
        "must equal 100 synthetic USDC",
    )?;
    require_equal(
        risk.max_planned_loss_fraction,
        MAX_PLANNED_LOSS,
        "risk.max_planned_loss_fraction",
        "must equal 0.005",
    )?;
    require_equal(
        risk.daily_loss_breaker_fraction,
        MAX_DAILY_BREAKER,
        "risk.daily_loss_breaker_fraction",
        "must equal 0.015",
    )?;
    require_equal(
        risk.weekly_loss_breaker_fraction,
        MAX_WEEKLY_BREAKER,
        "risk.weekly_loss_breaker_fraction",
        "must equal 0.04",
    )?;
    require_equal(
        risk.hard_drawdown_fraction,
        MAX_HARD_DRAWDOWN,
        "risk.hard_drawdown_fraction",
        "must equal 0.08",
    )?;
    require_equal(
        risk.consecutive_loss_limit,
        3,
        "risk.consecutive_loss_limit",
        "must equal 3",
    )?;
    require_equal(
        risk.cooldown_hours,
        12,
        "risk.cooldown_hours",
        "must equal 12",
    )?;
    require_equal(
        risk.max_entries_per_day,
        6,
        "risk.max_entries_per_day",
        "must equal 6",
    )?;
    require_equal(
        risk.max_open_positions,
        1,
        "risk.max_open_positions",
        "must equal 1",
    )?;
    require_equal(
        risk.minimum_leverage.value(),
        5,
        "risk.minimum_leverage",
        "must equal 5",
    )?;
    require_equal(
        risk.max_leverage.value(),
        20,
        "risk.max_leverage",
        "must equal 20",
    )?;
    require_equal(
        risk.max_margin_fraction,
        MAX_MARGIN_FRACTION,
        "risk.max_margin_fraction",
        "must equal 0.25",
    )?;
    if risk.fixed_fee_per_side.value() < MINIMUM_FEE_BPS {
        return Err(invalid(
            "risk.fixed_fee_bps_per_side",
            "must be at least 7.5 bps",
        ));
    }
    Ok(())
}

fn require_equal<T: PartialEq>(
    actual: T,
    expected: T,
    field: &'static str,
    requirement: &'static str,
) -> Result<(), ConfigError> {
    if actual != expected {
        return Err(invalid(field, requirement));
    }
    Ok(())
}

fn validate_rules(rules: &RulesConfig) -> Result<(), ConfigError> {
    match rules {
        RulesConfig::CollectOnly => Ok(()),
        RulesConfig::Active(active) => validate_active_rules(active),
    }
}

fn validate_active_rules(active: &ActiveRulesConfig) -> Result<(), ConfigError> {
    validate_filename(&active.artifact_file, "rules.artifact_file")?;
    validate_digest(&active.artifact_digest, "rules.artifact_digest")?;
    validate_filename(
        &active.validation_report_file,
        "rules.validation_report_file",
    )?;
    validate_digest(
        &active.validation_report_digest,
        "rules.validation_report_digest",
    )
}

fn required_owned(value: Option<String>, field: &'static str) -> Result<String, ConfigError> {
    value.ok_or_else(|| invalid(field, "is required in active mode"))
}

fn validate_filename(value: &str, field: &'static str) -> Result<(), ConfigError> {
    if value.is_empty()
        || matches!(value, "." | "..")
        || value.contains(['/', '\\', ':', '\0'])
        || value.chars().any(char::is_control)
        || Path::new(value).is_absolute()
        || Path::new(value).components().count() != 1
    {
        return Err(invalid(field, "must be a single plain filename component"));
    }
    Ok(())
}

fn validate_digest(value: &str, field: &'static str) -> Result<(), ConfigError> {
    let Some(hex) = value.strip_prefix("b3:") else {
        return Err(invalid(
            field,
            "must use the b3: prefix and lowercase BLAKE3 hex",
        ));
    };
    if hex.len() != 64
        || !hex
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(invalid(
            field,
            "must use the b3: prefix and 64 lowercase hex characters",
        ));
    }
    Ok(())
}

fn validate_approved_url(
    value: &str,
    approved: &'static str,
    field: &'static str,
) -> Result<(), ConfigError> {
    let parsed = Url::parse(value)
        .map_err(|_| invalid(field, "must be a canonical approved read-only endpoint"))?;
    if value != approved || parsed.as_str() != approved {
        return Err(invalid(
            field,
            "must be a canonical approved read-only endpoint",
        ));
    }
    Ok(())
}

fn validate_local_path(value: &str, field: &'static str) -> Result<(), ConfigError> {
    if value.is_empty()
        || value.chars().any(char::is_control)
        || value.contains(['\\', ':'])
        || value.starts_with("//")
        || Path::new(value)
            .components()
            .any(|component| component == Component::ParentDir)
    {
        return Err(invalid(
            field,
            "must be a relative or Unix absolute local path without controls, Windows syntax, or parent traversal",
        ));
    }
    Ok(())
}

fn validate_unix_socket_path(value: &str, field: &'static str) -> Result<(), ConfigError> {
    validate_local_path(value, field)?;
    let path = Path::new(value);
    if !path.is_absolute()
        || path.file_name().is_none()
        || !value.ends_with(".sock")
        || value.len() > 107
    {
        return Err(invalid(
            field,
            "must be an absolute Unix socket path ending in .sock and fitting sockaddr_un",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use rust_decimal_macros::dec;

    use super::{ConfigError, PaperConfig, RulesMode, validate_local_path};
    use crate::domain::{Leverage, MarginMode, Usdc};

    const EXAMPLE: &str = include_str!("../../../config/paper.example.toml");
    const DIGEST: &str = "b3:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    fn replace_once(source: &str, old: &str, new: &str) -> String {
        assert!(source.contains(old), "fixture did not contain {old:?}");
        source.replacen(old, new, 1)
    }

    fn active_config(artifact_file: &str, validation_report_file: &str) -> String {
        replace_once(
            EXAMPLE,
            "mode = \"collect_only\"",
            &format!(
                "mode = \"active\"\nartifact_file = \"{artifact_file}\"\nartifact_digest = \"{DIGEST}\"\nvalidation_report_file = \"{validation_report_file}\"\nvalidation_report_digest = \"{DIGEST}\""
            ),
        )
    }

    fn rendered_error_chain(error: &ConfigError) -> String {
        let mut rendered = format!("{error}\n{error:?}");
        let mut source = error.source();
        while let Some(current) = source {
            rendered.push_str(&format!("\n{current}\n{current:?}"));
            source = current.source();
        }
        rendered
    }

    #[test]
    fn toml_errors_do_not_expose_unknown_secret_bearing_input() {
        let sentinel = "sk-adversarial-do-not-log-4A3E7160";
        let input = replace_once(
            EXAMPLE,
            "[rules]",
            &format!("[rules]\nunknown_secret = \"{sentinel}\""),
        );

        let error = PaperConfig::from_toml(&input).expect_err("unknown fields must be rejected");
        let rendered = rendered_error_chain(&error);

        assert!(!rendered.contains(sentinel), "sentinel leaked: {rendered}");
        assert!(
            !rendered.contains("unknown_secret"),
            "secret-bearing input line leaked: {rendered}"
        );
    }

    #[test]
    fn example_config_parses_as_collect_only() -> Result<(), Box<dyn Error>> {
        let cfg = PaperConfig::from_toml(EXAMPLE)?;

        assert_eq!(
            cfg.endpoints().info_url(),
            "https://api.hyperliquid.xyz/info"
        );
        assert_eq!(
            cfg.endpoints().websocket_url(),
            "wss://api.hyperliquid.xyz/ws"
        );
        assert_eq!(
            cfg.endpoints().archive_url(),
            "https://hyperliquid-archive.s3.amazonaws.com/"
        );
        assert_eq!(cfg.storage().sqlite_path(), "state/trench.sqlite");
        assert_eq!(cfg.feed().required_history_days(), 30);
        assert_eq!(cfg.risk().initial_equity(), Usdc::new(dec!(100))?);
        assert_eq!(cfg.risk().max_leverage(), Leverage::new(20)?);
        assert_eq!(cfg.margin_mode(), MarginMode::Isolated);
        assert_eq!(cfg.rules().mode(), RulesMode::CollectOnly);
        assert!(cfg.rules().active().is_none());
        assert_eq!(
            cfg.runtime().admin_socket_path(),
            "/run/trench/trenchd.sock"
        );
        Ok(())
    }

    #[test]
    fn runtime_admin_socket_must_be_a_bounded_absolute_unix_path() {
        let cases = vec![
            "runtime/trenchd.sock".to_owned(),
            "/run/trench/trenchd".to_owned(),
            "/run/trench/../trenchd.sock".to_owned(),
            "/run/trench/\u{0}trenchd.sock".to_owned(),
            format!("/run/{}.sock", "a".repeat(108)),
        ];
        for socket in cases {
            let input = replace_once(EXAMPLE, "/run/trench/trenchd.sock", &socket);
            assert!(
                PaperConfig::from_toml(&input).is_err(),
                "accepted {socket:?}"
            );
        }
    }

    #[test]
    fn config_rejects_cross_margin() {
        let input = replace_once(
            EXAMPLE,
            "margin_mode = \"isolated\"",
            "margin_mode = \"cross\"",
        );

        assert!(PaperConfig::from_toml(&input).is_err());
    }

    #[test]
    fn config_rejects_fee_below_seven_and_a_half_bps() {
        let input = replace_once(
            EXAMPLE,
            "fixed_fee_bps_per_side = \"7.5\"",
            "fixed_fee_bps_per_side = \"7.49\"",
        );

        assert!(PaperConfig::from_toml(&input).is_err());
    }

    #[test]
    fn config_accepts_more_conservative_fee() {
        let input = replace_once(
            EXAMPLE,
            "fixed_fee_bps_per_side = \"7.5\"",
            "fixed_fee_bps_per_side = \"8\"",
        );

        assert!(PaperConfig::from_toml(&input).is_ok());
    }

    #[test]
    fn config_rejects_risk_values_above_approved_limits() {
        let cases = [
            (
                "max_planned_loss_fraction = \"0.005\"",
                "max_planned_loss_fraction = \"0.0051\"",
            ),
            (
                "daily_loss_breaker_fraction = \"0.015\"",
                "daily_loss_breaker_fraction = \"0.0151\"",
            ),
            (
                "weekly_loss_breaker_fraction = \"0.04\"",
                "weekly_loss_breaker_fraction = \"0.041\"",
            ),
            (
                "hard_drawdown_fraction = \"0.08\"",
                "hard_drawdown_fraction = \"0.081\"",
            ),
            ("consecutive_loss_limit = 3", "consecutive_loss_limit = 4"),
            ("max_entries_per_day = 6", "max_entries_per_day = 7"),
            ("max_leverage = 20", "max_leverage = 21"),
            (
                "max_margin_fraction = \"0.25\"",
                "max_margin_fraction = \"0.251\"",
            ),
        ];

        for (old, new) in cases {
            let input = replace_once(EXAMPLE, old, new);
            assert!(PaperConfig::from_toml(&input).is_err(), "accepted {new}");
        }
    }

    #[test]
    fn config_rejects_cooldown_shorter_than_twelve_hours() {
        let input = replace_once(EXAMPLE, "cooldown_hours = 12", "cooldown_hours = 11");

        assert!(PaperConfig::from_toml(&input).is_err());
    }

    #[test]
    fn config_rejects_non_frozen_risk_values() {
        let cases = [
            (
                "initial_equity_usdc = \"100.00\"",
                "initial_equity_usdc = \"99\"",
            ),
            ("max_open_positions = 1", "max_open_positions = 0"),
            ("minimum_leverage = 5", "minimum_leverage = 6"),
        ];

        for (old, new) in cases {
            let input = replace_once(EXAMPLE, old, new);
            assert!(PaperConfig::from_toml(&input).is_err(), "accepted {new}");
        }
    }

    #[test]
    fn config_rejects_all_frozen_risk_deviations() {
        let cases = [
            (
                "initial_equity_usdc = \"100.00\"",
                "initial_equity_usdc = \"99.99\"",
            ),
            (
                "initial_equity_usdc = \"100.00\"",
                "initial_equity_usdc = \"100.01\"",
            ),
            (
                "max_planned_loss_fraction = \"0.005\"",
                "max_planned_loss_fraction = \"0.0049\"",
            ),
            (
                "max_planned_loss_fraction = \"0.005\"",
                "max_planned_loss_fraction = \"0.0051\"",
            ),
            (
                "daily_loss_breaker_fraction = \"0.015\"",
                "daily_loss_breaker_fraction = \"0.0149\"",
            ),
            (
                "daily_loss_breaker_fraction = \"0.015\"",
                "daily_loss_breaker_fraction = \"0.0151\"",
            ),
            (
                "weekly_loss_breaker_fraction = \"0.04\"",
                "weekly_loss_breaker_fraction = \"0.039\"",
            ),
            (
                "weekly_loss_breaker_fraction = \"0.04\"",
                "weekly_loss_breaker_fraction = \"0.041\"",
            ),
            (
                "hard_drawdown_fraction = \"0.08\"",
                "hard_drawdown_fraction = \"0.079\"",
            ),
            (
                "hard_drawdown_fraction = \"0.08\"",
                "hard_drawdown_fraction = \"0.081\"",
            ),
            ("consecutive_loss_limit = 3", "consecutive_loss_limit = 2"),
            ("consecutive_loss_limit = 3", "consecutive_loss_limit = 4"),
            ("cooldown_hours = 12", "cooldown_hours = 11"),
            ("cooldown_hours = 12", "cooldown_hours = 13"),
            ("max_entries_per_day = 6", "max_entries_per_day = 5"),
            ("max_entries_per_day = 6", "max_entries_per_day = 7"),
            ("max_open_positions = 1", "max_open_positions = 0"),
            ("max_open_positions = 1", "max_open_positions = 2"),
            ("minimum_leverage = 5", "minimum_leverage = 4"),
            ("minimum_leverage = 5", "minimum_leverage = 6"),
            ("max_leverage = 20", "max_leverage = 19"),
            ("max_leverage = 20", "max_leverage = 21"),
            (
                "max_margin_fraction = \"0.25\"",
                "max_margin_fraction = \"0.249\"",
            ),
            (
                "max_margin_fraction = \"0.25\"",
                "max_margin_fraction = \"0.251\"",
            ),
        ];

        let accepted: Vec<_> = cases
            .into_iter()
            .filter_map(|(old, new)| {
                PaperConfig::from_toml(&replace_once(EXAMPLE, old, new))
                    .is_ok()
                    .then_some(new)
            })
            .collect();

        assert!(
            accepted.is_empty(),
            "accepted frozen risk deviations: {}",
            accepted.join(", ")
        );
    }

    #[test]
    fn config_rejects_changes_to_frozen_feed_values() {
        let cases = [
            (
                "universe_refresh_seconds = 3600",
                "universe_refresh_seconds = 3599",
            ),
            ("required_history_days = 30", "required_history_days = 29"),
            (
                "required_bar_coverage = \"0.995\"",
                "required_bar_coverage = \"0.994\"",
            ),
            ("coverage_window_days = 7", "coverage_window_days = 6"),
            (
                "max_effective_spread_bps = \"15\"",
                "max_effective_spread_bps = \"16\"",
            ),
            (
                "minimum_daily_notional_usdc = \"5000000\"",
                "minimum_daily_notional_usdc = \"4999999\"",
            ),
            (
                "depth_probe_notional_usdc = \"500\"",
                "depth_probe_notional_usdc = \"499\"",
            ),
            (
                "minimum_depth_multiple = \"100\"",
                "minimum_depth_multiple = \"99\"",
            ),
            ("tradeable_market_count = 20", "tradeable_market_count = 19"),
            (
                "warm_buffer_market_count = 10",
                "warm_buffer_market_count = 9",
            ),
        ];

        for (old, new) in cases {
            let input = replace_once(EXAMPLE, old, new);
            assert!(PaperConfig::from_toml(&input).is_err(), "accepted {new}");
        }
    }

    #[test]
    fn config_rejects_unknown_fields_at_every_level() {
        let cases = [
            format!("{EXAMPLE}\nunknown = true\n"),
            replace_once(EXAMPLE, "[feeds]", "[feeds]\nunknown = true"),
            replace_once(EXAMPLE, "[risk]", "[risk]\nunknown = true"),
            replace_once(EXAMPLE, "[rules]", "[rules]\nunknown = true"),
        ];

        for input in cases {
            assert!(PaperConfig::from_toml(&input).is_err());
        }
    }

    #[test]
    fn config_rejects_strategy_values_outside_the_artifact() {
        for field in ["threshold", "atr_floor", "take_profit"] {
            let input = replace_once(EXAMPLE, "[rules]", &format!("[rules]\n{field} = \"1\""));
            assert!(PaperConfig::from_toml(&input).is_err(), "accepted {field}");
        }
    }

    #[test]
    fn active_rules_require_all_four_artifact_fields() {
        let fields = [
            "artifact_file = \"rules.toml\"\n",
            &format!("artifact_digest = \"{DIGEST}\"\n"),
            "validation_report_file = \"rules-report.toml\"\n",
            &format!("validation_report_digest = \"{DIGEST}\""),
        ];
        let complete = active_config("rules.toml", "rules-report.toml");

        for field in fields {
            let input = complete.replacen(field, "", 1);
            assert!(
                PaperConfig::from_toml(&input).is_err(),
                "accepted missing {field:?}"
            );
        }
    }

    #[test]
    fn collect_only_rules_reject_artifact_fields() {
        let input = replace_once(
            EXAMPLE,
            "mode = \"collect_only\"",
            "mode = \"collect_only\"\nartifact_file = \"rules.toml\"",
        );

        assert!(PaperConfig::from_toml(&input).is_err());
    }

    #[test]
    fn research_digest_excludes_artifact_references_but_commits_frozen_engine_gates() {
        let active = active_config("rules-artifact.json", "rules-validation.json");
        assert_eq!(
            PaperConfig::research_digest(EXAMPLE).expect("collect digest"),
            PaperConfig::research_digest(&active).expect("active digest"),
        );
        let altered_storage = replace_once(
            EXAMPLE,
            "sqlite_path = \"state/trench.sqlite\"",
            "sqlite_path = \"state/other.sqlite\"",
        );
        assert_ne!(
            PaperConfig::research_digest(EXAMPLE).expect("original digest"),
            PaperConfig::research_digest(&altered_storage).expect("changed digest"),
        );
    }

    #[test]
    fn active_rules_expose_complete_validated_artifact_state() {
        let cfg = PaperConfig::from_toml(&active_config("rules.toml", "rules-report.toml"))
            .expect("complete active state must parse");
        let active = cfg
            .rules()
            .active()
            .expect("active mode must carry artifact state");

        assert_eq!(cfg.rules().mode(), RulesMode::Active);
        assert_eq!(active.artifact_file(), "rules.toml");
        assert_eq!(active.artifact_digest(), DIGEST);
        assert_eq!(active.validation_report_file(), "rules-report.toml");
        assert_eq!(active.validation_report_digest(), DIGEST);
    }

    #[test]
    fn active_rules_reject_non_component_filenames() {
        for filename in [
            "",
            ".",
            "..",
            "/rules.toml",
            "dir/rules.toml",
            "dir\\rules.toml",
            "C:rules.toml",
            "C:\\rules.toml",
            "rules\0.toml",
        ] {
            let input = active_config(filename, "report.toml");
            assert!(
                PaperConfig::from_toml(&input).is_err(),
                "accepted {filename:?}"
            );
        }
    }

    #[test]
    fn active_rules_reject_invalid_blake3_digests() {
        let cases = [
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            "b3:0123456789ABCDEF0123456789abcdef0123456789abcdef0123456789abcdef",
            "b3:0123",
            "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        ];

        for digest in cases {
            let input = active_config("rules.toml", "report.toml").replacen(DIGEST, digest, 1);
            assert!(PaperConfig::from_toml(&input).is_err(), "accepted {digest}");
        }
    }

    #[test]
    fn public_endpoints_reject_unsafe_urls() {
        let unsafe_action_endpoint = format!("https://api.hyperliquid.xyz/{}/{}", "ex", "change");
        let cases = [
            (
                "https://api.hyperliquid.xyz/info",
                "http://api.hyperliquid.xyz/info",
            ),
            (
                "https://api.hyperliquid.xyz/info",
                "https://user@api.hyperliquid.xyz/info",
            ),
            (
                "https://api.hyperliquid.xyz/info",
                "https://api.hyperliquid.xyz/info?key=value",
            ),
            (
                "https://api.hyperliquid.xyz/info",
                "https://api.hyperliquid.xyz/info#fragment",
            ),
            (
                "wss://api.hyperliquid.xyz/ws",
                "ws://api.hyperliquid.xyz/ws",
            ),
            (
                "https://api.hyperliquid.xyz/info",
                unsafe_action_endpoint.as_str(),
            ),
            (
                "https://api.hyperliquid.xyz/info",
                "https://example.com/info",
            ),
            (
                "https://api.hyperliquid.xyz/info",
                "https://api.hyperliquid.xyz.evil/info",
            ),
            (
                "https://api.hyperliquid.xyz/info",
                "https://api.hyperliquid.xyz:443/info",
            ),
            (
                "https://api.hyperliquid.xyz/info",
                "https://API.HYPERLIQUID.XYZ/info",
            ),
            (
                "https://api.hyperliquid.xyz/info",
                "https://api.hyperliquid.xyz./info",
            ),
            (
                "https://api.hyperliquid.xyz/info",
                "https://api.hyperliquid.xyz/info/",
            ),
            (
                "https://api.hyperliquid.xyz/info",
                "https://api.hyperliquid.xyz/%69nfo",
            ),
            (
                "https://api.hyperliquid.xyz/info",
                "https://[api.hyperliquid.xyz]/info",
            ),
            (
                "wss://api.hyperliquid.xyz/ws",
                "wss://api.hyperliquid.xyz/info",
            ),
            (
                "wss://api.hyperliquid.xyz/ws",
                "wss://user@api.hyperliquid.xyz/ws",
            ),
            (
                "wss://api.hyperliquid.xyz/ws",
                "wss://api.hyperliquid.xyz/ws#fragment",
            ),
            ("wss://api.hyperliquid.xyz/ws", "wss://example.com/ws"),
            (
                "https://hyperliquid-archive.s3.amazonaws.com/",
                "https://example.com",
            ),
            (
                "https://hyperliquid-archive.s3.amazonaws.com/",
                "https://hyperliquid-archive.s3.amazonaws.com/private/path",
            ),
            (
                "https://hyperliquid-archive.s3.amazonaws.com/",
                concat!(
                    "https://hyperliquid-archive.s3.amazonaws.com/?",
                    "to",
                    "ken=fixture"
                ),
            ),
        ];

        for (old, new) in cases {
            let input = replace_once(EXAMPLE, old, new);
            assert!(PaperConfig::from_toml(&input).is_err(), "accepted {new}");
        }
    }

    #[test]
    fn public_endpoints_reject_non_public_or_malformed_hosts() {
        let cases = [
            "https://localhost/info",
            "https://foo.LOCALHOST/info",
            "https://127.0.0.1/info",
            "https://127.1/info",
            "https://127.0.1/info",
            "https://0177.1/info",
            "https://0x7f.1/info",
            "https://10.1/info",
            "https://169.16646145/info",
            "https://0.1/info",
            "https://224.0.0.1/info",
            "https://[fe80::1]/info",
            "https://.api.hyperliquid.xyz/info",
            "https://api..hyperliquid.xyz/info",
            "https://-api.hyperliquid.xyz/info",
        ];

        for url in cases {
            let input = replace_once(EXAMPLE, "https://api.hyperliquid.xyz/info", url);
            assert!(PaperConfig::from_toml(&input).is_err(), "accepted {url}");
        }
    }

    #[test]
    fn storage_paths_reject_empty_urls_and_parent_traversal() {
        for path in [
            "",
            "file:///tmp/trench.sqlite",
            "https://example.com/state",
            "../state/trench.sqlite",
        ] {
            let input = replace_once(EXAMPLE, "state/trench.sqlite", path);
            assert!(PaperConfig::from_toml(&input).is_err(), "accepted {path:?}");
        }
    }

    #[test]
    fn storage_paths_reject_controls_and_windows_prefix_forms() {
        let cases = [
            "state/line\nbreak.sqlite",
            "state/tab\tname.sqlite",
            "state/delete\u{7f}.sqlite",
            r"\\server\share\trench.sqlite",
            "//server/share/trench.sqlite",
            r"\\?\C:\state\trench.sqlite",
            r"\\.\pipe\trench.sqlite",
            r"C:\state\trench.sqlite",
            "C:/state/trench.sqlite",
            "C:state/trench.sqlite",
        ];

        for path in cases {
            assert!(
                validate_local_path(path, "storage.sqlite_path").is_err(),
                "accepted {path:?}"
            );
        }
    }

    #[test]
    fn storage_paths_reject_every_unicode_control_character() {
        for codepoint in 0..=0x9f {
            let Some(character) = char::from_u32(codepoint) else {
                continue;
            };
            if !character.is_control() {
                continue;
            }
            let path = format!("state/{character}trench.sqlite");
            assert!(
                validate_local_path(&path, "storage.sqlite_path").is_err(),
                "accepted U+{codepoint:04X}"
            );
        }
    }

    #[test]
    fn storage_paths_accept_relative_and_absolute_unix_paths() {
        for path in ["state/trench.sqlite", "/var/lib/trench/trench.sqlite"] {
            assert!(
                validate_local_path(path, "storage.sqlite_path").is_ok(),
                "rejected {path:?}"
            );
        }
    }
}
