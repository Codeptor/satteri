use std::time::Duration;

use reqwest::header::CONTENT_TYPE;
use reqwest::redirect::Policy;
use serde::Serialize;
use thiserror::Error;
use trench_core::domain::Market;
use url::{Host, Url};

use crate::normalize::{
    decode_all_mids, decode_candles, decode_funding, decode_l2_book, decode_meta,
};
use crate::{Candle, FundingRecord, L2Book, MetaAndAssetContexts};

const OFFICIAL_INFO_URL: &str = "https://api.hyperliquid.xyz/info";
const PRODUCTION_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const PRODUCTION_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
#[cfg(debug_assertions)]
const TEST_CONNECT_TIMEOUT: Duration = Duration::from_millis(100);
#[cfg(debug_assertions)]
const TEST_REQUEST_TIMEOUT: Duration = Duration::from_millis(100);
const USER_AGENT: &str = concat!(
    "satteri-trench-hyperliquid/",
    env!("CARGO_PKG_VERSION"),
    " (paper-only; read-only info)"
);

/// Maximum accepted Hyperliquid info response body size.
pub const INFO_RESPONSE_MAX_BYTES: usize = 1_048_576;

/// A stable read-only client construction, request, or response failure.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum InfoError {
    /// The production endpoint was not the exact approved HTTPS info URL.
    #[error("invalid Hyperliquid info endpoint")]
    InvalidEndpoint,
    /// The fixed HTTP client could not be constructed.
    #[error("failed to construct Hyperliquid info client")]
    ClientBuild,
    /// A typed request violated a relationship or domain invariant.
    #[error("invalid info request field `{field}`: {requirement}")]
    InvalidRequest {
        /// The invalid request field.
        field: &'static str,
        /// The invariant required by the endpoint.
        requirement: &'static str,
    },
    /// The request exceeded its fixed deadline.
    #[error("Hyperliquid info request timed out")]
    Timeout,
    /// The transport failed without returning an HTTP response.
    #[error("Hyperliquid info transport failed")]
    Transport,
    /// The endpoint returned a non-success status.
    #[error("Hyperliquid info endpoint returned HTTP {code}")]
    HttpStatus {
        /// The returned HTTP status code.
        code: u16,
    },
    /// A successful response did not declare JSON content.
    #[error("Hyperliquid info response content type was not application/json")]
    InvalidContentType,
    /// A response exceeded the fixed byte limit.
    #[error("Hyperliquid info response exceeded {max_bytes} bytes")]
    ResponseTooLarge {
        /// The configured response limit.
        max_bytes: usize,
    },
    /// JSON was malformed, missing required fields, or had the wrong wire type.
    #[error("Hyperliquid info response could not be decoded")]
    Decode,
    /// Decoded response data violated a domain or request invariant.
    #[error("invalid info response field `{field}`: {requirement}")]
    InvalidResponse {
        /// The invalid response field.
        field: &'static str,
        /// The required invariant.
        requirement: &'static str,
    },
}

/// Inclusive explicit epoch-millisecond request bounds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimeRange {
    start_ms: i64,
    end_ms: i64,
}

impl TimeRange {
    /// Creates positive, increasing explicit time bounds.
    ///
    /// # Errors
    ///
    /// Returns [`InfoError::InvalidRequest`] unless both timestamps are positive
    /// and `start_ms < end_ms`.
    pub fn new(start_ms: i64, end_ms: i64) -> Result<Self, InfoError> {
        if start_ms <= 0 || end_ms <= 0 {
            return Err(invalid_request(
                "time",
                "must use positive epoch milliseconds",
            ));
        }
        if start_ms >= end_ms {
            return Err(invalid_request("time", "start must be earlier than end"));
        }
        Ok(Self { start_ms, end_ms })
    }

    /// Returns the inclusive start timestamp in epoch milliseconds.
    #[must_use]
    pub const fn start_ms(self) -> i64 {
        self.start_ms
    }

    /// Returns the inclusive end timestamp in epoch milliseconds.
    #[must_use]
    pub const fn end_ms(self) -> i64 {
        self.end_ms
    }

    pub(crate) const fn contains(self, timestamp_ms: i64) -> bool {
        timestamp_ms >= self.start_ms && timestamp_ms <= self.end_ms
    }
}

/// Candle intervals supported by the paper strategies.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CandleInterval {
    /// Fifteen-minute candles.
    FifteenMinutes,
    /// One-hour candles.
    OneHour,
}

impl CandleInterval {
    pub(crate) const fn as_wire(self) -> &'static str {
        match self {
            Self::FifteenMinutes => "15m",
            Self::OneHour => "1h",
        }
    }
}

/// Allowed mantissas for five-significant-figure L2 aggregation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum L2Mantissa {
    /// Multiples of one.
    One,
    /// Multiples of two.
    Two,
    /// Multiples of five.
    Five,
}

impl L2Mantissa {
    const fn value(self) -> u8 {
        match self {
            Self::One => 1,
            Self::Two => 2,
            Self::Five => 5,
        }
    }
}

/// Closed L2 price-aggregation choices accepted by Hyperliquid.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum L2BookPrecision {
    /// Omit significant-figure aggregation.
    Full,
    /// Aggregate to two significant figures.
    Two,
    /// Aggregate to three significant figures.
    Three,
    /// Aggregate to four significant figures.
    Four,
    /// Aggregate to five significant figures, optionally with a valid mantissa.
    Five {
        /// Optional `1`, `2`, or `5` mantissa.
        mantissa: Option<L2Mantissa>,
    },
}

/// Cloneable, immutable client for the five approved public `/info` reads.
#[derive(Debug, Clone)]
pub struct InfoClient {
    info_url: Url,
    client: reqwest::Client,
}

impl InfoClient {
    /// Constructs a client for the exact official HTTPS `/info` endpoint.
    ///
    /// The endpoint must contain no credentials, query, fragment, or explicit
    /// port. Redirects and environment proxies are disabled, and rustls is used.
    ///
    /// # Errors
    ///
    /// Returns [`InfoError::InvalidEndpoint`] for any noncanonical endpoint and
    /// [`InfoError::ClientBuild`] when the fixed reqwest client cannot be built.
    pub fn new(info_url: &str) -> Result<Self, InfoError> {
        let parsed = validate_production_url(info_url)?;
        let client = build_client(true, PRODUCTION_CONNECT_TIMEOUT, PRODUCTION_REQUEST_TIMEOUT)?;
        Ok(Self {
            info_url: parsed,
            client,
        })
    }

    /// Constructs a debug-only client for an exact loopback HTTP `/info` URL.
    ///
    /// This hook exists only so integration tests can use an in-process HTTP
    /// server. It accepts only a numeric loopback host, requires an explicit
    /// nondefault port, and is not compiled when debug assertions are disabled.
    #[cfg(debug_assertions)]
    #[doc(hidden)]
    pub fn new_loopback_for_test(info_url: &str) -> Result<Self, InfoError> {
        let parsed = validate_loopback_test_url(info_url)?;
        let client = build_client(false, TEST_CONNECT_TIMEOUT, TEST_REQUEST_TIMEOUT)?;
        Ok(Self {
            info_url: parsed,
            client,
        })
    }

    /// Fetches native perpetual metadata and its index-aligned asset contexts.
    ///
    /// # Errors
    ///
    /// Returns [`InfoError`] for transport, protocol, decode, or invariant failures.
    pub async fn meta_and_asset_contexts(&self) -> Result<MetaAndAssetContexts, InfoError> {
        let body = self
            .post_json(&MetaAndAssetContextsRequest {
                request_type: "metaAndAssetCtxs",
            })
            .await?;
        decode_meta(&body)
    }

    /// Fetches positive mids for native perpetual markets, excluding spot IDs.
    ///
    /// # Errors
    ///
    /// Returns [`InfoError`] for transport, protocol, decode, or invariant failures.
    pub async fn all_mids(
        &self,
    ) -> Result<std::collections::BTreeMap<Market, trench_core::domain::Price>, InfoError> {
        let body = self
            .post_json(&AllMidsRequest {
                request_type: "allMids",
            })
            .await?;
        decode_all_mids(&body)
    }

    /// Fetches an L2 snapshot using only an allowed precision relationship.
    ///
    /// # Errors
    ///
    /// Rejects non-native coin identifiers and invalid responses.
    pub async fn l2_book(
        &self,
        market: &Market,
        precision: L2BookPrecision,
    ) -> Result<L2Book, InfoError> {
        validate_native_request_market(market)?;
        let (n_sig_figs, mantissa) = l2_precision_fields(precision);
        let body = self
            .post_json(&L2BookRequest {
                request_type: "l2Book",
                coin: market.as_str(),
                n_sig_figs,
                mantissa,
            })
            .await?;
        decode_l2_book(&body, market)
    }

    /// Fetches recent candles for an explicit market, interval, and time range.
    ///
    /// # Errors
    ///
    /// Rejects non-native coin identifiers and invalid responses.
    pub async fn candle_snapshot(
        &self,
        market: &Market,
        interval: CandleInterval,
        range: TimeRange,
    ) -> Result<Vec<Candle>, InfoError> {
        validate_native_request_market(market)?;
        let body = self
            .post_json(&CandleSnapshotRequest {
                request_type: "candleSnapshot",
                req: CandleRequest {
                    coin: market.as_str(),
                    interval: interval.as_wire(),
                    start_time: range.start_ms,
                    end_time: range.end_ms,
                },
            })
            .await?;
        decode_candles(&body, market, interval, range)
    }

    /// Fetches inclusive historical funding over explicit time bounds.
    ///
    /// # Errors
    ///
    /// Rejects non-native coin identifiers and invalid responses.
    pub async fn funding_history(
        &self,
        market: &Market,
        range: TimeRange,
    ) -> Result<Vec<FundingRecord>, InfoError> {
        validate_native_request_market(market)?;
        let body = self
            .post_json(&FundingHistoryRequest {
                request_type: "fundingHistory",
                coin: market.as_str(),
                start_time: range.start_ms,
                end_time: range.end_ms,
            })
            .await?;
        decode_funding(&body, market, range)
    }

    async fn post_json<T: Serialize + ?Sized>(&self, request: &T) -> Result<Vec<u8>, InfoError> {
        let mut response = self
            .client
            .post(self.info_url.clone())
            .json(request)
            .send()
            .await
            .map_err(map_transport_error)?;

        if !response.status().is_success() {
            return Err(InfoError::HttpStatus {
                code: response.status().as_u16(),
            });
        }
        validate_json_content_type(&response)?;
        if response.content_length().is_some_and(|length| {
            length > u64::try_from(INFO_RESPONSE_MAX_BYTES).unwrap_or(u64::MAX)
        }) {
            return Err(InfoError::ResponseTooLarge {
                max_bytes: INFO_RESPONSE_MAX_BYTES,
            });
        }

        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(map_transport_error)? {
            let next_len =
                body.len()
                    .checked_add(chunk.len())
                    .ok_or(InfoError::ResponseTooLarge {
                        max_bytes: INFO_RESPONSE_MAX_BYTES,
                    })?;
            if next_len > INFO_RESPONSE_MAX_BYTES {
                return Err(InfoError::ResponseTooLarge {
                    max_bytes: INFO_RESPONSE_MAX_BYTES,
                });
            }
            body.extend_from_slice(&chunk);
        }
        Ok(body)
    }
}

#[derive(Serialize)]
struct MetaAndAssetContextsRequest {
    #[serde(rename = "type")]
    request_type: &'static str,
}

#[derive(Serialize)]
struct AllMidsRequest {
    #[serde(rename = "type")]
    request_type: &'static str,
}

#[derive(Serialize)]
struct L2BookRequest<'a> {
    #[serde(rename = "type")]
    request_type: &'static str,
    coin: &'a str,
    #[serde(rename = "nSigFigs", skip_serializing_if = "Option::is_none")]
    n_sig_figs: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    mantissa: Option<u8>,
}

#[derive(Serialize)]
struct CandleSnapshotRequest<'a> {
    #[serde(rename = "type")]
    request_type: &'static str,
    req: CandleRequest<'a>,
}

#[derive(Serialize)]
struct CandleRequest<'a> {
    coin: &'a str,
    interval: &'static str,
    #[serde(rename = "startTime")]
    start_time: i64,
    #[serde(rename = "endTime")]
    end_time: i64,
}

#[derive(Serialize)]
struct FundingHistoryRequest<'a> {
    #[serde(rename = "type")]
    request_type: &'static str,
    coin: &'a str,
    #[serde(rename = "startTime")]
    start_time: i64,
    #[serde(rename = "endTime")]
    end_time: i64,
}

fn validate_production_url(value: &str) -> Result<Url, InfoError> {
    let parsed = Url::parse(value).map_err(|_| InfoError::InvalidEndpoint)?;
    if value != OFFICIAL_INFO_URL
        || parsed.as_str() != OFFICIAL_INFO_URL
        || parsed.scheme() != "https"
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.port().is_some()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
        || parsed.path() != "/info"
    {
        return Err(InfoError::InvalidEndpoint);
    }
    Ok(parsed)
}

#[cfg(debug_assertions)]
fn validate_loopback_test_url(value: &str) -> Result<Url, InfoError> {
    let parsed = Url::parse(value).map_err(|_| InfoError::InvalidEndpoint)?;
    let is_loopback = match parsed.host() {
        Some(Host::Ipv4(address)) => address.is_loopback(),
        Some(Host::Ipv6(address)) => address.is_loopback(),
        _ => false,
    };
    if parsed.scheme() != "http"
        || !is_loopback
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.port().is_none()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
        || parsed.path() != "/info"
        || parsed.as_str() != value
    {
        return Err(InfoError::InvalidEndpoint);
    }
    Ok(parsed)
}

fn build_client(
    https_only: bool,
    connect_timeout: Duration,
    request_timeout: Duration,
) -> Result<reqwest::Client, InfoError> {
    reqwest::Client::builder()
        .redirect(Policy::none())
        .use_rustls_tls()
        .https_only(https_only)
        .no_proxy()
        .user_agent(USER_AGENT)
        .connect_timeout(connect_timeout)
        .timeout(request_timeout)
        .build()
        .map_err(|_| InfoError::ClientBuild)
}

fn validate_json_content_type(response: &reqwest::Response) -> Result<(), InfoError> {
    let is_json = response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .is_some_and(|value| value.trim().eq_ignore_ascii_case("application/json"));
    if !is_json {
        return Err(InfoError::InvalidContentType);
    }
    Ok(())
}

fn map_transport_error(error: reqwest::Error) -> InfoError {
    if error.is_timeout() {
        InfoError::Timeout
    } else {
        InfoError::Transport
    }
}

fn validate_native_request_market(market: &Market) -> Result<(), InfoError> {
    if !market
        .as_str()
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric())
    {
        return Err(invalid_request("coin", "must be a native perpetual symbol"));
    }
    Ok(())
}

fn invalid_request(field: &'static str, requirement: &'static str) -> InfoError {
    InfoError::InvalidRequest { field, requirement }
}

fn l2_precision_fields(precision: L2BookPrecision) -> (Option<u8>, Option<u8>) {
    match precision {
        L2BookPrecision::Full => (None, None),
        L2BookPrecision::Two => (Some(2), None),
        L2BookPrecision::Three => (Some(3), None),
        L2BookPrecision::Four => (Some(4), None),
        L2BookPrecision::Five { mantissa } => (Some(5), mantissa.map(L2Mantissa::value)),
    }
}
