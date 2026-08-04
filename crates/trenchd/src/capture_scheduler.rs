//! Deterministic bounded scheduling for complete public-context captures.

use std::time::Duration;

use thiserror::Error;
use trench_core::domain::Market;
use trench_core::event::TimestampNs;
use trench_hyperliquid::{
    ContextCaptureError, ContextCaptureRequest, MAX_CONTEXT_EVENTS, MAX_CONTEXT_FUNDING_RECORDS,
    MAX_CONTEXT_MARKETS, TimeRange,
};

const MILLISECOND_NS: i64 = 1_000_000;
const FIFTEEN_MINUTES_MS: i64 = 15 * 60 * 1_000;
const HOUR_MS: i64 = 60 * 60 * 1_000;
const METADATA_EVENT_BUDGET: usize = MAX_CONTEXT_MARKETS * 3;
const DETAILED_MARKET_EVENT_BUDGET: usize = MAX_CONTEXT_FUNDING_RECORDS + 4;

/// Largest detailed rotating scope which remains valid even when every market
/// returns the full bounded funding history.
pub(crate) const MAX_SCHEDULED_DETAILED_MARKETS: usize =
    (MAX_CONTEXT_EVENTS - METADATA_EVENT_BUDGET) / DETAILED_MARKET_EVENT_BUDGET;

/// One deterministic, single-flight public-context schedule.
#[derive(Debug, Clone)]
pub(crate) struct CaptureScheduler {
    markets: Vec<Market>,
    next_market: usize,
    in_flight: Option<usize>,
}

impl CaptureScheduler {
    /// Creates a canonical rotating detailed scope from the current dynamic universe.
    #[must_use]
    pub(crate) fn new(mut markets: Vec<Market>) -> Self {
        markets.sort();
        markets.dedup();
        Self {
            markets,
            next_market: 0,
            in_flight: None,
        }
    }

    /// Returns whether a completed capture can include detailed market facts.
    #[must_use]
    pub(crate) fn has_detailed_markets(&self) -> bool {
        !self.markets.is_empty()
    }

    /// Replaces the rotating detail scope only after an immutable capture
    /// provides a fresh dynamic-universe observation.
    pub(crate) fn replace_markets(&mut self, mut markets: Vec<Market>) {
        if self.in_flight() {
            return;
        }
        markets.sort();
        markets.dedup();
        if self.markets == markets {
            return;
        }
        self.markets = markets;
        self.next_market = 0;
    }

    /// Returns whether a complete batch is awaiting authority handling.
    #[must_use]
    pub(crate) fn in_flight(&self) -> bool {
        self.in_flight.is_some()
    }

    /// Starts exactly one capture using only already-completed source windows.
    ///
    /// A second timer tick while the worker is running is intentionally skipped;
    /// it may not create a competing REST batch or a second persistence path.
    pub(crate) fn dispatch(
        &mut self,
        scheduled_at: TimestampNs,
    ) -> Result<Option<ContextCaptureRequest>, CaptureScheduleError> {
        if self.in_flight() {
            return Ok(None);
        }
        let count = self.markets.len().min(MAX_SCHEDULED_DETAILED_MARKETS);
        let detailed_markets = (0..count)
            .map(|offset| self.markets[(self.next_market + offset) % self.markets.len()].clone())
            .collect::<Vec<_>>();
        let now_ms = scheduled_at.value().div_euclid(MILLISECOND_NS);
        let fifteen_minutes = completed_window(now_ms, FIFTEEN_MINUTES_MS)?;
        let hourly = completed_window(now_ms, HOUR_MS)?;
        let request =
            ContextCaptureRequest::new(detailed_markets, fifteen_minutes, hourly, hourly)?;
        self.in_flight = Some(count);
        Ok(Some(request))
    }

    /// Resolves the outstanding batch and advances the rotating detail scope
    /// only after a complete authority persistence/admission success.
    pub(crate) fn complete(&mut self, persisted: bool) {
        let Some(count) = self.in_flight.take() else {
            return;
        };
        if persisted && !self.markets.is_empty() {
            self.next_market = (self.next_market + count) % self.markets.len();
        }
    }
}

fn completed_window(now_ms: i64, interval_ms: i64) -> Result<TimeRange, CaptureScheduleError> {
    let completed_boundary = now_ms
        .div_euclid(interval_ms)
        .checked_mul(interval_ms)
        .ok_or(CaptureScheduleError::TimestampOutOfRange)?;
    let start_ms = completed_boundary
        .checked_sub(interval_ms)
        .ok_or(CaptureScheduleError::TimestampOutOfRange)?;
    let end_ms = completed_boundary
        .checked_sub(1)
        .ok_or(CaptureScheduleError::TimestampOutOfRange)?;
    TimeRange::new(start_ms, end_ms).map_err(CaptureScheduleError::Range)
}

/// A scheduled capture could not be constructed from its explicit UTC time.
#[derive(Debug, Error)]
pub(crate) enum CaptureScheduleError {
    /// The daemon clock could not be reduced to a valid completed UTC window.
    #[error("capture schedule time is outside the supported UTC range")]
    TimestampOutOfRange,
    /// The bounded Task-31 capture contract rejected an otherwise explicit scope.
    #[error(transparent)]
    Capture(#[from] ContextCaptureError),
    /// The exact public `/info` time range was invalid.
    #[error(transparent)]
    Range(#[from] trench_hyperliquid::InfoError),
}

/// Frozen cadence shared by the feed-universe refresh and context collection.
#[must_use]
pub(crate) fn cadence(seconds: u32) -> Duration {
    Duration::from_secs(u64::from(seconds))
}

#[cfg(test)]
mod tests {
    use trench_core::domain::Market;
    use trench_core::event::TimestampNs;

    use super::{CaptureScheduler, HOUR_MS, MAX_SCHEDULED_DETAILED_MARKETS, cadence};

    fn market(value: &str) -> Market {
        Market::new(value).expect("fixture market")
    }

    fn timestamp_ms(value: i64) -> TimestampNs {
        TimestampNs::new(i128::from(value) * 1_000_000).expect("fixture timestamp")
    }

    #[test]
    fn scheduler_uses_only_closed_windows_at_its_explicit_timer_boundary() {
        let mut scheduler = CaptureScheduler::new(vec![market("BTC")]);
        let request = scheduler
            .dispatch(timestamp_ms(HOUR_MS * 2 + 123))
            .expect("scheduled capture request")
            .expect("first timer tick must dispatch");

        assert_eq!(request.detailed_markets(), &[market("BTC")]);
        assert!(scheduler.in_flight());
        assert_eq!(cadence(3_600).as_secs(), 3_600);
    }

    #[test]
    fn scheduler_is_single_flight_and_rotates_only_after_persistence() {
        let markets = (0..=MAX_SCHEDULED_DETAILED_MARKETS)
            .map(|index| market(&format!("M{index:02}")))
            .collect::<Vec<_>>();
        let mut scheduler = CaptureScheduler::new(markets.clone());
        let first = scheduler
            .dispatch(timestamp_ms(HOUR_MS * 2))
            .expect("first request")
            .expect("first request dispatches");
        assert_eq!(
            scheduler
                .dispatch(timestamp_ms(HOUR_MS * 3))
                .expect("contended tick"),
            None,
            "the timer cannot overlap a capture worker"
        );

        scheduler.complete(false);
        let retry = scheduler
            .dispatch(timestamp_ms(HOUR_MS * 3))
            .expect("retry request")
            .expect("failed capture is retried before rotating");
        assert_eq!(first.detailed_markets(), retry.detailed_markets());

        scheduler.complete(true);
        let rotated = scheduler
            .dispatch(timestamp_ms(HOUR_MS * 4))
            .expect("rotated request")
            .expect("next successful timer tick");
        assert_ne!(first.detailed_markets(), rotated.detailed_markets());
    }
}
