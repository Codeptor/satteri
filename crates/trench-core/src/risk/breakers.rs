//! Frozen per-ledger breaker accounting with explicit UTC reconciliation.

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use super::BreakerState;
    use crate::domain::Usdc;
    use crate::event::TimestampNs;

    const HOUR_NS: i128 = 3_600_000_000_000;
    const DAY_NS: i128 = 24 * HOUR_NS;

    fn timestamp(value: i128) -> TimestampNs {
        TimestampNs::new(value).expect("test timestamp must be valid")
    }

    fn usdc(value: rust_decimal::Decimal) -> Usdc {
        Usdc::new(value).expect("test USDC amount must be valid")
    }

    #[test]
    fn frozen_budgets_trip_at_the_exact_100_usdc_boundaries() {
        let at = timestamp(1_785_715_200_000_000_000);
        let state = BreakerState::new(at, usdc(dec!(100))).expect("state must initialize");

        assert!(state.allows_planned_loss(usdc(dec!(0.50)), usdc(dec!(100))));
        assert!(!state.allows_planned_loss(usdc(dec!(0.5001)), usdc(dec!(100))));

        let daily = state
            .record_equity(at, usdc(dec!(98.50)))
            .expect("daily boundary mark must apply");
        assert!(daily.daily_loss_tripped());
        assert_eq!(
            daily
                .daily_budget_remaining()
                .expect("daily budget must be representable"),
            usdc(dec!(0))
        );

        let weekly = state
            .record_equity(at, usdc(dec!(96)))
            .expect("weekly boundary mark must apply");
        assert!(weekly.weekly_loss_tripped());
        assert_eq!(
            weekly
                .weekly_budget_remaining()
                .expect("weekly budget must be representable"),
            usdc(dec!(0))
        );

        let hard = state
            .record_equity(at, usdc(dec!(92)))
            .expect("hard drawdown mark must apply");
        assert!(hard.hard_drawdown_latched());
        assert!(hard.requires_reduce_only());
    }

    #[test]
    fn period_budgets_never_recover_before_reconciliation() {
        let at = timestamp(1_785_715_200_000_000_000);
        let initial = BreakerState::new(at, usdc(dec!(100))).expect("state must initialize");
        let loss = initial
            .record_equity(at, usdc(dec!(99)))
            .expect("loss mark must apply");
        let recovery = loss
            .record_equity(at, usdc(dec!(100)))
            .expect("recovery mark must apply");

        assert_eq!(
            loss.daily_budget_remaining()
                .expect("daily budget must be representable"),
            usdc(dec!(0.50))
        );
        assert_eq!(
            recovery
                .daily_budget_remaining()
                .expect("daily budget must be representable"),
            usdc(dec!(0.50))
        );
        assert_eq!(
            loss.weekly_budget_remaining()
                .expect("weekly budget must be representable"),
            usdc(dec!(3))
        );
        assert_eq!(
            recovery
                .weekly_budget_remaining()
                .expect("weekly budget must be representable"),
            usdc(dec!(3))
        );
    }

    #[test]
    fn three_losses_create_a_twelve_hour_cooldown_and_six_entries_block_the_seventh() {
        let at = timestamp(1_785_715_200_000_000_000);
        let mut state = BreakerState::new(at, usdc(dec!(100))).expect("state must initialize");
        for loss in [dec!(-0.01), dec!(-0.01), dec!(-0.01)] {
            state = state
                .record_closed_trade(at, loss)
                .expect("loss must be recorded");
        }

        assert!(!state.entry_allowed(timestamp(i128::from(at.value()) + 11 * HOUR_NS)));
        assert!(state.entry_allowed(timestamp(i128::from(at.value()) + 12 * HOUR_NS)));

        let after_cooldown = timestamp(i128::from(at.value()) + 12 * HOUR_NS);
        for _ in 0..6 {
            state = state
                .record_entry(after_cooldown)
                .expect("entry inside the daily limit must apply");
        }
        assert!(!state.entry_allowed(after_cooldown));
        assert!(state.record_entry(after_cooldown).is_err());
    }

    #[test]
    fn daily_and_weekly_resets_require_reconciliation_but_hard_drawdown_does_not_reset() {
        let at = timestamp(1_785_715_200_000_000_000);
        let next_day = timestamp(i128::from(at.value()) + DAY_NS);
        let state = BreakerState::new(at, usdc(dec!(100)))
            .expect("state must initialize")
            .record_equity(at, usdc(dec!(92)))
            .expect("loss mark must apply");

        assert!(state.daily_loss_tripped());
        assert!(state.weekly_loss_tripped());
        assert!(!state.entry_allowed(next_day));

        let reconciled = state
            .reconcile(next_day, usdc(dec!(92)))
            .expect("reconciliation should reset elapsed periods");
        assert!(!reconciled.daily_loss_tripped());
        assert!(reconciled.weekly_loss_tripped());
        assert!(reconciled.hard_drawdown_latched());
    }

    #[test]
    fn reconciliation_can_reset_elapsed_budgets_at_zero_equity() {
        let at = timestamp(1_785_715_200_000_000_000);
        let next_day = timestamp(i128::from(at.value()) + DAY_NS);
        let state = BreakerState::new(at, usdc(dec!(100)))
            .expect("state must initialize")
            .record_equity(at, usdc(dec!(0)))
            .expect("zero-equity mark must apply");

        let reconciled = state
            .reconcile(next_day, usdc(dec!(0)))
            .expect("zero-equity reconciliation must remain valid");

        assert_eq!(
            reconciled
                .daily_budget_remaining()
                .expect("daily budget must be representable"),
            usdc(dec!(0))
        );
        assert!(reconciled.hard_drawdown_latched());
    }

    #[test]
    fn epoch_adjacent_week_anchors_clamp_the_unrepresentable_monday_and_roll_on_monday() {
        let epoch = timestamp(0);
        for day in 0..4 {
            let state = BreakerState::new(timestamp(day * DAY_NS), usdc(dec!(100)))
                .expect("epoch-adjacent state");
            assert_eq!(state.weekly().anchor(), epoch, "day {day}");
        }

        let first_monday = timestamp(4 * DAY_NS);
        let following_monday = timestamp(11 * DAY_NS);
        let monday_state =
            BreakerState::new(first_monday, usdc(dec!(100))).expect("first Monday state");
        assert_eq!(monday_state.weekly().anchor(), first_monday);

        let sunday_state =
            BreakerState::new(timestamp(3 * DAY_NS), usdc(dec!(100))).expect("Sunday state");
        let rollover = sunday_state
            .reconcile(first_monday, usdc(dec!(100)))
            .expect("Monday reconcile");
        assert_eq!(rollover.weekly().anchor(), first_monday);

        let following_monday_state =
            BreakerState::new(following_monday, usdc(dec!(100))).expect("following Monday state");
        assert_eq!(following_monday_state.weekly().anchor(), following_monday);
    }

    proptest! {
        #[test]
        fn daily_and_weekly_remaining_budget_are_monotonic_before_reconciliation(
            first_loss_cents in 0_i64..=10_000,
            second_loss_cents in 0_i64..=10_000,
        ) {
            let at = timestamp(1_785_715_200_000_000_000);
            let initial = BreakerState::new(at, usdc(dec!(100))).expect("state must initialize");
            let first_equity = Usdc::new(
                Decimal::ONE_HUNDRED - Decimal::new(first_loss_cents, 2),
            )
            .expect("bounded property equity must be nonnegative");
            let first = initial
                .record_equity(at, first_equity)
                .expect("first property mark must apply");
            let second_equity = Usdc::new(
                Decimal::ONE_HUNDRED - Decimal::new(second_loss_cents, 2),
            )
            .expect("bounded property equity must be nonnegative");
            let second = first
                .record_equity(at, second_equity)
                .expect("second property mark must apply");

            prop_assert!(
                second
                    .daily_budget_remaining()
                    .expect("daily budget must be representable")
                    <= first
                        .daily_budget_remaining()
                        .expect("daily budget must be representable")
            );
            prop_assert!(
                second
                    .weekly_budget_remaining()
                    .expect("weekly budget must be representable")
                    <= first
                        .weekly_budget_remaining()
                        .expect("weekly budget must be representable")
            );
        }
    }
}

use rust_decimal::Decimal;
use thiserror::Error;

use crate::domain::{DomainError, Usdc};
use crate::event::{DurationNs, EventError, TimestampNs};

const TRADE_BUDGET_FRACTION: Decimal = Decimal::from_parts(5, 0, 0, false, 3);
const DAILY_LOSS_FRACTION: Decimal = Decimal::from_parts(15, 0, 0, false, 3);
const WEEKLY_LOSS_FRACTION: Decimal = Decimal::from_parts(4, 0, 0, false, 2);
const HARD_DRAWDOWN_FRACTION: Decimal = Decimal::from_parts(8, 0, 0, false, 2);
const DAY_NS: i64 = 86_400_000_000_000;
const COOLDOWN_NS: i64 = 43_200_000_000_000;
const MAX_ENTRIES_PER_DAY: u8 = 6;
const CONSECUTIVE_LOSS_LIMIT: u8 = 3;

/// One frozen loss budget measured from a reconciled UTC-period anchor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LossBudget {
    anchor: TimestampNs,
    starting_equity: Usdc,
    fraction: Decimal,
    limit: Usdc,
    used: Usdc,
    tripped: bool,
}

impl LossBudget {
    fn new(
        anchor: TimestampNs,
        starting_equity: Usdc,
        fraction: Decimal,
    ) -> Result<Self, BreakerError> {
        let limit = checked_fraction(starting_equity, fraction, "period loss limit")?;
        Ok(Self {
            anchor,
            starting_equity,
            fraction,
            limit,
            used: Usdc::new(Decimal::ZERO)?,
            tripped: false,
        })
    }

    fn record_equity(&self, equity: Usdc) -> Result<Self, BreakerError> {
        let observed = self
            .starting_equity
            .value()
            .checked_sub(equity.value())
            .unwrap_or(Decimal::ZERO)
            .max(Decimal::ZERO);
        let used = self.used.value().max(observed);
        let used = Usdc::new(used)?;
        Ok(Self {
            anchor: self.anchor,
            starting_equity: self.starting_equity,
            fraction: self.fraction,
            limit: self.limit,
            used,
            tripped: self.tripped || used >= self.limit,
        })
    }

    fn reset(&self, anchor: TimestampNs, equity: Usdc) -> Result<Self, BreakerError> {
        Self::new(anchor, equity, self.fraction)
    }

    /// Returns the UTC period anchor established by reconciliation.
    #[must_use]
    pub const fn anchor(&self) -> TimestampNs {
        self.anchor
    }

    /// Returns the immutable starting equity for this period.
    #[must_use]
    pub const fn starting_equity(&self) -> Usdc {
        self.starting_equity
    }

    /// Returns the frozen loss limit for this period.
    #[must_use]
    pub const fn limit(&self) -> Usdc {
        self.limit
    }

    /// Returns the monotonic loss amount consumed in this period.
    #[must_use]
    pub const fn used(&self) -> Usdc {
        self.used
    }

    /// Returns remaining loss capacity without allowing a recovery to restore it.
    pub fn remaining(&self) -> Result<Usdc, BreakerError> {
        let remaining = self
            .limit
            .value()
            .checked_sub(self.used.value())
            .unwrap_or(Decimal::ZERO)
            .max(Decimal::ZERO);
        Usdc::new(remaining).map_err(BreakerError::from)
    }

    /// Returns whether the budget has been exhausted.
    #[must_use]
    pub const fn tripped(&self) -> bool {
        self.tripped
    }
}

/// Immutable breaker/accounting state owned by exactly one ledger.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BreakerState {
    daily: LossBudget,
    weekly: LossBudget,
    high_water_equity: Usdc,
    hard_drawdown_latched: bool,
    entries_today: u8,
    consecutive_losses: u8,
    cooldown_until: Option<TimestampNs>,
    last_transition_at: TimestampNs,
}

impl BreakerState {
    /// Establishes all UTC anchors at a ledger's explicit creation time.
    ///
    /// # Errors
    ///
    /// Returns an error if fixed budget arithmetic cannot be represented.
    pub fn new(at: TimestampNs, equity: Usdc) -> Result<Self, BreakerError> {
        Ok(Self {
            daily: LossBudget::new(utc_day_start(at)?, equity, DAILY_LOSS_FRACTION)?,
            weekly: LossBudget::new(utc_week_start(at)?, equity, WEEKLY_LOSS_FRACTION)?,
            high_water_equity: equity,
            hard_drawdown_latched: false,
            entries_today: 0,
            consecutive_losses: 0,
            cooldown_until: None,
            last_transition_at: at,
        })
    }

    /// Tests a proposed loss against the 0.5-percent current-equity trade budget.
    #[must_use]
    pub fn allows_planned_loss(&self, planned_loss: Usdc, equity: Usdc) -> bool {
        checked_fraction(equity, TRADE_BUDGET_FRACTION, "trade budget")
            .is_ok_and(|budget| planned_loss <= budget)
    }

    /// Records an equity observation without restoring previously consumed budgets.
    ///
    /// # Errors
    ///
    /// Rejects event-time reversals and checked arithmetic failures.
    pub fn record_equity(&self, at: TimestampNs, equity: Usdc) -> Result<Self, BreakerError> {
        self.require_monotonic_time(at)?;
        let daily = self.daily.record_equity(equity)?;
        let weekly = self.weekly.record_equity(equity)?;
        let high_water_equity = self.high_water_equity.max(equity);
        let drawdown = high_water_equity
            .value()
            .checked_sub(equity.value())
            .unwrap_or(Decimal::ZERO);
        let hard_limit = checked_fraction(
            high_water_equity,
            HARD_DRAWDOWN_FRACTION,
            "hard drawdown limit",
        )?;
        Ok(Self {
            daily,
            weekly,
            high_water_equity,
            hard_drawdown_latched: self.hard_drawdown_latched || drawdown >= hard_limit.value(),
            entries_today: self.entries_today,
            consecutive_losses: self.consecutive_losses,
            cooldown_until: self.cooldown_until,
            last_transition_at: at,
        })
    }

    /// Records a complete realized trade and starts the fixed cooldown on loss three.
    ///
    /// # Errors
    ///
    /// Rejects event-time reversals and timestamp overflow.
    pub fn record_closed_trade(
        &self,
        at: TimestampNs,
        net_pnl: Decimal,
    ) -> Result<Self, BreakerError> {
        self.require_monotonic_time(at)?;
        let consecutive_losses = if net_pnl < Decimal::ZERO {
            self.consecutive_losses.saturating_add(1)
        } else {
            0
        };
        let cooldown_until = if consecutive_losses >= CONSECUTIVE_LOSS_LIMIT {
            Some(at.checked_add(DurationNs::new(i128::from(COOLDOWN_NS))?)?)
        } else {
            self.cooldown_until
        };
        Ok(Self {
            daily: self.daily.clone(),
            weekly: self.weekly.clone(),
            high_water_equity: self.high_water_equity,
            hard_drawdown_latched: self.hard_drawdown_latched,
            entries_today: self.entries_today,
            consecutive_losses,
            cooldown_until,
            last_transition_at: at,
        })
    }

    /// Records one accepted entry after all current entry blockers have cleared.
    ///
    /// # Errors
    ///
    /// Rejects blocked entries and event-time reversals.
    pub fn record_entry(&self, at: TimestampNs) -> Result<Self, BreakerError> {
        if !self.entry_allowed(at) {
            return Err(BreakerError::EntryBlocked);
        }
        Ok(Self {
            daily: self.daily.clone(),
            weekly: self.weekly.clone(),
            high_water_equity: self.high_water_equity,
            hard_drawdown_latched: self.hard_drawdown_latched,
            entries_today: self.entries_today.saturating_add(1),
            consecutive_losses: self.consecutive_losses,
            cooldown_until: self.cooldown_until,
            last_transition_at: at,
        })
    }

    /// Advances elapsed UTC periods only through an explicit reconciliation.
    ///
    /// # Errors
    ///
    /// Rejects event-time reversals and checked arithmetic failures.
    pub fn reconcile(&self, at: TimestampNs, equity: Usdc) -> Result<Self, BreakerError> {
        self.require_monotonic_time(at)?;
        let daily_start = utc_day_start(at)?;
        let weekly_start = utc_week_start(at)?;
        let daily_changed = daily_start > self.daily.anchor;
        let weekly_changed = weekly_start > self.weekly.anchor;
        Ok(Self {
            daily: if daily_changed {
                self.daily.reset(daily_start, equity)?
            } else {
                self.daily.clone()
            },
            weekly: if weekly_changed {
                self.weekly.reset(weekly_start, equity)?
            } else {
                self.weekly.clone()
            },
            high_water_equity: self.high_water_equity.max(equity),
            hard_drawdown_latched: self.hard_drawdown_latched,
            entries_today: if daily_changed { 0 } else { self.entries_today },
            consecutive_losses: self.consecutive_losses,
            cooldown_until: self.cooldown_until,
            last_transition_at: at,
        })
    }

    /// Returns whether an entry may occur at this explicit time.
    #[must_use]
    pub fn entry_allowed(&self, at: TimestampNs) -> bool {
        at >= self.last_transition_at
            && !self.daily.tripped
            && !self.weekly.tripped
            && !self.hard_drawdown_latched
            && self.entries_today < MAX_ENTRIES_PER_DAY
            && self.cooldown_until.is_none_or(|until| at >= until)
    }

    /// Returns the daily breaker state.
    #[must_use]
    pub const fn daily(&self) -> &LossBudget {
        &self.daily
    }

    /// Returns the weekly breaker state.
    #[must_use]
    pub const fn weekly(&self) -> &LossBudget {
        &self.weekly
    }

    /// Returns the high-water equity used by the hard-drawdown latch.
    #[must_use]
    pub const fn high_water_equity(&self) -> Usdc {
        self.high_water_equity
    }

    /// Returns whether the 8-percent high-water breaker is permanently latched.
    #[must_use]
    pub const fn hard_drawdown_latched(&self) -> bool {
        self.hard_drawdown_latched
    }

    /// Returns whether a loss breaker requires an immediate reduce-only close.
    #[must_use]
    pub const fn requires_reduce_only(&self) -> bool {
        self.daily.tripped || self.weekly.tripped || self.hard_drawdown_latched
    }

    /// Returns the remaining daily loss budget.
    pub fn daily_budget_remaining(&self) -> Result<Usdc, BreakerError> {
        self.daily.remaining()
    }

    /// Returns the remaining weekly loss budget.
    pub fn weekly_budget_remaining(&self) -> Result<Usdc, BreakerError> {
        self.weekly.remaining()
    }

    /// Returns whether the daily loss limit is currently active.
    #[must_use]
    pub const fn daily_loss_tripped(&self) -> bool {
        self.daily.tripped
    }

    /// Returns whether the weekly loss limit is currently active.
    #[must_use]
    pub const fn weekly_loss_tripped(&self) -> bool {
        self.weekly.tripped
    }

    /// Returns the number of accepted entries in the unreconciled UTC day.
    #[must_use]
    pub const fn entries_today(&self) -> u8 {
        self.entries_today
    }

    /// Returns the current consecutive-loss count.
    #[must_use]
    pub const fn consecutive_losses(&self) -> u8 {
        self.consecutive_losses
    }

    /// Returns the UTC time at which the cooldown ends, when active.
    #[must_use]
    pub const fn cooldown_until(&self) -> Option<TimestampNs> {
        self.cooldown_until
    }

    /// Returns the last explicit time accepted by breaker state.
    #[must_use]
    pub const fn last_transition_at(&self) -> TimestampNs {
        self.last_transition_at
    }

    fn require_monotonic_time(&self, at: TimestampNs) -> Result<(), BreakerError> {
        if at < self.last_transition_at {
            return Err(BreakerError::NonMonotonicTime {
                previous: self.last_transition_at,
                current: at,
            });
        }
        Ok(())
    }
}

/// Breaker transition rejection or checked-arithmetic failure.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum BreakerError {
    /// A timestamp moved backward relative to prior immutable state.
    #[error("breaker transition time must not move backward")]
    NonMonotonicTime {
        /// Prior explicit event time.
        previous: TimestampNs,
        /// Rejected current event time.
        current: TimestampNs,
    },
    /// A new entry was blocked by an active frozen breaker.
    #[error("entry is blocked by an active breaker")]
    EntryBlocked,
    /// A checked domain conversion failed.
    #[error(transparent)]
    Domain(#[from] DomainError),
    /// Explicit timestamp arithmetic failed.
    #[error(transparent)]
    Event(#[from] EventError),
    /// Checked decimal arithmetic failed.
    #[error("checked arithmetic failed while calculating {operation}")]
    Arithmetic {
        /// The operation that failed.
        operation: &'static str,
    },
}

fn checked_fraction(
    value: Usdc,
    fraction: Decimal,
    operation: &'static str,
) -> Result<Usdc, BreakerError> {
    value
        .value()
        .checked_mul(fraction)
        .ok_or(BreakerError::Arithmetic { operation })
        .and_then(|value| Usdc::new(value).map_err(BreakerError::from))
}

fn utc_day_start(at: TimestampNs) -> Result<TimestampNs, BreakerError> {
    TimestampNs::new(i128::from(at.value() / DAY_NS) * i128::from(DAY_NS))
        .map_err(BreakerError::from)
}

fn utc_week_start(at: TimestampNs) -> Result<TimestampNs, BreakerError> {
    let day = at.value() / DAY_NS;
    let days_since_monday = (day + 3).rem_euclid(7);
    // `TimestampNs` begins at the Unix epoch, so its initial partial UTC week
    // cannot be anchored to the preceding Monday.
    let week_start_day = if day < days_since_monday {
        0
    } else {
        day - days_since_monday
    };
    TimestampNs::new(i128::from(week_start_day) * i128::from(DAY_NS)).map_err(BreakerError::from)
}
