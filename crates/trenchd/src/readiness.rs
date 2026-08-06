//! Explicit, scoped paper-trading readiness.
//!
//! Readiness is an observable state owned by the daemon authority loop. It is
//! deliberately separate from core execution: an executable book can advance
//! a mandatory exit even while the rules sleeve is unavailable.

use std::collections::{BTreeMap, BTreeSet};

use serde::Serialize;
use trench_core::domain::Market;
use trench_core::event::TimestampNs;

const STALE_MARKET_DATA_NS: i64 = 300_000_000_000;

/// A global prerequisite that blocks new paper entries for every ledger.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GlobalBlocker {
    /// The daemon has no positive NTP-health assertion.
    Ntp,
    /// SQLite recovery or durable-state reconciliation is incomplete.
    SqliteReconciliation,
    /// The atomic market-data store is not writable and verified.
    Storage,
    /// The public market-data stream is disconnected or unhealthy.
    Stream,
    /// The dynamic-universe metadata is absent or stale.
    Metadata,
    /// The latest bounded public-context capture did not complete and persist.
    ContextCapture,
    /// The active universe does not have fresh executable books.
    FreshBooks,
}

/// A market-local condition that quarantines one market from entry generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketBlocker {
    /// Gap recovery has not completed for the market.
    Recovery,
    /// No current executable L2 book is available.
    ExecutableBook,
    /// Common feature inputs are not warmed and valid for the market.
    CommonFeatures,
    /// The market was removed by data-quality validation.
    DataQuality,
    /// Last BBO at bar-close is missing or older than 5 minutes.
    StaleBbo,
    /// Last AllMids is missing or older than 5 minutes.
    StaleAllMids,
}

/// A rules-ledger-local condition that blocks only rules entries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RulesBlocker {
    /// The frozen rules artifact/configuration was not validated.
    Configuration,
    /// The rules sleeve lacks its required warmed feature history.
    SleeveWarmup,
    /// No source-bound point-in-time universe witness has been activated.
    UniverseWitness,
    /// No source-bound risk-policy/book witness has been activated.
    RiskWitness,
}

/// Per-market readiness facts, each set explicitly by the authority loop.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct MarketGates {
    recovered: bool,
    executable_book: bool,
    common_features_warm: bool,
    data_quality_valid: bool,
}

impl MarketGates {
    /// Records completion of a verified market recovery boundary.
    pub fn set_recovered(&mut self, value: bool) {
        self.recovered = value;
    }

    /// Records availability of a current executable book.
    pub fn set_executable_book(&mut self, value: bool) {
        self.executable_book = value;
    }

    /// Records whether common features are warmed for entries.
    pub fn set_common_features_warm(&mut self, value: bool) {
        self.common_features_warm = value;
    }

    /// Records the data-quality quarantine outcome.
    pub fn set_data_quality_valid(&mut self, value: bool) {
        self.data_quality_valid = value;
    }

    /// Returns whether a mandatory broker exit may use this market's book.
    #[must_use]
    pub const fn execution_ready(self) -> bool {
        self.recovered && self.executable_book
    }

    fn blockers(self) -> BTreeSet<MarketBlocker> {
        let mut blockers = BTreeSet::new();
        if !self.recovered {
            blockers.insert(MarketBlocker::Recovery);
        }
        if !self.executable_book {
            blockers.insert(MarketBlocker::ExecutableBook);
        }
        if !self.common_features_warm {
            blockers.insert(MarketBlocker::CommonFeatures);
        }
        if !self.data_quality_valid {
            blockers.insert(MarketBlocker::DataQuality);
        }
        blockers
    }
}

/// Pure hierarchical readiness state.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Readiness {
    ntp_synchronized: bool,
    sqlite_reconciled: bool,
    storage_writable: bool,
    stream_connected: bool,
    metadata_current: bool,
    context_capture_current: bool,
    fresh_book_markets: BTreeSet<Market>,
    markets: BTreeMap<Market, MarketGates>,
    rules_configuration_valid: bool,
    rules_sleeve_warm: bool,
    universe_witness_valid: bool,
    risk_witness_valid: bool,
    current_time: Option<TimestampNs>,
    bbo_close_at: BTreeMap<Market, TimestampNs>,
    all_mids_at: BTreeMap<Market, TimestampNs>,
}

impl Readiness {
    /// Registers a market as part of the current dynamic universe.
    pub fn register_market(&mut self, market: Market) {
        self.markets.entry(market).or_default();
    }

    /// Mutates one registered market's independently observable gates.
    pub fn market_gates_mut(&mut self, market: &Market) -> Option<&mut MarketGates> {
        self.markets.get_mut(market)
    }

    /// Replaces the current market set whose books are required globally.
    pub fn set_fresh_book_markets(&mut self, markets: BTreeSet<Market>) {
        self.fresh_book_markets = markets;
    }

    /// Records the NTP health assertion supplied by the platform probe.
    pub fn set_ntp_synchronized(&mut self, value: bool) {
        self.ntp_synchronized = value;
    }

    /// Records completion of durable SQLite recovery and reconciliation.
    pub fn set_sqlite_reconciled(&mut self, value: bool) {
        self.sqlite_reconciled = value;
    }

    /// Records successful writable atomic-store validation.
    pub fn set_storage_writable(&mut self, value: bool) {
        self.storage_writable = value;
    }

    /// Records the public market-data connection state.
    pub fn set_stream_connected(&mut self, value: bool) {
        self.stream_connected = value;
    }

    /// Records current dynamic-universe metadata availability.
    pub fn set_metadata_current(&mut self, value: bool) {
        self.metadata_current = value;
    }

    /// Records whether the latest complete public-context batch was persisted
    /// and routed by the authority loop.
    pub fn set_context_capture_current(&mut self, value: bool) {
        self.context_capture_current = value;
    }

    /// Records rules-artifact/configuration validation.
    pub fn set_rules_configuration_valid(&mut self, value: bool) {
        self.rules_configuration_valid = value;
    }

    /// Records warmed rules sleeve features.
    pub fn set_rules_sleeve_warm(&mut self, value: bool) {
        self.rules_sleeve_warm = value;
    }

    /// Records whether a verified point-in-time universe witness is active.
    pub fn set_universe_witness_valid(&mut self, value: bool) {
        self.universe_witness_valid = value;
    }

    /// Records whether source-bound risk-policy/book witnesses are active.
    pub fn set_risk_witness_valid(&mut self, value: bool) {
        self.risk_witness_valid = value;
    }

    /// Records the current wall-clock time used for staleness checks.
    pub fn set_current_time(&mut self, at: TimestampNs) {
        self.current_time = Some(at);
    }

    /// Records the last BBO at bar-close time for a market.
    pub fn set_bbo_close_at(&mut self, market: Market, at: TimestampNs) {
        self.bbo_close_at.insert(market, at);
    }

    /// Records the last AllMids time for a market.
    pub fn set_all_mids_at(&mut self, market: Market, at: TimestampNs) {
        self.all_mids_at.insert(market, at);
    }

    fn is_stale(last: Option<TimestampNs>, now: Option<TimestampNs>) -> bool {
        match (last, now) {
            (Some(last), Some(now)) => now
                .checked_duration_since(last)
                .map_or(true, |age| age.value() > STALE_MARKET_DATA_NS),
            (None, Some(_)) => true,
            _ => false,
        }
    }

    /// Returns every global condition currently blocking fresh entries.
    #[must_use]
    pub fn global_blockers(&self) -> BTreeSet<GlobalBlocker> {
        let mut blockers = BTreeSet::new();
        if !self.ntp_synchronized {
            blockers.insert(GlobalBlocker::Ntp);
        }
        if !self.sqlite_reconciled {
            blockers.insert(GlobalBlocker::SqliteReconciliation);
        }
        if !self.storage_writable {
            blockers.insert(GlobalBlocker::Storage);
        }
        if !self.stream_connected {
            blockers.insert(GlobalBlocker::Stream);
        }
        if !self.metadata_current {
            blockers.insert(GlobalBlocker::Metadata);
        }
        if !self.context_capture_current {
            blockers.insert(GlobalBlocker::ContextCapture);
        }
        if self.fresh_book_markets.is_empty()
            || !self.fresh_book_markets.iter().all(|market| {
                self.markets
                    .get(market)
                    .is_some_and(|gates| gates.executable_book)
            })
        {
            blockers.insert(GlobalBlocker::FreshBooks);
        }
        blockers
    }

    /// Returns market-local entry blockers without duplicating global state.
    #[must_use]
    pub fn market_blockers(&self, market: &Market) -> BTreeSet<MarketBlocker> {
        let mut blockers = self.markets.get(market).map_or_else(
            || BTreeSet::from([MarketBlocker::Recovery, MarketBlocker::ExecutableBook]),
            |gates| gates.blockers(),
        );
        if Self::is_stale(self.bbo_close_at.get(market).copied(), self.current_time) {
            blockers.insert(MarketBlocker::StaleBbo);
        }
        if Self::is_stale(self.all_mids_at.get(market).copied(), self.current_time) {
            blockers.insert(MarketBlocker::StaleAllMids);
        }
        blockers
    }

    /// Returns rules-ledger-local blockers.
    #[must_use]
    pub fn rules_blockers(&self) -> BTreeSet<RulesBlocker> {
        let mut blockers = BTreeSet::new();
        if !self.rules_configuration_valid {
            blockers.insert(RulesBlocker::Configuration);
        }
        if !self.rules_sleeve_warm {
            blockers.insert(RulesBlocker::SleeveWarmup);
        }
        if !self.universe_witness_valid {
            blockers.insert(RulesBlocker::UniverseWitness);
        }
        if !self.risk_witness_valid {
            blockers.insert(RulesBlocker::RiskWitness);
        }
        blockers
    }

    /// Returns whether new rules-only entries are authorized for one market.
    #[must_use]
    pub fn rules_entry_ready(&self, market: &Market) -> bool {
        self.global_blockers().is_empty()
            && self.market_blockers(market).is_empty()
            && self.rules_blockers().is_empty()
    }

    /// Returns whether a mandatory exit may be passed to the broker.
    ///
    /// This intentionally ignores global and strategy gates: open paper risk
    /// must not be stranded merely because entry generation is paused.
    #[must_use]
    pub fn mandatory_exit_ready(&self, market: &Market) -> bool {
        self.markets
            .get(market)
            .is_some_and(|gates| gates.execution_ready())
    }

    /// Produces a stable, transport-safe status projection.
    #[must_use]
    pub fn snapshot(&self) -> ReadinessSnapshot {
        let markets = self
            .markets
            .iter()
            .map(|(market, gates)| MarketReadinessSnapshot {
                market: market.as_str().to_owned(),
                entry_blockers: gates.blockers().into_iter().collect(),
                rules_entry_ready: self.rules_entry_ready(market),
                mandatory_exit_ready: self.mandatory_exit_ready(market),
            })
            .collect();
        ReadinessSnapshot {
            global_blockers: self.global_blockers().into_iter().collect(),
            rules_blockers: self.rules_blockers().into_iter().collect(),
            markets,
        }
    }
}

/// A serializable readiness status returned only through the local admin IPC.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ReadinessSnapshot {
    /// Global entry blockers in deterministic order.
    pub global_blockers: Vec<GlobalBlocker>,
    /// Rules-only entry blockers in deterministic order.
    pub rules_blockers: Vec<RulesBlocker>,
    /// Current dynamic-universe market readiness in stable market order.
    pub markets: Vec<MarketReadinessSnapshot>,
}

/// One market's transport-safe readiness state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MarketReadinessSnapshot {
    /// Checked native perpetual identifier.
    pub market: String,
    /// Market-local entry blockers in deterministic order.
    pub entry_blockers: Vec<MarketBlocker>,
    /// Whether all global, market, and rules entry gates are currently open.
    pub rules_entry_ready: bool,
    /// Whether a mandatory exit can use a recovered executable book.
    pub mandatory_exit_ready: bool,
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::{GlobalBlocker, MarketBlocker, Readiness, RulesBlocker};
    use trench_core::domain::Market;

    fn market() -> Market {
        Market::new("SOL").expect("fixture market")
    }

    #[test]
    fn global_blockers_hold_entries_closed_until_every_gate_is_earned() {
        let market = market();
        let mut readiness = Readiness::default();
        readiness.register_market(market.clone());
        readiness.set_fresh_book_markets(BTreeSet::from([market.clone()]));

        assert_eq!(
            readiness.global_blockers(),
            BTreeSet::from([
                GlobalBlocker::Ntp,
                GlobalBlocker::SqliteReconciliation,
                GlobalBlocker::Storage,
                GlobalBlocker::Stream,
                GlobalBlocker::Metadata,
                GlobalBlocker::ContextCapture,
                GlobalBlocker::FreshBooks,
            ])
        );
        assert!(!readiness.rules_entry_ready(&market));

        readiness.set_ntp_synchronized(true);
        readiness.set_sqlite_reconciled(true);
        readiness.set_storage_writable(true);
        readiness.set_stream_connected(true);
        readiness.set_metadata_current(true);
        readiness.set_context_capture_current(true);
        let gates = readiness
            .market_gates_mut(&market)
            .expect("registered market");
        gates.set_recovered(true);
        gates.set_executable_book(true);
        gates.set_common_features_warm(true);
        gates.set_data_quality_valid(true);
        readiness.set_rules_configuration_valid(true);
        readiness.set_rules_sleeve_warm(true);
        readiness.set_universe_witness_valid(true);
        readiness.set_risk_witness_valid(true);

        assert!(readiness.global_blockers().is_empty());
        assert!(readiness.rules_entry_ready(&market));
    }

    #[test]
    fn market_quarantine_is_scoped_to_the_unhealthy_market() {
        let sol = market();
        let btc = Market::new("BTC").expect("fixture market");
        let mut readiness = Readiness::default();
        readiness.register_market(sol.clone());
        readiness.register_market(btc.clone());
        readiness.set_fresh_book_markets(BTreeSet::from([sol.clone(), btc.clone()]));
        for enabled in [
            Readiness::set_ntp_synchronized,
            Readiness::set_sqlite_reconciled,
            Readiness::set_storage_writable,
            Readiness::set_stream_connected,
            Readiness::set_metadata_current,
            Readiness::set_context_capture_current,
            Readiness::set_rules_configuration_valid,
            Readiness::set_rules_sleeve_warm,
            Readiness::set_universe_witness_valid,
            Readiness::set_risk_witness_valid,
        ] {
            enabled(&mut readiness, true);
        }
        for current in [&sol, &btc] {
            let gates = readiness
                .market_gates_mut(current)
                .expect("registered market");
            gates.set_recovered(true);
            gates.set_executable_book(true);
            gates.set_common_features_warm(true);
            gates.set_data_quality_valid(true);
        }
        readiness
            .market_gates_mut(&sol)
            .expect("registered SOL")
            .set_data_quality_valid(false);

        assert_eq!(
            readiness.market_blockers(&sol),
            BTreeSet::from([MarketBlocker::DataQuality])
        );
        assert!(!readiness.rules_entry_ready(&sol));
        assert!(readiness.rules_entry_ready(&btc));
    }

    #[test]
    fn rules_warmup_is_local_to_rules_and_does_not_weaken_execution() {
        let market = market();
        let mut readiness = Readiness::default();
        readiness.register_market(market.clone());
        let gates = readiness
            .market_gates_mut(&market)
            .expect("registered market");
        gates.set_recovered(true);
        gates.set_executable_book(true);

        assert_eq!(
            readiness.rules_blockers(),
            BTreeSet::from([
                RulesBlocker::Configuration,
                RulesBlocker::RiskWitness,
                RulesBlocker::SleeveWarmup,
                RulesBlocker::UniverseWitness,
            ])
        );
        assert!(!readiness.rules_entry_ready(&market));
        assert!(readiness.mandatory_exit_ready(&market));
    }

    #[test]
    fn open_position_mandatory_exit_uses_a_book_while_strategy_is_unready() {
        let market = market();
        let mut readiness = Readiness::default();
        readiness.register_market(market.clone());
        let gates = readiness
            .market_gates_mut(&market)
            .expect("registered market");
        gates.set_recovered(true);
        gates.set_executable_book(true);

        assert!(!readiness.rules_entry_ready(&market));
        assert!(readiness.mandatory_exit_ready(&market));
    }

    #[test]
    fn stale_bbo_or_all_mids_blocks_fresh_entries_but_allows_mandatory_exit() {
        let market = market();
        let mut readiness = Readiness::default();
        readiness.register_market(market.clone());
        readiness.set_fresh_book_markets(BTreeSet::from([market.clone()]));
        for enabled in [
            Readiness::set_ntp_synchronized,
            Readiness::set_sqlite_reconciled,
            Readiness::set_storage_writable,
            Readiness::set_stream_connected,
            Readiness::set_metadata_current,
            Readiness::set_context_capture_current,
            Readiness::set_rules_configuration_valid,
            Readiness::set_rules_sleeve_warm,
            Readiness::set_universe_witness_valid,
            Readiness::set_risk_witness_valid,
        ] {
            enabled(&mut readiness, true);
        }
        {
            let gates = readiness.market_gates_mut(&market).expect("market");
            gates.set_recovered(true);
            gates.set_executable_book(true);
            gates.set_common_features_warm(true);
            gates.set_data_quality_valid(true);
        }
        let now = trench_core::event::TimestampNs::new(1_000_000_000_000).expect("now");
        let fresh_bbo =
            trench_core::event::TimestampNs::new(i128::from(now.value() - 60_000_000_000))
                .expect("fresh bbo");
        let fresh_mids =
            trench_core::event::TimestampNs::new(i128::from(now.value() - 60_000_000_000))
                .expect("fresh mids");
        let stale = trench_core::event::TimestampNs::new(i128::from(now.value() - 400_000_000_000))
            .expect("stale");

        readiness.set_current_time(now);
        readiness.set_bbo_close_at(market.clone(), fresh_bbo);
        readiness.set_all_mids_at(market.clone(), fresh_mids);
        assert!(readiness.rules_entry_ready(&market));
        assert!(readiness.mandatory_exit_ready(&market));

        readiness.set_bbo_close_at(market.clone(), stale);
        assert_eq!(
            readiness.market_blockers(&market),
            BTreeSet::from([MarketBlocker::StaleBbo])
        );
        assert!(!readiness.rules_entry_ready(&market));
        assert!(readiness.mandatory_exit_ready(&market));

        readiness.set_bbo_close_at(market.clone(), fresh_bbo);
        readiness.set_all_mids_at(market.clone(), stale);
        assert_eq!(
            readiness.market_blockers(&market),
            BTreeSet::from([MarketBlocker::StaleAllMids])
        );
        assert!(!readiness.rules_entry_ready(&market));
        assert!(readiness.mandatory_exit_ready(&market));

        readiness.set_all_mids_at(market.clone(), fresh_mids);
        assert!(readiness.rules_entry_ready(&market));
    }
}
