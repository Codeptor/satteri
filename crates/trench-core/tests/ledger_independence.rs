use rust_decimal_macros::dec;

use trench_core::book::OrderBook;
use trench_core::domain::{DomainError, LedgerId, Leverage, Market, Price, Usdc};
use trench_core::event::{BookLevel, BookSnapshot, DurationNs, MarketEvent, TimestampNs};
use trench_core::ledger::{
    BookFreshness, BookFreshnessStatus, BookStaleReason, EntryFill, ExitFill, LedgerError,
    LedgerState, MarkCosts, PositionSide,
};

fn timestamp(value: i128) -> TimestampNs {
    TimestampNs::new(value).expect("test timestamp must be valid")
}

fn price(value: rust_decimal::Decimal) -> Price {
    Price::new(value).expect("test price must be valid")
}

fn usdc(value: rust_decimal::Decimal) -> Usdc {
    Usdc::new(value).expect("test USDC amount must be valid")
}

fn level(price_value: rust_decimal::Decimal, quantity: rust_decimal::Decimal) -> BookLevel {
    BookLevel::new(
        price(price_value),
        trench_core::domain::Quantity::new(quantity).expect("quantity must be valid"),
    )
}

fn book(at: TimestampNs, bids: Vec<BookLevel>, asks: Vec<BookLevel>) -> OrderBook {
    book_with_source(at, at, bids, asks)
}

fn book_with_source(
    event_time: TimestampNs,
    received_at: TimestampNs,
    bids: Vec<BookLevel>,
    asks: Vec<BookLevel>,
) -> OrderBook {
    let event = MarketEvent::book_snapshot(
        event_time,
        received_at,
        Market::new("BTC").expect("market must be valid"),
        BookSnapshot::new(1, bids, asks),
    )
    .expect("book event must be valid");
    OrderBook::apply_snapshot(
        None,
        &event,
        received_at
            .checked_duration_since(event_time)
            .expect("source event must not follow receipt"),
    )
    .expect("book must be valid")
}

fn freshness() -> BookFreshness {
    BookFreshness::new(DurationNs::new(1_000_000_000).expect("one-second age must be valid"))
}

fn long_entry() -> EntryFill {
    EntryFill::new(
        Market::new("BTC").expect("market must be valid"),
        PositionSide::Long,
        dec!(1),
        price(dec!(100)),
        Leverage::new(5).expect("leverage must be valid"),
        usdc(dec!(0.075)),
    )
    .expect("entry fill must be valid")
}

fn short_entry() -> EntryFill {
    EntryFill::new(
        Market::new("BTC").expect("market must be valid"),
        PositionSide::Short,
        dec!(1),
        price(dec!(100)),
        Leverage::new(5).expect("leverage must be valid"),
        usdc(dec!(0)),
    )
    .expect("entry fill must be valid")
}

#[test]
fn a_fill_in_one_ledger_cannot_mutate_another_ledger() {
    let at = timestamp(1_785_715_200_000_000_000);
    let initial_book = book(
        at,
        vec![level(dec!(99), dec!(2))],
        vec![level(dec!(101), dec!(2))],
    );
    let rules = LedgerState::new(LedgerId::RulesOnly, at).expect("rules ledger should initialize");
    let champion =
        LedgerState::new(LedgerId::MlChampion, at).expect("champion ledger should initialize");
    let champion_before = champion.clone();

    let rules_after = rules
        .mark_to_book(at, Some(&initial_book), freshness(), MarkCosts::none())
        .expect("fresh book should be recorded for rules")
        .into_state()
        .open_position(at, long_entry(), usdc(dec!(0.50)))
        .expect("rules ledger should accept its first isolated fill")
        .into_state();

    assert_ne!(rules_after, rules);
    assert_eq!(champion, champion_before);
    assert_eq!(champion.cash(), usdc(dec!(100)));
    assert!(champion.position().is_none());
}

#[test]
fn ledger_state_exposes_utc_anchors_and_frozen_breaker_counters() {
    let at = timestamp(1_785_715_200_000_000_000);
    let state = LedgerState::new(LedgerId::RulesOnly, at).expect("ledger must initialize");

    assert_eq!(state.high_water_equity(), usdc(dec!(100)));
    assert_eq!(state.entries_today(), 0);
    assert_eq!(state.consecutive_losses(), 0);
    assert_eq!(state.cooldown_until(), None);
    assert_eq!(state.daily_anchor().value() % 86_400_000_000_000, 0);
    assert_eq!(
        (state.weekly_anchor().value() + 259_200_000_000_000) % 604_800_000_000_000,
        0
    );
}

#[test]
fn entries_reject_negative_sizes_and_any_overlap() {
    let at = timestamp(1_785_715_200_000_000_000);
    assert!(matches!(
        EntryFill::new(
            Market::new("BTC").expect("market must be valid"),
            PositionSide::Long,
            dec!(-1),
            price(dec!(100)),
            Leverage::new(5).expect("leverage must be valid"),
            usdc(dec!(0)),
        ),
        Err(LedgerError::Domain(DomainError::NegativeQuantity))
    ));

    let executable_book = book(
        at,
        vec![level(dec!(99), dec!(2))],
        vec![level(dec!(101), dec!(2))],
    );
    let open = LedgerState::new(LedgerId::RulesOnly, at)
        .expect("ledger must initialize")
        .mark_to_book(at, Some(&executable_book), freshness(), MarkCosts::none())
        .expect("fresh book must be recorded")
        .into_state()
        .open_position(at, long_entry(), usdc(dec!(0.50)))
        .expect("first position should open")
        .into_state();

    assert!(matches!(
        open.open_position(at, long_entry(), usdc(dec!(0.50))),
        Err(LedgerError::PositionAlreadyOpen)
    ));
}

#[test]
fn long_marks_walk_bid_depth_and_apply_the_mandatory_exit_boundary() {
    let at = timestamp(1_785_715_200_000_000_000);
    let initial_book = book(
        at,
        vec![level(dec!(99), dec!(2))],
        vec![level(dec!(101), dec!(2))],
    );
    let exit_book = book(
        at,
        vec![level(dec!(99), dec!(0.4)), level(dec!(98), dec!(0.2))],
        vec![level(dec!(110), dec!(2))],
    );
    let flat = LedgerState::new(LedgerId::RulesOnly, at).expect("ledger must initialize");
    let fresh = flat
        .mark_to_book(at, Some(&initial_book), freshness(), MarkCosts::none())
        .expect("fresh flat book must be recorded")
        .into_state();
    let open = fresh
        .open_position(at, long_entry(), usdc(dec!(0.50)))
        .expect("fresh book should allow entry")
        .into_state();

    let marked = open
        .mark_to_book(
            at,
            Some(&exit_book),
            freshness(),
            MarkCosts::new(usdc(dec!(0.10)), usdc(dec!(0.20))),
        )
        .expect("executable mark must apply")
        .into_state();

    assert_eq!(marked.unrealized_pnl(), dec!(-2.292));
    assert_eq!(marked.equity(), usdc(dec!(97.633)));
    assert!(marked.liquidity_incomplete());
    assert!(!marked.executable_mark_stale());
    assert!(marked.breakers().daily_loss_tripped());
}

#[test]
fn short_marks_use_ask_depth_never_midpoint() {
    let at = timestamp(1_785_715_200_000_000_000);
    let entry_book = book(
        at,
        vec![level(dec!(99), dec!(2))],
        vec![level(dec!(101), dec!(2))],
    );
    let exit_book = book(
        at,
        vec![level(dec!(1), dec!(100))],
        vec![level(dec!(101), dec!(1))],
    );
    let entry = EntryFill::new(
        Market::new("BTC").expect("market must be valid"),
        PositionSide::Short,
        dec!(1),
        price(dec!(100)),
        Leverage::new(5).expect("leverage must be valid"),
        usdc(dec!(0)),
    )
    .expect("entry fill must be valid");
    let state = LedgerState::new(LedgerId::RulesOnly, at)
        .expect("ledger must initialize")
        .mark_to_book(at, Some(&entry_book), freshness(), MarkCosts::none())
        .expect("fresh book must be recorded")
        .into_state()
        .open_position(at, entry, usdc(dec!(0.50)))
        .expect("fresh book should allow entry")
        .into_state()
        .mark_to_book(at, Some(&exit_book), freshness(), MarkCosts::none())
        .expect("executable mark must apply")
        .into_state();

    assert_eq!(state.unrealized_pnl(), dec!(-1));
    assert_eq!(state.equity(), usdc(dec!(99)));
}

#[test]
fn stale_books_preserve_the_last_executable_value_and_block_new_entries() {
    let at = timestamp(1_785_715_200_000_000_000);
    let executable_book = book(
        at,
        vec![level(dec!(99), dec!(2))],
        vec![level(dec!(101), dec!(2))],
    );
    let open = LedgerState::new(LedgerId::RulesOnly, at)
        .expect("ledger must initialize")
        .mark_to_book(at, Some(&executable_book), freshness(), MarkCosts::none())
        .expect("fresh book must be recorded")
        .into_state()
        .open_position(at, long_entry(), usdc(dec!(0.50)))
        .expect("fresh book should allow entry")
        .into_state()
        .mark_to_book(at, Some(&executable_book), freshness(), MarkCosts::none())
        .expect("mark must apply")
        .into_state();
    let valuation = open.equity();

    let stale = open
        .mark_to_book(at, None, freshness(), MarkCosts::none())
        .expect("missing book must preserve state")
        .into_state();

    assert_eq!(stale.equity(), valuation);
    assert!(stale.executable_mark_stale());

    let flat_stale = LedgerState::new(LedgerId::RulesOnly, at)
        .expect("ledger must initialize")
        .mark_to_book(at, None, freshness(), MarkCosts::none())
        .expect("missing flat book must be recorded")
        .into_state();
    assert!(matches!(
        flat_stale.open_position(at, long_entry(), usdc(dec!(0.50))),
        Err(LedgerError::StaleExecutableMark)
    ));
}

#[test]
fn accounting_conserves_cash_margin_and_marked_value_across_fees_and_funding() {
    let at = timestamp(1_785_715_200_000_000_000);
    let executable_book = book(
        at,
        vec![level(dec!(99), dec!(2))],
        vec![level(dec!(101), dec!(2))],
    );
    let open = LedgerState::new(LedgerId::RulesOnly, at)
        .expect("ledger must initialize")
        .mark_to_book(at, Some(&executable_book), freshness(), MarkCosts::none())
        .expect("fresh book must be recorded")
        .into_state()
        .open_position(at, long_entry(), usdc(dec!(0.50)))
        .expect("fresh book should allow entry")
        .into_state()
        .apply_funding_debit(at, usdc(dec!(0.20)))
        .expect("funding debit must apply")
        .into_state()
        .mark_to_book(
            at,
            Some(&executable_book),
            freshness(),
            MarkCosts::new(usdc(dec!(0.10)), usdc(dec!(0))),
        )
        .expect("mark must apply")
        .into_state();

    assert_eq!(open.cash(), usdc(dec!(79.725)));
    assert_eq!(open.isolated_margin(), usdc(dec!(20)));
    assert_eq!(open.unrealized_pnl(), dec!(-1.10));
    assert_eq!(open.equity(), usdc(dec!(98.625)));
    assert_eq!(
        open.cash().value() + open.isolated_margin().value() + open.unrealized_pnl(),
        open.equity().value()
    );

    let closed = open
        .close_position(at, ExitFill::new(price(dec!(99)), usdc(dec!(0.10))))
        .expect("complete close must apply")
        .into_state();
    assert!(closed.position().is_none());
    assert_eq!(closed.equity(), usdc(dec!(98.625)));
    assert_eq!(closed.realized_pnl(), dec!(-1));
    assert_eq!(closed.fees_paid(), usdc(dec!(0.175)));
    assert_eq!(closed.funding_paid(), usdc(dec!(0.20)));
    assert_eq!(
        closed.cash().value() + closed.isolated_margin().value() + closed.unrealized_pnl(),
        closed.equity().value()
    );
}

#[test]
fn source_timestamps_make_both_long_and_short_marks_fresh_at_the_inclusive_bound() {
    let at = timestamp(1_785_715_200_000_000_010);
    let source_book = book_with_source(
        timestamp(1_785_715_200_000_000_000),
        timestamp(1_785_715_200_000_000_005),
        vec![level(dec!(99), dec!(2))],
        vec![level(dec!(101), dec!(2))],
    );
    let max_age = BookFreshness::new(DurationNs::new(10).expect("max age must be valid"));

    let long = LedgerState::new(LedgerId::RulesOnly, at)
        .expect("ledger must initialize")
        .mark_to_book(at, Some(&source_book), max_age, MarkCosts::none())
        .expect("inclusive source age must be fresh")
        .into_state()
        .open_position(at, long_entry(), usdc(dec!(0.50)))
        .expect("fresh source book must allow long entry")
        .into_state()
        .mark_to_book(at, Some(&source_book), max_age, MarkCosts::none())
        .expect("fresh source book must mark long")
        .into_state();
    let short = LedgerState::new(LedgerId::MlChampion, at)
        .expect("ledger must initialize")
        .mark_to_book(at, Some(&source_book), max_age, MarkCosts::none())
        .expect("inclusive source age must be fresh")
        .into_state()
        .open_position(at, short_entry(), usdc(dec!(0.50)))
        .expect("fresh source book must allow short entry")
        .into_state()
        .mark_to_book(at, Some(&source_book), max_age, MarkCosts::none())
        .expect("fresh source book must mark short")
        .into_state();

    assert!(matches!(
        long.book_freshness(),
        BookFreshnessStatus::Fresh { .. }
    ));
    assert!(matches!(
        short.book_freshness(),
        BookFreshnessStatus::Fresh { .. }
    ));
    assert!(!long.executable_mark_stale());
    assert!(!short.executable_mark_stale());
}

#[test]
fn stale_long_source_book_preserves_valuation_and_blocks_a_later_entry() {
    let source_at = timestamp(1_785_715_200_000_000_000);
    let stale_at = timestamp(1_785_715_200_000_000_006);
    let source_book = book_with_source(
        source_at,
        source_at,
        vec![level(dec!(99), dec!(2))],
        vec![level(dec!(101), dec!(2))],
    );
    let max_age = BookFreshness::new(DurationNs::new(5).expect("max age must be valid"));
    let open = LedgerState::new(LedgerId::RulesOnly, source_at)
        .expect("ledger must initialize")
        .mark_to_book(source_at, Some(&source_book), max_age, MarkCosts::none())
        .expect("fresh source book must be recorded")
        .into_state()
        .open_position(source_at, long_entry(), usdc(dec!(0.50)))
        .expect("fresh source book must allow entry")
        .into_state()
        .mark_to_book(source_at, Some(&source_book), max_age, MarkCosts::none())
        .expect("fresh source book must value position")
        .into_state();
    let valuation = open.equity();

    let stale = open
        .mark_to_book(stale_at, Some(&source_book), max_age, MarkCosts::none())
        .expect("stale source book must record a stale transition")
        .into_state();
    assert_eq!(stale.equity(), valuation);
    assert!(stale.executable_mark_stale());
    assert!(matches!(
        stale.book_freshness(),
        BookFreshnessStatus::Stale {
            reason: BookStaleReason::TooOld,
            ..
        }
    ));

    let flat = stale
        .close_position(stale_at, ExitFill::new(price(dec!(99)), usdc(dec!(0))))
        .expect("reduce-only close should remain possible")
        .into_state();
    assert!(matches!(
        flat.open_position(stale_at, long_entry(), usdc(dec!(0.50))),
        Err(LedgerError::StaleExecutableMark)
    ));
}

#[test]
fn future_short_source_book_marks_stale_without_using_its_ask() {
    let at = timestamp(1_785_715_200_000_000_000);
    let fresh_book = book(
        at,
        vec![level(dec!(99), dec!(2))],
        vec![level(dec!(101), dec!(2))],
    );
    let future_book = book_with_source(
        timestamp(1_785_715_200_000_000_001),
        timestamp(1_785_715_200_000_000_001),
        vec![level(dec!(1), dec!(100))],
        vec![level(dec!(101), dec!(1))],
    );
    let max_age = BookFreshness::new(DurationNs::new(5).expect("max age must be valid"));
    let open = LedgerState::new(LedgerId::MlChampion, at)
        .expect("ledger must initialize")
        .mark_to_book(at, Some(&fresh_book), max_age, MarkCosts::none())
        .expect("fresh source book must be recorded")
        .into_state()
        .open_position(at, short_entry(), usdc(dec!(0.50)))
        .expect("fresh source book must allow entry")
        .into_state()
        .mark_to_book(at, Some(&fresh_book), max_age, MarkCosts::none())
        .expect("fresh source book must value short")
        .into_state();
    let valuation = open.equity();

    let stale = open
        .mark_to_book(at, Some(&future_book), max_age, MarkCosts::none())
        .expect("future source book must record a stale transition")
        .into_state();

    assert_eq!(stale.equity(), valuation);
    assert!(stale.executable_mark_stale());
    assert!(matches!(
        stale.book_freshness(),
        BookFreshnessStatus::Stale {
            reason: BookStaleReason::FutureEventTime,
            ..
        }
    ));
}

#[test]
fn replay_cannot_look_ahead_to_a_book_received_after_the_transition() {
    let at = timestamp(1_785_715_200_000_000_010);
    let look_ahead_book = book_with_source(
        timestamp(1_785_715_200_000_000_009),
        timestamp(1_785_715_200_000_000_011),
        vec![level(dec!(99), dec!(2))],
        vec![level(dec!(101), dec!(2))],
    );
    let max_age = BookFreshness::new(DurationNs::new(10).expect("max age must be valid"));
    let stale = LedgerState::new(LedgerId::RulesOnly, at)
        .expect("ledger must initialize")
        .mark_to_book(at, Some(&look_ahead_book), max_age, MarkCosts::none())
        .expect("look-ahead source book must record a stale transition")
        .into_state();

    assert!(stale.executable_mark_stale());
    assert!(matches!(
        stale.book_freshness(),
        BookFreshnessStatus::Stale {
            reason: BookStaleReason::FutureReceiptTime,
            ..
        }
    ));
    assert!(matches!(
        stale.open_position(at, long_entry(), usdc(dec!(0.50))),
        Err(LedgerError::StaleExecutableMark)
    ));
}

#[test]
fn entry_cannot_reuse_a_source_book_after_its_explicit_age_bound_expires() {
    let source_at = timestamp(1_785_715_200_000_000_000);
    let entry_at = timestamp(1_785_715_200_000_000_006);
    let source_book = book(
        source_at,
        vec![level(dec!(99), dec!(2))],
        vec![level(dec!(101), dec!(2))],
    );
    let max_age = BookFreshness::new(DurationNs::new(5).expect("max age must be valid"));
    let marked = LedgerState::new(LedgerId::RulesOnly, source_at)
        .expect("ledger must initialize")
        .mark_to_book(source_at, Some(&source_book), max_age, MarkCosts::none())
        .expect("source book must initially be fresh")
        .into_state();

    assert!(matches!(
        marked.open_position(entry_at, long_entry(), usdc(dec!(0.50))),
        Err(LedgerError::StaleExecutableMark)
    ));
}
