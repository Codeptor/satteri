use rust_decimal_macros::dec;

use trench_core::book::OrderBook;
use trench_core::domain::{DomainError, LedgerId, Leverage, Market, Price, Usdc};
use trench_core::event::{BookLevel, BookSnapshot, DurationNs, MarketEvent, TimestampNs};
use trench_core::ledger::{EntryFill, ExitFill, LedgerError, LedgerState, MarkCosts, PositionSide};

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
    let event = MarketEvent::book_snapshot(
        at,
        at,
        Market::new("BTC").expect("market must be valid"),
        BookSnapshot::new(1, bids, asks),
    )
    .expect("book event must be valid");
    OrderBook::apply_snapshot(
        None,
        &event,
        DurationNs::new(0).expect("zero age must be valid"),
    )
    .expect("book must be valid")
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
        .mark_to_book(at, Some(&initial_book), MarkCosts::none())
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
        .mark_to_book(at, Some(&executable_book), MarkCosts::none())
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
        .mark_to_book(at, Some(&initial_book), MarkCosts::none())
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
        .mark_to_book(at, Some(&entry_book), MarkCosts::none())
        .expect("fresh book must be recorded")
        .into_state()
        .open_position(at, entry, usdc(dec!(0.50)))
        .expect("fresh book should allow entry")
        .into_state()
        .mark_to_book(at, Some(&exit_book), MarkCosts::none())
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
        .mark_to_book(at, Some(&executable_book), MarkCosts::none())
        .expect("fresh book must be recorded")
        .into_state()
        .open_position(at, long_entry(), usdc(dec!(0.50)))
        .expect("fresh book should allow entry")
        .into_state()
        .mark_to_book(at, Some(&executable_book), MarkCosts::none())
        .expect("mark must apply")
        .into_state();
    let valuation = open.equity();

    let stale = open
        .mark_to_book(at, None, MarkCosts::none())
        .expect("missing book must preserve state")
        .into_state();

    assert_eq!(stale.equity(), valuation);
    assert!(stale.executable_mark_stale());

    let flat_stale = LedgerState::new(LedgerId::RulesOnly, at)
        .expect("ledger must initialize")
        .mark_to_book(at, None, MarkCosts::none())
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
        .mark_to_book(at, Some(&executable_book), MarkCosts::none())
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
