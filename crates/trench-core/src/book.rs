//! Immutable validated order books and deterministic visible-depth walking.

use blake3::Hasher;
use rust_decimal::Decimal;
use thiserror::Error;

use crate::domain::{Bps, DomainError, EventId, Market, Price, Quantity, Side, Usdc};
use crate::event::{BookLevel, DurationNs, EventError, MarketEvent, MarketEventKind, TimestampNs};

const BPS_DENOMINATOR: u32 = 10_000;

/// One side of an order book.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BookSide {
    /// Resting buy liquidity.
    Bid,
    /// Resting sell liquidity.
    Ask,
}

/// Snapshot validation or deterministic depth-walk failure.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum BookError {
    /// The supplied event was not a full book snapshot.
    #[error("market event must contain a book snapshot")]
    ExpectedSnapshot,
    /// A transition attempted to replace a book with a different market.
    #[error("snapshot market must match the prior order book")]
    MarketChanged,
    /// Exchange time did not advance strictly beyond the prior snapshot.
    #[error("snapshot exchange time must advance strictly beyond the prior snapshot")]
    NonMonotonicTime {
        /// Prior authoritative exchange time.
        previous: TimestampNs,
        /// Rejected current exchange time.
        current: TimestampNs,
    },
    /// Caller-supplied freshness was exceeded.
    #[error("snapshot age exceeds the caller-supplied freshness limit")]
    Stale {
        /// Observed receipt latency.
        age: DurationNs,
        /// Maximum caller-approved age.
        max_age: DurationNs,
    },
    /// One required side had no visible liquidity.
    #[error("{side:?} side must contain at least one level")]
    EmptySide {
        /// Empty side.
        side: BookSide,
    },
    /// A visible level had zero quantity.
    #[error("{side:?} level {index} must have positive quantity")]
    ZeroQuantity {
        /// Invalid side.
        side: BookSide,
        /// Zero-based level index.
        index: usize,
    },
    /// A side repeated a price.
    #[error("{side:?} side contains a duplicate price")]
    DuplicatePrice {
        /// Invalid side.
        side: BookSide,
        /// Repeated price.
        price: Price,
    },
    /// Bids were not descending or asks were not ascending.
    #[error("{side:?} levels are not strictly price sorted")]
    Unsorted {
        /// Invalid side.
        side: BookSide,
    },
    /// Best bid was equal to or above best ask.
    #[error("book is crossed or locked")]
    CrossedOrLocked {
        /// Rejected best bid.
        best_bid: Price,
        /// Rejected best ask.
        best_ask: Price,
    },
    /// A walk requested no quote notional.
    #[error("requested quote notional must be greater than zero")]
    ZeroRequestedQuote,
    /// A limit band reached or exceeded 100 percent.
    #[error("limit band must be less than 10000 bps")]
    InvalidLimitBand {
        /// Rejected band.
        band: Bps,
    },
    /// Checked decimal arithmetic failed.
    #[error("checked arithmetic failed while calculating {operation}")]
    Arithmetic {
        /// Failed calculation.
        operation: &'static str,
    },
    /// A checked domain conversion failed.
    #[error(transparent)]
    Domain(#[from] DomainError),
    /// Explicit event-time arithmetic failed.
    #[error(transparent)]
    Event(#[from] EventError),
}

/// Immutable, fully validated visible L2 snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderBook {
    event_id: EventId,
    event_time: TimestampNs,
    received_at: TimestampNs,
    market: Market,
    sequence: u64,
    bids: Vec<BookLevel>,
    asks: Vec<BookLevel>,
}

impl OrderBook {
    /// Applies one full snapshot as a pure transition from optional prior state.
    ///
    /// `max_age` is supplied by the caller so replay and runtime use the same
    /// explicit freshness policy without reading wall-clock time.
    ///
    /// # Errors
    ///
    /// Rejects wrong event kinds, stale/nonmonotonic/mismatched snapshots,
    /// empty or invalid levels, and crossed or locked books.
    pub fn apply_snapshot(
        previous: Option<&Self>,
        event: &MarketEvent,
        max_age: DurationNs,
    ) -> Result<Self, BookError> {
        let MarketEventKind::BookSnapshot(snapshot) = event.kind() else {
            return Err(BookError::ExpectedSnapshot);
        };

        if let Some(previous) = previous {
            if previous.market != *event.market() {
                return Err(BookError::MarketChanged);
            }
            if event.event_time() <= previous.event_time {
                return Err(BookError::NonMonotonicTime {
                    previous: previous.event_time,
                    current: event.event_time(),
                });
            }
        }

        let age = event
            .received_at()
            .checked_duration_since(event.event_time())?;
        if age > max_age {
            return Err(BookError::Stale { age, max_age });
        }

        validate_side(BookSide::Bid, snapshot.bids())?;
        validate_side(BookSide::Ask, snapshot.asks())?;

        let best_bid = snapshot
            .bids()
            .first()
            .ok_or(BookError::EmptySide {
                side: BookSide::Bid,
            })?
            .price();
        let best_ask = snapshot
            .asks()
            .first()
            .ok_or(BookError::EmptySide {
                side: BookSide::Ask,
            })?
            .price();
        if best_bid >= best_ask {
            return Err(BookError::CrossedOrLocked { best_bid, best_ask });
        }

        Ok(Self {
            event_id: event.event_id().clone(),
            event_time: event.event_time(),
            received_at: event.received_at(),
            market: event.market().clone(),
            sequence: snapshot.sequence(),
            bids: snapshot.bids().to_vec(),
            asks: snapshot.asks().to_vec(),
        })
    }

    /// Returns a canonical commitment over this validated full-depth source book.
    #[must_use]
    pub fn commitment_digest(&self) -> String {
        let mut hasher = Hasher::new_derive_key("trench.order-book.v1");
        for value in [
            self.event_id.as_str().to_owned(),
            self.event_time.value().to_string(),
            self.received_at.value().to_string(),
            self.market.as_str().to_owned(),
            self.sequence.to_string(),
        ] {
            hasher.update(&(value.len() as u64).to_be_bytes());
            hasher.update(value.as_bytes());
        }
        for (side, levels) in [(b"bid".as_slice(), &self.bids), (b"ask", &self.asks)] {
            hasher.update(side);
            for level in levels {
                for value in [
                    level.price().value().to_string(),
                    level.quantity().value().to_string(),
                ] {
                    hasher.update(&(value.len() as u64).to_be_bytes());
                    hasher.update(value.as_bytes());
                }
            }
        }
        hasher.finalize().to_hex().to_string()
    }

    /// Walks visible liquidity without mutating this snapshot.
    ///
    /// Buys consume asks no higher than the inclusive band above best ask;
    /// sells consume bids no lower than the inclusive band below best bid.
    ///
    /// # Errors
    ///
    /// Rejects zero requested quote, bands at or above 100 percent, empty
    /// relevant sides, and unrepresentable checked decimal arithmetic.
    pub fn walk(
        &self,
        side: Side,
        requested_quote: Usdc,
        limit_band: Bps,
    ) -> Result<WalkResult, BookError> {
        if requested_quote.value().is_zero() {
            return Err(BookError::ZeroRequestedQuote);
        }
        if limit_band.value() >= Decimal::from(BPS_DENOMINATOR) {
            return Err(BookError::InvalidLimitBand { band: limit_band });
        }

        let (book_side, levels) = match side {
            Side::Buy => (BookSide::Ask, self.asks.as_slice()),
            Side::Sell => (BookSide::Bid, self.bids.as_slice()),
        };
        let best = levels
            .first()
            .ok_or(BookError::EmptySide { side: book_side })?
            .price();
        let limit = checked_limit_price(best, side, limit_band)?;
        let mut remaining = requested_quote;
        let mut fills = Vec::new();

        for level in levels
            .iter()
            .take_while(|level| is_inside_limit(level.price(), side, limit))
        {
            let visible_quote = checked_notional(*level)?;
            let fill_quantity = if visible_quote <= remaining {
                level.quantity()
            } else {
                checked_partial_quantity(remaining, level.price())?
            };
            if fill_quantity.value().is_zero() {
                break;
            }
            let fill_quote = checked_notional(BookLevel::new(level.price(), fill_quantity))?;
            if fill_quote > remaining {
                return Err(BookError::Arithmetic {
                    operation: "bounded partial fill",
                });
            }
            remaining = checked_sub_usdc(remaining, fill_quote)?;
            fills.push(LevelFill::new(level.price(), fill_quantity, fill_quote));
            if remaining.value().is_zero() {
                break;
            }
        }

        let filled_quote = checked_sub_usdc(requested_quote, remaining)?;
        Ok(WalkResult {
            fills,
            filled_quote,
            remaining_quote: remaining,
        })
    }

    /// Returns the source event identity.
    #[must_use]
    pub const fn event_id(&self) -> &EventId {
        &self.event_id
    }

    /// Returns authoritative snapshot exchange time.
    #[must_use]
    pub const fn event_time(&self) -> TimestampNs {
        self.event_time
    }

    /// Returns snapshot receipt time.
    #[must_use]
    pub const fn received_at(&self) -> TimestampNs {
        self.received_at
    }

    /// Returns the snapshot market.
    #[must_use]
    pub const fn market(&self) -> &Market {
        &self.market
    }

    /// Returns the source sequence.
    #[must_use]
    pub const fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Returns strictly descending bids.
    #[must_use]
    pub fn bids(&self) -> &[BookLevel] {
        &self.bids
    }

    /// Returns strictly ascending asks.
    #[must_use]
    pub fn asks(&self) -> &[BookLevel] {
        &self.asks
    }
}

/// One exact per-level fill from an immutable depth walk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LevelFill {
    price: Price,
    quantity: Quantity,
    quote: Usdc,
}

impl LevelFill {
    const fn new(price: Price, quantity: Quantity, quote: Usdc) -> Self {
        Self {
            price,
            quantity,
            quote,
        }
    }

    /// Returns execution price.
    #[must_use]
    pub const fn price(&self) -> Price {
        self.price
    }

    /// Returns filled asset quantity.
    #[must_use]
    pub const fn quantity(&self) -> Quantity {
        self.quantity
    }

    /// Returns exact filled quote notional.
    #[must_use]
    pub const fn quote(&self) -> Usdc {
        self.quote
    }
}

/// Exact result of a bounded immutable depth walk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalkResult {
    fills: Vec<LevelFill>,
    filled_quote: Usdc,
    remaining_quote: Usdc,
}

impl WalkResult {
    /// Returns ordered per-level fills.
    #[must_use]
    pub fn fills(&self) -> &[LevelFill] {
        &self.fills
    }

    /// Returns total quote notional filled.
    #[must_use]
    pub const fn filled_quote(&self) -> Usdc {
        self.filled_quote
    }

    /// Returns unfilled requested quote notional.
    #[must_use]
    pub const fn remaining_quote(&self) -> Usdc {
        self.remaining_quote
    }

    /// Returns the number of visible levels consumed.
    #[must_use]
    pub fn levels_consumed(&self) -> usize {
        self.fills.len()
    }
}

fn validate_side(side: BookSide, levels: &[BookLevel]) -> Result<(), BookError> {
    if levels.is_empty() {
        return Err(BookError::EmptySide { side });
    }
    if let Some((index, _)) = levels
        .iter()
        .enumerate()
        .find(|(_, level)| level.quantity().value().is_zero())
    {
        return Err(BookError::ZeroQuantity { side, index });
    }
    for pair in levels.windows(2) {
        let left = pair[0].price();
        let right = pair[1].price();
        if left == right {
            return Err(BookError::DuplicatePrice { side, price: left });
        }
        let sorted = match side {
            BookSide::Bid => left > right,
            BookSide::Ask => left < right,
        };
        if !sorted {
            return Err(BookError::Unsorted { side });
        }
    }
    Ok(())
}

fn checked_limit_price(best: Price, side: Side, band: Bps) -> Result<Price, BookError> {
    let adjustment = best
        .value()
        .checked_mul(band.value())
        .and_then(|value| value.checked_div(Decimal::from(BPS_DENOMINATOR)))
        .ok_or(BookError::Arithmetic {
            operation: "walk limit adjustment",
        })?;
    let limit = match side {
        Side::Buy => best.value().checked_add(adjustment),
        Side::Sell => best.value().checked_sub(adjustment),
    }
    .ok_or(BookError::Arithmetic {
        operation: "walk limit price",
    })?;
    Price::new(limit).map_err(BookError::from)
}

fn is_inside_limit(price: Price, side: Side, limit: Price) -> bool {
    match side {
        Side::Buy => price <= limit,
        Side::Sell => price >= limit,
    }
}

fn checked_notional(level: BookLevel) -> Result<Usdc, BookError> {
    level
        .price()
        .checked_notional(level.quantity())
        .map_err(|_| BookError::Arithmetic {
            operation: "visible level notional",
        })
}

fn checked_partial_quantity(remaining: Usdc, price: Price) -> Result<Quantity, BookError> {
    let mut value = remaining
        .value()
        .checked_div(price.value())
        .ok_or(BookError::Arithmetic {
            operation: "partial fill quantity",
        })?;
    let initial = Quantity::new(value)?;
    if price.checked_notional(initial)? > remaining {
        let quantum = Decimal::new(1, value.scale());
        value = value.checked_sub(quantum).ok_or(BookError::Arithmetic {
            operation: "bounded partial fill quantity",
        })?;
    }
    Quantity::new(value).map_err(BookError::from)
}

fn checked_sub_usdc(left: Usdc, right: Usdc) -> Result<Usdc, BookError> {
    let value = left
        .value()
        .checked_sub(right.value())
        .ok_or(BookError::Arithmetic {
            operation: "remaining quote notional",
        })?;
    Usdc::new(value).map_err(BookError::from)
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use super::{BookError, BookSide, OrderBook};
    use crate::domain::{Bps, Market, Price, Quantity, Side, Usdc};
    use crate::event::{BookLevel, BookSnapshot, DurationNs, MarketEvent, TimestampNs};

    fn timestamp(value: i128) -> TimestampNs {
        TimestampNs::new(value).expect("test timestamp must be valid")
    }

    fn duration(value: i128) -> DurationNs {
        DurationNs::new(value).expect("test duration must be valid")
    }

    fn price(value: Decimal) -> Price {
        Price::new(value).expect("test price must be valid")
    }

    fn quantity(value: Decimal) -> Quantity {
        Quantity::new(value).expect("test quantity must be valid")
    }

    fn usdc(value: Decimal) -> Usdc {
        Usdc::new(value).expect("test USDC must be valid")
    }

    fn bps(value: Decimal) -> Bps {
        Bps::new(value).expect("test bps must be valid")
    }

    fn level(price_value: Decimal, quantity_value: Decimal) -> BookLevel {
        BookLevel::new(price(price_value), quantity(quantity_value))
    }

    fn snapshot_event_for(
        market_name: &str,
        event_time: i128,
        received_at: i128,
        sequence: u64,
        bids: Vec<BookLevel>,
        asks: Vec<BookLevel>,
    ) -> MarketEvent {
        MarketEvent::book_snapshot(
            timestamp(event_time),
            timestamp(received_at),
            Market::new(market_name).expect("test market must be valid"),
            BookSnapshot::new(sequence, bids, asks),
        )
        .expect("test snapshot event must have valid common fields")
    }

    fn snapshot_event(
        event_time: i128,
        received_at: i128,
        bids: Vec<BookLevel>,
        asks: Vec<BookLevel>,
    ) -> MarketEvent {
        snapshot_event_for("BTC", event_time, received_at, 1, bids, asks)
    }

    fn valid_book() -> OrderBook {
        let event = snapshot_event(
            100,
            100,
            vec![level(dec!(99), dec!(5)), level(dec!(98), dec!(5))],
            vec![level(dec!(100), dec!(1)), level(dec!(101), dec!(1))],
        );
        OrderBook::apply_snapshot(None, &event, duration(0))
            .expect("test snapshot must produce a book")
    }

    #[test]
    fn apply_snapshot_rejects_crossed_or_locked_books() {
        for (bid, ask) in [(dec!(100), dec!(100)), (dec!(101), dec!(100))] {
            let event = snapshot_event(
                100,
                100,
                vec![level(bid, dec!(1))],
                vec![level(ask, dec!(1))],
            );

            assert_eq!(
                OrderBook::apply_snapshot(None, &event, duration(0)),
                Err(BookError::CrossedOrLocked {
                    best_bid: price(bid),
                    best_ask: price(ask),
                })
            );
        }
    }

    #[test]
    fn apply_snapshot_rejects_stale_books_against_caller_freshness() {
        let event = snapshot_event(
            100,
            201,
            vec![level(dec!(99), dec!(1))],
            vec![level(dec!(100), dec!(1))],
        );

        assert_eq!(
            OrderBook::apply_snapshot(None, &event, duration(100)),
            Err(BookError::Stale {
                age: duration(101),
                max_age: duration(100),
            })
        );
    }

    #[test]
    fn apply_snapshot_accepts_freshness_at_the_inclusive_boundary() {
        let event = snapshot_event(
            100,
            200,
            vec![level(dec!(99), dec!(1))],
            vec![level(dec!(100), dec!(1))],
        );

        assert!(OrderBook::apply_snapshot(None, &event, duration(100)).is_ok());
    }

    #[test]
    fn apply_snapshot_rejects_nonmonotonic_exchange_time() {
        let prior_event = snapshot_event(
            100,
            100,
            vec![level(dec!(99), dec!(1))],
            vec![level(dec!(100), dec!(1))],
        );
        let prior = OrderBook::apply_snapshot(None, &prior_event, duration(0))
            .expect("prior book must be valid");

        for event_time in [99, 100] {
            let event = snapshot_event(
                event_time,
                101,
                vec![level(dec!(99), dec!(1))],
                vec![level(dec!(100), dec!(1))],
            );
            assert_eq!(
                OrderBook::apply_snapshot(Some(&prior), &event, duration(2)),
                Err(BookError::NonMonotonicTime {
                    previous: timestamp(100),
                    current: timestamp(event_time),
                })
            );
        }
    }

    #[test]
    fn apply_snapshot_rejects_market_changes() {
        let prior = valid_book();
        let event = snapshot_event_for(
            "ETH",
            101,
            101,
            2,
            vec![level(dec!(99), dec!(1))],
            vec![level(dec!(100), dec!(1))],
        );

        assert_eq!(
            OrderBook::apply_snapshot(Some(&prior), &event, duration(0)),
            Err(BookError::MarketChanged)
        );
    }

    #[test]
    fn apply_snapshot_rejects_unsorted_sides() {
        let cases = [
            (
                vec![level(dec!(98), dec!(1)), level(dec!(99), dec!(1))],
                vec![level(dec!(100), dec!(1))],
                BookSide::Bid,
            ),
            (
                vec![level(dec!(99), dec!(1))],
                vec![level(dec!(101), dec!(1)), level(dec!(100), dec!(1))],
                BookSide::Ask,
            ),
        ];

        for (bids, asks, side) in cases {
            let event = snapshot_event(100, 100, bids, asks);
            assert_eq!(
                OrderBook::apply_snapshot(None, &event, duration(0)),
                Err(BookError::Unsorted { side })
            );
        }
    }

    #[test]
    fn apply_snapshot_rejects_duplicate_prices() {
        let cases = [
            (
                vec![level(dec!(99), dec!(1)), level(dec!(99), dec!(2))],
                vec![level(dec!(100), dec!(1))],
                BookSide::Bid,
                price(dec!(99)),
            ),
            (
                vec![level(dec!(99), dec!(1))],
                vec![level(dec!(100), dec!(1)), level(dec!(100), dec!(2))],
                BookSide::Ask,
                price(dec!(100)),
            ),
        ];

        for (bids, asks, side, duplicate_price) in cases {
            let event = snapshot_event(100, 100, bids, asks);
            assert_eq!(
                OrderBook::apply_snapshot(None, &event, duration(0)),
                Err(BookError::DuplicatePrice {
                    side,
                    price: duplicate_price,
                })
            );
        }
    }

    #[test]
    fn apply_snapshot_rejects_zero_quantity_levels() {
        let cases = [
            (
                vec![level(dec!(99), Decimal::ZERO)],
                vec![level(dec!(100), dec!(1))],
                BookSide::Bid,
            ),
            (
                vec![level(dec!(99), dec!(1))],
                vec![level(dec!(100), Decimal::ZERO)],
                BookSide::Ask,
            ),
        ];

        for (bids, asks, side) in cases {
            let event = snapshot_event(100, 100, bids, asks);
            assert_eq!(
                OrderBook::apply_snapshot(None, &event, duration(0)),
                Err(BookError::ZeroQuantity { side, index: 0 })
            );
        }
    }

    #[test]
    fn apply_snapshot_rejects_empty_book_sides() {
        let cases = [
            (vec![], vec![level(dec!(100), dec!(1))], BookSide::Bid),
            (vec![level(dec!(99), dec!(1))], vec![], BookSide::Ask),
        ];

        for (bids, asks, side) in cases {
            let event = snapshot_event(100, 100, bids, asks);
            assert_eq!(
                OrderBook::apply_snapshot(None, &event, duration(0)),
                Err(BookError::EmptySide { side })
            );
        }
    }

    #[test]
    fn buy_walk_matches_the_approved_visible_depth_example() {
        let book = valid_book();

        let fill = book
            .walk(Side::Buy, usdc(dec!(150)), bps(dec!(50)))
            .expect("walk must succeed");

        assert_eq!(fill.filled_quote(), usdc(dec!(100)));
        assert_eq!(fill.remaining_quote(), usdc(dec!(50)));
        assert_eq!(fill.levels_consumed(), 1);
        assert_eq!(
            fill.fills(),
            &[super::LevelFill::new(
                price(dec!(100)),
                quantity(dec!(1)),
                usdc(dec!(100)),
            )]
        );
    }

    #[test]
    fn sell_walk_consumes_bids_not_asks() {
        let event = snapshot_event(
            100,
            100,
            vec![level(dec!(100), dec!(1)), level(dec!(99), dec!(1))],
            vec![level(dec!(101), dec!(10))],
        );
        let book =
            OrderBook::apply_snapshot(None, &event, duration(0)).expect("book must be valid");

        let fill = book
            .walk(Side::Sell, usdc(dec!(150)), bps(dec!(50)))
            .expect("walk must succeed");

        assert_eq!(fill.filled_quote(), usdc(dec!(100)));
        assert_eq!(fill.remaining_quote(), usdc(dec!(50)));
        assert_eq!(fill.levels_consumed(), 1);
    }

    #[test]
    fn walk_includes_a_level_exactly_on_the_limit_band() {
        let event = snapshot_event(
            100,
            100,
            vec![level(dec!(99), dec!(1))],
            vec![level(dec!(100), dec!(1)), level(dec!(100.5), dec!(1))],
        );
        let book =
            OrderBook::apply_snapshot(None, &event, duration(0)).expect("book must be valid");

        let fill = book
            .walk(Side::Buy, usdc(dec!(500)), bps(dec!(50)))
            .expect("walk must succeed");

        assert_eq!(fill.filled_quote(), usdc(dec!(200.5)));
        assert_eq!(fill.levels_consumed(), 2);
    }

    #[test]
    fn walk_rejects_zero_requested_quote() {
        assert_eq!(
            valid_book().walk(Side::Buy, usdc(Decimal::ZERO), bps(dec!(50))),
            Err(BookError::ZeroRequestedQuote)
        );
    }

    #[test]
    fn walk_rejects_a_limit_band_at_or_above_one_hundred_percent() {
        for band in [dec!(10000), dec!(10001)] {
            assert_eq!(
                valid_book().walk(Side::Buy, usdc(dec!(1)), bps(band)),
                Err(BookError::InvalidLimitBand { band: bps(band) })
            );
        }
    }

    #[test]
    fn repeated_walk_is_deterministic_and_does_not_mutate_the_book() {
        let book = valid_book();
        let before = book.clone();

        let first = book
            .walk(Side::Buy, usdc(dec!(150)), bps(dec!(50)))
            .expect("first walk must succeed");
        let second = book
            .walk(Side::Buy, usdc(dec!(150)), bps(dec!(50)))
            .expect("replayed walk must succeed");

        assert_eq!(first, second);
        assert_eq!(book, before);
    }

    #[test]
    fn applying_a_new_snapshot_preserves_the_prior_book() {
        let prior = valid_book();
        let before = prior.clone();
        let next_event = snapshot_event_for(
            "BTC",
            101,
            101,
            2,
            vec![level(dec!(100), dec!(2))],
            vec![level(dec!(101), dec!(2))],
        );

        let next = OrderBook::apply_snapshot(Some(&prior), &next_event, duration(0))
            .expect("next book must be valid");

        assert_eq!(prior, before);
        assert_eq!(next.event_time(), timestamp(101));
        assert_eq!(next.bids(), &[level(dec!(100), dec!(2))]);
    }

    proptest! {
        #[test]
        fn walk_never_exceeds_request_or_eligible_visible_depth(
            base in 100_u64..10_000,
            quantities in prop::collection::vec(1_u64..100, 1..7),
            requested in 1_u64..100_000,
            band in 0_u32..10_000,
            buy in any::<bool>(),
        ) {
            let bids: Vec<_> = quantities
                .iter()
                .enumerate()
                .map(|(index, value)| {
                    level(
                        Decimal::from(base - u64::try_from(index).expect("small index")),
                        Decimal::from(*value),
                    )
                })
                .collect();
            let asks: Vec<_> = quantities
                .iter()
                .enumerate()
                .map(|(index, value)| {
                    level(
                        Decimal::from(base + 2 + u64::try_from(index).expect("small index")),
                        Decimal::from(*value),
                    )
                })
                .collect();
            let event = snapshot_event(100, 100, bids, asks);
            let book = OrderBook::apply_snapshot(None, &event, duration(0))
                .expect("generated book must be valid");
            let side = if buy { Side::Buy } else { Side::Sell };
            let band = bps(Decimal::from(band));
            let requested = usdc(Decimal::from(requested));
            let levels = if buy { book.asks() } else { book.bids() };
            let best = levels[0].price().value();
            let adjustment = best * band.value() / Decimal::from(10_000);
            let limit = if buy { best + adjustment } else { best - adjustment };
            let eligible_depth: Decimal = levels
                .iter()
                .take_while(|level| {
                    if buy {
                        level.price().value() <= limit
                    } else {
                        level.price().value() >= limit
                    }
                })
                .map(|level| level.price().value() * level.quantity().value())
                .sum();
            let before = book.clone();

            let result = book.walk(side, requested, band)
                .expect("bounded generated walk must succeed");

            prop_assert!(result.filled_quote().value() <= requested.value());
            prop_assert!(result.filled_quote().value() <= eligible_depth);
            prop_assert_eq!(
                result.filled_quote().value() + result.remaining_quote().value(),
                requested.value()
            );
            prop_assert_eq!(book, before);
        }
    }
}
