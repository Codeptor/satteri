use rust_decimal_macros::dec;
use trench_core::{
    candle::CandleAggregator,
    domain::{Market, Price, Quantity, Side},
    event::{
        BookLevel, BookSnapshot, CandleInterval, CompletedCandle, MarketEvent, TimestampNs, Trade,
    },
    validation::TimeRange,
};
use trench_hyperliquid::{
    Candle, CandleForTest, CandleInterval as VenueCandleInterval, GapRecovery, RecoveryEvidence,
    RecoveryResult, candle_for_test, recovery_request_from_events_for_test,
};

pub const FIFTEEN_MINUTES_NS: i64 = 900_000_000_000;
pub const ONE_HOUR_NS: i64 = 3_600_000_000_000;

pub fn timestamp(value: i64) -> TimestampNs {
    TimestampNs::new(i128::from(value)).expect("fixture timestamp")
}

pub fn market() -> Market {
    Market::new("SOL").expect("fixture market")
}

pub fn range(start: i64, end: i64) -> TimeRange {
    TimeRange::new(timestamp(start), timestamp(end)).expect("fixture range")
}

fn price() -> Price {
    Price::new(dec!(100)).expect("fixture price")
}

fn quantity(value: rust_decimal::Decimal) -> Quantity {
    Quantity::new(value).expect("fixture quantity")
}

pub fn trade(event_time: i64, received_at: i64, trade_id: u64) -> MarketEvent {
    MarketEvent::trade(
        timestamp(event_time),
        timestamp(received_at),
        market(),
        Trade::new(trade_id, Side::Buy, price(), quantity(dec!(1))).expect("fixture trade"),
    )
    .expect("fixture trade event")
}

pub fn book(event_time: i64, received_at: i64, sequence: u64) -> MarketEvent {
    MarketEvent::book_snapshot(
        timestamp(event_time),
        timestamp(received_at),
        market(),
        BookSnapshot::new(
            sequence,
            vec![BookLevel::new(
                Price::new(dec!(99)).expect("fixture bid"),
                quantity(dec!(1)),
            )],
            vec![BookLevel::new(
                Price::new(dec!(101)).expect("fixture ask"),
                quantity(dec!(1)),
            )],
        ),
    )
    .expect("fixture book event")
}

fn venue_candle(interval: VenueCandleInterval, open_time_ns: i64, active: bool) -> Candle {
    candle_for_test(CandleForTest {
        market: market(),
        interval,
        open_time_ms: open_time_ns / 1_000_000,
        open: price(),
        high: price(),
        low: price(),
        close: price(),
        volume: quantity(if active { dec!(2) } else { dec!(0) }),
        trade_count: u64::from(active) * 2,
    })
    .expect("fixture candle")
}

fn core_interval(interval: VenueCandleInterval) -> CandleInterval {
    match interval {
        VenueCandleInterval::FifteenMinutes => CandleInterval::FifteenMinutes,
        VenueCandleInterval::OneHour => CandleInterval::OneHour,
    }
}

fn completed_event(candle: &Candle) -> MarketEvent {
    let open_time = timestamp(
        candle
            .open_time_ms()
            .checked_mul(1_000_000)
            .expect("fixture open nanoseconds"),
    );
    let candle = CompletedCandle::new(
        core_interval(candle.interval()),
        open_time,
        candle.open(),
        candle.high(),
        candle.low(),
        candle.close(),
        candle.volume(),
        candle.trade_count(),
    )
    .expect("fixture completed candle");
    let close = open_time
        .checked_add(candle.interval().duration())
        .expect("fixture close nanoseconds");
    MarketEvent::completed_candle(close, close, market(), candle).expect("fixture candle event")
}

pub struct VerifiedRecoveryFixture {
    pub predecessor: MarketEvent,
    pub snapshot: MarketEvent,
    pub local_trades: Vec<MarketEvent>,
    pub official_candles: Vec<MarketEvent>,
    pub result: RecoveryResult,
}

pub fn verified_recovery() -> VerifiedRecoveryFixture {
    let predecessor = trade(1, 1, 1);
    let snapshot = book(1_000, 1_000, 1);
    let local_trade = trade(2, 2, 2);
    let official = vec![
        venue_candle(VenueCandleInterval::FifteenMinutes, 0, true),
        venue_candle(
            VenueCandleInterval::FifteenMinutes,
            FIFTEEN_MINUTES_NS,
            false,
        ),
        venue_candle(
            VenueCandleInterval::FifteenMinutes,
            FIFTEEN_MINUTES_NS * 2,
            false,
        ),
        venue_candle(
            VenueCandleInterval::FifteenMinutes,
            FIFTEEN_MINUTES_NS * 3,
            false,
        ),
        venue_candle(VenueCandleInterval::OneHour, 0, true),
    ];
    let official_candles = official.iter().map(completed_event).collect::<Vec<_>>();

    let request = recovery_request_from_events_for_test(1, Some(&predecessor), &snapshot);
    let mut recovery = GapRecovery::new();
    recovery.enqueue(request).expect("enqueue fixture request");
    let mut aggregator = CandleAggregator::new();
    aggregator.ingest(&predecessor).expect("ingest predecessor");
    let result = recovery
        .process_next_through(
            RecoveryEvidence::Reconciled {
                local_trades: std::slice::from_ref(&local_trade),
                official_candles: &official,
            },
            timestamp(ONE_HOUR_NS),
            &mut aggregator,
        )
        .expect("reconcile fixture")
        .expect("fixture result");
    assert!(
        result.verified_witness().is_some(),
        "fixture must reconcile"
    );

    VerifiedRecoveryFixture {
        predecessor,
        snapshot,
        local_trades: vec![local_trade],
        official_candles,
        result,
    }
}
