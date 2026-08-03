use std::time::Duration;

use trench_core::domain::Market;
use trench_hyperliquid::{WsClient, WsConfig, WsError, WsLimits};

fn market(value: &str) -> Market {
    Market::new(value).expect("valid native perpetual market")
}

#[test]
fn websocket_config_accepts_a_bounded_native_perpetual_universe() {
    let config = WsConfig::new(vec![market("BTC"), market("ETH")])
        .expect("small unique native universe must be accepted");

    assert_eq!(config.markets(), &[market("BTC"), market("ETH")]);
    assert_eq!(
        WsClient::new(config).markets(),
        &[market("BTC"), market("ETH")]
    );
}

#[test]
fn websocket_config_rejects_empty_or_duplicate_market_universes() {
    assert_eq!(WsConfig::new(Vec::new()), Err(WsError::EmptyUniverse));
    assert_eq!(
        WsConfig::new(vec![market("BTC"), market("BTC")]),
        Err(WsError::DuplicateMarket {
            market: market("BTC")
        })
    );
}

#[test]
fn websocket_limits_stay_below_public_connection_and_subscription_budgets() {
    let too_many_markets = (0..334).map(|index| market(&format!("M{index}"))).collect();
    assert_eq!(
        WsConfig::new(too_many_markets),
        Err(WsError::TooManyMarkets { max_markets: 333 })
    );

    assert_eq!(
        WsLimits::new(
            Duration::from_secs(5),
            Duration::from_secs(45),
            Duration::from_secs(60),
            Duration::from_secs(3),
            Duration::from_secs(30),
            5,
            64 * 1024,
            32,
        ),
        Err(WsError::InvalidConfig {
            field: "heartbeat_interval",
            requirement: "must be shorter than 60 seconds",
        })
    );
}
