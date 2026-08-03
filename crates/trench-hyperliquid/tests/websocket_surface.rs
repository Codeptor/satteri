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
    assert_eq!(
        WsConfig::new(vec![market("@107")]),
        Err(WsError::NonNativeMarket {
            market: market("@107")
        })
    );
}

#[test]
fn websocket_limits_stay_below_public_connection_and_subscription_budgets() {
    let too_many_markets = (0..34).map(|index| market(&format!("M{index}"))).collect();
    assert_eq!(
        WsConfig::new(too_many_markets),
        Err(WsError::TooManyMarkets { max_markets: 33 })
    );

    assert_eq!(
        WsLimits::new(
            Duration::from_secs(5),
            Duration::from_secs(45),
            Duration::from_secs(15),
            Duration::from_secs(60),
            Duration::from_secs(3),
            Duration::from_secs(30),
            5,
            64 * 1024,
            32,
            1_000,
        ),
        Err(WsError::InvalidConfig {
            field: "heartbeat_interval",
            requirement: "must be shorter than 60 seconds",
        })
    );
    assert_eq!(
        WsLimits::new(
            Duration::from_secs(5),
            Duration::from_secs(45),
            Duration::from_secs(15),
            Duration::from_secs(25),
            Duration::from_secs(3),
            Duration::from_secs(30),
            5,
            64 * 1024,
            32,
            1_000,
        ),
        Err(WsError::InvalidConfig {
            field: "reconnect_min_delay",
            requirement: "must be at least 4 seconds",
        })
    );
    assert_eq!(
        WsLimits::new(
            Duration::from_secs(5),
            Duration::from_secs(45),
            Duration::from_secs(15),
            Duration::from_secs(25),
            Duration::from_secs(4),
            Duration::from_secs(30),
            5,
            64 * 1024,
            32,
            0,
        ),
        Err(WsError::InvalidConfig {
            field: "max_trade_identities",
            requirement: "must be between 1 and 1000000",
        })
    );
}
