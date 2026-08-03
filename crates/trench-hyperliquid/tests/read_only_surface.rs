use std::error::Error;

use rust_decimal::Decimal;
use serde_json::json;
use trench_core::domain::Market;
use trench_hyperliquid::{
    CandleInterval, InfoClient, InfoError, L2BookPrecision, L2Mantissa, TimeRange,
};
use wiremock::matchers::{body_json, header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const META_FIXTURE: &str = include_str!("../../../tests/fixtures/meta/native-perps.json");
const START_MS: i64 = 1_700_000_000_000;
const END_MS: i64 = 1_700_000_900_000;

async fn client_and_server() -> Result<(InfoClient, MockServer), Box<dyn Error>> {
    let server = MockServer::start().await;
    let client = InfoClient::new_loopback_for_test(&format!("{}/info", server.uri()))?;
    Ok((client, server))
}

#[tokio::test]
async fn meta_and_asset_contexts_posts_exact_request_and_normalizes_native_rows()
-> Result<(), Box<dyn Error>> {
    let (client, server) = client_and_server().await?;
    Mock::given(method("POST"))
        .and(path("/info"))
        .and(header("content-type", "application/json"))
        .and(body_json(json!({"type": "metaAndAssetCtxs"})))
        .respond_with(ResponseTemplate::new(200).set_body_raw(META_FIXTURE, "application/json"))
        .expect(1)
        .mount(&server)
        .await;

    let snapshot = client.meta_and_asset_contexts().await?;
    let assets = snapshot.assets();

    assert_eq!(assets.len(), 4);
    assert_eq!(assets[0].market().as_str(), "BTC");
    assert_eq!(assets[0].size_decimals(), 5);
    assert_eq!(assets[0].max_leverage().value(), 40);
    assert!(!assets[0].only_isolated());
    assert_eq!(assets[1].market().as_str(), "ETH");
    assert_eq!(assets[1].size_decimals(), 4);
    assert_eq!(assets[1].max_leverage().value(), 25);
    assert_eq!(assets[2].market().as_str(), "SOL");
    assert_eq!(assets[2].size_decimals(), 2);
    assert_eq!(assets[2].max_leverage().value(), 20);
    assert_eq!(assets[3].market().as_str(), "OLD");
    assert!(assets[3].only_isolated());
    assert_eq!(assets[3].context().impact_prices(), None);
    assert_eq!(assets[3].context().mid_price(), None);
    assert_eq!(assets[3].context().premium(), None);
    assert!(assets[0].context().impact_prices().is_some());
    assert!(assets[0].context().mid_price().is_some());
    assert!(assets[0].context().premium().is_some());
    assert_eq!(
        assets[1].context().funding_rate().value(),
        Decimal::new(-4, 6)
    );
    Ok(())
}

#[tokio::test]
async fn all_mids_posts_exact_request_and_retains_only_native_perpetuals()
-> Result<(), Box<dyn Error>> {
    let (client, server) = client_and_server().await?;
    Mock::given(method("POST"))
        .and(path("/info"))
        .and(header("content-type", "application/json"))
        .and(body_json(json!({"type": "allMids"})))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "SOL": "148.22",
            "BTC": "64120.75",
            "ETH": "3120.3",
            "@107": "not-a-decimal",
            "#5": "not-a-decimal",
            "PURR/USDC": "not-a-decimal",
            "xyz:XYZ100": "not-a-decimal"
        })))
        .expect(1)
        .mount(&server)
        .await;

    let mids = client.all_mids().await?;

    assert_eq!(mids.len(), 3);
    assert_eq!(
        mids.get(&Market::new("BTC")?).map(|price| price.value()),
        Some(Decimal::new(6_412_075, 2))
    );
    Ok(())
}

#[tokio::test]
async fn l2_book_posts_exact_precision_request_and_preserves_level_order()
-> Result<(), Box<dyn Error>> {
    let (client, server) = client_and_server().await?;
    Mock::given(method("POST"))
        .and(path("/info"))
        .and(header("content-type", "application/json"))
        .and(body_json(json!({
            "type": "l2Book",
            "coin": "BTC",
            "nSigFigs": 5,
            "mantissa": 2
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "coin": "BTC",
            "time": START_MS,
            "levels": [
                [
                    {"px": "64120.5", "sz": "1.5", "n": 2},
                    {"px": "64120.0", "sz": "2.25", "n": 3}
                ],
                [
                    {"px": "64121.0", "sz": "0.75", "n": 1},
                    {"px": "64121.5", "sz": "4.0", "n": 4}
                ]
            ]
        })))
        .expect(1)
        .mount(&server)
        .await;

    let book = client
        .l2_book(
            &Market::new("BTC")?,
            L2BookPrecision::Five {
                mantissa: Some(L2Mantissa::Two),
            },
        )
        .await?;

    assert_eq!(book.market().as_str(), "BTC");
    assert_eq!(book.time_ms(), START_MS);
    assert_eq!(book.bids().len(), 2);
    assert_eq!(book.bids()[0].price().value(), Decimal::new(641_205, 1));
    assert_eq!(book.bids()[1].order_count(), 3);
    assert_eq!(book.asks().len(), 2);
    Ok(())
}

#[tokio::test]
async fn l2_book_serializes_every_closed_precision_relationship() -> Result<(), Box<dyn Error>> {
    let (client, server) = client_and_server().await?;
    let cases = [
        (
            L2BookPrecision::Full,
            json!({"type": "l2Book", "coin": "BTC"}),
        ),
        (
            L2BookPrecision::Two,
            json!({"type": "l2Book", "coin": "BTC", "nSigFigs": 2}),
        ),
        (
            L2BookPrecision::Three,
            json!({"type": "l2Book", "coin": "BTC", "nSigFigs": 3}),
        ),
        (
            L2BookPrecision::Four,
            json!({"type": "l2Book", "coin": "BTC", "nSigFigs": 4}),
        ),
        (
            L2BookPrecision::Five { mantissa: None },
            json!({"type": "l2Book", "coin": "BTC", "nSigFigs": 5}),
        ),
        (
            L2BookPrecision::Five {
                mantissa: Some(L2Mantissa::One),
            },
            json!({"type": "l2Book", "coin": "BTC", "nSigFigs": 5, "mantissa": 1}),
        ),
        (
            L2BookPrecision::Five {
                mantissa: Some(L2Mantissa::Two),
            },
            json!({"type": "l2Book", "coin": "BTC", "nSigFigs": 5, "mantissa": 2}),
        ),
        (
            L2BookPrecision::Five {
                mantissa: Some(L2Mantissa::Five),
            },
            json!({"type": "l2Book", "coin": "BTC", "nSigFigs": 5, "mantissa": 5}),
        ),
    ];

    for (_, request) in &cases {
        Mock::given(method("POST"))
            .and(path("/info"))
            .and(body_json(request))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "coin": "BTC",
                "time": START_MS,
                "levels": [[], []]
            })))
            .expect(1)
            .mount(&server)
            .await;
    }

    let market = Market::new("BTC")?;
    for (precision, _) in cases {
        client.l2_book(&market, precision).await?;
    }
    Ok(())
}

#[tokio::test]
async fn candle_snapshot_posts_exact_typed_range_request() -> Result<(), Box<dyn Error>> {
    let (client, server) = client_and_server().await?;
    Mock::given(method("POST"))
        .and(path("/info"))
        .and(header("content-type", "application/json"))
        .and(body_json(json!({
            "type": "candleSnapshot",
            "req": {
                "coin": "ETH",
                "interval": "15m",
                "startTime": START_MS,
                "endTime": END_MS
            }
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([{
            "t": START_MS,
            "T": END_MS - 1,
            "s": "ETH",
            "i": "15m",
            "o": "3100.0",
            "c": "3120.0",
            "h": "3130.0",
            "l": "3090.0",
            "v": "12500.5",
            "n": 420
        }])))
        .expect(1)
        .mount(&server)
        .await;

    let candles = client
        .candle_snapshot(
            &Market::new("ETH")?,
            CandleInterval::FifteenMinutes,
            TimeRange::new(START_MS, END_MS)?,
        )
        .await?;

    assert_eq!(candles.len(), 1);
    assert_eq!(candles[0].market().as_str(), "ETH");
    assert_eq!(candles[0].interval(), CandleInterval::FifteenMinutes);
    assert_eq!(candles[0].open_time_ms(), START_MS);
    assert_eq!(candles[0].close_time_ms(), END_MS - 1);
    assert_eq!(candles[0].trade_count(), 420);
    Ok(())
}

#[tokio::test]
async fn candle_snapshot_serializes_the_closed_one_hour_interval() -> Result<(), Box<dyn Error>> {
    let (client, server) = client_and_server().await?;
    Mock::given(method("POST"))
        .and(path("/info"))
        .and(body_json(json!({
            "type": "candleSnapshot",
            "req": {
                "coin": "ETH",
                "interval": "1h",
                "startTime": START_MS,
                "endTime": END_MS
            }
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([])))
        .expect(1)
        .mount(&server)
        .await;

    assert!(
        client
            .candle_snapshot(
                &Market::new("ETH")?,
                CandleInterval::OneHour,
                TimeRange::new(START_MS, END_MS)?,
            )
            .await?
            .is_empty()
    );
    Ok(())
}

#[tokio::test]
async fn funding_history_posts_exact_inclusive_explicit_range_request() -> Result<(), Box<dyn Error>>
{
    let (client, server) = client_and_server().await?;
    Mock::given(method("POST"))
        .and(path("/info"))
        .and(header("content-type", "application/json"))
        .and(body_json(json!({
            "type": "fundingHistory",
            "coin": "SOL",
            "startTime": START_MS,
            "endTime": END_MS
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([{
            "coin": "SOL",
            "fundingRate": "-0.000012",
            "premium": "-0.00003",
            "time": START_MS
        }])))
        .expect(1)
        .mount(&server)
        .await;

    let records = client
        .funding_history(&Market::new("SOL")?, TimeRange::new(START_MS, END_MS)?)
        .await?;

    assert_eq!(records.len(), 1);
    assert_eq!(records[0].market().as_str(), "SOL");
    assert_eq!(records[0].time_ms(), START_MS);
    assert_eq!(records[0].funding_rate().value(), Decimal::new(-12, 6));
    assert_eq!(records[0].premium().value(), Decimal::new(-3, 5));
    Ok(())
}

#[tokio::test]
async fn candle_snapshot_accepts_five_thousand_rows_and_rejects_one_more()
-> Result<(), Box<dyn Error>> {
    let (client, server) = client_and_server().await?;
    let candle = json!({
        "t": START_MS,
        "T": END_MS - 1,
        "s": "ETH",
        "i": "15m",
        "o": "3100.0",
        "c": "3120.0",
        "h": "3130.0",
        "l": "3090.0",
        "v": "12500.5",
        "n": 420
    });
    Mock::given(method("POST"))
        .and(path("/info"))
        .and(body_json(json!({
            "type": "candleSnapshot",
            "req": {
                "coin": "ETH",
                "interval": "15m",
                "startTime": START_MS,
                "endTime": END_MS
            }
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(vec![candle.clone(); 5_000]))
        .expect(1)
        .mount(&server)
        .await;

    assert_eq!(
        client
            .candle_snapshot(
                &Market::new("ETH")?,
                CandleInterval::FifteenMinutes,
                TimeRange::new(START_MS, END_MS)?,
            )
            .await?
            .len(),
        5_000
    );

    let (client, server) = client_and_server().await?;
    Mock::given(method("POST"))
        .and(path("/info"))
        .and(body_json(json!({
            "type": "candleSnapshot",
            "req": {
                "coin": "ETH",
                "interval": "15m",
                "startTime": START_MS,
                "endTime": END_MS
            }
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(vec![candle; 5_001]))
        .expect(1)
        .mount(&server)
        .await;

    let error = client
        .candle_snapshot(
            &Market::new("ETH")?,
            CandleInterval::FifteenMinutes,
            TimeRange::new(START_MS, END_MS)?,
        )
        .await
        .expect_err("more than 5000 rows must be rejected");

    assert_eq!(
        error,
        InfoError::InvalidResponse {
            field: "candleSnapshot",
            requirement: "must contain at most 5000 candles",
        }
    );
    Ok(())
}
