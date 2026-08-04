use serde_json::{Value, json};
use trench_core::domain::Market;
use trench_hyperliquid::{
    Candle, CandleInterval, FundingRecord, InfoClient, InfoError, L2Book, L2BookPrecision,
    MetaAndAssetContexts, TimeRange,
};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const META_FIXTURE: &str = include_str!("../../../tests/fixtures/meta/native-perps.json");
const START_MS: i64 = 1_700_000_000_000;
const END_MS: i64 = 1_700_000_900_000;

fn meta_fixture() -> Value {
    serde_json::from_str(META_FIXTURE).expect("checked fixture")
}

fn l2_fixture() -> Value {
    json!({
        "coin": "BTC",
        "time": START_MS,
        "levels": [
            [{"px": "64120.5", "sz": "1.5", "n": 2}],
            [{"px": "64121.0", "sz": "0.75", "n": 1}]
        ]
    })
}

fn candle_fixture() -> Value {
    json!({
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
    })
}

fn funding_fixture() -> Value {
    json!({
        "coin": "SOL",
        "fundingRate": "-0.000012",
        "premium": "-0.00003",
        "time": START_MS
    })
}

async fn json_client(body: Value) -> (InfoClient, MockServer) {
    raw_client(
        serde_json::to_string(&body).expect("serializable test body"),
        "application/json",
    )
    .await
}

async fn raw_client(body: String, content_type: &'static str) -> (InfoClient, MockServer) {
    let server = MockServer::start().await;
    let client = InfoClient::new_loopback_for_test(&format!("{}/info", server.uri()))
        .expect("wiremock loopback URL");
    Mock::given(method("POST"))
        .and(path("/info"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(body, content_type))
        .expect(1)
        .mount(&server)
        .await;
    (client, server)
}

async fn meta_result(body: Value) -> Result<MetaAndAssetContexts, InfoError> {
    let (client, _server) = json_client(body).await;
    client.meta_and_asset_contexts().await
}

async fn meta_raw_result(body: &str) -> Result<MetaAndAssetContexts, InfoError> {
    let (client, _server) = raw_client(body.to_owned(), "application/json").await;
    client.meta_and_asset_contexts().await
}

async fn mids_result(body: Value) -> Result<usize, InfoError> {
    let (client, _server) = json_client(body).await;
    client.all_mids().await.map(|mids| mids.len())
}

async fn l2_result(body: Value) -> Result<L2Book, InfoError> {
    let (client, _server) = json_client(body).await;
    client
        .l2_book(
            &Market::new("BTC").expect("valid market"),
            L2BookPrecision::Full,
        )
        .await
}

async fn candles_result(body: Value) -> Result<Vec<Candle>, InfoError> {
    candles_result_for_interval(body, CandleInterval::FifteenMinutes).await
}

async fn candles_result_for_interval(
    body: Value,
    interval: CandleInterval,
) -> Result<Vec<Candle>, InfoError> {
    let (client, _server) = json_client(body).await;
    client
        .candle_snapshot(
            &Market::new("ETH").expect("valid market"),
            interval,
            TimeRange::new(START_MS, END_MS).expect("valid range"),
        )
        .await
}

async fn funding_result(body: Value) -> Result<Vec<FundingRecord>, InfoError> {
    let (client, _server) = json_client(body).await;
    client
        .funding_history(
            &Market::new("SOL").expect("valid market"),
            TimeRange::new(START_MS, END_MS).expect("valid range"),
        )
        .await
}

#[tokio::test]
async fn metadata_ignores_unknown_fields_but_enforces_required_shape_and_alignment() {
    let mut with_unknown = meta_fixture();
    with_unknown[0]["futureMetaField"] = json!({"nested": true});
    with_unknown[0]["universe"][0]["futureAssetField"] = json!(17);
    with_unknown[1][0]["futureContextField"] = json!("ignored");
    assert_eq!(
        meta_result(with_unknown)
            .await
            .expect("unknown fields must be ignored")
            .assets()
            .len(),
        4
    );

    assert_eq!(meta_raw_result("{}").await, Err(InfoError::Decode));
    assert_eq!(
        meta_raw_result("[{\"universe\":[]}]").await,
        Err(InfoError::Decode)
    );

    let mut unaligned = meta_fixture();
    unaligned[1].as_array_mut().expect("context array").pop();
    assert_eq!(
        meta_result(unaligned).await,
        Err(InfoError::InvalidResponse {
            field: "metaAndAssetCtxs",
            requirement: "universe and context arrays must be index-aligned",
        })
    );

    let mut missing = meta_fixture();
    missing[1][0]
        .as_object_mut()
        .expect("context object")
        .remove("markPx");
    assert_eq!(meta_result(missing).await, Err(InfoError::Decode));

    let mut wrong_type = meta_fixture();
    wrong_type[0]["universe"][0]["maxLeverage"] = json!("40");
    assert_eq!(meta_result(wrong_type).await, Err(InfoError::Decode));

    let mut duplicate = meta_fixture();
    duplicate[0]["universe"][1]["name"] = json!("BTC");
    assert_eq!(
        meta_result(duplicate).await,
        Err(InfoError::InvalidResponse {
            field: "universe.name",
            requirement: "native market names must be unique",
        })
    );
}

#[tokio::test]
async fn metadata_nullable_fields_must_still_be_present() {
    for field in ["impactPxs", "midPx", "premium"] {
        let mut missing = meta_fixture();
        missing[1][0]
            .as_object_mut()
            .expect("context object")
            .remove(field);
        assert_eq!(meta_result(missing).await, Err(InfoError::Decode));
    }
}

#[tokio::test]
async fn metadata_retains_explicit_venue_delisting_state() {
    let mut body = meta_fixture();
    body[0]["universe"][3]["isDelisted"] = json!(true);

    let metadata = meta_result(body)
        .await
        .expect("explicit delisting state must normalize");
    let old = metadata
        .assets()
        .iter()
        .find(|asset| asset.market().as_str() == "OLD")
        .expect("fixture OLD market");
    assert!(old.is_delisted());
    assert!(!metadata.assets()[0].is_delisted());
}

#[tokio::test]
async fn metadata_rejects_invalid_exact_decimals_and_numeric_domains() {
    for (pointer, replacement, field, requirement) in [
        (
            "/0/universe/0/maxLeverage",
            json!(0),
            "universe.maxLeverage",
            "must be positive",
        ),
        (
            "/1/0/dayNtlVlm",
            json!("-1"),
            "assetCtx.dayNtlVlm",
            "must be a nonnegative decimal string",
        ),
        (
            "/1/0/markPx",
            json!("0"),
            "assetCtx.markPx",
            "must be a positive decimal string",
        ),
        (
            "/1/0/openInterest",
            json!("-1"),
            "assetCtx.openInterest",
            "must be a nonnegative decimal string",
        ),
        (
            "/1/0/funding",
            json!("not-a-decimal"),
            "assetCtx.funding",
            "must be an exact decimal string",
        ),
        (
            "/1/0/impactPxs/0",
            json!("not-a-decimal"),
            "assetCtx.impactPxs",
            "must be an exact decimal string",
        ),
        (
            "/1/0/impactPxs/1",
            json!("0"),
            "assetCtx.impactPxs",
            "must be a positive decimal string",
        ),
        (
            "/1/0/midPx",
            json!("not-a-decimal"),
            "assetCtx.midPx",
            "must be an exact decimal string",
        ),
        (
            "/1/0/midPx",
            json!("0"),
            "assetCtx.midPx",
            "must be a positive decimal string",
        ),
        (
            "/1/0/premium",
            json!("not-a-decimal"),
            "assetCtx.premium",
            "must be an exact decimal string",
        ),
    ] {
        let mut body = meta_fixture();
        *body.pointer_mut(pointer).expect("fixture pointer") = replacement;
        assert_eq!(
            meta_result(body).await,
            Err(InfoError::InvalidResponse { field, requirement })
        );
    }

    let mut invalid_impact_shape = meta_fixture();
    invalid_impact_shape[1][0]["impactPxs"] = json!(["1"]);
    assert_eq!(
        meta_result(invalid_impact_shape).await,
        Err(InfoError::Decode)
    );
}

#[tokio::test]
async fn all_mids_requires_a_string_map_and_positive_native_prices() {
    assert_eq!(mids_result(json!([])).await, Err(InfoError::Decode));
    assert_eq!(mids_result(json!({"BTC": 1})).await, Err(InfoError::Decode));

    for (body, field, requirement) in [
        (
            json!({"BTC": "0"}),
            "allMids.price",
            "must be a positive decimal string",
        ),
        (
            json!({"BTC": "-1"}),
            "allMids.price",
            "must be a positive decimal string",
        ),
        (
            json!({"BTC": "not-a-decimal"}),
            "allMids.price",
            "must be an exact decimal string",
        ),
    ] {
        assert_eq!(
            mids_result(body).await,
            Err(InfoError::InvalidResponse { field, requirement })
        );
    }

    assert_eq!(
        mids_result(json!({
            "BTC": "1",
            "@107": "not-a-decimal",
            "#5": "not-a-decimal",
            "PURR/USDC": "not-a-decimal",
            "xyz:XYZ100": "not-a-decimal"
        }))
        .await,
        Ok(1)
    );
}

#[tokio::test]
async fn l2_book_ignores_unknown_fields_and_enforces_required_wire_shape() {
    let mut with_unknown = l2_fixture();
    with_unknown["futureBookField"] = json!(true);
    with_unknown["levels"][0][0]["futureLevelField"] = json!("ignored");
    assert_eq!(
        l2_result(with_unknown)
            .await
            .expect("unknown fields must be ignored")
            .bids()
            .len(),
        1
    );

    let mut missing = l2_fixture();
    missing
        .as_object_mut()
        .expect("book object")
        .remove("levels");
    assert_eq!(l2_result(missing).await, Err(InfoError::Decode));

    let mut wrong_type = l2_fixture();
    wrong_type["time"] = json!(START_MS.to_string());
    assert_eq!(l2_result(wrong_type).await, Err(InfoError::Decode));

    let mut wrong_sides = l2_fixture();
    wrong_sides["levels"] = json!([[], [], []]);
    assert_eq!(l2_result(wrong_sides).await, Err(InfoError::Decode));
}

#[tokio::test]
async fn l2_book_enforces_coin_time_level_count_and_positive_levels() {
    for (pointer, replacement, field, requirement) in [
        (
            "/coin",
            json!("ETH"),
            "l2Book.coin",
            "must match the requested coin",
        ),
        (
            "/time",
            json!(0),
            "l2Book.time",
            "must be positive epoch milliseconds",
        ),
        (
            "/levels/0/0/px",
            json!("0"),
            "l2Book.levels.px",
            "must be a positive decimal string",
        ),
        (
            "/levels/0/0/sz",
            json!("0"),
            "l2Book.levels.sz",
            "must be positive",
        ),
        (
            "/levels/0/0/sz",
            json!("-1"),
            "l2Book.levels.sz",
            "must be a nonnegative decimal string",
        ),
        (
            "/levels/0/0/n",
            json!(0),
            "l2Book.levels.n",
            "must be a positive integer",
        ),
        (
            "/levels/0/0/n",
            json!(-1),
            "l2Book.levels.n",
            "must be a positive integer",
        ),
    ] {
        let mut body = l2_fixture();
        *body.pointer_mut(pointer).expect("fixture pointer") = replacement;
        assert_eq!(
            l2_result(body).await,
            Err(InfoError::InvalidResponse { field, requirement })
        );
    }

    let level = json!({"px": "1", "sz": "1", "n": 1});
    let mut boundary = l2_fixture();
    boundary["levels"][0] = Value::Array(vec![level.clone(); 20]);
    assert_eq!(
        l2_result(boundary)
            .await
            .expect("20 levels is permitted")
            .bids()
            .len(),
        20
    );

    let mut over = l2_fixture();
    over["levels"][1] = Value::Array(vec![level; 21]);
    assert_eq!(
        l2_result(over).await,
        Err(InfoError::InvalidResponse {
            field: "l2Book.levels",
            requirement: "must contain at most 20 levels per side",
        })
    );
}

#[tokio::test]
async fn candles_ignore_unknown_fields_but_enforce_required_types() {
    let mut with_unknown = candle_fixture();
    with_unknown["futureCandleField"] = json!({"ignored": true});
    assert_eq!(
        candles_result(json!([with_unknown]))
            .await
            .expect("unknown fields must be ignored")
            .len(),
        1
    );

    let mut missing = candle_fixture();
    missing.as_object_mut().expect("candle object").remove("T");
    assert_eq!(
        candles_result(json!([missing])).await,
        Err(InfoError::Decode)
    );

    let mut wrong_type = candle_fixture();
    wrong_type["n"] = json!("420");
    assert_eq!(
        candles_result(json!([wrong_type])).await,
        Err(InfoError::Decode)
    );
}

#[tokio::test]
async fn candles_enforce_requested_identity_time_price_volume_and_count() {
    for (pointer, replacement, field, requirement) in [
        (
            "/s",
            json!("BTC"),
            "candleSnapshot.s",
            "must match the requested coin",
        ),
        (
            "/i",
            json!("1h"),
            "candleSnapshot.i",
            "must match the requested interval",
        ),
        (
            "/t",
            json!(0),
            "candleSnapshot.t",
            "must be positive epoch milliseconds",
        ),
        (
            "/T",
            json!(START_MS - 1),
            "candleSnapshot.T",
            "must equal the declared interval close",
        ),
        (
            "/o",
            json!("0"),
            "candleSnapshot.o",
            "must be a positive decimal string",
        ),
        (
            "/h",
            json!("3000"),
            "candleSnapshot.ohlc",
            "high and low must bound open and close",
        ),
        (
            "/v",
            json!("-1"),
            "candleSnapshot.v",
            "must be a nonnegative decimal string",
        ),
        (
            "/n",
            json!(-1),
            "candleSnapshot.n",
            "must be a nonnegative integer",
        ),
    ] {
        let mut candle = candle_fixture();
        *candle.pointer_mut(pointer).expect("fixture pointer") = replacement;
        assert_eq!(
            candles_result(json!([candle])).await,
            Err(InfoError::InvalidResponse { field, requirement })
        );
    }

    let mut first_boundary = candle_fixture();
    first_boundary["t"] = json!(START_MS - 100);
    first_boundary["T"] = json!(START_MS + 899_899);
    let mut last_boundary = candle_fixture();
    last_boundary["t"] = json!(END_MS - 100);
    last_boundary["T"] = json!(END_MS + 899_899);
    assert_eq!(
        candles_result(json!([first_boundary, last_boundary]))
            .await
            .expect("boundary candles may extend beyond unaligned request bounds")
            .len(),
        2
    );

    let mut outside = candle_fixture();
    outside["t"] = json!(END_MS + 1);
    outside["T"] = json!(END_MS + 900_000);
    assert_eq!(
        candles_result(json!([outside])).await,
        Err(InfoError::InvalidResponse {
            field: "candleSnapshot.time",
            requirement: "must overlap the requested inclusive range",
        })
    );

    let mut zero_volume = candle_fixture();
    zero_volume["v"] = json!("0");
    assert_eq!(
        candles_result(json!([zero_volume])).await,
        Err(InfoError::InvalidResponse {
            field: "candleSnapshot.activity",
            requirement: "volume must be zero if and only if trade count is zero",
        })
    );
}

#[tokio::test]
async fn candles_reject_a_one_hour_close_with_the_wrong_duration() {
    let mut candle = candle_fixture();
    candle["i"] = json!("1h");

    assert_eq!(
        candles_result_for_interval(json!([candle]), CandleInterval::OneHour).await,
        Err(InfoError::InvalidResponse {
            field: "candleSnapshot.T",
            requirement: "must equal the declared interval close",
        })
    );
}

#[tokio::test]
async fn candles_reject_a_fifteen_minute_close_that_is_one_millisecond_late() {
    let mut candle = candle_fixture();
    candle["T"] = json!(END_MS);

    assert_eq!(
        candles_result(json!([candle])).await,
        Err(InfoError::InvalidResponse {
            field: "candleSnapshot.T",
            requirement: "must equal the declared interval close",
        })
    );
}

#[tokio::test]
async fn candles_accept_an_exact_one_hour_close_that_overlaps_the_requested_range() {
    let mut candle = candle_fixture();
    candle["i"] = json!("1h");
    candle["T"] = json!(START_MS + 3_600_000 - 1);

    assert_eq!(
        candles_result_for_interval(json!([candle]), CandleInterval::OneHour)
            .await
            .expect("one-hour candle must use its declared close")
            .len(),
        1
    );
}

#[tokio::test]
async fn candles_reject_declared_close_time_overflow() {
    let mut candle = candle_fixture();
    candle["t"] = json!(i64::MAX);
    candle["T"] = json!(i64::MAX);

    assert_eq!(
        candles_result(json!([candle])).await,
        Err(InfoError::InvalidResponse {
            field: "candleSnapshot.T",
            requirement: "declared interval close must not overflow",
        })
    );
}

#[tokio::test]
async fn candles_reject_positive_volume_without_trades() {
    let mut candle = candle_fixture();
    candle["n"] = json!(0);

    assert_eq!(
        candles_result(json!([candle])).await,
        Err(InfoError::InvalidResponse {
            field: "candleSnapshot.activity",
            requirement: "volume must be zero if and only if trade count is zero",
        })
    );
}

#[tokio::test]
async fn zero_trade_candles_must_have_flat_ohlc() {
    let mut candle = candle_fixture();
    candle["v"] = json!("0");
    candle["n"] = json!(0);

    assert_eq!(
        candles_result(json!([candle])).await,
        Err(InfoError::InvalidResponse {
            field: "candleSnapshot.ohlc",
            requirement: "zero-trade candles must be flat",
        })
    );
}

#[tokio::test]
async fn flat_zero_trade_candles_are_valid() {
    let mut candle = candle_fixture();
    candle["o"] = json!("3100");
    candle["c"] = json!("3100");
    candle["h"] = json!("3100");
    candle["l"] = json!("3100");
    candle["v"] = json!("0");
    candle["n"] = json!(0);

    assert_eq!(
        candles_result(json!([candle]))
            .await
            .expect("flat zero-trade candle must be valid")
            .len(),
        1
    );
}

#[tokio::test]
async fn funding_ignores_unknown_fields_and_enforces_identity_time_and_exact_rates() {
    let mut with_unknown = funding_fixture();
    with_unknown["futureFundingField"] = json!(true);
    assert_eq!(
        funding_result(json!([with_unknown]))
            .await
            .expect("unknown fields must be ignored")
            .len(),
        1
    );

    let mut missing = funding_fixture();
    missing
        .as_object_mut()
        .expect("funding object")
        .remove("premium");
    assert_eq!(
        funding_result(json!([missing])).await,
        Err(InfoError::Decode)
    );

    let mut wrong_type = funding_fixture();
    wrong_type["time"] = json!(START_MS.to_string());
    assert_eq!(
        funding_result(json!([wrong_type])).await,
        Err(InfoError::Decode)
    );

    for (pointer, replacement, field, requirement) in [
        (
            "/coin",
            json!("BTC"),
            "fundingHistory.coin",
            "must match the requested coin",
        ),
        (
            "/time",
            json!(0),
            "fundingHistory.time",
            "must be positive epoch milliseconds",
        ),
        (
            "/time",
            json!(END_MS + 1),
            "fundingHistory.time",
            "must fall within the requested inclusive range",
        ),
        (
            "/fundingRate",
            json!("not-a-decimal"),
            "fundingHistory.fundingRate",
            "must be an exact decimal string",
        ),
    ] {
        let mut record = funding_fixture();
        *record.pointer_mut(pointer).expect("fixture pointer") = replacement;
        assert_eq!(
            funding_result(json!([record])).await,
            Err(InfoError::InvalidResponse { field, requirement })
        );
    }

    let mut at_end = funding_fixture();
    at_end["time"] = json!(END_MS);
    assert_eq!(
        funding_result(json!([at_end]))
            .await
            .expect("inclusive end is valid")[0]
            .time_ms(),
        END_MS
    );
}
