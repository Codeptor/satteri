use std::error::Error;
use std::time::Duration;

use serde_json::json;
use trench_core::domain::Market;
use trench_hyperliquid::{
    CandleInterval, INFO_RESPONSE_MAX_BYTES, InfoClient, InfoError, L2BookPrecision, TimeRange,
};
use wiremock::matchers::{body_json, header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const START_MS: i64 = 1_700_000_000_000;
const END_MS: i64 = 1_700_000_900_000;

async fn client_and_server() -> Result<(InfoClient, MockServer), Box<dyn Error>> {
    let server = MockServer::start().await;
    let client = InfoClient::new_loopback_for_test(&format!("{}/info", server.uri()))?;
    Ok((client, server))
}

fn assert_error_does_not_contain(error: &InfoError, secret: &str) {
    let rendered = format!("{error} {error:?}");
    assert!(!rendered.contains(secret), "error leaked response body");
}

#[test]
fn production_endpoint_is_exactly_the_approved_https_url() {
    InfoClient::new("https://api.hyperliquid.xyz/info").expect("approved URL must be accepted");

    for rejected in [
        "http://api.hyperliquid.xyz/info",
        "https://api.hyperliquid.xyz/info/",
        "https://api.hyperliquid.xyz/exchange",
        "https://api.hyperliquid.xyz:443/info",
        "https://api.hyperliquid.xyz/info?type=allMids",
        "https://api.hyperliquid.xyz/info#fragment",
        "https://user:pass@api.hyperliquid.xyz/info",
        "https://hyperliquid.xyz/info",
        "https://API.HYPERLIQUID.XYZ/info",
    ] {
        assert!(matches!(
            InfoClient::new(rejected),
            Err(InfoError::InvalidEndpoint)
        ));
    }
}

#[test]
fn debug_transport_accepts_only_canonical_numeric_loopback_http() {
    InfoClient::new_loopback_for_test("http://127.0.0.1:32123/info")
        .expect("numeric IPv4 loopback must be accepted");
    InfoClient::new_loopback_for_test("http://[::1]:32123/info")
        .expect("numeric IPv6 loopback must be accepted");

    for rejected in [
        "http://localhost:32123/info",
        "http://192.0.2.1:32123/info",
        "https://127.0.0.1:32123/info",
        "http://127.0.0.1/info",
        "http://127.0.0.1:80/info",
        "http://127.0.0.1:32123/info/",
        "http://127.0.0.1:32123/exchange",
        "http://127.0.0.1:32123/info?type=allMids",
        "http://127.0.0.1:32123/info#fragment",
        "http://user:pass@127.0.0.1:32123/info",
    ] {
        assert!(matches!(
            InfoClient::new_loopback_for_test(rejected),
            Err(InfoError::InvalidEndpoint)
        ));
    }
}

#[test]
fn explicit_time_ranges_reject_nonpositive_and_nonincreasing_bounds() {
    for (start, end) in [(0, 1), (-1, 1), (1, 0), (1, -1)] {
        assert!(matches!(
            TimeRange::new(start, end),
            Err(InfoError::InvalidRequest { field: "time", .. })
        ));
    }
    for (start, end) in [(1, 1), (2, 1)] {
        assert_eq!(
            TimeRange::new(start, end),
            Err(InfoError::InvalidRequest {
                field: "time",
                requirement: "start must be earlier than end",
            })
        );
    }

    let range = TimeRange::new(1, 2).expect("positive increasing range");
    assert_eq!(range.start_ms(), 1);
    assert_eq!(range.end_ms(), 2);
}

#[tokio::test]
async fn native_coin_validation_happens_before_network_io() -> Result<(), Box<dyn Error>> {
    let (client, server) = client_and_server().await?;
    let spot = Market::new("@107")?;
    let dex_qualified = Market::new("dex:BTC")?;
    let range = TimeRange::new(START_MS, END_MS)?;

    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(500))
        .expect(0)
        .mount(&server)
        .await;

    assert!(matches!(
        client.l2_book(&spot, L2BookPrecision::Full).await,
        Err(InfoError::InvalidRequest { field: "coin", .. })
    ));
    assert!(matches!(
        client
            .candle_snapshot(&dex_qualified, CandleInterval::OneHour, range)
            .await,
        Err(InfoError::InvalidRequest { field: "coin", .. })
    ));
    assert!(matches!(
        client.funding_history(&spot, range).await,
        Err(InfoError::InvalidRequest { field: "coin", .. })
    ));
    Ok(())
}

#[tokio::test]
async fn non_success_statuses_are_stable_and_never_retried() -> Result<(), Box<dyn Error>> {
    for status in [429_u16, 500] {
        let (client, server) = client_and_server().await?;
        Mock::given(method("POST"))
            .and(path("/info"))
            .and(body_json(json!({"type": "allMids"})))
            .respond_with(
                ResponseTemplate::new(status)
                    .set_body_raw(r#"{"apiKey":"status-body-secret"}"#, "application/json"),
            )
            .expect(1)
            .mount(&server)
            .await;

        let error = client.all_mids().await.expect_err("status must fail");
        assert_eq!(error, InfoError::HttpStatus { code: status });
        assert_error_does_not_contain(&error, "status-body-secret");
    }
    Ok(())
}

#[tokio::test]
async fn redirects_are_returned_without_following_location() -> Result<(), Box<dyn Error>> {
    let (client, server) = client_and_server().await?;
    Mock::given(method("POST"))
        .and(path("/info"))
        .respond_with(
            ResponseTemplate::new(302)
                .insert_header("location", format!("{}/target", server.uri())),
        )
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(path("/target"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"BTC": "1"})))
        .expect(0)
        .mount(&server)
        .await;

    assert_eq!(
        client.all_mids().await,
        Err(InfoError::HttpStatus { code: 302 })
    );
    Ok(())
}

#[tokio::test]
async fn successful_responses_require_application_json() -> Result<(), Box<dyn Error>> {
    for content_type in ["text/plain", "application/problem+json"] {
        let (client, server) = client_and_server().await?;
        Mock::given(method("POST"))
            .and(path("/info"))
            .respond_with(ResponseTemplate::new(200).set_body_raw(r#"{"BTC":"1"}"#, content_type))
            .expect(1)
            .mount(&server)
            .await;

        assert_eq!(client.all_mids().await, Err(InfoError::InvalidContentType));
    }

    let (client, server) = client_and_server().await?;
    Mock::given(method("POST"))
        .and(path("/info"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;
    assert_eq!(client.all_mids().await, Err(InfoError::InvalidContentType));
    Ok(())
}

#[tokio::test]
async fn fixed_request_deadline_is_enforced() -> Result<(), Box<dyn Error>> {
    let (client, server) = client_and_server().await?;
    Mock::given(method("POST"))
        .and(path("/info"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_delay(Duration::from_millis(300))
                .set_body_json(json!({"BTC": "1"})),
        )
        .expect(1)
        .mount(&server)
        .await;

    assert_eq!(client.all_mids().await, Err(InfoError::Timeout));
    Ok(())
}

#[tokio::test]
async fn response_body_limit_accepts_the_boundary_and_rejects_one_more_byte()
-> Result<(), Box<dyn Error>> {
    let prefix = r#"{"BTC":"1"}"#;
    let exact = format!(
        "{prefix}{}",
        " ".repeat(INFO_RESPONSE_MAX_BYTES - prefix.len())
    );
    let too_large = format!("{exact} ");

    let (client, server) = client_and_server().await?;
    Mock::given(method("POST"))
        .and(path("/info"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(exact, "application/json"))
        .expect(1)
        .mount(&server)
        .await;
    assert_eq!(client.all_mids().await?.len(), 1);

    let (client, server) = client_and_server().await?;
    Mock::given(method("POST"))
        .and(path("/info"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(too_large, "application/json"))
        .expect(1)
        .mount(&server)
        .await;
    assert_eq!(
        client.all_mids().await,
        Err(InfoError::ResponseTooLarge {
            max_bytes: INFO_RESPONSE_MAX_BYTES,
        })
    );
    Ok(())
}

#[tokio::test]
async fn malformed_response_bodies_are_not_exposed_by_errors() -> Result<(), Box<dyn Error>> {
    let (client, server) = client_and_server().await?;
    let secret = "decode-body-secret";
    Mock::given(method("POST"))
        .and(path("/info"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_raw(format!(r#"{{"apiKey":"{secret}""#), "application/json"),
        )
        .expect(1)
        .mount(&server)
        .await;

    let error = client
        .all_mids()
        .await
        .expect_err("malformed JSON must fail");
    assert_eq!(error, InfoError::Decode);
    assert_error_does_not_contain(&error, secret);
    Ok(())
}

#[tokio::test]
async fn clones_are_send_sync_and_share_safe_concurrent_reads() -> Result<(), Box<dyn Error>> {
    fn assert_send_sync_clone<T: Send + Sync + Clone>() {}
    assert_send_sync_clone::<InfoClient>();

    let (client, server) = client_and_server().await?;
    Mock::given(method("POST"))
        .and(path("/info"))
        .and(header(
            "user-agent",
            "satteri-trench-hyperliquid/0.1.0 (paper-only; read-only info)",
        ))
        .and(body_json(json!({"type": "allMids"})))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"BTC": "64120.75"})))
        .expect(4)
        .mount(&server)
        .await;

    let second = client.clone();
    let third = client.clone();
    let fourth = client.clone();
    let (a, b, c, d) = tokio::try_join!(
        client.all_mids(),
        second.all_mids(),
        third.all_mids(),
        fourth.all_mids()
    )?;
    assert!(a == b && b == c && c == d);
    Ok(())
}
