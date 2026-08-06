use rust_decimal_macros::dec;
use trench_core::{
    domain::Market,
    universe::{ListingState, UniverseError},
};
use trench_storage::research_sidecar::{
    UniverseCandidateInput, UniverseCandidateInputError, UniverseDepthInput,
};

fn depth() -> UniverseDepthInput {
    UniverseDepthInput::new(dec!(50_000), dec!(75_000), dec!(100_000))
        .expect("fixture depth must be monotonic")
}

fn candidate_input() -> UniverseCandidateInput {
    UniverseCandidateInput::new(
        Market::new("SOL").expect("fixture market"),
        true,
        ListingState::Active,
        true,
        true,
        true,
        20,
        30,
        dec!(0.995),
        true,
        dec!(0.999),
        dec!(6_000_000),
        dec!(2_000_000),
        dec!(10),
        depth(),
        depth(),
    )
    .expect("fixture candidate input")
}

#[test]
fn candidate_input_reconstructs_checked_core_candidate() {
    let input = candidate_input();

    let candidate = input.recompute().expect("candidate must recompute");

    assert_eq!(candidate.market().as_str(), "SOL");
    assert!(candidate.is_native_perpetual());
    assert_eq!(
        candidate.history().trailing_seven_day_coverage(),
        dec!(0.995)
    );
    assert_eq!(
        candidate.liquidity().depth().bid().at_50_bps().value(),
        dec!(100_000)
    );
}

#[test]
fn candidate_input_rejects_invalid_fraction() {
    let error = UniverseCandidateInput::new(
        Market::new("SOL").expect("fixture market"),
        true,
        ListingState::Active,
        true,
        true,
        true,
        20,
        30,
        dec!(1.001),
        true,
        dec!(0.999),
        dec!(6_000_000),
        dec!(2_000_000),
        dec!(10),
        depth(),
        depth(),
    )
    .expect_err("coverage above one must fail");

    assert!(matches!(
        error,
        UniverseCandidateInputError::Universe(UniverseError::InvalidFraction {
            field: "trailing_seven_day_coverage"
        })
    ));
}

#[test]
fn candidate_input_rejects_nonmonotonic_depth() {
    let error = UniverseDepthInput::new(dec!(50_000), dec!(49_999), dec!(100_000))
        .expect_err("depth that decreases with a wider band must fail");

    assert!(matches!(
        error,
        UniverseCandidateInputError::Universe(UniverseError::NonMonotonicDepth { .. })
    ));
}

#[test]
fn candidate_input_rejects_tampered_digest_during_deserialization() {
    let mut payload = serde_json::to_value(candidate_input()).expect("serialize candidate input");
    payload["digest"] = serde_json::Value::String(format!("b3:{}", "0".repeat(64)));

    let error = serde_json::from_value::<UniverseCandidateInput>(payload)
        .expect_err("digest tampering must fail");

    assert!(error.to_string().contains("digest does not match"));
}

#[test]
fn candidate_input_rejects_unknown_serialized_fields() {
    let mut payload = serde_json::to_value(candidate_input()).expect("serialize candidate input");
    payload["activation"] = serde_json::Value::Null;

    assert!(serde_json::from_value::<UniverseCandidateInput>(payload).is_err());
}
