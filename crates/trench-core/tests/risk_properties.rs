use proptest::prelude::*;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

use trench_core::domain::{Price, Quantity, Usdc};
use trench_core::ledger::PositionSide;
use trench_core::risk::liquidation::{
    LiquidationInput, MaintenanceTier, MaintenanceTiers, calculate,
};

fn usdc(value: Decimal) -> Usdc {
    Usdc::new(value).expect("generated value is nonnegative")
}

fn tier_table() -> MaintenanceTiers {
    MaintenanceTiers::new(vec![
        MaintenanceTier::new(usdc(dec!(0)), None, dec!(0.025), usdc(dec!(0)))
            .expect("fixed maintenance tier"),
    ])
    .expect("fixed tier table")
}

proptest! {
    #[test]
    fn liquidation_is_positive_and_directionally_adverse_for_positive_reference_equity(
        quantity_millis in 1_i64..=20_000,
        reference_cents in 1_000_i64..=1_000_000,
        equity_millis in 1_i64..=10_000,
        long in any::<bool>(),
    ) {
        let quantity = Quantity::new(Decimal::new(quantity_millis, 3)).expect("positive quantity");
        let reference = Price::new(Decimal::new(reference_cents, 2)).expect("positive price");
        let equity = usdc(Decimal::new(equity_millis, 3));
        let side = if long { PositionSide::Long } else { PositionSide::Short };
        let input = LiquidationInput::new(quantity, side, reference, equity, tier_table())
            .expect("valid liquidation input");

        if let Ok(result) = calculate(&input) {
            prop_assert!(result.price().value() > Decimal::ZERO);
            prop_assert_eq!(result.tier_index(), 0);
        }
    }
}

#[test]
fn exact_long_and_short_reference_examples_remain_stable() {
    let common = |side| {
        LiquidationInput::new(
            Quantity::new(dec!(1)).expect("quantity"),
            side,
            Price::new(dec!(100)).expect("price"),
            usdc(dec!(5)),
            tier_table(),
        )
        .expect("input")
    };
    assert_eq!(
        calculate(&common(PositionSide::Long))
            .expect("long")
            .price()
            .value(),
        dec!(100) - dec!(2.5) / dec!(0.975),
    );
    assert_eq!(
        calculate(&common(PositionSide::Short))
            .expect("short")
            .price()
            .value(),
        dec!(100) + dec!(2.5) / dec!(1.025),
    );
}
