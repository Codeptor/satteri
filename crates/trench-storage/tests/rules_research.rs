use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use trench_core::event::TimestampNs;
use trench_core::strategy::rules::RuleConfig;
use trench_core::validation::{
    EngineReplayOutcome, ReplayPhase, ResearchProvenance, RuleGrid, RuleReplay, RuleReplayRequest,
    RulesValidationReport, TimeRange, ValidationError, ValidationPlan,
};
use trench_storage::rules_research::RulesResearchRun;

const DAY_NS: i64 = 86_400_000_000_000;

fn timestamp(days: i64) -> TimestampNs {
    TimestampNs::new(i128::from(days * DAY_NS)).expect("day boundary")
}

fn digest(index: u8) -> String {
    format!("b3:{index:064x}")
}

fn provenance() -> ResearchProvenance {
    ResearchProvenance {
        config_digest: digest(1),
        code_digest: digest(2),
        data_digest: digest(3),
        universe_digest: digest(4),
        feature_schema_digest: digest(5),
        data_cutoff: timestamp(7),
    }
}

fn outcome(net_pnl: Decimal, closed_trades: u32, index: u8) -> EngineReplayOutcome {
    EngineReplayOutcome::new(
        net_pnl,
        dec!(1),
        closed_trades,
        digest(index),
        digest(index.saturating_add(1)),
        digest(index.saturating_add(2)),
        digest(index.saturating_add(3)),
    )
    .expect("valid engine outcome")
}

struct FixtureReplay {
    outer_test_trades: u32,
}

impl RuleReplay for FixtureReplay {
    fn replay(
        &mut self,
        request: RuleReplayRequest,
    ) -> Result<EngineReplayOutcome, ValidationError> {
        let config = request.config;
        let base = config.threshold().value() * dec!(100)
            + config.atr_floor().value()
            + config.take_profit().value() / dec!(100);
        let phase_bias = match request.phase {
            ReplayPhase::InnerValidation { inner_fold } => Decimal::from(inner_fold),
            ReplayPhase::Calibration => dec!(10),
            ReplayPhase::OuterTest => dec!(20),
        };
        let trades = match request.phase {
            ReplayPhase::OuterTest => self.outer_test_trades,
            _ => 1,
        };
        Ok(outcome(
            base + phase_bias,
            trades,
            request.outer_fold as u8 + 10,
        ))
    }
}

fn plan() -> ValidationPlan {
    ValidationPlan::build(timestamp(0), ValidationPlan::minimum_complete_days())
        .expect("stripped outer fold")
}

#[test]
fn runner_emits_reopened_report_and_artifact_for_eligible_multimarket_replay() {
    let mut replay = FixtureReplay {
        outer_test_trades: 34,
    };
    let run = RulesResearchRun::run(&plan(), provenance(), Vec::new(), &mut replay)
        .expect("eligible rules run");

    assert!(run.eligible());
    assert!(run.artifact_bytes().is_some());
    assert_eq!(
        RulesValidationReport::from_canonical_json(run.report_bytes()).expect("reopen report"),
        *run.report()
    );
    assert!(run.report().validate_active_pair().is_ok());
}

#[test]
fn runner_keeps_ineligible_report_fail_closed_without_artifact() {
    let mut replay = FixtureReplay {
        outer_test_trades: 0,
    };
    let run = RulesResearchRun::run(&plan(), provenance(), Vec::new(), &mut replay)
        .expect("ineligible report is still canonical");

    assert!(!run.eligible());
    assert!(run.artifact_bytes().is_none());
    assert!(matches!(
        run.report().eligibility(),
        trench_core::validation::ResearchEligibility::Ineligible { .. }
    ));
}

#[allow(dead_code)]
fn _request_shape_is_stable(config: RuleConfig) -> RuleReplayRequest {
    RuleReplayRequest {
        config,
        outer_fold: 0,
        phase: ReplayPhase::Calibration,
        training: None,
        evaluation: TimeRange::new(timestamp(0), timestamp(1)).expect("range"),
    }
}

#[test]
fn runner_uses_the_declared_twelve_candidate_grid() {
    assert_eq!(RuleGrid::declared().len(), 12);
}
