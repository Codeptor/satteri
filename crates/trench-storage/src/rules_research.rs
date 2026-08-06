//! Production rules-only walk-forward orchestration over an authoritative replay.
//!
//! The core validator owns the nested-fold protocol and selection rules. This
//! storage boundary supplies the durable hand-off used by offline jobs: a
//! replay implementation (normally [`crate::research::EngineRuleReplay`]) is
//! run once for every market event in each fold, then the report and optional
//! artifact are reopened from their canonical bytes before being returned.
//! Reopening here is intentional: callers never receive an in-memory result
//! that was not proven serializable and content-addressed.

use trench_core::validation::{
    ExcludedGap, ResearchProvenance, RuleReplay, RulesArtifact, RulesValidationReport,
    ValidationError, ValidationPlan,
};

/// Maximum canonical payload accepted from one offline research run.
const MAX_RESEARCH_OUTPUT_BYTES: usize = 16 * 1024 * 1024;

/// Canonical output of a rules-only walk-forward run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RulesResearchRun {
    report: RulesValidationReport,
    report_bytes: Vec<u8>,
    artifact_bytes: Option<Vec<u8>>,
}

impl RulesResearchRun {
    /// Runs the immutable nested walk-forward protocol over one authoritative
    /// replay adapter. The adapter is expected to consume all selected markets
    /// from the same source stream; no per-market or approximate simulator is
    /// introduced at this boundary.
    pub fn run<E: RuleReplay>(
        plan: &ValidationPlan,
        provenance: ResearchProvenance,
        excluded_gaps: Vec<ExcludedGap>,
        replay: &mut E,
    ) -> Result<Self, ValidationError> {
        let report = RulesValidationReport::run(plan, provenance, excluded_gaps, replay)?;
        let report_bytes = report.canonical_json()?;
        ensure_output_size(report_bytes.len())?;

        // `from_canonical_json` verifies the report digest and all fold
        // invariants. Equality additionally catches accidental noncanonical
        // serialization changes before the bytes are persisted by a caller.
        let reopened = RulesValidationReport::from_canonical_json(&report_bytes)?;
        if reopened != report {
            return Err(ValidationError::InvalidJson);
        }

        let artifact_bytes = report
            .artifact()
            .map(|artifact| -> Result<Vec<u8>, ValidationError> {
                let bytes = artifact.canonical_json()?;
                ensure_output_size(bytes.len())?;
                let reopened = RulesArtifact::from_canonical_json(&bytes)?;
                if reopened != *artifact {
                    return Err(ValidationError::InvalidJson);
                }
                Ok(bytes)
            })
            .transpose()?;

        if artifact_bytes.is_some() {
            report.validate_active_pair()?;
        }

        Ok(Self {
            report,
            report_bytes,
            artifact_bytes,
        })
    }

    /// Returns the verified validation report.
    #[must_use]
    pub const fn report(&self) -> &RulesValidationReport {
        &self.report
    }

    /// Returns canonical report bytes suitable for atomic persistence.
    #[must_use]
    pub fn report_bytes(&self) -> &[u8] {
        &self.report_bytes
    }

    /// Returns canonical artifact bytes only when every hard eligibility gate
    /// passed. Ineligible runs never manufacture a fallback artifact.
    #[must_use]
    pub fn artifact_bytes(&self) -> Option<&[u8]> {
        self.artifact_bytes.as_deref()
    }

    /// Returns the verified frozen artifact, if the report is eligible.
    #[must_use]
    pub const fn artifact(&self) -> Option<&RulesArtifact> {
        self.report.artifact()
    }

    /// Returns whether the run produced an active-mode artifact.
    #[must_use]
    pub fn eligible(&self) -> bool {
        self.artifact_bytes.is_some()
    }
}

fn ensure_output_size(size: usize) -> Result<(), ValidationError> {
    (size <= MAX_RESEARCH_OUTPUT_BYTES)
        .then_some(())
        .ok_or(ValidationError::InvalidJson)
}
