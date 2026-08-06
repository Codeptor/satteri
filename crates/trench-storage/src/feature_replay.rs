//! Source-bound feature-input commitments for offline research replay.
//!
//! A feature witness commits only the inputs and their expected recomputation
//! coordinates. It intentionally cannot carry a feature snapshot: replay
//! must rebuild that derived value from the immutable source facts.

use std::collections::{BTreeMap, BTreeSet};

use blake3::Hasher;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use trench_core::{
    domain::{EventId, Market, Sleeve},
    event::{CandleInterval, MarketEvent, MarketEventKind, TimestampNs},
};

use crate::research_runs::{
    AvailabilitySourceReference, ResearchRunError, VerifiedResearchSourcePlan,
};

const FEATURE_INPUT_WITNESS_VERSION: u8 = 1;
const MAX_INPUT_REFERENCES: usize = 1_000_000;
const DIGEST_HEX_BYTES: usize = blake3::OUT_LEN * 2;

/// Fail-closed verification errors for a feature-input commitment.
#[derive(Debug, Error)]
pub enum FeatureReplayError {
    /// The immutable availability source plan could not be inspected.
    #[error(transparent)]
    ResearchRun(#[from] ResearchRunError),
    /// The serialized witness is malformed or its self-commitment has drifted.
    #[error("invalid feature input witness: {reason}")]
    InvalidWitness { reason: &'static str },
    /// A listed source input was not available at the frozen decision boundary.
    #[error("feature input source was received after its decision boundary")]
    LateSource,
    /// The decision reference did not resolve to its exact declared completed candle.
    #[error("feature decision coordinate does not match its source candle")]
    DecisionCoordinateMismatch,
    /// A recomputed feature contract differs from the immutable input witness.
    #[error("recomputed feature contract does not match its source-bound witness")]
    RecomputedContractMismatch,
    /// Bounded source input materialization exceeded the fixed replay limit.
    #[error("feature input witness exceeded its fixed resource limit")]
    ResourceLimit,
}

/// Serializable sleeve tag used by the immutable feature-input wire format.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum FeatureSleeve {
    FifteenMinute,
    OneHour,
}

impl FeatureSleeve {
    const fn from_sleeve(value: Sleeve) -> Self {
        match value {
            Sleeve::FifteenMinute => Self::FifteenMinute,
            Sleeve::OneHour => Self::OneHour,
        }
    }

    const fn sleeve(self) -> Sleeve {
        match self {
            Self::FifteenMinute => Sleeve::FifteenMinute,
            Self::OneHour => Sleeve::OneHour,
        }
    }

    const fn matches_interval(self, interval: CandleInterval) -> bool {
        matches!(
            (self, interval),
            (Self::FifteenMinute, CandleInterval::FifteenMinutes)
                | (Self::OneHour, CandleInterval::OneHour)
        )
    }
}

/// The non-authoritative output commitments expected from one feature recomputation.
///
/// This is deliberately a transient verification input, not a serializable feature
/// snapshot. The caller computes it after replaying [`VerifiedFeatureInputs`].
#[derive(Debug, Clone, Copy)]
pub struct RecomputedFeatureContract<'a> {
    /// Recomputed market coordinate.
    pub market: &'a Market,
    /// Recomputed decision sleeve.
    pub sleeve: Sleeve,
    /// Recomputed explicit as-of time.
    pub decision_at: TimestampNs,
    /// Recomputed frozen universe activation commitment.
    pub universe_activation_digest: &'a str,
    /// Recomputed feature schema commitment.
    pub feature_schema_digest: &'a str,
    /// Recomputed point-in-time feature input-range commitment.
    pub input_range_digest: &'a str,
    /// Recomputed long-horizon input commitment.
    pub long_history_digest: &'a str,
}

/// Canonical source-bound feature inputs for exactly one completed-candle decision.
///
/// The wire carries no `FeatureSnapshot` or other derived feature values. Its digest
/// covers every coordinate and every exact raw source reference, so callers cannot
/// retarget a witness after it has been frozen.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeatureInputWitness {
    decision_event_id: String,
    market: String,
    sleeve: FeatureSleeve,
    decision_at_ns: i64,
    universe_activation_digest: String,
    feature_schema_digest: String,
    input_range_digest: String,
    long_history_digest: String,
    input_references: Vec<AvailabilitySourceReference>,
    commitment_digest: String,
}

impl FeatureInputWitness {
    /// Freezes one feature-recomputation contract over exact raw availability facts.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        decision_event_id: EventId,
        market: Market,
        sleeve: Sleeve,
        decision_at: TimestampNs,
        universe_activation_digest: impl Into<String>,
        feature_schema_digest: impl Into<String>,
        input_range_digest: impl Into<String>,
        long_history_digest: impl Into<String>,
        mut input_references: Vec<AvailabilitySourceReference>,
    ) -> Result<Self, FeatureReplayError> {
        input_references.sort();
        let mut value = Self {
            decision_event_id: decision_event_id.as_str().to_owned(),
            market: market.as_str().to_owned(),
            sleeve: FeatureSleeve::from_sleeve(sleeve),
            decision_at_ns: decision_at.value(),
            universe_activation_digest: universe_activation_digest.into(),
            feature_schema_digest: feature_schema_digest.into(),
            input_range_digest: input_range_digest.into(),
            long_history_digest: long_history_digest.into(),
            input_references,
            commitment_digest: String::new(),
        };
        value.validate_shape()?;
        value.commitment_digest = value.expected_commitment_digest()?;
        Ok(value)
    }

    /// Returns the decision's immutable normalized event identity.
    #[must_use]
    pub fn decision_event_id(&self) -> &str {
        &self.decision_event_id
    }

    /// Returns the declared decision market.
    #[must_use]
    pub fn market(&self) -> &str {
        &self.market
    }

    /// Returns the declared decision sleeve.
    #[must_use]
    pub const fn sleeve(&self) -> Sleeve {
        self.sleeve.sleeve()
    }

    /// Returns the explicit source-time decision boundary.
    #[must_use]
    pub const fn decision_at_ns(&self) -> i64 {
        self.decision_at_ns
    }

    /// Returns the canonical input-witness commitment.
    #[must_use]
    pub fn commitment_digest(&self) -> &str {
        &self.commitment_digest
    }

    /// Returns the exact canonical raw source references needed for recomputation.
    #[must_use]
    pub fn input_references(&self) -> &[AvailabilitySourceReference] {
        &self.input_references
    }

    /// Descriptor-safely binds every input reference to a verified final source plan.
    ///
    /// This never returns a derived feature snapshot. It returns only the exact,
    /// timely normalized source facts needed by a caller's deterministic recomputer.
    pub fn verify_against(
        &self,
        source_plan: &VerifiedResearchSourcePlan,
    ) -> Result<VerifiedFeatureInputs, FeatureReplayError> {
        self.validate()?;
        source_plan.validate_source_references(&self.input_references)?;

        let requested = self
            .input_references
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        let mut resolved = BTreeMap::new();
        for record in source_plan.availability_run().records() {
            let record = record?;
            let reference = record.source_reference();
            if !requested.contains(&reference) {
                continue;
            }
            if record.event().received_at().value() > self.decision_at_ns {
                return Err(FeatureReplayError::LateSource);
            }
            if resolved.insert(reference, record.event().clone()).is_some() {
                return Err(FeatureReplayError::InvalidWitness {
                    reason: "one feature input reference resolved more than once",
                });
            }
        }
        if resolved.len() != self.input_references.len() {
            return Err(FeatureReplayError::InvalidWitness {
                reason: "a feature input reference was absent after source-plan verification",
            });
        }

        let decision = resolved
            .values()
            .find(|event| event.event_id().as_str() == self.decision_event_id)
            .ok_or(FeatureReplayError::DecisionCoordinateMismatch)?;
        if decision.market().as_str() != self.market
            || decision.event_time().value() != self.decision_at_ns
            || !matches!(decision.kind(), MarketEventKind::CompletedCandle(candle) if self.sleeve.matches_interval(candle.interval()))
        {
            return Err(FeatureReplayError::DecisionCoordinateMismatch);
        }

        let events = self
            .input_references
            .iter()
            .map(|reference| {
                resolved
                    .remove(reference)
                    .ok_or(FeatureReplayError::InvalidWitness {
                        reason: "feature input reference resolved inconsistently",
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(VerifiedFeatureInputs {
            witness: self.clone(),
            events,
        })
    }

    fn validate(&self) -> Result<(), FeatureReplayError> {
        self.validate_shape()?;
        if self.commitment_digest != self.expected_commitment_digest()? {
            return Err(FeatureReplayError::InvalidWitness {
                reason: "feature input witness commitment digest does not match its contents",
            });
        }
        Ok(())
    }

    fn validate_shape(&self) -> Result<(), FeatureReplayError> {
        EventId::new(self.decision_event_id.clone()).map_err(|_| {
            FeatureReplayError::InvalidWitness {
                reason: "feature decision event identifier is invalid",
            }
        })?;
        Market::new(self.market.clone()).map_err(|_| FeatureReplayError::InvalidWitness {
            reason: "feature decision market is invalid",
        })?;
        TimestampNs::new(i128::from(self.decision_at_ns)).map_err(|_| {
            FeatureReplayError::InvalidWitness {
                reason: "feature decision timestamp is invalid",
            }
        })?;
        for digest in [
            &self.universe_activation_digest,
            &self.feature_schema_digest,
            &self.input_range_digest,
            &self.long_history_digest,
        ] {
            validate_core_digest(digest)?;
        }
        if self.input_references.is_empty() || self.input_references.len() > MAX_INPUT_REFERENCES {
            return Err(FeatureReplayError::ResourceLimit);
        }
        let mut prior = None;
        for reference in &self.input_references {
            reference.validate()?;
            if prior.is_some_and(|previous: &AvailabilitySourceReference| previous >= reference) {
                return Err(FeatureReplayError::InvalidWitness {
                    reason: "feature input references are not strictly canonical",
                });
            }
            prior = Some(reference);
        }
        Ok(())
    }

    fn expected_commitment_digest(&self) -> Result<String, FeatureReplayError> {
        let wire = FeatureInputWitnessDigestWire {
            version: FEATURE_INPUT_WITNESS_VERSION,
            decision_event_id: &self.decision_event_id,
            market: &self.market,
            sleeve: self.sleeve,
            decision_at_ns: self.decision_at_ns,
            universe_activation_digest: &self.universe_activation_digest,
            feature_schema_digest: &self.feature_schema_digest,
            input_range_digest: &self.input_range_digest,
            long_history_digest: &self.long_history_digest,
            input_references: &self.input_references,
        };
        let bytes = serde_json::to_vec(&wire).map_err(|_| FeatureReplayError::InvalidWitness {
            reason: "feature input witness could not be canonically serialized",
        })?;
        let mut hasher = Hasher::new_derive_key("trench.feature-input-witness.v1");
        hasher.update(&(bytes.len() as u64).to_be_bytes());
        hasher.update(&bytes);
        Ok(format!("b3:{}", hasher.finalize().to_hex()))
    }
}

#[derive(Serialize)]
struct FeatureInputWitnessDigestWire<'a> {
    version: u8,
    decision_event_id: &'a str,
    market: &'a str,
    sleeve: FeatureSleeve,
    decision_at_ns: i64,
    universe_activation_digest: &'a str,
    feature_schema_digest: &'a str,
    input_range_digest: &'a str,
    long_history_digest: &'a str,
    input_references: &'a [AvailabilitySourceReference],
}

/// Exact source facts proven timely for one feature recomputation.
#[derive(Debug, Clone)]
pub struct VerifiedFeatureInputs {
    witness: FeatureInputWitness,
    events: Vec<MarketEvent>,
}

impl VerifiedFeatureInputs {
    /// Runs a caller-supplied deterministic feature recomputer over only verified source facts.
    pub fn recompute<T, E>(
        self,
        recompute: impl FnOnce(&[MarketEvent]) -> Result<T, E>,
    ) -> Result<T, E> {
        recompute(&self.events)
    }

    /// Verifies that a recomputer's non-authoritative output commitments still match the witness.
    pub fn verify_recomputed_contract(
        &self,
        recomputed: RecomputedFeatureContract<'_>,
    ) -> Result<(), FeatureReplayError> {
        if recomputed.market.as_str() != self.witness.market()
            || recomputed.sleeve != self.witness.sleeve()
            || recomputed.decision_at.value() != self.witness.decision_at_ns()
            || recomputed.universe_activation_digest != self.witness.universe_activation_digest
            || recomputed.feature_schema_digest != self.witness.feature_schema_digest
            || recomputed.input_range_digest != self.witness.input_range_digest
            || recomputed.long_history_digest != self.witness.long_history_digest
        {
            return Err(FeatureReplayError::RecomputedContractMismatch);
        }
        Ok(())
    }

    /// Returns the immutable input commitment used by this replay.
    #[must_use]
    pub fn witness(&self) -> &FeatureInputWitness {
        &self.witness
    }
}

/// Core feature and universe commitments are canonical *bare* lowercase BLAKE3 hex.
/// Raw source-member commitments remain `b3:`-prefixed within their source references.
fn validate_core_digest(value: &str) -> Result<(), FeatureReplayError> {
    if value.len() != DIGEST_HEX_BYTES
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(FeatureReplayError::InvalidWitness {
            reason: "feature commitment digest is not canonical lowercase BLAKE3 hex",
        });
    }
    Ok(())
}
