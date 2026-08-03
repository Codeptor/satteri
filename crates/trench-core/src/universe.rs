//! Immutable tradeable-universe contracts shared by point-in-time feature consumers.

use std::collections::BTreeSet;

use blake3::Hasher;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use thiserror::Error;

use crate::domain::Market;
use crate::event::TimestampNs;

/// Maximum markets that may be tradeable in one frozen universe snapshot.
pub const MAX_TRADEABLE_MARKETS: usize = 20;
const HOUR_NS: i64 = 3_600_000_000_000;

/// Invalid immutable tradeable-universe input.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum UniverseError {
    /// A frozen universe must name at least one eligible market.
    #[error("tradeable universe must contain at least one market")]
    Empty,
    /// A caller attempted to provide more members than the fixed tradeable rank range.
    #[error("tradeable universe has {actual} markets, exceeding the fixed limit {limit}")]
    TooManyMarkets {
        /// Number of supplied unique markets.
        actual: usize,
        /// Maximum fixed tradeable membership.
        limit: usize,
    },
    /// Serialized membership did not match its deterministic content digest.
    #[error("tradeable universe digest does not match its serialized membership")]
    DigestMismatch,
    /// A universe must freeze at a completed UTC hour.
    #[error("tradeable universe boundary {as_of_time} is not a completed UTC hour")]
    NotCompletedHour {
        /// Rejected immutable snapshot boundary.
        as_of_time: TimestampNs,
    },
}

/// Immutable, serializable membership at one explicit UTC decision boundary.
///
/// Task 8 will construct this from its completed hourly selector snapshot. It
/// intentionally carries only the rank-1-to-20 tradeable set; warm-only or
/// absent markets cannot leak into a strategy cross-section through this type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TradeableUniverse {
    as_of_time: TimestampNs,
    markets: BTreeSet<Market>,
    digest: String,
}

impl TradeableUniverse {
    /// Creates a checked frozen tradeable universe.
    ///
    /// # Errors
    ///
    /// Rejects empty membership and more than the fixed rank-1-to-20 range.
    pub fn new(
        as_of_time: TimestampNs,
        markets: impl IntoIterator<Item = Market>,
    ) -> Result<Self, UniverseError> {
        if as_of_time.value() % HOUR_NS != 0 {
            return Err(UniverseError::NotCompletedHour { as_of_time });
        }
        let markets = markets.into_iter().collect::<BTreeSet<_>>();
        if markets.is_empty() {
            return Err(UniverseError::Empty);
        }
        if markets.len() > MAX_TRADEABLE_MARKETS {
            return Err(UniverseError::TooManyMarkets {
                actual: markets.len(),
                limit: MAX_TRADEABLE_MARKETS,
            });
        }
        Ok(Self {
            as_of_time,
            digest: universe_digest(as_of_time, &markets),
            markets,
        })
    }

    /// Returns the explicit completed boundary at which this membership froze.
    #[must_use]
    pub const fn as_of_time(&self) -> TimestampNs {
        self.as_of_time
    }

    /// Returns the ordered immutable rank-1-to-20 market membership.
    #[must_use]
    pub const fn markets(&self) -> &BTreeSet<Market> {
        &self.markets
    }

    /// Returns whether a market is eligible for cross-sectional ranks.
    #[must_use]
    pub fn contains(&self, market: &Market) -> bool {
        self.markets.contains(market)
    }

    /// Returns whether this is the sole valid completed-hour membership for a decision time.
    ///
    /// A Task 8 selector freezes exactly once per completed UTC hour. A 15-minute
    /// decision uses that snapshot through the next hourly refresh, never a
    /// future membership and never a snapshot older than the immediately
    /// preceding completed hour.
    #[must_use]
    pub const fn is_current_for(&self, decision_time: TimestampNs) -> bool {
        let delta = decision_time.value() - self.as_of_time.value();
        delta >= 0 && delta < HOUR_NS
    }

    /// Returns a stable content digest used in feature provenance and snapshot hashes.
    #[must_use]
    pub fn digest(&self) -> &str {
        &self.digest
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TradeableUniverseWire {
    as_of_time_ns: i64,
    markets: Vec<String>,
    digest: String,
}

impl Serialize for TradeableUniverse {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        TradeableUniverseWire {
            as_of_time_ns: self.as_of_time.value(),
            markets: self
                .markets
                .iter()
                .map(|market| market.as_str().to_owned())
                .collect(),
            digest: self.digest.clone(),
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for TradeableUniverse {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = TradeableUniverseWire::deserialize(deserializer)?;
        let as_of_time =
            TimestampNs::new(i128::from(wire.as_of_time_ns)).map_err(serde::de::Error::custom)?;
        let markets = wire
            .markets
            .into_iter()
            .map(Market::new)
            .collect::<Result<Vec<_>, _>>()
            .map_err(serde::de::Error::custom)?;
        let universe = Self::new(as_of_time, markets).map_err(serde::de::Error::custom)?;
        if universe.digest != wire.digest {
            return Err(serde::de::Error::custom(UniverseError::DigestMismatch));
        }
        Ok(universe)
    }
}

fn universe_digest(as_of_time: TimestampNs, markets: &BTreeSet<Market>) -> String {
    let mut hasher = Hasher::new_derive_key("trench.tradeable-universe.v1");
    hasher.update(&as_of_time.value().to_be_bytes());
    for market in markets {
        hasher.update(market.as_str().as_bytes());
        hasher.update(&[0]);
    }
    hasher.finalize().to_hex().to_string()
}

#[cfg(test)]
mod tests {
    use crate::domain::Market;
    use crate::event::TimestampNs;

    use super::TradeableUniverse;

    #[test]
    fn round_trip_preserves_checked_membership_and_digest() {
        let universe = TradeableUniverse::new(
            TimestampNs::new(3_600_000_000_000).expect("timestamp must be valid"),
            [
                Market::new("ETH").expect("market must be valid"),
                Market::new("BTC").expect("market must be valid"),
            ],
        )
        .expect("membership must be valid");

        let encoded = serde_json::to_string(&universe).expect("universe must serialize");
        let decoded: TradeableUniverse =
            serde_json::from_str(&encoded).expect("universe must deserialize");
        assert_eq!(decoded, universe);
    }

    #[test]
    fn serialized_universe_rejects_unknown_wire_fields() {
        let universe = TradeableUniverse::new(
            TimestampNs::new(3_600_000_000_000).expect("timestamp must be valid"),
            [Market::new("BTC").expect("market must be valid")],
        )
        .expect("membership must be valid");
        let mut encoded = serde_json::to_value(universe).expect("universe must serialize");
        encoded["unexpected"] = serde_json::Value::Bool(true);

        assert!(
            serde_json::from_value::<TradeableUniverse>(encoded).is_err(),
            "unknown universe wire fields must fail closed"
        );
    }

    #[test]
    fn completed_hour_membership_is_current_for_each_following_fifteen_minute_decision() {
        let hour = TimestampNs::new(3_600_000_000_000).expect("timestamp must be valid");
        let universe =
            TradeableUniverse::new(hour, [Market::new("BTC").expect("market must be valid")])
                .expect("membership must be valid");

        for offset in [
            0_i128,
            900_000_000_000,
            1_800_000_000_000,
            2_700_000_000_000,
        ] {
            assert!(
                universe.is_current_for(
                    TimestampNs::new(i128::from(hour.value()) + offset)
                        .expect("decision timestamp must be valid")
                )
            );
        }
    }

    #[test]
    fn completed_hour_membership_rejects_future_and_stale_decisions() {
        let hour = TimestampNs::new(3_600_000_000_000).expect("timestamp must be valid");
        let universe =
            TradeableUniverse::new(hour, [Market::new("BTC").expect("market must be valid")])
                .expect("membership must be valid");

        assert!(
            !universe.is_current_for(
                TimestampNs::new(i128::from(hour.value()) - 900_000_000_000)
                    .expect("decision timestamp must be valid")
            )
        );
        assert!(
            !universe.is_current_for(
                TimestampNs::new(i128::from(hour.value()) + 3_600_000_000_000)
                    .expect("decision timestamp must be valid")
            )
        );
    }
}
