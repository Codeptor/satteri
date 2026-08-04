# Research Evidence Compiler Design

**Date:** 2026-08-04
**Status:** Approved
**Project root:** repository root (`./`)

## Decision

Build a paper-only, offline `ResearchEvidenceCompiler` before any live rules
reactor. It compiles immutable, committed market-data shards into a
content-addressed research sidecar that the existing Engine-backed rules replay
can verify. It never opens a paper position, creates a strategy artifact,
changes daemon readiness, or reads a wallet, signer, AWS credential, or
Telegram source.

The compiler is the sole bridge from raw public facts to research inputs. It
must recompute point-in-time values from raw witnesses instead of trusting
serialized feature or policy objects.

## Context

The current collector is durable and restart-safe, but it only routes passive
market facts. It does not instantiate `CommonFeatureEngine`,
`UniverseSelector`, `RiskEngine`, or `RulesStrategy`. The current
`EngineRulesReplay` validates a real Engine path but its facts are in-memory
and `DeterministicReplay` is deliberately bounded to 100,000 events and 16 MiB.
That is appropriate for fixtures and recovery, not 455-day research.

Historical candles captured at a later receipt time cannot become historical
decision inputs: a feature may use a candle only when the candle was available
by its completed boundary. The official archive is useful L2 evidence, but it
does not supply the complete trade, candle, funding, metadata, context, and
risk-input history needed to invent a point-in-time decision stream.

The first valid rules research run therefore requires 455 complete evaluation
days plus 90 days of causal warmup: roughly 545 continuous calendar days of
provenanced source coverage. Gaps remain explicit exclusions; they are never
filled from current data.

## Scope

### In scope

- A sharded, immutable source-plan format over committed Parquet partitions and
  capture batches.
- A staged-and-renamed research-sidecar format with bounded manifests, shard
  counts, byte counts, and records per shard.
- One canonical availability ordering:
  `(received_at, event_time, event_id)`.
- Recomputable witnesses for recovery, hourly universe snapshots, completed-bar
  feature inputs, and raw risk-policy inputs.
- A streaming compiler that produces only verified `ResearchFacts` consumed by
  the existing authoritative Engine rules replay.
- Deterministic exclusions and coverage reports for unavailable source ranges.
- Offline fixtures exceeding 100,000 events to prove sharding rather than a
  relaxed fixture limit.

### Explicitly out of scope

- Rules activation, live paper entries, or changing `RulesStartup`.
- `ml_champion`, model training, or any Python worker.
- AWS/archive downloading, credential storage, billing, wallet, signer,
  exchange action, or Telegram code.
- Replacing the current bounded `DeterministicReplay` fixture/recovery API.
- Any midpoint, current-universe, or approximate-cost substitute for missing
  historical evidence.

## Architecture

```
committed Parquet shards
        │  verified membership + identity
        ▼
ResearchSourcePlan ──► streaming compiler ──► ResearchSidecar
                               │                    │
             raw witnesses + recomputation          │
                               │                    ▼
                  excluded gaps / coverage   EngineRulesReplay
                                                (offline only)
```

`ResearchSourcePlan` is a read-only list of exact committed partitions/batches,
their provenance, UTC coverage, and canonical digest. It stores no filesystem
paths. Each member carries one immutable locator:

- `LegacyPartition { key, partition_id, manifest_digest }`; or
- `CapturePartition { batch_id, key, partition_id, batch_manifest_digest,
  partition_manifest_digest }`.

At plan-build time, the caller supplies the canonical configured Parquet root.
A `VerifiedShardResolver` constructs the one allowed final path below that root,
uses no root scan, rejects symlinks, rereads the exact legacy or capture-batch
manifest, and requires every committed digest to match before exposing rows.
Copying, linking, and arbitrary external paths are not source-plan operations.

`ResearchSourcePlanBuilder` then creates immutable availability runs inside the
private plan directory. One validated source member is bounded by the existing
partition limits, so it is sorted into an initial run by
`(received_at, event_time, event_id)`. The builder repeatedly merges at most
64 already-sorted runs into a new staged run until one final run remains. Each
run record contains the canonical normalized event plus its original member
ordinal, event ID, member digest, and a pre-run `member_set_digest`. That
digest covers only the canonically sorted original locators, member manifests,
and provenance, so it exists before any run is written. The final run digest
and every original member digest are then committed by the enclosing
`source_plan_digest`; it is computed last and never appears in a run record.
Thus plans with more than 64 members remain bounded, every copied event retains
an end-to-end reference to verified raw source evidence, and the digest graph
is acyclic.

The compiler opens one verified shard at a time, orders records by availability,
and maintains bounded per-market state. It derives candles only from trades
available by the bar close, applies recovery fences before exposing a book,
creates the full hourly `UniverseSelector` input, and derives `RiskPolicy` from
validated raw inputs. It writes witnesses and expected digests; it never writes
a deserialized `FeatureSnapshot` or `RiskPolicy` as authoritative state.

`StreamingRuleReplay` owns one bounded `EngineState`, market/recovery state,
feature engine, and source cursor for one requested fold. It opens only the
final, self-validating availability run and consumes records in canonical
order. It must neither reset state at a source-member boundary nor materialize
a complete `ResearchFacts` map. Decision witnesses are looked up from the
sidecar by decision ID as the cursor reaches that event. Its result stream uses
the existing Engine persistence records and an explicit fold-end checkpoint, so
the validator can aggregate outcomes without holding the full source stream in
memory.

`ResearchSidecar` is atomically published like a capture batch: private staged
directory, content validation, fsync of payloads and manifest, directory fsync,
then one rename. A reader accepts only a final directory whose manifest, source
plan, and every witness digest agree.

## Decision clock

A 15-minute or one-hour decision has one immutable `decision_at`: the exact
completed candle close. The compiler may emit it only when that candle's
`source_available_at <= decision_at`. Every feature span, universe/risk input,
recovery witness, and executable book used by the decision must have both
`event_time <= decision_at` and `received_at <= decision_at`; an executable
book must also satisfy the one-second age rule at that same boundary. The
compiler processes raw records in canonical availability order, but it does not
move a late bar's decision forward to its capture time. A late contributing
trade/candle therefore excludes that decision rather than creating look-ahead.

An entry request carries this same `decision_at`; the future live-reactor parity
fixture must consume the first valid post-decision execution event separately.
It must never treat that post-decision event as a feature or universe input.

## Sidecar contracts

| Contract | Persisted witness | Reader behavior |
| --- | --- | --- |
| Source plan | partition/batch ID, digest, coverage, provenance | Verify membership and source uniqueness before opening a shard. |
| Recovery | request identity, reconciled status/source, completion time, anchor ID, backfill IDs/digest | Revalidate against raw facts; construct a boundary only after proof. |
| Universe | raw hourly selector inputs, expected snapshot digest, source range | Re-run `UniverseSelector`; require byte-identical snapshot/digest. |
| Features | decision ID, expected snapshot digest, long-history input digest, source range | Re-run `CommonFeatureEngine`; reject any late or missing source. |
| Risk | raw venue constraints, book ID/digest, impact/funding distributions, config digest | Validate inputs then construct `RiskPolicy`; never deserialize one. |

Every decision record includes the source-plan digest, config/code/schema
digests, exact availability cutoff, and input event IDs. A decision is absent
when any required contract is absent, stale, late, or inconsistent.

## Coverage contract

The source plan declares the requested half-open evaluation and warmup
intervals plus a canonical `CoverageWitness` for each required
`(market, stream-kind)` interval. A witness is either:

- `Complete { first_event_id, last_event_id, digest, continuity_proof }`, where
  `continuity_proof` names a revalidated upstream archive manifest, paginated
  REST page chain, or captured WebSocket sequence/heartbeat range that covers
  the exact interval without omission. The proof carries its source digest,
  UTC range, and predecessor/successor identities. A bare operator assertion
  is not a proof. `ObservedNoEvents` uses the same proof contract; or
- `Unavailable { reason }`.

The compiler may not infer a quiet trade/BBO/L2/funding interval from the
absence of rows. Any required interval without a complete witness becomes a
merged, sorted, half-open excluded gap. Required streams are trades, executable
L2/BBO, funding, metadata/context, and hourly universe/risk inputs for every
market considered at that boundary. A market may have an explicit no-trade
interval only when its complete trade-stream witness proves it; it cannot use
that absence to satisfy a bar, book, or liquidity dependency.

## Failure behavior

- Missing or unproved data becomes a canonical excluded half-open UTC interval.
- A forged witness, plan mismatch, duplicate source identity, noncanonical
  ordering, resource limit breach, or partial sidecar fails the complete job.
- A failed compiler never publishes a partial sidecar and cannot alter SQLite.
- A replay with a different config/code/source-plan digest fails before Engine
  evaluation.
- The sidecar compiler does not alter daemon configuration or readiness. The
  separate future live-reactor specification must explicitly adopt these
  verified contracts before rules can be unsealed.

## Data acquisition boundary

Public REST and WebSocket data may seed and maintain current collection without
financial credentials. They are insufficient to bootstrap this research window:
the official candle surface caps results at 5,000 bars. The official historical
archive is requester-pays and L2-only. If used, an isolated acquisition machine
with explicit AWS billing authority stages compressed objects and immutable
manifests; the repository and VPS receive only verified files. Missing archive
objects stay missing.

## Acceptance criteria

1. A sidecar generated from a sharded fixture larger than 100,000 events opens
   without increasing `DeterministicReplay` limits.
   The corresponding source plan must use more than 64 members and prove that
   its multi-pass availability run has the same digest as a reference merge.
2. Recompiling the same source plan produces byte-identical manifests and
   Engine outcomes.
3. Reordering an event by late receipt, mutating a witness, reopening a recovery
   fence, changing a selector snapshot, or using a stale book fails closed.
4. Partial/staged sidecars, path/symlink attempts, duplicate source IDs, and
   source-plan drift are unreadable.
5. The compiler cannot produce a rules artifact or enable paper entries.
6. A future live-reactor parity fixture can prove byte-identical outcomes from
   the same verified source stream.

## Delivery sequence

1. Introduce sharded source-plan and atomic sidecar storage without Engine use.
2. Add canonical availability ordering and streaming source iteration.
3. Add recovery witness persistence/revalidation.
4. Add universe, feature, and raw risk-input witness compilers.
5. Bind the compiler to offline `EngineRulesReplay` and coverage reporting.
6. Specify the separate live-reactor phase only after compiler parity passes.
