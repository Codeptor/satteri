# Trench Rules Platform Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a deterministic, paper-only Rust service that ingests public Hyperliquid data and runs the independently accounted `rules_only` strategy through realistic risk and execution simulation.

**Architecture:** A pure `trench-core` crate owns deterministic domain logic, features, rules, risk, fills, and ledger transitions. Read-only venue I/O lives in `trench-hyperliquid`, persistence lives in `trench-storage`, and the `trenchd` binary is the only async orchestrator and SQLite writer. Raw analytical events go to atomic Parquet partitions; transactional decisions go through one SQLite WAL writer.

**Tech Stack:** Rust 2024, Tokio, reqwest/rustls, tokio-tungstenite, serde, rust_decimal, sqlx/SQLite, Apache Parquet, tracing, clap, proptest, wiremock.

---

## Scope and execution order

This is phase 1 of the approved [paper-bot design](../specs/2026-08-03-trench-paper-trading-bot-design.md). It produces a complete `rules_only` paper bot and deterministic replay tool. It deliberately does not create Python, ML, Telegram, wallet, signing, Trench order-submission, or Hyperliquid `/exchange` code. Run this plan before the ML and VPS plans.

Use `@test-driven-development`, `@rust-best-practices`, `@rust-async-patterns`, `@api-security-best-practices`, and `@SQLite Database Expert` while executing. Never modify the user-supplied `GETTING-STARTED.md` or `trench-perps-sdk-0.1.0.tgz`.

## Target file map

```text
AGENTS.md                                      project architecture, commands, invariants, known issues
Cargo.toml                                     Rust workspace and shared dependency policy
Cargo.lock                                     reproducible Rust dependency graph
rust-toolchain.toml                            pinned stable toolchain and components
rustfmt.toml                                   formatting policy
.gitignore                                     generated/runtime files only
config/paper.example.toml                      non-secret paper configuration
scripts/check-paper-boundary.sh                rejects wallet/action/live-execution code
crates/trench-core/
  Cargo.toml
  src/lib.rs                                   public deterministic API only
  src/domain.rs                                validated value types and identifiers
  src/config.rs                                typed paper/risk/universe configuration
  src/event.rs                                 normalized point-in-time market events
  src/book.rs                                  order-book state and deterministic depth walking
  src/candle.rs                                trade aggregation and completed bars
  src/universe.rs                              hourly hard gates and liquidity ranking
  src/features/mod.rs                          point-in-time feature snapshot API
  src/features/common.rs                       indicators shared by both future strategies
  src/features/rules.rs                        six interpretable signal families
  src/strategy/mod.rs                          signal/cost-acceptance/intent interfaces
  src/strategy/rules.rs                        regime, entry, rank, and exit logic
  src/risk/mod.rs                              authoritative risk-decision API
  src/risk/breakers.rs                         daily/weekly/drawdown/cooldown state machine
  src/risk/liquidation.rs                      tier-aware isolated liquidation calculation
  src/risk/sizing.rs                           cost-inclusive bisection and leverage selection
  src/broker/mod.rs                            paper-order state machine
  src/broker/cost.rs                           fees, funding, spread, impact decomposition
  src/broker/fill.rs                           IOC, partial-fill, mandatory-exit behavior
  src/ledger.rs                                cash, position, equity, and reconciliation
  src/engine.rs                                bar-close arbitration and event transitions
  src/validation.rs                            rules grid and chronological fold manifests
  tests/ledger_independence.rs                 state-isolation property test
  tests/risk_properties.rs                     risk and accounting properties
crates/trench-hyperliquid/
  Cargo.toml
  src/lib.rs                                   exports only read-only clients
  src/info.rs                                  `/info` REST requests and response types
  src/ws.rs                                    public WebSocket subscriptions/reconnect
  src/archive.rs                               explicit local official-archive importer
  src/normalize.rs                             wire payload to trench-core event conversion
  tests/read_only_surface.rs                   proves no action endpoint is constructible
crates/trench-storage/
  Cargo.toml
  migrations/0001_core.sql                     transactional schema
  src/lib.rs
  src/sqlite.rs                                one-writer WAL store and atomic transitions
  src/parquet.rs                               validated temporary-write/rename partitions
  src/replay.rs                                ordered event reader and manifests
  tests/recovery.rs                            crash/reopen reconciliation tests
crates/trenchd/
  Cargo.toml
  src/main.rs                                  CLI entry and Tokio runtime
  src/app.rs                                   component wiring and shutdown
  src/admin.rs                                 authenticated local admin protocol
  src/readiness.rs                             scoped readiness state machine
  src/writer.rs                                bounded persistence writer
  src/commands.rs                              collect, run, replay, doctor subcommands
tests/fixtures/
  archive/l2-sample.lz4                        official-format archive parser fixture
  stream/basic.jsonl                           deterministic captured-event fixture
  stream/gap-and-partial.jsonl                 gap/partial-exit fixture
  meta/native-perps.json                       point-in-time metadata fixture
```

## Invariants used by every task

- Prices, quantities, USDC, and fee calculations use `rust_decimal::Decimal`; indicators may use finite `f64` internally but must validate before becoming a domain value.
- `trench-core` has no Tokio, network, SQL, filesystem, or wall-clock dependency. All time and input enter explicitly.
- Every state transition is a pure function of prior state, normalized event, frozen config hash, and strategy version.
- Only completed bars produce decisions. `event_time` orders exchange facts; `received_at` measures latency.
- No module contains a private key type, wallet configuration, signing dependency, `/exchange` URL, or action request.
- Tests run offline by default. Network smoke tests are explicit ignored tests.

### Task 1: Scaffold the paper-only workspace

**Files:**
- Create: `AGENTS.md`
- Create: `Cargo.toml`
- Create: `rust-toolchain.toml`
- Create: `rustfmt.toml`
- Create: `.gitignore`
- Create: `config/paper.example.toml`
- Create: `crates/trench-core/Cargo.toml`
- Create: `crates/trench-core/src/lib.rs`
- Create: `crates/trench-hyperliquid/Cargo.toml`
- Create: `crates/trench-hyperliquid/src/lib.rs`
- Create: `crates/trench-storage/Cargo.toml`
- Create: `crates/trench-storage/src/lib.rs`
- Create: `crates/trenchd/Cargo.toml`
- Create: `crates/trenchd/src/main.rs`

- [ ] **Step 1: Add a workspace smoke test that initially has no packages**

Run: `cargo metadata --no-deps --format-version 1`

Expected: FAIL because the root has no `Cargo.toml`.

- [ ] **Step 2: Create the workspace manifests**

Use edition 2024 and resolver 3. The root workspace must list exactly the four crates above and centralize these dependencies: `anyhow`, `arrow-array`, `arrow-schema`, `async-trait`, `blake3`, `bytes`, `clap`, `futures-util`, `parquet`, `proptest`, `rand`, `reqwest` with `rustls-tls` and no native TLS, `rust_decimal`, `rust_decimal_macros`, `serde`, `serde_json`, `sqlx` with SQLite/Tokio/rustls/migrations, `tempfile`, `thiserror`, `time`, `tokio` with `full`, `tokio-tungstenite` with rustls, `tokio-util`, `toml`, `tracing`, `tracing-subscriber`, `uuid`, and `wiremock`. Commit `Cargo.lock` after the first successful build.

`crates/trench-core/src/lib.rs` starts with crate-level documentation only; each later task adds its module declaration together with the real module file. Do not create placeholder implementations. `trenchd` must return `anyhow::Result<()>` and initialize `tracing`, never `println!`.

- [ ] **Step 3: Record project conventions**

`AGENTS.md` must state the four-crate boundary, paper-only security invariant, single-writer rule, deterministic-time rule, commands (`cargo fmt --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test --workspace`), current known issue (VPS `systemd-networkd-wait-online` failure), and links to the design and three phase plans.

The example TOML must contain only public endpoints, local paths, feed thresholds, and the frozen risk numbers. It must have no wallet, account address, API key, or generic secret field.

- [ ] **Step 4: Verify the workspace**

Run: `cargo fmt --check && cargo metadata --no-deps --format-version 1 >/dev/null && cargo test --workspace`

Expected: all four packages build and zero tests fail.

- [ ] **Step 5: Commit**

```bash
git add AGENTS.md Cargo.toml Cargo.lock rust-toolchain.toml rustfmt.toml .gitignore config crates
git commit -m "build: scaffold paper trading workspace"
```

### Task 2: Add validated domain and configuration types

**Files:**
- Create: `crates/trench-core/src/domain.rs`
- Create: `crates/trench-core/src/config.rs`
- Modify: `crates/trench-core/src/lib.rs`
- Test: inline unit tests in both new modules

- [ ] **Step 1: Write failing value-type tests**

Cover rejection of zero/negative/non-finite prices, negative quantities, unknown ledger names, leverage outside `5..=20`, cross margin, fee values below the required 7.5 bps per side, and risk settings above the approved limits. Rules configuration has only `mode=collect_only|active`; active mode requires single-component artifact/report filenames plus both digests, while threshold/ATR/take-profit values are forbidden in TOML because they must come from the validated artifact. Include this collector happy-path shape:

```rust
let cfg = PaperConfig::from_toml(include_str!("../../../config/paper.example.toml"))?;
assert_eq!(cfg.risk.initial_equity, Usdc::new(dec!(100))?);
assert_eq!(cfg.risk.max_leverage, Leverage::new(20)?);
assert_eq!(cfg.margin_mode, MarginMode::Isolated);
assert_eq!(cfg.rules.mode, RulesMode::CollectOnly);
```

- [ ] **Step 2: Run the tests and observe the missing modules**

Run: `cargo test -p trench-core domain::tests && cargo test -p trench-core config::tests`

Expected: FAIL because the types do not exist.

- [ ] **Step 3: Implement strong types and fail-closed config parsing**

Implement private-field newtypes `Price`, `Quantity`, `Usdc`, `Bps`, `Leverage`, `Market`, `RunId`, and `EventId`; enums `Side`, `Sleeve`, `LedgerId`, `RulesMode`, and the single-variant `MarginMode::Isolated`. Constructors return `DomainError` via `thiserror`. `PaperConfig::validate` must enforce every fixed limit from design sections 6 and 8, require `rules.artifact_file`, `rules.artifact_digest`, `rules.validation_report_file`, and `rules.validation_report_digest` only in active mode, forbid independently configurable selected rule values, and reject unknown TOML fields with `#[serde(deny_unknown_fields)]`. Both file fields must be plain UTF-8 filenames: no absolute path, separator, `.`/`..`, NUL, or platform prefix.

Do not implement implicit `From<f64>` conversions. Implement explicit checked helpers and arithmetic that preserves units.

- [ ] **Step 4: Run focused and workspace tests**

Run: `cargo test -p trench-core domain::tests && cargo test -p trench-core config::tests && cargo clippy -p trench-core --all-targets -- -D warnings`

Expected: PASS with no warnings.

- [ ] **Step 5: Commit**

```bash
git add crates/trench-core config/paper.example.toml
git commit -m "feat(core): add validated paper domain types"
```

### Task 3: Define deterministic events and order-book mechanics

**Files:**
- Create: `crates/trench-core/src/event.rs`
- Create: `crates/trench-core/src/book.rs`
- Modify: `crates/trench-core/src/lib.rs`
- Test: inline unit tests

- [ ] **Step 1: Write failing event-order and depth-walk tests**

Test that duplicate trade identity `(block_time, coin, tid)` is stable, exchange time sorts before receive time, a crossed/stale book is rejected, and this ask book fills 150 USDC deterministically:

```rust
let asks = [(price!(100), qty!(1)), (price!(101), qty!(1))];
let fill = book.walk(Side::Buy, usdc!(150), bps!(50))?;
assert_eq!(fill.filled_quote, usdc!(100));
assert_eq!(fill.remaining_quote, usdc!(50));
assert_eq!(fill.levels_consumed, 1);
```

- [ ] **Step 2: Verify failure**

Run: `cargo test -p trench-core event::tests && cargo test -p trench-core book::tests`

Expected: FAIL for missing `event` and `book` modules.

- [ ] **Step 3: Implement normalized events and immutable snapshots**

`MarketEvent` must contain `event_id`, `event_time`, `received_at`, `market`, and one `MarketEventKind`: metadata, asset context, book snapshot, BBO, trade, funding, or completed candle. Build event IDs from canonical exchange identity with BLAKE3. `OrderBook::apply_snapshot` validates sorted unique levels, positive values, uncrossed BBO, monotonic exchange time, and freshness supplied by the caller. `walk` may consume only visible levels inside the caller's limit band and returns exact per-level fills without mutating the snapshot.

- [ ] **Step 4: Run tests and properties**

Add a proptest proving filled quote never exceeds requested quote or visible eligible depth. Run: `cargo test -p trench-core event::tests && cargo test -p trench-core book::tests`

Expected: PASS, including proptest.

- [ ] **Step 5: Commit**

```bash
git add crates/trench-core
git commit -m "feat(core): normalize events and order books"
```

### Task 4: Create the single-writer SQLite journal

**Files:**
- Create: `crates/trench-storage/migrations/0001_core.sql`
- Create: `crates/trench-storage/src/sqlite.rs`
- Modify: `crates/trench-storage/src/lib.rs`
- Create: `crates/trench-storage/tests/recovery.rs`

- [ ] **Step 1: Write a failing atomic-transition test**

Open a temporary database, append one event plus one risk rejection in a transaction, reopen it, and assert both rows exist. Inject a failure between statements and assert neither row exists. Also assert `PRAGMA journal_mode` is `wal`, `synchronous` is `2`, and `foreign_keys` is `1`.

- [ ] **Step 2: Verify failure**

Run: `cargo test -p trench-storage --test recovery`

Expected: FAIL because no migration or store exists.

- [ ] **Step 3: Add the initial schema and store**

The migration must create constrained tables for `runs`, `config_manifests`, `events`, `universe_snapshots`, `feature_snapshots`, `signals`, `order_intents`, `risk_decisions`, `paper_orders`, `fills`, `positions`, `funding_entries`, `equity_snapshots`, `breaker_transitions`, `health_transitions`, and `reconciliation_checkpoints`. Every child references `run_id`; decision/fill tables carry `ledger_id`; event IDs and transition IDs are unique. Monetary values are canonical decimal strings, never SQLite `REAL`.

`SqliteStore::open` uses one connection, WAL, `synchronous=FULL`, foreign keys, a busy timeout, migrations, and `0700` parent/`0600` database permissions on Unix. Expose transaction methods, not a raw pool.

- [ ] **Step 4: Run storage tests**

Run: `cargo test -p trench-storage && cargo clippy -p trench-storage --all-targets -- -D warnings`

Expected: PASS; rollback test leaves no partial state.

- [ ] **Step 5: Commit**

```bash
git add crates/trench-storage
git commit -m "feat(storage): add atomic SQLite event journal"
```

### Task 5: Implement the read-only Hyperliquid REST boundary

**Files:**
- Create: `crates/trench-hyperliquid/src/info.rs`
- Create: `crates/trench-hyperliquid/src/normalize.rs`
- Modify: `crates/trench-hyperliquid/src/lib.rs`
- Create: `crates/trench-hyperliquid/tests/read_only_surface.rs`
- Create: `tests/fixtures/meta/native-perps.json`

- [ ] **Step 1: Write failing fixture and endpoint tests**

Use `wiremock` to require `POST /info` with only these request variants: `metaAndAssetCtxs`, `allMids`, `l2Book`, `candleSnapshot`, and historical funding. Deserialize the fixture and assert BTC/ETH/SOL are ordinary native-perp rows with point-in-time leverage/precision. Assert the public client base type exposes no action URL or signer.

- [ ] **Step 2: Verify failure**

Run: `cargo test -p trench-hyperliquid --test read_only_surface`

Expected: FAIL because `InfoClient` is absent.

- [ ] **Step 3: Implement the constrained client**

`InfoClient` owns one validated HTTPS `/info` URL and `reqwest::Client`; the URL constructor must require HTTPS except in tests. Model decimal strings as strings at the wire boundary and convert through checked domain constructors. Set connect/request deadlines, a descriptive user agent, bounded response size, and explicit status/body errors. Do not add a generic JSON request method to the public API.

- [ ] **Step 4: Test malformed and oversized responses**

Add cases for unknown fields that are safe to ignore, missing required fields, malformed decimals, HTTP 429/500, deadline, and body over the configured limit. Run: `cargo test -p trench-hyperliquid`

Expected: PASS without network access.

- [ ] **Step 5: Commit**

```bash
git add crates/trench-hyperliquid tests/fixtures/meta
git commit -m "feat(market): add read-only Hyperliquid info client"
```

### Task 6: Add WebSocket normalization, reconnect, and gap accounting

**Files:**
- Create: `crates/trench-hyperliquid/src/ws.rs`
- Modify: `crates/trench-hyperliquid/src/normalize.rs`
- Modify: `crates/trench-hyperliquid/src/lib.rs`
- Test: inline async tests with a local WebSocket server

- [ ] **Step 1: Write failing subscription and disconnect tests**

Assert subscriptions can be built only for `allMids`, `l2Book`, `bbo`, `trades`, and active-asset context. Feed duplicate trades, out-of-order books, malformed frames, ping/pong, and a forced disconnect. Expected outputs are normalized events plus explicit `GapOpened`/`GapClosed` control events; no strategy-ready signal may appear before a fresh snapshot.

- [ ] **Step 2: Verify failure**

Run: `cargo test -p trench-hyperliquid ws::tests`

Expected: FAIL because `WsClient` is absent.

- [ ] **Step 3: Implement bounded async ingestion**

Use rustls, `tokio::select!`, a bounded `mpsc` output, heartbeat timeout, capped exponential reconnect with full jitter, and cancellation tokens. On reconnect, obtain fresh metadata/book snapshots through `InfoClient`, emit the exact gap plus a typed `GapRecoveryRequest` for each affected market/interval, then resume incremental data in quarantined state. Duplicate identities are dropped; non-monotonic/crossed data emits quarantine state rather than being repaired. Task 7A consumes recovery requests after candle aggregation exists.

- [ ] **Step 4: Run async tests**

Run: `cargo test -p trench-hyperliquid ws::tests -- --nocapture`

Expected: PASS with bounded channel/backpressure and deterministic test time.

- [ ] **Step 5: Commit**

```bash
git add crates/trench-hyperliquid
git commit -m "feat(market): add resilient public WebSocket ingestion"
```

### Task 6A: Parse official historical market archives locally

**Files:**
- Create: `crates/trench-hyperliquid/src/archive.rs`
- Modify: `crates/trench-hyperliquid/src/lib.rs`
- Create: `tests/fixtures/archive/l2-sample.lz4`
- Test: bounded parser tests

- [ ] **Step 1: Write failing official-format parser tests**

Parse a minimal byte-for-byte fixture in the official requester-pays archive format and assert normalized market, event time, book/trade identity, decimal precision, ordering, and content digest. Reject truncated compression, unknown record version, path/manifest mismatch, duplicate conflicts, future timestamps, and an interval whose required L2/BBO source is absent.

- [ ] **Step 2: Verify failure**

Run: `cargo test -p trench-hyperliquid archive::tests`

Expected: FAIL because the archive importer is absent.

- [ ] **Step 3: Implement a bounded explicit-path archive reader**

`ArchiveReader::open(source_root, manifest)` accepts only previously downloaded official archive files beneath a resolved root, verifies requester-pays source metadata/digests, streams decompression with bounded memory, and yields normalized events through the same wire conversion as live data. It contains no storage, candle, AWS client, or credential dependency; an operator may download requester-pays data on a trusted workstation, but credentials never enter the repository or VPS.

- [ ] **Step 4: Validate archive completeness metadata**

Return ordered normalized events plus exact present/missing/conflicting source spans. Do not derive candles or write Parquet in this task; Task 7A performs candle reconciliation after the aggregator exists and Task 14 integrates atomic storage.

- [ ] **Step 5: Run importer tests**

Run: `cargo test -p trench-hyperliquid archive::tests`

Expected: PASS with deterministic event/span digests.

- [ ] **Step 6: Commit**

```bash
git add crates/trench-hyperliquid tests/fixtures/archive
git commit -m "feat(data): parse official historical archives"
```

### Task 7: Build completed candles and point-in-time common features

**Files:**
- Create: `crates/trench-core/src/candle.rs`
- Create: `crates/trench-core/src/features/mod.rs`
- Create: `crates/trench-core/src/features/common.rs`
- Modify: `crates/trench-core/src/lib.rs`
- Test: inline unit and property tests

- [ ] **Step 1: Write failing candle and no-lookahead tests**

Create trades straddling a 15-minute boundary and assert the earlier candle emits once, only after its close. Reorder duplicate inputs and assert byte-identical candles. For EMA, ATR14, RSI14, ADX14, realized volatility, Donchian20, volume robust z-score, premium/OI/funding windows, spread/depth/trade imbalance, and cross-sectional ranks, alter a future bar and assert every earlier snapshot remains unchanged.

- [ ] **Step 2: Verify failure**

Run: `cargo test -p trench-core candle::tests && cargo test -p trench-core features::common::tests`

Expected: FAIL for missing modules.

- [ ] **Step 3: Implement bar aggregation and finite feature snapshots**

Support exactly `15m` and `1h`. Store warmup state per `(market, sleeve)` and emit immutable snapshots carrying `as_of_time`, input event range, completeness flags, and a stable feature-schema hash. Use completed 1-hour data for regime inputs even when evaluating the 15-minute sleeve. Missing inputs make the market/sleeve unready; do not impute.

- [ ] **Step 4: Run deterministic properties**

Add properties for idempotent duplicate handling, no non-finite feature, and identical replay output. Run: `cargo test -p trench-core candle::tests && cargo test -p trench-core features::common::tests`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/trench-core
git commit -m "feat(features): add point-in-time market features"
```

### Task 7A: Reconcile live gaps and archive candles

**Files:**
- Create: `crates/trench-hyperliquid/src/recovery.rs`
- Modify: `crates/trench-hyperliquid/src/lib.rs`
- Modify: `crates/trench-hyperliquid/src/ws.rs`
- Test: recovery tests using the candle aggregator and archive fixture

- [ ] **Step 1: Write failing live-gap reconciliation tests**

Feed a Task-6 `GapRecoveryRequest`, mock `candleSnapshot` responses for the exact 15-minute/1-hour range, and provide locally derived trades. Assert matching OHLCV closes the gap and rebuilds warmup, a mismatch persists a conflict/quarantine span, and an unavailable interval stays unavailable. No strategy-ready event may occur before every required interval is resolved or explicitly unavailable.

- [ ] **Step 2: Write failing archive reconciliation tests**

Stream Task-6A normalized archive trades/books through the real `CandleAggregator`, compare against point-in-time official candles, and assert deterministic completed candles plus present/missing/conflicting spans. Reject synthesized books, current-universe substitution, or midpoint fills.

- [ ] **Step 3: Verify failure**

Run: `cargo test -p trench-hyperliquid recovery::tests`

Expected: FAIL because recovery orchestration is absent.

- [ ] **Step 4: Implement recovery as a pure event producer**

`GapRecovery` accepts a typed request, explicit REST/archive streams, and prior candle state, then emits normalized backfill events plus a complete `RecoveryResult`; it does not write storage or own readiness. Live WebSocket orchestration consumes the result and exits quarantine only on a complete result. Archive import and durable persistence are connected later in Task 14 after the Parquet store exists.

- [ ] **Step 5: Run focused tests**

Run: `cargo test -p trench-hyperliquid recovery::tests && cargo test -p trench-core candle::tests`

Expected: PASS with byte-stable event/span digests.

- [ ] **Step 6: Commit**

```bash
git add crates/trench-hyperliquid
git commit -m "feat(data): reconcile market data gaps"
```

### Task 8: Implement the dynamic native-perp universe

**Files:**
- Create: `crates/trench-core/src/universe.rs`
- Modify: `crates/trench-core/src/lib.rs`
- Test: inline unit tests using `tests/fixtures/meta/native-perps.json`

- [ ] **Step 1: Write failing hard-gate and ranking tests**

Cover every hard gate from design section 6.2. Construct 31 eligible markets with tied metrics and prove deterministic market-symbol tie-breaking, ranks 1-20 tradeable, 21-30 warm-only, and 31 absent. Prove a hard-gate failure removes a member immediately while a rank change activates only at the next completed strategy bar. BTC, ETH, and SOL must pass or fail by identical logic.

- [ ] **Step 2: Verify failure**

Run: `cargo test -p trench-core universe::tests`

Expected: FAIL because `UniverseSelector` is absent.

- [ ] **Step 3: Implement structural eligibility and frozen scoring**

Use the fixed 500 USDC probe and exact hard thresholds. Compute robust cross-sectional percentiles and score `0.30*volume + 0.20*OI + 0.30*inverse_spread + 0.15*depth + 0.05*continuity`. Return a complete snapshot with metric inputs, ranks, memberships, and machine-readable exclusion reasons. The selector accepts an explicit hourly timestamp and point-in-time history coverage; it has no strategy/risk input.

- [ ] **Step 4: Run universe tests**

Run: `cargo test -p trench-core universe::tests && cargo clippy -p trench-core --all-targets -- -D warnings`

Expected: PASS with stable ordering.

- [ ] **Step 5: Commit**

```bash
git add crates/trench-core
git commit -m "feat(universe): select deep native perp markets"
```

### Task 9: Implement the auditable rules strategy

**Files:**
- Create: `crates/trench-core/src/features/rules.rs`
- Create: `crates/trench-core/src/strategy/mod.rs`
- Create: `crates/trench-core/src/strategy/rules.rs`
- Modify: `crates/trench-core/src/lib.rs`
- Test: inline tests and fixed golden score fixtures

- [ ] **Step 1: Write failing family/regime tests**

Test each family independently with hand-calculated inputs, including sign behavior for momentum and derivatives. Test trend/range/transition/extreme/high-volatility regimes, the exact fixed weight table, three-family agreement, adverse-swing stop bounds, 2R target, opposite-signal exit, and four-bar timeout. Separately test that an un-sized signal accepts/rejects a public `CostQuote` using the exact 1.5x edge gate without seeing the sealed order. Serialize the explanation and assert it contains all six scores, weights, threshold, regime, cost estimate, and rejection reasons.

- [ ] **Step 2: Verify failure**

Run: `cargo test -p trench-core features::rules::tests && cargo test -p trench-core strategy::rules::tests`

Expected: FAIL because rule scoring does not exist.

- [ ] **Step 3: Implement the exact approved formulas**

Implement `robust_z`, `unit`, imbalance, all six clipped families, regime selection, and the table in design section 7.2 without configurable weights. `RulesStrategy::on_bar` returns zero or more un-sized `SignalCandidate`s carrying market, side, sleeve, gross edge, stop, target, time exit, snapshot hash, and full explanation. `Strategy::accept_cost(candidate, CostQuote)` may return an `OrderIntent` containing the opaque `quote_id`, but `CostQuote` exposes only total/attributed cost fractions, freshness/source digests, and feasibility reasons. It never exposes ledger PnL, quantity, margin, leverage, the sealed approved order, or ML output.

- [ ] **Step 4: Run score and snapshot tests**

Run: `cargo test -p trench-core features::rules::tests && cargo test -p trench-core strategy::rules::tests`

Expected: PASS with byte-stable golden explanations.

- [ ] **Step 5: Commit**

```bash
git add crates/trench-core
git commit -m "feat(strategy): add interpretable rules signals"
```

### Task 10: Implement breaker state and ledger accounting

**Files:**
- Create: `crates/trench-core/src/risk/breakers.rs`
- Create: `crates/trench-core/src/ledger.rs`
- Create: `crates/trench-core/tests/ledger_independence.rs`
- Modify: `crates/trench-core/src/risk/mod.rs`
- Modify: `crates/trench-core/src/lib.rs`

- [ ] **Step 1: Write failing breaker/accounting tests**

Use a 100 USDC ledger to trigger exactly 0.5% trade budget, 1.5% daily, 4% weekly, and 8% high-water drawdown boundaries. Assert three realized losses create a 12-hour cooldown, six UTC-day entries allow no seventh, daily/weekly reset only after reconciliation, and hard drawdown stays latched. For an open long, prove unrealized PnL and breaker equity use a size-aware executable bid-side exit quote; for a short, use the ask side. Apply identical fills to two ledgers then mutate one; assert the other is byte-identical to its pre-mutation state.

- [ ] **Step 2: Verify failure**

Run: `cargo test -p trench-core risk::breakers::tests && cargo test -p trench-core --test ledger_independence`

Expected: FAIL because breaker/ledger state is absent.

- [ ] **Step 3: Implement exact accounting transitions**

Ledger state contains cash, isolated margin, one optional position, realized/unrealized PnL, fees, funding, equity, high-water mark, UTC anchors, entry count, consecutive losses, cooldown, and breaker states. Every mutation returns a journalable transition plus the new state; reject negative size, overlapping positions, averaging, pyramiding, cross-ledger netting, and unreconciled resets. `mark_to_book` walks enough opposite-side depth to close the full remaining size, subtracts estimated exit fees/funding, and uses the 200-bps mandatory-exit boundary price for any visible-depth shortfall while flagging `liquidity_incomplete`; it never marks at mid or mark price. A missing/stale book preserves the last executable valuation with a stale flag and blocks new entries until a fresh executable mark arrives.

- [ ] **Step 4: Add accounting properties**

Prove `equity = cash + isolated_margin + marked_position_value_adjustment`, fee/funding debits conserve value, and breaker budgets never increase inside their period. Run: `cargo test -p trench-core risk::breakers::tests && cargo test -p trench-core --test ledger_independence`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/trench-core
git commit -m "feat(risk): add independent ledgers and breakers"
```

### Task 11: Add tier-aware liquidation and cost-inclusive sizing

**Files:**
- Create: `crates/trench-core/src/risk/liquidation.rs`
- Create: `crates/trench-core/src/risk/sizing.rs`
- Create: `crates/trench-core/tests/risk_properties.rs`
- Modify: `crates/trench-core/src/risk/mod.rs`

- [ ] **Step 1: Write failing liquidation examples**

For `q=1`, `p_ref=100`, isolated equity `5`, maintenance rate `0.025`, and deduction `0`, assert long liquidation `97.435897...` and short liquidation `102.439024...`. Add a two-tier boundary case and assert only the tier containing `q * liq_price` is accepted. Assert funding debit moves liquidation adversely and liquidation distance must be at least `2.5 * stop_distance`.

- [ ] **Step 2: Write failing sizing properties**

Build deterministic book/cost fixtures and assert the selected rounded notional is the greatest one whose entry-to-stressed-stop loss plus both fees and funding reserve is at most 0.5% equity. Assert margin stays at or below 25% equity, the lowest safe integer leverage in `5..=20` wins, venue leverage/precision/minimums apply, and increasing any cost cannot increase size. Quote several candidates from the same immutable flat-ledger/book snapshot, expose only their public costs, consume exactly one sealed approval by `quote_id`, and reject reuse or a quote whose ledger/book/config digest changed.

- [ ] **Step 3: Verify failure**

Run: `cargo test -p trench-core risk::liquidation::tests && cargo test -p trench-core risk::sizing::tests && cargo test -p trench-core --test risk_properties`

Expected: FAIL because liquidation/sizing are absent.

- [ ] **Step 4: Implement deterministic solvers**

Implement the exact reference-equity liquidation equation from design section 8.2 and piecewise tier search. Implement decimal bisection over venue-rounded notional, stressed impact as `max(2*current, trailing_30d_p99)`, worst scheduled funding reserve, all non-edge breaker/asset/depth/cost caps, then lowest safe leverage selection. `RiskEngine::quote_candidate` returns a `RiskQuote` containing a public `CostQuote` plus a private sealed `ApprovedOrder`; the seal binds quote ID, candidate, ledger, book, universe, config, and event digests. `consume_quote` releases the approved order exactly once only after strategy cost acceptance. Return exhaustive machine-readable rejections and record 5x/10x/15x/20x counterfactuals without influencing approval.

- [ ] **Step 5: Run focused, property, and lint checks**

Run: `cargo test -p trench-core risk::liquidation::tests && cargo test -p trench-core risk::sizing::tests && cargo test -p trench-core --test risk_properties && cargo clippy -p trench-core --all-targets -- -D warnings`

Expected: PASS, including monotonicity properties.

- [ ] **Step 6: Commit**

```bash
git add crates/trench-core
git commit -m "feat(risk): size isolated positions with liquidation limits"
```

### Task 12: Implement the realistic paper broker

**Files:**
- Create: `crates/trench-core/src/broker/mod.rs`
- Create: `crates/trench-core/src/broker/cost.rs`
- Create: `crates/trench-core/src/broker/fill.rs`
- Modify: `crates/trench-core/src/lib.rs`
- Test: inline state-machine tests

- [ ] **Step 1: Write failing entry/fill tests**

Test 7.5 bps per-side fixed fees, visible-level walking, first valid post-decision book selection, observed latency attribution, partial entry cancellation, below-minimum dust forced exit, mark-triggered stop/TP, worse post-gap fill, and funding at venue timestamps. Cost output must separate protocol fee, builder fee, spread, depth, latency, funding, and gross alpha. Assert marked equity and every daily/weekly/drawdown check use the broker's executable full-exit quote, never mid/mark.

- [ ] **Step 2: Write failing mandatory-exit tests**

Assert normal exits retry at 50 bps for five seconds, then become mandatory. Stops/breakers become mandatory immediately. Each unsuccessful fresh-book attempt widens 25 bps up to 200 bps, blocks entries until rounded size is zero, and never fabricates closure when data ends. Test book liquidation first and two-thirds-maintenance backstop second.

- [ ] **Step 3: Verify failure**

Run: `cargo test -p trench-core broker::`

Expected: FAIL because broker modules do not exist.

- [ ] **Step 4: Implement the broker state machine**

Use explicit states `PendingEntry`, `Open`, `NormalExit`, `MandatoryExit`, `Flat`, `Liquidated`, and `Unresolved`. Consume only immutable books/events supplied by the engine. Every transition emits order/fill/cost records and updated stops/targets sized to actual fills. Decision-to-book and trigger-to-book latencies are persisted as append-only observed samples with deployment/run digests for later robustness sampling. Primary fills are taker-only; maker results are isolated counterfactual records.

- [ ] **Step 5: Run broker tests**

Run: `cargo test -p trench-core broker:: && cargo clippy -p trench-core --all-targets -- -D warnings`

Expected: PASS with no closure of residual exposure.

- [ ] **Step 6: Commit**

```bash
git add crates/trench-core
git commit -m "feat(broker): simulate realistic perpetual fills"
```

### Task 13: Integrate arbitration, risk, broker, and journal transitions

**Files:**
- Create: `crates/trench-core/src/engine.rs`
- Modify: `crates/trench-core/src/lib.rs`
- Modify: `crates/trench-storage/src/sqlite.rs`
- Test: `crates/trench-core/tests/ledger_independence.rs`

- [ ] **Step 1: Write a failing bar-close integration test**

Feed two markets and two sleeves at one boundary. Assert the engine obtains sealed risk-sized quotes for every signal from the same immutable ledger/book snapshot, strategies see only public costs, accepted candidates rank by conservative net edge, and only the best accepted quote is consumed. Assert stale/unselected quotes cannot execute, an open ledger rejects new entries, exit priority is breaker/liquidation prevention, stop, TP, opposite signal, then time, and every input/quote/decision/rejection/fill transition shares one causality ID.

- [ ] **Step 2: Verify failure**

Run: `cargo test -p trench-core engine::tests && cargo test -p trench-core --test ledger_independence`

Expected: FAIL because `Engine` is absent.

- [ ] **Step 3: Implement a pure transition engine**

`Engine::apply(event, prior_state, context) -> Vec<Transition>` owns no clock or I/O. It updates market state, detects completed boundaries, requests un-sized signals, asks risk for sealed risk-sized quotes against one immutable snapshot, passes only each public cost quote back to the owning strategy, ranks accepted intents by conservative net edge, consumes one still-fresh sealed quote, routes its paper order, applies fills/funding/executable-book marks, and emits atomic persistence batches. This signal → sealed quote → cost acceptance → single consume sequence is the only entry path in runtime and replay. It must allow mandatory exits while strategy readiness is false and must never call a strategy during replay with a mismatched config/version hash.

- [ ] **Step 4: Persist an engine batch atomically**

Extend `SqliteStore` so one engine batch writes event, snapshots, signal, intent, risk result, order/fills, ledger, breakers, and checkpoint in one transaction. Reapplying an existing event ID is an idempotent no-op with an audit counter.

- [ ] **Step 5: Run integration tests**

Run: `cargo test -p trench-core engine::tests && cargo test -p trench-core --test ledger_independence && cargo test -p trench-storage`

Expected: PASS and identical state under deterministic replay.

- [ ] **Step 6: Commit**

```bash
git add crates/trench-core crates/trench-storage
git commit -m "feat(engine): connect rules risk and paper execution"
```

### Task 14: Add atomic Parquet storage and deterministic replay

**Files:**
- Create: `crates/trench-storage/src/parquet.rs`
- Create: `crates/trench-storage/src/replay.rs`
- Modify: `crates/trench-storage/src/lib.rs`
- Create: `crates/trenchd/src/commands.rs`
- Modify: `crates/trenchd/src/main.rs`
- Create: `tests/fixtures/stream/basic.jsonl`
- Create: `tests/fixtures/stream/gap-and-partial.jsonl`
- Test: `crates/trench-storage/tests/recovery.rs`

- [ ] **Step 1: Write failing partition/replay tests**

Write an event batch to a temp directory, inject failure before rename, and assert only a `.tmp` sibling exists and is ignored. Complete a write and assert schema hash, row count, min/max event time, and content digest match its manifest. Stream the official archive fixture through `ArchiveReader` and `GapRecovery` into the same sink and assert identical normalized partitions to an equivalent live fixture. Replay shuffled input sources and assert ordered events and final engine digest equal the golden result.

- [ ] **Step 2: Verify failure**

Run: `cargo test -p trench-storage parquet && cargo test -p trench-storage replay && cargo test -p trench-storage --test recovery`

Expected: FAIL because Parquet/replay code is absent.

- [ ] **Step 3: Implement partitioning and manifests**

Partition high-rate rows by UTC date/event kind/market, write a bounded batch to a temporary sibling, fsync file and directory, validate by reopening, then atomically rename. Retain normalized trades, bounded BBO/L2, candles, contexts, funding, and feature matrices. Replay merges partitions by `(event_time, deterministic_kind_order, event_id)` and verifies config, code, schema, and data digests before yielding.

Now add `trenchd import-archive --config PATH --source ABSOLUTE_PATH --manifest ABSOLUTE_PATH`. It composes the already-tested `ArchiveReader` and `GapRecovery` with this atomic Parquet sink, persists present/missing/conflicting intervals, and emits a content-addressed import manifest. It has no AWS/credential code and never writes through an alternate storage path.

- [ ] **Step 4: Add golden fixtures**

The basic fixture must open and close one rules trade. The gap fixture must exercise disconnect quarantine, a partial entry, a mandatory residual exit, and unresolved end-of-data rejection. Store expected final ledger/event digests alongside each fixture.

- [ ] **Step 5: Run replay tests**

Run: `cargo test -p trench-storage && cargo test -p trench-core engine::tests && cargo test -p trenchd commands::import_archive_tests`

Expected: PASS with byte-stable digests.

- [ ] **Step 6: Commit**

```bash
git add crates/trench-storage crates/trenchd tests/fixtures/stream
git commit -m "feat(replay): persist and replay market events"
```

### Task 15: Wire `trenchd`, scoped readiness, and startup recovery

**Files:**
- Create: `crates/trenchd/src/app.rs`
- Create: `crates/trenchd/src/admin.rs`
- Create: `crates/trenchd/src/readiness.rs`
- Create: `crates/trenchd/src/writer.rs`
- Modify: `crates/trenchd/src/commands.rs`
- Modify: `crates/trenchd/src/main.rs`
- Modify: `crates/trenchd/Cargo.toml`

- [ ] **Step 1: Write failing readiness/startup tests**

Test global blockers for NTP, SQLite/reconciliation, storage, stream, metadata, and fresh books; per-market quarantine; and rules-only warmup/config readiness. Test that an open position can still reach mandatory-exit handling. Reopen a database with an interrupted state and assert startup reconstructs ledger/equity/breakers before subscribing. Test the local admin socket rejects non-owner peers, unknown schema/request types, oversized/truncated frames, and every state-changing request before readiness/reconciliation.

- [ ] **Step 2: Verify failure**

Run: `cargo test -p trenchd`

Expected: FAIL because app/readiness/commands are absent.

- [ ] **Step 3: Implement CLI and bounded orchestration**

Use clap subcommands:

```text
trenchd doctor --config PATH
trenchd collect --config PATH [--duration DURATION]
trenchd run --config PATH
trenchd replay --config PATH --manifest PATH
trenchd status --socket PATH [--json]
```

`app` must use `tokio::select!`, bounded channels, cancellation, graceful drain, and exact shutdown checkpoints. The writer owns `SqliteStore`; network tasks never write directly. Startup follows design section 11.3 and records missed decisions rather than recreating them. `doctor` is read-only and exits nonzero with machine-readable reason codes.

`admin` binds only a configured Unix socket under a `0700` runtime directory, sets the socket to `0600`, checks Linux `SO_PEERCRED` for the daemon UID or root, uses a versioned length-prefixed bounded protocol, and routes requests into the authority event loop over a bounded channel. Phase 1 exposes status/readiness/reconciliation only; later plans add shadow, backup, retention, and forward-run commands through the same writer-owned path. No admin TCP port or raw database handle exists.

- [ ] **Step 4: Run daemon tests and offline doctor**

Run: `cargo test -p trenchd && cargo run -p trenchd -- doctor --config config/paper.example.toml`

Expected: tests PASS; doctor reports expected local missing-runtime directories without panicking or contacting an action endpoint.

- [ ] **Step 5: Commit**

```bash
git add crates/trenchd
git commit -m "feat(daemon): run and recover the rules paper bot"
```

### Task 16: Enforce the paper boundary and phase-1 acceptance suite

**Files:**
- Create: `scripts/check-paper-boundary.sh`
- Modify: `AGENTS.md`
- Test: workspace and golden replays

- [ ] **Step 1: Write the boundary check and prove it catches a fixture**

The script must fail if tracked runtime/config/manifests contain `/exchange`, `private_key`, `api_hash`, mnemonic/seed/wallet fields, signer crates, `ethers`, or `@trench/perps-sdk`; allow only documentation references through an explicit path allowlist. It must also assert the Hyperliquid crate exposes only `/info` and `/ws` destinations. Test the script against a temporary violating file, remove that file, then run clean.

- [ ] **Step 2: Run the complete quality gate**

Run:

```bash
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
./scripts/check-paper-boundary.sh
git diff --check
```

Expected: every command exits 0.

- [ ] **Step 3: Run deterministic acceptance replays**

Run both fixtures twice with separate empty data directories and compare manifests, decision rows, fills, breaker transitions, and final ledgers byte-for-byte. Expected: basic replay closes flat; gap replay is unresolved and promotion-ineligible; repeated output digests match.

- [ ] **Step 4: Run an explicit public-data smoke test**

Run: `cargo run -p trenchd -- collect --config config/paper.example.toml --duration 60s`

Expected: public metadata plus normalized events are stored, at least one universe snapshot is explainable, no order action is sent, and SIGINT exits with a reconciliation checkpoint. Mark this step skipped only when the executor has no network access; offline acceptance must still pass.

- [ ] **Step 5: Document verified phase-1 behavior**

Update `AGENTS.md` known issues and exact commands only from observed results. Do not claim 30-day universe eligibility or alpha until enough real data exists.

- [ ] **Step 6: Commit**

```bash
git add scripts/check-paper-boundary.sh AGENTS.md
git commit -m "test: enforce paper-only platform invariants"
```

### Task 17: Select and freeze `rules_only` with nested walk-forward replay

**Files:**
- Create: `crates/trench-core/src/validation.rs`
- Modify: `crates/trench-core/src/config.rs`
- Modify: `crates/trench-core/src/strategy/rules.rs`
- Modify: `crates/trench-core/src/lib.rs`
- Modify: `crates/trenchd/src/commands.rs`
- Modify: `crates/trenchd/src/app.rs`
- Modify: `crates/trench-storage/src/replay.rs`
- Modify: `config/paper.example.toml`
- Test: chronological-fold and end-to-end selection tests

- [ ] **Step 1: Write failing fold and grid tests**

Assert each outer fold uses 305 development days, 60 chronological no-tuning calibration days, and 30 untouched test days, rolling 30 days. Inner folds train/select on days `1-185`, `1-215`, `1-245`, and `1-275` and validate on the next 30 days, with four-hour purge/embargo. Enumerate exactly the 12 declared rule configurations from threshold `{0.55,0.60,0.65}`, ATR floor `{1.25,1.50}`, and take-profit `{1.5R,2R}`; reject undeclared parameters.

- [ ] **Step 2: Write a failing authoritative-selection test**

Replay every candidate/fold through the same universe, sealed risk quote, paper broker, executable-book marking, fees, funding, and ledger engine used at runtime. Assert selection uses median inner-fold net expectancy then lower turnover, never calibration/test outcomes. Require three outer tests and 100 aggregate closed trades before forward eligibility; insufficient trustworthy history returns a hard ineligible report.

- [ ] **Step 3: Verify failure**

Run: `cargo test -p trench-core validation::tests && cargo test -p trenchd commands::rules_research_tests`

Expected: FAIL because rules walk-forward selection is absent.

- [ ] **Step 4: Implement the deterministic research command**

Add:

```text
trenchd research rules --config PATH --manifest PATH --output DIRECTORY
```

The command builds point-in-time folds from the replay manifest, runs declared candidates through `Engine`, writes one immutable prediction/intent/trade/cost stream per candidate/fold, freezes the selected rules config before each outer test, and emits a canonical `rules-validation.json`. That report includes code/config/data/universe/schema digests, fold boundaries, excluded gaps, every tried selection, inner/test outcomes, trade count, and a content-addressed `rules-artifact.json`. The artifact itself contains the selected threshold, ATR floor, take-profit, immutable family/regime definitions, code/feature/data cutoffs, artifact version, and aggregate BLAKE3 digest. It does not calculate a second approximate cost model.

`RulesStrategy::from_artifact` is the only active-mode constructor. At startup `trenchd` canonicalizes the supplied config file itself, resolves the configured artifact and validation-report filenames as regular non-symlink siblings of that physical config target, verifies both configured/content/code/feature/data digests and eligibility, and builds the strategy from the artifact values. This same resolution rule applies to `doctor`: a staged config validates only staged sibling files and never dereferences `/opt/trenchbot/current`. A missing/mismatched/ineligible artifact or report leaves only `rules_only` unready; arbitrary threshold/ATR/TP TOML values are invalid. Update `paper.example.toml` with a truthful `collect_only` example and document the exact active fields without fake digests.

- [ ] **Step 5: Prove no test reuse and stable freezing**

Alter an outer-test outcome and assert the selected config for that fold is unchanged; alter a development outcome and assert the new artifact/report digest captures any legitimate change. Start active mode with each invalid filename, symlink, wrong artifact/report digest, wrong code/feature/data digest, and selected-value mismatch and assert the rules ledger is unready and cannot signal. Validate a staged config while another directory is the active `current` target and prove only staged sibling files are read; start with the exact artifact/report pair and assert the runtime explanation records the same selected values/digests. Run: `cargo test -p trench-core validation::tests && cargo test -p trenchd commands::rules_research_tests && cargo test -p trenchd app::rules_artifact_tests`

Expected: PASS with byte-stable reports for fixed fixtures.

- [ ] **Step 6: Commit**

```bash
git add crates/trench-core crates/trenchd crates/trench-storage config/paper.example.toml
git commit -m "feat(research): validate and freeze rules strategy"
```

## Phase-1 completion gate

Phase 1 is complete only when the workspace gate passes, both golden replays are deterministic, `rules_only` can collect public data and maintain an auditable 100-USDC ledger, failures preserve or force-close exposure exactly as specified, the static boundary check proves no live-action or Telegram surface exists, and the rules research command either produces a valid frozen outer-fold artifact or truthfully reports insufficient history. Statistical robustness, generic replacement shadows, and final forward promotion are completed in phase 2; do not begin that plan against a failing phase-1 baseline.
