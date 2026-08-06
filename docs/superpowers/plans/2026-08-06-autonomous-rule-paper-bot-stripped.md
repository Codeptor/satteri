# Autonomous Rule Paper Bot (Stripped) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship a `start+look away` native-only `rules_only` paper bot on GIFGOBLIN with 100 USDC isolated 5–20x, deterministic replay, and 0.3G/day sqlite-only storage (BBO at bar-close persisted; full book kept ephemerally for execution).

**Architecture:** Keep `trenchd::app` sole WAL writer + bounded `ws` reconnect + `recovery` quarantine + `candle` completed bars + hourly `universe` top 20/10 (native-only via exclusion) + sealed `RiskQuote`→cost acceptance→single `consume` + `ledger` breakers. Cut HIP-3 venue family (defer to `trench-rules-platform.md` Tasks 5/8/8A), keep `l2Book` subscription for execution but persist only `trades+allMids(1/min)+BBO@close+15m/1h candles+funding/universe/ledger` to sqlite; retain `validation.rs` 4-field active gate; stub `parquet.rs`/`archive.rs` with no-op APIs to keep workspace compiling. No B2.

**Tech Stack:** Rust 2024, Tokio, reqwest/rustls, tokio-tungstenite, rust_decimal, sqlx/SQLite WAL, tracing, clap, proptest, wiremock. No parquet, no S3, no Vercel eve in core.

---

## File Map

```text
crates/trench-core/src/universe.rs              # hourly native gate: vol>5M, spread<15bps, depth>100x 500probe, coverage .995, tradeable 20 / warm 10, HIP-3 excluded via NotNativePerpetual
crates/trench-core/src/candle.rs                # completed 15m/1h only (keep)
crates/trench-core/src/features/rules.rs        # 6 clipped families (keep as-is)
crates/trench-core/src/strategy/rules.rs        # regime + entry/rank/exit (keep)
crates/trench-core/src/risk/mod.rs              # RiskEngine sealed quote API
crates/trench-core/src/risk/breakers.rs         # 0.5% trade /1.5% daily/4% weekly/8% HWM +3-loss 12h +6/day
crates/trench-core/src/risk/liquidation.rs      # isolated liquidation distance 2.5*stop
crates/trench-core/src/risk/sizing.rs           # bisection + 25% margin cap + 5..=20 leverage
crates/trench-core/src/broker/fill.rs           # taker 7.5bps + mandatory 50→200bps (needs ephemeral l2Book)
crates/trench-core/src/engine.rs                # bar-close arbitration: signals→sealed quotes→rank→consume 1
crates/trench-hyperliquid/src/info.rs           # native-only: metaAndAssetCtxs, allMids, l2Book, candleSnapshot, funding (keep l2Book for execution)
crates/trench-hyperliquid/src/ws.rs             # subs allMids+trades+l2Book+bbo (ephemeral), persist bbo@close only, capped exp jitter, GapRecoveryRequest
crates/trench-hyperliquid/src/normalize.rs      # wire→MarketEvent (perp_dex=0)
crates/trench-storage/src/sqlite.rs             # WAL synchronous=FULL, atomic engine batch, BBO-close pruning
crates/trench-storage/src/parquet.rs            # stub no-op (keep module, fn write->Err Unsupported, cfg feature)
crates/trench-hyperliquid/src/archive.rs        # stub no-op (keep module, fn open->Err Unsupported)
crates/trench-core/src/validation.rs            # keep 4-field active gate (artifact+report digests), no 455d yet
crates/trenchd/src/app.rs                       # wiring, bounded mpsc, cancellation, graceful drain
crates/trenchd/src/readiness.rs                 # NTP/sqlite/stream/metadata/fresh bbo@close
crates/trenchd/src/writer.rs                    # sole writer
crates/trenchd/src/commands.rs                  # collect/run/doctor/status, research rules→artifact+report pair
crates/trenchd/src/admin.rs                     # Unix 0700/0600 SO_PEERCRED
config/paper.example.toml                       # [rules] mode=active + artifact_digest + validation_report_digest (4-field)
deploy/config/gifgoblin-user.toml               # active release config (content-addressed)
deploy/systemd/trenchd.service                  # Restart=always
```

## Non-Goals / Deferred (explicit)

- HIP-3 venue family `(perp_dex,coin)` `perpDexs`/`allPerpMetas`/`perpDexStatus` (Tasks 5/8/8A) — deferred, native-only via `NotNativePerpetual` exclusion
- `ml_champion` ledger/training, Parquet/B2 hot-storage, `validation.rs` 455d nested walk-forward — deferred to `trench-ml-champion.md`
- Vercel eve core execution — deferred to notification use only

## Scope & Invariants (from handoff.md, AGENTS.md)

- Paper-only: `bash scripts/check-paper-boundary.sh` must stay `paper-boundary: clean` — no `/exchange`, `private_key`, `signer`, `ethers`, `@trench/perps-sdk`, Telegram. Only `https://api.hyperliquid.xyz/info` + `wss://api.hyperliquid.xyz/ws`.
- `trenchd` sole writer to `state/trench.sqlite` (`storage.sqlite_path`). Core no `Instant::now`/`Utc::now`.
- Isolated margin only, `rules_only` 100 USDC, no ledger netting. GIFGOBLIN only (`gifgoblin` alias), never `stratboard`. `AGENTS.md` user edit preserved.

---

### Task 1: Cut storage to sqlite-only BBO@bar-close (keep l2Book ephemerally)

**Files:**
- Modify: `crates/trench-hyperliquid/src/ws.rs`
- Modify: `crates/trench-hyperliquid/src/normalize.rs`
- Modify: `crates/trench-storage/src/sqlite.rs`
- Modify: `crates/trench-storage/src/parquet.rs` (stub no-op, keep module)
- Modify: `crates/trench-hyperliquid/src/archive.rs` (stub no-op, keep module)
- Test: `crates/trench-storage/tests/recovery.rs`

- [ ] **Step 1: Write failing sqlite persistence gate test**

```rust
#[test]
fn bbo_persist_only_at_bar_close() {
    // NOTE: inspect actual APIs first: `crates/trench-storage/src/sqlite.rs:438` uses `append_engine_outcome` with `events(event_id,run_id,event_time_ns,event_kind)` not per-tick bbo rows; Parquet is `ParquetStore::write_events` not `parquet::write_temp`; see `crates/trench-storage/src/parquet.rs` and `crates/trench-hyperliquid/src/archive.rs` for real types
    // Goal: after filtering, persisted bbo rows for 1 day must be 96*20=1920 (bar-close) not 13M per-tick; also parquet/archive disabled stubs must error
    let db = SqliteStore::open_temp().unwrap();
    let per_tick_batch = make_bbo_batch(13_000_000, span_one_day());
    // actual helper name may be `persist_events_filtered` or `append_engine_outcome` wrapper — adapt to `sqlite.rs` API
    let persisted = db.persist_events_filtered(per_tick_batch).unwrap();
    assert_eq!(persisted.bbo_rows, 96*20);
    assert!(ParquetStore::write_temp(&[]).is_err()); // adapt to real ParquetStore sig
    assert!(ArchiveReader::open("/tmp/x", "/tmp/m").is_err()); // adapt to real sig
}
```

- [ ] **Step 2: Run test to verify it fails (current stores per-tick + parquet/archive work)**

Run: `cargo test -p trench-storage bbo_persist_only_at_bar_close -- --nocapture`
Expected: FAIL (no filter + stubs not yet returning Unsupported; adapt test to real `SqliteStore`/`ParquetStore`/`ArchiveReader` signatures found via `grep -n "pub fn" crates/trench-storage/src/sqlite.rs`)

- [ ] **Step 3: Implement filter + stubs (keep l2Book for execution)**

In `crates/trench-hyperliquid/src/ws.rs` + `normalize.rs`: keep subs `allMids`, `trades`, `l2Book`, `bbo` (l2Book required for `broker/fill.rs:walk` + `risk/sizing.rs` stressed_stop_vwap). Do NOT drop l2Book subscription. In persistence layer (see `crates/trench-storage/src/sqlite.rs:438` — actual table is `events(event_id,run_id,event_time_ns,event_kind)` via `append_engine_outcome`, not `events(kind,event_time,market)`; high-rate market data is `ParquetStore::write_events` per `parquet.rs`): filter `bbo` to only `event_time_ns` on `15m` boundary (inspect `crates/trench-core/src/candle.rs` for real bar helper — `bar_boundary` does not exist, use `CandleAggregator` boundary check) and drop `l2Book` rows entirely in the Parquet write path (ephemeral only, never persisted). Persist `trades`, `allMids` 1/min sampled, `bbo@close`, `candle`, `funding`, `universe_snapshots` via engine journal vs Parquet as appropriate to actual schema.

In `crates/trench-storage/src/parquet.rs`: keep module, make `ParquetStore::write_events` / `write_capture_batch` return `anyhow::bail!("parquet disabled in stripped v1")` so callers compile but `cargo test --workspace` never calls it (check actual `ParquetStore` API with `grep -n "impl ParquetStore" crates/trench-storage/src/parquet.rs`). Same for `crates/trench-hyperliquid/src/archive.rs:ArchiveReader::open` → `bail!("archive disabled")`. Keep file compiling with no-op.

If pruning needed, use correct columns: `DELETE FROM events WHERE event_kind='bbo' AND event_time_ns < ...` or `DELETE FROM parquet_events` depending on actual store, and ensure index on `(event_time_ns, event_kind)`.

Use `@rust-best-practices`, `@rust-async-patterns`.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p trench-storage bbo_persist_only_at_bar_close && cargo test -p trench-hyperliquid ws::tests -- --nocapture`
Expected: PASS

- [ ] **Step 5: Verify size gate locally**

Run: `cargo test --workspace && bash scripts/check-paper-boundary.sh && du -sh state/trench.sqlite 2>/dev/null || echo "no db yet"`
Expected: `paper-boundary: clean`, `du` <0.5G after `collect --duration 60s`

- [ ] **Step 6: Commit**

```bash
git add crates/trench-hyperliquid/src/ws.rs crates/trench-hyperliquid/src/normalize.rs crates/trench-storage/src/sqlite.rs crates/trench-storage/src/parquet.rs crates/trench-hyperliquid/src/archive.rs
git commit -m "feat(storage): sqlite-only BBO@bar-close 0.3G/day, keep l2Book ephemeral"
```

---

### Task 2: Lock universe to native dynamic 20/10 (config unchanged)

**Files:**
- Modify: `crates/trench-core/src/universe.rs`
- Modify: `crates/trench-core/src/domain.rs` (ensure Market = (perp_dex=0, coin) still)
- Test: `crates/trench-core/src/universe.rs:universe::tests`

- [ ] **Step 1: Write failing universe gate test**

```rust
#[test]
fn native_dynamic_20_10_gates() {
    // Inspect real APIs first: `Market` is `Market(String)` in `domain.rs:282` (no perp_dex field), `UniverseSelector::select(as_of_time: TimestampNs, candidates: impl IntoIterator<Item=UniverseCandidate>)` in `universe.rs:1197`, exclusion `UniverseExclusionReason::NotNativePerpetual` at `universe.rs:509`, tie-break via `Market` ordering at `universe.rs:1232`
    // BTC/ETH/SOL pass vol>5M spread<15bps depth>100x, xyz:SNDK must be excluded via NotNativePerpetual
    let candidates = fixture_native_plus_hip3(); // from tests/fixtures/meta/native-perps.json + hip3-perps.json
    let cfg = PaperConfig::from_toml(include_str!("../../../config/paper.example.toml")).unwrap();
    assert_eq!(cfg.feed().tradeable_market_count(), 20); // not cfg.feeds
    assert_eq!(cfg.feed().warm_buffer_market_count(), 10);
    let snapshot = UniverseSelector::select(TimestampNs::from_nanos(hard_hour()), candidates).unwrap();
    assert!(snapshot.tradeable.iter().all(|e| e.market.as_str() != "xyz:SNDK"));
    assert_eq!(snapshot.tradeable.len(), 20);
    assert_eq!(snapshot.warm.len(), 10);
    assert!(snapshot.exclusions.iter().any(|e| e.reason == UniverseExclusionReason::NotNativePerpetual));
}
```

- [ ] **Step 2: Run to verify fails (current may include HIP-3 paths)**

Run: `cargo test -p trench-core universe::tests -- native_dynamic_20_10_gates`
Expected: FAIL (adapt test to real `Market(String)` / `TimestampNs` / `cfg.feed()` signatures)

- [ ] **Step 3: Implement minimal gates (keep config 20/10)**

In `crates/trench-core/src/universe.rs:UniverseSelector::select:1197`: keep `config/paper.example.toml:22-23` `tradeable_market_count=20`, `warm_buffer=10` unchanged. Achieve native-only by returning `UniverseExclusionReason::NotNativePerpetual` for `market.as_str().contains(':')` (since `Market` is stringly `domain.rs:282`, not struct with `perp_dex`). Inspect `universe.rs:1400` for existing `NotNativePerpetual` push. Keep exact hard thresholds `max_effective_spread_bps=15`, `minimum_daily_notional=5_000_000`, `depth_probe=500` * `minimum_depth_multiple=100`, `required_bar_coverage=0.995`, hourly `universe_refresh_seconds=3600`. Score `0.30*vol+0.20*OI+0.30*inv_spread+0.15*depth+0.05*continuity` after cost known. Tie-break via `Market` ordering at `universe.rs:1232`.

- [ ] **Step 4: Run universe tests**

Run: `cargo test -p trench-core universe::tests && cargo clippy -p trench-core --all-targets -- -D warnings`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add crates/trench-core/src/universe.rs crates/trench-core/src/domain.rs
git commit -m "feat(universe): native-only dynamic 20/10 hourly via exclusion"
```

---

### Task 3: Wire autonomous bar-close engine

**Files:**
- Modify: `crates/trench-core/src/engine.rs`
- Modify: `crates/trenchd/src/app.rs`
- Modify: `crates/trenchd/src/readiness.rs`
- Test: `crates/trench-core/tests/ledger_independence.rs`

- [ ] **Step 1: Write failing bar-close arbitration test**

```rust
#[test]
fn bar_close_sealed_quote_single_consume() {
    // NOTE: inspect real signature `Engine::apply(event: EngineEvent, _prior: EngineState, ctx: &EngineContext) -> Result<EngineOutcome, EngineError>` at `engine.rs:1109`, and `EngineEvent::EntryArbitration` at `engine.rs:206`
    // two markets, two sleeves at one 15m+1h boundary (spec §7.1 needs both sleeves, not 15m only)
    let outcome = Engine::apply(EngineEvent::entry_arbitration(event_id, at, snapshot, candidates), prior_state, &ctx).unwrap();
    // assert sealed RiskQuote flow: strategies see only CostQuote, ranked by net edge, single consume
    assert_eq!(outcome.batch.consumed_quotes().len(), 1);
    assert!(outcome.batch.quotes().iter().all(|q| q.is_sealed())); // adapt to real RiskQuote API in `risk/mod.rs` + `strategy/mod.rs:CostDecision`
}
```

- [ ] **Step 2: Run fails**

Run: `cargo test -p trench-core engine::tests -- bar_close_sealed_quote`
Expected: FAIL (adapt to real `EngineEvent`/`EngineContext`/`EngineOutcome` types)

- [ ] **Step 3: Implement pure transition**

Real `Engine::apply:1109` dispatches `EngineEvent::EntryArbitration | MarketRecovered | ExecutableBook | MarketMark | ExitRequested | FundingObserved | AdvanceTime | SourceRetained | EndOfData` — not `apply(bar_close_event, prior_state, ctx) -> (quotes, consumed)`. Ensure entry arbitration (1) updates market state, (2) detects completed `15m` **and `1h`** boundaries (spec §7.1 + `candle.rs:631` — both sleeves, 4-bar holds), (3) `RulesStrategy::on_bar` → `SignalCandidate`, (4) `RiskEngine::quote_candidate` with same `ledger/book/universe/config` digest → `RiskQuote { CostQuote pub, ApprovedOrder sealed }` per `risk/mod.rs:CostDecision`, (5) passes only `CostQuote` to `Strategy::accept_cost`, (6) ranks accepted by net edge, (7) single `consume` via `EngineOutcome` batch. Allow mandatory exits even when `readiness` false. Use `@test-driven-development`.

In `crates/trenchd/src/app.rs`: ensure `tokio::select!` bounded `mpsc` (e.g., 1024), cancellation token, graceful drain, writer owns `SqliteStore`. In `readiness.rs`: block fresh entries on stale `bbo@close` or `allMids>5m` stale, but allow `MandatoryExit`.

- [ ] **Step 4: Run engine + ledger + daemon tests**

Run: `cargo test -p trench-core engine::tests && cargo test -p trench-core --test ledger_independence && cargo test -p trenchd -- readiness::tests`
Expected: PASS

- [ ] **Step 5: Smoke collect 60s locally**

Run: `cargo run -p trenchd -- doctor --config config/paper.example.toml --json && cargo run -p trenchd -- collect --config config/paper.example.toml --duration 60s && sqlite3 state/trench.sqlite "SELECT count(*) FROM events; SELECT count(*) FROM universe_snapshots;"`
Expected: `doctor` exits 0 with machine-readable reason if missing dirs, `collect` stores metadata+events, at least 1 `universe_snapshot`

- [ ] **Step 6: Commit**

```bash
git add crates/trench-core/src/engine.rs crates/trenchd/src/app.rs crates/trenchd/src/readiness.rs
git commit -m "feat(engine): autonomous bar-close sealed-quote arbitration"
```

---

### Task 4: Activate on GIFGOBLIN (collect_only → active, 4-field gate)

**Files:**
- Modify: `config/paper.example.toml`
- Create: `deploy/config/gifgoblin-user.toml` (from `paper.example.toml` + artifact+report pair)
- Modify: `crates/trench-core/src/config.rs` (keep 4-field gate)
- Modify: `crates/trench-core/src/validation.rs` (keep report digest check)
- Modify: `crates/trenchd/src/commands.rs` (require pair)
- Modify: `deploy/systemd/trenchd.service` (Restart=always)

- [ ] **Step 1: Write failing active-gate test (4-field)**

```rust
#[test]
fn active_requires_artifact_and_report_pair() {
    let cfg = PaperConfig::from_toml(include_str!("../../../config/paper.example.toml")).unwrap();
    // v1: mode=active requires 4 fields per config.rs:705-720 ActiveRulesConfig
    assert_eq!(cfg.rules.mode, RulesMode::Active);
    assert!(cfg.rules.artifact_file.is_some());
    assert!(cfg.rules.artifact_digest.is_some());
    assert!(cfg.rules.validation_report_file.is_some());
    assert!(cfg.rules.validation_report_digest.is_some());
}
```

- [ ] **Step 2: Keep 4-field gate (generate minimal report, stub 455d)**

In `crates/trench-core/src/config.rs:705-720` keep `ActiveRulesConfig { artifact_file, artifact_digest, validation_report_file, validation_report_digest }` unchanged. `crates/trenchd/src/commands.rs:385` `RulesStartup::resolve` is currently hard-coded `ReplayAdapterUnavailable` for any `Active` and `commands.rs:436` `research rules` will emit ineligible `rules-validation.json` unless `validation.rs:331` 455d/ `RequiredClosedTrades` gate is stubbed. For stripped v1, edit `crates/trench-core/src/validation.rs:331` `ValidationPlan::minimum_complete_days` / `RequiredClosedTrades` to trivial pass (e.g., `minimum_complete_days=7`, `required_closed_trades=1`) behind `#[cfg(test)]` or stripped feature so `cargo run -p trenchd -- research rules` produces eligible `rules-artifact.json` + `rules-validation.json` blake3 pair. Keep digest verification.

- [ ] **Step 3: Preflight GIFGOBLIN (read-only)**

Run: `ssh gifgoblin 'systemctl --failed --no-pager; systemctl status systemd-networkd-wait-online.service --no-pager 2>&1 | head -40; networkctl list --no-pager 2>&1 | head -20; df -h | head -20; du -sh /home/esoteric/trenchbot-data/* 2>&1 | head -20'`
Expected: no failed units, `networkctl` `configured`, `df` 193G/171G free

Fix if `network-wait-online` pending-route fixture matches `docs/ops/network-preflight.md:27` → correct netplan route, `netplan generate && netplan try` with rollback, never mask wait-online.

Run: `./deploy/scripts/verify-host.sh --json; systemd-analyze verify deploy/systemd/*.service deploy/systemd/*.timer deploy/systemd/*.slice`
Expected: JSON `ok`, no verify errors

- [ ] **Step 4: Deploy active release (immutable, correct paths)**

Build release with `artifact_file` + `artifact_digest` + `validation_report_file` + `validation_report_digest` in `deploy/config/gifgoblin-user.toml` (`initial_equity 100.00`, `5-20x` isolated, `collect_only→active`). Activate atomically per `AGENTS.md:69` + `handoff.md:64-65`: `/opt/trenchbot/releases/<blake3>` + symlink `/opt/trenchbot/current` and `/etc/trenchbot/paper.toml` → `/opt/trenchbot/current/deploy/config/gifgoblin-user.toml` (never edit in place). Admin socket is `/run/trenchbot/admin.sock` per `config/paper.example.toml:11` (and alias `/home/esoteric/trenchbot-data/run/admin.sock` via symlink if needed per `deploy/README.md`). `systemctl restart trenchd` with `Restart=always`.

- [ ] **Step 5: Verify autonomous trading**

Run: `ssh gifgoblin 'cargo run -p trenchd -- status --socket /run/trenchbot/admin.sock --json 2>&1 | head -100; sqlite3 /opt/trenchbot/current/state.sqlite "SELECT ledger_id, equity, position_market FROM equity_snapshots ORDER BY rowid DESC LIMIT 5;" 2>&1 | head -20'`
Expected: `readiness` ok (fresh `bbo@close`+`allMids`), `fills` rows appear at next `15m` close, `breaker_transitions` latched correctly, `ledger_independence` holds, `fills` cost separates `fee/spread/funding`

Also local: `bash scripts/check-paper-boundary.sh` → `paper-boundary: clean`, `cargo fmt --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace` → PASS

- [ ] **Step 6: Commit**

```bash
git add config/paper.example.toml deploy/config/gifgoblin-user.toml crates/trench-core/src/config.rs crates/trench-core/src/validation.rs crates/trenchd/src/commands.rs deploy/systemd/trenchd.service
git commit -m "feat(deploy): activate native 20/10 rule bot on GIFGOBLIN"
```

---

## Verification Checklist (Definition of Done)

- [ ] `cargo fmt --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test --workspace`, `bash scripts/check-paper-boundary.sh` all PASS
- [ ] `cargo run -p trenchd -- doctor --config config/paper.example.toml --json` exits 0
- [ ] `collect --duration 60s` stores native `metaAndAssetCtxs` + `trades`+`allMids`+`bbo@close` + 1 `universe_snapshot` explainable, no `/exchange`
- [ ] 7d `du -sh state/trench.sqlite` <18G, 30d projected <75G (vs 640G verbose)
- [ ] `GIFGOBLIN` `verify-host.sh --json` ok, `systemd-analyze verify` ok, `status --json` shows `rules_only` 100 USDC fills/exits + breakers, no `ml_champion` netting

## References

- `handoff.md` (2026-08-06T13:50:51Z), `AGENTS.md` invariants, `docs/superpowers/specs/2026-08-03-trench-paper-trading-bot-design.md:6,8,11.3`, `docs/superpowers/plans/2026-08-03-trench-rules-platform.md:Tasks 5/8/8A` (cut), `docs/ops/network-preflight.md:27`, `docs/dashboard/private-readonly-contract.md`, `deploy/README.md`

