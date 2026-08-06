# Satteri agent handoff

**Snapshot:** 2026-08-06T13:50:51Z  
**Repository:** `/home/esoteric/satteri`  
**Branch:** `main`  
**Last pushed commit:** `e1eda0c` (`docs: add HIP-3 venue universe plan`)  
**Remote:** `https://github.com/Codeptor/satteri.git`

## Objective

Deliver a fully working, paper-only long/short perpetual-futures bot on the
GIFGOBLIN VPS. Each visible ledger starts with 100 synthetic USDC and uses
isolated margin with simulated 5x–20x leverage. No real wallet, deposit,
signer, live order, Telegram session, or `/exchange` path is allowed.

This handoff is not a completion claim. The running bot is not active yet.

## Current truth

### Repository

- Rust workspace crates are present and tested: `trench-core`,
  `trench-hyperliquid`, `trench-storage`, and `trenchd`.
- Deterministic engine, paper broker, risk/breakers, SQLite WAL persistence,
  Parquet capture/replay, recovery, and research-evidence scaffolding exist.
- `trenchd` remains fail-closed. `RulesStartup::resolve` returns
  `ReplayAdapterUnavailable` for active rules, while `run`/`collect` operate as
  collection-only and keep fresh entries disabled.
- `research rules` can build causal source evidence, but it does not yet have
  the typed universe/feature/risk replay adapter required to produce an
  executable rules artifact.
- The checked-in configs remain `[rules] mode = "collect_only"` with 20
  tradeable markets, 10 warm-buffer markets, 30-day runtime warmup, and the
  100-USDC isolated-risk policy.
- HIP-3 support is currently documentation-only. The code still rejects
  non-native markets: inspect `Market`, `NotNativePerpetual`,
  `NonNativeMarket`, and the native-only `metaAndAssetCtxs`/WebSocket paths.
- Top-level worktree has a user edit to `AGENTS.md`. Preserve it; do not reset,
  overwrite, or commit it unless explicitly requested.
- `web/satta` is a separate nested Git repository at commit `80da9b4`. It is a
  read-only dashboard shell, not a finished dashboard.

### Last local verification

Passed:

```sh
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
bash scripts/check-paper-boundary.sh
```

The boundary check currently reports `paper-boundary: clean`. Dashboard
`npm run lint` and `npm run typecheck` also pass from `web/satta`; run
`npm run build` before dashboard handoff.

### GIFGOBLIN observation

Read-only SSH observation was performed through the `gifgoblin` alias only;
**do not touch `stratboard`**.

- Filesystem: 193G total, approximately 171G free at observation time.
- `/home/esoteric/trenchbot/config/paper.toml` is collection-only.
- Releases include `04865c5`; no resolved `/home/esoteric/trenchbot/current`
  target was observed.
- No `trenchd` process or tmux session was running at observation time.
- `/home/esoteric/trenchbot-data/run/admin.sock` exists but is stale/unserved.
- Data observed: approximately 904M SQLite, 487M Parquet, plus archived capture
  directories. Do not delete or archive anything without an explicit, scoped
  operation and a verified backup policy.

The remote checks did not mutate the host.

## Non-negotiable boundaries

- Only public Hyperliquid `https://api.hyperliquid.xyz/info` and
  `wss://api.hyperliquid.xyz/ws` read paths are allowed in the paper adapter.
- No wallets, private keys, signers, account secrets, deposits, faucets,
  Telegram, human-call ingestion, live exchange actions, or SDK submission
  code in this project.
- `trenchd` is the sole SQLite writer. Core logic receives explicit UTC event
  and decision times; it must not read wall-clock time implicitly.
- Isolated margin only. `rules_only` and `ml_champion` ledgers stay completely
  independent.
- Never edit `/etc/trenchbot/paper.toml` in place. Releases must be immutable,
  content-addressed, and activated atomically.
- Never run commands against `stratboard`; the only VPS in scope is GIFGOBLIN.

## Required implementation sequence

### 1. Implement HIP-3 as a real venue family

The approved design now includes native and HIP-3 markets, but implementation
is pending. Follow Tasks 5, 8, and 8A in
`docs/superpowers/plans/2026-08-03-trench-rules-platform.md`.

Required work:

- Replace symbol-only identity with a collision-safe `(perp_dex, coin)` market
  identity. Native uses the default DEX; HIP-3 examples are `xyz:SNDK`.
- Add typed read-only requests for `perpDexs`, `allPerpMetas`,
  `perpDexStatus`, `perpDexLimits`, and DEX-qualified metadata/context/book/
  funding calls.
- Preserve DEX identity in events, candles, books, features, universe
  snapshots, risk policies, SQLite rows, Parquet partitions, replay manifests,
  and artifact digests.
- Add status/oracle heartbeat and divergence, isolated-only margin, exact fee
  scale/deployer fee, asset/DEX OI capacity, session availability, and
  halt/settlement witnesses.
- A stale oracle, DEX halt, settlement, collateral change, unknown fee, or
  exhausted OI cap must block fresh entries for the affected market/DEX only.
- A HIP-3 halt/settlement must create a durable venue-settlement/mandatory-exit
  path; never invent a favorable book fill.
- Update native-only tests and add `meta/hip3-perps.json` fixtures. Do not add
  a compatibility fallback that silently treats HIP-3 as native.

### 2. Finish source and witness production

- Accumulate/import a verified source window with exact availability and
  continuity evidence. Official Hyperliquid archive data is requester-pays,
  may be incomplete/delayed, and does not provide historical candles; do not
  synthesize missing books or candles.
- Runtime `required_history_days = 30` is only warmup. The rules validation
  protocol requires 455 complete evaluation days plus the declared warmup;
  insufficient history is a hard ineligibility, not permission to shorten
  folds.
- Generate verified recovery, universe, feature, and risk witnesses. Keep
  native and each HIP-3 DEX/asset separate in all joins and reports.
- Produce `rules-validation.json` and a content-addressed `rules-artifact.json`
  only from the authoritative replay path. Do not hand-write an artifact or
  flip `mode` to active to bypass gates.

### 3. Activate only after evidence is valid

- Update the release config from `collect_only` to active only when the exact
  artifact/report pair, code/config/feature/data digests, typed witnesses, and
  runtime reactor are implemented and verified together.
- Restart only the exact GIFGOBLIN Trench unit/process. Keep Telegram, ML
  worker, dashboard, and all unrelated host workloads out of this activation.
- Verify status/readiness, simulated fills, partial fills, exits, funding,
  leverage counterfactuals, liquidation distance, daily/weekly/drawdown
  breakers, restart/reconciliation, and ledger independence.

### 4. Dashboard later

`web/satta` is intentionally separate and read-only. It should consume the
private daemon status/API contract server-side and display both ledgers,
universe/exclusion reasons, readiness blockers, risk decisions, fills/exits,
equity, and live candles. It must never connect browser-side to the Unix
socket or gain order/wallet/Telegram capabilities.

## Data/storage facts

- Current collector format is intentionally verbose: the observed 18-market
  raw rate projected roughly 640–700 GiB for 30 days. GIFGOBLIN's 193G disk is
  not a long-term raw archive.
- Recommended topology is immutable B2 archive plus local hot working storage;
  never put SQLite on object storage. Benchmark compression/replay before
  provisioning a separate hot-storage VPS.
- Keep source manifests, provenance, availability runs, excluded gaps, and
  content digests with every promoted run.

## Useful commands

```sh
# Rust gates, in this order
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
bash scripts/check-paper-boundary.sh

# inspect current paper config without activating anything
sed -n '1,140p' deploy/config/gifgoblin-user.toml

# local daemon commands
cargo run -p trenchd -- doctor --config config/paper.example.toml --json
cargo run -p trenchd -- collect --config config/paper.example.toml --duration 60s
cargo run -p trenchd -- research rules --config ABS_CONFIG --manifest ABS_MANIFEST --output ABS_OUTPUT

# dashboard gates (nested repository; npm, not pnpm/bun)
cd web/satta
npm run lint
npm run typecheck
npm run build
```

## Definition of done

Do not report completion until a fresh verification proves: public native +
eligible HIP-3 ingestion; typed point-in-time universe/feature/risk witnesses;
validated rules artifact/report; active paper mode with no live-action path;
actual simulated fills and exits; isolated 5x–20x risk behavior; funding,
slippage, liquidation, and breaker handling; deterministic restart/replay;
independent 100-USDC ledgers; and a read-only status surface. If any evidence
is missing, leave the goal active and state the exact blocker.

## Primary references

- `AGENTS.md` — current project instructions and invariants
- `docs/superpowers/specs/2026-08-03-trench-paper-trading-bot-design.md`
- `docs/superpowers/plans/2026-08-03-trench-rules-platform.md`
- `docs/data-source-map.md`
- `docs/runbooks/trench-paper-operations.md`
- `docs/dashboard/private-readonly-contract.md`
- `deploy/README.md`
