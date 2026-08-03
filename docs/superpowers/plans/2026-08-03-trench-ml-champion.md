# Trench ML Champion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a separately validated LightGBM `ml_champion` ledger, reproducible temporal research pipeline, frozen inference worker, and forward-shadow promotion system without weakening the Rust risk authority.

**Architecture:** Rust remains authoritative for features, deadlines, risk, paper fills, ledgers, and replay; Python trains and serves immutable models over a versioned MessagePack Unix socket. Point-in-time Parquet datasets feed chronological walk-forward jobs, while content-addressed artifacts and SQLite registrations make every prediction and promotion auditable. ML failure degrades only the ML ledger.

**Tech Stack:** Rust 2024, Tokio Unix sockets, MessagePack; Python 3.12 managed by uv, NumPy, pandas, PyArrow, LightGBM, scikit-learn, SciPy, SHAP, arch bootstrap, msgspec, pytest, Ruff, Pyright.

---

## Scope and prerequisites

Execute this only after [the rules-platform plan](2026-08-03-trench-rules-platform.md) passes its completion gate. This phase implements the approved [ML design](../specs/2026-08-03-trench-paper-trading-bot-design.md) and leaves `rules_only` behavior unchanged. It creates no live executor, wallet, Telegram integration, TCP inference port, online learning, automatic promotion, or production use of research-only foundation-model artifacts.

Use `@test-driven-development`, `@quantitative-research`, `@backtesting-frameworks`, `@statistical-analysis`, `@risk-metrics-calculation`, and `@api-security-best-practices` during execution. Use `uv` for every Python environment/package/test command.

## Target file map

```text
schemas/inference-v1.json                       canonical IPC field contract
tests/fixtures/ipc/request-v1.msgpack          Rust/Python compatibility fixture
tests/fixtures/ipc/response-v1.msgpack         Rust/Python compatibility fixture
tests/fixtures/ml/tiny-features.parquet         deterministic point-in-time dataset
tests/fixtures/ml/tiny-trades.parquet           paper-broker outcomes for gates
config/ml.example.toml                          non-secret offline/runtime ML settings
Cargo.toml                                     adds the shared MessagePack dependency
Cargo.lock                                     pins the expanded Rust graph
crates/trench-core/src/features/ml.rs           declared feature vector and schema hash
crates/trench-core/src/strategy/ml.rs           forecast-to-intent/exit policy
crates/trench-core/src/shadow.rs                evaluation-only ledger copies
crates/trench-core/tests/ml_ledger.rs            strategy independence/replay tests
crates/trench-storage/migrations/0002_ml.sql     artifacts, forecasts, shadows, reports
crates/trench-storage/src/ml.rs                 atomic ML registrations and journals
crates/trenchd/src/ml_client.rs                 deadline-bound Unix-socket client
crates/trenchd/src/app.rs                       ML readiness/inference wiring
ml/
  pyproject.toml                                package, dependencies, tool policy
  uv.lock                                       fully resolved environment
  src/trench_ml/__init__.py
  src/trench_ml/config.py                       strict typed config
  src/trench_ml/schema.py                       msgspec IPC structs and validation
  src/trench_ml/data/features.py                point-in-time feature reads/validation
  src/trench_ml/data/labels.py                  four-bar gross-return/cost labels
  src/trench_ml/data/folds.py                   purged/embargoed chronological folds
  src/trench_ml/models/lightgbm.py              bounded search and dual heads
  src/trench_ml/models/calibration.py           temperature and split-conformal fit
  src/trench_ml/models/artifact.py              safe serialization/fingerprints
  src/trench_ml/evaluation/metrics.py           cost/calibration/trading metrics
  src/trench_ml/evaluation/robustness.py        bootstrap/PBO/DSR/stress suite
  src/trench_ml/evaluation/promotion.py         exact absolute/paired gates
  src/trench_ml/licenses.py                     digest-bound challenger policy
  src/trench_ml/worker.py                       Unix-socket champion/shadow inference
  src/trench_ml/cli.py                          dataset/train/evaluate/serve/promote CLI
  tests/                                        mirrors every module above
reports/schemas/promotion-v1.json               machine-readable report contract
models/.gitkeep                                 runtime artifacts remain ignored
```

## Cross-language invariants

- `schema_version=1`; unknown versions or fields fail closed.
- Feature order is declared once in Rust, exported with a BLAKE3 schema digest, and checked by Python. Names are never silently reordered.
- IPC envelopes carry `event_id`, `event_time`, `as_of_time`, `producer_version`, `run_id`, `config_hash`, payload type, and payload.
- Python never receives quantity, wallet, margin authority, a database write handle, or exchange client.
- Artifacts use LightGBM text models plus canonical JSON/NumPy files with `allow_pickle=False`; Python pickle/joblib model loading is forbidden.
- Runtime predictions are recorded before outcomes and replay consumes recorded forecasts instead of invoking a changing model.

### Task 1: Scaffold the uv project and canonical IPC contract

**Files:**
- Create: `ml/pyproject.toml`
- Create: `ml/src/trench_ml/__init__.py`
- Create: `ml/src/trench_ml/config.py`
- Create: `ml/src/trench_ml/schema.py`
- Create: `ml/tests/test_schema.py`
- Create: `schemas/inference-v1.json`
- Create: `config/ml.example.toml`
- Modify: `.gitignore`

- [ ] **Step 1: Write failing strict-schema tests**

Test valid request/response round trips and rejection of unknown schema version, unknown field, non-finite feature, stale `as_of_time`, duplicate market/sleeve rows, probability sum outside tolerance, missing feature, digest mismatch, and response ID mismatch. Use this payload shape:

```python
request = InferenceRequest(
    schema_version=1,
    event_id="evt-1",
    event_time_ns=1_786_000_000_000_000_000,
    as_of_time_ns=1_786_000_000_000_000_000,
    producer_version="test",
    run_id="run-1",
    config_hash="b3:test",
    feature_schema_hash="b3:features-v1",
    rows=(FeatureRow(market="SOL", sleeve="15m", values=(0.1, 0.2)),),
)
```

- [ ] **Step 2: Verify failure**

Run: `cd ml && uv run pytest tests/test_schema.py -q`

Expected: FAIL because the package/schema does not exist.

- [ ] **Step 3: Create the locked Python project**

Require Python `>=3.12,<3.13`. Runtime dependencies: `arch`, `blake3`, `lightgbm`, `msgspec`, `numpy`, `pandas`, `pyarrow`, `scikit-learn`, `scipy`, and `shap`. Development dependencies: `pytest`, `pytest-asyncio`, `ruff`, and `pyright`. Configure Ruff for Python 3.12 with formatting/import/type-safety rules and Pyright strict mode for `src/trench_ml`. Resolve and commit `uv.lock` with `uv lock`.

`config/ml.example.toml` contains paths, Unix-socket timeout, seeds, fixed fold dates/lengths, declared LightGBM grid, and artifact limits only. Use `msgspec.Struct(forbid_unknown_fields=True, frozen=True)` for IPC types and explicit validation functions for semantic constraints.

- [ ] **Step 4: Run schema quality checks**

Run: `cd ml && uv run pytest tests/test_schema.py -q && uv run ruff check . && uv run ruff format --check . && uv run pyright`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add ml/pyproject.toml ml/uv.lock ml/src ml/tests schemas config/ml.example.toml .gitignore
git commit -m "build(ml): scaffold locked inference project"
```

### Task 2: Export the declared point-in-time ML feature matrix

**Files:**
- Create: `crates/trench-core/src/features/ml.rs`
- Modify: `crates/trench-core/src/features/mod.rs`
- Modify: `crates/trench-storage/src/parquet.rs`
- Create: `tests/fixtures/ml/tiny-features.parquet`
- Test: inline Rust tests

- [ ] **Step 1: Write failing schema and no-lookahead tests**

Assert the feature order exactly covers returns `1/2/4/8/16/32`, EMA ratio/slopes, RSI14, ADX14, ATR14, realized volatility `8/20/64`, Donchian20 position, volume robust z, funding level/percentile, premium, OI changes `1/4/16`, spread/depth/trade imbalance, impact, cross-sectional return ranks `4/16/96`, breadth, and cyclic UTC hour/day. Assert it contains no rules-family score. Mutating data after `as_of_time` must not change a row or schema hash.

- [ ] **Step 2: Verify failure**

Run: `cargo test -p trench-core features::ml::tests`

Expected: FAIL because ML feature export is absent.

- [ ] **Step 3: Implement a fixed feature vector**

Add `MlFeatureRow { market, sleeve, as_of_time, universe_snapshot_id, names, values, completeness, schema_hash }`. Build only from the phase-1 immutable common snapshot; reject missing/non-finite inputs rather than impute. Serialize names in canonical order and derive the digest from type/name/unit/window metadata, not sample values. Extend Parquet output with event-time, receive-time, universe/config digests, and source-event range.

- [ ] **Step 4: Generate and verify the tiny fixture**

Generate the committed fixture from deterministic source events, reopen it in Rust, and assert its content digest. Run: `cargo test -p trench-core features::ml::tests && cargo test -p trench-storage parquet`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/trench-core crates/trench-storage tests/fixtures/ml/tiny-features.parquet
git commit -m "feat(ml): export point-in-time feature matrices"
```

### Task 3: Validate feature reads and construct cost-aware labels

**Files:**
- Create: `ml/src/trench_ml/data/__init__.py`
- Create: `ml/src/trench_ml/data/features.py`
- Create: `ml/src/trench_ml/data/labels.py`
- Create: `ml/tests/data/test_features.py`
- Create: `ml/tests/data/test_labels.py`

- [ ] **Step 1: Write failing point-in-time read tests**

Assert Python verifies Parquet schema/content/config/universe digests, monotonic `(as_of_time, market, sleeve)`, uniqueness, feature order, finite values, and source times no later than `as_of_time`. Add a leakage sentinel column with a future timestamp and require a hard failure.

- [ ] **Step 2: Write failing label tests**

For every row, select `p0` from the first valid BBO after bar close and `p1` from the first BBO at/after four sleeve bars. Compute `log(p1/p0)` and a fixed-100-USDC point-in-time round-trip cost using fees, funding, and book impact. Assert short/flat/long thresholds exactly match design section 7.3 and samples with gaps/non-tradeable state are absent rather than imputed.

- [ ] **Step 3: Verify failure**

Run: `cd ml && uv run pytest tests/data/test_features.py tests/data/test_labels.py -q`

Expected: FAIL because data modules are absent.

- [ ] **Step 4: Implement immutable dataset builders**

Return typed pandas frames with stable row IDs, explicit outcome timestamps, gross return, cost probe, and integer class `0=short,1=flat,2=long`. Refuse duplicated rows, timezone-naive timestamps, missing cost inputs, or any join whose source timestamp exceeds its row cutoff. Persist a data manifest with source partition digests and excluded-row reason counts.

- [ ] **Step 5: Run tests and lint**

Run: `cd ml && uv run pytest tests/data -q && uv run ruff check src/trench_ml/data tests/data`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add ml/src/trench_ml/data ml/tests/data
git commit -m "feat(ml): build leakage-safe labels"
```

### Task 4: Implement purged chronological walk-forward folds

**Files:**
- Create: `ml/src/trench_ml/data/folds.py`
- Create: `ml/tests/data/test_folds.py`

- [ ] **Step 1: Write failing exact-boundary tests**

Construct 400 daily timestamps and assert each outer fold uses 305 development days, 60 calibration days, and 30 untouched test days, then rolls 30 days. Assert inner training ends on days 185/215/245/275 with the next 30 days as validation. Purge any label horizon crossing a boundary and apply a four-hour embargo. Assert no row ID appears in train/calibration/test leakage sets.

- [ ] **Step 2: Verify failure**

Run: `cd ml && uv run pytest tests/data/test_folds.py -q`

Expected: FAIL because fold construction is absent.

- [ ] **Step 3: Implement folds as immutable index manifests**

`OuterFold` and `InnerFold` store UTC boundaries, ordered row IDs, purged IDs, embargoed IDs, and digests. Require at least three completed outer test folds and 100 aggregate closed paper trades before forward eligibility; never shorten a fold for insufficient history.

- [ ] **Step 4: Run fold properties**

Add randomized tests proving strict chronological order and no outcome-time overlap. Run: `cd ml && uv run pytest tests/data/test_folds.py -q`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add ml/src/trench_ml/data/folds.py ml/tests/data/test_folds.py
git commit -m "feat(ml): add purged temporal folds"
```

### Task 5: Train deterministic LightGBM regression and class heads

**Files:**
- Create: `ml/src/trench_ml/models/__init__.py`
- Create: `ml/src/trench_ml/models/lightgbm.py`
- Create: `ml/tests/models/test_lightgbm.py`

- [ ] **Step 1: Write failing deterministic-search tests**

Train twice on the tiny fixture with the same seeds and assert identical predictions, chosen parameters, feature order, and model-text digest. Assert the search is exactly the Cartesian grid: leaves `{15,31}`, learning rate `{0.03,0.05}`, minimum leaf data `{200,1000}`, feature fraction `{0.7,1.0}`, L2 `{1,10}`; all other settings match the design. A changed seed must be declared in the manifest.

- [ ] **Step 2: Verify failure**

Run: `cd ml && uv run pytest tests/models/test_lightgbm.py -q`

Expected: FAIL because model training is absent.

- [ ] **Step 3: Implement dual-head training and selection**

Train one Huber regression (`alpha=0.9`) and one three-class model per sleeve. Use development-fold inverse-frequency class weights, `deterministic=true`, `force_col_wise=true`, at most 2,000 trees, early stopping 100, bagging fraction 0.8/frequency 1, declared NumPy/LightGBM seeds, and one native thread count recorded in the manifest. Score each candidate through the phase-1 paper-cost evaluator; select median inner-fold net expectancy then lower complexity. Refit only on the 305 development days.

- [ ] **Step 4: Run model tests**

Run: `cd ml && uv run pytest tests/models/test_lightgbm.py -q && uv run ruff check src/trench_ml/models tests/models`

Expected: PASS. Tests may use a reduced fixture but cannot alter production grid definitions.

- [ ] **Step 5: Commit**

```bash
git add ml/src/trench_ml/models ml/tests/models
git commit -m "feat(ml): train deterministic LightGBM heads"
```

### Task 6: Calibrate forecasts and create safe frozen artifacts

**Files:**
- Create: `ml/src/trench_ml/models/calibration.py`
- Create: `ml/src/trench_ml/models/artifact.py`
- Create: `ml/tests/models/test_calibration.py`
- Create: `ml/tests/models/test_artifact.py`

- [ ] **Step 1: Write failing calibration tests**

Fit one positive temperature on the chronological 60-day calibration window and assert probabilities sum to one. Fit the one-sided 80% split-conformal residual quantile and test finite-sample quantile indexing. Reject an artifact when ECE exceeds 0.05 or calibrated multiclass Brier is worse than raw.

- [ ] **Step 2: Write failing artifact-security tests**

Round-trip LightGBM text, canonical JSON calibration/config/license manifests, and NumPy arrays with `allow_pickle=False`. Alter one byte and require digest failure. Attempt a pickle/joblib file and require refusal. Assert artifact data cutoff precedes its first test/forward prediction.

- [ ] **Step 3: Verify failure**

Run: `cd ml && uv run pytest tests/models/test_calibration.py tests/models/test_artifact.py -q`

Expected: FAIL because calibration/artifact modules are absent.

- [ ] **Step 4: Implement content-addressed artifacts**

An artifact directory contains `regressor.txt`, `classifier.txt`, `calibration.json`, `feature-schema.json`, `training-manifest.json`, `license-manifest.json`, and `artifact.json`. `artifact.json` lists BLAKE3 digests of every file and the aggregate digest. Load into a new immutable directory, verify everything before constructing boosters, and atomically switch a `champion.json` pointer only through the later manual promotion command.

- [ ] **Step 5: Run tests**

Run: `cd ml && uv run pytest tests/models -q && uv run pyright`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add ml/src/trench_ml/models ml/tests/models
git commit -m "feat(ml): freeze calibrated model artifacts"
```

### Task 7: Serve immutable inference over a protected Unix socket

**Files:**
- Create: `ml/src/trench_ml/worker.py`
- Create: `ml/tests/test_worker.py`
- Create: `tests/fixtures/ipc/request-v1.msgpack`
- Create: `tests/fixtures/ipc/response-v1.msgpack`

- [ ] **Step 1: Write failing worker protocol tests**

Start the worker in a temporary directory; assert socket mode `0600`, length-prefixed MessagePack framing, maximum frame/row limits, exact request-response ID, model/config/schema digest echo, finite outputs, calibrated `short/flat/long` probabilities, regression point estimate, and directional conformal lower bound. Test stale, duplicate, oversized, truncated, wrong-UID (where supported), and deadline-cancelled requests.

- [ ] **Step 2: Verify failure**

Run: `cd ml && uv run pytest tests/test_worker.py -q`

Expected: FAIL because the worker is absent.

- [ ] **Step 3: Implement bounded inference**

Load one verified champion artifact per sleeve at process start and never reload in place. Bind only an explicitly configured Unix path beneath a `0700` directory, remove only a stale socket owned by the current UID, cap concurrent requests with an asyncio semaphore, and use bounded frame reads. Return structured errors without stack traces or environment dumps. Candidate shadow artifacts are separate immutable registry entries and can never replace the champion pointer in this process.

- [ ] **Step 4: Generate compatibility fixtures and rerun**

Write fixtures through Python, record their digests, and do not regenerate them during normal tests. Run: `cd ml && uv run pytest tests/test_worker.py -q && uv run ruff check src/trench_ml/worker.py tests/test_worker.py`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add ml/src/trench_ml/worker.py ml/tests/test_worker.py tests/fixtures/ipc
git commit -m "feat(ml): serve frozen Unix-socket inference"
```

### Task 8: Add the Rust inference client and scoped ML readiness

**Files:**
- Create: `crates/trenchd/src/ml_client.rs`
- Modify: `crates/trenchd/src/app.rs`
- Modify: `crates/trenchd/src/readiness.rs`
- Modify: `crates/trenchd/Cargo.toml`
- Modify: `Cargo.toml`
- Modify: `Cargo.lock`
- Test: inline Rust async tests using the committed IPC fixtures

- [ ] **Step 1: Write failing compatibility/deadline tests**

Decode Python request/response fixtures in Rust and assert field-for-field parity; encode through the declared canonical field order and compare with the fixture bytes. Test unknown schema, stale response, digest mismatch, duplicate response ID, non-finite output, worker disconnect, and a fixed two-second default deadline. Assert every failure skips only that ML boundary while global and `rules_only` readiness remain unchanged.

- [ ] **Step 2: Verify failure**

Run: `cargo test -p trenchd ml_client::tests && cargo test -p trenchd readiness::tests`

Expected: FAIL because `MlClient` is absent.

- [ ] **Step 3: Implement the bounded client**

Add `rmp-serde` to the workspace and use a Unix stream, length-prefixed MessagePack, maximum frame/row limits, exact schema structs, one in-flight request per connection, and `tokio::time::timeout`. Construct each request from a frozen Rust feature batch and persist request timestamp before send; persist the response or failure with actual latency. Never retry a missed bar-close forecast or open TCP.

- [ ] **Step 4: Run Rust/Python contract tests**

Run: `cargo test -p trenchd ml_client::tests && cd ml && uv run pytest tests/test_worker.py tests/test_schema.py -q`

Expected: PASS with fixture digests unchanged.

- [ ] **Step 5: Commit**

```bash
git add crates/trenchd
git commit -m "feat(daemon): add scoped ML inference client"
```

### Task 9: Convert validated forecasts into `ml_champion` intents

**Files:**
- Create: `crates/trench-core/src/strategy/ml.rs`
- Modify: `crates/trench-core/src/strategy/mod.rs`
- Modify: `crates/trench-core/src/engine.rs`
- Create: `crates/trench-core/tests/ml_ledger.rs`

- [ ] **Step 1: Write failing entry/exit tests**

Assert entry requires regression/class sign agreement, calibrated directional probability at least 0.58, one-sided 80% lower directional net return above zero, and predicted gross movement at least 1.5 times the current risk-sized full cost. Assert it uses the same ATR/swing stop, configured 2R target, four-bar timeout, and gated opposite-class exit as rules. Reject mismatched artifact/config/feature hashes and late forecasts.

- [ ] **Step 2: Write failing independence tests**

Feed identical market events to `rules_only` and `ml_champion`; mutate each signal stream separately and assert the other ledger's positions, PnL, breakers, and decisions are unchanged. Assert each starts at exactly 100 synthetic USDC and each has at most one position.

- [ ] **Step 3: Verify failure**

Run: `cargo test -p trench-core strategy::ml::tests && cargo test -p trench-core --test ml_ledger`

Expected: FAIL because the ML strategy/ledger path is absent.

- [ ] **Step 4: Implement forecast events and ML arbitration**

Record each accepted/failed response as an immutable decision input with its request ID and receive time. `MlStrategy` receives no rules scores and emits the same un-sized `OrderIntent` type. The existing risk/broker path is reused unchanged with independent state. At a missed/failed forecast, emit an auditable skip and continue rules processing.

- [ ] **Step 5: Run strategy and independence tests**

Run: `cargo test -p trench-core strategy::ml::tests && cargo test -p trench-core --test ml_ledger && cargo clippy -p trench-core --all-targets -- -D warnings`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/trench-core
git commit -m "feat(strategy): add independent ML champion ledger"
```

### Task 10: Implement robustness metrics and exact promotion gates

**Files:**
- Create: `ml/src/trench_ml/evaluation/__init__.py`
- Create: `ml/src/trench_ml/evaluation/metrics.py`
- Create: `ml/src/trench_ml/evaluation/robustness.py`
- Create: `ml/src/trench_ml/evaluation/promotion.py`
- Create: `ml/tests/evaluation/test_metrics.py`
- Create: `ml/tests/evaluation/test_robustness.py`
- Create: `ml/tests/evaluation/test_promotion.py`
- Create: `tests/fixtures/ml/tiny-trades.parquet`
- Create: `reports/schemas/promotion-v1.json`

- [ ] **Step 1: Write failing metric tests**

Use hand-calculated fixtures for net expectancy, Sharpe/Sortino/Calmar/Omega, maximum drawdown, ECE, multiclass Brier, PSI, asset/month concentration, and paired daily return. Test missing/NaN/zero-variance behavior explicitly; undefined metrics fail eligibility rather than become zero.

- [ ] **Step 2: Write failing robustness tests**

Use seeded stationary bootstrap from `arch`, block-trade Monte Carlo, cost/latency multipliers, leave-one-asset/regime-out, parameter perturbation, feature-family ablation, deflated Sharpe, and combinatorially symmetric cross-validation PBO. Add TreeSHAP checks that explanations use the frozen feature order and that aggregate importance/stability is reported by fold, asset, and regime. Assert time/block structure is preserved and seed/config/digest are recorded.

- [ ] **Step 3: Write failing gate-table tests**

Express every design section 10.3 threshold in a typed gate table. Test each gate failing alone: 90 days, 100 trades, 95% bootstrap lower mean above zero, DSR probability 0.95, PBO 0.20, positive 1.5x expectancy, no 2x breaker, 35% asset/40% month concentration, positive without best asset, ECE/weekly drift/PSI limits, zero liquidation, no 8% breaker, stress survival, and paired lower bound/no-worse drawdown for replacements.

- [ ] **Step 4: Verify failure**

Run: `cd ml && uv run pytest tests/evaluation -q`

Expected: FAIL because evaluation modules are absent.

- [ ] **Step 5: Implement deterministic reports**

Reports must separate gross alpha, protocol fee, builder fee, spread, depth, latency, funding, and liquidation; include every gate as pass/fail/not-applicable plus evidence digest; and distinguish offline outer-fold, provisional bootstrap, forward absolute, and paired replacement eligibility. Never auto-relax or auto-promote.

- [ ] **Step 6: Run evaluation tests**

Run: `cd ml && uv run pytest tests/evaluation -q && uv run ruff check src/trench_ml/evaluation tests/evaluation && uv run pyright`

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add ml/src/trench_ml/evaluation ml/tests/evaluation tests/fixtures/ml/tiny-trades.parquet reports/schemas
git commit -m "feat(research): add robust promotion gates"
```

### Task 11: Add immutable shadow-run persistence and execution

**Files:**
- Create: `crates/trench-storage/migrations/0002_ml.sql`
- Create: `crates/trench-storage/src/ml.rs`
- Modify: `crates/trench-storage/src/lib.rs`
- Create: `crates/trench-core/src/shadow.rs`
- Modify: `crates/trench-core/src/lib.rs`
- Modify: `crates/trench-core/src/engine.rs`
- Test: storage and core unit tests

- [ ] **Step 1: Write failing registration tests**

Register a candidate with immutable code/config/feature/model/data-cutoff/license digests before its first prediction. Assert an altered digest, late registration, more than three production-eligible shadows, duplicate active version, research-only artifact, or mutation after outcomes is rejected.

- [ ] **Step 2: Write failing shadow-isolation tests**

Drive a shadow through the same market events, broker, and risk engine with its own synthetic 100 USDC. Assert it cannot affect visible ledger state, universe, readiness, or order source; its timestamped prediction must precede the corresponding outcome. Replay must reproduce it from recorded predictions.

- [ ] **Step 3: Verify failure**

Run: `cargo test -p trench-storage ml::tests && cargo test -p trench-core shadow::tests`

Expected: FAIL because ML tables/shadow state are absent.

- [ ] **Step 4: Implement the generic evaluation-only lifecycle**

The migration adds `model_artifacts`, `artifact_files`, `shadow_runs`, `shadow_predictions`, `shadow_ledger_transitions`, and `promotion_reports` with immutable triggers/constraints. `ShadowRun` wraps an isolated engine/ledger and is never a `LedgerId`; expose results only to evaluation/reporting. Active shadow count is checked transactionally.

- [ ] **Step 5: Run persistence/isolation tests**

Run: `cargo test -p trench-storage ml::tests && cargo test -p trench-core shadow::tests`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/trench-storage crates/trench-core
git commit -m "feat(research): add isolated forward shadow runs"
```

### Task 12: Enforce challenger licensing and runtime eligibility

**Files:**
- Create: `ml/src/trench_ml/licenses.py`
- Create: `ml/tests/test_licenses.py`
- Create: `ml/licenses/challengers.toml`

- [ ] **Step 1: Write failing policy tests**

Seed registry entries for LightGBM, XGBoost, CatBoost, RealMLP, TabICLv2, Nori-30M, TabPFN-3, and Google TabFM. Assert TabPFN-3 and default TabFM weights are research-only; their outputs cannot enter runtime, shadow eligibility, distillation/training, or promotion. Assert a changed model/license digest forces re-review and unknown status fails closed.

- [ ] **Step 2: Verify failure**

Run: `cd ml && uv run pytest tests/test_licenses.py -q`

Expected: FAIL because the registry does not exist.

- [ ] **Step 3: Implement digest-bound license policy**

Store source URL, retrieval date, code license, weight license, output restrictions, allowed purposes, reviewer, and source/model/license digests. Runtime/artifact code asks `assert_allowed(artifact, purpose)` for `offline_research`, `train`, `runtime_inference`, `shadow`, and `promotion`. There is no permissive default.

- [ ] **Step 4: Run policy and artifact tests**

Run: `cd ml && uv run pytest tests/test_licenses.py tests/models/test_artifact.py -q`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add ml/src/trench_ml/licenses.py ml/tests/test_licenses.py ml/licenses
git commit -m "feat(ml): enforce challenger artifact licenses"
```

### Task 13: Add reproducible research and manual promotion CLI

**Files:**
- Create: `ml/src/trench_ml/cli.py`
- Create: `ml/tests/test_cli.py`
- Modify: `ml/pyproject.toml`
- Create: `models/.gitkeep`

- [ ] **Step 1: Write failing CLI tests**

Test these commands against temporary paths:

```text
trench-ml dataset build --config ...
trench-ml train --config ... --sleeve 15m|1h
trench-ml evaluate --artifact ... --manifest ...
trench-ml shadow register --artifact ...
trench-ml promote --artifact ... --report ... --target ...
trench-ml serve --config ...
```

Assert nonzero exit for insufficient history, incomplete folds, failed gate, research-only license, stale report/artifact mismatch, existing target, and non-atomic target filesystem. Assert `promote` requires an explicit report path and never runs from `evaluate`.

- [ ] **Step 2: Verify failure**

Run: `cd ml && uv run pytest tests/test_cli.py -q`

Expected: FAIL because CLI entry points are absent.

- [ ] **Step 3: Implement orchestration without hidden state**

Each command resolves one strict config, prints a concise structured summary, writes a manifest/report atomically, and exits. `promote` verifies absolute/paired requirements, artifact/report/data/code/license digests, then installs into a new immutable directory and atomically replaces `champion.json`; it never edits a model. LightGBM v1 may use the provisional no-incumbent path only after offline eligibility and must be labeled provisional until 90 days/100 forward trades pass.

- [ ] **Step 4: Run CLI quality checks**

Run: `cd ml && uv run pytest tests/test_cli.py -q && uv run ruff check . && uv run ruff format --check . && uv run pyright`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add ml/src/trench_ml/cli.py ml/tests/test_cli.py ml/pyproject.toml models/.gitkeep
git commit -m "feat(ml): add reproducible research commands"
```

### Task 14: Prove end-to-end two-ledger behavior

**Files:**
- Create: `tests/fixtures/stream/two-ledger.jsonl`
- Modify: `crates/trench-core/tests/ml_ledger.rs`
- Create: `ml/tests/test_inference_parity.py`
- Modify: `AGENTS.md`

- [ ] **Step 1: Add offline/runtime inference parity**

For both sleeves, run a frozen tiny artifact over the committed feature fixture offline and through the socket. Assert bit-stable class probabilities within declared numerical tolerance, regression predictions, conformal bounds, intent decisions, and artifact/schema/config digests.

- [ ] **Step 2: Add a complete shared-stream replay**

The new fixture must cause rules and ML to make independently explainable decisions through the same risk/broker code. Replay twice and compare both visible ledgers, forecasts, costs, fills, skips, and shadow state byte-for-byte. Kill/restart the worker before a boundary; assert only ML skips and later recovers after a matching handshake.

- [ ] **Step 3: Run the full quality gate**

Run:

```bash
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cd ml
uv run pytest -q
uv run ruff check .
uv run ruff format --check .
uv run pyright
cd ..
./scripts/check-paper-boundary.sh
git diff --check
```

Expected: every command exits 0.

- [ ] **Step 4: Record truthful eligibility state**

Update `AGENTS.md` with observed tests and current data-history eligibility. If 365+ trustworthy days or three outer folds are unavailable, `ml_champion` must remain unready/provisional; do not weaken the fold or promotion requirements to make the demo trade.

- [ ] **Step 5: Commit**

```bash
git add tests/fixtures/stream/two-ledger.jsonl crates/trench-core/tests/ml_ledger.rs ml/tests/test_inference_parity.py AGENTS.md
git commit -m "test: validate independent rules and ML ledgers"
```

## Phase-2 completion gate

Phase 2 is complete when both visible ledgers replay independently through the shared risk/broker, Python and Rust agree on a frozen artifact, late/bad inference pauses only ML, all research outputs are point-in-time and reproducible, and shadow/promotion/license gates fail closed. Real forward promotion remains impossible until the untouched 90-day/100-trade evidence exists; that waiting period is an experimental requirement, not unfinished software.
