# Trench ML Champion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a separately validated LightGBM `ml_champion` ledger plus the shared robustness, forward-shadow, and promotion machinery required to validate both `rules_only` and ML without weakening the Rust risk authority.

**Architecture:** Rust remains authoritative for features, deadlines, risk, paper fills, ledgers, and replay; Python trains and serves immutable models over a versioned MessagePack Unix socket. Point-in-time Parquet datasets feed chronological walk-forward jobs, while content-addressed artifacts and SQLite registrations make every prediction and promotion auditable. ML failure degrades only the ML ledger.

**Tech Stack:** Rust 2024, Tokio Unix sockets, MessagePack; Python 3.12 managed by uv, NumPy, pandas, PyArrow, LightGBM, scikit-learn, SciPy, SHAP, arch bootstrap, msgspec, pytest, Ruff, Pyright.

---

## Scope and prerequisites

Execute this only after [the rules-platform plan](2026-08-03-trench-rules-platform.md) passes its completion gate. This phase implements the approved [ML design](../specs/2026-08-03-trench-paper-trading-bot-design.md), consumes the frozen rules walk-forward artifact, and applies the same robustness/forward gates to rules without changing its production logic. It creates no live executor, wallet, Telegram integration, TCP inference port, online learning, automatic promotion, or production use of research-only foundation-model artifacts.

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
crates/trench-core/src/labels.rs                durable four-bar BBO/book-cost observations
crates/trench-core/src/strategy/ml.rs           forecast-to-intent/exit policy
crates/trench-core/src/shadow.rs                evaluation-only ledger copies
crates/trench-core/tests/ml_ledger.rs            strategy independence/replay tests
crates/trench-storage/migrations/0002_strategy_validation.sql
                                                 artifacts, forecasts, shadows, reports
crates/trench-storage/src/strategy.rs           atomic strategy registrations and journals
crates/trenchd/src/ml_client.rs                 deadline-bound Unix-socket client
crates/trenchd/src/admin.rs                     shadow/forward admin protocol extensions
crates/trenchd/src/app.rs                       ML readiness/inference wiring
crates/trenchd/src/commands.rs                  Rust broker evaluation/shadow/run commands
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
  src/trench_ml/evaluation_bridge.py            invokes authoritative Rust replay evaluator
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

- `schema_version=1`; each envelope has an explicit `payload_type`; unknown versions, payload types, or fields fail closed.
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
request = Envelope(
    schema_version=1,
    event_id="evt-1",
    event_time_ns=1_786_000_000_000_000_000,
    as_of_time_ns=1_786_000_000_000_000_000,
    producer_version="test",
    run_id="run-1",
    config_hash="b3:test",
    payload_type="inference_request",
    payload=InferenceRequest(
        feature_schema_hash="b3:features-v1",
        artifact_ids=("b3:champion",),
        rows=(FeatureRow(market="SOL", sleeve="15m", values=(0.1, 0.2)),),
    ),
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
- Create: `crates/trench-core/src/labels.rs`
- Modify: `crates/trench-core/src/features/mod.rs`
- Modify: `crates/trench-core/src/engine.rs`
- Modify: `crates/trench-storage/src/parquet.rs`
- Create: `tests/fixtures/ml/tiny-features.parquet`
- Test: inline Rust tests

- [ ] **Step 1: Write failing schema and no-lookahead tests**

Assert the feature order exactly covers returns `1/2/4/8/16/32`, EMA ratio/slopes, RSI14, ADX14, ATR14, realized volatility `8/20/64`, Donchian20 position, volume robust z, funding level/percentile, premium, OI changes `1/4/16`, spread/depth/trade imbalance, impact, cross-sectional return ranks `4/16/96`, breadth, and cyclic UTC hour/day. Assert it contains no rules-family score. Mutating data after `as_of_time` must not change a row or schema hash. For every completed bar, assert a pending label anchor captures the first valid post-close BBO/book; exactly four sleeve bars later it finalizes from the first valid book with both long/short fixed-100-USDC VWAP impact, fees, intervening funding, universe state, gap flags, and source event IDs.

- [ ] **Step 2: Verify failure**

Run: `cargo test -p trench-core features::ml::tests && cargo test -p trench-core labels::tests`

Expected: FAIL because ML feature export is absent.

- [ ] **Step 3: Implement a fixed feature vector**

Add `MlFeatureRow { market, sleeve, as_of_time, universe_snapshot_id, names, values, completeness, schema_hash }`. Build only from the phase-1 immutable common snapshot; reject missing/non-finite inputs rather than impute. Serialize names in canonical order and derive the digest from type/name/unit/window metadata, not sample values. Extend Parquet output with event-time, receive-time, universe/config digests, and source-event range.

Add `LabelObservation` as a compact engine state machine keyed by `(market,sleeve,bar_close)`. It stores `p0/p1` mids, side-specific 100-USDC entry/exit VWAP impacts, fees, observed funding path, conservative `cost_probe=max(long_cost,short_cost)`, data-quality/universe flags, and exact source IDs. Finalized observations are immutable Parquet rows retained indefinitely; invalid/gapped anchors are finalized with exclusion reasons. Live events and imported official archive events use the same materializer, so raw BBO/L2 can expire without deleting training labels.

- [ ] **Step 4: Generate and verify the tiny fixture**

Generate the committed fixture and finalized label observations from deterministic source events, reopen them in Rust, and assert their content digests. Run: `cargo test -p trench-core features::ml::tests && cargo test -p trench-core labels::tests && cargo test -p trench-storage parquet`

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

For every row, consume the Rust-materialized `LabelObservation`, verify its source IDs/times and fixed-100-USDC long/short book walks, then compute `log(p1/p0)` and use its conservative cost probe. Assert short/flat/long thresholds exactly match design section 7.3 and samples with gaps/non-tradeable state are absent rather than imputed. A raw BBO/L2 lookup in Python is a test failure; those high-rate partitions may already have expired.

- [ ] **Step 3: Verify failure**

Run: `cd ml && uv run pytest tests/data/test_features.py tests/data/test_labels.py -q`

Expected: FAIL because data modules are absent.

- [ ] **Step 4: Implement immutable dataset builders**

Return typed pandas frames with stable row IDs, explicit outcome timestamps, gross return, side-specific costs, conservative cost probe, and integer class `0=short,1=flat,2=long`. Refuse duplicated rows, timezone-naive timestamps, missing cost inputs, or any feature join whose source timestamp exceeds its row cutoff. Persist a data manifest with feature/label/archive partition digests and excluded-row reason counts.

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

### Task 4A: Implement calibration primitives and the ML decision policy

**Files:**
- Create: `ml/src/trench_ml/models/__init__.py`
- Create: `ml/src/trench_ml/models/calibration.py`
- Create: `ml/tests/models/test_calibration.py`
- Create: `crates/trench-core/src/strategy/ml.rs`
- Modify: `crates/trench-core/src/strategy/mod.rs`
- Modify: `crates/trench-core/src/lib.rs`
- Test: Python calibration and Rust policy tests

- [ ] **Step 1: Write failing calibration tests**

Fit one positive temperature on chronological calibration rows and assert probabilities sum to one. Fit the one-sided 80% split-conformal residual quantile with correct finite-sample indexing. Reject non-finite/raw-order mismatches, ECE above 0.05, or calibrated multiclass Brier worse than raw. For inner model selection, assert the last 30 chronological days of each purged inner-training window are temporary calibration-only rows and never booster-training or validation rows.

- [ ] **Step 2: Write failing Rust decision-policy tests**

Construct a frozen `MlForecast` without a trained model and assert it becomes an un-sized `SignalCandidate`. Given a public `CostQuote`, require regression/class direction agreement, probability `>=0.58`, directional 80% conformal lower net bound above zero, and predicted gross movement `>=1.5 * full_cost`; return an intent bound only to the opaque quote ID. Reject late or artifact/config/feature/schema mismatches. Test shared stop/target/time/opposite-class behavior without an ML ledger.

- [ ] **Step 3: Verify failure**

Run: `cd ml && uv run pytest tests/models/test_calibration.py -q && cd .. && cargo test -p trench-core strategy::ml::tests`

Expected: FAIL because calibration and the policy primitive are absent.

- [ ] **Step 4: Implement reusable primitives**

Implement deterministic temperature/conformal fit/transform functions that operate on explicit arrays and return canonical JSON state. Implement `MlDecisionPolicy` as pure Rust over `MlForecast`, common feature state, and `CostQuote`; it owns no worker, model, ledger, risk, or I/O. Define the authoritative forecast Parquet/wire fields here so offline evaluation and the later runtime client share the exact policy input.

- [ ] **Step 5: Run focused quality checks**

Run: `cd ml && uv run pytest tests/models/test_calibration.py -q && uv run ruff check src/trench_ml/models/calibration.py tests/models/test_calibration.py && cd .. && cargo test -p trench-core strategy::ml::tests && cargo clippy -p trench-core --all-targets -- -D warnings`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add ml/src/trench_ml/models ml/tests/models crates/trench-core
git commit -m "feat(ml): add calibrated decision policy"
```

### Task 5: Train deterministic LightGBM regression and class heads

**Files:**
- Create: `ml/src/trench_ml/models/lightgbm.py`
- Create: `ml/src/trench_ml/evaluation_bridge.py`
- Create: `ml/tests/models/test_lightgbm.py`
- Create: `ml/tests/test_evaluation_bridge.py`
- Modify: `crates/trenchd/src/commands.rs`

- [ ] **Step 1: Write failing deterministic-search tests**

Train twice on the tiny fixture with the same seeds and assert identical predictions, chosen parameters, feature order, and model-text digest. Assert the search is exactly the Cartesian grid: leaves `{15,31}`, learning rate `{0.03,0.05}`, minimum leaf data `{200,1000}`, feature fraction `{0.7,1.0}`, L2 `{1,10}`; all other settings match the design. A changed seed must be declared in the manifest. Write candidate predictions to Parquet, evaluate them twice through the Rust bridge, and assert byte-identical intents, sealed costs, fills, and net-expectancy results.

- [ ] **Step 2: Verify failure**

Run: `cd ml && uv run pytest tests/models/test_lightgbm.py tests/test_evaluation_bridge.py -q`

Expected: FAIL because model training is absent.

- [ ] **Step 3: Implement dual-head training and selection**

Add the authoritative command:

```text
trenchd evaluate-forecasts --config PATH --replay-manifest PATH --forecast-parquet PATH --output DIRECTORY
```

It verifies row/data/config/schema digests, replays forecasts through the same signal → sealed risk quote → cost acceptance → broker/ledger engine as runtime, and atomically emits trades, attributed costs, rejections, daily equity, and a result digest. It has no alternate sizing/fill implementation.

`RustBrokerEvaluator` invokes that binary with an argv list (never a shell), explicit timeout/temp directory, and verifies the returned manifest/digests before exposing net expectancy. Train one Huber regression (`alpha=0.9`) and one three-class model per sleeve. Use development-fold inverse-frequency class weights, `deterministic=true`, `force_col_wise=true`, at most 2,000 trees, early stopping 100, bagging fraction 0.8/frequency 1, declared NumPy/LightGBM seeds, and one native thread count recorded in the manifest.

For each inner fold, reserve its last 30 purged training days for temporary temperature/conformal calibration, train boosters only on the preceding inner-training rows, transform validation forecasts with those temporary calibrators, then score every candidate only through the Rust bridge and the already-implemented `MlDecisionPolicy`. Select median inner-fold net expectancy then lower complexity. After selection, refit boosters on all 305 development days; Task 6 fits the final calibrators only on the subsequent 60-day calibration window.

- [ ] **Step 4: Run model tests**

Run: `cargo test -p trenchd commands::evaluate_forecasts_tests && cd ml && uv run pytest tests/models/test_lightgbm.py tests/test_evaluation_bridge.py -q && uv run ruff check src/trench_ml/models src/trench_ml/evaluation_bridge.py tests/models tests/test_evaluation_bridge.py`

Expected: PASS. Tests may use a reduced fixture but cannot alter production grid definitions.

- [ ] **Step 5: Commit**

```bash
git add crates/trenchd/src/commands.rs ml/src/trench_ml/models ml/src/trench_ml/evaluation_bridge.py ml/tests/models ml/tests/test_evaluation_bridge.py
git commit -m "feat(ml): train deterministic LightGBM heads"
```

### Task 6: Calibrate forecasts and create safe frozen artifacts

**Files:**
- Modify: `ml/src/trench_ml/models/calibration.py`
- Create: `ml/src/trench_ml/models/artifact.py`
- Modify: `ml/tests/models/test_calibration.py`
- Create: `ml/tests/models/test_artifact.py`

- [ ] **Step 1: Write failing calibration tests**

Using the selected boosters refit on all 305 development days, fit the existing temperature/conformal primitives on exactly the subsequent 60 chronological days and assert no development/test row enters. Reject final calibration when ECE exceeds 0.05 or calibrated multiclass Brier is worse than raw, and bind its row/dataset/model digests into the artifact.

- [ ] **Step 2: Write failing artifact-security tests**

Round-trip LightGBM text, canonical JSON calibration/config/license manifests, and NumPy arrays with `allow_pickle=False`. Alter one byte and require digest failure. Attempt a pickle/joblib file and require refusal. Assert artifact data cutoff precedes its first test/forward prediction.

- [ ] **Step 3: Verify failure**

Run: `cd ml && uv run pytest tests/models/test_calibration.py tests/models/test_artifact.py -q`

Expected: FAIL because final artifact assembly is absent.

- [ ] **Step 4: Implement content-addressed artifacts**

Fit final calibration only through the Task-4A primitives and the declared 60-day manifest. An artifact directory contains `regressor.txt`, `classifier.txt`, `calibration.json`, `feature-schema.json`, `training-manifest.json`, `license-manifest.json`, and `artifact.json`. `artifact.json` lists BLAKE3 digests of every file and the aggregate digest. Load into a new immutable directory, verify everything before constructing boosters, and emit a candidate pointer only through the later manual promotion command.

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

Start the worker in a temporary directory; assert socket mode `0600`, length-prefixed MessagePack framing, maximum frame/row limits, exact request-response ID, model/config/schema digest echo, finite outputs, calibrated `short/flat/long` probabilities, regression point estimate, and directional conformal lower bound. Load the champion named by canonical `champion.json`, echo its full digest set during handshake, and fail closed on a missing/mutated/incompatible pointer. Reconcile desired shadow sets `{A,B}`, `{B,C}`, and `{}`; prove removed boosters unload, replacements do not consume a fourth slot, and a failed new-set load leaves the prior set intact. Request champion plus candidates in one bar batch and prove candidate failure cannot alter champion output. Test stale, duplicate, oversized, truncated, wrong-UID (where supported), unregistered artifact, over-three desired set, digest collision, and deadline-cancelled requests.

- [ ] **Step 2: Verify failure**

Run: `cd ml && uv run pytest tests/test_worker.py -q`

Expected: FAIL because the worker is absent.

- [ ] **Step 3: Implement bounded inference**

Load one verified champion artifact per sleeve from the configured canonical `champion.json` at process start and never reload a digest in place. Bind only an explicitly configured Unix path beneath a `0700` directory, remove only a stale socket owned by the current UID, cap concurrent requests with an asyncio semaphore, and use bounded frame reads. Return structured errors without stack traces or environment dumps.

Implement explicit `sync_shadows`, `list_artifacts`, and `infer` payload types. `sync_shadows` receives the complete desired digest/path set, validates every path beneath the model root and every artifact/license/schema/config digest, loads additions into a temporary registry, then atomically swaps registries and drops removed boosters only if the entire set succeeds. The set is capped at three and never changes the champion pointer. Only the authenticated Rust daemon peer may call it. Inference requests name exact artifact digests and responses are keyed by digest. On restart the worker starts champion-only; the Rust handshake syncs the active database set before its next bar, with missed boundaries recorded rather than backfilled.

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

Decode Python request/response fixtures in Rust and assert field-for-field parity; encode through the declared canonical field order and compare with the fixture bytes. Test unknown schema/payload type, stale response, digest mismatch, duplicate response ID, non-finite output, worker disconnect, expected-versus-handshake champion mismatch, shadow full-set sync/list/recovery including removals, candidate-only failure, and a fixed two-second default deadline. Assert champion failure skips only that ML boundary, candidate failure skips only that shadow, and global/`rules_only` readiness remain unchanged.

- [ ] **Step 2: Verify failure**

Run: `cargo test -p trenchd ml_client::tests && cargo test -p trenchd readiness::tests`

Expected: FAIL because `MlClient` is absent.

- [ ] **Step 3: Implement the bounded client**

Add `rmp-serde` to the workspace and use a Unix stream, length-prefixed MessagePack, maximum frame/row limits, exact schema structs, one in-flight request per connection, and `tokio::time::timeout`. The startup handshake returns the champion artifact/report/code/config/feature/schema/license digest set; the client compares it with the writer's pending/active champion before allowing ML readiness. Construct each request from a frozen Rust feature batch plus the registered champion/active-shadow digest list and persist request timestamps before send; persist every per-artifact response/failure with actual latency. Expose bounded full-set sync/list calls used by startup and every registration/pause reconciliation. Never retry a missed bar-close forecast or open TCP.

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
- Modify: `crates/trench-core/src/strategy/ml.rs`
- Modify: `crates/trench-core/src/strategy/mod.rs`
- Modify: `crates/trench-core/src/engine.rs`
- Create: `crates/trench-core/tests/ml_ledger.rs`

- [ ] **Step 1: Write failing entry/exit tests**

Assert an ML forecast first produces an un-sized `SignalCandidate`. After the engine obtains a sealed risk quote, entry requires regression/class sign agreement, calibrated directional probability at least 0.58, one-sided 80% lower directional net return above zero, and predicted gross movement at least 1.5 times the quote's public risk-sized full cost. Assert the strategy never receives quantity/leverage/margin or the sealed order and uses the same ATR/swing stop, configured 2R target, four-bar timeout, and gated opposite-class exit as rules. Reject mismatched artifact/config/feature hashes and late forecasts.

- [ ] **Step 2: Write failing independence tests**

Feed identical market events to `rules_only` and `ml_champion`; mutate each signal stream separately and assert the other ledger's positions, PnL, breakers, and decisions are unchanged. Assert each starts at exactly 100 synthetic USDC and each has at most one position.

- [ ] **Step 3: Verify failure**

Run: `cargo test -p trench-core strategy::ml::tests && cargo test -p trench-core --test ml_ledger`

Expected: FAIL because runtime ML ledger integration is absent.

- [ ] **Step 4: Implement forecast events and ML arbitration**

Record each accepted/failed response as an immutable decision input with its request ID and receive time. `MlStrategy` receives no rules scores, emits the same un-sized `SignalCandidate` type, and applies the common public-cost acceptance interface to produce an `OrderIntent` bound to an opaque quote ID. The existing sealed risk/broker path is reused unchanged with independent state. At a missed/failed forecast, emit an auditable skip and continue rules processing.

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

Use hand-calculated fixtures for both the frozen rules report and ML trades: net expectancy, Sharpe/Sortino/Calmar/Omega, maximum drawdown, ECE, multiclass Brier, PSI, asset/month concentration, and paired daily return. Test missing/NaN/zero-variance behavior explicitly; undefined required metrics fail eligibility rather than become zero, while ML-only calibration metrics are explicitly not applicable to rules.

- [ ] **Step 2: Write failing robustness tests**

Require named, independently digestible scenarios—not one generic stress flag:

- bull, bear, range, high-volatility, low-liquidity, funding-extreme, listing, delisting, and exchange-gap partitions from point-in-time labels;
- base, 1.5x, and 2x complete-cost/latency replays plus a frozen severe replay using 2x fees/funding, 3x spread/depth/latency, and 10% missed entry/exit attempts;
- seeded block-trade Monte Carlo over returns plus empirical missed-fill, slippage, and latency paths;
- sampling from the append-only measured deployment decision-latency distribution, with missing deployment samples making forward promotion ineligible rather than substituting zero latency;
- leave-one-asset and leave-one-regime-out, declared parameter perturbations, and feature-family ablations;
- deflated Sharpe and combinatorially symmetric cross-validation PBO;
- for ML, prediction-decile gross/net-return monotonicity plus weekly ECE/PSI drift;
- TreeSHAP using the frozen feature order, summarized by fold, asset, and regime.

Assert time/block structure is preserved and every scenario records sample count, exclusions, seeds, config/data/latency digest, outcome, and unresolved-state/breaker/liquidation flags. A missing required regime/result cannot be reported as survival.

- [ ] **Step 3: Write failing gate-table tests**

Express every design section 10.3 threshold in a typed gate table keyed by `strategy_kind=rules|ml`. Test each common gate failing alone: 90 days, 100 trades, 95% bootstrap lower mean above zero, DSR probability 0.95, PBO 0.20, positive 1.5x expectancy, no 2x breaker, 35% asset/40% month concentration, positive without best asset, zero liquidation, no 8% breaker, every named robustness result present with no state corruption, and paired lower bound/no-worse drawdown for replacements. Test ECE/weekly drift/PSI only for ML and prove they cannot accidentally block or silently pass a required common rules gate. Decile monotonicity is always reported for ML and may diagnose/reject a candidate through the declared report review, but no undeclared numeric promotion threshold is invented.

- [ ] **Step 4: Verify failure**

Run: `cd ml && uv run pytest tests/evaluation -q`

Expected: FAIL because evaluation modules are absent.

- [ ] **Step 5: Implement deterministic reports**

The evaluator accepts either the phase-1 content-addressed `rules-artifact.json` plus its authoritative Rust trade streams or an ML artifact plus its authoritative forecast evaluation streams. It must execute and embed the full named scenario matrix from Step 2, not consume a caller-supplied summary boolean. Reports must separate gross alpha, protocol fee, builder fee, spread, depth, latency, funding, and liquidation; include every gate/scenario as pass/fail/not-applicable plus evidence digest; and distinguish offline outer-fold, provisional bootstrap, forward absolute, and paired replacement eligibility for each strategy kind. Rules and ML never share selection results or signals. Never auto-relax or auto-promote.

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
- Create: `crates/trench-storage/migrations/0002_strategy_validation.sql`
- Create: `crates/trench-storage/src/strategy.rs`
- Modify: `crates/trench-storage/src/lib.rs`
- Create: `crates/trench-core/src/shadow.rs`
- Modify: `crates/trench-core/src/lib.rs`
- Modify: `crates/trench-core/src/engine.rs`
- Modify: `crates/trenchd/src/admin.rs`
- Modify: `crates/trenchd/src/app.rs`
- Modify: `crates/trenchd/src/commands.rs`
- Modify: `crates/trenchd/src/ml_client.rs`
- Test: storage and core unit tests

- [ ] **Step 1: Write failing registration tests**

Register both a rules-config candidate and an ML artifact with immutable kind/code/config/feature/model-or-rules/data-cutoff/license digests before their first decision. Assert an altered digest, late registration, more than three production-eligible shadows in aggregate, duplicate active version, research-only artifact, or mutation after outcomes is rejected. Test versioned admin commands for register/list/pause, `release prepare`, `run rotate`, `champion activate`, and `forward activate`; non-owner, stale report, non-reconciled run, non-flat visible ledger, reused run ID, and already-started outcomes fail closed. Inject crashes before/after release quiescence, the pending champion transaction, champion-pointer rename, worker restart, and handshake; no decision may cross a release/champion digest boundary or be journaled under the old run with the new digest or under the new run with the old digest.

- [ ] **Step 2: Write failing shadow-isolation tests**

Drive rules and ML shadows through the same market events, sealed quote flow, broker, and risk engine with separate synthetic 100 USDC states. Assert they cannot affect visible ledger state, universe, readiness, or order source; each timestamped rules signal/ML prediction must be durably journaled before its corresponding broker outcome. Restart the worker and daemon, reconcile active registrations, and assert scheduling resumes only after exact artifact handshakes. Replay must reproduce shadows from recorded decisions without invoking Python.

- [ ] **Step 3: Verify failure**

Run: `cargo test -p trench-storage strategy::tests && cargo test -p trench-core shadow::tests`

Expected: FAIL because strategy-validation tables/shadow state are absent.

- [ ] **Step 4: Implement the generic evaluation-only lifecycle**

The migration adds generic `strategy_artifacts` (`kind=rules|ml`), `artifact_files`, `shadow_runs`, `shadow_decisions`, `shadow_ledger_transitions`, `release_activations`, `champion_activations`, `forward_runs`, and `promotion_reports` with immutable triggers/constraints. `ShadowRun` wraps an isolated engine/ledger and is never a `LedgerId`; expose results only to evaluation/reporting. Active shadow count is checked transactionally. A forward run stores the exact code/config/rules/model/feature/schema/license digests. It may be inserted as `burn_in` only for a fully verified current digest set or as `burn_in_pending` only for a champion awaiting handshake; permitted transitions are `burn_in_pending -> burn_in -> forward_active -> closed` and `burn_in -> forward_active -> closed`. Immutable constraints reject decisions whose run/artifact digests do not match.

Extend the authenticated admin protocol with:

```text
trenchd shadow register --socket PATH --kind rules|ml --artifact-manifest PATH
trenchd shadow list --socket PATH
trenchd shadow pause --socket PATH --run-id ID --reason TEXT
trenchd release prepare --socket PATH --release-manifest PATH
trenchd run rotate --socket PATH --run-manifest PATH --reason initial|release_change|rules_change [--prepare-id ID]
trenchd champion activate --socket PATH --candidate-pointer PATH --run-manifest PATH
trenchd forward activate --socket PATH --run-manifest PATH --burn-in-report PATH
```

`release prepare` verifies the candidate release manifest/digests through an explicit immutable path, immediately blocks new entries while preserving normal and mandatory exit handling, and returns non-ready until both visible ledgers are flat/reconciled with no pending or unresolved order. It then records a durable `release_pending` ID and candidate digest in one writer transaction. Repeated calls for the same digest are idempotent; a different candidate is rejected until the pending change is explicitly abandoned before any file activation. The entry block survives restart, eliminating the status-check/activation race.

`run rotate` is the generic writer-owned boundary for an initial run or a code/config/rules release change. Startup compares the running release and rules digests with the last run and blocks all new decisions on mismatch. For `release_change|rules_change`, it also requires a matching durable `release_pending` ID created before file activation. At a completed boundary, rotation requires flat/reconciled visible ledgers and no pending/unresolved order, closes the prior run if present, creates fresh 100-USDC ledgers, binds a new `burn_in` run to every current digest, consumes the pending release record when applicable, and only then arms the next decision boundary. It is idempotent by proposed run ID and cannot reuse outcomes. Release tooling may switch files and restart exact services, but cannot bypass this database transition.

`champion activate` is the only code path that may replace live `champion.json`. At the next completed boundary it pauses all visible-strategy decision scheduling, drains inference, and requires both visible ledgers to be flat/reconciled with no pending or unresolved order. In one writer transaction it closes any prior evidence run with reason `champion_change`, creates a fresh 100-USDC-per-ledger `burn_in_pending` run bound to the candidate and current release/rules digests, and records a pending activation before atomically writing/fsyncing the champion pointer. It then remains ML-unready and returns `pending_worker_restart`; it never records a prediction yet. If pointer publication fails, the durable pending state is retryable but the closed run never resumes.

After the operator restarts only the worker, the exact startup handshake completes the pending activation: one writer transaction records the new champion, changes that same run to `burn_in`, and arms scheduling for the next bar boundary. A wrong/old worker leaves the run pending and all visible decisions paused. Startup reconciliation resumes or safely fails this state after a crash based on the database, pointer, and worker digests; there is no rollback to the closed evidence run. Thus the new run ID/digest binding is durable before inference resumes. `forward activate` can later change only this same run from `burn_in` to `forward_active` after its matching 24-hour report.

For ML registration, the writer first inserts the verified candidate as `pending`; the daemon computes the desired set of current active shadows plus that candidate and calls transactional `sync_shadows`. On success the writer activates it at the next bar boundary; on failure it marks the candidate failed and re-syncs the prior active set. Pausing/removing a shadow changes database state first, inference immediately omits it, and a full-set sync unloads it. Because sync replaces the complete set, a stale loaded digest can never permanently consume a slot; periodic/startup reconciliation compares database desired state with `list_artifacts` until equal. For rules, the daemon verifies the frozen rules artifact locally without worker state.

At each completed bar the daemon schedules visible champion plus every active ML shadow in one bounded inference batch and computes active rules shadows locally; it journals each decision before routing it through that shadow's isolated engine. Worker/candidate failure pauses only that shadow. Startup completes database/worker set reconciliation before scheduling.

- [ ] **Step 5: Run persistence/isolation tests**

Run: `cargo test -p trench-storage strategy::tests && cargo test -p trench-core shadow::tests && cargo test -p trenchd admin::shadow_tests && cargo test -p trenchd admin::release_prepare_tests && cargo test -p trenchd admin::run_rotation_tests && cargo test -p trenchd admin::champion_activation_tests`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/trench-storage crates/trench-core crates/trenchd
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
trench-ml config check --config ...
trench-ml train --config ... --sleeve 15m|1h
trench-ml evaluate --kind rules|ml --artifact ... --manifest ...
trench-ml promote --artifact ... --report ... --target ...
trench-ml serve --config ...
```

Rust admin CLI tests from Task 11 cover shadow registration and forward activation. Here assert nonzero exit for insufficient history, incomplete folds, failed common/rules/ML gates, research-only license, stale report/artifact mismatch, existing target, and non-atomic target filesystem. Assert `promote` requires an explicit report path and never runs from `evaluate`.

- [ ] **Step 2: Verify failure**

Run: `cd ml && uv run pytest tests/test_cli.py -q`

Expected: FAIL because CLI entry points are absent.

- [ ] **Step 3: Implement orchestration without hidden state**

Each command resolves one strict config, prints a concise structured summary, writes a manifest/report atomically, and exits. `evaluate` accepts the frozen rules artifact/trade streams or an ML artifact/forecast streams and applies the correct typed gate table. `promote` verifies absolute/paired requirements, artifact/report/data/code/license digests, then installs into a new immutable staging directory and emits an approved release pointer for an explicit operator activation; it never edits a model or running strategy. LightGBM v1 may use the provisional no-incumbent path only after offline eligibility and must be labeled provisional until 90 days/100 forward trades pass. Rules replacements always require the same absolute and paired forward evidence before an approved release is emitted.

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

Phase 2 is complete when both visible ledgers replay independently through the shared sealed-quote risk/broker, Python and Rust agree on a frozen ML artifact, the frozen rules artifact has passed its separate generic robustness report, late/bad inference pauses only ML, all research outputs are point-in-time and reproducible, active rules/ML shadows are automatically scheduled and recovered, and shadow/promotion/license gates fail closed. Real forward promotion remains impossible until the untouched 90-day/100-trade evidence exists; that waiting period is an experimental requirement, not unfinished software.
