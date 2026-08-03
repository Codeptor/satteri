# Trench Paper Trading Bot Design

**Date:** 2026-08-03
**Status:** Approved
**Project root:** repository root (`./`)

## 1. Decision summary

Build a paper-only, market-data-driven perpetual-futures research and execution simulator for Trench/Hyperliquid. It runs two independently validated strategies against separate synthetic accounts:

1. `rules_only`
2. `ml_champion`

Each ledger starts with exactly 100 synthetic USDC. Both receive the same point-in-time market events and the same paper-broker and risk rules, but they never share signals, positions, PnL, loss limits, or model output. The system may trade long or short across a dynamic universe that includes SOL whenever it passes the same liquidity rules as every other asset.

The first deployment uses public Hyperliquid mainnet data and cannot send an order. It has no wallet address requirement, signer, private key, builder-fee approval, deposit, faucet balance, or live executor. Adaptive isolated leverage from 5x through 20x is simulated and validated; it does not authorize live leverage.

Telegram is completely outside this project. There is no Telegram credential schema, session, channel identifier, ingestion code, OCR, media storage, ledger, placeholder, or extension hook.

## 2. Goals

- Produce deterministic, replayable paper execution from real public market data.
- Compare an interpretable multi-timeframe rules strategy with a separately validated ML strategy.
- Monitor every listed native perp cheaply, then concentrate detailed analysis on a dynamic deep-liquidity universe.
- Model fees, spread, book depth, slippage, latency, funding, partial fills, and liquidation before attributing alpha.
- Protect a 100 USDC account with hard, strategy-independent risk constraints.
- Support 15-minute and 1-hour decision sleeves without allowing simultaneous positions within one ledger.
- Make every signal, risk decision, fill, state transition, and model/configuration version auditable.
- Run continuously on the generic `trench-vps` deployment target without disrupting out-of-scope workloads.
- Keep the architecture ready for a separately approved live-execution project without putting live-only code or secrets in the paper deployment.

## 3. Non-goals

- Real-money or testnet order submission.
- Wallet onboarding, builder-fee approval, deposits, withdrawals, or a mock-USDC faucet.
- Telegram or any human-call ledger.
- Market making, sub-second/HFT execution, or running a Hyperliquid node.
- Spot trading, options, portfolio margin, cross margin, or multi-position portfolios.
- HIP-3/deployer perps in the first universe. Their venue-specific fees and historical coverage require separate validation.
- Online learning or self-modifying strategy parameters.
- Using an LLM to generate trades.
- Storing unbounded raw order-book data.
- PostgreSQL, Redis, Kafka, Kubernetes, or distributed writers.

The shared VPS deployment is acceptable only for paper trading because it holds no financial or personal-session secret. It is not an acceptable future home for a live trading key. Live activation requires a separate security design and an exclusive signer trust boundary.

## 4. Source constraints and SDK boundary

The supplied `@trench/perps-sdk` v0.1.0 package materials expose REST reads and order construction/submission but no WebSocket client. For reproducibility, this design freezes their 3 bps-per-filled-order builder-fee value as a paper-simulation assumption. It is not a claim about a publicly verifiable or current venue fee.

Therefore:

- Rust consumes Hyperliquid public REST and WebSocket APIs directly for research and paper trading.
- The Trench SDK is the eventual live-action adapter, not the market-data engine.
- A Bun/TypeScript `trench-executor` remains a future process boundary only. It is absent from the paper deployment and cannot be reached by the paper daemon.
- The paper build must not import a private-key signer, expose an `/exchange` action path, or accept wallet-related environment variables.
- Adding the live sidecar requires a new approved spec, successful paper-promotion gates, and a non-shared signer host.

## 5. System architecture

### 5.1 Runtime components

**Rust authority (`trenchd`)**

- Discovers markets and owns WebSocket/REST connections.
- Normalizes exchange events and maintains candles, books, trades, funding, and asset context.
- Computes common point-in-time features.
- Runs `rules_only`.
- Requests `ml_champion` forecasts at bar close.
- Arbitrates candidates, enforces all risk rules, and simulates execution.
- Owns both ledgers and is the only SQLite writer.
- Replays persisted market data deterministically.

**Python ML worker (`trench-ml`)**

- Trains, calibrates, evaluates, and explains candidate models offline.
- Serves only frozen, versioned champion inference at 15-minute or 1-hour bar close.
- Receives a versioned feature batch and returns forecasts; it cannot size positions or bypass risk.
- Has no exchange action client and no database write access.
- If unavailable or late, `ml_champion` skips that decision. `rules_only` continues unaffected.

**Offline research jobs**

- Run walk-forward training, robustness tests, SHAP, challenger evaluations, and report generation.
- May use temporary Lightning T4/L4 compute for licensed research-only foundation models.
- Automatically stop ephemeral compute after the job.
- Never alter the running champion directly; promotion installs a signed/fingerprinted frozen artifact through an explicit release step.

**Future Bun sidecar (`trench-executor`)**

- Encapsulates `@trench/perps-sdk` and all signing/submission behavior.
- Is documented only as a boundary. It is neither implemented nor deployed in the paper milestone.

### 5.2 Interfaces

Rust and Python communicate over a Unix-domain socket using MessagePack. Every envelope contains:

- `schema_version`
- `event_id`
- `event_time`
- `as_of_time`
- `producer_version`
- `run_id`
- `config_hash`
- payload type and payload

Requests have a strict deadline. Unknown schema versions, non-finite values, stale `as_of_time`, missing features, model/config mismatches, or duplicate response IDs fail closed. No TCP inference port is opened.

Strategies emit an `OrderIntent` containing direction, market, horizon, confidence/edge estimate, invalidation stop, and optional exit horizon. They do not emit quantity, margin, or leverage. Only the Rust risk engine can turn an intent into a paper order.

### 5.3 Readiness scope

Readiness is hierarchical rather than one global boolean:

- **Global:** NTP, SQLite/reconciliation, writable storage, market connection, current universe metadata, and fresh books. A global failure blocks both ledgers.
- **Market:** data quality and warmed common features for one coin. A market failure removes only that coin from candidate generation and blocks execution in it.
- **`rules_only`:** valid rules configuration and warmed sleeve features. A failure pauses only this ledger.
- **`ml_champion`:** all common readiness plus a matching model/config fingerprint, successful worker handshake, calibrated artifact, and an on-time forecast. A failure or deadline miss skips only that ML decision and reports overall service health as degraded.

An open position can always enter mandatory-exit handling when an executable book exists, even if its strategy is otherwise unready. Readiness state and reason are persisted at every transition.

## 6. Market data and dynamic universe

### 6.1 Feeds

Use public mainnet data without an account:

- `allMids` for cheap whole-universe monitoring.
- `metaAndAssetCtxs`/perp metadata for listing state, size precision, maximum leverage, day notional volume, open interest, funding, mark, oracle, and impact prices.
- `l2Book`, `bbo`, and `trades` for detailed execution and microstructure data.
- 15-minute and 1-hour candles, derived from normalized trades and reconciled against exchange candles.
- Historical L2 snapshots and asset contexts from the official requester-pays archive where needed, while treating missing/archive-lagged periods as unavailable rather than imputing them optimistically.

All internal timestamps are UTC. Exchange time is authoritative for event ordering; local receipt time is retained to measure latency. Duplicate trades use `(block_time, coin, tid)` as the identity. Candle identity is `(coin, interval, open_time)`.

### 6.2 Eligibility

The first version considers native perps only. Discovery runs hourly using only information available at that time. Structural eligibility is intentionally independent of strategy output and risk sizing. A market must satisfy every hard gate:

- not delisted or paused;
- live mid, mark, and metadata;
- exchange maximum leverage of at least 5x;
- at least 30 calendar days of usable local 15-minute history;
- at least 99.5% required-bar coverage over the trailing seven days;
- fresh detailed feed during the scoring window;
- trailing 24-hour notional volume of at least 5 million USDC;
- median effective spread no greater than 15 bps;
- executable depth inside 50 bps at least 100 times a fixed 500 USDC probe notional.

Eligible markets receive a robust liquidity score: 30% trailing notional-volume percentile, 20% open-interest percentile, 30% inverse effective-spread percentile, 15% depth percentile at 10/25/50 bps, and 5% feed continuity/freshness. Percentile values are computed cross-sectionally from the structurally eligible set and the formula is frozen for the run.

The tradeable universe is exactly ranks 1-20 at the latest completed hourly snapshot. Ranks 21-30 form a non-tradeable warm buffer that retains detailed subscriptions and features to make later entry deterministic. A hard-gate failure removes a market immediately from both sets; there is no delayed grace period. Hourly rank changes become tradeable at the next completed strategy bar. Trade-time cost and depth are then recomputed at the risk-sized notional; failing either check rejects that intent without changing universe rank.

Every universe snapshot and exclusion reason is stored. Backtests reconstruct the point-in-time universe; they may not use today's listings or liquidity to select historical assets. BTC, ETH, and SOL receive no special treatment.

### 6.3 Data quality behavior

- A WebSocket disconnect stops new entries immediately.
- Reconnect obtains a fresh snapshot, backfills recoverable gaps, and rebuilds indicators before readiness returns.
- Stale books, crossed books, non-monotonic timestamps, missing candles, or abnormal price jumps quarantine that market.
- No mid-price fallback is allowed for execution when a valid book is absent.
- Open paper positions remain recorded during a gap; stops are executed at the first subsequent executable price and the gap is flagged. The simulator never invents a favorable fill.

## 7. Decision model

### 7.1 Shared cadence and arbitration

Each strategy owns two sleeves:

- `15m`: evaluated on each completed 15-minute bar with a nominal one-hour prediction/holding horizon.
- `1h`: evaluated on each completed one-hour bar with a nominal four-hour prediction/holding horizon.

At a decision boundary, a strategy may produce candidates across all eligible markets and both sleeves. Candidates are ranked by conservative cost-adjusted edge. A ledger with no position may accept only the highest-ranked candidate that passes risk. A ledger with an open position rejects all new entries; only the owning sleeve's exit, stop, take-profit, time exit, or hard risk breaker may close it.

Features are computed once from immutable point-in-time snapshots and shared as inputs. Outputs are not shared: neither strategy can consume the other's signal, forecast, trade, or PnL.

### 7.2 `rules_only`

This is an auditable multi-timeframe ensemble. Unless a duration is explicit, windows are measured in the sleeve's completed bars. Define:

```text
atrp       = ATR(14) / close
robust_z   = clip((x - rolling_median(x, n)) / (1.4826 * rolling_MAD(x, n) + eps), -3, 3) / 3
unit(x)    = tanh(x)
imbalance  = (bid_notional - ask_notional) / (bid_notional + ask_notional)
```

Each family is clipped to `[-1, 1]`:

- **Trend:** equal-weight `unit((EMA8-EMA32)/ATR14)` and `unit(slope(EMA8,4)/ATR14)`, multiplied by `clip((ADX14-15)/20, 0, 1)`.
- **Momentum/breakout:** 35% volatility-scaled 4-bar return, 35% volatility-scaled 16-bar return, 20% `clip((close-Donchian20_mid)/(Donchian20_half_range+eps),-1,1)`, and 10% `robust_z(volume,20) * sign(return_4)`.
- **Mean reversion:** negative `robust_z(close-EMA20,20)`, used only in a range regime.
- **Microstructure:** equal-weight depth imbalance inside 10, 25, and 50 bps plus aggressive-buy/sell notional imbalance over the last 5 and 15 minutes, multiplied by `1 - clip(spread_bps/15,0,1)`.
- **Derivatives/crowding:** `0.50 * robust_z(premium,30d) + 0.30 * robust_z(OI_change_4,30d) * sign(return_4) - 0.20 * robust_z(funding,30d)`.
- **Cross-sectional:** equal-weight `2 * percentile_rank - 1` for liquidity-adjusted 4-bar and 16-bar return among the current point-in-time tradeable universe.

The regime is computed from completed 1-hour bars:

- trend when `ADX14 >= 25` and `abs(EMA8-EMA32)/ATR14 >= 0.35`;
- range when `ADX14 <= 20`;
- transition otherwise, with no new entry;
- extreme volatility when 20-bar realized volatility is above its trailing 90-day 95th percentile, with no new entry;
- high volatility from the 80th through 95th percentile, which adds 0.10 to the selected entry threshold.

Fixed regime weights are:

| Regime | Trend | Momentum | Mean reversion | Microstructure | Derivatives | Cross-sectional |
|---|---:|---:|---:|---:|---:|---:|
| Trend | 0.30 | 0.25 | 0.00 | 0.20 | 0.10 | 0.15 |
| Range | 0.00 | 0.10 | 0.35 | 0.25 | 0.20 | 0.10 |

An entry requires `abs(composite) >= threshold`, at least three active families with score magnitude at least 0.15 agreeing with its sign, and `abs(composite) * atrp * sqrt(4)` at least 1.5 times the risk engine's estimated round-trip cost fraction. Direction is the composite sign.

For a long, adverse-swing distance is entry minus the 10-bar low; for a short, it is the 10-bar high minus entry. The invalidation distance is `min(max(1.25 * ATR14, adverse_swing_distance), 2.5 * ATR14)`; if that swing is not on the adverse side it is `1.5 * ATR14`. Take-profit is 2R. Exit priority is risk breaker/liquidation prevention, stop, take-profit, opposite composite crossing 0.25, then the four-bar sleeve time limit.

Only three rule values are selectable in nested training: base threshold `{0.55, 0.60, 0.65}`, ATR floor `{1.25, 1.50}`, and take-profit `{1.5R, 2.0R}`. Regime definitions and family weights are not optimized. Selection maximizes median inner-fold net expectancy after the full paper broker, with lower turnover as the tie-breaker. The chosen configuration is frozen for the outer/forward window.

### 7.3 `ml_champion`

The production baseline is LightGBM, with an independent model artifact for each sleeve. This choice is deliberate: the 2026 BeyondArena study reports that tree-based and conventional deep models still dominate foundation models on non-IID/temporal tasks, which matches financial data better than random IID tabular benchmarks.

The declared ML feature set contains returns over 1/2/4/8/16/32 bars; EMA8/EMA32 ratio and slopes; RSI14; ADX14; ATR14; realized volatility over 8/20/64 bars; Donchian20 position; volume robust z-score; funding level and 30-day percentile; premium; open-interest changes over 1/4/16 bars; spread; depth and trade imbalances; short-horizon impact; cross-sectional 4/16/96-bar return ranks; breadth; and cyclic UTC hour/day encodings. It contains neither rules-family scores nor any outcome or statistic computed after `as_of_time`.

For each market/bar, `p0` is the BBO mid from the first valid book after bar close and `p1` is the first valid BBO mid at or after four sleeve bars. Samples with a data gap or non-tradeable universe state are absent, not imputed. Define:

```text
gross_return = ln(p1 / p0)
cost_probe   = point-in-time round-trip fees + funding + book impact for a fixed 100 USDC notional
class        = short if gross_return < -cost_probe
               flat  if abs(gross_return) <= cost_probe
               long  if gross_return > cost_probe
```

Each sleeve has a LightGBM Huber-regression head (`alpha=0.9`) for `gross_return` and a three-class head for direction with development-fold inverse-frequency class weights. The regression receives a one-sided 80% split-conformal residual bound from the chronological calibration window. Classifier raw scores use one-parameter temperature scaling fitted on that same 60-day calibration window. The artifact is invalid unless expected calibration error is at most 0.05 and calibrated multiclass Brier score is no worse than the raw model.

At inference, expected net edge uses the risk-sized paper-broker cost rather than the 100 USDC label probe. Entry requires regression/class directional agreement, calibrated probability at least 0.58, the one-sided 80% lower bound on directional net return above zero, and predicted gross movement at least 1.5 times complete expected cost. Stops and take-profit use the same explicit ATR/swing and 2R policy as `rules_only`; an opposite class meeting the same probability/edge gate exits early, otherwise the position times out after four sleeve bars.

The bounded LightGBM search uses `num_leaves {15,31}`, `learning_rate {0.03,0.05}`, `min_data_in_leaf {200,1000}`, `feature_fraction {0.7,1.0}`, and `lambda_l2 {1,10}`. Other settings are fixed: at most 2,000 trees, early stopping after 100 rounds, bagging fraction 0.8, bagging frequency 1, deterministic CPU mode, and declared seeds. Selection uses median inner-fold net expectancy after paper costs, then lower model complexity. Training is scheduled and offline; the running artifact is immutable.

Initial challenger registry:

- production-eligible after license verification: XGBoost, CatBoost, RealMLP, TabICLv2, and Nori-30M;
- research-only: TabPFN-3 and Google TabFM default weights.

TabPFN-3's 2026 license restricts its model and outputs to non-commercial, non-production use. Google TabFM's source is Apache-2.0 but its default pretrained weights are separately non-commercial/non-production. Their outputs cannot drive either paper ledger, train/distill the production champion, or be promoted without a suitable license. They may appear only in isolated offline benchmark reports. Nori-30M is presently Apache-2.0; every challenger license is rechecked and recorded by artifact digest before use.

## 8. Risk engine

Risk is authoritative, identical in logic, and independent in state for each ledger.

### 8.1 Ledger limits

- Initial equity: 100.00 synthetic USDC.
- Planned loss per trade: at most 0.5% of current equity.
- Daily realized-plus-marked loss breaker: 1.5% from UTC-day starting equity.
- Weekly breaker: 4% from Monday 00:00 UTC starting equity.
- Hard drawdown: 8% from the ledger high-water mark; latched until manual research review.
- Consecutive-loss cooldown: three losses trigger a 12-hour entry pause.
- Maximum entries: six per UTC day.
- Maximum open positions: one per ledger.
- No martingale, averaging down, pyramiding, or cross-ledger netting.

Global connection, clock, database, storage, or reconciliation failures block both ledgers. Market data-quality failures block that market. Rules/model/config failures block only their owning ledger as defined in Section 5.3. A risk rejection is persisted with machine-readable reasons.

Crossing the daily, weekly, or hard-drawdown breaker creates an immediate reduce-only paper close at the first executable book and blocks new entries. Daily and weekly breakers reset only at their UTC boundary after ledger reconciliation; the hard-drawdown breaker remains latched for manual research review.

### 8.2 Position sizing and leverage

The strategy supplies an expected entry and invalidation stop. Planned loss includes all costs, not only the stop distance. For proposed notional `n`, the risk engine evaluates:

```text
risk_budget = 0.005 * current_equity

planned_loss(n) = abs(entry_vwap(n) - stressed_stop_vwap(n)) * position_size(n)
                + entry_fee(n)
                + exit_fee(n)
                + funding_reserve(n, max_holding_time)
```

`entry_vwap` walks the current book. `stressed_stop_vwap` adjusts the absolute stop adversely by the worse of twice current spread/depth impact and the trailing 30-day 99th-percentile observed impact for that notional band. Funding reserve uses the worse absolute value of current funding and its trailing 30-day 99th percentile for every funding timestamp through the four-bar maximum hold. Gap loss can exceed planned loss in reality and is reported separately; it is never hidden by the sizing claim.

Because impact depends on size, notional is solved by deterministic bisection for the greatest venue-rounded `n` whose `planned_loss(n) <= risk_budget`. It is then reduced for available depth, asset limits, remaining daily/weekly/drawdown budget, and the 25% equity margin cap. The engine chooses the lowest integer leverage in `[5, 20]` for which `isolated_margin = notional / leverage + reserved_entry_cost` remains at or below 25% of current equity. It also requires:

- leverage no greater than the live asset/margin-tier maximum;
- estimated liquidation distance at least 2.5 times stop distance;
- executable stop and entry within the allowed slippage band;
- positive edge after the complete conservative cost estimate.

Size is rounded down to the venue precision. If rounding violates minimum tradable notional, materially changes planned risk, or produces zero size, the trade is skipped. If no leverage from 5x through 20x satisfies all constraints, the trade is skipped. Stops come from market invalidation/volatility, never from a desired leverage value.

Liquidation is calculated from the point-in-time Hyperliquid margin table, not from a fixed leverage approximation. Let `q` be absolute position size, `side` be `+1` for a long and `-1` for a short, `p_ref` be the current reference mark, and `equity_ref` be current isolated equity after booked fees, funding, and unrealized PnL at `p_ref`. For each candidate margin tier:

```text
maintenance_margin_ref = q * p_ref * maintenance_margin_rate - maintenance_deduction
margin_available_ref    = equity_ref - maintenance_margin_ref
liq_price               = p_ref - side * (margin_available_ref / q)
                               / (1 - maintenance_margin_rate * side)
```

This is the closed-form solution of `equity_ref + side * q * (liq_price - p_ref) = q * liq_price * maintenance_margin_rate - maintenance_deduction`; maintenance is evaluated once, at the reference mark, while the denominator accounts for its price slope. The solver accepts a candidate only when `q * liq_price` lies inside that tier; otherwise it evaluates the adjacent tier until it finds the unique piecewise-valid solution. Funding changes `equity_ref` before reevaluation. Unit tests compare the implementation with venue-reported liquidation examples and current metadata. The distance check uses mark-price liquidation because Hyperliquid uses mark price for margining.

Every accepted trade also records counterfactual margin, liquidation path, return on margin, and survivability at exactly 5x, 10x, 15x, and 20x. A level above the point-in-time asset maximum is recorded as infeasible rather than simulated as tradable. These counterfactuals never affect the primary ledger.

### 8.3 Margin mode

All simulated positions use isolated margin. Cross margin and portfolio margin are invalid configuration values. A future live adapter must issue `updateLeverage(..., mode: "isolated")` and reconcile the venue response before entry, but this action is outside the paper milestone.

## 9. Paper execution

### 9.1 Cost model

Primary results assume taker execution at the lowest Hyperliquid fee tier:

- Hyperliquid perp taker fee: 4.5 bps per side.
- Trench builder fee: 3.0 bps per filled order, frozen from the supplied `@trench/perps-sdk` v0.1.0 package materials as a paper-simulation assumption rather than a public venue-fee claim.
- Fixed fee subtotal: 7.5 bps per side, approximately 15 bps round trip before spread, depth slippage, latency, and funding.

The fee schedule and any asset-specific fee modifier are snapshotted with each run. Unknown fee state uses the more expensive plausible value or makes the asset ineligible. Discounts, staking, referrals, and maker rebates are not assumed.

### 9.2 Fill rules

- Entry and normal exit use simulated IOC marketable limits.
- An order reaches the first valid book received after the real signal/risk pipeline completes, naturally including observed paper-system latency.
- Size walks the visible book level by level. Fees apply only to filled notional.
- Unfilled entry quantity is canceled and never chased. A valid partial entry becomes the complete position and its stops/targets are resized to actual fill quantity; a below-minimum dust fill enters mandatory-exit handling.
- Stop/TP triggers use mark price, matching venue behavior, and fill from the first executable post-trigger book. A price gap fills at the worse executable price.
- Funding accrues at venue timestamps using the observed rate and position notional.
- Backtests sample from the measured deployment-latency distribution and add explicit 2x latency, 2x spread/slippage, and fee stress scenarios.
- A conservative maker counterfactual may be reported only when an order was resting before a later opposite-side trade and queue-ahead volume was exhausted. It is never mixed into primary taker results.

An exit is complete only when rounded position size is zero. Strategy reversals, take-profit, and time exits begin with a 50 bps IOC band and retry the remaining reduce-only quantity on every fresh book for five seconds. Any residual then becomes a mandatory exit. Stops and risk breakers are mandatory immediately. Mandatory exits:

1. start with `max(50 bps, 2 * current_spread_bps)`;
2. sweep all available depth inside the band;
3. persist the residual as `exit_pending` and retry on every fresh book;
4. widen by 25 bps after each unsuccessful attempt, capped at 200 bps;
5. block every new entry in that ledger until flat.

There is no timeout that declares an unfilled residual closed. If data ends while exposure remains, the run is marked unresolved and cannot pass validation. If mark price crosses the exact liquidation price first, the simulator follows venue behavior: for this sub-100k position it submits the whole remainder to the book; if equity then falls below two-thirds of maintenance margin with residual exposure, the remaining isolated position is backstop-liquidated and remaining maintenance margin is forfeited. Any such event fails promotion.

PnL is marked from executable bid/ask, not mid, and every report separates gross alpha, protocol fee, builder fee, spread, depth slippage, latency loss, funding, and liquidation loss.

## 10. Validation and promotion

Rules and ML are validated separately. Shared dates, data, and costs are permitted; shared signals, tuned parameters, or selection based on the other strategy's test result are not.

### 10.1 Walk-forward protocol

- Nested chronological walk-forward optimization only.
- Each outer fold uses 365 lookback days: 305 development days followed by a 60-day chronological calibration window, then a 30-day untouched test window. The entire fold advances by 30 days.
- Within the 305 development days, four expanding inner folds train on days 1-185, 1-215, 1-245, and 1-275 respectively and validate on each following 30-day block. The selected configuration is refit on all 305 development days; ML calibration alone uses the next 60 days.
- Purging removes samples whose four-bar label/holding horizon overlaps the next fold; a four-hour embargo is applied at every boundary.
- Universe membership, scalers, feature selection, hyperparameters, calibration, and thresholds are fit only within each training fold.
- At least three outer test folds and 100 aggregate closed trades are required before a version may enter forward paper evaluation. Test windows are concatenated once for evaluation and never reused for tuning.
- Delisted/new assets and missing-history periods remain point-in-time correct.
- Every experiment stores code commit, data manifest, universe snapshots, config hash, random seeds, model digest, and package lock.

Insufficient trustworthy history is a hard research failure, not permission to shorten a fold or fill gaps synthetically. The daemon may collect forward data while a strategy remains ineligible for promotion.

### 10.2 Robustness suite

- Stationary/block bootstrap confidence intervals preserving autocorrelation.
- Deflated Sharpe ratio and probability-of-backtest-overfitting analysis.
- Monte Carlo reorderings of trade blocks, slippage, latency, and missed-fill paths.
- Bull, bear, range, high-volatility, low-liquidity, funding-extreme, listing/delisting, and exchange-gap regimes.
- Base, 1.5x, 2x, and severe transaction-cost stress.
- Leave-one-asset and leave-one-regime-out concentration checks.
- Parameter perturbation and feature-family ablation.
- Calibration drift, feature drift, and prediction-decile monotonicity tests for ML.

### 10.3 Promotion gate

A frozen strategy/model version is eligible for champion status only after at least 90 calendar days of untouched forward paper operation and at least 100 closed trades. It must also satisfy all of the following:

- 95% stationary-block-bootstrap lower bound on mean net return per trade above zero;
- deflated Sharpe probability at least 0.95 and probability of backtest overfitting at most 0.20 across declared selections;
- positive net expectancy at 1.5x all costs and no hard breaker at 2x costs/latency;
- no single asset above 35% of positive PnL, no single month above 40%, and positive point expectancy after removing the best asset;
- for ML, expected calibration error at most 0.05, no two consecutive weekly windows above 0.08, and no population-stability index above 0.20 for more than 20% of features;
- zero simulated liquidations in the primary ledger;
- no 8% hard-drawdown breaker;
- survival under every declared regime and data-gap stress without state corruption;
- for a replacement version, a one-sided 95% block-bootstrap lower bound above zero on paired daily net-return difference versus the frozen incumbent, with no larger maximum drawdown.

Failure keeps the current frozen version or disables that ledger; it never auto-relaxes a gate. `rules_only` and `ml_champion` can both remain active because they are separate experiments, not champion/challenger aliases of one account.

### 10.4 Forward shadow lifecycle

The two user-visible ledgers remain exactly `rules_only` and `ml_champion`. Candidate versions use internal evaluation-only `shadow_run` state, never a third user strategy or an order source for the visible ledgers.

Before its first forward prediction, a candidate records immutable code, config, feature-schema, model, data-cutoff, and license digests. Its predictions must be timestamped and journaled before the corresponding outcome exists. Each shadow run receives the same subsequent market events and executes through an isolated copy of the same paper broker/risk engine with 100 synthetic USDC. It cannot influence universe selection, live risk state, or either visible ledger. At most three production-eligible shadows run concurrently; research-only TabPFN/TabFM outputs never enter this path.

LightGBM v1 bootstraps `ml_champion` as a clearly marked provisional version only after passing the offline outer-fold gate. Because no incumbent exists, its first promotion uses the absolute 90-day/100-trade criteria without the paired-improvement requirement. Every later ML or rules replacement requires both absolute criteria and paired improvement against the incumbent. Promotion is a manual release action after a report is approved; it never occurs automatically.

## 11. Persistence and deterministic recovery

### 11.1 Storage split

**SQLite WAL** stores transactional and low-rate state:

- run/config/model manifests;
- universe snapshots and exclusions;
- feature snapshot references;
- signals and component explanations;
- order intents and risk decisions;
- paper orders, fills, positions, funding, equity, and breakers;
- immutable challenger registrations and internal `shadow_run` states;
- health transitions and reconciliation checkpoints.

**Partitioned Parquet** stores high-rate analytical data:

- normalized trades;
- BBO/L2 snapshots with bounded retention;
- candles, asset contexts, funding, and feature matrices;
- backtest and robustness outputs.

The SQLite connection uses WAL, `synchronous=FULL`, foreign keys, parameterized statements, and explicit atomic transactions. Rust owns one bounded writer queue; Python and any CLI/dashboard are read-only or communicate through Rust. High-rate data never contends as row-by-row SQLite writes.

### 11.2 Filesystem and retention

- Linux ext4 only; no `/mnt/c` or network filesystem.
- Data directory mode `0700`; database/WAL/model artifacts mode `0600`.
- Parquet partitions are written to a temporary sibling and atomically renamed after validation.
- Raw detailed-book retention begins at seven days and is reduced automatically before disk usage exceeds 70%.
- Candles, feature snapshots, fills, and compacted ledgers are retained indefinitely within the paper experiment.
- Online SQLite backups protect against logical corruption. No external backup containing secrets is needed because paper mode has none; future backup policy is a separate deployment decision.

### 11.3 Startup sequence

1. Verify UTC/NTP synchronization and filesystem permissions.
2. Run SQLite quick/integrity checks and validate schema version.
3. Reconstruct both ledgers and compare positions, fills, equity, and breakers.
4. Validate Parquet manifests and ignore incomplete temporary partitions.
5. Reconnect, snapshot, and backfill recoverable gaps.
6. Warm all rolling features using only pre-decision history.
7. Attempt to load/fingerprint the frozen ML artifact and complete its worker handshake; failure leaves only `ml_champion` unready.
8. Enter global ready state when global invariants pass, then publish independent rules/ML readiness. Global-ready plus ML-degraded is a valid operating state.

There is no attempt to recreate a missed favorable paper order after downtime. Missed decisions are recorded as missed.

## 12. Deployment and operations

Measure the `trench-vps` target at deployment time and persist a redacted preflight fixture. It must provide Ubuntu 24.04 on x86_64, at least 4 vCPUs, 8 GB RAM, 80 GB free ext4 storage, synchronized NTP, working default-route/DNS/TLS checks, and enough unused capacity for the resource slice below. The paper hot path remains bounded by ephemeral foundation-model work and finite raw-book retention.

Deployment principles:

- dedicated unprivileged `trenchbot` service account;
- native systemd services and a `trenchbot.slice`, leaving capacity for out-of-scope workloads;
- no public application port; metrics/admin bind to loopback;
- explicit CPU/memory/file-descriptor limits and restart policy;
- structured `tracing` logs with no environment dump;
- pinned, reproducible dependencies and immutable release directory;
- global readiness requires database, NTP, market stream, data freshness, storage, and risk reconciliation; model handshake is an `ml_champion`-only readiness condition;
- deployment is blocked whenever `systemd-networkd-wait-online` fails; the evidenced route/configuration cause must be corrected rather than suppressed;
- out-of-scope workloads and host configuration are not modified by this project.

Required alerts include process restart loops, WS disconnect/gap, stale market, decision latency, rejected/partial fills, breaker changes, equity mismatch, model deadline/drift, SQLite checkpoint failure, disk above 70%, memory pressure, NTP loss, and missed bar boundaries.

## 13. Testing strategy

### Rust

- Unit tests for feature math, event ordering, universe gates, fee/funding/PnL math, isolated sizing, leverage selection, liquidation distance, breakers, and ledger independence.
- Property tests for conservation of cash/equity, no position above limits, no negative fill size, monotonic loss budgets, and deterministic replay.
- Golden recorded-stream replays with byte-stable ledger outcomes for a fixed build/config.
- Integration tests for snapshot/reconnect/backfill, duplicates, out-of-order events, partial fills, price gaps, SQLite restart, and Parquet crash recovery.
- Negative tests proving no paper binary can construct or send an exchange action.

### Python

- `pytest` tests for point-in-time feature joins, purging/embargo, fold isolation, calibration, artifact fingerprints, deterministic seeds, and promotion gates.
- Leakage sentinel features that must fail validation if accidentally admitted.
- Small fixture datasets for LightGBM inference parity between offline reports and the runtime worker.
- License-manifest tests preventing research-only model artifacts from entering the runtime registry.

### End-to-end

- Run both ledgers over the same captured stream and prove positions/PnL remain independent.
- Kill/restart Rust and Python at each state transition and verify fail-closed recovery.
- Inject stale books, delayed forecasts, corrupted partitions, high spread, fee jumps, and NTP loss.
- Verify every decision can be reconstructed from persisted source events, config, model/rules version, risk snapshot, and fill inputs.

## 14. Acceptance criteria for the paper milestone

- Continuous public mainnet market ingestion with automatic reconnect and explicit gap accounting.
- Hourly point-in-time universe selection with SOL treated like every other market.
- Independent `rules_only` and `ml_champion` ledgers, each starting at 100.00 USDC.
- 15-minute and 1-hour sleeves produce auditable signals without cross-strategy leakage.
- All orders pass the stated isolated 5x–20x risk engine and realistic paper broker.
- Deterministic replay reproduces signals, risk decisions, fills, and equity.
- Cost reports decompose fees, spread, slippage, latency, funding, and gross return.
- Restart/data/model failures block entries and preserve ledger integrity.
- No wallet or action endpoint exists in the running deployment.
- No Telegram-related code, data, configuration, dependency, or secret exists in the repository or VPS deployment.
- The system can collect the required 90-day/100-trade untouched forward evidence without manual intervention.

## 15. Alternatives rejected

- **Single blended strategy:** rejected because it hides whether rules or ML created alpha and contaminates comparison.
- **All-TypeScript runtime:** rejected because the SDK is useful for eventual execution, but Rust provides the authoritative long-running data/risk/replay core.
- **All-Python runtime:** rejected because Python remains ideal for research while Rust gives stronger operational and state invariants.
- **Foundation model as initial champion:** rejected because current 2026 evidence is strongest on IID tables, temporal evidence favors conventional models, licensing is uneven, and GPU cost is unnecessary.
- **PostgreSQL:** rejected because one VPS and one writer gain simplicity and deterministic local recovery from SQLite WAL; Parquet handles analytical volume.
- **Dedicated/VDS CPU:** not required by the baseline paper design; deployment proceeds only when the measured target passes preflight and slice-capacity checks.
- **Telegram integration:** rejected because personal messaging sessions are outside the paper-only boundary.

## 16. Primary references

### Trench and Hyperliquid

- Supplied `@trench/perps-sdk` v0.1.0 package materials (frozen paper-simulation input; not a public fee source)
- [Hyperliquid WebSocket API](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket)
- [Hyperliquid WebSocket subscriptions](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions)
- [Hyperliquid perpetual metadata](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint/perpetuals)
- [Hyperliquid fee schedule](https://hyperliquid.gitbook.io/hyperliquid-docs/trading/fees)
- [Hyperliquid liquidations](https://hyperliquid.gitbook.io/hyperliquid-docs/trading/liquidations)
- [Hyperliquid margin tiers](https://hyperliquid.gitbook.io/hyperliquid-docs/trading/margin-tiers)
- [Hyperliquid historical data](https://hyperliquid.gitbook.io/hyperliquid-docs/historical-data)

### 2026 tabular-model evidence

- [Beyond IID / BeyondArena](https://arxiv.org/abs/2606.30410)
- [TabPFN-3 technical report](https://arxiv.org/abs/2605.13986)
- [TabPFN-3 license](https://huggingface.co/Prior-Labs/tabpfn_3/blob/main/LICENSE)
- [Google Research TabFM announcement](https://www.research.google/blog/introducing-tabfm-a-zero-shot-foundation-model-for-tabular-data/)
- [Google TabFM repository and weight-license notice](https://github.com/google-research/tabfm)
- [TabICLv2 paper](https://arxiv.org/abs/2602.11139)
- [TabICLv2 repository](https://github.com/soda-inria/tabicl)
- [Nori-30M model card](https://huggingface.co/Synthefy/Nori-30M)
