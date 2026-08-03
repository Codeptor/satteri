# Frozen `rules_only` defaults

Status: approved production baseline for paper research.

`rules_only` consumes only an immutable `FeatureSnapshot` and matching
`LongHorizonFeatureHistory`. It never receives a wallet, account, position
size, margin, leverage, PnL, order, or a directly supplied universe. The
snapshot's frozen universe digest is carried into every candidate.

The fixed ensemble is the six-family model in design section 7.2. Family
weights and regime definitions are not parameters. The selected default is:

- entry threshold: `0.60`;
- ATR floor: `1.50 * ATR(14)`;
- take-profit: `2R`.

Walk-forward research may select only the twelve combinations of threshold
`{0.55, 0.60, 0.65}`, ATR floor `{1.25, 1.50}`, and take-profit `{1.5R, 2R}`.

The frozen mathematical conventions are:

- `scaled_return_4 = clip(return_4 / (atrp14 * sqrt(4)), -1, 1)`;
- `scaled_return_16 = clip(return_16 / (atrp14 * sqrt(16)), -1, 1)`;
- nearest-rank percentile index is `ceil(p * n) - 1`, after validating
  `0 < p <= 1`;
- high volatility is `rv20 >= p80 && rv20 <= p95`; extreme volatility is
  `rv20 > p95`;
- `unit(x)` is `libm::tanh` over finite `f64`, converted back to `Decimal`
  with twelve-place half-even rounding.

Non-positive ATR or ATR-percent scales are typed, auditable no-signal
decisions. There is no zero-volatility division fallback. A candidate proceeds
only after the public risk quote is fresh, bound to the candidate, feasible,
and satisfies `gross_edge >= 1.5 * complete_cost_fraction`; equality accepts.
