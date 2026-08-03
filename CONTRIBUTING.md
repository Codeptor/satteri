# Contributing

Satteri is under active construction. Keep changes scoped, deterministic, and
consistent with the approved design documents linked from the README.

## Development gates

The repository pins its Rust toolchain. Run all Rust gates before submitting a
change:

```sh
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

Python ML tooling is not present yet. Once a Python workspace and `uv.lock`
exist, Python changes must use `uv` and pass:

```sh
uv sync --frozen --all-groups
uv run ruff format --check .
uv run ruff check .
uv run pyright
uv run pytest
```

Run any narrower tests relevant to the change while developing, then run the
full applicable gates before committing.

## Paper-only boundary

Contributions must not add wallets, signers, account credentials, private
keys, deposits, live order submission, `/exchange` actions, or Telegram
integration. All positions, margin, fills, and equity remain simulated. Domain
logic must use explicit event/as-of timestamps rather than reading wall-clock
time implicitly.

## Repository hygiene

- Use concise conventional commits such as `feat(core): add risk limit`,
  `fix(storage): preserve replay order`, or `docs: clarify paper boundary`.
- Add or update tests with behavior changes.
- Do not commit secrets, `.env` files, credentials, personal paths, raw market
  captures, databases, Parquet data, model weights, generated artifacts, or
  local tool state.
- Keep dependency and generated lockfile changes intentional and reviewable.

By contributing, you agree that your contribution is licensed under the
project's `MIT OR Apache-2.0` terms.
