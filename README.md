# Satteri

> **Status:** under active construction. The repository is not yet ready for
> running experiments or deployment.

Satteri is the implementation of the Trench paper-trading research design: a
paper-only, deterministic, dynamic-universe long/short bot for Hyperliquid
perpetual markets. It is built to make strategy, risk, execution, and recovery
decisions reproducible from explicit market events and frozen configuration.

Each visible ledger starts with **100 synthetic USDC** and simulates isolated
margin at **5–20x leverage**. Satteri has no wallet, deposit flow, signer,
private-key handling, live-order path, or Telegram integration. It cannot trade
real funds.

## Ledgers

The target system keeps two independently validated and independently
accounted paper ledgers:

- `rules_only` — the deterministic rules strategy currently being built.
- `ml_champion` — a future champion selected by offline Python training and
  evaluated through the same Rust paper broker and risk engine.

Neither ledger can net positions, margin, or risk against the other.

## Architecture

The Rust workspace owns the deterministic hot path:

- `trench-core` — domain, strategy, risk, broker, and ledger logic.
- `trench-hyperliquid` — read-only public market-data adapters.
- `trench-storage` — SQLite/Parquet persistence and deterministic replay.
- `trenchd` — async orchestration and the single SQLite writer.

Python ML tooling is planned for offline training and validation. It will not
own live state or bypass the Rust risk and paper-execution boundaries.

The approved [system design](docs/superpowers/specs/2026-08-03-trench-paper-trading-bot-design.md)
and phased plans for the [rules platform](docs/superpowers/plans/2026-08-03-trench-rules-platform.md),
[ML champion](docs/superpowers/plans/2026-08-03-trench-ml-champion.md), and
[VPS operations](docs/superpowers/plans/2026-08-03-trench-vps-operations.md)
describe the intended system in detail.

## Development

The pinned Rust toolchain is declared in `rust-toolchain.toml`. Current Rust
checks are:

```sh
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for contribution boundaries and the
future Python gates.

## License

Licensed under either the [Apache License, Version 2.0](LICENSE-APACHE) or the
[MIT License](LICENSE-MIT), at your option.
