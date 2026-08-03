# Trench paper trading workspace

## Crate boundaries

- `trench-core`: deterministic domain, strategy, risk, broker, and ledger logic.
- `trench-hyperliquid`: read-only public Hyperliquid REST, WebSocket, archive, and normalization adapters.
- `trench-storage`: SQLite and Parquet persistence and deterministic replay.
- `trenchd`: async orchestration and the sole SQLite writer.

## Invariants

- This is paper-only software. It must not contain wallets, signers, account configuration, private keys, live order submission, `/exchange` actions, Telegram, or secret fields.
- `trenchd` is the only SQLite writer; all other components communicate through bounded interfaces.
- Domain transitions use explicit UTC event/as-of times and frozen configuration. No core logic may read wall-clock time implicitly.
- Paper positions use isolated margin only. Initial ledger equity is synthetic USDC, not an account balance.

## Commands

```sh
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

## References

- [Approved paper-trading design](docs/superpowers/specs/2026-08-03-trench-paper-trading-bot-design.md)
- [Rules platform plan](docs/superpowers/plans/2026-08-03-trench-rules-platform.md)
- [ML champion plan](docs/superpowers/plans/2026-08-03-trench-ml-champion.md)
- [VPS operations plan](docs/superpowers/plans/2026-08-03-trench-vps-operations.md)

## Deployment fixture

The VPS preflight fixture includes an unresolved
`systemd-networkd-wait-online` failure caused by pending route configuration.
Correct the route configuration; do not suppress or replace the wait-online
unit.
