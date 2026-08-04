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

## Runtime topology

- `trenchd::app` is the authority loop. It is the only path from bounded replay/WebSocket facts through `EngineWriter` admission, pure engine application, and atomic SQLite append. Task-15 wires source-clock persistence only: `run` and `collect` are explicitly `collection_only` and reject active rules until typed market/recovery routing and a frozen strategy artifact are activated together.
- Public context capture is a single-flight, read-only worker on the frozen universe cadence. It returns only complete normalized batches; the authority atomically persists each full batch before routing its facts. Capture errors, cancellation, or worker loss set the global context-capture readiness blocker and cannot enable entries.
- The admin endpoint is a Linux Unix socket only: private `0700` directory, `0600` socket, daemon-UID/root peers, bounded versioned status protocol, and no mutable commands in phase 1.
- Startup never deserializes a debug checkpoint into executable state. A fresh journal replays verified Task-14 source facts through the authority path. Any prior engine checkpoint fails closed until a complete deterministic state-restorer is available.
- Strategy readiness controls fresh entries only. Mandatory-exit capability is separately reported from recovered executable-book state and must not be weakened by strategy warmup/config failures.

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
