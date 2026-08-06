# Trench paper trading workspace

## Crate boundaries

- `crates/trench-core`: deterministic domain, strategy, risk, broker, ledger (isolated margin, synthetic 100 USDC). No wall-clock; explicit UTC event/as-of + frozen config.
- `crates/trench-hyperliquid`: read-only Hyperliquid adapters. Only `https://api.hyperliquid.xyz/info` and `wss://api.hyperliquid.xyz/ws` — enforced by `scripts/check-paper-boundary.sh`.
- `crates/trench-storage`: SQLite + Parquet persistence and deterministic replay. No direct writes outside `trenchd`.
- `crates/trenchd`: async orchestration and **sole SQLite writer**. Owns `trenchd::app` authority loop (`EngineWriter` admission → pure engine → atomic append).
- `ml/`: offline Python training/inference (`trench_ml`, `src/trench_ml/`). Never owns live state, never bypasses Rust risk/broker.
- `web/satta/`: private read-only dashboard (Next 16.2.6). Scaffold-only; no wallet/order/exchange/Telegram. Reads daemon Unix socket server-side, never browser→socket.
- `deploy/`: paper-only host boundary (systemd, `paper.toml` `collect_only`, preflight/smoke). No ML worker.

## Invariants (do not violate)

- Paper-only: no wallets, signers, private keys, mnemonics, account secrets, live order submission, `/exchange` actions, Telegram (`teloxide`/`telethon`/`gramjs` etc.), or secret-bearing fields. Boundary enforced by `bash scripts/check-paper-boundary.sh` — run before submitting Rust changes.
- `trenchd` is the only writer to `state/trench.sqlite` (`storage.sqlite_path`). Others use bounded interfaces.
- Domain transitions use explicit UTC times + frozen config. No `Instant::now`/`Utc::now` in core logic.
- Isolated margin only; ledgers `rules_only` and `ml_champion` are independently accounted.

## Commands

Toolchain pinned in `rust-toolchain.toml` (`1.93.0`, minimal + `clippy`/`rustfmt`). Do not override.

```sh
# Rust gates (workspace root) — run before every commit
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
# focused: cargo test -p trench-core event::tests
#          cargo test -p trench-core --test ml_ledger
#          cargo test -p trench-storage parquet

# Paper boundary (workspace root)
bash scripts/check-paper-boundary.sh

# Python ML (ml/) — Python 3.12, uv only (no pip/venv)
uv sync --frozen --all-groups
uv run ruff format --check .
uv run ruff check .
uv run pyright
uv run pytest
# focused: uv run pytest tests/test_schema.py -q -k test_name

# Dashboard (web/satta/) — npm (has package-lock.json), not pnpm/bun
npm run lint
npm run typecheck   # tsc --noEmit
npm run build
# shadcn: npx shadcn@latest add button  → components/ui/

# Deploy checks (workspace root)
./deploy/scripts/verify-host.sh --json
systemd-analyze verify deploy/systemd/*.service deploy/systemd/*.timer deploy/systemd/*.slice
# smoke (on target): /opt/trenchbot/current/deploy/scripts/smoke-test.sh --config /etc/trenchbot/paper.toml --socket /run/trenchbot/admin.sock --json [--require-health --require-maintenance]
```

Order matters: `fmt --check` → `clippy` → `test`. Run narrower tests while developing, full gates before commit.

## Runtime topology

- `trenchd::app` is the sole path from bounded replay/WebSocket facts through admission, engine, SQLite. `run`/`collect` are `collection_only` until typed market/recovery routing + frozen strategy artifact activate together.
- Public context capture: single-flight read-only worker on frozen universe cadence; must return complete normalized batch; persisted atomically before routing. Errors/cancellation/worker loss set global readiness blocker (cannot enable entries).
- Admin endpoint: Linux Unix socket only (`0700` dir, `0600` socket, daemon-UID/root peers), versioned status protocol, no mutable commands in phase 1.
- Startup: fresh journal replay of Task-14 source facts through authority path; never deserialize debug checkpoint into executable state (fails closed).
- Strategy readiness gates fresh entries only; mandatory-exit capability is separately reported from recovered executable-book state.

## Conventions & gotchas

- `rustfmt.toml`: `edition = "2024"`, `newline_style = "Unix"`, `use_field_init_shorthand`/`use_try_shorthand`. Release profile `lto = "fat"`, `codegen-units = 1`, `opt-level = 3`, `panic = "abort"`.
- Config: `config/paper.example.toml` is the checked-in default (`[risk] initial_equity_usdc = "100.00"`, `[rules] mode = "collect_only"`, isolated margin 5–20x). Do not edit deployed `/etc/trenchbot/paper.toml` in place; releases supply a verified content-addressed artifact.
- Next dashboard: breaking APIs vs training data — marker in `web/satta/AGENTS.md`; read `web/satta/node_modules/next/dist/docs/` before coding. Keep polling read-only, fail closed on stale status.
- Ignored artifacts (`.gitignore`): `/target/`, `/data/`, `/state/`, `*.sqlite*`, `ml/.venv/`, `/models/*`, `/reports/generated/` — never commit.
- Network `GETTING-STARTED.md` describes the separate `trench-perps-sdk` npm tgz (requires `ethers`, ESM, Node ≥18) — unrelated to this Rust/paper workspace.
- Commits: concise conventional (`feat(core):`, `fix(storage):`, `docs:`).

## Deployment fixture

`deploy/tests/fixtures/network-wait-online-failure.json` models a pending-route `systemd-networkd-wait-online` failure (link `configuring`, routes pending, DNS/TLS ok). If evidence in `docs/ops/network-preflight.md` matches, correct the protected netplan/cloud-init route declaration, `netplan generate && netplan try` (with rollback), verify `networkctl` → `configured` and `systemctl --failed` empty through reboot window. Do not mask/disable the wait unit, mark link optional, use `--any`, or install an override.

## References

- [Approved paper-trading design](docs/superpowers/specs/2026-08-03-trench-paper-trading-bot-design.md)
- [Rules platform plan](docs/superpowers/plans/2026-08-03-trench-rules-platform.md)
- [ML champion plan](docs/superpowers/plans/2026-08-03-trench-ml-champion.md)
- [VPS operations plan](docs/superpowers/plans/2026-08-03-trench-vps-operations.md)
- [Network preflight evidence](docs/ops/network-preflight.md) · [Dashboard contract](docs/dashboard/private-readonly-contract.md)
