# Trench VPS Operations Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Package, harden, deploy, and operate the completed paper bot continuously on the measured shared VPS without exposing ports, secrets, or live-trading capability.

**Architecture:** CI produces a content-addressed static Rust binary plus a locked Python source bundle; a root-run installer prepares an immutable release before atomically activating it. Dedicated systemd units run as the unprivileged `trenchbot` account inside a bounded slice, while loopback health/metrics, journal alerts, online SQLite backups, and deterministic doctor/smoke commands provide operations evidence. Existing VPS containers and services are outside the deployment boundary.

**Tech Stack:** GitHub Actions, x86_64-unknown-linux-musl Rust release, uv/Python 3.12, systemd services/slice/timers, SQLite online backup, Prometheus text metrics over loopback, Bash with ShellCheck/Bats.

---

## Scope and prerequisites

Execute this after both the [rules platform](2026-08-03-trench-rules-platform.md) and [ML champion plan](2026-08-03-trench-ml-champion.md) pass; the ML unit may remain disabled until a valid champion is installed. The target is SSH alias `gifgoblin`, measured as Ubuntu 24.04, 6 shared EPYC vCPUs, about 12 GB RAM, and 174 GB free disk. Re-measure rather than trusting those numbers at deployment time.

Current read-only evidence narrows the network blocker: `eth0` is routable and netplan reports it online, IPv4 and IPv6 HTTPS work, but networkd keeps its administrative state at `configuring`; the generated default-route states remain `requesting/configuring`, both generated wait-online commands time out, and the unit stays failed. Root access is required to inspect the protected cloud-init netplan and networkd journal. The deployment must correct that pending route configuration until `eth0` becomes `configured`; it must not suppress the unit or replace it with an optimistic wait override.

Use `@devops-engineer`, `@deploy-checklist`, `@runbook`, `@api-security-best-practices`, and `@risk-management` during execution. Do not touch GIF Goblin, Discord, SearXNG, Caddy, Docker configuration, firewall rules unrelated to this service, or any Telegram material. The other administrator has root-equivalent access, so this host is suitable only because the paper deployment contains no confidential trading key or personal session.

## Target file map

```text
.github/workflows/ci.yml                       Rust/Python/static-boundary quality gate
.github/workflows/release.yml                  reproducible release bundle and provenance
Cargo.toml                                     health/metrics/systemd notification dependencies
Cargo.lock                                     pinned expanded Rust graph
crates/trenchd/src/health.rs                   loopback liveness/readiness/metrics server
crates/trenchd/src/systemd.rs                  watchdog/status notification
crates/trenchd/src/retention.rs                disk thresholds and bounded-book retention
crates/trenchd/src/admin.rs                    backup/retention admin protocol extensions
crates/trenchd/src/commands.rs                 status, backup, compact, smoke commands
crates/trench-storage/src/backup.rs            SQLite online backup and verification
deploy/config/paper.toml                       production paper config without secrets
deploy/config/ml.toml                          production ML worker config without secrets
deploy/systemd/trenchbot.slice                 shared-host CPU/memory/task budget
deploy/systemd/trenchd.service                 Rust authority service
deploy/systemd/trench-ml.service               optional frozen inference service
deploy/systemd/trench-backup.service           oneshot verified online backup
deploy/systemd/trench-backup.timer             daily backup schedule
deploy/systemd/trench-retention.service        oneshot retention enforcement
deploy/systemd/trench-retention.timer          hourly disk check
deploy/sysusers.d/trenchbot.conf               dedicated nologin account
deploy/tmpfiles.d/trenchbot.conf               explicit runtime/data directories and modes
deploy/scripts/build-bundle.sh                 deterministic local/CI bundle assembly
deploy/scripts/verify-host.sh                  read-only VPS prerequisite checks
deploy/scripts/install-release.sh              staged verified install and activation
deploy/scripts/install-model.sh                approved ML artifact install/activation
deploy/scripts/smoke-test.sh                   post-activation health/state checks
deploy/tests/build-bundle.bats                 bundle mutation/missing-file tests
deploy/tests/install-release.bats              path/digest/idempotency script tests
deploy/tests/install-model.bats                model/report/atomic-pointer tests
deploy/tests/smoke-test.bats                    scoped activation/failure fixtures
deploy/tests/systemd.bats                      unit hardening/static tests
deploy/tests/verify-host.bats                   prerequisite failure tests
docs/runbooks/trench-paper-operations.md        operator commands and incident actions
docs/ops/network-preflight.md                   evidence and resolution of current wait-online failure
```

## Deployment invariants

- No wallet, signer, exchange-action dependency, Telegram data, external webhook token, or generic secret environment file exists on the VPS.
- `/opt/trenchbot/releases/<release-digest>` is root-owned and immutable after activation; `/opt/trenchbot/current` is an atomic symlink.
- `/var/lib/trenchbot`, `/var/backups/trenchbot`, and `/run/trenchbot` are the only writable service paths and use `0700` directories/`0600` data files.
- Metrics and health bind to exactly `127.0.0.1:9464`; a port conflict is a hard preflight failure, never an automatic neighboring-port selection.
- Database migrations are forward-only. Never activate an older binary against a newer schema; retain the verified pre-migration online backup for explicit recovery.
- Systemd and journal visibility do not hide data from the co-administrator with root access. Live keys require a different host/spec.

### Task 1: Add loopback health, bounded metrics, and systemd watchdog

**Files:**
- Create: `crates/trenchd/src/health.rs`
- Create: `crates/trenchd/src/systemd.rs`
- Modify: `crates/trenchd/src/app.rs`
- Modify: `crates/trenchd/src/readiness.rs`
- Modify: `crates/trenchd/src/main.rs`
- Modify: `crates/trenchd/Cargo.toml`
- Modify: `Cargo.toml`
- Modify: `Cargo.lock`

- [ ] **Step 1: Write failing health-scope tests**

Test `/health/live` remains 200 while the process event loop is responsive, `/health/ready` is 503 for any global blocker, and ML-only failure returns global ready with `ml_champion=degraded`. Assert responses expose reason codes, timestamps, and version/config/schema digests but no environment, filesystem details, feature values, positions, or model paths.

- [ ] **Step 2: Write failing metric/watchdog tests**

Require low-cardinality metrics for process restart, WS state/gap, data age, decision latency, rejected/partial fills, breaker state, equity mismatch, ML deadline/drift, SQLite checkpoint, disk/memory pressure, NTP, and missed bars. Labels may be ledger/sleeve/market/reason from bounded enums; never use run/event IDs. Assert watchdog notification occurs only after startup reconciliation and once per half-watchdog interval.

- [ ] **Step 3: Verify failure**

Run: `cargo test -p trenchd health::tests && cargo test -p trenchd systemd::tests`

Expected: FAIL because health/systemd modules are absent.

- [ ] **Step 4: Implement the loopback server and notifier**

Add `axum`, `metrics`, `metrics-exporter-prometheus`, and `sd-notify` workspace dependencies. Bind a prevalidated `127.0.0.1:9464` listener only; reject unspecified/IPv6-any/public addresses in config. Serve only `GET /health/live`, `GET /health/ready`, and `GET /metrics`, with header/body/time limits. Send `READY=1`, bounded `STATUS=...`, `WATCHDOG=1`, and `STOPPING=1` messages from reconciled state.

- [ ] **Step 5: Run focused and workspace tests**

Run: `cargo test -p trenchd health::tests && cargo test -p trenchd systemd::tests && cargo clippy -p trenchd --all-targets -- -D warnings`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add Cargo.toml Cargo.lock crates/trenchd
git commit -m "feat(ops): expose private health and metrics"
```

### Task 2: Add verified online backup and retention controls

**Files:**
- Create: `crates/trench-storage/src/backup.rs`
- Modify: `crates/trench-storage/src/lib.rs`
- Create: `crates/trenchd/src/retention.rs`
- Modify: `crates/trenchd/src/admin.rs`
- Modify: `crates/trenchd/src/commands.rs`
- Modify: `crates/trenchd/src/app.rs`
- Test: storage/daemon unit tests

- [ ] **Step 1: Write failing backup tests**

Create a WAL database with active reads, request an online backup through the single writer, reopen the backup, run quick/integrity checks, and compare schema/run/checkpoint/equity digests. Inject interruption before atomic rename and assert the incomplete sibling is ignored. Reject a backup destination outside the configured explicit backup root.

- [ ] **Step 2: Write failing disk-threshold tests**

At below 65% usage, retain seven raw-book days. At 65-69.99%, delete only fully manifested raw L2/BBO partitions oldest-first while retaining compact trades/candles/features/ledgers and every finalized `LabelObservation` required by a data/run manifest. At 70% or more, block new entries and emit an active alert; never delete transactional data, durable labels, archive/data manifests, or an open-position dependency. Use an injected filesystem-usage provider and race it against an active Parquet writer in tests.

- [ ] **Step 3: Verify failure**

Run: `cargo test -p trench-storage backup::tests && cargo test -p trenchd retention::tests`

Expected: FAIL because backup/retention modules are absent.

- [ ] **Step 4: Implement explicit maintenance commands**

Extend the phase-1 authenticated Unix admin protocol and add client commands:

```text
trenchd status --config PATH --json
trenchd backup --config PATH --destination EXPLICIT_PATH
trenchd compact --config PATH
trenchd smoke --config PATH
```

`status`, `backup`, `compact`, and `smoke` are thin clients to the running daemon's versioned admin socket; they never open SQLite or mutate Parquet directly. The daemon handles each request inside its authority loop: backup coordinates with the sole writer, writes a temporary sibling, fsyncs, verifies, and renames; retention first fences/flushes the Parquet writer, resolves and validates each partition beneath the configured data root, journals removals through the SQLite writer, then un-fences. It never expands an unresolved variable/glob or recursively targets a broad directory. A down/unready daemon makes the timer command fail and alert rather than creating a second writer.

- [ ] **Step 5: Run maintenance tests**

Run: `cargo test -p trench-storage backup::tests && cargo test -p trenchd retention::tests && cargo test -p trenchd commands::tests`

Expected: PASS; destructive fixtures operate only inside `tempfile` roots.

- [ ] **Step 6: Commit**

```bash
git add crates/trench-storage crates/trenchd
git commit -m "feat(ops): add verified backups and retention"
```

### Task 3: Build reproducible CI and release bundles

**Files:**
- Create: `.github/workflows/ci.yml`
- Create: `.github/workflows/release.yml`
- Create: `deploy/scripts/build-bundle.sh`
- Create: `deploy/tests/build-bundle.bats`
- Test: workflow syntax and bundle manifest checks

- [ ] **Step 1: Write a failing bundle-verification test**

Run the script without required build outputs and assert nonzero exit with missing-file reason. Provide temporary Rust/Python/config/unit files, generate a bundle, alter one byte, and assert manifest verification fails.

- [ ] **Step 2: Implement the CI workflow**

On pull request and main push, run Rust format/clippy/tests, Python `uv sync --frozen --all-groups`, pytest/Ruff/Pyright, shellcheck/Bats, `systemd-analyze verify`, secret scan, license policy tests, and `scripts/check-paper-boundary.sh`. Cache only dependency downloads keyed by lockfiles; never cache artifacts/models/data.

- [ ] **Step 3: Implement release assembly**

For an explicit signed/annotated tag or manual workflow dispatch, build `trenchd` for `x86_64-unknown-linux-musl`, download a pinned x86_64 Linux `uv` binary and verify its published checksum, then package both binaries, `ml/src`, `ml/pyproject.toml`, `ml/uv.lock`, schemas, both production configs, the approved content-addressed rules artifact/report, systemd/sysusers/tmpfiles files, and scripts. Place `paper.toml`, its declared `rules-artifact.json`, and `rules-validation.json` together under `deploy/config/`; a forward-capable bundle is rejected unless both filenames/digests exactly match that pair. An explicitly labeled collector-only bundle may omit the pair but cannot start forward evidence. Exclude test fixtures, other reports, datasets, SDK archive, docs, Git metadata, and model binaries. Emit `release-manifest.json` with Git commit, tag, Rust/Python/uv/lock/schema/config/rules-artifact/report digests, per-file BLAKE3/SHA-256, and build timestamp; generate a provenance attestation and immutable artifact digest.

- [ ] **Step 4: Verify locally**

Run:

```bash
shellcheck deploy/scripts/build-bundle.sh
bats deploy/tests
./deploy/scripts/build-bundle.sh --verify-fixture
git diff --check
```

Expected: every command exits 0 and mutation detection is exercised.

- [ ] **Step 5: Commit**

```bash
git add .github deploy/scripts/build-bundle.sh deploy/tests
git commit -m "ci: build verified paper-bot releases"
```

### Task 4: Define the hardened systemd deployment

**Files:**
- Create: `deploy/config/paper.toml`
- Create: `deploy/config/ml.toml`
- Create: `deploy/systemd/trenchbot.slice`
- Create: `deploy/systemd/trenchd.service`
- Create: `deploy/systemd/trench-ml.service`
- Create: `deploy/systemd/trench-backup.service`
- Create: `deploy/systemd/trench-backup.timer`
- Create: `deploy/systemd/trench-retention.service`
- Create: `deploy/systemd/trench-retention.timer`
- Create: `deploy/sysusers.d/trenchbot.conf`
- Create: `deploy/tmpfiles.d/trenchbot.conf`
- Create: `deploy/tests/systemd.bats`

- [ ] **Step 1: Write failing unit validation checks**

Run `systemd-analyze verify` against the initial absent files and assert failure. Add a Bats static test requiring user/group `trenchbot`, nologin home, exact writable paths, exact `/etc/trenchbot/paper.toml` and `/etc/trenchbot/ml.toml` arguments, loopback endpoint, no `EnvironmentFile` containing secrets, no Docker, and no dependency on unrelated services. Parse both TOML files with production config types and reject unknown/default-placeholder paths.

- [ ] **Step 2: Create account/directories and slice policy**

`sysusers.d` creates a system `trenchbot` user/group with `/var/lib/trenchbot` and `/usr/sbin/nologin`. `tmpfiles.d` creates `/etc/trenchbot` as root:`trenchbot` `0750`, plus `/var/lib/trenchbot/{sqlite,parquet,models}`, `/var/backups/trenchbot`, and `/run/trenchbot` at `0700`. The slice uses `CPUQuota=300%`, `MemoryHigh=3G`, `MemoryMax=4G`, `TasksMax=512`, and accounting enabled, leaving the majority of the measured host memory plus three vCPUs available to other workloads.

`deploy/config/paper.toml` contains the exact production public REST/WS endpoints, `/var/lib` data paths, `/run/trenchbot/admin.sock`, `/run/trenchbot/ml.sock`, `127.0.0.1:9464`, frozen universe/risk/fee settings, `rules.mode=active`, sibling filenames `rules-artifact.json` and `rules-validation.json`, their release-supplied digests, and no independently tunable rule values or secret field. Both files are packaged beside `paper.toml`; `trenchd` resolves them relative to the canonical physical config target, so staged validation reads the staged pair and `/etc/trenchbot/paper.toml` reads the pair in the atomically selected release. `deploy/config/ml.toml` contains the Unix socket, content-addressed model root, worker limits/deadline, feature/config digests supplied at release time, and no database/exchange/credential field.

- [ ] **Step 3: Create hardened service units**

Both services use `User/Group=trenchbot`, `UMask=0077`, `NoNewPrivileges`, empty capability sets, `PrivateTmp`, `ProtectSystem=strict`, `ProtectHome`, kernel/control-group/device protections, `RestrictAddressFamilies=AF_UNIX AF_INET AF_INET6`, explicit `ReadWritePaths`, `LimitNOFILE=65536`, bounded restart delay, start/stop timeouts, and the slice. `trenchd` executes `/opt/trenchbot/current/bin/trenchd run --config /etc/trenchbot/paper.toml`, uses `Type=notify`, and has `WatchdogSec=30s`; `trench-ml` executes `/opt/trenchbot/current/ml/.venv/bin/trench-ml serve --config /etc/trenchbot/ml.toml`, uses `Type=simple`, and becomes ready only through the Rust handshake. ML has `ConditionPathExists=/var/lib/trenchbot/models/champion.json`; its absence is a valid ML-degraded state, not a restart loop.

Backup/retention units are oneshot admin clients: they execute the current `trenchd backup|compact --config /etc/trenchbot/paper.toml` commands, which contact `/run/trenchbot/admin.sock`; they never open storage directly. They run daily/hourly timers with randomized delay and persistent catch-up. No unit reads a private credential.

- [ ] **Step 4: Validate units and security score**

Run: `systemd-analyze verify deploy/systemd/*.service deploy/systemd/*.timer deploy/systemd/*.slice && systemd-analyze security --offline=yes deploy/systemd/trenchd.service`

Expected: verification exits 0; any security exposure above the documented outbound-network/filesystem needs is fixed or recorded with rationale.

- [ ] **Step 5: Commit**

```bash
git add deploy/config deploy/systemd deploy/sysusers.d deploy/tmpfiles.d
git commit -m "ops: define hardened paper-bot services"
```

### Task 5: Make VPS preflight deterministic and resolve wait-online

**Files:**
- Create: `deploy/scripts/verify-host.sh`
- Create: `deploy/tests/verify-host.bats`
- Create: `docs/ops/network-preflight.md`

- [ ] **Step 1: Write failing preflight fixtures**

Test rejection of wrong architecture/OS, less than 4 vCPUs, less than 8 GB RAM, less than 80 GB free ext4 disk, unsynchronized NTP, missing default route/DNS/TLS reachability, port 9464 already bound, failed system units, unsupported Python, and release/data paths on a network or `/mnt/c` filesystem. Test success with the measured-host fixture.

- [ ] **Step 2: Implement a read-only host verifier**

The script must use explicit commands/paths, output a compact JSON result plus human summary, and never install, stop, restart, or edit anything. It checks `uname`, `/etc/os-release`, `nproc`, `/proc/meminfo`, `findmnt`, `df`, `timedatectl`, `ip route`, `resolvectl`, TLS reachability to official Hyperliquid endpoints, `ss`, `systemctl --failed`, existing containers/resource usage, Python 3.12, `libgomp`, and current service/path collisions.

- [ ] **Step 3: Capture the live network failure evidence**

Run read-only:

```bash
ssh gifgoblin 'systemctl --failed --no-pager'
ssh gifgoblin 'systemctl status systemd-networkd-wait-online.service --no-pager'
ssh gifgoblin 'systemctl cat systemd-networkd-wait-online.service'
ssh gifgoblin 'networkctl list --no-pager'
ssh gifgoblin 'ip route'
ssh gifgoblin 'resolvectl status'
```

Record exact failed link/unit, boot impact, current route/DNS/NTP evidence, protected netplan inputs, networkd journal cause, and ownership in `docs/ops/network-preflight.md`; do not record credentials or unrelated service configuration. Confirm the already observed state: `eth0` is operationally routable/online while administratively `configuring`, and its generated default routes retain pending configuration flags.

- [ ] **Step 4: Resolve only the evidenced cause**

Using root plus provider-console access, back up the exact protected netplan file, inspect the pending IPv4/IPv6 default-route entries and networkd journal, and correct the erroneous cloud-init/netplan route declaration so `networkctl` reports `eth0` as `configured`. Validate with `netplan generate`, use `netplan try` with automatic rollback before `netplan apply`, and preserve both IPv4 and verified IPv6 reachability. Then rerun both generated wait-online commands, restart only the wait-online unit, and confirm `systemctl --failed` remains clean through an agreed reboot window. Do not mark `eth0` optional, disable wait-online, clear the failure without fixing it, or mask it with `--any`.

- [ ] **Step 5: Run local and remote preflight**

Run: `bats deploy/tests/verify-host.bats && ssh gifgoblin 'bash -s -- --json' < deploy/scripts/verify-host.sh`

Expected: fixture tests PASS and the live result is all green, including wait-online and port availability.

- [ ] **Step 6: Commit**

```bash
git add deploy/scripts/verify-host.sh deploy/tests/verify-host.bats docs/ops/network-preflight.md
git commit -m "ops: verify shared VPS prerequisites"
```

### Task 6: Implement safe staged release installation

**Files:**
- Create: `deploy/scripts/install-release.sh`
- Create: `deploy/tests/install-release.bats`

- [ ] **Step 1: Write failing path/digest tests**

Use temporary explicit roots to test bad/missing digest, traversal/symlink escape, wrong owner/mode, missing/invalid paper or ML config, rules-artifact/report mismatch, existing digest with different content, failed uv sync, bad binary doctor, schema incompatibility, absent/stale/wrong-digest `release_pending` ID, an active non-flat/unreconciled forward run, and interruption immediately before/after the single activation rename. Cover first install plus an upgrade where `current` still points to a different valid artifact/report pair: staged doctor must read only the staged config siblings. Assert no broad recursive deletion, the old `current` target remains active on pre-rename failure, both stable `/etc` config links always resolve through the same `current` target, and a repeated identical install is idempotent.

- [ ] **Step 2: Implement immutable preparation**

Implement `install-release.sh prepare --bundle ABSOLUTE_FILE --release-root /opt/trenchbot/releases --config-root /etc/trenchbot --data-root /var/lib/trenchbot`. Resolve/validate every target, verify manifest/provenance/file hashes including bundled `uv` and the rules artifact/report pair, create one same-filesystem temporary release directory with `mktemp -d`, install the binaries, run the bundled `uv sync --frozen --no-dev` inside staging, and place `paper.toml`, `rules-artifact.json`, and `rules-validation.json` together under staged `deploy/config/`. Invoke the staged binary as `STAGE/bin/trenchd doctor --config STAGE/deploy/config/paper.toml`; its canonical-config sibling rule must make it validate only those staged files, without a staging override or any `/opt/trenchbot/current` lookup. Run staged `trench-ml config check` likewise, require public/Unix/local paths and exact release/rules digests, set config/artifact/report ownership root:`trenchbot` mode `0640`, make all release files root-owned/read-only, and atomically rename the directory to `/opt/trenchbot/releases/<digest>`. Print that inert immutable path/digest; do not touch `current`. An identical prepare is idempotent.

- [ ] **Step 3: Implement explicit activation**

Implement `install-release.sh activate --release /opt/trenchbot/releases/<digest> --release-root /opt/trenchbot/releases --config-root /etc/trenchbot --data-root /var/lib/trenchbot --prepare-id ID`. Before an existing deployment is upgraded, invoke `trenchd release prepare` against that immutable release manifest; the old daemon durably blocks entries, reaches flat/reconciled state, and returns the ID. Activation re-verifies every release file and queries the old admin socket, refusing unless the ID is still pending and matches the exact release digest; request and verify an online backup before a schema change. For a first install only, replace `--prepare-id` with `--initial` and prove `current`, the database, and any prior run are absent. If `current` already resolves to the exact verified digest, return success before requiring either flag and change nothing.

Create `/etc/trenchbot/paper.toml -> /opt/trenchbot/current/deploy/config/paper.toml` and `/etc/trenchbot/ml.toml -> /opt/trenchbot/current/deploy/config/ml.toml` once; on later installs require those exact stable targets and never replace them. Create one new `current` symlink sibling and atomically rename only it to `/opt/trenchbot/current`. That single directory indirection switches the binary, Python environment, both configs, schemas, and packaged rules artifact/report as one filesystem operation. Never activate an older schema binary afterward. Retain the previous release and backup. The installer does not start/restart services or directly mutate SQLite; immutable preparation, admin quiescence, filesystem activation, exact service restart, and writer-owned `run rotate` remain separately journaled operations.

- [ ] **Step 4: Run script tests**

Run: `shellcheck deploy/scripts/install-release.sh && bats deploy/tests/install-release.bats`

Expected: PASS with failure injection leaving prior activation untouched.

- [ ] **Step 5: Commit**

```bash
git add deploy/scripts/install-release.sh deploy/tests/install-release.bats
git commit -m "ops: install verified immutable releases"
```

### Task 6A: Install an approved ML champion independently

**Files:**
- Create: `deploy/scripts/install-model.sh`
- Create: `deploy/tests/install-model.bats`

- [ ] **Step 1: Write failing artifact/report/activation tests**

Use explicit temporary roots and a tiny valid artifact to cover bad artifact/report/license/config/feature/schema/code digests, research-only license, failed promotion gates, path traversal or symlink escape, wrong ownership/mode, an existing digest with different content, incompatibility with the active release, and interruption immediately before/after candidate publication. Assert this script can never create or replace live `champion.json`, a failed install leaves prior immutable model state untouched, and an identical reinstall is idempotent.

- [ ] **Step 2: Implement verified staging**

The root-run script accepts exactly `--artifact ABSOLUTE_DIRECTORY --report ABSOLUTE_FILE --model-root /var/lib/trenchbot/models --release-root /opt/trenchbot/current`. Resolve every path, reject sources beneath the model target, verify the current release/config/schema digests, then invoke the current release's `trench-ml promote` as `trenchbot` with an argv array and a private same-filesystem staging target. That command must verify the artifact, license, offline/forward promotion report, data cutoff, and paired gates before producing an approved pointer. Reopen every copied file, verify its manifest, and make the content-addressed model directory read-only.

- [ ] **Step 3: Publish an inert candidate pointer**

Write canonical `candidate-<digest>.json` to a sibling temporary file containing the immutable artifact path plus artifact/report/code/config/feature/schema/license digests, fsync the file and model-root directory, then atomically rename only that candidate file. The script has no permission or code path to replace `champion.json`, mutate forward-run state, or restart a service. The authenticated writer-owned `trenchd champion activate` transition from the ML plan is the sole activation path.

- [ ] **Step 4: Run script tests**

Run: `shellcheck deploy/scripts/install-model.sh && bats deploy/tests/install-model.bats`

Expected: PASS with failure injection preserving the prior champion, publishing only an inert candidate, and using no secret or network dependency.

- [ ] **Step 5: Commit**

```bash
git add deploy/scripts/install-model.sh deploy/tests/install-model.bats
git commit -m "ops: install verified ML champions"
```

### Task 7: Activate services without disturbing neighboring workloads

**Files:**
- Create: `deploy/scripts/smoke-test.sh`
- Create: `deploy/tests/smoke-test.bats`
- Test: live host smoke evidence

- [ ] **Step 1: Write smoke-test failure fixtures**

Test nonzero exit for inactive/failed units, watchdog miss, non-loopback listener, readiness blocker, schema/config/run digest mismatch, a missing required run rotation, ledger not exactly 100 USDC on a fresh run, unexpected writable path, disk above threshold, and modification/restart of protected neighboring services.

- [ ] **Step 2: Snapshot neighboring state before activation**

Over SSH, record hashes/status/start timestamps for existing containers and GIF Goblin/Discord/SearXNG/Caddy services, current listeners, load, memory, disk, and failed units. Store only comparison digests/status in the deployment report, not unrelated logs/configuration.

- [ ] **Step 3: Install system definitions, release, and optional champion**

Run sysusers/tmpfiles, copy verified unit files, daemon-reload, and use `install-release.sh prepare` to publish an inert release. For an upgrade, call `trenchd release prepare` against that release manifest and wait for its durable ID; stop only `trench-ml.service`, then call `install-release.sh activate` with the ID. For a proven first install use its explicit `--initial` path. Start/restart only `trenchd.service` and start the backup/retention timers in decisions-paused/ML-degraded mode. If an approved replacement ML artifact is in scope, transfer only its artifact directory plus promotion report, run `install-model.sh`, inspect the inert candidate digest, and invoke `trenchd champion activate --socket /run/trenchbot/admin.sock --candidate-pointer ABSOLUTE_PATH --run-manifest ABSOLUTE_PATH`. Require the command to report a durable `pending_worker_restart` transition and a fresh run ID before starting only `trench-ml.service`; wait until status reports the exact champion digest and that run in `burn_in` before any ML inference boundary. If the existing champion remains compatible, start only `trench-ml.service`, require its exact current-release handshake, then invoke `trenchd run rotate --socket /run/trenchbot/admin.sock --run-manifest ABSOLUTE_PATH --reason initial|release_change|rules_change` with the pending prepare ID when applicable. With no champion installed, invoke the same rotation in explicit ML-degraded mode. Require the new `burn_in` run before decisions resume; otherwise leave decisions paused. Use exact unit names and never restart all services or Docker.

- [ ] **Step 4: Run post-activation smoke**

The script checks loopback endpoints, systemd status/watchdog, journal error codes, current-release digest, config/schema fingerprint, exact active-or-burn-in run/digest binding, SQLite integrity/reconciliation, event freshness, universe explanation, two ledger identities/equity, timers, resource slice, writable paths, listeners, and protected-neighbor before/after state.

Run: `ssh gifgoblin 'sudo /opt/trenchbot/current/deploy/scripts/smoke-test.sh --config /etc/trenchbot/paper.toml'`

Expected: PASS; only Trench units have new start timestamps and no public listener appears.

- [ ] **Step 5: Commit**

```bash
git add deploy/scripts/smoke-test.sh deploy/tests/smoke-test.bats
git commit -m "ops: add isolated activation smoke test"
```

### Task 8: Exercise recovery and alert contracts on the live paper service

**Files:**
- Modify: `deploy/scripts/smoke-test.sh`
- Create: `docs/runbooks/trench-paper-operations.md`

- [ ] **Step 1: Define non-destructive fault scenarios**

Use fixture/replay mode or service-specific controls to inject WS disconnect/gap, stale book, delayed/invalid ML response, SQLite writer pause, incomplete Parquet partition, disk-threshold provider, NTP-readiness failure, missed bar, partial exit, and process SIGTERM. Do not alter host clock, fill the real disk, corrupt the live database, or stop unrelated networking.

- [ ] **Step 2: Verify each readiness and alert transition**

For every scenario, assert the expected global/market/ledger scope, new-entry block, mandatory-exit availability, structured priority journal record, active bounded metric, persistence transition, recovery criteria, and no neighbor restart. A process kill at each ledger transition must reopen to the same reconciled digest.

- [ ] **Step 3: Verify backup recovery in an isolated directory**

Restore the latest online backup to a fresh explicit temporary data root, replay from its checkpoint, and compare ledger/event/config digests with live state at the same event boundary. Never overwrite the live database during this test.

- [ ] **Step 4: Write the operator runbook from observed commands**

Document status/readiness, journal filters, start/stop/restart of exact Trench units, release verification, backup/restore to a new root, disk retention, ML-degraded operation, unresolved exposure, breaker review, forward-evidence report, schema migration rule, and escalation. Explicitly state that journal/metric alerts are local signals and must be actively monitored over SSH until a separately approved external notification channel exists.

- [ ] **Step 5: Run final recovery smoke**

Run: `ssh gifgoblin 'sudo /opt/trenchbot/current/deploy/scripts/smoke-test.sh --fault-suite safe'`

Expected: every scoped fault is detected/recovered, backup replay matches, and protected services remain unchanged.

- [ ] **Step 6: Commit**

```bash
git add deploy/scripts/smoke-test.sh docs/runbooks/trench-paper-operations.md
git commit -m "docs: add paper bot operations runbook"
```

### Task 9: Start the untouched forward-paper experiment

**Files:**
- Modify: `docs/runbooks/trench-paper-operations.md`
- Create: `docs/ops/forward-run-template.md`

- [ ] **Step 1: Freeze the run manifest before outcomes**

Record release/config/schema/rules/model/data-cutoff/license digests, universe formula, fees, risk limits, latency policy, start UTC time, starting equity for both visible ledgers, active shadows, host-preflight digest, and declared promotion gates. Do not add a model artifact that lacks offline eligibility.

- [ ] **Step 2: Prove 24-hour operational stability before counting promotion time**

Require continuous normalized data, expected hourly universe snapshots, completed 15m/1h boundaries, zero unexplained gaps/state mismatches/restart loops, verified daily backup, bounded retention, and no neighbor impact. Any code/config/model change creates a new run ID and restarts untouched evidence.

- [ ] **Step 3: Begin the immutable evidence window**

Invoke the phase-2 Rust admin client explicitly:

```text
trenchd forward activate --socket /run/trenchbot/admin.sock --run-manifest ABSOLUTE_PATH --burn-in-report ABSOLUTE_PATH
```

The writer verifies the run is already the `burn_in` run created by the champion/release activation transition, that its code/config/rules/model digests match the manifest and worker handshake, and that the same-run 24-hour burn-in report passes before atomically marking it `forward_active`. A stale run ID or a report spanning an earlier champion/release is rejected. The daemon then schedules both visible strategies and all registered rules/ML shadows automatically; it does not auto-promote, relax minimum trades, backfill missed decisions, or count pre-registration outcomes.

- [ ] **Step 4: Validate daily/weekly operator checks**

Daily: readiness, gaps, reconciliation, backup, disk, NTP, breaker/exposure, missed boundaries, and neighbor resources. Weekly: cost decomposition, asset/month concentration, calibration/drift, ledger independence, shadow count, unresolved states, and manifest immutability. Record evidence digests rather than editing prior reports.

- [ ] **Step 5: Run the complete repository and host gate**

Run local Rust/Python/shell/systemd checks from the prior plans, verify a clean Git worktree except the two preserved user reference files, then rerun remote preflight and smoke. Expected: all pass; the service is collecting untouched evidence but makes no claim of alpha or promotion before 90 days/100 closed trades.

- [ ] **Step 6: Commit**

```bash
git add docs/runbooks/trench-paper-operations.md docs/ops/forward-run-template.md
git commit -m "docs: define untouched forward paper run"
```

## Phase-3 completion gate

Phase 3 is complete when the measured host passes preflight, the wait-online failure is genuinely resolved, verified immutable services run inside their slice, only loopback endpoints exist, backup/recovery and scoped fault tests pass, neighboring workloads are unchanged, and a frozen forward run is collecting evidence. This does not authorize live trading or claim that either strategy has alpha; those conclusions require the untouched promotion gates.
