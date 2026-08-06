# Paper deployment scaffold

This directory contains the paper-only host boundary: a strict collector
configuration, hardened systemd definitions, private directories, and
read-only preflight/smoke checks. It intentionally contains no ML worker or
dashboard and has no account/action capability.

The checked-in `paper.toml` is `rules.mode = "collect_only"`. A release may
become forward-capable only when the release builder supplies and verifies an
approved content-addressed rules artifact/report pair; no operator should edit
the file in place.

Before installing anything on a target, run:

```sh
./deploy/scripts/verify-host.sh --json
systemd-analyze verify deploy/systemd/*.service deploy/systemd/*.timer deploy/systemd/*.slice
```

The verifier is read-only. A failed wait-online check requires correcting the
evidenced network configuration, never masking or replacing the wait unit.

The post-activation smoke check is also read-only:

```sh
/opt/trenchbot/current/deploy/scripts/smoke-test.sh \
  --config /etc/trenchbot/paper.toml --socket /run/trenchbot/admin.sock --json
```

The checked-in daemon currently exposes readiness through the authenticated
Unix status socket. Loopback health/metrics and writer-owned backup/retention
commands are optional smoke gates until their corresponding daemon features
are installed. Require them explicitly when the release advertises them:

```sh
/opt/trenchbot/current/deploy/scripts/smoke-test.sh \
  --config /etc/trenchbot/paper.toml --socket /run/trenchbot/admin.sock \
  --require-health --require-maintenance --json
```

The `trench-backup` and `trench-retention` units are inert operational
scaffolding until those versioned admin commands exist in the installed
binary. Do not enable their timers as evidence of a completed backup policy;
the smoke check reports them only when `--require-maintenance` is requested.
