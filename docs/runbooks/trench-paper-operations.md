# Trench paper operations runbook

This runbook operates a paper-only, public-data daemon. It contains no wallet,
signer, exchange-action path, personal session, Telegram integration, or
secret-bearing environment file. Journal and loopback metrics are local
signals; until an external notification channel is separately approved, an
operator must monitor them over the approved SSH path.

## Status and readiness

```sh
sudo systemctl status trenchd.service --no-pager
sudo systemctl show trenchd.service -p ActiveState -p SubState -p WatchdogUSec
sudo /opt/trenchbot/current/bin/trenchd status \
  --socket /run/trenchbot/admin.sock --json
sudo /opt/trenchbot/current/deploy/scripts/smoke-test.sh \
  --config /etc/trenchbot/paper.toml --json
```

`health/live` means the process event loop responds. `health/ready` is an
entry-readiness signal: a `503` is an explicit blocker, not evidence that a
mandatory exit may be abandoned. Inspect readiness reason codes and the
daemon's reconciled status before resuming any release activity.

## Journals and exact-unit actions

```sh
sudo journalctl -u trenchd.service -b --no-pager
sudo journalctl -u trenchd.service -p warning..alert -b --no-pager
sudo systemctl start trenchd.service
sudo systemctl stop trenchd.service
sudo systemctl restart trenchd.service
sudo systemctl start trench-backup.timer trench-retention.timer
```

Do not use `systemctl restart` on the container runtime, reverse proxy,
networking, or another tenant workload. A failed unit is investigated before
restarting the exact Trench unit.

## Release and backup boundaries

The release installer prepares and verifies an immutable directory beneath
`/opt/trenchbot/releases/<digest>` and changes only the `current` symlink at
activation. It does not start services or open SQLite. Confirm the selected
release without changing it:

```sh
readlink -f /opt/trenchbot/current
sudo find /opt/trenchbot/current -xdev -type f -printf '%m %u:%g %p\n'
sudo systemctl cat trenchd.service
```

Backup and retention timers are admin clients. The authority remains the sole
SQLite writer and must coordinate online backup/retention. If the installed
binary does not advertise the requested versioned admin command, the timer
must fail closed and alert; never run `sqlite3`, a second daemon, or a direct
Parquet deletion from a timer.

To test recovery, restore a verified backup into a new explicit temporary root,
run integrity/reconciliation checks there, and compare digests at the same
event boundary. Never overwrite the active database during a recovery test.

## Network, disk, and degraded operation

Run the read-only preflight before activation and whenever a host changes:

```sh
sudo /opt/trenchbot/current/deploy/scripts/verify-host.sh --json
```

At the disk threshold, leave entries blocked and inspect the retention alert;
the authority must fence/flush the Parquet writer before removing only fully
manifested raw-book partitions. Never remove a ledger, open-position
dependency, label observation, manifest, or transactional SQLite data.

Until an approved rules artifact is installed, this checkout's production
config is explicitly `rules.mode = "collect_only"`. The daemon may collect
public evidence and retain mandatory-exit capability after recovery, but it
must not be treated as forward-active. A configuration/rules change creates a
new immutable release and run manifest; it never edits prior evidence.

## Escalation and unresolved exposure

Escalate a failed wait-online, public listener, filesystem-boundary,
reconciliation, or watchdog check to the host owner with the compact JSON
result and exact journal reason. Do not suppress the check or make an
adjacent-port exception. If a host has an unresolved unrelated workload or
route failure, leave Trench stopped/entry-paused and preserve the evidence.
