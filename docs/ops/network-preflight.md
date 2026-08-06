# Network preflight evidence

The VPS gate is deployment-time and read-only. Run
`deploy/scripts/verify-host.sh --json` over the approved SSH alias and retain
the redacted JSON result with the release record. Never check in a hostname,
address, hardware serial, route address, or journal payload.

## Required evidence

Capture only the state needed to explain a failed gate:

```sh
ssh trench-vps 'systemctl --failed --no-pager'
ssh trench-vps 'systemctl status systemd-networkd-wait-online.service --no-pager'
ssh trench-vps 'systemctl cat systemd-networkd-wait-online.service'
ssh trench-vps 'networkctl list --no-pager'
ssh trench-vps 'ip route'
ssh trench-vps 'resolvectl status'
```

The operator report records the affected link/unit, operational state, route
and DNS/NTP/TLS result, journal cause, acceptance test, and remediation owner.
It does not copy unrelated service configuration or credentials.

## Pending-route failure contract

`deploy/tests/fixtures/network-wait-online-failure.json` is a generic fixture,
not a claim about the target. It describes the case where the primary link is
routable and DNS/TLS work, but networkd remains `configuring`, generated
default routes remain pending, and `systemd-networkd-wait-online` fails.

When deployment evidence matches that shape, correct the exact protected
netplan/cloud-init route declaration that caused the pending state. Back up
that input, run `netplan generate`, use `netplan try` with automatic rollback,
then apply only after the candidate preserves verified IPv4 and IPv6
reachability. Confirm `networkctl` reports `configured`, rerun the wait-online
checks, and verify `systemctl --failed` stays empty through the agreed reboot
window.

Do not mask or disable wait-online, mark the link optional, use `--any`, clear
the failure without fixing its cause, or install an optimistic wait override.
