#!/usr/bin/env bats

setup() {
    ROOT="$BATS_TEST_DIRNAME/../.."
    SCRIPT="$ROOT/deploy/scripts/verify-host.sh"
}

@test "verify-host is read-only and has a JSON mode" {
    run rg -q -- '--json' "$SCRIPT"
    [ "$status" -eq 0 ]
    ! rg -n '(^|[[:space:]])(apt|dnf|yum|systemctl[[:space:]]+(start|stop|restart)|netplan[[:space:]]+(apply|try)|rm[[:space:]])' "$SCRIPT"
}

@test "redacted fixtures contain no host identity or credentials" {
    run rg -n -i 'hostname|address|password|credential|account_action|messaging' "$ROOT/deploy/tests/fixtures"
    [ "$status" -eq 1 ]
}

@test "network failure fixture requires correction, not suppression" {
    run rg -q 'configuring' "$ROOT/deploy/tests/fixtures/network-wait-online-failure.json"
    [ "$status" -eq 0 ]
    run rg -q 'configured and wait-online is active' "$ROOT/deploy/tests/fixtures/network-wait-online-failure.json"
    [ "$status" -eq 0 ]
    ! rg -i 'mask|disable|optional|replace' "$ROOT/deploy/tests/fixtures/network-wait-online-failure.json"
}
