#!/usr/bin/env bats

setup() {
    ROOT="$BATS_TEST_DIRNAME/../.."
    SCRIPT="$ROOT/deploy/scripts/smoke-test.sh"
}

@test "smoke test is observational" {
    ! rg -n '(^|[[:space:]])(systemctl[[:space:]]+(start|stop|restart)|rm[[:space:]]|sqlite3|parquet)' "$SCRIPT"
    run rg -q '/health/live' "$SCRIPT"
    [ "$status" -eq 0 ]
    run rg -q '127\\.0\\.0\\.1:9464' "$SCRIPT"
    [ "$status" -eq 0 ]
}

@test "smoke test rejects an absent activation" {
    run "$SCRIPT" --json --config /nonexistent/trench.toml
    [ "$status" -ne 0 ]
    [[ "$output" == *'"ok":false'* ]]
}
