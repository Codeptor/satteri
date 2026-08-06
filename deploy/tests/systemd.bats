#!/usr/bin/env bats

setup() {
    ROOT="$BATS_TEST_DIRNAME/../.."
}

@test "paper config is strict and collect-only" {
    run rg '^mode = "collect_only"$' "$ROOT/deploy/config/paper.toml"
    [ "$status" -eq 0 ]
    ! rg -i 'EnvironmentFile|account_action|messaging' "$ROOT/deploy/config/paper.toml"
}

@test "service units use the dedicated account and slice" {
    for unit in trenchd.service trench-backup.service trench-retention.service; do
        run rg -q '^User=trenchbot$' "$ROOT/deploy/systemd/$unit"
        [ "$status" -eq 0 ]
        run rg -q '^Group=trenchbot$' "$ROOT/deploy/systemd/$unit"
        [ "$status" -eq 0 ]
        run rg -q '^Slice=trenchbot.slice$' "$ROOT/deploy/systemd/$unit"
        [ "$status" -eq 0 ]
        ! rg -i 'EnvironmentFile|docker|account_action|messaging' "$ROOT/deploy/systemd/$unit"
    done
}

@test "units expose only the explicit writable paths" {
    run rg -q '^ReadWritePaths=/var/lib/trenchbot /var/backups/trenchbot /run/trenchbot$' "$ROOT/deploy/systemd/trenchd.service"
    [ "$status" -eq 0 ]
    run rg -q '^ReadWritePaths=/var/backups/trenchbot /run/trenchbot$' "$ROOT/deploy/systemd/trench-backup.service"
    [ "$status" -eq 0 ]
}

@test "sysusers and tmpfiles are private" {
    run rg -q '^u trenchbot .* /usr/sbin/nologin$' "$ROOT/deploy/sysusers.d/trenchbot.conf"
    [ "$status" -eq 0 ]
    run rg -q '^d /run/trenchbot 0700 trenchbot trenchbot -$' "$ROOT/deploy/tmpfiles.d/trenchbot.conf"
    [ "$status" -eq 0 ]
}
