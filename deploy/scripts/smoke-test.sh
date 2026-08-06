#!/usr/bin/env bash
# Non-destructive post-activation smoke checks. This script observes only the
# Trench units and loopback endpoints; it never restarts a workload or mutates
# SQLite/Parquet data.

set -euo pipefail

config=/etc/trenchbot/paper.toml
socket=/run/trenchbot/admin.sock
json_only=0
while (($# > 0)); do
    case "$1" in
        --config)
            (($# >= 2)) || { printf '%s\n' '--config needs a path' >&2; exit 2; }
            config=$2
            shift
            ;;
        --socket)
            (($# >= 2)) || { printf '%s\n' '--socket needs a path' >&2; exit 2; }
            socket=$2
            shift
            ;;
        --json) json_only=1 ;;
        --help)
            printf 'usage: %s [--config ABSOLUTE_PATH] [--socket ABSOLUTE_PATH] [--json]\n' "$0"
            exit 0
            ;;
        *)
            printf 'unknown argument: %s\n' "$1" >&2
            exit 2
            ;;
    esac
    shift
done

checks=()
failures=()

json_escape() {
    local value=$1
    value=${value//\\/\\\\}
    value=${value//\"/\\\"}
    value=${value//$'\n'/\\n}
    value=${value//$'\r'/\\r}
    printf '%s' "$value"
}

record() {
    local name=$1
    local ok=$2
    local detail=$3
    local json_ok=false
    [[ "$ok" == 1 ]] && json_ok=true
    checks+=("{\"name\":\"$(json_escape "$name")\",\"ok\":$json_ok,\"detail\":\"$(json_escape "$detail")\"}")
    if [[ "$ok" != 1 ]]; then
        failures+=("$name")
    fi
    if ((json_only == 0)); then
        printf ' %-28s %s (%s)\n' "$name" "$([[ "$ok" == 1 ]] && printf PASS || printf FAIL)" "$detail" >&2
    fi
}

if [[ "$config" == /* && -f "$config" ]]; then
    config_target=$(readlink -f -- "$config" 2>/dev/null || printf '')
    if [[ -n "$config_target" && -f "$config_target" && ! -L "$config_target" ]]; then
        record config 1 "$config_target"
    else
        record config 0 'config target is not a regular file'
    fi
else
    record config 0 'config must be an existing absolute file'
fi

if [[ "$socket" == /* && "$socket" == *.sock ]]; then
    record admin_socket 1 "$socket"
else
    record admin_socket 0 'admin socket must be an absolute .sock path'
fi

current=/opt/trenchbot/current
current_target=''
if [[ -L "$current" ]]; then
    current_target=$(readlink -f -- "$current" 2>/dev/null || printf '')
fi
if [[ -n "$current_target" && -d "$current_target" && "$current_target" == /opt/trenchbot/releases/* ]]; then
    record current_release 1 "$current_target"
else
    record current_release 0 'current must resolve beneath /opt/trenchbot/releases'
fi

if command -v systemctl >/dev/null 2>&1 && systemctl is-active --quiet trenchd.service 2>/dev/null; then
    record trenchd_service 1 active
else
    record trenchd_service 0 inactive
fi

watchdog=''
if command -v systemctl >/dev/null 2>&1; then
    watchdog=$(systemctl show trenchd.service --property=WatchdogUSec --value 2>/dev/null || true)
fi
if [[ "$watchdog" =~ ^[0-9]+(us|ms|s|min|h|d)$ && "$watchdog" != 0us ]]; then
    record watchdog 1 "$watchdog"
else
    record watchdog 0 "WatchdogSec unavailable ($watchdog)"
fi

live_status=none
ready_status=none
if command -v curl >/dev/null 2>&1; then
    if live_status=$(curl --silent --show-error --output /dev/null --connect-timeout 2 --max-time 5 \
        --write-out '%{http_code}' http://127.0.0.1:9464/health/live 2>/dev/null); then
        :
    else
        live_status=none
    fi
    if ready_status=$(curl --silent --show-error --output /dev/null --connect-timeout 2 --max-time 5 \
        --write-out '%{http_code}' http://127.0.0.1:9464/health/ready 2>/dev/null); then
        :
    else
        ready_status=none
    fi
fi
[[ "$live_status" == 200 ]] && live_ok=1 || live_ok=0
[[ "$ready_status" == 200 || "$ready_status" == 503 ]] && ready_ok=1 || ready_ok=0
record health_live "$live_ok" "HTTP $live_status"
record health_ready "$ready_ok" "HTTP $ready_status (503 is an explicit entry blocker)"

listener_ok=0
listener=''
if command -v ss >/dev/null 2>&1; then
    listener=$(ss --listening --numeric --tcp 2>/dev/null | awk '$0 ~ /:9464([[:space:]]|$)/ {print}' || true)
    if [[ "$listener" =~ 127\.0\.0\.1:9464 ]]; then
        listener_ok=1
    fi
fi
record loopback_listener "$listener_ok" \
    "$([[ "$listener_ok" == 1 ]] && printf 127.0.0.1:9464 || printf 'missing or non-loopback listener')"

status_ok=0
status_detail='status request failed'
status_bin=/opt/trenchbot/current/bin/trenchd
if [[ -x "$status_bin" && -S "$socket" ]]; then
    status_json=$("$status_bin" status --socket "$socket" --json 2>/dev/null || true)
    if [[ "$status_json" == *'"ok":true'* && "$status_json" == *'"reconciled":true'* ]]; then
        status_ok=1
        status_detail='authority reconciled'
    fi
fi
record authority_status "$status_ok" "$status_detail"

failed_units=''
if command -v systemctl >/dev/null 2>&1; then
    failed_units=$(systemctl --failed --no-legend --plain 2>/dev/null || true)
fi
if [[ -z "$failed_units" ]]; then
    record failed_units 1 none
else
    record failed_units 0 'one or more systemd units are failed'
fi

timers_ok=0
if command -v systemctl >/dev/null 2>&1 \
    && systemctl is-active --quiet trench-backup.timer 2>/dev/null \
    && systemctl is-active --quiet trench-retention.timer 2>/dev/null; then
    timers_ok=1
fi
record maintenance_timers "$timers_ok" \
    "$([[ "$timers_ok" == 1 ]] && printf active || printf 'backup/retention timer not active')"

paths_ok=1
for path in /var/lib/trenchbot /var/lib/trenchbot/sqlite /var/lib/trenchbot/parquet /var/backups/trenchbot /run/trenchbot; do
    if [[ ! -d "$path" ]]; then
        paths_ok=0
        continue
    fi
    mode=$(stat -c '%a' -- "$path" 2>/dev/null || printf invalid)
    [[ "$mode" == 700 ]] || paths_ok=0
done
record private_paths "$paths_ok" \
    "$([[ "$paths_ok" == 1 ]] && printf '0700 directories' || printf 'missing or mode is not 0700')"

if ((${#failures[@]} == 0)); then
    overall=true
else
    overall=false
fi

if ((json_only == 0)); then
    if [[ "$overall" == true ]]; then
        printf '\nsmoke: PASS\n' >&2
    else
        printf '\nsmoke: FAIL (%s)\n' "${failures[*]}" >&2
    fi
fi

printf '{"schema_version":1,"ok":%s,"checks":[%s],"failed_checks":[' "$overall" "$(IFS=,; printf '%s' "${checks[*]}")"
for index in "${!failures[@]}"; do
    ((index > 0)) && printf ','
    printf '"%s"' "$(json_escape "${failures[index]}")"
done
printf ']}\n'

[[ "$overall" == true ]]
