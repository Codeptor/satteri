#!/usr/bin/env bash
# Read-only VPS preflight. It never installs packages, edits network files, or
# starts/stops/restarts a service. Run it locally or over SSH as an observer.

set -euo pipefail

json_only=0
while (($# > 0)); do
    case "$1" in
        --json) json_only=1 ;;
        --help)
            printf 'usage: %s [--json]\n' "$0"
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

have_command() {
    command -v "$1" >/dev/null 2>&1
}

if ! have_command uname; then
    record architecture 0 'uname is unavailable'
else
    machine=$(uname -m 2>/dev/null || printf unknown)
    if [[ "$machine" == x86_64 ]]; then
        record architecture 1 "$machine"
    else
        record architecture 0 "$machine (requires x86_64)"
    fi
fi

os_id=unknown
os_version=unknown
if [[ -r /etc/os-release ]]; then
    # shellcheck disable=SC1091
    . /etc/os-release
    os_id=${ID:-unknown}
    os_version=${VERSION_ID:-unknown}
fi
if [[ "$os_id" == ubuntu && "$os_version" == 24.04 ]]; then
    record operating_system 1 "$os_id $os_version"
else
    record operating_system 0 "$os_id $os_version (requires Ubuntu 24.04)"
fi

if have_command nproc; then
    cpu_count=$(nproc --all 2>/dev/null || printf 0)
else
    cpu_count=0
fi
if [[ "$cpu_count" =~ ^[0-9]+$ && "$cpu_count" -ge 4 ]]; then
    record vcpu_count 1 "$cpu_count"
else
    record vcpu_count 0 "$cpu_count (requires at least 4)"
fi

memory_kib=0
if [[ -r /proc/meminfo ]]; then
    memory_kib=$(awk '/^MemTotal:/ {print $2; exit}' /proc/meminfo)
fi
if [[ "$memory_kib" =~ ^[0-9]+$ && "$memory_kib" -ge 8388608 ]]; then
    record memory 1 "${memory_kib} KiB"
else
    record memory 0 "${memory_kib:-0} KiB (requires at least 8388608 KiB)"
fi

filesystem_gate() {
    local path=$1
    local name=$2
    local mount_info=''
    local mount_type=''
    local mount_target=''
    if have_command findmnt; then
        mount_info=$(findmnt --noheadings --output FSTYPE,SOURCE,TARGET --target "$path" 2>/dev/null || true)
    fi
    mount_type=$(awk 'NR == 1 {print $1}' <<<"$mount_info")
    mount_target=$(awk 'NR == 1 {print $3}' <<<"$mount_info")
    case "$mount_type" in
        ext4)
            if [[ "$mount_target" == /mnt/c* ]]; then
                record "$name" 0 "ext4 mounted beneath /mnt/c"
            else
                record "$name" 1 "ext4 ($mount_target)"
            fi
            ;;
        '') record "$name" 0 "cannot resolve $path filesystem" ;;
        *) record "$name" 0 "$mount_type (requires local ext4)" ;;
    esac
}

filesystem_gate /opt release_storage
filesystem_gate /var/lib data_storage
filesystem_gate /var/backups backup_storage

free_kib=0
if have_command df; then
    free_kib=$(df --output=avail -k /opt 2>/dev/null | awk 'NR == 2 {print $1}')
fi
free_kib=${free_kib:-0}
if [[ "$free_kib" =~ ^[0-9]+$ && "$free_kib" -ge 83886080 ]]; then
    record free_disk 1 "${free_kib} KiB"
else
    record free_disk 0 "${free_kib} KiB (requires at least 83886080 KiB)"
fi

ntp_state='unavailable'
if have_command timedatectl; then
    ntp_state=$(timedatectl show --property=NTPSynchronized --value 2>/dev/null || printf unavailable)
fi
if [[ "$ntp_state" == yes ]]; then
    record ntp 1 synchronized
else
    record ntp 0 "$ntp_state"
fi

default_route=''
if have_command ip; then
    default_route=$(ip -o route show default 2>/dev/null | head -n 1 || true)
fi
if [[ -n "$default_route" ]]; then
    record default_route 1 present
else
    record default_route 0 missing
fi

dns_ok=0
if have_command resolvectl; then
    if resolvectl query api.hyperliquid.xyz >/dev/null 2>&1; then
        dns_ok=1
    fi
fi
record dns "$dns_ok" 'api.hyperliquid.xyz'

tls_ok=0
http_status=none
if have_command curl; then
    http_status=$(curl --silent --show-error --proto '=https' --tlsv1.2 --output /dev/null --connect-timeout 5 --max-time 10 \
        --write-out '%{http_code}' https://api.hyperliquid.xyz/info 2>/dev/null || printf none)
    if [[ "$http_status" =~ ^[1-5][0-9][0-9]$ ]]; then
        tls_ok=1
    fi
fi
record tls "$tls_ok" "api.hyperliquid.xyz status=$http_status"

port_busy=0
listeners=''
if have_command ss; then
    listeners=$(ss --listening --numeric --tcp --udp 2>/dev/null | awk '$0 ~ /:9464([[:space:]]|$)/ {print}' || true)
    [[ -n "$listeners" ]] && port_busy=1
fi
record metrics_port "$([[ "$port_busy" == 0 ]] && printf 1 || printf 0)" \
    "$([[ "$port_busy" == 0 ]] && printf free || printf 'bound (127.0.0.1:9464 must be free)')"

failed_units=''
wait_online=0
if have_command systemctl; then
    failed_units=$(systemctl --failed --no-legend --plain 2>/dev/null || true)
    if systemctl is-active --quiet systemd-networkd-wait-online.service 2>/dev/null; then
        wait_online=1
    fi
fi
if [[ -z "$failed_units" ]]; then
    record failed_units 1 none
else
    record failed_units 0 'one or more systemd units are failed'
fi
record networkd_wait_online "$wait_online" \
    "$([[ "$wait_online" == 1 ]] && printf active || printf 'not active; inspect networkd routes before deployment')"

python_ok=0
python_version=missing
if have_command python3.12; then
    python_version=$(python3.12 -c 'import platform; print(platform.python_version())' 2>/dev/null || printf invalid)
    [[ "$python_version" =~ ^3\.12\.[0-9]+$ ]] && python_ok=1
fi
record python312 "$python_ok" "$python_version"

libgomp_ok=0
if have_command ldconfig && ldconfig -p 2>/dev/null | grep -q 'libgomp\.so\.1'; then
    libgomp_ok=1
fi
record libgomp "$libgomp_ok" \
    "$([[ "$libgomp_ok" == 1 ]] && printf present || printf missing)"

if ((${#failures[@]} == 0)); then
    overall=true
else
    overall=false
fi

if ((json_only == 0)); then
    if [[ "$overall" == true ]]; then
        printf '\npreflight: PASS\n' >&2
    else
        printf '\npreflight: FAIL (%s)\n' "${failures[*]}" >&2
    fi
fi

printf '{"schema_version":1,"ok":%s,"checks":[%s],"failed_checks":[' "$overall" "$(IFS=,; printf '%s' "${checks[*]}")"
for index in "${!failures[@]}"; do
    ((index > 0)) && printf ','
    printf '"%s"' "$(json_escape "${failures[index]}")"
done
printf ']}\n'

[[ "$overall" == true ]]
