#!/usr/bin/env bash

set -euo pipefail

export LC_ALL=C

repository_root="$(git rev-parse --show-toplevel)"
cd "$repository_root"

if ! command -v perl >/dev/null 2>&1; then
    printf '%s\n' 'paper-boundary: Perl is required to inspect concatenated source literals' >&2
    exit 1
fi

failures=0

report_failure() {
    printf 'paper-boundary: %s\n' "$*" >&2
    failures=1
}

is_documentation_path() {
    case "$1" in
        AGENTS.md | README.md | CONTRIBUTING.md | SECURITY.md | LICENSE-* | docs/*)
            return 0
            ;;
        *)
            return 1
            ;;
    esac
}

is_test_path() {
    case "$1" in
        tests/* | crates/*/tests/* | ml/tests/*)
            return 0
            ;;
        *)
            return 1
            ;;
    esac
}

is_manifest_path() {
    case "$1" in
        Cargo.toml | Cargo.lock | */Cargo.toml | */Cargo.lock | pyproject.toml | uv.lock | \
        */pyproject.toml | */uv.lock | package.json | package-lock.json | pnpm-lock.yaml | \
        bun.lock | bun.lockb | */package.json | */package-lock.json | */pnpm-lock.yaml | \
        */bun.lock | */bun.lockb | requirements*.txt | */requirements*.txt)
            return 0
            ;;
        *)
            return 1
            ;;
    esac
}

check_pattern() {
    local path="$1"
    local label="$2"
    local pattern="$3"
    local -a pipeline_status=()

    if git cat-file blob ":$path" | grep -a -E -i -q -- "$pattern"; then
        printf -v path '%q' "$path"
        report_failure "forbidden ${label} in tracked file ${path}"
        return
    else
        pipeline_status=("${PIPESTATUS[@]}")
        if ((pipeline_status[0] != 0 || (pipeline_status[1] != 0 && pipeline_status[1] != 1))); then
            printf -v path '%q' "$path"
            report_failure "could not scan tracked file ${path}"
        fi
    fi
}

reconstruct_tracked_literals() {
    git cat-file blob ":$1" \
        | perl -0pe 's/\b(?:b)?r(#+)"(.*?)"\1/"$2"/gs; s/\b(?:b)?r"(.*?)"/"$1"/gs; 1 while s/"((?:[^"\\]|\\.)*)"[\s,+]*"((?:[^"\\]|\\.)*)"/"$1$2"/g'
}

check_reconstructed_pattern() {
    local path="$1"
    local label="$2"
    local pattern="$3"
    local -a pipeline_status=()

    if reconstruct_tracked_literals "$path" | grep -a -E -i -q -- "$pattern"; then
        printf -v path '%q' "$path"
        report_failure "forbidden ${label} in tracked file ${path}"
        return
    else
        pipeline_status=("${PIPESTATUS[@]}")
        if ((pipeline_status[0] != 0 || (pipeline_status[1] != 0 && pipeline_status[1] != 1))); then
            printf -v path '%q' "$path"
            report_failure "could not scan tracked file ${path}"
        fi
    fi
}

is_regular_tracked_file() {
    local entry
    local mode

    entry="$(git ls-files -s -- "$1")"
    mode="${entry%% *}"
    [[ "$mode" == '100644' || "$mode" == '100755' ]]
}

sensitive_field_pattern="(^|[^[:alnum:]_])(private[[:space:]_-]*key|api[[:space:]_-]*hash|api[[:space:]_-]*key|mnemonic([[:space:]_-]*(phrase|words?|key))?|wallet([[:space:]_-]*(address|key|id|path|file))?|account([[:space:]_-]*(address|id))?|sec""ret([[:space:]_-]*(key|token))?|token|password|credential(s)?)[[:space:]'\"]*[:=]"
seed_field_pattern="(^|[^[:alnum:]_])seed([[:space:]_-]*(phrase|words?|key))?[[:space:]'\"]*[:=]"
action_endpoint_pattern="/(e""x[[:space:]\"'(),+]*c""hange|[\"'(),+][[:space:]\"'(),+]*e""x[[:space:]\"'(),+]*c""hange)"
forbidden_sdk_terms=(
    "ether""s"
    "@trench/perps-""sdk"
    "tele""gram"
    "telo""xide"
    "tele""thon"
    "gram""js"
    "gram""mers"
    "td""lib"
    "mt""proto"
    "tg""botapi"
)
signer_crate_pattern='(^|[^[:alnum:]_])signer([^[:alnum:]_]|$)'

while IFS= read -r -d '' path; do
    if is_documentation_path "$path"; then
        continue
    fi

    if is_test_path "$path"; then
        continue
    fi

    if ! is_regular_tracked_file "$path"; then
        printf -v path '%q' "$path"
        report_failure "tracked runtime/config/manifest file is not a regular file: ${path}"
        continue
    fi

    check_reconstructed_pattern "$path" 'action endpoint' "$action_endpoint_pattern"
    check_pattern "$path" 'sensitive field' "$sensitive_field_pattern"

    check_pattern "$path" 'sensitive field' "$seed_field_pattern"

    for term in "${forbidden_sdk_terms[@]}"; do
        check_pattern "$path" 'live or messaging SDK' "(^|[^[:alnum:]_])${term}([^[:alnum:]_]|$)"
    done

    if is_manifest_path "$path"; then
        check_pattern "$path" 'signing dependency' "$signer_crate_pattern"
    fi
done < <(git ls-files -z)

seen_info=0
seen_ws=0

while IFS= read -r -d '' path; do
    case "$path" in
        crates/trench-hyperliquid/src/*.rs)
            ;;
        *)
            continue
            ;;
    esac

    if ! is_regular_tracked_file "$path"; then
        printf -v path '%q' "$path"
        report_failure "tracked adapter source is not a regular file: ${path}"
        continue
    fi

    while IFS= read -r destination; do
        case "$destination" in
            https://api.hyperliquid.xyz/info)
                seen_info=1
                ;;
            wss://api.hyperliquid.xyz/ws)
                seen_ws=1
                ;;
            ws://\{\})
                ;;
            *)
                printf -v path '%q' "$path"
                printf -v destination '%q' "$destination"
                report_failure "adapter destination ${destination} is outside /info and /ws in ${path}"
                ;;
        esac
    done < <(
        reconstruct_tracked_literals "$path" \
            | grep -a -E -o '(https?|wss?)://[^[:space:]"`]+' \
            || true
    )
done < <(git ls-files -z -- crates/trench-hyperliquid/src)

if ((seen_info != 1)); then
    report_failure 'missing exact public /info adapter destination'
fi

if ((seen_ws != 1)); then
    report_failure 'missing exact public /ws adapter destination'
fi

if ((failures != 0)); then
    exit 1
fi

printf 'paper-boundary: clean\n'
