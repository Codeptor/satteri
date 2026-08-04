#!/usr/bin/env bash

set -euo pipefail

repository_root="$(git rev-parse --show-toplevel)"
fixture_path=''
fixture_relative=''

assert_fixture_removed() {
    if [[ -e "$fixture_path" ]] || git -C "$repository_root" ls-files --error-unmatch -- "$fixture_relative" >/dev/null 2>&1; then
        printf '%s\n' 'paper-boundary test fixture was not removed cleanly' >&2
        exit 1
    fi
}

expect_rejected_fixture() {
    if "$repository_root/scripts/check-paper-boundary.sh" >/dev/null 2>&1; then
        printf '%s\n' 'paper-boundary test expected the tracked fixture to be rejected' >&2
        exit 1
    fi
}

remove_fixture() {
    git -C "$repository_root" restore --staged -- "$fixture_relative"
    rm -f -- "$fixture_path"
    assert_fixture_removed
    fixture_path=''
    fixture_relative=''
}

cleanup() {
    if [[ -n "$fixture_relative" ]]; then
        git -C "$repository_root" restore --staged -- "$fixture_relative" >/dev/null 2>&1 || true
    fi

    if [[ -n "$fixture_path" && -e "$fixture_path" ]]; then
        rm -f -- "$fixture_path"
    fi
}

trap cleanup EXIT

fixture_path="$(mktemp "$repository_root/config/.paper boundary.XXXXXX.toml")"
fixture_relative="${fixture_path#"$repository_root/"}"

printf '%s\n' 'private_key = "fixture-only"' >"$fixture_path"
git -C "$repository_root" add -- "$fixture_relative"
expect_rejected_fixture
remove_fixture

fixture_path="$(mktemp "$repository_root/config/.paper camel-case.XXXXXX.json")"
fixture_relative="${fixture_path#"$repository_root/"}"

printf '%s\n' '{"seedPhrase":"fixture-only","walletAddress":"fixture-only","secretToken":"fixture-only"}' >"$fixture_path"
git -C "$repository_root" add -- "$fixture_relative"
expect_rejected_fixture
remove_fixture

fixture_path="$(mktemp "$repository_root/crates/trench-hyperliquid/src/.paper endpoint.XXXXXX.rs")"
fixture_relative="${fixture_path#"$repository_root/"}"

printf '%s\n' 'const ACTION: &str = concat!("https", "://api.hyperliquid.xyz/", "ex", "change");' >"$fixture_path"
git -C "$repository_root" add -- "$fixture_relative"
expect_rejected_fixture
remove_fixture

fixture_path="$(mktemp "$repository_root/crates/trench-hyperliquid/src/.paper raw-endpoint.XXXXXX.rs")"
fixture_relative="${fixture_path#"$repository_root/"}"

printf '%s\n' 'const ACTION: &str = concat!(r"https", r"://api.hyperliquid.xyz/", r"ex", r"change");' >"$fixture_path"
git -C "$repository_root" add -- "$fixture_relative"
expect_rejected_fixture
remove_fixture

fixture_path="$(mktemp "$repository_root/crates/trench-hyperliquid/src/.paper hashed-raw-endpoint.XXXXXX.rs")"
fixture_relative="${fixture_path#"$repository_root/"}"

printf '%s\n' 'const ACTION: &str = concat!(r#"https"#, r#"://api.hyperliquid.xyz/"#, r#"ex"#, r#"change"#);' >"$fixture_path"
git -C "$repository_root" add -- "$fixture_relative"
expect_rejected_fixture
remove_fixture

fixture_path="$(mktemp "$repository_root/crates/trenchd/src/.paper raw-endpoint.XXXXXX.rs")"
fixture_relative="${fixture_path#"$repository_root/"}"

printf '%s\n' 'const ACTION: &str = concat!(r"https", r"://api.hyperliquid.xyz/", r"ex", r"change");' >"$fixture_path"
git -C "$repository_root" add -- "$fixture_relative"
expect_rejected_fixture
remove_fixture

fixture_path="$(mktemp "$repository_root/compose.yaml.paper-boundary.XXXXXX")"
fixture_relative="${fixture_path#"$repository_root/"}"

printf '%s\n' 'PRIVATE_KEY: fixture-only' >"$fixture_path"
git -C "$repository_root" add -- "$fixture_relative"
expect_rejected_fixture
remove_fixture

fixture_path="$(mktemp "$repository_root/docs/.paper boundary.XXXXXX.md")"
fixture_relative="${fixture_path#"$repository_root/"}"

printf '%s\n' '/exchange is excluded from the paper-only runtime.' >"$fixture_path"
git -C "$repository_root" add -- "$fixture_relative"

"$repository_root/scripts/check-paper-boundary.sh" >/dev/null

git -C "$repository_root" restore --staged -- "$fixture_relative"
rm -f -- "$fixture_path"
assert_fixture_removed

fixture_path=''
fixture_relative=''

"$repository_root/scripts/check-paper-boundary.sh"
