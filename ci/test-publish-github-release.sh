#!/usr/bin/env bash

set -Eeuo pipefail

readonly TEST_SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

# shellcheck source=publish-github-release.sh
source "${TEST_SCRIPT_DIR}/publish-github-release.sh"

# The publisher runs on Jenkins Linux where sha256sum is required. Keep this
# helper test portable to macOS without relaxing the production requirement.
if ! command -v sha256sum >/dev/null 2>&1; then
    asset_sha256() {
        shasum -a 256 "$1" | awk '{print $1}'
    }
fi

TEST_TEMP_DIR="$(mktemp -d)"
trap 'rm -rf -- "${TEST_TEMP_DIR}"' EXIT
TEMP_DIR="${TEST_TEMP_DIR}"

readonly TEST_ARTIFACT_DIR="${TEST_TEMP_DIR}/artifacts"
readonly TEST_EXPECTED_ASSETS="${TEST_TEMP_DIR}/expected-assets"
mkdir -p "${TEST_ARTIFACT_DIR}"

readonly -a TEST_ASSET_NAMES=(
    SHA256SUMS
    serial-platform-v0.7.0-linux-x86_64-ubuntu20.04.tar.gz
    serial-platform-v0.7.0-macos-aarch64.zip
    serial-platform-v0.7.0-macos-x86_64.zip
    serial-platform-v0.7.0-windows-x86_64.zip
)
printf '%s\n' "${TEST_ASSET_NAMES[@]}" | LC_ALL=C sort >"${TEST_EXPECTED_ASSETS}"

for test_name in "${TEST_ASSET_NAMES[@]}"; do
    printf 'fixture for %s\n' "${test_name}" >"${TEST_ARTIFACT_DIR}/${test_name}"
done

write_release_fixture() {
    local output="$1"
    local draft="$2"
    shift 2
    jq -n \
        --argjson draft "${draft}" \
        --args '$ARGS.positional | {draft: $draft, assets: map({name: .})}' \
        "$@" >"${output}"
}

expect_failure() {
    local description="$1"
    shift
    if ("$@") >/dev/null 2>&1; then
        printf 'expected failure: %s\n' "${description}" >&2
        exit 1
    fi
}

valid_release="${TEST_TEMP_DIR}/valid-release.json"
write_release_fixture "${valid_release}" true "${TEST_ASSET_NAMES[@]}"
assert_exact_release_asset_set \
    "${valid_release}" "${TEST_EXPECTED_ASSETS}" \
    "${TEST_TEMP_DIR}/valid-actual" "valid fixture"

missing_release="${TEST_TEMP_DIR}/missing-release.json"
write_release_fixture "${missing_release}" true "${TEST_ASSET_NAMES[@]:0:4}"
expect_failure "missing asset" assert_exact_release_asset_set \
    "${missing_release}" "${TEST_EXPECTED_ASSETS}" \
    "${TEST_TEMP_DIR}/missing-actual" "missing fixture"

extra_release="${TEST_TEMP_DIR}/extra-release.json"
write_release_fixture \
    "${extra_release}" true "${TEST_ASSET_NAMES[@]}" unexpected-debug.zip
expect_failure "extra asset" assert_exact_release_asset_set \
    "${extra_release}" "${TEST_EXPECTED_ASSETS}" \
    "${TEST_TEMP_DIR}/extra-actual" "extra fixture"

duplicate_release="${TEST_TEMP_DIR}/duplicate-release.json"
write_release_fixture \
    "${duplicate_release}" true "${TEST_ASSET_NAMES[@]}" "${TEST_ASSET_NAMES[1]}"
expect_failure "duplicate asset" assert_exact_release_asset_set \
    "${duplicate_release}" "${TEST_EXPECTED_ASSETS}" \
    "${TEST_TEMP_DIR}/duplicate-actual" "duplicate fixture"

digest_release="${TEST_TEMP_DIR}/digest-release.json"
digest_name="${TEST_ASSET_NAMES[1]}"
digest="$(asset_sha256 "${TEST_ARTIFACT_DIR}/${digest_name}")"
jq --arg name "${digest_name}" --arg digest "sha256:${digest}" \
    '(.assets[] | select(.name == $name)).digest = $digest' \
    "${valid_release}" >"${digest_release}"
verify_release_api_digests "${digest_release}" "${TEST_ARTIFACT_DIR}"

bad_digest_release="${TEST_TEMP_DIR}/bad-digest-release.json"
jq --arg name "${digest_name}" \
    '(.assets[] | select(.name == $name)).digest = ("sha256:" + ("0" * 64))' \
    "${valid_release}" >"${bad_digest_release}"
expect_failure "mismatched API digest" verify_release_api_digests \
    "${bad_digest_release}" "${TEST_ARTIFACT_DIR}"

printf 'publish-github-release helper tests passed\n'
