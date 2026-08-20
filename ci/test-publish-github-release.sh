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
        --argjson id 4242 \
        --arg tag v0.7.0 \
        --argjson draft "${draft}" \
        --args '$ARGS.positional | {id: $id, tag_name: $tag, draft: $draft, assets: map({name: .})}' \
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
assert_release_snapshot "${valid_release}" 4242 v0.7.0 true
expect_failure "release database ID changed" assert_release_snapshot \
    "${valid_release}" 4243 v0.7.0 true
wrong_tag_release="${TEST_TEMP_DIR}/wrong-tag-release.json"
jq '.tag_name = "v0.7.1"' "${valid_release}" >"${wrong_tag_release}"
expect_failure "release tag changed" assert_release_snapshot \
    "${wrong_tag_release}" 4242 v0.7.0 true
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

published_release="${TEST_TEMP_DIR}/published-release.json"
write_release_fixture "${published_release}" false "${TEST_ASSET_NAMES[@]}"
expect_failure "draft changed before publish" assert_release_snapshot \
    "${published_release}" 4242 v0.7.0 true
assert_verified_release_snapshot \
    "${published_release}" 4242 v0.7.0 false \
    "${TEST_EXPECTED_ASSETS}" "${TEST_TEMP_DIR}/published-assets" \
    "${TEST_ARTIFACT_DIR}" "published fixture"

published_missing_asset="${TEST_TEMP_DIR}/published-missing-asset.json"
write_release_fixture \
    "${published_missing_asset}" false "${TEST_ASSET_NAMES[@]:0:4}"
expect_failure "published snapshot missing asset" assert_verified_release_snapshot \
    "${published_missing_asset}" 4242 v0.7.0 false \
    "${TEST_EXPECTED_ASSETS}" "${TEST_TEMP_DIR}/published-missing-assets" \
    "${TEST_ARTIFACT_DIR}" "published missing fixture"

# Draft releases return 404 from /releases/tags/{tag}. Exercise discovery and
# the final snapshot through the list and database-ID endpoints, with the mock
# deliberately rejecting any accidental use of the tag endpoint.
MOCK_GH_CALLS="${TEST_TEMP_DIR}/mock-gh-calls"
MOCK_GH_MODE=valid
gh() {
    printf '%s\n' "$*" >>"${MOCK_GH_CALLS}"
    case "$*" in
        *'/releases/tags/'*)
            printf 'HTTP 404: Not Found\n' >&2
            return 1
            ;;
        *'/releases?per_page=100'*)
            case "${MOCK_GH_MODE}" in
                valid)
                    jq -c . "${valid_release}"
                    ;;
                zero)
                    ;;
                invalid_json)
                    printf 'not-json\n'
                    ;;
                duplicate)
                    jq -c . "${valid_release}"
                    jq -c . "${valid_release}"
                    ;;
                api_failure)
                    printf 'HTTP 503: Service Unavailable\n' >&2
                    return 2
                    ;;
                *)
                    printf 'unexpected list mock mode: %s\n' "${MOCK_GH_MODE}" >&2
                    return 2
                    ;;
            esac
            ;;
        *'--method PATCH'*'/releases/4242'*)
            cat "${published_release}"
            ;;
        *'--method GET'*'/releases/4242'*)
            case "${MOCK_GH_MODE}" in
                id_failure)
                    printf 'HTTP 503: Service Unavailable\n' >&2
                    return 2
                    ;;
                published)
                    cat "${published_release}"
                    ;;
                published_bad_assets)
                    cat "${published_missing_asset}"
                    ;;
                *)
                    cat "${valid_release}"
                    ;;
            esac
            ;;
        *)
            printf 'unexpected mocked gh call: %s\n' "$*" >&2
            return 1
            ;;
    esac
}

discovered_release="${TEST_TEMP_DIR}/discovered-release.json"
discover_release_by_tag v0.7.0 "${discovered_release}"
release_database_id="$(jq -er .id "${discovered_release}")"
[[ "${release_database_id}" == 4242 ]]
id_snapshot="${TEST_TEMP_DIR}/id-snapshot.json"
fetch_release_snapshot_by_id "${release_database_id}" "${id_snapshot}"
assert_release_snapshot "${id_snapshot}" 4242 v0.7.0 true
if grep -q '/releases/tags/' "${MOCK_GH_CALLS}"; then
    printf 'draft flow unexpectedly called the tag endpoint\n' >&2
    exit 1
fi
grep -q '/releases/4242' "${MOCK_GH_CALLS}"

MOCK_GH_MODE=zero
if discover_release_by_tag v0.7.0 "${TEST_TEMP_DIR}/zero-release.json"; then
    printf 'zero-match discovery unexpectedly succeeded\n' >&2
    exit 1
else
    zero_status=$?
fi
[[ "${zero_status}" -eq "${RELEASE_NOT_FOUND_STATUS}" ]]

MOCK_GH_MODE=invalid_json
expect_failure "invalid discovery JSON" discover_release_by_tag \
    v0.7.0 "${TEST_TEMP_DIR}/invalid-release.json"
MOCK_GH_MODE=api_failure
expect_failure "release-list API failure is not treated as no release" \
    discover_release_by_tag v0.7.0 "${TEST_TEMP_DIR}/unavailable-release.json"
MOCK_GH_MODE=valid
expect_failure "discovery output write failure" discover_release_by_tag \
    v0.7.0 "${TEST_TEMP_DIR}/missing-directory/release.json"
MOCK_GH_MODE=duplicate
expect_failure "duplicate release across pages" discover_release_by_tag \
    v0.7.0 "${TEST_TEMP_DIR}/duplicate-release-pages.json"

MOCK_GH_MODE=published
patch_response="${TEST_TEMP_DIR}/patch-response.json"
publish_release_by_id 4242 "${patch_response}"
post_publish_snapshot="${TEST_TEMP_DIR}/post-publish-snapshot.json"
fetch_release_snapshot_by_id 4242 "${post_publish_snapshot}"
assert_verified_release_snapshot \
    "${post_publish_snapshot}" 4242 v0.7.0 false \
    "${TEST_EXPECTED_ASSETS}" "${TEST_TEMP_DIR}/post-publish-assets" \
    "${TEST_ARTIFACT_DIR}" "post-publish fixture"
grep -q -- '--method PATCH repos/aleygey/serial-platform/releases/4242' \
    "${MOCK_GH_CALLS}"
grep -q -- '-F draft=false' "${MOCK_GH_CALLS}"
grep -q -- '-f make_latest=true' "${MOCK_GH_CALLS}"
if grep -q -- '--method PATCH .*releases/tags/' "${MOCK_GH_CALLS}"; then
    printf 'publish unexpectedly used a tag endpoint\n' >&2
    exit 1
fi

expect_failure "post-publish state remained draft" assert_verified_release_snapshot \
    "${valid_release}" 4242 v0.7.0 false \
    "${TEST_EXPECTED_ASSETS}" "${TEST_TEMP_DIR}/bad-post-state-assets" \
    "${TEST_ARTIFACT_DIR}" "bad post-publish state"
MOCK_GH_MODE=published_bad_assets
fetch_release_snapshot_by_id 4242 "${TEST_TEMP_DIR}/bad-post-assets.json"
expect_failure "post-publish asset set changed" assert_verified_release_snapshot \
    "${TEST_TEMP_DIR}/bad-post-assets.json" 4242 v0.7.0 false \
    "${TEST_EXPECTED_ASSETS}" "${TEST_TEMP_DIR}/bad-post-assets-list" \
    "${TEST_ARTIFACT_DIR}" "bad post-publish assets"
MOCK_GH_MODE=id_failure
expect_failure "database-ID snapshot API failure" fetch_release_snapshot_by_id \
    4242 "${TEST_TEMP_DIR}/unavailable-id-snapshot.json"

printf 'publish-github-release helper tests passed\n'
