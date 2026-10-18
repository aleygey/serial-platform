#!/usr/bin/env bash
set -Eeuo pipefail
readonly TEST_SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=release-actions.sh
source "${TEST_SCRIPT_DIR}/release-actions.sh"
TEST_TEMP_DIR="$(mktemp -d)"
trap 'rm -rf -- "${TEST_TEMP_DIR}"' EXIT
readonly HEAD_SHA=1111111111111111111111111111111111111111
readonly OTHER_SHA=2222222222222222222222222222222222222222
readonly TAG_SHA=3333333333333333333333333333333333333333
expect_failure() {
    local description="$1"
    shift
    if ("$@") >/dev/null 2>&1; then
        printf 'expected failure: %s\n' "${description}" >&2
        exit 1
    fi
}
[[ "$(release_decision missing '' '' "${HEAD_SHA}")" == true ]]
[[ "$(release_decision draft tag "${HEAD_SHA}" "${HEAD_SHA}")" == true ]]
[[ "$(release_decision published tag "${OTHER_SHA}" "${HEAD_SHA}")" == false ]]
expect_failure 'lightweight tag' release_decision missing commit "${HEAD_SHA}" "${HEAD_SHA}"
expect_failure 'unpublished tag at another source' release_decision draft tag "${OTHER_SHA}" "${HEAD_SHA}"
expect_failure 'ambiguous state' release_decision invalid '' '' "${HEAD_SHA}"

MOCK_MODE=missing
MOCK_CALLS="${TEST_TEMP_DIR}/calls"
REMOTE_PRESENT="${TEST_TEMP_DIR}/remote-present"
MOCK_LOCAL_TAG_TYPE=''
MOCK_LOCAL_COMMIT="${HEAD_SHA}"
MOCK_PREVIOUS_VERSION='0.0.0'
CURRENT_VERSION="$(sed -n 's/^version = "\(.*\)"/\1/p' "${ACTIONS_PROJECT_ROOT}/Cargo.toml" | head -n 1)"
gh() {
    printf '%s\n' "$*" >>"${MOCK_CALLS}"
    case "$*" in
        *'/releases?per_page=100')
            case "${MOCK_MODE}" in
                missing) printf '[]\n' ;;
                published) printf '[{"tag_name":"v%s","draft":false}]\n' "${CURRENT_VERSION}" ;;
                draft) printf '[{"tag_name":"v%s","draft":true}]\n' "${CURRENT_VERSION}" ;;
                duplicate) printf '[{"tag_name":"v%s","draft":true}]\n[{"tag_name":"v%s","draft":true}]\n' "${CURRENT_VERSION}" "${CURRENT_VERSION}" ;;
                bad_json) printf 'invalid-json\n' ;;
                no_body) return 0 ;;
                api_failure) return 22 ;;
                *) return 1 ;;
            esac ;;
        *'/git/matching-refs/tags/'*)
            [[ "${MOCK_MODE}" == ref_read_failure ]] && return 22
            if [[ -f "${REMOTE_PRESENT}" ]]; then
                local kind=tag
                [[ "${MOCK_MODE}" == lightweight ]] && kind=commit
                jq -n --arg kind "${kind}" --arg sha "${TAG_SHA}" \
                    '[{ref:"refs/tags/v0.0.1",object:{type:$kind,sha:$sha}}]'
            else
                # Prefix matches must not be mistaken for the exact tag.
                printf '[{"ref":"refs/tags/v0.0.10","object":{"type":"tag","sha":"%s"}}]\n' "${TAG_SHA}"
            fi ;;
        *'--method POST'*'/git/tags '*)
            printf '%s\n' "${TAG_SHA}" ;;
        *'--method POST'*'/git/refs '*)
            [[ "${MOCK_MODE}" == ref_denied ]] && return 1
            touch "${REMOTE_PRESENT}"
            [[ "${MOCK_MODE}" == ref_ambiguous || "${MOCK_MODE}" == ref_conflict ]] && return 1
            printf '{}\n' ;;
        *"/git/tags/${TAG_SHA}")
            local target="${HEAD_SHA}"
            [[ "${MOCK_MODE}" == wrong_commit || "${MOCK_MODE}" == ref_conflict ]] && target="${OTHER_SHA}"
            jq -n --arg commit "${target}" '{tag:"v0.0.1",object:{type:"commit",sha:$commit}}' ;;
        *) printf 'unexpected mock gh call: %s\n' "$*" >&2; return 1 ;;
    esac
}
git() {
    printf 'git %s\n' "$*" >>"${MOCK_CALLS}"
    case "$*" in
        'fetch --no-tags origin refs/tags/v0.0.1:refs/tags/v0.0.1') ;;
        'cat-file -t refs/tags/v0.0.1') printf 'tag\n' ;;
        'rev-parse refs/tags/v0.0.1^{commit}') printf '%s\n' "${HEAD_SHA}" ;;
        'show '*':Cargo.toml') printf 'version = "%s"\n' "${MOCK_PREVIOUS_VERSION}" ;;
        'show-ref --verify --quiet '*) [[ -n "${MOCK_LOCAL_TAG_TYPE}" ]] ;;
        'cat-file -t '*) printf '%s\n' "${MOCK_LOCAL_TAG_TYPE}" ;;
        'rev-parse refs/tags/'*) printf '%s\n' "${MOCK_LOCAL_COMMIT}" ;;
        'rev-parse HEAD') printf '%s\n' "${HEAD_SHA}" ;;
        *) printf 'unexpected mock git call: %s\n' "$*" >&2; return 1 ;;
    esac
}
[[ "$(release_state "v${CURRENT_VERSION}")" == missing ]]
for MOCK_MODE in bad_json no_body api_failure duplicate; do
    expect_failure "release discovery ${MOCK_MODE}" release_state "v${CURRENT_VERSION}"
done
MOCK_MODE=published
[[ "$(release_state "v${CURRENT_VERSION}")" == published ]]
MOCK_MODE=draft
[[ "$(release_state "v${CURRENT_VERSION}")" == draft ]]

GITHUB_OUTPUT="${TEST_TEMP_DIR}/outputs"
GITHUB_EVENT_NAME=push
RELEASE_BEFORE_SHA="${OTHER_SHA}"
MOCK_PREVIOUS_VERSION="${CURRENT_VERSION}"
: >"${MOCK_CALLS}"
prepare_release "${CURRENT_VERSION}" "v${CURRENT_VERSION}" "${HEAD_SHA}"
grep -qx release=false "${GITHUB_OUTPUT}"
! grep -q /releases "${MOCK_CALLS}"
MOCK_PREVIOUS_VERSION='0.0.0'
MOCK_MODE=missing
: >"${GITHUB_OUTPUT}"
prepare_release "${CURRENT_VERSION}" "v${CURRENT_VERSION}" "${HEAD_SHA}"
grep -qx release=true "${GITHUB_OUTPUT}"
MOCK_LOCAL_TAG_TYPE=tag
MOCK_LOCAL_COMMIT="${OTHER_SHA}"
expect_failure 'version bump cannot move unpublished tag' prepare_release "${CURRENT_VERSION}" "v${CURRENT_VERSION}" "${HEAD_SHA}"
MOCK_MODE=published
: >"${GITHUB_OUTPUT}"
prepare_release "${CURRENT_VERSION}" "v${CURRENT_VERSION}" "${HEAD_SHA}"
grep -qx release=false "${GITHUB_OUTPUT}"
RELEASE_BEFORE_SHA=0000000000000000000000000000000000000000
expect_failure 'unverifiable before SHA' prepare_release "${CURRENT_VERSION}" "v${CURRENT_VERSION}" "${HEAD_SHA}"

MOCK_MODE=missing
ensure_release_tag v0.0.1 "${HEAD_SHA}"
: >"${MOCK_CALLS}"
ensure_release_tag v0.0.1 "${HEAD_SHA}"
! grep -q -- '--method POST' "${MOCK_CALLS}"
for MOCK_MODE in lightweight wrong_commit; do
    expect_failure "existing tag ${MOCK_MODE}" ensure_release_tag v0.0.1 "${HEAD_SHA}"
done
rm "${REMOTE_PRESENT}"
MOCK_MODE=ref_ambiguous
ensure_release_tag v0.0.1 "${HEAD_SHA}"
rm "${REMOTE_PRESENT}"
MOCK_MODE=ref_denied
expect_failure 'failed ref write must not become success' ensure_release_tag v0.0.1 "${HEAD_SHA}"
MOCK_MODE=ref_read_failure
expect_failure 'reference API read error cannot create tag' ensure_release_tag v0.0.1 "${HEAD_SHA}"
MOCK_MODE=ref_conflict
expect_failure 'conflicting tag created concurrently' ensure_release_tag v0.0.1 "${HEAD_SHA}"
! grep -Eq -- 'git push|--force|--method PATCH'  "${MOCK_CALLS}"

GITHUB_REPOSITORY='another/repository'
GITHUB_REF=refs/heads/main
GITHUB_SHA="${HEAD_SHA}"
GH_TOKEN=fixture-token
expect_failure 'fork release blocked' release_actions_main tag
GITHUB_REPOSITORY="${EXPECTED_REPOSITORY}"
GITHUB_REF=refs/heads/topic
expect_failure 'non-main dispatch blocked' release_actions_main tag
GITHUB_REF=refs/heads/main
GITHUB_EVENT_NAME=pull_request
expect_failure 'PR release blocked' release_actions_main tag
GITHUB_EVENT_NAME=workflow_dispatch
GITHUB_SHA="${OTHER_SHA}"
expect_failure 'wrong checkout blocked' release_actions_main tag
# PR validation needs no token and must not even query the release API.
workspace_version() { printf '%s\n' "${CURRENT_VERSION}"; }
GITHUB_EVENT_NAME=pull_request
GITHUB_REF=refs/pull/38/merge
GITHUB_SHA="${HEAD_SHA}"
unset GH_TOKEN
: >"${GITHUB_OUTPUT}"
: >"${MOCK_CALLS}"
release_actions_main validate-pr
grep -qx release=true "${GITHUB_OUTPUT}"
! grep -q '^api ' "${MOCK_CALLS}"
# Manual main dispatch may retry an unpublished version without a new bump.
GITHUB_EVENT_NAME=workflow_dispatch
GITHUB_REF=refs/heads/main
GH_TOKEN=fixture-token
MOCK_MODE=missing
MOCK_LOCAL_TAG_TYPE=''
: >"${GITHUB_OUTPUT}"
release_actions_main prepare
grep -qx release=true "${GITHUB_OUTPUT}"
printf 'release-actions helper tests passed\n' 
