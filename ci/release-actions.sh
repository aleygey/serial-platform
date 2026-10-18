#!/usr/bin/env bash
# Release orchestration for GitHub Actions. Never moves or overwrites a tag.
set -Eeuo pipefail

readonly ACTIONS_SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
readonly ACTIONS_PROJECT_ROOT="$(cd -- "${ACTIONS_SCRIPT_DIR}/.." && pwd)"
readonly EXPECTED_REPOSITORY='aleygey/serial-platform'

release_fail() {
    printf 'error: %s\n' "$*" >&2
    exit 1
}

workspace_version() {
    cargo metadata --locked --no-deps --format-version 1 | jq -er '
        [.packages[] | select(.source == null) | .version] | unique
        | if length == 1 then .[0] else error("workspace versions differ") end
    '
}

# Pure decision helper, also exercised with tag/release race fixtures.
release_decision() {
    local state="$1" tag_type="$2" tag_commit="$3" source_commit="$4"
    case "${state}" in
        published) printf 'false\n'; return ;;
        missing|draft) ;;
        *) release_fail "invalid or ambiguous release state: ${state}" ;;
    esac
    if [[ -n "${tag_type}" ]]; then
        [[ "${tag_type}" == tag ]] || release_fail 'release tag must be annotated'
        [[ "${tag_commit}" == "${source_commit}" ]] \
            || release_fail 'unpublished release tag points to another commit; refusing to move it'
    fi
    printf 'true\n'
}

release_state() {
    local tag="$1" releases
    releases="$(gh api --paginate "repos/${EXPECTED_REPOSITORY}/releases?per_page=100")" \
        || release_fail 'could not list releases; refusing to treat an API failure as a new version'
    jq -ers --arg tag "${tag}" '
        if length == 0 or any(.[]; type != "array") then error("invalid release listing") else . end
        | [ .[][] | select(.tag_name == $tag) ]
        | if length == 0 then "missing"
          elif length != 1 then error("duplicate release tag")
          elif .[0].draft == true then "draft"
          elif .[0].draft == false then "published"
          else error("invalid release state") end
    ' <<<"${releases}"
}

# The matching-refs endpoint returns [] for an absent tag, without conflating
# HTTP authentication/network failures with absence. Filter out prefix matches.
remote_tag_ref() {
    local tag="$1" refs
    refs="$(gh api "repos/${EXPECTED_REPOSITORY}/git/matching-refs/tags/${tag}")" \
        || release_fail "could not inspect remote tag ${tag}"
    jq -er --arg ref "refs/tags/${tag}" '
        [ .[] | select(.ref == $ref) ]
        | if length == 0 then ""
          elif length == 1 then .[0]
          else error("duplicate remote tag reference") end
    ' <<<"${refs}"
}

assert_remote_annotated_tag() {
    local tag="$1" source_commit="$2" reference="$3" tag_object
    [[ -n "${reference}" ]] || release_fail "remote tag ${tag} is missing"
    [[ "$(jq -er '.object.type' <<<"${reference}")" == tag ]] \
        || release_fail "remote tag ${tag} must be annotated"
    local object_sha
    object_sha="$(jq -er '.object.sha' <<<"${reference}")"
    [[ "${object_sha}" =~ ^[0-9a-f]{40}$ ]] || release_fail 'invalid tag object SHA'
    tag_object="$(gh api "repos/${EXPECTED_REPOSITORY}/git/tags/${object_sha}")" \
        || release_fail "could not inspect annotated tag ${tag}"
    jq -e --arg commit "${source_commit}" --arg tag "${tag}" \
        '.tag == $tag and .object.type == "commit" and .object.sha == $commit' \
        <<<"${tag_object}" >/dev/null \
        || release_fail "remote tag ${tag} does not point directly to the validated source commit"
}

prepare_release() {
    local version="$1" tag="$2" head="$3"
    # A merge with a version bump can release. Later docs/source commits with
    # the same version cannot start a new release implicitly. Re-running the
    # original version-bump run retains its original event.before and SHA.
    if [[ "${GITHUB_EVENT_NAME:-}" == push ]]; then
        local before="${RELEASE_BEFORE_SHA:-}" previous_version
        [[ "${before}" =~ ^[0-9a-f]{40}$ && "${before}" != 0000000000000000000000000000000000000000 ]] \
            || release_fail 'push must have a verifiable previous main commit'
        previous_version="$(git show "${before}:Cargo.toml" | sed -n 's/^version = "\(.*\)"/\1/p' | head -n 1)"
        [[ -n "${previous_version}" ]] || release_fail 'previous workspace version is missing'
        if [[ "${previous_version}" == "${version}" ]]; then
            printf 'release=false\n' >>"${GITHUB_OUTPUT}"
            printf 'Workspace version is unchanged; no release requested.\n'
            return
        fi
    fi
    local state tag_type='' tag_commit='' decision
    state="$(release_state "${tag}")" || release_fail "could not determine release state"
    if git show-ref --verify --quiet "refs/tags/${tag}"; then
        tag_type="$(git cat-file -t "refs/tags/${tag}")"
        tag_commit="$(git rev-parse "refs/tags/${tag}^{commit}")"
    fi
    decision="$(release_decision "${state}" "${tag_type}" "${tag_commit}" "${head}")" || return
    if [[ "${decision}" == true ]]; then
        [[ -s "docs/releases/${tag}.md" ]] || release_fail "release notes missing: docs/releases/${tag}.md"
        jq -e --arg version "${version}" '.version == $version' crates/serial-desktop/package.json >/dev/null
        jq -e --arg version "${version}" \
            '.version == $version and .packages[""].version == $version' \
            crates/serial-desktop/package-lock.json >/dev/null
    fi
    printf 'release=%s\n' "${decision}" >>"${GITHUB_OUTPUT}"
    printf 'Release %s: %s (existing state: %s)\n' "${tag}" "${decision}" "${state}"
}

ensure_release_tag() {
    local tag="$1" head="$2" reference
    reference="$(remote_tag_ref "${tag}")" || release_fail "could not resolve remote tag"
    if [[ -z "${reference}" ]]; then
        local tag_sha
        tag_sha="$(gh api --method POST "repos/${EXPECTED_REPOSITORY}/git/tags" \
            -f tag="${tag}" -f message="Serial Platform ${tag}" \
            -f object="${head}" -f type=commit --jq '.sha')" \
            || release_fail "could not create annotated tag object for ${tag}"
        [[ "${tag_sha}" =~ ^[0-9a-f]{40}$ ]] || release_fail 'GitHub returned an invalid tag object SHA'
        if ! gh api --method POST "repos/${EXPECTED_REPOSITORY}/git/refs" \
            -f ref="refs/tags/${tag}" -f sha="${tag_sha}" >/dev/null; then
            printf 'Tag creation returned an error; checking whether the exact tag was committed.\n'
        fi
        # A conflicting/ambiguous API response is only accepted after reading
        # back an annotated tag for this exact commit. No force updates exist.
        reference="$(remote_tag_ref "${tag}")" || release_fail "could not resolve remote tag"
    fi
    assert_remote_annotated_tag "${tag}" "${head}" "${reference}"
    # This repository is public; checkout credentials remain unpersisted.
    git fetch --no-tags origin "refs/tags/${tag}:refs/tags/${tag}"
    [[ "$(git cat-file -t "refs/tags/${tag}")" == tag ]] || release_fail 'fetched tag is not annotated'
    [[ "$(git rev-parse "refs/tags/${tag}^{commit}")" == "${head}" ]] \
        || release_fail 'fetched tag does not match the validated source'
}

release_actions_main() {
    cd "${ACTIONS_PROJECT_ROOT}"
    [[ "${GITHUB_REPOSITORY:-}" == "${EXPECTED_REPOSITORY}" ]] \
        || release_fail 'release orchestration only runs in the canonical repository'
    if [[ "${1:-}" == validate-pr ]]; then
        [[ "${GITHUB_EVENT_NAME:-}" == pull_request ]] || release_fail 'validate-pr only accepts pull request events'
    else
        [[ "${GITHUB_REF:-}" == refs/heads/main ]] || release_fail 'release orchestration only runs on main'
        case "${GITHUB_EVENT_NAME:-}" in
            push|workflow_dispatch) ;;
            *) release_fail 'release orchestration only accepts main push or manual events' ;;
        esac
        [[ -n "${GH_TOKEN:-}" ]] || release_fail 'the job-scoped GH_TOKEN is required'
    fi
    local head version tag
    head="$(git rev-parse HEAD)"
    [[ "${head}" == "${GITHUB_SHA:-}" ]] || release_fail 'checkout differs from the immutable workflow SHA'
    version="$(workspace_version)" || release_fail "could not read locked workspace version"
    [[ "${version}" =~ ^(0|[1-9][0-9]*)\.[0-9]+\.[0-9]+([+-][0-9A-Za-z.-]+)?$ ]] \
        || release_fail 'unsupported workspace version'
    tag="v${version}"
    case "${1:-}" in
        prepare)
            [[ -n "${GITHUB_OUTPUT:-}" ]] || release_fail 'GITHUB_OUTPUT is required'
            printf 'version=%s\ntag=%s\n' "${version}" "${tag}" >>"${GITHUB_OUTPUT}"
            prepare_release "${version}" "${tag}" "${head}"
            ;;
        validate-pr)
            [[ -n "${GITHUB_OUTPUT:-}" ]] || release_fail 'GITHUB_OUTPUT is required'
            [[ -s "docs/releases/${tag}.md" ]] || release_fail "release notes missing: docs/releases/${tag}.md"
            jq -e --arg version "${version}" '.version == $version' crates/serial-desktop/package.json >/dev/null
            jq -e --arg version "${version}" '.version == $version and .packages[""].version == $version' crates/serial-desktop/package-lock.json >/dev/null
            printf 'version=%s\ntag=%s\nrelease=true\n' "${version}" "${tag}" >>"${GITHUB_OUTPUT}"
            ;;
        tag) ensure_release_tag "${tag}" "${head}" ;;
        *) release_fail 'usage: ci/release-actions.sh prepare|validate-pr|tag' ;;
    esac
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
    release_actions_main "$@"
fi
