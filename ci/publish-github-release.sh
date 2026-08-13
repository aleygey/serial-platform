#!/usr/bin/env bash

set -Eeuo pipefail

readonly SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
readonly PROJECT_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
readonly ARTIFACT_DIR="${PROJECT_ROOT}/target/artifacts"
readonly GITHUB_REPOSITORY="aleygey/serial-platform"

CURRENT_PHASE="startup"
TEMP_DIR=""

log() {
    printf '\n==> %s\n' "$*"
}

fail() {
    printf 'error: %s\n' "$*" >&2
    exit 1
}

on_error() {
    local exit_code=$?
    printf 'error: phase "%s" failed with exit code %s\n' \
        "${CURRENT_PHASE}" "${exit_code}" >&2
    exit "${exit_code}"
}

cleanup() {
    if [[ -n "${TEMP_DIR}" && -d "${TEMP_DIR}" ]]; then
        rm -rf -- "${TEMP_DIR}"
    fi
}

trap on_error ERR
trap cleanup EXIT

require_command() {
    command -v "$1" >/dev/null 2>&1 || fail "required command not found: $1"
}

asset_sha256() {
    sha256sum "$1" | awk '{print $1}'
}

download_asset() {
    local tag="$1"
    local name="$2"
    local destination_dir="$3"

    mkdir -p "${destination_dir}"
    gh release download "${tag}" \
        --repo "${GITHUB_REPOSITORY}" \
        --pattern "${name}" \
        --dir "${destination_dir}"
    [[ -f "${destination_dir}/${name}" ]] \
        || fail "GitHub Release asset download is missing: ${name}"
}

assert_same_asset() {
    local local_path="$1"
    local remote_path="$2"
    local local_digest
    local remote_digest
    local_digest="$(asset_sha256 "${local_path}")"
    remote_digest="$(asset_sha256 "${remote_path}")"
    [[ "${local_digest}" == "${remote_digest}" ]] \
        || fail "GitHub Release already has a different asset: $(basename "${local_path}")"
}

main() {
    CURRENT_PHASE="validation"

    local build_profile="${1:-}"
    [[ "${build_profile}" == "release" ]] \
        || fail "GitHub Release publishing accepts only the release profile"

    require_command git
    require_command cargo
    require_command gh
    require_command jq
    require_command sha256sum
    require_command awk
    require_command sort
    require_command comm
    require_command cmp
    require_command sed
    require_command grep
    require_command find
    require_command mktemp
    require_command tr
    require_command basename

    [[ -n "${GH_TOKEN:-}" ]] \
        || fail "GH_TOKEN is required; bind Jenkins credential github-release-token"

    cd "${PROJECT_ROOT}"

    local package_version
    package_version="$(
        cargo metadata --locked --no-deps --format-version 1 \
            | jq -er '
                [.packages[] | select(.source == null) | .version]
                | unique
                | if length == 1 then .[0] else error("workspace versions differ") end
            '
    )"
    [[ "${package_version}" =~ ^(0|[1-9][0-9]*)\.[0-9]+\.[0-9]+([+-][0-9A-Za-z.-]+)?$ ]] \
        || fail "workspace version is not a supported SemVer: ${package_version}"

    local tag="v${package_version}"
    local head_commit
    local tag_commit
    head_commit="$(git rev-parse HEAD)"
    [[ "$(git cat-file -t "${tag}" 2>/dev/null || true)" == "tag" ]] \
        || fail "${tag} must be an annotated local tag"
    tag_commit="$(git rev-parse "${tag}^{}")"
    [[ "${tag_commit}" == "${head_commit}" ]] \
        || fail "${tag} points to ${tag_commit}, but Jenkins checked out ${head_commit}"

    local remote_ref_json
    local remote_tag_json
    remote_ref_json="$(
        gh api "repos/${GITHUB_REPOSITORY}/git/ref/tags/${tag}"
    )" || fail "${tag} is not available on GitHub"
    local remote_tag_commit
    [[ "$(jq -er '.object.type' <<<"${remote_ref_json}")" == "tag" ]] \
        || fail "GitHub ${tag} must be an annotated tag"
    remote_tag_json="$(
        gh api \
            "repos/${GITHUB_REPOSITORY}/git/tags/$(jq -er '.object.sha' <<<"${remote_ref_json}")"
    )"
    [[ "$(jq -er '.object.type' <<<"${remote_tag_json}")" == "commit" ]] \
        || fail "GitHub ${tag} must resolve directly to a commit"
    remote_tag_commit="$(jq -er '.object.sha' <<<"${remote_tag_json}")"
    [[ "${remote_tag_commit}" == "${head_commit}" ]] \
        || fail "GitHub ${tag} points to ${remote_tag_commit}, not ${head_commit}"

    [[ -d "${ARTIFACT_DIR}" ]] \
        || fail "artifact directory is missing: ${ARTIFACT_DIR}"

    local -a archive_names=(
        "serial-platform-v${package_version}-linux-x86_64-ubuntu20.04.tar.gz"
        "serial-platform-v${package_version}-windows-x86_64.zip"
        "serial-platform-v${package_version}-macos-aarch64.zip"
        "serial-platform-v${package_version}-macos-x86_64.zip"
    )
    local -a asset_names=("${archive_names[@]}" "SHA256SUMS")

    local name
    for name in "${asset_names[@]}"; do
        [[ -f "${ARTIFACT_DIR}/${name}" ]] \
            || fail "required four-platform Release asset is missing: ${name}"
    done
    if find "${ARTIFACT_DIR}" -maxdepth 1 -type f -name '*debug*' -print -quit \
        | grep -q .; then
        fail "Debug artifacts are forbidden in a GitHub Release"
    fi

    TEMP_DIR="$(mktemp -d)"
    local expected_assets_file="${TEMP_DIR}/expected-assets"
    local actual_assets_file="${TEMP_DIR}/actual-assets"
    local expected_checksums_file="${TEMP_DIR}/expected-checksums"
    local actual_checksums_file="${TEMP_DIR}/actual-checksums"
    printf '%s\n' "${asset_names[@]}" | LC_ALL=C sort >"${expected_assets_file}"
    find "${ARTIFACT_DIR}" -maxdepth 1 -type f -print \
        | while IFS= read -r path; do basename "${path}"; done \
        | LC_ALL=C sort >"${actual_assets_file}"
    if ! cmp -s "${expected_assets_file}" "${actual_assets_file}"; then
        comm -23 "${expected_assets_file}" "${actual_assets_file}" \
            | sed 's/^/missing asset: /' >&2
        comm -13 "${expected_assets_file}" "${actual_assets_file}" \
            | sed 's/^/unexpected asset: /' >&2
        fail "artifact directory does not contain the exact Release asset set"
    fi

    printf '%s\n' "${archive_names[@]}" | LC_ALL=C sort >"${expected_checksums_file}"
    awk '{print $2}' "${ARTIFACT_DIR}/SHA256SUMS" | sed 's/^\*//' \
        | LC_ALL=C sort >"${actual_checksums_file}"
    cmp -s "${expected_checksums_file}" "${actual_checksums_file}" \
        || fail "SHA256SUMS must contain each of the four archives exactly once"
    (
        cd "${ARTIFACT_DIR}"
        sha256sum --check --strict SHA256SUMS
    )

    CURRENT_PHASE="release-discovery"
    local release_json="${TEMP_DIR}/release.json"
    local release_exists=false
    if gh release view "${tag}" \
        --repo "${GITHUB_REPOSITORY}" \
        --json isDraft,assets >"${release_json}" 2>"${TEMP_DIR}/release-view.err"; then
        release_exists=true
    else
        if gh api "repos/${GITHUB_REPOSITORY}/releases/tags/${tag}" \
            >/dev/null 2>"${TEMP_DIR}/release-api.err"; then
            fail "gh release view failed even though GitHub reports ${tag} exists"
        fi
        local api_error
        api_error="$(tr '\n' ' ' <"${TEMP_DIR}/release-api.err")"
        grep -Eiq 'HTTP 404|Not Found' <<<"${api_error}" \
            || fail "could not query GitHub Release ${tag}: ${api_error}"
    fi

    if [[ "${release_exists}" == "false" ]]; then
        log "Creating draft GitHub Release ${tag}"
        gh release create "${tag}" \
            --repo "${GITHUB_REPOSITORY}" \
            --verify-tag \
            --draft \
            --title "Serial Platform ${tag}" \
            --generate-notes
        gh release view "${tag}" \
            --repo "${GITHUB_REPOSITORY}" \
            --json isDraft,assets >"${release_json}"
    fi

    local is_draft
    is_draft="$(jq -er '.isDraft' "${release_json}")"
    local remote_names_file="${TEMP_DIR}/remote-assets"
    jq -r '.assets[].name' "${release_json}" | LC_ALL=C sort >"${remote_names_file}"
    local unexpected_remote
    unexpected_remote="$(comm -13 "${expected_assets_file}" "${remote_names_file}")"
    [[ -z "${unexpected_remote}" ]] \
        || fail "GitHub Release contains unexpected assets: ${unexpected_remote//$'\n'/, }"

    CURRENT_PHASE="asset-upload"
    for name in "${asset_names[@]}"; do
        local remote_count
        remote_count="$(jq --arg name "${name}" '[.assets[] | select(.name == $name)] | length' "${release_json}")"
        case "${remote_count}" in
            0)
                [[ "${is_draft}" == "true" ]] \
                    || fail "published GitHub Release is missing asset: ${name}"
                log "Uploading ${name}"
                gh release upload "${tag}" \
                    "${ARTIFACT_DIR}/${name}" \
                    --repo "${GITHUB_REPOSITORY}"
                ;;
            1)
                local remote_dir="${TEMP_DIR}/existing/${name}"
                download_asset "${tag}" "${name}" "${remote_dir}"
                assert_same_asset \
                    "${ARTIFACT_DIR}/${name}" \
                    "${remote_dir}/${name}"
                log "Keeping identical existing asset ${name}"
                ;;
            *)
                fail "GitHub Release contains duplicate assets named ${name}"
                ;;
        esac
    done

    CURRENT_PHASE="remote-verification"
    local verification_dir="${TEMP_DIR}/verification"
    for name in "${asset_names[@]}"; do
        local asset_dir="${verification_dir}/${name}"
        download_asset "${tag}" "${name}" "${asset_dir}"
        assert_same_asset \
            "${ARTIFACT_DIR}/${name}" \
            "${asset_dir}/${name}"
    done

    if [[ "${is_draft}" == "true" ]]; then
        CURRENT_PHASE="publish"
        log "Publishing verified GitHub Release ${tag}"
        gh release edit "${tag}" \
            --repo "${GITHUB_REPOSITORY}" \
            --draft=false \
            --latest
    else
        log "GitHub Release ${tag} is already published with identical assets"
    fi
}

main "$@"
