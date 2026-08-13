#!/usr/bin/env bash

set -Eeuo pipefail

readonly SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
readonly PROJECT_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
readonly ARTIFACT_DIR="${PROJECT_ROOT}/target/macos-artifacts"
readonly PACKAGE_VERSION="$(sed -n 's/^version = "\(.*\)"/\1/p' "${PROJECT_ROOT}/Cargo.toml" | head -n 1)"
readonly GIT_COMMIT="$(git -C "${PROJECT_ROOT}" rev-parse HEAD)"
readonly SOURCE_DATE_EPOCH="$(git -C "${PROJECT_ROOT}" show -s --format=%ct HEAD)"
readonly DEPLOYMENT_TARGET="11.0"
readonly MCP_TOOL_COUNT=18
readonly ARM_TARGET="aarch64-apple-darwin"
readonly X86_TARGET="x86_64-apple-darwin"
readonly -a REQUIRED_BINARIES=(serial seriald serialctl serial-mcp)
readonly -a BUILD_PACKAGES=(serial-cli seriald serialctl serial-mcp)

export MACOSX_DEPLOYMENT_TARGET="${DEPLOYMENT_TARGET}"
export COPYFILE_DISABLE=1
export SOURCE_DATE_EPOCH
# macOS does not ship the Linux-style C.UTF-8 locale. A stable C locale also
# keeps tool output deterministic and avoids Perl warnings from shasum.
export LANG=C
export LC_ALL=C
export TZ=UTC

CURRENT_PHASE="startup"

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

trap on_error ERR

require_command() {
    command -v "$1" >/dev/null 2>&1 || fail "required command not found: $1"
}

host_target() {
    rustc -vV | sed -n 's/^host: //p'
}

ensure_rust_target() {
    local target="$1"

    if rustup target list --installed | grep -Fx "${target}" >/dev/null; then
        return 0
    fi
    rustup target add "${target}"
}

ensure_build_environment() {
    [[ "$(uname -s)" == "Darwin" ]] \
        || fail "macOS packages must be built on a native macOS agent"
    [[ "$(uname -m)" == "arm64" ]] \
        || fail "this builder requires an Apple Silicon (arm64) macOS agent"

    local command
    for command in \
        git rustc cargo rustup cc jq file strings shasum \
        zip unzip xcrun arch codesign; do
        require_command "${command}"
    done

    local developer_tool
    for developer_tool in lipo otool vtool; do
        xcrun --find "${developer_tool}" >/dev/null \
            || fail "required Apple developer tool not found: ${developer_tool}"
    done

    local sdk_path
    sdk_path="$(xcrun --sdk macosx --show-sdk-path)"
    [[ -d "${sdk_path}" ]] || fail "the macOS SDK path is missing: ${sdk_path}"
    export SDKROOT="${sdk_path}"

    [[ "$(host_target)" == "${ARM_TARGET}" ]] \
        || fail "Rust host must be ${ARM_TARGET}; found $(host_target)"
    arch -x86_64 /usr/bin/true \
        || fail "Rosetta 2 is required for x86_64 package smoke tests"
}

assert_workspace_version() {
    cargo metadata --locked --no-deps --format-version 1 \
        | jq -e --arg version "${PACKAGE_VERSION}" \
            '.packages | map(select(.source == null)) | all(.version == $version)' \
            >/dev/null \
        || fail "workspace package versions do not all match ${PACKAGE_VERSION}"
}

assert_profile() {
    case "$1" in
        release|debug)
            ;;
        *)
            fail "profile must be release or debug: $1"
            ;;
    esac
}

profile_directory() {
    case "$1" in
        release)
            printf 'release\n'
            ;;
        debug)
            printf 'debug\n'
            ;;
    esac
}

profile_suffix() {
    case "$1" in
        release)
            printf '\n'
            ;;
        debug)
            printf '%s\n' '-debug'
            ;;
    esac
}

run_environment() {
    CURRENT_PHASE="environment"
    log "Checking the native macOS release environment"

    ensure_build_environment

    git --version
    rustc --version
    cargo --version
    cargo fmt --version
    cargo clippy --version
    cc --version | sed -n '1p'
    printf 'macOS SDK: %s (%s)\n' \
        "${SDKROOT}" "$(xcrun --sdk macosx --show-sdk-version)"
    printf 'deployment target: %s\n' "${MACOSX_DEPLOYMENT_TARGET}"
    rustc -vV

    ensure_rust_target "${ARM_TARGET}"
    ensure_rust_target "${X86_TARGET}"
    assert_workspace_version
}

run_fetch() {
    CURRENT_PHASE="fetch"
    log "Fetching locked Cargo dependencies"
    require_command cargo
    cargo fetch --locked
}

run_test() {
    local profile="${1:-debug}"
    assert_profile "${profile}"
    CURRENT_PHASE="test"
    log "Running the locked workspace tests for both macOS architectures"
    ensure_build_environment
    run_target_tests "${profile}"
}

run_target_tests() {
    local profile="$1"

    ensure_rust_target "${ARM_TARGET}"
    ensure_rust_target "${X86_TARGET}"

    CURRENT_PHASE="test-${profile}-arm64"
    log "Running the ${profile} workspace tests on native arm64"
    case "${profile}" in
        release)
            cargo test \
                --workspace \
                --release \
                --locked \
                --target-dir "${PROJECT_ROOT}/target/macos-package-tests" \
                --target "${ARM_TARGET}"
            ;;
        debug)
            cargo test \
                --workspace \
                --locked \
                --target-dir "${PROJECT_ROOT}/target/macos-package-tests" \
                --target "${ARM_TARGET}"
            ;;
    esac

    CURRENT_PHASE="test-${profile}-x86_64"
    log "Running the ${profile} workspace tests as x86_64 through Rosetta"
    case "${profile}" in
        release)
            CARGO_TARGET_X86_64_APPLE_DARWIN_RUNNER="arch -x86_64" \
                cargo test \
                    --workspace \
                    --release \
                    --locked \
                    --target-dir "${PROJECT_ROOT}/target/macos-package-tests" \
                    --target "${X86_TARGET}"
            ;;
        debug)
            CARGO_TARGET_X86_64_APPLE_DARWIN_RUNNER="arch -x86_64" \
                cargo test \
                    --workspace \
                    --locked \
                    --target-dir "${PROJECT_ROOT}/target/macos-package-tests" \
                    --target "${X86_TARGET}"
            ;;
    esac
}

build_target() {
    local target="$1"
    local profile="$2"
    local -a cargo_arguments=(build --locked --target "${target}")
    local package

    for package in "${BUILD_PACKAGES[@]}"; do
        cargo_arguments+=(-p "${package}")
    done

    log "Building ${target} (${profile}) with deployment target ${DEPLOYMENT_TARGET}"
    ensure_rust_target "${target}"
    case "${profile}" in
        release)
            cargo "${cargo_arguments[@]}" --release
            ;;
        debug)
            cargo "${cargo_arguments[@]}"
            ;;
    esac
}

assert_version_marker() {
    local binary="$1"
    grep -aF -m 1 "${PACKAGE_VERSION}" "${binary}" >/dev/null \
        || fail "binary does not contain version ${PACKAGE_VERSION}: ${binary}"
}

assert_macho() {
    local binary="$1"
    local architecture="$2"
    local description
    local actual_architectures
    local macho_architecture

    description="$(file -b "${binary}")"
    printf '%s: %s\n' "$(basename "${binary}")" "${description}"
    case "${architecture}" in
        aarch64)
            grep -Eq 'Mach-O 64-bit executable arm64' <<<"${description}" \
                || fail "not an arm64 Mach-O executable: ${binary}"
            macho_architecture="arm64"
            ;;
        x86_64)
            grep -Eq 'Mach-O 64-bit executable x86_64' <<<"${description}" \
                || fail "not an x86_64 Mach-O executable: ${binary}"
            macho_architecture="x86_64"
            ;;
    esac

    actual_architectures="$(xcrun lipo -archs "${binary}")"
    [[ "${actual_architectures}" == "${macho_architecture}" ]] \
        || fail "unexpected Mach-O architectures (${actual_architectures}): ${binary}"

    local vtool_info
    local otool_info
    vtool_info="$(xcrun vtool -show-build "${binary}")"
    otool_info="$(xcrun otool -l "${binary}")"
    grep -Eq "minos[[:space:]]+${DEPLOYMENT_TARGET}([[:space:]]|$)" \
        <<<"${vtool_info}" \
        || fail "vtool did not report deployment target ${DEPLOYMENT_TARGET}: ${binary}"
    grep -Eq "minos[[:space:]]+${DEPLOYMENT_TARGET}([[:space:]]|$)" \
        <<<"${otool_info}" \
        || fail "otool did not report deployment target ${DEPLOYMENT_TARGET}: ${binary}"

    local dependencies
    dependencies="$(
        xcrun otool -L "${binary}" \
            | sed -n '2,$s/^[[:space:]]*\([^[:space:]]*\).*/\1/p'
    )"
    local dependency
    while IFS= read -r dependency; do
        [[ -z "${dependency}" ]] && continue
        case "${dependency}" in
            /usr/lib/*|/System/Library/*)
                ;;
            *)
                fail "unexpected non-system Mach-O dependency ${dependency}: ${binary}"
                ;;
        esac
    done <<<"${dependencies}"

    assert_version_marker "${binary}"
}

smoke_binary() {
    local binary="$1"
    local architecture="$2"

    case "${architecture}" in
        aarch64)
            "${binary}" --version | grep -F "${PACKAGE_VERSION}" >/dev/null \
                || fail "native binary does not report version ${PACKAGE_VERSION}: ${binary}"
            ;;
        x86_64)
            arch -x86_64 "${binary}" --version | grep -F "${PACKAGE_VERSION}" >/dev/null \
                || fail "Rosetta binary does not report version ${PACKAGE_VERSION}: ${binary}"
            ;;
    esac
}

smoke_package_binaries() {
    local package_dir="$1"
    local architecture="$2"

    local binary
    for binary in "${REQUIRED_BINARIES[@]}"; do
        smoke_binary "${package_dir}/${binary}" "${architecture}"
    done

    local tool_count
    case "${architecture}" in
        aarch64)
            "${package_dir}/serial" --help >/dev/null
            tool_count="$("${package_dir}/serial-mcp" --dump-tools | jq -e 'length')"
            ;;
        x86_64)
            arch -x86_64 "${package_dir}/serial" --help >/dev/null
            tool_count="$(
                arch -x86_64 "${package_dir}/serial-mcp" --dump-tools \
                    | jq -e 'length'
            )"
            ;;
    esac
    [[ "${tool_count}" == "${MCP_TOOL_COUNT}" ]] \
        || fail "${architecture} serial-mcp exposes ${tool_count} tools; expected ${MCP_TOOL_COUNT}"
}

assert_no_distribution_signature() {
    local package_dir="$1"
    local binary

    for binary in "${REQUIRED_BINARIES[@]}"; do
        local signature_info
        if ! codesign -dv "${package_dir}/${binary}" >/dev/null 2>&1; then
            continue
        fi
        signature_info="$(codesign -dv "${package_dir}/${binary}" 2>&1)"
        if grep -Eiq 'Signature=adhoc|flags=.*adhoc' <<<"${signature_info}"; then
            continue
        fi
        fail "unexpected distribution signature; BUILD-INFO would be inaccurate: ${package_dir}/${binary}"
    done
}

copy_release_materials() {
    local package_dir="$1"

    cp "${PROJECT_ROOT}/README.md" "${package_dir}/"
    cp "${PROJECT_ROOT}/DOCUMENTATION.md" "${package_dir}/"
    cp "${PROJECT_ROOT}/ROADMAP.md" "${package_dir}/"
    cp "${PROJECT_ROOT}/LICENSE" "${package_dir}/"
    cp -R "${PROJECT_ROOT}/adapters" "${package_dir}/"
    cp -R "${PROJECT_ROOT}/docs" "${package_dir}/"
}

write_build_info() {
    local destination="$1"
    local rust_target="$2"
    local architecture="$3"
    local profile="$4"

    jq -n \
        --arg version "${PACKAGE_VERSION}" \
        --arg git_commit "${GIT_COMMIT}" \
        --argjson source_date_epoch "${SOURCE_DATE_EPOCH}" \
        --arg rust_target "${rust_target}" \
        --arg architecture "${architecture}" \
        --arg cargo_profile "${profile}" \
        --arg deployment_target "${DEPLOYMENT_TARGET}" \
        --arg macos_sdk "$(xcrun --sdk macosx --show-sdk-version)" \
        --arg rustc "$(rustc --version)" \
        --argjson mcp_tool_count "${MCP_TOOL_COUNT}" \
        '{
            version: $version,
            git_commit: $git_commit,
            source_date_epoch: $source_date_epoch,
            rust_target: $rust_target,
            architecture: $architecture,
            cargo_profile: $cargo_profile,
            compatibility: ("macOS " + $deployment_target + "+; " + $architecture),
            deployment_target: $deployment_target,
            macos_sdk: $macos_sdk,
            rustc: $rustc,
            mcp_tool_count: $mcp_tool_count,
            build_system: "Jenkins-compatible native macOS builder",
            distribution: {
                signing_status: "unsigned (Mach-O ad-hoc signatures may be present)",
                developer_id_signed: false,
                notarization_status: "not_submitted",
                notarized: false
            }
        }' >"${destination}"
}

write_package_manifest() {
    local package_dir="$1"
    local manifest_file="${package_dir}/MANIFEST.sha256"

    : >"${manifest_file}"
    while IFS= read -r file; do
        local relative_path="${file#"${package_dir}/"}"
        local digest
        digest="$(shasum -a 256 "${file}" | awk '{print $1}')"
        printf '%s  %s\n' "${digest}" "${relative_path}" >>"${manifest_file}"
    done < <(
        find "${package_dir}" -type f ! -name MANIFEST.sha256 -print \
            | LC_ALL=C sort
    )
    (
        cd "${package_dir}"
        shasum -a 256 -c MANIFEST.sha256
    )
}

normalize_package_metadata() {
    local package_dir="$1"
    local binary

    # ZIP stores Unix permission bits. Normalize them as well as timestamps so
    # different agent umasks or checkout settings cannot change the archive.
    find "${package_dir}" -type d -exec chmod 0755 {} +
    find "${package_dir}" -type f -exec chmod 0644 {} +
    for binary in "${REQUIRED_BINARIES[@]}"; do
        chmod 0755 "${package_dir}/${binary}"
    done

    # BSD touch lacks GNU's -d @epoch form. Perl is shipped by macOS and lets
    # us set both atime and mtime exactly without depending on the local zone.
    find "${package_dir}" -print0 \
        | perl -0ne \
            'die "SOURCE_DATE_EPOCH missing\n" unless defined $ENV{SOURCE_DATE_EPOCH}; chomp; utime($ENV{SOURCE_DATE_EPOCH}, $ENV{SOURCE_DATE_EPOCH}, $_) or die "utime $_: $!\n"'
}

create_zip_archive() {
    local package_name="$1"
    local archive="${ARTIFACT_DIR}/${package_name}.zip"

    rm -f "${archive}"
    (
        cd "${ARTIFACT_DIR}"
        find "${package_name}" -print \
            | LC_ALL=C sort \
            | zip -X -q "${archive}" -@
    )
    unzip -tq "${archive}"
    local archive_entries
    archive_entries="$(unzip -Z1 "${archive}")"
    grep -Fx "${package_name}/docs/MCP_TOOLS.md" <<<"${archive_entries}" >/dev/null
    local binary
    for binary in "${REQUIRED_BINARIES[@]}"; do
        grep -Fx "${package_name}/${binary}" <<<"${archive_entries}" >/dev/null
    done
    grep -Fx "${package_name}/BUILD-INFO.json" <<<"${archive_entries}" >/dev/null
    grep -Fx "${package_name}/MANIFEST.sha256" <<<"${archive_entries}" >/dev/null
}

package_target() {
    local target="$1"
    local architecture="$2"
    local profile="$3"
    local release_dir="${PROJECT_ROOT}/target/${target}/$(profile_directory "${profile}")"
    local package_name="serial-platform-v${PACKAGE_VERSION}$(profile_suffix "${profile}")-macos-${architecture}"
    local package_dir="${ARTIFACT_DIR}/${package_name}"

    [[ -d "${release_dir}" ]] \
        || fail "Cargo output directory is missing: ${release_dir}"
    rm -rf "${package_dir}"
    mkdir -p "${package_dir}"

    local binary
    for binary in "${REQUIRED_BINARIES[@]}"; do
        [[ -f "${release_dir}/${binary}" ]] \
            || fail "built binary is missing: ${release_dir}/${binary}"
        install -m 0755 "${release_dir}/${binary}" "${package_dir}/${binary}"
        assert_macho "${package_dir}/${binary}" "${architecture}"
    done

    smoke_package_binaries "${package_dir}" "${architecture}"
    assert_no_distribution_signature "${package_dir}"
    copy_release_materials "${package_dir}"
    write_build_info \
        "${package_dir}/BUILD-INFO.json" \
        "${target}" \
        "${architecture}" \
        "${profile}"
    write_package_manifest "${package_dir}"
    normalize_package_metadata "${package_dir}"
    create_zip_archive "${package_name}"
    rm -rf "${package_dir}"
    printf 'packaged %s\n' "${package_name}"
}

write_archive_checksums() {
    local -a archives=("${ARTIFACT_DIR}"/*.zip)
    ((${#archives[@]} == 2)) \
        || fail "expected exactly two macOS archives, found ${#archives[@]}"

    (
        cd "${ARTIFACT_DIR}"
        : >SHA256SUMS
        local archive
        for archive in "${archives[@]}"; do
            shasum -a 256 "$(basename "${archive}")" >>SHA256SUMS
        done
        shasum -a 256 -c SHA256SUMS
    )
}

run_package() {
    local profile="$1"
    assert_profile "${profile}"
    CURRENT_PHASE="package-${profile}"

    ensure_build_environment
    assert_workspace_version

    if [[ -e "${ARTIFACT_DIR}" ]]; then
        case "${ARTIFACT_DIR}" in
            "${PROJECT_ROOT}"/target/macos-artifacts)
                rm -rf "${ARTIFACT_DIR}"
                ;;
            *)
                fail "refusing to clean unexpected artifact directory: ${ARTIFACT_DIR}"
                ;;
        esac
    fi
    mkdir -p "${ARTIFACT_DIR}"

    build_target "${ARM_TARGET}" "${profile}"
    build_target "${X86_TARGET}" "${profile}"
    package_target "${ARM_TARGET}" "aarch64" "${profile}"
    package_target "${X86_TARGET}" "x86_64" "${profile}"
    write_archive_checksums
}

run_all() {
    local profile="$1"
    assert_profile "${profile}"
    run_environment
    run_fetch
    run_test "${profile}"
    run_package "${profile}"
}

usage() {
    cat <<'EOF'
Usage: ci/build-macos.sh <command> [profile]

Commands:
  env                Check the native Apple Silicon macOS toolchain and Rosetta
  fetch              Download dependencies pinned by Cargo.lock
  test [profile]     Run both-architecture tests (default profile: debug)
  package <profile>  Build, verify and package aarch64 + x86_64 (release|debug)
  all <profile>      Run env, fetch, test and package (release|debug)
EOF
}

cd "${PROJECT_ROOT}"

case "${1:-}" in
    env)
        [[ "$#" == 1 ]] || fail "env does not accept a profile"
        run_environment
        ;;
    fetch)
        [[ "$#" == 1 ]] || fail "fetch does not accept a profile"
        run_fetch
        ;;
    test)
        [[ "$#" -le 2 ]] || fail "test accepts at most one profile"
        run_test "${2:-debug}"
        ;;
    package)
        [[ "$#" == 2 ]] || fail "package requires exactly one profile: release or debug"
        run_package "$2"
        ;;
    all)
        [[ "$#" == 2 ]] || fail "all requires exactly one profile: release or debug"
        run_all "$2"
        ;;
    help|-h|--help)
        usage
        ;;
    *)
        usage >&2
        exit 2
        ;;
esac
