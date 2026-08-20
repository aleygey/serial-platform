#!/usr/bin/env bash

set -Eeuo pipefail

readonly SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
readonly PROJECT_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
readonly ARTIFACT_DIR="${PROJECT_ROOT}/target/artifacts"
readonly PACKAGE_VERSION="$(sed -n 's/^version = "\(.*\)"/\1/p' "${PROJECT_ROOT}/Cargo.toml" | head -n 1)"
readonly GIT_COMMIT="$(git -C "${PROJECT_ROOT}" rev-parse HEAD)"
readonly SOURCE_DATE_EPOCH="$(git -C "${PROJECT_ROOT}" show -s --format=%ct HEAD)"
readonly MCP_TOOL_COUNT=19

CURRENT_PHASE="startup"
BUILD_PROFILE="release"
PROFILE_DIR="release"
CARGO_PROFILE_ARGS=(--release)

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

configure_profile() {
    local requested_profile="${1:-release}"

    case "${requested_profile}" in
        release)
            BUILD_PROFILE="release"
            PROFILE_DIR="release"
            CARGO_PROFILE_ARGS=(--release)
            ;;
        debug)
            BUILD_PROFILE="debug"
            PROFILE_DIR="debug"
            # Keep the array non-empty for Bash 3.2 + `set -u` (the default
            # shell on macOS Jenkins helpers). Cargo's built-in `dev` profile
            # writes to the conventional target/debug directory.
            CARGO_PROFILE_ARGS=(--profile dev)
            ;;
        *)
            fail "unsupported build profile: ${requested_profile}; expected release or debug"
            ;;
    esac
}

profile_suffix() {
    if [[ "${BUILD_PROFILE}" == "debug" ]]; then
        printf '%s\n' '-debug'
    fi
}

host_output_dir() {
    printf '%s\n' "${PROJECT_ROOT}/target/${PROFILE_DIR}"
}

host_target() {
    rustc -vV | sed -n 's/^host: //p'
}

ensure_rust_target() {
    local target="$1"

    [[ "$(host_target)" == "${target}" ]] && return 0
    if command -v rustup >/dev/null 2>&1; then
        rustup target add "${target}"
    else
        rustc --print target-libdir --target "${target}" >/dev/null 2>&1 \
            || fail "Rust target is not installed and rustup is unavailable: ${target}"
    fi
}

assert_workspace_version() {
    cargo metadata --locked --no-deps --format-version 1 \
        | jq -e --arg version "${PACKAGE_VERSION}" \
            '.packages | map(select(.source == null)) | all(.version == $version)' \
            >/dev/null \
        || fail "workspace package versions do not all match ${PACKAGE_VERSION}"
}

dump_tool_count() {
    local binary="$(host_output_dir)/serial-mcp"
    [[ -x "${binary}" ]] \
        || fail "host serial-mcp is missing; run ci/build.sh build ${BUILD_PROFILE} before packaging"
    "${binary}" --dump-tools | jq -e 'length' \
        || fail "serial-mcp --dump-tools did not return a JSON array"
}

run_environment() {
    CURRENT_PHASE="environment"
    log "Checking Rust release environment"

    local command
    for command in \
        git rustc cargo cc cargo-zigbuild zig \
        jq file strings readelf sha256sum tar gzip zip unzip; do
        require_command "${command}"
    done
    require_command x86_64-w64-mingw32-gcc
    require_command x86_64-w64-mingw32-ar
    require_command x86_64-w64-mingw32-objdump

    git --version
    rustc --version
    cargo --version
    cargo fmt --version
    cargo clippy --version
    cargo zigbuild --help >/dev/null
    zig version
    cc --version | sed -n '1p'
    rustc -vV

    assert_workspace_version
}

run_fetch() {
    CURRENT_PHASE="fetch"
    log "Fetching locked Cargo dependencies"
    cargo fetch --locked
}

run_fmt() {
    CURRENT_PHASE="format"
    log "Checking Rust formatting"
    cargo fmt --all -- --check
}

run_clippy() {
    CURRENT_PHASE="clippy"
    log "Running Clippy"
    cargo clippy --workspace --all-targets --locked -- -D warnings
}

run_test() {
    CURRENT_PHASE="test"
    log "Running workspace tests"
    cargo test --workspace --locked
}

run_build() {
    CURRENT_PHASE="build"
    log "Building host ${BUILD_PROFILE} binaries"
    cargo build \
        "${CARGO_PROFILE_ARGS[@]}" \
        --locked \
        -p serial-cli \
        -p seriald \
        -p serialctl \
        -p serial-mcp \
        -p serial-desktop
}

run_smoke() {
    CURRENT_PHASE="smoke"
    log "Smoke-testing host ${BUILD_PROFILE} binaries"

    local output_dir
    output_dir="$(host_output_dir)"

    local binary
    for binary in serial seriald serialctl serial-mcp serial-desktop; do
        [[ -x "${output_dir}/${binary}" ]] \
            || fail "${BUILD_PROFILE} binary is missing or not executable: ${output_dir}/${binary}"
        "${output_dir}/${binary}" --version | grep -F "${PACKAGE_VERSION}" >/dev/null \
            || fail "${binary} does not report version ${PACKAGE_VERSION}"
    done

    "${output_dir}/serial" --help >/dev/null
    "${output_dir}/serial-desktop" --help >/dev/null
    local tool_count
    tool_count="$(dump_tool_count)"
    [[ "${tool_count}" == "${MCP_TOOL_COUNT}" ]] \
        || fail "serial-mcp exposes ${tool_count} tools; expected ${MCP_TOOL_COUNT}"
}

resolve_profile_dir() {
    local target="$1"
    local versioned_target="${2:-}"

    if [[ -d "${PROJECT_ROOT}/target/${target}/${PROFILE_DIR}" ]]; then
        printf '%s\n' "${PROJECT_ROOT}/target/${target}/${PROFILE_DIR}"
    elif [[ -n "${versioned_target}" && -d "${PROJECT_ROOT}/target/${versioned_target}/${PROFILE_DIR}" ]]; then
        printf '%s\n' "${PROJECT_ROOT}/target/${versioned_target}/${PROFILE_DIR}"
    else
        fail "Cargo ${BUILD_PROFILE} directory is missing for ${target}"
    fi
}

assert_binary_version_marker() {
    local binary="$1"
    strings "${binary}" | grep -F "${PACKAGE_VERSION}" >/dev/null \
        || fail "binary does not contain the workspace version marker ${PACKAGE_VERSION}: ${binary}"
}

assert_linux_binary() {
    local binary="$1"
    local description
    description="$(file -b "${binary}")"
    printf '%s: %s\n' "$(basename "${binary}")" "${description}"
    grep -Eq 'ELF 64-bit LSB.*x86-64' <<<"${description}" \
        || fail "not an x86_64 Linux ELF binary: ${binary}"
    readelf -l "${binary}" | grep -F '/lib64/ld-linux-x86-64.so.2' >/dev/null \
        || fail "unexpected Linux ELF interpreter: ${binary}"

    local highest_glibc
    highest_glibc="$(
        readelf --version-info "${binary}" \
            | grep -o 'GLIBC_[0-9][0-9.]*' \
            | sort -Vu \
            | tail -n 1
    )"
    [[ -n "${highest_glibc}" ]] \
        || fail "no GLIBC version requirement found in ${binary}"
    local newest
    newest="$(printf '%s\n' "${highest_glibc}" 'GLIBC_2.31' | sort -V | tail -n 1)"
    [[ "${newest}" == 'GLIBC_2.31' ]] \
        || fail "${binary} requires ${highest_glibc}, newer than GLIBC_2.31"

    local needed
    needed="$(
        readelf -d "${binary}" \
            | sed -n 's/.*Shared library: \[\([^]]*\)\].*/\1/p'
    )"
    printf '%s\n' "${needed}"
    local library
    while IFS= read -r library; do
        [[ -z "${library}" ]] && continue
        case "${library}" in
            libc.so.6|libm.so.6|libgcc_s.so.1|libpthread.so.0|libdl.so.2|librt.so.1|libutil.so.1|ld-linux-x86-64.so.2)
                ;;
            libX11.so.6|libX11-xcb.so.1|libXcursor.so.1|libXfixes.so.3|libXi.so.6|libXinerama.so.1|libXrandr.so.2|libXrender.so.1|libxcb.so.1|libEGL.so.1|libGL.so.1|libxkbcommon.so.0)
                [[ "$(basename "${binary}")" == "serial-desktop" ]] \
                    || fail "non-desktop Linux binary unexpectedly links GUI library ${library}: ${binary}"
                ;;
            *)
                fail "Linux package has an unexpected dynamic dependency ${library}: ${binary}"
                ;;
        esac
    done <<<"${needed}"
    assert_binary_version_marker "${binary}"
}

assert_windows_binary() {
    local binary="$1"
    local description
    description="$(file -b "${binary}")"
    printf '%s: %s\n' "$(basename "${binary}")" "${description}"
    grep -Eq 'PE32\+ executable.*x86-64' <<<"${description}" \
        || fail "not an x86_64 Windows PE32+ binary: ${binary}"
    local runtime_imports
    runtime_imports="$(
        x86_64-w64-mingw32-objdump -p "${binary}" \
            | sed -n 's/^[[:space:]]*DLL Name: //p'
    )"
    printf '%s\n' "${runtime_imports}"
    if grep -Eiq '^(libgcc_s|libstdc\+\+|libwinpthread|libssp|libiconv|libintl)' \
        <<<"${runtime_imports}"; then
        fail "Windows package has an unbundled MinGW runtime import: ${binary}"
    fi
    assert_binary_version_marker "${binary}"
}

write_build_info() {
    local destination="$1"
    local rust_target="$2"
    local compatibility="$3"
    local tool_count="$4"
    local desktop_runtime="$5"
    local desktop_local_service="$6"

    jq -n \
        --arg version "${PACKAGE_VERSION}" \
        --arg git_commit "${GIT_COMMIT}" \
        --arg rust_target "${rust_target}" \
        --arg compatibility "${compatibility}" \
        --arg desktop_runtime "${desktop_runtime}" \
        --arg desktop_local_service "${desktop_local_service}" \
        --arg cargo_profile "${BUILD_PROFILE}" \
        --arg rustc "$(rustc --version)" \
        --arg zig "$(zig version)" \
        --argjson source_date_epoch "${SOURCE_DATE_EPOCH}" \
        --argjson mcp_tool_count "${tool_count}" \
        '{
            version: $version,
            git_commit: $git_commit,
            source_date_epoch: $source_date_epoch,
            rust_target: $rust_target,
            compatibility: $compatibility,
            cargo_profile: $cargo_profile,
            rustc: $rustc,
            zig: $zig,
            mcp_tool_count: $mcp_tool_count,
            desktop: {
                included: true,
                executable: (if $rust_target | startswith("x86_64-pc-windows") then "serial-desktop.exe" else "serial-desktop" end),
                runtime: $desktop_runtime,
                local_service: $desktop_local_service
            },
            build_system: "Jenkins"
        }' >"${destination}"
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

write_package_manifest() {
    local package_dir="$1"
    (
        cd "${package_dir}"
        find . -type f ! -name MANIFEST.sha256 -print0 \
            | LC_ALL=C sort -z \
            | while IFS= read -r -d '' file; do
                sha256sum "${file}"
            done >MANIFEST.sha256
        sha256sum --check MANIFEST.sha256
    )
}

normalize_package_metadata() {
    local package_dir="$1"
    local binary

    # ZIP records Unix mode bits, so normalize modes as well as timestamps.
    find "${package_dir}" -type d -exec chmod 0755 {} +
    find "${package_dir}" -type f -exec chmod 0644 {} +
    for binary in serial seriald serialctl serial-mcp serial-desktop \
        serial.exe seriald.exe serialctl.exe serial-mcp.exe serial-desktop.exe; do
        if [[ -f "${package_dir}/${binary}" ]]; then
            chmod 0755 "${package_dir}/${binary}"
        fi
    done
    find "${package_dir}" -exec touch -d "@${SOURCE_DATE_EPOCH}" {} +
}

create_tar_archive() {
    local package_name="$1"
    local archive="${ARTIFACT_DIR}/${package_name}.tar.gz"
    (
        cd "${ARTIFACT_DIR}"
        tar \
            --sort=name \
            --mtime="@${SOURCE_DATE_EPOCH}" \
            --owner=0 \
            --group=0 \
            --numeric-owner \
            -cf - \
            "${package_name}" \
            | gzip -n >"${archive}"
    )
    gzip -t "${archive}"
    tar -tzf "${archive}" \
        | grep -Fx "${package_name}/docs/MCP_TOOLS.md" >/dev/null
    tar -tzf "${archive}" \
        | grep -Fx "${package_name}/serial-desktop" >/dev/null
    tar -tzf "${archive}" \
        | grep -Fx "${package_name}/seriald" >/dev/null
}

create_zip_archive() {
    local package_name="$1"
    local archive="${ARTIFACT_DIR}/${package_name}.zip"
    (
        cd "${ARTIFACT_DIR}"
        find "${package_name}" -print \
            | LC_ALL=C sort \
            | zip -X -q "${archive}" -@
    )
    unzip -tq "${archive}"
    unzip -Z1 "${archive}" \
        | grep -Fx "${package_name}/docs/MCP_TOOLS.md" >/dev/null
    unzip -Z1 "${archive}" \
        | grep -Fx "${package_name}/serial-desktop.exe" >/dev/null
}

run_package_target() {
    local target="$1"
    local profile_dir
    local package_name
    local package_dir
    local compatibility
    local build_target
    local desktop_runtime
    local desktop_local_service
    local -a binaries

    case "${target}" in
        x86_64-unknown-linux-gnu)
            local zig_target="x86_64-unknown-linux-gnu.2.31"
            log "Building ${zig_target} with cargo-zigbuild"
            ensure_rust_target "${target}"
            cargo zigbuild \
                "${CARGO_PROFILE_ARGS[@]}" \
                --locked \
                --target "${zig_target}" \
                -p serial-cli \
                -p seriald \
                -p serialctl \
                -p serial-mcp \
                -p serial-desktop
            profile_dir="$(resolve_profile_dir "${target}" "${zig_target}")"
            package_name="serial-platform-v${PACKAGE_VERSION}$(profile_suffix)-linux-x86_64-ubuntu20.04"
            compatibility="Ubuntu 20.04+ x86_64; GNU/Linux glibc <= 2.31"
            build_target="${zig_target}"
            desktop_runtime="X11 display plus system X11/OpenGL runtime libraries"
            desktop_local_service="sibling serial and seriald executables are included"
            binaries=(serial seriald serialctl serial-mcp serial-desktop)
            ;;
        x86_64-pc-windows-gnu)
            log "Building ${target} with the GNU/MinGW-w64 ABI"
            ensure_rust_target "${target}"
            RUSTFLAGS="${RUSTFLAGS:+${RUSTFLAGS} }-C target-feature=+crt-static" \
                cargo build \
                    "${CARGO_PROFILE_ARGS[@]}" \
                    --locked \
                    --target "${target}" \
                    -p serial-cli \
                    -p seriald \
                    -p serialctl \
                    -p serial-mcp \
                    -p serial-desktop
            profile_dir="$(resolve_profile_dir "${target}")"
            package_name="serial-platform-v${PACKAGE_VERSION}$(profile_suffix)-windows-x86_64"
            compatibility="Windows x86_64; GNU/MinGW-w64 ABI (not MSVC)"
            build_target="${target}"
            desktop_runtime="Windows x86_64 desktop environment; MinGW runtime is statically linked"
            desktop_local_service="sibling seriald.exe is included"
            binaries=(serial.exe seriald.exe serialctl.exe serial-mcp.exe serial-desktop.exe)
            ;;
        *)
            fail "unsupported release target: ${target}"
            ;;
    esac

    package_dir="${ARTIFACT_DIR}/${package_name}"
    rm -rf "${package_dir}"
    mkdir -p "${package_dir}"

    local binary
    for binary in "${binaries[@]}"; do
        [[ -f "${profile_dir}/${binary}" ]] \
            || fail "${BUILD_PROFILE} binary is missing: ${profile_dir}/${binary}"
        install -m 0755 "${profile_dir}/${binary}" "${package_dir}/${binary}"
        case "${target}" in
            x86_64-unknown-linux-gnu)
                assert_linux_binary "${package_dir}/${binary}"
                ;;
            x86_64-pc-windows-gnu)
                assert_windows_binary "${package_dir}/${binary}"
                ;;
        esac
    done

    copy_release_materials "${package_dir}"
    local tool_count
    tool_count="$(dump_tool_count)"
    [[ "${tool_count}" == "${MCP_TOOL_COUNT}" ]] \
        || fail "serial-mcp exposes ${tool_count} tools; expected ${MCP_TOOL_COUNT}"
    write_build_info \
        "${package_dir}/BUILD-INFO.json" \
        "${build_target}" \
        "${compatibility}" \
        "${tool_count}" \
        "${desktop_runtime}" \
        "${desktop_local_service}"
    write_package_manifest "${package_dir}"
    case "${target}" in
        x86_64-unknown-linux-gnu)
            grep -F "  ./serial-desktop" "${package_dir}/MANIFEST.sha256" >/dev/null \
                || fail "Linux package manifest does not cover serial-desktop"
            grep -F "  ./seriald" "${package_dir}/MANIFEST.sha256" >/dev/null \
                || fail "Linux package manifest does not cover the local daemon"
            ;;
        x86_64-pc-windows-gnu)
            grep -F "  ./serial-desktop.exe" "${package_dir}/MANIFEST.sha256" >/dev/null \
                || fail "Windows package manifest does not cover serial-desktop.exe"
            ;;
    esac
    normalize_package_metadata "${package_dir}"

    case "${target}" in
        x86_64-unknown-linux-gnu)
            create_tar_archive "${package_name}"
            ;;
        x86_64-pc-windows-gnu)
            create_zip_archive "${package_name}"
            ;;
    esac
    rm -rf "${package_dir}"
    printf 'packaged %s\n' "${package_name}"
}

write_archive_checksums() {
    local -a archives=()
    local path
    shopt -s nullglob
    for path in "${ARTIFACT_DIR}"/*.tar.gz "${ARTIFACT_DIR}"/*.zip; do
        archives+=("${path}")
    done
    shopt -u nullglob
    ((${#archives[@]} > 0)) || fail "no release archives were created"

    (
        cd "${ARTIFACT_DIR}"
        : >SHA256SUMS
        for path in "${archives[@]}"; do
            sha256sum "$(basename "${path}")" >>SHA256SUMS
        done
        sha256sum --check SHA256SUMS
    )
    touch -d "@${SOURCE_DATE_EPOCH}" "${ARTIFACT_DIR}/SHA256SUMS"
}

run_package() {
    CURRENT_PHASE="package"

    local targets_csv="${1:-}"
    [[ -n "${targets_csv}" ]] || fail "package requires a comma-separated Rust target list"
    [[ "${targets_csv}" =~ ^[A-Za-z0-9_,-]+$ ]] \
        || fail "invalid package target list: ${targets_csv}"
    [[ "${targets_csv}" != ,* && "${targets_csv}" != *, && "${targets_csv}" != *,,* ]] \
        || fail "package target list contains an empty item: ${targets_csv}"

    local targets
    IFS=',' read -r -a targets <<<"${targets_csv}"
    ((${#targets[@]} > 0)) || fail "package target list is empty"
    local seen_targets=","
    local target
    for target in "${targets[@]}"; do
        [[ -n "${target}" ]] || fail "package target list contains an empty item"
        case "${target}" in
            x86_64-unknown-linux-gnu|x86_64-pc-windows-gnu)
                ;;
            *)
                fail "unsupported release target: ${target}"
                ;;
        esac
        [[ "${seen_targets}" != *",${target},"* ]] \
            || fail "duplicate package target: ${target}"
        seen_targets+="${target},"
    done

    assert_workspace_version
    rm -rf "${ARTIFACT_DIR}"
    mkdir -p "${ARTIFACT_DIR}"

    for target in "${targets[@]}"; do
        run_package_target "${target}"
    done
    write_archive_checksums
}

run_checksums() {
    CURRENT_PHASE="checksums"
    [[ -d "${ARTIFACT_DIR}" ]] \
        || fail "artifact directory is missing: ${ARTIFACT_DIR}"
    write_archive_checksums
}

run_all() {
    run_environment
    run_fetch
    run_fmt
    run_clippy
    run_test
    run_build
    run_smoke
}

usage() {
    cat <<'EOF'
Usage: ci/build.sh <command>

Commands:
  env      Check the native and cross-release toolchain
  fetch    Download dependencies pinned by Cargo.lock
  fmt      Check formatting without modifying files
  clippy   Run Clippy for the whole workspace
  test     Run all workspace tests
  build [release|debug]
           Build native binaries for smoke tests (default: release)
  package <targets> [release|debug]
           Build x86_64 Linux/Windows archives (default: release)
  checksums
           Recreate SHA256SUMS for already-collected archives
  smoke [release|debug]
           Check native versions and the exact 19-tool MCP registry
  all      Run every native CI command in order (default)
EOF
}

cd "${PROJECT_ROOT}"

case "${1:-all}" in
    env)
        run_environment
        ;;
    fetch)
        run_fetch
        ;;
    fmt)
        run_fmt
        ;;
    clippy)
        run_clippy
        ;;
    test)
        run_test
        ;;
    build)
        configure_profile "${2:-release}"
        run_build
        ;;
    package)
        configure_profile "${3:-release}"
        run_package "${2:-}"
        ;;
    checksums)
        run_checksums
        ;;
    smoke)
        configure_profile "${2:-release}"
        run_smoke
        ;;
    all)
        run_all
        ;;
    help|-h|--help)
        usage
        ;;
    *)
        usage >&2
        fail "unknown command: $1"
        ;;
esac
