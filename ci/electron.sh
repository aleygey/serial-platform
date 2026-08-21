#!/usr/bin/env bash

set -Eeuo pipefail

readonly SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
readonly PROJECT_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
readonly DESKTOP_DIR="${PROJECT_ROOT}/crates/serial-desktop"
readonly CARGO_VERSION="$(sed -n 's/^version = "\(.*\)"/\1/p' "${PROJECT_ROOT}/Cargo.toml" | head -n 1)"
readonly NODE_VERSION="24.19.0"
readonly NODE_TOOL_ROOT="${PROJECT_ROOT}/target/tooling"

# GitHub release assets are intermittently unreachable from the macOS builder.
# Electron's installer still verifies the downloaded archive against the
# checksums shipped in the locked npm package.
export ELECTRON_MIRROR="${ELECTRON_MIRROR:-https://npmmirror.com/mirrors/electron/}"
export ELECTRON_BUILDER_BINARIES_MIRROR="${ELECTRON_BUILDER_BINARIES_MIRROR:-https://npmmirror.com/mirrors/electron-builder-binaries/}"

STAGED_RESOURCES_BIN=""

cleanup_staged_sidecars() {
    if [[ -n "${STAGED_RESOURCES_BIN}" && -d "${STAGED_RESOURCES_BIN}" ]]; then
        rm -rf "${STAGED_RESOURCES_BIN}"
    fi
}

trap cleanup_staged_sidecars EXIT

fail() {
    printf 'error: %s\n' "$*" >&2
    exit 1
}

require_command() {
    command -v "$1" >/dev/null 2>&1 || fail "required command not found: $1"
}

ensure_node_runtime() {
    local current_major=""
    if command -v node >/dev/null 2>&1 && command -v npm >/dev/null 2>&1; then
        current_major="$(node -p 'process.versions.node.split(".")[0]' 2>/dev/null || true)"
        if [[ "${current_major}" == "24" ]]; then
            return 0
        fi
    fi

    local platform
    local architecture
    local archive
    local checksum
    platform="$(uname -s)"
    architecture="$(uname -m)"
    case "${platform}:${architecture}" in
        Linux:x86_64)
            archive="node-v${NODE_VERSION}-linux-x64.tar.gz"
            checksum="f625d97cd707df4ff96254916fbc5ff014f09c09effe5a1e0ca8f6d41a8789d4"
            ;;
        Linux:aarch64|Linux:arm64)
            archive="node-v${NODE_VERSION}-linux-arm64.tar.gz"
            checksum="d28c8a5bf0a808f0ed434a1dce8c54ae98f0371c0bd86ac58abc613f73e6643f"
            ;;
        Darwin:arm64)
            archive="node-v${NODE_VERSION}-darwin-arm64.tar.gz"
            checksum="8294b7aa9b03997481c06babf1e8b270c859358f27da57a11509afe537ac381d"
            ;;
        Darwin:x86_64)
            archive="node-v${NODE_VERSION}-darwin-x64.tar.gz"
            checksum="d1b5e999db158c62fe8f7267a4476b035d8bd93b1a605bac24a3f0dd166e3316"
            ;;
        *)
            fail "Node.js 24.x is required; automatic bootstrap is unavailable on ${platform} ${architecture}"
            ;;
    esac

    local install_dir="${NODE_TOOL_ROOT}/${archive%.tar.gz}"
    if [[ ! -x "${install_dir}/bin/node" || ! -x "${install_dir}/bin/npm" ]]; then
        require_command curl
        require_command tar
        mkdir -p "${NODE_TOOL_ROOT}"
        local temporary_dir
        temporary_dir="$(mktemp -d "${NODE_TOOL_ROOT}/node-download.XXXXXX")"
        local archive_path="${temporary_dir}/${archive}"
        local mirror="${NODEJS_ORG_MIRROR:-https://npmmirror.com/mirrors/node}"
        mirror="${mirror%/}"
        curl --fail --location --retry 3 --connect-timeout 20 --max-time 600 \
            "${mirror}/v${NODE_VERSION}/${archive}" \
            --output "${archive_path}"
        local actual_checksum
        case "${platform}" in
            Darwin) actual_checksum="$(shasum -a 256 "${archive_path}" | awk '{print $1}')" ;;
            *) actual_checksum="$(sha256sum "${archive_path}" | awk '{print $1}')" ;;
        esac
        [[ "${actual_checksum}" == "${checksum}" ]] \
            || fail "Node.js archive checksum mismatch for ${archive}"
        tar -xzf "${archive_path}" -C "${temporary_dir}"
        [[ -x "${temporary_dir}/${archive%.tar.gz}/bin/node" ]] \
            || fail "Node.js archive did not contain the expected runtime"
        mv "${temporary_dir}/${archive%.tar.gz}" "${install_dir}"
        rm -f "${archive_path}"
        rmdir "${temporary_dir}"
    fi
    export PATH="${install_dir}/bin:${PATH}"
}

desktop_version() {
    node -p "require('${DESKTOP_DIR}/package.json').version"
}

assert_versions() {
    local version
    local node_version
    local node_major
    ensure_node_runtime
    node_version="$(node --version)"
    node_major="${node_version#v}"
    node_major="${node_major%%.*}"
    [[ "${node_major}" == "24" ]] \
        || fail "Electron builds require Node.js 24; found ${node_version}"
    version="$(desktop_version)"
    [[ "${version}" == "${CARGO_VERSION}" ]] \
        || fail "Electron version ${version} does not match Cargo ${CARGO_VERSION}"
}

run_environment() {
    ensure_node_runtime
    require_command node
    require_command npm
    node --version
    npm --version
    assert_versions
}

run_fetch() {
    run_environment
    (
        cd "${DESKTOP_DIR}"
        npm ci --no-audit --no-fund
    )
}

run_typecheck() {
    assert_versions
    (
        cd "${DESKTOP_DIR}"
        npm run typecheck
    )
}

run_test() {
    assert_versions
    (
        cd "${DESKTOP_DIR}"
        npm run test:run
    )
}

run_build() {
    assert_versions
    (
        cd "${DESKTOP_DIR}"
        npm run build
    )
}

stage_sidecars() {
    local binary_dir="$1"
    local executable_suffix="$2"
    local resources_bin="${DESKTOP_DIR}/resources/bin"

    [[ -f "${binary_dir}/serial${executable_suffix}" ]] \
        || fail "desktop sidecar is missing: ${binary_dir}/serial${executable_suffix}"
    [[ -f "${binary_dir}/seriald${executable_suffix}" ]] \
        || fail "desktop sidecar is missing: ${binary_dir}/seriald${executable_suffix}"

    rm -rf "${resources_bin}"
    mkdir -p "${resources_bin}"
    STAGED_RESOURCES_BIN="${resources_bin}"
    install -m 0755 "${binary_dir}/serial${executable_suffix}" \
        "${resources_bin}/serial${executable_suffix}"
    install -m 0755 "${binary_dir}/seriald${executable_suffix}" \
        "${resources_bin}/seriald${executable_suffix}"
}

single_match() {
    local description="$1"
    shift
    local -a matches=("$@")
    ((${#matches[@]} == 1)) \
        || fail "expected one ${description}, found ${#matches[@]}"
    printf '%s\n' "${matches[0]}"
}

package_macos() {
    local architecture="$1"
    local destination="$2"
    local arch_flag
    case "${architecture}" in
        arm64) arch_flag="--arm64" ;;
        x86_64) arch_flag="--x64" ;;
        *) fail "unsupported macOS Electron architecture: ${architecture}" ;;
    esac

    (
        cd "${DESKTOP_DIR}"
        CSC_IDENTITY_AUTO_DISCOVERY=false \
            npm exec electron-builder -- --mac dir "${arch_flag}" --publish never
    )
    local -a candidates=()
    while IFS= read -r candidate; do
        candidates+=("${candidate}")
    done < <(find "${DESKTOP_DIR}/dist" -maxdepth 2 -type d -name 'Serial Platform.app' -print)
    local app
    app="$(single_match 'macOS Serial Platform.app' "${candidates[@]}")"
    rm -rf "${destination}/Serial Platform.app"
    cp -R "${app}" "${destination}/Serial Platform.app"

    local app_binary="${destination}/Serial Platform.app/Contents/MacOS/Serial Platform"
    [[ -x "${app_binary}" ]] || fail "packaged macOS App executable is missing"
    case "${architecture}" in
        arm64)
            file "${app_binary}" | grep -Eq 'Mach-O 64-bit executable arm64' \
                || fail "Electron macOS executable is not arm64"
            ;;
        x86_64)
            file "${app_binary}" | grep -Eq 'Mach-O 64-bit executable x86_64' \
                || fail "Electron macOS executable is not x86_64"
            ;;
    esac
    [[ -x "${destination}/Serial Platform.app/Contents/Resources/bin/serial" ]] \
        || fail "macOS App does not contain the serial sidecar"
    [[ -x "${destination}/Serial Platform.app/Contents/Resources/bin/seriald" ]] \
        || fail "macOS App does not contain the seriald sidecar"
    local sidecar
    for sidecar in serial seriald; do
        local sidecar_description
        sidecar_description="$(
            file -b "${destination}/Serial Platform.app/Contents/Resources/bin/${sidecar}"
        )"
        case "${architecture}" in
            arm64)
                grep -Eq 'Mach-O 64-bit executable arm64' <<<"${sidecar_description}" \
                    || fail "macOS App sidecar ${sidecar} is not arm64"
                ;;
            x86_64)
                grep -Eq 'Mach-O 64-bit executable x86_64' <<<"${sidecar_description}" \
                    || fail "macOS App sidecar ${sidecar} is not x86_64"
                ;;
        esac
    done
    local info_plist="${destination}/Serial Platform.app/Contents/Info.plist"
    [[ "$(/usr/libexec/PlistBuddy -c 'Print :CFBundleShortVersionString' "${info_plist}")" == "${CARGO_VERSION}" ]] \
        || fail "macOS App version does not match ${CARGO_VERSION}"
    [[ "$(/usr/libexec/PlistBuddy -c 'Print :LSMinimumSystemVersion' "${info_plist}")" == "12.0" ]] \
        || fail "macOS App minimum system version must be 12.0"
    local permission_key
    for permission_key in \
        NSAudioCaptureUsageDescription \
        NSBluetoothAlwaysUsageDescription \
        NSBluetoothPeripheralUsageDescription \
        NSCameraUsageDescription \
        NSMicrophoneUsageDescription; do
        ! /usr/libexec/PlistBuddy -c "Print :${permission_key}" "${info_plist}" >/dev/null 2>&1 \
            || fail "macOS App declares unused permission ${permission_key}"
    done
}

package_linux() {
    local destination="$1"
    (
        cd "${DESKTOP_DIR}"
        npm exec electron-builder -- --linux AppImage --x64 --publish never
    )
    local -a candidates=()
    while IFS= read -r candidate; do
        candidates+=("${candidate}")
    done < <(find "${DESKTOP_DIR}/dist" -maxdepth 1 -type f -name '*.AppImage' -print)
    local artifact
    artifact="$(single_match 'Linux AppImage' "${candidates[@]}")"
    local unpacked_resources="${DESKTOP_DIR}/dist/linux-unpacked/resources/bin"
    local electron_binary="${DESKTOP_DIR}/dist/linux-unpacked/serial-platform-desktop"
    [[ -x "${electron_binary}" ]] \
        || fail "Linux Electron executable is missing from the unpacked application"
    file "${electron_binary}" | grep -Eq 'ELF 64-bit LSB.*x86-64' \
        || fail "Linux Electron executable is not x86_64"
    local highest_glibc
    highest_glibc="$(
        readelf --version-info "${electron_binary}" \
            | grep -o 'GLIBC_[0-9][0-9.]*' \
            | sort -Vu \
            | tail -n 1
    )"
    [[ -n "${highest_glibc}" ]] \
        || fail "Linux Electron executable has no GLIBC version requirement"
    local newest_glibc
    newest_glibc="$(printf '%s\n' "${highest_glibc}" GLIBC_2.31 | sort -V | tail -n 1)"
    [[ "${newest_glibc}" == "GLIBC_2.31" ]] \
        || fail "Linux Electron executable requires ${highest_glibc}, newer than GLIBC_2.31"
    local sidecar
    for sidecar in serial seriald; do
        [[ -x "${unpacked_resources}/${sidecar}" ]] \
            || fail "Linux Electron package does not contain ${sidecar}"
        cmp -s "${DESKTOP_DIR}/resources/bin/${sidecar}" "${unpacked_resources}/${sidecar}" \
            || fail "Linux Electron package sidecar differs from staged ${sidecar}"
    done
    install -m 0755 "${artifact}" "${destination}/Serial Platform.AppImage"
    file "${destination}/Serial Platform.AppImage" | grep -Eq 'ELF 64-bit LSB.*x86-64' \
        || fail "Electron Linux artifact is not an x86_64 AppImage"
}

package_windows() {
    local destination="$1"
    (
        cd "${DESKTOP_DIR}"
        npm exec electron-builder -- --win portable --x64 --publish never
    )
    local -a candidates=()
    while IFS= read -r candidate; do
        candidates+=("${candidate}")
    done < <(find "${DESKTOP_DIR}/dist" -maxdepth 1 -type f -name '*.exe' -print)
    local artifact
    artifact="$(single_match 'Windows portable executable' "${candidates[@]}")"
    local unpacked_resources="${DESKTOP_DIR}/dist/win-unpacked/resources/bin"
    local unpacked_app="${DESKTOP_DIR}/dist/win-unpacked/Serial Platform.exe"
    [[ -f "${unpacked_app}" ]] \
        || fail "Windows Electron executable is missing from the unpacked application"
    file "${unpacked_app}" | grep -Eq 'PE32\+ executable.*x86-64' \
        || fail "Electron Windows application is not an x86_64 PE32+ executable"
    local sidecar
    for sidecar in serial.exe seriald.exe; do
        [[ -f "${unpacked_resources}/${sidecar}" ]] \
            || fail "Windows Electron package does not contain ${sidecar}"
        cmp -s "${DESKTOP_DIR}/resources/bin/${sidecar}" "${unpacked_resources}/${sidecar}" \
            || fail "Windows Electron package sidecar differs from staged ${sidecar}"
    done
    install -m 0755 "${artifact}" "${destination}/Serial Platform.exe"
    file "${destination}/Serial Platform.exe" | grep -Eq 'PE32 executable.*Nullsoft Installer' \
        || fail "Electron Windows artifact is not an NSIS portable executable"
}

run_package() {
    local platform="${1:-}"
    local architecture="${2:-}"
    local binary_dir="${3:-}"
    local destination="${4:-}"
    [[ -n "${platform}" && -n "${architecture}" && -n "${binary_dir}" && -n "${destination}" ]] \
        || fail "package requires platform, architecture, sidecar directory, and destination"
    [[ -d "${destination}" ]] || fail "desktop package destination is missing: ${destination}"
    local command
    for command in file find install cmp; do
        require_command "${command}"
    done
    if [[ "${platform}" == "linux" ]]; then
        for command in readelf sort; do
            require_command "${command}"
        done
    fi
    assert_versions

    local suffix=""
    [[ "${platform}" == "windows" ]] && suffix=".exe"
    stage_sidecars "${binary_dir}" "${suffix}"
    rm -rf "${DESKTOP_DIR}/dist"

    case "${platform}" in
        macos) package_macos "${architecture}" "${destination}" ;;
        linux)
            [[ "${architecture}" == "x86_64" ]] \
                || fail "unsupported Linux Electron architecture: ${architecture}"
            package_linux "${destination}"
            ;;
        windows)
            [[ "${architecture}" == "x86_64" ]] \
                || fail "unsupported Windows Electron architecture: ${architecture}"
            package_windows "${destination}"
            ;;
        *) fail "unsupported Electron platform: ${platform}" ;;
    esac
}

usage() {
    cat <<'EOF'
Usage: ci/electron.sh <command>

Commands:
  env
  fetch
  typecheck
  test
  build
  package <macos|linux|windows> <arm64|x86_64> <sidecar-dir> <destination>
EOF
}

case "${1:-}" in
    env) run_environment ;;
    fetch) run_fetch ;;
    typecheck) run_typecheck ;;
    test) run_test ;;
    build) run_build ;;
    package) run_package "${2:-}" "${3:-}" "${4:-}" "${5:-}" ;;
    help|-h|--help) usage ;;
    *) usage >&2; exit 2 ;;
esac
