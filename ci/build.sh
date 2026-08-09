#!/usr/bin/env bash

set -Eeuo pipefail

readonly SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
readonly PROJECT_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
readonly RELEASE_DIR="${PROJECT_ROOT}/target/release"

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

run_environment() {
    CURRENT_PHASE="environment"
    log "Checking Rust build environment"

    require_command git
    require_command rustc
    require_command cargo
    require_command cc

    git --version
    rustc --version
    cargo --version
    cargo fmt --version
    cargo clippy --version
    cc --version | sed -n '1p'
    rustc -vV

    cargo metadata --locked --no-deps --format-version 1 >/dev/null
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
    log "Building release binaries"
    cargo build \
        --release \
        --locked \
        -p serial-cli \
        -p seriald \
        -p serialctl \
        -p serial-mcp
}

run_smoke() {
    CURRENT_PHASE="smoke"
    log "Smoke-testing release binaries"

    local binary
    for binary in serial seriald serialctl serial-mcp; do
        [[ -x "${RELEASE_DIR}/${binary}" ]] \
            || fail "release binary is missing or not executable: ${RELEASE_DIR}/${binary}"
        "${RELEASE_DIR}/${binary}" --version
    done

    "${RELEASE_DIR}/serial" --help >/dev/null
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
  env      Check required build tools and Cargo metadata
  fetch    Download dependencies pinned by Cargo.lock
  fmt      Check formatting without modifying files
  clippy   Run Clippy for the whole workspace
  test     Run all workspace tests
  build    Build the release binaries
  smoke    Run basic checks against release binaries
  all      Run every command in CI order (default)
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
        run_build
        ;;
    smoke)
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
