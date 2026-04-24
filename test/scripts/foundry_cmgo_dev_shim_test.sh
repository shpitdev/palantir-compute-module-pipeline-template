#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TMP_DIR="$(mktemp -d)"
trap 'rm -rf "${TMP_DIR}"' EXIT

pass() {
  echo "ok: $1"
}

fail() {
  echo "fail: $1" >&2
  exit 1
}

assert_contains_file() {
  local path="$1"
  local needle="$2"
  local message="$3"
  if ! grep -Fq -- "$needle" "$path"; then
    echo "--- ${path} ---" >&2
    cat "$path" >&2
    fail "$message (missing: ${needle})"
  fi
}

"${ROOT_DIR}/scripts/foundry-cmgo-dev" >/tmp/foundry-cmgo-dev-shim-noargs.out 2>&1 || true
assert_contains_file /tmp/foundry-cmgo-dev-shim-noargs.out "Usage:" "dev shim should forward zero args as zero args"
if grep -Fq "unknown command:" /tmp/foundry-cmgo-dev-shim-noargs.out; then
  cat /tmp/foundry-cmgo-dev-shim-noargs.out >&2
  fail "dev shim should not pass a spurious empty argument"
fi
pass "dev shim preserves no-args usage"

"${ROOT_DIR}/scripts/foundry-cmgo-dev" new \
  --name shim-default \
  --module example.com/acme/shim-default \
  --dir "${TMP_DIR}/shim-default" \
  --example dataset >/tmp/foundry-cmgo-dev-shim-default.out
assert_contains_file "${TMP_DIR}/shim-default/go.mod" "replace github.com/palantir/palantir-compute-module-pipeline-search => ${ROOT_DIR}" "dev shim should inject local replace for new"
pass "dev shim injects local replace by default"

"${ROOT_DIR}/scripts/foundry-cmgo-dev" new \
  --name shim-explicit \
  --module example.com/acme/shim-explicit \
  --dir "${TMP_DIR}/shim-explicit" \
  --example dataset \
  --local-replace /tmp/explicit-replace >/tmp/foundry-cmgo-dev-shim-explicit.out
assert_contains_file "${TMP_DIR}/shim-explicit/go.mod" "replace github.com/palantir/palantir-compute-module-pipeline-search => /tmp/explicit-replace" "dev shim should preserve explicit local replace"
pass "dev shim preserves explicit local replace"
