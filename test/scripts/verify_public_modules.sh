#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

(
  cd "${ROOT_DIR}/test/consumer"
  go test ./...
)

(
  cd "${ROOT_DIR}/test/template"
  go test ./...
)
