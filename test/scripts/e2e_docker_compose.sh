#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

COMPOSE_FILE="${E2E_COMPOSE_FILE:-docker-compose.test.yml}"
PROJECT_NAME="${E2E_COMPOSE_PROJECT_NAME:-palantir-compute-module-pipeline-search-e2e}"

OUTPUT_RID="${E2E_OUTPUT_RID:-ri.foundry.main.dataset.22222222-2222-2222-2222-222222222222}"
TXN_ID="${E2E_TXN_ID:-txn-000001}"
OUTPUT_FILENAME="${E2E_OUTPUT_FILENAME:-enriched.csv}"

EXPECTED_CSV="${E2E_EXPECTED_CSV:-${ROOT_DIR}/test/fixtures/expected_output.csv}"
ACTUAL_CSV="${E2E_ACTUAL_CSV:-${ROOT_DIR}/test/artifacts/mock-foundry/uploads/${OUTPUT_RID}/${TXN_ID}/${OUTPUT_FILENAME}}"

if docker compose version >/dev/null 2>&1; then
  COMPOSE=(docker compose)
elif command -v docker-compose >/dev/null 2>&1; then
  COMPOSE=(docker-compose)
else
  echo "docker compose not found (expected 'docker compose' or 'docker-compose')" >&2
  exit 1
fi

compose() {
  "${COMPOSE[@]}" -p "${PROJECT_NAME}" -f "${ROOT_DIR}/${COMPOSE_FILE}" "$@"
}

cleanup() {
  compose down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

UPLOADS_DIR="${ROOT_DIR}/test/artifacts/mock-foundry/uploads"
mkdir -p "${UPLOADS_DIR}"

# Best-effort cleanup. If a prior run wrote files as root, host cleanup can fail; fall back to a container.
if ! rm -rf "${UPLOADS_DIR:?}/"* 2>/dev/null; then
  docker run --rm --network=none -v "${UPLOADS_DIR}:/uploads" alpine:3.20 sh -c 'rm -rf /uploads/*'
fi

set +e
compose up --build --abort-on-container-exit --exit-code-from enricher
rc=$?
set -e
if [[ $rc -ne 0 ]]; then
  compose logs --no-color || true
  exit "$rc"
fi

if [[ ! -f "${ACTUAL_CSV}" ]]; then
  echo "expected output file not found: ${ACTUAL_CSV}" >&2
  echo "uploads dir contents:" >&2
  ls -R "${ROOT_DIR}/test/artifacts/mock-foundry/uploads" >&2 || true
  exit 1
fi

diff -u "${EXPECTED_CSV}" "${ACTUAL_CSV}"
