#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TMP_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/foundry-cmgo-generated.XXXXXX")"
SUCCESS=0

cleanup() {
  if [[ "${SUCCESS}" == "1" ]]; then
    rm -rf "${TMP_ROOT}"
  else
    echo "verify-generated: preserving failed artifacts at ${TMP_ROOT}" >&2
    find "${TMP_ROOT}" -path '*/.local/foundry-cmgo/*/run.log' -print >&2 2>/dev/null || true
  fi
}
trap cleanup EXIT INT TERM

BIN_DIR="${TMP_ROOT}/bin"
mkdir -p "${BIN_DIR}"
CLI_BIN="${BIN_DIR}/foundry-cmgo"

echo "verify-generated: building foundry-cmgo -> ${CLI_BIN}"
go -C "${ROOT_DIR}" build -trimpath -o "${CLI_BIN}" ./cmd/foundry-cmgo
export PATH="${BIN_DIR}:${PATH}"

run_project() {
  local example="$1"
  local rows_flag=()
  local project_dir="${TMP_ROOT}/${example}-starter"

  if [[ "${example}" == "dataset" ]]; then
    rows_flag=(--rows 1)
  fi

  echo "verify-generated: generating ${example} starter -> ${project_dir}"
  "${CLI_BIN}" new \
    --name "${example}-starter" \
    --module "example.com/acme/${example}-starter" \
    --dir "${project_dir}" \
    --example "${example}" \
    --local-replace "${ROOT_DIR}"

  echo "verify-generated: ${example}: go test ./..."
  (cd "${project_dir}" && go test ./...)

  echo "verify-generated: ${example}: foundry-cmgo preview ${rows_flag[*]}"
  (cd "${project_dir}" && foundry-cmgo preview "${rows_flag[@]}")

  echo "verify-generated: ${example}: foundry-cmgo build"
  (cd "${project_dir}" && foundry-cmgo build)

  echo "verify-generated: ${example}: foundry-cmgo inspect last/config/outputs"
  (cd "${project_dir}" && foundry-cmgo inspect last && foundry-cmgo inspect config && foundry-cmgo inspect outputs)

  echo "verify-generated: ${example}: artifacts"
  find "${project_dir}/.local/foundry-cmgo" -maxdepth 4 -type f \( -name 'last-run.json' -o -name 'run.log' -o -name 'records.jsonl' \) -print | sort
}

run_project dataset
run_project stream

SUCCESS=1
echo "verify-generated: passed dataset and stream generated-starter workflows"
