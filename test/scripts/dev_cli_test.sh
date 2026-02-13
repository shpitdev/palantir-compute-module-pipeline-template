#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TMP_DIR="$(mktemp -d)"
trap 'rm -rf "${TMP_DIR}"' EXIT

TESTS_RUN=0

pass() {
  TESTS_RUN=$((TESTS_RUN + 1))
  echo "ok: $1"
}

fail() {
  echo "fail: $1" >&2
  exit 1
}

assert_contains() {
  local haystack="$1"
  local needle="$2"
  local message="$3"
  if [[ "${haystack}" != *"${needle}"* ]]; then
    fail "${message} (missing: ${needle})"
  fi
}

assert_not_contains() {
  local haystack="$1"
  local needle="$2"
  local message="$3"
  if [[ "${haystack}" == *"${needle}"* ]]; then
    fail "${message} (unexpected: ${needle})"
  fi
}

assert_file_exists() {
  local path="$1"
  local message="$2"
  if [[ ! -e "${path}" ]]; then
    fail "${message} (${path})"
  fi
}

assert_empty_dir() {
  local path="$1"
  local message="$2"
  if find "${path}" -mindepth 1 -print -quit | grep -q .; then
    fail "${message} (${path})"
  fi
}

new_workspace() {
  local name="$1"
  local ws="${TMP_DIR}/${name}"
  mkdir -p "${ws}/test/fixtures" "${ws}/.local/mock-foundry/inputs" "${ws}/.local/mock-foundry/uploads" "${ws}/bin"
  cp "${ROOT_DIR}/dev" "${ws}/dev"
  chmod +x "${ws}/dev"
  cat >"${ws}/docker-compose.local.yml" <<'YAML'
services:
  mock-foundry:
    image: fake
YAML
  cat >"${ws}/test/fixtures/alias-map.json" <<'JSON'
{
  "input": {
    "rid": "ri.foundry.main.dataset.11111111-1111-1111-1111-111111111111",
    "branch": null
  },
  "output": {
    "rid": "ri.foundry.main.dataset.22222222-2222-2222-2222-222222222222",
    "branch": null
  }
}
JSON
  cat >"${ws}/bin/docker" <<'BASH'
#!/usr/bin/env bash
set -euo pipefail

: "${DEV_TEST_DOCKER_LOG:?missing log path}"
echo "$*" >> "${DEV_TEST_DOCKER_LOG}"

if [[ "${1:-}" == "--version" ]]; then
  echo "Docker version 27.0.0, build test"
  exit 0
fi

if [[ "${1:-}" == "compose" ]]; then
  exit 0
fi

if [[ "${1:-}" == "run" ]]; then
  if [[ "${DEV_TEST_AUTOFIX_RECOVER:-0}" == "1" ]]; then
    while [[ $# -gt 0 ]]; do
      case "$1" in
        -v)
          host_path="${2%%:*}"
          chmod -R u+w "${host_path}" 2>/dev/null || true
          shift 2
          ;;
        *)
          shift
          ;;
      esac
    done
  fi
  exit 0
fi

exit 0
BASH
  chmod +x "${ws}/bin/docker"
  echo "${ws}"
}

run_dev_capture() {
  local ws="$1"
  shift
  local output
  local status=0
  set +e
  output="$(
    cd "${ws}" && \
      PATH="${ws}/bin:${PATH}" \
      DEV_TEST_DOCKER_LOG="${ws}/docker.log" \
      DEV_TEST_AUTOFIX_RECOVER="${DEV_TEST_AUTOFIX_RECOVER:-0}" \
      ./dev "$@" 2>&1
  )"
  status=$?
  set -e
  printf '%s\n' "${status}"
  printf '%s\n' "${output}"
}

test_foundry_mock_alias_rejected() {
  local ws
  ws="$(new_workspace "foundry-mock-rejected")"
  local result
  result="$(run_dev_capture "${ws}" run foundry-mock)"
  local status
  status="$(echo "${result}" | sed -n '1p')"
  local output
  output="$(echo "${result}" | sed -n '2,$p')"
  [[ "${status}" == "2" ]] || fail "foundry-mock should return status 2 (got ${status})"
  assert_contains "${output}" "unknown run target: foundry-mock" "foundry-mock alias should be rejected"
  pass "foundry-mock alias rejected"
}

test_preflight_missing_fixture_fails_before_compose() {
  local ws
  ws="$(new_workspace "missing-fixture")"
  local result
  result="$(run_dev_capture "${ws}" run foundry-emulated)"
  local status
  status="$(echo "${result}" | sed -n '1p')"
  local output
  output="$(echo "${result}" | sed -n '2,$p')"
  [[ "${status}" != "0" ]] || fail "missing fixture should fail preflight"
  assert_contains "${output}" "fail: missing input fixture" "preflight should report missing input fixture"
  local docker_log
  docker_log="$(cat "${ws}/docker.log" 2>/dev/null || true)"
  assert_not_contains "${docker_log}" "compose -f docker-compose.local.yml up" "compose up must not run when preflight fails"
  pass "preflight missing fixture fails before compose"
}

test_preflight_autofix_recovers_permissions() {
  local ws
  ws="$(new_workspace "autofix-recovers")"
  local input_csv="${ws}/.local/mock-foundry/inputs/ri.foundry.main.dataset.11111111-1111-1111-1111-111111111111.csv"
  echo "email" >"${input_csv}"
  chmod 0555 "${ws}/.local/mock-foundry/uploads"
  local result
  result="$(DEV_TEST_AUTOFIX_RECOVER=1 run_dev_capture "${ws}" run foundry-emulated)"
  local status
  status="$(echo "${result}" | sed -n '1p')"
  local output
  output="$(echo "${result}" | sed -n '2,$p')"
  [[ "${status}" == "0" ]] || fail "auto-fix recovery should succeed (got ${status})"
  assert_contains "${output}" "ok: permission auto-fix completed" "auto-fix completion should be reported"
  assert_contains "${output}" "preflight: passed" "preflight should pass after auto-fix"
  local docker_log
  docker_log="$(cat "${ws}/docker.log")"
  assert_contains "${docker_log}" "run --rm -v" "auto-fix should invoke docker run"
  assert_contains "${docker_log}" "compose -f docker-compose.local.yml up --abort-on-container-exit --build" "compose up should run after successful preflight"
  pass "preflight auto-fix recovers permissions"
}

test_preflight_autofix_failure_has_remediation() {
  local ws
  ws="$(new_workspace "autofix-fails")"
  local input_csv="${ws}/.local/mock-foundry/inputs/ri.foundry.main.dataset.11111111-1111-1111-1111-111111111111.csv"
  echo "email" >"${input_csv}"
  chmod 0555 "${ws}/.local/mock-foundry/uploads"
  local result
  result="$(DEV_TEST_AUTOFIX_RECOVER=0 run_dev_capture "${ws}" run foundry-emulated)"
  local status
  status="$(echo "${result}" | sed -n '1p')"
  local output
  output="$(echo "${result}" | sed -n '2,$p')"
  [[ "${status}" != "0" ]] || fail "auto-fix failure path should fail"
  assert_contains "${output}" "remediation: docker run --rm -v" "failure output should include remediation command"
  local docker_log
  docker_log="$(cat "${ws}/docker.log")"
  assert_contains "${docker_log}" "run --rm -v" "failure path should still attempt auto-fix"
  assert_not_contains "${docker_log}" "compose -f docker-compose.local.yml up" "compose up must not run after failed auto-fix"
  pass "preflight auto-fix failure prints remediation"
}

test_clean_resets_uploads_preserves_inputs() {
  local ws
  ws="$(new_workspace "clean-command")"
  local input_csv="${ws}/.local/mock-foundry/inputs/ri.foundry.main.dataset.11111111-1111-1111-1111-111111111111.csv"
  local upload_dir="${ws}/.local/mock-foundry/uploads/ri.foundry.main.dataset.22222222-2222-2222-2222-222222222222/txn-1"
  mkdir -p "${upload_dir}"
  echo "email" >"${input_csv}"
  echo "artifact" >"${upload_dir}/readTable.csv"
  local result
  result="$(run_dev_capture "${ws}" clean)"
  local status
  status="$(echo "${result}" | sed -n '1p')"
  [[ "${status}" == "0" ]] || fail "clean should succeed (got ${status})"
  assert_file_exists "${input_csv}" "clean must preserve inputs"
  assert_empty_dir "${ws}/.local/mock-foundry/uploads" "clean must clear uploads"
  local docker_log
  docker_log="$(cat "${ws}/docker.log")"
  assert_contains "${docker_log}" "compose -f docker-compose.local.yml down -v --remove-orphans" "clean should run compose down"
  pass "clean resets uploads and preserves inputs"
}

test_help_lists_clean_and_not_foundry_mock() {
  local ws
  ws="$(new_workspace "help-output")"
  local result
  result="$(run_dev_capture "${ws}" help)"
  local status
  status="$(echo "${result}" | sed -n '1p')"
  local output
  output="$(echo "${result}" | sed -n '2,$p')"
  [[ "${status}" == "0" ]] || fail "help should succeed"
  assert_contains "${output}" "./dev clean" "help should list clean command"
  assert_not_contains "${output}" "foundry-mock" "help should not list foundry-mock alias"
  pass "help output updated"
}

test_foundry_mock_alias_rejected
test_preflight_missing_fixture_fails_before_compose
test_preflight_autofix_recovers_permissions
test_preflight_autofix_failure_has_remediation
test_clean_resets_uploads_preserves_inputs
test_help_lists_clean_and_not_foundry_mock

echo "dev cli tests passed (${TESTS_RUN})"
