#!/usr/bin/env sh
set -eu

resolve_self_path() {
  if [ -n "${ZSH_VERSION:-}" ]; then
    eval 'printf "%s\n" "${(%):-%N}"'
    return 0
  fi
  if [ -n "${BASH_VERSION:-}" ] && [ -n "${BASH_SOURCE:-}" ]; then
    printf '%s\n' "${BASH_SOURCE[0]}"
    return 0
  fi
  printf '%s\n' "$0"
}

finish() {
  exit_code="$1"
  return "$exit_code" 2>/dev/null || exit "$exit_code"
}

is_sourced() {
  if [ -n "${ZSH_VERSION:-}" ]; then
    case "${ZSH_EVAL_CONTEXT:-}" in
      *:file) return 0 ;;
    esac
    return 1
  fi
  if [ -n "${BASH_VERSION:-}" ]; then
    if eval '[ "${BASH_SOURCE[0]}" != "$0" ]'; then
      return 0
    fi
  fi
  return 1
}

refresh_current_shell() {
  if [ -n "${ZSH_VERSION:-}" ]; then
    rehash >/dev/null 2>&1 || true
    return 0
  fi
  if [ -n "${BASH_VERSION:-}" ]; then
    hash -r 2>/dev/null || true
    return 0
  fi
}

SELF_PATH=$(resolve_self_path)
SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$SELF_PATH")" && pwd)
INSTALLER="$SCRIPT_DIR/install.sh"

if [ ! -x "$INSTALLER" ]; then
  printf 'install-dev.sh: expected executable installer at %s\n' "$INSTALLER" >&2
  finish 1
fi

"$INSTALLER" --dev-only "$@"

if is_sourced; then
  refresh_current_shell
fi

if command -v foundry-cmgo-dev >/dev/null 2>&1; then
  printf 'foundry-cmgo-dev is ready: %s\n' "$(command -v foundry-cmgo-dev)"
fi

if is_sourced; then
  printf 'current shell refreshed\n'
else
  printf 'open a new shell if foundry-cmgo-dev is not visible yet\n'
fi

finish 0
