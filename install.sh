#!/usr/bin/env bash
set -euo pipefail

BIN_NAME="foundry-cmgo"
DEV_BIN_NAME="foundry-cmgo-dev"
DEFAULT_INSTALL_DIR="${HOME}/.local/bin"

INSTALL_DIR="${FOUNDRY_CMGO_INSTALL_DIR:-$DEFAULT_INSTALL_DIR}"
REPO_PATH=""
NO_SHELL_UPDATE=0
STABLE_ONLY=0
DEV_ONLY=0
declare -a SHELL_RC_FILES=()

usage() {
  cat <<'USAGE'
Usage:
  ./install.sh [options]

Installs:
  foundry-cmgo      Built binary from the current checkout
  foundry-cmgo-dev  Checkout-linked wrapper that always runs `go run ./cmd/foundry-cmgo`

Options:
  --install-dir PATH      Directory for installed commands. Default: ~/.local/bin
  --repo-path PATH        Repo root to install from. Default: directory containing this script
  --stable-only           Install only foundry-cmgo
  --dev-only              Install only foundry-cmgo-dev
  --shell-rc-file PATH    Shell rc file to update with PATH changes. Repeatable
  --no-shell-update       Do not edit shell startup files
  -h, --help              Show this help text
USAGE
}

log() {
  printf '%s\n' "$*" >&2
}

fail() {
  printf 'error: %s\n' "$*" >&2
  exit 1
}

abs_path() {
  local target="$1"
  if [[ "$target" == /* ]]; then
    printf '%s\n' "$target"
  else
    printf '%s/%s\n' "$PWD" "$target"
  fi
}

script_dir() {
  cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd
}

ensure_repo_path() {
  if [[ -z "$REPO_PATH" ]]; then
    REPO_PATH="$(script_dir)"
  fi
  REPO_PATH="$(abs_path "$REPO_PATH")"
  [[ -f "$REPO_PATH/go.mod" && -f "$REPO_PATH/cmd/foundry-cmgo/main.go" ]] || \
    fail "repo path does not look like palantir-compute-module-pipeline-search: $REPO_PATH"
}

choose_shell_rc_files() {
  if [[ ${#SHELL_RC_FILES[@]} -gt 0 ]]; then
    return 0
  fi

  case "$(basename "${SHELL:-}")" in
    zsh) SHELL_RC_FILES+=("${HOME}/.zshrc") ;;
    bash)
      if [[ "$(uname -s)" == "Darwin" ]]; then
        SHELL_RC_FILES+=("${HOME}/.bash_profile")
      else
        SHELL_RC_FILES+=("${HOME}/.bashrc")
      fi
      ;;
    *) SHELL_RC_FILES+=("${HOME}/.profile") ;;
  esac
}

update_shell_path() {
  (( NO_SHELL_UPDATE == 1 )) && return 0
  choose_shell_rc_files

  local marker_start="# >>> foundry-cmgo install >>>"
  local marker_end="# <<< foundry-cmgo install <<<"
  local rc_file tmp_file

  for rc_file in "${SHELL_RC_FILES[@]}"; do
    mkdir -p -- "$(dirname -- "$rc_file")"
    touch "$rc_file"
    tmp_file="$(mktemp)"
    awk -v start="$marker_start" -v end="$marker_end" '
      $0 == start { skipping=1; next }
      skipping && $0 == end { skipping=0; next }
      !skipping { print }
    ' "$rc_file" >"$tmp_file"
    cat "$tmp_file" >"$rc_file"
    rm -f "$tmp_file"
    {
      printf '\n%s\n' "$marker_start"
      printf 'case ":$PATH:" in\n'
      printf '  *:"%s":*) ;;\n' "$INSTALL_DIR"
      printf '  *) export PATH="%s:$PATH" ;;\n' "$INSTALL_DIR"
      printf 'esac\n%s\n\n' "$marker_end"
    } >>"$rc_file"
    log "updated shell config in $rc_file"
  done
}

path_contains_install_dir() {
  case ":$PATH:" in
    *:"$INSTALL_DIR":*) return 0 ;;
    *) return 1 ;;
  esac
}

install_stable() {
  log "building $BIN_NAME from $REPO_PATH"
  mkdir -p "$INSTALL_DIR"
  (cd "$REPO_PATH" && go build -trimpath -o "$INSTALL_DIR/$BIN_NAME" ./cmd/foundry-cmgo)
  log "installed $BIN_NAME to $INSTALL_DIR/$BIN_NAME"
}

install_dev() {
  local wrapper_path="$REPO_PATH/scripts/$DEV_BIN_NAME"
  local link_path="$INSTALL_DIR/$DEV_BIN_NAME"

  [[ -f "$wrapper_path" ]] || fail "dev wrapper not found: $wrapper_path"
  chmod +x "$wrapper_path"
  mkdir -p "$INSTALL_DIR"
  rm -f "$link_path"
  ln -s "$wrapper_path" "$link_path"
  log "installed $DEV_BIN_NAME to $link_path"
  log "$DEV_BIN_NAME follows checkout: $REPO_PATH"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --install-dir)
      [[ $# -ge 2 ]] || fail "--install-dir requires a value"
      INSTALL_DIR="$2"
      shift 2
      ;;
    --repo-path)
      [[ $# -ge 2 ]] || fail "--repo-path requires a value"
      REPO_PATH="$2"
      shift 2
      ;;
    --stable-only)
      STABLE_ONLY=1
      shift
      ;;
    --dev-only)
      DEV_ONLY=1
      shift
      ;;
    --shell-rc-file)
      [[ $# -ge 2 ]] || fail "--shell-rc-file requires a value"
      SHELL_RC_FILES+=("$2")
      shift 2
      ;;
    --no-shell-update)
      NO_SHELL_UPDATE=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      fail "unknown argument: $1"
      ;;
  esac
done

if (( STABLE_ONLY == 1 && DEV_ONLY == 1 )); then
  fail "choose at most one of --stable-only or --dev-only"
fi

ensure_repo_path
INSTALL_DIR="$(abs_path "$INSTALL_DIR")"

if (( DEV_ONLY == 0 )); then
  install_stable
fi
if (( STABLE_ONLY == 0 )); then
  install_dev
fi

update_shell_path

if path_contains_install_dir; then
  log "$INSTALL_DIR is already on PATH"
elif (( NO_SHELL_UPDATE == 1 )); then
  log "add $INSTALL_DIR to PATH before running $BIN_NAME or $DEV_BIN_NAME"
else
  log "open a new shell or source your shell rc file if the commands are not visible yet"
fi
