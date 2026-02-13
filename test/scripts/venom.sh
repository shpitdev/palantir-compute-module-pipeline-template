#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"

VENOM_VERSION="${VENOM_VERSION:-v1.3.0}"
OUT_BIN_DIR="${OUT_BIN_DIR:-${ROOT_DIR}/out/bin}"
VENOM_BIN="${VENOM_BIN:-${OUT_BIN_DIR}/venom}"

mkdir -p "${OUT_BIN_DIR}"

os="$(uname -s | tr '[:upper:]' '[:lower:]')"
arch="$(uname -m)"

case "${arch}" in
  x86_64|amd64) arch="amd64" ;;
  aarch64|arm64) arch="arm64" ;;
  armv7l|armv6l|arm) arch="arm" ;;
esac

asset="venom.${os}-${arch}"
if [[ "${os}" == "darwin" ]]; then
  asset="venom.darwin-${arch}"
elif [[ "${os}" == "linux" ]]; then
  asset="venom.linux-${arch}"
fi

if [[ ! -x "${VENOM_BIN}" ]]; then
  url="https://github.com/ovh/venom/releases/download/${VENOM_VERSION}/${asset}"
  echo "installing venom ${VENOM_VERSION} -> ${VENOM_BIN} (${asset})" >&2
  curl -fsSL -o "${VENOM_BIN}.tmp" "${url}"
  chmod +x "${VENOM_BIN}.tmp"
  mv "${VENOM_BIN}.tmp" "${VENOM_BIN}"
fi

exec "${VENOM_BIN}" "$@"
