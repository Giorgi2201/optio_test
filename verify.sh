#!/usr/bin/env bash
set -euo pipefail

# Root verify.sh entrypoint delegating to scripts/verify.sh
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [[ -f "${SCRIPT_DIR}/scripts/verify.sh" ]]; then
  exec bash "${SCRIPT_DIR}/scripts/verify.sh" "$@"
else
  echo "Error: Verification harness scripts/verify.sh not found!" >&2
  exit 1
fi
