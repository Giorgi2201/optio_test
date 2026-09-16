#!/usr/bin/env bash
set -euo pipefail

# Unified verification harness entrypoint
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

exec node "${ROOT_DIR}/scripts/verify/index.js" "$@"
