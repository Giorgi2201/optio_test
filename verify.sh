#!/usr/bin/env bash
set -euo pipefail

# Root verify.sh entrypoint executing the unified verification orchestrator
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

exec node "${SCRIPT_DIR}/scripts/verify/index.js" "$@"
