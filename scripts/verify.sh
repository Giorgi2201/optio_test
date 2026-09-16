#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

echo "======================================================================"
echo "              OPTIO SYSTEM VERIFICATION REPORT (v1.0)"
echo "======================================================================"

# Gate 1: Crash Recovery & Watermark Resumption
GATE1_STATUS=0
node "${SCRIPT_DIR}/verify/gate1.js" || GATE1_STATUS=$?

if [ ${GATE1_STATUS} -ne 0 ]; then
  echo "======================================================================"
  echo "VERIFICATION FAILED AT GATE 1 (Exit code: ${GATE1_STATUS})"
  echo "======================================================================"
  exit ${GATE1_STATUS}
fi

# Gate 2: Deduplication & Effectively-Once Delivery
GATE2_STATUS=0
node "${SCRIPT_DIR}/verify/gate2.js" || GATE2_STATUS=$?

if [ ${GATE2_STATUS} -ne 0 ]; then
  echo "======================================================================"
  echo "VERIFICATION FAILED AT GATE 2 (Exit code: ${GATE2_STATUS})"
  echo "======================================================================"
  exit ${GATE2_STATUS}
fi

# Gate 3: Receiver Outage, Anti-Busy-Loop & Self-Healing
GATE3_STATUS=0
node "${SCRIPT_DIR}/verify/gate3.js" || GATE3_STATUS=$?

if [ ${GATE3_STATUS} -ne 0 ]; then
  echo "======================================================================"
  echo "VERIFICATION FAILED AT GATE 3 (Exit code: ${GATE3_STATUS})"
  echo "======================================================================"
  exit ${GATE3_STATUS}
fi

echo "======================================================================"
echo "Gate 1, Gate 2 & Gate 3 verification completed successfully."
