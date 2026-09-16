#!/usr/bin/env node

/**
 * Gate 5 Verification Runner: Observability & Introspection Verification
 * Validates that operational health and metrics endpoints expose complete telemetry
 * answering the 5 fundamental operational questions:
 * 1. Where is the backfill? (cursor & completion pct)
 * 2. What is current throughput? (eps >= 0)
 * 3. How much incremental lag is there? (lag records & lag ms >= 0)
 * 4. How many records are in the DLQ? (dlq count >= 0)
 * 5. Is the system healthy or not? (overall in HEALTHY/DEGRADED/DOWN + postgres, elasticsearch, rabbitmq)
 */

const {
  getTelemetry,
  evaluateGate5Observability,
  sleep
} = require('./common.js');

/**
 * Runs the Gate 5 observability verification scenario.
 * Supports dependency injection for testing.
 */
async function runGate5(options = {}) {
  const getTelemetryFn = options.getTelemetry || getTelemetry;
  const sleepFn = options.sleep || sleep;
  const maxWaitMs = options.maxWaitMs || 10000;
  const pollIntervalMs = options.pollIntervalMs || 500;

  // Direct telemetry injection for tests or in-process coordinators
  let telemetry = options.telemetry || null;

  if (!telemetry) {
    const startWait = Date.now();
    while (Date.now() - startWait < maxWaitMs) {
      try {
        telemetry = await getTelemetryFn(options.url);
        if (telemetry) {
          break;
        }
      } catch {
        // Retry polling
      }
      await sleepFn(pollIntervalMs);
    }
  }

  const result = evaluateGate5Observability(telemetry);

  if (!options.silent) {
    console.log(result.output);
  }

  return result;
}

// Auto-execute if invoked directly as CLI script
if (require.main === module) {
  runGate5()
    .then((result) => {
      process.exit(result.passed ? 0 : 1);
    })
    .catch((err) => {
      console.error('[FATAL ERROR]', err);
      process.exit(1);
    });
}

module.exports = {
  runGate5,
  evaluateGate5Observability
};
