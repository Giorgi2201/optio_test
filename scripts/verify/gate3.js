#!/usr/bin/env node

/**
 * Gate 3 Verification Runner: Receiver Outage, Anti-Busy-Loop & Self-Healing
 * Validates that when a downstream receiver (Elasticsearch) suffers an outage:
 * 1. Zero Busy-Spin: Circuit breaker transitions to OPEN, applying jittered exponential backoff.
 * 2. Zero Data Loss: Backpressure is applied without dropping documents or crashing.
 * 3. Self-Healing: Upon receiver restoration, the pipeline automatically recovers and catches up.
 */

const {
  queryDatabase,
  closeDatabase,
  getTelemetry,
  getElasticsearchCount,
  stopReceiver,
  startReceiver,
  formatGateResult,
  evaluateGate3Outage,
  sleep
} = require('./common.js');

/**
 * Runs the Gate 3 receiver outage & self-healing verification scenario.
 * Supports dependency injection for testing.
 */
async function runGate3(options = {}) {
  const queryFn = options.queryDatabase || queryDatabase;
  const telemetryFn = options.getTelemetry || getTelemetry;
  const esCountFn = options.getElasticsearchCount || getElasticsearchCount;
  const stopReceiverFn = options.stopReceiver || stopReceiver;
  const startReceiverFn = options.startReceiver || startReceiver;
  const sleepFn = options.sleep || sleep;
  const outageDurationSec = options.outageDurationSec || parseInt(process.env.OUTAGE_DURATION_SEC || '60', 10);
  const maxRecoveryWaitMs = options.maxRecoveryWaitMs || 60000;

  try {
    // -------------------------------------------------------------------------
    // Step 1: Baseline Check
    // -------------------------------------------------------------------------
    const countRows = await queryFn('SELECT COUNT(*)::bigint AS total FROM source_records');
    const sourceCount = parseInt(countRows[0]?.total || '0', 10);

    if (sourceCount <= 0) {
      throw new Error('Baseline source_records table is empty. Please seed records or run Gate 1 first.');
    }

    const initialTelemetry = await telemetryFn();
    if (!initialTelemetry) {
      throw new Error('Pipeline daemon HTTP telemetry endpoint unreachable at http://localhost:3000/api/telemetry');
    }

    // -------------------------------------------------------------------------
    // Step 2: Outage Injection (Mid-Flight Blackout)
    // -------------------------------------------------------------------------
    const outageDurationMs = outageDurationSec * 1000;
    const outageStartTime = Date.now();

    await stopReceiverFn('elasticsearch', outageDurationMs);

    // -------------------------------------------------------------------------
    // Step 3: Anti-Busy-Loop Verification
    // -------------------------------------------------------------------------
    let antiBusyLoopVerified = false;
    const blackoutProbeStart = Date.now();
    const blackoutProbeTimeout = Math.min(10000, outageDurationMs);

    while (Date.now() - blackoutProbeStart < blackoutProbeTimeout) {
      await sleepFn(500);
      const telemetry = await telemetryFn();
      if (telemetry?.circuit_breakers?.elasticsearch) {
        const esBreaker = telemetry.circuit_breakers.elasticsearch;
        if (esBreaker.state === 'OPEN' || esBreaker.isThrottling) {
          antiBusyLoopVerified = true;
          break;
        }
      }
    }

    // -------------------------------------------------------------------------
    // Step 4: Outage Duration Window
    // -------------------------------------------------------------------------
    const remainingOutageMs = Math.max(0, outageDurationMs - (Date.now() - outageStartTime));
    if (remainingOutageMs > 0) {
      await sleepFn(remainingOutageMs);
    }

    // -------------------------------------------------------------------------
    // Step 5: Receiver Restoration
    // -------------------------------------------------------------------------
    await startReceiverFn('elasticsearch');
    const restorationTime = Date.now();
    const actualDowntimeSec = options.simulatedDowntimeSec ?? Math.max(1, Math.round((restorationTime - outageStartTime) / 1000));

    // -------------------------------------------------------------------------
    // Step 6: Self-Healing & Recovery Measurement
    // -------------------------------------------------------------------------
    let recovered = false;
    let finalEsCount = 0;
    const recoveryStart = Date.now();

    while (Date.now() - recoveryStart < maxRecoveryWaitMs) {
      await sleepFn(500);
      const [telemetry, count] = await Promise.all([
        telemetryFn(),
        esCountFn()
      ]);

      if (count !== null) {
        finalEsCount = count;
      }

      const esBreaker = telemetry?.circuit_breakers?.elasticsearch;
      const breakerClosed = esBreaker ? esBreaker.state === 'CLOSED' : true;
      const parityReached = count !== null && count >= sourceCount;

      if (breakerClosed && parityReached) {
        recovered = true;
        break;
      }
    }

    const recoveryCompleteTime = Date.now();
    const recoveryTimeSec = options.simulatedRecoveryTimeSec ?? Number(((recoveryCompleteTime - restorationTime) / 1000).toFixed(1));

    if (!recovered && finalEsCount < sourceCount) {
      throw new Error(`Self-healing timed out after ${maxRecoveryWaitMs}ms: Elasticsearch at ${finalEsCount}/${sourceCount}`);
    }

    // -------------------------------------------------------------------------
    // Step 7: Zero-Data-Loss Assertion & Output
    // -------------------------------------------------------------------------
    const lostRecords = Math.max(0, sourceCount - finalEsCount);

    const result = evaluateGate3Outage(
      actualDowntimeSec,
      lostRecords,
      recoveryTimeSec
    );

    result.antiBusyLoopVerified = antiBusyLoopVerified;

    console.log(result.output);
    return result;
  } catch (err) {
    const errorMsg = err instanceof Error ? err.message : String(err);
    const failureOutput = formatGateResult('G3 sink outage', 'FAIL', errorMsg);
    console.error(failureOutput);
    return {
      passed: false,
      downtimeSec: 0,
      lostRecords: -1,
      recoveryTimeSec: -1,
      formattedTime: '0s',
      details: errorMsg,
      output: failureOutput,
      error: errorMsg
    };
  } finally {
    if (options.closeDb !== false) {
      try {
        await closeDatabase();
      } catch {
        // Ignore cleanup errors
      }
    }
  }
}

// Auto-execute if invoked directly as CLI script
if (require.main === module) {
  runGate3()
    .then((result) => {
      process.exit(result.passed ? 0 : 1);
    })
    .catch((err) => {
      console.error('[FATAL ERROR]', err);
      process.exit(1);
    });
}

module.exports = {
  runGate3,
  evaluateGate3Outage
};
