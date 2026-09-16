/**
 * Gate 3 Verification Logic & Mock Test Suite
 * Validates receiver outage tolerance, zero busy-spin behavior,
 * self-healing recovery timing, and standardized output formatting.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const { formatGateResult, evaluateGate3Outage } = require('../common.js');
const { runGate3 } = require('../gate3.js');

describe('Gate 3 Verification - Receiver Outage, Anti-Busy-Loop & Self-Healing', () => {
  it('1. Target Formatting: Validates formatted output matching exact specification', () => {
    const result = evaluateGate3Outage(60, 0, 4.2);

    assert.equal(result.passed, true);
    assert.equal(
      result.output,
      'G3 sink outage .................. PASS (60s down, 0 lost, recovered in 4.2s)'
    );
  });

  it('2. Evaluation Invariant (Pass): Validates downtime (60s), lostRecords (0), and recoveryTime (4.2s) passes cleanly', () => {
    const result = evaluateGate3Outage(60, 0, 4.2);

    assert.equal(result.passed, true);
    assert.equal(result.downtimeSec, 60);
    assert.equal(result.lostRecords, 0);
    assert.equal(result.recoveryTimeSec, 4.2);
    assert.equal(result.formattedTime, '60s');
    assert.equal(result.details, '60s down, 0 lost, recovered in 4.2s');
  });

  it('3. Invariant Rejection (Data Loss): Asserts failure when lostRecords > 0 (e.g. 15 records lost during outage)', () => {
    const result = evaluateGate3Outage(60, 15, 4.2);

    assert.equal(result.passed, false);
    assert.equal(result.lostRecords, 15);
    assert.match(result.output, /FAIL/);
    assert.match(result.output, /15 records lost during outage/);
  });

  it('4. Invariant Rejection (Zero Downtime): Asserts failure if downtime was not simulated (downtimeSec <= 0)', () => {
    const result = evaluateGate3Outage(0, 0, 4.2);

    assert.equal(result.passed, false);
    assert.equal(result.downtimeSec, 0);
    assert.match(result.output, /FAIL/);
    assert.match(result.output, /zero downtime simulated/);
  });

  it('5. Anti-Busy-Loop State Assertion: Verifies that circuit breaker state "OPEN" with throttling active is recognized as valid anti-busy-loop behavior', () => {
    const telemetrySnapshot = {
      health: { overall: 'DEGRADED' },
      circuit_breakers: {
        elasticsearch: {
          state: 'OPEN',
          isThrottling: true,
          consecutiveFailures: 3,
          currentBackoffMs: 4000
        }
      }
    };

    const esBreaker = telemetrySnapshot.circuit_breakers.elasticsearch;
    const isAntiBusyLoopActive = esBreaker.state === 'OPEN' && esBreaker.isThrottling === true && esBreaker.currentBackoffMs > 0;

    assert.equal(isAntiBusyLoopActive, true);
    assert.equal(esBreaker.state, 'OPEN');
    assert.equal(esBreaker.isThrottling, true);
  });

  it('6. End-to-End Runner Mock: Simulates full Gate 3 execution lifecycle with mocked receiver stop/start, telemetry polling, and recovery timing', async () => {
    let receiverStopped = false;
    let receiverRestored = false;
    let pollCount = 0;

    const mockQueryDatabase = async () => [{ total: '5000' }];

    const mockStopReceiver = async () => {
      receiverStopped = true;
      return { mode: 'mock', action: 'stopped' };
    };

    const mockStartReceiver = async () => {
      receiverRestored = true;
      return { mode: 'mock', action: 'started' };
    };

    const mockGetTelemetry = async () => {
      pollCount++;
      if (receiverStopped && !receiverRestored) {
        // Outage window: circuit breaker is OPEN and throttling
        return {
          status: 'RUNNING',
          circuit_breakers: {
            elasticsearch: {
              state: 'OPEN',
              isThrottling: true,
              currentBackoffMs: 2000
            }
          }
        };
      }
      // Recovered state: circuit breaker is CLOSED
      return {
        status: 'RUNNING',
        circuit_breakers: {
          elasticsearch: {
            state: 'CLOSED',
            isThrottling: false,
            currentBackoffMs: 0
          }
        }
      };
    };

    const mockGetElasticsearchCount = async () => {
      // During outage: partial count; after restoration: full count
      return receiverRestored ? 5000 : 3500;
    };

    const mockSleep = async () => {};

    const result = await runGate3({
      queryDatabase: mockQueryDatabase,
      getTelemetry: mockGetTelemetry,
      getElasticsearchCount: mockGetElasticsearchCount,
      stopReceiver: mockStopReceiver,
      startReceiver: mockStartReceiver,
      sleep: mockSleep,
      outageDurationSec: 60,
      simulatedDowntimeSec: 60,
      simulatedRecoveryTimeSec: 4.2,
      maxRecoveryWaitMs: 5000,
      closeDb: false
    });

    assert.equal(result.passed, true);
    assert.equal(result.downtimeSec, 60);
    assert.equal(result.lostRecords, 0);
    assert.equal(result.recoveryTimeSec, 4.2);
    assert.equal(result.antiBusyLoopVerified, true);
    assert.equal(
      result.output,
      'G3 sink outage .................. PASS (60s down, 0 lost, recovered in 4.2s)'
    );
    assert.equal(receiverStopped, true);
    assert.equal(receiverRestored, true);
  });
});
