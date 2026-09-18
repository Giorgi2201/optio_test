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

  it('6. Invariant Rejection (Busy-Loop): Asserts failure when the circuit breaker was never observed OPEN / throttling', () => {
    const result = evaluateGate3Outage(60, 0, 4.2, false);

    assert.equal(result.passed, false);
    assert.equal(result.antiBusyLoopVerified, false);
    assert.match(result.output, /FAIL/);
    assert.match(result.output, /circuit breaker never opened/);
  });

  /**
   * Fake receiver + pipeline for orchestration tests. Time is virtual: sleep() advances the clock,
   * so measured downtime / recovery are deterministic.
   */
  function createOutageFixture({ breakerOpens = true, dropOne = false } = {}) {
    const state = { receiverDown: false, mutated: [] };
    let fakeNow = 5_000_000;

    return {
      state,
      now: () => fakeNow,
      sleep: async (ms) => {
        fakeNow += ms;
      },
      queryDatabase: async (sql) => (sql.includes('dead_letter_queue') ? [{ es_rejected: 0 }] : [{ total: '5000' }]),
      refreshElasticsearch: async () => !state.receiverDown,
      stopReceiver: async () => {
        state.receiverDown = true;
        return { mode: 'mock', action: 'stopped' };
      },
      startReceiver: async () => {
        state.receiverDown = false;
        return { mode: 'mock', action: 'started' };
      },
      mutateSourceRecords: async (count) => {
        state.mutated = Array.from({ length: count }, (_, i) => ({ id: 5000 - i, version: 2 }));
        return state.mutated;
      },
      getTelemetry: async () => ({
        status: 'RUNNING',
        backfill_status: 'COMPLETED',
        incremental_lag_records: state.receiverDown ? state.mutated.length : 0,
        circuit_breakers: {
          elasticsearch:
            state.receiverDown && breakerOpens
              ? { state: 'OPEN', isThrottling: true, currentBackoffMs: 2000, totalTrips: 1 }
              : { state: 'CLOSED', isThrottling: false, currentBackoffMs: 0, totalTrips: breakerOpens ? 1 : 0 }
        }
      }),
      getElasticsearchCount: async () => (state.receiverDown ? null : 5000),
      getElasticsearchDocs: async (ids) => {
        if (state.receiverDown) return null;
        return ids.map((id, i) => ({
          id: String(id),
          found: true,
          // Optionally leave one mutation permanently unreplicated to simulate loss
          source: { id: String(id), version: dropOne && i === 0 ? 1 : 2 }
        }));
      }
    };
  }

  it('7. End-to-End Runner Mock: outage -> mutation burst -> breaker OPEN -> restore -> every mutation lands, with measured downtime and recovery', async () => {
    const fixture = createOutageFixture();

    const result = await runGate3({
      ...fixture,
      outageDurationMs: 60000,
      mutationCount: 200,
      maxRecoveryWaitMs: 10000,
      quiesceTimeoutMs: 1000,
      silent: true,
      closeDb: false
    });

    assert.equal(result.passed, true);
    assert.equal(result.downtimeSec, 60);
    assert.equal(result.lostRecords, 0);
    assert.equal(result.mutatedCount, 200);
    assert.equal(result.antiBusyLoopVerified, true);
    assert.equal(result.recoveryTimeSec, 0.5);
    assert.equal(fixture.state.receiverDown, false);
    assert.equal(
      result.output,
      'G3 sink outage .................. PASS (60s down, 0 lost, recovered in 0.5s)'
    );
  });

  it('8. Invariant Rejection: fails when the breaker never opens during the blackout (busy-spin risk)', async () => {
    const fixture = createOutageFixture({ breakerOpens: false });

    const result = await runGate3({
      ...fixture,
      outageDurationMs: 5000,
      maxRecoveryWaitMs: 10000,
      quiesceTimeoutMs: 1000,
      silent: true,
      closeDb: false
    });

    assert.equal(result.passed, false);
    assert.equal(result.antiBusyLoopVerified, false);
    assert.equal(result.lostRecords, 0);
    assert.match(result.output, /circuit breaker never opened/);
  });

  it('9. Invariant Rejection: a mutation that never reaches Elasticsearch after restoration is counted as lost', async () => {
    const fixture = createOutageFixture({ dropOne: true });

    const result = await runGate3({
      ...fixture,
      outageDurationMs: 5000,
      mutationCount: 50,
      maxRecoveryWaitMs: 3000,
      quiesceTimeoutMs: 1000,
      silent: true,
      closeDb: false
    });

    assert.equal(result.passed, false);
    assert.equal(result.lostRecords, 1);
    assert.match(result.output, /1 records lost during outage/);
    // Receiver is always restored, even on failure
    assert.equal(fixture.state.receiverDown, false);
  });
});
