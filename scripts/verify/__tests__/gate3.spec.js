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
  function createOutageFixture({
    breakerOpens = true,
    dropOne = false,
    telemetryDarkDuringOutage = false,
    halfOpenUntilCanary = false
  } = {}) {
    // versions[id] = version currently indexed in the fake Elasticsearch
    const state = { receiverDown: false, mutated: [], mutateCalls: 0, indexed: new Map(), sourceVersion: new Map() };
    let fakeNow = 5_000_000;
    let trips = 0;

    const currentSourceVersion = (id) => state.sourceVersion.get(id) ?? 1;

    const breakerState = () => {
      if (!breakerOpens) return 'CLOSED';
      if (state.receiverDown) return 'OPEN';
      // After restoration: HALF_OPEN until a canary write has happened (second success), then CLOSED
      if (halfOpenUntilCanary && state.mutateCalls < 2) return 'HALF_OPEN';
      return 'CLOSED';
    };

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
        if (breakerOpens) trips++;
        return { mode: 'mock', action: 'stopped' };
      },
      startReceiver: async () => {
        state.receiverDown = false;
        return { mode: 'mock', action: 'started' };
      },
      mutateSourceRecords: async (count) => {
        state.mutateCalls++;
        const rows = Array.from({ length: count }, (_, i) => {
          const id = 5000 - i;
          const version = currentSourceVersion(id) + 1;
          state.sourceVersion.set(id, version);
          return { id, version };
        });
        if (state.mutateCalls === 1) state.mutated = rows;
        return rows;
      },
      getTelemetry: async () => {
        if (telemetryDarkDuringOutage && state.receiverDown) return null;
        return {
          status: 'RUNNING',
          backfill_status: 'COMPLETED',
          incremental_lag_records: state.receiverDown ? state.mutated.length : 0,
          circuit_breakers: {
            elasticsearch: {
              state: breakerState(),
              isThrottling: breakerState() !== 'CLOSED',
              currentBackoffMs: breakerState() === 'OPEN' ? 2000 : 0,
              totalTrips: trips
            }
          }
        };
      },
      getElasticsearchCount: async () => (state.receiverDown ? null : 5000),
      getElasticsearchDocs: async (ids) => {
        if (state.receiverDown) return null;
        return ids.map((id, i) => ({
          id: String(id),
          found: true,
          // Replication catches up to the source version once the receiver is back,
          // except optionally one record left permanently stale to simulate loss.
          source: { id: String(id), version: dropOne && i === 0 ? 1 : currentSourceVersion(Number(id)) }
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

  it('10. Telemetry dark during the blackout: the breaker trip is still confirmed via its trip counter after restoration', async () => {
    const fixture = createOutageFixture({ telemetryDarkDuringOutage: true });

    const result = await runGate3({
      ...fixture,
      outageDurationMs: 5000,
      maxRecoveryWaitMs: 10000,
      quiesceTimeoutMs: 1000,
      silent: true,
      closeDb: false
    });

    assert.equal(result.passed, true);
    assert.equal(result.antiBusyLoopVerified, true);
    assert.deepEqual(result.breakerTrips, { before: 0, after: 1 });
    assert.equal(result.lostRecords, 0);
  });

  it('11. HALF_OPEN starvation: once outage traffic has landed, canary writes let the breaker close instead of timing out', async () => {
    const fixture = createOutageFixture({ halfOpenUntilCanary: true });

    const result = await runGate3({
      ...fixture,
      outageDurationMs: 5000,
      mutationCount: 200,
      canarySize: 10,
      canaryIntervalMs: 0,
      maxRecoveryWaitMs: 10000,
      quiesceTimeoutMs: 1000,
      silent: true,
      closeDb: false
    });

    assert.equal(result.passed, true);
    assert.equal(result.canariesFired, 1);
    assert.equal(fixture.state.mutateCalls, 2);
    // Canary rows are verified too (they overlap the top-200 ids, so the set stays at 200)
    assert.equal(result.verifiedRecords, 200);
    assert.equal(result.lostRecords, 0);
    assert.match(result.output, /PASS/);
  });

  it('12. Canary budget is bounded: a breaker that never closes fails honestly after the recovery window', async () => {
    const fixture = createOutageFixture({ halfOpenUntilCanary: true });
    // Breaker stays HALF_OPEN no matter how many canaries land
    fixture.state.mutateCalls = -Infinity;
    const stuckMutate = fixture.mutateSourceRecords;
    fixture.mutateSourceRecords = async (count) => {
      const rows = await stuckMutate(count);
      fixture.state.mutateCalls = -Infinity;
      return rows;
    };

    const result = await runGate3({
      ...fixture,
      outageDurationMs: 5000,
      canaryIntervalMs: 0,
      maxCanaries: 3,
      maxRecoveryWaitMs: 5000,
      quiesceTimeoutMs: 1000,
      silent: true,
      closeDb: false
    });

    assert.equal(result.passed, false);
    assert.equal(result.canariesFired, 3);
    assert.match(result.output, /recovery timed out or failed/);
  });
});
