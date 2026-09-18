/**
 * Shared Verification Framework Test Suite
 * Covers the quiescence coordinator that Gates 1-4 use to avoid asserting sink counts
 * while a background backfill or CDC backlog is still streaming.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const { waitForPipelineQuiescence } = require('../common.js');

function fakeClock() {
  let fakeNow = 0;
  return {
    now: () => fakeNow,
    sleep: async (ms) => {
      fakeNow += ms;
    }
  };
}

describe('Shared Verification Framework - Pipeline Quiescence', () => {
  it('1. Resolves immediately when backfill is COMPLETED and incremental lag is zero', async () => {
    let polls = 0;
    const res = await waitForPipelineQuiescence({
      ...fakeClock(),
      getTelemetry: async () => {
        polls++;
        return { backfill_status: 'COMPLETED', incremental_lag_records: 0 };
      },
      timeoutMs: 10000
    });

    assert.equal(res.quiesced, true);
    assert.equal(polls, 1);
    assert.equal(res.reason, null);
  });

  it('2. Keeps polling while a backfill storm or CDC backlog is draining, then resolves', async () => {
    let cursor = 11000;
    let lag = 489000;
    const res = await waitForPipelineQuiescence({
      ...fakeClock(),
      getTelemetry: async () => {
        cursor = Math.min(500000, cursor + 100000);
        lag = Math.max(0, lag - 100000);
        return {
          backfill_status: cursor >= 500000 ? 'COMPLETED' : 'RUNNING',
          backfill_cursor: cursor,
          incremental_lag_records: lag
        };
      },
      timeoutMs: 60000,
      pollMs: 500
    });

    assert.equal(res.quiesced, true);
    assert.equal(res.telemetry.backfill_status, 'COMPLETED');
    assert.equal(res.telemetry.incremental_lag_records, 0);
    assert.equal(res.elapsedMs, 2000);
  });

  it('3. Times out with a diagnostic reason when the pipeline never settles', async () => {
    const res = await waitForPipelineQuiescence({
      ...fakeClock(),
      getTelemetry: async () => ({ backfill_status: 'RUNNING', backfill_cursor: 250000, incremental_lag_records: 1200 }),
      timeoutMs: 3000,
      pollMs: 500
    });

    assert.equal(res.quiesced, false);
    assert.match(res.reason, /backfill_status=RUNNING/);
    assert.match(res.reason, /incremental_lag_records=1200/);
  });

  it('4. Gives up after the unreachable grace period when no daemon is answering telemetry at all', async () => {
    let polls = 0;
    const res = await waitForPipelineQuiescence({
      ...fakeClock(),
      getTelemetry: async () => {
        polls++;
        return null;
      },
      timeoutMs: 60000,
      unreachableGraceMs: 2000,
      pollMs: 500
    });

    assert.equal(res.quiesced, false);
    assert.equal(res.reason, 'telemetry unreachable');
    assert.equal(res.elapsedMs, 2000);
    assert.equal(polls, 5);
  });

  it('5. Rides through a transient daemon restart between gates and still resolves', async () => {
    let polls = 0;
    const res = await waitForPipelineQuiescence({
      ...fakeClock(),
      getTelemetry: async () => {
        polls++;
        // Down for the first 4 polls (container restarting), then healthy and quiescent
        if (polls <= 4) return null;
        return { backfill_status: 'COMPLETED', incremental_lag_records: 0 };
      },
      timeoutMs: 60000,
      unreachableGraceMs: 15000,
      pollMs: 500
    });

    assert.equal(res.quiesced, true);
    assert.equal(polls, 5);
  });
});
