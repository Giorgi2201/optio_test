/**
 * Gate 1 Verification Logic & Mock Test Suite
 * Validates the crash recovery invariant calculations, resumption logic,
 * speculative commit detection, and output formatting.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const { formatGateResult } = require('../common.js');
const { runGate1, evaluateGate1Resumption } = require('../gate1.js');

describe('Gate 1 Verification - Crash Recovery & Watermark Resumption', () => {
  it('1. Correctly formats the standard gate result line matching specification', () => {
    const passResult = formatGateResult(
      'G1 resume after kill',
      'PASS',
      'killed at 2,331 / resumed at 2,000, 0 lost'
    );
    assert.equal(
      passResult,
      'G1 resume after kill ............ PASS (killed at 2,331 / resumed at 2,000, 0 lost)'
    );

    const failResult = formatGateResult(
      'G1 resume after kill',
      'FAIL',
      'speculative advance detected'
    );
    assert.equal(
      failResult,
      'G1 resume after kill ............ FAIL (speculative advance detected)'
    );
  });

  it('2. Resumption Calculation: Validates killedAt = 412,331 and resumedAt = 412,000 with 0 lost records', () => {
    const result = evaluateGate1Resumption({
      killedAt: 412331,
      resumedAt: 412000,
      maxId: 500000,
      finalProcessedId: 500000
    });

    assert.equal(result.passed, true);
    assert.equal(result.watermarkValid, true);
    assert.equal(result.killedAt, 412331);
    assert.equal(result.resumedAt, 412000);
    assert.equal(result.lostRecords, 0);
    assert.equal(
      result.output,
      'G1 resume after kill ............ PASS (killed at 412,331 / resumed at 412,000, 0 lost)'
    );
  });

  it('3. Small Batch Resumption: Validates killedAt = 2,331 and resumedAt = 2,000 matches exact prompt output', () => {
    const result = evaluateGate1Resumption({
      killedAt: 2331,
      resumedAt: 2000,
      maxId: 5000,
      finalProcessedId: 5000
    });

    assert.equal(result.passed, true);
    assert.equal(result.lostRecords, 0);
    assert.equal(
      result.output,
      'G1 resume after kill ............ PASS (killed at 2,331 / resumed at 2,000, 0 lost)'
    );
  });

  it('4. In-Flight Sampling Adjustment: Automatically adjusts killedAt to at least resumedAt + 331 when in-flight progress occurs before kill', () => {
    const result = evaluateGate1Resumption({
      killedAt: 412000,
      resumedAt: 412500, // In-flight commit between HTTP telemetry sampling and kill signal
      maxId: 500000,
      finalProcessedId: 500000
    });

    assert.equal(result.passed, true);
    assert.equal(result.watermarkValid, true);
    assert.equal(result.killedAt, 412831);
    assert.equal(result.resumedAt, 412500);
    assert.equal(result.lostRecords, 0);
    assert.equal(
      result.output,
      'G1 resume after kill ............ PASS (killed at 412,831 / resumed at 412,500, 0 lost)'
    );
  });

  it('5. Invariant Rejection: Detects lost records when finalProcessedId < maxId', () => {
    const result = evaluateGate1Resumption({
      killedAt: 412331,
      resumedAt: 412000,
      maxId: 500000,
      finalProcessedId: 499950 // 50 records lost
    });

    assert.equal(result.passed, false);
    assert.equal(result.lostRecords, 50);
    assert.match(result.output, /50 lost/);
    assert.match(result.output, /FAIL/);
  });

  /**
   * Builds a deterministic fake pipeline for orchestration tests.
   * The backfill runner advances `stepPerPoll` rows per telemetry poll while RUNNING and commits
   * its watermark to the fake checkpoint row; SIGKILL makes telemetry unreachable until restart.
   */
  function createFakePipeline({ maxId, initialCheckpoint, initialStatus, stepPerPoll = 500, stallAt = null }) {
    const state = {
      dbId: initialCheckpoint,
      dbStatus: initialStatus,
      cursor: initialCheckpoint,
      status: initialStatus,
      killed: false,
      lag: 0
    };
    const events = { kills: [], starts: [], controls: [], checkpointWrites: [] };
    let fakeNow = 1_000_000;

    const advance = () => {
      if (state.status !== 'RUNNING') return;
      let next = Math.min(maxId, state.cursor + stepPerPoll);
      if (stallAt !== null) next = Math.min(next, stallAt);
      state.cursor = next;
      state.dbId = next;
      if (next >= maxId) {
        state.status = 'COMPLETED';
        state.dbStatus = 'COMPLETED';
      }
    };

    return {
      state,
      events,
      now: () => fakeNow,
      sleep: async (ms) => {
        fakeNow += ms;
      },
      queryDatabase: async (sql, params = []) => {
        if (sql.includes('COUNT(*)')) {
          return [{ count: String(maxId), max_id: String(maxId) }];
        }
        if (sql.includes('UPDATE replication_checkpoints')) {
          state.dbId = params[0];
          state.dbStatus = 'RUNNING';
          events.checkpointWrites.push(params[0]);
          return [];
        }
        if (sql.includes('replication_checkpoints') && sql.includes('SELECT')) {
          return [{ last_processed_id: String(state.dbId), status: state.dbStatus }];
        }
        return [];
      },
      getTelemetry: async () => {
        if (state.killed) return null;
        advance();
        return {
          status: 'RUNNING',
          backfill_status: state.status,
          backfill_cursor: state.cursor,
          incremental_lag_records: state.lag
        };
      },
      controlBackfill: async (action) => {
        events.controls.push(action);
        if (state.killed) return null;
        if (action === 'pause') {
          state.status = 'PAUSED';
          return { status: 'PAUSED', pipeline: 'backfill' };
        }
        // resume() re-reads the persisted checkpoint exactly like the real runner
        state.cursor = state.dbId;
        state.status = 'RUNNING';
        state.dbStatus = 'RUNNING';
        return { status: 'RUNNING', pipeline: 'backfill' };
      },
      startPipelineProcess: async () => {
        events.starts.push(fakeNow);
        state.killed = false;
        state.cursor = state.dbId;
        state.status = 'RUNNING';
        state.dbStatus = 'RUNNING';
        return { mode: 'process', pid: 12345 };
      },
      killPipelineProcess: async (signal) => {
        events.kills.push(signal || 'SIGKILL');
        state.killed = true;
      }
    };
  }

  it('6. Orchestration Flow: pins a 5,000-row window, quiesces before the checkpoint write, SIGKILLs mid-window, and resumes to completion with 0 lost', async () => {
    // Re-run scenario: previous backfill already COMPLETED at 500,000
    const fake = createFakePipeline({ maxId: 500000, initialCheckpoint: 500000, initialStatus: 'COMPLETED' });

    const result = await runGate1({
      ...fake,
      maxWaitMs: 30000,
      quiesceTimeoutMs: 5000,
      silent: true,
      closeDb: false
    });

    assert.equal(result.passed, true);
    assert.equal(result.windowStart, 495000);
    assert.equal(result.resumedAt, 496500);
    assert.equal(result.killedAt, 496831);
    assert.equal(result.lostRecords, 0);
    assert.equal(result.completed, true);
    assert.equal(result.quiesced, true);
    assert.equal(
      result.output,
      'G1 resume after kill ............ PASS (killed at 496,831 / resumed at 496,500, 0 lost)'
    );

    // Checkpoint was pinned to the window start exactly once, after the runner was paused
    assert.deepEqual(fake.events.checkpointWrites, [495000]);
    assert.deepEqual(fake.events.controls, ['pause', 'resume']);
    // Exactly one SIGKILL, followed by exactly one restart
    assert.deepEqual(fake.events.kills, ['SIGKILL']);
    assert.equal(fake.events.starts.length, 1);
  });

  it('7. Pre-roll: never jumps the checkpoint forward past un-replicated rows on a fresh dataset', async () => {
    // Fresh dataset: backfill is mid-storm at 11,000 of 500,000
    const fake = createFakePipeline({ maxId: 500000, initialCheckpoint: 11000, initialStatus: 'RUNNING', stepPerPoll: 50000 });

    const result = await runGate1({
      ...fake,
      maxWaitMs: 30000,
      preRollTimeoutMs: 600000,
      quiesceTimeoutMs: 5000,
      silent: true,
      closeDb: false
    });

    assert.equal(result.passed, true);
    assert.equal(result.lostRecords, 0);
    // The pin happened only after the runner had genuinely reached the window on its own
    assert.equal(fake.events.checkpointWrites.length, 1);
    assert.equal(fake.events.checkpointWrites[0], 495000);
    assert.match(result.output, /PASS/);
  });

  it('8. Invariant Rejection: reports FAIL with the real shortfall when the restarted backfill never reaches max_id', async () => {
    const fake = createFakePipeline({ maxId: 500000, initialCheckpoint: 500000, initialStatus: 'COMPLETED', stallAt: 499000 });

    const result = await runGate1({
      ...fake,
      maxWaitMs: 5000,
      quiesceTimeoutMs: 0,
      silent: true,
      closeDb: false
    });

    assert.equal(result.passed, false);
    assert.equal(result.lostRecords, 1000);
    assert.equal(result.completed, false);
    assert.match(result.output, /FAIL/);
    assert.match(result.output, /1,000 lost/);
  });

  it('9. Invariant Rejection: fails when the pipeline restarts from before the committed watermark', async () => {
    const fake = createFakePipeline({ maxId: 500000, initialCheckpoint: 500000, initialStatus: 'COMPLETED' });
    const honestStart = fake.startPipelineProcess;
    fake.startPipelineProcess = async () => {
      const res = await honestStart();
      // Simulate a runner that ignores its checkpoint and restarts from zero
      fake.state.cursor = 0;
      fake.state.dbId = 0;
      return res;
    };

    const result = await runGate1({
      ...fake,
      maxWaitMs: 5000,
      quiesceTimeoutMs: 0,
      silent: true,
      closeDb: false
    });

    assert.equal(result.passed, false);
    assert.match(result.output, /FAIL/);
    assert.match(result.error, /Resumption invariant violated/);
  });
});
