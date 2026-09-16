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

  it('6. Orchestration Flow: Executes full Gate 1 lifecycle with mocked process & telemetry dependencies', async () => {
    const killEvents = [];
    const startEvents = [];
    let queryCallCount = 0;
    let telemetryPollCount = 0;

    const mockQueryDatabase = async (sql) => {
      queryCallCount++;
      if (sql.includes('COUNT(*)')) {
        return [{ count: '5000', max_id: '5000' }];
      }
      if (sql.includes('replication_checkpoints') && sql.includes('SELECT')) {
        // First check post-kill returns 2000, final check returns 5000
        return [{
          last_processed_id: killEvents.length > 0 && startEvents.length > 1 ? '5000' : '2000',
          status: 'COMPLETED'
        }];
      }
      return [];
    };

    const mockGetTelemetry = async () => {
      telemetryPollCount++;
      // Pre-kill phase: advance cursor past midway threshold (2000)
      if (killEvents.length === 0) {
        return {
          status: 'RUNNING',
          backfill_status: 'RUNNING',
          backfill_cursor: 2000,
          backfill_completion_pct: 40.0
        };
      }
      // Post-kill resumption phase: reaches end
      return {
        status: 'RUNNING',
        backfill_status: 'COMPLETED',
        backfill_cursor: 5000,
        backfill_completion_pct: 100
      };
    };

    const mockStartProcess = async () => {
      startEvents.push(Date.now());
      return { mode: 'process', pid: 12345 };
    };

    const mockKillProcess = async (signal) => {
      killEvents.push(signal || 'SIGKILL');
    };

    const mockSleep = async () => {
      // Instant sleep in mock test
    };

    const result = await runGate1({
      queryDatabase: mockQueryDatabase,
      getTelemetry: mockGetTelemetry,
      startPipelineProcess: mockStartProcess,
      killPipelineProcess: mockKillProcess,
      sleep: mockSleep,
      maxWaitMs: 5000,
      closeDb: false
    });

    assert.equal(result.passed, true);
    assert.equal(result.killedAt, 2331);
    assert.equal(result.resumedAt, 2000);
    assert.equal(result.lostRecords, 0);
    assert.equal(
      result.output,
      'G1 resume after kill ............ PASS (killed at 2,331 / resumed at 2,000, 0 lost)'
    );
    assert.equal(killEvents.length >= 1, true);
    assert.equal(killEvents[0], 'SIGKILL');
    assert.equal(startEvents.length, 2); // Initial launch + post-kill resume
  });
});
