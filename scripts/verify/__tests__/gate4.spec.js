/**
 * Gate 4 Verification Logic & Mock Test Suite
 * Validates partial batch failure tolerance, poison pill DLQ isolation,
 * prevention of total batch rollbacks, and retry context preservation.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const { formatGateResult, evaluateGate4PartialFailure } = require('../common.js');
const { runGate4 } = require('../gate4.js');

describe('Gate 4 Verification - Partial Batch Failure & DLQ Quarantine', () => {
  it('1. Target Formatting: Validates formatted output matching exact specification', () => {
    const result = evaluateGate4PartialFailure(497, 3, false, true);

    assert.equal(result.passed, true);
    assert.equal(
      result.output,
      'G4 partial batch failure ........ PASS (497 written, 3 in DLQ)'
    );
  });

  it('2. Evaluation Invariant (Pass): Validates writtenCount (497), dlqCount (3), batchRolledBack (false), and contextSufficient (true) passes cleanly', () => {
    const result = evaluateGate4PartialFailure(497, 3, false, true);

    assert.equal(result.passed, true);
    assert.equal(result.writtenCount, 497);
    assert.equal(result.dlqCount, 3);
    assert.equal(result.batchRolledBack, false);
    assert.equal(result.contextSufficient, true);
    assert.equal(result.details, '497 written, 3 in DLQ');
  });

  it('3. Invariant Rejection (Full Batch Rollback): Asserts failure when writtenCount === 0 (entire batch aborted)', () => {
    const result = evaluateGate4PartialFailure(0, 3, true, true);

    assert.equal(result.passed, false);
    assert.match(result.output, /FAIL/);
    assert.match(result.output, /batch was rolled back/);
  });

  it('4. Invariant Rejection (Dropped Poison Pills): Asserts failure when dlqCount !== 3 (records swallowed/dropped without DLQ routing)', () => {
    // Only 1 record quarantined, 2 dropped silently
    const result = evaluateGate4PartialFailure(497, 1, false, true);

    assert.equal(result.passed, false);
    assert.match(result.output, /FAIL/);
    assert.match(result.output, /1 in DLQ \(expected 3\)/);
  });

  it('5. Invariant Rejection (Insufficient Context): Asserts failure when DLQ rows lack error reasons or payloads needed for retry', () => {
    const result = evaluateGate4PartialFailure(497, 3, false, false);

    assert.equal(result.passed, false);
    assert.match(result.output, /FAIL/);
    assert.match(result.output, /insufficient DLQ retry context/);
  });

  it('6. End-to-End Runner Mock: Simulates full Gate 4 execution lifecycle with mocked DB, sinks, and DLQ queries', async () => {
    let seedCalled = false;
    let dlqQueried = false;

    const mockQueryDatabase = async (sql) => {
      if (sql.includes('dead_letter_queue')) {
        return [{ count: '10' }];
      }
      return [];
    };

    const mockSeedGate4Batch = async (total, corrupted) => {
      seedCalled = true;
      const insertedIds = Array.from({ length: total }, (_, i) => i + 1);
      const corruptedIds = [101, 202, 303];
      return { insertedIds, corruptedIds };
    };

    let esCalls = 0;
    const mockGetElasticsearchCount = async () => {
      esCalls++;
      return esCalls === 1 ? 5000 : 5497; // 5000 baseline, then 5497 (497 written)
    };

    const mockGetDLQEntries = async (ids) => {
      dlqQueried = true;
      return ids.map((id) => ({
        id,
        record_id: id,
        sink_target: 'ELASTICSEARCH',
        payload: { customer_id: `poison_${id}`, balance: 'NOT_A_NUMBER' },
        error_code: 'MAPPER_PARSING_EXCEPTION',
        error_message: 'Failed to parse field balance as float',
        status: 'PENDING',
        retry_count: 0
      }));
    };

    let fakeNow = 0;

    const result = await runGate4({
      queryDatabase: mockQueryDatabase,
      seedGate4Batch: mockSeedGate4Batch,
      getElasticsearchCount: mockGetElasticsearchCount,
      getDLQEntriesForRecords: mockGetDLQEntries,
      refreshElasticsearch: async () => true,
      getTelemetry: async () => ({ backfill_status: 'COMPLETED', incremental_lag_records: 0 }),
      sleep: async (ms) => {
        fakeNow += ms;
      },
      now: () => fakeNow,
      batchTotal: 500,
      corruptedCount: 3,
      maxWaitMs: 5000,
      silent: true,
      closeDb: false
    });

    assert.equal(result.passed, true);
    assert.equal(result.writtenCount, 497);
    assert.equal(result.dlqCount, 3);
    assert.equal(result.contextSufficient, true);
    assert.equal(result.quiesced, true);
    assert.equal(
      result.output,
      'G4 partial batch failure ........ PASS (497 written, 3 in DLQ)'
    );
    assert.equal(seedCalled, true);
    assert.equal(dlqQueried, true);
  });

  it('7. Measured Counts: written count comes from the real index delta, not from the runner options', async () => {
    let esCalls = 0;
    let fakeNow = 0;

    const result = await runGate4({
      queryDatabase: async () => [],
      seedGate4Batch: async (total) => ({
        insertedIds: Array.from({ length: total }, (_, i) => i + 1),
        corruptedIds: [101, 202, 303]
      }),
      // Only 490 of 497 valid records ever land
      getElasticsearchCount: async () => {
        esCalls++;
        return esCalls === 1 ? 5000 : 5490;
      },
      getDLQEntriesForRecords: async (ids) =>
        ids.map((id) => ({
          id,
          record_id: id,
          sink_target: 'ELASTICSEARCH',
          payload: { balance: 'NaN' },
          error_code: 'MAPPER_PARSING_EXCEPTION',
          error_message: 'bad balance',
          status: 'PENDING',
          retry_count: 0
        })),
      refreshElasticsearch: async () => true,
      getTelemetry: async () => ({ backfill_status: 'COMPLETED', incremental_lag_records: 0 }),
      sleep: async (ms) => {
        fakeNow += ms;
      },
      now: () => fakeNow,
      maxWaitMs: 3000,
      silent: true,
      closeDb: false
    });

    assert.equal(result.passed, false);
    assert.equal(result.writtenCount, 490);
    assert.match(result.output, /490 written \(expected 497\)/);
  });
});
