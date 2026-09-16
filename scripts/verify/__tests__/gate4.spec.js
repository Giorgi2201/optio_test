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

    const mockSleep = async () => {};

    const result = await runGate4({
      queryDatabase: mockQueryDatabase,
      seedGate4Batch: mockSeedGate4Batch,
      getElasticsearchCount: mockGetElasticsearchCount,
      getDLQEntriesForRecords: mockGetDLQEntries,
      sleep: mockSleep,
      batchTotal: 500,
      corruptedCount: 3,
      simulatedWrittenCount: 497,
      maxWaitMs: 5000,
      closeDb: false
    });

    assert.equal(result.passed, true);
    assert.equal(result.writtenCount, 497);
    assert.equal(result.dlqCount, 3);
    assert.equal(result.contextSufficient, true);
    assert.equal(
      result.output,
      'G4 partial batch failure ........ PASS (497 written, 3 in DLQ)'
    );
    assert.equal(seedCalled, true);
    assert.equal(dlqQueried, true);
  });
});
