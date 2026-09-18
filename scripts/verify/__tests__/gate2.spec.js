/**
 * Gate 2 Verification Logic & Mock Test Suite
 * Validates 1:1 dual-sink parity, deterministic deduplication,
 * effectively-once delivery guarantees, and output formatting.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const { formatGateResult, evaluateGate2Deduplication } = require('../common.js');
const { runGate2 } = require('../gate2.js');

describe('Gate 2 Verification - Deduplication & Effectively-Once Delivery', () => {
  it('1. Target Formatting: Validates formatted output with exact 2,000,000 numbers', () => {
    const result = evaluateGate2Deduplication(2000000, 2000000, 2000000, 0);

    assert.equal(result.passed, true);
    assert.equal(
      result.output,
      'G2 no duplicates ................ PASS (2,000,000 source / 2,000,000 sink / 0 dupes)'
    );
  });

  it('2. Evaluation Invariant (Pass): Validates matching sourceCount (5,000), esCount (5,000), and consumerCount (5,000) with 0 duplicates', () => {
    const result = evaluateGate2Deduplication(5000, 5000, 5000, 0);

    assert.equal(result.passed, true);
    assert.equal(result.sourceCount, 5000);
    assert.equal(result.sinkCount, 5000);
    assert.equal(result.consumerUniqueCount, 5000);
    assert.equal(result.duplicates, 0);
    assert.equal(
      result.output,
      'G2 no duplicates ................ PASS (5,000 source / 5,000 sink / 0 dupes)'
    );
  });

  it('3. Invariant Rejection (Sink Mismatch): Asserts failure when esCount does not match sourceCount (5,000 vs 4,950)', () => {
    const result = evaluateGate2Deduplication(5000, 4950, 5000, 0);

    assert.equal(result.passed, false);
    assert.match(result.output, /FAIL/);
    assert.match(result.output, /Elasticsearch parity failure/);
    assert.equal(
      result.output,
      'G2 no duplicates ................ FAIL (Elasticsearch parity failure (5000 vs 4950))'
    );
  });

  it('4. Invariant Rejection (Duplicate Leak): Asserts failure when duplicateCount > 0 (5,050 documents for 5,000 source rows)', () => {
    const result = evaluateGate2Deduplication(5000, 5050, 5000, 50);

    assert.equal(result.passed, false);
    assert.equal(result.duplicates, 50);
    assert.match(result.output, /FAIL/);
    assert.match(result.output, /50 duplicates detected in sink/);
  });

  it('5. Consumer Redelivery Assertion: Asserts that consumer duplicate suppression proves the Effectively-Once guarantee', () => {
    const consumerMetrics = {
      totalReceived: 5331,
      uniqueProcessed: 5000,
      duplicatesPrevented: 331,
      deadLettered: 0
    };

    // Assert that duplicatesPrevented accounts for repeated envelopes
    assert.equal(
      consumerMetrics.totalReceived,
      consumerMetrics.uniqueProcessed + consumerMetrics.duplicatesPrevented
    );
    assert.equal(consumerMetrics.duplicatesPrevented > 0, true);

    const result = evaluateGate2Deduplication(5000, 5000, consumerMetrics.uniqueProcessed, 0);
    assert.equal(result.passed, true);
    assert.equal(result.duplicates, 0);
  });

  function fakeClock() {
    let fakeNow = 1_000_000;
    return {
      now: () => fakeNow,
      sleep: async (ms) => {
        fakeNow += ms;
      }
    };
  }

  const quiescentTelemetry = async () => ({
    status: 'RUNNING',
    backfill_status: 'COMPLETED',
    backfill_cursor: 5000,
    incremental_lag_records: 0
  });

  it('6. End-to-End Runner Mock: Simulates full Gate 2 execution with mocked DB, Elasticsearch, and Consumer responses', async () => {
    const mockQueryDatabase = async () => [{ total: '5000' }];
    const mockGetElasticsearchCount = async () => 5000;
    const mockGetConsumerMetrics = async () => ({
      totalReceived: 5331,
      uniqueProcessed: 5000,
      duplicatesPrevented: 331,
      deadLettered: 0
    });

    const result = await runGate2({
      ...fakeClock(),
      queryDatabase: mockQueryDatabase,
      getElasticsearchCount: mockGetElasticsearchCount,
      refreshElasticsearch: async () => true,
      getConsumerMetrics: mockGetConsumerMetrics,
      getTelemetry: quiescentTelemetry,
      maxWaitMs: 5000,
      silent: true,
      closeDb: false
    });

    assert.equal(result.passed, true);
    assert.equal(result.sourceCount, 5000);
    assert.equal(result.sinkCount, 5000);
    assert.equal(result.consumerUniqueCount, 5000);
    assert.equal(result.duplicates, 0);
    assert.equal(result.duplicatesPrevented, 331);
    assert.equal(result.quiesced, true);
    assert.equal(
      result.output,
      'G2 no duplicates ................ PASS (5,000 source / 5,000 sink / 0 dupes)'
    );
  });

  const dlqAwareQuery = async (sql) => {
    if (sql.includes('dead_letter_queue')) {
      return [{ record_id: 4998 }, { record_id: 4999 }, { record_id: 5000 }];
    }
    return [{ total: '5000' }];
  };

  it('7. DLQ Offset: expected sink parity is sourceCount minus quarantined records that are genuinely absent from the index', async () => {
    let mgetIds = null;
    const result = await runGate2({
      ...fakeClock(),
      queryDatabase: dlqAwareQuery,
      getElasticsearchCount: async () => 4997,
      getElasticsearchDocs: async (ids) => {
        mgetIds = ids;
        return ids.map((id) => ({ id: String(id), found: false, source: null }));
      },
      refreshElasticsearch: async () => true,
      getConsumerMetrics: async () => ({ uniqueProcessed: 4997, duplicatesPrevented: 0 }),
      getTelemetry: quiescentTelemetry,
      maxWaitMs: 5000,
      silent: true,
      closeDb: false
    });

    assert.equal(result.passed, true);
    assert.deepEqual(mgetIds, ['4998', '4999', '5000']);
    assert.equal(result.dlqCount, 3);
    assert.equal(result.dlqMissingFromSink, 3);
    assert.equal(result.expectedSinkCount, 4997);
    assert.equal(
      result.output,
      'G2 no duplicates ................ PASS (5,000 source / 4,997 sink / 0 dupes)'
    );
  });

  it('10. Replayed poison pills: quarantined records already present in the index count as delivered, not as duplicates', async () => {
    // Scenario: operator replayed the 3 poison pills through the DLQ (indexed, RESOLVED), then a
    // re-processing pass re-quarantined the still-corrupt source rows as fresh PENDING entries.
    const result = await runGate2({
      ...fakeClock(),
      queryDatabase: dlqAwareQuery,
      getElasticsearchCount: async () => 5000,
      getElasticsearchDocs: async (ids) => ids.map((id) => ({ id: String(id), found: true, source: { id, version: 1 } })),
      refreshElasticsearch: async () => true,
      getConsumerMetrics: async () => null,
      getTelemetry: quiescentTelemetry,
      maxWaitMs: 5000,
      silent: true,
      closeDb: false
    });

    assert.equal(result.passed, true);
    assert.equal(result.dlqCount, 3);
    assert.equal(result.dlqAlreadyIndexed, 3);
    assert.equal(result.dlqMissingFromSink, 0);
    assert.equal(result.expectedSinkCount, 5000);
    assert.equal(result.duplicates, 0);
    assert.equal(
      result.output,
      'G2 no duplicates ................ PASS (5,000 source / 5,000 sink / 0 dupes)'
    );
  });

  it('11. Partially replayed quarantine: only the absent records are subtracted, and a real shortfall still fails', async () => {
    const result = await runGate2({
      ...fakeClock(),
      queryDatabase: dlqAwareQuery,
      // 1 of 3 quarantined records was replayed into the index -> expect 4998; index actually holds 4997
      getElasticsearchCount: async () => 4997,
      getElasticsearchDocs: async (ids) => ids.map((id, i) => ({ id: String(id), found: i === 0, source: null })),
      refreshElasticsearch: async () => true,
      getConsumerMetrics: async () => null,
      getTelemetry: quiescentTelemetry,
      maxWaitMs: 2000,
      silent: true,
      closeDb: false
    });

    assert.equal(result.passed, false);
    assert.equal(result.expectedSinkCount, 4998);
    assert.equal(result.duplicates, 0);
    assert.match(result.output, /Elasticsearch parity failure \(4998 vs 4997\)/);
    assert.doesNotMatch(result.output, /duplicates detected/);
  });

  it('8. Waits for late-arriving documents, then fails honestly on a persistent parity gap (no tolerance band)', async () => {
    let esPolls = 0;
    const result = await runGate2({
      ...fakeClock(),
      queryDatabase: async () => [{ total: '5000' }],
      getElasticsearchCount: async () => {
        esPolls++;
        return 4990; // 10 documents never arrive
      },
      refreshElasticsearch: async () => true,
      getConsumerMetrics: async () => null,
      getTelemetry: quiescentTelemetry,
      maxWaitMs: 3000,
      silent: true,
      closeDb: false
    });

    assert.equal(result.passed, false);
    assert.equal(esPolls > 1, true, 'must keep polling Elasticsearch until the wait budget is exhausted');
    assert.match(result.output, /FAIL/);
    assert.match(result.output, /Elasticsearch parity failure \(5000 vs 4990\)/);
  });

  it('9. Duplicate Leak: an index count above parity is reported as duplicates and fails the gate', async () => {
    const result = await runGate2({
      ...fakeClock(),
      queryDatabase: async () => [{ total: '5000' }],
      getElasticsearchCount: async () => 5050,
      refreshElasticsearch: async () => true,
      getConsumerMetrics: async () => ({ uniqueProcessed: 5000, duplicatesPrevented: 12 }),
      getTelemetry: quiescentTelemetry,
      maxWaitMs: 2000,
      silent: true,
      closeDb: false
    });

    assert.equal(result.passed, false);
    assert.equal(result.duplicates, 50);
    assert.match(result.output, /50 duplicates detected in sink/);
  });
});
