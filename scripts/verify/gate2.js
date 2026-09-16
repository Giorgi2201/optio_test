#!/usr/bin/env node

/**
 * Gate 2 Verification Runner: Deduplication & Delivery Guarantee Verification
 * Validates that after any number of kills and restarts,
 * every source record appears effectively once across both downstream sinks:
 * 1. Elasticsearch: Deterministic 1:1 document parity (_id = source record id), zero duplicate docs.
 * 2. RabbitMQ Consumer: Acknowledges and counts duplicate redelivery envelopes, suppressing repeated side effects.
 */

const {
  queryDatabase,
  closeDatabase,
  getElasticsearchCount,
  refreshElasticsearch,
  getConsumerMetrics,
  getTelemetry,
  formatGateResult,
  evaluateGate2Deduplication,
  sleep
} = require('./common.js');

/**
 * Runs the Gate 2 deduplication and parity verification scenario.
 * Supports dependency injection for testing.
 */
async function runGate2(options = {}) {
  const queryFn = options.queryDatabase || queryDatabase;
  const esCountFn = options.getElasticsearchCount || getElasticsearchCount;
  const refreshEsFn = options.refreshElasticsearch || refreshElasticsearch;
  const consumerMetricsFn = options.getConsumerMetrics || getConsumerMetrics;
  const sleepFn = options.sleep || sleep;
  const maxWaitMs = options.maxWaitMs !== undefined ? options.maxWaitMs : 60000;

  try {
    // -------------------------------------------------------------------------
    // Step 1: Source Count Baseline
    // -------------------------------------------------------------------------
    const countRows = await queryFn('SELECT COUNT(*)::bigint AS total FROM source_records');
    const sourceCount = parseInt(countRows[0]?.total || '0', 10);

    if (sourceCount <= 0) {
      throw new Error('Baseline source_records table is empty. Please seed records or run Gate 1 first.');
    }

    // -------------------------------------------------------------------------
    // Step 2: Replication Parity Wait / Completion Check
    // -------------------------------------------------------------------------
    const startWait = Date.now();
    let currentEsCount = null;
    let consumerMetrics = null;

    while (Date.now() - startWait < maxWaitMs) {
      try {
        await refreshEsFn();
      } catch {
        // Ignore refresh error
      }
      currentEsCount = await esCountFn();
      consumerMetrics = await consumerMetricsFn();

      if (
        currentEsCount !== null &&
        currentEsCount === sourceCount &&
        consumerMetrics !== null &&
        consumerMetrics.uniqueProcessed === sourceCount
      ) {
        break;
      }
      await sleepFn(500);
    }

    // Flush Lucene buffers before asserting final counts
    try {
      await refreshEsFn();
    } catch {
      // Ignore refresh error
    }
    const finalEsCount = await esCountFn();
    if (finalEsCount !== null) {
      currentEsCount = finalEsCount;
    }
    const finalConsumerMetrics = await consumerMetricsFn();
    if (finalConsumerMetrics !== null) {
      consumerMetrics = finalConsumerMetrics;
    }

    if (currentEsCount === null) {
      throw new Error('Elasticsearch cluster unreachable or records_search_index not found');
    }

    // -------------------------------------------------------------------------
    // Step 3: Sink 1 (Elasticsearch) Reconciliation
    // -------------------------------------------------------------------------
    const esCount = currentEsCount;
    const esDuplicates = Math.max(0, esCount - sourceCount);

    // -------------------------------------------------------------------------
    // Step 4: Sink 2 (Independent Consumer) Reconciliation
    // -------------------------------------------------------------------------
    if (!consumerMetrics) {
      throw new Error('Independent consumer microservice unreachable at http://localhost:3001/metrics');
    }

    const consumerUnique = consumerMetrics.uniqueProcessed || 0;
    const duplicatesPrevented = consumerMetrics.duplicatesPrevented || 0;
    const totalDuplicatesInSink = esDuplicates;

    // -------------------------------------------------------------------------
    // Step 5: Assertion & Output Formatting
    // -------------------------------------------------------------------------
    const result = evaluateGate2Deduplication(
      sourceCount,
      esCount,
      consumerUnique,
      totalDuplicatesInSink
    );

    result.duplicatesPrevented = duplicatesPrevented;

    console.log(result.output);
    return result;
  } catch (err) {
    const errorMsg = err instanceof Error ? err.message : String(err);
    const failureOutput = formatGateResult('G2 no duplicates', 'FAIL', errorMsg);
    console.error(failureOutput);
    return {
      passed: false,
      sourceCount: 0,
      sinkCount: 0,
      consumerUniqueCount: 0,
      duplicates: -1,
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
  runGate2()
    .then((result) => {
      process.exit(result.passed ? 0 : 1);
    })
    .catch((err) => {
      console.error('[FATAL ERROR]', err);
      process.exit(1);
    });
}

module.exports = {
  runGate2,
  evaluateGate2Deduplication
};
