#!/usr/bin/env node

/**
 * Gate 4 Verification Runner: Partial Batch Failure & DLQ Isolation
 * Validates that when a batch of 500 contains 3 rejected records:
 * 1. Exactly 497 valid records are written to sinks (Elasticsearch / RabbitMQ).
 * 2. Exactly 3 poison pills are quarantined in PostgreSQL dead_letter_queue with full context (payload, error reason).
 * 3. The entire batch is NOT rolled back.
 */

const {
  queryDatabase,
  closeDatabase,
  getTelemetry,
  getElasticsearchCount,
  refreshElasticsearch,
  seedGate4Batch,
  getDLQEntriesForRecords,
  formatGateResult,
  evaluateGate4PartialFailure,
  sleep
} = require('./common.js');

/**
 * Runs the Gate 4 partial batch failure & DLQ isolation verification scenario.
 * Supports dependency injection for testing.
 */
async function runGate4(options = {}) {
  const queryFn = options.queryDatabase || queryDatabase;
  const telemetryFn = options.getTelemetry || getTelemetry;
  const esCountFn = options.getElasticsearchCount || getElasticsearchCount;
  const refreshEsFn = options.refreshElasticsearch || refreshElasticsearch;
  const seedFn = options.seedGate4Batch || seedGate4Batch;
  const getDlqFn = options.getDLQEntriesForRecords || getDLQEntriesForRecords;
  const sleepFn = options.sleep || sleep;
  const maxWaitMs = options.maxWaitMs !== undefined ? options.maxWaitMs : 15000;

  try {
    // -------------------------------------------------------------------------
    // Step 1: DLQ & State Baseline
    // -------------------------------------------------------------------------
    const initialDlqRows = await queryFn('SELECT COUNT(*)::bigint AS count FROM dead_letter_queue');
    const initialDlqCount = parseInt(initialDlqRows[0]?.count || '0', 10);

    const initialEsCount = (await esCountFn()) || 0;

    // -------------------------------------------------------------------------
    // Step 2: Controlled Fault Batch Injection (500 records: 497 valid, 3 poisoned)
    // -------------------------------------------------------------------------
    const batchTotal = options.batchTotal || 500;
    const corruptedTarget = options.corruptedCount || 3;
    const expectedWritten = batchTotal - corruptedTarget; // 497

    const { insertedIds, corruptedIds } = await seedFn(batchTotal, corruptedTarget);

    if (insertedIds.length !== batchTotal || corruptedIds.length !== corruptedTarget) {
      throw new Error(
        `Failed to seed exact test batch: expected ${batchTotal} total (${corruptedTarget} poisoned), got ${insertedIds.length} inserted (${corruptedIds.length} poisoned)`
      );
    }

    // -------------------------------------------------------------------------
    // Step 3: Pipeline Ingestion Execution
    // -------------------------------------------------------------------------
    const startWait = Date.now();
    let currentEsCount = initialEsCount;
    let dlqEntries = [];

    while (Date.now() - startWait < maxWaitMs) {
      await sleepFn(500);

      try {
        await refreshEsFn();
      } catch {
        // Ignore refresh error
      }

      const [count, entries] = await Promise.all([
        esCountFn(),
        getDlqFn(corruptedIds)
      ]);

      if (count !== null) {
        currentEsCount = count;
      }
      dlqEntries = entries;

      const writtenToEs = currentEsCount - initialEsCount;
      if (writtenToEs >= expectedWritten && dlqEntries.length >= corruptedTarget) {
        break;
      }
    }

    // Flush Lucene buffers before final reconciliation
    try {
      await refreshEsFn();
    } catch {
      // Ignore refresh error
    }
    const finalCount = await esCountFn();
    if (finalCount !== null) {
      currentEsCount = finalCount;
    }

    // -------------------------------------------------------------------------
    // Step 4: Sink Commit Reconciliation
    // -------------------------------------------------------------------------
    const writtenCount = options.simulatedWrittenCount ?? (currentEsCount - initialEsCount);
    const batchRolledBack = writtenCount === 0;

    if (batchRolledBack) {
      throw new Error('Total batch rollback detected: 0 records written out of 500 due to poison pill errors.');
    }

    // -------------------------------------------------------------------------
    // Step 5: DLQ Quarantine & Context Verification
    // -------------------------------------------------------------------------
    const dlqRows = options.simulatedDlqRows ?? dlqEntries;
    const dlqCount = dlqRows.length;

    // Verify sufficient retry context: non-null payload, error_code, error_message, PENDING status
    let contextSufficient = dlqCount === corruptedTarget;
    for (const row of dlqRows) {
      const hasPayload = row.payload !== null && row.payload !== undefined && Object.keys(row.payload).length > 0;
      const hasErrorCode = typeof row.error_code === 'string' && row.error_code.length > 0;
      const hasErrorMessage = typeof row.error_message === 'string' && row.error_message.length > 0;
      const isPending = row.status === 'PENDING';

      if (!hasPayload || !hasErrorCode || !hasErrorMessage || !isPending) {
        contextSufficient = false;
        break;
      }
    }

    // -------------------------------------------------------------------------
    // Step 6: Output Formatting & Exit
    // -------------------------------------------------------------------------
    const result = evaluateGate4PartialFailure(
      writtenCount,
      dlqCount,
      batchRolledBack,
      contextSufficient
    );

    console.log(result.output);
    return result;
  } catch (err) {
    const errorMsg = err instanceof Error ? err.message : String(err);
    const failureOutput = formatGateResult('G4 partial batch failure', 'FAIL', errorMsg);
    console.error(failureOutput);
    return {
      passed: false,
      writtenCount: 0,
      dlqCount: 0,
      batchRolledBack: true,
      contextSufficient: false,
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
  runGate4()
    .then((result) => {
      process.exit(result.passed ? 0 : 1);
    })
    .catch((err) => {
      console.error('[FATAL ERROR]', err);
      process.exit(1);
    });
}

module.exports = {
  runGate4,
  evaluateGate4PartialFailure
};
