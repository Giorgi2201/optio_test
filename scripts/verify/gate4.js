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
  waitForPipelineQuiescence,
  formatGateResult,
  evaluateGate4PartialFailure,
  sleep
} = require('./common.js');

const DEFAULT_MAX_WAIT_MS = 30000;
const DEFAULT_QUIESCE_TIMEOUT_MS = 30000;

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
  const nowFn = options.now || Date.now;
  const maxWaitMs = options.maxWaitMs !== undefined ? options.maxWaitMs : DEFAULT_MAX_WAIT_MS;
  const quiesceTimeoutMs = options.quiesceTimeoutMs !== undefined ? options.quiesceTimeoutMs : DEFAULT_QUIESCE_TIMEOUT_MS;
  const log = options.silent ? () => {} : (msg) => console.log(`[GATE 4] ${msg}`);
  const warn = options.silent ? () => {} : (msg) => console.warn(`[GATE 4][WARN] ${msg}`);

  const safeRefresh = async () => {
    try {
      await refreshEsFn();
    } catch {
      // Ignore refresh error
    }
  };

  try {
    // -------------------------------------------------------------------------
    // Step 1: Isolate test state — settle the pipeline, clear the DLQ, snapshot the index
    // -------------------------------------------------------------------------
    // The 497/3 assertion is only exact if no other traffic (background backfill or CDC backlog)
    // is landing in the sink while the batch is processed.
    const quiesce = await waitForPipelineQuiescence({
      getTelemetry: telemetryFn,
      sleep: sleepFn,
      now: nowFn,
      timeoutMs: quiesceTimeoutMs,
      onProgress: (t) => log(`Waiting for quiescence: backfill=${t.backfill_status ?? 'n/a'}, cdc_lag=${t.incremental_lag_records ?? 0} rows`)
    });
    if (!quiesce.quiesced) {
      warn(`Pipeline not quiescent before injection (${quiesce.reason}); sink deltas may include unrelated traffic.`);
    }

    // Clear old DLQ rows at start of test to isolate test state and prevent accumulation
    await queryFn('DELETE FROM dead_letter_queue;');

    await safeRefresh();
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

    log(`Injected ${batchTotal} records (${expectedWritten} valid, ${corruptedTarget} poison) on top of ${initialEsCount.toLocaleString('en-US')} indexed documents.`);

    // -------------------------------------------------------------------------
    // Step 3: Pipeline Ingestion Execution — poll until exactly 497 landed and 3 quarantined
    // -------------------------------------------------------------------------
    const startWait = nowFn();
    let currentEsCount = initialEsCount;
    let dlqEntries = [];
    let lastLog = startWait;

    const uniqueDlqRecords = (rows) => new Set((rows || []).map((r) => r.record_id || r.id)).size;

    while (nowFn() - startWait < maxWaitMs) {
      await sleepFn(500);
      await safeRefresh();

      const [count, entries] = await Promise.all([
        esCountFn().catch(() => null),
        getDlqFn(corruptedIds).catch(() => dlqEntries)
      ]);

      if (count !== null) {
        currentEsCount = count;
      }
      dlqEntries = entries || [];

      const writtenToEs = currentEsCount - initialEsCount;
      const quarantined = uniqueDlqRecords(dlqEntries);
      if (writtenToEs >= expectedWritten && quarantined >= corruptedTarget) {
        break;
      }

      if (nowFn() - lastLog >= 5000) {
        lastLog = nowFn();
        log(`Ingesting: ${writtenToEs}/${expectedWritten} written, ${quarantined}/${corruptedTarget} in DLQ...`);
      }
    }

    // Flush Lucene buffers before final reconciliation
    await safeRefresh();
    const finalCount = await esCountFn().catch(() => null);
    if (finalCount !== null) {
      currentEsCount = finalCount;
    }

    // -------------------------------------------------------------------------
    // Step 4: Sink Commit Reconciliation
    // -------------------------------------------------------------------------
    const writtenCount = currentEsCount - initialEsCount;
    const batchRolledBack = writtenCount === 0;

    if (batchRolledBack) {
      throw new Error('Total batch rollback detected: 0 records written out of 500 due to poison pill errors.');
    }

    // -------------------------------------------------------------------------
    // Step 5: DLQ Quarantine & Context Verification
    // -------------------------------------------------------------------------
    try {
      const finalDlq = await getDlqFn(corruptedIds);
      if (finalDlq && finalDlq.length > 0) {
        dlqEntries = finalDlq;
      }
    } catch {
      // Ignore
    }

    const dlqRows = dlqEntries;

    // A poison pill may be dead-lettered once per sink; count distinct quarantined source records.
    const dlqCount = uniqueDlqRecords(dlqRows);

    // Verify sufficient retry context: non-null payload, error_code, error_message, PENDING status
    let contextSufficient = dlqCount === corruptedTarget;
    for (const row of dlqRows) {
      let payloadObj = row.payload;
      if (typeof payloadObj === 'string') {
        try {
          payloadObj = JSON.parse(payloadObj);
        } catch {
          // Keep raw
        }
      }
      const hasPayload = payloadObj !== null && payloadObj !== undefined && (typeof payloadObj === 'object' ? Object.keys(payloadObj).length > 0 : true);
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
    result.quiesced = quiesce.quiesced;

    if (!options.silent) {
      console.log(result.output);
    }
    return result;
  } catch (err) {
    const errorMsg = err instanceof Error ? err.message : String(err);
    const failureOutput = formatGateResult('G4 partial batch failure', 'FAIL', errorMsg);
    if (!options.silent) {
      console.error(failureOutput);
    }
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
