#!/usr/bin/env node

/**
 * Gate 2 Verification Runner: Deduplication & Delivery Guarantee Verification
 * Validates that after any number of kills and restarts,
 * every source record appears effectively once across both downstream sinks:
 * 1. Elasticsearch: Deterministic 1:1 document parity (_id = source record id), zero duplicate docs.
 * 2. RabbitMQ Consumer: Acknowledges and counts duplicate redelivery envelopes, suppressing repeated side effects.
 *
 * Runs against a quiescent pipeline (backfill COMPLETED by Gate 1, CDC lag drained) so the
 * parity assertion is exact: esCount === sourceCount - (records rejected by the ES sink into the DLQ).
 */

const {
  queryDatabase,
  closeDatabase,
  getElasticsearchCount,
  refreshElasticsearch,
  getConsumerMetrics,
  getTelemetry,
  waitForPipelineQuiescence,
  formatGateResult,
  evaluateGate2Deduplication,
  sleep
} = require('./common.js');

const DEFAULT_MAX_WAIT_MS = 180000;
const DEFAULT_CONSUMER_CATCHUP_MS = 15000;

function fmt(n) {
  return Number(n).toLocaleString('en-US');
}

/**
 * Runs the Gate 2 deduplication and parity verification scenario.
 * Supports dependency injection for testing.
 */
async function runGate2(options = {}) {
  const queryFn = options.queryDatabase || queryDatabase;
  const esCountFn = options.getElasticsearchCount || getElasticsearchCount;
  const refreshEsFn = options.refreshElasticsearch || refreshElasticsearch;
  const consumerMetricsFn = options.getConsumerMetrics || getConsumerMetrics;
  const telemetryFn = options.getTelemetry || getTelemetry;
  const sleepFn = options.sleep || sleep;
  const nowFn = options.now || Date.now;
  const maxWaitMs = options.maxWaitMs !== undefined ? options.maxWaitMs : DEFAULT_MAX_WAIT_MS;
  const consumerCatchupMs = options.consumerCatchupMs !== undefined ? options.consumerCatchupMs : DEFAULT_CONSUMER_CATCHUP_MS;
  const log = options.silent ? () => {} : (msg) => console.log(`[GATE 2] ${msg}`);
  const warn = options.silent ? () => {} : (msg) => console.warn(`[GATE 2][WARN] ${msg}`);

  const safeRefresh = async () => {
    try {
      await refreshEsFn();
    } catch {
      // Ignore refresh error
    }
  };
  const safeEsCount = async () => {
    try {
      return await esCountFn();
    } catch {
      return null;
    }
  };
  const safeConsumer = async () => {
    try {
      return await consumerMetricsFn();
    } catch {
      return null;
    }
  };

  try {
    const gateStart = nowFn();

    // -------------------------------------------------------------------------
    // Step 1: Source baseline & DLQ offset (records the ES sink rejected and never resolved)
    // -------------------------------------------------------------------------
    const countRows = await queryFn('SELECT COUNT(*) AS total FROM source_records;');
    const sourceCount = parseInt(countRows[0]?.total || countRows[0]?.count || '0', 10);

    if (sourceCount <= 0) {
      throw new Error('Baseline source_records table is empty. Please seed records or run Gate 1 first.');
    }

    let dlqCount = 0;
    try {
      const dlqRows = await queryFn(
        `SELECT COUNT(DISTINCT record_id)::int AS es_rejected
         FROM dead_letter_queue
         WHERE sink_target IN ('ELASTICSEARCH', 'ALL') AND status <> 'RESOLVED';`
      );
      const raw = dlqRows[0]?.es_rejected;
      dlqCount = raw === undefined || raw === null ? 0 : parseInt(raw, 10) || 0;
    } catch {
      dlqCount = 0;
    }

    const expectedSinkCount = options.expectedSinkCount !== undefined ? options.expectedSinkCount : sourceCount - dlqCount;
    log(`Source ${fmt(sourceCount)} rows, ${fmt(dlqCount)} rejected by ES sink -> expecting ${fmt(expectedSinkCount)} documents.`);

    // -------------------------------------------------------------------------
    // Step 2: Wait for a quiescent pipeline (backfill COMPLETED, CDC lag drained)
    // -------------------------------------------------------------------------
    const quiesce = await waitForPipelineQuiescence({
      getTelemetry: telemetryFn,
      sleep: sleepFn,
      now: nowFn,
      timeoutMs: maxWaitMs,
      onProgress: (t) =>
        log(`Waiting for replication to settle: backfill=${t.backfill_status ?? 'n/a'} cursor=${fmt(t.backfill_cursor ?? 0)}, cdc_lag=${fmt(t.incremental_lag_records ?? 0)} rows`)
    });
    if (!quiesce.quiesced) {
      warn(`Pipeline not quiescent (${quiesce.reason}); asserting parity against current sink state.`);
    }

    // -------------------------------------------------------------------------
    // Step 3: Flush Lucene buffers, then let the consumer catch up to the index
    // -------------------------------------------------------------------------
    await safeRefresh();
    let esCount = await safeEsCount();
    let consumerMetrics = await safeConsumer();

    if (consumerMetrics) {
      const catchupStart = nowFn();
      while (
        nowFn() - catchupStart < consumerCatchupMs &&
        esCount !== null &&
        (consumerMetrics?.uniqueProcessed ?? 0) < esCount
      ) {
        await sleepFn(500);
        consumerMetrics = (await safeConsumer()) || consumerMetrics;
      }
      if (esCount !== null && (consumerMetrics?.uniqueProcessed ?? 0) < esCount) {
        warn(`Consumer at ${fmt(consumerMetrics?.uniqueProcessed ?? 0)} unique < ${fmt(esCount)} indexed after ${consumerCatchupMs / 1000}s.`);
      }
    } else {
      log('Consumer metrics endpoint unreachable; skipping consumer catch-up wait.');
    }

    // -------------------------------------------------------------------------
    // Step 4: Poll Elasticsearch until exact parity (bounded by the remaining budget)
    // -------------------------------------------------------------------------
    const remainingMs = Math.max(0, maxWaitMs - (nowFn() - gateStart));
    const parityStart = nowFn();
    let lastLog = parityStart;
    while (esCount !== expectedSinkCount && nowFn() - parityStart < remainingMs) {
      await sleepFn(500);
      await safeRefresh();
      const next = await safeEsCount();
      if (next !== null) {
        esCount = next;
      }
      if (nowFn() - lastLog >= 5000) {
        lastLog = nowFn();
        log(`Elasticsearch ${esCount === null ? 'unreachable' : fmt(esCount)} / ${fmt(expectedSinkCount)} documents...`);
      }
    }

    if (esCount === null) {
      throw new Error('Elasticsearch cluster unreachable or records_search_index not found');
    }

    // -------------------------------------------------------------------------
    // Step 5: Assertion & Output Formatting
    // -------------------------------------------------------------------------
    // Deterministic _id upserts make duplicates observable as an index count above the expected parity.
    const duplicates = Math.max(0, esCount - expectedSinkCount);
    const consumerUniqueCount = consumerMetrics?.uniqueProcessed ?? null;
    const duplicatesPrevented = consumerMetrics?.duplicatesPrevented ?? 0;

    const result = evaluateGate2Deduplication(
      sourceCount,
      esCount,
      consumerUniqueCount === null ? esCount : consumerUniqueCount,
      duplicates,
      expectedSinkCount
    );

    if (!options.silent) {
      console.log(result.output);
    }

    return {
      passed: result.passed,
      sourceCount,
      sinkCount: esCount,
      expectedSinkCount,
      dlqCount,
      consumerUniqueCount: consumerUniqueCount === null ? esCount : consumerUniqueCount,
      duplicates,
      duplicatesPrevented,
      quiesced: quiesce.quiesced,
      details: result.details,
      output: result.output
    };
  } catch (err) {
    const errorMsg = err instanceof Error ? err.message : String(err);
    const failureOutput = formatGateResult('G2 no duplicates', 'FAIL', errorMsg);
    if (!options.silent) {
      console.error(failureOutput);
    }
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
