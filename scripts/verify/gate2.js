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
 *
 * Both receivers are asserted, not just the index:
 * - Elasticsearch must hold exactly the expected document count (and never more than the source universe).
 * - The independent consumer must report at least as many unique processed events as documents replicated.
 *   ">=" rather than "===" because every mutation is a distinct versioned event (rec_<id>_v<n>), so a
 *   consumer that has seen the G3 mutation bursts legitimately exceeds the record count. An unreachable
 *   consumer, or one below parity, fails the gate: the brief's "at least one independent consumer" is
 *   part of the delivery contract, not an optional extra.
 */

const {
  queryDatabase,
  closeDatabase,
  getElasticsearchCount,
  getElasticsearchDocs,
  refreshElasticsearch,
  getConsumerMetrics,
  getTelemetry,
  waitForPipelineQuiescence,
  formatGateResult,
  evaluateGate2Deduplication,
  sleep
} = require('./common.js');

const DEFAULT_MAX_WAIT_MS = 180000;
// Upper bound on quarantined record ids we will cross-check individually against the index.
const MAX_DLQ_RECORDS_TO_CROSSCHECK = 10000;

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
  const esDocsFn = options.getElasticsearchDocs || getElasticsearchDocs;
  const consumerMetricsFn = options.getConsumerMetrics || getConsumerMetrics;
  const telemetryFn = options.getTelemetry || getTelemetry;
  const sleepFn = options.sleep || sleep;
  const nowFn = options.now || Date.now;
  const maxWaitMs = options.maxWaitMs !== undefined ? options.maxWaitMs : DEFAULT_MAX_WAIT_MS;
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
    // Step 1: Source baseline & quarantine cross-check
    // -------------------------------------------------------------------------
    const countRows = await queryFn('SELECT COUNT(*) AS total FROM source_records;');
    const sourceCount = parseInt(countRows[0]?.total || countRows[0]?.count || '0', 10);

    if (sourceCount <= 0) {
      throw new Error('Baseline source_records table is empty. Please seed records or run Gate 1 first.');
    }

    /**
     * Effectively-once means every source record is either indexed or quarantined — never neither,
     * never twice. A quarantined record may still be present in the index (an operator replayed it
     * through the DLQ, or a later pass succeeded) and then be re-quarantined when a still-corrupt
     * source row is re-processed. So the expected index size is
     *   sourceCount - |quarantined records that are genuinely absent from the index|,
     * decided per record via _mget rather than by DLQ row counts.
     */
    const computeExpectation = async () => {
      let quarantinedIds = [];
      try {
        const rows = await queryFn(
          `SELECT DISTINCT record_id
           FROM dead_letter_queue
           WHERE sink_target IN ('ELASTICSEARCH', 'ALL') AND status <> 'RESOLVED'
           ORDER BY record_id
           LIMIT ${MAX_DLQ_RECORDS_TO_CROSSCHECK + 1};`
        );
        quarantinedIds = rows
          .map((r) => r.record_id)
          .filter((id) => id !== undefined && id !== null)
          .map((id) => String(id));
      } catch {
        quarantinedIds = [];
      }
      if (quarantinedIds.length > MAX_DLQ_RECORDS_TO_CROSSCHECK) {
        throw new Error(`dead_letter_queue holds more than ${fmt(MAX_DLQ_RECORDS_TO_CROSSCHECK)} quarantined records; refusing to assert parity against a poisoned dataset`);
      }

      let alreadyIndexed = 0;
      let missingFromSink = quarantinedIds.length;
      if (quarantinedIds.length > 0) {
        let docs = null;
        try {
          docs = await esDocsFn(quarantinedIds);
        } catch {
          docs = null;
        }
        if (docs) {
          alreadyIndexed = docs.filter((d) => d.found).length;
          missingFromSink = quarantinedIds.length - alreadyIndexed;
        }
      }

      return {
        quarantined: quarantinedIds.length,
        alreadyIndexed,
        missingFromSink,
        expectedSinkCount: options.expectedSinkCount !== undefined ? options.expectedSinkCount : sourceCount - missingFromSink
      };
    };

    let expectation = await computeExpectation();
    log(
      `Source ${fmt(sourceCount)} rows; ${fmt(expectation.quarantined)} quarantined for the ES sink` +
        (expectation.alreadyIndexed > 0 ? ` (${fmt(expectation.alreadyIndexed)} of them already indexed via replay)` : '') +
        ` -> expecting ${fmt(expectation.expectedSinkCount)} documents.`
    );

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
    // Step 3: Flush Lucene buffers, then poll BOTH receivers until parity (bounded budget)
    // -------------------------------------------------------------------------
    await safeRefresh();
    let esCount = await safeEsCount();
    let consumerMetrics = await safeConsumer();

    const consumerUnique = () => (consumerMetrics ? Number(consumerMetrics.uniqueProcessed ?? 0) : null);
    const esAtParity = () => esCount === expectation.expectedSinkCount;
    const consumerAtParity = () => {
      const unique = consumerUnique();
      return unique !== null && unique >= expectation.expectedSinkCount;
    };

    const remainingMs = Math.max(0, maxWaitMs - (nowFn() - gateStart));
    const parityStart = nowFn();
    let lastLog = parityStart;
    while ((!esAtParity() || !consumerAtParity()) && nowFn() - parityStart < remainingMs) {
      await sleepFn(500);
      if (!esAtParity()) {
        await safeRefresh();
        const next = await safeEsCount();
        if (next !== null) {
          esCount = next;
        }
      }
      if (!consumerAtParity()) {
        // Tolerate transient unreachability (consumer restart) by keeping the last good snapshot.
        consumerMetrics = (await safeConsumer()) || consumerMetrics;
      }
      // Quarantine state can change while we wait (replays, re-processing); re-derive the expectation.
      expectation = await computeExpectation();
      if (nowFn() - lastLog >= 5000) {
        lastLog = nowFn();
        const unique = consumerUnique();
        log(
          `Elasticsearch ${esCount === null ? 'unreachable' : fmt(esCount)} / ${fmt(expectation.expectedSinkCount)} documents ` +
            `(${fmt(expectation.missingFromSink)} quarantined & absent); consumer ${unique === null ? 'unreachable' : `${fmt(unique)} unique`}...`
        );
      }
    }

    if (esCount === null) {
      throw new Error('Elasticsearch cluster unreachable or records_search_index not found');
    }

    if (consumerMetrics) {
      log(
        `Consumer: ${fmt(consumerMetrics.uniqueProcessed ?? 0)} unique events processed, ` +
          `${fmt(consumerMetrics.duplicatesPrevented ?? 0)} redeliveries deduplicated, ` +
          `${fmt(consumerMetrics.deadLettered ?? 0)} dead-lettered.`
      );
    } else {
      warn('Consumer metrics endpoint unreachable for the whole wait budget; the independent consumer cannot be verified.');
    }

    // -------------------------------------------------------------------------
    // Step 5: Assertion & Output Formatting
    // -------------------------------------------------------------------------
    // _id is the source primary key, so the index can never hold two documents for one record; the only
    // way to exceed the source universe is orphan/duplicate documents. A shortfall is missing data, not dupes.
    const { expectedSinkCount, missingFromSink, alreadyIndexed, quarantined } = expectation;
    const duplicates = Math.max(0, esCount - sourceCount);
    const consumerUniqueCount = consumerUnique();
    const duplicatesPrevented = consumerMetrics?.duplicatesPrevented ?? 0;

    const result = evaluateGate2Deduplication(
      sourceCount,
      esCount,
      consumerUniqueCount,
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
      dlqCount: quarantined,
      dlqAlreadyIndexed: alreadyIndexed,
      dlqMissingFromSink: missingFromSink,
      consumerUniqueCount,
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
