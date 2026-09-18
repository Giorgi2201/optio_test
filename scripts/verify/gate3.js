#!/usr/bin/env node

/**
 * Gate 3 Verification Runner: Receiver Outage, Anti-Busy-Loop & Self-Healing
 * Validates that when a downstream receiver (Elasticsearch) suffers an outage:
 * 1. Zero Busy-Spin: Circuit breaker transitions to OPEN, applying jittered exponential backoff.
 * 2. Zero Data Loss: Backpressure is applied without dropping documents or crashing.
 * 3. Self-Healing: Upon receiver restoration, the pipeline automatically recovers and catches up.
 *
 * To make the outage observable on a quiescent pipeline, a bounded mutation burst is applied to
 * source rows while the receiver is down. Every mutated row must reach Elasticsearch with its new
 * version after restoration — that per-record check is what "0 lost" means here.
 */

const {
  queryDatabase,
  closeDatabase,
  getTelemetry,
  getElasticsearchCount,
  getElasticsearchDocs,
  refreshElasticsearch,
  mutateSourceRecords,
  waitForPipelineQuiescence,
  stopReceiver,
  startReceiver,
  formatGateResult,
  evaluateGate3Outage,
  sleep
} = require('./common.js');

const DEFAULT_OUTAGE_MS = 5000;
const DEFAULT_MUTATION_COUNT = 200;
const DEFAULT_MAX_RECOVERY_WAIT_MS = 120000;
const DEFAULT_PROBE_TIMEOUT_MS = 15000;
const DEFAULT_QUIESCE_TIMEOUT_MS = 30000;
const BREAKER_GRACE_MS = 5000;
// /api/telemetry runs live sink health probes; while a sink is down those probes can block for
// several seconds, so Gate 3 polls telemetry with a longer deadline than the other gates.
const DEFAULT_TELEMETRY_TIMEOUT_MS = 8000;
// HALF_OPEN -> CLOSED requires consecutive successful writes. On an idle pipeline the breaker would
// starve in HALF_OPEN forever, so once the outage traffic has landed we feed it small canary batches.
const DEFAULT_CANARY_SIZE = 10;
const DEFAULT_CANARY_INTERVAL_MS = 3000;
const DEFAULT_MAX_CANARIES = 5;

function fmt(n) {
  return Number(n).toLocaleString('en-US');
}

/**
 * Runs the Gate 3 receiver outage & self-healing verification scenario.
 * Supports dependency injection for testing.
 */
async function runGate3(options = {}) {
  const queryFn = options.queryDatabase || queryDatabase;
  const telemetryFn = options.getTelemetry || getTelemetry;
  const esCountFn = options.getElasticsearchCount || getElasticsearchCount;
  const esDocsFn = options.getElasticsearchDocs || getElasticsearchDocs;
  const refreshEsFn = options.refreshElasticsearch || refreshElasticsearch;
  const mutateFn = options.mutateSourceRecords || ((count) => mutateSourceRecords(count, queryFn));
  const stopReceiverFn = options.stopReceiver || stopReceiver;
  const startReceiverFn = options.startReceiver || startReceiver;
  const sleepFn = options.sleep || sleep;
  const nowFn = options.now || Date.now;

  const envOutageSec = parseInt(process.env.OUTAGE_DURATION_SEC || '', 10);
  const outageDurationMs =
    options.outageDurationMs !== undefined
      ? options.outageDurationMs
      : options.outageDurationSec !== undefined
        ? options.outageDurationSec * 1000
        : Number.isFinite(envOutageSec) && envOutageSec > 0
          ? envOutageSec * 1000
          : DEFAULT_OUTAGE_MS;
  const mutationCount = options.mutationCount || DEFAULT_MUTATION_COUNT;
  const maxRecoveryWaitMs = options.maxRecoveryWaitMs || DEFAULT_MAX_RECOVERY_WAIT_MS;
  const probeTimeoutMs = options.probeTimeoutMs !== undefined ? options.probeTimeoutMs : DEFAULT_PROBE_TIMEOUT_MS;
  const quiesceTimeoutMs = options.quiesceTimeoutMs !== undefined ? options.quiesceTimeoutMs : DEFAULT_QUIESCE_TIMEOUT_MS;
  const telemetryTimeoutMs = options.telemetryTimeoutMs || DEFAULT_TELEMETRY_TIMEOUT_MS;
  const canarySize = options.canarySize || DEFAULT_CANARY_SIZE;
  const canaryIntervalMs = options.canaryIntervalMs !== undefined ? options.canaryIntervalMs : DEFAULT_CANARY_INTERVAL_MS;
  const maxCanaries = options.maxCanaries !== undefined ? options.maxCanaries : DEFAULT_MAX_CANARIES;
  const log = options.silent ? () => {} : (msg) => console.log(`[GATE 3] ${msg}`);
  const warn = options.silent ? () => {} : (msg) => console.warn(`[GATE 3][WARN] ${msg}`);

  const pollTelemetry = async () => {
    try {
      return await telemetryFn(undefined, telemetryTimeoutMs);
    } catch {
      return null;
    }
  };
  const safeRefresh = async () => {
    try {
      await refreshEsFn();
    } catch {
      // Receiver may be down
    }
  };
  const safeEsCount = async () => {
    try {
      return await esCountFn();
    } catch {
      return null;
    }
  };
  const safeEsDocs = async (ids) => {
    try {
      return await esDocsFn(ids);
    } catch {
      return null;
    }
  };

  let receiverDown = false;

  try {
    // -------------------------------------------------------------------------
    // Step 1: Baseline & pipeline liveness
    // -------------------------------------------------------------------------
    const countRows = await queryFn('SELECT COUNT(*)::bigint AS total FROM source_records');
    const sourceCount = parseInt(countRows[0]?.total || '0', 10);
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

    let telemetry = null;
    const probeStart = nowFn();
    while (nowFn() - probeStart < probeTimeoutMs) {
      telemetry = await pollTelemetry();
      if (telemetry) break;
      await sleepFn(500);
    }
    if (!telemetry) {
      throw new Error('Pipeline daemon HTTP telemetry endpoint unreachable (PIPELINE_TELEMETRY_URL)');
    }

    // Start from a settled pipeline so the outage window contains only our traffic.
    const quiesce = await waitForPipelineQuiescence({
      getTelemetry: telemetryFn,
      sleep: sleepFn,
      now: nowFn,
      timeoutMs: quiesceTimeoutMs,
      onProgress: (t) => log(`Waiting for quiescence: backfill=${t.backfill_status ?? 'n/a'}, cdc_lag=${fmt(t.incremental_lag_records ?? 0)} rows`)
    });
    if (!quiesce.quiesced) {
      warn(`Pipeline not quiescent before outage (${quiesce.reason}).`);
    }

    await safeRefresh();
    const baselineEsCount = await safeEsCount();
    const baselineBreaker = (quiesce.telemetry || telemetry)?.circuit_breakers?.elasticsearch;
    const baselineTrips = typeof baselineBreaker?.totalTrips === 'number' ? baselineBreaker.totalTrips : 0;
    log(`Baseline: ${fmt(sourceCount)} source rows, ${baselineEsCount === null ? 'n/a' : fmt(baselineEsCount)} indexed, expecting ${fmt(expectedSinkCount)}; ES breaker ${baselineBreaker?.state ?? 'n/a'} (${fmt(baselineTrips)} trips).`);

    // Evidence that the breaker tripped: seen OPEN / HALF_OPEN / throttling, or its trip counter advanced.
    const breakerTripped = (b) =>
      Boolean(
        b &&
          (b.state === 'OPEN' ||
            b.state === 'HALF_OPEN' ||
            b.isThrottling === true ||
            (typeof b.totalTrips === 'number' && b.totalTrips > baselineTrips))
      );

    // -------------------------------------------------------------------------
    // Step 2: Outage injection, then traffic that must survive it
    // -------------------------------------------------------------------------
    const outageStart = nowFn();
    const stopInfo = await stopReceiverFn('elasticsearch', outageDurationMs);
    receiverDown = true;
    log(`Elasticsearch outage injected via ${stopInfo?.mode || 'unknown'} for ${outageDurationMs / 1000}s.`);

    const mutated = await mutateFn(Math.min(mutationCount, sourceCount));
    if (!Array.isArray(mutated) || mutated.length === 0) {
      throw new Error('Mutation burst produced no rows; cannot verify replication through the outage');
    }
    const expectedVersions = new Map(mutated.map((m) => [String(m.id), m.version]));
    log(`Mutated ${fmt(mutated.length)} source rows during the blackout.`);

    // -------------------------------------------------------------------------
    // Step 3: Anti-busy-loop verification (breaker must OPEN / throttle)
    // -------------------------------------------------------------------------
    let antiBusyLoopVerified = false;
    let breakerSnapshot = null;
    const probeDeadline = outageStart + outageDurationMs + BREAKER_GRACE_MS;

    while (nowFn() < probeDeadline) {
      await sleepFn(250);
      telemetry = await pollTelemetry();
      const esBreaker = telemetry?.circuit_breakers?.elasticsearch;
      if (breakerTripped(esBreaker)) {
        antiBusyLoopVerified = true;
        breakerSnapshot = esBreaker;
        break;
      }
    }
    if (antiBusyLoopVerified) {
      log(`Circuit breaker ${breakerSnapshot.state} during blackout (backoff ${fmt(breakerSnapshot.currentBackoffMs ?? 0)}ms, trips ${fmt(breakerSnapshot.totalTrips ?? 0)}).`);
    } else {
      log('Breaker trip not yet observable during the blackout (telemetry health probes block while the sink is down); will confirm via trip counter after restoration.');
    }

    // -------------------------------------------------------------------------
    // Step 4: Hold the outage window, then restore
    // -------------------------------------------------------------------------
    const remainingOutageMs = Math.max(0, outageStart + outageDurationMs - nowFn());
    if (remainingOutageMs > 0) {
      await sleepFn(remainingOutageMs);
    }

    await startReceiverFn('elasticsearch');
    receiverDown = false;
    const restoredAt = nowFn();
    const downtimeSec = Math.max(1, Math.round((restoredAt - outageStart) / 1000));

    // -------------------------------------------------------------------------
    // Step 5: Self-healing & per-record recovery measurement
    // -------------------------------------------------------------------------
    let recovered = false;
    let recoveredAt = null;
    let matched = 0;
    let finalEsCount = null;
    let lastLog = restoredAt;
    let canariesFired = 0;
    let lastCanaryAt = null;
    let lastBreaker = null;

    while (nowFn() - restoredAt < maxRecoveryWaitMs) {
      await sleepFn(500);
      await safeRefresh();

      telemetry = await pollTelemetry();
      const count = await safeEsCount();
      const ids = [...expectedVersions.keys()];
      const docs = await safeEsDocs(ids);

      if (count !== null) {
        finalEsCount = count;
      }
      if (docs) {
        matched = docs.filter((d) => d.found && Number(d.source?.version ?? -1) >= expectedVersions.get(String(d.id))).length;
      }

      const esBreaker = telemetry?.circuit_breakers?.elasticsearch;
      if (esBreaker) {
        lastBreaker = esBreaker;
        if (!antiBusyLoopVerified && breakerTripped(esBreaker)) {
          antiBusyLoopVerified = true;
          log(`Circuit breaker trip confirmed after restoration: state=${esBreaker.state}, trips ${fmt(baselineTrips)} -> ${fmt(esBreaker.totalTrips ?? 0)}.`);
        }
      }

      const breakerClosed = esBreaker ? esBreaker.state === 'CLOSED' : true;
      const countParity = count !== null && count >= expectedSinkCount;
      const versionsLanded = matched === expectedVersions.size;

      if (breakerClosed && countParity && versionsLanded) {
        recovered = true;
        recoveredAt = nowFn();
        break;
      }

      // Outage traffic has landed but the breaker is still probing (HALF_OPEN): give it canary
      // writes so it can accumulate its consecutive successes and close — an idle pipeline cannot.
      if (
        versionsLanded &&
        esBreaker &&
        esBreaker.state !== 'CLOSED' &&
        canariesFired < maxCanaries &&
        (lastCanaryAt === null || nowFn() - lastCanaryAt >= canaryIntervalMs)
      ) {
        const canary = await mutateFn(canarySize);
        for (const c of canary || []) {
          expectedVersions.set(String(c.id), c.version);
        }
        canariesFired++;
        lastCanaryAt = nowFn();
        log(`Breaker ${esBreaker.state} with all outage mutations landed; fired canary batch ${canariesFired}/${maxCanaries} (${(canary || []).length} rows) to close it.`);
      }

      if (nowFn() - lastLog >= 5000) {
        lastLog = nowFn();
        log(`Recovering: breaker=${esBreaker?.state ?? 'n/a'}, indexed=${count === null ? 'n/a' : fmt(count)}/${fmt(expectedSinkCount)}, mutations landed=${matched}/${expectedVersions.size}`);
      }
    }

    const lostRecords = expectedVersions.size - matched;
    const recoveryTimeSec = recovered ? Number(((recoveredAt - restoredAt) / 1000).toFixed(1)) : -1;

    if (!recovered) {
      warn(`Self-healing did not complete within ${maxRecoveryWaitMs / 1000}s: breaker ${lastBreaker?.state ?? 'n/a'}, indexed ${finalEsCount === null ? 'n/a' : fmt(finalEsCount)}/${fmt(expectedSinkCount)}, mutations landed ${matched}/${expectedVersions.size}.`);
    }
    if (!antiBusyLoopVerified) {
      warn(`Circuit breaker never showed a trip: state ${lastBreaker?.state ?? 'n/a'}, trips ${fmt(lastBreaker?.totalTrips ?? baselineTrips)} (baseline ${fmt(baselineTrips)}).`);
    }

    // -------------------------------------------------------------------------
    // Step 6: Evaluate & report measured values
    // -------------------------------------------------------------------------
    const result = evaluateGate3Outage(downtimeSec, lostRecords, recoveryTimeSec, antiBusyLoopVerified);
    result.mutatedCount = mutated.length;
    result.canariesFired = canariesFired;
    result.verifiedRecords = expectedVersions.size;
    result.breakerTrips = { before: baselineTrips, after: lastBreaker?.totalTrips ?? null };
    result.finalEsCount = finalEsCount;
    result.expectedSinkCount = expectedSinkCount;
    result.outageMode = stopInfo?.mode || 'unknown';

    if (!options.silent) {
      console.log(result.output);
    }
    return result;
  } catch (err) {
    const errorMsg = err instanceof Error ? err.message : String(err);
    const failureOutput = formatGateResult('G3 sink outage', 'FAIL', errorMsg);
    if (!options.silent) {
      console.error(failureOutput);
    }
    return {
      passed: false,
      downtimeSec: 0,
      lostRecords: -1,
      recoveryTimeSec: -1,
      formattedTime: '0s',
      details: errorMsg,
      output: failureOutput,
      error: errorMsg
    };
  } finally {
    if (receiverDown) {
      try {
        await startReceiverFn('elasticsearch');
      } catch {
        // Best-effort restore
      }
    }
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
  runGate3()
    .then((result) => {
      process.exit(result.passed ? 0 : 1);
    })
    .catch((err) => {
      console.error('[FATAL ERROR]', err);
      process.exit(1);
    });
}

module.exports = {
  runGate3,
  evaluateGate3Outage
};
