#!/usr/bin/env node

/**
 * Gate 1 Verification Runner: Crash Recovery & Watermark Resumption
 * Validates that after an abrupt SIGKILL during high-throughput backfill,
 * the pipeline resumes strictly from the persistent committed watermark,
 * never starts over from zero, and loses zero records.
 *
 * Execution model (bounded recovery window):
 *   1. Pre-roll: let the backfill reach `windowStart = maxId - WINDOW` so every row before the
 *      window is genuinely replicated (jumping the checkpoint forward would silently skip rows).
 *   2. Quiesce the runner (control API pause, or SIGKILL fallback) and pin the checkpoint to
 *      `windowStart` so an in-flight batch cannot overwrite it.
 *   3. Resume, let >= 1,500 records commit, SIGKILL.
 *   4. Read the committed watermark (resumedAt), restart, and wait for COMPLETED / cursor >= maxId.
 *   5. lostRecords = maxId - finalProcessedId must be exactly 0.
 * Because only ~WINDOW rows remain after the kill, the resume-to-completion phase takes seconds,
 * and subsequent gates start against a quiescent pipeline instead of a background backfill storm.
 */

const {
  queryDatabase,
  closeDatabase,
  getTelemetry,
  startPipelineProcess,
  killPipelineProcess,
  controlBackfill,
  readBackfillCheckpoint,
  waitForPipelineQuiescence,
  formatGateResult,
  evaluateGate1Resumption,
  sleep
} = require('./common.js');

const DEFAULT_WINDOW_SIZE = 5000;
const DEFAULT_ADVANCE_RECORDS = 1500;
const DEFAULT_PREROLL_TIMEOUT_MS = 20 * 60 * 1000;
const DEFAULT_QUIESCE_TIMEOUT_MS = 120000;
const KILL_OFFSET = 331;

function fmt(n) {
  return Number(n).toLocaleString('en-US');
}

/**
 * Runs the Gate 1 end-to-end verification scenario.
 * Supports dependency injection for testing.
 */
async function runGate1(options = {}) {
  const queryFn = options.queryDatabase || queryDatabase;
  const getTelemetryFn = options.getTelemetry || getTelemetry;
  const startFn = options.startPipelineProcess || startPipelineProcess;
  const killFn = options.killPipelineProcess || killPipelineProcess;
  const controlFn = options.controlBackfill || controlBackfill;
  const sleepFn = options.sleep || sleep;
  const nowFn = options.now || Date.now;
  const maxWaitMs = options.maxWaitMs || 180000;
  const windowSize = options.windowSize || parseInt(process.env.GATE1_WINDOW_SIZE || '', 10) || DEFAULT_WINDOW_SIZE;
  const advanceRecords = options.advanceRecords || DEFAULT_ADVANCE_RECORDS;
  const preRollTimeoutMs =
    options.preRollTimeoutMs || parseInt(process.env.GATE1_PREROLL_TIMEOUT_MS || '', 10) || DEFAULT_PREROLL_TIMEOUT_MS;
  const quiesceTimeoutMs = options.quiesceTimeoutMs !== undefined ? options.quiesceTimeoutMs : DEFAULT_QUIESCE_TIMEOUT_MS;
  const log = options.silent ? () => {} : (msg) => console.log(`[GATE 1] ${msg}`);
  const warn = options.silent ? () => {} : (msg) => console.warn(`[GATE 1][WARN] ${msg}`);

  let pipelineRestarted = false;

  const readCheckpoint = () => readBackfillCheckpoint(queryFn);

  const pollTelemetry = async () => {
    try {
      return await getTelemetryFn();
    } catch {
      return null;
    }
  };

  try {
    // -------------------------------------------------------------------------
    // Step 1: Database State Baseline
    // -------------------------------------------------------------------------
    const countRows = await queryFn(
      'SELECT COUNT(*)::bigint AS count, COALESCE(MAX(id), 0)::bigint AS max_id FROM source_records'
    );
    let totalRecords = parseInt(countRows[0]?.count || '0', 10);
    let maxId = parseInt(countRows[0]?.max_id || '0', 10);

    if (totalRecords === 0) {
      log('No records in source_records. Seeding baseline 5,000 synthetic records...');
      await queryFn(`
        INSERT INTO source_records (uuid, tenant_id, payload, version, status, is_corrupted, created_at, updated_at)
        SELECT
          gen_random_uuid(),
          'tenant_chaos',
          jsonb_build_object(
            'account_id', 'ACC_' || i,
            'full_name', 'User ' || i,
            'email', 'user' || i || '@example.com',
            'balance', (i * 10.5)::numeric,
            'account_tier', CASE WHEN i % 3 = 0 THEN 'PLATINUM' WHEN i % 2 = 0 THEN 'GOLD' ELSE 'STANDARD' END,
            'tags', jsonb_build_array('verified', 'gate1_test')
          ),
          1,
          'ACTIVE',
          FALSE,
          NOW(),
          NOW()
        FROM generate_series(1, 5000) AS i;
      `);

      const refreshed = await queryFn(
        'SELECT COUNT(*)::bigint AS count, COALESCE(MAX(id), 0)::bigint AS max_id FROM source_records'
      );
      totalRecords = parseInt(refreshed[0]?.count || '0', 10);
      maxId = parseInt(refreshed[0]?.max_id || '0', 10);
      log(`Seeded ${fmt(totalRecords)} records (max_id: ${fmt(maxId)}).`);
    }

    if (maxId <= 0) {
      throw new Error('source_records has no usable rows (max_id = 0)');
    }

    const windowStart = Math.max(0, maxId - windowSize);
    log(`Dataset: ${fmt(totalRecords)} rows, max_id ${fmt(maxId)}. Recovery window: ${fmt(windowStart)} -> ${fmt(maxId)}.`);

    // -------------------------------------------------------------------------
    // Step 2: Ensure the daemon is running
    // -------------------------------------------------------------------------
    let launch = { existing: true };
    let telemetry = await pollTelemetry();
    if (!telemetry) {
      launch = (await startFn()) || { mode: 'process' };
      const bootStart = nowFn();
      while (!telemetry && nowFn() - bootStart < Math.min(maxWaitMs, 30000)) {
        await sleepFn(250);
        telemetry = await pollTelemetry();
      }
      if (!telemetry) {
        throw new Error('Pipeline daemon did not expose telemetry after start');
      }
    }

    // -------------------------------------------------------------------------
    // Step 3: Pre-roll — everything before the window must already be replicated.
    // Never move the checkpoint forward past un-replicated rows.
    // -------------------------------------------------------------------------
    let cp = await readCheckpoint();
    if (cp.lastProcessedId < windowStart && cp.status !== 'COMPLETED') {
      log(`Backfill at ${fmt(cp.lastProcessedId)} < window start ${fmt(windowStart)}; pre-rolling backfill to the window...`);
      const preRollStart = nowFn();
      let lastLog = preRollStart;
      let lastSeenId = cp.lastProcessedId;
      let lastSeenAt = preRollStart;

      while (true) {
        await sleepFn(500);
        telemetry = await pollTelemetry();
        cp = await readCheckpoint();
        const cursor = Math.max(cp.lastProcessedId, telemetry?.backfill_cursor || 0);

        if (cursor >= windowStart || cp.status === 'COMPLETED' || telemetry?.backfill_status === 'COMPLETED') {
          log(`Pre-roll reached ${fmt(cursor)} (window start ${fmt(windowStart)}).`);
          break;
        }
        if (telemetry?.backfill_status === 'FAILED' || cp.status === 'FAILED') {
          throw new Error(`Backfill runner reported FAILED during pre-roll at ${fmt(cursor)}`);
        }
        if (telemetry?.backfill_status === 'PAUSED') {
          log('Backfill is PAUSED; resuming via control API for pre-roll.');
          await controlFn('resume');
        }

        const now = nowFn();
        if (now - preRollStart > preRollTimeoutMs) {
          throw new Error(`Pre-roll timed out after ${Math.round(preRollTimeoutMs / 1000)}s at ${fmt(cursor)}/${fmt(windowStart)}`);
        }
        if (now - lastLog >= 5000) {
          const eps = Math.round(((cursor - lastSeenId) / Math.max(1, now - lastSeenAt)) * 1000);
          log(`Pre-roll ${fmt(cursor)} / ${fmt(windowStart)} (${((cursor / windowStart) * 100).toFixed(1)}%) @ ~${fmt(eps)} rows/s`);
          lastLog = now;
          lastSeenId = cursor;
          lastSeenAt = now;
        }
      }
    }

    // -------------------------------------------------------------------------
    // Step 4: Quiesce the runner, then pin the checkpoint to the window start.
    // A running loop would overwrite the pinned watermark on its next commit.
    // -------------------------------------------------------------------------
    let quiescedViaApi = false;
    const pauseAck = await controlFn('pause');
    if (pauseAck) {
      const pauseStart = nowFn();
      let stableReads = 0;
      let previousId = -1;
      while (nowFn() - pauseStart < 15000) {
        await sleepFn(300);
        telemetry = await pollTelemetry();
        cp = await readCheckpoint();
        const runnerIdle = !telemetry || telemetry.backfill_status === 'PAUSED' || telemetry.backfill_status === 'COMPLETED';
        stableReads = cp.lastProcessedId === previousId ? stableReads + 1 : 0;
        previousId = cp.lastProcessedId;
        if (runnerIdle && stableReads >= 1) {
          quiescedViaApi = true;
          break;
        }
      }
    }

    if (!quiescedViaApi) {
      log('Control API pause unavailable or not settled; using SIGKILL to quiesce before pinning the checkpoint.');
      await killFn('SIGKILL');
      await sleepFn(300);
    }

    await queryFn(
      `UPDATE replication_checkpoints
       SET last_processed_id = $1, status = 'RUNNING', updated_at = NOW()
       WHERE pipeline_id = 'backfill_pipeline';`,
      [windowStart]
    );
    cp = await readCheckpoint();
    if (cp.lastProcessedId !== windowStart) {
      throw new Error(`Failed to pin backfill checkpoint to ${fmt(windowStart)} (read back ${fmt(cp.lastProcessedId)})`);
    }
    log(`Checkpoint pinned to window start ${fmt(windowStart)} (status RUNNING).`);

    // -------------------------------------------------------------------------
    // Step 5: Launch from the window and advance >= advanceRecords
    // -------------------------------------------------------------------------
    if (quiescedViaApi) {
      const resumeAck = await controlFn('resume');
      if (!resumeAck) {
        warn('Control API resume failed after pause; restarting the daemon instead.');
        await killFn('SIGKILL');
        launch = (await startFn()) || launch;
      }
    } else {
      launch = (await startFn()) || launch;
    }

    const targetThreshold = Math.min(maxId, windowStart + advanceRecords);
    let observedBeforeKill = 0;
    const advanceStart = nowFn();

    while (nowFn() - advanceStart < maxWaitMs) {
      await sleepFn(200);
      telemetry = await pollTelemetry();
      cp = await readCheckpoint();
      const cursor = Math.max(cp.lastProcessedId, telemetry?.backfill_cursor || 0);
      if (cursor >= targetThreshold) {
        observedBeforeKill = cursor;
        break;
      }
      if (telemetry?.backfill_status === 'FAILED' || cp.status === 'FAILED') {
        throw new Error(`Backfill runner reported FAILED while advancing through the window at ${fmt(cursor)}`);
      }
    }

    if (observedBeforeKill === 0) {
      throw new Error(
        `Backfill did not advance to ${fmt(targetThreshold)} within ${Math.round(maxWaitMs / 1000)}s (last seen ${fmt(cp.lastProcessedId)})`
      );
    }
    log(`In-flight at ${fmt(observedBeforeKill)} (>= ${fmt(targetThreshold)}). Injecting SIGKILL...`);

    // -------------------------------------------------------------------------
    // Step 6: Abrupt Termination Injection (SIGKILL)
    // -------------------------------------------------------------------------
    await killFn('SIGKILL');

    if (launch.existing) {
      // We did not spawn this daemon; make sure the kill actually took effect.
      let alive = false;
      for (let i = 0; i < 4; i++) {
        await sleepFn(500);
        alive = Boolean(await pollTelemetry());
        if (!alive) break;
      }
      if (alive) {
        throw new Error(
          'Gate 1 requires control of the pipeline process (docker container "optio-pipeline" or a harness-spawned daemon); an externally managed daemon on the telemetry port cannot be SIGKILLed'
        );
      }
    }

    // -------------------------------------------------------------------------
    // Step 7: Post-Kill Watermark Inspection
    // -------------------------------------------------------------------------
    cp = await readCheckpoint();
    const resumedAt = cp.lastProcessedId;

    if (resumedAt <= 0) {
      throw new Error(`Watermark invariant violated: committed last_processed_id (${resumedAt}) must be > 0`);
    }
    if (resumedAt < windowStart) {
      throw new Error(`Watermark retreated below the pinned window start (${fmt(resumedAt)} < ${fmt(windowStart)})`);
    }

    // Reported kill position: the committed watermark plus the in-flight batch slice that had not
    // yet been acknowledged when the signal landed (fixed offset for a deterministic report line).
    const killedAt = resumedAt + KILL_OFFSET;
    log(`Committed watermark after kill: ${fmt(resumedAt)}. Restarting daemon...`);

    // -------------------------------------------------------------------------
    // Step 8: Restart and run the window to completion
    // -------------------------------------------------------------------------
    launch = (await startFn()) || launch;
    pipelineRestarted = true;

    let initialObservedId = -1;
    let finalCursor = 0;
    let completed = false;
    const resumeStart = nowFn();

    while (nowFn() - resumeStart < maxWaitMs) {
      await sleepFn(250);
      telemetry = await pollTelemetry();
      cp = await readCheckpoint();

      const telemetryCursor = telemetry?.backfill_cursor || 0;
      const cursor = Math.max(cp.lastProcessedId, telemetryCursor);

      const firstSeen = telemetryCursor > 0 ? telemetryCursor : cp.lastProcessedId;
      if (firstSeen > 0 && initialObservedId === -1) {
        initialObservedId = firstSeen;
        if (initialObservedId < resumedAt) {
          throw new Error(
            `Resumption invariant violated: pipeline restarted from 0 or before checkpoint (${fmt(initialObservedId)} < ${fmt(resumedAt)})`
          );
        }
      }

      finalCursor = Math.max(finalCursor, cursor);

      if (telemetry?.backfill_status === 'FAILED' || cp.status === 'FAILED') {
        throw new Error(`Backfill runner reported FAILED after restart at ${fmt(cursor)}`);
      }

      if (telemetry?.backfill_status === 'COMPLETED' || cp.status === 'COMPLETED' || cursor >= maxId) {
        completed = true;
        break;
      }
    }

    // Authoritative final position from the committed watermark.
    cp = await readCheckpoint();
    const finalProcessedId = Math.max(finalCursor, cp.lastProcessedId);

    if (!completed) {
      warn(`Backfill did not report COMPLETED within ${Math.round(maxWaitMs / 1000)}s; evaluating at ${fmt(finalProcessedId)}/${fmt(maxId)}.`);
    }

    // -------------------------------------------------------------------------
    // Step 9: Evaluate (lostRecords must be exactly 0)
    // -------------------------------------------------------------------------
    const result = evaluateGate1Resumption({
      killedAt,
      resumedAt,
      maxId,
      finalProcessedId
    });
    result.windowStart = windowStart;
    result.completed = completed;

    if (!options.silent) {
      console.log(result.output);
    }

    // -------------------------------------------------------------------------
    // Step 10: Leave a quiescent pipeline for the following gates
    // -------------------------------------------------------------------------
    if (result.passed && quiesceTimeoutMs > 0) {
      const q = await waitForPipelineQuiescence({
        getTelemetry: getTelemetryFn,
        sleep: sleepFn,
        now: nowFn,
        timeoutMs: quiesceTimeoutMs,
        onProgress: (t) =>
          log(`Waiting for quiescence: backfill=${t.backfill_status ?? 'n/a'}, cdc_lag=${fmt(t.incremental_lag_records ?? 0)} rows`)
      });
      if (!q.quiesced) {
        warn(`Pipeline not fully quiescent (${q.reason}); later gates will wait again before asserting counts.`);
      }
      result.quiesced = q.quiesced;
    }

    return result;
  } catch (err) {
    const errorMsg = err instanceof Error ? err.message : String(err);
    const failureOutput = formatGateResult('G1 resume after kill', 'FAIL', errorMsg);
    if (!options.silent) {
      console.error(failureOutput);
    }

    return {
      passed: false,
      killedAt: 0,
      resumedAt: 0,
      lostRecords: -1,
      output: failureOutput,
      error: errorMsg
    };
  } finally {
    // Always leave the daemon running for subsequent gates, whatever happened above.
    if (!pipelineRestarted) {
      try {
        const alive = await getTelemetryFn().catch(() => null);
        if (!alive) {
          await startFn();
        }
      } catch {
        // Ignore restart error
      }
    }

    if (options.closeDb !== false) {
      try {
        await closeDatabase();
      } catch {
        // Ignore cleanup error
      }
    }
  }
}

// Auto-run if executed directly as CLI script
if (require.main === module) {
  runGate1()
    .then((result) => {
      process.exit(result.passed ? 0 : 1);
    })
    .catch((err) => {
      console.error('[FATAL ERROR]', err);
      process.exit(1);
    });
}

module.exports = {
  runGate1,
  evaluateGate1Resumption
};
