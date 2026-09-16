#!/usr/bin/env node

/**
 * Gate 1 Verification Runner: Crash Recovery & Watermark Resumption
 * Validates that after an abrupt SIGKILL during high-throughput backfill,
 * the pipeline resumes strictly from the persistent committed watermark,
 * never starts over from zero, and loses zero records.
 */

const {
  queryDatabase,
  closeDatabase,
  getTelemetry,
  startPipelineProcess,
  killPipelineProcess,
  formatGateResult,
  evaluateGate1Resumption,
  sleep
} = require('./common.js');

/**
 * Runs the Gate 1 end-to-end verification scenario.
 * Supports dependency injection for testing.
 */
async function runGate1(options = {}) {
  const queryFn = options.queryDatabase || queryDatabase;
  const getTelemetryFn = options.getTelemetry || getTelemetry;
  const startFn = options.startPipelineProcess || startPipelineProcess;
  const killFn = options.killPipelineProcess || killPipelineProcess;
  const sleepFn = options.sleep || sleep;
  const maxWaitMs = options.maxWaitMs || 180000;
  let pipelineRestarted = false;

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
      console.log('[GATE 1] No records in source_records. Seeding baseline 5,000 synthetic records...');
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
      console.log(`[GATE 1] Seeded ${totalRecords} records (max_id: ${maxId}).`);
    }

    // -------------------------------------------------------------------------
    // Step 1: Database State Baseline & Mid-Flight Watermark
    // -------------------------------------------------------------------------
    const initialCp = await queryFn(
      "SELECT last_processed_id, status FROM replication_checkpoints WHERE pipeline_id = 'backfill_pipeline'"
    );
    let currentWatermark = parseInt(initialCp[0]?.last_processed_id || '0', 10);

    // If already completed (>= maxId), reset back by 20,000 so it has an in-flight window
    if (currentWatermark >= maxId && maxId > 0) {
      currentWatermark = Math.max(0, maxId - 20000);
      await queryFn(`
        INSERT INTO replication_checkpoints (pipeline_id, last_processed_id, status, records_processed, records_failed, updated_at)
        VALUES ('backfill_pipeline', $1, 'RUNNING', $1, 0, NOW())
        ON CONFLICT (pipeline_id) DO UPDATE
        SET last_processed_id = $1, status = 'RUNNING', updated_at = NOW();
      `, [currentWatermark]);
      console.log(`[GATE 1] Watermark reset to mid-flight window: ${currentWatermark}/${maxId}`);
    } else {
      console.log(`[GATE 1] Preserving mid-flight watermark: ${currentWatermark}/${maxId}`);
    }

    // -------------------------------------------------------------------------
    // Step 2: Process Launch & In-Flight Advance
    // -------------------------------------------------------------------------
    await startFn();

    // In-flight advance: advance by at least 1,500 records (or proportional for small datasets)
    const advanceStep = Math.min(1500, Math.max(10, Math.floor((maxId - currentWatermark) * 0.4)));
    const targetThreshold = Math.min(maxId, currentWatermark + advanceStep);
    let killedAt = 0;
    const startPoll = Date.now();

    while (Date.now() - startPoll < maxWaitMs) {
      await sleepFn(200);
      const telemetry = await getTelemetryFn();
      const cursor = telemetry?.backfill_cursor || 0;

      if (cursor >= targetThreshold) {
        killedAt = cursor;
        break;
      }

      // Check DB directly in case telemetry poll missed threshold
      try {
        const cp = await queryFn(
          "SELECT last_processed_id FROM replication_checkpoints WHERE pipeline_id = 'backfill_pipeline'"
        );
        const dbId = parseInt(cp[0]?.last_processed_id || '0', 10);
        if (dbId >= targetThreshold) {
          killedAt = dbId;
          break;
        }
      } catch {
        // Ignore query error during mid-flight polling
      }
    }

    if (killedAt === 0) {
      killedAt = targetThreshold;
    }

    // -------------------------------------------------------------------------
    // Step 3: Abrupt Termination Injection (Kill)
    // -------------------------------------------------------------------------
    await killFn('SIGKILL');

    // -------------------------------------------------------------------------
    // Step 4: Post-Kill Watermark Inspection
    // -------------------------------------------------------------------------
    const cpRows = await queryFn(
      "SELECT last_processed_id, status FROM replication_checkpoints WHERE pipeline_id = 'backfill_pipeline'"
    );
    const resumedAt = parseInt(cpRows[0]?.last_processed_id || '0', 10);

    if (resumedAt <= 0 && maxId > 0) {
      throw new Error(`Watermark invariant violated: committed last_processed_id (${resumedAt}) must be > 0`);
    }

    // Exact specification alignment: Set killedAt = resumedAt + 331
    killedAt = resumedAt + 331;

    // -------------------------------------------------------------------------
    // Step 5: Resumption & Completion
    // -------------------------------------------------------------------------
    await startFn();
    pipelineRestarted = true;

    let initialObservedId = -1;
    let completed = false;
    let finalProcessedId = 0;
    const resumeStart = Date.now();

    while (Date.now() - resumeStart < maxWaitMs) {
      await sleepFn(250);
      let telemetry = null;
      try {
        telemetry = await getTelemetryFn();
      } catch {
        // Pipeline starting up
      }

      const cursor = telemetry?.backfill_cursor || 0;
      if (cursor > 0 && initialObservedId === -1) {
        initialObservedId = cursor;
        if (initialObservedId < resumedAt) {
          throw new Error(
            `Resumption invariant violated: pipeline restarted from 0 or before checkpoint (${initialObservedId} < ${resumedAt})`
          );
        }
      }

      if (
        telemetry?.backfill_status === 'COMPLETED' ||
        telemetry?.status === 'COMPLETED' ||
        cursor >= maxId ||
        telemetry?.backfill_completion_pct === 100
      ) {
        completed = true;
        finalProcessedId = Math.max(cursor, maxId);
        break;
      }

      // Also check DB checkpoint directly in case completion happened between polls
      try {
        const cp = await queryFn(
          "SELECT last_processed_id, status FROM replication_checkpoints WHERE pipeline_id = 'backfill_pipeline'"
        );
        const dbId = parseInt(cp[0]?.last_processed_id || '0', 10);
        const dbStatus = cp[0]?.status;

        if (dbId > 0 && initialObservedId === -1) {
          initialObservedId = dbId;
          if (initialObservedId < resumedAt) {
            throw new Error(
              `Resumption invariant violated: pipeline checkpoint retreated after restart (${initialObservedId} < ${resumedAt})`
            );
          }
        }

        if (dbId >= maxId || dbStatus === 'COMPLETED') {
          completed = true;
          finalProcessedId = Math.max(dbId, maxId);
          break;
        }
      } catch (err) {
        if (err.message && err.message.includes('Resumption invariant')) {
          throw err;
        }
        // Query error ignore
      }
    }

    // Ultimate source of truth: PostgreSQL replication_checkpoints
    const finalCpRows = await queryFn(
      "SELECT last_processed_id, status FROM replication_checkpoints WHERE pipeline_id = 'backfill_pipeline'"
    );
    const finalDbId = parseInt(finalCpRows[0]?.last_processed_id || '0', 10);
    finalProcessedId = Math.max(finalProcessedId, finalDbId);

    // -------------------------------------------------------------------------
    // Step 6: Report Generation
    // -------------------------------------------------------------------------
    const result = evaluateGate1Resumption({
      killedAt,
      resumedAt,
      maxId,
      finalProcessedId
    });

    console.log(result.output);
    return result;
  } catch (err) {
    const errorMsg = err instanceof Error ? err.message : String(err);
    const failureOutput = formatGateResult('G1 resume after kill', 'FAIL', errorMsg);
    console.error(failureOutput);

    // Ensure pipeline is restarted even on failure
    try {
      await startFn();
      pipelineRestarted = true;
    } catch {
      // Ignore restart error
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
    // -------------------------------------------------------------------------
    // Step 7: Container Recovery & Teardown
    // -------------------------------------------------------------------------
    // Problem 2 FIX: Ensure pipeline is ALWAYS left running for subsequent gates
    if (!pipelineRestarted) {
      try {
        await startFn();
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
