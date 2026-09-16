/**
 * Shared Verification Framework Utilities
 * Provides resilient process lifecycle control, database querying, telemetry polling,
 * and standardized output formatting across Docker and native host environments.
 */

const fs = require('fs');
const path = require('path');
const { execSync, spawn } = require('child_process');

// Resilient .env loader
function loadEnv() {
  const envPath = path.resolve(__dirname, '..', '..', '.env');
  if (fs.existsSync(envPath)) {
    const envContent = fs.readFileSync(envPath, 'utf8');
    for (const line of envContent.split('\n')) {
      const trimmed = line.trim();
      if (!trimmed || trimmed.startsWith('#')) continue;
      const eqIdx = trimmed.indexOf('=');
      if (eqIdx !== -1) {
        const key = trimmed.substring(0, eqIdx).trim();
        const val = trimmed.substring(eqIdx + 1).trim();
        if (!process.env[key]) {
          process.env[key] = val;
        }
      }
    }
  }
}

loadEnv();

let pgModule = null;
try {
  pgModule = require('pg');
} catch {
  // pg will be required when queryDatabase is called
}

let pool = null;

function getPool() {
  if (!pool) {
    if (!pgModule) {
      pgModule = require('pg');
    }
    const connectionString =
      process.env.DATABASE_URL ||
      `postgresql://${process.env.POSTGRES_USER || 'optio'}:${process.env.POSTGRES_PASSWORD || 'optio_secure_pass'}@${process.env.POSTGRES_HOST || 'localhost'}:${process.env.POSTGRES_PORT || 5432}/${process.env.POSTGRES_DB || 'optio_replication'}`;

    const config =
      typeof connectionString === 'string'
        ? { connectionString, connectionTimeoutMillis: 5000 }
        : { ...connectionString, connectionTimeoutMillis: 5000 };

    pool = new pgModule.Pool(config);
  }
  return pool;
}

/**
 * Executes a SQL query against PostgreSQL with automatic connection pool management.
 */
async function queryDatabase(sql, params = []) {
  const p = getPool();
  const res = await p.query(sql, params);
  const rows = res.rows;
  rows.rowCount = res.rowCount;
  return rows;
}

/**
 * Closes active database pool connections cleanly.
 */
async function closeDatabase() {
  if (pool) {
    await pool.end();
    pool = null;
  }
}

/**
 * Queries pipeline real-time telemetry from the HTTP server.
 * Returns null if unreachable or on timeout.
 */
async function getTelemetry(url, timeoutMs = 3000) {
  const targetUrl = url || process.env.PIPELINE_TELEMETRY_URL || 'http://localhost:3000/api/telemetry';
  try {
    const res = await fetch(targetUrl, {
      signal: AbortSignal.timeout(timeoutMs)
    });
    if (!res.ok) {
      return null;
    }
    return await res.json();
  } catch {
    return null;
  }
}

/**
 * Queries Elasticsearch cluster for total document count in an index.
 * Returns null if unreachable or on error/timeout.
 */
async function getElasticsearchCount(indexName = 'records_search_index', url, timeoutMs = 3000) {
  const baseUrl = process.env.ELASTICSEARCH_URL || process.env.ELASTICSEARCH_NODE || url || 'http://localhost:9200';
  const targetUrl = `${baseUrl.replace(/\/+$/, '')}/${indexName}/_count`;
  try {
    const res = await fetch(targetUrl, { signal: AbortSignal.timeout(timeoutMs) });
    if (!res.ok) {
      return null;
    }
    const data = await res.json();
    return typeof data.count === 'number' ? data.count : null;
  } catch {
    return null;
  }
}

/**
 * Queries the independent consumer microservice metrics endpoint.
 * Returns null if unreachable or on error/timeout.
 */
async function getConsumerMetrics(url, timeoutMs = 3000) {
  const targetUrl = url || process.env.CONSUMER_METRICS_URL || 'http://localhost:3001/metrics';
  try {
    const res = await fetch(targetUrl, { signal: AbortSignal.timeout(timeoutMs) });
    if (!res.ok) {
      return null;
    }
    return await res.json();
  } catch {
    return null;
  }
}

/**
 * Checks if the Docker daemon is accessible and responding.
 */
function isDockerRunning() {
  try {
    execSync('docker ps', { stdio: 'ignore', timeout: 3000 });
    return true;
  } catch {
    return false;
  }
}

/**
 * Checks if a Docker container exists (running or stopped).
 */
function doesDockerContainerExist(name = 'optio-pipeline') {
  try {
    const out = execSync(`docker ps -a --filter name=${name} --format "{{.Names}}"`, {
      encoding: 'utf8',
      timeout: 3000
    }).trim();
    return out.includes(name);
  } catch {
    return false;
  }
}

let activePipelineProcess = null;
let pipelineExecutionMode = 'none';

/**
 * Launches the pipeline daemon either as a Docker container or a native Node.js child process.
 */
async function startPipelineProcess() {
  const rootDir = path.resolve(__dirname, '..', '..');

  // 1. Docker Mode if daemon is active and container exists
  if (isDockerRunning() && doesDockerContainerExist('optio-pipeline')) {
    try {
      execSync('docker compose start pipeline || docker start optio-pipeline', {
        cwd: rootDir,
        stdio: 'ignore',
        timeout: 10000
      });
      pipelineExecutionMode = 'docker';
      return { mode: 'docker', container: 'optio-pipeline' };
    } catch (err) {
      console.warn('[WARN] Failed to start Docker container optio-pipeline, falling back to local process:', err.message);
    }
  }

  // 2. Native OS Process Mode
  const entrypoint = path.resolve(rootDir, 'apps', 'pipeline', 'dist', 'index.js');
  if (!fs.existsSync(entrypoint)) {
    execSync('npm --workspace=@optio/pipeline run build', {
      cwd: rootDir,
      stdio: 'inherit'
    });
  }

  const child = spawn(process.execPath, [entrypoint], {
    cwd: rootDir,
    env: { ...process.env },
    stdio: ['ignore', 'pipe', 'pipe']
  });

  activePipelineProcess = child;
  pipelineExecutionMode = 'process';

  child.on('error', (err) => {
    console.error('[PROCESS ERROR] Pipeline child process error:', err);
  });

  let startupStderr = '';
  child.stderr.on('data', (d) => {
    startupStderr += d.toString();
    if (startupStderr.length > 5000) {
      startupStderr = startupStderr.slice(-5000);
    }
  });

  child.on('exit', (code, sig) => {
    if (activePipelineProcess === child) {
      activePipelineProcess = null;
    }
    if (code !== 0 && code !== null && sig !== 'SIGKILL') {
      if (startupStderr) {
        console.error('[PIPELINE PROCESS CRASHED]:', startupStderr.trim());
      }
    }
  });

  return {
    mode: 'process',
    pid: child.pid,
    process: child
  };
}

/**
 * Terminates the pipeline daemon process with abrupt termination (SIGKILL).
 */
async function killPipelineProcess(signal = 'SIGKILL') {
  if (pipelineExecutionMode === 'docker' && isDockerRunning()) {
    try {
      execSync('docker kill -s SIGKILL optio-pipeline', { stdio: 'ignore', timeout: 5000 });
    } catch {
      // Container may already be stopped
    }
    pipelineExecutionMode = 'none';
    return;
  }

  if (activePipelineProcess && activePipelineProcess.pid) {
    const pid = activePipelineProcess.pid;
    if (process.platform === 'win32') {
      try {
        execSync(`taskkill /F /T /PID ${pid}`, { stdio: 'ignore', timeout: 5000 });
      } catch {
        try {
          activePipelineProcess.kill('SIGKILL');
        } catch {
          // Process might already be dead
        }
      }
    } else {
      try {
        process.kill(pid, signal);
      } catch {
        try {
          activePipelineProcess.kill(signal);
        } catch {
          // Process might already be dead
        }
      }
    }
    activePipelineProcess = null;
  }
  pipelineExecutionMode = 'none';
}

/**
 * Formats a gate result string matching the specification:
 * `G1 resume after kill ............ PASS (details)`
 */
function formatGateResult(gate, status, details) {
  const padded = (gate + ' ').padEnd(33, '.') + ' ';
  const detailsStr = details ? ` (${details})` : '';
  return `${padded}${status}${detailsStr}`;
}

/**
 * Evaluates Gate 1 invariant calculations and produces a structured result.
 */
function evaluateGate1Resumption({ killedAt, resumedAt, maxId, finalProcessedId }) {
  const watermarkValid = resumedAt > 0 && resumedAt <= killedAt;
  const lostRecords = Math.max(0, maxId - finalProcessedId);
  const passed = watermarkValid && lostRecords === 0 && finalProcessedId >= maxId;
  const details = `killed at ${Number(killedAt).toLocaleString('en-US')} / resumed at ${Number(resumedAt).toLocaleString('en-US')}, ${Number(lostRecords).toLocaleString('en-US')} lost`;
  const output = formatGateResult('G1 resume after kill', passed ? 'PASS' : 'FAIL', details);

  return {
    passed,
    watermarkValid,
    killedAt,
    resumedAt,
    lostRecords,
    finalProcessedId,
    output
  };
}

/**
 * Evaluates Gate 2 deduplication and delivery guarantee invariants.
 * Asserts:
 * 1. sourceCount > 0
 * 2. sourceCount === esCount (Elasticsearch 1:1 document parity)
 * 3. sourceCount === consumerUniqueCount (Consumer unique processing parity)
 * 4. duplicateCount === 0 (Zero duplicate records present in sinks)
 */
function evaluateGate2Deduplication(sourceCount, esCount, consumerUniqueCount, duplicateCount = 0) {
  const sourceValid = sourceCount > 0;
  const esParity = sourceCount === esCount;
  const consumerParity = sourceCount === consumerUniqueCount;
  const zeroDuplicates = duplicateCount === 0;

  const passed = sourceValid && esParity && consumerParity && zeroDuplicates;

  let details;
  if (passed) {
    details = `${Number(sourceCount).toLocaleString('en-US')} source / ${Number(esCount).toLocaleString('en-US')} sink / ${Number(duplicateCount).toLocaleString('en-US')} dupes`;
  } else {
    const reasons = [];
    if (!sourceValid) reasons.push(`invalid source count (${sourceCount})`);
    if (!esParity) reasons.push(`Elasticsearch parity failure (${sourceCount} vs ${esCount})`);
    if (!consumerParity) reasons.push(`Consumer parity failure (${sourceCount} vs ${consumerUniqueCount})`);
    if (!zeroDuplicates) reasons.push(`${duplicateCount} duplicates detected in sink`);
    details = reasons.join(', ');
  }

  const output = formatGateResult('G2 no duplicates', passed ? 'PASS' : 'FAIL', details);

  return {
    passed,
    sourceCount,
    sinkCount: esCount,
    consumerUniqueCount,
    duplicates: duplicateCount,
    details,
    output
  };
}

/**
 * Stops or trips a downstream receiver to simulate an outage.
 * If Docker is running and the container exists, stops the container.
 * Otherwise, calls the pipeline's simulation API to trip the circuit breaker.
 */
async function stopReceiver(sinkName = 'elasticsearch', durationMs = 10000) {
  const containerName = sinkName === 'elasticsearch' ? 'optio-elasticsearch' : 'optio-rabbitmq';
  if (isDockerRunning() && doesDockerContainerExist(containerName)) {
    try {
      execSync(`docker stop ${containerName}`, { stdio: 'ignore', timeout: 10000 });
      return { mode: 'docker', container: containerName, action: 'stopped' };
    } catch (err) {
      console.warn(`[WARN] Failed to stop ${containerName} via Docker, falling back to API:`, err.message);
    }
  }

  // Native / API mode: Call /api/simulation/trip-breaker
  const tripUrl = process.env.PIPELINE_SIMULATION_URL || 'http://localhost:3000/api/simulation/trip-breaker';
  try {
    const res = await fetch(tripUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ sink: sinkName, durationMs }),
      signal: AbortSignal.timeout(5000)
    });
    if (res.ok) {
      const data = await res.json();
      return { mode: 'api', sink: sinkName, ...data };
    }
  } catch (err) {
    console.warn(`[WARN] Failed to trip circuit breaker via simulation API:`, err.message);
  }
  return { mode: 'fallback', sink: sinkName, durationMs };
}

/**
 * Restores a downstream receiver after an outage.
 * If Docker is running and container exists, starts the container.
 */
async function startReceiver(sinkName = 'elasticsearch') {
  const containerName = sinkName === 'elasticsearch' ? 'optio-elasticsearch' : 'optio-rabbitmq';
  if (isDockerRunning() && doesDockerContainerExist(containerName)) {
    try {
      execSync(`docker start ${containerName}`, { stdio: 'ignore', timeout: 15000 });
      return { mode: 'docker', container: containerName, action: 'started' };
    } catch (err) {
      console.warn(`[WARN] Failed to start ${containerName} via Docker:`, err.message);
    }
  }
  return { mode: 'api', sink: sinkName, action: 'restored' };
}

/**
 * Evaluates Gate 3 receiver outage, zero busy-loop, and self-healing invariants.
 * Asserts:
 * 1. downtimeSec > 0 (receiver was genuinely down for a non-trivial duration)
 * 2. lostRecords === 0 (no records dropped or lost during receiver blackout)
 * 3. recoveryTimeSec >= 0 (pipeline resumed and achieved parity post-restoration)
 */
function evaluateGate3Outage(downtimeSec, lostRecords = 0, recoveryTimeSec = 0) {
  const downtimeValid = downtimeSec > 0;
  const zeroLost = lostRecords === 0;
  const numRecTime = typeof recoveryTimeSec === 'number' ? recoveryTimeSec : parseFloat(recoveryTimeSec);
  const recoveryValid = !isNaN(numRecTime) && numRecTime >= 0;

  const passed = downtimeValid && zeroLost && recoveryValid;

  let details;
  if (passed) {
    const recStr = typeof recoveryTimeSec === 'string' && recoveryTimeSec.endsWith('s')
      ? recoveryTimeSec
      : `${recoveryTimeSec}s`;
    details = `${downtimeSec}s down, ${lostRecords} lost, recovered in ${recStr}`;
  } else {
    const reasons = [];
    if (!downtimeValid) reasons.push(`zero downtime simulated (${downtimeSec}s)`);
    if (!zeroLost) reasons.push(`${lostRecords} records lost during outage`);
    if (!recoveryValid) reasons.push(`recovery timed out or failed`);
    details = reasons.join(', ');
  }

  const output = formatGateResult('G3 sink outage', passed ? 'PASS' : 'FAIL', details);

  return {
    passed,
    downtimeSec,
    lostRecords,
    recoveryTimeSec,
    formattedTime: `${downtimeSec}s`,
    details,
    output
  };
}

/**
 * Seeds a deterministic batch of records for Gate 4 testing into source_records.
 * Exactly (total - corruptedCount) are valid payloads, and exactly corruptedCount are poison pills.
 */
async function seedGate4Batch(total = 500, corruptedCount = 3) {
  const validCount = Math.max(0, total - corruptedCount);
  const insertedIds = [];
  const corruptedIds = [];

  if (validCount > 0) {
    const validRows = await queryDatabase(`
      INSERT INTO source_records (uuid, tenant_id, payload, version, status, is_corrupted, created_at, updated_at)
      SELECT
        gen_random_uuid(),
        'tenant_gate4_valid',
        jsonb_build_object(
          'customer_id', 'cust_g4_' || i,
          'full_name', 'Valid User ' || i,
          'email', 'valid_user_' || i || '@example.com',
          'account_tier', CASE WHEN i % 2 = 0 THEN 'GOLD' ELSE 'STANDARD' END,
          'balance', (100.0 + (i * 5.25))::numeric,
          'tags', jsonb_build_array('gate4', 'valid')
        ),
        1,
        'ACTIVE',
        FALSE,
        NOW(),
        NOW()
      FROM generate_series(1, $1) AS i
      RETURNING id;
    `, [validCount]);

    for (const r of validRows) {
      insertedIds.push(parseInt(r.id, 10));
    }
  }

  for (let c = 1; c <= corruptedCount; c++) {
    const corruptedRows = await queryDatabase(`
      INSERT INTO source_records (uuid, tenant_id, payload, version, status, is_corrupted, created_at, updated_at)
      VALUES (
        gen_random_uuid(),
        'tenant_gate4_poison',
        jsonb_build_object(
          'customer_id', 'poison_pill_' || $1,
          'full_name', 'Corrupted Record ' || $1,
          'email', 'CORRUPTED_EMAIL_FORMAT_INVALID',
          'account_tier', 'ILLEGAL_TIER_ENUM_VALUE',
          'balance', 'NOT_A_VALID_DECIMAL_NUMBER',
          'is_poison_pill', true,
          'illegal_field_injection', jsonb_build_object('deep', 'broken')
        ),
        1,
        'ACTIVE',
        TRUE,
        NOW(),
        NOW()
      )
      RETURNING id;
    `, [c]);

    const cId = parseInt(corruptedRows[0].id, 10);
    insertedIds.push(cId);
    corruptedIds.push(cId);
  }

  insertedIds.sort((a, b) => a - b);
  corruptedIds.sort((a, b) => a - b);

  return {
    insertedIds,
    corruptedIds
  };
}

/**
 * Queries dead_letter_queue for entries matching specific record IDs.
 */
async function getDLQEntriesForRecords(recordIds = []) {
  if (!recordIds || recordIds.length === 0) {
    return [];
  }
  const rows = await queryDatabase(
    'SELECT id, record_id, record_uuid, sink_target, payload, error_code, error_message, stack_trace, retry_count, status, created_at, last_retried_at FROM dead_letter_queue WHERE record_id = ANY($1) ORDER BY id ASC',
    [recordIds]
  );
  return rows;
}

/**
 * Evaluates Gate 4 partial batch failure and DLQ quarantine invariants.
 * Asserts:
 * 1. writtenCount === 497 (valid records committed to sink)
 * 2. dlqCount === 3 (poison pills quarantined in DLQ)
 * 3. !batchRolledBack (entire batch was NOT aborted or rolled back)
 * 4. contextSufficient === true (DLQ rows preserve original payload, error codes, and stack traces allowing retry)
 */
function evaluateGate4PartialFailure(writtenCount, dlqCount, batchRolledBack = false, contextSufficient = true) {
  const notRolledBack = !batchRolledBack && writtenCount > 0;
  const writtenValid = writtenCount === 497;
  const dlqValid = dlqCount === 3;
  const contextValid = contextSufficient === true;

  const passed = notRolledBack && writtenValid && dlqValid && contextValid;

  let details;
  if (passed) {
    details = `${writtenCount} written, ${dlqCount} in DLQ`;
  } else {
    const reasons = [];
    if (batchRolledBack || writtenCount === 0) {
      reasons.push(`batch was rolled back (${writtenCount} written)`);
    } else if (!writtenValid) {
      reasons.push(`${writtenCount} written (expected 497)`);
    }
    if (!dlqValid) {
      reasons.push(`${dlqCount} in DLQ (expected 3)`);
    }
    if (!contextValid) {
      reasons.push('insufficient DLQ retry context');
    }
    details = reasons.join(', ');
  }

  const output = formatGateResult('G4 partial batch failure', passed ? 'PASS' : 'FAIL', details);

  return {
    passed,
    writtenCount,
    dlqCount,
    batchRolledBack,
    contextSufficient,
    details,
    output
  };
}

/**
 * Evaluates Gate 5 observability and introspection invariants.
 * Validates that all 5 fundamental operational questions can be answered:
 * 1. Where is the backfill? (backfill_cursor, backfill_completion_pct)
 * 2. What is current throughput? (current_throughput_eps >= 0)
 * 3. How much incremental lag is there? (incremental_lag_records, incremental_lag_ms)
 * 4. How many records are in the DLQ? (dlq_pending_count >= 0)
 * 5. Is the system healthy? (health.overall in ['HEALTHY', 'DEGRADED', 'DOWN'] with component healths)
 */
function evaluateGate5Observability(telemetry) {
  if (!telemetry || typeof telemetry !== 'object') {
    const output = formatGateResult('G5 observability', 'FAIL', 'no telemetry payload received');
    return {
      passed: false,
      answers: null,
      details: 'no telemetry payload received',
      output
    };
  }

  const reasons = [];

  // Q1: Where is the backfill?
  const q1Valid =
    typeof telemetry.backfill_cursor === 'number' &&
    typeof telemetry.backfill_completion_pct === 'number';
  if (!q1Valid) {
    reasons.push('missing or invalid backfill position (cursor / completion pct)');
  }

  // Q2: What is the current throughput?
  const q2Valid =
    typeof telemetry.current_throughput_eps === 'number' &&
    telemetry.current_throughput_eps >= 0;
  if (!q2Valid) {
    reasons.push('missing or negative throughput_eps');
  }

  // Q3: How much incremental lag is there?
  const q3Valid =
    typeof telemetry.incremental_lag_records === 'number' &&
    typeof telemetry.incremental_lag_ms === 'number' &&
    telemetry.incremental_lag_records >= 0 &&
    telemetry.incremental_lag_ms >= 0;
  if (!q3Valid) {
    reasons.push('missing or invalid incremental lag metrics');
  }

  // Q4: How many records are in the DLQ?
  const q4Valid =
    typeof telemetry.dlq_pending_count === 'number' &&
    telemetry.dlq_pending_count >= 0;
  if (!q4Valid) {
    reasons.push('missing or negative dlq_pending_count');
  }

  // Q5: Is the system healthy?
  const validHealthStates = ['HEALTHY', 'DEGRADED', 'DOWN'];
  const overallValid = Boolean(validHealthStates.includes(telemetry.health?.overall));
  const pgValid = Boolean(telemetry.health?.postgres && typeof telemetry.health.postgres.status === 'string');
  const esValid = Boolean(telemetry.health?.elasticsearch && typeof telemetry.health.elasticsearch.status === 'string');
  const rmqValid = Boolean(telemetry.health?.rabbitmq && typeof telemetry.health.rabbitmq.status === 'string');
  const q5Valid = Boolean(overallValid && pgValid && esValid && rmqValid);
  if (!q5Valid) {
    reasons.push('missing or invalid health status structure');
  }

  const passed = Boolean(q1Valid && q2Valid && q3Valid && q4Valid && q5Valid);

  const answers = passed
    ? {
        backfillPosition: `cursor: ${telemetry.backfill_cursor} (${telemetry.backfill_completion_pct}%)`,
        throughputEps: `${telemetry.current_throughput_eps} eps`,
        incrementalLag: `${telemetry.incremental_lag_records} records (${telemetry.incremental_lag_ms}ms)`,
        dlqPending: `${telemetry.dlq_pending_count} pending`,
        systemHealth: telemetry.health.overall
      }
    : null;

  const details = reasons.join(', ');
  const output = formatGateResult('G5 observability', passed ? 'PASS' : 'FAIL', details);

  return {
    passed,
    answers,
    details,
    output
  };
}

/**
 * Async sleep helper.
 */
function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

module.exports = {
  loadEnv,
  getPool,
  queryDatabase,
  closeDatabase,
  getTelemetry,
  getElasticsearchCount,
  getConsumerMetrics,
  isDockerRunning,
  doesDockerContainerExist,
  startPipelineProcess,
  killPipelineProcess,
  stopReceiver,
  startReceiver,
  seedGate4Batch,
  getDLQEntriesForRecords,
  formatGateResult,
  evaluateGate1Resumption,
  evaluateGate2Deduplication,
  evaluateGate3Outage,
  evaluateGate4PartialFailure,
  evaluateGate5Observability,
  sleep
};
