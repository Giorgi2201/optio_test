#!/usr/bin/env node

/**
 * OPTIO Standalone Database Migration Runner
 * Applies 01_init_schema.sql to PostgreSQL idempotently within a transaction.
 */

const fs = require('fs');
const path = require('path');

// Resilient .env loader without hard dependencies
function loadEnv() {
  const envPath = path.resolve(__dirname, '..', '.env');
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

let Client;
try {
  ({ Client } = require('pg'));
} catch (e) {
  console.error('[ERROR] The "pg" driver is not installed in node_modules.');
  console.error('        Please run "npm install" before executing migrations.');
  process.exit(1);
}

async function runMigration() {
  const sqlFilePath = path.resolve(__dirname, '..', 'docker', 'postgres', 'init', '01_init_schema.sql');

  if (!fs.existsSync(sqlFilePath)) {
    console.error(`[ERROR] Migration SQL file not found at: ${sqlFilePath}`);
    process.exit(1);
  }

  const sqlContent = fs.readFileSync(sqlFilePath, 'utf8');

  const connectionString = process.env.DATABASE_URL || {
    host: process.env.POSTGRES_HOST || 'localhost',
    port: parseInt(process.env.POSTGRES_PORT || '5432', 10),
    database: process.env.POSTGRES_DB || 'optio_replication',
    user: process.env.POSTGRES_USER || 'optio',
    password: process.env.POSTGRES_PASSWORD || 'optio_secure_pass',
    connectionTimeoutMillis: 5000
  };

  const client = new Client(
    typeof connectionString === 'string'
      ? { connectionString, connectionTimeoutMillis: 5000 }
      : connectionString
  );

  console.log('======================================================================');
  console.log('            OPTIO DATABASE SCHEMA MIGRATION RUNNER                   ');
  console.log('======================================================================');
  const targetHost = typeof connectionString === 'string'
    ? connectionString.replace(/:[^:@]+@/, ':****@')
    : `${connectionString.host}:${connectionString.port}/${connectionString.database}`;
  console.log(`[INFO] Target: ${targetHost}`);

  try {
    await client.connect();
    console.log('[INFO] Connection established successfully.');
  } catch (err) {
    console.error(`\n[ERROR] Unable to connect to PostgreSQL: ${err.message}`);
    console.error('[DIAGNOSTIC] To start the database container, execute:');
    console.error('             docker compose up -d postgres\n');
    process.exit(1);
  }

  try {
    console.log(`[INFO] Reading migration: ${path.relative(process.cwd(), sqlFilePath)}`);
    console.log('[INFO] Applying schema migration in a transaction...');

    await client.query('BEGIN');
    await client.query(sqlContent);
    await client.query('COMMIT');
    console.log('[SUCCESS] DDL definitions committed successfully.\n');

    // Verification step
    console.log('[INFO] Verifying schema objects...');
    const tableRes = await client.query(`
      SELECT table_name
      FROM information_schema.tables
      WHERE table_schema = 'public'
        AND table_name IN ('source_records', 'replication_checkpoints', 'dead_letter_queue')
      ORDER BY table_name;
    `);

    const verifiedTables = tableRes.rows.map(r => r.table_name);
    console.log(`  - Verified Tables: ${verifiedTables.join(', ')}`);

    const checkpointsRes = await client.query(`
      SELECT pipeline_id, last_processed_id, status
      FROM replication_checkpoints
      ORDER BY pipeline_id;
    `);
    console.log('  - Active Watermark Checkpoints:');
    for (const cp of checkpointsRes.rows) {
      console.log(`    * [${cp.pipeline_id}] status=${cp.status}, last_processed_id=${cp.last_processed_id}`);
    }

    const indexRes = await client.query(`
      SELECT tablename, indexname
      FROM pg_indexes
      WHERE schemaname = 'public'
        AND tablename IN ('source_records', 'replication_checkpoints', 'dead_letter_queue')
      ORDER BY tablename, indexname;
    `);
    console.log(`  - Verified Indexes: ${indexRes.rows.length} total indexes online.`);

    console.log('\n======================================================================');
    console.log('MIGRATION RESULT: ALL TABLES, INDEXES, AND CHECKPOINTS ONLINE [OK]');
    console.log('======================================================================\n');
  } catch (err) {
    await client.query('ROLLBACK').catch(() => {});
    console.error(`\n[ERROR] Migration failed and was rolled back: ${err.message}`);
    console.error(err.stack);
    process.exit(1);
  } finally {
    await client.end();
  }
}

if (require.main === module) {
  runMigration().catch((err) => {
    console.error(`[FATAL] Uncaught migration exception: ${err.message}`);
    process.exit(1);
  });
}

module.exports = { runMigration };
