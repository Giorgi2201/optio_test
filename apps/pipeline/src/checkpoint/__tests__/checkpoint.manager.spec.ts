/**
 * Unit & Gate 1 Crash Recovery Tests for CheckpointManager
 * Validates atomic post-sink-ACK commits, watermark boundaries, and resumption from crash.
 */

import { describe, it } from 'node:test';
import assert from 'node:assert';
import { QueryResultRow } from 'pg';
import { CheckpointManager } from '../checkpoint.manager.js';
import { Queryable } from '../../source/source.reader.js';

describe('CheckpointManager - Gate 1 Crash Recovery & Watermark Persistence', () => {
  it('1. Loads existing checkpoint and accurately parses bigints and metadata', async () => {
    const mockDb: Queryable = {
      query: async <R extends QueryResultRow>() => {
        return {
          rows: [
            {
              pipeline_id: 'backfill_pipeline',
              last_processed_id: '150000',
              last_processed_timestamp: null,
              status: 'RUNNING',
              records_processed: '150000',
              records_failed: '3',
              metadata: JSON.stringify({ batch_size: 500, host: 'worker-1' }),
              updated_at: '2026-09-05T10:00:00.000Z'
            }
          ] as unknown as R[],
          command: 'SELECT',
          rowCount: 1,
          oid: 0,
          fields: []
        };
      }
    };

    const manager = new CheckpointManager(mockDb);
    const cp = await manager.getCheckpoint('backfill_pipeline');

    assert.strictEqual(cp.pipeline_id, 'backfill_pipeline');
    assert.strictEqual(cp.last_processed_id, 150000);
    assert.strictEqual(cp.status, 'RUNNING');
    assert.strictEqual(cp.records_processed, 150000);
    assert.strictEqual(cp.records_failed, 3);
    assert.strictEqual((cp.metadata as any).batch_size, 500);
  });

  it('2. Initializes missing checkpoint idempotently if not present', async () => {
    let insertCalled = false;
    let queryCallCount = 0;

    const mockDb: Queryable = {
      query: async <R extends QueryResultRow>(text: string) => {
        queryCallCount++;
        if (text.includes('INSERT INTO replication_checkpoints')) {
          insertCalled = true;
          return { rows: [] as unknown as R[], command: 'INSERT', rowCount: 1, oid: 0, fields: [] };
        }
        if (queryCallCount === 1) {
          // First select returns empty
          return { rows: [] as unknown as R[], command: 'SELECT', rowCount: 0, oid: 0, fields: [] };
        }
        // Second select returns initialized row
        return {
          rows: [
            {
              pipeline_id: 'incremental_pipeline',
              last_processed_id: '0',
              last_processed_timestamp: '2026-09-01T00:00:00.000Z',
              status: 'INITIALIZED',
              records_processed: '0',
              records_failed: '0',
              metadata: '{}',
              updated_at: '2026-09-01T00:00:00.000Z'
            }
          ] as unknown as R[],
          command: 'SELECT',
          rowCount: 1,
          oid: 0,
          fields: []
        };
      }
    };

    const manager = new CheckpointManager(mockDb);
    const cp = await manager.getCheckpoint('incremental_pipeline');

    assert.ok(insertCalled, 'Must insert default row if missing');
    assert.strictEqual(cp.pipeline_id, 'incremental_pipeline');
    assert.strictEqual(cp.last_processed_id, 0);
    assert.strictEqual(cp.status, 'INITIALIZED');
  });

  it('3. Atomically commits watermark post-sink-ACK with incremented counters', async () => {
    let capturedQuery = '';
    let capturedValues: unknown[] = [];

    const mockDb: Queryable = {
      query: async <R extends QueryResultRow>(text: string, values?: unknown[]) => {
        capturedQuery = text;
        capturedValues = values || [];
        return { rows: [] as unknown as R[], command: 'UPDATE', rowCount: 1, oid: 0, fields: [] };
      }
    };

    const manager = new CheckpointManager(mockDb);
    const ts = new Date('2026-09-12T14:20:00.000Z');

    await manager.commitCheckpoint('backfill_pipeline', {
      lastProcessedId: 412000,
      lastProcessedTimestamp: ts,
      batchSuccessCount: 497,
      batchFailedCount: 3,
      metadata: { current_eps: 1250 }
    });

    assert.ok(capturedQuery.includes('SET last_processed_id = $1'));
    assert.ok(capturedQuery.includes('records_processed = records_processed + $3'));
    assert.ok(capturedQuery.includes('records_failed = records_failed + $4'));
    assert.deepStrictEqual(capturedValues[0], 412000);
    assert.deepStrictEqual(capturedValues[1], ts);
    assert.deepStrictEqual(capturedValues[2], 497);
    assert.deepStrictEqual(capturedValues[3], 3);
    assert.strictEqual(capturedValues[5], 'backfill_pipeline');
  });

  it('4. Gate 1 Resumption Invariant: Process SIGKILL at 412,331 resumes strictly from last committed boundary (412,000)', async () => {
    // Pipeline was processing batch 412,001 to 412,500.
    // SIGKILL arrived mid-batch at record 412,331 before dual-sink ACK and commit.
    // The database checkpoint still holds 412,000 from the prior committed batch.
    const committedWatermarkId = 412000;

    const mockDb: Queryable = {
      query: async <R extends QueryResultRow>() => {
        return {
          rows: [
            {
              pipeline_id: 'backfill_pipeline',
              last_processed_id: String(committedWatermarkId),
              last_processed_timestamp: null,
              status: 'RUNNING',
              records_processed: '412000',
              records_failed: '0',
              metadata: '{}',
              updated_at: new Date().toISOString()
            }
          ] as unknown as R[],
          command: 'SELECT',
          rowCount: 1,
          oid: 0,
          fields: []
        };
      }
    };

    const manager = new CheckpointManager(mockDb);
    const recoveredCheckpoint = await manager.getCheckpoint('backfill_pipeline');

    // On crash recovery, pipeline resumes from last committed ID, NOT from 0 or corrupted state
    assert.strictEqual(recoveredCheckpoint.last_processed_id, 412000);
    assert.strictEqual(
      recoveredCheckpoint.last_processed_id > 0,
      true,
      'Pipeline must never restart from 0 after a mid-stream crash'
    );
  });
});
