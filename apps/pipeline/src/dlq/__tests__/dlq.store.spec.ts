/**
 * Unit & Gate 4 Partial Batch Isolation Tests for DLQStore
 * Validates persistent poison pill capture without aborting valid records.
 */

import { describe, it } from 'node:test';
import assert from 'node:assert';
import { QueryResultRow } from 'pg';
import { DLQStore, NewDLQEntry } from '../dlq.store.js';
import { Queryable } from '../../source/source.reader.js';

describe('DLQStore - Gate 4 Poison Pill Isolation & Persistence', () => {
  it('1. Persists batch of isolated failures with diagnostic metadata and returns DLQ IDs', async () => {
    let capturedQuery = '';
    let capturedValues: unknown[] = [];

    const mockDb: Queryable = {
      query: async <R extends QueryResultRow>(text: string, values?: unknown[]) => {
        capturedQuery = text;
        capturedValues = values || [];
        return {
          rows: [{ id: '101' }, { id: '102' }, { id: '103' }] as unknown as R[],
          command: 'INSERT',
          rowCount: 3,
          oid: 0,
          fields: []
        };
      }
    };

    const store = new DLQStore(mockDb);

    const failures: NewDLQEntry[] = [
      {
        recordId: 15,
        recordUuid: '00000000-0000-0000-0000-000000000015',
        sinkTarget: 'ELASTICSEARCH',
        payload: { balance: 'not_a_number' },
        errorCode: 'ES_MAPPER_PARSING_EXCEPTION',
        errorMessage: 'failed to parse field [balance] of type [double]',
        stackTrace: 'Error at ES Sink'
      },
      {
        recordId: 120,
        recordUuid: '00000000-0000-0000-0000-000000000120',
        sinkTarget: 'RABBITMQ',
        payload: { raw: 'corrupted_json' },
        errorCode: 'AMQP_PAYLOAD_CORRUPTED',
        errorMessage: 'Synthetic poison pill flag detected',
        stackTrace: null
      },
      {
        recordId: 480,
        recordUuid: '00000000-0000-0000-0000-000000000480',
        sinkTarget: 'ALL',
        payload: { invalid: true },
        errorCode: 'VALIDATION_FAILED',
        errorMessage: 'Schema constraint violation'
      }
    ];

    const insertedIds = await store.persistFailures(failures);

    assert.ok(capturedQuery.includes('INSERT INTO dead_letter_queue'));
    assert.ok(capturedQuery.includes('RETURNING id'));
    assert.deepStrictEqual(insertedIds, [101, 102, 103]);

    // Check parameter alignment for first entry
    assert.strictEqual(capturedValues[0], 15);
    assert.strictEqual(capturedValues[1], '00000000-0000-0000-0000-000000000015');
    assert.strictEqual(capturedValues[2], 'ELASTICSEARCH');
    assert.strictEqual(capturedValues[4], 'ES_MAPPER_PARSING_EXCEPTION');
  });

  it('2. Empty failure batch returns empty array without executing database query', async () => {
    let queryExecuted = false;
    const mockDb: Queryable = {
      query: async <R extends QueryResultRow>() => {
        queryExecuted = true;
        return { rows: [] as unknown as R[], command: 'INSERT', rowCount: 0, oid: 0, fields: [] };
      }
    };

    const store = new DLQStore(mockDb);
    const result = await store.persistFailures([]);

    assert.strictEqual(result.length, 0);
    assert.strictEqual(queryExecuted, false);
  });

  it('3. getPendingCount retrieves unresolved poison pills for Gate 5 telemetry', async () => {
    const mockDb: Queryable = {
      query: async <R extends QueryResultRow>() => {
        return {
          rows: [{ pending_count: '12' }] as unknown as R[],
          command: 'SELECT',
          rowCount: 1,
          oid: 0,
          fields: []
        };
      }
    };

    const store = new DLQStore(mockDb);
    const count = await store.getPendingCount();

    assert.strictEqual(count, 12);
  });

  it('4. markStatus updates state to RESOLVED and records timestamp', async () => {
    let capturedQuery = '';
    let capturedValues: unknown[] = [];

    const mockDb: Queryable = {
      query: async <R extends QueryResultRow>(text: string, values?: unknown[]) => {
        capturedQuery = text;
        capturedValues = values || [];
        return { rows: [] as unknown as R[], command: 'UPDATE', rowCount: 1, oid: 0, fields: [] };
      }
    };

    const store = new DLQStore(mockDb);
    await store.markStatus(101, 'RESOLVED');

    assert.ok(capturedQuery.includes('UPDATE dead_letter_queue'));
    assert.ok(capturedQuery.includes('SET status = $1'));
    assert.ok(capturedQuery.includes('last_retried_at = NOW()'));
    assert.deepStrictEqual(capturedValues, ['RESOLVED', 101]);
  });
});
