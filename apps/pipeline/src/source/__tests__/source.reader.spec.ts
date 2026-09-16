/**
 * Unit & Keyset Mechanics Tests for SourceReader
 * Validates O(1) keyset queries, composite incremental tie-breaking, and row mapping.
 */

import { describe, it } from 'node:test';
import assert from 'node:assert';
import { QueryResultRow } from 'pg';
import { SourceReader, Queryable } from '../source.reader.js';

describe('SourceReader - Keyset & Cursor Extraction', () => {
  it('1. fetchBackfillBatch executes keyset query on id > $1 with bounded limit', async () => {
    let capturedQuery = '';
    let capturedValues: unknown[] = [];

    const mockDb: Queryable = {
      query: async <R extends QueryResultRow>(text: string, values?: unknown[]) => {
        capturedQuery = text;
        capturedValues = values || [];
        return {
          rows: [
            {
              id: '412001',
              uuid: '00000000-0000-0000-0000-000000412001',
              tenant_id: 'tenant_gamma',
              payload: { customer_id: 'CUST-412001', first_name: 'David', last_name: 'Smith', email: 'd@x.com', account_tier: 'STANDARD', balance: 50, metadata: { signup_channel: 'WEB', country: 'US', tags: [] } },
              version: '1',
              status: 'ACTIVE',
              is_corrupted: false,
              created_at: new Date('2026-09-01T10:00:00.000Z'),
              updated_at: new Date('2026-09-01T10:00:00.000Z')
            }
          ] as unknown as R[],
          command: 'SELECT',
          rowCount: 1,
          oid: 0,
          fields: []
        };
      }
    };

    const reader = new SourceReader(mockDb, { batchSize: 500 });
    const records = await reader.fetchBackfillBatch(412000, 1000);

    // Assert query structure
    assert.ok(capturedQuery.includes('WHERE id > $1'));
    assert.ok(capturedQuery.includes('ORDER BY id ASC'));
    assert.ok(capturedQuery.includes('LIMIT $2'));
    assert.deepStrictEqual(capturedValues, [412000, 1000]);

    // Assert mapping
    assert.strictEqual(records.length, 1);
    assert.strictEqual(records[0].id, 412001);
    assert.strictEqual(records[0].tenant_id, 'tenant_gamma');
    assert.strictEqual(records[0].created_at, '2026-09-01T10:00:00.000Z');
  });

  it('2. fetchIncrementalBatch executes composite keyset query with tie-breaker clause', async () => {
    let capturedQuery = '';
    let capturedValues: unknown[] = [];

    const mockDb: Queryable = {
      query: async <R extends QueryResultRow>(text: string, values?: unknown[]) => {
        capturedQuery = text;
        capturedValues = values || [];
        return {
          rows: [] as unknown as R[],
          command: 'SELECT',
          rowCount: 0,
          oid: 0,
          fields: []
        };
      }
    };

    const reader = new SourceReader(mockDb, { batchSize: 250 });
    const timestamp = new Date('2026-09-10T15:30:00.000Z');
    await reader.fetchIncrementalBatch(timestamp, 8500, 250);

    assert.ok(
      capturedQuery.includes('(updated_at > $1) OR (updated_at = $1 AND id > $2)'),
      'Query must use composite keyset tie-breaker'
    );
    assert.ok(capturedQuery.includes('ORDER BY updated_at ASC, id ASC'));
    assert.ok(capturedQuery.includes('LIMIT $3'));
    assert.deepStrictEqual(capturedValues, [timestamp, 8500, 250]);
  });

  it('3. Correctly deserializes stringified JSON payloads and bigints', async () => {
    const mockDb: Queryable = {
      query: async <R extends QueryResultRow>() => {
        return {
          rows: [
            {
              id: '9007199254740991',
              uuid: '123e4567-e89b-12d3-a456-426614174000',
              tenant_id: 'tenant_delta',
              payload: JSON.stringify({
                customer_id: 'CUST-999',
                first_name: 'Nino',
                last_name: 'Kapanadze',
                email: 'nino@example.com',
                account_tier: 'ENTERPRISE',
                balance: 100000.55,
                metadata: { signup_channel: 'API', country: 'GE', tags: ['vip'] }
              }),
              version: '3',
              status: 'ACTIVE',
              is_corrupted: false,
              created_at: '2026-09-02T08:00:00.000Z',
              updated_at: '2026-09-02T08:00:00.000Z'
            }
          ] as unknown as R[],
          command: 'SELECT',
          rowCount: 1,
          oid: 0,
          fields: []
        };
      }
    };

    const reader = new SourceReader(mockDb);
    const [record] = await reader.fetchBackfillBatch(0);

    assert.strictEqual(record.id, 9007199254740991);
    assert.strictEqual(record.version, 3);
    assert.strictEqual(record.payload.customer_id, 'CUST-999');
    assert.strictEqual(record.payload.first_name, 'Nino');
    assert.strictEqual(record.payload.account_tier, 'ENTERPRISE');
    assert.strictEqual(record.payload.balance, 100000.55);
  });

  it('4. getSourceMetadata computes maxId and totalCount', async () => {
    const mockDb: Queryable = {
      query: async <R extends QueryResultRow>() => {
        return {
          rows: [{ max_id: '500000', total_count: '500000' }] as unknown as R[],
          command: 'SELECT',
          rowCount: 1,
          oid: 0,
          fields: []
        };
      }
    };

    const reader = new SourceReader(mockDb);
    const meta = await reader.getSourceMetadata();

    assert.strictEqual(meta.maxId, 500000);
    assert.strictEqual(meta.totalCount, 500000);
  });

  it('5. getIncrementalLag accurately computes lag count and millisecond delta', async () => {
    const twoMinutesAgo = new Date(Date.now() - 120000);
    const mockDb: Queryable = {
      query: async <R extends QueryResultRow>() => {
        return {
          rows: [
            { lag_count: '450', newest_timestamp: twoMinutesAgo.toISOString() }
          ] as unknown as R[],
          command: 'SELECT',
          rowCount: 1,
          oid: 0,
          fields: []
        };
      }
    };

    const reader = new SourceReader(mockDb);
    const lag = await reader.getIncrementalLag(new Date(), 0);

    assert.strictEqual(lag.lagRecords, 450);
    // Allow slight variance around 120000ms
    assert.ok(lag.lagMs >= 115000 && lag.lagMs <= 125000, `lagMs ${lag.lagMs} should be approximately 120,000ms`);
  });
});
