/**
 * Unit & Error Decomposition Tests for ElasticsearchSink
 * Validates deterministic mapping, idempotent upsert payloads, and Gate 4 fault isolation.
 */

import { describe, it } from 'node:test';
import assert from 'node:assert';
import { ElasticsearchSink } from '../elasticsearch.sink.js';
import { SourceRecord } from '@optio/shared';
import { Client } from '@elastic/elasticsearch';

// Helper to create a mock SourceRecord
function createMockRecord(id: number, isCorrupted = false): SourceRecord {
  return {
    id,
    uuid: `00000000-0000-0000-0000-${String(id).padStart(12, '0')}`,
    tenant_id: 'tenant_alpha',
    payload: {
      customer_id: `CUST-${id}`,
      first_name: 'Giorgi',
      last_name: 'Beridze',
      email: `user.${id}@example.com`,
      account_tier: 'PREMIUM',
      balance: 1250.50,
      metadata: {
        signup_channel: 'WEB',
        country: 'GE',
        tags: ['verified', 'kyc_complete']
      }
    },
    version: 1,
    status: 'ACTIVE',
    is_corrupted: isCorrupted,
    created_at: '2026-09-01T12:00:00.000Z',
    updated_at: '2026-09-01T12:00:00.000Z'
  };
}

describe('ElasticsearchSink Adapter', () => {
  const dummyClient = {} as unknown as Client;
  const sink = new ElasticsearchSink(dummyClient, { indexName: 'records_search_index' });

  it('1. Deterministically transforms SourceRecord to SearchDocument with exact ID mapping', () => {
    const record = createMockRecord(42);
    const searchDoc = sink.transformRecord(record);

    assert.strictEqual(searchDoc.id, '42', 'ES _id must strictly equal source record ID as string');
    assert.strictEqual(searchDoc.source_uuid, record.uuid);
    assert.strictEqual(searchDoc.tenant_id, 'tenant_alpha');
    assert.strictEqual(searchDoc.customer_id, 'CUST-42');
    assert.strictEqual(searchDoc.full_name, 'Giorgi Beridze');
    assert.strictEqual(searchDoc.email, 'user.42@example.com');
    assert.strictEqual(searchDoc.account_tier, 'PREMIUM');
    assert.strictEqual(searchDoc.balance, 1250.50);
    assert.strictEqual(searchDoc.status, 'ACTIVE');
    assert.strictEqual(searchDoc.version, 1);
    assert.deepStrictEqual(searchDoc.tags, ['verified', 'kyc_complete']);
    assert.strictEqual(searchDoc.country, 'GE');
    assert.strictEqual(searchDoc.source_created_at, '2026-09-01T12:00:00.000Z');
    assert.ok(searchDoc.synced_at, 'synced_at must be populated');
  });

  it('2. Decomposes bulk response: 497 successful items and 3 isolated poison pills (Gate 4)', () => {
    const totalRecords = 500;
    const records: SourceRecord[] = [];
    const corruptedIndices = new Set([15, 120, 480]);

    for (let i = 1; i <= totalRecords; i++) {
      records.push(createMockRecord(i, corruptedIndices.has(i)));
    }

    // Build simulated Elasticsearch bulk response
    const mockItems = records.map((record) => {
      if (corruptedIndices.has(record.id)) {
        return {
          update: {
            _index: 'records_search_index',
            _id: String(record.id),
            status: 400,
            error: {
              type: 'mapper_parsing_exception',
              reason: `failed to parse field [balance] of type [double] in document with id '${record.id}'`
            }
          }
        };
      }
      return {
        update: {
          _index: 'records_search_index',
          _id: String(record.id),
          _version: 1,
          result: 'created',
          status: 201
        }
      };
    });

    const mockResponse = {
      errors: true,
      items: mockItems
    };

    const result = sink.parseBulkResponse(records, mockResponse);

    // Assertions for Gate 4: Partial batch failure handling
    assert.strictEqual(result.successCount, 497, 'Exactly 497 valid records must succeed');
    assert.strictEqual(result.failedCount, 3, 'Exactly 3 corrupted records must be flagged');
    assert.strictEqual(result.successfulIds.length, 497);
    assert.strictEqual(result.failures.length, 3);

    // Check failed records isolation
    const failedIds = result.failures.map((f) => f.recordId);
    assert.deepStrictEqual(failedIds, [15, 120, 480]);

    for (const failure of result.failures) {
      assert.strictEqual(failure.errorCode, 'mapper_parsing_exception');
      assert.ok(failure.errorReason.includes('failed to parse field [balance]'));
    }

    // Confirm that valid record IDs do not include failed IDs
    for (const failedId of failedIds) {
      assert.ok(!result.successfulIds.includes(failedId), `Failed ID ${failedId} must not be in successfulIds`);
    }
  });

  it('3. Guarantees deterministic idempotency across repeated writes (Gate 2)', () => {
    // Repeated ingestion of record 100 with same or newer version
    const initialRecord = createMockRecord(100);
    const doc1 = sink.transformRecord(initialRecord);

    const updatedRecord = { ...createMockRecord(100), version: 2 };
    const doc2 = sink.transformRecord(updatedRecord);

    // Document _id remains identical across versions
    assert.strictEqual(doc1.id, doc2.id);
    assert.strictEqual(doc1.id, '100');
    assert.strictEqual(doc2.version, 2);
  });

  it('4. Handles completely clean batch without errors fast-path', () => {
    const records = [createMockRecord(1), createMockRecord(2), createMockRecord(3)];
    const cleanResponse = {
      errors: false,
      items: [
        { update: { status: 200 } },
        { update: { status: 200 } },
        { update: { status: 200 } }
      ]
    };

    const result = sink.parseBulkResponse(records, cleanResponse);
    assert.strictEqual(result.successCount, 3);
    assert.strictEqual(result.failedCount, 0);
    assert.deepStrictEqual(result.successfulIds, [1, 2, 3]);
    assert.strictEqual(result.failures.length, 0);
  });
});
