/**
 * Unit & Integration Test Suite for DLQReplayer
 * Validates re-driving poison pills, status transition to RESOLVED on success,
 * retry count increments on failure, and batch replay aggregation.
 */

import { describe, it } from 'node:test';
import assert from 'node:assert';
import { Pool } from 'pg';
import { DLQEntry, SourceRecord } from '@optio/shared';
import { DLQReplayer } from '../dlq.replayer.js';
import { DLQStore } from '../dlq.store.js';
import { ElasticsearchSink, BulkUpsertResult } from '../../sinks/elasticsearch/elasticsearch.sink.js';
import { RabbitMQSink, BatchPublishResult } from '../../sinks/rabbitmq/rabbitmq.sink.js';

describe('DLQReplayer - Poison Pill Replay & Remediation', () => {
  it('1. Successfully replays pending item to Elasticsearch and marks status as RESOLVED', async () => {
    let statusMarked: string | null = null;
    let esDispatched = false;

    const mockEntry: DLQEntry = {
      id: 10,
      record_id: 101,
      record_uuid: 'uuid-101',
      sink_target: 'ELASTICSEARCH',
      payload: {
        customer_id: 'cust-101',
        first_name: 'Fixed',
        last_name: 'Customer',
        email: 'fixed@test.com',
        account_tier: 'STANDARD',
        balance: 250
      },
      error_code: 'ES_PARSE_ERR',
      error_message: 'Temporary mapping error',
      stack_trace: null,
      retry_count: 0,
      status: 'PENDING',
      created_at: new Date().toISOString(),
      last_retried_at: null
    };

    const mockDLQStore = {
      getEntryById: async (id: number) => (id === 10 ? mockEntry : null),
      markStatus: async (_id: number, s: string) => {
        statusMarked = s;
      },
      recordRetryFailure: async () => {}
    } as unknown as DLQStore;

    const mockESSink = {
      bulkUpsert: async (records: SourceRecord[]): Promise<BulkUpsertResult> => {
        esDispatched = true;
        assert.strictEqual(records.length, 1);
        assert.strictEqual(records[0].id, 101);
        return {
          successCount: 1,
          failedCount: 0,
          successfulIds: [101],
          failures: []
        };
      }
    } as unknown as ElasticsearchSink;

    const mockRMQSink = {
      publishBatch: async () => ({
        successCount: 0,
        failedCount: 0,
        successfulIds: [],
        failures: []
      })
    } as unknown as RabbitMQSink;

    const replayer = new DLQReplayer({} as Pool, mockDLQStore, mockESSink, mockRMQSink);

    const result = await replayer.retryEntry(10);

    assert.strictEqual(result.success, true);
    assert.strictEqual(esDispatched, true);
    assert.strictEqual(statusMarked, 'RESOLVED');
  });

  it('2. Records failure and increments retry_count when target sink rejects the replayed item', async () => {
    let failureRecorded = false;
    let statusMarked: string | null = null;

    const mockEntry: DLQEntry = {
      id: 25,
      record_id: 205,
      record_uuid: 'uuid-205',
      sink_target: 'RABBITMQ',
      payload: { customer_id: 'cust-205' },
      error_code: 'AMQP_NACK',
      error_message: 'Broker rejected',
      stack_trace: null,
      retry_count: 1,
      status: 'PENDING',
      created_at: new Date().toISOString(),
      last_retried_at: null
    };

    const mockDLQStore = {
      getEntryById: async (id: number) => (id === 25 ? mockEntry : null),
      markStatus: async (_id: number, s: string) => {
        statusMarked = s;
      },
      recordRetryFailure: async (id: number) => {
        if (id === 25) failureRecorded = true;
      }
    } as unknown as DLQStore;

    const mockESSink = {
      bulkUpsert: async () => ({
        successCount: 0,
        failedCount: 0,
        successfulIds: [],
        failures: []
      })
    } as unknown as ElasticsearchSink;

    const mockRMQSink = {
      publishBatch: async (_records: SourceRecord[]): Promise<BatchPublishResult> => {
        return {
          successCount: 0,
          failedCount: 1,
          successfulIds: [],
          failures: [
            {
              recordId: 205,
              errorCode: 'AMQP_NACK',
              errorReason: 'Queue disk full'
            }
          ]
        };
      }
    } as unknown as RabbitMQSink;

    const replayer = new DLQReplayer({} as Pool, mockDLQStore, mockESSink, mockRMQSink);

    const result = await replayer.retryEntry(25);

    assert.strictEqual(result.success, false);
    assert.strictEqual(result.error, 'Queue disk full');
    assert.strictEqual(failureRecorded, true, 'Must record failure in DB');
    assert.strictEqual(statusMarked, null, 'Must NOT mark RESOLVED on failure');
  });

  it('3. Returns success immediately if item is already marked RESOLVED', async () => {
    let esCalled = false;

    const mockEntry: DLQEntry = {
      id: 30,
      record_id: 300,
      record_uuid: 'uuid-300',
      sink_target: 'ELASTICSEARCH',
      payload: {},
      error_code: 'ES_ERR',
      error_message: '',
      stack_trace: null,
      retry_count: 0,
      status: 'RESOLVED',
      created_at: new Date().toISOString(),
      last_retried_at: null
    };

    const mockDLQStore = {
      getEntryById: async () => mockEntry
    } as unknown as DLQStore;

    const mockESSink = {
      bulkUpsert: async () => {
        esCalled = true;
        return { successCount: 0, failedCount: 0, successfulIds: [], failures: [] };
      }
    } as unknown as ElasticsearchSink;

    const replayer = new DLQReplayer({} as Pool, mockDLQStore, mockESSink, {} as RabbitMQSink);

    const result = await replayer.retryEntry(30);

    assert.strictEqual(result.success, true);
    assert.strictEqual(esCalled, false, 'Should not re-dispatch resolved record');
  });

  it('4. retryAllPending: Processes pending entries in batches and summarizes resolved and failed items', async () => {
    const entries: DLQEntry[] = [
      {
        id: 1,
        record_id: 10,
        record_uuid: 'uuid-10',
        sink_target: 'ELASTICSEARCH',
        payload: { email: 'pass@test.com' },
        error_code: 'ES_ERR',
        error_message: 'Temporary glitch',
        stack_trace: null,
        retry_count: 0,
        status: 'PENDING',
        created_at: new Date().toISOString(),
        last_retried_at: null
      },
      {
        id: 2,
        record_id: 20,
        record_uuid: 'uuid-20',
        sink_target: 'ELASTICSEARCH',
        payload: { email: 'fail@test.com' },
        error_code: 'ES_ERR',
        error_message: 'Permanent malformed schema',
        stack_trace: null,
        retry_count: 0,
        status: 'PENDING',
        created_at: new Date().toISOString(),
        last_retried_at: null
      }
    ];

    let pendingCallCount = 0;
    const mockDLQStore = {
      getPendingEntries: async () => {
        pendingCallCount++;
        return pendingCallCount === 1 ? entries : [];
      },
      getEntryById: async (id: number) => entries.find((e) => e.id === id) || null,
      markStatus: async () => {},
      recordRetryFailure: async () => {}
    } as unknown as DLQStore;

    const mockESSink = {
      bulkUpsert: async (records: SourceRecord[]): Promise<BulkUpsertResult> => {
        if (records[0].id === 10) {
          return { successCount: 1, failedCount: 0, successfulIds: [10], failures: [] };
        }
        return {
          successCount: 0,
          failedCount: 1,
          successfulIds: [],
          failures: [{ recordId: 20, errorCode: 'ES_PERM_ERR', errorReason: 'Corrupted payload', rawItem: null }]
        };
      }
    } as unknown as ElasticsearchSink;

    const replayer = new DLQReplayer({} as Pool, mockDLQStore, mockESSink, {} as RabbitMQSink);

    const summary = await replayer.retryAllPending();

    assert.strictEqual(summary.retried, 2);
    assert.strictEqual(summary.resolved, 1);
    assert.strictEqual(summary.failed, 1);
  });
});
