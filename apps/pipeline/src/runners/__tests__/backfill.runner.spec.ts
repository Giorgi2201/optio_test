/**
 * Unit & Gate Resilience Test Suite for BackfillRunner
 * Validates Gate 1 Resumption, Gate 4 Partial Batch Isolation,
 * Gate 3 Circuit Breaker Throttling, and Pause/Resume Lifecycle.
 */

import { describe, it } from 'node:test';
import assert from 'node:assert';
import {
  PipelineStatus,
  ReplicationCheckpoint,
  SourceRecord,
  CustomerPayload
} from '@optio/shared';
import { BackfillRunner } from '../backfill.runner.js';
import { SourceReader } from '../../source/source.reader.js';
import { CheckpointManager, CheckpointCommitData } from '../../checkpoint/checkpoint.manager.js';
import { DLQStore, NewDLQEntry } from '../../dlq/dlq.store.js';
import { ElasticsearchSink, BulkUpsertResult } from '../../sinks/elasticsearch/elasticsearch.sink.js';
import { RabbitMQSink, BatchPublishResult } from '../../sinks/rabbitmq/rabbitmq.sink.js';
import { CircuitBreaker } from '../../resilience/circuit-breaker.js';

function createMockRecords(startId: number, count: number): SourceRecord[] {
  const records: SourceRecord[] = [];
  const now = new Date().toISOString();
  for (let i = 0; i < count; i++) {
    const id = startId + i;
    const payload: CustomerPayload = {
      customer_id: `cust-${id}`,
      first_name: `First${id}`,
      last_name: `Last${id}`,
      email: `user${id}@test.com`,
      account_tier: 'STANDARD',
      balance: 100 + id,
      metadata: {
        signup_channel: 'WEB',
        country: 'US',
        tags: ['test']
      }
    };
    records.push({
      id,
      uuid: `uuid-${id}`,
      tenant_id: 'tenant_test',
      payload,
      version: 1,
      status: 'ACTIVE',
      is_corrupted: false,
      created_at: now,
      updated_at: now
    });
  }
  return records;
}

describe('BackfillRunner - Resilient Keyset Replication & Fault Gates', () => {
  it('1. Gate 1 Resumption: Initializes from existing checkpoint, seeks id > last_processed_id, commits watermark post-ACK', async () => {
    const recordedSeeks: { lastId: number; limit?: number }[] = [];
    const committedCheckpoints: CheckpointCommitData[] = [];
    const statusUpdates: { pipelineId: string; status: PipelineStatus }[] = [];

    // Mock Checkpoint: Existing progress at record 412,000
    const mockCheckpoint: ReplicationCheckpoint = {
      pipeline_id: 'backfill_pipeline',
      last_processed_id: 412000,
      last_processed_timestamp: null,
      status: 'INITIALIZED',
      records_processed: 412000,
      records_failed: 0,
      metadata: {},
      updated_at: new Date().toISOString()
    };

    const mockCheckpointManager = {
      getCheckpoint: async () => mockCheckpoint,
      updateStatus: async (pipelineId: string, status: PipelineStatus) => {
        statusUpdates.push({ pipelineId, status });
      },
      commitCheckpoint: async (_pipelineId: string, data: CheckpointCommitData) => {
        committedCheckpoints.push(data);
      }
    } as unknown as CheckpointManager;

    // Mock SourceReader: Batch 1 returns 500 records (412001 - 412500); Batch 2 returns 0 (EOF)
    let batchCallCount = 0;
    const mockSourceReader = {
      fetchBackfillBatch: async (lastId: number, limit?: number) => {
        recordedSeeks.push({ lastId, limit });
        batchCallCount++;
        if (batchCallCount === 1) {
          return createMockRecords(412001, 500);
        }
        return [];
      }
    } as unknown as SourceReader;

    const mockDLQStore = {
      persistFailures: async () => []
    } as unknown as DLQStore;

    let esUpsertCalled = false;
    const mockESSink = {
      bulkUpsert: async (records: SourceRecord[]): Promise<BulkUpsertResult> => {
        esUpsertCalled = true;
        return {
          successCount: records.length,
          failedCount: 0,
          successfulIds: records.map(r => r.id),
          failures: []
        };
      }
    } as unknown as ElasticsearchSink;

    let rmqPublishCalled = false;
    const mockRMQSink = {
      publishBatch: async (records: SourceRecord[]): Promise<BatchPublishResult> => {
        rmqPublishCalled = true;
        return {
          successCount: records.length,
          failedCount: 0,
          successfulIds: records.map(r => r.id),
          failures: []
        };
      }
    } as unknown as RabbitMQSink;

    const esBreaker = new CircuitBreaker({ name: 'es_test' });
    const rmqBreaker = new CircuitBreaker({ name: 'rmq_test' });

    const runner = new BackfillRunner(
      mockSourceReader,
      mockCheckpointManager,
      mockDLQStore,
      mockESSink,
      mockRMQSink,
      esBreaker,
      rmqBreaker,
      { batchSize: 500, pipelineId: 'backfill_pipeline' }
    );

    await runner.start();

    // Verification of Gate 1 Resumption Invariants
    assert.strictEqual(recordedSeeks.length, 2);
    assert.strictEqual(recordedSeeks[0].lastId, 412000, 'Must resume strictly from 412,000, not 0');
    assert.strictEqual(recordedSeeks[1].lastId, 412500, 'Next batch must seek after 412,500');

    assert.strictEqual(esUpsertCalled, true);
    assert.strictEqual(rmqPublishCalled, true);

    assert.strictEqual(committedCheckpoints.length, 1);
    assert.strictEqual(committedCheckpoints[0].lastProcessedId, 412500);
    assert.strictEqual(committedCheckpoints[0].batchSuccessCount, 500);
    assert.strictEqual(committedCheckpoints[0].batchFailedCount, 0);

    const metrics = runner.getMetrics();
    assert.strictEqual(metrics.status, 'COMPLETED');
    assert.strictEqual(metrics.lastProcessedId, 412500);
    assert.strictEqual(metrics.totalProcessed, 412500); // 412000 initial + 500
    assert.strictEqual(metrics.totalFailed, 0);
  });

  it('2. Gate 4 Partial Batch Isolation: Rejection of 3/500 records isolates 3 to DLQ, writes 497, and commits without rollback', async () => {
    const committedCheckpoints: CheckpointCommitData[] = [];
    const persistedDLQEntries: NewDLQEntry[][] = [];

    const mockCheckpoint: ReplicationCheckpoint = {
      pipeline_id: 'backfill_pipeline',
      last_processed_id: 0,
      last_processed_timestamp: null,
      status: 'INITIALIZED',
      records_processed: 0,
      records_failed: 0,
      metadata: {},
      updated_at: new Date().toISOString()
    };

    const mockCheckpointManager = {
      getCheckpoint: async () => mockCheckpoint,
      updateStatus: async () => {},
      commitCheckpoint: async (_pipelineId: string, data: CheckpointCommitData) => {
        committedCheckpoints.push(data);
      }
    } as unknown as CheckpointManager;

    let fetchCount = 0;
    const mockSourceReader = {
      fetchBackfillBatch: async () => {
        fetchCount++;
        if (fetchCount === 1) {
          return createMockRecords(1, 500);
        }
        return [];
      }
    } as unknown as SourceReader;

    const mockDLQStore = {
      persistFailures: async (entries: NewDLQEntry[]) => {
        persistedDLQEntries.push(entries);
        return entries.map((_, i) => i + 1);
      }
    } as unknown as DLQStore;

    // Simulate Elasticsearch rejecting 3 specific records (record 10, 25, 100) due to schema parse errors
    const mockESSink = {
      bulkUpsert: async (records: SourceRecord[]): Promise<BulkUpsertResult> => {
        const failedIds = new Set([10, 25, 100]);
        const failures = records
          .filter(r => failedIds.has(r.id))
          .map(r => ({
            recordId: r.id,
            errorReason: 'mapper_parsing_exception: failed to parse field',
            errorCode: 'ES_PARSE_ERROR',
            rawItem: r.payload
          }));

        return {
          successCount: records.length - failures.length,
          failedCount: failures.length,
          successfulIds: records.filter(r => !failedIds.has(r.id)).map(r => r.id),
          failures
        };
      }
    } as unknown as ElasticsearchSink;

    // RabbitMQ successfully publishes all 500 records
    const mockRMQSink = {
      publishBatch: async (records: SourceRecord[]): Promise<BatchPublishResult> => {
        return {
          successCount: records.length,
          failedCount: 0,
          successfulIds: records.map(r => r.id),
          failures: []
        };
      }
    } as unknown as RabbitMQSink;

    const esBreaker = new CircuitBreaker({ name: 'es_breaker' });
    const rmqBreaker = new CircuitBreaker({ name: 'rmq_breaker' });

    const runner = new BackfillRunner(
      mockSourceReader,
      mockCheckpointManager,
      mockDLQStore,
      mockESSink,
      mockRMQSink,
      esBreaker,
      rmqBreaker,
      { batchSize: 500 }
    );

    await runner.start();

    // Gate 4 Isolation Assertions:
    // 1. DLQ store persisted exactly 3 poison pills
    assert.strictEqual(persistedDLQEntries.length, 1);
    assert.strictEqual(persistedDLQEntries[0].length, 3);
    assert.deepStrictEqual(
      persistedDLQEntries[0].map(e => e.recordId).sort((a, b) => (a! - b!)),
      [10, 25, 100]
    );
    assert.strictEqual(persistedDLQEntries[0][0].sinkTarget, 'ELASTICSEARCH');
    assert.strictEqual(persistedDLQEntries[0][0].errorCode, 'ES_PARSE_ERROR');

    // 2. Checkpoint committed successfully without rolling back the batch
    assert.strictEqual(committedCheckpoints.length, 1);
    assert.strictEqual(committedCheckpoints[0].lastProcessedId, 500);
    assert.strictEqual(committedCheckpoints[0].batchSuccessCount, 497);
    assert.strictEqual(committedCheckpoints[0].batchFailedCount, 3);

    // 3. Runner metrics reflect accurate processed and failed counts
    const metrics = runner.getMetrics();
    assert.strictEqual(metrics.totalProcessed, 497);
    assert.strictEqual(metrics.totalFailed, 3);
    assert.strictEqual(metrics.lastProcessedId, 500);
  });

  it('3. Gate 3 Circuit Breaker Integration: Dispatches are executed via CircuitBreakers', async () => {
    let esBreakerExecuted = false;
    let rmqBreakerExecuted = false;

    const mockCheckpoint: ReplicationCheckpoint = {
      pipeline_id: 'backfill_pipeline',
      last_processed_id: 0,
      last_processed_timestamp: null,
      status: 'INITIALIZED',
      records_processed: 0,
      records_failed: 0,
      metadata: {},
      updated_at: new Date().toISOString()
    };

    const mockCheckpointManager = {
      getCheckpoint: async () => mockCheckpoint,
      updateStatus: async () => {},
      commitCheckpoint: async () => {}
    } as unknown as CheckpointManager;

    let fetchCount = 0;
    const mockSourceReader = {
      fetchBackfillBatch: async () => {
        fetchCount++;
        return fetchCount === 1 ? createMockRecords(1, 10) : [];
      }
    } as unknown as SourceReader;

    const mockDLQStore = {
      persistFailures: async () => []
    } as unknown as DLQStore;

    const mockESSink = {
      bulkUpsert: async (records: SourceRecord[]) => ({
        successCount: records.length,
        failedCount: 0,
        successfulIds: records.map(r => r.id),
        failures: []
      })
    } as unknown as ElasticsearchSink;

    const mockRMQSink = {
      publishBatch: async (records: SourceRecord[]) => ({
        successCount: records.length,
        failedCount: 0,
        successfulIds: records.map(r => r.id),
        failures: []
      })
    } as unknown as RabbitMQSink;

    // Custom circuit breaker instances tracking execution
    const esBreaker = new CircuitBreaker({ name: 'es_spy' });
    const originalEsExecute = esBreaker.execute.bind(esBreaker);
    esBreaker.execute = async (op) => {
      esBreakerExecuted = true;
      return originalEsExecute(op);
    };

    const rmqBreaker = new CircuitBreaker({ name: 'rmq_spy' });
    const originalRmqExecute = rmqBreaker.execute.bind(rmqBreaker);
    rmqBreaker.execute = async (op) => {
      rmqBreakerExecuted = true;
      return originalRmqExecute(op);
    };

    const runner = new BackfillRunner(
      mockSourceReader,
      mockCheckpointManager,
      mockDLQStore,
      mockESSink,
      mockRMQSink,
      esBreaker,
      rmqBreaker,
      { batchSize: 10 }
    );

    await runner.start();

    assert.strictEqual(esBreakerExecuted, true);
    assert.strictEqual(rmqBreakerExecuted, true);
    assert.strictEqual(esBreaker.getState(), 'CLOSED');
    assert.strictEqual(rmqBreaker.getState(), 'CLOSED');
  });

  it('4. Natural Completion: Marks COMPLETED and ceases loop when source returns 0 records', async () => {
    let finalStatus: PipelineStatus | null = null;

    const mockCheckpoint: ReplicationCheckpoint = {
      pipeline_id: 'backfill_pipeline',
      last_processed_id: 100,
      last_processed_timestamp: null,
      status: 'INITIALIZED',
      records_processed: 100,
      records_failed: 0,
      metadata: {},
      updated_at: new Date().toISOString()
    };

    const mockCheckpointManager = {
      getCheckpoint: async () => mockCheckpoint,
      updateStatus: async (_pipelineId: string, status: PipelineStatus) => {
        finalStatus = status;
      },
      commitCheckpoint: async () => {}
    } as unknown as CheckpointManager;

    // Immediately returns 0 records (e.g. table empty or sync already up to date)
    const mockSourceReader = {
      fetchBackfillBatch: async () => []
    } as unknown as SourceReader;

    const mockDLQStore = {
      persistFailures: async () => []
    } as unknown as DLQStore;

    let esCalled = false;
    const mockESSink = {
      bulkUpsert: async () => {
        esCalled = true;
        return { successCount: 0, failedCount: 0, successfulIds: [], failures: [] };
      }
    } as unknown as ElasticsearchSink;

    let rmqCalled = false;
    const mockRMQSink = {
      publishBatch: async () => {
        rmqCalled = true;
        return { successCount: 0, failedCount: 0, successfulIds: [], failures: [] };
      }
    } as unknown as RabbitMQSink;

    const esBreaker = new CircuitBreaker({ name: 'es_b' });
    const rmqBreaker = new CircuitBreaker({ name: 'rmq_b' });

    const runner = new BackfillRunner(
      mockSourceReader,
      mockCheckpointManager,
      mockDLQStore,
      mockESSink,
      mockRMQSink,
      esBreaker,
      rmqBreaker
    );

    await runner.start();

    assert.strictEqual(runner.getMetrics().status, 'COMPLETED');
    assert.strictEqual(finalStatus, 'COMPLETED');
    assert.strictEqual(esCalled, false, 'No sink calls should be made on empty fetch');
    assert.strictEqual(rmqCalled, false);
  });

  it('5. Pause & Resume Lifecycle: stopRequested cleanly pauses the execution loop and resumes', async () => {
    const statuses: PipelineStatus[] = [];
    let currentWatermark = 0;

    const mockCheckpointManager = {
      getCheckpoint: async () => ({
        pipeline_id: 'backfill_pipeline',
        last_processed_id: currentWatermark,
        last_processed_timestamp: null,
        status: 'INITIALIZED' as PipelineStatus,
        records_processed: currentWatermark,
        records_failed: 0,
        metadata: {},
        updated_at: new Date().toISOString()
      }),
      updateStatus: async (_id: string, s: PipelineStatus) => {
        statuses.push(s);
      },
      commitCheckpoint: async (_id: string, data: CheckpointCommitData) => {
        currentWatermark = data.lastProcessedId;
      }
    } as unknown as CheckpointManager;

    let fetchIteration = 0;
    const mockSourceReader = {
      fetchBackfillBatch: async (lastId: number) => {
        fetchIteration++;
        if (fetchIteration === 1) {
          return createMockRecords(1, 100);
        }
        if (fetchIteration === 2) {
          return createMockRecords(lastId + 1, 100);
        }
        return [];
      }
    } as unknown as SourceReader;

    const mockDLQStore = {
      persistFailures: async () => []
    } as unknown as DLQStore;

    let upsertCount = 0;
    const mockESSink = {
      bulkUpsert: async (records: SourceRecord[]) => {
        upsertCount++;
        if (upsertCount === 1) {
          // Trigger pause during batch 1 execution
          await runner.pause();
        }
        return {
          successCount: records.length,
          failedCount: 0,
          successfulIds: records.map(r => r.id),
          failures: []
        };
      }
    } as unknown as ElasticsearchSink;

    const mockRMQSink = {
      publishBatch: async (records: SourceRecord[]) => ({
        successCount: records.length,
        failedCount: 0,
        successfulIds: records.map(r => r.id),
        failures: []
      })
    } as unknown as RabbitMQSink;

    const runner = new BackfillRunner(
      mockSourceReader,
      mockCheckpointManager,
      mockDLQStore,
      mockESSink,
      mockRMQSink,
      new CircuitBreaker({ name: 'es_pause' }),
      new CircuitBreaker({ name: 'rmq_pause' }),
      { batchSize: 100 }
    );

    // Start runner - will process batch 1 and pause before batch 2
    await runner.start();

    assert.strictEqual(runner.getMetrics().status, 'PAUSED');
    assert.strictEqual(runner.getMetrics().lastProcessedId, 100);
    assert(statuses.includes('PAUSED'));

    // Resume execution
    await runner.resume();

    assert.strictEqual(runner.getMetrics().status, 'COMPLETED');
    assert.strictEqual(runner.getMetrics().lastProcessedId, 200);
    assert.strictEqual(runner.getMetrics().totalProcessed, 200);
  });
});
