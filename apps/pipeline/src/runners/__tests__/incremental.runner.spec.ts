/**
 * Unit & Gate Resilience Test Suite for IncrementalRunner
 * Validates Composite Keyset Watermarks, Mutation Ingestion, Millisecond Tie-Breaking,
 * Gate 5 Replication Lag Tracking, and Gate 4 Error Isolation.
 */

import { describe, it } from 'node:test';
import assert from 'node:assert';
import {
  PipelineStatus,
  ReplicationCheckpoint,
  SourceRecord,
  CustomerPayload,
  ReplicationEventType
} from '@optio/shared';
import { IncrementalRunner } from '../incremental.runner.js';
import { SourceReader, IncrementalLag } from '../../source/source.reader.js';
import { CheckpointManager, CheckpointCommitData } from '../../checkpoint/checkpoint.manager.js';
import { DLQStore, NewDLQEntry } from '../../dlq/dlq.store.js';
import { ElasticsearchSink, BulkUpsertResult } from '../../sinks/elasticsearch/elasticsearch.sink.js';
import { RabbitMQSink, BatchPublishResult } from '../../sinks/rabbitmq/rabbitmq.sink.js';
import { CircuitBreaker } from '../../resilience/circuit-breaker.js';

function createMockMutation(id: number, updatedAt: string): SourceRecord {
  const payload: CustomerPayload = {
    customer_id: `cust-${id}`,
    first_name: `First${id}`,
    last_name: `Last${id}`,
    email: `user${id}@test.com`,
    account_tier: 'PREMIUM',
    balance: 500 + id,
    metadata: {
      signup_channel: 'WEB',
      country: 'US',
      tags: ['incremental_test']
    }
  };

  return {
    id,
    uuid: `uuid-${id}`,
    tenant_id: 'tenant_test',
    payload,
    version: 2,
    status: 'ACTIVE',
    is_corrupted: false,
    created_at: '2026-09-01T00:00:00.000Z',
    updated_at: updatedAt
  };
}

describe('IncrementalRunner - Continuous CDC & Composite Watermark Resilience', () => {
  it('1. Composite Keyset Watermark Resumption: Initializes from persisted timestamp and ID watermark', async () => {
    let capturedLagTimestamp: Date | null = null;
    let capturedLagId: number | null = null;
    let capturedFetchTimestamp: Date | null = null;
    let capturedFetchId: number | null = null;

    const initialTimestampStr = '2026-09-16T10:00:00.000Z';
    const initialId = 15420;

    const mockCheckpoint: ReplicationCheckpoint = {
      pipeline_id: 'incremental_pipeline',
      last_processed_id: initialId,
      last_processed_timestamp: initialTimestampStr,
      status: 'INITIALIZED',
      records_processed: 5000,
      records_failed: 2,
      metadata: {},
      updated_at: new Date().toISOString()
    };

    const mockCheckpointManager = {
      getCheckpoint: async () => mockCheckpoint,
      updateStatus: async () => {},
      commitCheckpoint: async () => {}
    } as unknown as CheckpointManager;

    let runner: IncrementalRunner;

    const mockSourceReader = {
      getIncrementalLag: async (ts: Date, id: number): Promise<IncrementalLag> => {
        capturedLagTimestamp = ts;
        capturedLagId = id;
        return { lagRecords: 0, lagMs: 0 };
      },
      fetchIncrementalBatch: async (ts: Date, id: number): Promise<SourceRecord[]> => {
        capturedFetchTimestamp = ts;
        capturedFetchId = id;
        // Pause after first poll check to exit test cleanly
        await runner.pause();
        return [];
      }
    } as unknown as SourceReader;

    const mockDLQStore = { persistFailures: async () => [] } as unknown as DLQStore;
    const mockESSink = { bulkUpsert: async () => ({ successCount: 0, failedCount: 0, successfulIds: [], failures: [] }) } as unknown as ElasticsearchSink;
    const mockRMQSink = { publishBatch: async () => ({ successCount: 0, failedCount: 0, successfulIds: [], failures: [] }) } as unknown as RabbitMQSink;

    runner = new IncrementalRunner(
      mockSourceReader,
      mockCheckpointManager,
      mockDLQStore,
      mockESSink,
      mockRMQSink,
      new CircuitBreaker({ name: 'es_test' }),
      new CircuitBreaker({ name: 'rmq_test' }),
      { pollIntervalMs: 1 }
    );

    await runner.start();

    // Verify composite seeking parameters
    assert.strictEqual((capturedLagTimestamp as unknown as Date)?.toISOString(), initialTimestampStr);
    assert.strictEqual(capturedLagId, initialId);
    assert.strictEqual((capturedFetchTimestamp as unknown as Date)?.toISOString(), initialTimestampStr);
    assert.strictEqual(capturedFetchId, initialId);

    const metrics = runner.getMetrics();
    assert.strictEqual(metrics.lastProcessedId, initialId);
    assert.strictEqual(metrics.lastProcessedTimestamp.toISOString(), initialTimestampStr);
    assert.strictEqual(metrics.totalMutationsProcessed, 5000);
  });

  it('2. Mutation Ingestion: Dispatches RECORD_MUTATED to RMQ, bulk upserts to ES, and commits composite watermark', async () => {
    const committedCheckpoints: CheckpointCommitData[] = [];
    let rmqEventType: ReplicationEventType | null = null;
    let esRecordsCount = 0;
    let rmqRecordsCount = 0;

    const mockCheckpoint: ReplicationCheckpoint = {
      pipeline_id: 'incremental_pipeline',
      last_processed_id: 100,
      last_processed_timestamp: '2026-09-16T10:00:00.000Z',
      status: 'INITIALIZED',
      records_processed: 0,
      records_failed: 0,
      metadata: {},
      updated_at: new Date().toISOString()
    };

    const mockCheckpointManager = {
      getCheckpoint: async () => mockCheckpoint,
      updateStatus: async () => {},
      commitCheckpoint: async (_id: string, data: CheckpointCommitData) => {
        committedCheckpoints.push(data);
      }
    } as unknown as CheckpointManager;

    let runner: IncrementalRunner;
    let fetchIteration = 0;

    const mutations = [
      createMockMutation(101, '2026-09-16T10:01:00.000Z'),
      createMockMutation(102, '2026-09-16T10:02:00.000Z'),
      createMockMutation(103, '2026-09-16T10:03:00.000Z')
    ];

    const mockSourceReader = {
      getIncrementalLag: async () => ({ lagRecords: 3, lagMs: 1500 }),
      fetchIncrementalBatch: async () => {
        fetchIteration++;
        if (fetchIteration === 1) {
          return mutations;
        }
        await runner.pause();
        return [];
      }
    } as unknown as SourceReader;

    const mockDLQStore = { persistFailures: async () => [] } as unknown as DLQStore;

    const mockESSink = {
      bulkUpsert: async (records: SourceRecord[]): Promise<BulkUpsertResult> => {
        esRecordsCount = records.length;
        return {
          successCount: records.length,
          failedCount: 0,
          successfulIds: records.map(r => r.id),
          failures: []
        };
      }
    } as unknown as ElasticsearchSink;

    const mockRMQSink = {
      publishBatch: async (records: SourceRecord[], eventType: ReplicationEventType): Promise<BatchPublishResult> => {
        rmqRecordsCount = records.length;
        rmqEventType = eventType;
        return {
          successCount: records.length,
          failedCount: 0,
          successfulIds: records.map(r => r.id),
          failures: []
        };
      }
    } as unknown as RabbitMQSink;

    runner = new IncrementalRunner(
      mockSourceReader,
      mockCheckpointManager,
      mockDLQStore,
      mockESSink,
      mockRMQSink,
      new CircuitBreaker({ name: 'es_test' }),
      new CircuitBreaker({ name: 'rmq_test' }),
      { batchSize: 500, pollIntervalMs: 1 }
    );

    await runner.start();

    // Verify dual-sink dispatch
    assert.strictEqual(esRecordsCount, 3);
    assert.strictEqual(rmqRecordsCount, 3);
    assert.strictEqual(rmqEventType, 'RECORD_MUTATED');

    // Verify post-ACK atomic checkpoint commit
    assert.strictEqual(committedCheckpoints.length, 1);
    assert.strictEqual(committedCheckpoints[0].lastProcessedId, 103);
    assert.strictEqual(
      committedCheckpoints[0].lastProcessedTimestamp?.toISOString(),
      '2026-09-16T10:03:00.000Z'
    );
    assert.strictEqual(committedCheckpoints[0].batchSuccessCount, 3);
    assert.strictEqual(committedCheckpoints[0].batchFailedCount, 0);

    const metrics = runner.getMetrics();
    assert.strictEqual(metrics.totalMutationsProcessed, 3);
    assert.strictEqual(metrics.lastProcessedId, 103);
    assert.strictEqual(metrics.lastProcessedTimestamp.toISOString(), '2026-09-16T10:03:00.000Z');
  });

  it('3. Millisecond Tie-Breaking: Handles records with identical timestamps using ID tie-breaker', async () => {
    const seekQueries: { ts: string; id: number }[] = [];
    const sharedTimestamp = '2026-09-16T10:15:00.123Z';

    const mockCheckpoint: ReplicationCheckpoint = {
      pipeline_id: 'incremental_pipeline',
      last_processed_id: 50,
      last_processed_timestamp: sharedTimestamp,
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

    let runner: IncrementalRunner;
    let pollCount = 0;

    const mockSourceReader = {
      getIncrementalLag: async () => ({ lagRecords: 1, lagMs: 100 }),
      fetchIncrementalBatch: async (ts: Date, id: number) => {
        seekQueries.push({ ts: ts.toISOString(), id });
        pollCount++;
        if (pollCount === 1) {
          // Returns mutation sharing the exact same millisecond timestamp but id > 50
          return [createMockMutation(51, sharedTimestamp)];
        }
        await runner.pause();
        return [];
      }
    } as unknown as SourceReader;

    const mockDLQStore = { persistFailures: async () => [] } as unknown as DLQStore;
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

    runner = new IncrementalRunner(
      mockSourceReader,
      mockCheckpointManager,
      mockDLQStore,
      mockESSink,
      mockRMQSink,
      new CircuitBreaker({ name: 'es_test' }),
      new CircuitBreaker({ name: 'rmq_test' }),
      { batchSize: 500, pollIntervalMs: 1 }
    );

    await runner.start();

    // Verify seek query preserved the millisecond timestamp and advanced ID
    assert.strictEqual(seekQueries[0].ts, sharedTimestamp);
    assert.strictEqual(seekQueries[0].id, 50);

    // Next seek must query for (ts = sharedTimestamp AND id > 51)
    assert.strictEqual(seekQueries[1].ts, sharedTimestamp);
    assert.strictEqual(seekQueries[1].id, 51);
  });

  it('4. Gate 5 Lag Tracking: Continuously tracks and exposes lagRecords and lagMs', async () => {
    let capturedMetadata: Record<string, unknown> | undefined;

    const mockCheckpoint: ReplicationCheckpoint = {
      pipeline_id: 'incremental_pipeline',
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
      commitCheckpoint: async (_id: string, data: CheckpointCommitData) => {
        capturedMetadata = data.metadata;
      }
    } as unknown as CheckpointManager;

    let runner: IncrementalRunner;
    let pollCount = 0;

    const mockSourceReader = {
      getIncrementalLag: async () => ({
        lagRecords: 487,
        lagMs: 12540
      }),
      fetchIncrementalBatch: async () => {
        pollCount++;
        if (pollCount === 1) {
          return [createMockMutation(1, '2026-09-16T10:30:00.000Z')];
        }
        await runner.pause();
        return [];
      }
    } as unknown as SourceReader;

    const mockDLQStore = { persistFailures: async () => [] } as unknown as DLQStore;
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

    runner = new IncrementalRunner(
      mockSourceReader,
      mockCheckpointManager,
      mockDLQStore,
      mockESSink,
      mockRMQSink,
      new CircuitBreaker({ name: 'es_test' }),
      new CircuitBreaker({ name: 'rmq_test' }),
      { pollIntervalMs: 1 }
    );

    await runner.start();

    // Verify Gate 5 metrics exposed via runner
    const metrics = runner.getMetrics();
    assert.strictEqual(metrics.lagRecords, 487);
    assert.strictEqual(metrics.lagMs, 12540);

    // Verify Gate 5 metrics committed to persistent checkpoint metadata
    assert.strictEqual(capturedMetadata?.lagRecords, 487);
    assert.strictEqual(capturedMetadata?.lagMs, 12540);
  });

  it('5. Gate 4 Error Isolation: Isolates rejected mutations into DLQ without aborting polling loop', async () => {
    const dlqEntriesPersisted: NewDLQEntry[][] = [];
    const committedCheckpoints: CheckpointCommitData[] = [];

    const mockCheckpoint: ReplicationCheckpoint = {
      pipeline_id: 'incremental_pipeline',
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
      commitCheckpoint: async (_id: string, data: CheckpointCommitData) => {
        committedCheckpoints.push(data);
      }
    } as unknown as CheckpointManager;

    let runner: IncrementalRunner;
    let pollCount = 0;

    const mockSourceReader = {
      getIncrementalLag: async () => ({ lagRecords: 0, lagMs: 0 }),
      fetchIncrementalBatch: async () => {
        pollCount++;
        if (pollCount === 1) {
          return [
            createMockMutation(201, '2026-09-16T11:00:00.000Z'),
            createMockMutation(202, '2026-09-16T11:00:01.000Z'), // Poison pill in ES
            createMockMutation(203, '2026-09-16T11:00:02.000Z')
          ];
        }
        await runner.pause();
        return [];
      }
    } as unknown as SourceReader;

    const mockDLQStore = {
      persistFailures: async (entries: NewDLQEntry[]) => {
        dlqEntriesPersisted.push(entries);
        return [1];
      }
    } as unknown as DLQStore;

    // Simulate ES rejecting mutation 202
    const mockESSink = {
      bulkUpsert: async (records: SourceRecord[]): Promise<BulkUpsertResult> => ({
        successCount: 2,
        failedCount: 1,
        successfulIds: [201, 203],
        failures: [
          {
            recordId: 202,
            errorReason: 'strict_dynamic_mapping_exception',
            errorCode: 'ES_MAPPING_ERROR',
            rawItem: records[1].payload
          }
        ]
      })
    } as unknown as ElasticsearchSink;

    const mockRMQSink = {
      publishBatch: async (records: SourceRecord[]): Promise<BatchPublishResult> => ({
        successCount: records.length,
        failedCount: 0,
        successfulIds: records.map(r => r.id),
        failures: []
      })
    } as unknown as RabbitMQSink;

    runner = new IncrementalRunner(
      mockSourceReader,
      mockCheckpointManager,
      mockDLQStore,
      mockESSink,
      mockRMQSink,
      new CircuitBreaker({ name: 'es_test' }),
      new CircuitBreaker({ name: 'rmq_test' }),
      { pollIntervalMs: 1 }
    );

    await runner.start();

    // Verify DLQ isolation
    assert.strictEqual(dlqEntriesPersisted.length, 1);
    assert.strictEqual(dlqEntriesPersisted[0].length, 1);
    assert.strictEqual(dlqEntriesPersisted[0][0].recordId, 202);
    assert.strictEqual(dlqEntriesPersisted[0][0].sinkTarget, 'ELASTICSEARCH');

    // Verify partial batch commit without rollback
    assert.strictEqual(committedCheckpoints.length, 1);
    assert.strictEqual(committedCheckpoints[0].lastProcessedId, 203);
    assert.strictEqual(committedCheckpoints[0].batchSuccessCount, 2);
    assert.strictEqual(committedCheckpoints[0].batchFailedCount, 1);

    const metrics = runner.getMetrics();
    assert.strictEqual(metrics.totalMutationsProcessed, 2);
    assert.strictEqual(metrics.totalMutationsFailed, 1);
    assert.strictEqual(metrics.status, 'PAUSED'); // Loop remained healthy and paused cleanly
  });

  it('6. Clean Pause & Resume Lifecycle: Cancels active sleep and resumes seamlessly', async () => {
    const statuses: PipelineStatus[] = [];

    const mockCheckpointManager = {
      getCheckpoint: async () => ({
        pipeline_id: 'incremental_pipeline',
        last_processed_id: 50,
        last_processed_timestamp: '2026-09-16T10:00:00.000Z',
        status: 'INITIALIZED' as PipelineStatus,
        records_processed: 50,
        records_failed: 0,
        metadata: {},
        updated_at: new Date().toISOString()
      }),
      updateStatus: async (_id: string, s: PipelineStatus) => {
        statuses.push(s);
      },
      commitCheckpoint: async () => {}
    } as unknown as CheckpointManager;

    let pollCount = 0;
    const mockSourceReader = {
      getIncrementalLag: async () => ({ lagRecords: 0, lagMs: 0 }),
      fetchIncrementalBatch: async () => {
        pollCount++;
        if (pollCount === 1) {
          return [createMockMutation(51, '2026-09-16T10:01:00.000Z')];
        }
        return [];
      }
    } as unknown as SourceReader;

    const mockDLQStore = { persistFailures: async () => [] } as unknown as DLQStore;
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

    const runner = new IncrementalRunner(
      mockSourceReader,
      mockCheckpointManager,
      mockDLQStore,
      mockESSink,
      mockRMQSink,
      new CircuitBreaker({ name: 'es_test' }),
      new CircuitBreaker({ name: 'rmq_test' }),
      { pollIntervalMs: 5000 } // Long poll interval
    );

    // Start in background
    const startPromise = runner.start();

    // Allow first poll to complete and enter sleep
    await new Promise(r => setTimeout(r, 50));

    // Pause while sleeping on the 5000ms timer
    const pauseStart = Date.now();
    await runner.pause();
    await startPromise;
    const pauseDuration = Date.now() - pauseStart;

    // Verify pause cancelled the 5000ms timer promptly (< 500ms)
    assert(pauseDuration < 1000, `Pause took ${pauseDuration}ms, must interrupt immediately`);
    assert.strictEqual(runner.getMetrics().status, 'PAUSED');
    assert(statuses.includes('PAUSED'));

    // Resume execution
    const resumePromise = runner.resume();
    await new Promise(r => setTimeout(r, 50));
    await runner.pause();
    await resumePromise;

    assert.strictEqual(runner.getMetrics().status, 'PAUSED');
  });
});
