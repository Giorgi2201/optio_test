/**
 * Unit & Concurrency Test Suite for PipelineCoordinator & HTTP Server
 * Validates simultaneous dual-mode execution, Gate 5 aggregated telemetry,
 * dynamic runner control APIs, and graceful teardown.
 */

import { describe, it } from 'node:test';
import assert from 'node:assert';
import http from 'http';
import { Pool } from 'pg';
import { Client as ESClient } from '@elastic/elasticsearch';
import {
  DLQEntry,
  PipelineStatus,
  PipelineTelemetry
} from '@optio/shared';
import { PipelineCoordinator } from '../pipeline.coordinator.js';
import { BackfillRunner, BackfillMetrics } from '../../runners/backfill.runner.js';
import { IncrementalRunner, IncrementalMetrics } from '../../runners/incremental.runner.js';
import { CheckpointManager } from '../../checkpoint/checkpoint.manager.js';
import { DLQStore } from '../../dlq/dlq.store.js';
import { ElasticsearchSink, ElasticsearchHealthResult } from '../../sinks/elasticsearch/elasticsearch.sink.js';
import { RabbitMQSink, RabbitMQHealthResult } from '../../sinks/rabbitmq/rabbitmq.sink.js';
import { SourceReader, SourceMetadata } from '../../source/source.reader.js';
import { CircuitBreaker } from '../../resilience/circuit-breaker.js';
import { createPipelineServer } from '../../server.js';

describe('PipelineCoordinator - Dual-Mode Concurrency & Aggregated Observability', () => {
  it('1. Concurrent Launch: Starts both Backfill and Incremental runners simultaneously without blocking', async () => {
    let backfillStarted = false;
    let incrementalStarted = false;
    let rmqConnected = false;

    // Simulate runners that stay running (long-running promise)
    const mockBackfillRunner = {
      start: async () => {
        backfillStarted = true;
        // Never resolves immediately to verify it doesn't block coordinator.start()
        await new Promise(() => {});
      }
    } as unknown as BackfillRunner;

    const mockIncrementalRunner = {
      start: async () => {
        incrementalStarted = true;
        await new Promise(() => {});
      }
    } as unknown as IncrementalRunner;

    const mockESSink = {
      getClient: () => ({
        indices: {
          exists: async () => true
        }
      }) as unknown as ESClient,
      getIndexName: () => 'records_search_index',
      healthCheck: async () => ({ healthy: true, latencyMs: 2 })
    } as unknown as ElasticsearchSink;

    const mockRMQSink = {
      connect: async () => {
        rmqConnected = true;
      },
      healthCheck: async () => ({ healthy: true, latencyMs: 3, connectionState: 'CONNECTED' })
    } as unknown as RabbitMQSink;

    const mockPgPool = {
      query: async () => ({ rows: [] }),
      end: async () => {}
    } as unknown as Pool;

    const coordinator = new PipelineCoordinator(
      mockBackfillRunner,
      mockIncrementalRunner,
      {} as CheckpointManager,
      {} as DLQStore,
      mockESSink,
      mockRMQSink,
      {} as SourceReader,
      new CircuitBreaker({ name: 'es' }),
      new CircuitBreaker({ name: 'rmq' }),
      mockPgPool
    );

    // coordinator.start() should return promptly even though runner.start() loops are infinite
    const startStart = Date.now();
    await coordinator.start();
    const duration = Date.now() - startStart;

    assert(duration < 500, `coordinator.start() must return without blocking (took ${duration}ms)`);
    assert.strictEqual(rmqConnected, true, 'RabbitMQ sink must be connected');
    assert.strictEqual(backfillStarted, true, 'Backfill runner must be started concurrently');
    assert.strictEqual(incrementalStarted, true, 'Incremental runner must be started concurrently');
    assert.strictEqual(coordinator.getStatus(), 'RUNNING');
  });

  it('2. Telemetry Aggregation: Fulfills Gate 5 by aggregating progress, lag, DLQ, and health', async () => {
    const mockBackfillMetrics: BackfillMetrics = {
      pipelineId: 'backfill_pipeline',
      status: 'RUNNING',
      lastProcessedId: 250000,
      totalProcessed: 250000,
      totalFailed: 5,
      currentThroughputEps: 8500,
      isRunning: true
    };

    const mockIncrementalMetrics: IncrementalMetrics = {
      pipelineId: 'incremental_pipeline',
      status: 'PAUSED',
      lastProcessedTimestamp: new Date('2026-09-16T10:10:00.000Z'),
      lastProcessedId: 1000,
      totalMutationsProcessed: 320,
      totalMutationsFailed: 1,
      lagRecords: 14,
      lagMs: 420,
      isRunning: false
    };

    const mockSourceMetadata: SourceMetadata = {
      maxId: 500000,
      totalCount: 500000
    };

    const mockBackfillRunner = {
      getMetrics: () => mockBackfillMetrics
    } as unknown as BackfillRunner;

    const mockIncrementalRunner = {
      getMetrics: () => mockIncrementalMetrics
    } as unknown as IncrementalRunner;

    const mockDLQStore = {
      getPendingCount: async () => 6
    } as unknown as DLQStore;

    const mockSourceReader = {
      getSourceMetadata: async () => mockSourceMetadata
    } as unknown as SourceReader;

    const mockESSink = {
      healthCheck: async (): Promise<ElasticsearchHealthResult> => ({
        healthy: true,
        latencyMs: 12,
        clusterStatus: 'green'
      })
    } as unknown as ElasticsearchSink;

    const mockRMQSink = {
      healthCheck: async (): Promise<RabbitMQHealthResult> => ({
        healthy: true,
        latencyMs: 8,
        connectionState: 'OPEN'
      })
    } as unknown as RabbitMQSink;

    const mockPgPool = {
      query: async () => ({ rows: [{ '?column?': 1 }] })
    } as unknown as Pool;

    const esBreaker = new CircuitBreaker({ name: 'es_b' });
    const rmqBreaker = new CircuitBreaker({ name: 'rmq_b' });

    const coordinator = new PipelineCoordinator(
      mockBackfillRunner,
      mockIncrementalRunner,
      {} as CheckpointManager,
      mockDLQStore,
      mockESSink,
      mockRMQSink,
      mockSourceReader,
      esBreaker,
      rmqBreaker,
      mockPgPool
    );

    const telemetry: PipelineTelemetry = await coordinator.getTelemetry();

    // Verification of Gate 5 Telemetry Invariants
    // Runner statuses must be reported independently and verbatim from each runner's metrics.
    const expectedBackfillStatus: PipelineStatus = 'RUNNING';
    const expectedIncrementalStatus: PipelineStatus = 'PAUSED';
    assert.strictEqual(telemetry.backfill_status, expectedBackfillStatus);
    assert.strictEqual(telemetry.incremental_status, expectedIncrementalStatus);

    assert.strictEqual(telemetry.backfill_cursor, 250000);
    assert.strictEqual(telemetry.backfill_total_records, 500000);
    assert.strictEqual(telemetry.backfill_completion_pct, 50); // 250000 / 500000 * 100
    assert.strictEqual(telemetry.current_throughput_eps, 8500);
    assert.strictEqual(telemetry.incremental_lag_records, 14);
    assert.strictEqual(telemetry.incremental_lag_ms, 420);
    assert.strictEqual(telemetry.dlq_pending_count, 6);

    assert.strictEqual(telemetry.health.overall, 'HEALTHY');
    assert.strictEqual(telemetry.health.postgres.status, 'UP');
    assert.strictEqual(telemetry.health.elasticsearch.status, 'UP');
    assert.strictEqual(telemetry.health.rabbitmq.status, 'UP');
    assert.strictEqual(telemetry.circuit_breakers?.elasticsearch.state, 'CLOSED');
    assert.strictEqual(telemetry.circuit_breakers?.rabbitmq.state, 'CLOSED');
  });

  it('3. Pause & Resume Controls: Invokes respective runner controls dynamically', async () => {
    let bfPaused = false;
    let bfResumed = false;
    let incPaused = false;
    let incResumed = false;

    const mockBackfillRunner = {
      pause: async () => { bfPaused = true; },
      resume: async () => { bfResumed = true; }
    } as unknown as BackfillRunner;

    const mockIncrementalRunner = {
      pause: async () => { incPaused = true; },
      resume: async () => { incResumed = true; }
    } as unknown as IncrementalRunner;

    const coordinator = new PipelineCoordinator(
      mockBackfillRunner,
      mockIncrementalRunner,
      {} as CheckpointManager,
      {} as DLQStore,
      {} as ElasticsearchSink,
      {} as RabbitMQSink,
      {} as SourceReader,
      new CircuitBreaker({ name: 'es' }),
      new CircuitBreaker({ name: 'rmq' }),
      {} as Pool
    );

    await coordinator.pauseBackfill();
    assert.strictEqual(bfPaused, true);

    await coordinator.resumeBackfill();
    assert.strictEqual(bfResumed, true);

    await coordinator.pauseIncremental();
    assert.strictEqual(incPaused, true);

    await coordinator.resumeIncremental();
    assert.strictEqual(incResumed, true);
  });

  it('4. Teardown: stop() gracefully pauses runners and terminates sink and pool connections', async () => {
    let bfPaused = false;
    let incPaused = false;
    let rmqClosed = false;
    let esClosed = false;
    let pgEnded = false;

    const mockBackfillRunner = {
      pause: async () => { bfPaused = true; }
    } as unknown as BackfillRunner;

    const mockIncrementalRunner = {
      pause: async () => { incPaused = true; }
    } as unknown as IncrementalRunner;

    const mockESSink = {
      close: async () => { esClosed = true; }
    } as unknown as ElasticsearchSink;

    const mockRMQSink = {
      close: async () => { rmqClosed = true; }
    } as unknown as RabbitMQSink;

    const mockPgPool = {
      end: async () => { pgEnded = true; }
    } as unknown as Pool;

    const coordinator = new PipelineCoordinator(
      mockBackfillRunner,
      mockIncrementalRunner,
      {} as CheckpointManager,
      {} as DLQStore,
      mockESSink,
      mockRMQSink,
      {} as SourceReader,
      new CircuitBreaker({ name: 'es' }),
      new CircuitBreaker({ name: 'rmq' }),
      mockPgPool
    );

    await coordinator.stop();

    assert.strictEqual(bfPaused, true);
    assert.strictEqual(incPaused, true);
    assert.strictEqual(rmqClosed, true);
    assert.strictEqual(esClosed, true);
    assert.strictEqual(pgEnded, true);
    assert.strictEqual(coordinator.getStatus(), 'STOPPED');
  });

  it('5. HTTP Observability & Control API: Endpoints respond with valid telemetry and trigger actions', async () => {
    let bfPaused = false;
    let incPaused = false;

    const mockBackfillRunner = {
      getMetrics: (): BackfillMetrics => ({
        pipelineId: 'backfill_pipeline',
        status: 'RUNNING',
        lastProcessedId: 100,
        totalProcessed: 100,
        totalFailed: 0,
        currentThroughputEps: 500,
        isRunning: true
      }),
      pause: async () => { bfPaused = true; },
      resume: async () => {}
    } as unknown as BackfillRunner;

    const mockIncrementalRunner = {
      getMetrics: (): IncrementalMetrics => ({
        pipelineId: 'incremental_pipeline',
        status: 'RUNNING',
        lastProcessedTimestamp: new Date(),
        lastProcessedId: 100,
        totalMutationsProcessed: 10,
        totalMutationsFailed: 0,
        lagRecords: 0,
        lagMs: 0,
        isRunning: true
      }),
      pause: async () => { incPaused = true; },
      resume: async () => {}
    } as unknown as IncrementalRunner;

    const sampleDLQ: DLQEntry[] = [
      {
        id: 1,
        record_id: 42,
        record_uuid: 'uuid-42',
        sink_target: 'ELASTICSEARCH',
        payload: { email: 'bad@test.com' },
        error_code: 'ES_PARSE_ERR',
        error_message: 'Corrupted field',
        stack_trace: null,
        retry_count: 0,
        status: 'PENDING',
        created_at: new Date().toISOString(),
        last_retried_at: null
      }
    ];

    const mockDLQStore = {
      getPendingCount: async () => 1,
      getPendingEntries: async () => sampleDLQ
    } as unknown as DLQStore;

    const mockESSink = {
      healthCheck: async () => ({ healthy: true, latencyMs: 5 })
    } as unknown as ElasticsearchSink;

    const mockRMQSink = {
      healthCheck: async () => ({ healthy: true, latencyMs: 3, connectionState: 'OPEN' })
    } as unknown as RabbitMQSink;

    const mockPgPool = {
      query: async () => ({ rows: [{ '?column?': 1 }] })
    } as unknown as Pool;

    const coordinator = new PipelineCoordinator(
      mockBackfillRunner,
      mockIncrementalRunner,
      {} as CheckpointManager,
      mockDLQStore,
      mockESSink,
      mockRMQSink,
      { getSourceMetadata: async () => ({ maxId: 100, totalCount: 100 }) } as SourceReader,
      new CircuitBreaker({ name: 'es' }),
      new CircuitBreaker({ name: 'rmq' }),
      mockPgPool
    );

    const server = createPipelineServer(coordinator, mockDLQStore);

    await new Promise<void>((resolve) => {
      server.listen(0, () => resolve());
    });

    const addr = server.address();
    const port = typeof addr === 'object' && addr ? addr.port : 3000;
    const baseUrl = `http://localhost:${port}`;

    try {
      // 1. GET /health
      const healthRes = await fetch(`${baseUrl}/health`);
      assert.strictEqual(healthRes.status, 200);
      const healthBody = await healthRes.json() as { status: string };
      assert.strictEqual(healthBody.status, 'HEALTHY');

      // 2. GET /api/telemetry
      const telemRes = await fetch(`${baseUrl}/api/telemetry`);
      assert.strictEqual(telemRes.status, 200);
      const telemBody = await telemRes.json() as PipelineTelemetry;
      assert.strictEqual(telemBody.backfill_cursor, 100);
      assert.strictEqual(telemBody.dlq_pending_count, 1);
      assert.strictEqual(telemBody.backfill_status, 'RUNNING');
      assert.strictEqual(telemBody.incremental_status, 'RUNNING');

      // 3. GET /api/dlq
      const dlqRes = await fetch(`${baseUrl}/api/dlq`);
      assert.strictEqual(dlqRes.status, 200);
      const dlqBody = await dlqRes.json() as { entries: DLQEntry[]; count: number };
      assert.strictEqual(dlqBody.count, 1);
      assert.strictEqual(dlqBody.entries[0].record_id, 42);

      // 4. POST /api/control/backfill/pause
      const pauseBfRes = await fetch(`${baseUrl}/api/control/backfill/pause`, { method: 'POST' });
      assert.strictEqual(pauseBfRes.status, 200);
      assert.strictEqual(bfPaused, true);

      // 5. POST /api/control/incremental/pause
      const pauseIncRes = await fetch(`${baseUrl}/api/control/incremental/pause`, { method: 'POST' });
      assert.strictEqual(pauseIncRes.status, 200);
      assert.strictEqual(incPaused, true);
    } finally {
      await new Promise<void>((resolve) => {
        server.close(() => resolve());
      });
    }
  });
});
