/**
 * Concurrency Coordinator & Unified Telemetry Aggregator
 * Orchestrates dual-mode execution (simultaneous Backfill and Incremental CDC loops)
 * and aggregates Gate 5 real-time telemetry.
 */

import { Pool } from 'pg';
import {
  PipelineTelemetry,
  ComponentHealth,
  SystemHealth
} from '@optio/shared';
import { BackfillRunner } from '../runners/backfill.runner.js';
import { IncrementalRunner } from '../runners/incremental.runner.js';
import { CheckpointManager } from '../checkpoint/checkpoint.manager.js';
import { DLQStore } from '../dlq/dlq.store.js';
import { ElasticsearchSink } from '../sinks/elasticsearch/elasticsearch.sink.js';
import { RabbitMQSink } from '../sinks/rabbitmq/rabbitmq.sink.js';
import { SourceReader } from '../source/source.reader.js';
import { CircuitBreaker } from '../resilience/circuit-breaker.js';
import { ensureElasticsearchIndex } from '../sinks/elasticsearch/schema.js';

export type CoordinatorStatus = 'INITIALIZED' | 'RUNNING' | 'STOPPED';

export class PipelineCoordinator {
  public readonly backfillRunner: BackfillRunner;
  public readonly incrementalRunner: IncrementalRunner;
  public readonly checkpointManager: CheckpointManager;
  public readonly dlqStore: DLQStore;
  public readonly elasticsearchSink: ElasticsearchSink;
  public readonly rabbitmqSink: RabbitMQSink;
  public readonly sourceReader: SourceReader;
  public readonly esCircuitBreaker: CircuitBreaker;
  public readonly rmqCircuitBreaker: CircuitBreaker;
  public readonly pgPool: Pool;

  private status: CoordinatorStatus = 'INITIALIZED';

  constructor(
    backfillRunner: BackfillRunner,
    incrementalRunner: IncrementalRunner,
    checkpointManager: CheckpointManager,
    dlqStore: DLQStore,
    elasticsearchSink: ElasticsearchSink,
    rabbitmqSink: RabbitMQSink,
    sourceReader: SourceReader,
    esCircuitBreaker: CircuitBreaker,
    rmqCircuitBreaker: CircuitBreaker,
    pgPool: Pool
  ) {
    this.backfillRunner = backfillRunner;
    this.incrementalRunner = incrementalRunner;
    this.checkpointManager = checkpointManager;
    this.dlqStore = dlqStore;
    this.elasticsearchSink = elasticsearchSink;
    this.rabbitmqSink = rabbitmqSink;
    this.sourceReader = sourceReader;
    this.esCircuitBreaker = esCircuitBreaker;
    this.rmqCircuitBreaker = rmqCircuitBreaker;
    this.pgPool = pgPool;
  }

  /**
   * Initializes sinks and concurrently launches backfill and incremental pollers as independent background loops.
   */
  public async start(): Promise<void> {
    console.log('[COORDINATOR] Initializing downstream topology and sink connections...');

    // 1. Ensure Elasticsearch search index exists
    try {
      await ensureElasticsearchIndex(
        this.elasticsearchSink.getClient(),
        this.elasticsearchSink.getIndexName()
      );
      console.log(`[COORDINATOR] Elasticsearch index '${this.elasticsearchSink.getIndexName()}' verified.`);
    } catch (err: unknown) {
      console.error('[COORDINATOR] Warning: Failed to provision Elasticsearch index:', err);
    }

    // 2. Connect RabbitMQ ConfirmChannel & declare durable topology
    try {
      await this.rabbitmqSink.connect();
      console.log('[COORDINATOR] RabbitMQ topology declared and confirmed.');
    } catch (err: unknown) {
      console.error('[COORDINATOR] Warning: Failed to connect RabbitMQ sink:', err);
    }

    // 3. Concurrently launch runners as independent unblocked loops
    console.log('[COORDINATOR] Launching dual-mode Backfill and Incremental runners simultaneously...');
    this.status = 'RUNNING';

    this.backfillRunner.start().catch((err: unknown) => {
      console.error('[COORDINATOR] Backfill runner crashed with unhandled error:', err);
    });

    this.incrementalRunner.start().catch((err: unknown) => {
      console.error('[COORDINATOR] Incremental runner crashed with unhandled error:', err);
    });

    console.log('[COORDINATOR] Both replication loops actively running in background.');
  }

  /**
   * Aggregates real-time Gate 5 telemetry for observability, health checks, and UI dashboard.
   */
  public async getTelemetry(): Promise<PipelineTelemetry> {
    // 1. Health Probes
    let pgHealth: ComponentHealth;
    const pgStart = Date.now();
    try {
      await this.pgPool.query('SELECT 1');
      pgHealth = {
        status: 'UP',
        latency_ms: Date.now() - pgStart
      };
    } catch (err: unknown) {
      pgHealth = {
        status: 'DOWN',
        latency_ms: Date.now() - pgStart,
        message: err instanceof Error ? err.message : String(err)
      };
    }

    const esCheck = await this.elasticsearchSink.healthCheck();
    const esHealth: ComponentHealth = {
      status: esCheck.healthy ? 'UP' : 'DOWN',
      latency_ms: esCheck.latencyMs,
      message: esCheck.error || (esCheck.clusterStatus ? `Cluster status: ${esCheck.clusterStatus}` : undefined)
    };

    const rmqCheck = await this.rabbitmqSink.healthCheck();
    const rmqHealth: ComponentHealth = {
      status: rmqCheck.healthy ? 'UP' : 'DOWN',
      latency_ms: rmqCheck.latencyMs,
      message: rmqCheck.error || `State: ${rmqCheck.connectionState}`
    };

    const allUp = pgHealth.status === 'UP' && esHealth.status === 'UP' && rmqHealth.status === 'UP';
    const anyDown = pgHealth.status === 'DOWN' || esHealth.status === 'DOWN' || rmqHealth.status === 'DOWN';
    const overall: 'HEALTHY' | 'DEGRADED' | 'DOWN' = allUp ? 'HEALTHY' : (anyDown ? 'DOWN' : 'DEGRADED');

    const health: SystemHealth = {
      overall,
      postgres: pgHealth,
      elasticsearch: esHealth,
      rabbitmq: rmqHealth
    };

    // 2. Metrics Collection
    const backfillMetrics = this.backfillRunner.getMetrics();
    const incrementalMetrics = this.incrementalRunner.getMetrics();

    let sourceMetadata = { maxId: 0, totalCount: 0 };
    try {
      sourceMetadata = await this.sourceReader.getSourceMetadata();
    } catch {
      // Degraded read
    }

    let dlqPendingCount = 0;
    try {
      dlqPendingCount = await this.dlqStore.getPendingCount();
    } catch {
      // Degraded read
    }

    // Calculate backfill completion percentage
    let backfillCompletionPct = 0;
    if (sourceMetadata.maxId > 0) {
      backfillCompletionPct = Math.min(
        100,
        Math.round((backfillMetrics.lastProcessedId / sourceMetadata.maxId) * 10000) / 100
      );
    } else if (sourceMetadata.totalCount === 0) {
      backfillCompletionPct = 100;
    }

    return {
      status: this.status,
      backfill_status: backfillMetrics.status,
      incremental_status: incrementalMetrics.status,
      backfill_cursor: backfillMetrics.lastProcessedId,
      backfill_total_records: sourceMetadata.totalCount,
      backfill_completion_pct: backfillCompletionPct,
      current_throughput_eps: backfillMetrics.currentThroughputEps,
      incremental_lag_records: incrementalMetrics.lagRecords,
      incremental_lag_ms: incrementalMetrics.lagMs,
      dlq_pending_count: dlqPendingCount,
      health,
      circuit_breakers: {
        elasticsearch: this.esCircuitBreaker.getMetrics(),
        rabbitmq: this.rmqCircuitBreaker.getMetrics()
      },
      timestamp: new Date().toISOString()
    };
  }

  /**
   * Pauses the bulk backfill runner.
   */
  public async pauseBackfill(): Promise<void> {
    console.log('[COORDINATOR] Pausing bulk backfill runner...');
    await this.backfillRunner.pause();
  }

  /**
   * Resumes the bulk backfill runner.
   */
  public async resumeBackfill(): Promise<void> {
    console.log('[COORDINATOR] Resuming bulk backfill runner...');
    this.backfillRunner.resume().catch((err: unknown) => {
      console.error('[COORDINATOR] Error resuming backfill runner:', err);
    });
  }

  /**
   * Pauses the incremental synchronization runner.
   */
  public async pauseIncremental(): Promise<void> {
    console.log('[COORDINATOR] Pausing incremental CDC runner...');
    await this.incrementalRunner.pause();
  }

  /**
   * Resumes the incremental synchronization runner.
   */
  public async resumeIncremental(): Promise<void> {
    console.log('[COORDINATOR] Resuming incremental CDC runner...');
    this.incrementalRunner.resume().catch((err: unknown) => {
      console.error('[COORDINATOR] Error resuming incremental runner:', err);
    });
  }

  /**
   * Gracefully shuts down both runners, sinks, and connection pools.
   */
  public async stop(): Promise<void> {
    console.log('[COORDINATOR] Stopping coordinator and shutting down all resources...');
    this.status = 'STOPPED';

    await Promise.allSettled([
      this.backfillRunner.pause(),
      this.incrementalRunner.pause()
    ]);

    await Promise.allSettled([
      this.rabbitmqSink.close(),
      this.elasticsearchSink.close(),
      this.pgPool.end()
    ]);

    console.log('[COORDINATOR] Graceful shutdown complete.');
  }

  public getStatus(): CoordinatorStatus {
    return this.status;
  }
}
