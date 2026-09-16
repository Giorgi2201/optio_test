/**
 * Initial Bulk Backfill Pipeline Runner
 * Unites Keyset extraction, dual-sink replication, Circuit Breaker throttling (Gate 3),
 * DLQ poison pill isolation (Gate 4), and post-ACK atomic checkpointing (Gate 1).
 */

import { PipelineStatus, SourceRecord } from '@optio/shared';
import { SourceReader } from '../source/source.reader.js';
import { CheckpointManager } from '../checkpoint/checkpoint.manager.js';
import { DLQStore, NewDLQEntry } from '../dlq/dlq.store.js';
import { ElasticsearchSink } from '../sinks/elasticsearch/elasticsearch.sink.js';
import { RabbitMQSink } from '../sinks/rabbitmq/rabbitmq.sink.js';
import { CircuitBreaker } from '../resilience/circuit-breaker.js';

export interface BackfillRunnerOptions {
  batchSize?: number;
  pipelineId?: string;
}

export interface BackfillMetrics {
  pipelineId: string;
  status: PipelineStatus;
  lastProcessedId: number;
  totalProcessed: number;
  totalFailed: number;
  currentThroughputEps: number;
  isRunning: boolean;
}

export class BackfillRunner {
  private readonly sourceReader: SourceReader;
  private readonly checkpointManager: CheckpointManager;
  private readonly dlqStore: DLQStore;
  private readonly elasticsearchSink: ElasticsearchSink;
  private readonly rabbitmqSink: RabbitMQSink;
  private readonly esCircuitBreaker: CircuitBreaker;
  private readonly rmqCircuitBreaker: CircuitBreaker;

  public readonly pipelineId: string;
  public readonly batchSize: number;

  private status: PipelineStatus = 'INITIALIZED';
  private lastProcessedId = 0;
  private totalProcessed = 0;
  private totalFailed = 0;
  private currentThroughputEps = 0;
  private startTime: number | null = null;
  private stopRequested = false;

  constructor(
    sourceReader: SourceReader,
    checkpointManager: CheckpointManager,
    dlqStore: DLQStore,
    elasticsearchSink: ElasticsearchSink,
    rabbitmqSink: RabbitMQSink,
    esCircuitBreaker: CircuitBreaker,
    rmqCircuitBreaker: CircuitBreaker,
    options: BackfillRunnerOptions = {}
  ) {
    this.sourceReader = sourceReader;
    this.checkpointManager = checkpointManager;
    this.dlqStore = dlqStore;
    this.elasticsearchSink = elasticsearchSink;
    this.rabbitmqSink = rabbitmqSink;
    this.esCircuitBreaker = esCircuitBreaker;
    this.rmqCircuitBreaker = rmqCircuitBreaker;

    this.pipelineId = options.pipelineId || 'backfill_pipeline';
    this.batchSize = Math.max(1, options.batchSize || 500);
  }

  /**
   * Starts or resumes bulk backfill replication.
   * Gate 1 Invariant: Resumes strictly from last committed persistent watermark.
   */
  public async start(): Promise<void> {
    this.stopRequested = false;

    // 1. Recover last verified watermark
    const checkpoint = await this.checkpointManager.getCheckpoint(this.pipelineId);
    this.lastProcessedId = checkpoint.last_processed_id;
    this.totalProcessed = checkpoint.records_processed;
    this.totalFailed = checkpoint.records_failed;

    if (this.stopRequested) {
      this.status = 'PAUSED';
      return;
    }

    this.status = 'RUNNING';
    await this.checkpointManager.updateStatus(this.pipelineId, 'RUNNING');
    this.startTime = Date.now();

    console.log(
      `[BACKFILL RUNNER] Started pipeline '${this.pipelineId}' resuming from watermark ID ${this.lastProcessedId}`
    );

    // 2. Continuous Keyset Streaming Loop
    try {
      while (!this.stopRequested) {
        // Step A: Keyset Batch Seek (O(1) Memory & Query)
        const records = await this.sourceReader.fetchBackfillBatch(
          this.lastProcessedId,
          this.batchSize
        );

        // Check for natural completion of historical dataset
        if (records.length === 0) {
          this.status = 'COMPLETED';
          await this.checkpointManager.updateStatus(this.pipelineId, 'COMPLETED');
          console.log(
            `[BACKFILL RUNNER] Reached end of source table. Completed at ID ${this.lastProcessedId}.`
          );
          break;
        }

        // Step B: Concurrent Dual-Sink Dispatch protected by Circuit Breakers (Gate 3)
        const [esResult, rmqResult] = await Promise.all([
          this.esCircuitBreaker.execute(() =>
            this.elasticsearchSink.bulkUpsert(records)
          ),
          this.rmqCircuitBreaker.execute(() =>
            this.rabbitmqSink.publishBatch(records, 'RECORD_BACKFILLED')
          )
        ]);

        // Step C: Gate 4 Poison Pill Isolation & DLQ Routing
        const recordMap = new Map<number, SourceRecord>();
        for (const r of records) {
          recordMap.set(r.id, r);
        }

        const dlqEntries: NewDLQEntry[] = [];
        const uniqueFailedIds = new Set<number>();

        if (esResult.failures.length > 0) {
          for (const f of esResult.failures) {
            uniqueFailedIds.add(f.recordId);
            const source = recordMap.get(f.recordId);
            dlqEntries.push({
              recordId: f.recordId,
              recordUuid: source?.uuid || null,
              sinkTarget: 'ELASTICSEARCH',
              payload: source?.payload || f.rawItem || {},
              errorCode: f.errorCode,
              errorMessage: f.errorReason
            });
          }
        }

        if (rmqResult.failures.length > 0) {
          for (const f of rmqResult.failures) {
            uniqueFailedIds.add(f.recordId);
            const source = recordMap.get(f.recordId);
            dlqEntries.push({
              recordId: f.recordId,
              recordUuid: source?.uuid || null,
              sinkTarget: 'RABBITMQ',
              payload: source?.payload || f.rawItem || {},
              errorCode: f.errorCode,
              errorMessage: f.errorReason
            });
          }
        }

        // Persist failed poison pills transactionally to PostgreSQL DLQ
        if (dlqEntries.length > 0) {
          await this.dlqStore.persistFailures(dlqEntries);
        }

        const batchFailedCount = uniqueFailedIds.size;
        const batchSuccessCount = records.length - batchFailedCount;

        this.totalProcessed += batchSuccessCount;
        this.totalFailed += batchFailedCount;

        // Step D: Gate 1 Atomic Watermark Persistence
        // Committed STRICTLY post-sink acknowledgment
        const newLastProcessedId = records[records.length - 1].id;

        // Recalculate rolling throughput (Gate 5)
        const now = Date.now();
        const elapsedSec = Math.max(0.001, (now - (this.startTime || now)) / 1000);
        this.currentThroughputEps = Math.round(this.totalProcessed / elapsedSec);

        await this.checkpointManager.commitCheckpoint(this.pipelineId, {
          lastProcessedId: newLastProcessedId,
          lastProcessedTimestamp: null,
          batchSuccessCount,
          batchFailedCount,
          metadata: {
            throughputEps: this.currentThroughputEps,
            lastBatchSize: records.length
          }
        });

        this.lastProcessedId = newLastProcessedId;
      }
    } catch (err) {
      this.status = 'FAILED';
      try {
        await this.checkpointManager.updateStatus(this.pipelineId, 'FAILED');
      } catch {
        // Ignore secondary error while persisting failure status
      }
      throw err;
    }

    if (this.stopRequested) {
      this.status = 'PAUSED';
    }
  }

  /**
   * Pauses the backfill execution loop cleanly after current in-flight batch commits.
   */
  public async pause(): Promise<void> {
    this.stopRequested = true;
    this.status = 'PAUSED';
    await this.checkpointManager.updateStatus(this.pipelineId, 'PAUSED');
    console.log(`[BACKFILL RUNNER] Pipeline '${this.pipelineId}' paused at ID ${this.lastProcessedId}`);
  }

  /**
   * Resumes backfill streaming.
   */
  public async resume(): Promise<void> {
    this.stopRequested = false;
    return this.start();
  }

  /**
   * Returns current operational metrics snapshot for Gate 5 and UI dashboard.
   */
  public getMetrics(): BackfillMetrics {
    return {
      pipelineId: this.pipelineId,
      status: this.status,
      lastProcessedId: this.lastProcessedId,
      totalProcessed: this.totalProcessed,
      totalFailed: this.totalFailed,
      currentThroughputEps: this.currentThroughputEps,
      isRunning: this.status === 'RUNNING'
    };
  }
}
