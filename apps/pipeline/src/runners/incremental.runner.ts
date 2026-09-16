/**
 * Continuous Incremental Synchronization Runner
 * Implements composite watermark polling (updated_at, id), real-time replication lag tracking (Gate 5),
 * dual-sink dispatch with circuit breakers (Gate 3), and DLQ poison pill isolation (Gate 4).
 */

import { PipelineStatus, SourceRecord } from '@optio/shared';
import { SourceReader } from '../source/source.reader.js';
import { CheckpointManager } from '../checkpoint/checkpoint.manager.js';
import { DLQStore, NewDLQEntry } from '../dlq/dlq.store.js';
import { ElasticsearchSink } from '../sinks/elasticsearch/elasticsearch.sink.js';
import { RabbitMQSink } from '../sinks/rabbitmq/rabbitmq.sink.js';
import { CircuitBreaker } from '../resilience/circuit-breaker.js';

export interface IncrementalRunnerOptions {
  batchSize?: number;
  pollIntervalMs?: number;
  pipelineId?: string;
}

export interface IncrementalMetrics {
  pipelineId: string;
  status: PipelineStatus;
  lastProcessedTimestamp: Date;
  lastProcessedId: number;
  totalMutationsProcessed: number;
  totalMutationsFailed: number;
  lagRecords: number;
  lagMs: number;
  isRunning: boolean;
}

export class IncrementalRunner {
  private readonly sourceReader: SourceReader;
  private readonly checkpointManager: CheckpointManager;
  private readonly dlqStore: DLQStore;
  private readonly elasticsearchSink: ElasticsearchSink;
  private readonly rabbitmqSink: RabbitMQSink;
  private readonly esCircuitBreaker: CircuitBreaker;
  private readonly rmqCircuitBreaker: CircuitBreaker;

  public readonly pipelineId: string;
  public readonly batchSize: number;
  public readonly pollIntervalMs: number;

  private status: PipelineStatus = 'INITIALIZED';
  private lastProcessedTimestamp: Date = new Date(0);
  private lastProcessedId = 0;
  private totalMutationsProcessed = 0;
  private totalMutationsFailed = 0;
  private currentLagRecords = 0;
  private currentLagMs = 0;
  private stopRequested = false;
  private pollTimer: NodeJS.Timeout | null = null;
  private sleepResolve: (() => void) | null = null;

  constructor(
    sourceReader: SourceReader,
    checkpointManager: CheckpointManager,
    dlqStore: DLQStore,
    elasticsearchSink: ElasticsearchSink,
    rabbitmqSink: RabbitMQSink,
    esCircuitBreaker: CircuitBreaker,
    rmqCircuitBreaker: CircuitBreaker,
    options: IncrementalRunnerOptions = {}
  ) {
    this.sourceReader = sourceReader;
    this.checkpointManager = checkpointManager;
    this.dlqStore = dlqStore;
    this.elasticsearchSink = elasticsearchSink;
    this.rabbitmqSink = rabbitmqSink;
    this.esCircuitBreaker = esCircuitBreaker;
    this.rmqCircuitBreaker = rmqCircuitBreaker;

    this.pipelineId = options.pipelineId || 'incremental_pipeline';
    this.batchSize = Math.max(1, options.batchSize || 500);
    this.pollIntervalMs = Math.max(0, options.pollIntervalMs ?? 1000);
  }

  /**
   * Starts or resumes continuous incremental synchronization.
   */
  public async start(): Promise<void> {
    this.stopRequested = false;

    // 1. Recover starting watermark
    const checkpoint = await this.checkpointManager.getCheckpoint(this.pipelineId);
    this.lastProcessedId = checkpoint.last_processed_id;
    this.lastProcessedTimestamp = checkpoint.last_processed_timestamp
      ? new Date(checkpoint.last_processed_timestamp)
      : new Date(0);
    this.totalMutationsProcessed = checkpoint.records_processed;
    this.totalMutationsFailed = checkpoint.records_failed;

    if (this.stopRequested) {
      this.status = 'PAUSED';
      return;
    }

    this.status = 'RUNNING';
    await this.checkpointManager.updateStatus(this.pipelineId, 'RUNNING');

    console.log(
      `[INCREMENTAL RUNNER] Started pipeline '${this.pipelineId}' from timestamp ${this.lastProcessedTimestamp.toISOString()} (ID ${this.lastProcessedId})`
    );

    // 2. Continuous Polling Loop
    try {
      while (!this.stopRequested) {
        // Step 1: Compute Current Replication Lag (Gate 5)
        const lagInfo = await this.sourceReader.getIncrementalLag(
          this.lastProcessedTimestamp,
          this.lastProcessedId
        );
        this.currentLagRecords = lagInfo.lagRecords;
        this.currentLagMs = lagInfo.lagMs;

        // Step 2: Extract Incremental Mutation Keyset Batch
        const records = await this.sourceReader.fetchIncrementalBatch(
          this.lastProcessedTimestamp,
          this.lastProcessedId,
          this.batchSize
        );

        if (records.length === 0) {
          // No pending mutations; sleep before next poll cycle
          await this.sleep(this.pollIntervalMs);
          continue;
        }

        // Step 3: Dual-Sink Concurrent Mutation Dispatch via Circuit Breakers (Gate 3)
        let esResult;
        let rmqResult;

        try {
          [esResult, rmqResult] = await Promise.all([
            this.esCircuitBreaker.execute(() =>
              this.elasticsearchSink.bulkUpsert(records)
            ),
            this.rmqCircuitBreaker.execute(() =>
              this.rabbitmqSink.publishBatch(records, 'RECORD_MUTATED')
            )
          ]);
        } catch (dispatchErr: unknown) {
          console.error(
            `[INCREMENTAL RUNNER] Downstream dispatch failed (throttled by circuit breaker):`,
            dispatchErr
          );
          // Do not advance watermark on downstream failure; wait before retrying
          await this.sleep(this.pollIntervalMs);
          continue;
        }

        // Step 4: Gate 4 Error Isolation & DLQ Routing
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

        if (dlqEntries.length > 0) {
          await this.dlqStore.persistFailures(dlqEntries);
        }

        const batchFailedCount = uniqueFailedIds.size;
        const batchSuccessCount = records.length - batchFailedCount;

        this.totalMutationsProcessed += batchSuccessCount;
        this.totalMutationsFailed += batchFailedCount;

        // Step 5: Atomic Watermark Persistence
        const lastRecord = records[records.length - 1];
        const newTimestamp = new Date(lastRecord.updated_at);
        const newId = lastRecord.id;

        await this.checkpointManager.commitCheckpoint(this.pipelineId, {
          lastProcessedId: newId,
          lastProcessedTimestamp: newTimestamp,
          batchSuccessCount,
          batchFailedCount,
          metadata: {
            lagRecords: this.currentLagRecords,
            lagMs: this.currentLagMs
          }
        });

        this.lastProcessedTimestamp = newTimestamp;
        this.lastProcessedId = newId;

        // If batch was full, immediately continue to drain backlog without delay; otherwise sleep
        if (records.length < this.batchSize) {
          await this.sleep(this.pollIntervalMs);
        }
      }
    } catch (err: unknown) {
      this.status = 'FAILED';
      try {
        await this.checkpointManager.updateStatus(this.pipelineId, 'FAILED');
      } catch {
        // Ignore secondary failure
      }
      throw err;
    }

    if (this.stopRequested) {
      this.status = 'PAUSED';
    }
  }

  /**
   * Pauses continuous synchronization cleanly.
   */
  public async pause(): Promise<void> {
    this.stopRequested = true;
    this.status = 'PAUSED';

    if (this.pollTimer) {
      clearTimeout(this.pollTimer);
      this.pollTimer = null;
    }

    if (this.sleepResolve) {
      const resolve = this.sleepResolve;
      this.sleepResolve = null;
      resolve();
    }

    await this.checkpointManager.updateStatus(this.pipelineId, 'PAUSED');
    console.log(
      `[INCREMENTAL RUNNER] Pipeline '${this.pipelineId}' paused at timestamp ${this.lastProcessedTimestamp.toISOString()} (ID ${this.lastProcessedId})`
    );
  }

  /**
   * Resumes continuous incremental synchronization.
   */
  public async resume(): Promise<void> {
    this.stopRequested = false;
    return this.start();
  }

  /**
   * Returns current operational metrics snapshot for Gate 5 and UI dashboard.
   */
  public getMetrics(): IncrementalMetrics {
    return {
      pipelineId: this.pipelineId,
      status: this.status,
      lastProcessedTimestamp: this.lastProcessedTimestamp,
      lastProcessedId: this.lastProcessedId,
      totalMutationsProcessed: this.totalMutationsProcessed,
      totalMutationsFailed: this.totalMutationsFailed,
      lagRecords: this.currentLagRecords,
      lagMs: this.currentLagMs,
      isRunning: this.status === 'RUNNING'
    };
  }

  /**
   * Non-blocking asynchronous sleep that can be interrupted immediately by pause().
   */
  private sleep(ms: number): Promise<void> {
    if (ms <= 0 || this.stopRequested) {
      return Promise.resolve();
    }
    return new Promise((resolve) => {
      this.sleepResolve = resolve;
      this.pollTimer = setTimeout(() => {
        this.pollTimer = null;
        this.sleepResolve = null;
        resolve();
      }, ms);
    });
  }
}
