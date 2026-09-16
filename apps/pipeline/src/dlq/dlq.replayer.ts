/**
 * Dead Letter Queue (DLQ) Replay Engine
 * Implements operator-driven re-ingestion to target sinks (Gate 4 & UI Panel 3).
 */

import crypto from 'crypto';
import { Pool } from 'pg';
import {
  SourceRecord,
  CustomerPayload,
  AccountTier,
  CustomerMetadata
} from '@optio/shared';
import { DLQStore } from './dlq.store.js';
import { ElasticsearchSink } from '../sinks/elasticsearch/elasticsearch.sink.js';
import { RabbitMQSink } from '../sinks/rabbitmq/rabbitmq.sink.js';

export interface DLQRetryResult {
  success: boolean;
  error?: string;
}

export interface DLQRetrySummary {
  retried: number;
  resolved: number;
  failed: number;
}

export class DLQReplayer {
  private readonly pgPool: Pool;
  private readonly dlqStore: DLQStore;
  private readonly elasticsearchSink: ElasticsearchSink;
  private readonly rabbitmqSink: RabbitMQSink;

  constructor(
    pgPool: Pool,
    dlqStore: DLQStore,
    elasticsearchSink: ElasticsearchSink,
    rabbitmqSink: RabbitMQSink
  ) {
    this.pgPool = pgPool;
    this.dlqStore = dlqStore;
    this.elasticsearchSink = elasticsearchSink;
    this.rabbitmqSink = rabbitmqSink;
  }

  /**
   * Retries an individual dead-lettered item by ID.
   * If successful, updates DLQ status to 'RESOLVED'.
   * If failed, increments retry_count and updates last_retried_at.
   */
  public async retryEntry(dlqId: number): Promise<DLQRetryResult> {
    const entry = await this.dlqStore.getEntryById(dlqId);
    if (!entry) {
      return { success: false, error: `DLQ entry ${dlqId} not found` };
    }

    if (entry.status === 'RESOLVED') {
      return { success: true };
    }

    // Reconstruct SourceRecord from DLQ payload and diagnostic context
    const payloadObj =
      typeof entry.payload === 'object' && entry.payload !== null
        ? entry.payload
        : {};

    const fallbackBalance =
      typeof payloadObj.balance === 'number'
        ? payloadObj.balance
        : Number(payloadObj.balance);

    const reconstructedPayload: CustomerPayload = {
      customer_id:
        (payloadObj.customer_id as string) ||
        `cust_dlq_${entry.record_id || dlqId}`,
      first_name: (payloadObj.first_name as string) || 'Replayed',
      last_name: (payloadObj.last_name as string) || 'Customer',
      email:
        (payloadObj.email as string) ||
        `replayed_${entry.record_id || dlqId}@test.com`,
      account_tier:
        (payloadObj.account_tier as AccountTier) === 'PREMIUM' ||
        (payloadObj.account_tier as AccountTier) === 'ENTERPRISE'
          ? (payloadObj.account_tier as AccountTier)
          : 'STANDARD',
      balance: isNaN(fallbackBalance) ? 0 : fallbackBalance,
      metadata: (payloadObj.metadata as CustomerMetadata) || {
        signup_channel: 'WEB',
        country: 'US',
        tags: ['dlq_replayed']
      }
    };

    const record: SourceRecord = {
      id: entry.record_id || dlqId,
      uuid: entry.record_uuid || crypto.randomUUID(),
      tenant_id: (payloadObj.tenant_id as string) || 'tenant_default',
      payload: reconstructedPayload,
      version: 1,
      status: 'ACTIVE',
      is_corrupted: false,
      created_at: entry.created_at,
      updated_at: new Date().toISOString()
    };

    try {
      let replayFailed = false;
      let failureReason = '';

      // 1. Dispatch to Elasticsearch if target is ELASTICSEARCH or ALL
      if (entry.sink_target === 'ELASTICSEARCH' || entry.sink_target === 'ALL') {
        const esRes = await this.elasticsearchSink.bulkUpsert([record]);
        if (esRes.failures.length > 0) {
          replayFailed = true;
          failureReason = esRes.failures[0].errorReason;
        }
      }

      // 2. Dispatch to RabbitMQ if target is RABBITMQ or ALL
      if (!replayFailed && (entry.sink_target === 'RABBITMQ' || entry.sink_target === 'ALL')) {
        const rmqRes = await this.rabbitmqSink.publishBatch([record], 'RECORD_MUTATED');
        if (rmqRes.failures.length > 0) {
          replayFailed = true;
          failureReason = rmqRes.failures[0].errorReason;
        }
      }

      if (replayFailed) {
        await this.dlqStore.recordRetryFailure(dlqId);
        return { success: false, error: failureReason };
      }

      // Mark RESOLVED upon confirmation
      await this.dlqStore.markStatus(dlqId, 'RESOLVED');
      return { success: true };
    } catch (err: unknown) {
      const errorMsg = err instanceof Error ? err.message : String(err);
      await this.dlqStore.recordRetryFailure(dlqId);
      return { success: false, error: errorMsg };
    }
  }

  /**
   * Replays all currently pending DLQ entries in batches.
   */
  public async retryAllPending(): Promise<DLQRetrySummary> {
    const summary: DLQRetrySummary = { retried: 0, resolved: 0, failed: 0 };
    const batchSize = 100;
    const maxIterations = 50; // Safety cap

    for (let i = 0; i < maxIterations; i++) {
      const pending = await this.dlqStore.getPendingEntries(batchSize);
      if (pending.length === 0) {
        break;
      }

      for (const item of pending) {
        summary.retried++;
        const res = await this.retryEntry(item.id);
        if (res.success) {
          summary.resolved++;
        } else {
          summary.failed++;
        }
      }

      // If all items in this batch failed, break to avoid infinite loop on stubborn poison pills
      if (summary.failed >= pending.length && summary.resolved === 0) {
        break;
      }
    }

    return summary;
  }
}
