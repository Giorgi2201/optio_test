/**
 * Production-Grade Elasticsearch Sink Adapter
 * Implements deterministic idempotent upserts (_id = source record ID)
 * and Gate 4 bulk error decomposition.
 */

import { Client } from '@elastic/elasticsearch';
import { SourceRecord, SearchDocument } from '@optio/shared';

export interface BulkFailureEntry {
  recordId: number;
  errorReason: string;
  errorCode: string;
  rawItem: unknown;
}

export interface BulkUpsertResult {
  successCount: number;
  failedCount: number;
  successfulIds: number[];
  failures: BulkFailureEntry[];
}

export interface ElasticsearchSinkOptions {
  indexName: string;
  timeoutMs?: number;
}

export interface ElasticsearchHealthResult {
  healthy: boolean;
  latencyMs: number;
  clusterStatus?: string;
  error?: string;
}

export class ElasticsearchSink {
  private readonly client: Client;
  private readonly indexName: string;
  private readonly timeoutMs: number;

  constructor(client: Client, options: ElasticsearchSinkOptions) {
    this.client = client;
    this.indexName = options.indexName;
    this.timeoutMs = options.timeoutMs ?? 5000;
  }

  /**
   * Healthcheck probe for Gate 5 / Circuit Breakers.
   */
  public async healthCheck(): Promise<ElasticsearchHealthResult> {
    const start = Date.now();
    try {
      // Fail-fast probe: a health check must never wait out an outage behind client retries.
      const res = await this.client.cluster.health(
        {},
        { requestTimeout: this.timeoutMs, maxRetries: 0 }
      );
      const latencyMs = Date.now() - start;
      const healthy = res.status !== 'red';
      return {
        healthy,
        latencyMs,
        clusterStatus: res.status
      };
    } catch (err: unknown) {
      const latencyMs = Date.now() - start;
      const errorMsg = err instanceof Error ? err.message : String(err);
      return {
        healthy: false,
        latencyMs,
        error: errorMsg
      };
    }
  }

  /**
   * Deterministically maps a relational SourceRecord into a SearchDocument.
   * - Stringifies record.id to match Elasticsearch _id for deterministic upserts.
   * - Joins first_name and last_name for full-text search.
   * - Populates current synced_at timestamp.
   */
  public transformRecord(record: SourceRecord): SearchDocument {
    const firstName = record.payload.first_name || '';
    const lastName = record.payload.last_name || '';
    const fullName = `${firstName} ${lastName}`.trim();
    const tags = Array.isArray(record.payload.metadata?.tags)
      ? record.payload.metadata.tags
      : [];
    const country = typeof record.payload.metadata?.country === 'string'
      ? record.payload.metadata.country
      : '';

    return {
      id: String(record.id),
      source_uuid: record.uuid,
      tenant_id: record.tenant_id,
      customer_id: record.payload.customer_id,
      full_name: fullName,
      email: record.payload.email,
      account_tier: record.payload.account_tier,
      balance: record.payload.balance,
      status: record.status,
      version: record.version,
      tags: tags,
      country: country,
      source_created_at: record.created_at,
      source_updated_at: record.updated_at,
      synced_at: new Date().toISOString()
    };
  }

  /**
   * Performs bulk upsert operations with deterministic IDs.
   * Decomposes bulk errors to isolate poison pills from valid records (Gate 4).
   * Does NOT throw exceptions on partial item rejections.
   */
  public async bulkUpsert(records: SourceRecord[]): Promise<BulkUpsertResult> {
    if (records.length === 0) {
      return {
        successCount: 0,
        failedCount: 0,
        successfulIds: [],
        failures: []
      };
    }

    const operations: unknown[] = [];

    for (const record of records) {
      const searchDoc = this.transformRecord(record);
      // Action metadata line
      operations.push({
        update: {
          _index: this.indexName,
          _id: String(record.id)
        }
      });
      // Document line with doc_as_upsert: true
      operations.push({
        doc: searchDoc,
        doc_as_upsert: true
      });
    }

    // Fail-fast contract (AGENTS.md §3.3): bound every request deterministically and disable the
    // client's internal retries. Retry/backoff policy belongs to the circuit breaker wrapping this
    // call; otherwise a request can silently wait out a sink outage and the breaker never observes it.
    const response = await this.client.bulk(
      {
        operations,
        refresh: false
      },
      { requestTimeout: this.timeoutMs, maxRetries: 0 }
    );

    return this.parseBulkResponse(records, response);
  }

  /**
   * Internal bulk response decomposition engine.
   * Matches input records with item status codes to isolate individual failures.
   */
  public parseBulkResponse(
    records: SourceRecord[],
    response: { errors?: boolean; items?: unknown[] }
  ): BulkUpsertResult {
    const successfulIds: number[] = [];
    const failures: BulkFailureEntry[] = [];

    // Fast path: No errors reported across entire batch
    if (!response.errors) {
      for (const record of records) {
        successfulIds.push(record.id);
      }
      return {
        successCount: successfulIds.length,
        failedCount: 0,
        successfulIds,
        failures
      };
    }

    // Decompose items individually
    const items = response.items || [];

    for (let i = 0; i < records.length; i++) {
      const record = records[i];
      const itemWrapper = items[i] as Record<string, unknown> | undefined;
      const actionItem = (itemWrapper?.update ||
        itemWrapper?.index ||
        itemWrapper?.create ||
        (itemWrapper ? Object.values(itemWrapper)[0] : null)) as
        | { status?: number; error?: { reason?: string; type?: string } }
        | null
        | undefined;

      const status = actionItem?.status ?? 200;

      if (status >= 200 && status < 300) {
        successfulIds.push(record.id);
      } else {
        const errorObj = actionItem?.error;
        failures.push({
          recordId: record.id,
          errorReason: errorObj?.reason || `Elasticsearch HTTP ${status} rejection`,
          errorCode: errorObj?.type || `ES_STATUS_${status}`,
          rawItem: actionItem
        });
      }
    }

    return {
      successCount: successfulIds.length,
      failedCount: failures.length,
      successfulIds,
      failures
    };
  }

  public getClient(): Client {
    return this.client;
  }

  public getIndexName(): string {
    return this.indexName;
  }

  public async close(): Promise<void> {
    await this.client.close();
  }
}
