/**
 * Transactional Checkpoint Manager for Replication Pipelines
 * Enforces Gate 1 Crash Recovery invariant: checkpoints are committed strictly post-sink-ACK.
 */

import { QueryResultRow } from 'pg';
import { ReplicationCheckpoint, PipelineStatus } from '@optio/shared';
import { Queryable } from '../source/source.reader.js';

export interface CheckpointCommitData {
  lastProcessedId: number;
  lastProcessedTimestamp?: Date | null;
  batchSuccessCount: number;
  batchFailedCount: number;
  metadata?: Record<string, unknown>;
}

interface RawCheckpointRow extends QueryResultRow {
  pipeline_id: string;
  last_processed_id: string | number;
  last_processed_timestamp: string | Date | null;
  status: string;
  records_processed: string | number;
  records_failed: string | number;
  metadata: string | Record<string, unknown>;
  updated_at: string | Date;
}

export class CheckpointManager {
  private readonly db: Queryable;

  constructor(db: Queryable) {
    this.db = db;
  }

  /**
   * Retrieves the current checkpoint watermark for a given pipeline job.
   * If missing, initializes a row with last_processed_id = 0 idempotently.
   */
  public async getCheckpoint(pipelineId: string): Promise<ReplicationCheckpoint> {
    const selectQuery = `
      SELECT pipeline_id, last_processed_id, last_processed_timestamp, status,
             records_processed, records_failed, metadata, updated_at
      FROM replication_checkpoints
      WHERE pipeline_id = $1;
    `;

    let res = await this.db.query<RawCheckpointRow>(selectQuery, [pipelineId]);

    if (res.rows.length === 0) {
      // Initialize missing checkpoint idempotently
      const insertQuery = `
        INSERT INTO replication_checkpoints (
          pipeline_id, last_processed_id, last_processed_timestamp, status,
          records_processed, records_failed, metadata, updated_at
        )
        VALUES ($1, 0, NULL, 'INITIALIZED', 0, 0, '{}'::jsonb, NOW())
        ON CONFLICT (pipeline_id) DO NOTHING;
      `;
      await this.db.query(insertQuery, [pipelineId]);
      res = await this.db.query<RawCheckpointRow>(selectQuery, [pipelineId]);
    }

    const row = res.rows[0];
    return mapRowToCheckpoint(row);
  }

  /**
   * Atomically commits a verified batch boundary watermark.
   * STRICT CONTRACT: Must be executed strictly AFTER both downstream sinks have acknowledged.
   */
  public async commitCheckpoint(
    pipelineId: string,
    watermark: CheckpointCommitData
  ): Promise<void> {
    const updateQuery = `
      UPDATE replication_checkpoints
      SET last_processed_id = $1,
          last_processed_timestamp = $2,
          records_processed = records_processed + $3,
          records_failed = records_failed + $4,
          metadata = COALESCE($5::jsonb, metadata),
          updated_at = NOW()
      WHERE pipeline_id = $6;
    `;

    const metadataJson = watermark.metadata ? JSON.stringify(watermark.metadata) : null;

    await this.db.query(updateQuery, [
      watermark.lastProcessedId,
      watermark.lastProcessedTimestamp || null,
      watermark.batchSuccessCount,
      watermark.batchFailedCount,
      metadataJson,
      pipelineId
    ]);
  }

  /**
   * Updates lifecycle status ('INITIALIZED', 'RUNNING', 'PAUSED', 'COMPLETED', 'FAILED').
   */
  public async updateStatus(
    pipelineId: string,
    status: PipelineStatus
  ): Promise<void> {
    const queryText = `
      UPDATE replication_checkpoints
      SET status = $1,
          updated_at = NOW()
      WHERE pipeline_id = $2;
    `;
    await this.db.query(queryText, [status, pipelineId]);
  }

  /**
   * Resets watermark to 0 (used for fresh test runs or manual re-backfill).
   */
  public async resetCheckpoint(pipelineId: string): Promise<void> {
    const queryText = `
      UPDATE replication_checkpoints
      SET last_processed_id = 0,
          last_processed_timestamp = NULL,
          records_processed = 0,
          records_failed = 0,
          status = 'INITIALIZED',
          updated_at = NOW()
      WHERE pipeline_id = $1;
    `;
    await this.db.query(queryText, [pipelineId]);
  }
}

function mapRowToCheckpoint(row: RawCheckpointRow): ReplicationCheckpoint {
  let metadata: Record<string, unknown>;
  if (typeof row.metadata === 'string') {
    try {
      metadata = JSON.parse(row.metadata) as Record<string, unknown>;
    } catch {
      metadata = {};
    }
  } else {
    metadata = row.metadata || {};
  }

  return {
    pipeline_id: row.pipeline_id,
    last_processed_id: typeof row.last_processed_id === 'string'
      ? parseInt(row.last_processed_id, 10)
      : Number(row.last_processed_id),
    last_processed_timestamp: row.last_processed_timestamp
      ? (row.last_processed_timestamp instanceof Date
          ? row.last_processed_timestamp.toISOString()
          : String(row.last_processed_timestamp))
      : null,
    status: row.status as PipelineStatus,
    records_processed: typeof row.records_processed === 'string'
      ? parseInt(row.records_processed, 10)
      : Number(row.records_processed),
    records_failed: typeof row.records_failed === 'string'
      ? parseInt(row.records_failed, 10)
      : Number(row.records_failed),
    metadata,
    updated_at: row.updated_at instanceof Date
      ? row.updated_at.toISOString()
      : String(row.updated_at)
  };
}
