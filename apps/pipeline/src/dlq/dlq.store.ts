/**
 * Persistent Dead Letter Queue (DLQ) Store
 * Persists isolated poison pills to PostgreSQL with diagnostic context (Gate 4).
 */

import { QueryResultRow } from 'pg';
import { DLQEntry, DLQStatus, SinkTarget } from '@optio/shared';
import { Queryable } from '../source/source.reader.js';

export interface NewDLQEntry {
  recordId?: number | null;
  recordUuid?: string | null;
  sinkTarget: SinkTarget;
  payload: unknown;
  errorCode: string;
  errorMessage: string;
  stackTrace?: string | null;
}

interface RawDLQRow extends QueryResultRow {
  id: string | number;
  record_id: string | number | null;
  record_uuid: string | null;
  sink_target: string;
  payload: string | Record<string, unknown>;
  error_code: string;
  error_message: string;
  stack_trace: string | null;
  retry_count: string | number;
  status: string;
  created_at: string | Date;
  last_retried_at: string | Date | null;
}

export class DLQStore {
  private readonly db: Queryable;

  constructor(db: Queryable) {
    this.db = db;
  }

  /**
   * Persists a batch of isolated poison pills transactionally into dead_letter_queue.
   * Returns the IDs of the newly created DLQ entries.
   */
  public async persistFailures(failures: NewDLQEntry[]): Promise<number[]> {
    if (failures.length === 0) {
      return [];
    }

    const valueClauses: string[] = [];
    const values: unknown[] = [];
    let paramIdx = 1;

    for (const f of failures) {
      valueClauses.push(
        `($${paramIdx}, $${paramIdx + 1}, $${paramIdx + 2}, $${paramIdx + 3}::jsonb, $${paramIdx + 4}, $${paramIdx + 5}, $${paramIdx + 6}, 'PENDING', 0, NOW())`
      );
      paramIdx += 7;

      values.push(
        f.recordId ?? null,
        f.recordUuid ?? null,
        f.sinkTarget,
        typeof f.payload === 'string' ? f.payload : JSON.stringify(f.payload ?? {}),
        f.errorCode,
        f.errorMessage,
        f.stackTrace ?? null
      );
    }

    const queryText = `
      INSERT INTO dead_letter_queue (
        record_id, record_uuid, sink_target, payload, error_code,
        error_message, stack_trace, status, retry_count, created_at
      )
      VALUES ${valueClauses.join(', ')}
      RETURNING id;
    `;

    const res = await this.db.query<{ id: string | number }>(queryText, values);
    return res.rows.map((r) =>
      typeof r.id === 'string' ? parseInt(r.id, 10) : Number(r.id)
    );
  }

  /**
   * Returns current count of unresolved DLQ items for Gate 5 and observability dashboard.
   */
  public async getPendingCount(): Promise<number> {
    const queryText = `
      SELECT COUNT(*) AS pending_count
      FROM dead_letter_queue
      WHERE status = 'PENDING';
    `;
    const res = await this.db.query<{ pending_count: string | number }>(queryText);
    const count = res.rows[0]?.pending_count;
    return typeof count === 'string' ? parseInt(count, 10) : Number(count || 0);
  }

  /**
   * Fetches pending DLQ entries ordered by age for inspection or operator replay.
   */
  public async getPendingEntries(limit = 100): Promise<DLQEntry[]> {
    const queryText = `
      SELECT id, record_id, record_uuid, sink_target, payload, error_code,
             error_message, stack_trace, retry_count, status, created_at, last_retried_at
      FROM dead_letter_queue
      WHERE status = 'PENDING'
      ORDER BY created_at ASC
      LIMIT $1;
    `;
    const res = await this.db.query<RawDLQRow>(queryText, [limit]);
    return res.rows.map(mapRowToDLQEntry);
  }

  /**
   * Updates state of a DLQ entry ('PENDING', 'RETRYING', 'RESOLVED', 'ABANDONED').
   */
  public async markStatus(dlqId: number, status: DLQStatus): Promise<void> {
    const queryText = `
      UPDATE dead_letter_queue
      SET status = $1,
          last_retried_at = NOW()
      WHERE id = $2;
    `;
    await this.db.query(queryText, [status, dlqId]);
  }

  /**
   * Retrieves a single DLQ entry by ID.
   */
  public async getEntryById(dlqId: number): Promise<DLQEntry | null> {
    const queryText = `
      SELECT id, record_id, record_uuid, sink_target, payload, error_code,
             error_message, stack_trace, retry_count, status, created_at, last_retried_at
      FROM dead_letter_queue
      WHERE id = $1;
    `;
    const res = await this.db.query<RawDLQRow>(queryText, [dlqId]);
    if (res.rows.length === 0) {
      return null;
    }
    return mapRowToDLQEntry(res.rows[0]);
  }

  /**
   * Increments retry_count and updates last_retried_at on failed replay attempt.
   */
  public async recordRetryFailure(dlqId: number): Promise<void> {
    const queryText = `
      UPDATE dead_letter_queue
      SET retry_count = retry_count + 1,
          last_retried_at = NOW()
      WHERE id = $1;
    `;
    await this.db.query(queryText, [dlqId]);
  }
}

function mapRowToDLQEntry(row: RawDLQRow): DLQEntry {
  let payload: Record<string, unknown>;
  if (typeof row.payload === 'string') {
    try {
      payload = JSON.parse(row.payload) as Record<string, unknown>;
    } catch {
      payload = { raw: row.payload };
    }
  } else {
    payload = row.payload || {};
  }

  return {
    id: typeof row.id === 'string' ? parseInt(row.id, 10) : Number(row.id),
    record_id: row.record_id !== null ? (typeof row.record_id === 'string' ? parseInt(row.record_id, 10) : Number(row.record_id)) : null,
    record_uuid: row.record_uuid,
    sink_target: row.sink_target as SinkTarget,
    payload,
    error_code: row.error_code,
    error_message: row.error_message,
    stack_trace: row.stack_trace,
    retry_count: typeof row.retry_count === 'string' ? parseInt(row.retry_count, 10) : Number(row.retry_count),
    status: row.status as DLQStatus,
    created_at: row.created_at instanceof Date ? row.created_at.toISOString() : String(row.created_at),
    last_retried_at: row.last_retried_at ? (row.last_retried_at instanceof Date ? row.last_retried_at.toISOString() : String(row.last_retried_at)) : null
  };
}
