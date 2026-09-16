/**
 * Resilience, Checkpointing, and Dead Letter Queue (DLQ) Contracts
 */

export type PipelineStatus =
  | 'INITIALIZED'
  | 'RUNNING'
  | 'PAUSED'
  | 'COMPLETED'
  | 'FAILED';

export interface ReplicationCheckpoint {
  pipeline_id: string;
  last_processed_id: number;
  last_processed_timestamp: string | null;
  status: PipelineStatus;
  records_processed: number;
  records_failed: number;
  metadata: Record<string, unknown>;
  updated_at: string;
}

export type DLQStatus =
  | 'PENDING'
  | 'RETRYING'
  | 'RESOLVED'
  | 'ABANDONED';

export type SinkTarget =
  | 'ELASTICSEARCH'
  | 'RABBITMQ'
  | 'ALL';

export interface DLQEntry {
  id: number;
  record_id: number | null;
  record_uuid: string | null;
  sink_target: SinkTarget;
  payload: Record<string, unknown>;
  error_code: string;
  error_message: string;
  stack_trace: string | null;
  retry_count: number;
  status: DLQStatus;
  created_at: string;
  last_retried_at: string | null;
}
