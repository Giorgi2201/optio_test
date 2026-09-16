/**
 * RabbitMQ Event Stream Contracts
 * Payloads published to AMQP exchanges and processed by downstream consumers.
 */

import { CustomerPayload } from './source.js';

export type ReplicationEventType =
  | 'RECORD_BACKFILLED'
  | 'RECORD_MUTATED'
  | 'RECORD_DELETED';

export interface ReplicationEventMetadata {
  pipeline_id: string;
  batch_sequence: number;
}

export interface ReplicationEvent {
  event_id: string; // UUIDv4 deduplication identifier
  event_type: ReplicationEventType;
  timestamp: string; // ISO 8601 publish timestamp
  source_id: number;
  source_uuid: string;
  version: number;
  tenant_id: string;
  payload: CustomerPayload;
  metadata: ReplicationEventMetadata;
}
