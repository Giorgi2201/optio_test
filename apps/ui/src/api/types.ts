/**
 * HTTP response contracts for the pipeline control & observability server (apps/pipeline/src/server.ts).
 * Domain models are imported from @optio/shared; only transport-level envelopes are declared here.
 */

import type { DLQEntry, SearchDocument, SystemHealth } from '@optio/shared';

export type { PipelineTelemetry, SystemHealth, ComponentHealth, CircuitBreakerTelemetry, DLQEntry, SearchDocument } from '@optio/shared';

export interface HealthResponse {
  status: SystemHealth['overall'];
  health: SystemHealth;
  timestamp: string;
}

export interface SearchQuery {
  q?: string;
  tier?: string;
  page?: number;
  limit?: number;
}

export interface SearchResponse {
  total: number;
  page: number;
  limit: number;
  documents: SearchDocument[];
  error?: string;
}

export interface DLQListResponse {
  entries: DLQEntry[];
  count: number;
}

export interface DLQRetryResponse {
  success: boolean;
  error?: string;
}

export interface DLQRetryAllResponse {
  retried: number;
  resolved: number;
  failed: number;
  error?: string;
}

export type RunnerAction = 'pause' | 'resume';

export interface ControlResponse {
  status: 'PAUSED' | 'RUNNING';
  pipeline: 'backfill' | 'incremental';
}

export interface InjectCorruptionResponse {
  recordId: number;
  uuid: string;
}

export interface MutateSourceResponse {
  mutatedCount: number;
}

export type SinkName = 'elasticsearch' | 'rabbitmq';

export interface TripBreakerResponse {
  target: SinkName;
  durationMs: number;
}
