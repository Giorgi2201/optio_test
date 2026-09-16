/**
 * Gate 5 Observability & UI Dashboard Telemetry Contracts
 */

export type CircuitBreakerState = 'CLOSED' | 'OPEN' | 'HALF_OPEN';

export interface ComponentHealth {
  status: 'UP' | 'DOWN' | 'DEGRADED';
  latency_ms: number;
  message?: string;
}

export interface SystemHealth {
  overall: 'HEALTHY' | 'DEGRADED' | 'DOWN';
  postgres: ComponentHealth;
  elasticsearch: ComponentHealth;
  rabbitmq: ComponentHealth;
}

export interface PipelineTelemetry {
  backfill_cursor: number;
  backfill_total_records: number;
  backfill_completion_pct: number;
  current_throughput_eps: number;
  incremental_lag_records: number;
  incremental_lag_ms: number;
  dlq_pending_count: number;
  health: SystemHealth;
  timestamp: string;
}
