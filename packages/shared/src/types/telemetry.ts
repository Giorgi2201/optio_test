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

export interface CircuitBreakerTelemetry {
  name?: string;
  state: CircuitBreakerState;
  consecutiveFailures?: number;
  consecutiveSuccesses?: number;
  totalTrips?: number;
  currentBackoffMs?: number;
  totalDowntimeMs?: number;
  isThrottling?: boolean;
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
  circuit_breakers?: {
    elasticsearch: CircuitBreakerTelemetry;
    rabbitmq: CircuitBreakerTelemetry;
  };
  timestamp: string;
}
