/**
 * Sink-Isolated Circuit Breaker & Jittered Exponential Backoff Engine
 * Enforces Gate 3: Receiver Outage and Zero CPU Busy-Looping Resilience.
 */

import { CircuitBreakerState } from '@optio/shared';

export interface CircuitBreakerOptions {
  name: string;
  failureThreshold?: number;
  baseBackoffMs?: number;
  maxBackoffMs?: number;
  jitterMs?: number;
  halfOpenSuccessThreshold?: number;
  healthProbeFn?: () => Promise<{ healthy: boolean }>;
  sleepFn?: (ms: number) => Promise<void>;
}

export interface CircuitBreakerMetrics {
  name: string;
  state: CircuitBreakerState;
  consecutiveFailures: number;
  consecutiveSuccesses: number;
  totalTrips: number;
  currentBackoffMs: number;
  totalDowntimeMs: number;
  isThrottling: boolean;
}

export class CircuitBreaker {
  public readonly name: string;
  public readonly failureThreshold: number;
  public readonly baseBackoffMs: number;
  public readonly maxBackoffMs: number;
  public readonly jitterMs: number;
  public readonly halfOpenSuccessThreshold: number;
  private readonly healthProbeFn?: () => Promise<{ healthy: boolean }>;
  private readonly sleepFn: (ms: number) => Promise<void>;

  private state: CircuitBreakerState = 'CLOSED';
  private consecutiveFailures = 0;
  private consecutiveSuccesses = 0;
  private lastFailureTime: number | null = null;
  private nextAttemptTime: number | null = null;
  private currentBackoffMs = 0;
  private totalTrips = 0;
  private outageStartTime: number | null = null;
  private totalDowntimeMs = 0;

  constructor(options: CircuitBreakerOptions) {
    this.name = options.name;
    this.failureThreshold = options.failureThreshold ?? 3;
    this.baseBackoffMs = options.baseBackoffMs ?? 1000;
    this.maxBackoffMs = options.maxBackoffMs ?? 30000;
    this.jitterMs = options.jitterMs ?? 500;
    this.halfOpenSuccessThreshold = options.halfOpenSuccessThreshold ?? 2;
    this.healthProbeFn = options.healthProbeFn;
    this.sleepFn = options.sleepFn ?? ((ms) => new Promise((r) => setTimeout(r, ms)));
  }

  /**
   * Computes exponential backoff with full jitter:
   * backoff = min(maxBackoff, base * 2^(failures - 1)) + rand(0, jitter)
   */
  public calculateBackoffMs(failures: number): number {
    const exponent = Math.max(0, failures - 1);
    const exponential = this.baseBackoffMs * Math.pow(2, exponent);
    const capped = Math.min(this.maxBackoffMs, exponential);
    const jitter = Math.random() * this.jitterMs;
    return Math.round(capped + jitter);
  }

  /**
   * Executes a downstream sink operation with circuit breaking and anti-busy-loop throttling.
   */
  public async execute<T>(operation: () => Promise<T>): Promise<T> {
    // 1. Check if Circuit Breaker is OPEN
    if (this.state === 'OPEN') {
      const now = Date.now();

      if (this.nextAttemptTime !== null && now < this.nextAttemptTime) {
        const sleepMs = Math.max(1, this.nextAttemptTime - now);
        // Anti-Busy-Loop invariant: sleep asynchronously rather than CPU spinning
        await this.sleepFn(sleepMs);
      }

      // Check health probe if configured
      if (this.healthProbeFn) {
        try {
          const probe = await this.healthProbeFn();
          if (!probe.healthy) {
            const backoff = this.calculateBackoffMs(this.consecutiveFailures);
            this.currentBackoffMs = backoff;
            this.nextAttemptTime = Date.now() + backoff;
            throw new Error(`[CIRCUIT BREAKER: ${this.name}] Downstream sink health probe reported unhealthy.`);
          }
        } catch (probeErr: unknown) {
          const backoff = this.calculateBackoffMs(this.consecutiveFailures);
          this.currentBackoffMs = backoff;
          this.nextAttemptTime = Date.now() + backoff;
          throw probeErr;
        }
      }

      // Transition to HALF_OPEN to trial trial requests
      this.state = 'HALF_OPEN';
      this.consecutiveSuccesses = 0;
    }

    // 2. Execute protected operation
    try {
      const result = await operation();

      // On Success
      if (this.state === 'HALF_OPEN') {
        this.consecutiveSuccesses++;
        if (this.consecutiveSuccesses >= this.halfOpenSuccessThreshold) {
          // Self-healing: Sink has stabilized; close circuit breaker
          this.state = 'CLOSED';
          if (this.outageStartTime !== null) {
            this.totalDowntimeMs += Date.now() - this.outageStartTime;
            this.outageStartTime = null;
          }
          this.consecutiveFailures = 0;
          this.currentBackoffMs = 0;
          this.nextAttemptTime = null;
        }
      } else if (this.state === 'CLOSED') {
        this.consecutiveFailures = 0;
        this.currentBackoffMs = 0;
      }

      return result;
    } catch (err: unknown) {
      // On Failure
      this.consecutiveFailures++;
      this.consecutiveSuccesses = 0;
      this.lastFailureTime = Date.now();

      if (this.state === 'HALF_OPEN' || this.consecutiveFailures >= this.failureThreshold) {
        this.totalTrips++;
        if (this.outageStartTime === null) {
          this.outageStartTime = Date.now();
        }
        this.state = 'OPEN';
        const backoff = this.calculateBackoffMs(this.consecutiveFailures);
        this.currentBackoffMs = backoff;
        this.nextAttemptTime = Date.now() + backoff;
      }

      throw err;
    }
  }

  /**
   * Returns current circuit breaker state ('CLOSED' | 'OPEN' | 'HALF_OPEN').
   */
  public getState(): CircuitBreakerState {
    return this.state;
  }

  /**
   * Returns operational metrics snapshot for Gate 5 and UI telemetry.
   */
  public getMetrics(): CircuitBreakerMetrics {
    return {
      name: this.name,
      state: this.state,
      consecutiveFailures: this.consecutiveFailures,
      consecutiveSuccesses: this.consecutiveSuccesses,
      totalTrips: this.totalTrips,
      currentBackoffMs: this.currentBackoffMs,
      totalDowntimeMs: this.totalDowntimeMs,
      isThrottling: this.state !== 'CLOSED'
    };
  }

  /**
   * Resets circuit breaker to closed state with zero failures (for testing/remediation).
   */
  public reset(): void {
    this.state = 'CLOSED';
    this.consecutiveFailures = 0;
    this.consecutiveSuccesses = 0;
    this.lastFailureTime = null;
    this.nextAttemptTime = null;
    this.currentBackoffMs = 0;
    this.outageStartTime = null;
  }

  /**
   * Programmatically trips the circuit breaker to OPEN state (for Gate 3 chaos testing).
   */
  public trip(durationMs?: number): void {
    this.state = 'OPEN';
    this.totalTrips++;
    this.consecutiveFailures = Math.max(this.consecutiveFailures, this.failureThreshold);
    this.lastFailureTime = Date.now();
    if (this.outageStartTime === null) {
      this.outageStartTime = Date.now();
    }
    const backoff = durationMs ?? this.calculateBackoffMs(this.consecutiveFailures);
    this.currentBackoffMs = backoff;
    this.nextAttemptTime = Date.now() + backoff;

    if (durationMs && durationMs > 0) {
      setTimeout(() => {
        if (this.state === 'OPEN') {
          this.state = 'HALF_OPEN';
          this.consecutiveSuccesses = 0;
        }
      }, durationMs);
    }
  }
}
