/**
 * Unit & Gate 3 Resilience Test Suite for CircuitBreaker
 * Validates 3-state transitions, anti-busy-loop throttling, exponential backoff, and self-healing.
 */

import { describe, it } from 'node:test';
import assert from 'node:assert';
import { CircuitBreaker } from '../circuit-breaker.js';

describe('CircuitBreaker - Gate 3 Receiver Outage & Zero Busy-Loop Resilience', () => {
  it('1. Executes normally in CLOSED state without delays', async () => {
    const breaker = new CircuitBreaker({ name: 'test_es_breaker' });
    let executed = false;

    const result = await breaker.execute(async () => {
      executed = true;
      return 'OK';
    });

    assert.strictEqual(result, 'OK');
    assert.strictEqual(executed, true);
    assert.strictEqual(breaker.getState(), 'CLOSED');
    assert.strictEqual(breaker.getMetrics().consecutiveFailures, 0);
  });

  it('2. Trips from CLOSED to OPEN after 3 consecutive failures', async () => {
    const breaker = new CircuitBreaker({
      name: 'test_breaker',
      failureThreshold: 3,
      baseBackoffMs: 100,
      jitterMs: 0
    });

    const failingOp = async () => {
      throw new Error('Connection refused: Elasticsearch node down');
    };

    // Failure 1
    await assert.rejects(breaker.execute(failingOp), /Connection refused/);
    assert.strictEqual(breaker.getState(), 'CLOSED');
    assert.strictEqual(breaker.getMetrics().consecutiveFailures, 1);

    // Failure 2
    await assert.rejects(breaker.execute(failingOp), /Connection refused/);
    assert.strictEqual(breaker.getState(), 'CLOSED');
    assert.strictEqual(breaker.getMetrics().consecutiveFailures, 2);

    // Failure 3: Reaches threshold, trips to OPEN
    await assert.rejects(breaker.execute(failingOp), /Connection refused/);
    assert.strictEqual(breaker.getState(), 'OPEN');
    assert.strictEqual(breaker.getMetrics().consecutiveFailures, 3);
    assert.strictEqual(breaker.getMetrics().totalTrips, 1);
  });

  it('3. Anti-Busy-Loop Invariant: In OPEN state, sleeps asynchronously rather than CPU spinning', async () => {
    let sleepDurationCaptured = 0;

    const breaker = new CircuitBreaker({
      name: 'test_breaker',
      failureThreshold: 1,
      baseBackoffMs: 500,
      jitterMs: 0,
      sleepFn: async (ms) => {
        sleepDurationCaptured = ms;
      }
    });

    // Trip the breaker immediately with 1 failure
    await assert.rejects(
      breaker.execute(async () => {
        throw new Error('Sink down');
      })
    );
    assert.strictEqual(breaker.getState(), 'OPEN');

    // Next execution should trigger sleepFn (anti-busy-loop throttling)
    const successResult = await breaker.execute(async () => 'recovered_data');

    assert.strictEqual(successResult, 'recovered_data');
    assert.ok(
      sleepDurationCaptured >= 450 && sleepDurationCaptured <= 550,
      `sleepFn must have been called with ~500ms, was ${sleepDurationCaptured}ms`
    );
  });

  it('4. Calculates exponential backoff doubling on consecutive failures capped at maxBackoffMs', () => {
    const breaker = new CircuitBreaker({
      name: 'test_breaker',
      baseBackoffMs: 1000,
      maxBackoffMs: 10000,
      jitterMs: 0
    });

    // 1 failure: base * 2^0 = 1000
    assert.strictEqual(breaker.calculateBackoffMs(1), 1000);
    // 2 failures: base * 2^1 = 2000
    assert.strictEqual(breaker.calculateBackoffMs(2), 2000);
    // 3 failures: base * 2^2 = 4000
    assert.strictEqual(breaker.calculateBackoffMs(3), 4000);
    // 4 failures: base * 2^3 = 8000
    assert.strictEqual(breaker.calculateBackoffMs(4), 8000);
    // 5 failures: capped at 10000
    assert.strictEqual(breaker.calculateBackoffMs(5), 10000);
    // 10 failures: capped at 10000
    assert.strictEqual(breaker.calculateBackoffMs(10), 10000);
  });

  it('5. Gate 3 Self-Healing Contract: Recovers seamlessly from outage (OPEN -> HALF_OPEN -> CLOSED)', async () => {
    let sinkOnline = false;
    let sleepCalls = 0;

    const breaker = new CircuitBreaker({
      name: 'es_self_healing',
      failureThreshold: 2,
      halfOpenSuccessThreshold: 2,
      baseBackoffMs: 200,
      jitterMs: 0,
      sleepFn: async () => {
        sleepCalls++;
      }
    });

    const protectedSinkOperation = async (payload: string) => {
      if (!sinkOnline) {
        throw new Error('503 Service Unavailable: Elasticsearch cluster down');
      }
      return `indexed_${payload}`;
    };

    // Phase 1: Sink is down, inject 2 failures to trip breaker
    await assert.rejects(breaker.execute(() => protectedSinkOperation('batch_1')));
    await assert.rejects(breaker.execute(() => protectedSinkOperation('batch_1')));
    assert.strictEqual(breaker.getState(), 'OPEN');
    assert.strictEqual(breaker.getMetrics().isThrottling, true);

    // Phase 2: Downstream sink recovers
    sinkOnline = true;

    // Phase 3: Execute next batch -> transitions to HALF_OPEN, succeeds 1st trial
    const res1 = await breaker.execute(() => protectedSinkOperation('batch_1'));
    assert.strictEqual(res1, 'indexed_batch_1');
    assert.strictEqual(breaker.getState(), 'HALF_OPEN');

    // Phase 4: Execute next batch -> succeeds 2nd trial, closes breaker
    const res2 = await breaker.execute(() => protectedSinkOperation('batch_2'));
    assert.strictEqual(res2, 'indexed_batch_2');
    assert.strictEqual(breaker.getState(), 'CLOSED');
    assert.strictEqual(breaker.getMetrics().isThrottling, false);
    assert.strictEqual(breaker.getMetrics().consecutiveFailures, 0);
    assert.ok(sleepCalls >= 1, 'Anti-busy-loop sleep must have prevented CPU spinning');
  });

  it('6. Metrics snapshot accurately captures telemetry for Gate 5 dashboard', () => {
    const breaker = new CircuitBreaker({ name: 'rmq_breaker' });
    const metrics = breaker.getMetrics();

    assert.strictEqual(metrics.name, 'rmq_breaker');
    assert.strictEqual(metrics.state, 'CLOSED');
    assert.strictEqual(metrics.consecutiveFailures, 0);
    assert.strictEqual(metrics.totalTrips, 0);
    assert.strictEqual(metrics.isThrottling, false);
  });
});
