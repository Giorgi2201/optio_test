/**
 * Unit & Integration Test Suite for ChaosService
 * Validates poison pill injection, real-time mutation bursts, and programmatic circuit breaker tripping.
 */

import { describe, it } from 'node:test';
import assert from 'node:assert';
import { Pool } from 'pg';
import { ChaosService } from '../chaos.service.js';
import { SourceReader } from '../../source/source.reader.js';
import { CircuitBreaker } from '../../resilience/circuit-breaker.js';

describe('ChaosService - Synthetic Fault Injection & Outage Simulation', () => {
  it('1. Injects a synthetic corrupted poison pill into source_records', async () => {
    let executedQuery = '';
    let executedValues: unknown[] = [];

    const mockPgPool = {
      query: async (sql: string, values: unknown[]) => {
        executedQuery = sql;
        executedValues = values;
        return {
          rows: [{ id: '999999', uuid: '00000000-0000-0000-0000-000000000001' }]
        };
      }
    } as unknown as Pool;

    const chaos = new ChaosService(
      mockPgPool,
      {} as SourceReader,
      new CircuitBreaker({ name: 'es' }),
      new CircuitBreaker({ name: 'rmq' })
    );

    const result = await chaos.injectCorruptedRecord('tenant_test');

    assert.strictEqual(result.recordId, 999999);
    assert.strictEqual(result.uuid, '00000000-0000-0000-0000-000000000001');
    assert(executedQuery.includes('INSERT INTO source_records'));
    assert(executedQuery.includes('true')); // is_corrupted = true
    assert.strictEqual(executedValues[0], 'tenant_test');

    // Parse injected payload to verify corruption characteristics
    const payload = JSON.parse(executedValues[1] as string) as Record<string, unknown>;
    assert.strictEqual(payload.balance, 'NOT_A_VALID_NUMERIC_BALANCE');
  });

  it('2. Generates source mutations to create CDC lag traffic', async () => {
    let executedQuery = '';
    let queryLimit = 0;

    const mockPgPool = {
      query: async (sql: string, values: unknown[]) => {
        executedQuery = sql;
        queryLimit = values?.[0] as number;
        return {
          rowCount: 15
        };
      }
    } as unknown as Pool;

    const chaos = new ChaosService(
      mockPgPool,
      {} as SourceReader,
      new CircuitBreaker({ name: 'es' }),
      new CircuitBreaker({ name: 'rmq' })
    );

    const result = await chaos.generateSourceMutations(15);

    assert.strictEqual(result.mutatedCount, 15);
    assert(executedQuery.includes('UPDATE source_records'));
    assert(executedQuery.includes('updated_at = NOW()'));
    assert.strictEqual(queryLimit, 15);
  });

  it('3. Programmatically trips the circuit breaker to simulate receiver blackout (Gate 3)', async () => {
    const esBreaker = new CircuitBreaker({ name: 'elasticsearch_test' });
    const rmqBreaker = new CircuitBreaker({ name: 'rabbitmq_test' });

    assert.strictEqual(esBreaker.getState(), 'CLOSED');

    const chaos = new ChaosService(
      {} as Pool,
      {} as SourceReader,
      esBreaker,
      rmqBreaker
    );

    const result = await chaos.simulateSinkOutage('elasticsearch', 100);

    assert.strictEqual(result.target, 'elasticsearch');
    assert.strictEqual(result.durationMs, 100);
    assert.strictEqual(esBreaker.getState(), 'OPEN');
    assert.strictEqual(esBreaker.getMetrics().totalTrips, 1);

    // Wait for timeout to transition to HALF_OPEN
    await new Promise((r) => setTimeout(r, 120));
    assert.strictEqual(esBreaker.getState(), 'HALF_OPEN');
  });
});
