/**
 * Chaos Simulation Service
 * Powers UI Panel 4 and automated chaos tests (poison injection, mutation bursts, circuit breaker outages).
 */

import { Pool } from 'pg';
import { SourceReader } from '../source/source.reader.js';
import { CircuitBreaker } from '../resilience/circuit-breaker.js';

export interface CorruptedRecordResult {
  recordId: number;
  uuid: string;
}

export interface MutationBurstResult {
  mutatedCount: number;
}

export interface SinkOutageResult {
  target: 'elasticsearch' | 'rabbitmq';
  durationMs: number;
}

export class ChaosService {
  private readonly pgPool: Pool;
  private readonly sourceReader: SourceReader;
  private readonly esCircuitBreaker: CircuitBreaker;
  private readonly rmqCircuitBreaker: CircuitBreaker;

  constructor(
    pgPool: Pool,
    sourceReader: SourceReader,
    esCircuitBreaker: CircuitBreaker,
    rmqCircuitBreaker: CircuitBreaker
  ) {
    this.pgPool = pgPool;
    this.sourceReader = sourceReader;
    this.esCircuitBreaker = esCircuitBreaker;
    this.rmqCircuitBreaker = rmqCircuitBreaker;
  }

  /**
   * Injects a synthetic corrupted poison pill into source_records designed to trigger
   * Elasticsearch / schema parsing rejections for Gate 4 DLQ verification.
   */
  public async injectCorruptedRecord(tenantId?: string): Promise<CorruptedRecordResult> {
    const corruptedPayload = {
      customer_id: `corrupted_cust_${Date.now()}`,
      first_name: 'Poison',
      last_name: 'Pill',
      email: 'malformed-email-address',
      account_tier: 'UNSUPPORTED_TIER_TYPE',
      balance: 'NOT_A_VALID_NUMERIC_BALANCE', // String balance causes Elasticsearch double mapping failure
      metadata: {
        corrupted: true,
        injected_by: 'chaos_service',
        timestamp: new Date().toISOString()
      }
    };

    const query = `
      INSERT INTO source_records (
        uuid, tenant_id, payload, version, status, is_corrupted, created_at, updated_at
      )
      VALUES (
        gen_random_uuid(),
        $1,
        $2::jsonb,
        1,
        'ACTIVE',
        true,
        NOW(),
        NOW()
      )
      RETURNING id, uuid;
    `;

    const res = await this.pgPool.query<{ id: string | number; uuid: string }>(query, [
      tenantId || 'tenant_chaos',
      JSON.stringify(corruptedPayload)
    ]);

    const row = res.rows[0];
    const recordId = typeof row.id === 'string' ? parseInt(row.id, 10) : Number(row.id);

    console.log(
      `[CHAOS SERVICE] Injected poison pill record #${recordId} (${row.uuid}) with corrupted payload.`
    );

    return {
      recordId,
      uuid: row.uuid
    };
  }

  /**
   * Updates existing source records with new timestamps and modified balances to simulate
   * real-time write traffic for Gate 5 incremental replication lag observation.
   */
  public async generateSourceMutations(count: number): Promise<MutationBurstResult> {
    const targetCount = Math.max(1, count);

    const updateQuery = `
      WITH candidates AS (
        SELECT id FROM source_records
        ORDER BY RANDOM()
        LIMIT $1
      )
      UPDATE source_records s
      SET version = s.version + 1,
          payload = jsonb_set(
            s.payload,
            '{balance}',
            to_jsonb(COALESCE((s.payload->>'balance')::numeric, 0) + 15.75)
          ),
          updated_at = NOW()
      FROM candidates c
      WHERE s.id = c.id;
    `;

    const res = await this.pgPool.query(updateQuery, [targetCount]);
    let mutatedCount = res.rowCount || 0;

    // Fallback: If table is empty, insert new mutations
    if (mutatedCount === 0) {
      for (let i = 0; i < targetCount; i++) {
        await this.pgPool.query(
          `INSERT INTO source_records (uuid, tenant_id, payload, version, status, is_corrupted, created_at, updated_at)
           VALUES (gen_random_uuid(), 'tenant_chaos', $1::jsonb, 1, 'ACTIVE', false, NOW(), NOW())`,
          [
            JSON.stringify({
              customer_id: `mutation_${Date.now()}_${i}`,
              first_name: 'Mutation',
              last_name: `User${i}`,
              email: `mutation_${i}@example.com`,
              account_tier: 'STANDARD',
              balance: 200 + i * 10,
              metadata: { tag: 'chaos_burst', generated_at: new Date().toISOString() }
            })
          ]
        );
        mutatedCount++;
      }
    }

    console.log(`[CHAOS SERVICE] Generated ${mutatedCount} source mutations for CDC lag tracking.`);
    return { mutatedCount };
  }

  /**
   * Programmatically trips the designated Circuit Breaker to OPEN state for the specified duration,
   * testing Gate 3 receiver blackout resilience without stopping background processes.
   */
  public async simulateSinkOutage(
    target: 'elasticsearch' | 'rabbitmq',
    durationMs: number
  ): Promise<SinkOutageResult> {
    const duration = Math.max(100, durationMs);
    const breaker = target === 'elasticsearch' ? this.esCircuitBreaker : this.rmqCircuitBreaker;

    breaker.trip(duration);
    console.log(
      `[CHAOS SERVICE] Programmatically tripped '${breaker.name}' to OPEN state for ${duration}ms.`
    );

    return {
      target,
      durationMs: duration
    };
  }
}
