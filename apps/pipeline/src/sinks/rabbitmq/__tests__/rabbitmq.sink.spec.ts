/**
 * Unit & Resilience Test Suite for RabbitMQSink
 * Validates confirm channels, flow control (drain), deterministic message IDs, and Gate 4 isolation.
 */

import { describe, it } from 'node:test';
import assert from 'node:assert';
import { EventEmitter } from 'events';
import { RabbitMQSink } from '../rabbitmq.sink.js';
import { ensureRabbitMQTopology } from '../topology.js';
import { SourceRecord } from '@optio/shared';
import { ConfirmChannel } from 'amqplib';

// Helper to create mock SourceRecord
function createMockRecord(id: number, isCorrupted = false): SourceRecord {
  return {
    id,
    uuid: `00000000-0000-0000-0000-${String(id).padStart(12, '0')}`,
    tenant_id: 'tenant_alpha',
    payload: {
      customer_id: `CUST-${id}`,
      first_name: 'Giorgi',
      last_name: 'Beridze',
      email: `user.${id}@example.com`,
      account_tier: 'PREMIUM',
      balance: 1250.50,
      metadata: {
        signup_channel: 'WEB',
        country: 'GE',
        tags: ['verified', 'kyc_complete']
      }
    },
    version: 1,
    status: 'ACTIVE',
    is_corrupted: isCorrupted,
    created_at: '2026-09-01T12:00:00.000Z',
    updated_at: '2026-09-01T12:00:00.000Z'
  };
}

describe('RabbitMQSink Adapter & AMQP Topology', () => {
  it('1. Declares durable topology (exchanges, queues, DLX) idempotently', async () => {
    const assertedExchanges: string[] = [];
    const assertedQueues: string[] = [];
    const boundQueues: Array<{ queue: string; exchange: string; pattern: string }> = [];

    const mockChannel = {
      assertExchange: async (name: string, type: string, opts: unknown) => {
        assertedExchanges.push(name);
        return { exchange: name };
      },
      assertQueue: async (name: string, opts: unknown) => {
        assertedQueues.push(name);
        return { queue: name, messageCount: 0, consumerCount: 0 };
      },
      bindQueue: async (queue: string, exchange: string, pattern: string) => {
        boundQueues.push({ queue, exchange, pattern });
      }
    } as unknown as ConfirmChannel;

    const report = await ensureRabbitMQTopology(mockChannel);

    assert.ok(report.declared);
    assert.ok(assertedExchanges.includes('replication.events'));
    assert.ok(assertedExchanges.includes('replication.dlq.exchange'));
    assert.ok(assertedQueues.includes('replication.events.queue'));
    assert.ok(assertedQueues.includes('replication.dlq.queue'));
    assert.ok(boundQueues.some((b) => b.queue === 'replication.events.queue' && b.pattern === 'record.*'));
    assert.ok(boundQueues.some((b) => b.queue === 'replication.dlq.queue' && b.pattern === '#'));
  });

  it('2. Transforms SourceRecord to ReplicationEvent with deterministic messageId format', () => {
    const sink = new RabbitMQSink();
    const record = createMockRecord(77);
    const event = sink.transformRecord(record, 'RECORD_MUTATED', 5);

    assert.strictEqual(event.event_type, 'RECORD_MUTATED');
    assert.strictEqual(event.source_id, 77);
    assert.strictEqual(event.source_uuid, record.uuid);
    assert.strictEqual(event.version, 1);
    assert.strictEqual(event.tenant_id, 'tenant_alpha');
    assert.strictEqual(event.payload.customer_id, 'CUST-77');
    assert.strictEqual(event.metadata.batch_sequence, 5);
    assert.ok(event.event_id, 'event_id must be generated');
  });

  it('3. Publishes batch awaiting publisher confirms (Gate 2 Effectively-Once)', async () => {
    const sink = new RabbitMQSink({ exchangeName: 'replication.events' });
    const publishedMessages: Array<{ exchange: string; routingKey: string; options: any }> = [];
    let confirmsAwaited = false;

    const mockChannel = {
      publish: (exchange: string, routingKey: string, content: Buffer, options: any) => {
        publishedMessages.push({ exchange, routingKey, options });
        return true;
      },
      waitForConfirms: async () => {
        confirmsAwaited = true;
      }
    } as unknown as ConfirmChannel;

    sink.setMockChannel(mockChannel);

    const records = [createMockRecord(1), createMockRecord(2), createMockRecord(3)];
    const result = await sink.publishBatch(records, 'RECORD_BACKFILLED');

    assert.strictEqual(result.successCount, 3);
    assert.strictEqual(result.failedCount, 0);
    assert.deepStrictEqual(result.successfulIds, [1, 2, 3]);
    assert.ok(confirmsAwaited, 'waitForConfirms must be awaited for persistence guarantee');

    // Verify deterministic messageId and persistence options
    assert.strictEqual(publishedMessages.length, 3);
    assert.strictEqual(publishedMessages[0].options.messageId, 'rec_1_v1');
    assert.strictEqual(publishedMessages[0].options.persistent, true);
    assert.strictEqual(publishedMessages[0].routingKey, 'record.backfilled');
  });

  it('4. Handles flow control backpressure: pauses until "drain" when socket buffer full', async () => {
    const sink = new RabbitMQSink();
    const emitter = new EventEmitter();
    let drainWaited = false;
    let publishCount = 0;

    const mockChannel = Object.assign(emitter, {
      publish: () => {
        publishCount++;
        if (publishCount === 1) {
          // Simulate socket buffer full on first message
          setTimeout(() => {
            drainWaited = true;
            emitter.emit('drain');
          }, 10);
          return false;
        }
        return true;
      },
      waitForConfirms: async () => {}
    }) as unknown as ConfirmChannel;

    sink.setMockChannel(mockChannel);

    const records = [createMockRecord(10), createMockRecord(20)];
    const result = await sink.publishBatch(records, 'RECORD_MUTATED');

    assert.strictEqual(result.successCount, 2);
    assert.ok(drainWaited, 'Publisher must have paused and waited for drain event');
  });

  it('5. Gate 4 Partial Batch Isolation: 497 records confirmed, 3 poisoned records isolated to DLQ', async () => {
    const sink = new RabbitMQSink();
    const publishedIds: number[] = [];

    const mockChannel = {
      publish: (exchange: string, routingKey: string, content: Buffer, options: any) => {
        publishedIds.push(options.headers['x-source-id']);
        return true;
      },
      waitForConfirms: async () => {}
    } as unknown as ConfirmChannel;

    sink.setMockChannel(mockChannel);

    const totalRecords = 500;
    const records: SourceRecord[] = [];
    const corruptedIds = new Set([10, 250, 499]);

    for (let i = 1; i <= totalRecords; i++) {
      records.push(createMockRecord(i, corruptedIds.has(i)));
    }

    const result = await sink.publishBatch(records, 'RECORD_BACKFILLED');

    assert.strictEqual(result.successCount, 497, '497 valid records must succeed');
    assert.strictEqual(result.failedCount, 3, '3 poisoned records must be rejected');
    assert.strictEqual(result.successfulIds.length, 497);
    assert.strictEqual(result.failures.length, 3);

    const isolatedIds = result.failures.map((f) => f.recordId);
    assert.deepStrictEqual(isolatedIds, [10, 250, 499]);

    for (const failure of result.failures) {
      assert.strictEqual(failure.errorCode, 'AMQP_PAYLOAD_CORRUPTED');
      assert.ok(failure.errorReason.includes('Synthetic poison pill detected'));
    }

    // Ensure corrupted IDs were never sent over wire to broker
    for (const badId of corruptedIds) {
      assert.ok(!publishedIds.includes(badId), `Corrupted ID ${badId} must never be published to broker`);
    }
  });
});
