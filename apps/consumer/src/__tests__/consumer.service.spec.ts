/**
 * Unit & Gate Resilience Test Suite for Independent Event Consumer
 * Tests: Deduplication Store, Effectively-Once Delivery (Gate 2), DLQ Routing (Gate 4), and Observability Server.
 */

import { describe, it } from 'node:test';
import assert from 'node:assert';
import http from 'http';
import { DeduplicationStore } from '../deduplication.js';
import { EventConsumerService } from '../consumer.service.js';
import { createConsumerServer } from '../server.js';
import { Channel, ConsumeMessage } from 'amqplib';

// Helper to construct mock AMQP ConsumeMessage
function createMockConsumeMessage(
  messageId: string,
  sourceId: number,
  isCorrupted = false
): ConsumeMessage {
  let content: Buffer;

  if (isCorrupted) {
    content = Buffer.from('{"is_corrupted": true, "raw": "bad_json_payload"');
  } else {
    content = Buffer.from(
      JSON.stringify({
        event_id: `evt_${messageId}`,
        event_type: 'RECORD_BACKFILLED',
        timestamp: '2026-09-01T12:00:00.000Z',
        source_id: sourceId,
        source_uuid: '00000000-0000-0000-0000-000000000001',
        version: 1,
        tenant_id: 'tenant_alpha',
        payload: {
          customer_id: `CUST-${sourceId}`,
          first_name: 'Elena',
          last_name: 'Maisuradze',
          email: 'elena@example.com',
          account_tier: 'STANDARD',
          balance: 100.0,
          metadata: { signup_channel: 'WEB', country: 'GE', tags: ['verified'] }
        },
        metadata: { pipeline_id: 'backfill', batch_sequence: 1 }
      })
    );
  }

  return {
    content,
    fields: {
      deliveryTag: sourceId,
      redelivered: false,
      exchange: 'replication.events',
      routingKey: 'record.backfilled',
      consumerTag: 'consumer_1'
    },
    properties: {
      messageId,
      timestamp: Date.now(),
      contentType: 'application/json',
      contentEncoding: 'utf8',
      headers: {},
      deliveryMode: 2,
      priority: 0,
      correlationId: undefined,
      replyTo: undefined,
      expiration: undefined,
      type: undefined,
      userId: undefined,
      appId: undefined,
      clusterId: undefined
    }
  };
}

describe('Independent Event Consumer & Deduplication Engine', () => {
  it('1. DeduplicationStore correctly tracks keys and enforces bounded capacity', () => {
    const store = new DeduplicationStore(3); // Small capacity to test FIFO eviction

    assert.strictEqual(store.isDuplicate('msg_1'), false);
    store.record('msg_1');
    assert.strictEqual(store.isDuplicate('msg_1'), true);

    store.record('msg_2');
    store.record('msg_3');
    assert.strictEqual(store.size(), 3);

    // Record 4th key; msg_1 should be evicted
    store.record('msg_4');
    assert.strictEqual(store.size(), 3);
    assert.strictEqual(store.isDuplicate('msg_1'), false, 'Oldest key must be evicted');
    assert.strictEqual(store.isDuplicate('msg_4'), true);
  });

  it('2. Processes unique message: increments uniqueProcessed and acks message', async () => {
    const acknowledged: number[] = [];
    const nacked: number[] = [];

    const mockChannel = {
      ack: (msg: ConsumeMessage) => acknowledged.push(msg.fields.deliveryTag),
      nack: (msg: ConsumeMessage) => nacked.push(msg.fields.deliveryTag)
    } as unknown as Channel;

    const consumer = new EventConsumerService();
    consumer.setMockChannel(mockChannel);

    const msg = createMockConsumeMessage('rec_100_v1', 100);
    await consumer.handleMessage(msg, mockChannel);

    const metrics = consumer.getMetrics();
    assert.strictEqual(metrics.totalReceived, 1);
    assert.strictEqual(metrics.uniqueProcessed, 1);
    assert.strictEqual(metrics.duplicatesPrevented, 0);
    assert.strictEqual(metrics.deadLettered, 0);
    assert.deepStrictEqual(acknowledged, [100]);
    assert.strictEqual(nacked.length, 0);
  });

  it('3. Redelivered message (Gate 2): receives identical messageId, increments duplicatesPrevented, acks without re-executing', async () => {
    const acknowledged: number[] = [];
    const mockChannel = {
      ack: (msg: ConsumeMessage) => acknowledged.push(msg.fields.deliveryTag),
      nack: () => {}
    } as unknown as Channel;

    const consumer = new EventConsumerService();
    consumer.setMockChannel(mockChannel);

    // Initial message
    const msg1 = createMockConsumeMessage('rec_200_v1', 200);
    await consumer.handleMessage(msg1, mockChannel);

    // Redelivered identical message (simulate at-least-once transport retry after crash)
    const msg2 = createMockConsumeMessage('rec_200_v1', 200);
    await consumer.handleMessage(msg2, mockChannel);

    const metrics = consumer.getMetrics();
    assert.strictEqual(metrics.totalReceived, 2);
    assert.strictEqual(metrics.uniqueProcessed, 1, 'Unique processed count must remain 1');
    assert.strictEqual(metrics.duplicatesPrevented, 1, 'Duplicate must be intercepted');
    assert.strictEqual(acknowledged.length, 2, 'Both deliveries must be acknowledged');
  });

  it('4. Corrupted payload (Gate 4): rejects with nack(false, false) for DLQ routing', async () => {
    const nackedArgs: Array<{ deliveryTag: number; allUpTo: boolean; requeue: boolean }> = [];
    const mockChannel = {
      ack: () => {},
      nack: (msg: ConsumeMessage, allUpTo: boolean, requeue: boolean) => {
        nackedArgs.push({ deliveryTag: msg.fields.deliveryTag, allUpTo, requeue });
      }
    } as unknown as Channel;

    const consumer = new EventConsumerService();
    consumer.setMockChannel(mockChannel);

    const poisonedMsg = createMockConsumeMessage('rec_999_v1', 999, true);
    await consumer.handleMessage(poisonedMsg, mockChannel);

    const metrics = consumer.getMetrics();
    assert.strictEqual(metrics.totalReceived, 1);
    assert.strictEqual(metrics.uniqueProcessed, 0);
    assert.strictEqual(metrics.deadLettered, 1);
    assert.strictEqual(nackedArgs.length, 1);
    assert.strictEqual(nackedArgs[0].deliveryTag, 999);
    assert.strictEqual(nackedArgs[0].requeue, false, 'requeue must be false to trigger DLQ');
  });

  it('5. HTTP Observability Server responds with metrics matching internal state', async () => {
    const consumer = new EventConsumerService();
    const server = createConsumerServer(consumer, 0); // Port 0 for random free port

    await new Promise<void>((resolve) => server.listen(0, resolve));
    const address = server.address() as { port: number };
    const port = address.port;

    try {
      // Simulate 1 unique message processed
      const mockChannel = { ack: () => {}, nack: () => {} } as unknown as Channel;
      consumer.setMockChannel(mockChannel);
      await consumer.handleMessage(createMockConsumeMessage('rec_500_v1', 500), mockChannel);

      const response = await new Promise<string>((resolve, reject) => {
        http.get(`http://localhost:${port}/metrics`, (res) => {
          assert.strictEqual(res.statusCode, 200);
          let data = '';
          res.on('data', (chunk) => (data += chunk));
          res.on('end', () => resolve(data));
        }).on('error', reject);
      });

      const metricsJson = JSON.parse(response);
      assert.strictEqual(metricsJson.totalReceived, 1);
      assert.strictEqual(metricsJson.uniqueProcessed, 1);
      assert.strictEqual(metricsJson.dedupStoreSize, 1);
    } finally {
      server.close();
    }
  });
});
