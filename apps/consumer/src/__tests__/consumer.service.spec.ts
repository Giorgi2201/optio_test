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
import { Channel, ChannelModel, ConsumeMessage } from 'amqplib';

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

  // --- Startup ordering & broker-restart resilience -------------------------------------

  interface FakeBroker {
    connectCalls: number;
    assertedQueues: Array<{ queue: string; options: unknown }>;
    assertedExchanges: string[];
    bindings: Array<{ queue: string; exchange: string; pattern: string }>;
    consumedQueues: string[];
    closeHandlers: Array<() => void>;
    connect: (url: string) => Promise<ChannelModel>;
  }

  function createFakeBroker(failuresBeforeSuccess = 0): FakeBroker {
    const broker: FakeBroker = {
      connectCalls: 0,
      assertedQueues: [],
      assertedExchanges: [],
      bindings: [],
      consumedQueues: [],
      closeHandlers: [],
      connect: async () => {
        broker.connectCalls++;
        if (broker.connectCalls <= failuresBeforeSuccess) {
          throw new Error('ECONNREFUSED broker not ready');
        }
        const channel = {
          on: () => channel,
          assertExchange: async (exchange: string) => {
            broker.assertedExchanges.push(exchange);
            return { exchange };
          },
          assertQueue: async (queue: string, options: unknown) => {
            broker.assertedQueues.push({ queue, options });
            return { queue, messageCount: 0, consumerCount: 0 };
          },
          bindQueue: async (queue: string, exchange: string, pattern: string) => {
            broker.bindings.push({ queue, exchange, pattern });
            return {};
          },
          prefetch: async () => {},
          consume: async (queue: string) => {
            broker.consumedQueues.push(queue);
            return { consumerTag: `ctag-${broker.connectCalls}` };
          },
          cancel: async () => {},
          close: async () => {},
          ack: () => {},
          nack: () => {}
        };
        const connection = {
          on: (event: string, handler: () => void) => {
            if (event === 'close') broker.closeHandlers.push(handler);
            return connection;
          },
          createChannel: async () => channel,
          close: async () => {}
        };
        return connection as unknown as ChannelModel;
      }
    };
    return broker;
  }

  it('6. Declares the queue idempotently before consuming so it can boot before the pipeline', async () => {
    const broker = createFakeBroker();
    const consumer = new EventConsumerService({
      queueName: 'replication.events.queue',
      exchangeName: 'replication.events',
      dlxExchangeName: 'replication.dlq.exchange',
      connect: broker.connect
    });

    await consumer.start();

    // Must mirror apps/pipeline/src/sinks/rabbitmq/topology.ts exactly, or the broker rejects with PRECONDITION_FAILED.
    assert.deepStrictEqual(broker.assertedExchanges, ['replication.events']);
    assert.deepStrictEqual(broker.assertedQueues, [
      {
        queue: 'replication.events.queue',
        options: { durable: true, arguments: { 'x-dead-letter-exchange': 'replication.dlq.exchange' } }
      }
    ]);
    assert.deepStrictEqual(broker.bindings, [
      { queue: 'replication.events.queue', exchange: 'replication.events', pattern: 'record.*' }
    ]);
    assert.deepStrictEqual(broker.consumedQueues, ['replication.events.queue']);
    assert.strictEqual(consumer.healthCheck().healthy, true);

    await consumer.stop();
  });

  it('7. Supervised start retries with bounded backoff until the broker accepts the connection', async () => {
    const broker = createFakeBroker(2);
    const consumer = new EventConsumerService({
      connect: broker.connect,
      baseBackoffMs: 5,
      maxBackoffMs: 20
    });

    consumer.runSupervised();

    const deadline = Date.now() + 2000;
    while (!consumer.healthCheck().healthy && Date.now() < deadline) {
      await new Promise((r) => setTimeout(r, 5));
    }

    assert.strictEqual(consumer.healthCheck().healthy, true, 'consumer must eventually come up');
    assert.strictEqual(broker.connectCalls, 3, 'two refused attempts, then success');
    assert.deepStrictEqual(broker.consumedQueues, ['replication.events.queue']);

    await consumer.stop();
  });

  it('8. Reconnects automatically after the broker drops the connection (broker restart)', async () => {
    const broker = createFakeBroker();
    const consumer = new EventConsumerService({
      connect: broker.connect,
      baseBackoffMs: 5,
      maxBackoffMs: 20
    });

    consumer.runSupervised();
    let deadline = Date.now() + 2000;
    while (!consumer.healthCheck().healthy && Date.now() < deadline) {
      await new Promise((r) => setTimeout(r, 5));
    }
    assert.strictEqual(broker.connectCalls, 1);

    // Simulate the broker closing the connection out from under us.
    broker.closeHandlers[0]();
    assert.strictEqual(consumer.healthCheck().healthy, false, 'health must reflect the dropped connection');

    deadline = Date.now() + 2000;
    while (broker.connectCalls < 2 && Date.now() < deadline) {
      await new Promise((r) => setTimeout(r, 5));
    }
    while (!consumer.healthCheck().healthy && Date.now() < deadline) {
      await new Promise((r) => setTimeout(r, 5));
    }

    assert.strictEqual(broker.connectCalls, 2, 'a fresh connection must be established');
    assert.strictEqual(consumer.healthCheck().healthy, true);
    assert.deepStrictEqual(broker.consumedQueues, ['replication.events.queue', 'replication.events.queue']);

    await consumer.stop();
    // stop() must cancel any pending reconnect so the process can exit.
    broker.closeHandlers.forEach((h) => h());
    await new Promise((r) => setTimeout(r, 50));
    assert.strictEqual(broker.connectCalls, 2, 'no reconnect after an intentional stop');
  });
});
