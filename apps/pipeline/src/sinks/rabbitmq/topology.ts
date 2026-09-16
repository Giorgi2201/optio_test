/**
 * RabbitMQ AMQP Topology Provisioner
 * Declares exchanges, queues, DLX, and bindings with durability and resilience invariants.
 */

import { Channel } from 'amqplib';

export interface RabbitMQTopologyConfig {
  exchangeName: string;
  queueName: string;
  dlxExchangeName: string;
  dlxQueueName: string;
  routingPattern: string;
}

export const DEFAULT_TOPOLOGY_CONFIG: RabbitMQTopologyConfig = {
  exchangeName: 'replication.events',
  queueName: 'replication.events.queue',
  dlxExchangeName: 'replication.dlq.exchange',
  dlxQueueName: 'replication.dlq.queue',
  routingPattern: 'record.*'
};

export interface RabbitMQTopologyReport {
  exchange: string;
  queue: string;
  dlxExchange: string;
  dlxQueue: string;
  declared: boolean;
}

/**
 * Declares durable AMQP topology idempotently.
 */
export async function ensureRabbitMQTopology(
  channel: Channel,
  customConfig?: Partial<RabbitMQTopologyConfig>
): Promise<RabbitMQTopologyReport> {
  const config: RabbitMQTopologyConfig = {
    ...DEFAULT_TOPOLOGY_CONFIG,
    ...customConfig
  };

  // 1. Declare Dead-Letter Exchange (DLX)
  await channel.assertExchange(config.dlxExchangeName, 'topic', {
    durable: true
  });

  // 2. Declare Dead-Letter Queue and bind to DLX
  await channel.assertQueue(config.dlxQueueName, {
    durable: true
  });
  await channel.bindQueue(config.dlxQueueName, config.dlxExchangeName, '#');

  // 3. Declare Main Events Exchange
  await channel.assertExchange(config.exchangeName, 'topic', {
    durable: true
  });

  // 4. Declare Main Events Queue with dead-letter forwarding to DLX
  await channel.assertQueue(config.queueName, {
    durable: true,
    arguments: {
      'x-dead-letter-exchange': config.dlxExchangeName
    }
  });

  // 5. Bind Main Queue to Main Exchange for 'record.*' patterns
  await channel.bindQueue(
    config.queueName,
    config.exchangeName,
    config.routingPattern
  );

  return {
    exchange: config.exchangeName,
    queue: config.queueName,
    dlxExchange: config.dlxExchangeName,
    dlxQueue: config.dlxQueueName,
    declared: true
  };
}
