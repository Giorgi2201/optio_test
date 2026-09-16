/**
 * Independent Consumer Microservice Entrypoint
 */

import dotenv from 'dotenv';
import path from 'path';
import { EventConsumerService } from './consumer.service.js';
import { createConsumerServer } from './server.js';

// Load environment variables from monorepo root .env if present
const envPath = typeof __dirname !== 'undefined'
  ? path.resolve(__dirname, '../../../.env')
  : path.resolve(process.cwd(), '.env');
dotenv.config({ path: envPath });

const amqpUrl = process.env.RABBITMQ_URL || 'amqp://optio:optio_secure_pass@localhost:5672';
const queueName = process.env.RABBITMQ_QUEUE || 'replication.events.queue';
const port = parseInt(process.env.CONSUMER_PORT || '3001', 10);

const consumer = new EventConsumerService({ amqpUrl, queueName });
const server = createConsumerServer(consumer, port);

async function bootstrap() {
  console.log('======================================================================');
  console.log('      OPTIO INDEPENDENT EVENT STREAM CONSUMER SERVICE (v1.0)         ');
  console.log('======================================================================');

  // Start HTTP Telemetry Server
  server.listen(port, () => {
    console.log(`[HTTP] Consumer Observability Server listening on port ${port}`);
    console.log(`  -> Health:  http://localhost:${port}/health`);
    console.log(`  -> Metrics: http://localhost:${port}/metrics`);
  });

  // Start AMQP Consumer
  try {
    await consumer.start();
  } catch (err: unknown) {
    const msg = err instanceof Error ? err.message : String(err);
    console.warn(`[WARNING] Consumer AMQP startup delayed: ${msg}`);
    console.log('          Consumer will be ready once RabbitMQ is reachable.');
  }

  // Graceful shutdown handling
  const shutdown = async (signal: string) => {
    console.log(`\n[CONSUMER] Received ${signal}. Initiating graceful shutdown...`);
    server.close();
    await consumer.stop();
    console.log('[CONSUMER] Shutdown complete.');
    process.exit(0);
  };

  process.on('SIGINT', () => shutdown('SIGINT'));
  process.on('SIGTERM', () => shutdown('SIGTERM'));
}

if (process.env.NODE_ENV !== 'test') {
  bootstrap().catch((err) => {
    console.error('[FATAL] Uncaught consumer bootstrap error:', err);
    process.exit(1);
  });
}

export { EventConsumerService, createConsumerServer };
