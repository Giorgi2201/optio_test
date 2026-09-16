/**
 * @optio/pipeline
 * Replication daemon engine, concurrency coordinator, and HTTP observability server.
 */

import http from 'http';
import dotenv from 'dotenv';
import pg from 'pg';
import { Client as ESClient } from '@elastic/elasticsearch';

import { ensureElasticsearchIndex } from './sinks/elasticsearch/schema.js';
import { ElasticsearchSink } from './sinks/elasticsearch/elasticsearch.sink.js';
import { RabbitMQSink } from './sinks/rabbitmq/rabbitmq.sink.js';
import { SourceReader } from './source/source.reader.js';
import { CheckpointManager } from './checkpoint/checkpoint.manager.js';
import { DLQStore } from './dlq/dlq.store.js';
import { DLQReplayer } from './dlq/dlq.replayer.js';
import { ChaosService } from './simulation/chaos.service.js';
import { SearchService } from './search/search.service.js';
import { CircuitBreaker } from './resilience/circuit-breaker.js';
import { BackfillRunner } from './runners/backfill.runner.js';
import { IncrementalRunner } from './runners/incremental.runner.js';
import { PipelineCoordinator } from './coordinator/pipeline.coordinator.js';
import { createPipelineServer } from './server.js';

export * from './sinks/elasticsearch/schema.js';
export * from './sinks/elasticsearch/elasticsearch.sink.js';
export * from './sinks/rabbitmq/topology.js';
export * from './sinks/rabbitmq/rabbitmq.sink.js';
export * from './source/source.reader.js';
export * from './checkpoint/checkpoint.manager.js';
export * from './dlq/dlq.store.js';
export * from './dlq/dlq.replayer.js';
export * from './simulation/chaos.service.js';
export * from './search/search.service.js';
export * from './resilience/circuit-breaker.js';
export * from './runners/backfill.runner.js';
export * from './runners/incremental.runner.js';
export * from './coordinator/pipeline.coordinator.js';
export * from './server.js';

export interface BootstrapResult {
  coordinator: PipelineCoordinator;
  server: http.Server;
}

/**
 * Boots the entire dual-mode replication daemon, starts HTTP API, and coordinates runners.
 */
export async function bootstrap(): Promise<BootstrapResult> {
  dotenv.config();

  const databaseUrl =
    process.env.DATABASE_URL ||
    'postgres://optio:optio_secure_pass@localhost:5432/optio_db';
  const elasticsearchUrl =
    process.env.ELASTICSEARCH_URL || 'http://localhost:9200';
  const rabbitmqUrl =
    process.env.RABBITMQ_URL ||
    'amqp://optio:optio_secure_pass@localhost:5672';
  const port = parseInt(process.env.PIPELINE_PORT || process.env.PORT || '3000', 10);
  const batchSize = parseInt(process.env.BATCH_SIZE || '500', 10);
  const pollIntervalMs = parseInt(process.env.POLL_INTERVAL_MS || '1000', 10);

  const pgPool = new pg.Pool({ connectionString: databaseUrl });
  const esClient = new ESClient({ node: elasticsearchUrl });

  const esSink = new ElasticsearchSink(esClient, { indexName: 'records_search_index' });
  const rmqSink = new RabbitMQSink({ connectionString: rabbitmqUrl });

  const esCircuitBreaker = new CircuitBreaker({
    name: 'elasticsearch_breaker',
    failureThreshold: 3,
    baseBackoffMs: 1000,
    maxBackoffMs: 30000,
    healthProbeFn: () => esSink.healthCheck()
  });

  const rmqCircuitBreaker = new CircuitBreaker({
    name: 'rabbitmq_breaker',
    failureThreshold: 3,
    baseBackoffMs: 1000,
    maxBackoffMs: 30000,
    healthProbeFn: () => rmqSink.healthCheck()
  });

  const sourceReader = new SourceReader(pgPool, { batchSize });
  const checkpointManager = new CheckpointManager(pgPool);
  const dlqStore = new DLQStore(pgPool);

  const backfillRunner = new BackfillRunner(
    sourceReader,
    checkpointManager,
    dlqStore,
    esSink,
    rmqSink,
    esCircuitBreaker,
    rmqCircuitBreaker,
    { batchSize, pipelineId: 'backfill_pipeline' }
  );

  const incrementalRunner = new IncrementalRunner(
    sourceReader,
    checkpointManager,
    dlqStore,
    esSink,
    rmqSink,
    esCircuitBreaker,
    rmqCircuitBreaker,
    { batchSize, pollIntervalMs, pipelineId: 'incremental_pipeline' }
  );

  const coordinator = new PipelineCoordinator(
    backfillRunner,
    incrementalRunner,
    checkpointManager,
    dlqStore,
    esSink,
    rmqSink,
    sourceReader,
    esCircuitBreaker,
    rmqCircuitBreaker,
    pgPool
  );

  const dlqReplayer = new DLQReplayer(pgPool, dlqStore, esSink, rmqSink);
  const chaosService = new ChaosService(pgPool, sourceReader, esCircuitBreaker, rmqCircuitBreaker);
  const searchService = new SearchService(esClient, 'records_search_index');

  const server = createPipelineServer(
    coordinator,
    dlqStore,
    dlqReplayer,
    chaosService,
    searchService
  );

  await new Promise<void>((resolve) => {
    server.listen(port, () => {
      console.log(`[PIPELINE DAEMON] HTTP control & telemetry API listening on port ${port}`);
      resolve();
    });
  });

  await coordinator.start();

  const shutdown = async (signal: string) => {
    console.log(`[PIPELINE DAEMON] Received ${signal}. Initiating graceful shutdown...`);
    server.close();
    await coordinator.stop();
    process.exit(0);
  };

  process.on('SIGINT', () => shutdown('SIGINT'));
  process.on('SIGTERM', () => shutdown('SIGTERM'));

  return { coordinator, server };
}

// Auto-execute if invoked directly as CLI script
if (typeof require !== 'undefined' && typeof module !== 'undefined' && require.main === module) {
  bootstrap().catch((err) => {
    console.error('[FATAL] Failed to start pipeline daemon:', err);
    process.exit(1);
  });
}
