/**
 * Production-Grade RabbitMQ Event Stream Sink
 * Implements AMQP Confirm Channels, flow control (drain backpressure),
 * and deterministic message deduplication keys (Gate 2 & Gate 4).
 */

import amqplib, { ChannelModel, ConfirmChannel } from 'amqplib';
import crypto from 'crypto';
import { SourceRecord, ReplicationEvent, ReplicationEventType } from '@optio/shared';
import { ensureRabbitMQTopology } from './topology.js';

export interface BatchPublishFailure {
  recordId: number;
  errorReason: string;
  errorCode: string;
  rawItem?: unknown;
}

export interface BatchPublishResult {
  successCount: number;
  failedCount: number;
  successfulIds: number[];
  failures: BatchPublishFailure[];
}

export interface RabbitMQSinkOptions {
  connectionString?: string;
  exchangeName?: string;
  pipelineId?: string;
}

export interface RabbitMQHealthResult {
  healthy: boolean;
  latencyMs: number;
  connectionState: string;
  error?: string;
}

export class RabbitMQSink {
  private readonly connectionString: string;
  private readonly exchangeName: string;
  private readonly pipelineId: string;
  private connection: ChannelModel | null = null;
  private channel: ConfirmChannel | null = null;
  private isConnected: boolean = false;

  constructor(options: RabbitMQSinkOptions = {}) {
    this.connectionString =
      options.connectionString ||
      process.env.RABBITMQ_URL ||
      'amqp://optio:optio_secure_pass@localhost:5672';
    this.exchangeName = options.exchangeName || 'replication.events';
    this.pipelineId = options.pipelineId || 'pipeline_daemon';
  }

  /**
   * Establishes AMQP connection, creates a ConfirmChannel, and ensures durable topology.
   */
  public async connect(): Promise<void> {
    if (this.isConnected && this.channel) {
      return;
    }

    const conn = await amqplib.connect(this.connectionString);
    this.connection = conn;

    conn.on('error', (err) => {
      this.isConnected = false;
      console.error('[RABBITMQ SINK] Connection error:', err.message);
    });

    conn.on('close', () => {
      this.isConnected = false;
      this.channel = null;
    });

    const ch = await conn.createConfirmChannel();
    this.channel = ch;

    ch.on('error', (err) => {
      console.error('[RABBITMQ SINK] Channel error:', err.message);
    });

    ch.on('close', () => {
      this.channel = null;
    });

    // Ensure exchanges, queues, and DLX are declared
    await ensureRabbitMQTopology(ch, { exchangeName: this.exchangeName });
    this.isConnected = true;
  }

  /**
   * Closes active channel and connection gracefully.
   */
  public async disconnect(): Promise<void> {
    try {
      if (this.channel) {
        await this.channel.close();
      }
      if (this.connection) {
        await this.connection.close();
      }
    } catch {
      // Graceful teardown
    } finally {
      this.channel = null;
      this.connection = null;
      this.isConnected = false;
    }
  }

  /**
   * Healthcheck probe for Gate 5 and Circuit Breakers.
   */
  public async healthCheck(): Promise<RabbitMQHealthResult> {
    const start = Date.now();
    if (!this.isConnected || !this.channel) {
      return {
        healthy: false,
        latencyMs: 0,
        connectionState: 'DISCONNECTED',
        error: 'AMQP channel is not connected'
      };
    }

    try {
      // Check that channel is open and responsive
      await this.channel.checkExchange(this.exchangeName);
      const latencyMs = Date.now() - start;
      return {
        healthy: true,
        latencyMs,
        connectionState: 'CONNECTED'
      };
    } catch (err: unknown) {
      const latencyMs = Date.now() - start;
      const errorMsg = err instanceof Error ? err.message : String(err);
      return {
        healthy: false,
        latencyMs,
        connectionState: 'ERROR',
        error: errorMsg
      };
    }
  }

  /**
   * Transforms a relational SourceRecord into a canonical ReplicationEvent.
   */
  public transformRecord(
    record: SourceRecord,
    eventType: ReplicationEventType,
    batchSequence = 1
  ): ReplicationEvent {
    return {
      event_id: crypto.randomUUID(),
      event_type: eventType,
      timestamp: new Date().toISOString(),
      source_id: record.id,
      source_uuid: record.uuid,
      version: record.version,
      tenant_id: record.tenant_id,
      payload: record.payload,
      metadata: {
        pipeline_id: this.pipelineId,
        batch_sequence: batchSequence
      }
    };
  }

  /**
   * Publishes a bounded batch of records with publisher confirmations.
   * Isolates poisoned records (Gate 4) and handles AMQP socket backpressure.
   */
  public async publishBatch(
    records: SourceRecord[],
    eventType: ReplicationEventType
  ): Promise<BatchPublishResult> {
    if (records.length === 0) {
      return {
        successCount: 0,
        failedCount: 0,
        successfulIds: [],
        failures: []
      };
    }

    if (!this.channel) {
      throw new Error('Cannot publish batch: RabbitMQ ConfirmChannel is not connected.');
    }

    const successfulIds: number[] = [];
    const failures: BatchPublishFailure[] = [];
    const inFlightIds: number[] = [];

    const routingKey =
      eventType === 'RECORD_BACKFILLED'
        ? 'record.backfilled'
        : 'record.mutated';

    for (let i = 0; i < records.length; i++) {
      const record = records[i];

      // Gate 4: Isolate poison pills / corrupted records before sending to broker
      if (record.is_corrupted) {
        failures.push({
          recordId: record.id,
          errorReason: `Synthetic poison pill detected on source record ID ${record.id}`,
          errorCode: 'AMQP_PAYLOAD_CORRUPTED',
          rawItem: record
        });
        continue;
      }

      try {
        const event = this.transformRecord(record, eventType, i + 1);
        const buffer = Buffer.from(JSON.stringify(event));

        // Gate 2: Deterministic messageId and persistent deliveryMode (2)
        const publishOptions: amqplib.Options.Publish = {
          persistent: true,
          messageId: `rec_${record.id}_v${record.version}`,
          timestamp: Date.now(),
          contentType: 'application/json',
          headers: {
            'x-source-id': record.id,
            'x-version': record.version,
            'x-tenant-id': record.tenant_id
          }
        };

        const canContinue = this.channel.publish(
          this.exchangeName,
          routingKey,
          buffer,
          publishOptions
        );

        inFlightIds.push(record.id);

        // Flow control / backpressure handling
        if (!canContinue) {
          await new Promise<void>((resolve) => {
            this.channel!.once('drain', () => resolve());
          });
        }
      } catch (err: unknown) {
        const errorMsg = err instanceof Error ? err.message : String(err);
        failures.push({
          recordId: record.id,
          errorReason: `Serialization/publish error: ${errorMsg}`,
          errorCode: 'AMQP_PUBLISH_EXCEPTION',
          rawItem: record
        });
      }
    }

    // Await broker confirmation for all in-flight messages in the batch
    if (inFlightIds.length > 0) {
      try {
        await this.channel.waitForConfirms();
        successfulIds.push(...inFlightIds);
      } catch (confirmErr: unknown) {
        const errorMsg = confirmErr instanceof Error ? confirmErr.message : String(confirmErr);
        // Broker NACKed the batch
        for (const id of inFlightIds) {
          failures.push({
            recordId: id,
            errorReason: `Broker NACK on batch confirm: ${errorMsg}`,
            errorCode: 'AMQP_BROKER_NACK'
          });
        }
      }
    }

    return {
      successCount: successfulIds.length,
      failedCount: failures.length,
      successfulIds,
      failures
    };
  }

  // Method to allow unit tests to inject mock channels
  public setMockChannel(channel: ConfirmChannel): void {
    this.channel = channel;
    this.isConnected = true;
  }
}
