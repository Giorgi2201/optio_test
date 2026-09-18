/**
 * Independent RabbitMQ Event Consumer Service
 * Delivers effectively-once processing over at-least-once transport
 * with deterministic deduplication and DLQ routing (Gate 2 & Gate 4).
 */

import amqplib, { ChannelModel, Channel, ConsumeMessage } from 'amqplib';
import { DeduplicationStore } from './deduplication.js';
import { ReplicationEvent } from '@optio/shared';

export interface ConsumerMetrics {
  totalReceived: number;
  uniqueProcessed: number;
  duplicatesPrevented: number;
  deadLettered: number;
  dedupStoreSize: number;
  lastProcessedAt: string | null;
}

export type AmqpConnectFn = (url: string) => Promise<ChannelModel>;

export interface ConsumerServiceOptions {
  amqpUrl?: string;
  queueName?: string;
  /** Exchange the queue is bound to. Must match the pipeline's topology declaration. */
  exchangeName?: string;
  /** Dead-letter exchange attached to the queue. Must match the pipeline's declaration exactly. */
  dlxExchangeName?: string;
  routingPattern?: string;
  prefetch?: number;
  deduplicationStore?: DeduplicationStore;
  /** Bounded exponential backoff (with full jitter) used by the supervised reconnect loop. */
  baseBackoffMs?: number;
  maxBackoffMs?: number;
  /** Injectable connector for tests; defaults to amqplib.connect. */
  connect?: AmqpConnectFn;
}

export class EventConsumerService {
  private readonly amqpUrl: string;
  private readonly queueName: string;
  private readonly exchangeName: string;
  private readonly dlxExchangeName: string;
  private readonly routingPattern: string;
  private readonly prefetch: number;
  private readonly deduplicationStore: DeduplicationStore;
  private readonly baseBackoffMs: number;
  private readonly maxBackoffMs: number;
  private readonly connect: AmqpConnectFn;

  private connection: ChannelModel | null = null;
  private channel: Channel | null = null;
  private consumerTag: string | null = null;
  private isRunning = false;

  private stopping = false;
  private reconnectAttempts = 0;
  private reconnectTimer: NodeJS.Timeout | null = null;

  private metrics = {
    totalReceived: 0,
    uniqueProcessed: 0,
    duplicatesPrevented: 0,
    deadLettered: 0,
    lastProcessedAt: null as string | null
  };

  constructor(options: ConsumerServiceOptions = {}) {
    this.amqpUrl =
      options.amqpUrl ||
      process.env.RABBITMQ_URL ||
      'amqp://optio:optio_secure_pass@localhost:5672';
    this.queueName = options.queueName || 'replication.events.queue';
    this.exchangeName = options.exchangeName || 'replication.events';
    this.dlxExchangeName = options.dlxExchangeName || 'replication.dlq.exchange';
    this.routingPattern = options.routingPattern || 'record.*';
    this.prefetch = options.prefetch || 100;
    this.deduplicationStore =
      options.deduplicationStore || new DeduplicationStore(500000);
    this.baseBackoffMs = options.baseBackoffMs ?? 1000;
    this.maxBackoffMs = options.maxBackoffMs ?? 30000;
    this.connect = options.connect || ((url) => amqplib.connect(url));
  }

  /**
   * Supervised entrypoint for long-running deployments. Starts consuming and, on any startup
   * failure or dropped connection, retries with bounded exponential backoff and full jitter.
   * Never throws: the consumer must be able to boot before the broker (or the pipeline's
   * topology declaration) exists and must survive a broker restart without operator action.
   */
  public runSupervised(): void {
    this.stopping = false;
    void this.attemptStart();
  }

  private async attemptStart(): Promise<void> {
    if (this.stopping) return;
    try {
      await this.start();
      this.reconnectAttempts = 0;
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      this.scheduleReconnect(msg);
    }
  }

  private scheduleReconnect(reason: string): void {
    if (this.stopping || this.reconnectTimer) return;
    const cap = Math.min(this.maxBackoffMs, this.baseBackoffMs * 2 ** this.reconnectAttempts);
    // Full jitter in [cap/2, cap]: never a tight loop, never a synchronized herd.
    const delayMs = Math.round(cap / 2 + Math.random() * (cap / 2));
    this.reconnectAttempts++;
    console.warn(
      `[CONSUMER] AMQP consumption unavailable (${reason}); retrying in ${delayMs}ms (attempt ${this.reconnectAttempts}).`
    );
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      void this.attemptStart();
    }, delayMs);
  }

  /**
   * Connects to RabbitMQ, declares the queue idempotently (so startup order relative to the
   * pipeline does not matter), configures prefetch flow control, and starts consuming.
   * Throws on failure; use runSupervised() for automatic retry.
   */
  public async start(): Promise<void> {
    if (this.isRunning) return;

    let conn: ChannelModel | null = null;
    try {
      conn = await this.connect(this.amqpUrl);
      this.connection = conn;

      conn.on('error', (err) => {
        console.error('[CONSUMER] AMQP Connection error:', err.message);
      });

      conn.on('close', () => {
        const wasRunning = this.isRunning;
        this.isRunning = false;
        this.channel = null;
        this.connection = null;
        if (wasRunning && !this.stopping) {
          this.scheduleReconnect('connection closed');
        }
      });

      const ch = await conn.createChannel();
      this.channel = ch;

      ch.on('error', (err) => {
        console.error('[CONSUMER] Channel error:', err.message);
      });

      // Idempotent topology declaration. Arguments MUST mirror the pipeline's
      // ensureRabbitMQTopology() exactly, otherwise the broker rejects with PRECONDITION_FAILED.
      // Without this, a consumer that boots before the pipeline fails with 404 NOT_FOUND on consume.
      await ch.assertExchange(this.exchangeName, 'topic', { durable: true });
      await ch.assertQueue(this.queueName, {
        durable: true,
        arguments: { 'x-dead-letter-exchange': this.dlxExchangeName }
      });
      await ch.bindQueue(this.queueName, this.exchangeName, this.routingPattern);

      // Flow control: bounded prefetch prevents heap exhaustion under burst
      await ch.prefetch(this.prefetch);

      const consumeResult = await ch.consume(
        this.queueName,
        async (msg) => {
          await this.handleMessage(msg, ch);
        },
        { noAck: false }
      );

      this.consumerTag = consumeResult.consumerTag;
      this.isRunning = true;
      console.log(`[CONSUMER] Listening on queue '${this.queueName}' (prefetch=${this.prefetch})`);
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      console.error(`[CONSUMER] Failed to start consumer: ${msg}`);
      // Release the half-open connection so a retry does not leak sockets.
      this.channel = null;
      this.connection = null;
      this.isRunning = false;
      if (conn) {
        await conn.close().catch(() => {});
      }
      throw err;
    }
  }

  /**
   * Processes a single inbound message.
   * Isolates poisoned payloads to DLQ (Gate 4) and prevents duplicates (Gate 2).
   */
  public async handleMessage(
    msg: ConsumeMessage | null,
    channelOverride?: Channel
  ): Promise<void> {
    if (!msg) return;

    const ch = channelOverride || this.channel;
    if (!ch) {
      throw new Error('No active channel available to acknowledge/reject message.');
    }

    this.metrics.totalReceived++;

    const rawContent = msg.content.toString('utf8');
    let event: ReplicationEvent | null = null;
    let isCorrupted = false;

    // 1. Gate 4 Validation: Check for malformed JSON or synthetic chaos corruption
    try {
      if (rawContent.includes('"is_corrupted":true') || rawContent.includes('"is_corrupted": true')) {
        isCorrupted = true;
      } else {
        event = JSON.parse(rawContent) as ReplicationEvent;
        if (!event || typeof event !== 'object' || !event.event_id) {
          isCorrupted = true;
        }
      }
    } catch {
      isCorrupted = true;
    }

    if (isCorrupted || !event) {
      this.metrics.deadLettered++;
      // nack with requeue=false automatically routes to dead_letter_queue via DLX
      ch.nack(msg, false, false);
      return;
    }

    // 2. Gate 2 Effectively-Once: Deduplication via deterministic messageId
    const messageId =
      msg.properties.messageId ||
      `rec_${event.source_id}_v${event.version}` ||
      event.event_id;

    if (this.deduplicationStore.isDuplicate(messageId)) {
      this.metrics.duplicatesPrevented++;
      // Acknowledge message so broker removes duplicate, but skip business execution
      ch.ack(msg);
      return;
    }

    // 3. Record unique message and process business logic
    this.deduplicationStore.record(messageId);
    this.metrics.uniqueProcessed++;
    this.metrics.lastProcessedAt = new Date().toISOString();

    // Acknowledge successful processing
    ch.ack(msg);
  }

  /**
   * Retrieves current metrics snapshot.
   */
  public getMetrics(): ConsumerMetrics {
    return {
      totalReceived: this.metrics.totalReceived,
      uniqueProcessed: this.metrics.uniqueProcessed,
      duplicatesPrevented: this.metrics.duplicatesPrevented,
      deadLettered: this.metrics.deadLettered,
      dedupStoreSize: this.deduplicationStore.size(),
      lastProcessedAt: this.metrics.lastProcessedAt
    };
  }

  /**
   * Resets metrics and clears deduplication cache (for isolated verification runs).
   */
  public resetMetrics(): void {
    this.metrics = {
      totalReceived: 0,
      uniqueProcessed: 0,
      duplicatesPrevented: 0,
      deadLettered: 0,
      lastProcessedAt: null
    };
    this.deduplicationStore.clear();
  }

  /**
   * Healthcheck probe for Gate 5 and cluster monitoring.
   */
  public healthCheck(): {
    healthy: boolean;
    connection: string;
    metrics: ConsumerMetrics;
  } {
    const isHealthy = this.isRunning && this.channel !== null;
    return {
      healthy: isHealthy,
      connection: isHealthy ? 'CONNECTED' : 'DISCONNECTED',
      metrics: this.getMetrics()
    };
  }

  /**
   * Graceful shutdown.
   */
  public async stop(): Promise<void> {
    this.stopping = true;
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    try {
      if (this.channel && this.consumerTag) {
        await this.channel.cancel(this.consumerTag).catch(() => {});
      }
      if (this.channel) {
        await this.channel.close().catch(() => {});
      }
      if (this.connection) {
        await this.connection.close().catch(() => {});
      }
    } finally {
      this.channel = null;
      this.connection = null;
      this.isRunning = false;
    }
  }

  public setMockChannel(channel: Channel): void {
    this.channel = channel;
    this.isRunning = true;
  }
}
