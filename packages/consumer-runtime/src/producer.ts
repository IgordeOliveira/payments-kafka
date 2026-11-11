import { createLogger, type Logger } from './logger.js';
import {
  kafkaClientLogger,
  kafkaClientLogLevel,
  newKafka,
  type KafkaProducer,
  type LibrdkafkaConfig,
} from './kafka.js';

/** A record to publish. */
export interface OutgoingRecord {
  topic: string;
  /** Record key; use `merchant_id` for everything on the pix.* topics. */
  key?: string | Buffer | null;
  /** A string or Buffer is sent as is; any other value is sent as `JSON.stringify(value)`. */
  value: unknown;
  headers?: Record<string, string>;
  /** Explicit partition. Leave unset to partition by key (the normal case). */
  partition?: number;
}

/** Publishes records; every `send` resolves only once the broker acknowledged (acks=all). */
export interface RecordProducer {
  send(record: OutgoingRecord): Promise<void>;
  /** Sends several records to possibly different topics; resolves when all are acknowledged. */
  sendMany(records: readonly OutgoingRecord[]): Promise<void>;
}

export interface ManagedProducer extends RecordProducer {
  connect(): Promise<void>;
  /** Flushes pending records and disconnects. Safe to call more than once. */
  disconnect(): Promise<void>;
}

export interface CreateProducerOptions {
  /** Bootstrap servers, e.g. `config.kafkaBootstrapServers`. */
  bootstrapServers: string;
  logger?: Logger;
  clientId?: string;
  /** librdkafka overrides, e.g. `{ 'message.timeout.ms': 5000 }`. */
  kafkaConfig?: LibrdkafkaConfig;
}

/**
 * Creates an idempotent producer with `acks=all` and `linger.ms=0`, suitable
 * for awaiting each record (the runtime awaits every retry/DLQ hand-off before
 * committing, and handlers await their own publishes before returning).
 */
export function createProducer(options: CreateProducerOptions): ManagedProducer {
  const logger = options.logger ?? createLogger({ level: 'warn' });
  const producer: KafkaProducer = newKafka().producer({
    'bootstrap.servers': options.bootstrapServers,
    'client.id': options.clientId ?? 'payments-kafka-producer',
    acks: -1,
    'enable.idempotence': true,
    'linger.ms': 0,
    ...options.kafkaConfig,
    kafkaJS: { logger: kafkaClientLogger(logger), logLevel: kafkaClientLogLevel(logger) },
  });

  let state: 'idle' | 'connected' | 'closed' = 'idle';

  const toMessage = (record: OutgoingRecord) => ({
    key: record.key ?? null,
    value: serializeValue(record.value),
    ...(record.headers ? { headers: record.headers } : {}),
    ...(record.partition !== undefined ? { partition: record.partition } : {}),
  });

  const managed: ManagedProducer = {
    async connect() {
      if (state !== 'idle') return;
      await producer.connect();
      state = 'connected';
    },
    async send(record) {
      await producer.send({ topic: record.topic, messages: [toMessage(record)] });
    },
    async sendMany(records) {
      await Promise.all(
        records.map((record) =>
          producer.send({ topic: record.topic, messages: [toMessage(record)] }),
        ),
      );
    },
    async disconnect() {
      if (state !== 'connected') {
        state = 'closed';
        return;
      }
      state = 'closed';
      try {
        await producer.flush({ timeout: 10_000 });
      } finally {
        await producer.disconnect();
      }
    },
  };
  return managed;
}

function serializeValue(value: unknown): string | Buffer {
  if (typeof value === 'string' || Buffer.isBuffer(value)) return value;
  return JSON.stringify(value);
}
