/**
 * Test helpers for services built on the runtime:
 * `import { startKafka, ... } from '@payments-kafka/consumer-runtime/testing'`.
 *
 * They need `testcontainers` (add it to your service's devDependencies) and a
 * running Docker daemon. Nothing here touches the fixed Compose ports.
 */
import { randomUUID } from 'node:crypto';
import { createServer } from 'node:net';
import { GenericContainer, Wait, type StartedTestContainer } from 'testcontainers';
import type { RuntimeConfig } from '../config.js';
import type { FraudAlert, PaymentEvent } from '../contracts.js';
import { createAdmin, newKafka, KafkaJS, type LibrdkafkaConfig } from '../kafka.js';
import { createLogger, type Logger, type LogLevel } from '../logger.js';
import { createProducer, type OutgoingRecord } from '../producer.js';
import { decodeHeaders, TOPICS } from '../protocol.js';

// ---------------------------------------------------------------------------
// Kafka in a container
// ---------------------------------------------------------------------------

/** Same image as the Compose stack. Override with `KAFKA_TEST_IMAGE`. */
export const DEFAULT_KAFKA_IMAGE = 'apache/kafka:4.4.0';

export interface StartedKafka {
  /** `127.0.0.1:<random port>`; pass it as `kafkaBootstrapServers`. */
  bootstrapServers: string;
  container: StartedTestContainer;
  stop(): Promise<void>;
}

/**
 * Starts a single-node Kafka (KRaft) in Docker on a random free host port,
 * configured like the Compose broker (`auto.create.topics.enable=false`).
 * Takes a few seconds; start it once per test run (Vitest `globalSetup`).
 */
export async function startKafka(options: { image?: string } = {}): Promise<StartedKafka> {
  const image = options.image ?? process.env['KAFKA_TEST_IMAGE'] ?? DEFAULT_KAFKA_IMAGE;
  const hostPort = await freePort();
  const container = await new GenericContainer(image)
    .withExposedPorts({ container: 9092, host: hostPort })
    .withEnvironment({
      KAFKA_NODE_ID: '1',
      KAFKA_PROCESS_ROLES: 'broker,controller',
      KAFKA_LISTENERS: 'PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093',
      KAFKA_ADVERTISED_LISTENERS: `PLAINTEXT://127.0.0.1:${hostPort}`,
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: 'CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT',
      KAFKA_CONTROLLER_LISTENER_NAMES: 'CONTROLLER',
      KAFKA_CONTROLLER_QUORUM_VOTERS: '1@localhost:9093',
      KAFKA_INTER_BROKER_LISTENER_NAME: 'PLAINTEXT',
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: '1',
      KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS: '1',
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: '1',
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: '1',
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: '0',
      KAFKA_GROUP_MIN_SESSION_TIMEOUT_MS: '1000',
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: 'false',
      KAFKA_NUM_PARTITIONS: '1',
    })
    .withWaitStrategy(Wait.forLogMessage(/Kafka Server started/))
    .withStartupTimeout(180_000)
    .start();
  return {
    bootstrapServers: `127.0.0.1:${hostPort}`,
    container,
    stop: async () => {
      await container.stop();
    },
  };
}

function freePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const server = createServer();
    server.unref();
    server.on('error', reject);
    server.listen(0, '127.0.0.1', () => {
      const address = server.address();
      const port = typeof address === 'object' && address ? address.port : 0;
      server.close(() => resolve(port));
    });
  });
}

// ---------------------------------------------------------------------------
// Topics
// ---------------------------------------------------------------------------

export interface TopicSpec {
  topic: string;
  /** Default 1. */
  partitions?: number;
}

/** Creates topics (replication factor 1). Existing topics are left untouched. */
export async function createTopics(
  bootstrapServers: string,
  topics: ReadonlyArray<string | TopicSpec>,
): Promise<void> {
  const admin = createAdmin(bootstrapServers);
  await admin.connect();
  try {
    const existing = new Set(await admin.listTopics());
    const toCreate = topics
      .map((t) => (typeof t === 'string' ? { topic: t } : t))
      .filter((t) => !existing.has(t.topic));
    if (toCreate.length > 0) {
      await admin.createTopics({
        topics: toCreate.map((t) => ({
          topic: t.topic,
          numPartitions: t.partitions ?? 1,
          replicationFactor: 1,
        })),
      });
    }
  } finally {
    await admin.disconnect();
  }
}

/** Topic names a runtime uses; pass `retry` and `dlq` as `retryTopic`/`dlqTopic`. */
export interface RuntimeTopics {
  payments: string;
  fraudAlerts: string;
  retry: string;
  dlq: string;
}

/**
 * Creates the four pipeline topics with the partition counts of
 * docs/architecture.md (6, 6, 6, 1). With `unique: true` (default) every name
 * gets a random suffix so tests sharing one broker do not interfere; with
 * `unique: false` the real names (`pix.payments.v1`...) are used.
 */
export async function createRuntimeTopics(
  bootstrapServers: string,
  options: { unique?: boolean } = {},
): Promise<RuntimeTopics> {
  const suffix = options.unique === false ? '' : `.${uniqueId('t')}`;
  const topics: RuntimeTopics = {
    payments: `${TOPICS.payments}${suffix}`,
    fraudAlerts: `${TOPICS.fraudAlerts}${suffix}`,
    retry: `${TOPICS.retry}${suffix}`,
    dlq: `${TOPICS.dlq}${suffix}`,
  };
  await createTopics(bootstrapServers, [
    { topic: topics.payments, partitions: 6 },
    { topic: topics.fraudAlerts, partitions: 6 },
    { topic: topics.retry, partitions: 6 },
    { topic: topics.dlq, partitions: 1 },
  ]);
  return topics;
}

// ---------------------------------------------------------------------------
// Produce and collect
// ---------------------------------------------------------------------------

export type TestRecord = Omit<OutgoingRecord, 'topic'>;

/** Publishes records to one topic and waits for the broker acknowledgements. */
export async function produce(
  bootstrapServers: string,
  topic: string,
  records: readonly TestRecord[],
): Promise<void> {
  const producer = createProducer({
    bootstrapServers,
    clientId: 'test-producer',
    logger: createLogger({ level: 'error' }),
  });
  await producer.connect();
  try {
    for (const record of records) await producer.send({ ...record, topic });
  } finally {
    await producer.disconnect();
  }
}

export interface CollectedRecord {
  topic: string;
  partition: number;
  offset: string;
  key: string | null;
  /** The value as a UTF-8 string (null for a tombstone). */
  value: string | null;
  headers: Record<string, string>;
  /** `JSON.parse(value)`. */
  json<T = unknown>(): T;
}

/**
 * Reads a topic from the beginning with a throwaway consumer group.
 * - With `count`: resolves as soon as `count` records were read, and rejects
 *   if that does not happen within `timeoutMs`.
 * - Without `count`: reads for `timeoutMs` and returns everything seen (use it
 *   to assert that something did *not* arrive).
 */
export async function collect(
  bootstrapServers: string,
  topic: string,
  options: { count?: number; timeoutMs?: number } = {},
): Promise<CollectedRecord[]> {
  const timeoutMs = options.timeoutMs ?? 20_000;
  const records: CollectedRecord[] = [];
  const consumer = newKafka().consumer({
    'bootstrap.servers': bootstrapServers,
    'enable.auto.commit': false,
    'auto.offset.reset': 'earliest',
    kafkaJS: { groupId: uniqueId('collector'), logLevel: KafkaJS.logLevel.ERROR },
  });
  let done!: () => void;
  const finished = new Promise<void>((resolve) => {
    done = resolve;
  });
  await consumer.connect();
  try {
    await consumer.subscribe({ topics: [topic] });
    await consumer.run({
      eachMessage: async ({ topic: t, partition, message }) => {
        const value = message.value ? message.value.toString('utf8') : null;
        records.push({
          topic: t,
          partition,
          offset: message.offset,
          key: message.key ? message.key.toString('utf8') : null,
          value,
          headers: decodeHeaders(message.headers),
          json: <T>() => JSON.parse(value ?? 'null') as T,
        });
        if (options.count !== undefined && records.length >= options.count) done();
      },
    });
    const timer = setTimeout(done, timeoutMs);
    await finished;
    clearTimeout(timer);
  } finally {
    await consumer.disconnect();
  }
  if (options.count !== undefined && records.length < options.count) {
    throw new Error(
      `collect(${topic}): expected ${options.count} record(s) within ${timeoutMs} ms, got ${records.length}`,
    );
  }
  return records.sort((a, b) => a.partition - b.partition || Number(a.offset) - Number(b.offset));
}

/**
 * Committed offsets of a consumer group on a topic, by partition. A
 * partition without a commit maps to -1. The committed offset is the *next*
 * offset to read (last handled offset + 1).
 */
export async function committedOffsets(
  bootstrapServers: string,
  groupId: string,
  topic: string,
): Promise<Record<number, number>> {
  const admin = createAdmin(bootstrapServers);
  await admin.connect();
  try {
    const offsets: Record<number, number> = {};
    for (const { partition } of await admin.fetchTopicOffsets(topic)) offsets[partition] = -1;
    const [result] = await admin.fetchOffsets({ groupId, topics: [topic] });
    for (const { partition, offset } of result?.partitions ?? []) offsets[partition] = Number(offset);
    return offsets;
  } finally {
    await admin.disconnect();
  }
}

/** Number of members currently in a consumer group (0 if the group is empty or unknown). */
export async function groupMemberCount(bootstrapServers: string, groupId: string): Promise<number> {
  const admin = createAdmin(bootstrapServers);
  await admin.connect();
  try {
    const { groups } = await admin.describeGroups([groupId]);
    return groups[0]?.members.length ?? 0;
  } finally {
    await admin.disconnect();
  }
}

// ---------------------------------------------------------------------------
// Configuration, logging, polling, fixtures
// ---------------------------------------------------------------------------

/** A RuntimeConfig for tests: unique group id, short backoff, silent logs. */
export function testRuntimeConfig(
  bootstrapServers: string,
  overrides: Partial<RuntimeConfig> = {},
): RuntimeConfig {
  return {
    kafkaBootstrapServers: bootstrapServers,
    groupId: uniqueId('group'),
    retryMaxAttempts: 2,
    retryBackoffMs: 200,
    logLevel: 'silent',
    ...overrides,
  };
}

/**
 * librdkafka consumer settings that make group changes fast in tests (a
 * killed member is evicted after ~3 s instead of 45 s). Pass as
 * `kafka: { consumer: FAST_GROUP_CONFIG }`.
 */
export const FAST_GROUP_CONFIG: LibrdkafkaConfig = {
  'session.timeout.ms': 3000,
  'heartbeat.interval.ms': 300,
};

export interface CapturingLogger {
  logger: Logger;
  /** Every line written so far, parsed. */
  lines: Array<Record<string, unknown>>;
  /** Lines whose `msg` equals `msg`. */
  find(msg: string): Array<Record<string, unknown>>;
}

/** A JSON logger that keeps its lines in memory, for asserting on log output. */
export function createCapturingLogger(level: LogLevel = 'debug'): CapturingLogger {
  const lines: Array<Record<string, unknown>> = [];
  const logger = createLogger({
    level,
    destination: {
      write(chunk: string) {
        for (const line of chunk.split('\n')) {
          if (line.trim()) lines.push(JSON.parse(line) as Record<string, unknown>);
        }
      },
    },
  });
  return { logger, lines, find: (msg) => lines.filter((line) => line['msg'] === msg) };
}

/** Polls `fn` until it returns a truthy value, or rejects after `timeoutMs`. */
export async function waitFor<T>(
  fn: () => T | Promise<T>,
  options: { timeoutMs?: number; intervalMs?: number; message?: string } = {},
): Promise<NonNullable<T>> {
  const timeoutMs = options.timeoutMs ?? 20_000;
  const intervalMs = options.intervalMs ?? 100;
  const deadline = Date.now() + timeoutMs;
  let lastError: unknown;
  while (Date.now() < deadline) {
    try {
      const value = await fn();
      if (value) return value as NonNullable<T>;
    } catch (error) {
      lastError = error;
    }
    await sleep(intervalMs);
  }
  const reason = lastError ? ` (last error: ${(lastError as Error).message})` : '';
  throw new Error(`waitFor timed out after ${timeoutMs} ms: ${options.message ?? 'condition'}${reason}`);
}

export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/** `<prefix>-<random>`: for group ids and topic suffixes. */
export function uniqueId(prefix: string): string {
  return `${prefix}-${randomUUID().slice(0, 8)}`;
}

/** A valid payment event (random ids); override any field. */
export function makePaymentEvent(overrides: Partial<PaymentEvent> = {}): PaymentEvent {
  return {
    event_id: randomUUID(),
    event_type: 'payment.created',
    schema_version: 1,
    occurred_at: new Date().toISOString(),
    payment_id: randomUUID(),
    merchant_id: 'merchant-001',
    sequence: 1,
    amount_cents: 4590,
    currency: 'BRL',
    ...overrides,
  };
}

/** A valid fraud alert (random ids); override any field. */
export function makeFraudAlert(overrides: Partial<FraudAlert> = {}): FraudAlert {
  const triggeredBy = overrides.triggered_by_event_id ?? randomUUID();
  const rule = overrides.rule ?? 'amount_outlier';
  return {
    alert_id: `${rule}:${triggeredBy}`,
    schema_version: 1,
    rule,
    merchant_id: 'merchant-001',
    payment_id: randomUUID(),
    triggered_by_event_id: triggeredBy,
    occurred_at: new Date().toISOString(),
    details: { amount_cents: 990000, merchant_mean_cents: 5210 },
    ...overrides,
  };
}
