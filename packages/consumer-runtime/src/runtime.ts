import type { RuntimeConfig } from './config.js';
import {
  loadContracts,
  type Contracts,
  type SchemaName,
  type SchemaTypes,
} from './contracts.js';
import { errorMessage, isNonRetryableError } from './errors.js';
import {
  ERROR_CODES,
  kafkaClientLogger,
  kafkaClientLogLevel,
  newKafka,
  type KafkaConsumer,
  type LibrdkafkaConfig,
} from './kafka.js';
import { createLogger, recordLogFields, type Logger } from './logger.js';
import { createProducer, type ManagedProducer, type RecordProducer } from './producer.js';
import {
  backoffDelayMs,
  decodeHeaders,
  dlqHeaders,
  HEADERS,
  parseRetryHeaders,
  retryHeaders,
  TOPICS,
  type DlqReason,
  type OriginalCoordinates,
} from './protocol.js';


/** A source topic and the contract its values must satisfy. */
export interface TopicSubscription<S extends SchemaName = SchemaName> {
  topic: string;
  schema: S;
}

export interface TopicPartition {
  topic: string;
  partition: number;
}

/**
 * What a handler receives. `schema` discriminates the union when a service
 * subscribes to several contracts, so `if (message.schema === 'fraud-alert')`
 * narrows `message.value` to `FraudAlert`.
 *
 * `topic`, `partition` and `offset` always describe where the record was
 * first published (for a retry, they come from the `x-original-*` headers);
 * `record` describes the Kafka record actually being consumed, which is on
 * the retry topic when `attempt > 0`.
 */
export type ConsumedMessage<S extends SchemaName = SchemaName> = S extends SchemaName
  ? {
      readonly schema: S;
      /** The parsed value, already validated against the contract. */
      readonly value: SchemaTypes[S];
      readonly topic: string;
      readonly partition: number;
      readonly offset: string;
      readonly key: string | null;
      readonly headers: Readonly<Record<string, string>>;
      /** 0 on first delivery, then the retry number (`x-retry-attempt`). */
      readonly attempt: number;
      /** Record timestamp (epoch milliseconds, as a string), as given by the client. */
      readonly timestamp: string;
      readonly record: { readonly topic: string; readonly partition: number; readonly offset: string };
    }
  : never;

export interface HandlerContext {
  /** Child logger already carrying group, topic, partition, offset, key, attempt and event_id/alert_id. */
  readonly logger: Logger;
  /** The runtime's producer (acks=all, idempotent). Await its sends before returning. */
  readonly producer: RecordProducer;
  readonly groupId: string;
}

/**
 * Processes one record. Return (or resolve) when the record is fully handled;
 * its offset is committed right after. The return value is awaited and
 * otherwise ignored. Throw to retry later; throw a `NonRetryableError` to send
 * the record to the DLQ immediately.
 */
export type MessageHandler<S extends SchemaName> = (
  message: ConsumedMessage<S>,
  ctx: HandlerContext,
) => unknown;

export interface ConsumerRuntimeOptions<S extends SchemaName> {
  /** Usually `loadConfig({ serviceName })`. */
  config: RuntimeConfig;
  /** Source topics. The retry topic is subscribed to automatically; do not list it. */
  topics: readonly TopicSubscription<S>[];
  handler: MessageHandler<S>;
  /** Defaults to `createLogger({ level: config.logLevel })`. Every line gets `group`. */
  logger?: Logger;
  /**
   * Called after partitions are assigned to this instance, before any of
   * their records is handled.
   */
  onPartitionsAssigned?: (partitions: TopicPartition[]) => unknown;
  /**
   * Called when partitions are taken away (rebalance, shutdown), after the
   * handler calls that were running for them have settled. `lost` is true
   * when the group already gave them to another member (e.g. session timeout).
   */
  onPartitionsRevoked?: (partitions: TopicPartition[], details: { lost: boolean }) => unknown;
  /** Stop gracefully on SIGTERM/SIGINT. Default `true`. */
  handleSignals?: boolean;
  /** Default `pix.payments.retry.v1`. Override only in tests. */
  retryTopic?: string;
  /** Default `pix.payments.dlq.v1`. Override only in tests. */
  dlqTopic?: string;
  /** Where the JSON Schemas live. Default `<repo>/contracts`. */
  contractsDir?: string;
  /** Maximum number of partitions handled in parallel (one record at a time each). Default 12. */
  partitionsConsumedConcurrently?: number;
  /** librdkafka overrides for the consumer and the producer (e.g. `'session.timeout.ms'`). */
  kafka?: { consumer?: LibrdkafkaConfig; producer?: LibrdkafkaConfig };
}

export interface ConsumerRuntime {
  /** Connects, subscribes and starts consuming. Resolves once the consumer is running. */
  start(): Promise<void>;
  /**
   * Graceful stop: stop fetching, let in-flight handlers finish and commit,
   * leave the group, flush the producer. Idempotent.
   */
  stop(): Promise<void>;
  /** Resolves once the runtime has stopped (after `stop()` or a termination signal). */
  readonly stopped: Promise<void>;
  /** The runtime's producer, connected between `start()` and `stop()`. */
  readonly producer: RecordProducer;
  readonly logger: Logger;
  /** Partitions currently assigned to this instance. */
  assignment(): TopicPartition[];
}

const DEFAULT_CONCURRENCY = 12;
/** Longest delay a Node.js timer accepts. */
const MAX_TIMER_MS = 2_147_483_647;
/** Upper bound for waiting on in-flight handlers during a revocation. */
const REVOKE_DRAIN_TIMEOUT_MS = 30_000;

type RuntimeState = 'new' | 'starting' | 'running' | 'stopping' | 'stopped';

/**
 * Creates a consumer for one consumer group. See the package README for the
 * full contract (commits, retry/DLQ protocol, shutdown).
 */
export function createConsumerRuntime<const S extends SchemaName>(
  options: ConsumerRuntimeOptions<S>,
): ConsumerRuntime {
  const { config } = options;
  const groupId = config.groupId;
  const retryTopic = options.retryTopic ?? TOPICS.retry;
  const dlqTopic = options.dlqTopic ?? TOPICS.dlq;
  const log = (options.logger ?? createLogger({ level: config.logLevel })).child({
    group: groupId,
  });

  const schemaByTopic = new Map<string, S>();
  if (options.topics.length === 0) throw new Error('createConsumerRuntime: topics must not be empty');
  for (const { topic, schema } of options.topics) {
    if (topic === retryTopic || topic === dlqTopic) {
      throw new Error(
        `createConsumerRuntime: do not subscribe to ${topic} explicitly; the runtime handles retry and DLQ topics`,
      );
    }
    if (schemaByTopic.has(topic)) throw new Error(`createConsumerRuntime: duplicate topic ${topic}`);
    schemaByTopic.set(topic, schema);
  }

  const contracts: Contracts = loadContracts(
    options.contractsDir ? { contractsDir: options.contractsDir } : {},
  );
  const producer: ManagedProducer = createProducer({
    bootstrapServers: config.kafkaBootstrapServers,
    logger: log,
    clientId: `${groupId}-producer`,
    ...(options.kafka?.producer ? { kafkaConfig: options.kafka.producer } : {}),
  });

  let state: RuntimeState = 'new';
  let consumer: KafkaConsumer | undefined;
  let startPromise: Promise<void> | undefined;
  let stopPromise: Promise<void> | undefined;
  let resolveStopped!: () => void;
  const stopped = new Promise<void>((resolve) => {
    resolveStopped = resolve;
  });
  /** Resume timers of partitions paused until a retry is due (or after a failed hand-off). */
  const resumeTimers = new Map<string, NodeJS.Timeout>();
  /** The record currently being processed, per partition. */
  const inFlight = new Map<string, Promise<void>>();
  const signalHandlers: Array<[NodeJS.Signals, () => void]> = [];

  const partitionKey = (topic: string, partition: number) => `${topic}:${partition}`;

  // -------------------------------------------------------------------------
  // Commits and redelivery
  // -------------------------------------------------------------------------

  async function commit(record: TopicPartition & { offset: string }, recordLog: Logger) {
    try {
      await consumer!.commitOffsets([
        {
          topic: record.topic,
          partition: record.partition,
          offset: (BigInt(record.offset) + 1n).toString(),
        },
      ]);
    } catch (error) {
      // Not fatal for correctness: the record will be delivered again (at-least-once).
      recordLog.warn(
        { err: errorMessage(error) },
        'offset commit failed; the record may be delivered again',
      );
    }
  }

  /**
   * Pauses the record's partition, rewinds it to the record and resumes it
   * after `delayMs`, so the same record is fetched again later. The consumer
   * keeps polling meanwhile, so the instance stays in the group.
   */
  function pauseAndRewind(
    record: TopicPartition & { offset: string },
    delayMs: number,
    recordLog: Logger,
    reason: string,
  ) {
    const key = partitionKey(record.topic, record.partition);
    const c = consumer!;
    c.pause([{ topic: record.topic, partitions: [record.partition] }]);
    c.seek({ topic: record.topic, partition: record.partition, offset: record.offset });
    clearTimeout(resumeTimers.get(key));
    const delay = Math.min(Math.max(delayMs, 0), MAX_TIMER_MS);
    const timer = setTimeout(() => {
      resumeTimers.delete(key);
      if (state !== 'running') return;
      const assigned = c
        .assignment()
        .some((tp) => tp.topic === record.topic && tp.partition === record.partition);
      if (!assigned) return;
      try {
        c.resume([{ topic: record.topic, partitions: [record.partition] }]);
        log.debug({ topic: record.topic, partition: record.partition }, 'partition resumed');
      } catch (error) {
        log.warn(
          { topic: record.topic, partition: record.partition, err: errorMessage(error) },
          'could not resume partition',
        );
      }
    }, delay);
    resumeTimers.set(key, timer);
    recordLog.debug(
      { reason, resume_at: new Date(Date.now() + delay).toISOString() },
      'partition paused',
    );
  }

  // -------------------------------------------------------------------------
  // Hand-off to the retry topic or the DLQ, always before the commit
  // -------------------------------------------------------------------------

  async function handOff(
    target: { topic: string; headers: Record<string, string> },
    source: {
      record: TopicPartition & { offset: string };
      key: Buffer | null;
      value: Buffer | null;
    },
    recordLog: Logger,
  ): Promise<boolean> {
    try {
      await producer.send({
        topic: target.topic,
        key: source.key,
        value: source.value ?? Buffer.alloc(0),
        headers: target.headers,
      });
      return true;
    } catch (error) {
      const delay = Math.max(config.retryBackoffMs, 1000);
      recordLog.error(
        { target_topic: target.topic, err: errorMessage(error), redeliver_in_ms: delay },
        'hand-off failed; offset not committed, record will be redelivered',
      );
      pauseAndRewind(source.record, delay, recordLog, 'hand-off failed');
      return false;
    }
  }

  async function deadLetter(
    reason: DlqReason,
    error: string,
    attempts: number,
    original: OriginalCoordinates,
    source: {
      record: TopicPartition & { offset: string };
      key: Buffer | null;
      value: Buffer | null;
    },
    recordLog: Logger,
  ) {
    const ok = await handOff(
      {
        topic: dlqTopic,
        headers: dlqHeaders({ group: groupId, reason, attempts, original, error }),
      },
      source,
      recordLog,
    );
    if (!ok) return;
    recordLog.warn({ reason, attempts, err: error, dlq_topic: dlqTopic }, 'record sent to DLQ');
    await commit(source.record, recordLog);
  }

  // -------------------------------------------------------------------------
  // The per-record pipeline
  // -------------------------------------------------------------------------

  async function processRecord(
    topic: string,
    partition: number,
    message: {
      key: Buffer | null;
      value: Buffer | null;
      offset: string;
      timestamp: string;
      headers?: Record<string, unknown>;
    },
  ): Promise<void> {
    const record = { topic, partition, offset: message.offset };
    const key = message.key ? message.key.toString('utf8') : null;
    const headers = decodeHeaders(message.headers);
    const source = { record, key: message.key, value: message.value };
    let attempt = 0;
    let original: OriginalCoordinates = record;
    let recordLog = log.child(recordLogFields({ ...record, key, attempt }));

    if (topic === retryTopic) {
      const owner = headers[HEADERS.retryGroup];
      if (owner !== groupId) {
        recordLog.debug({ owner }, 'skipping retry record owned by another group');
        await commit(record, recordLog);
        return;
      }
      const parsed = parseRetryHeaders(headers);
      if (!parsed.ok) {
        await deadLetter('invalid_message', parsed.error, 0, record, source, recordLog);
        return;
      }
      attempt = parsed.meta.attempt;
      original = parsed.meta.original;
      recordLog = log.child(recordLogFields({ ...record, key, attempt }));
      const wait = parsed.meta.notBefore - Date.now();
      if (wait > 0) {
        pauseAndRewind(record, wait, recordLog, 'retry not due yet');
        return;
      }
    }

    const schema = schemaByTopic.get(original.topic);
    if (schema === undefined) {
      await deadLetter(
        'invalid_message',
        `no schema configured for topic ${original.topic}`,
        attempt,
        original,
        source,
        recordLog,
      );
      return;
    }

    const decoded = contracts.decode(schema, message.value);
    if (!decoded.ok) {
      await deadLetter('invalid_message', decoded.error, attempt, original, source, recordLog);
      return;
    }

    recordLog = log.child(recordLogFields({ ...record, key, attempt, value: decoded.value }));
    const consumed = {
      schema,
      value: decoded.value,
      topic: original.topic,
      partition: original.partition,
      offset: original.offset,
      key,
      headers,
      attempt,
      timestamp: message.timestamp,
      record,
    } as unknown as ConsumedMessage<S>;

    try {
      await options.handler(consumed, { logger: recordLog, producer, groupId });
    } catch (error) {
      const reasonText = errorMessage(error);
      if (isNonRetryableError(error)) {
        await deadLetter('non_retryable', reasonText, attempt, original, source, recordLog);
        return;
      }
      if (attempt >= config.retryMaxAttempts) {
        await deadLetter('retries_exhausted', reasonText, attempt, original, source, recordLog);
        return;
      }
      const nextAttempt = attempt + 1;
      const delay = backoffDelayMs(config.retryBackoffMs, nextAttempt);
      const notBefore = Date.now() + delay;
      const ok = await handOff(
        {
          topic: retryTopic,
          headers: retryHeaders({
            group: groupId,
            attempt: nextAttempt,
            notBefore,
            original,
            error: reasonText,
          }),
        },
        source,
        recordLog,
      );
      if (!ok) return;
      recordLog.warn(
        { err: reasonText, next_attempt: nextAttempt, delay_ms: delay },
        'handler failed; record sent to retry topic',
      );
      await commit(record, recordLog);
      return;
    }

    recordLog.debug('record handled');
    await commit(record, recordLog);
  }

  // -------------------------------------------------------------------------
  // Rebalance hooks
  // -------------------------------------------------------------------------

  async function onRebalance(
    err: { code: number; message?: string },
    assignment: TopicPartition[],
    fns: { assignmentLost(): boolean },
  ): Promise<void> {
    const partitions = assignment.map(({ topic, partition }) => ({ topic, partition }));
    if (err.code === ERROR_CODES.ERR__ASSIGN_PARTITIONS) {
      log.info({ partitions, count: partitions.length }, 'partitions assigned');
      await runHook('onPartitionsAssigned', () => options.onPartitionsAssigned?.(partitions));
      return;
    }
    if (err.code === ERROR_CODES.ERR__REVOKE_PARTITIONS) {
      const lost = fns.assignmentLost();
      log.info({ partitions, count: partitions.length, lost }, 'partitions revoked');
      const pending: Promise<void>[] = [];
      for (const { topic, partition } of partitions) {
        const key = partitionKey(topic, partition);
        clearTimeout(resumeTimers.get(key));
        resumeTimers.delete(key);
        const running = inFlight.get(key);
        if (running) pending.push(running);
      }
      if (pending.length > 0) {
        let timer: NodeJS.Timeout | undefined;
        await Promise.race([
          Promise.allSettled(pending),
          new Promise<void>((resolve) => {
            timer = setTimeout(() => {
              log.warn('in-flight handlers still running after revocation drain timeout');
              resolve();
            }, REVOKE_DRAIN_TIMEOUT_MS);
          }),
        ]);
        clearTimeout(timer);
      }
      await runHook('onPartitionsRevoked', () =>
        options.onPartitionsRevoked?.(partitions, { lost }),
      );
      return;
    }
    log.error({ code: err.code, err: err.message }, 'rebalance error');
  }

  async function runHook(name: string, fn: () => unknown) {
    try {
      await fn();
    } catch (error) {
      log.error({ hook: name, err: errorMessage(error) }, 'rebalance hook failed');
    }
  }

  // -------------------------------------------------------------------------
  // Lifecycle
  // -------------------------------------------------------------------------

  async function doStart() {
    state = 'starting';
    try {
      await producer.connect();
      consumer = newKafka().consumer({
        'bootstrap.servers': config.kafkaBootstrapServers,
        'client.id': `${groupId}-consumer`,
        'enable.auto.commit': false,
        'auto.offset.reset': 'earliest',
        rebalance_cb: (
          err: { code: number; message?: string },
          assignment: TopicPartition[],
          fns: { assignmentLost(): boolean },
        ) => onRebalance(err, assignment, fns),
        ...options.kafka?.consumer,
        kafkaJS: {
          groupId,
          logger: kafkaClientLogger(log),
          logLevel: kafkaClientLogLevel(log),
        },
      });
      await consumer.connect();
      await consumer.subscribe({ topics: [...schemaByTopic.keys(), retryTopic] });
      await consumer.run({
        partitionsConsumedConcurrently:
          options.partitionsConsumedConcurrently ?? DEFAULT_CONCURRENCY,
        eachMessage: async ({ topic, partition, message }) => {
          const key = partitionKey(topic, partition);
          const work = processRecord(topic, partition, message).catch((error: unknown) => {
            // Defensive: processRecord handles its own errors. If something
            // unexpected escapes, do not commit and fetch the record again later.
            log.error(
              { topic, partition, offset: message.offset, err: errorMessage(error) },
              'unexpected error while processing record; it will be redelivered',
            );
            try {
              pauseAndRewind(
                { topic, partition, offset: message.offset },
                Math.max(config.retryBackoffMs, 1000),
                log,
                'unexpected error',
              );
            } catch {
              // ignore: the partition will be redelivered after a rebalance at worst
            }
          });
          inFlight.set(key, work);
          try {
            await work;
          } finally {
            if (inFlight.get(key) === work) inFlight.delete(key);
          }
        },
      });
      state = 'running';
    } catch (error) {
      log.error({ err: errorMessage(error) }, 'consumer runtime failed to start');
      await shutdownClients();
      state = 'stopped';
      resolveStopped();
      throw error;
    }

    if (options.handleSignals ?? true) installSignalHandlers();
    log.info(
      {
        topics: [...schemaByTopic.keys()],
        retry_topic: retryTopic,
        dlq_topic: dlqTopic,
        retry_max_attempts: config.retryMaxAttempts,
        retry_backoff_ms: config.retryBackoffMs,
      },
      'consumer runtime started',
    );
  }

  async function shutdownClients() {
    for (const timer of resumeTimers.values()) clearTimeout(timer);
    resumeTimers.clear();
    try {
      await consumer?.disconnect();
    } catch (error) {
      log.error({ err: errorMessage(error) }, 'error while disconnecting consumer');
    }
    try {
      await producer.disconnect();
    } catch (error) {
      log.error({ err: errorMessage(error) }, 'error while disconnecting producer');
    }
  }

  function installSignalHandlers() {
    for (const signal of ['SIGTERM', 'SIGINT'] as const) {
      const handler = () => {
        if (state === 'stopping' || state === 'stopped') {
          log.warn({ signal }, 'second termination signal; exiting immediately');
          process.exit(1);
        }
        log.info({ signal }, 'termination signal received; stopping gracefully');
        void runtime.stop();
      };
      process.on(signal, handler);
      signalHandlers.push([signal, handler]);
    }
  }

  function removeSignalHandlers() {
    for (const [signal, handler] of signalHandlers) process.off(signal, handler);
    signalHandlers.length = 0;
  }

  const runtime: ConsumerRuntime = {
    start() {
      if (state !== 'new') return Promise.reject(new Error(`cannot start: runtime is ${state}`));
      startPromise = doStart();
      return startPromise;
    },
    stop() {
      stopPromise ??= (async () => {
        if (startPromise) await startPromise.catch(() => undefined);
        if (state === 'stopped') {
          removeSignalHandlers();
          return;
        }
        if (state === 'new') {
          state = 'stopped';
          resolveStopped();
          return;
        }
        state = 'stopping';
        log.info('stopping: finishing in-flight records, then leaving the group');
        await shutdownClients();
        removeSignalHandlers();
        state = 'stopped';
        log.info('consumer runtime stopped');
        resolveStopped();
      })();
      return stopPromise;
    },
    stopped,
    producer,
    logger: log,
    assignment() {
      if (!consumer || state !== 'running') return [];
      try {
        return consumer.assignment().map(({ topic, partition }) => ({ topic, partition }));
      } catch {
        return [];
      }
    },
  };
  return runtime;
}
