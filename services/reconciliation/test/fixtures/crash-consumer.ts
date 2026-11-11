/**
 * A reconciliation consumer in its own process, killed by the redelivery test.
 *
 * Env: BOOTSTRAP, GROUP, TOPIC, RETRY_TOPIC, DLQ_TOPIC, DATABASE_URL, TARGET_EVENT, MODE.
 *  - MODE=after-commit:  for TARGET_EVENT, the database transaction commits,
 *    then the handler hangs before returning, so the Kafka offset is never
 *    committed. The process reports "committed" over IPC and waits to be killed.
 *  - MODE=before-commit: for TARGET_EVENT, every effect is written inside the
 *    transaction, then the handler hangs before COMMIT. The process reports
 *    "in-transaction" and waits to be killed (Postgres aborts the transaction
 *    when the connection drops).
 * Every other event is handled normally by the real handler.
 */
import { createConsumerRuntime, createLogger } from '@payments-kafka/consumer-runtime';
import { FAST_GROUP_CONFIG } from '@payments-kafka/consumer-runtime/testing';
import pg from 'pg';
import { createHandler } from '../../src/handler.js';

const env = (name: string): string => {
  const value = process.env[name];
  if (!value) throw new Error(`missing ${name}`);
  return value;
};

const send = (message: Record<string, unknown>) => process.send?.(message);
const forever = () => new Promise<never>(() => undefined);

const mode = env('MODE');
const target = env('TARGET_EVENT');
const pool = new pg.Pool({ connectionString: env('DATABASE_URL') });

let current: string | undefined;
const handler = createHandler(pool, {
  beforeCommit: async () => {
    if (mode === 'before-commit' && current === target) {
      send({ type: 'in-transaction', event_id: current });
      await forever();
    }
  },
});

const runtime = createConsumerRuntime({
  config: {
    kafkaBootstrapServers: env('BOOTSTRAP'),
    groupId: env('GROUP'),
    retryMaxAttempts: 4,
    retryBackoffMs: 200,
    logLevel: 'info',
  },
  logger: createLogger({ level: 'info', bindings: { service: 'reconciliation' } }),
  topics: [{ topic: env('TOPIC'), schema: 'payment-event' }],
  retryTopic: env('RETRY_TOPIC'),
  dlqTopic: env('DLQ_TOPIC'),
  kafka: { consumer: FAST_GROUP_CONFIG },
  // One partition at a time keeps `current` unambiguous.
  partitionsConsumedConcurrently: 1,
  handler: async (message, ctx) => {
    current = message.value.event_id;
    await handler(message, ctx);
    if (mode === 'after-commit' && current === target) {
      send({ type: 'committed', event_id: current });
      await forever();
    }
  },
});

await runtime.start();
send({ type: 'started' });
await runtime.stopped;
