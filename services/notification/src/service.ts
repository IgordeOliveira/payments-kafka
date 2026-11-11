import {
  createConsumerRuntime,
  createLogger,
  TOPICS,
  type ConsumerRuntime,
  type ConsumerRuntimeOptions,
  type Logger,
  type RuntimeConfig,
} from '@payments-kafka/consumer-runtime';
import type pg from 'pg';
import { SERVICE_NAME } from './config.js';
import { createPool, runMigrations } from './db.js';
import { createClaim } from './dedupe.js';
import { createNotificationHandler } from './handler.js';
import { createConsoleNotifier, type Notifier } from './notifier.js';

type RuntimeOverrides = Pick<
  ConsumerRuntimeOptions<'payment-event' | 'fraud-alert'>,
  'handleSignals' | 'retryTopic' | 'dlqTopic' | 'contractsDir' | 'kafka' | 'onPartitionsAssigned' | 'onPartitionsRevoked'
>;

export interface NotificationServiceOptions extends RuntimeOverrides {
  config: RuntimeConfig;
  databaseUrl: string;
  logger?: Logger;
  /** Defaults to the console notifier (JSON lines on stdout). */
  notifier?: Notifier;
  /** Source topics. Default `pix.payments.v1` and `pix.fraud-alerts.v1`; override only in tests. */
  topics?: { payments: string; fraudAlerts: string };
}

export interface NotificationService {
  runtime: ConsumerRuntime;
  pool: pg.Pool;
  /** Graceful stop: the runtime first, then the database pool. Idempotent. */
  stop(): Promise<void>;
  /** Resolves once the runtime stopped (signal or `stop()`) and the pool is closed. */
  stopped: Promise<void>;
}

/** Applies the migrations, then starts consuming. Rejects (after cleaning up) if either fails. */
export async function startNotificationService(options: NotificationServiceOptions): Promise<NotificationService> {
  const { config, databaseUrl, notifier, topics, logger: givenLogger, ...runtimeOverrides } = options;
  const logger = givenLogger ?? createLogger({ level: config.logLevel, bindings: { service: SERVICE_NAME } });
  const pool = createPool(databaseUrl, logger);
  let runtime: ConsumerRuntime;
  try {
    await runMigrations(pool, { logger });
    runtime = createConsumerRuntime({
      ...runtimeOverrides,
      config,
      logger,
      topics: [
        { topic: topics?.payments ?? TOPICS.payments, schema: 'payment-event' },
        { topic: topics?.fraudAlerts ?? TOPICS.fraudAlerts, schema: 'fraud-alert' },
      ],
      handler: createNotificationHandler({
        claim: createClaim(pool),
        notifier: notifier ?? createConsoleNotifier({ service: SERVICE_NAME }),
      }),
    });
    await runtime.start();
  } catch (error) {
    await pool.end().catch(() => undefined);
    throw error;
  }
  const stopped = runtime.stopped.then(() => pool.end());
  return {
    runtime,
    pool,
    stopped,
    stop: async () => {
      await runtime.stop();
      await stopped;
    },
  };
}
