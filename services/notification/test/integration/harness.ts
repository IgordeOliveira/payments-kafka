import {
  committedOffsets,
  createCapturingLogger,
  createRuntimeTopics,
  FAST_GROUP_CONFIG,
  testRuntimeConfig,
  waitFor,
  type CapturingLogger,
  type RuntimeTopics,
} from '@payments-kafka/consumer-runtime/testing';
import type { RuntimeConfig } from '@payments-kafka/consumer-runtime';
import { afterEach, inject } from 'vitest';
import { createConsoleNotifier, type Notifier } from '../../src/notifier.js';
import { startNotificationService, type NotificationService } from '../../src/service.js';
import { createDatabase } from './postgres.js';

export const bootstrapServers = inject('kafkaBootstrapServers');
export const postgresAdminUrl = inject('postgresAdminUrl');

const running: NotificationService[] = [];

afterEach(async () => {
  await Promise.all(running.splice(0).map((service) => service.stop()));
});

export function freshTopics(): Promise<RuntimeTopics> {
  return createRuntimeTopics(bootstrapServers);
}

export function freshDatabase(): Promise<string> {
  return createDatabase(postgresAdminUrl);
}

/** The parsed JSON lines the console notifier wrote. */
export type NotificationLine = Record<string, unknown> & { type: string; kind: string; notification_key: string };

/**
 * The real console notifier, writing into memory instead of stdout. Pass a
 * shared one to several instances to count notifications across them.
 */
export function capturingNotifier(): { notifier: Notifier; lines: NotificationLine[] } {
  const lines: NotificationLine[] = [];
  const notifier = createConsoleNotifier({
    service: 'notification',
    write: (line) => lines.push(JSON.parse(line) as NotificationLine),
  });
  return { notifier, lines };
}

export interface StartedTestService {
  service: NotificationService;
  log: CapturingLogger;
  config: RuntimeConfig;
}

/**
 * Starts the notification service against the test broker, the given topics
 * and database, with a capturing logger and fast group settings, and waits
 * until it owns partitions. Stopped automatically after each test.
 */
export async function startService(args: {
  topics: RuntimeTopics;
  databaseUrl: string;
  notifier: Notifier;
  config?: Partial<RuntimeConfig>;
  waitForAssignment?: boolean;
}): Promise<StartedTestService> {
  const log = createCapturingLogger('debug');
  const config = testRuntimeConfig(bootstrapServers, args.config);
  const service = await startNotificationService({
    config,
    databaseUrl: args.databaseUrl,
    logger: log.logger,
    notifier: args.notifier,
    topics: { payments: args.topics.payments, fraudAlerts: args.topics.fraudAlerts },
    retryTopic: args.topics.retry,
    dlqTopic: args.topics.dlq,
    handleSignals: false,
    kafka: { consumer: FAST_GROUP_CONFIG },
  });
  running.push(service);
  if (args.waitForAssignment ?? true) {
    await waitFor(() => service.runtime.assignment().length > 0, { message: 'partitions assigned' });
  }
  return { service, log, config };
}

/** Log lines with a given `msg` and `notification_key`. */
export function logsFor(log: CapturingLogger, msg: string, key: string): Array<Record<string, unknown>> {
  return log.find(msg).filter((line) => line['notification_key'] === key);
}

/** Waits until `groupId` committed `count` records in total on `topic`. */
export async function waitForCommitted(groupId: string, topic: string, count: number): Promise<void> {
  await waitFor(
    async () => {
      const offsets = await committedOffsets(bootstrapServers, groupId, topic);
      return Object.values(offsets).reduce((sum, offset) => sum + Math.max(offset, 0), 0) === count;
    },
    { message: `${count} records committed on ${topic}`, timeoutMs: 60_000 },
  );
}
