/**
 * The notification service in its own Node.js process, driven by an
 * integration test over IPC, with the real console notifier writing to
 * stdout.
 *
 * Env: BOOTSTRAP, GROUP, PAYMENTS_TOPIC, ALERTS_TOPIC, RETRY_TOPIC, DLQ_TOPIC,
 * DATABASE_URL, MODE.
 *  - MODE=hang-after-send: after writing a notification the handler never
 *    returns, so the offset is never committed (the test then kills the
 *    process: "crash after sending, before committing").
 *  - MODE=normal: plain service.
 */
import { createLogger } from '@payments-kafka/consumer-runtime';
import { FAST_GROUP_CONFIG } from '@payments-kafka/consumer-runtime/testing';
import { createConsoleNotifier, type Notifier } from '../../src/notifier.js';
import { startNotificationService } from '../../src/service.js';

const env = (name: string): string => {
  const value = process.env[name];
  if (!value) throw new Error(`missing ${name}`);
  return value;
};

const send = (message: Record<string, unknown>) => process.send?.(message);

const consoleNotifier = createConsoleNotifier({ service: 'notification' });
const notifier: Notifier = {
  send: async (notification) => {
    await consoleNotifier.send(notification);
    send({ type: 'sent', notification_key: notification.notification_key });
    if (env('MODE') === 'hang-after-send') await new Promise<never>(() => undefined);
  },
};

const service = await startNotificationService({
  config: {
    kafkaBootstrapServers: env('BOOTSTRAP'),
    groupId: env('GROUP'),
    retryMaxAttempts: 2,
    retryBackoffMs: 200,
    logLevel: 'info',
  },
  databaseUrl: env('DATABASE_URL'),
  logger: createLogger({ level: 'info', bindings: { service: 'notification' } }),
  notifier,
  topics: { payments: env('PAYMENTS_TOPIC'), fraudAlerts: env('ALERTS_TOPIC') },
  retryTopic: env('RETRY_TOPIC'),
  dlqTopic: env('DLQ_TOPIC'),
  kafka: { consumer: FAST_GROUP_CONFIG },
  onPartitionsAssigned: (partitions) => send({ type: 'assigned', partitions }),
});
await service.stopped;
process.disconnect?.();
