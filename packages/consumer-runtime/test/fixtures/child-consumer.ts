/**
 * A consumer process driven by an integration test over IPC.
 *
 * Env: BOOTSTRAP, GROUP, TOPIC, RETRY_TOPIC, DLQ_TOPIC, MODE.
 *  - MODE=hang:    the handler never returns (the test kills the process).
 *  - MODE=release: the handler waits for a "release" IPC message.
 * The test may send "sigterm" over IPC to emulate a SIGTERM on platforms
 * (Windows) where a real one cannot be delivered to a Node.js process.
 */
import { createConsumerRuntime } from '../../src/index.js';
import { createLogger } from '../../src/logger.js';
import { FAST_GROUP_CONFIG } from '../../src/testing/index.js';

const env = (name: string): string => {
  const value = process.env[name];
  if (!value) throw new Error(`missing ${name}`);
  return value;
};

const send = (message: Record<string, unknown>) => process.send?.(message);

let release: (() => void) | undefined;
process.on('message', (message) => {
  if (message === 'release') release?.();
  if (message === 'sigterm') process.emit('SIGTERM', 'SIGTERM');
});

const runtime = createConsumerRuntime({
  config: {
    kafkaBootstrapServers: env('BOOTSTRAP'),
    groupId: env('GROUP'),
    retryMaxAttempts: 2,
    retryBackoffMs: 200,
    logLevel: 'info',
  },
  logger: createLogger({ level: 'info' }),
  topics: [{ topic: env('TOPIC'), schema: 'payment-event' }],
  retryTopic: env('RETRY_TOPIC'),
  dlqTopic: env('DLQ_TOPIC'),
  kafka: { consumer: FAST_GROUP_CONFIG },
  onPartitionsAssigned: (partitions) => send({ type: 'assigned', partitions }),
  handler: async (message) => {
    const info = { partition: message.partition, offset: message.offset, event_id: message.value.event_id };
    send({ type: 'handling', ...info });
    if (env('MODE') === 'hang') await new Promise<never>(() => undefined);
    await new Promise<void>((resolve) => {
      release = resolve;
    });
    send({ type: 'handled', ...info });
  },
});

await runtime.start();
await runtime.stopped;
send({ type: 'stopped' });
// Close the IPC channel so that it does not keep the process alive; the
// test asserts that the process then exits on its own.
process.disconnect?.();
