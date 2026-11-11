import { afterEach, inject } from 'vitest';
import type { RuntimeConfig } from '../../src/config.js';
import type { SchemaName } from '../../src/contracts.js';
import {
  createConsumerRuntime,
  type ConsumerRuntime,
  type ConsumerRuntimeOptions,
} from '../../src/runtime.js';
import {
  createCapturingLogger,
  createRuntimeTopics,
  FAST_GROUP_CONFIG,
  testRuntimeConfig,
  waitFor,
  type CapturingLogger,
  type RuntimeTopics,
} from '../../src/testing/index.js';

export const bootstrapServers = inject('kafkaBootstrapServers');

const running: ConsumerRuntime[] = [];

afterEach(async () => {
  await Promise.all(running.splice(0).map((runtime) => runtime.stop()));
});

export async function freshTopics(): Promise<RuntimeTopics> {
  return createRuntimeTopics(bootstrapServers);
}

export interface StartedTestRuntime {
  runtime: ConsumerRuntime;
  log: CapturingLogger;
  config: RuntimeConfig;
}

/**
 * Starts a runtime wired to the test broker and the given topics, with a
 * capturing logger and fast group settings, and waits until it owns
 * partitions. Stopped automatically after each test.
 */
export async function startRuntime<const S extends SchemaName>(
  topics: RuntimeTopics,
  options: Omit<ConsumerRuntimeOptions<S>, 'config'> & { config?: Partial<RuntimeConfig> },
): Promise<StartedTestRuntime> {
  const log = createCapturingLogger('debug');
  const config = testRuntimeConfig(bootstrapServers, options.config);
  const runtime = createConsumerRuntime<S>({
    logger: log.logger,
    handleSignals: false,
    retryTopic: topics.retry,
    dlqTopic: topics.dlq,
    ...options,
    config,
    kafka: { ...options.kafka, consumer: { ...FAST_GROUP_CONFIG, ...options.kafka?.consumer } },
  });
  running.push(runtime);
  await runtime.start();
  await waitFor(() => runtime.assignment().length > 0, { message: 'partitions assigned' });
  return { runtime, log, config };
}

export function deferred<T = void>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((r) => {
    resolve = r;
  });
  return { promise, resolve };
}
