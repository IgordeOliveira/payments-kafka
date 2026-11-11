import {
  createConsumerRuntime,
  TOPICS,
  type ConsumerRuntime,
  type LibrdkafkaConfig,
  type Logger,
  type RuntimeConfig,
} from '@payments-kafka/consumer-runtime';
import type pg from 'pg';
import { createHandler } from './handler.js';

export const SERVICE_NAME = 'reconciliation';

export interface ReconciliationRuntimeOptions {
  config: RuntimeConfig;
  pool: pg.Pool;
  logger?: Logger;
  /** Topic overrides, for tests only (isolated topics). */
  topics?: { payments?: string; retry?: string; dlq?: string };
  handleSignals?: boolean;
  kafka?: { consumer?: LibrdkafkaConfig; producer?: LibrdkafkaConfig };
}

/** One member of the `reconciliation` consumer group, consuming `pix.payments.v1` (plus the retry topic). */
export function createReconciliationRuntime(options: ReconciliationRuntimeOptions): ConsumerRuntime {
  return createConsumerRuntime({
    config: options.config,
    ...(options.logger ? { logger: options.logger } : {}),
    topics: [{ topic: options.topics?.payments ?? TOPICS.payments, schema: 'payment-event' }],
    ...(options.topics?.retry ? { retryTopic: options.topics.retry } : {}),
    ...(options.topics?.dlq ? { dlqTopic: options.topics.dlq } : {}),
    ...(options.handleSignals === undefined ? {} : { handleSignals: options.handleSignals }),
    ...(options.kafka ? { kafka: options.kafka } : {}),
    handler: createHandler(options.pool),
  });
}
