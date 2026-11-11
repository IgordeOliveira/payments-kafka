import {
  createConsumerRuntime,
  type ConsumerRuntime,
  type HandlerContext,
  type PaymentEvent,
} from '@payments-kafka/consumer-runtime';
import {
  createCapturingLogger,
  FAST_GROUP_CONFIG,
  testRuntimeConfig,
  type CapturingLogger,
  type RuntimeTopics,
} from '@payments-kafka/consumer-runtime/testing';
import { DEFAULT_ANTIFRAUD_CONFIG, type AntifraudConfig } from '../../src/config.js';
import { createAntifraudService, type AntifraudService } from '../../src/service.js';

export interface Instance {
  runtime: ConsumerRuntime;
  service: AntifraudService;
  log: CapturingLogger;
}

/**
 * Starts one antifraud instance on isolated topics, wired exactly like
 * main.ts. `wrapContext` lets a test interpose on what the handler gets
 * (e.g. a producer that fails once).
 */
export async function startInstance(options: {
  bootstrap: string;
  topics: RuntimeTopics;
  groupId: string;
  config?: AntifraudConfig;
  wrapContext?: (ctx: HandlerContext) => HandlerContext;
}): Promise<Instance> {
  const log = createCapturingLogger('info');
  const service = createAntifraudService({
    config: options.config ?? DEFAULT_ANTIFRAUD_CONFIG,
    alertsTopic: options.topics.fraudAlerts,
    logger: log.logger,
  });
  const wrap = options.wrapContext ?? ((ctx) => ctx);
  const runtime = createConsumerRuntime({
    config: testRuntimeConfig(options.bootstrap, { groupId: options.groupId }),
    logger: log.logger,
    topics: [{ topic: options.topics.payments, schema: 'payment-event' }],
    retryTopic: options.topics.retry,
    dlqTopic: options.topics.dlq,
    handleSignals: false,
    kafka: { consumer: FAST_GROUP_CONFIG },
    handler: (message, ctx) => service.handler(message, wrap(ctx)),
    onPartitionsAssigned: service.onPartitionsAssigned,
    onPartitionsRevoked: service.onPartitionsRevoked,
  });
  await runtime.start();
  return { runtime, service, log };
}

/** `count` payment.created events for one merchant, `stepMs` apart in event time from `start`. */
export function createdEvents(
  merchantId: string,
  count: number,
  start: number,
  stepMs: number,
  makeEvent: (overrides: Partial<PaymentEvent>) => PaymentEvent,
): PaymentEvent[] {
  return Array.from({ length: count }, (_, i) =>
    makeEvent({ merchant_id: merchantId, occurred_at: new Date(start + i * stepMs).toISOString() }),
  );
}

export const asRecords = (events: readonly PaymentEvent[]) =>
  events.map((event) => ({ key: event.merchant_id, value: event }));
