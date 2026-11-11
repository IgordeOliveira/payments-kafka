/**
 * A complete service built on @payments-kafka/consumer-runtime.
 * (Illustrative only: its "large payment" rule is not the antifraud service.)
 *
 * It consumes two topics with different contracts through one handler,
 * publishes records of its own, rejects some records as non-retryable and
 * reacts to partition assignment changes. Commits, validation, retries, the
 * DLQ and graceful shutdown all come from the runtime.
 */
import {
  createConsumerRuntime,
  createLogger,
  envInteger,
  loadConfig,
  NonRetryableError,
  TOPICS,
  type ConsumedMessage,
  type FraudAlert,
  type HandlerContext,
} from '@payments-kafka/consumer-runtime';

const SERVICE_NAME = 'sample';

// Service-specific settings use the same env helpers as loadConfig():
// unset means default, an invalid value fails at startup naming the variable.
const LARGE_PAYMENT_CENTS = envInteger(process.env, 'SAMPLE_LARGE_PAYMENT_CENTS', 500_000, { min: 1 });

/** In-memory state, kept only for the partitions this instance owns. */
const paymentsSeenByMerchant = new Map<string, number>();

type Message = ConsumedMessage<'payment-event' | 'fraud-alert'>;

async function handle(message: Message, ctx: HandlerContext): Promise<void> {
  // `schema` tells the two contracts apart and narrows `message.value`.
  if (message.schema === 'fraud-alert') {
    ctx.logger.info({ rule: message.value.rule }, 'fraud alert received');
    return;
  }

  const event = message.value; // PaymentEvent, already validated
  if (event.event_type === 'payment.refunded' && event.amount_cents > 100_000_000) {
    // Retrying cannot fix this: straight to the DLQ (x-dlq-reason: non_retryable).
    throw new NonRetryableError(`refund of ${event.amount_cents} cents is above the sample limit`);
  }

  paymentsSeenByMerchant.set(
    event.merchant_id,
    (paymentsSeenByMerchant.get(event.merchant_id) ?? 0) + 1,
  );

  if (event.event_type === 'payment.created' && event.amount_cents >= LARGE_PAYMENT_CENTS) {
    const rule = 'amount_outlier';
    const alert: FraudAlert = {
      alert_id: `${rule}:${event.event_id}`, // deterministic, so redelivery is harmless
      schema_version: 1,
      rule,
      merchant_id: event.merchant_id,
      payment_id: event.payment_id,
      triggered_by_event_id: event.event_id,
      occurred_at: event.occurred_at,
      details: { amount_cents: event.amount_cents, threshold_cents: LARGE_PAYMENT_CENTS },
    };
    // Await the acknowledgement before returning: the offset is committed
    // right after the handler returns. Throwing here (e.g. broker down)
    // sends the event through the retry path instead.
    await ctx.producer.send({ topic: TOPICS.fraudAlerts, key: alert.merchant_id, value: alert });
    ctx.logger.info({ alert_id: alert.alert_id }, 'fraud alert published');
  }

  ctx.logger.info({ event_type: event.event_type }, 'payment event handled');
}

async function main(): Promise<void> {
  const config = loadConfig({ serviceName: SERVICE_NAME });
  const logger = createLogger({ level: config.logLevel, bindings: { service: SERVICE_NAME } });

  const runtime = createConsumerRuntime({
    config,
    logger,
    topics: [
      { topic: TOPICS.payments, schema: 'payment-event' },
      { topic: TOPICS.fraudAlerts, schema: 'fraud-alert' },
    ],
    handler: handle,
    // Called after the in-flight handlers of those partitions finished. The
    // state is keyed by merchant, not by partition, so drop all of it and
    // rebuild from the records this instance receives next.
    onPartitionsRevoked: () => paymentsSeenByMerchant.clear(),
  });

  await runtime.start();
  // Resolves after SIGTERM/SIGINT: in-flight records were finished and
  // committed, and the instance left the group.
  await runtime.stopped;
  // Close the service's own resources here (database pool...).
}

main().catch((error: unknown) => {
  // ConfigError and startup failures end up here.
  createLogger().fatal({ err: error instanceof Error ? error.message : String(error) }, 'startup failed');
  process.exit(1);
});
