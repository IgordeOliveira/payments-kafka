/**
 * The consumer handler: applies an event and maps the result to the runtime's
 * outcomes (docs/architecture.md, "Retry and DLQ protocol"):
 *
 * | Apply result | Handler            | Runtime                         |
 * | ------------ | ------------------ | ------------------------------- |
 * | applied      | returns            | offset committed                |
 * | duplicate    | returns            | offset committed                |
 * | gap          | throws OutOfOrderError (retryable) | retry topic, DLQ when exhausted |
 * | conflict     | throws NonRetryableError           | DLQ (`non_retryable`)           |
 *
 * Any other error (database down...) escapes as is and is therefore retryable.
 */
import {
  NonRetryableError,
  type ConsumedMessage,
  type HandlerContext,
  type PaymentEvent,
} from '@payments-kafka/consumer-runtime';
import type pg from 'pg';
import { applyEvent, type ApplyOptions, type ApplyResult } from './apply.js';

/** A predecessor of the event has not been applied yet. Retryable. */
export class OutOfOrderError extends Error {
  override readonly name = 'OutOfOrderError';
}

/** Returns for applied and duplicate events, throws for gap and conflict. */
export function assertHandled(result: ApplyResult): void {
  switch (result.outcome) {
    case 'applied':
    case 'duplicate':
      return;
    case 'gap':
      throw new OutOfOrderError(`out of order: ${result.reason}`);
    case 'conflict':
      throw new NonRetryableError(`conflicting event: ${result.reason}`);
  }
}

export type ReconciliationHandler = (
  message: ConsumedMessage<'payment-event'>,
  ctx: HandlerContext,
) => Promise<void>;

export function createHandler(pool: pg.Pool, options: ApplyOptions = {}): ReconciliationHandler {
  return async (message, ctx) => {
    const event: PaymentEvent = message.value;
    const result = await applyEvent(pool, event, options);
    if (result.outcome === 'applied') {
      ctx.logger.info(
        {
          event_type: event.event_type,
          payment_id: event.payment_id,
          merchant_id: event.merchant_id,
          status: result.status,
          ledger_entry: result.ledgerEntry,
        },
        'payment event applied',
      );
    } else if (result.outcome === 'duplicate') {
      ctx.logger.info({ event_type: event.event_type, payment_id: event.payment_id }, 'duplicate event ignored');
    }
    assertHandled(result);
  };
}
