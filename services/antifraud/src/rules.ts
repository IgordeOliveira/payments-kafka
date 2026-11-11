/**
 * Fraud rules over one merchant's payment events.
 *
 * Everything here is deterministic and free of I/O and of the wall clock:
 * rules read time from the event's `occurred_at`, so processing the same
 * ordered stream from empty state always yields the same alerts. The rule
 * functions update the merchant state they are given and return the alerts
 * the event triggered.
 */
import type { FraudAlert, FraudRule, PaymentEvent } from '@payments-kafka/consumer-runtime';
import type { AntifraudConfig } from './config.js';

/** Event times (epoch ms) inside a sliding window, plus the once-per-breach flag. */
export interface SlidingWindow {
  /** Sorted ascending; only times newer than `newest - windowMs` are kept. */
  times: number[];
  /** True after the rule fired, until the window count is back at the limit or below. */
  breached: boolean;
}

export interface MerchantState {
  readonly merchantId: string;
  /** Sum and count of the `payment.created` amounts in the running mean (outliers excluded). */
  amountSumCents: number;
  amountSamples: number;
  created: SlidingWindow;
  refunds: SlidingWindow;
  /**
   * Recently applied event ids, least recently seen first, each with the
   * alerts it triggered. A redelivered event does not touch the rules again;
   * its alerts are returned again so they can be re-published (same alert_id).
   */
  seen: Map<string, FraudAlert[]>;
}

export interface Evaluation {
  alerts: FraudAlert[];
  /** The event had already been applied; `alerts` are the ones it triggered then. */
  duplicate: boolean;
}

export function newMerchantState(merchantId: string): MerchantState {
  return {
    merchantId,
    amountSumCents: 0,
    amountSamples: 0,
    created: { times: [], breached: false },
    refunds: { times: [], breached: false },
    seen: new Map(),
  };
}

/** The merchant's running mean of payment amounts, or `undefined` before the first sample. */
export function meanAmountCents(state: MerchantState): number | undefined {
  return state.amountSamples === 0 ? undefined : state.amountSumCents / state.amountSamples;
}

/**
 * Applies one event to the merchant's state and returns the alerts it
 * triggers. Only `payment.created` and `payment.refunded` feed the rules;
 * other event types are ignored.
 */
export function evaluate(state: MerchantState, event: PaymentEvent, config: AntifraudConfig): Evaluation {
  if (event.event_type !== 'payment.created' && event.event_type !== 'payment.refunded') {
    return { alerts: [], duplicate: false };
  }

  const previous = state.seen.get(event.event_id);
  if (previous !== undefined) {
    // Refresh recency (LRU) and hand back the alerts this event produced.
    state.seen.delete(event.event_id);
    state.seen.set(event.event_id, previous);
    return { alerts: previous, duplicate: true };
  }

  const alerts: FraudAlert[] = [];
  if (event.event_type === 'payment.created') {
    // Burst first: it only looks at times, and the outlier rule decides
    // whether the amount enters the mean.
    pushIfAlert(alerts, paymentBurst(state, event, config));
    pushIfAlert(alerts, amountOutlier(state, event, config));
  } else {
    pushIfAlert(alerts, refundExcess(state, event, config));
  }

  state.seen.set(event.event_id, alerts);
  while (state.seen.size > config.seenEventIdsPerMerchant) {
    const oldest = state.seen.keys().next().value;
    if (oldest === undefined) break;
    state.seen.delete(oldest);
  }
  return { alerts, duplicate: false };
}

/**
 * `amount_outlier` on `payment.created`: fires when the amount exceeds
 * `outlierFactor` x the running mean, once `outlierMinSamples` payments are in
 * the mean. An outlier is not added to the mean; every other payment is.
 */
export function amountOutlier(
  state: MerchantState,
  event: PaymentEvent,
  config: AntifraudConfig,
): FraudAlert | null {
  const mean = meanAmountCents(state);
  if (
    mean !== undefined &&
    state.amountSamples >= config.outlierMinSamples &&
    event.amount_cents > config.outlierFactor * mean
  ) {
    return buildAlert('amount_outlier', event, {
      amount_cents: event.amount_cents,
      merchant_mean_cents: Math.round(mean),
      factor: config.outlierFactor,
      samples: state.amountSamples,
    });
  }
  state.amountSumCents += event.amount_cents;
  state.amountSamples += 1;
  return null;
}

/**
 * `payment_burst` on `payment.created`: fires once when more than
 * `burstLimit` payments fall in a `burstWindowMs` sliding window.
 */
export function paymentBurst(
  state: MerchantState,
  event: PaymentEvent,
  config: AntifraudConfig,
): FraudAlert | null {
  return windowRule('payment_burst', state.created, event, config.burstLimit, config.burstWindowMs);
}

/**
 * `refund_excess` on `payment.refunded`: fires once when more than
 * `refundLimit` refunds fall in a `refundWindowMs` sliding window.
 */
export function refundExcess(
  state: MerchantState,
  event: PaymentEvent,
  config: AntifraudConfig,
): FraudAlert | null {
  return windowRule('refund_excess', state.refunds, event, config.refundLimit, config.refundWindowMs);
}

/**
 * Adds the event to the window and applies the once-per-breach logic: fire
 * when the count goes above `limit`, stay quiet while it stays above, re-arm
 * when it is back at `limit` or below.
 */
function windowRule(
  rule: FraudRule,
  window: SlidingWindow,
  event: PaymentEvent,
  limit: number,
  windowMs: number,
): FraudAlert | null {
  const count = observe(window, Date.parse(event.occurred_at), windowMs);
  if (count === undefined) return null;
  if (count <= limit) {
    window.breached = false;
    return null;
  }
  if (window.breached) return null;
  window.breached = true;
  return buildAlert(rule, event, { count, limit, window_ms: windowMs });
}

/**
 * Inserts `time` (kept sorted, so late retried events land in place), drops
 * times that left the window ending at the newest time, and returns the
 * number of events in that window. Returns `undefined` without changing the
 * window for an event older than the window itself (too late to count).
 */
export function observe(window: SlidingWindow, time: number, windowMs: number): number | undefined {
  const { times } = window;
  const newest = times.length > 0 ? Math.max(times[times.length - 1] as number, time) : time;
  if (time <= newest - windowMs) return undefined;

  times.splice(upperBound(times, time), 0, time);
  const cutoff = newest - windowMs;
  const firstInside = upperBound(times, cutoff);
  if (firstInside > 0) times.splice(0, firstInside);
  return times.length;
}

/** Index of the first element greater than `value` in a sorted array. */
function upperBound(sorted: readonly number[], value: number): number {
  let low = 0;
  let high = sorted.length;
  while (low < high) {
    const mid = (low + high) >>> 1;
    if ((sorted[mid] as number) <= value) low = mid + 1;
    else high = mid;
  }
  return low;
}

export function buildAlert(rule: FraudRule, event: PaymentEvent, details: Record<string, unknown>): FraudAlert {
  return {
    alert_id: `${rule}:${event.event_id}`,
    schema_version: 1,
    rule,
    merchant_id: event.merchant_id,
    payment_id: event.payment_id,
    triggered_by_event_id: event.event_id,
    occurred_at: event.occurred_at,
    details,
  };
}

function pushIfAlert(alerts: FraudAlert[], alert: FraudAlert | null): void {
  if (alert) alerts.push(alert);
}
