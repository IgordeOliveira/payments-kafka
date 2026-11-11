import { describe, expect, it } from 'vitest';
import type { PaymentEvent } from '@payments-kafka/consumer-runtime';
import { DEFAULT_ANTIFRAUD_CONFIG } from '../../src/config.js';
import { evaluate, newMerchantState } from '../../src/rules.js';
import { created, event, refunded } from './fixtures.js';
import { run } from './run.js';

/**
 * A mixed stream for three merchants that triggers all three rules:
 * history + outlier, a burst, a refund spree, plus confirmed/expired noise.
 */
function mixedStream(): PaymentEvent[] {
  const events: PaymentEvent[] = [];
  for (let i = 0; i < 25; i += 1) events.push(created(i * 1000, 5000, 'merchant-a'));
  events.push(created(30_000, 990_000, 'merchant-a'));
  for (let i = 0; i < 40; i += 1) events.push(created(i * 20, 3000, 'merchant-b'));
  for (let i = 0; i < 8; i += 1) events.push(refunded(i * 1000, 'merchant-c'));
  events.push(event(500, { event_type: 'payment.confirmed', sequence: 2, merchant_id: 'merchant-a' }));
  events.push(event(600, { event_type: 'payment.expired', sequence: 2, merchant_id: 'merchant-b' }));
  // Interleave merchants by event time, as a topic with several partitions would.
  return events.sort((x, y) => Date.parse(x.occurred_at) - Date.parse(y.occurred_at));
}

const ids = (alerts: { alert_id: string }[]) => new Set(alerts.map((a) => a.alert_id));

describe('idempotency and determinism', () => {
  it('the stream triggers every rule once', () => {
    expect(run(mixedStream()).map((a) => a.rule).sort()).toEqual([
      'amount_outlier',
      'payment_burst',
      'refund_excess',
    ]);
  });

  it('a stream with every event delivered twice yields the same alert_id set', () => {
    const stream = mixedStream();
    const once = run(stream);
    const twice = run(stream.flatMap((e) => [e, { ...e }]));
    expect(ids(twice)).toEqual(ids(once));
  });

  it('a stream redelivered as a whole (at-least-once after a crash) yields the same alert_id set', () => {
    const stream = mixedStream();
    expect(ids(run([...stream, ...stream]))).toEqual(ids(run(stream)));
  });

  it('a duplicate does not change rule state and returns the alerts of the first delivery', () => {
    const state = newMerchantState('merchant-001');
    const config = DEFAULT_ANTIFRAUD_CONFIG;
    const events = Array.from({ length: 31 }, (_, i) => created(i * 10));
    for (const e of events) evaluate(state, e, config);
    const snapshot = structuredClone({ ...state, seen: [...state.seen.keys()] });

    const last = events[30]!;
    const again = evaluate(state, last, config);
    expect(again.duplicate).toBe(true);
    expect(again.alerts.map((a) => a.alert_id)).toEqual([`payment_burst:${last.event_id}`]);
    expect(evaluate(state, events[0]!, config)).toEqual({ alerts: [], duplicate: true });

    expect(state.amountSamples).toBe(snapshot.amountSamples);
    expect(state.created).toEqual(snapshot.created);
  });

  it('replaying the same stream from empty state yields identical alerts', () => {
    const stream = mixedStream();
    const first = run(stream);
    const replay = run(structuredClone(stream));
    expect(replay).toEqual(first);
  });

  it('bounds the seen-id set per merchant (LRU)', () => {
    const config = { ...DEFAULT_ANTIFRAUD_CONFIG, seenEventIdsPerMerchant: 3 };
    const state = newMerchantState('merchant-001');
    const [a, b, c, d] = [created(0), created(1000), created(2000), created(3000)];
    for (const e of [a, b, c]) evaluate(state, e!, config);
    evaluate(state, a!, config); // touch a: b becomes the least recently seen
    evaluate(state, d!, config);
    expect([...state.seen.keys()]).toEqual([c!.event_id, a!.event_id, d!.event_id]);
  });
});
