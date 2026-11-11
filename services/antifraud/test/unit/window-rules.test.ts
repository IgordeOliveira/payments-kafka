import { describe, expect, it } from 'vitest';
import { observe } from '../../src/rules.js';
import { created, refunded } from './fixtures.js';
import { rulesOf, run } from './run.js';

/** `count` created events starting at `startMs`, `stepMs` apart (event time). */
function burst(count: number, startMs: number, stepMs: number, merchantId = 'merchant-001') {
  return Array.from({ length: count }, (_, i) => created(startMs + i * stepMs, 5000, merchantId));
}

function refunds(count: number, startMs: number, stepMs: number, merchantId = 'merchant-001') {
  return Array.from({ length: count }, (_, i) => refunded(startMs + i * stepMs, merchantId));
}

describe('sliding window', () => {
  it('counts events newer than newest - window and drops the rest', () => {
    const window = { times: [] as number[], breached: false };
    expect(observe(window, 0, 1000)).toBe(1);
    expect(observe(window, 500, 1000)).toBe(2);
    expect(observe(window, 1000, 1000)).toBe(2); // 0 left the window (0 <= 1000 - 1000)
    expect(observe(window, 700, 1000)).toBe(3); // late but inside: inserted in order
    expect(window.times).toEqual([500, 700, 1000]);
    expect(observe(window, 0, 1000)).toBeUndefined(); // older than the window: ignored
    expect(window.times).toEqual([500, 700, 1000]);
  });
});

describe('payment_burst', () => {
  it('fires exactly once for 40 payments within one second (spec scenario)', () => {
    const events = burst(40, 0, 25);
    const alerts = run(events);
    expect(rulesOf(alerts)).toEqual(['payment_burst']);
    // It fires on the 31st payment: the first one above the limit.
    expect(alerts[0]?.alert_id).toBe(`payment_burst:${events[30]?.event_id}`);
    expect(alerts[0]?.details).toEqual({ count: 31, limit: 30, window_ms: 10_000 });
  });

  it('stays quiet at exactly the limit', () => {
    expect(run(burst(30, 0, 25))).toEqual([]);
  });

  it('stays quiet at a normal pace (never more than the limit in any window)', () => {
    // One payment every 400 ms: 25 per 10 s window, for 5 minutes.
    expect(run(burst(750, 0, 400))).toEqual([]);
  });

  it('re-arms after the window drains and fires again on the next breach', () => {
    const first = burst(40, 0, 25);
    const second = burst(40, 60_000, 25);
    expect(run([...first, ...second]).map((a) => a.alert_id)).toEqual([
      `payment_burst:${first[30]?.event_id}`,
      `payment_burst:${second[30]?.event_id}`,
    ]);
  });

  it('does not fire again while the count stays above the limit', () => {
    // 40 quick payments, then a sustained pace that keeps more than 30 in the window.
    expect(rulesOf(run([...burst(40, 0, 25), ...burst(100, 1000, 200)]))).toEqual(['payment_burst']);
  });

  it('keeps windows per merchant: two merchants with 20 each in the same window stay quiet', () => {
    const a = burst(20, 0, 50, 'merchant-001');
    const b = burst(20, 10, 50, 'merchant-002');
    expect(run(a.flatMap((e, i) => [e, b[i]!]))).toEqual([]);
  });

  it('ignores refunds', () => {
    expect(rulesOf(run(refunds(40, 0, 25)))).not.toContain('payment_burst');
  });
});

describe('refund_excess', () => {
  it('fires exactly once for 8 refunds within 10 seconds (spec scenario)', () => {
    const events = refunds(8, 0, 1250);
    const alerts = run(events);
    expect(rulesOf(alerts)).toEqual(['refund_excess']);
    expect(alerts[0]?.alert_id).toBe(`refund_excess:${events[5]?.event_id}`);
    expect(alerts[0]?.details).toEqual({ count: 6, limit: 5, window_ms: 60_000 });
  });

  it('stays quiet at exactly the limit', () => {
    expect(run(refunds(5, 0, 1000))).toEqual([]);
  });

  it('stays quiet at a normal pace', () => {
    // One refund every 15 s: 4 per 60 s window, for an hour.
    expect(run(refunds(240, 0, 15_000))).toEqual([]);
  });

  it('re-arms after the window drains and fires again on the next breach', () => {
    const first = refunds(8, 0, 1000);
    const second = refunds(8, 200_000, 1000);
    expect(run([...first, ...second]).map((a) => a.alert_id)).toEqual([
      `refund_excess:${first[5]?.event_id}`,
      `refund_excess:${second[5]?.event_id}`,
    ]);
  });

  it('does not fire again while the count stays above the limit', () => {
    expect(rulesOf(run([...refunds(8, 0, 1000), ...refunds(20, 10_000, 5000)]))).toEqual(['refund_excess']);
  });

  it('keeps windows per merchant', () => {
    const a = refunds(4, 0, 1000, 'merchant-001');
    const b = refunds(4, 500, 1000, 'merchant-002');
    expect(run([...a, ...b])).toEqual([]);
  });

  it('ignores created payments', () => {
    expect(run([...refunds(5, 0, 1000), ...burst(20, 6000, 100)])).toEqual([]);
  });
});
