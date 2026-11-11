import { describe, expect, it } from 'vitest';
import { loadContracts } from '@payments-kafka/consumer-runtime';
import { DEFAULT_ANTIFRAUD_CONFIG } from '../../src/config.js';
import { amountOutlier, evaluate, meanAmountCents, newMerchantState } from '../../src/rules.js';
import { created, event } from './fixtures.js';
import { run } from './run.js';

const config = DEFAULT_ANTIFRAUD_CONFIG;

/** `count` normal payments of about 5000 cents, one second apart (no burst). */
function history(count: number, merchantId = 'merchant-001') {
  return Array.from({ length: count }, (_, i) => created(i * 1000, 4900 + (i % 3) * 100, merchantId));
}

describe('amount_outlier', () => {
  it('fires for a large payment after 20 payments of about 5000 cents (spec scenario)', () => {
    const outlier = created(30_000, 990_000);
    const alerts = run([...history(20), outlier]);

    expect(alerts).toHaveLength(1);
    const [alert] = alerts;
    expect(alert).toMatchObject({
      alert_id: `amount_outlier:${outlier.event_id}`,
      schema_version: 1,
      rule: 'amount_outlier',
      merchant_id: outlier.merchant_id,
      payment_id: outlier.payment_id,
      triggered_by_event_id: outlier.event_id,
      occurred_at: outlier.occurred_at,
      details: { amount_cents: 990_000, merchant_mean_cents: 4995, factor: 5, samples: 20 },
    });
    expect(loadContracts().validate('fraud-alert', alert)).toEqual({ ok: true, value: alert });
  });

  it('stays quiet during warm-up (fewer payments than the minimum samples)', () => {
    expect(run([...history(19), created(30_000, 990_000)])).toEqual([]);
    expect(run([created(0, 990_000)])).toEqual([]);
  });

  it('fires only above factor x mean, not at it', () => {
    const flat = () => history(20).map((e) => ({ ...e, amount_cents: 5000 }));
    expect(run([...flat(), created(30_000, 25_000)])).toEqual([]);
    expect(run([...flat(), created(30_000, 25_001)])).toHaveLength(1);
  });

  it('does not add the outlier to the mean, so a second outlier still fires', () => {
    const state = newMerchantState('merchant-001');
    for (const e of history(20)) evaluate(state, e, config);
    const meanBefore = meanAmountCents(state);

    expect(amountOutlier(state, created(30_000, 990_000), config)).not.toBeNull();
    expect(meanAmountCents(state)).toBe(meanBefore);
    expect(state.amountSamples).toBe(20);

    expect(amountOutlier(state, created(31_000, 990_000), config)).not.toBeNull();
    // A normal payment does enter the mean.
    expect(amountOutlier(state, created(32_000, 5000), config)).toBeNull();
    expect(state.amountSamples).toBe(21);
  });

  it('only looks at payment.created (a large refund is ignored)', () => {
    const refund = event(30_000, { event_type: 'payment.refunded', sequence: 3, amount_cents: 990_000 });
    expect(run([...history(20), refund])).toEqual([]);
  });

  it('keeps one mean per merchant', () => {
    // merchant-002 only has large payments; merchant-001 history must not affect it.
    const big = Array.from({ length: 20 }, (_, i) => created(i * 1000, 900_000, 'merchant-002'));
    expect(run([...history(20), ...big, created(30_000, 990_000, 'merchant-002')])).toEqual([]);
  });
});
