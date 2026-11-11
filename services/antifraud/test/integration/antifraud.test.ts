import { afterEach, describe, expect, inject, it } from 'vitest';
import { loadContracts, type HandlerContext } from '@payments-kafka/consumer-runtime';
import {
  collect,
  createRuntimeTopics,
  makePaymentEvent,
  produce,
  uniqueId,
  waitFor,
} from '@payments-kafka/consumer-runtime/testing';
import { asRecords, createdEvents, startInstance, type Instance } from './harness.js';

const bootstrap = inject('kafkaBootstrapServers');
const contracts = loadContracts();

let instances: Instance[] = [];
afterEach(async () => {
  await Promise.all(instances.map((instance) => instance.runtime.stop()));
  instances = [];
});

async function start(options: Omit<Parameters<typeof startInstance>[0], 'bootstrap'>): Promise<Instance> {
  const instance = await startInstance({ bootstrap, ...options });
  instances.push(instance);
  return instance;
}

describe('antifraud service on Kafka', () => {
  it('publishes one schema-valid payment_burst alert, keyed by merchant_id, for a burst of 40 payments', async () => {
    const topics = await createRuntimeTopics(bootstrap);
    const groupId = uniqueId('antifraud');
    await start({ topics, groupId });

    const now = Date.now();
    const burst = createdEvents('merchant-burst', 40, now, 25, makePaymentEvent);
    const calm = createdEvents('merchant-calm', 20, now, 25, makePaymentEvent);
    await produce(bootstrap, topics.payments, asRecords([...burst, ...calm]));

    const [first] = await collect(bootstrap, topics.fraudAlerts, { count: 1, timeoutMs: 30_000 });
    const alert = first!.json();
    expect(first!.key).toBe('merchant-burst');
    expect(contracts.validate('fraud-alert', alert).ok).toBe(true);
    expect(alert).toMatchObject({
      alert_id: `payment_burst:${burst[30]!.event_id}`,
      rule: 'payment_burst',
      merchant_id: 'merchant-burst',
      payment_id: burst[30]!.payment_id,
      triggered_by_event_id: burst[30]!.event_id,
      occurred_at: burst[30]!.occurred_at,
      details: { count: 31, limit: 30, window_ms: 10_000 },
    });

    // Exactly one: wait until every event was handled, then read everything.
    await waitFor(() => instances[0]!.service.merchantState('merchant-calm')?.amountSamples === 20, {
      message: 'all events handled',
    });
    const all = await collect(bootstrap, topics.fraudAlerts, { timeoutMs: 3000 });
    expect(all).toHaveLength(1);
  });

  it('drops merchant state when a second instance takes partitions over, and starts empty', async () => {
    const topics = await createRuntimeTopics(bootstrap);
    const groupId = uniqueId('antifraud');
    const a = await start({ topics, groupId });
    await waitFor(() => a.runtime.assignment().length === 12, { message: 'A owns every partition' });

    // 25 payments per merchant within 10 s (event time): below the burst limit.
    const now = Date.now();
    const merchants = Array.from({ length: 12 }, (_, i) => `merchant-${String(i).padStart(3, '0')}`);
    const firstHalf = merchants.flatMap((m) => createdEvents(m, 25, now, 20, makePaymentEvent));
    await produce(bootstrap, topics.payments, asRecords(firstHalf));
    await waitFor(
      () => merchants.every((m) => a.service.merchantState(m)?.amountSamples === 25),
      { message: 'A built state for every merchant' },
    );

    const b = await start({ topics, groupId });
    await waitFor(
      () =>
        a.runtime.assignment().length > 0 &&
        b.runtime.assignment().length > 0 &&
        a.runtime.assignment().length + b.runtime.assignment().length === 12,
      { timeoutMs: 30_000, message: 'partitions split between A and B' },
    );

    // The takeover is logged and A no longer holds state for the revoked partitions.
    const dropped = a.log.find('merchant state dropped for revoked partitions');
    expect(dropped.length).toBeGreaterThan(0);
    expect(dropped.reduce((sum, line) => sum + Number(line['merchants_dropped']), 0)).toBe(12);
    expect(a.service.merchantIds()).toEqual([]);
    expect(b.service.merchantIds()).toEqual([]);

    // 10 more payments per merchant in the same event-time window: 35 in total,
    // but neither instance remembers the first 25 (the warm-up trade-off).
    const secondHalf = merchants.flatMap((m) => createdEvents(m, 10, now + 600, 20, makePaymentEvent));
    await produce(bootstrap, topics.payments, asRecords(secondHalf));
    await waitFor(
      () =>
        merchants.every(
          (m) => (a.service.merchantState(m) ?? b.service.merchantState(m))?.amountSamples === 10,
        ),
      { message: 'second half handled from empty state' },
    );

    // Each merchant lives in exactly one instance: the owner of its partition.
    const inA = new Set(a.service.merchantIds());
    const inB = new Set(b.service.merchantIds());
    expect([...inA].filter((m) => inB.has(m))).toEqual([]);
    expect(new Set([...inA, ...inB])).toEqual(new Set(merchants));
    expect(inA.size).toBeGreaterThan(0);
    expect(inB.size).toBeGreaterThan(0);

    expect(await collect(bootstrap, topics.fraudAlerts, { timeoutMs: 3000 })).toEqual([]);
  });

  it('sends the event to the retry topic when the alert publish fails, and publishes the alert on retry', async () => {
    const topics = await createRuntimeTopics(bootstrap);
    const groupId = uniqueId('antifraud');
    let failuresLeft = 1;
    const failOnce = (ctx: HandlerContext): HandlerContext => ({
      ...ctx,
      producer: {
        send: (record) => ctx.producer.send(record),
        sendMany: async (records) => {
          if (failuresLeft > 0) {
            failuresLeft -= 1;
            throw new Error('simulated alert publish failure');
          }
          await ctx.producer.sendMany(records);
        },
      },
    });
    const instance = await start({ topics, groupId, wrapContext: failOnce });

    const burst = createdEvents('merchant-retry', 31, Date.now(), 25, makePaymentEvent);
    const trigger = burst[30]!;
    await produce(bootstrap, topics.payments, asRecords(burst));

    const [retried] = await collect(bootstrap, topics.retry, { count: 1, timeoutMs: 30_000 });
    expect(retried!.headers['x-retry-group']).toBe(groupId);
    expect(retried!.headers['x-retry-attempt']).toBe('1');
    expect(retried!.headers['x-error-message']).toContain('simulated alert publish failure');
    expect(retried!.json()).toMatchObject({ event_id: trigger.event_id });

    const [published] = await collect(bootstrap, topics.fraudAlerts, { count: 1, timeoutMs: 30_000 });
    expect(published!.key).toBe('merchant-retry');
    expect(published!.json()).toMatchObject({ alert_id: `payment_burst:${trigger.event_id}` });
    expect(contracts.validate('fraud-alert', published!.json()).ok).toBe(true);

    expect(instance.log.find('handler failed; record sent to retry topic')).toHaveLength(1);
    expect(await collect(bootstrap, topics.dlq, { timeoutMs: 2000 })).toEqual([]);
    expect(await collect(bootstrap, topics.fraudAlerts, { timeoutMs: 2000 })).toHaveLength(1);
  });
});
