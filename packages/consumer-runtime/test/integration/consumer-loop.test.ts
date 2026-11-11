import { describe, expect, it } from 'vitest';
import type { PaymentEvent } from '../../src/contracts.js';
import type { ConsumedMessage } from '../../src/runtime.js';
import {
  collect,
  committedOffsets,
  makePaymentEvent,
  produce,
  sleep,
  waitFor,
} from '../../src/testing/index.js';
import { bootstrapServers, freshTopics, startRuntime } from './harness.js';

describe('consumer loop', () => {
  it('commits the offset past a record once its handler completes', async () => {
    const topics = await freshTopics();
    const handled: ConsumedMessage<'payment-event'>[] = [];
    const { config } = await startRuntime(topics, {
      topics: [{ topic: topics.payments, schema: 'payment-event' }],
      handler: (message) => {
        handled.push(message);
      },
    });

    const events = [1, 2, 3].map(() => makePaymentEvent({ merchant_id: 'merchant-commit' }));
    await produce(
      bootstrapServers,
      topics.payments,
      events.map((event) => ({ key: event.merchant_id, value: event })),
    );

    await waitFor(() => handled.length === 3, { message: 'three records handled' });
    expect(handled.map((m) => m.value.event_id)).toEqual(events.map((e) => e.event_id));
    expect(handled.every((m) => m.attempt === 0 && m.key === 'merchant-commit')).toBe(true);
    expect(handled[0]?.value).toEqual(events[0]);

    const partition = handled[0]!.partition;
    const lastOffset = Number(handled[2]!.offset);
    await waitFor(
      async () =>
        (await committedOffsets(bootstrapServers, config.groupId, topics.payments))[partition] ===
        lastOffset + 1,
      { message: 'committed offset = last handled offset + 1' },
    );
  });

  it('delivers records of a partition in order and one at a time, partitions in parallel', async () => {
    const topics = await freshTopics();
    const order = new Map<number, number[]>();
    const active = new Map<number, number>();
    let maxActivePerPartition = 0;
    let activeOverall = 0;
    let maxActiveOverall = 0;
    let handledCount = 0;

    await startRuntime(topics, {
      topics: [{ topic: topics.payments, schema: 'payment-event' }],
      handler: async (message) => {
        const p = message.partition;
        active.set(p, (active.get(p) ?? 0) + 1);
        activeOverall += 1;
        maxActivePerPartition = Math.max(maxActivePerPartition, active.get(p)!);
        maxActiveOverall = Math.max(maxActiveOverall, activeOverall);
        await sleep(15 + Math.random() * 25);
        order.set(p, [...(order.get(p) ?? []), message.value.amount_cents]);
        active.set(p, active.get(p)! - 1);
        activeOverall -= 1;
        handledCount += 1;
      },
    });

    // 15 records for merchant A on partition 0 and 15 for merchant B on
    // partition 1, interleaved; amount_cents carries the publish order.
    const records: Array<{ key: string; value: PaymentEvent; partition: number }> = [];
    for (let i = 1; i <= 15; i += 1) {
      for (const [merchant, partition] of [
        ['merchant-a', 0],
        ['merchant-b', 1],
      ] as const) {
        records.push({
          key: merchant,
          partition,
          value: makePaymentEvent({ merchant_id: merchant, amount_cents: i }),
        });
      }
    }
    await produce(bootstrapServers, topics.payments, records);

    await waitFor(() => handledCount === 30, { message: 'all records handled' });
    const expected = Array.from({ length: 15 }, (_, i) => i + 1);
    expect(order.get(0)).toEqual(expected);
    expect(order.get(1)).toEqual(expected);
    expect(maxActivePerPartition).toBe(1);
    expect(maxActiveOverall).toBeGreaterThanOrEqual(2);
  });

  it('sends invalid records to the DLQ without calling the handler, and commits them', async () => {
    const topics = await freshTopics();
    const handled: string[] = [];
    const { config } = await startRuntime(topics, {
      topics: [{ topic: topics.payments, schema: 'payment-event' }],
      handler: (message) => {
        handled.push(message.value.event_id);
      },
    });

    const valid = makePaymentEvent({ merchant_id: 'merchant-dlq' });
    const schemaViolation = { ...makePaymentEvent({ merchant_id: 'merchant-dlq' }), currency: 'USD' };
    await produce(bootstrapServers, topics.payments, [
      { key: 'merchant-dlq', value: '{this is not json', partition: 2 },
      { key: 'merchant-dlq', value: schemaViolation, partition: 2 },
      { key: 'merchant-dlq', value: valid, partition: 2 },
    ]);

    const dlq = await collect(bootstrapServers, topics.dlq, { count: 2 });
    expect(handled).toEqual([valid.event_id]);

    const [notJson, violation] = dlq;
    expect(notJson?.key).toBe('merchant-dlq');
    expect(notJson?.value).toBe('{this is not json');
    expect(notJson?.headers).toEqual({
      'x-original-topic': topics.payments,
      'x-original-partition': '2',
      'x-original-offset': '0',
      'x-error-message': expect.stringMatching(/not valid JSON/),
      'x-dlq-group': config.groupId,
      'x-dlq-reason': 'invalid_message',
      'x-dlq-attempts': '0',
    });
    expect(violation?.json()).toEqual(schemaViolation);
    expect(violation?.headers).toMatchObject({
      'x-original-offset': '1',
      'x-dlq-reason': 'invalid_message',
      'x-error-message': expect.stringContaining('currency'),
    });

    await waitFor(
      async () =>
        (await committedOffsets(bootstrapServers, config.groupId, topics.payments))[2] === 3,
      { message: 'offsets committed past the invalid records' },
    );
  });
});
