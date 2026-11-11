import { describe, expect, inject, it } from 'vitest';
import {
  collect,
  createTopics,
  makePaymentEvent,
  produce,
  uniqueId,
} from '../../src/testing/index.js';

const bootstrapServers = inject('kafkaBootstrapServers');

describe('testing helpers', () => {
  it('creates a topic, produces one record and collects it back', async () => {
    const topic = uniqueId('smoke');
    await createTopics(bootstrapServers, [{ topic, partitions: 2 }]);
    const event = makePaymentEvent({ merchant_id: 'merchant-smoke' });

    await produce(bootstrapServers, topic, [
      { key: event.merchant_id, value: event, headers: { 'x-test': 'yes' } },
    ]);
    const [record] = await collect(bootstrapServers, topic, { count: 1 });

    expect(record?.key).toBe('merchant-smoke');
    expect(record?.json()).toEqual(event);
    expect(record?.headers['x-test']).toBe('yes');
  });
});
