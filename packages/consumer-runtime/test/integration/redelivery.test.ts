import { describe, expect, it } from 'vitest';
import {
  committedOffsets,
  makePaymentEvent,
  produce,
  uniqueId,
  waitFor,
} from '../../src/testing/index.js';
import { spawnChildConsumer } from './child.js';
import { bootstrapServers, freshTopics, startRuntime } from './harness.js';

describe('at-least-once delivery', () => {
  it('redelivers the unfinished record to another instance after a consumer is killed mid-handler', async () => {
    const topics = await freshTopics();
    const groupId = uniqueId('crash');

    const child = spawnChildConsumer({ bootstrapServers, groupId, topics, mode: 'hang' });
    await child.next('assigned');

    const events = [1, 2, 3].map((i) =>
      makePaymentEvent({ merchant_id: 'merchant-crash', amount_cents: i }),
    );
    await produce(
      bootstrapServers,
      topics.payments,
      events.map((value) => ({ key: value.merchant_id, value, partition: 0 })),
    );

    // The child is inside the handler of the first record: kill it abruptly.
    const handling = await child.next('handling');
    expect(handling).toMatchObject({ partition: 0, offset: '0', event_id: events[0]!.event_id });
    child.process.kill('SIGKILL');
    await child.exitCode;

    expect((await committedOffsets(bootstrapServers, groupId, topics.payments))[0]).toBe(-1);

    // A second instance of the same group takes over and receives every
    // record, starting with the one that was being handled: none is skipped.
    const received: Array<{ offset: string; event_id: string }> = [];
    await startRuntime(topics, {
      config: { groupId },
      topics: [{ topic: topics.payments, schema: 'payment-event' }],
      handler: (message) => {
        received.push({ offset: message.offset, event_id: message.value.event_id });
      },
    });

    await waitFor(() => received.length >= 3, { message: 'three records redelivered' });
    expect(received).toEqual(
      events.map((event, index) => ({ offset: String(index), event_id: event.event_id })),
    );
    await waitFor(
      async () => (await committedOffsets(bootstrapServers, groupId, topics.payments))[0] === 3,
      { message: 'offsets committed by the second instance' },
    );
  });
});
