import {
  committedOffsets,
  makeFraudAlert,
  makePaymentEvent,
  produce,
  uniqueId,
  waitFor,
} from '@payments-kafka/consumer-runtime/testing';
import { describe, expect, it } from 'vitest';
import { spawnChildService } from './child.js';
import {
  bootstrapServers,
  capturingNotifier,
  freshDatabase,
  freshTopics,
  logsFor,
  startService,
  waitForCommitted,
} from './harness.js';
import { queryOnce } from './postgres.js';

const ALREADY_CLAIMED = 'notification already claimed; not sent again';

describe('never notify twice', () => {
  it('notifies a duplicated event and a regenerated alert once, also after a full restart', async () => {
    const topics = await freshTopics();
    const databaseUrl = await freshDatabase();
    const { notifier, lines } = capturingNotifier();
    const first = await startService({ topics, databaseUrl, notifier });

    const confirmed = makePaymentEvent({ event_type: 'payment.confirmed', sequence: 2, merchant_id: 'merchant-007' });
    const alert = makeFraudAlert({ merchant_id: 'merchant-007' });
    // The same event twice on its partition, and once more on another
    // partition (as a chaos-mode producer could do); the same alert_id
    // republished, as antifraud does when it reprocesses an event.
    await produce(bootstrapServers, topics.payments, [
      { key: confirmed.merchant_id, value: confirmed, partition: 1 },
      { key: confirmed.merchant_id, value: confirmed, partition: 1 },
      { key: confirmed.merchant_id, value: confirmed, partition: 4 },
    ]);
    await produce(bootstrapServers, topics.fraudAlerts, [
      { key: alert.merchant_id, value: alert, partition: 2 },
      { key: alert.merchant_id, value: { ...alert, occurred_at: new Date().toISOString() }, partition: 2 },
    ]);
    await waitForCommitted(first.config.groupId, topics.payments, 3);
    await waitForCommitted(first.config.groupId, topics.fraudAlerts, 2);

    expect(lines.map((line) => line.notification_key).sort()).toEqual([alert.alert_id, confirmed.event_id].sort());
    expect(logsFor(first.log, ALREADY_CLAIMED, confirmed.event_id)).toHaveLength(2);
    expect(logsFor(first.log, ALREADY_CLAIMED, alert.alert_id)).toHaveLength(1);

    // Full restart: every instance stops; a new one with a fresh group
    // re-reads both topics from the beginning against the same database.
    await first.service.stop();
    const restarted = await startService({ topics, databaseUrl, notifier });
    await waitForCommitted(restarted.config.groupId, topics.payments, 3);
    await waitForCommitted(restarted.config.groupId, topics.fraudAlerts, 2);

    expect(lines).toHaveLength(2);
    expect(logsFor(restarted.log, ALREADY_CLAIMED, confirmed.event_id)).toHaveLength(3);
    expect(logsFor(restarted.log, ALREADY_CLAIMED, alert.alert_id)).toHaveLength(2);
  });

  it('does not notify again when the event is redelivered after a crash between send and commit', async () => {
    const topics = await freshTopics();
    const databaseUrl = await freshDatabase();
    const groupId = uniqueId('notification-crash');

    const child = spawnChildService({ bootstrapServers, groupId, topics, databaseUrl, mode: 'hang-after-send' });
    await child.next('assigned');

    const confirmed = makePaymentEvent({ event_type: 'payment.confirmed', sequence: 2 });
    await produce(bootstrapServers, topics.payments, [{ key: confirmed.merchant_id, value: confirmed, partition: 0 }]);

    // The notification is on the child's stdout; the handler has not returned.
    await child.next('sent');
    await waitFor(() => child.notifications().length === 1, { message: 'notification line on stdout' });
    expect(child.notifications()[0]).toMatchObject({
      type: 'notification',
      kind: 'payment.confirmed',
      notification_key: confirmed.event_id,
    });
    child.process.kill('SIGKILL');
    await child.exitCode;
    expect((await committedOffsets(bootstrapServers, groupId, topics.payments))[0]).toBe(-1);

    // Restart in the same group: the record is redelivered, the claim is
    // already there, nothing is sent and the offset is now committed.
    const { notifier, lines } = capturingNotifier();
    const restarted = await startService({ topics, databaseUrl, notifier, config: { groupId } });
    await waitForCommitted(groupId, topics.payments, 1);

    expect(logsFor(restarted.log, ALREADY_CLAIMED, confirmed.event_id)).toHaveLength(1);
    expect(lines).toEqual([]);
    expect(child.notifications()).toHaveLength(1);
  });

  it('notifies exactly once when two instances of the group handle copies of the same events concurrently', async () => {
    const topics = await freshTopics();
    const databaseUrl = await freshDatabase();
    const groupId = uniqueId('notification-race');
    const { notifier, lines } = capturingNotifier(); // shared: counts across both instances

    const a = await startService({ topics, databaseUrl, notifier, config: { groupId } });
    const b = await startService({ topics, databaseUrl, notifier, config: { groupId } });
    // Wait for the rebalance to settle: both instances own payment partitions.
    await waitFor(
      () =>
        [a, b].every((instance) =>
          instance.service.runtime.assignment().some((tp) => tp.topic === topics.payments),
        ) &&
        a.service.runtime.assignment().length + b.service.runtime.assignment().length === 18,
      { message: 'both instances own partitions' },
    );

    // Every event is published once to each of the 6 partitions, so both
    // instances handle copies of it at about the same time.
    const events = Array.from({ length: 20 }, () => makePaymentEvent({ event_type: 'payment.confirmed', sequence: 2 }));
    await produce(
      bootstrapServers,
      topics.payments,
      [0, 1, 2, 3, 4, 5].flatMap((partition) =>
        events.map((value) => ({ key: value.merchant_id, value, partition })),
      ),
    );
    await waitForCommitted(groupId, topics.payments, events.length * 6);

    expect(lines).toHaveLength(events.length);
    expect(new Set(lines.map((line) => line.notification_key))).toEqual(new Set(events.map((e) => e.event_id)));
    // Both instances took part: each sent some and rejected some claims.
    for (const instance of [a, b]) {
      expect(instance.log.find('notification sent').length + instance.log.find(ALREADY_CLAIMED).length).toBeGreaterThan(0);
    }
    expect(await queryOnce(databaseUrl, 'SELECT count(*)::int AS n FROM sent_notifications')).toEqual([{ n: 20 }]);
  });
});
