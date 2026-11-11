import { describe, expect, it } from 'vitest';
import type { TopicPartition } from '../../src/runtime.js';
import {
  committedOffsets,
  groupMemberCount,
  makePaymentEvent,
  produce,
  sleep,
  uniqueId,
  waitFor,
} from '../../src/testing/index.js';
import { spawnChildConsumer } from './child.js';
import { bootstrapServers, deferred, freshTopics, startRuntime } from './harness.js';

const sortPartitions = (partitions: TopicPartition[]) =>
  [...partitions].sort((a, b) => a.topic.localeCompare(b.topic) || a.partition - b.partition);

describe('graceful shutdown', () => {
  it('on SIGTERM finishes the in-flight record, commits it, leaves the group and exits', async () => {
    const topics = await freshTopics();
    const groupId = uniqueId('graceful');
    const child = spawnChildConsumer({ bootstrapServers, groupId, topics, mode: 'release' });
    await child.next('assigned');

    const first = makePaymentEvent({ merchant_id: 'merchant-graceful' });
    await produce(bootstrapServers, topics.payments, [
      { key: first.merchant_id, value: first, partition: 0 },
    ]);
    await child.next('handling', (m) => m['event_id'] === first.event_id);

    // A real SIGTERM where the platform can deliver one; on Windows the
    // child emits the same 'SIGTERM' process event when told over IPC.
    if (process.platform === 'win32') child.process.send('sigterm');
    else child.process.kill('SIGTERM');

    // The handler is still running: the process must wait for it.
    await sleep(1000);
    expect(child.process.exitCode).toBeNull();
    expect(child.messages.some((m) => m.type === 'stopped')).toBe(false);

    child.process.send('release');
    await child.next('handled', (m) => m['event_id'] === first.event_id);
    await child.next('stopped');
    const exitCode = await Promise.race([child.exitCode, sleep(15_000).then(() => 'timeout')]);
    expect(exitCode).toBe(0);
    // It left the group explicitly: no waiting for the 3 s session timeout.
    expect(await groupMemberCount(bootstrapServers, groupId)).toBe(0);

    expect((await committedOffsets(bootstrapServers, groupId, topics.payments))[0]).toBe(1);
    const messages = child.stdout.map((line) => JSON.parse(line) as Record<string, unknown>);
    expect(messages.some((m) => m['msg'] === 'termination signal received; stopping gracefully')).toBe(true);
    expect(messages.some((m) => m['msg'] === 'consumer runtime stopped')).toBe(true);

    // The next instance of the group does not receive the finished record again.
    const received: string[] = [];
    await startRuntime(topics, {
      config: { groupId },
      topics: [{ topic: topics.payments, schema: 'payment-event' }],
      handler: (message) => {
        received.push(message.value.event_id);
      },
    });
    const second = makePaymentEvent({ merchant_id: 'merchant-graceful' });
    await produce(bootstrapServers, topics.payments, [
      { key: second.merchant_id, value: second, partition: 0 },
    ]);
    await waitFor(() => received.includes(second.event_id), { message: 'second record handled' });
    expect(received).toEqual([second.event_id]);
  });
});

describe('rebalance visibility', () => {
  it('logs revoked and assigned partitions on both instances when a second one joins', async () => {
    const topics = await freshTopics();
    const groupId = uniqueId('rebalance');
    const hookCalls: Array<{ instance: string; kind: string; partitions: TopicPartition[] }> = [];
    const options = (instance: string) => ({
      config: { groupId },
      topics: [{ topic: topics.payments, schema: 'payment-event' as const }],
      handler: () => undefined,
      onPartitionsAssigned: (partitions: TopicPartition[]) => {
        hookCalls.push({ instance, kind: 'assigned', partitions });
      },
      onPartitionsRevoked: (partitions: TopicPartition[]) => {
        hookCalls.push({ instance, kind: 'revoked', partitions });
      },
    });

    const a = await startRuntime(topics, options('a'));
    const all = sortPartitions(a.runtime.assignment());
    // 6 partitions of the source topic + 6 of the retry topic.
    expect(all).toHaveLength(12);
    expect(sortPartitions(a.log.find('partitions assigned')[0]?.['partitions'] as TopicPartition[])).toEqual(all);

    const b = await startRuntime(topics, options('b'));
    await waitFor(
      () =>
        a.runtime.assignment().length > 0 &&
        b.runtime.assignment().length > 0 &&
        a.runtime.assignment().length + b.runtime.assignment().length === 12,
      { message: 'partitions split between the two instances' },
    );

    expect(await groupMemberCount(bootstrapServers, groupId)).toBe(2);

    const aRevoked = a.log.find('partitions revoked');
    const aAssigned = a.log.find('partitions assigned');
    const bAssigned = b.log.find('partitions assigned');
    expect(aRevoked.length).toBeGreaterThanOrEqual(1);
    expect(aAssigned.length).toBeGreaterThanOrEqual(2);
    expect(bAssigned.length).toBeGreaterThanOrEqual(1);
    expect(aRevoked[0]).toMatchObject({ level: 'info', group: groupId, lost: false });

    const aNow = sortPartitions(aAssigned.at(-1)?.['partitions'] as TopicPartition[]);
    const bNow = sortPartitions(bAssigned.at(-1)?.['partitions'] as TopicPartition[]);
    expect(sortPartitions([...aNow, ...bNow])).toEqual(all);
    expect(aNow).toEqual(sortPartitions(a.runtime.assignment()));
    expect(bNow).toEqual(sortPartitions(b.runtime.assignment()));

    // The hooks saw the same events as the logs.
    expect(hookCalls.filter((c) => c.instance === 'a').map((c) => c.kind)).toEqual(
      expect.arrayContaining(['assigned', 'revoked', 'assigned']),
    );
    expect(hookCalls.some((c) => c.instance === 'b' && c.kind === 'assigned')).toBe(true);
  });
});

describe('revocation hook', () => {
  it('runs onPartitionsRevoked only after the in-flight handler of a revoked partition finished', async () => {
    const topics = await freshTopics();
    const groupId = uniqueId('drain');
    const events: string[] = [];
    const entered = deferred();
    const release = deferred();

    await startRuntime(topics, {
      config: { groupId },
      topics: [{ topic: topics.payments, schema: 'payment-event' }],
      handler: async (message) => {
        if (message.value.merchant_id !== 'merchant-slow') return;
        events.push('handler started');
        entered.resolve();
        await release.promise;
        events.push('handler finished');
      },
      onPartitionsRevoked: () => {
        events.push('revoked hook');
      },
    });

    const slow = makePaymentEvent({ merchant_id: 'merchant-slow' });
    await produce(bootstrapServers, topics.payments, [{ key: slow.merchant_id, value: slow, partition: 0 }]);
    await entered.promise;

    // A second member joins: every partition of the first one is revoked
    // (eager protocol) while its handler is still running.
    const joining = startRuntime(topics, {
      config: { groupId },
      topics: [{ topic: topics.payments, schema: 'payment-event' }],
      handler: () => undefined,
    });
    await sleep(2000);
    expect(events).toEqual(['handler started']);

    release.resolve();
    await joining;
    await waitFor(() => events.includes('revoked hook'), { message: 'revocation hook called' });
    expect(events.slice(0, 3)).toEqual(['handler started', 'handler finished', 'revoked hook']);
    await waitFor(
      async () => (await committedOffsets(bootstrapServers, groupId, topics.payments))[0] === 1,
      { message: 'in-flight record committed before the partition moved' },
    );
  });
});
