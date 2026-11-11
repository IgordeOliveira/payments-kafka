import { describe, expect, it } from 'vitest';
import { NonRetryableError } from '../../src/errors.js';
import { HEADERS } from '../../src/protocol.js';
import {
  collect,
  committedOffsets,
  createTopics,
  makePaymentEvent,
  produce,
  sleep,
  uniqueId,
  waitFor,
} from '../../src/testing/index.js';
import { bootstrapServers, freshTopics, startRuntime } from './harness.js';

describe('retry topic', () => {
  it('publishes a failed record to the retry topic with the protocol headers, then commits', async () => {
    const topics = await freshTopics();
    const { config } = await startRuntime(topics, {
      config: { retryBackoffMs: 60_000 },
      topics: [{ topic: topics.payments, schema: 'payment-event' }],
      handler: () => {
        throw new Error('database unavailable');
      },
    });

    const event = makePaymentEvent({ merchant_id: 'merchant-retry' });
    const before = Date.now();
    await produce(bootstrapServers, topics.payments, [
      { key: event.merchant_id, value: event, partition: 3 },
    ]);
    const [retry] = await collect(bootstrapServers, topics.retry, { count: 1 });
    const after = Date.now();

    expect(retry?.key).toBe('merchant-retry');
    expect(retry?.json()).toEqual(event);
    expect(retry?.headers).toEqual({
      'x-retry-group': config.groupId,
      'x-retry-attempt': '1',
      'x-retry-not-before': expect.stringMatching(/^\d+$/),
      'x-original-topic': topics.payments,
      'x-original-partition': '3',
      'x-original-offset': '0',
      'x-error-message': 'database unavailable',
    });
    const notBefore = Number(retry?.headers[HEADERS.retryNotBefore]);
    expect(notBefore).toBeGreaterThanOrEqual(before + 60_000);
    expect(notBefore).toBeLessThanOrEqual(after + 60_000);

    await waitFor(
      async () =>
        (await committedOffsets(bootstrapServers, config.groupId, topics.payments))[3] === 1,
      { message: 'source offset committed after the hand-off' },
    );
  });

  it('doubles the delay on each retry, honours it, then dead-letters with retries_exhausted', async () => {
    const topics = await freshTopics();
    const invocations: Array<{ attempt: number; at: number }> = [];
    const { config } = await startRuntime(topics, {
      config: { retryMaxAttempts: 3, retryBackoffMs: 400 },
      topics: [{ topic: topics.payments, schema: 'payment-event' }],
      handler: (message) => {
        invocations.push({ attempt: message.attempt, at: Date.now() });
        throw new Error(`still failing (attempt ${message.attempt})`);
      },
    });

    const event = makePaymentEvent({ merchant_id: 'merchant-backoff' });
    await produce(bootstrapServers, topics.payments, [
      { key: event.merchant_id, value: event, partition: 1 },
    ]);

    const [dead] = await collect(bootstrapServers, topics.dlq, { count: 1, timeoutMs: 30_000 });
    const retries = await collect(bootstrapServers, topics.retry, { count: 3 });

    // One first delivery plus RETRY_MAX_ATTEMPTS retries.
    expect(invocations.map((i) => i.attempt)).toEqual([0, 1, 2, 3]);
    expect(retries.map((r) => r.headers[HEADERS.retryAttempt])).toEqual(['1', '2', '3']);

    // x-retry-not-before = failure time + 400 * 2^(attempt-1).
    const delays = retries.map(
      (r, k) => Number(r.headers[HEADERS.retryNotBefore]) - invocations[k]!.at,
    );
    const expected = [400, 800, 1600];
    delays.forEach((delay, k) => {
      expect(delay).toBeGreaterThanOrEqual(expected[k]!);
      expect(delay).toBeLessThan(expected[k]! + 300);
    });
    // Each retry ran only once it was due.
    retries.forEach((r, k) => {
      expect(invocations[k + 1]!.at).toBeGreaterThanOrEqual(Number(r.headers[HEADERS.retryNotBefore]));
    });

    expect(dead?.json()).toEqual(event);
    expect(dead?.key).toBe('merchant-backoff');
    expect(dead?.headers).toEqual({
      'x-original-topic': topics.payments,
      'x-original-partition': '1',
      'x-original-offset': '0',
      'x-error-message': 'still failing (attempt 3)',
      'x-dlq-group': config.groupId,
      'x-dlq-reason': 'retries_exhausted',
      'x-dlq-attempts': '3',
    });

    // Not retried again.
    await sleep(1500);
    expect(invocations).toHaveLength(4);
    expect(await collect(bootstrapServers, topics.retry, { timeoutMs: 3000 })).toHaveLength(3);
  });

  it('does not republish or dead-letter a record whose retry succeeds', async () => {
    const topics = await freshTopics();
    const seen: Array<{ attempt: number; topic: string; recordTopic: string; offset: string }> = [];
    await startRuntime(topics, {
      topics: [{ topic: topics.payments, schema: 'payment-event' }],
      handler: (message) => {
        seen.push({
          attempt: message.attempt,
          topic: message.topic,
          recordTopic: message.record.topic,
          offset: message.offset,
        });
        if (message.attempt === 0) throw new Error('transient');
      },
    });

    const event = makePaymentEvent();
    await produce(bootstrapServers, topics.payments, [{ key: event.merchant_id, value: event }]);

    await waitFor(() => seen.length === 2, { message: 'first delivery and one retry' });
    expect(seen[1]).toEqual({
      attempt: 1,
      topic: topics.payments,
      recordTopic: topics.retry,
      offset: seen[0]!.offset,
    });
    await sleep(1000);
    expect(seen).toHaveLength(2);
    expect(await collect(bootstrapServers, topics.retry, { timeoutMs: 3000 })).toHaveLength(1);
    expect(await collect(bootstrapServers, topics.dlq, { timeoutMs: 3000 })).toHaveLength(0);
  });

  it('skips and commits retry records owned by another group', async () => {
    const topics = await freshTopics();
    const handled: string[] = [];
    const { config } = await startRuntime(topics, {
      topics: [{ topic: topics.payments, schema: 'payment-event' }],
      handler: (message) => {
        handled.push(message.value.event_id);
      },
    });

    const foreign = makePaymentEvent();
    const own = makePaymentEvent();
    const retryHeadersFor = (group: string) => ({
      [HEADERS.retryGroup]: group,
      [HEADERS.retryAttempt]: '1',
      [HEADERS.retryNotBefore]: String(Date.now()),
      [HEADERS.originalTopic]: topics.payments,
      [HEADERS.originalPartition]: '0',
      [HEADERS.originalOffset]: '0',
      [HEADERS.errorMessage]: 'boom',
    });
    await produce(bootstrapServers, topics.retry, [
      { key: 'k', value: foreign, partition: 4, headers: retryHeadersFor('reconciliation') },
      { key: 'k', value: own, partition: 4, headers: retryHeadersFor(config.groupId) },
    ]);

    await waitFor(() => handled.length === 1, { message: 'own retry record handled' });
    expect(handled).toEqual([own.event_id]);
    await waitFor(
      async () => (await committedOffsets(bootstrapServers, config.groupId, topics.retry))[4] === 2,
      { message: 'foreign and own retry records committed' },
    );
    expect(handled).toEqual([own.event_id]);
  });

  it('waits for x-retry-not-before by pausing the partition, without leaving the group', async () => {
    const topics = await freshTopics();
    const handled: Array<{ event_id: string; attempt: number; at: number }> = [];
    const { runtime, log, config } = await startRuntime(topics, {
      topics: [{ topic: topics.payments, schema: 'payment-event' }],
      // A short poll interval: sleeping in the handler for the 12 s delay
      // below would get this member evicted. Pausing must not.
      kafka: { consumer: { 'max.poll.interval.ms': 4000 } },
      handler: (message) => {
        handled.push({ event_id: message.value.event_id, attempt: message.attempt, at: Date.now() });
      },
    });

    const delayed = makePaymentEvent();
    const notBefore = Date.now() + 12_000;
    await produce(bootstrapServers, topics.retry, [
      {
        key: delayed.merchant_id,
        value: delayed,
        partition: 2,
        headers: {
          [HEADERS.retryGroup]: config.groupId,
          [HEADERS.retryAttempt]: '1',
          [HEADERS.retryNotBefore]: String(notBefore),
          [HEADERS.originalTopic]: topics.payments,
          [HEADERS.originalPartition]: '0',
          [HEADERS.originalOffset]: '7',
          [HEADERS.errorMessage]: 'boom',
        },
      },
    ]);

    // While the retry partition is paused, other partitions keep flowing.
    await sleep(2000);
    const live = makePaymentEvent();
    await produce(bootstrapServers, topics.payments, [{ key: live.merchant_id, value: live }]);
    await waitFor(() => handled.some((h) => h.event_id === live.event_id), {
      message: 'live record handled while the retry waits',
    });
    expect(handled.some((h) => h.event_id === delayed.event_id)).toBe(false);

    await waitFor(() => handled.find((h) => h.event_id === delayed.event_id), {
      timeoutMs: 30_000,
      message: 'delayed retry handled',
    });
    const retried = handled.find((h) => h.event_id === delayed.event_id)!;
    expect(retried.attempt).toBe(1);
    expect(retried.at).toBeGreaterThanOrEqual(notBefore);

    // Still the same, uninterrupted group membership.
    expect(log.find('partitions revoked')).toHaveLength(0);
    expect(log.find('partitions assigned')).toHaveLength(1);
    expect(runtime.assignment().length).toBeGreaterThan(0);
    expect(log.find('partition paused').length).toBeGreaterThanOrEqual(1);
  });
});

describe('dead letter queue', () => {
  it('sends a NonRetryableError straight to the DLQ, skipping the retry topic', async () => {
    const topics = await freshTopics();
    let calls = 0;
    const { config } = await startRuntime(topics, {
      topics: [{ topic: topics.payments, schema: 'payment-event' }],
      handler: () => {
        calls += 1;
        throw new NonRetryableError('conflicting event for a step already taken');
      },
    });

    const event = makePaymentEvent();
    await produce(bootstrapServers, topics.payments, [
      { key: event.merchant_id, value: event, partition: 5 },
    ]);

    const [dead] = await collect(bootstrapServers, topics.dlq, { count: 1 });
    expect(dead?.json()).toEqual(event);
    expect(dead?.headers).toEqual({
      'x-original-topic': topics.payments,
      'x-original-partition': '5',
      'x-original-offset': '0',
      'x-error-message': 'conflicting event for a step already taken',
      'x-dlq-group': config.groupId,
      'x-dlq-reason': 'non_retryable',
      'x-dlq-attempts': '0',
    });
    expect(calls).toBe(1);
    expect(await collect(bootstrapServers, topics.retry, { timeoutMs: 3000 })).toHaveLength(0);
    await waitFor(
      async () =>
        (await committedOffsets(bootstrapServers, config.groupId, topics.payments))[5] === 1,
      { message: 'offset committed after the DLQ hand-off' },
    );
  });
});

describe('hand-off safety', () => {
  it('does not commit the source offset while the retry topic cannot be written', async () => {
    const topics = await freshTopics();
    // The broker does not auto-create topics, so publishing to this retry
    // topic fails until the test creates it.
    const missingRetryTopic = `${topics.retry}.${uniqueId('missing')}`;
    const attempts: number[] = [];
    const { log, config } = await startRuntime(
      { ...topics, retry: missingRetryTopic },
      {
        topics: [{ topic: topics.payments, schema: 'payment-event' }],
        kafka: {
          producer: { 'message.timeout.ms': 1500 },
          consumer: { 'topic.metadata.refresh.interval.ms': 1000 },
        },
        handler: (message) => {
          attempts.push(message.attempt);
          if (message.attempt === 0) throw new Error('handler failure');
        },
      },
    );

    const event = makePaymentEvent();
    await produce(bootstrapServers, topics.payments, [
      { key: event.merchant_id, value: event, partition: 0 },
    ]);

    // The record is delivered again and again: the failed hand-off did not commit.
    await waitFor(() => attempts.filter((a) => a === 0).length >= 2, {
      timeoutMs: 30_000,
      message: 'record redelivered after a failed hand-off',
    });
    expect((await committedOffsets(bootstrapServers, config.groupId, topics.payments))[0]).toBe(-1);
    expect(log.find('hand-off failed; offset not committed, record will be redelivered').length).toBeGreaterThanOrEqual(1);

    // Once the retry topic exists the hand-off succeeds, the source offset is
    // committed and the retry is eventually handled.
    await createTopics(bootstrapServers, [{ topic: missingRetryTopic, partitions: 6 }]);
    await waitFor(
      async () =>
        (await committedOffsets(bootstrapServers, config.groupId, topics.payments))[0] === 1,
      { timeoutMs: 45_000, message: 'source offset committed after a successful hand-off' },
    );
    await waitFor(() => attempts.includes(1), { timeoutMs: 45_000, message: 'retry handled' });
  });
});
