import { createServer } from 'node:net';
import { collect, makePaymentEvent, produce, sleep, waitFor } from '@payments-kafka/consumer-runtime/testing';
import { PostgreSqlContainer, type StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import { getContainerRuntimeClient } from 'testcontainers';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { bootstrapServers, capturingNotifier, freshTopics, startService, waitForCommitted } from './harness.js';
import { createDatabase, POSTGRES_IMAGE, queryOnce } from './postgres.js';

/** A free TCP port on this machine, chosen by the OS. */
function freePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const server = createServer();
    server.once('error', reject);
    server.listen(0, () => {
      const address = server.address();
      server.close(() => (typeof address === 'object' && address ? resolve(address.port) : reject(new Error('no port'))));
    });
  });
}

describe('storage failures', () => {
  // A Postgres of its own that this test stops and starts again. The host
  // port is fixed (random, chosen at run time) so that it survives the
  // restart; a dynamic mapping could change when the container starts again.
  let postgres: StartedPostgreSqlContainer;

  beforeAll(async () => {
    const port = await freePort();
    postgres = await new PostgreSqlContainer(POSTGRES_IMAGE).withExposedPorts({ container: 5432, host: port }).start();
  });

  afterAll(async () => {
    await postgres?.stop();
  });

  async function stopPostgres(): Promise<void> {
    const client = await getContainerRuntimeClient();
    await client.container.stop(client.container.getById(postgres.getId()), { timeout: 0 });
  }

  async function startPostgres(): Promise<void> {
    const client = await getContainerRuntimeClient();
    await client.container.start(client.container.getById(postgres.getId()));
  }

  it('sends the record to the retry topic without notifying while Postgres is down, then notifies once', async () => {
    const topics = await freshTopics();
    const databaseUrl = await createDatabase(postgres.getConnectionUri());
    const { notifier, lines } = capturingNotifier();
    // Retries are due 1, 2, 4, 8 and 16 s after each failure: ~31 s in total
    // for Postgres to come back before the record would be dead-lettered.
    const { config, log } = await startService({
      topics,
      databaseUrl,
      notifier,
      config: { retryMaxAttempts: 5, retryBackoffMs: 1_000 },
    });

    await stopPostgres();
    const confirmed = makePaymentEvent({ event_type: 'payment.confirmed', sequence: 2 });
    await produce(bootstrapServers, topics.payments, [{ key: confirmed.merchant_id, value: confirmed }]);

    const [retry] = await collect(bootstrapServers, topics.retry, { count: 1, timeoutMs: 30_000 });
    expect(retry?.headers).toMatchObject({ 'x-retry-group': config.groupId, 'x-retry-attempt': '1' });
    expect(retry?.json()).toMatchObject({ event_id: confirmed.event_id });
    expect(log.find('handler failed; record sent to retry topic').length).toBeGreaterThan(0);
    // Let at least one retry fail as well while the database is still down.
    await sleep(1_500);
    expect(lines).toEqual([]);

    await startPostgres();
    await waitFor(() => lines.length > 0, { timeoutMs: 45_000, message: 'notification after Postgres is back' });
    // Wait for the record to be fully handled (offset of the retry record that
    // succeeded committed), then make sure nothing else arrives.
    await waitFor(() => log.find('notification sent').length === 1, { message: 'notification sent logged' });
    await sleep(2_000);

    expect(lines).toEqual([expect.objectContaining({ kind: 'payment.confirmed', notification_key: confirmed.event_id })]);
    expect(await collect(bootstrapServers, topics.dlq, { timeoutMs: 1_000 })).toEqual([]);
    expect(
      await queryOnce(databaseUrl, 'SELECT notification_key FROM sent_notifications'),
    ).toEqual([{ notification_key: confirmed.event_id }]);
    await waitForCommitted(config.groupId, topics.payments, 1);
  });
});
