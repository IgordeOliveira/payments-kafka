import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { afterEach, describe, expect, it } from 'vitest';
import {
  collect,
  createRuntimeTopics,
  makePaymentEvent,
  produce,
  uniqueId,
  waitFor,
} from '../../src/testing/index.js';
import { bootstrapServers } from './harness.js';

const sample = fileURLToPath(new URL('../../examples/sample-service.ts', import.meta.url));
const cleanups: Array<() => void> = [];
afterEach(() => cleanups.splice(0).forEach((cleanup) => cleanup()));

describe('README sample service', () => {
  it('runs against Kafka: handles both contracts and publishes its own records', async () => {
    // The sample uses the real topic names (pix.payments.v1...).
    const topics = await createRuntimeTopics(bootstrapServers, { unique: false });
    const groupId = uniqueId('sample');
    const child = spawn(process.execPath, ['--import', 'tsx', sample], {
      env: {
        ...process.env,
        KAFKA_BOOTSTRAP_SERVERS: bootstrapServers,
        KAFKA_GROUP_ID: groupId,
        LOG_LEVEL: 'info',
        SAMPLE_LARGE_PAYMENT_CENTS: '100000',
      },
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    cleanups.push(() => child.kill('SIGKILL'));
    const lines: Array<Record<string, unknown>> = [];
    let stderr = '';
    child.stdout.setEncoding('utf8').on('data', (chunk: string) => {
      for (const line of chunk.split('\n')) if (line.trim()) lines.push(JSON.parse(line));
    });
    child.stderr.setEncoding('utf8').on('data', (chunk: string) => (stderr += chunk));
    const logged = (msg: string, predicate: (line: Record<string, unknown>) => boolean = () => true) =>
      waitFor(() => lines.find((line) => line['msg'] === msg && predicate(line)), {
        timeoutMs: 45_000,
        message: `log "${msg}" (stderr: ${stderr.slice(-1000)})`,
      });

    await logged('partitions assigned');

    const small = makePaymentEvent({ merchant_id: 'merchant-sample', amount_cents: 990 });
    const large = makePaymentEvent({ merchant_id: 'merchant-sample', amount_cents: 250_000 });
    await produce(bootstrapServers, topics.payments, [
      { key: small.merchant_id, value: small },
      { key: large.merchant_id, value: large },
    ]);

    const handled = await logged('payment event handled', (l) => l['event_id'] === small.event_id);
    expect(handled).toMatchObject({
      level: 'info',
      service: 'sample',
      group: groupId,
      topic: topics.payments,
      key: 'merchant-sample',
      attempt: 0,
      event_type: 'payment.created',
    });

    // The large payment made the handler publish an alert, which the same
    // service then consumes through its second subscription.
    const alertId = `amount_outlier:${large.event_id}`;
    await logged('fraud alert received', (l) => l['alert_id'] === alertId);
    const alerts = await collect(bootstrapServers, topics.fraudAlerts, { timeoutMs: 3000 });
    expect(alerts.map((a) => a.json<{ alert_id: string }>().alert_id)).toContain(alertId);
  });
});
