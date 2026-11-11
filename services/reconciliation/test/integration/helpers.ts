/** Shared helpers for the integration tests: databases, event streams, runtimes. */
import { randomUUID } from 'node:crypto';
import type { ConsumerRuntime, PaymentEvent, PaymentEventType } from '@payments-kafka/consumer-runtime';
import {
  createCapturingLogger,
  createRuntimeTopics,
  FAST_GROUP_CONFIG,
  makePaymentEvent,
  produce,
  testRuntimeConfig,
  uniqueId,
  waitFor,
  type CapturingLogger,
  type RuntimeTopics,
} from '@payments-kafka/consumer-runtime/testing';
import pg from 'pg';
import { afterEach, inject } from 'vitest';
import { listBalances, type MerchantBalance } from '../../src/commands.js';
import { migrate } from '../../src/migrate.js';
import { createReconciliationRuntime } from '../../src/service.js';

export const bootstrap = inject('kafkaBootstrapServers');
const adminUrl = inject('postgresAdminUrl');

// ---------------------------------------------------------------------------
// Cleanup registry: runtimes are stopped before pools are closed.
// ---------------------------------------------------------------------------

const runtimes: ConsumerRuntime[] = [];
const pools: pg.Pool[] = [];

afterEach(async () => {
  await Promise.allSettled(runtimes.splice(0).map((r) => r.stop()));
  await Promise.allSettled(pools.splice(0).map((p) => p.end()));
});

// ---------------------------------------------------------------------------
// Databases
// ---------------------------------------------------------------------------

export interface TestDatabase {
  name: string;
  url: string;
  pool: pg.Pool;
}

function urlFor(database: string): string {
  const url = new URL(adminUrl);
  url.pathname = `/${database}`;
  return url.toString();
}

/** Creates an empty database (no tables). */
export async function createEmptyDatabase(): Promise<TestDatabase> {
  const name = uniqueId('recon').replace(/[^a-z0-9_]/gi, '_').toLowerCase();
  const admin = new pg.Client({ connectionString: adminUrl });
  await admin.connect();
  try {
    await admin.query(`CREATE DATABASE ${name}`);
  } finally {
    await admin.end();
  }
  const url = urlFor(name);
  return { name, url, pool: newPool(url) };
}

/** Creates a database with the schema migrated. */
export async function createDatabase(): Promise<TestDatabase> {
  const db = await createEmptyDatabase();
  await migrate(db.pool);
  return db;
}

export function newPool(url: string, max = 10): pg.Pool {
  const pool = new pg.Pool({ connectionString: url, max });
  pool.on('error', () => undefined);
  pools.push(pool);
  return pool;
}

// ---------------------------------------------------------------------------
// Events and streams
// ---------------------------------------------------------------------------

const SEQUENCE: Record<PaymentEventType, 1 | 2 | 3> = {
  'payment.created': 1,
  'payment.confirmed': 2,
  'payment.expired': 2,
  'payment.refunded': 3,
};

export type Path = 'created' | 'confirmed' | 'expired' | 'refunded';

/** The events of one payment, in order, up to and including `path`. */
export function lifecycle(merchantId: string, amountCents: number, path: Path, paymentId = randomUUID()): PaymentEvent[] {
  const types: PaymentEventType[] =
    path === 'created'
      ? ['payment.created']
      : path === 'confirmed'
        ? ['payment.created', 'payment.confirmed']
        : path === 'expired'
          ? ['payment.created', 'payment.expired']
          : ['payment.created', 'payment.confirmed', 'payment.refunded'];
  return types.map((type) =>
    makePaymentEvent({
      event_type: type,
      sequence: SEQUENCE[type],
      payment_id: paymentId,
      merchant_id: merchantId,
      amount_cents: amountCents,
    }),
  );
}

/** Small deterministic PRNG (mulberry32), so failures are reproducible. */
export function rng(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

export interface Stream {
  /** Records in production order (includes duplicates and swapped pairs). */
  records: PaymentEvent[];
  /** Distinct events, all of which are eventually applicable. */
  distinct: PaymentEvent[];
}

/**
 * A realistic stream: `merchants` × `paymentsPerMerchant` payments with mixed
 * outcomes, interleaved per merchant. A fraction of payments has the
 * confirmation produced before the creation (out of order, goes through the
 * retry topic), and a fraction of events is produced twice (duplicates).
 */
export function generateStream(options: {
  seed: number;
  merchants: number;
  paymentsPerMerchant: number;
  merchantPrefix?: string;
  outOfOrderRate?: number;
  duplicateRate?: number;
}): Stream {
  const random = rng(options.seed);
  const prefix = options.merchantPrefix ?? 'merchant';
  const outOfOrderRate = options.outOfOrderRate ?? 0.1;
  const duplicateRate = options.duplicateRate ?? 0.1;
  const distinct: PaymentEvent[] = [];
  const records: PaymentEvent[] = [];

  for (let m = 0; m < options.merchants; m++) {
    const merchantId = `${prefix}-${String(m).padStart(3, '0')}`;
    const queues: PaymentEvent[][] = [];
    for (let p = 0; p < options.paymentsPerMerchant; p++) {
      const roll = random();
      const path: Path = roll < 0.1 ? 'created' : roll < 0.3 ? 'expired' : roll < 0.75 ? 'confirmed' : 'refunded';
      const amount = 100 + Math.floor(random() * 99_900);
      const events = lifecycle(merchantId, amount, path);
      distinct.push(...events);
      if (events.length >= 2 && random() < outOfOrderRate) {
        const [created, second, ...others] = events as [PaymentEvent, PaymentEvent, ...PaymentEvent[]];
        queues.push([second, created, ...others]);
      } else {
        queues.push(events);
      }
    }
    // Interleave the payments of this merchant, keeping each payment's order.
    while (queues.some((q) => q.length > 0)) {
      const live = queues.filter((q) => q.length > 0);
      const queue = live[Math.floor(random() * live.length)]!;
      const next = queue.shift()!;
      records.push(next);
      if (random() < duplicateRate) records.push(next);
    }
  }
  return { records, distinct };
}

/** Balances as a pure function of a set of applicable events. */
export function expectedBalances(events: PaymentEvent[]): MerchantBalance[] {
  const byMerchant = new Map<string, MerchantBalance>();
  const seen = new Set<string>();
  for (const event of events) {
    if (seen.has(event.event_id)) continue;
    seen.add(event.event_id);
    const credit = event.event_type === 'payment.confirmed';
    const debit = event.event_type === 'payment.refunded';
    if (!credit && !debit) continue;
    const entry = byMerchant.get(event.merchant_id) ?? {
      merchant_id: event.merchant_id,
      balance_cents: 0,
      confirmed_count: 0,
      refunded_count: 0,
    };
    entry.balance_cents += credit ? event.amount_cents : -event.amount_cents;
    entry.confirmed_count += credit ? 1 : 0;
    entry.refunded_count += debit ? 1 : 0;
    byMerchant.set(event.merchant_id, entry);
  }
  return [...byMerchant.values()].sort((a, b) => (a.merchant_id < b.merchant_id ? -1 : 1));
}

export async function produceEvents(topic: string, events: PaymentEvent[]): Promise<void> {
  await produce(
    bootstrap,
    topic,
    events.map((event) => ({ key: event.merchant_id, value: event })),
  );
}

// ---------------------------------------------------------------------------
// Runtimes
// ---------------------------------------------------------------------------

export interface StartedService {
  runtime: ConsumerRuntime;
  logs: CapturingLogger;
  groupId: string;
}

export async function startService(options: {
  pool: pg.Pool;
  topics: RuntimeTopics;
  groupId?: string;
  retryMaxAttempts?: number;
  retryBackoffMs?: number;
}): Promise<StartedService> {
  const logs = createCapturingLogger('info');
  const config = testRuntimeConfig(bootstrap, {
    retryMaxAttempts: options.retryMaxAttempts ?? 8,
    retryBackoffMs: options.retryBackoffMs ?? 200,
    logLevel: 'info',
    ...(options.groupId ? { groupId: options.groupId } : {}),
  });
  const runtime = createReconciliationRuntime({
    config,
    pool: options.pool,
    logger: logs.logger,
    topics: { payments: options.topics.payments, retry: options.topics.retry, dlq: options.topics.dlq },
    handleSignals: false,
    kafka: { consumer: FAST_GROUP_CONFIG },
  });
  runtimes.push(runtime);
  await runtime.start();
  return { runtime, logs, groupId: config.groupId };
}

export async function newTopics(): Promise<RuntimeTopics> {
  return createRuntimeTopics(bootstrap);
}

// ---------------------------------------------------------------------------
// Assertions on state
// ---------------------------------------------------------------------------

export async function countRows(pool: pg.Pool, table: string): Promise<number> {
  const { rows } = await pool.query<{ n: string }>(`SELECT COUNT(*) AS n FROM ${table}`);
  return Number(rows[0]?.n ?? 0);
}

export async function waitForProcessed(pool: pg.Pool, count: number, timeoutMs = 90_000): Promise<void> {
  let last = -1;
  await waitFor(
    async () => {
      last = await countRows(pool, 'processed_events');
      return last >= count;
    },
    { timeoutMs, intervalMs: 200, message: `processed_events to reach ${count}` },
  ).catch((error: unknown) => {
    throw new Error(`${error instanceof Error ? error.message : String(error)} (last count ${last})`);
  });
}

export async function paymentRow(pool: pg.Pool, paymentId: string) {
  const { rows } = await pool.query<{ status: string; last_sequence: number; merchant_id: string; amount_cents: string }>(
    'SELECT status, last_sequence, merchant_id, amount_cents FROM payments WHERE payment_id = $1',
    [paymentId],
  );
  return rows[0];
}

/** Everything the service stores except timestamps, in a stable order. */
export async function snapshot(pool: pg.Pool) {
  const payments = await pool.query(
    'SELECT payment_id, merchant_id, amount_cents, status, last_sequence FROM payments ORDER BY payment_id',
  );
  const ledger = await pool.query(
    'SELECT event_id, merchant_id, payment_id, entry_type, amount_cents FROM ledger_entries ORDER BY event_id',
  );
  const processed = await pool.query('SELECT event_id FROM processed_events ORDER BY event_id');
  return {
    payments: payments.rows,
    ledger: ledger.rows,
    processed: processed.rows,
    balances: await listBalances(pool),
  };
}

export const balancesOf = listBalances;
