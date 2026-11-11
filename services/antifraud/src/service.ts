/**
 * The antifraud handler and its partition hooks, independent of process
 * wiring (see main.ts) so tests can run several instances in one process.
 */
import {
  createLogger,
  TOPICS,
  type ConsumedMessage,
  type HandlerContext,
  type Logger,
  type TopicPartition,
} from '@payments-kafka/consumer-runtime';
import type { AntifraudConfig } from './config.js';
import { evaluate, newMerchantState, type MerchantState } from './rules.js';

export interface AntifraudServiceOptions {
  config: AntifraudConfig;
  /** Where alerts are published. Default `pix.fraud-alerts.v1`; tests pass an isolated topic. */
  alertsTopic?: string;
  /** Used by the partition hooks; pass the runtime's logger. */
  logger?: Logger;
}

export interface AntifraudService {
  handler(message: ConsumedMessage<'payment-event'>, ctx: HandlerContext): Promise<void>;
  onPartitionsAssigned(partitions: TopicPartition[]): void;
  onPartitionsRevoked(partitions: TopicPartition[], details: { lost: boolean }): void;
  /** Merchants that currently have in-memory state. */
  merchantIds(): string[];
  merchantState(merchantId: string): MerchantState | undefined;
}

interface MerchantEntry {
  state: MerchantState;
  /** `<topic>:<partition>` the merchant's events come from (the original topic for retries). */
  partitionKey: string;
}

const partitionKey = (topic: string, partition: number) => `${topic}:${partition}`;

export function createAntifraudService(options: AntifraudServiceOptions): AntifraudService {
  const { config } = options;
  const alertsTopic = options.alertsTopic ?? TOPICS.fraudAlerts;
  const logger = options.logger ?? createLogger();
  const merchants = new Map<string, MerchantEntry>();

  /** Drops every merchant whose source partition is in `partitions`; returns how many. */
  function dropMerchants(partitions: TopicPartition[]): number {
    const keys = new Set(partitions.map((p) => partitionKey(p.topic, p.partition)));
    let dropped = 0;
    for (const [merchantId, entry] of merchants) {
      if (keys.has(entry.partitionKey)) {
        merchants.delete(merchantId);
        dropped += 1;
      }
    }
    return dropped;
  }

  return {
    async handler(message, ctx) {
      const event = message.value;
      const key = partitionKey(message.topic, message.partition);
      let entry = merchants.get(event.merchant_id);
      if (!entry) {
        entry = { state: newMerchantState(event.merchant_id), partitionKey: key };
        merchants.set(event.merchant_id, entry);
      }
      entry.partitionKey = key;

      const { alerts, duplicate } = evaluate(entry.state, event, config);
      if (duplicate) ctx.logger.debug({ alerts: alerts.length }, 'event already applied; rules skipped');
      if (alerts.length === 0) return;

      // Durable before the offset is committed: the runtime commits right
      // after this returns, and a failed send throws into the retry path.
      await ctx.producer.sendMany(
        alerts.map((alert) => ({ topic: alertsTopic, key: alert.merchant_id, value: alert })),
      );
      for (const alert of alerts) {
        ctx.logger.info(
          { alert_id: alert.alert_id, rule: alert.rule, merchant_id: alert.merchant_id, details: alert.details },
          'fraud alert published',
        );
      }
    },

    onPartitionsAssigned(partitions) {
      // Start empty: nothing kept from an earlier ownership of these partitions.
      const dropped = dropMerchants(partitions);
      if (dropped > 0) logger.info({ merchants_dropped: dropped }, 'merchant state reset for assigned partitions');
    },

    onPartitionsRevoked(partitions, { lost }) {
      const dropped = dropMerchants(partitions);
      logger.info(
        { merchants_dropped: dropped, merchants_kept: merchants.size, partitions: partitions.length, lost },
        'merchant state dropped for revoked partitions',
      );
    },

    merchantIds: () => [...merchants.keys()],
    merchantState: (merchantId) => merchants.get(merchantId)?.state,
  };
}
