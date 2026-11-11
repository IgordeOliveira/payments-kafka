import { envInteger, envNumber, type Env } from '@payments-kafka/consumer-runtime';

/** Rule thresholds (design.md of add-antifraud-consumer). */
export interface AntifraudConfig {
  /** `amount_outlier` fires when amount > factor x merchant mean. */
  outlierFactor: number;
  /** Payments a merchant must have before `amount_outlier` can fire. */
  outlierMinSamples: number;
  /** `payment_burst` fires when more than this many payments fall in the window. */
  burstLimit: number;
  burstWindowMs: number;
  /** `refund_excess` fires when more than this many refunds fall in the window. */
  refundLimit: number;
  refundWindowMs: number;
  /** Per-merchant capacity of the recently seen event_id set (the dedupe horizon). */
  seenEventIdsPerMerchant: number;
}

export const DEFAULT_ANTIFRAUD_CONFIG: Readonly<AntifraudConfig> = {
  outlierFactor: 5,
  outlierMinSamples: 20,
  burstLimit: 30,
  burstWindowMs: 10_000,
  refundLimit: 5,
  refundWindowMs: 60_000,
  seenEventIdsPerMerchant: 10_000,
};

/**
 * Reads the `ANTIFRAUD_*` variables. Unset means default; an invalid value
 * throws a `ConfigError` naming the variable (same rules as `loadConfig`).
 */
export function loadAntifraudConfig(env: Env = process.env): AntifraudConfig {
  const d = DEFAULT_ANTIFRAUD_CONFIG;
  return {
    outlierFactor: envNumber(env, 'ANTIFRAUD_OUTLIER_FACTOR', d.outlierFactor, { min: 1 }),
    outlierMinSamples: envInteger(env, 'ANTIFRAUD_OUTLIER_MIN_SAMPLES', d.outlierMinSamples, { min: 1 }),
    burstLimit: envInteger(env, 'ANTIFRAUD_BURST_LIMIT', d.burstLimit, { min: 1 }),
    burstWindowMs: envInteger(env, 'ANTIFRAUD_BURST_WINDOW_MS', d.burstWindowMs, { min: 1 }),
    refundLimit: envInteger(env, 'ANTIFRAUD_REFUND_LIMIT', d.refundLimit, { min: 1 }),
    refundWindowMs: envInteger(env, 'ANTIFRAUD_REFUND_WINDOW_MS', d.refundWindowMs, { min: 1 }),
    seenEventIdsPerMerchant: envInteger(
      env,
      'ANTIFRAUD_SEEN_EVENT_IDS_PER_MERCHANT',
      d.seenEventIdsPerMerchant,
      { min: 1 },
    ),
  };
}
