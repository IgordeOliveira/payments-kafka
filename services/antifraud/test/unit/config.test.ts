import { describe, expect, it } from 'vitest';
import { ConfigError } from '@payments-kafka/consumer-runtime';
import { DEFAULT_ANTIFRAUD_CONFIG, loadAntifraudConfig } from '../../src/config.js';

describe('loadAntifraudConfig', () => {
  it('uses the design.md defaults when nothing is set', () => {
    expect(loadAntifraudConfig({})).toEqual({
      outlierFactor: 5,
      outlierMinSamples: 20,
      burstLimit: 30,
      burstWindowMs: 10_000,
      refundLimit: 5,
      refundWindowMs: 60_000,
      seenEventIdsPerMerchant: 10_000,
    });
    expect(loadAntifraudConfig({})).toEqual(DEFAULT_ANTIFRAUD_CONFIG);
  });

  it('reads every ANTIFRAUD_* variable', () => {
    expect(
      loadAntifraudConfig({
        ANTIFRAUD_OUTLIER_FACTOR: '3.5',
        ANTIFRAUD_OUTLIER_MIN_SAMPLES: '10',
        ANTIFRAUD_BURST_LIMIT: '50',
        ANTIFRAUD_BURST_WINDOW_MS: '5000',
        ANTIFRAUD_REFUND_LIMIT: '2',
        ANTIFRAUD_REFUND_WINDOW_MS: '30000',
        ANTIFRAUD_SEEN_EVENT_IDS_PER_MERCHANT: '100',
      }),
    ).toEqual({
      outlierFactor: 3.5,
      outlierMinSamples: 10,
      burstLimit: 50,
      burstWindowMs: 5000,
      refundLimit: 2,
      refundWindowMs: 30_000,
      seenEventIdsPerMerchant: 100,
    });
  });

  it.each([
    ['ANTIFRAUD_OUTLIER_FACTOR', 'five'],
    ['ANTIFRAUD_OUTLIER_FACTOR', '0.5'],
    ['ANTIFRAUD_OUTLIER_MIN_SAMPLES', '2.5'],
    ['ANTIFRAUD_OUTLIER_MIN_SAMPLES', '0'],
    ['ANTIFRAUD_BURST_LIMIT', '-1'],
    ['ANTIFRAUD_BURST_LIMIT', ''],
    ['ANTIFRAUD_BURST_WINDOW_MS', '10s'],
    ['ANTIFRAUD_REFUND_LIMIT', 'abc'],
    ['ANTIFRAUD_REFUND_WINDOW_MS', '0'],
    ['ANTIFRAUD_SEEN_EVENT_IDS_PER_MERCHANT', '0'],
  ])('rejects %s=%j with a ConfigError naming the variable', (variable, value) => {
    let thrown: unknown;
    try {
      loadAntifraudConfig({ [variable]: value });
    } catch (error) {
      thrown = error;
    }
    expect(thrown).toBeInstanceOf(ConfigError);
    expect((thrown as ConfigError).variable).toBe(variable);
    expect((thrown as Error).message).toContain(variable);
  });
});
