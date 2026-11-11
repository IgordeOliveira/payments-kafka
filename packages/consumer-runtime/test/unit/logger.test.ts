import { describe, expect, it } from 'vitest';
import { recordLogFields } from '../../src/logger.js';
import { createCapturingLogger } from '../../src/testing/index.js';

describe('createLogger', () => {
  it('writes one JSON object per line with level label, ISO time and msg', () => {
    const { logger, lines } = createCapturingLogger('info');
    logger.child({ service: 'reconciliation', group: 'reconciliation' }).info({ extra: 1 }, 'hello');
    logger.debug('filtered out');

    expect(lines).toHaveLength(1);
    const line = lines[0]!;
    expect(line).toMatchObject({
      level: 'info',
      msg: 'hello',
      service: 'reconciliation',
      group: 'reconciliation',
      extra: 1,
    });
    expect(new Date(String(line['time'])).toISOString()).toBe(line['time']);
    expect(line).not.toHaveProperty('pid');
    expect(line).not.toHaveProperty('hostname');
  });

  it('emits the standard record fields', () => {
    const { logger, lines } = createCapturingLogger('info');
    const fields = recordLogFields({
      topic: 'pix.payments.v1',
      partition: 3,
      offset: '42',
      key: 'merchant-007',
      attempt: 0,
      value: { event_id: '5f0c1c1e-3a52-4d0a-9d0e-1f3b5a7c9e11', amount_cents: 1 },
    });
    logger.child({ group: 'antifraud' }).child(fields).info('record handled');

    expect(lines[0]).toEqual({
      level: 'info',
      time: expect.any(String),
      msg: 'record handled',
      group: 'antifraud',
      topic: 'pix.payments.v1',
      partition: 3,
      offset: '42',
      key: 'merchant-007',
      attempt: 0,
      event_id: '5f0c1c1e-3a52-4d0a-9d0e-1f3b5a7c9e11',
    });
  });

  it('uses alert_id for fraud alerts', () => {
    const fields = recordLogFields({
      topic: 'pix.fraud-alerts.v1',
      partition: 0,
      offset: '1',
      key: null,
      attempt: 2,
      value: { alert_id: 'amount_outlier:x' },
    });
    expect(fields).toEqual({
      topic: 'pix.fraud-alerts.v1',
      partition: 0,
      offset: '1',
      key: null,
      attempt: 2,
      alert_id: 'amount_outlier:x',
    });
  });
});
