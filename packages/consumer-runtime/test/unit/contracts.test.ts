import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import { describe, expect, it } from 'vitest';
import { defaultContractsDir, loadContracts } from '../../src/contracts.js';

const examplesDir = join(defaultContractsDir(), 'examples');
const readExamples = <T>(file: string): T[] =>
  JSON.parse(readFileSync(join(examplesDir, file), 'utf8')) as T[];

const contracts = loadContracts();

describe('contracts: every example in contracts/examples is classified as labelled', () => {
  const validPayments = readExamples<Record<string, unknown>>('payment-events.valid.json');
  const invalidPayments = readExamples<{ reason: string; event: unknown }>(
    'payment-events.invalid.json',
  );
  const validAlerts = readExamples<Record<string, unknown>>('fraud-alerts.valid.json');

  it('has examples to check', () => {
    expect(validPayments.length).toBeGreaterThan(0);
    expect(invalidPayments.length).toBeGreaterThan(0);
    expect(validAlerts.length).toBeGreaterThan(0);
  });

  it.each(validPayments.map((event) => [`${event['event_type']} ${event['event_id']}`, event]))(
    'accepts valid payment event %s',
    (_label, event) => {
      const result = contracts.validate('payment-event', event);
      expect(result).toEqual({ ok: true, value: event });
    },
  );

  it.each(invalidPayments.map(({ reason, event }) => [reason, event]))(
    'rejects invalid payment event: %s',
    (_reason, event) => {
      const result = contracts.validate('payment-event', event);
      expect(result.ok).toBe(false);
    },
  );

  it.each(validAlerts.map((alert) => [String(alert['alert_id']), alert]))(
    'accepts valid fraud alert %s',
    (_label, alert) => {
      expect(contracts.validate('fraud-alert', alert).ok).toBe(true);
    },
  );

  it('does not accept a payment event as a fraud alert, nor the reverse', () => {
    expect(contracts.validate('fraud-alert', validPayments[0]).ok).toBe(false);
    expect(contracts.validate('payment-event', validAlerts[0]).ok).toBe(false);
  });
});

describe('contracts: format assertions and decoding', () => {
  const valid = readExamples<Record<string, unknown>>('payment-events.valid.json')[0]!;

  it('asserts uuid and date-time formats', () => {
    expect(contracts.validate('payment-event', { ...valid, event_id: 'not-a-uuid' }).ok).toBe(false);
    expect(contracts.validate('payment-event', { ...valid, occurred_at: 'yesterday' }).ok).toBe(
      false,
    );
  });

  it('decodes a UTF-8 JSON buffer', () => {
    const result = contracts.decode('payment-event', Buffer.from(JSON.stringify(valid)));
    expect(result).toEqual({ ok: true, value: valid });
  });

  it('rejects a value that is not JSON, with a message', () => {
    const result = contracts.decode('payment-event', Buffer.from('{not json'));
    expect(result.ok).toBe(false);
    if (!result.ok) expect(result.error).toMatch(/not valid JSON/);
  });

  it('rejects a missing value', () => {
    expect(contracts.decode('payment-event', null)).toEqual({
      ok: false,
      error: 'record has no value',
    });
  });

  it('names the offending field in schema errors', () => {
    const result = contracts.validate('payment-event', { ...valid, amount_cents: '45.90' });
    expect(result.ok).toBe(false);
    if (!result.ok) expect(result.error).toContain('amount_cents');
  });

  it('fails clearly when the contracts directory is wrong', () => {
    expect(() => loadContracts({ contractsDir: join(examplesDir, 'nope') })).toThrow(
      /Cannot load contract "payment-event"/,
    );
  });
});
