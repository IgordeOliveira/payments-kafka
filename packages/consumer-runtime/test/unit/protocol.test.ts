import { describe, expect, it } from 'vitest';
import {
  backoffDelayMs,
  decodeHeaders,
  dlqHeaders,
  HEADERS,
  MAX_ERROR_MESSAGE_LENGTH,
  parseRetryHeaders,
  retryHeaders,
} from '../../src/protocol.js';

const original = { topic: 'pix.payments.v1', partition: 4, offset: '17' };

describe('retry/DLQ protocol helpers', () => {
  it('backoff doubles from RETRY_BACKOFF_MS', () => {
    expect([1, 2, 3, 4].map((attempt) => backoffDelayMs(1000, attempt))).toEqual([
      1000, 2000, 4000, 8000,
    ]);
    expect(() => backoffDelayMs(1000, 0)).toThrow(RangeError);
  });

  it('builds and parses retry headers', () => {
    const headers = retryHeaders({ group: 'g', attempt: 2, notBefore: 1234, original, error: 'boom' });
    expect(headers).toEqual({
      'x-retry-group': 'g',
      'x-retry-attempt': '2',
      'x-retry-not-before': '1234',
      'x-original-topic': 'pix.payments.v1',
      'x-original-partition': '4',
      'x-original-offset': '17',
      'x-error-message': 'boom',
    });
    expect(parseRetryHeaders(headers)).toEqual({
      ok: true,
      meta: { group: 'g', attempt: 2, notBefore: 1234, original },
    });
  });

  it('reports malformed retry headers', () => {
    const result = parseRetryHeaders({ [HEADERS.retryGroup]: 'g', [HEADERS.retryAttempt]: 'x' });
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.group).toBe('g');
      expect(result.error).toContain('x-retry-attempt');
      expect(result.error).toContain('x-original-topic');
    }
  });

  it('builds DLQ headers and truncates long error messages', () => {
    const headers = dlqHeaders({
      group: 'g',
      reason: 'retries_exhausted',
      attempts: 4,
      original,
      error: 'e'.repeat(5000),
    });
    expect(headers).toMatchObject({
      'x-dlq-group': 'g',
      'x-dlq-reason': 'retries_exhausted',
      'x-dlq-attempts': '4',
      'x-original-offset': '17',
    });
    expect(headers['x-error-message']).toHaveLength(MAX_ERROR_MESSAGE_LENGTH);
    expect(headers['x-error-message']?.endsWith('...')).toBe(true);
  });

  it('decodes Buffer and repeated header values', () => {
    expect(
      decodeHeaders({ a: Buffer.from('x'), b: [Buffer.from('1'), Buffer.from('2')], c: 'plain' }),
    ).toEqual({ a: 'x', b: '2', c: 'plain' });
  });
});
