import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { Ajv2020, type ValidateFunction } from 'ajv/dist/2020.js';
import addFormatsModule from 'ajv-formats';

// ajv-formats is CommonJS with a `default` property; normalize the import.
const addFormats = ((addFormatsModule as unknown as { default?: unknown }).default ??
  addFormatsModule) as unknown as typeof addFormatsModule.default;

/** Value of `pix.payments.v1`. See `contracts/payment-event.v1.schema.json`. */
export interface PaymentEvent {
  event_id: string;
  event_type: PaymentEventType;
  schema_version: 1;
  /** ISO-8601, UTC. */
  occurred_at: string;
  payment_id: string;
  merchant_id: string;
  /** created = 1, confirmed or expired = 2, refunded = 3. */
  sequence: 1 | 2 | 3;
  amount_cents: number;
  currency: 'BRL';
}

export type PaymentEventType =
  | 'payment.created'
  | 'payment.confirmed'
  | 'payment.expired'
  | 'payment.refunded';

/** Value of `pix.fraud-alerts.v1`. See `contracts/fraud-alert.v1.schema.json`. */
export interface FraudAlert {
  /** Deterministic: `<rule>:<triggered_by_event_id>`. */
  alert_id: string;
  schema_version: 1;
  rule: FraudRule;
  merchant_id: string;
  payment_id: string;
  triggered_by_event_id: string;
  /** `occurred_at` of the triggering event, ISO-8601, UTC. */
  occurred_at: string;
  details: Record<string, unknown>;
}

export type FraudRule = 'amount_outlier' | 'payment_burst' | 'refund_excess';

/** Maps each contract name to the TypeScript type of a valid value. */
export interface SchemaTypes {
  'payment-event': PaymentEvent;
  'fraud-alert': FraudAlert;
}

/** Name of a contract in `contracts/`, used in a topic's subscription. */
export type SchemaName = keyof SchemaTypes;

export const SCHEMA_FILES: Readonly<Record<SchemaName, string>> = {
  'payment-event': 'payment-event.v1.schema.json',
  'fraud-alert': 'fraud-alert.v1.schema.json',
};

export type ValidationResult<T> = { ok: true; value: T } | { ok: false; error: string };

export interface Contracts {
  /** Validates an already-parsed value against a contract. */
  validate<S extends SchemaName>(schema: S, value: unknown): ValidationResult<SchemaTypes[S]>;
  /**
   * Decodes a raw record value (UTF-8 JSON) and validates it. A missing value,
   * invalid JSON and a schema violation all yield `ok: false` with a message.
   */
  decode<S extends SchemaName>(
    schema: S,
    raw: Buffer | string | null | undefined,
  ): ValidationResult<SchemaTypes[S]>;
}

/**
 * Default location of the contracts: `<repo>/contracts`, resolved from this
 * module (`packages/consumer-runtime/{src,dist}/contracts.*`). Container
 * images keep the same layout (`/app/contracts`, see the README).
 */
export function defaultContractsDir(): string {
  return fileURLToPath(new URL('../../../contracts/', import.meta.url));
}

/**
 * Loads the JSON Schemas from `contracts/` and compiles them with Ajv
 * (draft 2020-12, `format` assertions enabled). Call once at startup.
 */
export function loadContracts(options: { contractsDir?: string } = {}): Contracts {
  const dir = options.contractsDir ?? defaultContractsDir();
  const ajv = new Ajv2020({ allErrors: true, strict: true, strictTypes: false });
  addFormats(ajv);

  const validators = new Map<SchemaName, ValidateFunction>();
  for (const [name, file] of Object.entries(SCHEMA_FILES) as [SchemaName, string][]) {
    const path = join(dir, file);
    let schema: unknown;
    try {
      schema = JSON.parse(readFileSync(path, 'utf8'));
    } catch (error) {
      throw new Error(`Cannot load contract "${name}" from ${path}: ${(error as Error).message}`, {
        cause: error,
      });
    }
    validators.set(name, ajv.compile(schema as object));
  }

  const validate = <S extends SchemaName>(
    schema: S,
    value: unknown,
  ): ValidationResult<SchemaTypes[S]> => {
    const fn = validators.get(schema);
    if (!fn) return { ok: false, error: `unknown schema "${schema}"` };
    if (fn(value)) return { ok: true, value: value as SchemaTypes[S] };
    return {
      ok: false,
      error: `schema ${schema} violated: ${ajv.errorsText(fn.errors, { dataVar: 'value' })}`,
    };
  };

  const decode = <S extends SchemaName>(
    schema: S,
    raw: Buffer | string | null | undefined,
  ): ValidationResult<SchemaTypes[S]> => {
    if (raw === null || raw === undefined) return { ok: false, error: 'record has no value' };
    const text = typeof raw === 'string' ? raw : raw.toString('utf8');
    let parsed: unknown;
    try {
      parsed = JSON.parse(text);
    } catch (error) {
      return { ok: false, error: `value is not valid JSON: ${(error as Error).message}` };
    }
    return validate(schema, parsed);
  };

  return { validate, decode };
}
