import type { FraudAlert, PaymentEvent } from '@payments-kafka/consumer-runtime';
import { DEFAULT_ANTIFRAUD_CONFIG, type AntifraudConfig } from '../../src/config.js';
import { evaluate, newMerchantState, type MerchantState } from '../../src/rules.js';

/** Feeds a stream through the rules with per-merchant state, as the service does. */
export function run(
  events: readonly PaymentEvent[],
  config: AntifraudConfig = DEFAULT_ANTIFRAUD_CONFIG,
  states = new Map<string, MerchantState>(),
): FraudAlert[] {
  const alerts: FraudAlert[] = [];
  for (const event of events) {
    let state = states.get(event.merchant_id);
    if (!state) {
      state = newMerchantState(event.merchant_id);
      states.set(event.merchant_id, state);
    }
    alerts.push(...evaluate(state, event, config).alerts);
  }
  return alerts;
}

export const rulesOf = (alerts: readonly FraudAlert[]) => alerts.map((a) => a.rule);
