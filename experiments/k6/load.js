// Run with grafana/k6:2.3.0
//
// Load scenario for the payment API: create -> confirm/expire -> (maybe) refund.
// Fed on stdin (`k6 run -`), so it has no local imports and no open().
//
// Env vars: API_URL, RUN_ID, RATE, RAMP, DURATION, MERCHANTS, CONFIRM_RATIO, REFUND_RATIO.
import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';

const API_URL = __ENV.API_URL || 'http://payment-api:8081';
const RUN_ID = __ENV.RUN_ID || 'manual';
const RATE = parseInt(__ENV.RATE || '200', 10);
const RAMP = __ENV.RAMP || '30s';
const DURATION = __ENV.DURATION || '2m';
const MERCHANTS = parseInt(__ENV.MERCHANTS || '50', 10);
const CONFIRM_RATIO = parseFloat(__ENV.CONFIRM_RATIO || '0.8');
const REFUND_RATIO = parseFloat(__ENV.REFUND_RATIO || '0.1');

const transitionsCreated = new Counter('transitions_created');
const transitionsConfirmed = new Counter('transitions_confirmed');
const transitionsExpired = new Counter('transitions_expired');
const transitionsRefunded = new Counter('transitions_refunded');

http.setResponseCallback(http.expectedStatuses(200, 201));

export const options = {
  summaryTrendStats: ["avg", "min", "med", "max", "p(90)", "p(95)", "p(99)"],
  scenarios: {
    load: {
      executor: 'ramping-arrival-rate',
      startRate: 0,
      timeUnit: '1s',
      preAllocatedVUs: Math.max(10, RATE),
      maxVUs: Math.max(50, RATE * 4),
      stages: [
        { target: RATE, duration: RAMP },
        { target: RATE, duration: DURATION },
        { target: 0, duration: RAMP },
      ],
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],
    http_req_duration: ['p(95)<500'],
  },
};

const JSON_HEADERS = { 'Content-Type': 'application/json' };

function post(path, name, body) {
  return http.post(`${API_URL}${path}`, body, {
    headers: JSON_HEADERS,
    timeout: '30s',
    tags: { name },
  });
}

export default function () {
  const nn = String(1 + Math.floor(Math.random() * MERCHANTS)).padStart(2, '0');
  const created = post(
    '/payments',
    'POST /payments',
    JSON.stringify({
      merchant_id: `load-${RUN_ID}-${nn}`,
      amount_cents: 100 + Math.floor(Math.random() * 49901),
    }),
  );
  if (!check(created, { 'create returned 201': (r) => r.status === 201 })) {
    return;
  }
  transitionsCreated.add(1);
  const id = created.json('payment_id');

  if (Math.random() < CONFIRM_RATIO) {
    const confirmed = post(`/payments/${id}/confirm`, 'POST /payments/{id}/confirm', null);
    if (confirmed.status !== 200) {
      return;
    }
    transitionsConfirmed.add(1);
    if (Math.random() < REFUND_RATIO) {
      const refunded = post(`/payments/${id}/refund`, 'POST /payments/{id}/refund', null);
      if (refunded.status === 200) {
        transitionsRefunded.add(1);
      }
    }
  } else {
    const expired = post(`/payments/${id}/expire`, 'POST /payments/{id}/expire', null);
    if (expired.status === 200) {
      transitionsExpired.add(1);
    }
  }
}

export function handleSummary(data) {
  return { stdout: JSON.stringify(data) };
}
