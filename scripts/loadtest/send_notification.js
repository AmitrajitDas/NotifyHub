import http from 'k6/http';
import { check, fail, sleep } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';

const scenario = __ENV.K6_SCENARIO || 'baseline';
const baseURL = (__ENV.NOTIFYHUB_BASE_URL || 'http://localhost:8080').replace(/\/$/, '');
const apiKey = __ENV.NOTIFYHUB_API_KEY || 'loadtest-api-key';
const channel = __ENV.NOTIFYHUB_CHANNEL || 'inapp';
const recipientPrefix = __ENV.NOTIFYHUB_RECIPIENT_PREFIX || 'k6-user';
const idempotency = (__ENV.NOTIFYHUB_IDEMPOTENCY || 'false') === 'true';

export const sendErrors = new Rate('notifyhub_send_errors');
export const accepted = new Counter('notifyhub_accepted');
export const sendLatency = new Trend('notifyhub_send_latency', true);

const scenarios = {
  smoke: {
    executor: 'constant-vus',
    vus: 1,
    duration: '15s',
  },
  baseline: {
    executor: 'ramping-arrival-rate',
    startRate: 5,
    timeUnit: '1s',
    preAllocatedVUs: 20,
    maxVUs: 100,
    stages: [
      { duration: '30s', target: 10 },
      { duration: '1m', target: 25 },
      { duration: '30s', target: 0 },
    ],
  },
  stress: {
    executor: 'ramping-arrival-rate',
    startRate: 10,
    timeUnit: '1s',
    preAllocatedVUs: 50,
    maxVUs: 300,
    stages: [
      { duration: '1m', target: 50 },
      { duration: '2m', target: 100 },
      { duration: '1m', target: 200 },
      { duration: '1m', target: 0 },
    ],
  },
};

export const options = {
  scenarios: {
    send_notifications: scenarios[scenario] || scenarios.baseline,
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],
    http_req_duration: ['p(95)<1500', 'p(99)<3000'],
    notifyhub_send_errors: ['rate<0.01'],
  },
};

export function setup() {
  const health = http.get(`${baseURL}/health`);
  if (health.status !== 200) {
    fail(`NotifyHub health check failed: status=${health.status} body=${health.body}`);
  }
}

export default function () {
  const n = `${__VU}-${__ITER}`;
  const payload = {
    type: 'loadtest',
    channel,
    recipient_id: `${recipientPrefix}-${n}`,
    recipient_address: `${recipientPrefix}-${n}`,
    payload: {
      title: 'Load test',
      body: `k6 ${scenario} notification ${n}`,
      source: 'k6',
    },
    priority: 5,
  };

  if (idempotency) {
    payload.idempotency_key = `k6-${scenario}-${n}`;
  }

  const started = Date.now();
  const res = http.post(`${baseURL}/api/v1/notifications`, JSON.stringify(payload), {
    headers: {
      'Content-Type': 'application/json',
      'X-API-Key': apiKey,
    },
    tags: {
      endpoint: 'send_notification',
      channel,
      scenario,
    },
  });

  sendLatency.add(Date.now() - started);

  const ok = check(res, {
    'accepted or rate-limited': (r) => r.status === 202 || r.status === 429,
    'accepted': (r) => r.status === 202,
  });

  if (res.status === 202) {
    accepted.add(1);
  }
  sendErrors.add(!ok || res.status >= 500);

  sleep(0.1);
}
