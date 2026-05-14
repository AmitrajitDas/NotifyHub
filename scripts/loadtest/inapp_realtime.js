import http from 'k6/http';
import ws from 'k6/ws';
import { check, fail, sleep } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';

const scenario = __ENV.K6_SCENARIO || 'smoke';
const baseURL = (__ENV.NOTIFYHUB_BASE_URL || 'http://localhost:8080').replace(/\/$/, '');
const wsURL = (__ENV.NOTIFYHUB_WS_URL || baseURL.replace(/^http/, 'ws')).replace(/\/$/, '');
const apiKey = __ENV.NOTIFYHUB_API_KEY || 'loadtest-api-key';
const recipientPrefix = __ENV.NOTIFYHUB_RECIPIENT_PREFIX || 'k6-inapp-user';
const connectJitterMs = intEnv('NOTIFYHUB_CONNECT_JITTER_MS', 1000);
const firstSendDelayMs = intEnv('NOTIFYHUB_FIRST_SEND_DELAY_MS', 1000);
const sendIntervalMs = intEnv('NOTIFYHUB_SEND_INTERVAL_MS', 5000);
const sendsPerClient = intEnv('NOTIFYHUB_SENDS_PER_CLIENT', 1);

export const wsConnected = new Counter('notifyhub_ws_connected');
export const wsMessages = new Counter('notifyhub_ws_messages');
export const wsExpectedMessages = new Counter('notifyhub_ws_expected_messages');
export const wsMissedMessages = new Counter('notifyhub_ws_missed_messages');
export const wsConnectionErrors = new Rate('notifyhub_ws_connection_errors');
export const sendErrors = new Rate('notifyhub_send_errors');
export const deliveryLatency = new Trend('notifyhub_ws_delivery_latency', true);
export const tokenLatency = new Trend('notifyhub_ws_token_latency', true);

const scenarioOptions = {
  smoke: {
    executor: 'constant-vus',
    vus: 5,
    duration: '30s',
  },
  baseline: {
    executor: 'ramping-vus',
    stages: [
      { duration: '30s', target: 100 },
      { duration: '2m', target: 100 },
      { duration: '30s', target: 0 },
    ],
  },
  prod_1000: {
    executor: 'ramping-vus',
    gracefulRampDown: '30s',
    stages: [
      { duration: '2m', target: 1000 },
      { duration: '5m', target: 1000 },
      { duration: '1m', target: 0 },
    ],
  },
};

export const options = {
  scenarios: {
    inapp_realtime: scenarioOptions[scenario] || scenarioOptions.smoke,
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],
    notifyhub_send_errors: ['rate<0.01'],
    notifyhub_ws_connection_errors: ['rate<0.01'],
    notifyhub_ws_delivery_latency: ['p(95)<2000', 'p(99)<5000'],
  },
};

export function setup() {
  const health = http.get(`${baseURL}/health`);
  if (health.status !== 200) {
    fail(`NotifyHub health check failed: status=${health.status} body=${health.body}`);
  }
}

export default function () {
  const recipientID = `${recipientPrefix}-${__VU}`;
  const tokenStarted = Date.now();
  const tokenRes = http.post(
    `${baseURL}/api/v1/ws-token`,
    JSON.stringify({ recipient_id: recipientID }),
    {
      headers: {
        'Content-Type': 'application/json',
        'X-API-Key': apiKey,
      },
      tags: { endpoint: 'ws_token', scenario },
    },
  );
  tokenLatency.add(Date.now() - tokenStarted);

  const tokenOK = check(tokenRes, {
    'ws token issued': (r) => r.status === 200 && !!responseData(r.body).token,
  });
  if (!tokenOK) {
    wsConnectionErrors.add(true);
    sleep(1);
    return;
  }

  const token = responseData(tokenRes.body).token;
  const url = `${wsURL}/api/v1/inbox/stream?token=${encodeURIComponent(token)}`;

  const expected = {};
  let sent = 0;
  let received = 0;

  const res = ws.connect(url, { tags: { endpoint: 'inbox_stream', scenario } }, (socket) => {
    socket.on('open', () => {
      wsConnected.add(1);

      const jitter = Math.floor(Math.random() * connectJitterMs);
      socket.setTimeout(() => {
        sendOne(recipientID, expected, sent);
        sent += 1;
        wsExpectedMessages.add(1);

        if (sendsPerClient > 1) {
          const interval = socket.setInterval(() => {
            if (sent >= sendsPerClient) {
              socket.clearInterval(interval);
              return;
            }
            sendOne(recipientID, expected, sent);
            sent += 1;
            wsExpectedMessages.add(1);
          }, sendIntervalMs);
        }
      }, firstSendDelayMs + jitter);
    });

    socket.on('message', (raw) => {
      const frame = safeJSON(raw);
      if (!frame || (frame.type !== 'message' && frame.type !== 'history')) {
        return;
      }

      const data = frame.data || {};
      const payload = data.payload || {};
      const id = payload.client_msg_id;
      const sentAt = payload.sent_at_ms;
      if (!id || !expected[id]) {
        return;
      }

      received += 1;
      wsMessages.add(1);
      deliveryLatency.add(Date.now() - Number(sentAt));
      delete expected[id];
    });

    socket.on('error', () => {
      wsConnectionErrors.add(true);
    });

    socket.setTimeout(() => {
      const missed = Object.keys(expected).length;
      if (missed > 0) {
        wsMissedMessages.add(missed);
      }
      check(received, {
        'received all expected ws messages': () => missed === 0,
      });
      socket.close();
    }, testWindowMs());
  });

  check(res, {
    'ws connected with 101': (r) => r && r.status === 101,
  });
  wsConnectionErrors.add(!res || res.status !== 101);
  sleep(1);
}

function sendOne(recipientID, expected, sequence) {
  const now = Date.now();
  const clientMsgID = `k6-${scenario}-${__VU}-${__ITER}-${sequence}-${now}`;
  expected[clientMsgID] = true;

  const res = http.post(
    `${baseURL}/api/v1/notifications`,
    JSON.stringify({
      type: 'loadtest',
      channel: 'inapp',
      recipient_id: recipientID,
      recipient_address: recipientID,
      payload: {
        title: 'Realtime load test',
        body: `message ${sequence} for ${recipientID}`,
        client_msg_id: clientMsgID,
        sent_at_ms: now,
        source: 'k6-inapp-realtime',
      },
      priority: 5,
    }),
    {
      headers: {
        'Content-Type': 'application/json',
        'X-API-Key': apiKey,
      },
      tags: { endpoint: 'send_notification', channel: 'inapp', scenario },
    },
  );

  const ok = check(res, {
    'notification accepted': (r) => r.status === 202,
  });
  sendErrors.add(!ok || res.status >= 500);
}

function safeJSON(raw) {
  try {
    return JSON.parse(raw);
  } catch (_) {
    return {};
  }
}

function responseData(raw) {
  const parsed = safeJSON(raw);
  return parsed.data || parsed;
}

function intEnv(name, fallback) {
  const value = Number.parseInt(__ENV[name] || '', 10);
  return Number.isFinite(value) ? value : fallback;
}

function testWindowMs() {
  return firstSendDelayMs + connectJitterMs + Math.max(5000, sendsPerClient * sendIntervalMs + 10000);
}
