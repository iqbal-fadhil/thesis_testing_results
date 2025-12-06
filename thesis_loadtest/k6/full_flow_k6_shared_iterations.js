import http from 'k6/http';
import { check, sleep } from 'k6';

const FRONTEND_BASE = __ENV.FRONTEND_BASE || 'https://microservices.iqbalfadhil.biz.id';
const AUTH_BASE     = __ENV.AUTH_BASE     || 'https://auth-microservices.iqbalfadhil.biz.id/api/auth';
const TEST_BASE     = __ENV.TEST_BASE     || 'https://test-microservices.iqbalfadhil.biz.id';
const USERNAME      = __ENV.LOADTEST_USER || 'student1';
const PASSWORD      = __ENV.LOADTEST_PASS || 'Student123!';

export let options = {
  // no built-in vus here; we'll pass --vus / --iterations via CLI or wrapper
  // keep default thresholds lightly to show failures if too many
  thresholds: {
    http_req_failed: ['rate<0.5'],
  },
};

function short(s, n = 800) {
  if (!s) return '';
  if (typeof s === 'object') s = JSON.stringify(s);
  return s.length > n ? s.slice(0, n) + '... (truncated)' : s;
}

export default function () {
  // front page
  let r = http.get(`${FRONTEND_BASE}/login`);
  check(r, { 'front status 200': (res) => res.status === 200 });

  // login
  let payload = JSON.stringify({ username: USERNAME, password: PASSWORD });
  let headers = { 'Content-Type': 'application/json' };
  r = http.post(`${AUTH_BASE}/login`, payload, { headers: headers });
  check(r, { 'login status 200': (res) => res.status === 200 });

  let j = null;
  try { j = r.json(); } catch (e) { j = null; }

  check(j, { 'login has token': (obj) => obj && (obj.token || obj.access_token) });

  // Build token usage: the auth service accepts either Authorization Bearer OR ?token=...
  let token = j && (j.token || j.access_token || (j.data && j.data.token));
  // prefer query param token because your /me example used ?token=...
  let meUrl = token ? `${AUTH_BASE}/me?token=${token}` : `${AUTH_BASE}/me`;
  r = http.get(meUrl, { headers: headers });
  check(r, { 'me status 200': (res) => res.status === 200 });

  // questions endpoint
  r = http.get(`${TEST_BASE}/questions`, { headers: headers });
  check(r, { 'questions status 200': (res) => res.status === 200 });

  // small think time
  sleep(1);
}
