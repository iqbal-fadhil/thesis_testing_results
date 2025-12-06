import http from 'k6/http';
import { check, sleep } from 'k6';

const BASE = __ENV.BASE || 'https://structure.englishqualification.my.id';
const LOGIN_PATH = __ENV.LOGIN_PATH || '/accounts/login/';
const PROFILE_PATH = __ENV.PROFILE_PATH || '/accounts/profile/';
const TEST_PATH = __ENV.TEST_PATH || '/accounts/test/'; // append id
const LOGOUT_PATH = __ENV.LOGOUT_PATH || '/accounts/logout/';
const USERNAME = __ENV.LOADTEST_USER || 'student1';
const PASSWORD = __ENV.LOADTEST_PASS || 'Student123!';

export let options = {
  thresholds: {
    http_req_failed: ['rate<0.5'],
  },
  // no built-in vus here if you prefer CLI controls
};

// helper to truncate long strings in checks/logs
function short(s, n = 400) {
  if (!s) return '';
  if (typeof s === 'object') s = JSON.stringify(s);
  return s.length > n ? s.slice(0, n) + '... (truncated)' : s;
}

// try to extract csrf token from HTML page (hidden input) or cookie
function extractCsrfFromBody(body) {
  // try double-quoted value then single-quoted
  let m = body.match(/name=['"]csrfmiddlewaretoken['"]\s+value=['"]([^'"]+)['"]/i);
  if (m && m[1]) return m[1];
  m = body.match(/<input[^>]*name=['"]csrfmiddlewaretoken['"][^>]*value=['"]([^'"]+)['"]/i);
  if (m && m[1]) return m[1];
  return null;
}

export default function () {
  const jar = http.cookieJar();

  // 1) GET login page to obtain csrftoken cookie and/or hidden token
  const loginUrl = `${BASE}${LOGIN_PATH}`;
  let res = http.get(loginUrl);
  check(res, { 'GET login page 200': (r) => r.status === 200 });

  // Try cookie first (Django sets 'csrftoken' cookie)
  let csrfToken = null;
  try {
    // k6 stores cookies by domain/path; cookieJar.cookies() convenience may vary:
    const cookies = jar.cookiesForURL(loginUrl);
    if (cookies && cookies['csrftoken'] && cookies['csrftoken'].length) {
      csrfToken = cookies['csrftoken'][0].value;
    }
  } catch (e) {
    // ignore if cookie extraction approach not available; we'll parse HTML
  }

  // fallback: try to extract from HTML hidden input
  if (!csrfToken) {
    csrfToken = extractCsrfFromBody(res.body);
  }

  // still fallback: try to read response.cookies (res.cookies)
  if (!csrfToken && res.cookies && res.cookies['csrftoken']) {
    // res.cookies['csrftoken'] is array
    csrfToken = res.cookies['csrftoken'][0].value;
  }

  check(csrfToken, { 'csrf token found': (t) => t !== null && t !== undefined });

  // 2) POST login form (Django default expects multipart/form or urlencoded; we'll use urlencoded)
  // We must include csrfmiddlewaretoken and set Referer header, and include csrftoken cookie
  const headers = {
    'Content-Type': 'application/x-www-form-urlencoded',
    'Referer': loginUrl,
    // Avoid setting `X-CSRFToken` unless your app expects it; Django will accept cookie+form.
  };

  // Ensure cookie is present in jar for subsequent requests
  if (csrfToken) {
    jar.set(BASE, 'csrftoken', csrfToken);
  }

  const payloadObj = {
    username: USERNAME,
    password: PASSWORD,
    csrfmiddlewaretoken: csrfToken || '',
    // if your LoginView uses 'next' or other fields, add them here
  };
  // encode as form body
  const formBody = Object.keys(payloadObj)
    .map((k) => `${encodeURIComponent(k)}=${encodeURIComponent(payloadObj[k])}`)
    .join('&');

  // When posting Django login form, include cookies from jar in the request by using headers Cookie
  let cookieHeader = '';
  const jarCookies = jar.cookiesForURL(loginUrl);
  if (jarCookies) {
    // assemble a simple Cookie header
    const cookiePairs = Object.keys(jarCookies).map((name) => {
      // cookie entry is array; take first
      const entry = jarCookies[name][0];
      return `${name}=${entry.value}`;
    });
    cookieHeader = cookiePairs.join('; ');
    if (cookieHeader) headers['Cookie'] = cookieHeader;
  }

  res = http.post(loginUrl, formBody, { headers: headers, redirects: 0 }); // don't auto-follow so we can inspect redirect
  // Django typically redirects on success (302) to profile; on failure it returns 200 with form+errors
  const loginSuccess = res.status === 302 || (res.status === 200 && res.body && /logout|profile/i.test(res.body) );
  check(res, {
    'login response is redirect (or contains logout/profile)': () => loginSuccess,
    'login response status <400': (r) => r.status < 400,
  });

  // After login, cookieJar should have sessionid; try to get it
  const sessionCookies = jar.cookiesForURL(BASE);
  let sessionid = null;
  if (sessionCookies && sessionCookies['sessionid']) {
    sessionid = sessionCookies['sessionid'][0].value;
  } else if (res.cookies && res.cookies['sessionid']) {
    sessionid = res.cookies['sessionid'][0].value;
    // also store it in jar for later
    jar.set(BASE, 'sessionid', sessionid);
  }
  check(sessionid, { 'sessionid present': (s) => s !== null });

  // Prepare authorised headers for subsequent requests
  const authHeaders = {
    'Referer': loginUrl,
    // keep Content-Type default for GETs
  };
  // include cookie manually to be sure
  if (sessionid || csrfToken) {
    let cookies = [];
    if (sessionid) cookies.push(`sessionid=${sessionid}`);
    if (csrfToken) cookies.push(`csrftoken=${csrfToken}`);
    authHeaders['Cookie'] = cookies.join('; ');
  }

  // 3) GET profile page
  let profileUrl = `${BASE}${PROFILE_PATH}`;
  res = http.get(profileUrl, { headers: authHeaders });
  check(res, {
    'profile status 200': (r) => r.status === 200,
    'profile contains username': (r) => r.body && r.body.indexOf(USERNAME) !== -1,
  });

  // 4) GET a test page (question) — pick id 1..10 or env provided
  const questionId = __ENV.QUESTION_ID ? parseInt(__ENV.QUESTION_ID) : Math.floor(Math.random() * 10) + 1;
  let testUrl = `${BASE}${TEST_PATH}${questionId}/`;
  res = http.get(testUrl, { headers: authHeaders });
  check(res, {
    'test page status 200': (r) => r.status === 200,
  });

  // 5) logout (if you have a custom logout view at /accounts/logout/)
  let logoutUrl = `${BASE}${LOGOUT_PATH}`;
  res = http.get(logoutUrl, { headers: authHeaders });
  check(res, {
    'logout status 200 or redirect': (r) => r.status === 200 || r.status === 302,
  });

  sleep(1);
}
