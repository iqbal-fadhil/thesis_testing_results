#!/usr/bin/env python3
"""
Async load tester (login-once-per-VU) with CSV output.

CSV format:
timestamp,architecture,layer,step,method,url,status,latency_ms,error

Usage:
  python load_test_csv.py --base https://example.com --vus 10 --iterations 20 --pause 0.5 --csv results.csv

Requires:
  pip install aiohttp yarl beautifulsoup4 lxml
"""
import asyncio
import aiohttp
import argparse
import csv
import os
import random
import sys
import time
from typing import Optional
from datetime import datetime, timezone
from bs4 import BeautifulSoup

# -----------------------
# Defaults
# -----------------------
DEFAULT_BASE = "https://monolithic.iqbalfadhil.biz.id"
DEFAULT_LOGIN_PATH = "/accounts/login/"
DEFAULT_PROFILE_PATH = "/accounts/profile/"
DEFAULT_TEST_PATH = "/accounts/test/"  # append id/
DEFAULT_LOGOUT_PATH = "/accounts/logout/"
DEFAULT_USERNAME = os.getenv("LOADTEST_USER", "user_1")
DEFAULT_PASSWORD = os.getenv("LOADTEST_PASS", "SecretPassword123!!")
DEFAULT_CSV = "results.csv"
DEFAULT_ARCH = "monolith_python"

SHORT_SNIPPET = 200

# -----------------------
# CSV writer task
# -----------------------
async def csv_writer_worker(csv_path: str, queue: asyncio.Queue, stop_event: asyncio.Event):
    """Consume rows from queue and write to CSV incrementally."""
    # ensure directory exists
    os.makedirs(os.path.dirname(csv_path) or ".", exist_ok=True)
    first_write = not os.path.exists(csv_path)
    # open in append mode
    with open(csv_path, "a", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        if first_write:
            writer.writerow(["timestamp", "architecture", "layer", "step", "method", "url", "status", "latency_ms", "error"])
            fh.flush()
        while True:
            try:
                row = await asyncio.wait_for(queue.get(), timeout=0.5)
            except asyncio.TimeoutError:
                if stop_event.is_set() and queue.empty():
                    break
                continue
            writer.writerow(row)
            fh.flush()
            queue.task_done()
    # drain remaining if any
    while not queue.empty():
        row = queue.get_nowait()
        with open(csv_path, "a", newline="", encoding="utf-8") as fh:
            csv.writer(fh).writerow(row)
            queue.task_done()


# -----------------------
# Helpers
# -----------------------
def iso_now():
    return datetime.now(timezone.utc).astimezone().isoformat()

def short(s, n=SHORT_SNIPPET):
    if s is None: return ""
    s = str(s)
    return s if len(s) <= n else s[:n] + "...(truncated)"

def extract_csrf_from_html(html: str) -> Optional[str]:
    if not html:
        return None
    soup = BeautifulSoup(html, "lxml")
    inp = soup.find("input", {"name": "csrfmiddlewaretoken"})
    if inp and inp.get("value"):
        return inp.get("value")
    return None

# -----------------------
# Request wrapper that measures latency and writes CSV row
# -----------------------
async def timed_request(session: aiohttp.ClientSession, method: str, url: str, *,
                        headers=None, data=None, params=None,
                        architecture="monolith_python", layer="unknown",
                        step="unknown", csv_queue: asyncio.Queue = None):
    start = time.perf_counter()
    status = None
    error = ""
    try:
        method_up = method.upper()
        if method_up == "GET":
            async with session.get(url, headers=headers, params=params) as resp:
                status = resp.status
                await resp.read()  # ensure body consumed so cookies are stored
                text_snippet = await resp.text()
        elif method_up == "POST":
            async with session.post(url, headers=headers, data=data, params=params, allow_redirects=False) as resp:
                status = resp.status
                # read body to allow inspection later
                text_snippet = await resp.text()
        else:
            # fallback
            async with session.request(method_up, url, headers=headers, data=data, params=params) as resp:
                status = resp.status
                text_snippet = await resp.text()
    except Exception as e:
        status = 0
        error = str(e)
        text_snippet = ""
    stop = time.perf_counter()
    latency_ms = (stop - start) * 1000.0
    # prepare csv row
    row = [iso_now(), architecture, layer, step, method.upper(), url, status, f"{latency_ms:.2f}", error]
    if csv_queue is not None:
        await csv_queue.put(row)
    return status, text_snippet, error, latency_ms

# -----------------------
# VU worker: login once, then iterations, then logout
# -----------------------
async def virtual_user(vu_index: int, base: str, paths: dict, username: str, password: str,
                       iterations: int, pause: float, csv_queue: asyncio.Queue,
                       architecture: str, question_id_env: Optional[str], login_retries: int = 2):
    """Each VU uses its own session (cookie jar). Login once, do iterations, logout at end."""
    timeout = aiohttp.ClientTimeout(total=90)
    connector = aiohttp.TCPConnector(limit_per_host=0)  # no per-host limit (tweak if needed)
    async with aiohttp.ClientSession(timeout=timeout, connector=connector, trust_env=True) as session:
        # jitter to avoid synchronized bursts
        await asyncio.sleep(random.uniform(0, 0.2))

        login_url = base.rstrip("/") + paths["login"]
        profile_url = base.rstrip("/") + paths["profile"]
        test_path = paths["test"]
        logout_url = base.rstrip("/") + paths["logout"]

        # attempt login with small retry loop
        logged_in = False
        last_login_err = ""
        for attempt in range(1, login_retries + 1):
            # GET login page fresh to fetch CSRF & cookies
            status_get, body, err, lat = await timed_request(session, "GET", login_url,
                                                             architecture=architecture, layer="frontend", step="open_login",
                                                             csv_queue=csv_queue)
            # extract csrf from cookie or hidden input
            csrftoken = None
            # try cookies returned in session.cookie_jar
            for c in session.cookie_jar:
                try:
                    if getattr(c, "key", None) == "csrftoken":
                        csrftoken = c.value
                        break
                except Exception:
                    continue
            if not csrftoken:
                # fallback to HTML hidden input
                csrftoken = extract_csrf_from_html(body)

            # prepare payload and headers
            payload = {
                "username": username,
                "password": password,
                "csrfmiddlewaretoken": csrftoken or ""
            }
            headers = {
                "Referer": login_url,
                "Content-Type": "application/x-www-form-urlencoded",
            }
            # include X-CSRFToken header too (helps some protections)
            if csrftoken:
                headers["X-CSRFToken"] = csrftoken

            # small jitter before POST to reduce bursts
            await asyncio.sleep(random.uniform(0, 0.1))

            # POST login
            status_post, post_body, post_err, post_lat = await timed_request(session, "POST", login_url,
                                                                             headers=headers, data=payload,
                                                                             architecture=architecture, layer="api_auth",
                                                                             step="login",
                                                                             csv_queue=csv_queue)
            # successful login expected 302 (redirect) or 200 with profile text (rare)
            if status_post in (301, 302):
                # session cookie should be present now
                # confirm sessionid exists
                sessionid = None
                for c in session.cookie_jar:
                    try:
                        if getattr(c, "key", None) == "sessionid":
                            sessionid = c.value
                            break
                    except Exception:
                        continue
                if sessionid:
                    logged_in = True
                    break
                else:
                    last_login_err = "no sessionid after 302"
            else:
                # status 200 - login page returned again (failed login). capture snippet for debug.
                last_login_err = f"login returned {status_post}"
            # short backoff before retry
            await asyncio.sleep(0.2 * attempt)

        if not logged_in:
            # record a row indicating login permanently failed for this VU and bail out
            await csv_queue.put([iso_now(), architecture, "api_auth", "login", "POST", login_url, 0, "0.0", f"login_failed:{last_login_err}"])
            # Do not attempt iterations if login failed
            return

        # At this point we're logged in. Proceed to iterations using the same session.
        for i in range(1, iterations + 1):
            # 1) GET profile (authorized)
            await asyncio.sleep(pause)  # pacing
            status_prof, prof_body, prof_err, prof_lat = await timed_request(session, "GET", profile_url,
                                                                            architecture=architecture, layer="frontend", step="profile",
                                                                            csv_queue=csv_queue)
            # 2) GET a test page
            # choose question id
            if question_id_env:
                try:
                    qid = int(question_id_env)
                except Exception:
                    qid = random.randint(1, 10)
            else:
                qid = random.randint(1, 10)
            test_url = base.rstrip("/") + test_path + str(qid) + "/"
            status_test, test_body, test_err, test_lat = await timed_request(session, "GET", test_url,
                                                                             architecture=architecture, layer="frontend", step="test_page",
                                                                             csv_queue=csv_queue)
            # Optionally: if profile shows not logged in (the test returned login page), treat it as a failure and try re-login
            # simple detection: if profile page contains known login marker (e.g., 'Login | TOEFL') then re-login once
            if status_prof == 200 and ("Login | TOEFL Preparation" in prof_body or "Please login" in prof_body):
                # session dropped; try to re-login once
                # log event to CSV
                await csv_queue.put([iso_now(), architecture, "api_auth", "rehydrate_login", "INFO", login_url, 0, "0.0", "session_lost_relogin"])
                # attempt single re-login
                # do GET and POST again (no long loop)
                status_get, body, err_get, _ = await timed_request(session, "GET", login_url,
                                                                   architecture=architecture, layer="frontend", step="open_login",
                                                                   csv_queue=csv_queue)
                csrftoken = extract_csrf_from_html(body) or None
                if not csrftoken:
                    for c in session.cookie_jar:
                        try:
                            if getattr(c, "key", None) == "csrftoken":
                                csrftoken = c.value
                                break
                        except Exception:
                            continue
                payload = {"username": username, "password": password, "csrfmiddlewaretoken": csrftoken or ""}
                headers = {"Referer": login_url, "Content-Type": "application/x-www-form-urlencoded"}
                if csrftoken:
                    headers["X-CSRFToken"] = csrftoken
                status_post, post_body, post_err, post_lat = await timed_request(session, "POST", login_url,
                                                                                 headers=headers, data=payload,
                                                                                 architecture=architecture, layer="api_auth",
                                                                                 step="login_recovery", csv_queue=csv_queue)
                # if still not logged in, break early
                if status_post not in (301, 302):
                    await csv_queue.put([iso_now(), architecture, "api_auth", "login_recovery_failed", "POST", login_url, status_post, f"{post_lat:.2f}", "login_recovery_failed"])
                    return

        # End: perform logout to clean server-side session (optional)
        await asyncio.sleep(random.uniform(0, 0.1))
        status_lo, lo_body, lo_err, lo_lat = await timed_request(session, "GET", logout_url,
                                                                 architecture=architecture, layer="api_auth", step="logout",
                                                                 csv_queue=csv_queue)
        # done for this VU
        return

# -----------------------
# Main runner
# -----------------------
def parse_args():
    p = argparse.ArgumentParser(description="Async load tester (login-once-per-VU) with CSV output")
    p.add_argument("--base", default=DEFAULT_BASE, help="Base URL (include scheme)")
    p.add_argument("--login-path", default=DEFAULT_LOGIN_PATH, help="Login path")
    p.add_argument("--profile-path", default=DEFAULT_PROFILE_PATH, help="Profile path")
    p.add_argument("--test-path", default=DEFAULT_TEST_PATH, help="Test path (append id/)")
    p.add_argument("--logout-path", default=DEFAULT_LOGOUT_PATH, help="Logout path")
    p.add_argument("--username", default=DEFAULT_USERNAME, help="Username")
    p.add_argument("--password", default=DEFAULT_PASSWORD, help="Password")
    p.add_argument("--vus", type=int, default=1, help="Number of virtual users")
    p.add_argument("--iterations", type=int, default=1, help="Iterations per VU (after login)")
    p.add_argument("--pause", type=float, default=1.0, help="Pause (seconds) between iterations")
    p.add_argument("--csv", default=DEFAULT_CSV, help="CSV output path")
    p.add_argument("--architecture", default=DEFAULT_ARCH, help="Architecture name to record in CSV")
    p.add_argument("--question-id", default=None, help="Optional fixed question id")
    return p.parse_args()

async def main_async(args):
    paths = {"login": args.login_path, "profile": args.profile_path, "test": args.test_path, "logout": args.logout_path}
    q = asyncio.Queue()
    stop_event = asyncio.Event()
    writer_task = asyncio.create_task(csv_writer_worker(args.csv, q, stop_event))

    # spawn VUs
    vu_tasks = []
    for v in range(1, args.vus + 1):
        t = asyncio.create_task(virtual_user(v, args.base, paths, args.username, args.password,
                                             args.iterations, args.pause, q, args.architecture, args.question_id))
        vu_tasks.append(t)

    # wait for all VUs to finish
    try:
        await asyncio.gather(*vu_tasks)
    finally:
        # signal writer to stop once queue drained
        stop_event.set()
        await q.join()
        await writer_task

def main():
    args = parse_args()
    print("Starting load test with:")
    print(f"  base={args.base} vus={args.vus} iterations={args.iterations} pause={args.pause} csv={args.csv}")
    try:
        asyncio.run(main_async(args))
    except KeyboardInterrupt:
        print("Interrupted", file=sys.stderr)

if __name__ == "__main__":
    main()

