#!/usr/bin/env python3
# load_full_flow_500vus.py
# macOS-friendly load tester with defaults tuned for 500 flows-per-level.
# Usage examples:
#   python3 load_full_flow_500vus.py
#   python3 load_full_flow_500vus.py --concurrency-levels 10,20,100,200,500 --flows-per-level 500

import argparse
import asyncio
import aiohttp
import time
import csv
import os
import random
import sys
import json
import platform
import signal
from datetime import datetime
from statistics import mean

# -----------------------
# Defaults (override via CLI args or env)
# -----------------------
DEFAULT_ARCH = os.environ.get("ARCHITECTURE_NAME", "ms_python_mysql")
DEFAULT_FRONTEND_BASE = os.environ.get("FRONTEND_BASE", "https://microservices.iqbalfadhil.biz.id")
DEFAULT_AUTH_BASE     = os.environ.get("AUTH_BASE", "https://auth-microservices.iqbalfadhil.biz.id/api/auth")
DEFAULT_TEST_BASE     = os.environ.get("TEST_BASE", "https://test-microservices.iqbalfadhil.biz.id")
DEFAULT_USERNAME = os.environ.get("LOADTEST_USER", "student1")
DEFAULT_PASSWORD = os.environ.get("LOADTEST_PASS", "Student123!")
DEFAULT_NUM_ANSWERS = int(os.environ.get("NUM_ANSWERS", "10"))

# sensible defaults requested
DEFAULT_CONCURRENCY_LEVELS = ["10","20","50"]
# default flows per level updated to 500
DEFAULT_FLOWS_PER_LEVEL = int(os.environ.get("FLOWS_PER_LEVEL", "50"))

RESULTS_DIR = os.environ.get("RESULTS_DIR", "results")
os.makedirs(RESULTS_DIR, exist_ok=True)

# -----------------------
# CLI
# -----------------------
parser = argparse.ArgumentParser(description="Load test flows (macOS friendly, 500 flows-per-level default)")
parser.add_argument("--architecture", "-a", default=DEFAULT_ARCH)
parser.add_argument("--frontend", default=DEFAULT_FRONTEND_BASE)
parser.add_argument("--auth", default=DEFAULT_AUTH_BASE)
parser.add_argument("--test", default=DEFAULT_TEST_BASE)
parser.add_argument("--username", default=DEFAULT_USERNAME)
parser.add_argument("--password", default=DEFAULT_PASSWORD)
parser.add_argument("--num-answers", type=int, default=DEFAULT_NUM_ANSWERS)
parser.add_argument("--concurrency-levels", default=",".join(DEFAULT_CONCURRENCY_LEVELS),
                    help="comma-separated list, e.g. 10,20,100,200,500")
parser.add_argument("--flows-per-level", type=int, default=DEFAULT_FLOWS_PER_LEVEL,
                    help="total flows to execute per concurrency level (must be >= concurrency to saturate)")
parser.add_argument("--timeout", type=int, default=60, help="per-request timeout (seconds)")
parser.add_argument("--csv-prefix", default="full_flow")
parser.add_argument("--max-retries", type=int, default=1, help="simple retry count for transient errors")
parser.add_argument("--tune-connector", action="store_true",
                    help="set connector.limit_per_host = concurrency to avoid per-host throttling")
parser.add_argument("--no-ulimit-check", action="store_true", help="skip ulimit check")
args = parser.parse_args()

# Normalize concurrency list to ints, ignore invalid
try:
    CONCURRENCY_LEVELS = [int(x) for x in args.concurrency_levels.split(",") if x.strip()]
except Exception:
    CONCURRENCY_LEVELS = [10,20,100,200,500]

ARCHITECTURE_NAME = args.architecture
FRONTEND_BASE = args.frontend.rstrip("/")
AUTH_BASE = args.auth.rstrip("/")
TEST_BASE = args.test.rstrip("/")
USERNAME = args.username
PASSWORD = args.password
NUM_ANSWERS = args.num_answers
FLOWS_PER_LEVEL = args.flows_per_level
TIMEOUT = args.timeout
MAX_RETRIES = args.max_retries
TUNE_CONNECTOR = args.tune_connector

# -----------------------
# niceties for macOS: detect ulimit -n and warn if low
# -----------------------
def check_ulimit():
    try:
        import resource
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        if soft < 2048:
            print(f"[⚠️] Current soft ulimit -n is {soft}. For high concurrency (>=100) consider increasing it (e.g. ulimit -n 4096).")
        else:
            print(f"[i] ulimit -n = {soft}.")
    except Exception:
        print("[i] Couldn't read ulimit -n (resource module unavailable).")

if platform.system() == "Darwin" and not args.no_ulimit_check:
    print("[i] Detected macOS (Darwin). Applying macOS-friendly defaults.")
    check_ulimit()

# -----------------------
# helper: measure request with optional retry
# -----------------------
async def measure_step(session, layer, step_name, method, url, retries=0, **kwargs):
    start = time.monotonic()
    status = None
    error = ""
    text = ""
    try:
        async with session.request(method, url, **kwargs) as resp:
            status = resp.status
            # attempt to read small bodies safely
            try:
                text = await resp.text()
            except Exception:
                text = ""
    except Exception as e:
        error = str(e)
        if retries < MAX_RETRIES:
            await asyncio.sleep(0.1 + retries * 0.2)
            return await measure_step(session, layer, step_name, method, url, retries=retries+1, **kwargs)
    end = time.monotonic()
    latency_ms = (end - start) * 1000.0

    row = {
        "timestamp": datetime.now().isoformat(),
        "architecture": ARCHITECTURE_NAME,
        "layer": layer,
        "step": step_name,
        "method": method,
        "url": url,
        "status": status if status is not None else "",
        "latency_ms": round(latency_ms, 2),
        "error": error,
    }
    return row, text

# -----------------------
# normalize JSON safely
# -----------------------
def safe_load_json(text):
    try:
        return json.loads(text)
    except Exception:
        return None

def normalize_questions(parsed_json):
    if parsed_json is None:
        return []
    if isinstance(parsed_json, list):
        return parsed_json
    if isinstance(parsed_json, dict):
        for key in ("questions","data","items","results"):
            if key in parsed_json and isinstance(parsed_json[key], list):
                return parsed_json[key]
        if "data" in parsed_json and isinstance(parsed_json["data"], dict):
            for sub in ("questions","items","results"):
                if sub in parsed_json["data"] and isinstance(parsed_json["data"][sub], list):
                    return parsed_json["data"][sub]
        vals = list(parsed_json.values())
        if vals and isinstance(vals[0], dict):
            return vals
    return []

# -----------------------
# single flow (robust)
# -----------------------
async def run_single_flow(flow_id, session):
    results = []

    # 1) open frontend login (optional)
    url_frontend_login = f"{FRONTEND_BASE}/login"
    row, _ = await measure_step(session, "frontend", "open_frontend_login", "GET", url_frontend_login)
    results.append(row)

    # 2) login to auth API
    url_login = f"{AUTH_BASE}/login"
    payload_login = {"username": USERNAME, "password": PASSWORD}
    row, body = await measure_step(session, "api_auth", "login", "POST", url_login, json=payload_login)
    results.append(row)

    token = None
    if not row["error"] and isinstance(row.get("status"), int) and row["status"] == 200:
        data = safe_load_json(body)
        if isinstance(data, dict):
            token = data.get("token") or data.get("access_token") or (data.get("data") and data["data"].get("token"))
    if not token:
        # stop here, return what we have
        return results

    # 3) get /me (optional)
    url_me = f"{AUTH_BASE}/me?token={token}"
    row, _ = await measure_step(session, "api_auth", "me", "GET", url_me)
    results.append(row)

    # 4) get questions
    url_questions = f"{TEST_BASE}/questions"
    row, body = await measure_step(session, "api_test", "get_questions", "GET", url_questions)
    results.append(row)

    questions = []
    if not row["error"] and isinstance(row.get("status"), int) and row["status"] == 200:
        parsed = safe_load_json(body)
        questions = normalize_questions(parsed)
        if not questions and parsed:
            print(f"[DEBUG] flow {flow_id}: unexpected /questions shape -> {type(parsed)}; sample: {str(parsed)[:400]}", file=sys.stderr)

    if not questions:
        return results

    # prepare answers
    answers_payload = {"answers": []}
    for q in questions[:NUM_ANSWERS]:
        if isinstance(q, dict):
            qid = q.get("id") or q.get("question_id") or q.get("qid")
        else:
            qid = q
        if qid is None:
            continue
        selected = random.choice(["A","B","C","D"])
        answers_payload["answers"].append({"question_id": qid, "selected_option": selected})

    # 5) submit answers
    url_submit = f"{TEST_BASE}/submit?token={token}"
    row, _ = await measure_step(session, "api_test", "submit_answers", "POST", url_submit, json=answers_payload)
    results.append(row)

    return results

# -----------------------
# run level concurrency
# -----------------------
async def run_level(concurrency, flows_per_level):
    # tune connector: avoid limiting per-host too low
    limit_per_host = concurrency if TUNE_CONNECTOR else max(50, concurrency // 2)
    # ensure connector.limit >= concurrency (aiohttp uses 'limit' as total open connections)
    connector = aiohttp.TCPConnector(limit=max(concurrency, 100), limit_per_host=max(limit_per_host, 50), enable_cleanup_closed=True)
    timeout = aiohttp.ClientTimeout(total=TIMEOUT)

    all_results = []
    start_time = time.monotonic()

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        sem = asyncio.Semaphore(concurrency)
        in_progress = 0
        completed_flows = 0

        stop_event = asyncio.Event()

        async def bounded_flow(fid):
            nonlocal completed_flows
            async with sem:
                res = await run_single_flow(fid, session)
                all_results.extend(res)
                completed_flows += 1
                if completed_flows % max(1, flows_per_level // 10) == 0 or completed_flows < 10:
                    print(f"[i] level={concurrency}: completed {completed_flows}/{flows_per_level} flows")

        tasks = [asyncio.create_task(bounded_flow(i)) for i in range(flows_per_level)]

        # handle Ctrl-C gracefully
        def _cancel_all():
            for t in tasks:
                t.cancel()
        loop = asyncio.get_running_loop()
        # don't overwrite user's signal handlers permanently; just set for duration
        old_sigint = signal.getsignal(signal.SIGINT)
        try:
            loop.add_signal_handler(signal.SIGINT, _cancel_all)
        except Exception:
            pass

        try:
            await asyncio.gather(*tasks, return_exceptions=True)
        except asyncio.CancelledError:
            print("[!] Run cancelled by user.")
        finally:
            # restore original SIGINT
            try:
                if old_sigint:
                    signal.signal(signal.SIGINT, old_sigint)
            except Exception:
                pass

    end_time = time.monotonic()
    duration = end_time - start_time

    latencies = [r["latency_ms"] for r in all_results if not r["error"] and isinstance(r.get("latency_ms"), (int,float))]
    errors = [r for r in all_results if r["error"] or (isinstance(r.get("status"), int) and r.get("status",0) >= 400)]
    success_count = len(all_results) - len(errors)

    summary = {
        "architecture": ARCHITECTURE_NAME,
        "concurrency": concurrency,
        "flows": flows_per_level,
        "total_steps": len(all_results),
        "duration_sec": round(duration, 2),
        "throughput_steps_per_sec": round(len(all_results) / duration, 2) if duration > 0 else 0,
        "avg_latency_ms": round(mean(latencies), 2) if latencies else 0,
        "min_latency_ms": round(min(latencies), 2) if latencies else 0,
        "max_latency_ms": round(max(latencies), 2) if latencies else 0,
        "error_count": len(errors),
        "error_rate": round(len(errors) / len(all_results), 4) if all_results else 0,
        "success_count": success_count,
    }
    return all_results, summary

# -----------------------
# CSV writers
# -----------------------
def write_detail_csv(concurrency, results, ts):
    filename = os.path.join(RESULTS_DIR, f"{args.csv_prefix}_{ARCHITECTURE_NAME}_{concurrency}u_{ts}.csv")
    fieldnames = ["timestamp","architecture","layer","step","method","url","status","latency_ms","error"]
    with open(filename, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in results:
            w.writerow(r)
    print(f"[+] Detail CSV written: {filename}")

def write_summary_csv(summaries, ts):
    filename = os.path.join(RESULTS_DIR, f"{args.csv_prefix}_summary_{ARCHITECTURE_NAME}_{ts}.csv")
    fieldnames = ["architecture","concurrency","flows","total_steps","duration_sec","throughput_steps_per_sec",
                  "avg_latency_ms","min_latency_ms","max_latency_ms","error_count","error_rate","success_count"]
    with open(filename, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for s in summaries:
            w.writerow(s)
    print(f"[+] Summary CSV written: {filename}")

# -----------------------
# main
# -----------------------
def main():
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    summaries = []

    # convert concurrency list to ints and clamp
    concurrency_vals = []
    for c in CONCURRENCY_LEVELS:
        try:
            iv = int(c)
            if iv > 20000:
                iv = 20000
            concurrency_vals.append(iv)
        except Exception:
            continue

    print(f"[i] Running with flows_per_level={FLOWS_PER_LEVEL}, concurrency_levels={concurrency_vals}, NUM_ANSWERS={NUM_ANSWERS}")
    if TUNE_CONNECTOR:
        print("[i] TUNE_CONNECTOR enabled: connector.limit_per_host will be set to concurrency (avoid per-host throttling).")

    for c in concurrency_vals:
        actual_concurrent = min(c, FLOWS_PER_LEVEL)
        print(f"\n=== ARCH={ARCHITECTURE_NAME} | requested_concurrency={c} | flows={FLOWS_PER_LEVEL} | actual_concurrent={actual_concurrent} ===")
        if FLOWS_PER_LEVEL < c:
            print(f"[⚠️] Note: flows_per_level ({FLOWS_PER_LEVEL}) < concurrency ({c}). Actual concurrent flows will be {FLOWS_PER_LEVEL} (not {c}).")
        # run level
        results, summary = asyncio.run(run_level(c, FLOWS_PER_LEVEL))
        print(summary)
        write_detail_csv(c, results, ts)
        summaries.append(summary)

    write_summary_csv(summaries, ts)

if __name__ == "__main__":
    main()