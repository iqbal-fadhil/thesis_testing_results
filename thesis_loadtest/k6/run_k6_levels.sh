#!/usr/bin/env bash
set -euo pipefail

# ensure run directory
mkdir -p k6_results

# levels to run (change if you want)
LEVELS=("10" "20" "50" "80" "100" "200" "500")
FLOWS=500
SCRIPT="full_flow_k6_shared_iterations.js"

# output CSV
OUTCSV="k6_results/k6_summary.csv"

# header
echo "architecture,concurrency,flows,total_steps,duration_sec,throughput_steps_per_sec,avg_latency_ms,min_latency_ms,max_latency_ms,error_count,error_rate,success_count" > "$OUTCSV"

for V in "${LEVELS[@]}"; do
  echo "=== Running VUs=${V}, flows=${FLOWS} ==="
  OUTJSON="k6_results/out_${V}.json"

  # run k6 (iterations = total iterations across all VUs)
  # we also export JSON summary to OUTJSON
  k6 run --vus "${V}" --iterations "${FLOWS}" --summary-export="${OUTJSON}" "${SCRIPT}"

  # parse the JSON with jq. tolerant to missing fields.
  # compute:
  # - total_steps = checks.passes + checks.fails (if present)
  # - duration_sec = iterations.count / iterations.rate (guard against zero)
  # - throughput_steps_per_sec = http_reqs.rate
  # - avg/min/max latencies from http_req_duration (in ms)
  # - error_count = http_req_failed.fails (if present) else 0
  # - error_rate = http_req_failed.value (if present) else 0
  # - success_count = checks.passes

  # jq expression:
  jq -r --arg arch "ms_go_pgsql" --arg v "${V}" --arg flows "${FLOWS}" '
    def getpath_safe(p): (getpath(p) // null);

    # metrics shortcuts
    $arch as $arch
    | ($v|tonumber) as $vnum
    | ($flows|tonumber) as $flowsnum
    | (.metrics.iterations.count // 0) as $iters_count
    | (.metrics.iterations.rate // 0) as $iters_rate
    | (if ($iters_rate == 0) then 0 else ($iters_count / $iters_rate) end) as $duration_sec
    | (.metrics.http_reqs.rate // 0) as $throughput
    | (.metrics.http_req_duration.avg // 0) as $avg_latency
    | (.metrics.http_req_duration.min // 0) as $min_latency
    | (.metrics.http_req_duration.max // 0) as $max_latency
    | (.metrics.http_req_failed.fails // 0) as $error_count
    | (.metrics.http_req_failed.value // 0) as $error_rate
    | (.metrics.checks.passes // 0) as $checks_passes
    | (.metrics.checks.fails // 0) as $checks_fails
    | (($checks_passes + $checks_fails) // 0) as $total_steps
    | [$arch, ($v|tostring), ($flows|tostring), ($total_steps|tostring), ($duration_sec|tostring), ($throughput|tostring), ($avg_latency|tostring), ($min_latency|tostring), ($max_latency|tostring), ($error_count|tostring), ($error_rate|tostring), ($checks_passes|tostring)] 
    | @csv
  ' "${OUTJSON}" >> "${OUTCSV}"

  echo "Saved summary -> ${OUTCSV}"
done

echo "All done. CSV: ${OUTCSV}"
