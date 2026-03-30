#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  bench/scripts/run_multi_dag_http_bench.sh --config <toml> --port <port> [options]

Options:
  --config <path>             Base DAGForge config file.
  --port <port>               HTTP port to benchmark.
  --dag-count <n>             Number of DAG IDs to generate. Default: 32
  --requests <n>              Total requests across all DAGs. Default: 4096
  --concurrency <n>           Total concurrency across all DAGs. Default: 64
  --scheduler-shards <n>      Override scheduler_shards in temp config.
  --runtime-shards <n>        Override scheduler.shards in temp config.
  --executor <type>           DAG task executor: shell | noop. Default: shell
  --log-level <level>         Server log level. Default: warn
  --work-dir <path>           Temp work directory root.
  --keep-work-dir             Keep generated temp files.

Example:
  bench/scripts/run_multi_dag_http_bench.sh \
    --config system_config.toml \
    --port 18889 \
    --dag-count 64 \
    --requests 4096 \
    --concurrency 64 \
    --scheduler-shards 8
EOF
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

CONFIG=""
PORT=""
DAG_COUNT=32
TOTAL_REQUESTS=4096
TOTAL_CONCURRENCY=64
SCHEDULER_SHARDS=""
RUNTIME_SHARDS=""
LOG_LEVEL="warn"
WORK_ROOT=""
KEEP_WORK_DIR=0
EXECUTOR_TYPE="shell"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --config)
      CONFIG="$2"
      shift 2
      ;;
    --port)
      PORT="$2"
      shift 2
      ;;
    --dag-count)
      DAG_COUNT="$2"
      shift 2
      ;;
    --requests)
      TOTAL_REQUESTS="$2"
      shift 2
      ;;
    --concurrency)
      TOTAL_CONCURRENCY="$2"
      shift 2
      ;;
    --scheduler-shards)
      SCHEDULER_SHARDS="$2"
      shift 2
      ;;
    --runtime-shards)
      RUNTIME_SHARDS="$2"
      shift 2
      ;;
    --log-level)
      LOG_LEVEL="$2"
      shift 2
      ;;
    --executor)
      EXECUTOR_TYPE="$2"
      shift 2
      ;;
    --work-dir)
      WORK_ROOT="$2"
      shift 2
      ;;
    --keep-work-dir)
      KEEP_WORK_DIR=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown option: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ -z "$CONFIG" || -z "$PORT" ]]; then
  usage >&2
  exit 1
fi

require_cmd perl
require_cmd hey
require_cmd curl
require_cmd mktemp

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CONFIG_ABS="$(cd "$ROOT_DIR" && python3 -c 'import os,sys; print(os.path.abspath(sys.argv[1]))' "$CONFIG")"
if [[ ! -f "$CONFIG_ABS" ]]; then
  echo "config not found: $CONFIG" >&2
  exit 1
fi

if [[ -z "$WORK_ROOT" ]]; then
  WORK_ROOT="$(mktemp -d /tmp/dagforge_multi_bench.XXXXXX)"
else
  mkdir -p "$WORK_ROOT"
fi

RUN_DIR="$WORK_ROOT/run"
DAG_DIR="$RUN_DIR/dags"
OUT_DIR="$RUN_DIR/out"
mkdir -p "$DAG_DIR" "$OUT_DIR"

cleanup() {
  set +e
  if [[ -f "$RUN_DIR/config.toml" ]]; then
    "$ROOT_DIR/bin/dagforge" serve stop -c "$RUN_DIR/config.toml" >/dev/null 2>&1 || true
  fi
  if [[ "$KEEP_WORK_DIR" -eq 0 ]]; then
    rm -rf "$WORK_ROOT"
  else
    echo "kept work dir: $WORK_ROOT"
  fi
}
trap cleanup EXIT

for ((i = 0; i < DAG_COUNT; ++i)); do
  dag_id="$(printf 'noop_http_bench_%03d' "$i")"
  if [[ "$EXECUTOR_TYPE" == "noop" ]]; then
    executor_block=$'executor = "noop"\n'
    command_line=""
  else
    executor_block=""
    command_line='command = "true"'
  fi
  cat >"$DAG_DIR/$dag_id.toml" <<EOF
id = "$dag_id"
name = "No-op HTTP Bench $i"
description = "Single-task DAG for multi-DAG HTTP end-to-end load testing"
max_active_runs = 100000

[[tasks]]
id = "noop"
name = "No-op"
${executor_block}${command_line}
EOF
done

cp "$CONFIG_ABS" "$RUN_DIR/config.toml"
PID_FILE="$RUN_DIR/dagforge.pid"
ABS_DAG_DIR="$DAG_DIR"

python3 - "$RUN_DIR/config.toml" "$ABS_DAG_DIR" "$PID_FILE" "$SCHEDULER_SHARDS" "$RUNTIME_SHARDS" <<'PY'
import pathlib
import re
import sys

cfg_path = pathlib.Path(sys.argv[1])
dag_dir = sys.argv[2]
pid_file = sys.argv[3]
scheduler_shards = sys.argv[4]
runtime_shards = sys.argv[5]
text = cfg_path.read_text()

def replace_or_append(pattern: str, replacement: str, section_hint: str | None = None) -> str:
    global text
    if re.search(pattern, text, flags=re.M):
        text = re.sub(pattern, replacement, text, count=1, flags=re.M)
    else:
        if section_hint and section_hint in text:
            text = text.replace(section_hint, section_hint + "\n" + replacement)
        else:
            text += "\n" + replacement + "\n"
    return text

replace_or_append(r'^directory\s*=\s*".*"$', f'directory = "{dag_dir}"')
replace_or_append(r'^pid_file\s*=\s*".*"$', f'pid_file = "{pid_file}"')
if 'pid_file =' not in text:
    text = f'pid_file = "{pid_file}"\n' + text
if scheduler_shards:
    replace_or_append(r'^scheduler_shards\s*=\s*\d+\s*$', f'scheduler_shards = {scheduler_shards}')
if runtime_shards:
    replace_or_append(r'^shards\s*=\s*\d+\s*$', f'shards = {runtime_shards}')

cfg_path.write_text(text)
PY

SERVER_LOG="$RUN_DIR/server.log"
"$ROOT_DIR/bin/dagforge" serve start -c "$RUN_DIR/config.toml" --log-level "$LOG_LEVEL" >"$SERVER_LOG" 2>&1 &
SERVER_PID=$!

for _ in $(seq 1 100); do
  if curl -fsS "http://127.0.0.1:$PORT/metrics" >/dev/null 2>&1; then
    break
  fi
  sleep 0.1
done

if ! curl -fsS "http://127.0.0.1:$PORT/metrics" >/dev/null 2>&1; then
  echo "server failed to start; last log lines:" >&2
  tail -n 80 "$SERVER_LOG" >&2 || true
  exit 1
fi

base_req=$((TOTAL_REQUESTS / DAG_COUNT))
req_rem=$((TOTAL_REQUESTS % DAG_COUNT))
base_conc=$((TOTAL_CONCURRENCY / DAG_COUNT))
conc_rem=$((TOTAL_CONCURRENCY % DAG_COUNT))

if (( base_req == 0 )); then
  echo "requests per DAG would be zero; increase --requests or reduce --dag-count" >&2
  exit 1
fi

start_ns="$(date +%s%N)"
hey_pids=()
for ((i = 0; i < DAG_COUNT; ++i)); do
  dag_id="$(printf 'noop_http_bench_%03d' "$i")"
  req="$base_req"
  conc="$base_conc"
  if (( i < req_rem )); then
    req=$((req + 1))
  fi
  if (( i < conc_rem )); then
    conc=$((conc + 1))
  fi
  if (( conc <= 0 )); then
    conc=1
  fi
  hey -n "$req" -c "$conc" -t 5 -m POST -T application/json -d '{}' \
    "http://127.0.0.1:$PORT/api/dags/$dag_id/trigger" \
    >"$OUT_DIR/$dag_id.txt" 2>&1 &
  hey_pids+=("$!")
done
for pid in "${hey_pids[@]}"; do
  wait "$pid"
done
end_ns="$(date +%s%N)"

python3 - "$OUT_DIR" "$DAG_COUNT" "$TOTAL_REQUESTS" "$TOTAL_CONCURRENCY" "$start_ns" "$end_ns" <<'PY'
import pathlib
import re
import sys

out_dir = pathlib.Path(sys.argv[1])
dag_count = int(sys.argv[2])
total_requests = int(sys.argv[3])
total_concurrency = int(sys.argv[4])
start_ns = int(sys.argv[5])
end_ns = int(sys.argv[6])

success = 0
errors = 0
status_500 = 0
avg_sum = 0.0
req_per_sec_sum = 0.0

for path in sorted(out_dir.glob("*.txt")):
    text = path.read_text()
    m = re.search(r"Requests/sec:\s+([0-9.]+)", text)
    if m:
      req_per_sec_sum += float(m.group(1))
    m = re.search(r"Average:\s+([0-9.]+)\s+secs", text)
    if m:
      avg_sum += float(m.group(1))
    for code, count in re.findall(r"\[(\d+)\]\s+(\d+)\s+responses", text):
      code_i = int(code)
      count_i = int(count)
      if code_i == 201:
        success += count_i
      elif code_i == 500:
        status_500 += count_i
    m = re.search(r"Error distribution:\n((?:.|\n)*)", text)
    if m:
      for count in re.findall(r"\[(\d+)\]", m.group(1)):
        errors += int(count)

wall_secs = (end_ns - start_ns) / 1_000_000_000
aggregate_rps = total_requests / wall_secs if wall_secs > 0 else 0.0

print(f"dag_count={dag_count}")
print(f"total_requests={total_requests}")
print(f"total_concurrency={total_concurrency}")
print(f"wall_time_secs={wall_secs:.4f}")
print(f"aggregate_req_per_sec={aggregate_rps:.1f}")
print(f"sum_child_req_per_sec={req_per_sec_sum:.1f}")
print(f"success_201={success}")
print(f"status_500={status_500}")
print(f"errors={errors}")
print(f"mean_child_avg_latency_ms={(avg_sum / dag_count) * 1000:.2f}")
PY

echo "--- sample child output ---"
head -n 40 "$OUT_DIR/noop_http_bench_000.txt"
