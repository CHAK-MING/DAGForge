import json
import os
import re
import sys

import pymysql
from bench_db_env import get_bench_db_name, open_bench_db_connection


DB_NAME = get_bench_db_name()

HTTP_RE = re.compile(
    r"HTTP trigger completed dag_id=(?P<dag_id>\S+) dag_run_id=(?P<run_id>\S+) latency_ms=(?P<latency>\d+)"
)
OWNER_RE = re.compile(
    r"trigger_run_on_dag_owner_shard dag_id=(?P<dag_id>\S+) dag_run_id=(?P<run_id>\S+) "
    r"ensure_rowids_ms=(?P<ensure>\d+) run_create_ms=(?P<create>\d+) total_ms=(?P<total>\d+)"
)
PERSIST_RE = re.compile(
    r"trigger_run_on_owner_shard dag_id=(?P<dag_id>\S+) dag_run_id=(?P<run_id>\S+) "
    r"first_persist_ms=(?P<persist>\d+) dispatch_ms=(?P<dispatch>\d+) total_ms=(?P<total>\d+)"
)


def parse_trigger_timings(lines):
    out = {}
    for raw in lines:
        line = re.sub(r"\x1b\[[0-9;]*m", "", raw.strip())
        for regex, fields in (
            (HTTP_RE, ("http_latency_ms", "latency")),
            (OWNER_RE, ("ensure_rowids_ms", "ensure", "run_create_ms", "create", "owner_total_ms", "total")),
            (PERSIST_RE, ("first_persist_ms", "persist", "dispatch_ms", "dispatch", "persist_total_ms", "total")),
        ):
            match = regex.search(line)
            if not match:
                continue
            run_id = match.group("run_id")
            item = out.setdefault(run_id, {})
            if regex is HTTP_RE:
                item["http_latency_ms"] = int(match.group("latency"))
            elif regex is OWNER_RE:
                item["ensure_rowids_ms"] = int(match.group("ensure"))
                item["run_create_ms"] = int(match.group("create"))
                item["owner_total_ms"] = int(match.group("total"))
            else:
                item["first_persist_ms"] = int(match.group("persist"))
                item["dispatch_ms"] = int(match.group("dispatch"))
                item["persist_total_ms"] = int(match.group("total"))
            break
    return out


def load_result_artifact(path):
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def open_db_connection():
    return open_bench_db_connection(pymysql)


def sql_quote(value):
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def run_ids_clause(run_ids):
    if not run_ids:
        raise ValueError("run_ids must not be empty")
    return ",".join(sql_quote(run_id) for run_id in run_ids)


def fetch_lag_breakdown(run_ids):
    sql = f"""
    WITH RankedAttempts AS (
        SELECT
            ti.run_rowid,
            ti.task_rowid,
            ti.attempt,
            ti.started_at,
            ti.finished_at,
            ROW_NUMBER() OVER (
                PARTITION BY ti.run_rowid, ti.task_rowid
                ORDER BY ti.attempt DESC
            ) AS attempt_rank
        FROM task_instances ti
        JOIN dag_runs r ON r.run_rowid = ti.run_rowid
        WHERE r.dag_run_id IN ({run_ids_clause(run_ids)}) AND ti.attempt > 0
    ),
    LatestTaskInstances AS (
        SELECT run_rowid, task_rowid, started_at, finished_at
        FROM RankedAttempts
        WHERE attempt_rank = 1
    ),
    TaskDeps AS (
        SELECT
            ti.run_rowid,
            ti.task_rowid,
            ti.started_at AS task_started_at,
            COUNT(td.dep_rowid) AS dep_count,
            MAX(up_ti.finished_at) AS max_upstream_finished_at,
        FROM LatestTaskInstances ti
        JOIN dag_runs r ON r.run_rowid = ti.run_rowid
        LEFT JOIN task_dependencies td
            ON td.dag_rowid = r.dag_rowid
            AND td.task_rowid = ti.task_rowid
        LEFT JOIN LatestTaskInstances up_ti
            ON up_ti.run_rowid = ti.run_rowid
            AND up_ti.task_rowid = td.depends_on_task_rowid
        GROUP BY ti.run_rowid, ti.task_rowid, ti.started_at
    ),
    LagCalc AS (
        SELECT
            dep_count,
            CASE
                WHEN task_started_at > 0 AND (
                    dep_count = 0 OR COALESCE(max_upstream_finished_at, 0) > 0
                ) THEN GREATEST(
                    task_started_at - COALESCE(max_upstream_finished_at, r.started_at),
                    0
                )
                ELSE NULL
            END AS lag_ms
        FROM TaskDeps td
        JOIN dag_runs r ON r.run_rowid = td.run_rowid
    )
    SELECT
        CASE WHEN dep_count = 0 THEN 'root' ELSE 'downstream' END AS task_kind,
        COUNT(lag_ms) AS tasks,
        AVG(lag_ms) AS avg_lag_ms,
        SUM(lag_ms) AS total_lag_ms,
        MAX(lag_ms) AS max_lag_ms
    FROM LagCalc
    GROUP BY task_kind
    """

    conn = open_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            rows = cursor.fetchall()
    finally:
        conn.close()

    out = {}
    for task_kind, tasks, avg_lag_ms, total_lag_ms, max_lag_ms in rows:
        out[task_kind] = {
            "tasks": int(tasks),
            "avg_lag_ms": float(avg_lag_ms or 0.0),
            "total_lag_ms": float(total_lag_ms or 0.0),
            "max_lag_ms": float(max_lag_ms or 0.0),
        }
    return out


def summarize_trigger_timings(parsed, run_ids):
    selected = [parsed[run_id] for run_id in run_ids if run_id in parsed]
    if not selected:
        return {}

    def avg(key):
        values = [entry[key] for entry in selected if key in entry]
        return (sum(values) / len(values)) if values else 0.0

    return {
        "runs_with_http_logs": sum(1 for entry in selected if "http_latency_ms" in entry),
        "avg_http_latency_ms": avg("http_latency_ms"),
        "avg_ensure_rowids_ms": avg("ensure_rowids_ms"),
        "avg_run_create_ms": avg("run_create_ms"),
        "avg_first_persist_ms": avg("first_persist_ms"),
        "avg_dispatch_ms": avg("dispatch_ms"),
        "avg_owner_total_ms": avg("owner_total_ms"),
        "avg_persist_total_ms": avg("persist_total_ms"),
    }


def main(argv):
    if len(argv) != 3:
        print(
            "usage: python3 bench/scripts/analyze_airflow_bench_breakdown.py "
            "bench_results/<scenario>.latest.json <dagforge.log>"
        )
        return 1

    result_path, log_path = argv[1], argv[2]
    artifact = load_result_artifact(result_path)
    with open(log_path, "r", encoding="utf-8", errors="replace") as handle:
        parsed = parse_trigger_timings(handle)

    run_ids = artifact["run_ids"]
    lag_breakdown = fetch_lag_breakdown(run_ids)
    trigger_summary = summarize_trigger_timings(parsed, run_ids)

    print(f"Scenario: {artifact['scenario']}")
    print(f"Official total task lag: scene1_linear_100x10 = 11.6s (Airflow 2.0 beta), 200s (Airflow 1.10.10)")
    print(
        f"Our total task lag: {artifact['results']['total_lag_ms'] / 1000.0:.3f}s "
        f"(avg {artifact['results']['avg_lag_ms']:.2f} ms/task)"
    )
    if trigger_summary:
        print(
            "Trigger path averages: "
            f"HTTP {trigger_summary.get('avg_http_latency_ms', 0.0):.2f} ms, "
            f"ensure_rowids {trigger_summary.get('avg_ensure_rowids_ms', 0.0):.2f} ms, "
            f"run_create {trigger_summary.get('avg_run_create_ms', 0.0):.2f} ms, "
            f"first_persist {trigger_summary.get('avg_first_persist_ms', 0.0):.2f} ms, "
            f"dispatch {trigger_summary.get('avg_dispatch_ms', 0.0):.2f} ms"
        )
    for task_kind in ("root", "downstream"):
        if task_kind in lag_breakdown:
            item = lag_breakdown[task_kind]
            print(
                f"{task_kind} lag: tasks={item['tasks']} avg={item['avg_lag_ms']:.2f} ms "
                f"total={item['total_lag_ms']:.2f} ms max={item['max_lag_ms']:.2f} ms"
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
