#!/usr/bin/env python3

from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Any, Iterable

from benchlib.artifacts import load_json
from benchlib.db import open_bench_db_connection
from benchlib.scenarios import OFFICIAL_TOTAL_LAG_S
from benchlib.sql import build_lag_breakdown_query


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
ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Analyze DAGForge Airflow benchmark trigger and lag breakdown"
    )
    parser.add_argument("result_json", help="Path to bench_results/<scenario>.latest.json")
    parser.add_argument("log_file", help="Path to dagforge log file")
    return parser.parse_args(argv)


def parse_trigger_timings(lines: Iterable[str]) -> dict[str, dict[str, int]]:
    parsed: dict[str, dict[str, int]] = {}
    for raw in lines:
        line = ANSI_RE.sub("", raw.strip())
        for regex in (HTTP_RE, OWNER_RE, PERSIST_RE):
            match = regex.search(line)
            if not match:
                continue
            run_id = match.group("run_id")
            item = parsed.setdefault(run_id, {})
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
    return parsed


def open_db_connection():
    import pymysql

    return open_bench_db_connection(pymysql)


def fetch_lag_breakdown(run_ids: list[str]) -> dict[str, dict[str, float]]:
    sql = build_lag_breakdown_query(run_ids)
    conn = open_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            rows = cursor.fetchall()
    finally:
        conn.close()

    summary: dict[str, dict[str, float]] = {}
    for task_kind, tasks, avg_lag_ms, total_lag_ms, max_lag_ms in rows:
        summary[str(task_kind)] = {
            "tasks": int(tasks),
            "avg_lag_ms": float(avg_lag_ms or 0.0),
            "total_lag_ms": float(total_lag_ms or 0.0),
            "max_lag_ms": float(max_lag_ms or 0.0),
        }
    return summary


def summarize_trigger_timings(
    parsed: dict[str, dict[str, int]], run_ids: list[str]
) -> dict[str, float]:
    selected = [parsed[run_id] for run_id in run_ids if run_id in parsed]
    if not selected:
        return {}

    def avg(key: str) -> float:
        values = [float(entry[key]) for entry in selected if key in entry]
        return (sum(values) / len(values)) if values else 0.0

    return {
        "runs_with_http_logs": float(
            sum(1 for entry in selected if "http_latency_ms" in entry)
        ),
        "avg_http_latency_ms": avg("http_latency_ms"),
        "avg_ensure_rowids_ms": avg("ensure_rowids_ms"),
        "avg_run_create_ms": avg("run_create_ms"),
        "avg_first_persist_ms": avg("first_persist_ms"),
        "avg_dispatch_ms": avg("dispatch_ms"),
        "avg_owner_total_ms": avg("owner_total_ms"),
        "avg_persist_total_ms": avg("persist_total_ms"),
    }


def print_summary(
    scenario_name: str,
    results: dict[str, Any],
    lag_breakdown: dict[str, dict[str, float]],
    trigger_summary: dict[str, float],
) -> None:
    print(f"Scenario: {scenario_name}")
    official = OFFICIAL_TOTAL_LAG_S.get(scenario_name)
    if official:
        print(
            "Official total task lag: "
            f"Airflow 2.0 beta = {official.get('airflow_2_0_beta')}s, "
            f"Airflow 1.10.10 = {official.get('airflow_1_10_10')}s"
        )

    print(
        f"Our total task lag: {float(results['total_lag_ms']) / 1000.0:.3f}s "
        f"(avg {float(results['avg_lag_ms']):.2f} ms/task)"
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
        if task_kind not in lag_breakdown:
            continue
        item = lag_breakdown[task_kind]
        print(
            f"{task_kind} lag: tasks={item['tasks']} avg={item['avg_lag_ms']:.2f} ms "
            f"total={item['total_lag_ms']:.2f} ms max={item['max_lag_ms']:.2f} ms"
        )


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    artifact = load_json(Path(args.result_json))
    with Path(args.log_file).open("r", encoding="utf-8", errors="replace") as handle:
        parsed = parse_trigger_timings(handle)

    scenario_name = str(artifact["scenario"])
    run_ids = list(artifact["run_ids"])
    lag_breakdown = fetch_lag_breakdown(run_ids)
    trigger_summary = summarize_trigger_timings(parsed, run_ids)
    print_summary(scenario_name, artifact["results"], lag_breakdown, trigger_summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
