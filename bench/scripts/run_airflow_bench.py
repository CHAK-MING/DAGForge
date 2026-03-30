#!/usr/bin/env python3

from __future__ import annotations

import argparse
import asyncio
import os
import time
from pathlib import Path
from typing import Any, Awaitable, Callable, Sequence

from benchlib.api import (
    BenchApiConfig,
    ensure_scenario_loaded as ensure_loaded_with_config,
    fetch_loaded_dag_ids as fetch_loaded_with_config,
    get_dag_history as get_history_with_config,
    get_run_state as get_run_state_with_config,
    trigger_dag as trigger_dag_with_config,
    wait_for_auto_scheduled_runs as wait_for_auto_runs_with_config,
    wait_for_runs as wait_for_runs_with_getter,
)
from benchlib.artifacts import write_benchmark_result
from benchlib.db import get_bench_db_name, open_bench_db_connection
from benchlib.scenarios import (
    ScenarioMeta,
    count_expected_tasks,
    ensure_generated_scenarios,
    load_scenario_meta,
    scenario_dag_ids,
)
from benchlib.sql import (
    build_dependency_summary_query,
    build_task_lag_query,
    build_task_persistence_query,
    sql_quote,
)


API_CONFIG = BenchApiConfig.from_env()
API_HOST = API_CONFIG.host
API_PORT = API_CONFIG.port
API_BASE = API_CONFIG.dags_base
HISTORY_BASE = API_CONFIG.history_base

DB_NAME = get_bench_db_name()
RESULTS_DIR = Path(os.environ.get("DAGFORGE_BENCH_RESULTS_DIR", "bench_results"))
RESET_SCENARIO_HISTORY = os.environ.get(
    "DAGFORGE_BENCH_RESET_SCENARIO_HISTORY", "0"
).lower() in ("1", "true", "yes", "on")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run one DAGForge Airflow benchmark scenario")
    parser.add_argument("scenario", nargs="?", help="Scenario directory name under bench/airflow_dags")
    parser.add_argument(
        "--results-dir",
        default=str(RESULTS_DIR),
        help="Directory to write scenario result artifacts",
    )
    parser.add_argument(
        "--api-host",
        default=API_HOST,
        help="DAGForge API host",
    )
    parser.add_argument(
        "--api-port",
        type=int,
        default=API_PORT,
        help="DAGForge API port",
    )
    parser.add_argument(
        "--base-dir",
        default="bench/airflow_dags",
        help="Base scenario directory root",
    )
    return parser.parse_args(argv)


def open_db_connection():
    import pymysql

    return open_bench_db_connection(pymysql)


def fetch_task_persistence_summary(run_ids: list[str]) -> dict[str, int]:
    conn = open_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(build_task_persistence_query(run_ids))
            total_tasks, finished_tasks = cursor.fetchone()
            return {
                "total_tasks": int(total_tasks or 0),
                "finished_tasks": int(finished_tasks or 0),
            }
    finally:
        conn.close()


def calculate_task_lag(run_ids: list[str]):
    conn = open_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(build_task_lag_query(run_ids))
            return cursor.fetchone()
    finally:
        conn.close()


def fetch_dependency_edge_count(run_ids: list[str]) -> int:
    conn = open_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(build_dependency_summary_query(run_ids))
            (edge_count,) = cursor.fetchone()
            return int(edge_count or 0)
    finally:
        conn.close()


def build_result_payload(
    lag_row: tuple[Any, Any, Any, Any, Any],
    persisted_total_tasks: int,
    wall_seconds: float,
    analysis_seconds: float,
) -> dict[str, float | int]:
    total_tasks, lag_tasks, total_lag_ms, avg_lag_ms, max_lag_ms = lag_row
    effective_tasks_per_s = (
        float(persisted_total_tasks) / wall_seconds if wall_seconds > 0 else 0.0
    )
    return {
        "total_tasks": int(total_tasks),
        "lag_tasks": int(lag_tasks),
        "total_lag_ms": float(total_lag_ms),
        "avg_lag_ms": float(avg_lag_ms),
        "max_lag_ms": float(max_lag_ms),
        "wall_seconds": float(wall_seconds),
        "analysis_seconds": float(analysis_seconds),
        "effective_tasks_per_s": float(effective_tasks_per_s),
    }


def bench_run_timeout_s() -> float:
    return float(os.environ.get("DAGFORGE_BENCH_RUN_TIMEOUT_S", "300"))


async def trigger_dag(session: Any, dag_id: str) -> str:
    return await trigger_dag_with_config(session, API_CONFIG, dag_id)


async def get_run_state(session: Any, run_id: str) -> str | None:
    return await get_run_state_with_config(session, API_CONFIG, run_id)


async def get_dag_history(session: Any, dag_id: str) -> Any:
    return await get_history_with_config(session, API_CONFIG, dag_id)


async def fetch_loaded_dag_ids(session: Any) -> set[str]:
    return await fetch_loaded_with_config(session, API_CONFIG)


async def ensure_scenario_loaded(session: Any, dag_ids: Sequence[str]) -> None:
    await ensure_loaded_with_config(session, API_CONFIG, dag_ids)


async def wait_for_runs(
    session: Any,
    run_ids: Sequence[str],
    get_run_state_fn: Callable[[Any, str], Awaitable[str | None]] | None = None,
    sleep_fn: Callable[[float], Awaitable[None]] = asyncio.sleep,
    timeout_s: float = 120.0,
    progress_interval_s: float = 5.0,
) -> None:
    getter = get_run_state_fn or get_run_state
    await wait_for_runs_with_getter(
        run_ids,
        lambda run_id: getter(session, run_id),
        sleep_fn=sleep_fn,
        timeout_s=timeout_s,
        progress_interval_s=progress_interval_s,
    )


async def wait_for_auto_scheduled_runs(
    session: Any,
    dag_ids: Sequence[str],
    expected_runs_per_dag: int = 1,
    sleep_fn: Callable[[float], Awaitable[None]] = asyncio.sleep,
    timeout_s: float = 120.0,
    progress_interval_s: float = 5.0,
) -> list[str]:
    return await wait_for_auto_runs_with_config(
        dag_ids,
        lambda dag_id: get_dag_history(session, dag_id),
        expected_runs_per_dag=expected_runs_per_dag,
        sleep_fn=sleep_fn,
        timeout_s=timeout_s,
        progress_interval_s=progress_interval_s,
    )


async def wait_for_task_persistence(
    run_ids: Sequence[str],
    expected_task_count: int,
    probe_fn: Callable[[list[str]], Awaitable[dict[str, int]]] | None = None,
    sleep_fn: Callable[[float], Awaitable[None]] = asyncio.sleep,
    timeout_s: float = 10.0,
    poll_interval_s: float = 0.05,
) -> dict[str, int]:
    if probe_fn is None:

        async def default_probe(ids: list[str]) -> dict[str, int]:
            return await asyncio.to_thread(fetch_task_persistence_summary, ids)

        probe_fn = default_probe

    deadline = time.monotonic() + timeout_s
    last_summary: dict[str, int] | None = None
    while time.monotonic() < deadline:
        last_summary = await probe_fn(list(run_ids))
        if (
            last_summary["total_tasks"] == expected_task_count
            and last_summary["finished_tasks"] == expected_task_count
        ):
            return last_summary
        await sleep_fn(poll_interval_s)

    raise TimeoutError(
        "timed out waiting for task instance persistence to stabilize; "
        f"expected={expected_task_count} last={last_summary}"
    )


def reset_scenario_history(dag_ids: Sequence[str]) -> int:
    if not dag_ids:
        return 0

    dag_ids_clause = ",".join(sql_quote(dag_id) for dag_id in dag_ids)
    conn = open_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT r.run_rowid
                FROM dag_runs r
                JOIN dags d ON d.dag_rowid = r.dag_rowid
                WHERE d.dag_id IN ({dag_ids_clause})
                """
            )
            run_rowids = [int(row[0]) for row in cursor.fetchall()]
            if not run_rowids:
                conn.commit()
                return 0

            run_rowids_clause = ",".join(str(rowid) for rowid in run_rowids)
            cursor.execute(
                """
                SELECT table_name
                FROM information_schema.columns
                WHERE table_schema = %s AND column_name = 'run_rowid'
                """,
                (DB_NAME,),
            )
            tables_with_run_rowid = [
                table_name
                for (table_name,) in cursor.fetchall()
                if table_name != "dag_runs"
            ]

            for table_name in tables_with_run_rowid:
                cursor.execute(
                    f"DELETE FROM {table_name} WHERE run_rowid IN ({run_rowids_clause})"
                )

            cursor.execute(
                f"DELETE FROM dag_runs WHERE run_rowid IN ({run_rowids_clause})"
            )
            deleted_runs = cursor.rowcount
            conn.commit()
            return deleted_runs
    finally:
        conn.close()


async def execute_scenario_workload(
    session: Any,
    dags: list[str],
    expected_tasks: int,
    scenario_meta: ScenarioMeta,
) -> dict[str, Any]:
    scenario_start = time.monotonic()

    if RESET_SCENARIO_HISTORY:
        deleted_runs = await asyncio.to_thread(reset_scenario_history, dags)
        print(
            "Reset scenario history enabled: "
            f"deleted {deleted_runs} dag_runs for this scenario."
        )

    if scenario_meta.mode == "auto_schedule":
        run_ids = await wait_for_auto_scheduled_runs(
            session,
            dags,
            expected_runs_per_dag=scenario_meta.expected_runs_per_dag,
            timeout_s=bench_run_timeout_s(),
        )
    else:
        print(f"Triggering {len(dags)} DAGs concurrently...")
        started_at = time.time()
        run_ids = await asyncio.gather(*(trigger_dag(session, dag_id) for dag_id in dags))
        print(f"All triggered in {time.time() - started_at:.3f}s")

    await wait_for_runs(session, run_ids, timeout_s=bench_run_timeout_s())

    print("Waiting for task instance persistence to stabilize...")
    persistence_summary = await wait_for_task_persistence(run_ids, expected_tasks)

    return {
        "run_ids": run_ids,
        "wall_seconds": time.monotonic() - scenario_start,
        "persisted_total_tasks": int(persistence_summary["total_tasks"]),
    }


async def analyze_scenario_workload(
    run_ids: list[str], dags: list[str], expected_tasks: int
) -> tuple[tuple[Any, Any, Any, Any, Any], float]:
    dependency_edges = await asyncio.to_thread(fetch_dependency_edge_count, run_ids)
    if expected_tasks > len(dags) and dependency_edges == 0:
        raise RuntimeError(
            "benchmark DAGs were loaded without dependency edges; "
            "regenerate the bench DAG files and restart DAGForge before comparing results"
        )

    print("Calculating Task Lag via DB...")
    started_at = time.monotonic()
    lag_row = await asyncio.to_thread(calculate_task_lag, run_ids)
    return lag_row, time.monotonic() - started_at


async def run_scenario(
    scenario_dir: str | Path,
    *,
    results_dir: str | Path,
) -> None:
    scenario_path = Path(scenario_dir)
    ensure_generated_scenarios(scenario_path)

    dags = scenario_dag_ids(scenario_path)
    expected_tasks = count_expected_tasks(scenario_path)
    scenario_meta = load_scenario_meta(scenario_path)
    scenario_name = scenario_path.name

    print(f"\n========== Running {scenario_name} ==========")
    print(f"Scenario DAGs: {len(dags)}; expected tasks: {expected_tasks}")
    if scenario_meta.mode != "manual":
        print(
            "Scenario mode: "
            f"{scenario_meta.mode} "
            f"(expected_runs_per_dag={scenario_meta.expected_runs_per_dag})"
        )

    import aiohttp

    async with aiohttp.ClientSession() as session:
        await ensure_scenario_loaded(session, dags)
        execution = await execute_scenario_workload(
            session,
            dags=dags,
            expected_tasks=expected_tasks,
            scenario_meta=scenario_meta,
        )
        lag_row, analysis_seconds = await analyze_scenario_workload(
            execution["run_ids"], dags, expected_tasks
        )
        result_payload = build_result_payload(
            lag_row=lag_row,
            persisted_total_tasks=execution["persisted_total_tasks"],
            wall_seconds=execution["wall_seconds"],
            analysis_seconds=analysis_seconds,
        )
        result_path = write_benchmark_result(
            results_dir,
            scenario_name,
            execution["run_ids"],
            expected_tasks,
            result_payload,
        )

    print(f"--- Results for {scenario_name} ---")
    print(f"Total Tasks Run: {result_payload['total_tasks']}")
    print(f"Lag Tasks Measured: {result_payload['lag_tasks']}")
    print(f"Total Task Lag:  {result_payload['total_lag_ms']:.2f} ms")
    print(f"Average Lag/Task: {result_payload['avg_lag_ms']:.2f} ms")
    print(f"Max Lag for 1 Task: {result_payload['max_lag_ms']:.2f} ms")
    print(f"Wall Time: {result_payload['wall_seconds']:.3f} s")
    print(f"Analysis Time: {result_payload['analysis_seconds']:.3f} s")
    print(f"Throughput: {result_payload['effective_tasks_per_s']:.1f} tasks/s")
    print(
        "Lag note: root tasks fall back to DAG run started_at, matching the "
        "official-style dependency-met baseline as closely as DAGForge exposes."
    )
    print(f"Result artifact: {result_path}")
    print("========================================================\n")


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    if not args.scenario:
        print("Please specify a scenario dir to run.")
        return 1

    global API_CONFIG, API_HOST, API_PORT, API_BASE, HISTORY_BASE
    API_CONFIG = BenchApiConfig(host=args.api_host, port=args.api_port)
    API_HOST = API_CONFIG.host
    API_PORT = API_CONFIG.port
    API_BASE = API_CONFIG.dags_base
    HISTORY_BASE = API_CONFIG.history_base

    base_dir = Path(args.base_dir)
    asyncio.run(
        run_scenario(base_dir / args.scenario, results_dir=args.results_dir)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
