import asyncio
import json
import os
import sys
import time
import subprocess
from pathlib import Path

import aiohttp
import pymysql


API_HOST = os.environ.get("DAGFORGE_BENCH_API_HOST", "127.0.0.1")
API_PORT = int(os.environ.get("DAGFORGE_BENCH_API_PORT", "8888"))
API_BASE = f"http://{API_HOST}:{API_PORT}/api/dags"
HISTORY_BASE = f"http://{API_HOST}:{API_PORT}/api/history"

DB_HOST = os.environ.get("DAGFORGE_BENCH_DB_HOST", "127.0.0.1")
DB_PORT = int(os.environ.get("DAGFORGE_BENCH_DB_PORT", "3306"))
DB_USER = os.environ.get("DAGFORGE_BENCH_DB_USER", "root")
DB_PASS = os.environ.get("DAGFORGE_BENCH_DB_PASS", "604222352mj")
DB_NAME = os.environ.get("DAGFORGE_BENCH_DB_NAME", "dagforge_perf16")
RESULTS_DIR = os.environ.get("DAGFORGE_BENCH_RESULTS_DIR", "bench_results")
RESET_SCENARIO_HISTORY = os.environ.get(
    "DAGFORGE_BENCH_RESET_SCENARIO_HISTORY", "0"
).lower() in ("1", "true", "yes", "on")
REPO_ROOT = Path(__file__).resolve().parents[2]


def scenario_dag_ids(scenario_dir):
    return sorted(
        f[:-5]
        for f in os.listdir(scenario_dir)
        if f.endswith(".toml")
    )


def ensure_generated_scenarios(scenario_dir: Path):
    if scenario_dir.exists():
        return
    subprocess.run(
        ["python3", "bench/scripts/gen_airflow_bench_dags.py"],
        cwd=REPO_ROOT,
        check=True,
        text=True,
        capture_output=False,
    )
    subprocess.run(
        ["python3", "bench/scripts/gen_realistic_bench_dags.py"],
        cwd=REPO_ROOT,
        check=True,
        text=True,
        capture_output=False,
    )


def load_scenario_meta(scenario_dir):
    meta_path = os.path.join(scenario_dir, "bench.meta.json")
    if not os.path.exists(meta_path):
        return {}
    with open(meta_path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def write_benchmark_result(output_dir, scenario_name, run_ids, expected_tasks, results):
    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, f"{scenario_name}.latest.json")
    payload = {
        "scenario": scenario_name,
        "run_ids": list(run_ids),
        "expected_tasks": expected_tasks,
        "results": dict(results),
    }
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
    return path


def count_expected_tasks(scenario_dir):
    total = 0
    for name in os.listdir(scenario_dir):
        if not name.endswith(".toml"):
            continue
        path = os.path.join(scenario_dir, name)
        with open(path, "r", encoding="utf-8") as handle:
            total += handle.read().count("[[tasks]]")
    return total


def sql_quote(value):
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def build_run_ids_clause(run_ids):
    if not run_ids:
        raise ValueError("run_ids must not be empty")
    return ",".join(sql_quote(run_id) for run_id in run_ids)


def build_latest_attempts_cte(run_ids):
    run_ids_clause = build_run_ids_clause(run_ids)
    return f"""
    WITH RankedAttempts AS (
        SELECT
            ti.run_rowid,
            ti.task_rowid,
            ti.attempt,
            ti.state,
            ti.started_at,
            ti.finished_at,
            ROW_NUMBER() OVER (
                PARTITION BY ti.run_rowid, ti.task_rowid
                ORDER BY ti.attempt DESC
            ) AS attempt_rank
        FROM task_instances ti
        JOIN dag_runs r ON r.run_rowid = ti.run_rowid
        WHERE r.dag_run_id IN ({run_ids_clause}) AND ti.attempt > 0
    ),
    LatestTaskInstances AS (
        SELECT
            run_rowid,
            task_rowid,
            attempt,
            state,
            started_at,
            finished_at
        FROM RankedAttempts
        WHERE attempt_rank = 1
    )
    """


def build_task_lag_query(run_ids):
    return (
        build_latest_attempts_cte(run_ids)
        + """
    ,
    TaskDeps AS (
        SELECT
            ti.run_rowid,
            ti.task_rowid,
            ti.started_at AS task_started_at,
            COUNT(td.dep_rowid) AS dep_count,
            MAX(up_ti.finished_at) AS max_upstream_finished_at
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
            td.run_rowid,
            td.task_rowid,
            td.task_started_at,
            COALESCE(td.max_upstream_finished_at, r.started_at) AS dependency_met_at,
            CASE
                WHEN td.task_started_at > 0 AND (
                    td.dep_count = 0 OR COALESCE(td.max_upstream_finished_at, 0) > 0
                ) THEN GREATEST(
                    td.task_started_at - COALESCE(td.max_upstream_finished_at, r.started_at),
                    0
                )
                ELSE NULL
            END AS lag_ms
        FROM TaskDeps td
        JOIN dag_runs r ON r.run_rowid = td.run_rowid
    )
    SELECT
        COUNT(*) AS total_tasks,
        COALESCE(SUM(CASE WHEN lag_ms IS NOT NULL THEN 1 ELSE 0 END), 0) AS lag_tasks,
        COALESCE(SUM(lag_ms), 0) AS total_lag_ms,
        COALESCE(AVG(lag_ms), 0) AS avg_lag_ms,
        COALESCE(MAX(lag_ms), 0) AS max_lag_ms
    FROM LagCalc;
    """
    )


def build_task_persistence_query(run_ids):
    return (
        build_latest_attempts_cte(run_ids)
        + """
    SELECT
        COUNT(*) AS total_tasks,
        COALESCE(
            SUM(CASE WHEN state NOT IN (0, 1, 5) THEN 1 ELSE 0 END),
            0
        ) AS finished_tasks
    FROM LatestTaskInstances;
    """
    )


def build_dependency_summary_query(run_ids):
    return f"""
    SELECT COUNT(*) AS dependency_edges
    FROM task_dependencies td
    JOIN dag_runs r ON r.dag_rowid = td.dag_rowid
    WHERE r.dag_run_id IN ({build_run_ids_clause(run_ids)});
    """


def open_db_connection():
    return pymysql.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
    )


def reset_scenario_history(dag_ids):
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

            cursor.execute(f"DELETE FROM dag_runs WHERE run_rowid IN ({run_rowids_clause})")
            deleted_runs = cursor.rowcount
            conn.commit()
            return deleted_runs
    finally:
        conn.close()


def fetch_task_persistence_summary(run_ids):
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


def calculate_task_lag(run_ids):
    conn = open_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(build_task_lag_query(run_ids))
            return cursor.fetchone()
    finally:
        conn.close()


def fetch_dependency_edge_count(run_ids):
    conn = open_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(build_dependency_summary_query(run_ids))
            (edge_count,) = cursor.fetchone()
            return int(edge_count or 0)
    finally:
        conn.close()


def bench_run_timeout_s():
    return float(os.environ.get("DAGFORGE_BENCH_RUN_TIMEOUT_S", "300"))


async def trigger_dag(session, dag_id):
    url = f"{API_BASE}/{dag_id}/trigger"
    async with session.post(url, json={}) as resp:
        if resp.status >= 400:
            body = await resp.text()
            raise RuntimeError(f"trigger failed for {dag_id}: HTTP {resp.status}: {body}")
        res = await resp.json()
        dag_run_id = res.get("dag_run_id")
        if not dag_run_id:
            raise RuntimeError(f"trigger response missing dag_run_id for {dag_id}")
        return dag_run_id


async def get_run_state(session, run_id):
    url = f"{HISTORY_BASE}/{run_id}"
    async with session.get(url) as resp:
        if resp.status == 200:
            res = await resp.json()
            return res.get("state")
        return None


async def get_dag_history(session, dag_id):
    url = f"{API_BASE}/{dag_id}/history"
    async with session.get(url) as resp:
        if resp.status >= 400:
            body = await resp.text()
            raise RuntimeError(f"failed to fetch dag history for {dag_id}: HTTP {resp.status}: {body}")
        return await resp.json()


def history_run_ids(payload):
    if isinstance(payload, list):
        runs = payload
    elif isinstance(payload, dict):
        runs = payload.get("runs", [])
    else:
        return []

    run_ids = []
    for entry in runs:
        if not isinstance(entry, dict):
            continue
        run_id = entry.get("dag_run_id") or entry.get("run_id")
        if isinstance(run_id, str) and run_id:
            run_ids.append(run_id)
    return run_ids


async def fetch_loaded_dag_ids(session):
    async with session.get(API_BASE) as resp:
        if resp.status >= 400:
            body = await resp.text()
            raise RuntimeError(f"failed to list dags: HTTP {resp.status}: {body}")
        payload = await resp.json()
        dags = payload.get("dags", [])
        return {dag.get("dag_id") for dag in dags if dag.get("dag_id")}


async def ensure_scenario_loaded(session, dag_ids):
    loaded_dags = await fetch_loaded_dag_ids(session)
    missing = [dag_id for dag_id in dag_ids if dag_id not in loaded_dags]
    if missing:
        preview = ", ".join(missing[:5])
        raise RuntimeError(
            "scenario DAGs are not loaded in the running server; "
            f"missing={len(missing)} first_missing=[{preview}]"
        )


async def wait_for_runs(
    session,
    run_ids,
    get_run_state_fn=None,
    sleep_fn=asyncio.sleep,
    timeout_s=120.0,
    progress_interval_s=5.0,
):
    if get_run_state_fn is None:
        get_run_state_fn = get_run_state

    print(f"Waiting for {len(run_ids)} DAG runs to complete...")
    pending = set(run_ids)
    start_wait = time.monotonic()
    last_progress = start_wait
    while pending:
        states = await asyncio.gather(
            *(get_run_state_fn(session, run_id) for run_id in pending)
        )
        for run_id, state in zip(list(pending), states, strict=False):
            if state and state.lower() in ("success", "failed"):
                pending.remove(run_id)
        if pending:
            now = time.monotonic()
            if now - last_progress >= progress_interval_s:
                preview = ", ".join(sorted(pending)[:3])
                print(
                    f"Still waiting on {len(pending)} DAG runs "
                    f"after {now - start_wait:.1f}s; sample=[{preview}]"
                )
                last_progress = now
            await sleep_fn(0.1)
        if time.monotonic() - start_wait > timeout_s:
            preview = ", ".join(sorted(pending)[:5])
            raise TimeoutError(
                "timed out waiting for DAG runs to complete; "
                f"remaining={len(pending)} sample=[{preview}]"
            )


async def wait_for_auto_scheduled_runs(
    session,
    dag_ids,
    expected_runs_per_dag=1,
    sleep_fn=asyncio.sleep,
    timeout_s=120.0,
    progress_interval_s=5.0,
):
    print(
        "Waiting for scheduler-created DAG runs to appear "
        f"({expected_runs_per_dag} per DAG)..."
    )
    pending = set(dag_ids)
    observed = {}
    start_wait = time.monotonic()
    last_progress = start_wait
    while pending:
        payloads = await asyncio.gather(*(get_dag_history(session, dag_id) for dag_id in pending))
        for dag_id, payload in zip(list(pending), payloads, strict=False):
            run_ids = history_run_ids(payload)
            if len(run_ids) >= expected_runs_per_dag:
                observed[dag_id] = run_ids[:expected_runs_per_dag]
                pending.remove(dag_id)
        if pending:
            now = time.monotonic()
            if now - last_progress >= progress_interval_s:
                preview = ", ".join(sorted(pending)[:3])
                print(
                    f"Still waiting for {len(pending)} DAG histories "
                    f"after {now - start_wait:.1f}s; sample=[{preview}]"
                )
                last_progress = now
            await sleep_fn(0.5)
        if time.monotonic() - start_wait > timeout_s:
            preview = ", ".join(sorted(pending)[:5])
            raise TimeoutError(
                "timed out waiting for scheduler-created DAG runs; "
                f"remaining={len(pending)} sample=[{preview}]"
            )

    flattened = [run_id for ids in observed.values() for run_id in ids]
    print(f"Observed {len(flattened)} scheduler-created DAG runs.")
    return flattened


async def wait_for_task_persistence(
    run_ids,
    expected_task_count,
    probe_fn=None,
    sleep_fn=asyncio.sleep,
    timeout_s=10.0,
    poll_interval_s=0.05,
):
    if probe_fn is None:
        async def default_probe(ids):
            return await asyncio.to_thread(fetch_task_persistence_summary, ids)

        probe_fn = default_probe

    deadline = time.monotonic() + timeout_s
    last_summary = None
    while time.monotonic() < deadline:
        last_summary = await probe_fn(run_ids)
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


async def run_scenario(scenario_dir):
    scenario_path = Path(scenario_dir)
    ensure_generated_scenarios(scenario_path)
    scenario_dir = str(scenario_path)
    dags = scenario_dag_ids(scenario_dir)
    expected_tasks = count_expected_tasks(scenario_dir)
    scenario_meta = load_scenario_meta(scenario_dir)
    scenario_mode = scenario_meta.get("mode", "manual")
    expected_runs_per_dag = int(scenario_meta.get("expected_runs_per_dag", 1))
    print(f"\n========== Running {os.path.basename(scenario_dir)} ==========")
    print(f"Scenario DAGs: {len(dags)}; expected tasks: {expected_tasks}")
    if scenario_mode != "manual":
        print(
            "Scenario mode: "
            f"{scenario_mode} (expected_runs_per_dag={expected_runs_per_dag})"
        )

    async with aiohttp.ClientSession() as session:
        await ensure_scenario_loaded(session, dags)
        scenario_start = time.monotonic()

        if RESET_SCENARIO_HISTORY:
            deleted_runs = await asyncio.to_thread(reset_scenario_history, dags)
            print(
                "Reset scenario history enabled: "
                f"deleted {deleted_runs} dag_runs for this scenario."
            )

        if scenario_mode == "auto_schedule":
            run_ids = await wait_for_auto_scheduled_runs(
                session,
                dags,
                expected_runs_per_dag=expected_runs_per_dag,
                timeout_s=bench_run_timeout_s(),
            )
        else:
            print(f"Triggering {len(dags)} DAGs concurrently...")
            t0 = time.time()
            run_ids = await asyncio.gather(
                *(trigger_dag(session, dag_id) for dag_id in dags)
            )
            print(f"All triggered in {time.time() - t0:.3f}s")

        await wait_for_runs(session, run_ids, timeout_s=bench_run_timeout_s())

        print("Waiting for task instance persistence to stabilize...")
        persistence_summary = await wait_for_task_persistence(run_ids, expected_tasks)
        print(
            "Task rows stabilized: "
            f"{persistence_summary['finished_tasks']}/{expected_tasks}"
        )

        dependency_edges = await asyncio.to_thread(fetch_dependency_edge_count, run_ids)
        if expected_tasks > len(dags) and dependency_edges == 0:
            raise RuntimeError(
                "benchmark DAGs were loaded without dependency edges; "
                "regenerate the bench DAG files and restart DAGForge before comparing results"
            )

        print("Calculating Task Lag via DB...")
        (
            total_tasks,
            lag_tasks,
            total_lag_ms,
            avg_lag_ms,
            max_lag_ms,
        ) = await asyncio.to_thread(calculate_task_lag, run_ids)
        wall_seconds = time.monotonic() - scenario_start
        effective_tasks_per_s = (
            float(total_tasks) / wall_seconds if wall_seconds > 0 else 0.0
        )
        result_payload = {
            "total_tasks": int(total_tasks),
            "lag_tasks": int(lag_tasks),
            "total_lag_ms": float(total_lag_ms),
            "avg_lag_ms": float(avg_lag_ms),
            "max_lag_ms": float(max_lag_ms),
            "wall_seconds": float(wall_seconds),
            "effective_tasks_per_s": float(effective_tasks_per_s),
        }
        result_path = write_benchmark_result(
            RESULTS_DIR,
            os.path.basename(scenario_dir),
            run_ids,
            expected_tasks,
            result_payload,
        )

        print(f"--- Results for {os.path.basename(scenario_dir)} ---")
        print(f"Total Tasks Run: {int(total_tasks)}")
        print(f"Lag Tasks Measured: {int(lag_tasks)}")
        print(
            "Total Task Lag:  "
            f"{float(total_lag_ms):.2f} ms"
        )
        print(f"Average Lag/Task: {float(avg_lag_ms):.2f} ms")
        print(f"Max Lag for 1 Task: {float(max_lag_ms):.2f} ms")
        print(f"Wall Time: {wall_seconds:.3f} s")
        print(f"Throughput: {effective_tasks_per_s:.1f} tasks/s")
        print(
            "Lag note: root tasks fall back to DAG run started_at, matching the "
            "official-style dependency-met baseline as closely as DAGForge exposes."
        )
        print(f"Result artifact: {result_path}")
        print("========================================================\n")


if __name__ == "__main__":
    base = "bench/airflow_dags"
    if len(sys.argv) > 1:
        scenario_path = Path(base) / sys.argv[1]
        ensure_generated_scenarios(scenario_path)
        asyncio.run(run_scenario(os.path.join(base, sys.argv[1])))
    else:
        print("Please specify a scenario dir to run.")
