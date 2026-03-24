import argparse
import json
import os
import re
import statistics
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

import pymysql
from bench_db_env import (
    apply_dagforge_db_env,
    get_bench_db_name,
    open_bench_db_connection,
)


OFFICIAL_TOTAL_LAG_S = {
    "scene1_linear_100x10": {"airflow_2_0_beta": 11.6, "airflow_1_10_10": 200.0},
    "scene2_linear_10x100": {"airflow_2_0_beta": 14.3, "airflow_1_10_10": 144.0},
    "scene3_tree_100x10": {"airflow_2_0_beta": 12.0, "airflow_1_10_10": 200.0},
}

SCENE_CLASSIFICATION = {
    "scene1_linear_100x10": "宽 DAG / 并行释放敏感",
    "scene2_linear_10x100": "深 DAG / 长关键路径敏感",
    "scene3_tree_100x10": "树型 DAG / 多后继传播敏感",
    "scene4_burst_ready_1x100": "超宽层 burst-ready / ready 洪峰释放敏感",
    "scene5_burst_ready_1x500": "超宽层 burst-ready / ready 洪峰释放敏感",
    "scene6_burst_ready_1x1000": "超宽层 burst-ready / ready 洪峰释放敏感",
    "scene7_diamond_100x10": "菱形 DAG / fan-out 后 fan-in 敏感",
    "scene8_fanout_100x10": "扇出 DAG / 单点释放敏感",
    "scene9_fanin_100x10": "扇入 DAG / 多前驱汇聚敏感",
    "scene10_mesh_100x10": "网状 DAG / 密集依赖传播敏感",
    "scene12_perf_pipeline_1x3": "真实场景 / perf pipeline stability",
    "scene13_perf_pipeline_mixed_1x21": "真实场景 / perf + XCom + sensor + trigger rules",
}

BURST_SWEEP_SCENARIOS = [
    "scene4_burst_ready_1x100",
    "scene5_burst_ready_1x500",
    "scene6_burst_ready_1x1000",
]

REALISTIC_SWEEP_SCENARIOS = [
    "scene7_diamond_100x10",
    "scene8_fanout_100x10",
    "scene9_fanin_100x10",
    "scene10_mesh_100x10",
]

DB_NAME = get_bench_db_name()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run Airflow-style benchmark scenarios multiple times and summarize stability."
        )
    )
    parser.add_argument(
        "--config",
        default="bench/airflow_bench.toml",
        help="Base DAGForge config file",
    )
    parser.add_argument(
        "--rounds",
        type=int,
        default=5,
        help="Runs per scenario",
    )
    parser.add_argument(
        "--api-base",
        default=os.environ.get("DAGFORGE_BENCH_API_BASE", "http://127.0.0.1:8888"),
        help="HTTP API base (host:port)",
    )
    parser.add_argument(
        "--results-dir",
        default="bench_results",
        help="Directory of run_airflow_bench.py result artifacts",
    )
    parser.add_argument(
        "--output-json",
        default="/tmp/dagforge_airflow_stability.json",
        help="Where to write aggregated JSON summary",
    )
    parser.add_argument(
        "--scenarios",
        nargs="+",
        default=[
            "scene1_linear_100x10",
            "scene2_linear_10x100",
            "scene3_tree_100x10",
        ],
        help="Scenario names under bench/airflow_dags/",
    )
    parser.add_argument(
        "--taskset-cpus",
        default="",
        help="Optional CPU set for dagforge daemon, e.g. 0,2,4,6",
    )
    parser.add_argument(
        "--startup-timeout",
        type=float,
        default=45.0,
        help="Max seconds to wait for API and scenario DAGs",
    )
    parser.add_argument(
        "--root-dir",
        default=".",
        help="Repository root",
    )
    parser.add_argument(
        "--include-burst-sweep",
        action="store_true",
        help="Append burst-ready scenarios (100/500/1000 ready fanout)",
    )
    parser.add_argument(
        "--include-realistic-topologies",
        action="store_true",
        help="Append diamond, fan-out, fan-in, and mesh scenarios",
    )
    return parser.parse_args()


def _run(cmd: list[str], cwd: Path, env: dict[str, str] | None = None, check: bool = True):
    return subprocess.run(
        cmd,
        cwd=str(cwd),
        env=env,
        check=check,
        text=True,
        capture_output=False,
    )


def ensure_generated_bench_scenarios(repo_root: Path, scenario_name: str) -> None:
    scenario_dir = repo_root / "bench" / "airflow_dags" / scenario_name
    if scenario_dir.exists():
        return
    _run(["python3", "bench/scripts/gen_airflow_bench_dags.py"], cwd=repo_root)
    _run(["python3", "bench/scripts/gen_realistic_bench_dags.py"], cwd=repo_root)


def open_db_connection():
    return open_bench_db_connection(pymysql)


def sql_quote(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def build_run_ids_clause(run_ids: list[str]) -> str:
    if not run_ids:
        raise ValueError("run_ids must not be empty")
    return ",".join(sql_quote(run_id) for run_id in run_ids)


def build_latest_attempts_cte(run_ids: list[str]) -> str:
    run_ids_clause = build_run_ids_clause(run_ids)
    return f"""
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
        WHERE r.dag_run_id IN ({run_ids_clause}) AND ti.attempt > 0
    ),
    LatestTaskInstances AS (
        SELECT run_rowid, task_rowid, started_at, finished_at
        FROM RankedAttempts
        WHERE attempt_rank = 1
    )
    """


def fetch_task_lag_distribution(run_ids: list[str]) -> list[tuple[int, int]]:
    sql = (
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
        WHERE ti.started_at > 0
        GROUP BY ti.run_rowid, ti.task_rowid, ti.started_at
    )
    SELECT
        CASE
            WHEN task_started_at > 0 AND (
                dep_count = 0 OR COALESCE(max_upstream_finished_at, 0) > 0
            ) THEN GREATEST(
                task_started_at - COALESCE(max_upstream_finished_at, r.started_at),
                0
            )
            ELSE NULL
        END AS lag_ms,
        dep_count
    FROM TaskDeps td
    JOIN dag_runs r ON r.run_rowid = td.run_rowid
    """
    )

    conn = open_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            rows = cursor.fetchall()
    finally:
        conn.close()
    return [(int(row[0]), int(row[1] or 0)) for row in rows if row[0] is not None]


def percentile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return float(values[0])
    arr = sorted(values)
    pos = (len(arr) - 1) * q
    lo = int(pos)
    hi = min(lo + 1, len(arr) - 1)
    frac = pos - lo
    return float(arr[lo] * (1.0 - frac) + arr[hi] * frac)


def summarize_lag_distribution(rows: list[tuple[int, int]]) -> dict:
    all_lags = [float(lag) for lag, _ in rows]
    root_lags = [float(lag) for lag, dep_count in rows if dep_count == 0]
    downstream_lags = [float(lag) for lag, dep_count in rows if dep_count > 0]
    return {
        "count": len(all_lags),
        "p50_ms": percentile(all_lags, 0.50),
        "p95_ms": percentile(all_lags, 0.95),
        "p99_ms": percentile(all_lags, 0.99),
        "max_ms": max(all_lags) if all_lags else 0.0,
        "root_avg_ms": statistics.fmean(root_lags) if root_lags else 0.0,
        "downstream_avg_ms": statistics.fmean(downstream_lags) if downstream_lags else 0.0,
    }


def fetch_makespan_raw(run_ids: list[str]) -> dict:
    cte = build_latest_attempts_cte(run_ids)
    task_sql = (
        cte
        + """
    SELECT
        ti.run_rowid,
        ti.task_rowid,
        GREATEST(ti.finished_at - ti.started_at, 0) AS task_duration_ms,
        r.started_at AS run_started_at,
        MAX(ti.finished_at) OVER (PARTITION BY ti.run_rowid) AS run_last_finished_at
    FROM LatestTaskInstances ti
    JOIN dag_runs r ON r.run_rowid = ti.run_rowid
    """
    )
    edge_sql = f"""
    SELECT
        r.run_rowid,
        td.task_rowid,
        td.depends_on_task_rowid
    FROM dag_runs r
    JOIN task_dependencies td ON td.dag_rowid = r.dag_rowid
    WHERE r.dag_run_id IN ({build_run_ids_clause(run_ids)})
    """

    conn = open_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(task_sql)
            task_rows = cursor.fetchall()
            cursor.execute(edge_sql)
            edge_rows = cursor.fetchall()
    finally:
        conn.close()

    return {"task_rows": task_rows, "edge_rows": edge_rows}


def compute_makespan_metrics(run_ids: list[str]) -> dict:
    raw = fetch_makespan_raw(run_ids)
    task_rows = raw["task_rows"]
    edge_rows = raw["edge_rows"]

    tasks_by_run = {}
    run_started = {}
    run_last_finished = {}
    for run_rowid, task_rowid, duration_ms, started_at, last_finished in task_rows:
        rr = int(run_rowid)
        tr = int(task_rowid)
        tasks_by_run.setdefault(rr, {})[tr] = float(duration_ms or 0.0)
        run_started[rr] = float(started_at or 0.0)
        run_last_finished[rr] = float(last_finished or 0.0)

    edges_by_run = {}
    for run_rowid, task_rowid, dep_task_rowid in edge_rows:
        rr = int(run_rowid)
        edges_by_run.setdefault(rr, []).append((int(dep_task_rowid), int(task_rowid)))

    efficiency_values = []
    amplification_values = []
    ideal_cp_values = []
    actual_e2e_values = []

    for rr, durations in tasks_by_run.items():
        nodes = set(durations.keys())
        if not nodes:
            continue

        children = {node: [] for node in nodes}
        indegree = {node: 0 for node in nodes}
        for dep_node, task_node in edges_by_run.get(rr, []):
            if dep_node in nodes and task_node in nodes:
                children[dep_node].append(task_node)
                indegree[task_node] += 1

        queue = [node for node in nodes if indegree[node] == 0]
        dp = {node: float(durations[node]) for node in nodes}
        head = 0
        processed = 0
        while head < len(queue):
            node = queue[head]
            head += 1
            processed += 1
            for nxt in children[node]:
                cand = dp[node] + durations[nxt]
                if cand > dp[nxt]:
                    dp[nxt] = cand
                indegree[nxt] -= 1
                if indegree[nxt] == 0:
                    queue.append(nxt)

        ideal_cp_ms = max(dp.values()) if processed == len(nodes) else max(durations.values())
        actual_e2e_ms = max(run_last_finished.get(rr, 0.0) - run_started.get(rr, 0.0), 0.0)

        if ideal_cp_ms <= 0.0 or actual_e2e_ms <= 0.0:
            continue

        efficiency_values.append(ideal_cp_ms / actual_e2e_ms)
        amplification_values.append(actual_e2e_ms / ideal_cp_ms)
        ideal_cp_values.append(ideal_cp_ms)
        actual_e2e_values.append(actual_e2e_ms)

    if not efficiency_values:
        return {
            "run_count": 0,
            "ideal_cp_ms_mean": 0.0,
            "actual_e2e_ms_mean": 0.0,
            "efficiency_mean": 0.0,
            "efficiency_p50": 0.0,
            "efficiency_p95": 0.0,
            "amplification_mean": 0.0,
            "amplification_p95": 0.0,
        }

    return {
        "run_count": len(efficiency_values),
        "ideal_cp_ms_mean": statistics.fmean(ideal_cp_values),
        "actual_e2e_ms_mean": statistics.fmean(actual_e2e_values),
        "efficiency_mean": statistics.fmean(efficiency_values),
        "efficiency_p50": percentile(efficiency_values, 0.50),
        "efficiency_p95": percentile(efficiency_values, 0.95),
        "amplification_mean": statistics.fmean(amplification_values),
        "amplification_p95": percentile(amplification_values, 0.95),
    }


def stop_existing_daemons(repo_root: Path) -> None:
    subprocess.run(
        ["pkill", "-f", "./build/bin/dagforge serve start"],
        cwd=str(repo_root),
        check=False,
        text=True,
        capture_output=True,
    )
    time.sleep(0.4)


def make_config_for_scenario(
    repo_root: Path,
    base_config: Path,
    scenario_name: str,
    out_path: Path,
) -> None:
    content = base_config.read_text(encoding="utf-8")
    lines = content.splitlines()
    in_dag_source = False
    replaced = False
    scenario_dir = str((repo_root / "bench" / "airflow_dags" / scenario_name).resolve())

    for i, line in enumerate(lines):
        if re.match(r"^\s*\[dag_source\]\s*$", line):
            in_dag_source = True
            continue
        if in_dag_source and re.match(r"^\s*\[.+\]\s*$", line):
            in_dag_source = False
        if in_dag_source and re.match(r"^\s*directory\s*=", line):
            lines[i] = f'directory = "{scenario_dir}"'
            replaced = True
            break

    if not replaced:
        raise RuntimeError(f"failed to find [dag_source].directory in {base_config}")

    out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def expected_dag_ids(repo_root: Path, scenario_name: str) -> set[str]:
    ensure_generated_bench_scenarios(repo_root, scenario_name)
    scenario_dir = repo_root / "bench" / "airflow_dags" / scenario_name
    if not scenario_dir.exists():
        raise RuntimeError(f"scenario directory not found: {scenario_dir}")
    return {p.stem for p in scenario_dir.glob("*.toml")}


def fetch_loaded_dag_ids(api_base: str) -> set[str]:
    url = api_base.rstrip("/") + "/api/dags"
    with urllib.request.urlopen(url, timeout=2.0) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    dags = payload.get("dags", [])
    return {item.get("dag_id") for item in dags if item.get("dag_id")}


def wait_scenario_ready(
    api_base: str,
    expect: set[str],
    timeout_s: float,
) -> None:
    deadline = time.monotonic() + timeout_s
    last_loaded = set()
    while time.monotonic() < deadline:
        try:
            last_loaded = fetch_loaded_dag_ids(api_base)
            missing = expect - last_loaded
            if not missing:
                return
        except urllib.error.URLError:
            pass
        time.sleep(0.3)

    sample = ", ".join(sorted((expect - last_loaded))[:5])
    raise RuntimeError(
        f"scenario DAGs not fully loaded before timeout, missing={len(expect - last_loaded)} sample=[{sample}]"
    )


def start_daemon_for_scenario(
    repo_root: Path,
    config_path: Path,
    scenario: str,
    taskset_cpus: str,
) -> Path:
    log_path = Path("/tmp") / f"dagforge_bench_{scenario}.log"
    if log_path.exists():
        log_path.unlink()

    cmd = [
        "./build/bin/dagforge",
        "serve",
        "start",
        "-c",
        str(config_path),
        "--daemon",
        "--log-file",
        str(log_path),
    ]
    if taskset_cpus:
        cmd = ["taskset", "-c", taskset_cpus] + cmd

    _run(cmd, cwd=repo_root, env=apply_dagforge_db_env(os.environ.copy()))
    return log_path


def run_one_round(repo_root: Path, scenario: str, results_dir: Path) -> dict:
    env = os.environ.copy()
    env["DAGFORGE_BENCH_RESET_SCENARIO_HISTORY"] = "1"
    env["DAGFORGE_BENCH_RESULTS_DIR"] = str(results_dir)
    cmd = [
        "uv",
        "run",
        "--with",
        "aiohttp",
        "--with",
        "pymysql",
        "bench/scripts/run_airflow_bench.py",
        scenario,
    ]
    t0 = time.monotonic()
    _run(cmd, cwd=repo_root, env=env)
    wall_seconds = time.monotonic() - t0

    latest = results_dir / f"{scenario}.latest.json"
    if not latest.exists():
        raise RuntimeError(f"result artifact missing: {latest}")
    payload = json.loads(latest.read_text(encoding="utf-8"))
    run_ids = payload.get("run_ids", [])
    lag_rows = fetch_task_lag_distribution(run_ids)
    lag_dist = summarize_lag_distribution(lag_rows)
    makespan = compute_makespan_metrics(run_ids)
    total_tasks = int(payload["results"]["total_tasks"])
    payload["derived"] = {
        "wall_seconds": wall_seconds,
        "effective_tasks_per_s": (total_tasks / wall_seconds) if wall_seconds > 0 else 0.0,
        "lag_distribution": lag_dist,
        "makespan": makespan,
    }
    return payload


def summarize_scene(scene: str, run_payloads: list[dict]) -> dict:
    lags_ms = [float(p["results"]["total_lag_ms"]) for p in run_payloads]
    lags_s = [x / 1000.0 for x in lags_ms]
    warm_s = lags_s[1:] if len(lags_s) > 1 else lags_s
    wall_s = [float(p["derived"]["wall_seconds"]) for p in run_payloads]
    tps = [float(p["derived"]["effective_tasks_per_s"]) for p in run_payloads]
    p95 = [float(p["derived"]["lag_distribution"]["p95_ms"]) for p in run_payloads]
    p99 = [float(p["derived"]["lag_distribution"]["p99_ms"]) for p in run_payloads]
    root_avg = [float(p["derived"]["lag_distribution"]["root_avg_ms"]) for p in run_payloads]
    downstream_avg = [
        float(p["derived"]["lag_distribution"]["downstream_avg_ms"]) for p in run_payloads
    ]
    efficiency = [float(p["derived"]["makespan"]["efficiency_mean"]) for p in run_payloads]
    amplification = [float(p["derived"]["makespan"]["amplification_mean"]) for p in run_payloads]
    ideal_cp_ms = [float(p["derived"]["makespan"]["ideal_cp_ms_mean"]) for p in run_payloads]
    actual_e2e_ms = [float(p["derived"]["makespan"]["actual_e2e_ms_mean"]) for p in run_payloads]

    mean_s = statistics.fmean(lags_s)
    stdev_s = statistics.stdev(lags_s) if len(lags_s) >= 2 else 0.0
    cv = (stdev_s / mean_s) if mean_s > 0 else 0.0

    warm_mean_s = statistics.fmean(warm_s)
    warm_stdev_s = statistics.stdev(warm_s) if len(warm_s) >= 2 else 0.0
    warm_cv = (warm_stdev_s / warm_mean_s) if warm_mean_s > 0 else 0.0

    official = OFFICIAL_TOTAL_LAG_S.get(scene, {})
    af2 = official.get("airflow_2_0_beta")
    af1 = official.get("airflow_1_10_10")

    return {
        "scene": scene,
        "class": SCENE_CLASSIFICATION.get(scene, "未分类场景"),
        "rounds": len(lags_s),
        "lags_seconds": lags_s,
        "mean_seconds": mean_s,
        "stdev_seconds": stdev_s,
        "cv": cv,
        "min_seconds": min(lags_s),
        "max_seconds": max(lags_s),
        "wall_seconds": wall_s,
        "wall_mean_seconds": statistics.fmean(wall_s),
        "wall_stdev_seconds": statistics.stdev(wall_s) if len(wall_s) >= 2 else 0.0,
        "tps_values": tps,
        "tps_mean": statistics.fmean(tps),
        "tps_stdev": statistics.stdev(tps) if len(tps) >= 2 else 0.0,
        "p95_ms_values": p95,
        "p99_ms_values": p99,
        "p95_ms_mean": statistics.fmean(p95),
        "p99_ms_mean": statistics.fmean(p99),
        "root_avg_ms_mean": statistics.fmean(root_avg),
        "downstream_avg_ms_mean": statistics.fmean(downstream_avg),
        "makespan_efficiency_values": efficiency,
        "makespan_efficiency_mean": statistics.fmean(efficiency),
        "makespan_efficiency_stdev": statistics.stdev(efficiency) if len(efficiency) >= 2 else 0.0,
        "makespan_amplification_values": amplification,
        "makespan_amplification_mean": statistics.fmean(amplification),
        "ideal_cp_ms_values": ideal_cp_ms,
        "ideal_cp_ms_mean": statistics.fmean(ideal_cp_ms),
        "actual_e2e_ms_values": actual_e2e_ms,
        "actual_e2e_ms_mean": statistics.fmean(actual_e2e_ms),
        "warm_mean_seconds": warm_mean_s,
        "warm_stdev_seconds": warm_stdev_s,
        "warm_cv": warm_cv,
        "official": official,
        "vs_airflow2x_mean": (af2 / mean_s) if af2 and mean_s > 0 else None,
        "vs_airflow2x_warm": (af2 / warm_mean_s) if af2 and warm_mean_s > 0 else None,
        "vs_airflow110x_mean": (af1 / mean_s) if af1 and mean_s > 0 else None,
        "vs_airflow110x_warm": (af1 / warm_mean_s) if af1 and warm_mean_s > 0 else None,
    }


def print_scene_summary(summary: dict) -> None:
    scene = summary["scene"]
    scene_class = summary.get("class", "")
    print(f"\n=== {scene} ===")
    if scene_class:
        print(f"class: {scene_class}")
    print(
        f"lags(s)={', '.join(f'{x:.3f}' for x in summary['lags_seconds'])}"
    )
    print(
        f"all-runs: mean={summary['mean_seconds']:.3f}s "
        f"stdev={summary['stdev_seconds']:.3f}s cv={summary['cv']*100:.2f}% "
        f"min={summary['min_seconds']:.3f}s max={summary['max_seconds']:.3f}s"
    )
    print(
        f"e2e-wall: mean={summary['wall_mean_seconds']:.3f}s "
        f"stdev={summary['wall_stdev_seconds']:.3f}s"
    )
    print(
        f"throughput: mean={summary['tps_mean']:.1f} tasks/s "
        f"stdev={summary['tps_stdev']:.1f}"
    )
    print(
        f"tail-lag: p95={summary['p95_ms_mean']:.2f} ms "
        f"p99={summary['p99_ms_mean']:.2f} ms"
    )
    print(
        f"root-vs-downstream avg lag: root={summary['root_avg_ms_mean']:.2f} ms "
        f"downstream={summary['downstream_avg_ms_mean']:.2f} ms"
    )
    print(
        f"makespan: ideal_cp={summary['ideal_cp_ms_mean']:.2f} ms "
        f"actual_e2e={summary['actual_e2e_ms_mean']:.2f} ms"
    )
    print(
        f"makespan efficiency={summary['makespan_efficiency_mean']:.3f} "
        f"(stdev={summary['makespan_efficiency_stdev']:.3f}), "
        f"amplification={summary['makespan_amplification_mean']:.2f}x"
    )
    print(
        f"warm-runs: mean={summary['warm_mean_seconds']:.3f}s "
        f"stdev={summary['warm_stdev_seconds']:.3f}s cv={summary['warm_cv']*100:.2f}%"
    )
    off = summary.get("official", {})
    if off:
        print(
            f"official: Airflow2.0={off.get('airflow_2_0_beta')}s "
            f"Airflow1.10.10={off.get('airflow_1_10_10')}s"
        )
        print(
            f"speedup vs Airflow2.0: all={summary['vs_airflow2x_mean']:.2f}x "
            f"warm={summary['vs_airflow2x_warm']:.2f}x"
        )
        print(
            f"speedup vs Airflow1.10.10: all={summary['vs_airflow110x_mean']:.2f}x "
            f"warm={summary['vs_airflow110x_warm']:.2f}x"
        )


def main() -> int:
    args = parse_args()
    repo_root = Path(args.root_dir).resolve()
    base_config = (repo_root / args.config).resolve()
    results_dir = (repo_root / args.results_dir).resolve()
    results_dir.mkdir(parents=True, exist_ok=True)

    if not base_config.exists():
        raise RuntimeError(f"config not found: {base_config}")
    scenarios = list(args.scenarios)
    if args.include_burst_sweep:
        for scene in BURST_SWEEP_SCENARIOS:
            if scene not in scenarios:
                scenarios.append(scene)
    if args.include_realistic_topologies:
        for scene in REALISTIC_SWEEP_SCENARIOS:
            if scene not in scenarios:
                scenarios.append(scene)

    all_summaries = []

    for scene in scenarios:
        ensure_generated_bench_scenarios(repo_root, scene)
        print(f"\n----- scenario {scene}: prepare daemon -----")
        scenario_expected = expected_dag_ids(repo_root, scene)
        temp_config = Path("/tmp") / f"dagforge_bench_{scene}.toml"
        make_config_for_scenario(repo_root, base_config, scene, temp_config)

        stop_existing_daemons(repo_root)
        log_path = start_daemon_for_scenario(
            repo_root=repo_root,
            config_path=temp_config,
            scenario=scene,
            taskset_cpus=args.taskset_cpus.strip(),
        )
        wait_scenario_ready(
            api_base=args.api_base,
            expect=scenario_expected,
            timeout_s=args.startup_timeout,
        )
        print(
            f"daemon ready for {scene}; dags={len(scenario_expected)} log={log_path}"
        )

        run_payloads = []
        for idx in range(args.rounds):
            print(f"\n[{scene}] run {idx + 1}/{args.rounds}")
            try:
                payload = run_one_round(
                    repo_root=repo_root,
                    scenario=scene,
                    results_dir=results_dir,
                )
            except Exception:
                # one fast retry after daemon restart for crash/segfault/no-listen cases
                print(f"[{scene}] run {idx + 1}: failed once, restarting daemon and retrying")
                stop_existing_daemons(repo_root)
                log_path = start_daemon_for_scenario(
                    repo_root=repo_root,
                    config_path=temp_config,
                    scenario=scene,
                    taskset_cpus=args.taskset_cpus.strip(),
                )
                wait_scenario_ready(
                    api_base=args.api_base,
                    expect=scenario_expected,
                    timeout_s=args.startup_timeout,
                )
                payload = run_one_round(
                    repo_root=repo_root,
                    scenario=scene,
                    results_dir=results_dir,
                )
            run_payloads.append(payload)
            lag_s = float(payload["results"]["total_lag_ms"]) / 1000.0
            wall_s = float(payload["derived"]["wall_seconds"])
            tps = float(payload["derived"]["effective_tasks_per_s"])
            p95 = float(payload["derived"]["lag_distribution"]["p95_ms"])
            p99 = float(payload["derived"]["lag_distribution"]["p99_ms"])
            eff = float(payload["derived"]["makespan"]["efficiency_mean"])
            amp = float(payload["derived"]["makespan"]["amplification_mean"])
            print(
                f"[{scene}] run {idx + 1}: total_task_lag={lag_s:.3f}s "
                f"wall={wall_s:.3f}s tps={tps:.1f} p95={p95:.2f}ms p99={p99:.2f}ms "
                f"eff={eff:.3f} amp={amp:.2f}x"
            )

        summary = summarize_scene(scene, run_payloads)
        print_scene_summary(summary)
        all_summaries.append(summary)

    out = {
        "generated_at_epoch_s": time.time(),
        "config": str(base_config),
        "rounds": args.rounds,
        "taskset_cpus": args.taskset_cpus,
        "scenarios": scenarios,
        "summaries": all_summaries,
    }
    out_path = Path(args.output_json)
    out_path.write_text(json.dumps(out, indent=2), encoding="utf-8")
    print(f"\nWrote summary JSON: {out_path}")

    stop_existing_daemons(repo_root)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
