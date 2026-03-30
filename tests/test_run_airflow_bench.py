import asyncio
import json
import importlib.util
import os
import pathlib
import tempfile
import sys
import types
import unittest
from contextlib import redirect_stdout
from io import StringIO


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[1]
SCRIPT_PATH = PROJECT_ROOT / "bench" / "scripts" / "run_airflow_bench.py"
sys.path.insert(0, str(SCRIPT_PATH.parent))

spec = importlib.util.spec_from_file_location("run_airflow_bench", SCRIPT_PATH)
bench = importlib.util.module_from_spec(spec)
assert spec.loader is not None
sys.modules.setdefault("aiohttp", types.SimpleNamespace(ClientSession=object))
sys.modules.setdefault("pymysql", types.SimpleNamespace(connect=None))
spec.loader.exec_module(bench)

gen_spec = importlib.util.spec_from_file_location(
    "gen_airflow_bench_dags",
    PROJECT_ROOT / "bench" / "scripts" / "gen_airflow_bench_dags.py",
)
generator = importlib.util.module_from_spec(gen_spec)
assert gen_spec.loader is not None
gen_spec.loader.exec_module(generator)

analysis_spec = importlib.util.spec_from_file_location(
    "analyze_airflow_bench_breakdown",
    PROJECT_ROOT / "bench" / "scripts" / "analyze_airflow_bench_breakdown.py",
)
analysis = importlib.util.module_from_spec(analysis_spec)

perf_pipeline_spec = importlib.util.spec_from_file_location(
    "perf_pipeline",
    PROJECT_ROOT / "bench" / "scripts" / "perf_pipeline.py",
)
perf_pipeline = importlib.util.module_from_spec(perf_pipeline_spec)
sys.modules.setdefault("perf_pipeline", perf_pipeline)


class BuildTaskLagQueryTests(unittest.TestCase):
    def test_root_task_baseline_falls_back_to_run_started_at(self):
        sql = bench.build_task_lag_query(["run_a", "run_b"])

        self.assertIn(
            "COALESCE(td.max_upstream_finished_at, r.started_at)",
            sql,
        )
        self.assertIn("task_started_at > 0", sql)
        self.assertNotIn("RootTaskStart", sql)

    def test_task_persistence_query_includes_task_state(self):
        sql = bench.build_task_persistence_query(["run_a"])
        self.assertIn("state", sql)
        self.assertIn("LatestTaskInstances", sql)
        self.assertIn("RankedAttempts", sql)


class WaitForPersistenceTests(unittest.IsolatedAsyncioTestCase):
    async def test_waits_until_task_rows_are_stable_after_run_completion(self):
        observed_calls = []
        responses = iter(
            [
                {"total_tasks": 999, "finished_tasks": 998},
                {"total_tasks": 1000, "finished_tasks": 999},
                {"total_tasks": 1000, "finished_tasks": 1000},
            ]
        )

        async def fake_probe(run_ids):
            observed_calls.append(tuple(run_ids))
            return next(responses)

        async def fake_sleep(_seconds):
            return None

        result = await bench.wait_for_task_persistence(
            ["run_a", "run_b"],
            expected_task_count=1000,
            probe_fn=fake_probe,
            sleep_fn=fake_sleep,
            timeout_s=1.0,
            poll_interval_s=0.0,
        )

        self.assertEqual(
            result,
            {"total_tasks": 1000, "finished_tasks": 1000},
        )
        self.assertEqual(
            observed_calls,
            [("run_a", "run_b"), ("run_a", "run_b"), ("run_a", "run_b")],
        )

    async def test_wait_for_runs_polls_until_all_runs_complete(self):
        responses = {
            "run_a": iter(["running", "success"]),
            "run_b": iter(["running", "failed"]),
        }
        sleeps = []

        async def fake_get_run_state(_session, run_id):
            return next(responses[run_id])

        async def fake_sleep(seconds):
            sleeps.append(seconds)

        await bench.wait_for_runs(
            session=object(),
            run_ids=["run_a", "run_b"],
            get_run_state_fn=fake_get_run_state,
            sleep_fn=fake_sleep,
            timeout_s=1.0,
            progress_interval_s=0.0,
        )

        self.assertEqual(sleeps, [0.1])


class BenchDagGeneratorTests(unittest.TestCase):
    def test_generator_uses_dependencies_field(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = pathlib.Path(tmpdir) / "dag.toml"
            generator.write_toml(str(output_path), "bench_dag", 3, "linear")
            content = output_path.read_text(encoding="utf-8")

        self.assertIn('dependencies = ["t0"]', content)
        self.assertNotIn('depends_on = [', content)
        self.assertNotIn('{ task_id = "t0" }', content)


class BenchResultArtifactTests(unittest.TestCase):
    def test_build_result_payload_keeps_analysis_time_out_of_throughput(self):
        payload = bench.build_result_payload(
            lag_row=(1001, 1001, 2008.0, 2.006, 8.0),
            persisted_total_tasks=1001,
            wall_seconds=0.1,
            analysis_seconds=1.05,
        )

        self.assertEqual(payload["total_tasks"], 1001)
        self.assertEqual(payload["analysis_seconds"], 1.05)
        self.assertAlmostEqual(payload["wall_seconds"], 0.1)
        self.assertAlmostEqual(payload["effective_tasks_per_s"], 10010.0)

    def test_benchmark_result_file_is_written_with_run_ids(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = bench.write_benchmark_result(
                output_dir=tmpdir,
                scenario_name="scene1_linear_100x10",
                run_ids=["run_1", "run_2"],
                expected_tasks=1000,
            results={
                "total_tasks": 1000,
                "lag_tasks": 998,
                "total_lag_ms": 23174.0,
                "avg_lag_ms": 23.17,
                "max_lag_ms": 324.0,
            },
        )

            payload = json.loads(pathlib.Path(output_path).read_text(encoding="utf-8"))

        self.assertEqual(payload["scenario"], "scene1_linear_100x10")
        self.assertEqual(payload["run_ids"], ["run_1", "run_2"])
        self.assertEqual(payload["expected_tasks"], 1000)
        self.assertEqual(payload["results"]["total_lag_ms"], 23174.0)
        self.assertEqual(payload["results"]["lag_tasks"], 998)


analysis_spec.loader.exec_module(analysis)
perf_pipeline_spec.loader.exec_module(perf_pipeline)
sql_spec = importlib.util.spec_from_file_location(
    "benchlib.sql",
    PROJECT_ROOT / "bench" / "scripts" / "benchlib" / "sql.py",
)
bench_sql = importlib.util.module_from_spec(sql_spec)
assert sql_spec.loader is not None
sql_spec.loader.exec_module(bench_sql)


class BreakdownAnalysisTests(unittest.TestCase):
    def test_parse_trigger_logs_extracts_existing_timing_fields(self):
        lines = [
            "[2026-03-13 02:00:00.000] [info] HTTP trigger completed dag_id=dag_a dag_run_id=run_a latency_ms=18",
            "[2026-03-13 02:00:00.000] [info] trigger_run_on_dag_owner_shard dag_id=dag_a dag_run_id=run_a ensure_rowids_ms=4 run_create_ms=12 total_ms=16",
            "[2026-03-13 02:00:00.000] [info] trigger_run_on_owner_shard dag_id=dag_a dag_run_id=run_a first_persist_ms=11 dispatch_ms=1 total_ms=12",
        ]

        parsed = analysis.parse_trigger_timings(lines)

        self.assertIn("run_a", parsed)
        self.assertEqual(parsed["run_a"]["http_latency_ms"], 18)
        self.assertEqual(parsed["run_a"]["ensure_rowids_ms"], 4)
        self.assertEqual(parsed["run_a"]["run_create_ms"], 12)
        self.assertEqual(parsed["run_a"]["first_persist_ms"], 11)
        self.assertEqual(parsed["run_a"]["dispatch_ms"], 1)

    def test_lag_breakdown_query_has_no_trailing_aggregate_comma(self):
        sql = bench_sql.build_lag_breakdown_query(["run_a"])
        self.assertNotIn("MAX(up_ti.finished_at) AS max_upstream_finished_at,", sql)
        self.assertIn("MAX(up_ti.finished_at) AS max_upstream_finished_at", sql)

    def test_summary_only_prints_official_numbers_for_matching_scene(self):
        buffer = StringIO()
        with redirect_stdout(buffer):
            analysis.print_summary(
                "scene99_unknown",
                {"total_lag_ms": 1234.0, "avg_lag_ms": 12.34},
                {"root": {"tasks": 1, "avg_lag_ms": 1.0, "total_lag_ms": 1.0, "max_lag_ms": 1.0}},
                {},
            )
        output = buffer.getvalue()
        self.assertIn("Scenario: scene99_unknown", output)
        self.assertNotIn("Airflow 2.0 beta", output)


class PerfPipelineReportTests(unittest.TestCase):
    def test_emit_report_accepts_six_analysis_payloads(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            old_results_dir = os.environ.get("DAGFORGE_PERF_PIPELINE_RESULTS_DIR")
            os.environ["DAGFORGE_PERF_PIPELINE_RESULTS_DIR"] = tmpdir
            try:
                rows = []
                for config_id in range(1, 7):
                    rows.append(
                        {
                            "config_id": config_id,
                            "benchmark_name": f"bench_{config_id}",
                            "real_time_us": float(config_id * 10),
                            "items_per_second": float(config_id * 100),
                            "derived": {
                                "ipc": float(config_id) / 10.0,
                                "cache_miss_rate": float(config_id) / 1000.0,
                                "branch_miss_rate": float(config_id) / 2000.0,
                            },
                            "notes": ["steady"],
                        }
                    )

                payload = perf_pipeline.emit_report(
                    [json.dumps(row) for row in rows],
                    "test_run",
                )

                self.assertEqual(payload, 0)
                report_path = pathlib.Path(tmpdir) / "test_run" / "report.md"
                report_json = pathlib.Path(tmpdir) / "test_run" / "report.json"
                self.assertTrue(report_path.exists())
                self.assertTrue(report_json.exists())
                loaded = json.loads(report_json.read_text(encoding="utf-8"))
                self.assertEqual(loaded["summary"]["benchmark_count"], 6)
                self.assertEqual(len(loaded["rows"]), 6)
            finally:
                if old_results_dir is None:
                    os.environ.pop("DAGFORGE_PERF_PIPELINE_RESULTS_DIR", None)
                else:
                    os.environ["DAGFORGE_PERF_PIPELINE_RESULTS_DIR"] = old_results_dir


if __name__ == "__main__":
    unittest.main()
