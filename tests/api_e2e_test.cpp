#include "dagforge/app/application.hpp"
#include "dagforge/client/http/http_client.hpp"
#include "dagforge/config/task_config.hpp"
#include "dagforge/util/json.hpp"

#include "test_utils.hpp"
#include "gtest/gtest.h"

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/use_future.hpp>

#include <array>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <format>
#include <fstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>

using namespace dagforge;

namespace {

auto env_or_default(const char *key, std::string fallback) -> std::string {
  if (const char *v = std::getenv(key); v && *v != '\0') {
    return v;
  }
  return fallback;
}

auto unique_token(std::string_view base) -> std::string {
  static std::atomic<std::uint64_t> seq{0};
  const auto n = seq.fetch_add(1, std::memory_order_relaxed);
  return std::format("{}_{}", base, n);
}

auto example_dags_dir() -> std::string {
  if (const char *v = std::getenv("DAGFORGE_TEST_DAGS_DIR"); v && *v != '\0') {
    return v;
  }
  const auto here = std::filesystem::path(__FILE__);
  return (here.parent_path().parent_path() / "dags").string();
}

auto make_test_config(std::uint16_t api_port) -> Config {
  Config cfg{};
  cfg.api.enabled = true;
  cfg.api.host = "127.0.0.1";
  cfg.api.port = api_port;
  cfg.scheduler.shards = 2;
  cfg.scheduler.max_concurrency = 16;
  cfg.database.host =
      env_or_default("DAGFORGE_TEST_MYSQL_HOST", cfg.database.host);
  cfg.database.username =
      env_or_default("DAGFORGE_TEST_MYSQL_USER", cfg.database.username);
  cfg.database.password =
      env_or_default("DAGFORGE_TEST_MYSQL_PASSWORD", cfg.database.password);
  cfg.database.database =
      env_or_default("DAGFORGE_TEST_MYSQL_DB", cfg.database.database);
  cfg.database.connect_timeout = 2;
  return cfg;
}

auto response_body_string(const http::HttpResponse &resp) -> std::string {
  return std::string(resp.body.begin(), resp.body.end());
}

auto http_post_json(Application &app, std::uint16_t port, std::string_view path,
                    std::string_view payload) -> Result<http::HttpResponse> {
  auto fut = boost::asio::co_spawn(
      app.runtime().shard(0).ctx(),
      [&]() -> task<Result<http::HttpResponse>> {
        auto client = co_await http::HttpClient::connect_tcp(
            app.runtime().shard(0).ctx(), "127.0.0.1", port);
        if (!client) {
          co_return fail(client.error());
        }
        auto resp = co_await (*client)->post_json(path, payload);
        co_return ok(resp);
      },
      boost::asio::use_future);
  return fut.get();
}

auto http_get(Application &app, std::uint16_t port, std::string_view path)
    -> Result<http::HttpResponse> {
  auto fut = boost::asio::co_spawn(
      app.runtime().shard(0).ctx(),
      [&]() -> task<Result<http::HttpResponse>> {
        auto client = co_await http::HttpClient::connect_tcp(
            app.runtime().shard(0).ctx(), "127.0.0.1", port);
        if (!client) {
          co_return fail(client.error());
        }
        auto resp = co_await (*client)->get(path);
        co_return ok(resp);
      },
      boost::asio::use_future);
  return fut.get();
}

auto poll_run_state(Application &app, std::uint16_t port,
                    std::string_view run_id, std::string_view expected_state,
                    std::chrono::seconds timeout = std::chrono::seconds(8))
    -> bool {
  const std::string expected{expected_state};
  return dagforge::test::poll_until(
      [&]() {
        auto history_resp =
            http_get(app, port, std::format("/api/history/{}", run_id));
        if (!history_resp || history_resp->status != http::HttpStatus::Ok) {
          return false;
        }
        auto history_json = parse_json(response_body_string(*history_resp));
        if (!history_json || !history_json->contains("state")) {
          return false;
        }
        const auto state_val = dump_json((*history_json)["state"]);
        const auto state = state_val.size() >= 2
                               ? state_val.substr(1, state_val.size() - 2)
                               : state_val;
        return state == expected;
      },
      timeout, std::chrono::milliseconds(100));
}

auto assert_all_tasks_terminal_or_expected(
    Application &app, std::uint16_t port, std::string_view run_id,
    const std::unordered_map<std::string, std::string> &expected_states = {},
    bool require_exact_task_set = false,
    std::chrono::seconds timeout = std::chrono::seconds(8)) -> bool {
  auto is_terminal = [](std::string_view state) {
    return state == "success" || state == "failed" ||
           state == "upstream_failed" || state == "skipped";
  };

  return dagforge::test::poll_until(
      [&]() {
        auto tasks_resp =
            http_get(app, port, std::format("/api/runs/{}/tasks", run_id));
        if (!tasks_resp || tasks_resp->status != http::HttpStatus::Ok) {
          return false;
        }
        auto tasks_json = parse_json(response_body_string(*tasks_resp));
        if (!tasks_json || !tasks_json->contains("tasks")) {
          return false;
        }

        const auto *arr = (*tasks_json)["tasks"].get_if<JsonValue::array_t>();
        if (!arr) {
          return false;
        }

        std::unordered_map<std::string, std::string> observed;
        observed.reserve(arr->size());
        for (const auto &entry : *arr) {
          const auto *tid = entry["task_id"].get_if<std::string>();
          const auto *st = entry["state"].get_if<std::string>();
          if (!tid || !st) {
            return false;
          }
          if (!is_terminal(*st)) {
            return false;
          }
          observed[*tid] = *st;
        }

        if (require_exact_task_set &&
            observed.size() != expected_states.size()) {
          return false;
        }

        for (const auto &[task_id, expected] : expected_states) {
          auto it = observed.find(task_id);
          if (it == observed.end() || it->second != expected) {
            return false;
          }
        }
        return true;
      },
      timeout, std::chrono::milliseconds(100));
}

auto poll_task_state(Application &app, std::uint16_t port,
                     std::string_view run_id, std::string_view task_id,
                     std::string_view expected_state,
                     std::chrono::seconds timeout = std::chrono::seconds(8))
    -> bool {
  const std::string expected{expected_state};
  return dagforge::test::poll_until(
      [&]() {
        auto tasks_resp =
            http_get(app, port, std::format("/api/runs/{}/tasks", run_id));
        if (!tasks_resp || tasks_resp->status != http::HttpStatus::Ok) {
          return false;
        }
        auto tasks_json = parse_json(response_body_string(*tasks_resp));
        if (!tasks_json || !tasks_json->contains("tasks")) {
          return false;
        }
        const auto *arr = (*tasks_json)["tasks"].get_if<JsonValue::array_t>();
        if (!arr) {
          return false;
        }
        for (const auto &entry : *arr) {
          const auto *tid = entry["task_id"].get_if<std::string>();
          const auto *st = entry["state"].get_if<std::string>();
          if (!tid || !st) {
            continue;
          }
          if (*tid == task_id) {
            return *st == expected;
          }
        }
        return false;
      },
      timeout, std::chrono::milliseconds(100));
}

auto wait_api_ready(Application &app, std::uint16_t port,
                    std::chrono::seconds timeout = std::chrono::seconds(3))
    -> bool {
  return dagforge::test::poll_until(
      [&]() {
        auto resp = http_get(app, port, "/api/dags");
        return resp.has_value() && resp->status == http::HttpStatus::Ok;
      },
      timeout, std::chrono::milliseconds(50));
}

auto write_dag_file(const std::filesystem::path &path, std::string_view content)
    -> bool {
  std::ofstream out(path, std::ios::out | std::ios::trunc);
  if (!out.is_open()) {
    return false;
  }
  out << content;
  out.flush();
  return out.good();
}

auto json_bool(const JsonValue &value) -> bool {
  if (const auto *b = value.get_if<bool>()) {
    return *b;
  }
  if (const auto *n = value.get_if<int64_t>()) {
    return *n != 0;
  }
  return false;
}

} // namespace

TEST(ApiE2EIntegrationTest, TriggerHistoryAndXComRoundtrip) {
  const auto port = dagforge::test::pick_unused_tcp_port_or_zero();
  ASSERT_NE(port, 0);

  Application app(make_test_config(port));
  ASSERT_TRUE(app.init().has_value());
  auto start_res = app.start();
  if (!start_res) {
    GTEST_SKIP() << "MySQL unavailable for API E2E: "
                 << start_res.error().message();
  }

  const auto dag_id = unique_token("bench_dag");

  DAGInfo dag{};
  dag.dag_id = DAGId{dag_id};
  dag.name = dag_id;
  dag.created_at = std::chrono::system_clock::now();
  dag.updated_at = dag.created_at;

  auto task_res = TaskConfig::builder()
                      .id("worker")
                      .name("worker")
                      .command("echo '{\"answer\":42}'")
                      .build();
  ASSERT_TRUE(task_res.has_value());
  task_res->xcom_push.push_back(XComPushConfig{
      .key = "payload",
      .source = XComSource::Json,
  });
  dag.tasks.push_back(*task_res);
  dag.rebuild_task_index();
  ASSERT_TRUE(app.dag_manager().upsert_dag(dag.dag_id, dag).has_value());

  auto trigger_resp = http_post_json(
      app, port, std::format("/api/dags/{}/trigger", dag_id), "{}");
  ASSERT_TRUE(trigger_resp.has_value());
  ASSERT_EQ(trigger_resp->status, http::HttpStatus::Created);

  auto trigger_json = parse_json(response_body_string(*trigger_resp));
  ASSERT_TRUE(trigger_json.has_value());
  ASSERT_TRUE(trigger_json->contains("dag_run_id"));
  const auto run_id = (*trigger_json)["dag_run_id"].as<std::string>();

  bool completed = poll_run_state(app, port, run_id, "success");
  ASSERT_TRUE(completed);
  ASSERT_TRUE(assert_all_tasks_terminal_or_expected(
      app, port, run_id, {{"worker", "success"}}, true));

  auto xcom_resp = http_get(
      app, port, std::format("/api/runs/{}/tasks/worker/xcom", run_id));
  ASSERT_TRUE(xcom_resp.has_value());
  ASSERT_EQ(xcom_resp->status, http::HttpStatus::Ok);
  auto xcom_json = parse_json(response_body_string(*xcom_resp));
  ASSERT_TRUE(xcom_json.has_value());
  ASSERT_TRUE(xcom_json->contains("xcom"));
  ASSERT_TRUE((*xcom_json)["xcom"].contains("payload"));
  ASSERT_EQ((*xcom_json)["xcom"]["payload"]["answer"].as<int>(), 42);

  app.stop();
}

TEST(ApiE2EIntegrationTest, DagPauseAndUnpauseEndpointsReflectPausedState) {
  const auto port = dagforge::test::pick_unused_tcp_port_or_zero();
  ASSERT_NE(port, 0);

  Application app(make_test_config(port));
  ASSERT_TRUE(app.init().has_value());
  auto start_res = app.start();
  if (!start_res) {
    GTEST_SKIP() << "MySQL unavailable for pause/unpause E2E: "
                 << start_res.error().message();
  }

  const auto dag_id = unique_token("pause_toggle_dag");
  DAGInfo dag{};
  dag.dag_id = DAGId{dag_id};
  dag.name = dag_id;
  dag.created_at = std::chrono::system_clock::now();
  dag.updated_at = dag.created_at;

  auto task_res = TaskConfig::builder()
                      .id("worker")
                      .name("worker")
                      .command("echo worker")
                      .build();
  ASSERT_TRUE(task_res.has_value());
  dag.tasks.push_back(*task_res);
  dag.rebuild_task_index();
  ASSERT_TRUE(app.dag_manager().upsert_dag(dag.dag_id, dag).has_value());

  auto dag_before = http_get(app, port, std::format("/api/dags/{}", dag_id));
  ASSERT_TRUE(dag_before.has_value());
  ASSERT_EQ(dag_before->status, http::HttpStatus::Ok);
  auto dag_before_json = parse_json(response_body_string(*dag_before));
  ASSERT_TRUE(dag_before_json.has_value());
  ASSERT_TRUE(dag_before_json->contains("is_paused"));
  EXPECT_FALSE(json_bool((*dag_before_json)["is_paused"]));

  auto pause_resp = http_post_json(
      app, port, std::format("/api/dags/{}/pause", dag_id), "{}");
  ASSERT_TRUE(pause_resp.has_value());
  ASSERT_EQ(pause_resp->status, http::HttpStatus::Ok);
  auto pause_json = parse_json(response_body_string(*pause_resp));
  ASSERT_TRUE(pause_json.has_value());
  ASSERT_TRUE(pause_json->contains("is_paused"));
  EXPECT_TRUE(json_bool((*pause_json)["is_paused"]));

  auto dag_paused = http_get(app, port, std::format("/api/dags/{}", dag_id));
  ASSERT_TRUE(dag_paused.has_value());
  ASSERT_EQ(dag_paused->status, http::HttpStatus::Ok);
  auto dag_paused_json = parse_json(response_body_string(*dag_paused));
  ASSERT_TRUE(dag_paused_json.has_value());
  ASSERT_TRUE(dag_paused_json->contains("is_paused"));
  EXPECT_TRUE(json_bool((*dag_paused_json)["is_paused"]));

  auto unpause_resp = http_post_json(
      app, port, std::format("/api/dags/{}/unpause", dag_id), "{}");
  ASSERT_TRUE(unpause_resp.has_value());
  ASSERT_EQ(unpause_resp->status, http::HttpStatus::Ok);
  auto unpause_json = parse_json(response_body_string(*unpause_resp));
  ASSERT_TRUE(unpause_json.has_value());
  ASSERT_TRUE(unpause_json->contains("is_paused"));
  EXPECT_FALSE(json_bool((*unpause_json)["is_paused"]));

  auto dag_unpaused = http_get(app, port, std::format("/api/dags/{}", dag_id));
  ASSERT_TRUE(dag_unpaused.has_value());
  ASSERT_EQ(dag_unpaused->status, http::HttpStatus::Ok);
  auto dag_unpaused_json = parse_json(response_body_string(*dag_unpaused));
  ASSERT_TRUE(dag_unpaused_json.has_value());
  ASSERT_TRUE(dag_unpaused_json->contains("is_paused"));
  EXPECT_FALSE(json_bool((*dag_unpaused_json)["is_paused"]));

  app.stop();
}

TEST(ApiE2EIntegrationTest,
     HotReloadUpdatesDagDefinitionWithoutLosingHistoricalRuns) {
  const auto port = dagforge::test::pick_unused_tcp_port_or_zero();
  ASSERT_NE(port, 0);

  const auto dag_dir = dagforge::test::make_temp_dir("dagforge_reload_dags_");
  ASSERT_FALSE(dag_dir.empty());

  const auto dag_path = std::filesystem::path(dag_dir) / "hot_reload.toml";
  ASSERT_TRUE(write_dag_file(dag_path,
                             R"(id = "hot_reload"
name = "Hot Reload Demo"
description = "v1"

[[tasks]]
id = "step_a"
name = "Step A"
command = "echo v1"
)"));

  auto cfg = make_test_config(port);
  cfg.dag_source.directory = dag_dir;

  Application app(cfg);
  ASSERT_TRUE(app.init().has_value());
  auto start_res = app.start();
  if (!start_res) {
    GTEST_SKIP() << "MySQL unavailable for hot-reload E2E: "
                 << start_res.error().message();
  }

  ASSERT_TRUE(wait_api_ready(app, port));

  ASSERT_TRUE(dagforge::test::poll_until(
      [&]() {
        auto dag_resp = http_get(app, port, "/api/dags/hot_reload");
        return dag_resp.has_value() && dag_resp->status == http::HttpStatus::Ok;
      },
      std::chrono::seconds(5), std::chrono::milliseconds(50)));

  auto trigger_v1 =
      http_post_json(app, port, "/api/dags/hot_reload/trigger", "{}");
  ASSERT_TRUE(trigger_v1.has_value());
  ASSERT_EQ(trigger_v1->status, http::HttpStatus::Created);
  auto trigger_v1_json = parse_json(response_body_string(*trigger_v1));
  ASSERT_TRUE(trigger_v1_json.has_value());
  const auto run_v1 = (*trigger_v1_json)["dag_run_id"].as<std::string>();

  ASSERT_TRUE(poll_run_state(app, port, run_v1, "success"));
  ASSERT_TRUE(assert_all_tasks_terminal_or_expected(
      app, port, run_v1, {{"step_a", "success"}}, true));

  std::this_thread::sleep_for(std::chrono::milliseconds(150));
  ASSERT_TRUE(write_dag_file(dag_path,
                             R"(id = "hot_reload"
name = "Hot Reload Demo"
description = "v2"

[[tasks]]
id = "step_a"
name = "Step A"
command = "echo v2"

[[tasks]]
id = "step_b"
name = "Step B"
command = "echo v2b"
dependencies = ["step_a"]
)"));

  ASSERT_TRUE(
      app.load_dags_from_directory(cfg.dag_source.directory).has_value());

  auto tasks_resp = http_get(app, port, "/api/dags/hot_reload/tasks");
  ASSERT_TRUE(tasks_resp.has_value());
  ASSERT_EQ(tasks_resp->status, http::HttpStatus::Ok);
  auto tasks_json = parse_json(response_body_string(*tasks_resp));
  ASSERT_TRUE(tasks_json.has_value());
  ASSERT_TRUE(tasks_json->contains("tasks"));
  const auto *arr = (*tasks_json)["tasks"].get_if<JsonValue::array_t>();
  ASSERT_NE(arr, nullptr);

  std::unordered_set<std::string> ids;
  for (const auto &entry : *arr) {
    if (const auto *id = entry.get_if<std::string>()) {
      ids.insert(*id);
    }
  }
  EXPECT_EQ(ids.size(), 2U);
  EXPECT_TRUE(ids.contains("step_a"));
  EXPECT_TRUE(ids.contains("step_b"));

  auto trigger_v2 =
      http_post_json(app, port, "/api/dags/hot_reload/trigger", "{}");
  ASSERT_TRUE(trigger_v2.has_value());
  ASSERT_EQ(trigger_v2->status, http::HttpStatus::Created);
  auto trigger_v2_json = parse_json(response_body_string(*trigger_v2));
  ASSERT_TRUE(trigger_v2_json.has_value());
  const auto run_v2 = (*trigger_v2_json)["dag_run_id"].as<std::string>();

  ASSERT_TRUE(poll_run_state(app, port, run_v2, "success"));
  ASSERT_TRUE(assert_all_tasks_terminal_or_expected(
      app, port, run_v2, {{"step_a", "success"}, {"step_b", "success"}}, true));

  auto run_v1_detail =
      http_get(app, port, std::format("/api/history/{}", run_v1));
  ASSERT_TRUE(run_v1_detail.has_value());
  ASSERT_EQ(run_v1_detail->status, http::HttpStatus::Ok);
  auto run_v1_detail_json = parse_json(response_body_string(*run_v1_detail));
  ASSERT_TRUE(run_v1_detail_json.has_value());
  EXPECT_EQ((*run_v1_detail_json)["state"].as<std::string>(), "success");

  auto run_v1_tasks =
      http_get(app, port, std::format("/api/runs/{}/tasks", run_v1));
  ASSERT_TRUE(run_v1_tasks.has_value());
  ASSERT_EQ(run_v1_tasks->status, http::HttpStatus::Ok);
  auto run_v1_tasks_json = parse_json(response_body_string(*run_v1_tasks));
  ASSERT_TRUE(run_v1_tasks_json.has_value());
  ASSERT_TRUE(run_v1_tasks_json->contains("tasks"));

  bool step_a_success = false;
  const auto *run_v1_arr =
      (*run_v1_tasks_json)["tasks"].get_if<JsonValue::array_t>();
  ASSERT_NE(run_v1_arr, nullptr);
  for (const auto &entry : *run_v1_arr) {
    const auto *tid = entry["task_id"].get_if<std::string>();
    const auto *st = entry["state"].get_if<std::string>();
    if (!tid || !st) {
      continue;
    }
    if (*tid == "step_a" && *st == "success") {
      step_a_success = true;
      break;
    }
  }
  EXPECT_TRUE(step_a_success);

  app.stop();
  std::filesystem::remove_all(dag_dir);
}

TEST(ApiE2EIntegrationTest, PublicExamplesCanTriggerAndComplete) {
  const auto port = dagforge::test::pick_unused_tcp_port_or_zero();
  ASSERT_NE(port, 0);

  auto cfg = make_test_config(port);
  cfg.dag_source.directory = example_dags_dir();

  Application app(cfg);
  ASSERT_TRUE(app.init().has_value());
  auto start_res = app.start();
  if (!start_res) {
    GTEST_SKIP() << "MySQL unavailable for examples E2E: "
                 << start_res.error().message();
  }

  ASSERT_TRUE(wait_api_ready(app, port));

  // Reload DAGs from examples directory so this test validates the public DAG
  // definitions even if DB contains historical DAG metadata.
  ASSERT_TRUE(
      app.load_dags_from_directory(cfg.dag_source.directory).has_value());

  // Cron/catchup sanity: daily_etl is cron-enabled and should not immediately
  // backfill runs on startup in this short window.
  auto dag_resp = http_get(app, port, "/api/dags/daily_etl");
  ASSERT_TRUE(dag_resp.has_value());
  ASSERT_EQ(dag_resp->status, http::HttpStatus::Ok);
  auto dag_json = parse_json(response_body_string(*dag_resp));
  ASSERT_TRUE(dag_json.has_value());
  ASSERT_TRUE(dag_json->contains("cron"));
  EXPECT_FALSE((*dag_json)["cron"].as<std::string>().empty());

  auto history_before = http_get(app, port, "/api/dags/daily_etl/history");
  ASSERT_TRUE(history_before.has_value());
  ASSERT_EQ(history_before->status, http::HttpStatus::Ok);
  auto history_before_json = parse_json(response_body_string(*history_before));
  ASSERT_TRUE(history_before_json.has_value());

  std::this_thread::sleep_for(std::chrono::milliseconds(500));
  auto history_after = http_get(app, port, "/api/dags/daily_etl/history");
  ASSERT_TRUE(history_after.has_value());
  ASSERT_EQ(history_after->status, http::HttpStatus::Ok);
  auto history_after_json = parse_json(response_body_string(*history_after));
  ASSERT_TRUE(history_after_json.has_value());
  ASSERT_EQ(dump_json(*history_before_json), dump_json(*history_after_json));

  const std::array<std::string_view, 5> dag_ids = {
      "hello_world", "xcom_pipeline", "branching", "daily_etl", "trigger_rules",
  };

  for (const auto dag_id : dag_ids) {
    auto trigger_resp = http_post_json(
        app, port, std::format("/api/dags/{}/trigger", dag_id), "{}");
    ASSERT_TRUE(trigger_resp.has_value()) << dag_id;
    ASSERT_EQ(trigger_resp->status, http::HttpStatus::Created) << dag_id;

    auto trigger_json = parse_json(response_body_string(*trigger_resp));
    ASSERT_TRUE(trigger_json.has_value()) << dag_id;
    ASSERT_TRUE(trigger_json->contains("dag_run_id")) << dag_id;
    const auto run_id = (*trigger_json)["dag_run_id"].as<std::string>();

    const auto expected_run_state =
        dag_id == std::string_view{"trigger_rules"} ? "failed" : "success";
    ASSERT_TRUE(poll_run_state(app, port, run_id, expected_run_state))
        << dag_id;

    auto tasks_resp =
        http_get(app, port, std::format("/api/runs/{}/tasks", run_id));
    ASSERT_TRUE(tasks_resp.has_value()) << dag_id;
    ASSERT_EQ(tasks_resp->status, http::HttpStatus::Ok) << dag_id;

    auto tasks_json = parse_json(response_body_string(*tasks_resp));
    ASSERT_TRUE(tasks_json.has_value()) << dag_id;
    ASSERT_TRUE(tasks_json->contains("tasks")) << dag_id;

    std::unordered_map<std::string, std::string> task_state;
    const auto *arr = (*tasks_json)["tasks"].get_if<JsonValue::array_t>();
    ASSERT_NE(arr, nullptr) << dag_id;
    for (const auto &entry : *arr) {
      const auto *tid = entry["task_id"].get_if<std::string>();
      const auto *st = entry["state"].get_if<std::string>();
      if (tid && st) {
        task_state[*tid] = *st;
      }
    }

    if (dag_id == std::string_view{"hello_world"}) {
      ASSERT_TRUE(assert_all_tasks_terminal_or_expected(
          app, port, run_id, {{"greet", "success"}, {"complete", "success"}},
          true));
      EXPECT_EQ(task_state.size(), 2U);
      EXPECT_EQ(task_state["greet"], "success");
      EXPECT_EQ(task_state["complete"], "success");
    } else if (dag_id == std::string_view{"xcom_pipeline"}) {
      ASSERT_TRUE(
          assert_all_tasks_terminal_or_expected(app, port, run_id,
                                                {{"generate_data", "success"},
                                                 {"process", "success"},
                                                 {"report", "success"}},
                                                true));
      EXPECT_EQ(task_state.size(), 3U);
      EXPECT_EQ(task_state["generate_data"], "success");
      EXPECT_EQ(task_state["process"], "success");
      EXPECT_EQ(task_state["report"], "success");

      auto xcom_resp =
          http_get(app, port, std::format("/api/runs/{}/xcom", run_id));
      ASSERT_TRUE(xcom_resp.has_value());
      ASSERT_EQ(xcom_resp->status, http::HttpStatus::Ok);
      auto xcom_json = parse_json(response_body_string(*xcom_resp));
      ASSERT_TRUE(xcom_json.has_value());
      ASSERT_TRUE((*xcom_json)["xcom"].contains("generate_data"));
      ASSERT_TRUE((*xcom_json)["xcom"]["generate_data"].contains("metadata"));
      EXPECT_EQ((*xcom_json)["xcom"]["generate_data"]["metadata"]["records"]
                    .as<int>(),
                1000);
      EXPECT_EQ((*xcom_json)["xcom"]["generate_data"]["metadata"]["source"]
                    .as<std::string>(),
                "api");
    } else if (dag_id == std::string_view{"branching"}) {
      ASSERT_TRUE(assert_all_tasks_terminal_or_expected(
          app, port, run_id,
          {{"check_size", "success"}, {"merge_results", "success"}}));
      EXPECT_EQ(task_state.size(), 4U);
      EXPECT_EQ(task_state["check_size"], "success");
      EXPECT_EQ(task_state["merge_results"], "success");
      const bool large_success = task_state["large_path"] == "success";
      const bool small_success = task_state["small_path"] == "success";
      const bool large_skipped = task_state["large_path"] == "skipped";
      const bool small_skipped = task_state["small_path"] == "skipped";
      EXPECT_TRUE((large_success && small_skipped) ||
                  (small_success && large_skipped));
    } else if (dag_id == std::string_view{"daily_etl"}) {
      ASSERT_TRUE(
          assert_all_tasks_terminal_or_expected(app, port, run_id,
                                                {{"extract", "success"},
                                                 {"validate", "success"},
                                                 {"transform", "success"},
                                                 {"load", "success"},
                                                 {"notify", "success"}},
                                                true));
      EXPECT_EQ(task_state.size(), 5U);
      EXPECT_EQ(task_state["extract"], "success");
      EXPECT_EQ(task_state["validate"], "success");
      EXPECT_EQ(task_state["transform"], "success");
      EXPECT_EQ(task_state["load"], "success");
      EXPECT_EQ(task_state["notify"], "success");
    } else if (dag_id == std::string_view{"trigger_rules"}) {
      ASSERT_TRUE(assert_all_tasks_terminal_or_expected(
          app, port, run_id,
          {{"start", "success"},
           {"upstream_success", "success"},
           {"upstream_failed", "failed"},
           {"branch_selector", "success"},
           {"branch_kept", "success"},
           {"branch_skipped", "skipped"},
           {"rule_all_success", "success"},
           {"rule_all_failed", "success"},
           {"rule_all_done", "success"},
           {"rule_one_success", "success"},
           {"rule_one_failed", "success"},
           {"rule_one_done", "success"},
           {"rule_none_failed", "success"},
           {"rule_none_skipped", "success"},
           {"rule_all_done_min_one_success", "success"},
           {"rule_all_skipped", "success"},
           {"rule_none_failed_min_one_success", "success"},
           {"rule_always", "success"},
           {"cleanup", "success"}},
          true));
      EXPECT_EQ(task_state.size(), 19U);
    }

    auto run_detail =
        http_get(app, port, std::format("/api/history/{}", run_id));
    ASSERT_TRUE(run_detail.has_value()) << dag_id;
    ASSERT_EQ(run_detail->status, http::HttpStatus::Ok) << dag_id;
    auto run_detail_json = parse_json(response_body_string(*run_detail));
    ASSERT_TRUE(run_detail_json.has_value()) << dag_id;
    ASSERT_TRUE(run_detail_json->contains("trigger_type")) << dag_id;
    EXPECT_EQ((*run_detail_json)["trigger_type"].as<std::string>(), "manual")
        << dag_id;
  }

  app.stop();
}

TEST(ApiE2EIntegrationTest, DependsOnPastBlocksByFailingCurrentTask) {
  const auto port = dagforge::test::pick_unused_tcp_port_or_zero();
  ASSERT_NE(port, 0);

  Application app(make_test_config(port));
  ASSERT_TRUE(app.init().has_value());
  auto start_res = app.start();
  if (!start_res) {
    GTEST_SKIP() << "MySQL unavailable for depends_on_past E2E: "
                 << start_res.error().message();
  }

  const auto dag_id = unique_token("depends_on_past_dag");

  DAGInfo dag{};
  dag.dag_id = DAGId{dag_id};
  dag.name = dag_id;
  dag.created_at = std::chrono::system_clock::now();
  dag.updated_at = dag.created_at;

  auto task = TaskConfig::builder()
                  .id("guarded")
                  .name("guarded")
                  .command("exit 1")
                  .build();
  ASSERT_TRUE(task.has_value());
  task->depends_on_past = true;
  task->max_retries = 0;
  dag.tasks.push_back(*task);
  dag.rebuild_task_index();
  ASSERT_TRUE(app.dag_manager().upsert_dag(dag.dag_id, dag).has_value());

  auto trigger_first = http_post_json(
      app, port, std::format("/api/dags/{}/trigger", dag_id), "{}");
  ASSERT_TRUE(trigger_first.has_value());
  ASSERT_EQ(trigger_first->status, http::HttpStatus::Created);
  auto first_json = parse_json(response_body_string(*trigger_first));
  ASSERT_TRUE(first_json.has_value());
  const auto first_run_id = (*first_json)["dag_run_id"].as<std::string>();
  ASSERT_TRUE(poll_run_state(app, port, first_run_id, "failed"));
  ASSERT_TRUE(assert_all_tasks_terminal_or_expected(
      app, port, first_run_id, {{"guarded", "failed"}}, true));

  auto trigger_second = http_post_json(
      app, port, std::format("/api/dags/{}/trigger", dag_id), "{}");
  ASSERT_TRUE(trigger_second.has_value());
  ASSERT_EQ(trigger_second->status, http::HttpStatus::Created);
  auto second_json = parse_json(response_body_string(*trigger_second));
  ASSERT_TRUE(second_json.has_value());
  const auto second_run_id = (*second_json)["dag_run_id"].as<std::string>();
  ASSERT_TRUE(poll_run_state(app, port, second_run_id, "failed"));
  ASSERT_TRUE(assert_all_tasks_terminal_or_expected(
      app, port, second_run_id, {{"guarded", "failed"}}, true));

  auto run_tasks_resp =
      http_get(app, port, std::format("/api/runs/{}/tasks", second_run_id));
  ASSERT_TRUE(run_tasks_resp.has_value());
  ASSERT_EQ(run_tasks_resp->status, http::HttpStatus::Ok);
  auto tasks_json = parse_json(response_body_string(*run_tasks_resp));
  ASSERT_TRUE(tasks_json.has_value());
  ASSERT_TRUE(tasks_json->contains("tasks"));

  bool found_guarded = false;
  for (const auto &entry : (*tasks_json)["tasks"].get_array()) {
    if (!entry.contains("task_id") || !entry.contains("state")) {
      continue;
    }
    if (entry["task_id"].as<std::string>() == "guarded") {
      found_guarded = true;
      EXPECT_EQ(entry["state"].as<std::string>(), "failed");
      EXPECT_GE(entry["attempt"].as<int>(), 1);
      break;
    }
  }
  EXPECT_TRUE(found_guarded);

  app.stop();
}

TEST(ApiE2EIntegrationTest, RunTasksEndpointReturnsDagCompleteTaskSet) {
  const auto port = dagforge::test::pick_unused_tcp_port_or_zero();
  ASSERT_NE(port, 0);

  Application app(make_test_config(port));
  ASSERT_TRUE(app.init().has_value());
  auto start_res = app.start();
  if (!start_res) {
    GTEST_SKIP() << "MySQL unavailable for run tasks completeness E2E: "
                 << start_res.error().message();
  }

  const auto dag_id = unique_token("tasks_fullset_dag");
  DAGInfo dag{};
  dag.dag_id = DAGId{dag_id};
  dag.name = dag_id;
  dag.created_at = std::chrono::system_clock::now();
  dag.updated_at = dag.created_at;

  auto t1 =
      TaskConfig::builder().id("t1").name("t1").command("echo t1").build();
  auto t2 =
      TaskConfig::builder().id("t2").name("t2").command("echo t2").build();
  ASSERT_TRUE(t1.has_value());
  ASSERT_TRUE(t2.has_value());
  t2->dependencies = {TaskDependency{.task_id = TaskId{"t1"}, .label = ""}};
  dag.tasks.push_back(*t1);
  dag.tasks.push_back(*t2);
  dag.rebuild_task_index();
  ASSERT_TRUE(app.dag_manager().upsert_dag(dag.dag_id, dag).has_value());

  auto dag_loaded = app.dag_manager().get_dag(dag.dag_id);
  ASSERT_TRUE(dag_loaded.has_value());
  ASSERT_EQ(dag_loaded->tasks.size(), 2U);

  auto graph = std::make_shared<DAG>();
  auto idx1 = graph->add_node(TaskId{"t1"});
  auto idx2 = graph->add_node(TaskId{"t2"});
  ASSERT_TRUE(idx1.has_value());
  ASSERT_TRUE(idx2.has_value());
  ASSERT_TRUE(graph->add_edge(*idx1, *idx2).has_value());

  const DAGRunId run_id{unique_token("tasks_fullset_run")};
  auto run = DAGRun::create(run_id, graph);
  ASSERT_TRUE(run.has_value());
  const auto now = std::chrono::system_clock::now();
  run->set_dag_rowid(dag_loaded->dag_rowid);
  run->set_dag_version(dag_loaded->version);
  run->set_trigger_type(TriggerType::Manual);
  run->set_scheduled_at(now);
  run->set_started_at(now);
  run->set_execution_date(now);

  TaskInstanceInfo ti{};
  ti.task_rowid = dag_loaded->tasks[0].task_rowid;
  ti.attempt = 1;
  ti.state = TaskState::Success;
  ti.started_at = now;
  ti.finished_at = now;

  auto create_fut = boost::asio::co_spawn(
      app.runtime().shard(0).ctx(),
      app.persistence_service()->create_run_with_task_instances(
          std::move(*run), std::vector<TaskInstanceInfo>{ti}),
      boost::asio::use_future);
  auto create_res = create_fut.get();
  ASSERT_TRUE(create_res.has_value());

  auto run_tasks_resp =
      http_get(app, port, std::format("/api/runs/{}/tasks", run_id.str()));
  ASSERT_TRUE(run_tasks_resp.has_value());
  ASSERT_EQ(run_tasks_resp->status, http::HttpStatus::Ok);
  auto tasks_json = parse_json(response_body_string(*run_tasks_resp));
  ASSERT_TRUE(tasks_json.has_value());
  ASSERT_TRUE(tasks_json->contains("tasks"));

  auto tasks = (*tasks_json)["tasks"].get_array();
  ASSERT_EQ(tasks.size(), 2U);

  std::unordered_map<std::string, std::string> states;
  for (const auto &entry : tasks) {
    states.emplace(entry["task_id"].as<std::string>(),
                   entry["state"].as<std::string>());
  }
  ASSERT_TRUE(states.contains("t1"));
  ASSERT_TRUE(states.contains("t2"));
  EXPECT_EQ(states["t1"], "success");
  EXPECT_EQ(states["t2"], "pending");

  app.stop();
}

TEST(ApiE2EIntegrationTest, FailurePathPropagatesUpstreamFailedToDependents) {
  const auto port = dagforge::test::pick_unused_tcp_port_or_zero();
  ASSERT_NE(port, 0);

  Application app(make_test_config(port));
  ASSERT_TRUE(app.init().has_value());
  auto start_res = app.start();
  if (!start_res) {
    GTEST_SKIP() << "MySQL unavailable for failure-path E2E: "
                 << start_res.error().message();
  }

  const auto dag_id = unique_token("fail_path_dag");
  DAGInfo dag{};
  dag.dag_id = DAGId{dag_id};
  dag.name = dag_id;
  dag.created_at = std::chrono::system_clock::now();
  dag.updated_at = dag.created_at;

  auto fail_task = TaskConfig::builder()
                       .id("failer")
                       .name("failer")
                       .command("exit 101")
                       .build();
  ASSERT_TRUE(fail_task.has_value());

  auto downstream = TaskConfig::builder()
                        .id("downstream")
                        .name("downstream")
                        .command("echo should_not_run")
                        .depends_on("failer")
                        .build();
  ASSERT_TRUE(downstream.has_value());

  dag.tasks.push_back(*fail_task);
  dag.tasks.push_back(*downstream);
  dag.rebuild_task_index();
  ASSERT_TRUE(app.dag_manager().upsert_dag(dag.dag_id, dag).has_value());

  auto trigger_resp = http_post_json(
      app, port, std::format("/api/dags/{}/trigger", dag_id), "{}");
  ASSERT_TRUE(trigger_resp.has_value());
  ASSERT_EQ(trigger_resp->status, http::HttpStatus::Created);
  auto trigger_json = parse_json(response_body_string(*trigger_resp));
  ASSERT_TRUE(trigger_json.has_value());
  const auto run_id = (*trigger_json)["dag_run_id"].as<std::string>();

  ASSERT_TRUE(poll_run_state(app, port, run_id, "failed"));
  ASSERT_TRUE(assert_all_tasks_terminal_or_expected(
      app, port, run_id,
      {{"failer", "failed"}, {"downstream", "upstream_failed"}}, true));

  auto tasks_resp =
      http_get(app, port, std::format("/api/runs/{}/tasks", run_id));
  ASSERT_TRUE(tasks_resp.has_value());
  ASSERT_EQ(tasks_resp->status, http::HttpStatus::Ok);
  auto tasks_json = parse_json(response_body_string(*tasks_resp));
  ASSERT_TRUE(tasks_json.has_value());
  ASSERT_TRUE(tasks_json->contains("tasks"));

  bool failer_failed = false;
  bool downstream_upstream_failed = false;
  const auto *tasks_arr = (*tasks_json)["tasks"].get_if<JsonValue::array_t>();
  ASSERT_NE(tasks_arr, nullptr) << "tasks field missing or not an array";
  for (const auto &entry : *tasks_arr) {
    const auto *tid = entry["task_id"].get_if<std::string>();
    const auto *st = entry["state"].get_if<std::string>();
    if (!tid || !st)
      continue;
    if (*tid == "failer" && *st == "failed") {
      failer_failed = true;
    }
    if (*tid == "downstream" && *st == "upstream_failed") {
      downstream_upstream_failed = true;
    }
  }

  EXPECT_TRUE(failer_failed);
  EXPECT_TRUE(downstream_upstream_failed);
  app.stop();
}

TEST(ApiE2EIntegrationTest,
     BranchingRouterSkipsUnselectedPathAndCompletesJoin) {
  const auto port = dagforge::test::pick_unused_tcp_port_or_zero();
  ASSERT_NE(port, 0);

  Application app(make_test_config(port));
  ASSERT_TRUE(app.init().has_value());
  auto start_res = app.start();
  if (!start_res) {
    GTEST_SKIP() << "MySQL unavailable for branching E2E: "
                 << start_res.error().message();
  }

  const auto dag_id = unique_token("branch_path_dag");
  DAGInfo dag{};
  dag.dag_id = DAGId{dag_id};
  dag.name = dag_id;
  dag.created_at = std::chrono::system_clock::now();
  dag.updated_at = dag.created_at;

  auto router = TaskConfig::builder()
                    .id("router")
                    .name("router")
                    .command("echo '[\"left\"]'")
                    .branch(true, "branch")
                    .build();
  ASSERT_TRUE(router.has_value());
  router->xcom_push.push_back(XComPushConfig{
      .key = "branch",
      .source = XComSource::Json,
  });

  auto left = TaskConfig::builder()
                  .id("left")
                  .name("left")
                  .command("echo left")
                  .depends_on("router")
                  .build();
  ASSERT_TRUE(left.has_value());

  auto right = TaskConfig::builder()
                   .id("right")
                   .name("right")
                   .command("echo right")
                   .depends_on("router")
                   .build();
  ASSERT_TRUE(right.has_value());

  auto join = TaskConfig::builder()
                  .id("join")
                  .name("join")
                  .command("echo joined")
                  .depends_on("left")
                  .depends_on("right")
                  .trigger_rule(TriggerRule::OneSuccess)
                  .build();
  ASSERT_TRUE(join.has_value());

  dag.tasks = {*router, *left, *right, *join};
  dag.rebuild_task_index();
  ASSERT_TRUE(app.dag_manager().upsert_dag(dag.dag_id, dag).has_value());

  auto trigger_resp = http_post_json(
      app, port, std::format("/api/dags/{}/trigger", dag_id), "{}");
  ASSERT_TRUE(trigger_resp.has_value());
  ASSERT_EQ(trigger_resp->status, http::HttpStatus::Created);
  auto trigger_json = parse_json(response_body_string(*trigger_resp));
  ASSERT_TRUE(trigger_json.has_value());
  const auto run_id = (*trigger_json)["dag_run_id"].as<std::string>();

  ASSERT_TRUE(poll_run_state(app, port, run_id, "success"));
  ASSERT_TRUE(assert_all_tasks_terminal_or_expected(app, port, run_id,
                                                    {{"router", "success"},
                                                     {"left", "success"},
                                                     {"right", "skipped"},
                                                     {"join", "success"}},
                                                    true));

  auto tasks_resp =
      http_get(app, port, std::format("/api/runs/{}/tasks", run_id));
  ASSERT_TRUE(tasks_resp.has_value());
  ASSERT_EQ(tasks_resp->status, http::HttpStatus::Ok);
  auto tasks_json = parse_json(response_body_string(*tasks_resp));
  ASSERT_TRUE(tasks_json.has_value());

  ASSERT_TRUE(tasks_json->contains("tasks"));
  std::string left_state;
  std::string right_state;
  std::string join_state;
  const auto *branch_tasks_arr =
      (*tasks_json)["tasks"].get_if<JsonValue::array_t>();
  ASSERT_NE(branch_tasks_arr, nullptr) << "tasks field missing or not an array";
  for (const auto &entry : *branch_tasks_arr) {
    const auto *tid = entry["task_id"].get_if<std::string>();
    const auto *st = entry["state"].get_if<std::string>();
    if (!tid || !st)
      continue;
    if (*tid == "left") {
      left_state = *st;
    } else if (*tid == "right") {
      right_state = *st;
    } else if (*tid == "join") {
      join_state = *st;
    }
  }

  EXPECT_EQ(left_state, "success");
  EXPECT_EQ(right_state, "skipped");
  EXPECT_EQ(join_state, "success");
  std::string router_state;
  for (const auto &entry : *branch_tasks_arr) {
    const auto *tid = entry["task_id"].get_if<std::string>();
    const auto *st = entry["state"].get_if<std::string>();
    if (!tid || !st)
      continue;
    if (*tid == "router") {
      router_state = *st;
      break;
    }
  }
  EXPECT_EQ(router_state, "success");

  app.stop();
}

TEST(ApiE2EIntegrationTest, ChaosRecoveryMarksOrphanRunsAndTasksFailed) {
  const auto port = dagforge::test::pick_unused_tcp_port_or_zero();
  ASSERT_NE(port, 0);
  auto cfg = make_test_config(port);

  Application app(cfg);
  ASSERT_TRUE(app.init().has_value());
  auto start_res = app.start();
  if (!start_res) {
    GTEST_SKIP() << "MySQL unavailable for chaos recovery test: "
                 << start_res.error().message();
  }
  const auto dag_id = unique_token("chaos_orphan_dag");
  const auto run_id_raw = unique_token("chaos_orphan_run");

  DAGInfo dag{};
  dag.dag_id = DAGId{dag_id};
  dag.name = dag_id;
  dag.created_at = std::chrono::system_clock::now();
  dag.updated_at = dag.created_at;

  auto task = TaskConfig::builder()
                  .id("worker")
                  .name("worker")
                  .command("echo recovery")
                  .build();
  ASSERT_TRUE(task.has_value());
  dag.tasks.push_back(*task);
  dag.rebuild_task_index();
  ASSERT_TRUE(app.dag_manager().upsert_dag(dag.dag_id, dag).has_value());

  auto dag_loaded = app.dag_manager().get_dag(dag.dag_id);
  ASSERT_TRUE(dag_loaded.has_value());
  ASSERT_FALSE(dag_loaded->tasks.empty());

  auto graph = std::make_shared<DAG>();
  ASSERT_TRUE(graph->add_node(TaskId{"worker"}).has_value());

  const DAGRunId run_id{run_id_raw};
  auto run = DAGRun::create(run_id, graph);
  ASSERT_TRUE(run.has_value());
  run->set_dag_rowid(dag_loaded->dag_rowid);
  run->set_dag_version(dag_loaded->version);
  run->set_trigger_type(TriggerType::Manual);
  const auto now = std::chrono::system_clock::now();
  run->set_scheduled_at(now);
  run->set_started_at(now);
  run->set_execution_date(now);

  TaskInstanceInfo ti{};
  ti.task_rowid = dag_loaded->tasks.front().task_rowid;
  ti.attempt = 1;
  ti.state = TaskState::Running;
  ti.started_at = now;

  auto create_fut = boost::asio::co_spawn(
      app.runtime().shard(0).ctx(),
      app.persistence_service()->create_run_with_task_instances(
          std::move(*run), std::vector<TaskInstanceInfo>{ti}),
      boost::asio::use_future);
  auto create_res = create_fut.get();
  ASSERT_TRUE(create_res.has_value());

  app.stop();

  Application restarted(cfg);
  ASSERT_TRUE(restarted.init().has_value());
  auto restart_res = restarted.start();
  ASSERT_TRUE(restart_res.has_value());
  ASSERT_TRUE(restarted.recover_from_crash().has_value());

  auto state_fut = boost::asio::co_spawn(
      restarted.runtime().shard(0).ctx(),
      restarted.persistence_service()->get_dag_run_state(run_id),
      boost::asio::use_future);
  auto state = state_fut.get();
  ASSERT_TRUE(state.has_value());
  EXPECT_EQ(*state, DAGRunState::Failed);

  auto tasks_fut = boost::asio::co_spawn(
      restarted.runtime().shard(0).ctx(),
      restarted.persistence_service()->get_task_instances(run_id),
      boost::asio::use_future);
  auto tasks = tasks_fut.get();
  ASSERT_TRUE(tasks.has_value());
  ASSERT_EQ(tasks->size(), 1U);
  for (const auto &ti : *tasks) {
    EXPECT_EQ(ti.state, TaskState::Failed);
  }

  restarted.stop();
}

TEST(ApiE2EIntegrationTest,
     RetryableOrphanRunCurrentPolicyStillMarksFailedAfterRecovery) {
  const auto port = dagforge::test::pick_unused_tcp_port_or_zero();
  ASSERT_NE(port, 0);
  auto cfg = make_test_config(port);

  Application app(cfg);
  ASSERT_TRUE(app.init().has_value());
  auto start_res = app.start();
  if (!start_res) {
    GTEST_SKIP() << "MySQL unavailable for retryable orphan test: "
                 << start_res.error().message();
  }

  const auto dag_id = unique_token("retry_orphan_dag");
  const auto run_id_raw = unique_token("retry_orphan_run");

  DAGInfo dag{};
  dag.dag_id = DAGId{dag_id};
  dag.name = dag_id;
  dag.created_at = std::chrono::system_clock::now();
  dag.updated_at = dag.created_at;

  auto retry_task = TaskConfig::builder()
                        .id("retry_worker")
                        .name("retry_worker")
                        .command("echo retry")
                        .retry(3, std::chrono::seconds(1))
                        .build();
  ASSERT_TRUE(retry_task.has_value());
  dag.tasks.push_back(*retry_task);
  dag.rebuild_task_index();
  ASSERT_TRUE(app.dag_manager().upsert_dag(dag.dag_id, dag).has_value());

  auto dag_loaded = app.dag_manager().get_dag(dag.dag_id);
  ASSERT_TRUE(dag_loaded.has_value());

  auto graph = std::make_shared<DAG>();
  ASSERT_TRUE(graph->add_node(TaskId{"retry_worker"}).has_value());

  const DAGRunId run_id{run_id_raw};
  auto run = DAGRun::create(run_id, graph);
  ASSERT_TRUE(run.has_value());
  run->set_dag_rowid(dag_loaded->dag_rowid);
  run->set_dag_version(dag_loaded->version);
  run->set_trigger_type(TriggerType::Manual);
  const auto now = std::chrono::system_clock::now();
  run->set_scheduled_at(now);
  run->set_started_at(now);
  run->set_execution_date(now);

  TaskInstanceInfo ti{};
  ti.task_rowid = dag_loaded->tasks.front().task_rowid;
  ti.attempt = 1;
  ti.state = TaskState::Running;
  ti.started_at = now;

  auto create_fut = boost::asio::co_spawn(
      app.runtime().shard(0).ctx(),
      app.persistence_service()->create_run_with_task_instances(
          std::move(*run), std::vector<TaskInstanceInfo>{ti}),
      boost::asio::use_future);
  auto create_res = create_fut.get();
  ASSERT_TRUE(create_res.has_value());

  app.stop();

  Application restarted(cfg);
  ASSERT_TRUE(restarted.init().has_value());
  ASSERT_TRUE(restarted.start().has_value());
  ASSERT_TRUE(restarted.recover_from_crash().has_value());

  auto state_fut = boost::asio::co_spawn(
      restarted.runtime().shard(0).ctx(),
      restarted.persistence_service()->get_dag_run_state(run_id),
      boost::asio::use_future);
  auto state = state_fut.get();
  ASSERT_TRUE(state.has_value());
  EXPECT_EQ(*state, DAGRunState::Failed);

  auto tasks_fut = boost::asio::co_spawn(
      restarted.runtime().shard(0).ctx(),
      restarted.persistence_service()->get_task_instances(run_id),
      boost::asio::use_future);
  auto tasks = tasks_fut.get();
  ASSERT_TRUE(tasks.has_value());
  ASSERT_EQ(tasks->size(), 1U);
  EXPECT_EQ(tasks->front().state, TaskState::Failed);

  restarted.stop();
}

TEST(ApiE2EIntegrationTest, MetricsEndpointExposesPrometheusText) {
  const auto port = dagforge::test::pick_unused_tcp_port_or_zero();
  ASSERT_NE(port, 0);

  Application app(make_test_config(port));
  ASSERT_TRUE(app.init().has_value());
  auto start_res = app.start();
  if (!start_res) {
    GTEST_SKIP() << "MySQL unavailable for metrics test: "
                 << start_res.error().message();
  }

  std::optional<http::HttpResponse> metrics_resp;
  const bool ready = dagforge::test::poll_until(
      [&]() {
        auto resp = http_get(app, port, "/metrics");
        if (!resp || resp->status != http::HttpStatus::Ok) {
          return false;
        }
        metrics_resp = std::move(*resp);
        return true;
      },
      std::chrono::seconds(3), std::chrono::milliseconds(50));
  ASSERT_TRUE(ready);
  ASSERT_TRUE(metrics_resp.has_value());

  auto text = response_body_string(*metrics_resp);
  EXPECT_NE(text.find("dagforge_active_coroutines_total"), std::string::npos);
  EXPECT_NE(text.find("dagforge_mysql_batch_write_ops"), std::string::npos);
  EXPECT_NE(text.find("dagforge_event_bus_queue_length"), std::string::npos);
  EXPECT_NE(text.find("dagforge_shard_stall_age_ms"), std::string::npos);

  app.stop();
}
