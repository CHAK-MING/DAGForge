#include "e2e_test_support.hpp"
#include "test_utils.hpp"

#include "gtest/gtest.h"

#include <filesystem>
#include <format>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>



#include "dagforge/app/application.hpp"
#include "dagforge/app/services/execution_service.hpp"
#include "dagforge/app/services/persistence_service.hpp"
#include "dagforge/app/services/scheduler_service.hpp"
#include "dagforge/util/json.hpp"


using namespace dagforge;

namespace {

auto repo_root() -> std::filesystem::path {
  const auto here = std::filesystem::path(__FILE__);
  return here.parent_path().parent_path();
}

auto fixture_root() -> std::filesystem::path {
  return repo_root() / "tests" / "fixtures" / "topologies";
}

auto fixture_path(std::string_view name) -> std::filesystem::path {
  return fixture_root() / std::string(name);
}

auto make_test_config(std::uint16_t api_port) -> SystemConfig {
  return dagforge::test::make_mysql_test_config(api_port);
}

auto wait_api_ready(std::uint16_t port) -> bool {
  return dagforge::test::wait_api_ready(port);
}

auto load_fixture_to_temp_dir(std::string_view name,
                              std::string_view dag_id_override = {})
    -> std::filesystem::path {
  const auto src = fixture_path(name);
  auto tmp = dagforge::test::make_temp_dir("dagforge_topology_case_");
  if (tmp.empty()) {
    throw std::runtime_error("failed to create temp directory");
  }
  const auto dst_name = dag_id_override.empty()
                            ? src.filename()
                            : std::filesystem::path(
                                  std::format("{}.toml", dag_id_override));
  const auto dst = std::filesystem::path(tmp) / dst_name;
  std::filesystem::copy_file(src, dst,
                             std::filesystem::copy_options::overwrite_existing);
  return std::filesystem::path(tmp);
}

auto trigger_dag(std::uint16_t port, std::string_view dag_id) -> std::string {
  return dagforge::test::trigger_dag(port, dag_id);
}

auto fetch_run_state(std::uint16_t port, std::string_view run_id)
    -> std::optional<std::string> {
  return dagforge::test::fetch_run_state(port, run_id);
}

using TaskSnapshot = dagforge::test::TaskSnapshot;

auto fetch_run_tasks(std::uint16_t port, std::string_view run_id)
    -> std::unordered_map<std::string, TaskSnapshot> {
  return dagforge::test::fetch_run_tasks(port, run_id);
}

auto wait_for_all_tasks_terminal(std::uint16_t port, std::string_view run_id,
                                 std::chrono::seconds timeout =
                                     std::chrono::seconds(30)) -> bool {
  auto is_terminal = [](std::string_view state) {
    return state == "success" || state == "failed" ||
           state == "upstream_failed" || state == "skipped";
  };

  return dagforge::test::poll_until(
      [&]() {
        auto tasks = fetch_run_tasks(port, run_id);
        if (tasks.empty()) {
          return false;
        }
        for (const auto &[task_id, snapshot] : tasks) {
          (void)task_id;
          if (!is_terminal(snapshot.state)) {
            return false;
          }
        }
        return true;
      },
      timeout, std::chrono::milliseconds(100));
}

struct TopologyCase {
  std::string fixture_name;
  std::string name;
  std::size_t expected_task_count;
};

class TopologyTomlE2ETest : public ::testing::TestWithParam<TopologyCase> {
protected:
  void SetUp() override {
    dagforge::test::reset_mysql_test_database(
        dagforge::test::make_mysql_test_config(0).database);
  }
  void TearDown() override {
    dagforge::test::reset_mysql_test_database(
        dagforge::test::make_mysql_test_config(0).database);
  }
};

TEST_P(TopologyTomlE2ETest, TopologyFixtureValidatesAndRunsToSuccess) {
  const auto &tc = GetParam();
  const auto port = dagforge::test::pick_unused_tcp_port_or_zero();
  ASSERT_NE(port, 0);

  auto validation =
      DAGInfoLoader::load_from_file(fixture_path(tc.fixture_name).string());
  ASSERT_TRUE(validation.has_value()) << validation.error().message();

  const auto dag_id = std::format("{}_{}", tc.name, port);
  const auto dag_dir = load_fixture_to_temp_dir(tc.fixture_name, dag_id);
  auto cfg = make_test_config(port);
  cfg.dag_source.directory = dag_dir.string();
  Application app(cfg);
  auto init_res = app.init();
  ASSERT_TRUE(init_res) << init_res.error().message();

  auto start_res = app.start();
  if (!start_res) {
    GTEST_SKIP() << "MySQL unavailable for topology E2E: "
                 << start_res.error().message();
  }

  ASSERT_TRUE(wait_api_ready(port));

  const auto run_id = trigger_dag(port, dag_id);
  ASSERT_TRUE(dagforge::test::poll_until(
      [&]() { return fetch_run_state(port, run_id) == "success"; },
      std::chrono::seconds(30), std::chrono::milliseconds(100)));
  ASSERT_TRUE(wait_for_all_tasks_terminal(port, run_id));

  const auto tasks = fetch_run_tasks(port, run_id);
  ASSERT_EQ(tasks.size(), tc.expected_task_count);
  for (const auto &[task_id, snapshot] : tasks) {
    EXPECT_EQ(snapshot.state, "success") << task_id;
  }

  app.stop();
  std::filesystem::remove_all(dag_dir);
}

INSTANTIATE_TEST_SUITE_P(
    BenchTopologies, TopologyTomlE2ETest,
    ::testing::Values(
        TopologyCase{.fixture_name = "scene01_linear.toml",
                     .name = "topology_linear",
                     .expected_task_count = 5},
        TopologyCase{.fixture_name = "scene02_tree.toml",
                     .name = "topology_tree",
                     .expected_task_count = 5},
        TopologyCase{.fixture_name = "scene03_burst_ready.toml",
                     .name = "topology_burst_ready",
                     .expected_task_count = 6},
        TopologyCase{.fixture_name = "scene04_diamond.toml",
                     .name = "topology_diamond",
                     .expected_task_count = 4},
        TopologyCase{.fixture_name = "scene05_fanout.toml",
                     .name = "topology_fanout",
                     .expected_task_count = 5},
        TopologyCase{.fixture_name = "scene06_fanin.toml",
                     .name = "topology_fanin",
                     .expected_task_count = 6},
        TopologyCase{.fixture_name = "scene07_mesh.toml",
                     .name = "topology_mesh",
                     .expected_task_count = 6}),
    [](const ::testing::TestParamInfo<TopologyCase> &param_info) {
      return param_info.param.name;
    });

} // namespace
