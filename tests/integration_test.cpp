#include "dagforge/app/application.hpp"
#include "dagforge/config/task_config.hpp"
#include "dagforge/dag/dag_manager.hpp"
#include "test_utils.hpp"

#include "gtest/gtest.h"

#include <netinet/in.h>
#include <filesystem>
#include <fstream>
#include <sys/socket.h>
#include <unistd.h>

using namespace dagforge;

namespace {

class ListeningSocket {
public:
  ListeningSocket() = default;
  ~ListeningSocket() {
    if (fd_ >= 0) {
      ::close(fd_);
    }
  }

  ListeningSocket(const ListeningSocket &) = delete;
  auto operator=(const ListeningSocket &) -> ListeningSocket & = delete;

  auto bind_loopback(std::uint16_t port) -> bool {
    fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd_ < 0) {
      return false;
    }

    int reuse = 1;
    ::setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (::bind(fd_, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0) {
      return false;
    }
    return ::listen(fd_, SOMAXCONN) == 0;
  }

private:
  int fd_{-1};
};

class DatabaseBackedIntegrationTest : public ::testing::Test {
protected:
  void SetUp() override {
    test::reset_mysql_test_database(test::make_test_config(0).database);
  }

  void TearDown() override {
    test::reset_mysql_test_database(test::make_test_config(0).database);
  }
};

} // namespace

TEST(IntegrationTest, ApplicationConstructs) {
  Application app;
  EXPECT_FALSE(app.is_running());
}

TEST(IntegrationTest, StopBeforeStartIsNoop) {
  Application app;
  EXPECT_FALSE(app.is_running());
  app.stop();
  EXPECT_FALSE(app.is_running());
}

TEST(IntegrationTest, DagManagerCreateDeleteInMemory) {
  Application app;

  DAGId dag_id{"it_dag"};
  DAGInfo info;
  info.dag_id = dag_id;
  info.name = "Integration DAG";

  auto create = app.dag_manager().create_dag(dag_id, info);
  ASSERT_TRUE(create.has_value());

  auto got = app.dag_manager().get_dag(dag_id);
  ASSERT_TRUE(got.has_value());
  EXPECT_EQ(got->name, "Integration DAG");

  auto del = app.dag_manager().delete_dag(dag_id);
  EXPECT_TRUE(del.has_value());
}

TEST(IntegrationTest, TriggerRunWhileStoppedReturnsInvalidState) {
  Application app;

  DAGInfo info;
  info.dag_id = DAGId{"stopped_trigger_dag"};
  info.name = "Stopped Trigger DAG";

  auto task = TaskConfig::builder()
                  .id("worker")
                  .name("worker")
                  .command("echo stopped")
                  .build();
  ASSERT_TRUE(task.has_value()) << task.error().message();
  info.tasks.push_back(std::move(*task));
  info.rebuild_task_index();

  const auto create_res = app.dag_manager().create_dag(info.dag_id, info);
  ASSERT_TRUE(create_res.has_value()) << create_res.error().message();

  const auto run_res = app.trigger_run_blocking(info.dag_id);
  ASSERT_FALSE(run_res.has_value());
  EXPECT_EQ(run_res.error(), make_error_code(Error::InvalidState));
}

TEST(IntegrationTest, LoadConfigRebuildsRuntimeFromUpdatedSchedulerSettings) {
  Application app;

  const auto temp_path = test::make_temp_path("dagforge_app_config_");
  ASSERT_FALSE(temp_path.empty());

  {
    std::ofstream out(temp_path, std::ios::trunc);
    ASSERT_TRUE(out.is_open());
    out << R"(
[scheduler]
shards = 3
scheduler_shards = 2
max_concurrency = 7

[api]
enabled = false
)";
  }

  const auto load_res = app.load_config(temp_path);
  ASSERT_TRUE(load_res.has_value()) << load_res.error().message();
  EXPECT_EQ(app.config().scheduler.shards, 3);
  EXPECT_EQ(app.runtime().shard_count(), 3);

  std::remove(temp_path.c_str());
}

TEST(IntegrationTest, StartFailureStopsRuntimeAndClearsRunningFlag) {
  SystemConfig cfg;
  cfg.api.enabled = false;
  cfg.database.host = "invalid-host-for-dagforge-tests";
  cfg.database.connect_timeout = 1;

  Application app(std::move(cfg));
  const auto start_res = app.start();

  ASSERT_FALSE(start_res.has_value());
  EXPECT_FALSE(app.is_running());
  EXPECT_FALSE(app.runtime().is_running());
}

TEST(IntegrationTest, InitDbOnlyFailureStopsRuntimeAndClearsRunningFlag) {
  SystemConfig cfg;
  cfg.api.enabled = false;
  cfg.database.host = "invalid-host-for-dagforge-tests";
  cfg.database.connect_timeout = 1;

  Application app(std::move(cfg));
  const auto init_res = app.init_db_only();

  ASSERT_FALSE(init_res.has_value());
  EXPECT_FALSE(app.is_running());
  EXPECT_FALSE(app.runtime().is_running());
}

TEST_F(DatabaseBackedIntegrationTest,
       StartFailureOnApiBindConflictRollsBackRuntimeState) {
  const auto port = test::pick_unused_tcp_port_or_zero();
  ASSERT_NE(port, 0);

  ListeningSocket occupied;
  ASSERT_TRUE(occupied.bind_loopback(port));

  auto cfg = test::make_test_config(port);
  cfg.api.enabled = true;

  Application app(std::move(cfg));
  const auto start_res = app.start();
  if (!start_res &&
      (start_res.error() == make_error_code(Error::DatabaseOpenFailed) ||
       start_res.error() == make_error_code(Error::DatabaseQueryFailed))) {
    GTEST_SKIP() << "MySQL unavailable for API bind conflict lifecycle test: "
                 << start_res.error().message();
  }

  ASSERT_FALSE(start_res.has_value());
  EXPECT_FALSE(app.is_running());
  EXPECT_FALSE(app.runtime().is_running());
}

TEST_F(DatabaseBackedIntegrationTest, LoadConfigWhileRunningReturnsInvalidState) {
  auto cfg = test::make_test_config(0);
  cfg.api.enabled = false;

  Application app(std::move(cfg));
  const auto start_res = app.start();
  if (!start_res) {
    GTEST_SKIP() << "MySQL unavailable for running-config lifecycle test: "
                 << start_res.error().message();
  }

  const auto temp_path = test::make_temp_path("dagforge_running_config_");
  ASSERT_FALSE(temp_path.empty());
  {
    std::ofstream out(temp_path, std::ios::trunc);
    ASSERT_TRUE(out.is_open());
    out << "[api]\nenabled = false\n";
  }

  const auto load_res = app.load_config(temp_path);
  EXPECT_FALSE(load_res.has_value());
  EXPECT_EQ(load_res.error(), make_error_code(Error::InvalidState));

  std::remove(temp_path.c_str());
  app.stop();
}

TEST(IntegrationTest, LoadDagsFromDirectoryIsTransactionalWhenAFileIsInvalid) {
  Application app;

  DAGId existing_dag_id{"existing_dag"};
  DAGInfo existing_info;
  existing_info.dag_id = existing_dag_id;
  existing_info.name = "Existing DAG";
  ASSERT_TRUE(
      app.dag_manager().create_dag(existing_dag_id, existing_info).has_value());

  const auto temp_dir = test::make_temp_dir("dagforge_app_dags_");
  ASSERT_FALSE(temp_dir.empty());

  {
    std::ofstream out(std::filesystem::path(temp_dir) / "valid.toml",
                      std::ios::trunc);
    ASSERT_TRUE(out.is_open());
    out << R"(
id = "valid_dag"
name = "valid_dag"

[[tasks]]
id = "task1"
command = "echo 1"
)";
  }

  {
    std::ofstream out(std::filesystem::path(temp_dir) / "invalid.toml",
                      std::ios::trunc);
    ASSERT_TRUE(out.is_open());
    out << R"(
id = "invalid_dag"
name = ""
tasks = []
)";
  }

  const auto load_res = app.load_dags_from_directory(temp_dir);
  EXPECT_FALSE(load_res.has_value());

  auto existing = app.dag_manager().get_dag(existing_dag_id);
  ASSERT_TRUE(existing.has_value());
  EXPECT_EQ(existing->name, "Existing DAG");
  EXPECT_FALSE(app.dag_manager().has_dag(DAGId{"valid_dag"}));

  std::filesystem::remove_all(temp_dir);
}

TEST_F(DatabaseBackedIntegrationTest, StopReturnsWithinBudgetEvenWithActiveRun) {
  auto cfg = test::make_test_config(0);
  cfg.api.enabled = false;

  Application app(std::move(cfg));
  const auto start_res = app.start();
  if (!start_res) {
    GTEST_SKIP() << "MySQL unavailable for lifecycle integration test: "
                 << start_res.error().message();
  }

  SensorExecutorConfig sensor;
  sensor.type = SensorType::File;
  sensor.target = "/tmp/dagforge_missing_shutdown_probe.flag";
  sensor.poke_interval = std::chrono::seconds(1);

  auto task = TaskConfig::builder()
                  .id("wait_forever")
                  .name("wait_forever")
                  .executor(ExecutorType::Sensor)
                  .config(sensor)
                  .timeout(std::chrono::seconds(30))
                  .retry(0, std::chrono::seconds(1))
                  .build();
  ASSERT_TRUE(task.has_value());

  DAGInfo info;
  do {
    info.dag_id = test::dag_id(test::unique_token("shutdown_dag"));
  } while (app.dag_manager().has_dag(info.dag_id));
  info.name = info.dag_id.str();
  info.tasks.push_back(std::move(*task));
  info.rebuild_task_index();

  const auto create_res = app.dag_manager().create_dag(info.dag_id, info);
  ASSERT_TRUE(create_res.has_value()) << create_res.error().message();

  const auto run_res = app.trigger_run_blocking(info.dag_id);
  ASSERT_TRUE(run_res.has_value()) << run_res.error().message();

  ASSERT_TRUE(test::poll_until([&]() { return app.has_active_runs(); },
                               std::chrono::seconds(2),
                               std::chrono::milliseconds(10)));

  const auto started_at = std::chrono::steady_clock::now();
  app.stop();
  const auto elapsed =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::steady_clock::now() - started_at);

  EXPECT_FALSE(app.is_running());
  EXPECT_FALSE(app.runtime().is_running());
  EXPECT_LT(elapsed, std::chrono::milliseconds(5000));
}

TEST_F(DatabaseBackedIntegrationTest, InitDbOnlyLeavesApplicationStopped) {
  auto cfg = test::make_test_config(0);
  cfg.api.enabled = false;

  Application app(std::move(cfg));
  const auto init_res = app.init_db_only();
  if (!init_res) {
    GTEST_SKIP() << "MySQL unavailable for init_db_only lifecycle test: "
                 << init_res.error().message();
  }

  EXPECT_FALSE(app.is_running());
  EXPECT_FALSE(app.runtime().is_running());
}

TEST_F(DatabaseBackedIntegrationTest, TriggerRunAfterStopReturnsInvalidState) {
  auto cfg = test::make_test_config(0);
  cfg.api.enabled = false;

  Application app(std::move(cfg));
  const auto start_res = app.start();
  if (!start_res) {
    GTEST_SKIP() << "MySQL unavailable for post-stop trigger test: "
                 << start_res.error().message();
  }

  DAGInfo info;
  do {
    info.dag_id = test::dag_id(test::unique_token("post_stop_dag"));
  } while (app.dag_manager().has_dag(info.dag_id));
  info.name = info.dag_id.str();

  auto task = TaskConfig::builder()
                  .id("worker")
                  .name("worker")
                  .command("echo post-stop")
                  .build();
  ASSERT_TRUE(task.has_value()) << task.error().message();
  info.tasks.push_back(std::move(*task));
  info.rebuild_task_index();

  const auto create_res = app.dag_manager().create_dag(info.dag_id, info);
  ASSERT_TRUE(create_res.has_value()) << create_res.error().message();

  app.stop();

  const auto run_res = app.trigger_run_blocking(info.dag_id);
  ASSERT_FALSE(run_res.has_value());
  EXPECT_EQ(run_res.error(), make_error_code(Error::InvalidState));
}

TEST_F(DatabaseBackedIntegrationTest,
       StartSucceedsWhenConfigWatcherDirectoryIsMissing) {
  auto cfg = test::make_test_config(0);
  cfg.api.enabled = false;
  cfg.dag_source.directory = "/nonexistent/path/for/config_watcher_start";

  Application app(std::move(cfg));
  const auto start_res = app.start();
  if (!start_res) {
    GTEST_SKIP() << "MySQL unavailable for config watcher lifecycle test: "
                 << start_res.error().message();
  }

  EXPECT_TRUE(app.is_running());
  EXPECT_TRUE(app.runtime().is_running());

  app.stop();
  EXPECT_FALSE(app.is_running());
}
