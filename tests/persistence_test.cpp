#include "dagforge/app/services/persistence_service.hpp"
#include "dagforge/config/task_config.hpp"
#include "dagforge/core/runtime.hpp"
#include "test_utils.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <cstdlib>
#include <format>
#include <memory>
#include <string>
#include <thread>

using namespace dagforge;
using namespace dagforge::test;

namespace {

auto env_or_default(const char *key, std::string fallback) -> std::string {
  if (const char *v = std::getenv(key); v && *v != '\0') {
    return v;
  }
  return fallback;
}

auto load_test_db_config() -> DatabaseConfig {
  DatabaseConfig cfg;
  cfg.host = env_or_default("DAGFORGE_TEST_MYSQL_HOST", cfg.host);
  cfg.username = env_or_default("DAGFORGE_TEST_MYSQL_USER", cfg.username);
  cfg.password = env_or_default("DAGFORGE_TEST_MYSQL_PASSWORD", cfg.password);
  cfg.database = env_or_default("DAGFORGE_TEST_MYSQL_DB", cfg.database);
  cfg.connect_timeout = 2;
  return cfg;
}

} // namespace

class PersistenceTest : public ::testing::Test {
protected:
  struct PersistedDag {
    DAGId dag_id;
    TaskId task_id;
    int64_t dag_rowid{0};
    int64_t task_rowid{0};
  };

  void SetUp() override {
    ASSERT_TRUE(runtime_.start().has_value());
    service_ =
        std::make_unique<PersistenceService>(runtime_, load_test_db_config());
    auto open_res = run_coro(service_->open());
    if (!open_res) {
      runtime_.stop();
      GTEST_SKIP() << "MySQL unavailable for persistence tests: "
                   << open_res.error().message();
    }
  }

  void TearDown() override {
    if (service_) {
      run_coro(service_->close());
      service_.reset();
    }
    runtime_.stop();
  }

  auto unique_token(std::string_view base) const -> std::string {
    static std::atomic<std::uint64_t> seq{0};
    const auto n = seq.fetch_add(1, std::memory_order_relaxed);
    const auto *info = ::testing::UnitTest::GetInstance()->current_test_info();
    const auto test_name = info ? info->name() : "test";
    return std::format("{}_{}_{}", base, test_name, n);
  }

  auto create_persisted_dag(const char *dag_name, const char *task_name)
      -> PersistedDag {
    PersistedDag out;
    out.dag_id = DAGId(unique_token(dag_name));
    out.task_id = TaskId(unique_token(task_name));

    DAGInfo dag{};
    dag.dag_id = out.dag_id;
    dag.name = out.dag_id.str();
    dag.created_at = std::chrono::system_clock::now();
    dag.updated_at = dag.created_at;

    auto dag_rowid = run_coro(service_->save_dag(dag));
    EXPECT_TRUE(dag_rowid.has_value());
    out.dag_rowid = dag_rowid.value_or(0);

    auto task = TaskConfig::builder()
                    .id(out.task_id.str())
                    .name(out.task_id.str())
                    .command("echo ok")
                    .build();
    EXPECT_TRUE(task.has_value());

    auto task_rowid = run_coro(service_->save_task(out.dag_id, *task));
    EXPECT_TRUE(task_rowid.has_value());
    out.task_rowid = task_rowid.value_or(0);
    return out;
  }

  auto create_run(const DAGRunId &run_id, int64_t dag_rowid) -> Result<DAGRun> {
    auto graph = std::make_shared<DAG>();
    if (auto add = graph->add_node(TaskId{"task_1"}); !add) {
      return std::unexpected(add.error());
    }
    auto run = DAGRun::create(run_id, graph);
    if (!run) {
      return std::unexpected(run.error());
    }
    run->set_dag_rowid(dag_rowid);
    run->set_dag_version(1);
    run->set_trigger_type(TriggerType::Manual);
    const auto now = std::chrono::system_clock::now();
    run->set_scheduled_at(now);
    run->set_started_at(now);
    run->set_finished_at(now);
    run->set_execution_date(now);
    return run;
  }

  Runtime runtime_{1};
  std::unique_ptr<PersistenceService> service_;
};

TEST_F(PersistenceTest, SaveDagAndListDags_RealServicePath) {
  auto persisted = create_persisted_dag("persist_dag", "persist_task");
  ASSERT_GT(persisted.dag_rowid, 0);
  ASSERT_GT(persisted.task_rowid, 0);

  auto dags = run_coro(service_->list_dags());
  ASSERT_TRUE(dags.has_value());
  ASSERT_FALSE(dags->empty());
  const auto it = std::ranges::find_if(
      *dags, [&](const DAGInfo &d) { return d.dag_id == persisted.dag_id; });
  EXPECT_NE(it, dags->end());
}

TEST_F(PersistenceTest, GetDagRunState_NonexistentId_ReturnsNotFound) {
  auto result = run_coro(service_->get_dag_run_state(DAGRunId("nonexistent")));
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error(), Error::NotFound);
}

TEST_F(PersistenceTest, SaveTaskInstance_NonexistentRun_ReturnsNotFound) {
  TaskInstanceInfo info{};
  info.task_rowid = 1;
  info.attempt = 1;
  info.state = TaskState::Running;
  info.started_at = std::chrono::system_clock::now();

  auto result =
      run_coro(service_->update_task_instance(DAGRunId("missing_run"), info));
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error(), Error::NotFound);
}

TEST_F(PersistenceTest, MarkIncompleteRunsFailed_RealServicePath) {
  auto persisted = create_persisted_dag("recovery_dag", "recovery_task");
  ASSERT_GT(persisted.dag_rowid, 0);
  ASSERT_GT(persisted.task_rowid, 0);

  const DAGRunId run_id{unique_token("recovery_run")};
  auto run = create_run(run_id, persisted.dag_rowid);
  ASSERT_TRUE(run.has_value());

  TaskInstanceInfo ti{};
  ti.task_rowid = persisted.task_rowid;
  ti.attempt = 1;
  ti.state = TaskState::Running;
  ti.started_at = std::chrono::system_clock::now();

  auto create_res = run_coro(service_->create_run_with_task_instances(
      std::move(*run), std::vector<TaskInstanceInfo>{ti}));
  ASSERT_TRUE(create_res.has_value());
  EXPECT_GT(create_res.value(), 0);

  auto mark_res = run_coro(service_->mark_incomplete_runs_failed());
  ASSERT_TRUE(mark_res.has_value());
  EXPECT_GE(mark_res.value(), 1U);

  auto state = run_coro(service_->get_dag_run_state(run_id));
  ASSERT_TRUE(state.has_value());
  EXPECT_EQ(state.value(), DAGRunState::Failed);
}

TEST_F(PersistenceTest, ClaimTaskInstances_ClaimsPendingRowsAsRunning) {
  auto persisted = create_persisted_dag("claim_dag", "claim_task");
  ASSERT_GT(persisted.dag_rowid, 0);
  ASSERT_GT(persisted.task_rowid, 0);

  const DAGRunId run_id{unique_token("claim_run")};
  auto run = create_run(run_id, persisted.dag_rowid);
  ASSERT_TRUE(run.has_value());

  TaskInstanceInfo ti{};
  ti.task_rowid = persisted.task_rowid;
  ti.attempt = 1;
  ti.state = TaskState::Pending;

  auto create_res = run_coro(service_->create_run_with_task_instances(
      std::move(*run), std::vector<TaskInstanceInfo>{ti}));
  ASSERT_TRUE(create_res.has_value());

  auto claimed = run_coro(service_->claim_task_instances(8, "worker-a"));
  ASSERT_TRUE(claimed.has_value());
  ASSERT_FALSE(claimed->empty());

  const auto it =
      std::ranges::find_if(*claimed, [&](const ClaimedTaskInstance &c) {
        return c.dag_run_id == run_id && c.task_rowid == persisted.task_rowid;
      });
  ASSERT_NE(it, claimed->end());
  EXPECT_EQ(it->attempt, 1);

  auto tasks = run_coro(service_->get_task_instances(run_id));
  ASSERT_TRUE(tasks.has_value());
  ASSERT_FALSE(tasks->empty());
  EXPECT_EQ(tasks->front().state, TaskState::Running);

  auto hb =
      run_coro(service_->touch_task_heartbeat(run_id, persisted.task_rowid, 1));
  EXPECT_TRUE(hb.has_value());
}

TEST_F(PersistenceTest,
       ReapZombieTaskInstances_MarksExpiredRunningTasksFailed) {
  auto persisted = create_persisted_dag("zombie_dag", "zombie_task");
  ASSERT_GT(persisted.dag_rowid, 0);
  ASSERT_GT(persisted.task_rowid, 0);

  const DAGRunId run_id{unique_token("zombie_run")};
  auto run = create_run(run_id, persisted.dag_rowid);
  ASSERT_TRUE(run.has_value());

  TaskInstanceInfo ti{};
  ti.task_rowid = persisted.task_rowid;
  ti.attempt = 1;
  ti.state = TaskState::Running;
  ti.started_at = std::chrono::system_clock::now();

  auto create_res = run_coro(service_->create_run_with_task_instances(
      std::move(*run), std::vector<TaskInstanceInfo>{ti}));
  ASSERT_TRUE(create_res.has_value());

  std::this_thread::sleep_for(std::chrono::milliseconds(5));
  auto reaped = run_coro(service_->reap_zombie_task_instances(1));
  ASSERT_TRUE(reaped.has_value());
  EXPECT_GE(*reaped, 1U);

  auto tasks = run_coro(service_->get_task_instances(run_id));
  ASSERT_TRUE(tasks.has_value());
  ASSERT_FALSE(tasks->empty());
  EXPECT_EQ(tasks->front().state, TaskState::Failed);
}

TEST_F(PersistenceTest, GetRunHistory_FindsOlderRunBeyondListWindow) {
  auto persisted = create_persisted_dag("history_dag", "history_task");
  ASSERT_GT(persisted.dag_rowid, 0);
  ASSERT_GT(persisted.task_rowid, 0);

  constexpr int kRuns = 1005;
  const auto base = std::chrono::system_clock::now() - std::chrono::hours(1);
  DAGRunId oldest_id{"uninitialized"};

  for (int i = 0; i < kRuns; ++i) {
    const DAGRunId run_id{unique_token("history_run")};
    if (i == 0) {
      oldest_id = run_id;
    }

    auto run = create_run(run_id, persisted.dag_rowid);
    ASSERT_TRUE(run.has_value());
    const auto ts = base + std::chrono::milliseconds(i);
    run->set_scheduled_at(ts);
    run->set_started_at(ts);
    run->set_finished_at(ts);
    run->set_execution_date(ts);

    TaskInstanceInfo ti{};
    ti.task_rowid = persisted.task_rowid;
    ti.attempt = 1;
    ti.state = TaskState::Success;
    ti.started_at = ts;
    ti.finished_at = ts;

    auto create_res = run_coro(service_->create_run_with_task_instances(
        std::move(*run), std::vector<TaskInstanceInfo>{ti}));
    ASSERT_TRUE(create_res.has_value());
  }

  auto history = run_coro(service_->get_run_history(oldest_id));
  ASSERT_TRUE(history.has_value());
  EXPECT_EQ(history->dag_run_id, oldest_id);
}

TEST_F(PersistenceTest, MarkIncompleteRunsFailed_DoesNotFailPendingTasks) {
  auto persisted =
      create_persisted_dag("recovery_mix_dag", "recovery_mix_task");
  ASSERT_GT(persisted.dag_rowid, 0);
  ASSERT_GT(persisted.task_rowid, 0);

  auto second_task = TaskConfig::builder()
                         .id(unique_token("recovery_mix_task2"))
                         .name("recovery_mix_task2")
                         .command("echo ok")
                         .build();
  ASSERT_TRUE(second_task.has_value());
  auto second_task_rowid =
      run_coro(service_->save_task(persisted.dag_id, *second_task));
  ASSERT_TRUE(second_task_rowid.has_value());

  const DAGRunId run_id{unique_token("recovery_mix_run")};
  auto run = create_run(run_id, persisted.dag_rowid);
  ASSERT_TRUE(run.has_value());

  const auto now = std::chrono::system_clock::now();
  TaskInstanceInfo running_ti{};
  running_ti.task_rowid = persisted.task_rowid;
  running_ti.attempt = 1;
  running_ti.state = TaskState::Running;
  running_ti.started_at = now;

  TaskInstanceInfo pending_ti{};
  pending_ti.task_rowid = *second_task_rowid;
  pending_ti.attempt = 0;
  pending_ti.state = TaskState::Pending;

  auto create_res = run_coro(service_->create_run_with_task_instances(
      std::move(*run), std::vector<TaskInstanceInfo>{running_ti, pending_ti}));
  ASSERT_TRUE(create_res.has_value());

  auto mark_res = run_coro(service_->mark_incomplete_runs_failed());
  ASSERT_TRUE(mark_res.has_value());

  auto tasks = run_coro(service_->get_task_instances(run_id));
  ASSERT_TRUE(tasks.has_value());
  ASSERT_EQ(tasks->size(), 2U);

  auto find_by_rowid = [&](int64_t rowid) {
    return std::ranges::find_if(*tasks, [&](const TaskInstanceInfo &ti) {
      return ti.task_rowid == rowid;
    });
  };

  auto running_it = find_by_rowid(persisted.task_rowid);
  ASSERT_NE(running_it, tasks->end());
  EXPECT_EQ(running_it->state, TaskState::Failed);

  auto pending_it = find_by_rowid(*second_task_rowid);
  ASSERT_NE(pending_it, tasks->end());
  EXPECT_EQ(pending_it->state, TaskState::Pending);
}

TEST(PersistenceOpenTest, OpenWithInvalidHostFailsGracefully) {
  Runtime runtime(1);
  ASSERT_TRUE(runtime.start().has_value());

  DatabaseConfig cfg = load_test_db_config();
  cfg.host = "invalid-host-for-dagforge-tests";
  cfg.connect_timeout = 1;

  PersistenceService service(runtime, cfg);
  auto open_res = run_coro(service.open());
  EXPECT_FALSE(open_res.has_value());

  runtime.stop();
}
