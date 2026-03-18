#include "dagforge/app/services/execution_service.hpp"

#include "gtest/gtest.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>

using namespace dagforge;

namespace {

class ImmediateSuccessExecutor final : public IExecutor {
public:
  auto start(ExecutorRequest req, ExecutionSink sink) -> Result<void> override {
    if (sink.on_complete) {
      ExecutorResult result;
      result.exit_code = 0;
      sink.on_complete(req.instance_id, std::move(result));
    }
    return ok();
  }

  auto cancel(const InstanceId &) -> void override {}
};

class ImmediateTimeoutExecutor final : public IExecutor {
public:
  auto start(ExecutorRequest req, ExecutionSink sink) -> Result<void> override {
    if (sink.on_complete) {
      ExecutorResult result;
      result.exit_code = kExitCodeTimeout;
      result.timed_out = true;
      result.error = "Execution timeout";
      sink.on_complete(req.instance_id, std::move(result));
    }
    return ok();
  }

  auto cancel(const InstanceId &) -> void override {}
};

auto make_single_task_run_context(const DAGRunId &dag_run_id)
    -> Result<ExecutionService::RunContext> {
  auto dag = std::make_shared<DAG>();
  if (auto r = dag->add_node(TaskId{"worker"}); !r) {
    return fail(r.error());
  }

  auto run_res = DAGRun::create(dag_run_id.clone(), dag);
  if (!run_res) {
    return fail(run_res.error());
  }

  auto run = std::make_unique<DAGRun>(std::move(*run_res));
  const auto now = std::chrono::system_clock::now();
  run->set_trigger_type(TriggerType::Manual);
  run->set_scheduled_at(now);
  run->set_started_at(now);
  run->set_execution_date(now);

  TaskConfig task_cfg;
  task_cfg.task_id = TaskId{"worker"};
  task_cfg.name = "worker";
  task_cfg.executor = ExecutorType::Shell;
  task_cfg.command = ":";

  std::vector<ExecutorConfig> executor_configs;
  executor_configs.emplace_back(ShellExecutorConfig{
      .command = ":",
      .working_dir = {},
      .execution_timeout = std::chrono::seconds(3600),
      .env = {},
  });

  std::vector<TaskConfig> task_configs;
  task_configs.emplace_back(std::move(task_cfg));

  return ok(ExecutionService::RunContext{
      .run = std::move(run),
      .executor_configs = std::make_shared<const std::vector<ExecutorConfig>>(
          std::move(executor_configs)),
      .task_configs = std::make_shared<const std::vector<TaskConfig>>(
          std::move(task_configs)),
      .dag_id = DAGId{"persist_test_dag"}});
}

} // namespace

TEST(ExecutionServiceTest, CompletedSingleTaskRunPersistsOnlyFinalRunSnapshot) {
  Runtime runtime(1);
  ASSERT_TRUE(runtime.start().has_value());

  ImmediateSuccessExecutor executor;
  ExecutionService execution(runtime, executor);

  std::mutex mu;
  std::condition_variable cv;
  std::vector<DAGRunState> persisted_states;
  bool completed = false;

  ExecutionCallbacks callbacks;
  callbacks.get_dag_id_by_run = [](const DAGRunId &) -> task<Result<DAGId>> {
    co_return ok(DAGId{"persist_test_dag"});
  };
  callbacks.get_max_retries = [](const DAGRunId &, NodeIndex) { return 0; };
  callbacks.get_retry_interval = [](const DAGRunId &, NodeIndex) {
    return std::chrono::seconds(0);
  };
  callbacks.on_persist_run = [&](std::shared_ptr<const DAGRun> run) {
    std::scoped_lock lock(mu);
    persisted_states.push_back(run->state());
  };
  callbacks.on_run_status = [&](const DAGRunId &, DAGRunState) {
    std::scoped_lock lock(mu);
    completed = true;
    cv.notify_one();
  };
  execution.set_callbacks(std::move(callbacks));

  const DAGRunId dag_run_id{"persist_test_run"};
  auto ctx = make_single_task_run_context(dag_run_id);
  ASSERT_TRUE(ctx.has_value());

  execution.start_run(dag_run_id, std::move(*ctx));

  {
    std::unique_lock lock(mu);
    ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(2),
                            [&] { return completed; }));
  }

  runtime.stop();

  ASSERT_EQ(persisted_states.size(), 1U);
  EXPECT_EQ(persisted_states.front(), DAGRunState::Success);
}

TEST(ExecutionServiceTest, TimeoutFailsRunWithoutRetrying) {
  Runtime runtime(1);
  ASSERT_TRUE(runtime.start().has_value());

  ImmediateTimeoutExecutor executor;
  ExecutionService execution(runtime, executor);

  std::mutex mu;
  std::condition_variable cv;
  std::vector<DAGRunState> persisted_states;
  std::vector<TaskState> persisted_task_states;
  bool completed = false;

  ExecutionCallbacks callbacks;
  callbacks.get_dag_id_by_run = [](const DAGRunId &) -> task<Result<DAGId>> {
    co_return ok(DAGId{"persist_test_dag"});
  };
  callbacks.get_max_retries = [](const DAGRunId &, NodeIndex) { return 3; };
  callbacks.get_retry_interval = [](const DAGRunId &, NodeIndex) {
    return std::chrono::seconds(0);
  };
  callbacks.on_persist_run = [&](std::shared_ptr<const DAGRun> run) {
    std::scoped_lock lock(mu);
    persisted_states.push_back(run->state());
  };
  callbacks.on_persist_task = [&](const DAGRunId &, const TaskInstanceInfo &info) {
    std::scoped_lock lock(mu);
    persisted_task_states.push_back(info.state);
  };
  callbacks.on_run_status = [&](const DAGRunId &, DAGRunState) {
    std::scoped_lock lock(mu);
    completed = true;
    cv.notify_one();
  };
  execution.set_callbacks(std::move(callbacks));

  const DAGRunId dag_run_id{"timeout_test_run"};
  auto ctx = make_single_task_run_context(dag_run_id);
  ASSERT_TRUE(ctx.has_value());

  execution.start_run(dag_run_id, std::move(*ctx));

  {
    std::unique_lock lock(mu);
    ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(2),
                            [&] { return completed; }));
  }

  runtime.stop();

  ASSERT_EQ(persisted_states.size(), 1U);
  EXPECT_EQ(persisted_states.front(), DAGRunState::Failed);
  ASSERT_FALSE(persisted_task_states.empty());
  EXPECT_EQ(persisted_task_states.back(), TaskState::Failed);
}
