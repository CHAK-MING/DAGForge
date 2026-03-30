#include "dagforge/app/services/execution_event_bridge.hpp"
#include "dagforge/app/services/execution_service.hpp"
#include "dagforge/app/services/scheduler_service.hpp"

#include "gtest/gtest.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>

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
  executor_configs.emplace_back(ShellExecutorConfig{.env = {}});

  std::vector<TaskConfig::Compiled> task_configs;
  task_configs.emplace_back(task_cfg.compiled());

  return ok(ExecutionService::RunContext{
      .run = std::move(run),
      .executor_configs = std::make_shared<const std::vector<ExecutorConfig>>(
          std::move(executor_configs)),
      .task_configs = std::make_shared<const std::vector<TaskConfig::Compiled>>(
          std::move(task_configs)),
      .dag_id = DAGId{"bridge_test_dag"}});
}

} // namespace

TEST(ExecutionEventBridgeTest, IncompleteDependenciesLeaveCallbacksUnwired) {
  Runtime runtime(1);
  ASSERT_TRUE(runtime.start().has_value());

  ImmediateSuccessExecutor executor;
  ExecutionService execution(runtime, executor);
  SchedulerService scheduler(runtime, 1);

  std::atomic<int> run_status_calls{0};
  std::atomic<int> run_completed_calls{0};
  std::atomic<std::uint64_t> dropped{0};
  std::atomic<std::uint64_t> writes{0};

  ExecutionEventBridge bridge({.runtime = &runtime,
                               .execution = &execution,
                               .scheduler = nullptr,
                               .persistence = nullptr,
                               .api_server = {},
                               .resolve_dag_id = {},
                               .on_run_status =
                                   [&](const DAGRunId &, DAGRunState) {
                                     run_status_calls.fetch_add(
                                         1, std::memory_order_acq_rel);
                                   },
                               .on_run_completed =
                                   [&](const DAGRunId &, const DAGRun &) {
                                     run_completed_calls.fetch_add(
                                         1, std::memory_order_acq_rel);
                                   },
                               .get_max_retries = {},
                               .get_retry_interval = {},
                               .on_scheduler_trigger = {},
                               .dropped_persistence_events = &dropped,
                               .mysql_batch_write_ops = &writes});
  bridge.wire();

  const DAGRunId dag_run_id{"bridge_incomplete_run"};
  auto ctx = make_single_task_run_context(dag_run_id);
  ASSERT_TRUE(ctx.has_value());
  execution.start_run(dag_run_id, std::move(*ctx));
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  runtime.stop();

  EXPECT_EQ(run_status_calls.load(std::memory_order_acquire), 0);
  EXPECT_EQ(run_completed_calls.load(std::memory_order_acquire), 0);
}

TEST(ExecutionEventBridgeTest, CompleteDependenciesForwardRunCompletionCallbacks) {
  Runtime runtime(1);
  ASSERT_TRUE(runtime.start().has_value());

  ImmediateSuccessExecutor executor;
  ExecutionService execution(runtime, executor);
  SchedulerService scheduler(runtime, 1);

  std::mutex mu;
  std::condition_variable cv;
  int run_status_calls = 0;
  int run_completed_calls = 0;
  std::atomic<std::uint64_t> dropped{0};
  std::atomic<std::uint64_t> writes{0};

  ExecutionEventBridge bridge(
      {.runtime = &runtime,
       .execution = &execution,
       .scheduler = &scheduler,
       .persistence = nullptr,
       .api_server = []() -> ApiServer * { return nullptr; },
       .resolve_dag_id = [](const DAGRunId &) -> std::optional<DAGId> {
         return DAGId{"bridge_test_dag"};
       },
       .on_run_status =
           [&](const DAGRunId &, DAGRunState) {
             std::scoped_lock lock(mu);
             ++run_status_calls;
             cv.notify_all();
           },
       .on_run_completed =
           [&](const DAGRunId &, const DAGRun &) {
             std::scoped_lock lock(mu);
             ++run_completed_calls;
             cv.notify_all();
           },
       .get_max_retries = [](const DAGRunId &, NodeIndex) { return 0; },
       .get_retry_interval = [](const DAGRunId &, NodeIndex) {
         return std::chrono::seconds(0);
       },
       .on_scheduler_trigger = [](const DAGId &,
                                  std::chrono::system_clock::time_point) {},
       .dropped_persistence_events = &dropped,
       .mysql_batch_write_ops = &writes});
  bridge.wire();

  const DAGRunId dag_run_id{"bridge_complete_run"};
  auto ctx = make_single_task_run_context(dag_run_id);
  ASSERT_TRUE(ctx.has_value());
  execution.start_run(dag_run_id, std::move(*ctx));

  {
    std::unique_lock lock(mu);
    ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(2), [&] {
      return run_status_calls == 1 && run_completed_calls == 1;
    }));
  }

  runtime.stop();

  EXPECT_EQ(run_status_calls, 1);
  EXPECT_EQ(run_completed_calls, 1);
}
