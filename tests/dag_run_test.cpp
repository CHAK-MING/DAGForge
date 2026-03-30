#include "dagforge/core/error.hpp"
#include "dagforge/dag/dag.hpp"
#include "dagforge/dag/dag_run.hpp"
#include "dagforge/util/id.hpp"

#include "gtest/gtest.h"

#include <memory_resource>

using namespace dagforge;

class DAGRunTest : public ::testing::Test {
protected:
  void SetUp() override {
    // dag_ is unique_ptr, but DAGRun needs shared_ptr.
    // We create a shared_ptr copy for the run.
    dag_ = std::make_unique<DAG>();
    auto shared_dag = std::make_shared<DAG>(*dag_);
    auto result = DAGRun::create(DAGRunId("test_run_id"), shared_dag);
    ASSERT_TRUE(result.has_value());
    dag_run_ = std::make_unique<DAGRun>(std::move(*result));
  }

  void TearDown() override {
    dag_run_.reset();
    dag_.reset();
  }

  std::unique_ptr<DAG> dag_;
  std::unique_ptr<DAGRun> dag_run_;
};

TEST_F(DAGRunTest, BasicConstruction) {
  DAG dag;
  auto run_result =
      DAGRun::create(DAGRunId("test_run_id"), std::make_shared<DAG>(dag));
  ASSERT_TRUE(run_result.has_value());
  auto &run = *run_result;

  EXPECT_EQ(run.id(), DAGRunId("test_run_id"));
  EXPECT_EQ(run.state(), DAGRunState::Running);
}

TEST_F(DAGRunTest, GetDag) {
  auto &dag_ref = dag_run_->dag();
  EXPECT_EQ(dag_ref.size(), dag_->size());
}

TEST_F(DAGRunTest, InitiallyComplete) { EXPECT_FALSE(dag_run_->is_complete()); }

TEST_F(DAGRunTest, InitiallyNotFailed) { EXPECT_FALSE(dag_run_->has_failed()); }

TEST_F(DAGRunTest, ReadyCountInitiallyZero) {
  EXPECT_EQ(dag_run_->ready_count(), 0);
}

TEST_F(DAGRunTest, GetReadyTasksEmpty) {
  auto tasks = dag_run_->get_ready_tasks();
  EXPECT_TRUE(tasks.empty());
}

TEST_F(DAGRunTest, SetScheduledAt) {
  auto now = std::chrono::system_clock::now();
  dag_run_->set_scheduled_at(now);
  EXPECT_EQ(dag_run_->scheduled_at(), now);
}

TEST_F(DAGRunTest, SetStartedAt) {
  auto now = std::chrono::system_clock::now();
  dag_run_->set_started_at(now);
  EXPECT_EQ(dag_run_->started_at(), now);
}

TEST_F(DAGRunTest, SetFinishedAt) {
  auto now = std::chrono::system_clock::now();
  dag_run_->set_finished_at(now);
  EXPECT_EQ(dag_run_->finished_at(), now);
}

TEST(DAGRunRowidTest, SetRunRowidPropagatesToTaskInfos) {
  DAG dag;
  ASSERT_TRUE(
      dag.add_node(TaskId{"task1"}, TriggerRule::AllSuccess).has_value());
  auto run_result =
      DAGRun::create(DAGRunId("rowid_run"), std::make_shared<DAG>(dag));
  ASSERT_TRUE(run_result.has_value());

  auto &run = *run_result;
  run.set_run_rowid(42);

  auto infos = run.all_task_info();
  ASSERT_EQ(infos.size(), 1);
  EXPECT_EQ(infos[0].run_rowid, 42);
}

TEST_F(DAGRunTest, TriggerTypeDefault) {
  EXPECT_EQ(dag_run_->trigger_type(), TriggerType::Manual);
}

TEST_F(DAGRunTest, SetTriggerType) {
  dag_run_->set_trigger_type(TriggerType::Schedule);
  EXPECT_EQ(dag_run_->trigger_type(), TriggerType::Schedule);
}

TEST_F(DAGRunTest, MarkTaskStartedNonExistent) {
  EXPECT_FALSE(dag_run_->mark_task_started(999, InstanceId("instance_id")));
}

TEST_F(DAGRunTest, MarkTaskCompletedNonExistent) {
  EXPECT_FALSE(dag_run_->mark_task_completed(999, 0));
}

TEST_F(DAGRunTest, MarkTaskFailedNonExistent) {
  EXPECT_FALSE(dag_run_->mark_task_failed(999, "error", 3));
}

TEST_F(DAGRunTest, SetInstanceIdNonExistent) {
  EXPECT_FALSE(dag_run_->set_instance_id(999, InstanceId("instance_id")));
}

TEST_F(DAGRunTest, GetTaskInfoNonExistent) {
  auto info = dag_run_->get_task_info(999);
  EXPECT_FALSE(info.has_value());
  EXPECT_EQ(info.error(), make_error_code(Error::NotFound));
}

TEST_F(DAGRunTest, AllTaskInfoEmpty) {
  auto infos = dag_run_->all_task_info();
  EXPECT_TRUE(infos.empty());
}

TEST_F(DAGRunTest, StateTransitions) {
  EXPECT_EQ(dag_run_->state(), DAGRunState::Running);

  EXPECT_FALSE(dag_run_->is_complete());
  EXPECT_FALSE(dag_run_->has_failed());
}

TEST_F(DAGRunTest, TimePoints) {
  auto scheduled = dag_run_->scheduled_at();
  auto started = dag_run_->started_at();
  auto finished = dag_run_->finished_at();

  EXPECT_EQ(scheduled, std::chrono::system_clock::time_point{});
  EXPECT_EQ(started, std::chrono::system_clock::time_point{});
  EXPECT_EQ(finished, std::chrono::system_clock::time_point{});
}

TEST_F(DAGRunTest, WithTasks) {
  DAG dag;
  ASSERT_TRUE(dag.add_node(TaskId("task1")).has_value());
  ASSERT_TRUE(dag.add_node(TaskId("task2")).has_value());
  ASSERT_TRUE(dag.add_edge(TaskId("task1"), TaskId("task2")).has_value());

  auto run_result =
      DAGRun::create(DAGRunId("run_with_tasks"), std::make_shared<DAG>(dag));
  ASSERT_TRUE(run_result.has_value());
  auto &run = *run_result;

  EXPECT_EQ(run.ready_count(), 1);

  auto ready = run.get_ready_tasks();
  EXPECT_EQ(ready.size(), 1);
  auto first_info = run.get_task_info(ready[0]);
  ASSERT_TRUE(first_info.has_value());
  EXPECT_EQ(first_info->state, TaskState::Ready);
}

TEST_F(DAGRunTest, TaskLifecycleStartToComplete) {
  DAG dag;
  auto idx_result = dag.add_node(TaskId("task1"));
  ASSERT_TRUE(idx_result.has_value());
  auto idx = *idx_result;

  auto run_result =
      DAGRun::create(DAGRunId("lifecycle_test"), std::make_shared<DAG>(dag));
  ASSERT_TRUE(run_result.has_value());
  auto &run = *run_result;

  // Initially ready
  EXPECT_EQ(run.ready_count(), 1);
  EXPECT_FALSE(run.is_complete());

  // Start task
  ASSERT_TRUE(run.mark_task_started(idx, InstanceId("inst1")));
  EXPECT_EQ(run.ready_count(), 0);
  auto info = run.get_task_info(idx);
  ASSERT_TRUE(info.has_value());
  EXPECT_EQ(info->state, TaskState::Running);

  // Complete task
  ASSERT_TRUE(run.mark_task_completed(idx, 0));
  info = run.get_task_info(idx);
  ASSERT_TRUE(info.has_value());
  EXPECT_EQ(info->state, TaskState::Success);
  EXPECT_TRUE(run.is_complete());
  EXPECT_EQ(run.state(), DAGRunState::Success);
}

TEST_F(DAGRunTest, TaskStartUsesProvidedTimestamp) {
  DAG dag;
  auto idx_result = dag.add_node(TaskId("task1"));
  ASSERT_TRUE(idx_result.has_value());
  auto idx = *idx_result;

  auto run_result = DAGRun::create(DAGRunId("lifecycle_timestamp_test"),
                                   std::make_shared<DAG>(dag));
  ASSERT_TRUE(run_result.has_value());
  auto &run = *run_result;

  const auto started_at =
      std::chrono::system_clock::time_point{std::chrono::milliseconds{1234}};
  ASSERT_TRUE(run.mark_task_started(idx, InstanceId("inst1"), started_at));

  auto info = run.get_task_info(idx);
  ASSERT_TRUE(info.has_value());
  EXPECT_EQ(info->state, TaskState::Running);
  EXPECT_EQ(info->started_at, started_at);
}

TEST_F(DAGRunTest, TaskFailureWithRetry) {
  DAG dag;
  auto idx_result = dag.add_node(TaskId("task1"));
  ASSERT_TRUE(idx_result.has_value());
  auto idx = *idx_result;

  auto run_result =
      DAGRun::create(DAGRunId("retry_test"), std::make_shared<DAG>(dag));
  ASSERT_TRUE(run_result.has_value());
  auto &run = *run_result;

  // Start and fail with retries remaining
  ASSERT_TRUE(run.mark_task_started(idx, InstanceId("inst1")));
  ASSERT_TRUE(
      run.mark_task_failed(idx, "error", 3)); // max_retries=3, attempt=1

  // Task should enter retry-wait state and not be immediately runnable
  auto info = run.get_task_info(idx);
  ASSERT_TRUE(info.has_value());
  EXPECT_EQ(info->state, TaskState::Retrying);
  EXPECT_EQ(run.ready_count(), 0);

  // Retry becomes runnable only when the scheduler arms it
  ASSERT_TRUE(run.mark_task_retry_ready(idx));
  info = run.get_task_info(idx);
  ASSERT_TRUE(info.has_value());
  EXPECT_EQ(info->state, TaskState::Ready);
  EXPECT_EQ(run.ready_count(), 1);
  EXPECT_FALSE(run.is_complete());
}

TEST_F(DAGRunTest, SeparatesPendingFromReadyStates) {
  DAG dag;
  auto root = dag.add_node(TaskId("root"));
  auto leaf = dag.add_node(TaskId("leaf"));
  ASSERT_TRUE(root.has_value());
  ASSERT_TRUE(leaf.has_value());
  ASSERT_TRUE(dag.add_edge(*root, *leaf).has_value());

  auto run_result =
      DAGRun::create(DAGRunId("pending_vs_ready"), std::make_shared<DAG>(dag));
  ASSERT_TRUE(run_result.has_value());
  auto &run = *run_result;

  auto root_info = run.get_task_info(*root);
  auto leaf_info = run.get_task_info(*leaf);
  ASSERT_TRUE(root_info.has_value());
  ASSERT_TRUE(leaf_info.has_value());

  EXPECT_EQ(root_info->state, TaskState::Ready);
  EXPECT_EQ(leaf_info->state, TaskState::Pending);
}

TEST_F(DAGRunTest, TaskFailureExhaustedRetries) {
  DAG dag;
  auto idx_result = dag.add_node(TaskId("task1"));
  ASSERT_TRUE(idx_result.has_value());
  auto idx = *idx_result;

  auto run_result =
      DAGRun::create(DAGRunId("exhaust_retry"), std::make_shared<DAG>(dag));
  ASSERT_TRUE(run_result.has_value());
  auto &run = *run_result;

  // Exhaust all retries: with max_retries=3, the 4th failed attempt is
  // terminal.
  for (int i = 0; i < 4; ++i) {
    ASSERT_TRUE(
        run.mark_task_started(idx, InstanceId("inst" + std::to_string(i))));
    ASSERT_TRUE(run.mark_task_failed(idx, "error", 3));
    if (i < 3) {
      ASSERT_TRUE(run.mark_task_retry_ready(idx));
    }
  }

  auto info = run.get_task_info(idx);
  ASSERT_TRUE(info.has_value());
  EXPECT_EQ(info->state, TaskState::Failed);
  EXPECT_TRUE(run.is_complete());
  EXPECT_TRUE(run.has_failed());
  EXPECT_EQ(run.state(), DAGRunState::Failed);
}

TEST_F(DAGRunTest, DownstreamUpstreamFailedWhenUpstreamFails) {
  DAG dag;
  auto idx1 = dag.add_node(TaskId("task1"));
  auto idx2 = dag.add_node(TaskId("task2"));
  ASSERT_TRUE(idx1.has_value());
  ASSERT_TRUE(idx2.has_value());
  ASSERT_TRUE(dag.add_edge(*idx1, *idx2).has_value());

  auto run_result =
      DAGRun::create(DAGRunId("downstream_fail"), std::make_shared<DAG>(dag));
  ASSERT_TRUE(run_result.has_value());
  auto &run = *run_result;

  ASSERT_TRUE(run.mark_task_started(*idx1, InstanceId("inst1")));
  ASSERT_TRUE(run.mark_task_failed(*idx1, "error", 0));

  auto info2 = run.get_task_info(*idx2);
  ASSERT_TRUE(info2.has_value());
  EXPECT_EQ(info2->state, TaskState::UpstreamFailed);
}

TEST_F(DAGRunTest, ComplexDAGReadyTasks) {
  //   task1 ---> task3
  //   task2 --/
  DAG dag;
  auto idx1 = dag.add_node(TaskId("task1"));
  auto idx2 = dag.add_node(TaskId("task2"));
  auto idx3 = dag.add_node(TaskId("task3"));
  ASSERT_TRUE(idx1.has_value());
  ASSERT_TRUE(idx2.has_value());
  ASSERT_TRUE(idx3.has_value());
  ASSERT_TRUE(dag.add_edge(*idx1, *idx3).has_value());
  ASSERT_TRUE(dag.add_edge(*idx2, *idx3).has_value());

  auto run_result =
      DAGRun::create(DAGRunId("complex_dag"), std::make_shared<DAG>(dag));
  ASSERT_TRUE(run_result.has_value());
  auto &run = *run_result;

  // task1 and task2 should be ready initially
  EXPECT_EQ(run.ready_count(), 2);

  // Complete task1
  ASSERT_TRUE(run.mark_task_started(*idx1, InstanceId("inst1")));
  ASSERT_TRUE(run.mark_task_completed(*idx1, 0));

  // task3 should still not be ready (waiting for task2)
  EXPECT_EQ(run.ready_count(), 1);
  auto ready = run.get_ready_tasks();
  EXPECT_EQ(ready.size(), 1);
  EXPECT_EQ(ready[0], *idx2);

  // Complete task2
  ASSERT_TRUE(run.mark_task_started(*idx2, InstanceId("inst2")));
  ASSERT_TRUE(run.mark_task_completed(*idx2, 0));

  // Now task3 should be ready
  EXPECT_EQ(run.ready_count(), 1);
  ready = run.get_ready_tasks();
  EXPECT_EQ(ready.size(), 1);
  EXPECT_EQ(ready[0], *idx3);
}

TEST_F(DAGRunTest, TriggerTypeToString) {
  EXPECT_EQ(to_string_view(TriggerType::Manual), "manual");
  EXPECT_EQ(to_string_view(TriggerType::Schedule), "schedule");
}

TEST_F(DAGRunTest, StringToTriggerType) {
  EXPECT_EQ(parse<TriggerType>("manual"), TriggerType::Manual);
  EXPECT_EQ(parse<TriggerType>("schedule"), TriggerType::Schedule);
  EXPECT_EQ(parse<TriggerType>("unknown"), TriggerType::Manual);
}

TEST_F(DAGRunTest, RestoreTaskInstanceCascadesThroughPendingDependents) {
  DAG dag;
  auto root = dag.add_node(TaskId("root"));
  auto mid = dag.add_node(TaskId("mid"));
  auto leaf = dag.add_node(TaskId("leaf"));
  ASSERT_TRUE(root.has_value());
  ASSERT_TRUE(mid.has_value());
  ASSERT_TRUE(leaf.has_value());
  ASSERT_TRUE(dag.add_edge(*root, *mid).has_value());
  ASSERT_TRUE(dag.add_edge(*mid, *leaf).has_value());

  auto run_result =
      DAGRun::create(DAGRunId("restore_cascade"), std::make_shared<DAG>(dag));
  ASSERT_TRUE(run_result.has_value());
  auto &run = *run_result;

  auto root_info = run.get_task_info(*root);
  ASSERT_TRUE(root_info.has_value());
  root_info->state = TaskState::Failed;
  root_info->finished_at = std::chrono::system_clock::now();

  ASSERT_TRUE(run.restore_task_instance(*root_info));

  auto mid_info = run.get_task_info(*mid);
  auto leaf_info = run.get_task_info(*leaf);
  ASSERT_TRUE(mid_info.has_value());
  ASSERT_TRUE(leaf_info.has_value());
  EXPECT_EQ(mid_info->state, TaskState::UpstreamFailed);
  EXPECT_EQ(leaf_info->state, TaskState::UpstreamFailed);
}

TEST_F(DAGRunTest, ReadyTasksPreserveActivationOrder) {
  DAG dag;
  auto first = dag.add_node(TaskId("first"));
  auto second = dag.add_node(TaskId("second"));
  ASSERT_TRUE(first.has_value());
  ASSERT_TRUE(second.has_value());

  auto run_result =
      DAGRun::create(DAGRunId("ready_order"), std::make_shared<DAG>(dag));
  ASSERT_TRUE(run_result.has_value());
  auto &run = *run_result;

  ASSERT_TRUE(run.mark_task_started(*first, InstanceId("inst-first")));
  ASSERT_TRUE(run.mark_task_failed(*first, "error", 2));
  ASSERT_TRUE(run.mark_task_started(*second, InstanceId("inst-second")));
  ASSERT_TRUE(run.mark_task_failed(*second, "error", 2));

  ASSERT_TRUE(run.mark_task_retry_ready(*second));
  ASSERT_TRUE(run.mark_task_retry_ready(*first));

  auto ready = run.get_ready_tasks();
  ASSERT_EQ(ready.size(), 2);
  EXPECT_EQ(ready[0], *second);
  EXPECT_EQ(ready[1], *first);
}

TEST_F(DAGRunTest, ReadyTaskStreamMatchesReadyOrder) {
  DAG dag;
  auto first = dag.add_node(TaskId("first"));
  auto second = dag.add_node(TaskId("second"));
  ASSERT_TRUE(first.has_value());
  ASSERT_TRUE(second.has_value());

  auto run_result = DAGRun::create(DAGRunId("ready_stream_order"),
                                   std::make_shared<DAG>(dag));
  ASSERT_TRUE(run_result.has_value());
  auto &run = *run_result;

  ASSERT_TRUE(run.mark_task_started(*first, InstanceId("inst-first")));
  ASSERT_TRUE(run.mark_task_failed(*first, "error", 1));
  ASSERT_TRUE(run.mark_task_started(*second, InstanceId("inst-second")));
  ASSERT_TRUE(run.mark_task_failed(*second, "error", 1));
  ASSERT_TRUE(run.mark_task_retry_ready(*second));
  ASSERT_TRUE(run.mark_task_retry_ready(*first));

  pmr::monotonic_buffer_resource resource;
  pmr::vector<NodeIndex> from_copy{&resource};
  run.copy_ready_tasks(from_copy);
  auto ready = run.get_ready_tasks();
  ASSERT_EQ(from_copy.size(), ready.size());
  EXPECT_EQ(from_copy[0], ready[0]);
  EXPECT_EQ(from_copy[1], ready[1]);
}

TEST_F(DAGRunTest, CompletionDeltaCapturesNewlyReadyDownstreamTasks) {
  DAG dag;
  auto root = dag.add_node(TaskId("root"));
  auto leaf = dag.add_node(TaskId("leaf"));
  ASSERT_TRUE(root.has_value());
  ASSERT_TRUE(leaf.has_value());
  ASSERT_TRUE(dag.add_edge(*root, *leaf).has_value());

  auto run_result =
      DAGRun::create(DAGRunId("completion_delta"), std::make_shared<DAG>(dag));
  ASSERT_TRUE(run_result.has_value());
  auto &run = *run_result;

  ASSERT_TRUE(run.mark_task_started(*root, InstanceId("inst-root")));
  auto delta = run.mark_task_completed(*root, 0);
  ASSERT_TRUE(delta.has_value());

  ASSERT_EQ(delta->ready_tasks.size(), 1U);
  EXPECT_EQ(delta->ready_tasks.front(), *leaf);
  EXPECT_TRUE(delta->terminal_tasks.empty());
}

TEST_F(DAGRunTest, FailureDeltaCapturesPropagatedTerminalTasks) {
  DAG dag;
  auto root = dag.add_node(TaskId("root"));
  auto mid = dag.add_node(TaskId("mid"));
  auto leaf = dag.add_node(TaskId("leaf"));
  ASSERT_TRUE(root.has_value());
  ASSERT_TRUE(mid.has_value());
  ASSERT_TRUE(leaf.has_value());
  ASSERT_TRUE(dag.add_edge(*root, *mid).has_value());
  ASSERT_TRUE(dag.add_edge(*mid, *leaf).has_value());

  auto run_result =
      DAGRun::create(DAGRunId("failure_delta"), std::make_shared<DAG>(dag));
  ASSERT_TRUE(run_result.has_value());
  auto &run = *run_result;

  ASSERT_TRUE(run.mark_task_started(*root, InstanceId("inst-root")));
  auto delta = run.mark_task_failed(*root, "boom", 0);
  ASSERT_TRUE(delta.has_value());

  ASSERT_EQ(delta->terminal_tasks.size(), 2U);
  EXPECT_EQ(delta->terminal_tasks[0], *mid);
  EXPECT_EQ(delta->terminal_tasks[1], *leaf);
  EXPECT_TRUE(delta->ready_tasks.empty());

  auto mid_info = run.get_task_info(*mid);
  auto leaf_info = run.get_task_info(*leaf);
  ASSERT_TRUE(mid_info.has_value());
  ASSERT_TRUE(leaf_info.has_value());
  EXPECT_EQ(mid_info->state, TaskState::UpstreamFailed);
  EXPECT_EQ(leaf_info->state, TaskState::UpstreamFailed);
}

TEST_F(DAGRunTest, RetryReadyDeltaCapturesReactivatedTask) {
  DAG dag;
  auto idx = dag.add_node(TaskId("retryable"));
  ASSERT_TRUE(idx.has_value());

  auto run_result =
      DAGRun::create(DAGRunId("retry_ready_delta"), std::make_shared<DAG>(dag));
  ASSERT_TRUE(run_result.has_value());
  auto &run = *run_result;

  ASSERT_TRUE(run.mark_task_started(*idx, InstanceId("inst-retry")));
  ASSERT_TRUE(run.mark_task_failed(*idx, "retry", 1));

  auto delta = run.mark_task_retry_ready(*idx);
  ASSERT_TRUE(delta.has_value());
  ASSERT_EQ(delta->ready_tasks.size(), 1U);
  EXPECT_EQ(delta->ready_tasks.front(), *idx);
  EXPECT_TRUE(delta->terminal_tasks.empty());
}
