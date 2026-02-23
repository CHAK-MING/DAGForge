// Parameterized tests for TriggerRule evaluation
// Tests: should_trigger() logic in DAGRun

#include "dagforge/config/task_config.hpp"
#include "dagforge/dag/dag.hpp"
#include "dagforge/dag/dag_run.hpp"
#include "dagforge/util/id.hpp"

#include <gtest/gtest.h>

#include <string>
#include <vector>

using namespace dagforge;

// ============================================================================
// Test Data Structures
// ============================================================================

// Represents an upstream task's final state for parameterized testing
enum class DepOutcome { Success, Failed, Skipped };

// Represents the expected downstream task state after trigger evaluation
enum class ExpectedResult { Ready, Skipped };

struct TriggerRuleTestCase {
  std::string name;
  TriggerRule rule;
  std::vector<DepOutcome> dep_outcomes;
  ExpectedResult expected;
};

auto operator<<(std::ostream &os, const TriggerRuleTestCase &tc)
    -> std::ostream & {
  os << tc.name;
  return os;
}

// ============================================================================
// Parameterized Test Fixture
// ============================================================================

class TriggerRuleTest : public ::testing::TestWithParam<TriggerRuleTestCase> {
protected:
  // Helper: apply outcome to upstream task
  void apply_outcome(DAGRun &run, NodeIndex idx, DepOutcome outcome) {
    switch (outcome) {
    case DepOutcome::Success:
      ASSERT_TRUE(run.mark_task_started(
          idx, InstanceId("inst_" + std::to_string(idx))));
      ASSERT_TRUE(run.mark_task_completed(idx, 0));
      break;
    case DepOutcome::Failed:
      ASSERT_TRUE(run.mark_task_started(
          idx, InstanceId("inst_" + std::to_string(idx))));
      ASSERT_TRUE(run.mark_task_failed(idx, "error", 0));
      break;
    case DepOutcome::Skipped:
      ASSERT_TRUE(run.mark_task_skipped(idx));
      break;
    }
  }
};

TEST_P(TriggerRuleTest, EvaluatesCorrectly) {
  const auto &tc = GetParam();

  // Arrange: build DAG with N upstream tasks -> 1 downstream
  DAG dag;
  std::vector<NodeIndex> upstream_indices;

  for (size_t i = 0; i < tc.dep_outcomes.size(); ++i) {
    auto idx = dag.add_node(TaskId("upstream_" + std::to_string(i)));
    ASSERT_TRUE(idx.has_value());
    upstream_indices.push_back(*idx);
  }

  auto downstream_idx = dag.add_node(TaskId("downstream"), tc.rule);
  ASSERT_TRUE(downstream_idx.has_value());
  for (auto idx : upstream_indices) {
    ASSERT_TRUE(dag.add_edge(idx, *downstream_idx).has_value());
  }

  auto run = DAGRun::create(DAGRunId("test"), std::make_shared<DAG>(dag));
  ASSERT_TRUE(run.has_value());

  // Act: apply outcomes to upstream tasks
  for (size_t i = 0; i < tc.dep_outcomes.size(); ++i) {
    apply_outcome(*run, upstream_indices[i], tc.dep_outcomes[i]);
  }

  // Assert: check downstream state
  if (tc.expected == ExpectedResult::Ready) {
    EXPECT_EQ(run->ready_count(), 1)
        << "Expected downstream to be ready for: " << tc.name;
    auto ready = run->get_ready_tasks();
    EXPECT_EQ(ready.size(), 1);
    if (!ready.empty()) {
      EXPECT_EQ(ready[0], *downstream_idx);
    }
  } else {
    auto info = run->get_task_info(*downstream_idx);
    ASSERT_TRUE(info.has_value()) << "Task info missing for: " << tc.name;

    const bool has_failed_dep = std::ranges::any_of(
        tc.dep_outcomes, [](DepOutcome o) { return o == DepOutcome::Failed; });
    const bool has_success_dep = std::ranges::any_of(
        tc.dep_outcomes, [](DepOutcome o) { return o == DepOutcome::Success; });
    const bool has_skipped_dep = std::ranges::any_of(
        tc.dep_outcomes, [](DepOutcome o) { return o == DepOutcome::Skipped; });
    const bool all_failed_dep =
        !tc.dep_outcomes.empty() && !has_success_dep && !has_skipped_dep;

    TaskState expected_state = TaskState::Skipped;
    if ((tc.rule == TriggerRule::AllSuccess && has_failed_dep) ||
        (tc.rule == TriggerRule::AllFailed && has_failed_dep &&
         !all_failed_dep) ||
        (tc.rule == TriggerRule::OneSuccess && !has_success_dep &&
         !has_skipped_dep) ||
        (tc.rule == TriggerRule::NoneFailed && has_failed_dep) ||
        (tc.rule == TriggerRule::AllDoneMinOneSuccess && !has_success_dep &&
         has_failed_dep) ||
        (tc.rule == TriggerRule::AllSkipped && has_failed_dep) ||
        (tc.rule == TriggerRule::NoneFailedMinOneSuccess && has_failed_dep)) {
      expected_state = TaskState::UpstreamFailed;
    }

    EXPECT_EQ(info->state, expected_state)
        << "Expected downstream state " << static_cast<int>(expected_state)
        << " for: " << tc.name;
  }
}

// ============================================================================
// Test Cases: AllSuccess
// ============================================================================

INSTANTIATE_TEST_SUITE_P(
    AllSuccess, TriggerRuleTest,
    ::testing::Values(
        TriggerRuleTestCase{"AllSuccess_AllDepsSucceed",
                            TriggerRule::AllSuccess,
                            {DepOutcome::Success, DepOutcome::Success},
                            ExpectedResult::Ready},
        TriggerRuleTestCase{"AllSuccess_OneDepFails",
                            TriggerRule::AllSuccess,
                            {DepOutcome::Success, DepOutcome::Failed},
                            ExpectedResult::Skipped},
        TriggerRuleTestCase{"AllSuccess_OneDepSkipped",
                            TriggerRule::AllSuccess,
                            {DepOutcome::Success, DepOutcome::Skipped},
                            ExpectedResult::Skipped},
        TriggerRuleTestCase{"AllSuccess_AllDepsFail",
                            TriggerRule::AllSuccess,
                            {DepOutcome::Failed, DepOutcome::Failed},
                            ExpectedResult::Skipped},
        TriggerRuleTestCase{"AllSuccess_SingleDepSucceeds",
                            TriggerRule::AllSuccess,
                            {DepOutcome::Success},
                            ExpectedResult::Ready},
        TriggerRuleTestCase{"AllSuccess_SingleDepFails",
                            TriggerRule::AllSuccess,
                            {DepOutcome::Failed},
                            ExpectedResult::Skipped}),
    [](const auto &i) { return i.param.name; });

// ============================================================================
// Test Cases: AllFailed
// ============================================================================

INSTANTIATE_TEST_SUITE_P(
    AllFailed, TriggerRuleTest,
    ::testing::Values(
        TriggerRuleTestCase{"AllFailed_AllDepsFail",
                            TriggerRule::AllFailed,
                            {DepOutcome::Failed, DepOutcome::Failed},
                            ExpectedResult::Ready},
        TriggerRuleTestCase{"AllFailed_OneDepSucceeds",
                            TriggerRule::AllFailed,
                            {DepOutcome::Failed, DepOutcome::Success},
                            ExpectedResult::Skipped},
        TriggerRuleTestCase{"AllFailed_OneDepSkipped",
                            TriggerRule::AllFailed,
                            {DepOutcome::Failed, DepOutcome::Skipped},
                            ExpectedResult::Skipped},
        TriggerRuleTestCase{"AllFailed_AllDepsSucceed",
                            TriggerRule::AllFailed,
                            {DepOutcome::Success, DepOutcome::Success},
                            ExpectedResult::Skipped},
        TriggerRuleTestCase{"AllFailed_SingleDepFails",
                            TriggerRule::AllFailed,
                            {DepOutcome::Failed},
                            ExpectedResult::Ready}),
    [](const auto &i) { return i.param.name; });

// ============================================================================
// Test Cases: AllDone
// ============================================================================

INSTANTIATE_TEST_SUITE_P(
    AllDone, TriggerRuleTest,
    ::testing::Values(
        TriggerRuleTestCase{"AllDone_AllDepsSucceed",
                            TriggerRule::AllDone,
                            {DepOutcome::Success, DepOutcome::Success},
                            ExpectedResult::Ready},
        TriggerRuleTestCase{"AllDone_AllDepsFail",
                            TriggerRule::AllDone,
                            {DepOutcome::Failed, DepOutcome::Failed},
                            ExpectedResult::Ready},
        TriggerRuleTestCase{"AllDone_MixedSuccessAndFailed",
                            TriggerRule::AllDone,
                            {DepOutcome::Success, DepOutcome::Failed},
                            ExpectedResult::Ready},
        TriggerRuleTestCase{"AllDone_WithSkipped",
                            TriggerRule::AllDone,
                            {DepOutcome::Success, DepOutcome::Skipped},
                            ExpectedResult::Ready},
        TriggerRuleTestCase{"AllDone_AllSkipped",
                            TriggerRule::AllDone,
                            {DepOutcome::Skipped, DepOutcome::Skipped},
                            ExpectedResult::Ready}),
    [](const auto &i) { return i.param.name; });

// ============================================================================
// Test Cases: OneSuccess
// ============================================================================

INSTANTIATE_TEST_SUITE_P(
    OneSuccess, TriggerRuleTest,
    ::testing::Values(
        TriggerRuleTestCase{"OneSuccess_OneSucceedsOneFails",
                            TriggerRule::OneSuccess,
                            {DepOutcome::Failed, DepOutcome::Success},
                            ExpectedResult::Ready},
        TriggerRuleTestCase{"OneSuccess_AllSucceed",
                            TriggerRule::OneSuccess,
                            {DepOutcome::Success, DepOutcome::Success},
                            ExpectedResult::Ready},
        TriggerRuleTestCase{"OneSuccess_AllFail",
                            TriggerRule::OneSuccess,
                            {DepOutcome::Failed, DepOutcome::Failed},
                            ExpectedResult::Skipped},
        TriggerRuleTestCase{"OneSuccess_SuccessAndSkipped",
                            TriggerRule::OneSuccess,
                            {DepOutcome::Success, DepOutcome::Skipped},
                            ExpectedResult::Ready},
        TriggerRuleTestCase{"OneSuccess_AllSkipped",
                            TriggerRule::OneSuccess,
                            {DepOutcome::Skipped, DepOutcome::Skipped},
                            ExpectedResult::Skipped}),
    [](const auto &i) { return i.param.name; });

// ============================================================================
// Test Cases: OneFailed
// ============================================================================

INSTANTIATE_TEST_SUITE_P(
    OneFailed, TriggerRuleTest,
    ::testing::Values(
        TriggerRuleTestCase{"OneFailed_OneFailsOneSucceeds",
                            TriggerRule::OneFailed,
                            {DepOutcome::Success, DepOutcome::Failed},
                            ExpectedResult::Ready},
        TriggerRuleTestCase{"OneFailed_AllFail",
                            TriggerRule::OneFailed,
                            {DepOutcome::Failed, DepOutcome::Failed},
                            ExpectedResult::Ready},
        TriggerRuleTestCase{"OneFailed_AllSucceed",
                            TriggerRule::OneFailed,
                            {DepOutcome::Success, DepOutcome::Success},
                            ExpectedResult::Skipped},
        TriggerRuleTestCase{"OneFailed_FailedAndSkipped",
                            TriggerRule::OneFailed,
                            {DepOutcome::Failed, DepOutcome::Skipped},
                            ExpectedResult::Ready},
        TriggerRuleTestCase{"OneFailed_AllSkipped",
                            TriggerRule::OneFailed,
                            {DepOutcome::Skipped, DepOutcome::Skipped},
                            ExpectedResult::Skipped}),
    [](const auto &i) { return i.param.name; });

// ============================================================================
// Test Cases: NoneFailed
// ============================================================================

INSTANTIATE_TEST_SUITE_P(
    NoneFailed, TriggerRuleTest,
    ::testing::Values(
        TriggerRuleTestCase{"NoneFailed_AllSucceed",
                            TriggerRule::NoneFailed,
                            {DepOutcome::Success, DepOutcome::Success},
                            ExpectedResult::Ready},
        TriggerRuleTestCase{"NoneFailed_OneFails",
                            TriggerRule::NoneFailed,
                            {DepOutcome::Success, DepOutcome::Failed},
                            ExpectedResult::Skipped},
        TriggerRuleTestCase{"NoneFailed_SuccessAndSkipped",
                            TriggerRule::NoneFailed,
                            {DepOutcome::Success, DepOutcome::Skipped},
                            ExpectedResult::Ready},
        TriggerRuleTestCase{"NoneFailed_AllSkipped",
                            TriggerRule::NoneFailed,
                            {DepOutcome::Skipped, DepOutcome::Skipped},
                            ExpectedResult::Ready}),
    [](const auto &i) { return i.param.name; });

// ============================================================================
// Test Cases: NoneSkipped
// ============================================================================

INSTANTIATE_TEST_SUITE_P(
    NoneSkipped, TriggerRuleTest,
    ::testing::Values(
        TriggerRuleTestCase{"NoneSkipped_AllSucceed",
                            TriggerRule::NoneSkipped,
                            {DepOutcome::Success, DepOutcome::Success},
                            ExpectedResult::Ready},
        TriggerRuleTestCase{"NoneSkipped_AllFailed",
                            TriggerRule::NoneSkipped,
                            {DepOutcome::Failed, DepOutcome::Failed},
                            ExpectedResult::Ready},
        TriggerRuleTestCase{"NoneSkipped_MixedSuccessAndFailed",
                            TriggerRule::NoneSkipped,
                            {DepOutcome::Success, DepOutcome::Failed},
                            ExpectedResult::Ready},
        TriggerRuleTestCase{"NoneSkipped_OneSkipped",
                            TriggerRule::NoneSkipped,
                            {DepOutcome::Success, DepOutcome::Skipped},
                            ExpectedResult::Skipped},
        TriggerRuleTestCase{"NoneSkipped_AllSkipped",
                            TriggerRule::NoneSkipped,
                            {DepOutcome::Skipped, DepOutcome::Skipped},
                            ExpectedResult::Skipped}),
    [](const auto &i) { return i.param.name; });

// ============================================================================
// Edge Case Tests (Non-Parameterized)
// ============================================================================

class TriggerRuleEdgeCaseTest : public ::testing::Test {};

TEST_F(TriggerRuleEdgeCaseTest, NoDependencies_AlwaysReady) {
  DAG dag;
  ASSERT_TRUE(
      dag.add_node(TaskId("task1"), TriggerRule::AllFailed).has_value());

  auto run = DAGRun::create(DAGRunId("test"), std::make_shared<DAG>(dag));
  ASSERT_TRUE(run.has_value());

  // Task with no deps should always be ready regardless of trigger rule
  EXPECT_EQ(run->ready_count(), 1);
}

TEST_F(TriggerRuleEdgeCaseTest, SkipCascadesToDownstream_AllSuccess) {
  // task1 -> task2 (AllSuccess) -> task3 (AllSuccess)
  DAG dag;
  auto idx1 = dag.add_node(TaskId("task1"));
  auto idx2 = dag.add_node(TaskId("task2"), TriggerRule::AllSuccess);
  auto idx3 = dag.add_node(TaskId("task3"), TriggerRule::AllSuccess);
  ASSERT_TRUE(idx1.has_value());
  ASSERT_TRUE(idx2.has_value());
  ASSERT_TRUE(idx3.has_value());
  ASSERT_TRUE(dag.add_edge(*idx1, *idx2).has_value());
  ASSERT_TRUE(dag.add_edge(*idx2, *idx3).has_value());

  auto run = DAGRun::create(DAGRunId("test"), std::make_shared<DAG>(dag));
  ASSERT_TRUE(run.has_value());

  ASSERT_TRUE(run->mark_task_started(*idx1, InstanceId("i1")));
  ASSERT_TRUE(run->mark_task_failed(*idx1, "error", 0));

  // task2 should be UpstreamFailed (AllSuccess with failed upstream)
  auto info2 = run->get_task_info(*idx2);
  ASSERT_TRUE(info2.has_value());
  EXPECT_EQ(info2->state, TaskState::UpstreamFailed);

  // task3 should cascade UpstreamFailed (upstream marked UpstreamFailed)
  auto info3 = run->get_task_info(*idx3);
  ASSERT_TRUE(info3.has_value());
  EXPECT_EQ(info3->state, TaskState::UpstreamFailed);
}

TEST_F(TriggerRuleEdgeCaseTest, AllDone_BreaksCascade) {
  // task1 -> task2 (AllSuccess) -> task3 (AllDone)
  DAG dag;
  auto idx1 = dag.add_node(TaskId("task1"));
  auto idx2 = dag.add_node(TaskId("task2"), TriggerRule::AllSuccess);
  auto idx3 = dag.add_node(TaskId("task3"), TriggerRule::AllDone);
  ASSERT_TRUE(idx1.has_value());
  ASSERT_TRUE(idx2.has_value());
  ASSERT_TRUE(idx3.has_value());
  ASSERT_TRUE(dag.add_edge(*idx1, *idx2).has_value());
  ASSERT_TRUE(dag.add_edge(*idx2, *idx3).has_value());

  auto run = DAGRun::create(DAGRunId("test"), std::make_shared<DAG>(dag));
  ASSERT_TRUE(run.has_value());

  ASSERT_TRUE(run->mark_task_started(*idx1, InstanceId("i1")));
  ASSERT_TRUE(run->mark_task_failed(*idx1, "error", 0));

  // task2 should be UpstreamFailed (AllSuccess with failed upstream)
  auto info2 = run->get_task_info(*idx2);
  ASSERT_TRUE(info2.has_value());
  EXPECT_EQ(info2->state, TaskState::UpstreamFailed);

  // task3 (AllDone) should be ready - breaks the cascade
  EXPECT_EQ(run->ready_count(), 1);
  auto ready = run->get_ready_tasks();
  EXPECT_EQ(ready.size(), 1);
  EXPECT_EQ(ready[0], *idx3);
}

TEST_F(TriggerRuleEdgeCaseTest, DiamondDAG_AllDoneJoin) {
  //       task1
  //      /     \\n  //   task2   task3
  //      \\     /
  //       join (AllDone)
  DAG dag;
  auto idx1 = dag.add_node(TaskId("task1"));
  auto idx2 = dag.add_node(TaskId("task2"));
  auto idx3 = dag.add_node(TaskId("task3"));
  auto idx_join = dag.add_node(TaskId("join"), TriggerRule::AllDone);
  ASSERT_TRUE(idx1.has_value());
  ASSERT_TRUE(idx2.has_value());
  ASSERT_TRUE(idx3.has_value());
  ASSERT_TRUE(idx_join.has_value());
  ASSERT_TRUE(dag.add_edge(*idx1, *idx2).has_value());
  ASSERT_TRUE(dag.add_edge(*idx1, *idx3).has_value());
  ASSERT_TRUE(dag.add_edge(*idx2, *idx_join).has_value());
  ASSERT_TRUE(dag.add_edge(*idx3, *idx_join).has_value());

  auto run = DAGRun::create(DAGRunId("test"), std::make_shared<DAG>(dag));
  ASSERT_TRUE(run.has_value());

  // Complete task1
  ASSERT_TRUE(run->mark_task_started(*idx1, InstanceId("i1")));
  ASSERT_TRUE(run->mark_task_completed(*idx1, 0));

  // task2 succeeds, task3 fails
  ASSERT_TRUE(run->mark_task_started(*idx2, InstanceId("i2")));
  ASSERT_TRUE(run->mark_task_completed(*idx2, 0));

  ASSERT_TRUE(run->mark_task_started(*idx3, InstanceId("i3")));
  ASSERT_TRUE(run->mark_task_failed(*idx3, "error", 0));

  // Join should be ready (AllDone: both deps are terminal)
  EXPECT_EQ(run->ready_count(), 1);
  auto ready = run->get_ready_tasks();
  EXPECT_EQ(ready.size(), 1);
  EXPECT_EQ(ready[0], *idx_join);
}

TEST_F(TriggerRuleEdgeCaseTest, ThreeDeps_OneSuccess_FirstSucceeds) {
  DAG dag;
  auto idx1 = dag.add_node(TaskId("task1"));
  auto idx2 = dag.add_node(TaskId("task2"));
  auto idx3 = dag.add_node(TaskId("task3"));
  auto idx_down = dag.add_node(TaskId("downstream"), TriggerRule::OneSuccess);
  ASSERT_TRUE(idx1.has_value());
  ASSERT_TRUE(idx2.has_value());
  ASSERT_TRUE(idx3.has_value());
  ASSERT_TRUE(idx_down.has_value());
  ASSERT_TRUE(dag.add_edge(*idx1, *idx_down).has_value());
  ASSERT_TRUE(dag.add_edge(*idx2, *idx_down).has_value());
  ASSERT_TRUE(dag.add_edge(*idx3, *idx_down).has_value());

  auto run = DAGRun::create(DAGRunId("test"), std::make_shared<DAG>(dag));
  ASSERT_TRUE(run.has_value());

  // Only first succeeds, rest fail
  ASSERT_TRUE(run->mark_task_started(*idx1, InstanceId("i1")));
  ASSERT_TRUE(run->mark_task_completed(*idx1, 0));
  ASSERT_TRUE(run->mark_task_started(*idx2, InstanceId("i2")));
  ASSERT_TRUE(run->mark_task_failed(*idx2, "error", 0));
  ASSERT_TRUE(run->mark_task_started(*idx3, InstanceId("i3")));
  ASSERT_TRUE(run->mark_task_failed(*idx3, "error", 0));

  EXPECT_EQ(run->ready_count(), 1);
  auto ready = run->get_ready_tasks();
  EXPECT_EQ(ready.size(), 1);
  EXPECT_EQ(ready[0], *idx_down);
}

// ============================================================================
// Serialization Tests
// ============================================================================

class TriggerRuleSerializationTest : public ::testing::Test {};

TEST_F(TriggerRuleSerializationTest, ToStringView) {
  EXPECT_EQ(to_string_view(TriggerRule::AllSuccess), "all_success");
  EXPECT_EQ(to_string_view(TriggerRule::AllFailed), "all_failed");
  EXPECT_EQ(to_string_view(TriggerRule::AllDone), "all_done");
  EXPECT_EQ(to_string_view(TriggerRule::OneSuccess), "one_success");
  EXPECT_EQ(to_string_view(TriggerRule::OneFailed), "one_failed");
  EXPECT_EQ(to_string_view(TriggerRule::NoneFailed), "none_failed");
  EXPECT_EQ(to_string_view(TriggerRule::NoneSkipped), "none_skipped");
}

TEST_F(TriggerRuleSerializationTest, Parse) {
  EXPECT_EQ(parse<TriggerRule>("all_success"), TriggerRule::AllSuccess);
  EXPECT_EQ(parse<TriggerRule>("all_failed"), TriggerRule::AllFailed);
  EXPECT_EQ(parse<TriggerRule>("all_done"), TriggerRule::AllDone);
  EXPECT_EQ(parse<TriggerRule>("one_success"), TriggerRule::OneSuccess);
  EXPECT_EQ(parse<TriggerRule>("one_failed"), TriggerRule::OneFailed);
  EXPECT_EQ(parse<TriggerRule>("none_failed"), TriggerRule::NoneFailed);
  EXPECT_EQ(parse<TriggerRule>("none_skipped"), TriggerRule::NoneSkipped);
}

TEST_F(TriggerRuleSerializationTest, ParseUnknownDefaultsToAllSuccess) {
  EXPECT_EQ(parse<TriggerRule>("unknown"), TriggerRule::AllSuccess);
  EXPECT_EQ(parse<TriggerRule>(""), TriggerRule::AllSuccess);
  EXPECT_EQ(parse<TriggerRule>("invalid_rule"), TriggerRule::AllSuccess);
}

// ============================================================================
// Test Cases: AllDoneMinOneSuccess (NEW)
// ============================================================================

INSTANTIATE_TEST_SUITE_P(
    AllDoneMinOneSuccess, TriggerRuleTest,
    ::testing::Values(
        TriggerRuleTestCase{"AllDoneMinOneSuccess_SuccessAndFailed",
                            TriggerRule::AllDoneMinOneSuccess,
                            {DepOutcome::Success, DepOutcome::Failed},
                            ExpectedResult::Ready},
        TriggerRuleTestCase{"AllDoneMinOneSuccess_AllSuccess",
                            TriggerRule::AllDoneMinOneSuccess,
                            {DepOutcome::Success, DepOutcome::Success},
                            ExpectedResult::Ready},
        TriggerRuleTestCase{"AllDoneMinOneSuccess_AllFailed",
                            TriggerRule::AllDoneMinOneSuccess,
                            {DepOutcome::Failed, DepOutcome::Failed},
                            ExpectedResult::Skipped},
        TriggerRuleTestCase{"AllDoneMinOneSuccess_SuccessAndSkipped",
                            TriggerRule::AllDoneMinOneSuccess,
                            {DepOutcome::Success, DepOutcome::Skipped},
                            ExpectedResult::Ready},
        TriggerRuleTestCase{"AllDoneMinOneSuccess_SkippedAndFailed",
                            TriggerRule::AllDoneMinOneSuccess,
                            {DepOutcome::Skipped, DepOutcome::Failed},
                            ExpectedResult::Skipped},
        TriggerRuleTestCase{"AllDoneMinOneSuccess_AllSkipped",
                            TriggerRule::AllDoneMinOneSuccess,
                            {DepOutcome::Skipped, DepOutcome::Skipped},
                            ExpectedResult::Skipped}),
    [](const auto &i) { return i.param.name; });

// ============================================================================
// Test Cases: AllSkipped (NEW)
// ============================================================================

INSTANTIATE_TEST_SUITE_P(
    AllSkipped, TriggerRuleTest,
    ::testing::Values(
        TriggerRuleTestCase{"AllSkipped_AllSkipped",
                            TriggerRule::AllSkipped,
                            {DepOutcome::Skipped, DepOutcome::Skipped},
                            ExpectedResult::Ready},
        TriggerRuleTestCase{"AllSkipped_OneSuccess",
                            TriggerRule::AllSkipped,
                            {DepOutcome::Skipped, DepOutcome::Success},
                            ExpectedResult::Skipped},
        TriggerRuleTestCase{"AllSkipped_OneFailed",
                            TriggerRule::AllSkipped,
                            {DepOutcome::Skipped, DepOutcome::Failed},
                            ExpectedResult::Skipped},
        TriggerRuleTestCase{"AllSkipped_AllSuccess",
                            TriggerRule::AllSkipped,
                            {DepOutcome::Success, DepOutcome::Success},
                            ExpectedResult::Skipped},
        TriggerRuleTestCase{"AllSkipped_SingleSkipped",
                            TriggerRule::AllSkipped,
                            {DepOutcome::Skipped},
                            ExpectedResult::Ready}),
    [](const auto &i) { return i.param.name; });

// ============================================================================
// Test Cases: OneDone (NEW)
// ============================================================================

INSTANTIATE_TEST_SUITE_P(
    OneDone, TriggerRuleTest,
    ::testing::Values(
        TriggerRuleTestCase{"OneDone_OneSuccess",
                            TriggerRule::OneDone,
                            {DepOutcome::Success, DepOutcome::Skipped},
                            ExpectedResult::Ready},
        TriggerRuleTestCase{"OneDone_OneFailed",
                            TriggerRule::OneDone,
                            {DepOutcome::Failed, DepOutcome::Skipped},
                            ExpectedResult::Ready},
        TriggerRuleTestCase{"OneDone_BothDone",
                            TriggerRule::OneDone,
                            {DepOutcome::Success, DepOutcome::Failed},
                            ExpectedResult::Ready},
        TriggerRuleTestCase{"OneDone_AllSkipped",
                            TriggerRule::OneDone,
                            {DepOutcome::Skipped, DepOutcome::Skipped},
                            ExpectedResult::Skipped},
        TriggerRuleTestCase{"OneDone_SingleSuccess",
                            TriggerRule::OneDone,
                            {DepOutcome::Success},
                            ExpectedResult::Ready},
        TriggerRuleTestCase{"OneDone_SingleFailed",
                            TriggerRule::OneDone,
                            {DepOutcome::Failed},
                            ExpectedResult::Ready}),
    [](const auto &i) { return i.param.name; });

// ============================================================================
// Test Cases: NoneFailedMinOneSuccess (NEW)
// ============================================================================

INSTANTIATE_TEST_SUITE_P(
    NoneFailedMinOneSuccess, TriggerRuleTest,
    ::testing::Values(
        TriggerRuleTestCase{"NoneFailedMinOneSuccess_SuccessAndSkipped",
                            TriggerRule::NoneFailedMinOneSuccess,
                            {DepOutcome::Success, DepOutcome::Skipped},
                            ExpectedResult::Ready},
        TriggerRuleTestCase{"NoneFailedMinOneSuccess_AllSuccess",
                            TriggerRule::NoneFailedMinOneSuccess,
                            {DepOutcome::Success, DepOutcome::Success},
                            ExpectedResult::Ready},
        TriggerRuleTestCase{"NoneFailedMinOneSuccess_AllSkipped",
                            TriggerRule::NoneFailedMinOneSuccess,
                            {DepOutcome::Skipped, DepOutcome::Skipped},
                            ExpectedResult::Skipped},
        TriggerRuleTestCase{"NoneFailedMinOneSuccess_SuccessAndFailed",
                            TriggerRule::NoneFailedMinOneSuccess,
                            {DepOutcome::Success, DepOutcome::Failed},
                            ExpectedResult::Skipped},
        TriggerRuleTestCase{"NoneFailedMinOneSuccess_AllFailed",
                            TriggerRule::NoneFailedMinOneSuccess,
                            {DepOutcome::Failed, DepOutcome::Failed},
                            ExpectedResult::Skipped},
        TriggerRuleTestCase{"NoneFailedMinOneSuccess_SingleSuccess",
                            TriggerRule::NoneFailedMinOneSuccess,
                            {DepOutcome::Success},
                            ExpectedResult::Ready}),
    [](const auto &i) { return i.param.name; });

// ============================================================================
// Test Cases: Always (NEW)
// ============================================================================

INSTANTIATE_TEST_SUITE_P(
    Always, TriggerRuleTest,
    ::testing::Values(
        TriggerRuleTestCase{"Always_AllFailed",
                            TriggerRule::Always,
                            {DepOutcome::Failed, DepOutcome::Failed},
                            ExpectedResult::Ready},
        TriggerRuleTestCase{"Always_AllSuccess",
                            TriggerRule::Always,
                            {DepOutcome::Success, DepOutcome::Success},
                            ExpectedResult::Ready},
        TriggerRuleTestCase{"Always_AllSkipped",
                            TriggerRule::Always,
                            {DepOutcome::Skipped, DepOutcome::Skipped},
                            ExpectedResult::Ready},
        TriggerRuleTestCase{
            "Always_MixedStates",
            TriggerRule::Always,
            {DepOutcome::Success, DepOutcome::Failed, DepOutcome::Skipped},
            ExpectedResult::Ready},
        TriggerRuleTestCase{"Always_SingleFailed",
                            TriggerRule::Always,
                            {DepOutcome::Failed},
                            ExpectedResult::Ready}),
    [](const auto &i) { return i.param.name; });

// ============================================================================
// Serialization Tests for New Rules
// ============================================================================

TEST_F(TriggerRuleSerializationTest, ToStringView_NewRules) {
  EXPECT_EQ(to_string_view(TriggerRule::AllDoneMinOneSuccess),
            "all_done_min_one_success");
  EXPECT_EQ(to_string_view(TriggerRule::AllSkipped), "all_skipped");
  EXPECT_EQ(to_string_view(TriggerRule::OneDone), "one_done");
  EXPECT_EQ(to_string_view(TriggerRule::NoneFailedMinOneSuccess),
            "none_failed_min_one_success");
  EXPECT_EQ(to_string_view(TriggerRule::Always), "always");
}

TEST_F(TriggerRuleSerializationTest, Parse_NewRules) {
  EXPECT_EQ(parse<TriggerRule>("all_done_min_one_success"),
            TriggerRule::AllDoneMinOneSuccess);
  EXPECT_EQ(parse<TriggerRule>("all_skipped"), TriggerRule::AllSkipped);
  EXPECT_EQ(parse<TriggerRule>("one_done"), TriggerRule::OneDone);
  EXPECT_EQ(parse<TriggerRule>("none_failed_min_one_success"),
            TriggerRule::NoneFailedMinOneSuccess);
  EXPECT_EQ(parse<TriggerRule>("always"), TriggerRule::Always);
}
