#include "dagforge/scheduler/retry_policy.hpp"

#include <gtest/gtest.h>

using namespace dagforge;

TEST(RetryPolicyTest, ClassifiesFailureCategoriesFromExitSignals) {
  EXPECT_EQ(classify_task_failure_category("depends_on_past blocked", 1),
            TaskFailureCategory::DependencyBlocked);
  EXPECT_EQ(classify_task_failure_category("timeout while waiting", 1),
            TaskFailureCategory::Timeout);
  EXPECT_EQ(
      classify_task_failure_category("Invalid configuration for shell executor",
                                     1),
      TaskFailureCategory::InvalidConfig);
  EXPECT_EQ(classify_task_failure_category("permission denied", 126),
            TaskFailureCategory::ShellExit126);
  EXPECT_EQ(classify_task_failure_category("command not found", 127),
            TaskFailureCategory::ShellExit127);
  EXPECT_EQ(classify_task_failure_category("immediate fail",
                                           kExitCodeImmediateFail),
            TaskFailureCategory::ImmediateFail);
  EXPECT_EQ(classify_task_failure_category("executor surfaced stderr", 9),
            TaskFailureCategory::ExecutorError);
  EXPECT_EQ(classify_task_failure_category("", 9),
            TaskFailureCategory::ExitError);
  EXPECT_EQ(classify_task_failure_category("", 0),
            TaskFailureCategory::Unknown);
}

TEST(RetryPolicyTest, SensorDefaultsToNoRetriesButKeepsExplicitOverride) {
  RetryPolicy default_policy{
      .max_retries = task_defaults::kMaxRetries,
      .retry_interval = std::chrono::seconds(60),
  };
  auto default_sensor_policy = resolve_retry_policy({
      .configured_policy = default_policy,
      .executor = ExecutorType::Sensor,
      .failure_category = TaskFailureCategory::ExecutorError,
  });
  EXPECT_EQ(default_sensor_policy.max_retries, 0);
  EXPECT_EQ(default_sensor_policy.retry_interval, std::chrono::seconds(60));

  RetryPolicy explicit_policy{
      .max_retries = 2,
      .retry_interval = std::chrono::seconds(15),
  };
  auto explicit_sensor_policy = resolve_retry_policy({
      .configured_policy = explicit_policy,
      .executor = ExecutorType::Sensor,
      .failure_category = TaskFailureCategory::ExecutorError,
  });
  EXPECT_EQ(explicit_sensor_policy.max_retries, 2);
  EXPECT_EQ(explicit_sensor_policy.retry_interval, std::chrono::seconds(15));
}

TEST(RetryPolicyTest, Shell126And127AreNeverRetried) {
  RetryPolicy configured{
      .max_retries = 5,
      .retry_interval = std::chrono::seconds(42),
  };

  auto exit_126 = resolve_retry_policy({
      .configured_policy = configured,
      .executor = ExecutorType::Shell,
      .failure_category = TaskFailureCategory::ShellExit126,
  });
  EXPECT_EQ(exit_126.max_retries, 0);
  EXPECT_EQ(exit_126.retry_interval, std::chrono::seconds(42));

  auto exit_127 = resolve_retry_policy({
      .configured_policy = configured,
      .executor = ExecutorType::Shell,
      .failure_category = TaskFailureCategory::ShellExit127,
  });
  EXPECT_EQ(exit_127.max_retries, 0);
  EXPECT_EQ(exit_127.retry_interval, std::chrono::seconds(42));
}

TEST(RetryPolicyTest, GenericFailuresKeepConfiguredRetries) {
  RetryPolicy configured{
      .max_retries = 4,
      .retry_interval = std::chrono::seconds(11),
  };

  auto resolved = resolve_retry_policy({
      .configured_policy = configured,
      .executor = ExecutorType::Docker,
      .failure_category = TaskFailureCategory::ExecutorError,
  });

  EXPECT_EQ(resolved.max_retries, 4);
  EXPECT_EQ(resolved.retry_interval, std::chrono::seconds(11));
}
