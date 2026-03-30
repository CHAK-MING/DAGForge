#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/config/task_policies.hpp"
#include "dagforge/executor/executor_types.hpp"
#include "dagforge/scheduler/task_state.hpp"
#endif

#include <cstdint>
#include <string_view>

namespace dagforge {

enum class TaskFailureCategory : std::uint8_t {
  Timeout,
  DependencyBlocked,
  InvalidConfig,
  ShellExit126,
  ShellExit127,
  ImmediateFail,
  ExitError,
  ExecutorError,
  Unknown,
};

[[nodiscard]] constexpr auto to_string_view(TaskFailureCategory value) noexcept
    -> std::string_view {
  switch (value) {
  case TaskFailureCategory::Timeout:
    return "timeout";
  case TaskFailureCategory::DependencyBlocked:
    return "dependency_blocked";
  case TaskFailureCategory::InvalidConfig:
    return "invalid_config";
  case TaskFailureCategory::ShellExit126:
    return "shell_exit_126";
  case TaskFailureCategory::ShellExit127:
    return "shell_exit_127";
  case TaskFailureCategory::ImmediateFail:
    return "immediate_fail";
  case TaskFailureCategory::ExitError:
    return "exit_error";
  case TaskFailureCategory::ExecutorError:
    return "executor_error";
  case TaskFailureCategory::Unknown:
    return "unknown";
  }
  return "unknown";
}

[[nodiscard]] inline auto
classify_task_failure_category(std::string_view error, int exit_code) noexcept
    -> TaskFailureCategory {
  if (error.find("depends_on_past blocked") != std::string_view::npos) {
    return TaskFailureCategory::DependencyBlocked;
  }
  if (exit_code == kExitCodeTimeout ||
      error.find("timeout") != std::string_view::npos) {
    return TaskFailureCategory::Timeout;
  }
  if (error.starts_with("Invalid configuration for ")) {
    return TaskFailureCategory::InvalidConfig;
  }
  if (exit_code == 126) {
    return TaskFailureCategory::ShellExit126;
  }
  if (exit_code == 127) {
    return TaskFailureCategory::ShellExit127;
  }
  if (exit_code == kExitCodeImmediateFail) {
    return TaskFailureCategory::ImmediateFail;
  }
  if (exit_code != 0 && !error.empty()) {
    return TaskFailureCategory::ExecutorError;
  }
  if (exit_code != 0) {
    return TaskFailureCategory::ExitError;
  }
  return TaskFailureCategory::Unknown;
}

struct RetryPolicyContext {
  RetryPolicy configured_policy{
      .max_retries = task_defaults::kMaxRetries,
      .retry_interval = task_defaults::kRetryInterval,
  };
  ExecutorType executor{ExecutorType::Shell};
  TaskFailureCategory failure_category{TaskFailureCategory::Unknown};
};

[[nodiscard]] constexpr auto
resolve_retry_policy(const RetryPolicyContext &context) noexcept
    -> RetryPolicy {
  auto policy = context.configured_policy;

  if (context.executor == ExecutorType::Sensor &&
      policy.max_retries == task_defaults::kMaxRetries) {
    policy.max_retries = 0;
  }

  switch (context.failure_category) {
  case TaskFailureCategory::ShellExit126:
  case TaskFailureCategory::ShellExit127:
    policy.max_retries = 0;
    break;
  default:
    break;
  }

  return policy;
}

} // namespace dagforge
