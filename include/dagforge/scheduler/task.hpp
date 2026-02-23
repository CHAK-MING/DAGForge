#pragma once

#include "dagforge/scheduler/cron.hpp"
#include "dagforge/util/enum.hpp"
#include "dagforge/util/id.hpp"

#include <boost/describe/enum.hpp>
#include <chrono>
#include <cstdint>
#include <string>

namespace dagforge {

enum class TaskState : std::uint8_t {
  Pending,
  Running,
  Success,
  Failed,
  UpstreamFailed,
  Retrying,
  Skipped,
};
BOOST_DESCRIBE_ENUM(TaskState, Pending, Running, Success, Failed,
                    UpstreamFailed, Retrying, Skipped)
DAGFORGE_DEFINE_ENUM_SERDE(TaskState, TaskState::Pending)

[[nodiscard]] constexpr bool is_terminal(TaskState s) noexcept {
  return s == TaskState::Success || s == TaskState::Failed ||
         s == TaskState::UpstreamFailed || s == TaskState::Skipped;
}

struct RetryPolicy {
  int max_retries{3};
  std::chrono::seconds retry_interval{std::chrono::seconds(0)};
};

struct ExecutionInfo {
  DAGId dag_id;
  TaskId task_id;
  std::string name;
  std::optional<CronExpr> cron_expr;
  std::optional<std::chrono::system_clock::time_point> start_date;
  std::optional<std::chrono::system_clock::time_point> end_date;
  bool catchup{false};
};

} // namespace dagforge
