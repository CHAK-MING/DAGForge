#pragma once

#include "dagforge/executor/executor.hpp"
#include "dagforge/util/enum.hpp"
#include "dagforge/util/id.hpp"
#include "dagforge/xcom/xcom_types.hpp"

#include "dagforge/core/error.hpp"
#include <boost/describe/enum.hpp>
#include <chrono>
#include <ranges>
#include <string>
#include <variant>
#include <vector>

namespace dagforge {

namespace task_defaults {
inline constexpr std::chrono::seconds kExecutionTimeout{3600};
inline constexpr std::chrono::seconds kRetryInterval{60};
inline constexpr int kMaxRetries{3};
} // namespace task_defaults

enum class TriggerRule : std::uint8_t {
  AllSuccess,              // Default: all upstream tasks succeeded
  AllFailed,               // All upstream tasks failed
  AllDone,                 // All upstream tasks completed (success or failed)
  OneSuccess,              // At least one upstream task succeeded
  OneFailed,               // At least one upstream task failed
  NoneFailed,              // No upstream task failed (may have skipped)
  NoneSkipped,             // No upstream task was skipped
  AllDoneMinOneSuccess,    // All done and at least one succeeded
  AllSkipped,              // All upstream tasks were skipped
  OneDone,                 // At least one upstream completed (success or fail)
  NoneFailedMinOneSuccess, // None failed and at least one succeeded
  Always,                  // Always trigger regardless of upstream states
};
BOOST_DESCRIBE_ENUM(TriggerRule, AllSuccess, AllFailed, AllDone, OneSuccess,
                    OneFailed, NoneFailed, NoneSkipped, AllDoneMinOneSuccess,
                    AllSkipped, OneDone, NoneFailedMinOneSuccess, Always)
DAGFORGE_DEFINE_ENUM_SERDE(TriggerRule, TriggerRule::AllSuccess)

enum class XComSource : std::uint8_t { Stdout, Stderr, ExitCode, Json };
BOOST_DESCRIBE_ENUM(XComSource, Stdout, Stderr, ExitCode, Json)
DAGFORGE_DEFINE_ENUM_SERDE(XComSource, XComSource::Stdout)

struct XComPushConfig {
  std::string key;
  XComSource source{XComSource::Stdout};
  std::string json_path;
  std::string regex_pattern;
  int regex_group{0};
};

struct TaskDependency {
  TaskId task_id;
  std::string label;

  bool operator==(const TaskId &other) const { return task_id == other; }
  bool operator==(const TaskDependency &other) const = default;
};

inline auto get_dep_task_ids(const std::vector<TaskDependency> &deps) {
  return deps |
         std::views::transform([](const TaskDependency &d) -> const TaskId & {
           return d.task_id;
         });
}

struct ShellTaskConfig {};

struct DockerTaskConfig {
  std::string image;
  std::string socket{"/var/run/docker.sock"};
  ImagePullPolicy pull_policy{ImagePullPolicy::Never};
};

struct SensorTaskConfig {
  SensorType type{SensorType::File};
  std::string target;
  std::chrono::seconds poke_interval{std::chrono::seconds(30)};
  bool soft_fail{false};
  int expected_status{200};
  std::string http_method{"GET"};
};

using ExecutorTaskConfig =
    std::variant<ShellTaskConfig, DockerTaskConfig, SensorTaskConfig>;

struct TaskConfig {
  struct Builder;
  static auto builder() -> Builder;

  int64_t task_rowid{0}; // Stable DB reference (FK -> dag_tasks.task_rowid)
  TaskId task_id;
  std::string name;
  std::string command;
  std::string working_dir;
  std::vector<TaskDependency> dependencies;
  ExecutorType executor{ExecutorType::Shell};
  ExecutorTaskConfig executor_config{ShellTaskConfig{}};
  std::chrono::seconds execution_timeout{task_defaults::kExecutionTimeout};
  std::chrono::seconds retry_interval{task_defaults::kRetryInterval};
  int max_retries{task_defaults::kMaxRetries};
  TriggerRule trigger_rule{TriggerRule::AllSuccess};
  bool is_branch{false};
  std::string branch_xcom_key{"branch"};
  bool depends_on_past{false};

  std::vector<XComPushConfig> xcom_push;
  std::vector<XComPullConfig> xcom_pull;
};

struct TaskConfig::Builder {
  TaskConfig config_;

  auto id(std::string id_str) -> Builder && {
    config_.task_id = TaskId{std::move(id_str)};
    return std::move(*this);
  }

  auto name(std::string n) -> Builder && {
    config_.name = std::move(n);
    return std::move(*this);
  }

  auto command(std::string cmd) -> Builder && {
    config_.command = std::move(cmd);
    return std::move(*this);
  }

  auto working_dir(std::string dir) -> Builder && {
    config_.working_dir = std::move(dir);
    return std::move(*this);
  }

  auto depends_on(std::string dep_id, std::string label = "") -> Builder && {
    config_.dependencies.push_back(
        {TaskId{std::move(dep_id)}, std::move(label)});
    return std::move(*this);
  }

  auto executor(ExecutorType type) -> Builder && {
    config_.executor = type;
    return std::move(*this);
  }

  auto config(ExecutorTaskConfig cfg) -> Builder && {
    config_.executor_config = std::move(cfg);
    return std::move(*this);
  }

  auto timeout(std::chrono::seconds t) -> Builder && {
    config_.execution_timeout = t;
    return std::move(*this);
  }

  auto retry(int max, std::chrono::seconds interval) -> Builder && {
    config_.max_retries = max;
    config_.retry_interval = interval;
    return std::move(*this);
  }

  auto trigger_rule(TriggerRule rule) -> Builder && {
    config_.trigger_rule = rule;
    return std::move(*this);
  }

  auto branch(bool is_br, std::string key = "branch") -> Builder && {
    config_.is_branch = is_br;
    config_.branch_xcom_key = std::move(key);
    return std::move(*this);
  }

  auto depends_on_past(bool d) -> Builder && {
    config_.depends_on_past = d;
    return std::move(*this);
  }

  auto push_xcom(XComPushConfig p) -> Builder && {
    config_.xcom_push.push_back(std::move(p));
    return std::move(*this);
  }

  auto pull_xcom(XComPullConfig p) -> Builder && {
    config_.xcom_pull.push_back(std::move(p));
    return std::move(*this);
  }

  [[nodiscard]] auto build() && -> Result<TaskConfig> {
    if (config_.task_id.empty()) {
      if (config_.name.empty()) {
        return fail(Error::InvalidArgument);
      }
      config_.task_id = TaskId{config_.name};
    }
    if (config_.command.empty() && config_.executor != ExecutorType::Sensor) {
      return fail(Error::InvalidArgument);
    }
    return ok(std::move(config_));
  }
};

inline auto TaskConfig::builder() -> Builder { return {}; }

} // namespace dagforge
