#pragma once

#include "dagforge/config/task_config.hpp"
#include "dagforge/core/error.hpp"
#include "dagforge/dag/dag_manager.hpp"

#include <chrono>
#include <string>
#include <vector>

namespace dagforge {

/// TaskDefaults: default values applied to tasks during TOML parsing.
/// Internal to the loader — not persisted.
struct TaskDefaults {
  std::chrono::seconds execution_timeout{task_defaults::kExecutionTimeout};
  std::chrono::seconds retry_interval{task_defaults::kRetryInterval};
  int max_retries{task_defaults::kMaxRetries};
  TriggerRule trigger_rule{TriggerRule::AllSuccess};
  ExecutorType executor{ExecutorType::Shell};
  std::string working_dir;
  bool depends_on_past{false};
};

/// DAGDefinition is now a type alias for DAGInfo.
/// The loader parses TOML directly into DAGInfo.
using DAGDefinition = DAGInfo;

/// Builder for constructing DAGInfo from code (tests, programmatic use).
struct DAGInfoBuilder {
  DAGInfo info_;

  auto id(std::string id_str) -> DAGInfoBuilder && {
    info_.dag_id = DAGId{std::move(id_str)};
    return std::move(*this);
  }

  auto name(std::string n) -> DAGInfoBuilder && {
    info_.name = std::move(n);
    return std::move(*this);
  }

  auto description(std::string d) -> DAGInfoBuilder && {
    info_.description = std::move(d);
    return std::move(*this);
  }

  auto schedule(std::string s) -> DAGInfoBuilder && {
    info_.cron = std::move(s);
    return std::move(*this);
  }

  auto start_date(std::chrono::system_clock::time_point t)
      -> DAGInfoBuilder && {
    info_.start_date = t;
    return std::move(*this);
  }

  auto end_date(std::chrono::system_clock::time_point t) -> DAGInfoBuilder && {
    info_.end_date = t;
    return std::move(*this);
  }

  auto task(TaskConfig t) -> DAGInfoBuilder && {
    info_.tasks.push_back(std::move(t));
    return std::move(*this);
  }

  auto max_active_runs(int m) -> DAGInfoBuilder && {
    info_.max_concurrent_runs = m;
    return std::move(*this);
  }

  auto catchup(bool c) -> DAGInfoBuilder && {
    info_.catchup = c;
    return std::move(*this);
  }

  auto source_file(std::string /*path*/) -> DAGInfoBuilder && {
    // source_file is no longer stored — silently ignored for API compat
    return std::move(*this);
  }

  [[nodiscard]] auto build() && -> Result<DAGInfo> {
    if (info_.dag_id.empty()) {
      if (info_.name.empty()) {
        return fail(Error::InvalidArgument);
      }
      info_.dag_id = DAGId{info_.name};
    }
    info_.rebuild_task_index();
    return ok(std::move(info_));
  }
};

class DAGDefinitionLoader {
public:
  [[nodiscard]] static auto load_from_file(std::string_view path,
                                           std::string *diagnostic = nullptr)
      -> Result<DAGInfo>;
  [[nodiscard]] static auto load_from_string(std::string_view toml_str,
                                             std::string *diagnostic = nullptr)
      -> Result<DAGInfo>;
  [[nodiscard]] static auto to_string(const DAGInfo &dag) -> std::string;

  /// Convenience builder (replaces DAGDefinition::builder())
  static auto builder() -> DAGInfoBuilder { return {}; }
};

} // namespace dagforge
