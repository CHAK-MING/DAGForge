#pragma once

#include "dagforge/config/task_config.hpp"
#include "dagforge/core/error.hpp"
#include "dagforge/dag/dag.hpp"
#include "dagforge/dag/dag_domain.hpp"
#include "dagforge/util/id.hpp"

#include <ankerl/unordered_dense.h>

#include <optional>
#include <ranges>
#include <string>
#include <vector>

namespace dagforge {

class PersistenceService;
class Runtime;

struct DAGInfo {
  int64_t dag_rowid{0};
  DAGId dag_id;
  int version{1};
  std::string name;
  std::string description;
  std::string tags; // JSON array string
  std::string cron;
  std::string timezone{"UTC"};
  int max_concurrent_runs{1};
  std::optional<std::chrono::system_clock::time_point> start_date;
  std::optional<std::chrono::system_clock::time_point> end_date;
  bool catchup{false};
  bool is_paused{false};
  int retention_days{30};
  std::chrono::system_clock::time_point created_at;
  std::chrono::system_clock::time_point updated_at;
  std::vector<TaskConfig> tasks;
  ankerl::unordered_dense::map<TaskId, std::size_t> task_index;

  auto rebuild_task_index() -> void {
    task_index.clear();
    for (auto [i, task] : tasks | std::views::enumerate) {
      task_index.emplace(task.task_id, static_cast<std::size_t>(i));
    }
  }

  [[nodiscard]] auto compute_reverse_adj() const
      -> ankerl::unordered_dense::map<TaskId, std::vector<TaskId>> {
    ankerl::unordered_dense::map<TaskId, std::vector<TaskId>> result;
    for (const auto &task : tasks) {
      for (const auto &dep : task.dependencies) {
        result[dep.task_id].push_back(task.task_id);
      }
    }
    return result;
  }

  [[nodiscard]] auto find_task(const TaskId &task_id) -> TaskConfig * {
    auto it = task_index.find(task_id);
    return it != task_index.end() ? &tasks[it->second] : nullptr;
  }

  [[nodiscard]] auto find_task(const TaskId &task_id) const
      -> const TaskConfig * {
    auto it = task_index.find(task_id);
    return it != task_index.end() ? &tasks[it->second] : nullptr;
  }
};

class DAGManager {
public:
  explicit DAGManager(PersistenceService *persistence = nullptr);
  ~DAGManager() = default;

  DAGManager(const DAGManager &) = delete;
  auto operator=(const DAGManager &) -> DAGManager & = delete;
  DAGManager(DAGManager &&) = delete;
  auto operator=(DAGManager &&) -> DAGManager & = delete;

  auto set_persistence_service(PersistenceService *persistence) -> void {
    persistence_ = persistence;
  }

  auto set_runtime(Runtime *runtime) -> void;

  // DAG CRUD
  [[nodiscard]] auto create_dag(DAGId dag_id, const DAGInfo &info)
      -> Result<void>;
  [[nodiscard]] auto upsert_dag(DAGId dag_id, const DAGInfo &info)
      -> Result<void>;
  [[nodiscard]] auto get_dag(const DAGId &dag_id) const -> Result<DAGInfo>;
  [[nodiscard]] auto list_dags() const -> std::vector<DAGInfo>;
  [[nodiscard]] auto delete_dag(const DAGId &dag_id) -> Result<void>;
  auto clear_all() -> void;

  // Task CRUD within DAG
  [[nodiscard]] auto add_task(const DAGId &dag_id, const TaskConfig &task)
      -> Result<void>;
  [[nodiscard]] auto update_task(const DAGId &dag_id, const TaskId &task_id,
                                 const TaskConfig &task) -> Result<void>;
  [[nodiscard]] auto delete_task(const DAGId &dag_id, const TaskId &task_id)
      -> Result<void>;
  [[nodiscard]] auto get_task(const DAGId &dag_id, const TaskId &task_id) const
      -> Result<TaskConfig>;

  // Validation
  [[nodiscard]] auto validate_dag(const DAGId &dag_id) const -> Result<void>;
  [[nodiscard]] auto
  would_create_cycle(const DAGId &dag_id, const TaskId &task_id,
                     const std::vector<TaskId> &dependencies) const -> bool;

  // Build DAG graph for execution
  [[nodiscard]] auto build_dag_graph(const DAGId &dag_id) const -> Result<DAG>;

  [[nodiscard]] auto load_from_database() -> Result<void>;

  auto patch_dag_state(const DAGId &dag_id, const DagStateRecord &state)
      -> void;
  auto apply_dag_state(const DAGId &dag_id, const DagStateRecord &state)
      -> void;

  [[nodiscard]] auto dag_count() const noexcept -> std::size_t;
  [[nodiscard]] auto has_dag(DAGId dag_id) const -> bool;

private:
  struct State {
    ankerl::unordered_dense::map<DAGId, DAGInfo> dags;
  };

  struct alignas(64) ShardSlot {
    State state;
  };

  [[nodiscard]] auto local_state() const noexcept -> const State &;
  auto broadcast_state(State next) -> void;

  [[nodiscard]] auto generate_dag_id() const -> DAGId;
  [[nodiscard]] auto find_dag(const DAGId &dag_id) -> DAGInfo *;
  [[nodiscard]] auto find_dag(const DAGId &dag_id) const -> const DAGInfo *;
  [[nodiscard]] auto
  would_create_cycle_internal(const DAGInfo &dag, const TaskId &task_id,
                              const std::vector<TaskId> &dependencies) const
      -> bool;

  // Per-shard local state; index == shard_id.  Slot 0 is always present and
  // used as the fallback for single-threaded / test scenarios.
  std::vector<ShardSlot> shard_slots_;
  Runtime *runtime_{nullptr};
  PersistenceService *persistence_;
};

} // namespace dagforge
