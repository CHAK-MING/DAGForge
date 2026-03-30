#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/config/task_config.hpp"
#endif

#include <ankerl/unordered_dense.h>

#include <chrono>
#include <vector>

namespace dagforge {

struct DagStateRecord {
  DAGId dag_id{};
  int64_t dag_rowid{0};
  int version{1};
  bool is_paused{false};
  std::chrono::system_clock::time_point created_at{};
  std::chrono::system_clock::time_point updated_at{};
  ankerl::unordered_dense::map<TaskId, int64_t> task_rowids{};

  [[nodiscard]] auto has_valid_rowid() const noexcept -> bool;
  [[nodiscard]] auto
  all_task_rowids_valid(const std::vector<TaskConfig> &tasks) const noexcept
      -> bool;
};

template <typename DAGInfoT>
[[nodiscard]] inline auto state_from_snapshot_info(const DAGInfoT &info)
    -> DagStateRecord;

template <typename DAGInfoT>
inline auto apply_dag_state_to_dag_info(DAGInfoT &dag,
                                        const DagStateRecord &state) -> void {
  if (state.dag_rowid > 0)
    dag.dag_rowid = state.dag_rowid;
  dag.version = std::max(dag.version, state.version);
  dag.is_paused = state.is_paused;
  dag.updated_at = std::max(dag.updated_at, state.updated_at);
  for (auto &task : dag.tasks) {
    if (auto it = state.task_rowids.find(task.task_id);
        it != state.task_rowids.end()) {
      task.task_rowid = it->second;
    }
  }
}

template <typename DAGInfoT>
inline auto merge_db_state_into_dag_info(DAGInfoT &file_dag,
                                         const DAGInfoT &db_dag) -> void {
  apply_dag_state_to_dag_info(file_dag, state_from_snapshot_info(db_dag));
  file_dag.rebuild_task_index();
  file_dag.compiled_graph.reset();
  file_dag.compiled_executor_configs.reset();
  file_dag.compiled_indexed_task_configs.reset();
}

template <typename DAGInfoT>
[[nodiscard]] inline auto state_from_snapshot_info(const DAGInfoT &info)
    -> DagStateRecord {
  DagStateRecord state;
  state.dag_id = info.dag_id;
  state.dag_rowid = info.dag_rowid;
  state.version = info.version;
  state.is_paused = info.is_paused;
  state.created_at = info.created_at;
  state.updated_at = info.updated_at;
  for (const auto &t : info.tasks) {
    if (t.task_rowid > 0)
      state.task_rowids.emplace(t.task_id, t.task_rowid);
  }
  return state;
}

} // namespace dagforge
