#pragma once

#include "dagforge/config/task_config.hpp"
#include "dagforge/util/id.hpp"

#include <ankerl/unordered_dense.h>

#include <chrono>
#include <string>
#include <vector>

namespace dagforge {

// DB-side runtime state for a DAG: rowids, version, pause flag, timestamps.
// Authoritative source is the DB; never derived from file content alone.
struct DagStateRecord {
  DAGId dag_id;
  int64_t dag_rowid{0};
  int version{1};
  bool is_paused{false};
  std::chrono::system_clock::time_point created_at;
  std::chrono::system_clock::time_point updated_at;
  ankerl::unordered_dense::map<TaskId, int64_t> task_rowids;

  [[nodiscard]] auto has_valid_rowid() const noexcept -> bool {
    return dag_rowid > 0;
  }

  [[nodiscard]] auto
  all_task_rowids_valid(const std::vector<TaskConfig> &tasks) const noexcept
      -> bool {
    if (dag_rowid <= 0)
      return false;
    for (const auto &t : tasks) {
      auto it = task_rowids.find(t.task_id);
      if (it == task_rowids.end() || it->second <= 0)
        return false;
    }
    return true;
  }
};

// Merge DB runtime state (rowids, version, is_paused, timestamps) into a
// file-loaded DAGInfo.  File definition is structural truth; DB is state truth.
template <typename DAGInfoT>
inline auto merge_db_state_into_dag_info(DAGInfoT &file_dag,
                                         const DAGInfoT &db_dag) -> void {
  if (db_dag.dag_rowid > 0)
    file_dag.dag_rowid = db_dag.dag_rowid;
  file_dag.version = std::max(file_dag.version, db_dag.version);
  file_dag.is_paused = db_dag.is_paused;
  file_dag.updated_at = std::max(file_dag.updated_at, db_dag.updated_at);

  ankerl::unordered_dense::map<TaskId, int64_t> rowids;
  rowids.reserve(db_dag.tasks.size());
  for (const auto &t : db_dag.tasks) {
    if (t.task_rowid > 0)
      rowids.insert_or_assign(t.task_id, t.task_rowid);
  }
  for (auto &task : file_dag.tasks) {
    if (auto it = rowids.find(task.task_id); it != rowids.end())
      task.task_rowid = it->second;
  }
  file_dag.rebuild_task_index();
}

// Extract a DagStateRecord from any DAGInfo-like type (duck-typed).
// Template avoids a circular include since DAGInfo includes this header.
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
