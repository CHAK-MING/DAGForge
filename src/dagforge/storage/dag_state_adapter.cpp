#include "dagforge/storage/dag_state_adapter.hpp"

namespace dagforge {

auto DagStateRecord::has_valid_rowid() const noexcept -> bool {
  return dag_rowid > 0;
}

auto DagStateRecord::all_task_rowids_valid(
    const std::vector<TaskConfig> &tasks) const noexcept -> bool {
  if (dag_rowid <= 0) {
    return false;
  }
  for (const auto &t : tasks) {
    auto it = task_rowids.find(t.task_id);
    if (it == task_rowids.end() || it->second <= 0) {
      return false;
    }
  }
  return true;
}

} // namespace dagforge
