#include "dagforge/dag/dag_manager.hpp"

#include "dagforge/app/services/persistence_service.hpp"
#include "dagforge/core/runtime.hpp"
#include "dagforge/util/log.hpp"

#include <algorithm>
#include <queue>
#include <ranges>
#include <unordered_set>

namespace dagforge {

namespace {

using DagMap = ankerl::unordered_dense::map<DAGId, DAGInfo>;

[[nodiscard]] auto find_dag_in(DagMap &dags, const DAGId &dag_id) -> DAGInfo * {
  auto it = dags.find(dag_id);
  return it != dags.end() ? &it->second : nullptr;
}

[[nodiscard]] auto find_dag_in(const DagMap &dags, const DAGId &dag_id)
    -> const DAGInfo * {
  auto it = dags.find(dag_id);
  return it != dags.end() ? &it->second : nullptr;
}

template <typename T>
[[nodiscard]] auto run_persistence_sync(PersistenceService *persistence,
                                        task<Result<T>> op) -> Result<T> {
  if (persistence == nullptr) {
    return fail(Error::InvalidState);
  }
  return persistence->sync_wait(std::move(op));
}

} // namespace

DAGManager::DAGManager(PersistenceService *persistence)
    : shard_slots_(1), // slot 0 always present; resized in set_runtime()
      persistence_(persistence) {}

auto DAGManager::set_runtime(Runtime *runtime) -> void {
  runtime_ = runtime;
  if (runtime_) {
    State current = shard_slots_[0].state;
    shard_slots_.resize(runtime_->shard_count());
    for (auto &slot : shard_slots_)
      slot.state = current;
  }
}

auto DAGManager::local_state() const noexcept -> const State & {
  if (runtime_) {
    const shard_id sid = runtime_->current_shard();
    if (sid != kInvalidShard && sid < shard_slots_.size()) {
      return shard_slots_[sid].state;
    }
  }
  return shard_slots_[0].state;
}

auto DAGManager::broadcast_state(State next) -> void {
  shard_slots_[0].state = next;
  if (runtime_) {
    for (unsigned i = 0; i < runtime_->shard_count(); ++i) {
      runtime_->post_to(static_cast<shard_id>(i),
                        [this, i, s = next]() mutable {
                          shard_slots_[i].state = std::move(s);
                        });
    }
  }
}

auto DAGManager::find_dag(const DAGId &dag_id) -> DAGInfo * {
  const auto &cur = local_state();
  auto it = cur.dags.find(dag_id);
  return it != cur.dags.end() ? const_cast<DAGInfo *>(&it->second) : nullptr;
}

auto DAGManager::find_dag(const DAGId &dag_id) const -> const DAGInfo * {
  return find_dag_in(local_state().dags, dag_id);
}

auto DAGManager::create_dag(DAGId dag_id, const DAGInfo &info) -> Result<void> {
  DAGInfo dag_copy = info;
  dag_copy.dag_id = dag_id;
  dag_copy.rebuild_task_index();

  if (find_dag_in(local_state().dags, dag_id)) {
    return fail(Error::AlreadyExists);
  }

  if (persistence_ && persistence_->is_open()) {
    auto persisted = run_persistence_sync(
        persistence_,
        persistence_->upsert_dag_definition(dag_id, dag_copy, false));
    if (!persisted) {
      log::error("Failed to persist DAG {}: {}", dag_id,
                 persisted.error().message());
      return fail(persisted.error());
    }
    dag_copy = std::move(*persisted);
  }

  if (find_dag_in(local_state().dags, dag_id)) {
    return fail(Error::AlreadyExists);
  }
  State next = local_state();
  next.dags.emplace(dag_id, std::move(dag_copy));
  broadcast_state(std::move(next));
  return ok();
}

auto DAGManager::patch_dag_state(const DAGId &dag_id,
                                 const DagStateRecord &state) -> void {
  State next = local_state();
  auto it = next.dags.find(dag_id);
  if (it == next.dags.end()) {
    return;
  }
  auto &dag = it->second;
  dag.dag_rowid = state.dag_rowid;
  dag.version = state.version;
  dag.is_paused = state.is_paused;
  dag.updated_at = state.updated_at;
  for (auto &task : dag.tasks) {
    if (auto rid = state.task_rowids.find(task.task_id);
        rid != state.task_rowids.end()) {
      task.task_rowid = rid->second;
    }
  }
  dag.rebuild_task_index();
  broadcast_state(std::move(next));
}

auto DAGManager::apply_dag_state(const DAGId &dag_id,
                                 const DagStateRecord &state) -> void {
  State next = local_state();
  auto it = next.dags.find(dag_id);
  if (it == next.dags.end()) {
    return;
  }
  auto &dag = it->second;
  dag.is_paused = state.is_paused;
  dag.version = state.version;
  dag.updated_at = state.updated_at;
  broadcast_state(std::move(next));
}

auto DAGManager::upsert_dag(DAGId dag_id, const DAGInfo &info) -> Result<void> {
  DAGInfo dag_copy = info;
  dag_copy.dag_id = dag_id;
  dag_copy.rebuild_task_index();

  const bool existed_pre = find_dag_in(local_state().dags, dag_id) != nullptr;

  if (persistence_ && persistence_->is_open()) {
    auto persisted = run_persistence_sync(
        persistence_,
        persistence_->upsert_dag_definition(dag_id, dag_copy, existed_pre));
    if (!persisted) {
      log::error("Failed to upsert DAG {} in database: {}", dag_id,
                 persisted.error().message());
      return fail(persisted.error());
    }
    dag_copy = std::move(*persisted);
  }

  const bool existed = find_dag_in(local_state().dags, dag_id) != nullptr;
  State next = local_state();
  next.dags.insert_or_assign(dag_id, std::move(dag_copy));
  broadcast_state(std::move(next));

  log::info("{} DAG: {}", existed ? "Updated" : "Created", dag_id);
  return ok();
}

auto DAGManager::get_dag(const DAGId &dag_id) const -> Result<DAGInfo> {
  const auto *dag = find_dag_in(local_state().dags, dag_id);
  if (!dag) {
    return fail(Error::NotFound);
  }
  return ok(*dag);
}

auto DAGManager::list_dags() const -> std::vector<DAGInfo> {
  return local_state().dags | std::views::values |
         std::ranges::to<std::vector>();
}

auto DAGManager::delete_dag(const DAGId &dag_id) -> Result<void> {
  if (!find_dag_in(local_state().dags, dag_id)) {
    return fail(Error::NotFound);
  }

  if (persistence_ && persistence_->is_open()) {
    auto res =
        run_persistence_sync(persistence_, persistence_->delete_dag(dag_id));
    if (!res) {
      log::error("Failed to delete DAG {} from database: {}", dag_id,
                 res.error().message());
      return fail(res.error());
    }
  }

  if (!find_dag_in(local_state().dags, dag_id)) {
    return fail(Error::NotFound);
  }
  State next = local_state();
  next.dags.erase(dag_id);
  broadcast_state(std::move(next));
  log::info("Deleted DAG: {}", dag_id);
  return ok();
}

auto DAGManager::clear_all() -> void { broadcast_state(State{}); }

auto DAGManager::add_task(const DAGId &dag_id, const TaskConfig &task)
    -> Result<void> {
  const State &cur = local_state();

  const auto *dag = find_dag_in(cur.dags, dag_id);
  if (!dag) {
    return fail(Error::NotFound);
  }
  if (dag->find_task(task.task_id)) {
    return fail(Error::AlreadyExists);
  }

  auto dep_ids = task.dependencies |
                 std::views::transform(&TaskDependency::task_id) |
                 std::ranges::to<std::vector>();

  if (would_create_cycle_internal(*dag, task.task_id, dep_ids)) {
    log::warn("Adding task {} would create a cycle in DAG {}", task.task_id,
              dag_id);
    return fail(Error::InvalidArgument);
  }

  for (const auto &dep : task.dependencies) {
    if (!dag->find_task(dep.task_id)) {
      log::warn("Dependency {} not found in DAG {}", dep.task_id, dag_id);
      return fail(Error::NotFound);
    }
  }

  TaskConfig saved_task = task;
  if (persistence_ && persistence_->is_open()) {
    auto persisted = run_persistence_sync(persistence_,
                                          persistence_->save_task(dag_id, task))
                         .or_else([&](std::error_code ec) -> Result<int64_t> {
                           log::error("Failed to persist task {} in DAG {}: {}",
                                      task.task_id, dag_id, ec.message());
                           return fail(ec);
                         });
    if (!persisted) {
      return fail(persisted.error());
    }
    saved_task.task_rowid = *persisted;
  }

  State next = cur;
  auto *dag_mut = find_dag_in(next.dags, dag_id);
  if (!dag_mut) {
    return fail(Error::NotFound);
  }

  dag_mut->tasks.emplace_back(std::move(saved_task));
  dag_mut->task_index[task.task_id] = dag_mut->tasks.size() - 1;
  dag_mut->updated_at = std::chrono::system_clock::now();

  broadcast_state(std::move(next));
  return ok();
}

auto DAGManager::update_task(const DAGId &dag_id, const TaskId &task_id,
                             const TaskConfig &task) -> Result<void> {
  const State &cur = local_state();

  const auto *dag = find_dag_in(cur.dags, dag_id);
  if (!dag) {
    return fail(Error::NotFound);
  }
  if (!dag->find_task(task_id)) {
    return fail(Error::NotFound);
  }

  auto dep_ids = task.dependencies |
                 std::views::transform(&TaskDependency::task_id) |
                 std::ranges::to<std::vector>();

  if (would_create_cycle_internal(*dag, task_id, dep_ids)) {
    return fail(Error::InvalidArgument);
  }

  if (persistence_ && persistence_->is_open()) {
    auto persisted =
        run_persistence_sync(persistence_,
                             persistence_->save_task(dag_id, task))
            .or_else([&](std::error_code ec) -> Result<int64_t> {
              log::error("Failed to persist task update {} in DAG {}: {}",
                         task_id, dag_id, ec.message());
              return fail(ec);
            });
    if (!persisted) {
      return fail(persisted.error());
    }
  }

  State next = cur;
  auto *dag_mut = find_dag_in(next.dags, dag_id);
  if (!dag_mut) {
    return fail(Error::NotFound);
  }
  auto *existing_ptr = dag_mut->find_task(task_id);
  if (!existing_ptr) {
    return fail(Error::NotFound);
  }

  *existing_ptr = task;
  existing_ptr->task_id = task_id;
  dag_mut->updated_at = std::chrono::system_clock::now();
  broadcast_state(std::move(next));

  log::info("Updated task {} in DAG {}", task_id, dag_id);
  return ok();
}

auto DAGManager::delete_task(const DAGId &dag_id, const TaskId &task_id)
    -> Result<void> {
  const State &cur = local_state();

  const auto *dag = find_dag_in(cur.dags, dag_id);
  if (!dag) {
    return fail(Error::NotFound);
  }

  for (const auto &t : dag->tasks) {
    if (std::ranges::any_of(t.dependencies, [&](const TaskDependency &d) {
          return d.task_id == task_id;
        })) {
      log::warn("Cannot delete task {} - task {} depends on it", task_id,
                t.task_id);
      return fail(Error::InvalidArgument);
    }
  }

  auto idx_it = dag->task_index.find(task_id);
  if (idx_it == dag->task_index.end()) {
    return fail(Error::NotFound);
  }

  if (persistence_ && persistence_->is_open()) {
    auto persisted =
        run_persistence_sync(persistence_,
                             persistence_->delete_task(dag_id, task_id))
            .or_else([&](std::error_code ec) -> Result<void> {
              log::error("Failed to delete task {} from DAG {} in database: {}",
                         task_id, dag_id, ec.message());
              return fail(ec);
            });
    if (!persisted) {
      return fail(persisted.error());
    }
  }

  State next = cur;
  auto *dag_mut = find_dag_in(next.dags, dag_id);
  if (!dag_mut) {
    return fail(Error::NotFound);
  }

  auto idx_it_inner = dag_mut->task_index.find(task_id);
  if (idx_it_inner == dag_mut->task_index.end()) {
    return fail(Error::NotFound);
  }

  const std::size_t idx = idx_it_inner->second;
  const std::size_t last_idx = dag_mut->tasks.size() - 1;
  if (idx != last_idx) {
    dag_mut->task_index[dag_mut->tasks[last_idx].task_id] = idx;
    std::swap(dag_mut->tasks[idx], dag_mut->tasks[last_idx]);
  }

  dag_mut->tasks.pop_back();
  dag_mut->task_index.erase(idx_it_inner);
  dag_mut->updated_at = std::chrono::system_clock::now();

  broadcast_state(std::move(next));
  log::info("Deleted task {} from DAG {}", task_id, dag_id);
  return ok();
}

auto DAGManager::get_task(const DAGId &dag_id, const TaskId &task_id) const
    -> Result<TaskConfig> {
  const auto *dag = find_dag_in(local_state().dags, dag_id);
  if (!dag) {
    return fail(Error::NotFound);
  }

  const auto *task = dag->find_task(task_id);
  if (!task) {
    return fail(Error::NotFound);
  }

  return ok(*task);
}

auto DAGManager::validate_dag(const DAGId &dag_id) const -> Result<void> {
  return build_dag_graph(dag_id).and_then(
      [](const DAG &dag) { return dag.is_valid(); });
}

auto DAGManager::would_create_cycle_internal(
    const DAGInfo &dag, const TaskId &task_id,
    const std::vector<TaskId> &dependencies) const -> bool {
  if (dependencies.empty()) {
    return false;
  }

  const auto reverse_adj = dag.compute_reverse_adj();

  std::unordered_set<TaskId> deps_set(dependencies.begin(), dependencies.end());
  std::unordered_set<TaskId> visited;
  std::queue<TaskId> q;
  q.push(task_id);

  while (!q.empty()) {
    auto current = q.front();
    q.pop();

    if (deps_set.contains(current)) {
      return true;
    }

    if (!visited.insert(current).second) {
      continue;
    }

    auto it = reverse_adj.find(current);
    if (it != reverse_adj.end()) {
      for (const auto &next : it->second) {
        q.push(next);
      }
    }
  }

  return false;
}

auto DAGManager::build_dag_graph(const DAGId &dag_id) const -> Result<DAG> {
  const auto *dag_info = find_dag_in(local_state().dags, dag_id);
  if (!dag_info) {
    return fail(Error::NotFound);
  }

  DAG dag;
  for (const auto &task : dag_info->tasks) {
    if (auto r = dag.add_node(task.task_id, task.trigger_rule, task.is_branch);
        !r) {
      return fail(r.error());
    }
  }

  for (const auto &task : dag_info->tasks) {
    for (const auto &dep : task.dependencies) {
      if (auto r = dag.add_edge(dep.task_id, task.task_id); !r) {
        return fail(r.error());
      }
    }
  }

  return ok(std::move(dag));
}

auto DAGManager::would_create_cycle(
    const DAGId &dag_id, const TaskId &task_id,
    const std::vector<TaskId> &dependencies) const -> bool {
  const auto *dag = find_dag_in(local_state().dags, dag_id);
  if (!dag) {
    return false;
  }
  return would_create_cycle_internal(*dag, task_id, dependencies);
}

auto DAGManager::load_from_database() -> Result<void> {
  if (!persistence_) {
    return ok();
  }

  return run_persistence_sync(persistence_, persistence_->list_dags())
      .and_then([&](std::vector<DAGInfo> &&dags_result) -> Result<void> {
        State next = local_state();
        std::size_t merged_into_file_dags = 0;
        std::size_t inserted_from_db = 0;

        for (auto &db_dag : dags_result) {
          db_dag.rebuild_task_index();
          DAGId id = db_dag.dag_id;

          auto it = next.dags.find(id);
          if (it == next.dags.end()) {
            // No file-loaded definition: use DB record as-is.
            next.dags.insert_or_assign(std::move(id), std::move(db_dag));
            ++inserted_from_db;
            continue;
          }

          // File definition exists: keep it as the structural truth and
          // merge only runtime state from the DB record.
          merge_db_state_into_dag_info(it->second, db_dag);
          ++merged_into_file_dags;
        }

        broadcast_state(std::move(next));
        log::info("Loaded {} DAGs from database (inserted={}, merged={})",
                  dags_result.size(), inserted_from_db, merged_into_file_dags);
        return ok();
      });
}

auto DAGManager::dag_count() const noexcept -> std::size_t {
  return local_state().dags.size();
}

auto DAGManager::has_dag(DAGId dag_id) const -> bool {
  return find_dag_in(local_state().dags, dag_id) != nullptr;
}

} // namespace dagforge
