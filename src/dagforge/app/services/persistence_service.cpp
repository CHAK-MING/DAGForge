#include "dagforge/app/services/persistence_service.hpp"

#include "dagforge/core/error.hpp"
#include "dagforge/dag/dag_run.hpp"
#include "dagforge/util/log.hpp"

#include <algorithm>
#include <unordered_set>

namespace dagforge {

PersistenceService::PersistenceService(Runtime &runtime,
                                       const DatabaseConfig &cfg)
    : db_pool_(std::max(1u, runtime.shard_count())),
      db_(db_pool_.get_executor(), cfg) {}

PersistenceService::~PersistenceService() {
  db_pool_.stop();
  db_pool_.join();
}

// ── Lifecycle
// ──────────────────────────────────────────────────────────────────

auto PersistenceService::open() -> task<Result<void>> {
  co_return co_await db_.open();
}

auto PersistenceService::close() -> task<void> { co_await db_.close(); }

auto PersistenceService::is_open() const noexcept -> bool {
  return db_.is_open();
}

// ── DAG management
// ─────────────────────────────────────────────────────────────

auto PersistenceService::save_dag(const DAGInfo &dag) -> task<Result<int64_t>> {
  co_return co_await db_.save_dag(dag);
}

auto PersistenceService::get_dag(const DAGId &dag_id) -> task<Result<DAGInfo>> {
  co_return co_await db_.get_dag(dag_id);
}

auto PersistenceService::list_dags() -> task<Result<std::vector<DAGInfo>>> {
  co_return co_await db_.list_dags();
}

auto PersistenceService::delete_dag(const DAGId &dag_id) -> task<Result<void>> {
  co_return co_await db_.delete_dag(dag_id);
}

auto PersistenceService::set_dag_paused(const DAGId &dag_id, bool paused)
    -> task<Result<void>> {
  co_return co_await db_.set_dag_paused(dag_id, paused);
}

auto PersistenceService::set_dag_active(const DAGId &dag_id, bool active)
    -> task<Result<void>> {
  co_return co_await db_.set_dag_active(dag_id, active);
}

// ── DAG definition upsert
// ──────────────────────────────────────────────────────

auto PersistenceService::upsert_dag_definition(const DAGId &dag_id,
                                               DAGInfo dag_info, bool existed)
    -> task<Result<DAGInfo>> {
  (void)existed;

  auto rowid_res = co_await db_.save_dag(dag_info);
  if (!rowid_res)
    co_return fail(rowid_res.error());
  dag_info.dag_rowid = *rowid_res;

  auto existing_tasks_res = co_await db_.get_tasks(dag_id);
  if (!existing_tasks_res) {
    co_return fail(existing_tasks_res.error());
  }

  std::unordered_set<std::string> incoming_task_ids;
  incoming_task_ids.reserve(dag_info.tasks.size());
  for (const auto &task : dag_info.tasks) {
    incoming_task_ids.insert(task.task_id.str());
  }

  for (const auto &existing_task : *existing_tasks_res) {
    if (incoming_task_ids.contains(existing_task.task_id.str())) {
      continue;
    }
    auto del_task = co_await db_.delete_task(dag_id, existing_task.task_id);
    if (!del_task && del_task.error() != make_error_code(Error::NotFound)) {
      co_return fail(del_task.error());
    }
  }

  for (auto &task : dag_info.tasks) {
    auto tr = co_await db_.save_task(dag_id, task);
    if (!tr)
      co_return fail(tr.error());
    task.task_rowid = *tr;
  }

  // Rebuild dependencies: clear old, then re-insert.
  (void)co_await db_.clear_task_dependencies(dag_id);
  for (const auto &t : dag_info.tasks) {
    if (t.dependencies.empty())
      continue;
    std::vector<TaskId> dep_ids;
    dep_ids.reserve(t.dependencies.size());
    for (const auto &dep : t.dependencies)
      dep_ids.emplace_back(dep.task_id);
    (void)co_await db_.save_task_dependencies(dag_id, t.task_id, dep_ids);
  }

  co_return ok(std::move(dag_info));
}

// ── Run management
// ─────────────────────────────────────────────────────────────

auto PersistenceService::save_dag_run(const DAGRun &run)
    -> task<Result<int64_t>> {
  co_return co_await db_.save_dag_run(run);
}

auto PersistenceService::update_dag_run(const DAGRun &run)
    -> task<Result<void>> {
  co_return (co_await db_.save_dag_run(run)).transform([](int64_t) {});
}

auto PersistenceService::get_dag_run_state(const DAGRunId &id)
    -> task<Result<DAGRunState>> {
  co_return co_await db_.get_dag_run_state(id);
}

// ── Atomic: create run + initial task instances
// ────────────────────────────────

auto PersistenceService::create_run_with_task_instances(
    DAGRun run, std::vector<TaskInstanceInfo> instances)
    -> task<Result<int64_t>> {
  // save_dag_run already does INSERT ... ON DUPLICATE KEY UPDATE, so
  // a crash-safe upsert is inherent. We just need the returned rowid
  // and then bulk-insert the initial task snapshots.
  auto rowid_res = co_await db_.save_dag_run(run);
  if (!rowid_res)
    co_return fail(rowid_res.error());

  const auto run_rowid = *rowid_res;
  for (auto &ti : instances)
    ti.run_rowid = run_rowid;

  auto batch_res = co_await db_.save_task_instances_batch(run.id(), instances);
  if (!batch_res)
    co_return fail(batch_res.error());

  co_return ok(run_rowid);
}

// ── Task instance persistence
// ──────────────────────────────────────────────────

auto PersistenceService::update_task_instance(const DAGRunId &run_id,
                                              const TaskInstanceInfo &ti)
    -> task<Result<void>> {
  co_return co_await db_.update_task_instance(run_id, ti);
}

auto PersistenceService::get_task_instances(const DAGRunId &run_id)
    -> task<Result<std::vector<TaskInstanceInfo>>> {
  co_return co_await db_.get_task_instances(run_id);
}

auto PersistenceService::save_task_instances_batch(
    const DAGRunId &run_id, const std::vector<TaskInstanceInfo> &instances)
    -> task<Result<void>> {
  co_return co_await db_.save_task_instances_batch(run_id, instances);
}

auto PersistenceService::save_task(const DAGId &dag_id,
                                   const TaskConfig &task_cfg)
    -> task<Result<int64_t>> {
  co_return co_await db_.save_task(dag_id, task_cfg);
}

auto PersistenceService::delete_task(const DAGId &dag_id, const TaskId &task_id)
    -> task<Result<void>> {
  co_return co_await db_.delete_task(dag_id, task_id);
}

auto PersistenceService::claim_task_instances(std::size_t limit,
                                              std::string_view worker_id)
    -> task<Result<std::vector<ClaimedTaskInstance>>> {
  co_return co_await db_.claim_task_instances(limit, worker_id);
}

auto PersistenceService::touch_task_heartbeat(const DAGRunId &run_id,
                                              int64_t task_rowid, int attempt)
    -> task<Result<void>> {
  co_return co_await db_.touch_task_heartbeat(run_id, task_rowid, attempt);
}

auto PersistenceService::reap_zombie_task_instances(
    std::int64_t heartbeat_timeout_ms) -> task<Result<std::size_t>> {
  co_return co_await db_.reap_zombie_task_instances(heartbeat_timeout_ms);
}

// ── XCom
// ───────────────────────────────────────────────────────────────────────

auto PersistenceService::save_xcom(const DAGRunId &run_id,
                                   const TaskId &task_id, std::string_view key,
                                   const JsonValue &value)
    -> task<Result<void>> {
  co_return co_await db_.save_xcom(run_id, task_id, key, value);
}

auto PersistenceService::get_xcom(const DAGRunId &run_id, const TaskId &task_id,
                                  std::string_view key)
    -> task<Result<XComEntry>> {
  co_return co_await db_.get_xcom(run_id, task_id, key);
}

auto PersistenceService::get_task_xcoms(const DAGRunId &run_id,
                                        const TaskId &task_id)
    -> task<Result<std::vector<XComEntry>>> {
  co_return co_await db_.get_task_xcoms(run_id, task_id);
}

auto PersistenceService::get_run_xcoms(const DAGRunId &run_id)
    -> task<Result<std::vector<XComTaskEntry>>> {
  co_return co_await db_.get_run_xcoms(run_id);
}

// ── History / scheduling queries
// ───────────────────────────────────────────────

auto PersistenceService::get_run_history(const DAGRunId &run_id)
    -> task<Result<RunHistoryEntry>> {
  co_return co_await db_.get_run_history(run_id);
}

auto PersistenceService::list_run_history(std::size_t limit)
    -> task<Result<std::vector<RunHistoryEntry>>> {
  co_return co_await db_.list_run_history(limit);
}

auto PersistenceService::list_dag_run_history(const DAGId &dag_id,
                                              std::size_t limit)
    -> task<Result<std::vector<RunHistoryEntry>>> {
  co_return co_await db_.list_dag_run_history(dag_id, limit);
}

auto PersistenceService::has_dag_run(const DAGId &dag_id,
                                     TimePoint execution_date)
    -> task<Result<bool>> {
  co_return co_await db_.has_dag_run(dag_id, execution_date);
}

auto PersistenceService::get_last_execution_date(const DAGId &dag_id)
    -> task<Result<TimePoint>> {
  co_return co_await db_.get_last_execution_date(dag_id);
}

// ── Watermarks
// ─────────────────────────────────────────────────────────────────

auto PersistenceService::save_watermark(const DAGId &dag_id, TimePoint ts)
    -> task<Result<void>> {
  co_return co_await db_.save_watermark(dag_id, ts);
}

auto PersistenceService::get_watermark(const DAGId &dag_id)
    -> task<Result<TimePoint>> {
  co_return co_await db_.get_watermark(dag_id);
}

auto PersistenceService::update_watermark_success(const DAGId &dag_id,
                                                  TimePoint ts)
    -> task<Result<void>> {
  co_return co_await db_.update_watermark_success(dag_id, ts);
}

auto PersistenceService::update_watermark_failure(const DAGId &dag_id,
                                                  TimePoint ts)
    -> task<Result<void>> {
  co_return co_await db_.update_watermark_failure(dag_id, ts);
}

// ── Previous task state
// ────────────────────────────────────────────────────────

auto PersistenceService::get_previous_task_state(
    const DAGId &dag_id, const TaskId &task_id,
    TimePoint current_execution_date, const DAGRunId &current_run_id)
    -> task<Result<TaskState>> {
  co_return co_await db_.get_previous_task_state(
      dag_id, task_id, current_execution_date, current_run_id);
}

// ── Recovery / debug
// ───────────────────────────────────────────────────────────

auto PersistenceService::mark_incomplete_runs_failed()
    -> task<Result<std::size_t>> {
  co_return co_await db_.mark_incomplete_runs_failed();
}

auto PersistenceService::clear_all_dag_data() -> task<Result<void>> {
  co_return co_await db_.clear_all_dag_data();
}

// ── Task Logs
// ───────────────────────────────────────────────────────────────

auto PersistenceService::append_task_log(const DAGRunId &run_id,
                                         const TaskId &task_id, int attempt,
                                         std::string_view stream,
                                         std::string_view content)
    -> task<Result<void>> {
  co_return co_await db_.append_task_log(run_id, task_id, attempt, stream,
                                         content);
}

auto PersistenceService::get_task_logs(const DAGRunId &run_id,
                                       const TaskId &task_id, int attempt,
                                       std::size_t limit)
    -> task<Result<std::vector<orm::TaskLogEntry>>> {
  co_return co_await db_.get_task_logs(run_id, task_id, attempt, limit);
}

auto PersistenceService::get_run_logs(const DAGRunId &run_id, std::size_t limit)
    -> task<Result<std::vector<orm::TaskLogEntry>>> {
  co_return co_await db_.get_run_logs(run_id, limit);
}

} // namespace dagforge
