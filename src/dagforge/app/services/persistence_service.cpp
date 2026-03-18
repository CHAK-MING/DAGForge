#include "dagforge/app/services/persistence_service.hpp"

#include "dagforge/core/error.hpp"
#include "dagforge/dag/dag_run.hpp"
#include "dagforge/util/log.hpp"

#include <algorithm>
#include <chrono>
#include <unordered_set>

namespace dagforge {

PersistenceService::PersistenceService(Runtime &runtime,
                                       const DatabaseConfig &cfg)
    : db_pool_(std::max(1u, runtime.shard_count())),
      create_run_batch_queue_(db_pool_.get_executor(), 4096),
      db_(db_pool_.get_executor(), cfg) {}

PersistenceService::~PersistenceService() {
  trigger_batch_writer_running_.store(false, std::memory_order_relaxed);
  create_run_batch_queue_.cancel();
  db_pool_.stop();
  db_pool_.join();
}

// ── Lifecycle
// ──────────────────────────────────────────────────────────────────

auto PersistenceService::open() -> task<Result<void>> {
  auto open_res = co_await db_.open();
  if (!open_res) {
    co_return open_res;
  }

  if (!trigger_batch_writer_running_.exchange(true, std::memory_order_relaxed)) {
    boost::asio::co_spawn(db_pool_, trigger_batch_writer_loop(),
                          boost::asio::detached);
  }
  co_return open_res;
}

auto PersistenceService::close() -> task<void> {
  trigger_batch_writer_running_.store(false, std::memory_order_relaxed);
  create_run_batch_queue_.cancel();
  co_await db_.close();
}

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

  if (auto prepared = dag_info.prepare_runtime_artifacts(); !prepared) {
    co_return fail(prepared.error());
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
  const auto missing_task_rowids =
      std::count_if(instances.begin(), instances.end(), [](const auto &ti) {
        return ti.task_rowid <= 0;
      });
  if (missing_task_rowids != 0) {
    const auto &first = instances.front();
    log::error(
        "create_run_with_task_instances has unresolved task_rowid values: "
        "dag_run_id={} run_rowid={} batch_size={} missing_task_rowid={} "
        "first_task_rowid={} first_attempt={} first_state={}",
        run.id(), run.run_rowid(), instances.size(), missing_task_rowids,
        first.task_rowid, first.attempt, enum_to_string(first.state));
  }

  auto caller_executor = co_await boost::asio::this_coro::executor;
  auto reply = std::make_shared<CreateRunBatchReply>(caller_executor, 1);
  auto request = std::make_shared<CreateRunBatchRequest>(CreateRunBatchRequest{
      .bundle = {.run = std::move(run), .instances = std::move(instances)},
      .caller_executor = caller_executor,
      .reply = reply,
  });

  if (!create_run_batch_queue_.try_send(boost::system::error_code{}, request)) {
    trigger_batch_rejected_total_.fetch_add(1, std::memory_order_relaxed);
    trigger_batch_fallback_total_.fetch_add(1, std::memory_order_relaxed);
    auto fallback_result = co_await db_.create_run_with_task_instances_transaction(
        request->bundle.run, request->bundle.instances);
    co_return fallback_result;
  }

  trigger_batch_requests_total_.fetch_add(1, std::memory_order_relaxed);
  trigger_batch_queue_depth_.fetch_add(1, std::memory_order_relaxed);

  bool receive_failed = false;
  try {
    co_return co_await reply->async_receive(use_awaitable);
  } catch (const std::exception &e) {
    log::error("Trigger batch reply receive failed: {}", e.what());
    receive_failed = true;
  } catch (...) {
    log::error("Trigger batch reply receive failed");
    receive_failed = true;
  }
  if (receive_failed) {
    trigger_batch_fallback_total_.fetch_add(1, std::memory_order_relaxed);
    auto fallback_result = co_await db_.create_run_with_task_instances_transaction(
        request->bundle.run, request->bundle.instances);
    co_return fallback_result;
  }
  co_return fail(Error::DatabaseQueryFailed);
}

auto PersistenceService::trigger_batch_queue_depth() const noexcept
    -> std::size_t {
  return trigger_batch_queue_depth_.load(std::memory_order_relaxed);
}

auto PersistenceService::trigger_batch_last_size() const noexcept -> std::size_t {
  return trigger_batch_last_size_.load(std::memory_order_relaxed);
}

auto PersistenceService::trigger_batch_last_linger_us() const noexcept
    -> std::uint64_t {
  return trigger_batch_last_linger_us_.load(std::memory_order_relaxed);
}

auto PersistenceService::trigger_batch_last_flush_ms() const noexcept
    -> std::uint64_t {
  return trigger_batch_last_flush_ms_.load(std::memory_order_relaxed);
}

auto PersistenceService::trigger_batch_requests_total() const noexcept
    -> std::uint64_t {
  return trigger_batch_requests_total_.load(std::memory_order_relaxed);
}

auto PersistenceService::trigger_batch_commits_total() const noexcept
    -> std::uint64_t {
  return trigger_batch_commits_total_.load(std::memory_order_relaxed);
}

auto PersistenceService::trigger_batch_fallback_total() const noexcept
    -> std::uint64_t {
  return trigger_batch_fallback_total_.load(std::memory_order_relaxed);
}

auto PersistenceService::trigger_batch_rejected_total() const noexcept
    -> std::uint64_t {
  return trigger_batch_rejected_total_.load(std::memory_order_relaxed);
}

auto PersistenceService::trigger_batch_wakeup_lag_us() const noexcept
    -> std::uint64_t {
  return trigger_batch_wakeup_lag_us_.load(std::memory_order_relaxed);
}

auto PersistenceService::trigger_batch_writer_loop() -> spawn_task {
  constexpr auto kLinger = std::chrono::milliseconds(1);
  // Cap single-transaction burst size to avoid inflating trigger tail latency
  // when many DAG runs are triggered at once (e.g. 100-way benchmark fanout).
  constexpr std::size_t kMaxBatchSize = 32;
  auto executor = co_await boost::asio::this_coro::executor;
  boost::asio::steady_timer timer(executor);
  std::optional<boost::mysql::pooled_connection> dedicated_conn;

  while (trigger_batch_writer_running_.load(std::memory_order_relaxed)) {
    bool should_backoff = false;
    try {
      if (!dedicated_conn.has_value()) {
        auto conn_res = co_await db_.acquire_batch_writer_connection();
        if (!conn_res) {
          log::error("Trigger batch writer failed to acquire connection: {}",
                     conn_res.error().message());
          should_backoff = true;
          continue;
        }
        dedicated_conn.emplace(std::move(*conn_res));
      }

      auto first = co_await create_run_batch_queue_.async_receive(use_awaitable);
      trigger_batch_queue_depth_.fetch_sub(1, std::memory_order_relaxed);
      auto first_enqueued_at = std::chrono::steady_clock::now();

      std::vector<CreateRunBatchRequestPtr> batch;
      batch.reserve(kMaxBatchSize);
      batch.push_back(std::move(first));

      timer.expires_after(kLinger);
      co_await timer.async_wait(use_awaitable);

      while (batch.size() < kMaxBatchSize) {
        bool drained = false;
        create_run_batch_queue_.try_receive(
            [&](boost::system::error_code ec, CreateRunBatchRequestPtr req) {
              if (ec || !req) {
                return;
              }
              trigger_batch_queue_depth_.fetch_sub(1, std::memory_order_relaxed);
              batch.push_back(std::move(req));
              drained = true;
            });
        if (!drained) {
          break;
        }
      }

      co_await flush_trigger_batch(dedicated_conn->get(), std::move(batch),
                                   first_enqueued_at);
    } catch (const std::exception &e) {
      if (!trigger_batch_writer_running_.load(std::memory_order_relaxed)) {
        break;
      }
      log::error("Trigger batch writer loop failed: {}", e.what());
      dedicated_conn.reset();
      should_backoff = true;
    } catch (...) {
      if (!trigger_batch_writer_running_.load(std::memory_order_relaxed)) {
        break;
      }
      log::error("Trigger batch writer loop failed");
      dedicated_conn.reset();
      should_backoff = true;
    }
    if (should_backoff) {
      timer.expires_after(std::chrono::milliseconds(10));
      co_await timer.async_wait(use_awaitable);
    }
  }
}

auto PersistenceService::flush_trigger_batch(
    boost::mysql::any_connection &conn,
    std::vector<CreateRunBatchRequestPtr> batch,
    std::chrono::steady_clock::time_point first_enqueued_at) -> task<void> {
  if (batch.empty()) {
    co_return;
  }

  const auto flush_started_at = std::chrono::steady_clock::now();
  trigger_batch_last_size_.store(batch.size(), std::memory_order_relaxed);
  trigger_batch_last_linger_us_.store(
      std::chrono::duration_cast<std::chrono::microseconds>(flush_started_at -
                                                            first_enqueued_at)
          .count(),
      std::memory_order_relaxed);

  std::vector<RunInsertBundle> bundles;
  bundles.reserve(batch.size());
  for (const auto &request : batch) {
    bundles.push_back(request->bundle);
  }

  auto batch_res =
      co_await db_.create_runs_with_task_instances_transaction(conn, bundles);
  const auto commit_done = std::chrono::steady_clock::now();
  trigger_batch_last_flush_ms_.store(
      std::chrono::duration_cast<std::chrono::milliseconds>(commit_done -
                                                            flush_started_at)
          .count(),
      std::memory_order_relaxed);

  if (batch_res) {
    trigger_batch_commits_total_.fetch_add(1, std::memory_order_relaxed);
    for (std::size_t i = 0; i < batch.size(); ++i) {
      publish_batch_result(batch[i], ok((*batch_res)[i]), commit_done);
    }
    co_return;
  }

  trigger_batch_fallback_total_.fetch_add(batch.size(), std::memory_order_relaxed);
  for (auto &request : batch) {
    auto result = co_await db_.create_run_with_task_instances_transaction(
        request->bundle.run, request->bundle.instances);
    publish_batch_result(request, std::move(result), commit_done);
  }
}

auto PersistenceService::publish_batch_result(
    const CreateRunBatchRequestPtr &request, Result<int64_t> result,
    std::chrono::steady_clock::time_point commit_done) -> void {
  boost::asio::post(
      request->caller_executor,
      [this, reply = request->reply, result = std::move(result), commit_done]() mutable {
        trigger_batch_wakeup_lag_us_.store(
            std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - commit_done)
                .count(),
            std::memory_order_relaxed);
        if (!reply->try_send(boost::system::error_code{}, std::move(result))) {
          log::warn("Trigger batch reply channel was full");
        }
      });
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
                                   const TaskId &task_id, std::string key,
                                   const JsonValue &value)
    -> task<Result<void>> {
  co_return co_await db_.save_xcom(run_id, task_id, std::move(key), value);
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

auto PersistenceService::list_dag_run_execution_dates(const DAGId &dag_id,
                                                      TimePoint start,
                                                      TimePoint end)
    -> task<Result<std::vector<TimePoint>>> {
  co_return co_await db_.list_dag_run_execution_dates(dag_id, start, end);
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
    std::int64_t task_rowid, TimePoint current_execution_date,
    const DAGRunId &current_run_id)
    -> task<Result<TaskState>> {
  co_return co_await db_.get_previous_task_state(
      task_rowid, current_execution_date, current_run_id);
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
                                         std::string stream,
                                         std::string content)
    -> task<Result<void>> {
  co_return co_await db_.append_task_log(run_id, task_id, attempt,
                                         std::move(stream),
                                         std::move(content));
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
