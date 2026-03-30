#include "dagforge/app/services/persistence_service.hpp"
#include "dagforge/util/log.hpp"

#include <algorithm>
#include <array>
#include <chrono>
#include <span>
#include <unordered_map>
#include <unordered_set>

namespace dagforge {

namespace {

constexpr std::array<std::uint64_t, 15> kIoLatencyBucketsNs{
    100'000ULL,       250'000ULL,       500'000ULL,       1'000'000ULL,
    2'500'000ULL,     5'000'000ULL,     10'000'000ULL,    25'000'000ULL,
    50'000'000ULL,    100'000'000ULL,   250'000'000ULL,   500'000'000ULL,
    1'000'000'000ULL, 2'500'000'000ULL, 10'000'000'000ULL};

#define DAGFORGE_DB_RETURN(expr, count_transaction)                            \
  do {                                                                         \
    const auto started_at = std::chrono::steady_clock::now();                  \
    auto result = co_await (expr);                                             \
    const auto elapsed_ns =                                                    \
        std::chrono::duration_cast<std::chrono::nanoseconds>(                  \
            std::chrono::steady_clock::now() - started_at)                     \
            .count();                                                          \
    db_query_duration_.observe_ns(                                             \
        static_cast<std::uint64_t>(elapsed_ns > 0 ? elapsed_ns : 0));          \
    if (result) {                                                              \
      if (count_transaction) {                                                 \
        db_transactions_total_.fetch_add(1, std::memory_order_relaxed);        \
      }                                                                        \
    } else {                                                                   \
      db_errors_total_.fetch_add(1, std::memory_order_relaxed);                \
    }                                                                          \
    co_return result;                                                          \
  } while (false)

} // namespace

PersistenceService::PersistenceService(Runtime &runtime,
                                       const DatabaseConfig &cfg,
                                       std::size_t create_run_batch_capacity,
                                       std::size_t task_update_batch_capacity)
    : runtime_(runtime), db_pool_(std::max(1u, runtime.shard_count())),
      create_run_batch_queue_(db_pool_.get_executor(),
                              create_run_batch_capacity),
      task_update_batch_queue_(db_pool_.get_executor(),
                               task_update_batch_capacity),
      task_update_batch_flush_histogram_(
          std::span<const std::uint64_t>(kIoLatencyBucketsNs)),
      db_(db_pool_.get_executor(), cfg) {}

PersistenceService::~PersistenceService() {
  trigger_batch_writer_running_.store(false, std::memory_order_relaxed);
  task_update_batch_writer_running_.store(false, std::memory_order_relaxed);
  create_run_batch_queue_.cancel();
  task_update_batch_queue_.cancel();
  wait_for_task_update_submitters_blocking();
}

// ── Lifecycle
// ──────────────────────────────────────────────────────────────────

auto PersistenceService::open() -> task<Result<void>> {
  const auto started_at = std::chrono::steady_clock::now();
  auto open_res = co_await db_.open();
  const auto elapsed_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                              std::chrono::steady_clock::now() - started_at)
                              .count();
  db_query_duration_.observe_ns(
      static_cast<std::uint64_t>(elapsed_ns > 0 ? elapsed_ns : 0));
  if (!open_res) {
    db_errors_total_.fetch_add(1, std::memory_order_relaxed);
    co_return open_res;
  }
  db_transactions_total_.fetch_add(1, std::memory_order_relaxed);

  if (!trigger_batch_writer_running_.exchange(true,
                                              std::memory_order_relaxed)) {
    boost::asio::co_spawn(db_pool_, trigger_batch_writer_loop(),
                          boost::asio::detached);
  }
  if (!task_update_batch_writer_running_.exchange(true,
                                                  std::memory_order_relaxed)) {
    boost::asio::co_spawn(db_pool_, task_update_batch_writer_loop(),
                          boost::asio::detached);
  }
  co_return open_res;
}

auto PersistenceService::close() -> task<void> {
  const auto started_at = std::chrono::steady_clock::now();
  trigger_batch_writer_running_.store(false, std::memory_order_relaxed);
  task_update_batch_writer_running_.store(false, std::memory_order_relaxed);
  create_run_batch_queue_.cancel();
  task_update_batch_queue_.cancel();
  co_await wait_for_task_update_submitters_async();
  co_await db_.close();
  const auto elapsed_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                              std::chrono::steady_clock::now() - started_at)
                              .count();
  db_query_duration_.observe_ns(
      static_cast<std::uint64_t>(elapsed_ns > 0 ? elapsed_ns : 0));
  db_transactions_total_.fetch_add(1, std::memory_order_relaxed);
}

auto PersistenceService::is_open() const noexcept -> bool {
  return db_.is_open();
}

auto PersistenceService::db_query_duration_histogram() const
    -> metrics::Histogram::Snapshot {
  return db_query_duration_.snapshot();
}

auto PersistenceService::db_connection_acquire_histogram() const
    -> metrics::Histogram::Snapshot {
  return db_.connection_acquire_histogram();
}

auto PersistenceService::db_errors_total() const noexcept -> std::uint64_t {
  return db_errors_total_.load(std::memory_order_relaxed);
}

auto PersistenceService::db_connection_acquire_failures_total() const noexcept
    -> std::uint64_t {
  return db_.connection_acquire_failures_total();
}

auto PersistenceService::db_transactions_total() const noexcept
    -> std::uint64_t {
  return db_transactions_total_.load(std::memory_order_relaxed);
}

auto PersistenceService::task_update_batch_flush_histogram() const
    -> metrics::Histogram::Snapshot {
  return task_update_batch_flush_histogram_.snapshot();
}

// ── DAG management
// ─────────────────────────────────────────────────────────────

auto PersistenceService::save_dag(const DAGInfo &dag) -> task<Result<int64_t>> {
  DAGFORGE_DB_RETURN(db_.save_dag(dag), true);
}

auto PersistenceService::get_dag(const DAGId &dag_id) -> task<Result<DAGInfo>> {
  DAGFORGE_DB_RETURN(db_.get_dag(dag_id), false);
}

auto PersistenceService::list_dags() -> task<Result<std::vector<DAGInfo>>> {
  DAGFORGE_DB_RETURN(db_.list_dags(), false);
}

auto PersistenceService::list_dag_states()
    -> task<Result<std::vector<DagStateRecord>>> {
  DAGFORGE_DB_RETURN(db_.list_dag_states(), false);
}

auto PersistenceService::delete_dag(const DAGId &dag_id) -> task<Result<void>> {
  DAGFORGE_DB_RETURN(db_.delete_dag(dag_id), true);
}

auto PersistenceService::set_dag_paused(const DAGId &dag_id, bool paused)
    -> task<Result<void>> {
  DAGFORGE_DB_RETURN(db_.set_dag_paused(dag_id, paused), true);
}

auto PersistenceService::set_dag_active(const DAGId &dag_id, bool active)
    -> task<Result<void>> {
  DAGFORGE_DB_RETURN(db_.set_dag_active(dag_id, active), true);
}

// ── DAG info upsert
// ──────────────────────────────────────────────────────

auto PersistenceService::upsert_dag_info(const DAGId &dag_id, DAGInfo dag_info,
                                         bool existed)
    -> task<Result<DAGInfo>> {
  (void)existed;

  const auto observe_elapsed =
      [this](std::chrono::steady_clock::time_point started_at) {
        const auto elapsed_ns =
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::steady_clock::now() - started_at)
                .count();
        db_query_duration_.observe_ns(
            static_cast<std::uint64_t>(elapsed_ns > 0 ? elapsed_ns : 0));
      };

  auto transaction_successful = false;
  auto finalize_transaction = [&]() {
    if (transaction_successful) {
      db_transactions_total_.fetch_add(1, std::memory_order_relaxed);
    } else {
      db_errors_total_.fetch_add(1, std::memory_order_relaxed);
    }
  };

  auto started_at = std::chrono::steady_clock::now();
  auto conn_res = co_await db_.acquire_batch_writer_connection();
  observe_elapsed(started_at);
  if (!conn_res) {
    finalize_transaction();
    co_return fail(conn_res.error());
  }
  auto &conn = conn_res->get();

  started_at = std::chrono::steady_clock::now();
  auto rowid_res = co_await db_.save_dag_on_connection(conn, dag_info);
  observe_elapsed(started_at);
  if (!rowid_res) {
    finalize_transaction();
    co_return fail(rowid_res.error());
  }
  dag_info.dag_rowid = *rowid_res;

  started_at = std::chrono::steady_clock::now();
  auto existing_tasks_res = co_await db_.get_tasks(dag_id);
  observe_elapsed(started_at);
  if (!existing_tasks_res) {
    finalize_transaction();
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
    started_at = std::chrono::steady_clock::now();
    auto del_task = co_await db_.delete_task(dag_id, existing_task.task_id);
    observe_elapsed(started_at);
    if (!del_task && del_task.error() != make_error_code(Error::NotFound)) {
      finalize_transaction();
      co_return fail(del_task.error());
    }
  }

  started_at = std::chrono::steady_clock::now();
  auto save_tasks_res =
      co_await db_.save_tasks_on_connection(conn, dag_info.dag_rowid,
                                            dag_info.tasks);
  observe_elapsed(started_at);
  if (!save_tasks_res) {
    finalize_transaction();
    co_return fail(save_tasks_res.error());
  }

  started_at = std::chrono::steady_clock::now();
  auto deps_res = co_await db_.replace_task_dependencies_on_connection(
      conn, dag_info.dag_rowid, dag_info.tasks);
  observe_elapsed(started_at);
  if (!deps_res) {
    finalize_transaction();
    co_return fail(deps_res.error());
  }

  if (auto prepared = dag_info.prepare_runtime_artifacts(); !prepared) {
    finalize_transaction();
    co_return fail(prepared.error());
  }

  conn_res->return_without_reset();
  transaction_successful = true;
  finalize_transaction();
  co_return ok(std::move(dag_info));
}

// ── Run management
// ─────────────────────────────────────────────────────────────

auto PersistenceService::save_dag_run(const DAGRun &run)
    -> task<Result<int64_t>> {
  DAGFORGE_DB_RETURN(db_.save_dag_run(run), true);
}

auto PersistenceService::update_dag_run(const DAGRun &run)
    -> task<Result<void>> {
  const auto started_at = std::chrono::steady_clock::now();
  auto result = (co_await db_.save_dag_run(run)).transform([](int64_t) {});
  const auto elapsed_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                              std::chrono::steady_clock::now() - started_at)
                              .count();
  db_query_duration_.observe_ns(
      static_cast<std::uint64_t>(elapsed_ns > 0 ? elapsed_ns : 0));
  if (result) {
    db_transactions_total_.fetch_add(1, std::memory_order_relaxed);
  } else {
    db_errors_total_.fetch_add(1, std::memory_order_relaxed);
  }
  co_return result;
}

auto PersistenceService::get_dag_run_state(const DAGRunId &id)
    -> task<Result<DAGRunState>> {
  DAGFORGE_DB_RETURN(db_.get_dag_run_state(id), false);
}

// ── Atomic: create run + initial task instances
// ────────────────────────────────

auto PersistenceService::create_run_with_task_instances(
    DAGRun run, std::vector<TaskInstanceInfo> instances)
    -> task<Result<int64_t>> {
  const auto missing_task_rowids =
      std::count_if(instances.begin(), instances.end(),
                    [](const auto &ti) { return ti.task_rowid <= 0; });
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
    const auto started_at = std::chrono::steady_clock::now();
    auto fallback_result =
        co_await db_.create_run_with_task_instances_transaction(
            request->bundle.run, request->bundle.instances);
    const auto elapsed_ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now() - started_at)
            .count();
    db_query_duration_.observe_ns(
        static_cast<std::uint64_t>(elapsed_ns > 0 ? elapsed_ns : 0));
    if (fallback_result) {
      db_transactions_total_.fetch_add(1, std::memory_order_relaxed);
    } else {
      db_errors_total_.fetch_add(1, std::memory_order_relaxed);
    }
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
    const auto started_at = std::chrono::steady_clock::now();
    auto fallback_result =
        co_await db_.create_run_with_task_instances_transaction(
            request->bundle.run, request->bundle.instances);
    const auto elapsed_ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now() - started_at)
            .count();
    db_query_duration_.observe_ns(
        static_cast<std::uint64_t>(elapsed_ns > 0 ? elapsed_ns : 0));
    if (fallback_result) {
      db_transactions_total_.fetch_add(1, std::memory_order_relaxed);
    } else {
      db_errors_total_.fetch_add(1, std::memory_order_relaxed);
    }
    co_return fallback_result;
  }
  co_return fail(Error::DatabaseQueryFailed);
}

auto PersistenceService::trigger_batch_queue_depth() const noexcept
    -> std::size_t {
  return trigger_batch_queue_depth_.load(std::memory_order_relaxed);
}

auto PersistenceService::trigger_batch_last_size() const noexcept
    -> std::size_t {
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

auto PersistenceService::trigger_batch_writer_acquire_failures_total()
    const noexcept -> std::uint64_t {
  return trigger_batch_writer_acquire_failures_total_.load(
      std::memory_order_relaxed);
}

auto PersistenceService::task_update_batch_queue_depth() const noexcept
    -> std::size_t {
  return task_update_batch_queue_depth_.load(std::memory_order_relaxed);
}

auto PersistenceService::task_update_batch_last_size() const noexcept
    -> std::size_t {
  return task_update_batch_last_size_.load(std::memory_order_relaxed);
}

auto PersistenceService::task_update_batch_last_linger_us() const noexcept
    -> std::uint64_t {
  return task_update_batch_last_linger_us_.load(std::memory_order_relaxed);
}

auto PersistenceService::task_update_batch_last_flush_ms() const noexcept
    -> std::uint64_t {
  return task_update_batch_last_flush_ms_.load(std::memory_order_relaxed);
}

auto PersistenceService::task_update_batch_requests_total() const noexcept
    -> std::uint64_t {
  return task_update_batch_requests_total_.load(std::memory_order_relaxed);
}

auto PersistenceService::task_update_batch_commits_total() const noexcept
    -> std::uint64_t {
  return task_update_batch_commits_total_.load(std::memory_order_relaxed);
}

auto PersistenceService::task_update_batch_fallback_total() const noexcept
    -> std::uint64_t {
  return task_update_batch_fallback_total_.load(std::memory_order_relaxed);
}

auto PersistenceService::task_update_batch_rejected_total() const noexcept
    -> std::uint64_t {
  return task_update_batch_rejected_total_.load(std::memory_order_relaxed);
}

auto PersistenceService::task_update_batch_wakeup_lag_us() const noexcept
    -> std::uint64_t {
  return task_update_batch_wakeup_lag_us_.load(std::memory_order_relaxed);
}

auto PersistenceService::task_update_batch_writer_acquire_failures_total()
    const noexcept -> std::uint64_t {
  return task_update_batch_writer_acquire_failures_total_.load(
      std::memory_order_relaxed);
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
      bool acquired = true;
      if (!dedicated_conn.has_value()) {
        auto conn_res = co_await db_.acquire_batch_writer_connection();
        if (!conn_res) {
          log::error("Trigger batch writer failed to acquire connection: {}",
                     conn_res.error().message());
          trigger_batch_writer_acquire_failures_total_.fetch_add(
              1, std::memory_order_relaxed);
          should_backoff = true;
          acquired = false;
        } else {
          dedicated_conn.emplace(std::move(*conn_res));
        }
      }

      if (acquired) {
        auto first =
            co_await create_run_batch_queue_.async_receive(use_awaitable);
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
                trigger_batch_queue_depth_.fetch_sub(1,
                                                     std::memory_order_relaxed);
                batch.push_back(std::move(req));
                drained = true;
              });
          if (!drained) {
            break;
          }
        }

        co_await flush_trigger_batch(dedicated_conn->get(), std::move(batch),
                                     first_enqueued_at);
      }
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

  const auto db_started_at = std::chrono::steady_clock::now();
  auto batch_res =
      co_await db_.create_runs_with_task_instances_transaction(conn, bundles);
  const auto commit_done = std::chrono::steady_clock::now();
  const auto elapsed_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                              commit_done - db_started_at)
                              .count();
  db_query_duration_.observe_ns(
      static_cast<std::uint64_t>(elapsed_ns > 0 ? elapsed_ns : 0));
  trigger_batch_last_flush_ms_.store(
      std::chrono::duration_cast<std::chrono::milliseconds>(commit_done -
                                                            flush_started_at)
          .count(),
      std::memory_order_relaxed);

  if (batch_res) {
    trigger_batch_commits_total_.fetch_add(1, std::memory_order_relaxed);
    db_transactions_total_.fetch_add(1, std::memory_order_relaxed);
    for (std::size_t i = 0; i < batch.size(); ++i) {
      publish_batch_result(batch[i], ok((*batch_res)[i]), commit_done);
    }
    co_return;
  }

  trigger_batch_fallback_total_.fetch_add(batch.size(),
                                          std::memory_order_relaxed);
  for (auto &request : batch) {
    const auto started_at = std::chrono::steady_clock::now();
    auto result = co_await db_.create_run_with_task_instances_transaction(
        request->bundle.run, request->bundle.instances);
    const auto fallback_elapsed_ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now() - started_at)
            .count();
    db_query_duration_.observe_ns(static_cast<std::uint64_t>(
        fallback_elapsed_ns > 0 ? fallback_elapsed_ns : 0));
    if (result) {
      db_transactions_total_.fetch_add(1, std::memory_order_relaxed);
    } else {
      db_errors_total_.fetch_add(1, std::memory_order_relaxed);
    }
    publish_batch_result(request, std::move(result), commit_done);
  }
}

auto PersistenceService::publish_batch_result(
    const CreateRunBatchRequestPtr &request, Result<int64_t> result,
    std::chrono::steady_clock::time_point commit_done) -> void {
  boost::asio::post(request->caller_executor, [this, reply = request->reply,
                                               result = std::move(result),
                                               commit_done]() mutable {
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

auto PersistenceService::task_update_batch_writer_loop() -> spawn_task {
  constexpr auto kLinger = std::chrono::milliseconds(1);
  constexpr std::size_t kMaxBatchSize = 128;
  auto executor = co_await boost::asio::this_coro::executor;
  boost::asio::steady_timer timer(executor);
  std::optional<boost::mysql::pooled_connection> dedicated_conn;

  while (task_update_batch_writer_running_.load(std::memory_order_relaxed)) {
    bool should_backoff = false;
    try {
      bool acquired = true;
      if (!dedicated_conn.has_value()) {
        auto conn_res = co_await db_.acquire_batch_writer_connection();
        if (!conn_res) {
          log::error(
              "Task update batch writer failed to acquire connection: {}",
              conn_res.error().message());
          task_update_batch_writer_acquire_failures_total_.fetch_add(
              1, std::memory_order_relaxed);
          should_backoff = true;
          acquired = false;
        } else {
          dedicated_conn.emplace(std::move(*conn_res));
        }
      }

      if (acquired) {
        auto first =
            co_await task_update_batch_queue_.async_receive(use_awaitable);
        task_update_batch_queue_depth_.fetch_sub(1, std::memory_order_relaxed);
        auto first_enqueued_at = std::chrono::steady_clock::now();

        std::vector<TaskUpdateRequestPtr> batch;
        batch.reserve(kMaxBatchSize);
        batch.push_back(std::move(first));

        timer.expires_after(kLinger);
        co_await timer.async_wait(use_awaitable);

        while (batch.size() < kMaxBatchSize) {
          bool drained = false;
          task_update_batch_queue_.try_receive([&](boost::system::error_code ec,
                                                   TaskUpdateRequestPtr req) {
            if (ec || !req) {
              return;
            }
            task_update_batch_queue_depth_.fetch_sub(1,
                                                     std::memory_order_relaxed);
            batch.push_back(std::move(req));
            drained = true;
          });
          if (!drained) {
            break;
          }
        }

        co_await flush_task_update_batch(dedicated_conn->get(),
                                         std::move(batch), first_enqueued_at);
      }
    } catch (const std::exception &e) {
      if (!task_update_batch_writer_running_.load(std::memory_order_relaxed)) {
        break;
      }
      log::error("Task update batch writer loop failed: {}", e.what());
      dedicated_conn.reset();
      should_backoff = true;
    } catch (...) {
      if (!task_update_batch_writer_running_.load(std::memory_order_relaxed)) {
        break;
      }
      log::error("Task update batch writer loop failed");
      dedicated_conn.reset();
      should_backoff = true;
    }
    if (should_backoff) {
      timer.expires_after(std::chrono::milliseconds(10));
      co_await timer.async_wait(use_awaitable);
    }
  }
}

auto PersistenceService::flush_task_update_batch(
    boost::mysql::any_connection &conn, std::vector<TaskUpdateRequestPtr> batch,
    std::chrono::steady_clock::time_point first_enqueued_at) -> task<void> {
  if (batch.empty()) {
    co_return;
  }

  const auto flush_started_at = std::chrono::steady_clock::now();
  task_update_batch_last_size_.store(batch.size(), std::memory_order_relaxed);
  task_update_batch_last_linger_us_.store(
      std::chrono::duration_cast<std::chrono::microseconds>(flush_started_at -
                                                            first_enqueued_at)
          .count(),
      std::memory_order_relaxed);

  std::unordered_map<std::string, std::vector<TaskUpdateRequestPtr>> by_run;
  by_run.reserve(batch.size());

  for (auto &request : batch) {
    if (!request) {
      continue;
    }
    by_run[request->run_id.str()].push_back(request);
  }

  const auto db_started_at = std::chrono::steady_clock::now();
  bool flush_ok = true;
  for (auto &[run_id_str, requests] : by_run) {
    std::unordered_map<std::string, TaskInstanceInfo> latest_by_task;
    latest_by_task.reserve(requests.size());
    for (const auto &req : requests) {
      const auto key =
          std::format("{}:{}", req->info.task_rowid, req->info.attempt);
      latest_by_task.insert_or_assign(key, req->info);
    }

    auto infos =
        latest_by_task | std::views::values | std::ranges::to<std::vector>();

    auto result = co_await db_.save_task_instances_batch_on_connection(
        conn, DAGRunId{run_id_str}, infos,
        infos.empty() ? -1 : infos.front().run_rowid, -1);
    if (!result) {
      flush_ok = false;
    }
  }

  const auto commit_done = std::chrono::steady_clock::now();
  const auto elapsed_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                              commit_done - db_started_at)
                              .count();
  db_query_duration_.observe_ns(
      static_cast<std::uint64_t>(elapsed_ns > 0 ? elapsed_ns : 0));
  task_update_batch_flush_histogram_.observe_ns(
      static_cast<std::uint64_t>(elapsed_ns > 0 ? elapsed_ns : 0));
  task_update_batch_last_flush_ms_.store(
      std::chrono::duration_cast<std::chrono::milliseconds>(commit_done -
                                                            flush_started_at)
          .count(),
      std::memory_order_relaxed);

  if (flush_ok) {
    task_update_batch_commits_total_.fetch_add(1, std::memory_order_relaxed);
    for (auto &request : batch) {
      publish_task_update_result(request, ok(), commit_done);
    }
    co_return;
  }

  task_update_batch_fallback_total_.fetch_add(batch.size(),
                                              std::memory_order_relaxed);
  for (auto &request : batch) {
    const auto started_at = std::chrono::steady_clock::now();
    auto result =
        co_await db_.update_task_instance(request->run_id, request->info);
    const auto fallback_elapsed_ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now() - started_at)
            .count();
    db_query_duration_.observe_ns(static_cast<std::uint64_t>(
        fallback_elapsed_ns > 0 ? fallback_elapsed_ns : 0));
    if (result) {
      db_transactions_total_.fetch_add(1, std::memory_order_relaxed);
    } else {
      db_errors_total_.fetch_add(1, std::memory_order_relaxed);
    }
    publish_task_update_result(request, std::move(result), commit_done);
  }
}

auto PersistenceService::publish_task_update_result(
    const TaskUpdateRequestPtr &request, Result<void> result,
    std::chrono::steady_clock::time_point commit_done) -> void {
  if (!request->reply) {
    if (!result) {
      log::error("Task update fire-and-forget persistence failed: {}",
                 result.error().message());
    }
    return;
  }
  boost::asio::post(request->caller_executor, [this, reply = request->reply,
                                               result = std::move(result),
                                               commit_done]() mutable {
    task_update_batch_wakeup_lag_us_.store(
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - commit_done)
            .count(),
        std::memory_order_relaxed);
    if (!reply->try_send(boost::system::error_code{}, std::move(result))) {
      log::warn("Task update batch reply channel was full");
    }
  });
}

// ── Task instance persistence
// ──────────────────────────────────────────────────

auto PersistenceService::enqueue_task_update_request(
    const TaskUpdateRequestPtr &request) -> bool {
  if (!task_update_batch_queue_.try_send(boost::system::error_code{},
                                         request)) {
    task_update_batch_rejected_total_.fetch_add(1, std::memory_order_relaxed);
    return false;
  }

  task_update_batch_requests_total_.fetch_add(1, std::memory_order_relaxed);
  task_update_batch_queue_depth_.fetch_add(1, std::memory_order_relaxed);
  return true;
}

auto PersistenceService::send_task_update_request(
    const TaskUpdateRequestPtr &request) -> task<Result<void>> {
  if (enqueue_task_update_request(request)) {
    co_return ok();
  }

  boost::system::error_code ec;
  co_await task_update_batch_queue_.async_send(
      boost::system::error_code{}, request,
      boost::asio::redirect_error(use_awaitable, ec));
  if (ec) {
    co_return fail(Error::DatabaseQueryFailed);
  }

  task_update_batch_requests_total_.fetch_add(1, std::memory_order_relaxed);
  task_update_batch_queue_depth_.fetch_add(1, std::memory_order_relaxed);
  co_return ok();
}

auto PersistenceService::submit_task_update_async(TaskUpdateRequestPtr request)
    -> spawn_task {
  const auto finish = [this]() {
    task_update_async_inflight_.fetch_sub(1, std::memory_order_acq_rel);
  };
  auto send_result = co_await send_task_update_request(request);
  if (!send_result) {
    db_errors_total_.fetch_add(1, std::memory_order_relaxed);
    log::error("Task update enqueue failed: {}", send_result.error().message());
  }
  finish();
}

auto PersistenceService::wait_for_task_update_submitters_async() -> task<void> {
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (task_update_async_inflight_.load(std::memory_order_acquire) > 0 &&
         std::chrono::steady_clock::now() < deadline) {
    co_await async_sleep(std::chrono::milliseconds(5));
  }
}

auto PersistenceService::wait_for_task_update_submitters_blocking() noexcept
    -> void {
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (task_update_async_inflight_.load(std::memory_order_acquire) > 0 &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
}

auto PersistenceService::update_task_instance(const DAGRunId &run_id,
                                              const TaskInstanceInfo &ti)
    -> task<Result<void>> {
  auto caller_executor = co_await boost::asio::this_coro::executor;
  auto reply = std::make_shared<TaskUpdateReply>(caller_executor, 1);
  auto request = std::make_shared<TaskUpdateRequest>(TaskUpdateRequest{
      .run_id = run_id.clone(),
      .info = ti,
      .caller_executor = caller_executor,
      .reply = reply,
  });

  auto send_result = co_await send_task_update_request(request);
  if (!send_result) {
    co_return fail(send_result.error());
  }

  bool receive_failed = false;
  try {
    co_return co_await reply->async_receive(use_awaitable);
  } catch (const std::exception &e) {
    log::error("Task update batch reply receive failed: {}", e.what());
    receive_failed = true;
  } catch (...) {
    log::error("Task update batch reply receive failed");
    receive_failed = true;
  }
  if (receive_failed) {
    db_errors_total_.fetch_add(1, std::memory_order_relaxed);
    co_return fail(Error::DatabaseQueryFailed);
  }
  co_return fail(Error::DatabaseQueryFailed);
}

auto PersistenceService::submit_task_instance_update(const DAGRunId &run_id,
                                                     TaskInstanceInfo ti)
    -> void {
  auto request = std::make_shared<TaskUpdateRequest>(TaskUpdateRequest{
      .run_id = run_id.clone(),
      .info = std::move(ti),
      .caller_executor = db_pool_.get_executor(),
      .reply = nullptr,
  });

  task_update_async_inflight_.fetch_add(1, std::memory_order_acq_rel);
  boost::asio::co_spawn(
      db_pool_, submit_task_update_async(std::move(request)),
      boost::asio::detached);
}

auto PersistenceService::get_task_instances(const DAGRunId &run_id)
    -> task<Result<std::vector<TaskInstanceInfo>>> {
  DAGFORGE_DB_RETURN(db_.get_task_instances(run_id), false);
}

auto PersistenceService::save_task_instances_batch(
    const DAGRunId &run_id, const std::vector<TaskInstanceInfo> &instances)
    -> task<Result<void>> {
  DAGFORGE_DB_RETURN(db_.save_task_instances_batch(run_id, instances), true);
}

auto PersistenceService::save_task(const DAGId &dag_id,
                                   const TaskConfig &task_cfg)
    -> task<Result<int64_t>> {
  DAGFORGE_DB_RETURN(db_.save_task(dag_id, task_cfg), true);
}

auto PersistenceService::delete_task(const DAGId &dag_id, const TaskId &task_id)
    -> task<Result<void>> {
  DAGFORGE_DB_RETURN(db_.delete_task(dag_id, task_id), true);
}

auto PersistenceService::claim_task_instances(std::size_t limit,
                                              std::string_view worker_id)
    -> task<Result<std::vector<ClaimedTaskInstance>>> {
  DAGFORGE_DB_RETURN(db_.claim_task_instances(limit, worker_id), false);
}

auto PersistenceService::touch_task_heartbeat(const TaskInstanceKey &key)
    -> task<Result<void>> {
  DAGFORGE_DB_RETURN(db_.touch_task_heartbeat(key), true);
}

auto PersistenceService::submit_task_heartbeat(TaskInstanceKey key) -> void {
  if (!key.valid()) {
    return;
  }
  boost::asio::co_spawn(
      db_pool_,
      [this, key]() -> spawn_task {
        const auto started_at = std::chrono::steady_clock::now();
        auto result = co_await db_.touch_task_heartbeat(key);
        const auto elapsed_ns =
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::steady_clock::now() - started_at)
                .count();
        db_query_duration_.observe_ns(
            static_cast<std::uint64_t>(elapsed_ns > 0 ? elapsed_ns : 0));
        if (result) {
          db_transactions_total_.fetch_add(1, std::memory_order_relaxed);
        } else {
          db_errors_total_.fetch_add(1, std::memory_order_relaxed);
          log::error("Task heartbeat persistence failed: {}",
                     result.error().message());
        }
      }(),
      boost::asio::detached);
}

auto PersistenceService::reap_zombie_task_instances(
    std::int64_t heartbeat_timeout_ms) -> task<Result<std::size_t>> {
  DAGFORGE_DB_RETURN(db_.reap_zombie_task_instances(heartbeat_timeout_ms),
                     true);
}

// ── XCom
// ───────────────────────────────────────────────────────────────────────

auto PersistenceService::save_xcom(const DAGRunId &run_id,
                                   const TaskId &task_id, std::string key,
                                   std::string value_json)
    -> task<Result<void>> {
  DAGFORGE_DB_RETURN(
      db_.save_xcom(run_id, task_id, std::move(key), std::move(value_json)),
      true);
}

auto PersistenceService::get_xcom(const DAGRunId &run_id, const TaskId &task_id,
                                  std::string_view key)
    -> task<Result<XComEntry>> {
  DAGFORGE_DB_RETURN(db_.get_xcom(run_id, task_id, key), false);
}

auto PersistenceService::get_task_xcoms(const DAGRunId &run_id,
                                        const TaskId &task_id)
    -> task<Result<std::vector<XComEntry>>> {
  DAGFORGE_DB_RETURN(db_.get_task_xcoms(run_id, task_id), false);
}

auto PersistenceService::get_run_xcoms(const DAGRunId &run_id)
    -> task<Result<std::vector<XComTaskEntry>>> {
  DAGFORGE_DB_RETURN(db_.get_run_xcoms(run_id), false);
}

// ── History / scheduling queries
// ───────────────────────────────────────────────

auto PersistenceService::get_run_history(const DAGRunId &run_id)
    -> task<Result<RunHistoryEntry>> {
  DAGFORGE_DB_RETURN(db_.get_run_history(run_id), false);
}

auto PersistenceService::list_run_history(std::size_t limit)
    -> task<Result<std::vector<RunHistoryEntry>>> {
  DAGFORGE_DB_RETURN(db_.list_run_history(limit), false);
}

auto PersistenceService::list_dag_run_history(const DAGId &dag_id,
                                              std::size_t limit)
    -> task<Result<std::vector<RunHistoryEntry>>> {
  DAGFORGE_DB_RETURN(db_.list_dag_run_history(dag_id, limit), false);
}

auto PersistenceService::has_dag_run(const DAGId &dag_id,
                                     TimePoint execution_date)
    -> task<Result<bool>> {
  DAGFORGE_DB_RETURN(db_.has_dag_run(dag_id, execution_date), false);
}

auto PersistenceService::list_dag_run_execution_dates(const DAGId &dag_id,
                                                      TimePoint start,
                                                      TimePoint end)
    -> task<Result<std::vector<TimePoint>>> {
  DAGFORGE_DB_RETURN(db_.list_dag_run_execution_dates(dag_id, start, end),
                     false);
}

auto PersistenceService::get_last_execution_date(const DAGId &dag_id)
    -> task<Result<TimePoint>> {
  DAGFORGE_DB_RETURN(db_.get_last_execution_date(dag_id), false);
}

// ── Watermarks
// ─────────────────────────────────────────────────────────────────

auto PersistenceService::save_watermark(const DAGId &dag_id, TimePoint ts)
    -> task<Result<void>> {
  DAGFORGE_DB_RETURN(db_.save_watermark(dag_id, ts), true);
}

auto PersistenceService::get_watermark(const DAGId &dag_id)
    -> task<Result<TimePoint>> {
  DAGFORGE_DB_RETURN(db_.get_watermark(dag_id), false);
}

auto PersistenceService::update_watermark_success(const DAGId &dag_id,
                                                  TimePoint ts)
    -> task<Result<void>> {
  DAGFORGE_DB_RETURN(db_.update_watermark_success(dag_id, ts), true);
}

auto PersistenceService::update_watermark_failure(const DAGId &dag_id,
                                                  TimePoint ts)
    -> task<Result<void>> {
  DAGFORGE_DB_RETURN(db_.update_watermark_failure(dag_id, ts), true);
}

// ── Previous task state
// ────────────────────────────────────────────────────────

auto PersistenceService::get_previous_task_state(
    std::int64_t task_rowid, TimePoint current_execution_date,
    const DAGRunId &current_run_id) -> task<Result<TaskState>> {
  DAGFORGE_DB_RETURN(db_.get_previous_task_state(
                         task_rowid, current_execution_date, current_run_id),
                     false);
}

// ── Recovery / debug
// ───────────────────────────────────────────────────────────

auto PersistenceService::mark_incomplete_runs_failed()
    -> task<Result<std::size_t>> {
  DAGFORGE_DB_RETURN(db_.mark_incomplete_runs_failed(), true);
}

auto PersistenceService::clear_all_dag_data() -> task<Result<void>> {
  DAGFORGE_DB_RETURN(db_.clear_all_dag_data(), true);
}

// ── Task Logs
// ───────────────────────────────────────────────────────────────

auto PersistenceService::append_task_log(const DAGRunId &run_id,
                                         const TaskId &task_id, int attempt,
                                         std::string stream,
                                         std::string content)
    -> task<Result<void>> {
  DAGFORGE_DB_RETURN(db_.append_task_log(run_id, task_id, attempt,
                                         std::move(stream), std::move(content)),
                     true);
}

auto PersistenceService::get_task_logs(const DAGRunId &run_id,
                                       const TaskId &task_id, int attempt,
                                       std::size_t limit)
    -> task<Result<std::vector<orm::TaskLogEntry>>> {
  DAGFORGE_DB_RETURN(db_.get_task_logs(run_id, task_id, attempt, limit), false);
}

auto PersistenceService::get_run_logs(const DAGRunId &run_id, std::size_t limit)
    -> task<Result<std::vector<orm::TaskLogEntry>>> {
  DAGFORGE_DB_RETURN(db_.get_run_logs(run_id, limit), false);
}

} // namespace dagforge
