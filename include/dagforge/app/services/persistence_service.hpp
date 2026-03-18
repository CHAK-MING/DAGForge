#pragma once

#include "dagforge/config/system_config.hpp"
#include "dagforge/config/task_config.hpp"
#include "dagforge/core/coroutine.hpp"
#include "dagforge/core/error.hpp"
#include "dagforge/core/runtime.hpp"
#include "dagforge/dag/dag_manager.hpp"
#include "dagforge/dag/dag_run.hpp"
#include "dagforge/storage/database_service.hpp"
#include "dagforge/storage/mysql_database.hpp"
#include "dagforge/util/id.hpp"
#include "dagforge/util/json.hpp"
#include "dagforge/util/log.hpp"
#include "dagforge/xcom/xcom_types.hpp"

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/experimental/concurrent_channel.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/thread_pool.hpp>
#include <boost/asio/use_future.hpp>

#include <cstddef>
#include <cstdint>
#include <future>
#include <memory>
#include <optional>
#include <span>
#include <string_view>
#include <vector>

namespace dagforge {

// PersistenceService is the primary database facade used by Application.
// It holds one MySQLDatabase (which internally manages a connection_pool
// per-shard) and forwards all calls as C++20 coroutines.
class PersistenceService {
public:
  using TimePoint = DatabaseService::TimePoint;
  using RunHistoryEntry = DatabaseService::RunHistoryEntry;

  explicit PersistenceService(Runtime &runtime, const DatabaseConfig &cfg);
  ~PersistenceService();

  PersistenceService(const PersistenceService &) = delete;
  auto operator=(const PersistenceService &) -> PersistenceService & = delete;

  // Lifecycle
  // -------------------------------------------------------------------
  [[nodiscard]] auto open() -> task<Result<void>>;
  auto close() -> task<void>;
  [[nodiscard]] auto is_open() const noexcept -> bool;
  template <typename T>
  [[nodiscard]] auto sync_wait(task<Result<T>> op) -> Result<T> {
    boost::asio::io_context io;
    auto fut = boost::asio::co_spawn(io, std::move(op), boost::asio::use_future);
    while (fut.wait_for(std::chrono::seconds(0)) != std::future_status::ready) {
      io.run_one();
    }
    io.restart();
    try {
      return fut.get();
    } catch (const std::future_error &e) {
      log::error("PersistenceService sync_wait failed: {}", e.what());
      return fail(Error::Unknown);
    } catch (const std::exception &e) {
      log::error("PersistenceService sync_wait failed: {}", e.what());
      return fail(Error::Unknown);
    }
  }
  auto sync_wait(task<void> op) -> void {
    boost::asio::io_context io;
    auto fut = boost::asio::co_spawn(io, std::move(op), boost::asio::use_future);
    while (fut.wait_for(std::chrono::seconds(0)) != std::future_status::ready) {
      io.run_one();
    }
    io.restart();
    try {
      fut.get();
    } catch (const std::future_error &e) {
      log::error("PersistenceService sync_wait(void) failed: {}", e.what());
    } catch (const std::exception &e) {
      log::error("PersistenceService sync_wait(void) failed: {}", e.what());
    }
  }

  // DAG management
  // --------------------------------------------------------------
  [[nodiscard]] auto save_dag(const DAGInfo &dag) -> task<Result<int64_t>>;
  [[nodiscard]] auto get_dag(const DAGId &dag_id) -> task<Result<DAGInfo>>;
  [[nodiscard]] auto list_dags() -> task<Result<std::vector<DAGInfo>>>;
  [[nodiscard]] auto delete_dag(const DAGId &dag_id) -> task<Result<void>>;
  [[nodiscard]] auto set_dag_paused(const DAGId &dag_id, bool paused)
      -> task<Result<void>>;
  [[nodiscard]] auto set_dag_active(const DAGId &dag_id, bool active)
      -> task<Result<void>>;

  // DAG definition upsert (called from config-reload thread)
  // --------------------
  [[nodiscard]] auto upsert_dag_definition(const DAGId &dag_id,
                                           DAGInfo dag_info, bool existed)
      -> task<Result<DAGInfo>>;

  // Run management
  // --------------------------------------------------------------
  [[nodiscard]] auto save_dag_run(const DAGRun &run) -> task<Result<int64_t>>;
  [[nodiscard]] auto update_dag_run(const DAGRun &run) -> task<Result<void>>;
  [[nodiscard]] auto get_dag_run_state(const DAGRunId &id)
      -> task<Result<DAGRunState>>;

  // Atomic create run + initial task instances (used by trigger_run)
  // -----------
  [[nodiscard]] auto
  create_run_with_task_instances(DAGRun run,
                                 std::vector<TaskInstanceInfo> instances)
      -> task<Result<int64_t>>;
  [[nodiscard]] auto trigger_batch_queue_depth() const noexcept -> std::size_t;
  [[nodiscard]] auto trigger_batch_last_size() const noexcept -> std::size_t;
  [[nodiscard]] auto trigger_batch_last_linger_us() const noexcept
      -> std::uint64_t;
  [[nodiscard]] auto trigger_batch_last_flush_ms() const noexcept
      -> std::uint64_t;
  [[nodiscard]] auto trigger_batch_requests_total() const noexcept
      -> std::uint64_t;
  [[nodiscard]] auto trigger_batch_commits_total() const noexcept
      -> std::uint64_t;
  [[nodiscard]] auto trigger_batch_fallback_total() const noexcept
      -> std::uint64_t;
  [[nodiscard]] auto trigger_batch_rejected_total() const noexcept
      -> std::uint64_t;
  [[nodiscard]] auto trigger_batch_wakeup_lag_us() const noexcept
      -> std::uint64_t;

  // Task instance persistence
  // ---------------------------------------------------
  [[nodiscard]] auto update_task_instance(const DAGRunId &run_id,
                                          const TaskInstanceInfo &ti)
      -> task<Result<void>>;
  [[nodiscard]] auto get_task_instances(const DAGRunId &run_id)
      -> task<Result<std::vector<TaskInstanceInfo>>>;
  [[nodiscard]] auto
  save_task_instances_batch(const DAGRunId &run_id,
                            const std::vector<TaskInstanceInfo> &instances)
      -> task<Result<void>>;
  [[nodiscard]] auto save_task(const DAGId &dag_id, const TaskConfig &task_cfg)
      -> task<Result<int64_t>>;
  [[nodiscard]] auto delete_task(const DAGId &dag_id, const TaskId &task_id)
      -> task<Result<void>>;
  [[nodiscard]] auto claim_task_instances(std::size_t limit,
                                          std::string_view worker_id)
      -> task<Result<std::vector<ClaimedTaskInstance>>>;
  [[nodiscard]] auto touch_task_heartbeat(const DAGRunId &run_id,
                                          int64_t task_rowid, int attempt)
      -> task<Result<void>>;
  [[nodiscard]] auto
  reap_zombie_task_instances(std::int64_t heartbeat_timeout_ms)
      -> task<Result<std::size_t>>;

  // XCom
  // ------------------------------------------------------------------------
  [[nodiscard]] auto save_xcom(const DAGRunId &run_id, const TaskId &task_id,
                               std::string key, const JsonValue &value)
      -> task<Result<void>>;
  [[nodiscard]] auto get_xcom(const DAGRunId &run_id, const TaskId &task_id,
                              std::string_view key) -> task<Result<XComEntry>>;
  [[nodiscard]] auto get_task_xcoms(const DAGRunId &run_id,
                                    const TaskId &task_id)
      -> task<Result<std::vector<XComEntry>>>;
  [[nodiscard]] auto get_run_xcoms(const DAGRunId &run_id)
      -> task<Result<std::vector<XComTaskEntry>>>;

  // History / scheduling queries
  // ------------------------------------------------
  [[nodiscard]] auto get_run_history(const DAGRunId &run_id)
      -> task<Result<RunHistoryEntry>>;
  [[nodiscard]] auto list_run_history(std::size_t limit)
      -> task<Result<std::vector<RunHistoryEntry>>>;
  [[nodiscard]] auto list_dag_run_history(const DAGId &dag_id,
                                          std::size_t limit)
      -> task<Result<std::vector<RunHistoryEntry>>>;
  [[nodiscard]] auto has_dag_run(const DAGId &dag_id, TimePoint execution_date)
      -> task<Result<bool>>;
  [[nodiscard]] auto list_dag_run_execution_dates(const DAGId &dag_id,
                                                  TimePoint start,
                                                  TimePoint end)
      -> task<Result<std::vector<TimePoint>>>;
  [[nodiscard]] auto get_last_execution_date(const DAGId &dag_id)
      -> task<Result<TimePoint>>;

  // Watermarks
  // ------------------------------------------------------------------
  [[nodiscard]] auto save_watermark(const DAGId &dag_id, TimePoint ts)
      -> task<Result<void>>;
  [[nodiscard]] auto get_watermark(const DAGId &dag_id)
      -> task<Result<TimePoint>>;
  [[nodiscard]] auto update_watermark_success(const DAGId &dag_id, TimePoint ts)
      -> task<Result<void>>;
  [[nodiscard]] auto update_watermark_failure(const DAGId &dag_id, TimePoint ts)
      -> task<Result<void>>;

  // Previous task state (for trigger-rule cross-run checks)
  // --------------------
  [[nodiscard]] auto get_previous_task_state(std::int64_t task_rowid,
                                             TimePoint current_execution_date,
                                             const DAGRunId &current_run_id)
      -> task<Result<TaskState>>;

  // Recovery at startup
  // ---------------------------------------------------------
  [[nodiscard]] auto mark_incomplete_runs_failed() -> task<Result<std::size_t>>;

  // Task Logs
  // ---------------------------------------------------------------
  [[nodiscard]] auto
  append_task_log(const DAGRunId &run_id, const TaskId &task_id, int attempt,
                  std::string stream, std::string content)
      -> task<Result<void>>;
  [[nodiscard]] auto get_task_logs(const DAGRunId &run_id,
                                   const TaskId &task_id, int attempt,
                                   std::size_t limit = 5000)
      -> task<Result<std::vector<orm::TaskLogEntry>>>;
  [[nodiscard]] auto get_run_logs(const DAGRunId &run_id,
                                  std::size_t limit = 10000)
      -> task<Result<std::vector<orm::TaskLogEntry>>>;

  // Debug / test
  // ----------------------------------------------------------------
  auto clear_all_dag_data() -> task<Result<void>>;

private:
  struct CreateRunBatchRequest;
  using CreateRunBatchRequestPtr = std::shared_ptr<CreateRunBatchRequest>;
  using CreateRunBatchQueue = boost::asio::experimental::concurrent_channel<
      boost::asio::any_io_executor,
      void(boost::system::error_code, CreateRunBatchRequestPtr)>;
  using CreateRunBatchReply = boost::asio::experimental::concurrent_channel<
      boost::asio::any_io_executor, void(boost::system::error_code, Result<int64_t>)>;

  struct CreateRunBatchRequest {
    RunInsertBundle bundle;
    boost::asio::any_io_executor caller_executor;
    std::shared_ptr<CreateRunBatchReply> reply;
  };

  auto trigger_batch_writer_loop() -> spawn_task;
  auto flush_trigger_batch(
      boost::mysql::any_connection &conn,
      std::vector<CreateRunBatchRequestPtr> batch,
      std::chrono::steady_clock::time_point first_enqueued_at) -> task<void>;
  auto publish_batch_result(const CreateRunBatchRequestPtr &request,
                            Result<int64_t> result,
                            std::chrono::steady_clock::time_point commit_done)
      -> void;

  boost::asio::thread_pool db_pool_;
  CreateRunBatchQueue create_run_batch_queue_;
  std::atomic<bool> trigger_batch_writer_running_{false};
  std::atomic<std::size_t> trigger_batch_queue_depth_{0};
  std::atomic<std::size_t> trigger_batch_last_size_{0};
  std::atomic<std::uint64_t> trigger_batch_last_linger_us_{0};
  std::atomic<std::uint64_t> trigger_batch_last_flush_ms_{0};
  std::atomic<std::uint64_t> trigger_batch_requests_total_{0};
  std::atomic<std::uint64_t> trigger_batch_commits_total_{0};
  std::atomic<std::uint64_t> trigger_batch_fallback_total_{0};
  std::atomic<std::uint64_t> trigger_batch_rejected_total_{0};
  std::atomic<std::uint64_t> trigger_batch_wakeup_lag_us_{0};
  storage::MySQLDatabase db_;
};

} // namespace dagforge
