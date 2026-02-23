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
#include "dagforge/xcom/xcom_types.hpp"

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/thread_pool.hpp>
#include <boost/asio/use_future.hpp>

#include <cstddef>
#include <cstdint>
#include <memory>
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
    auto fut = boost::asio::co_spawn(db_pool_.get_executor(), std::move(op),
                                     boost::asio::use_future);
    return fut.get();
  }
  auto sync_wait(task<void> op) -> void {
    auto fut = boost::asio::co_spawn(db_pool_.get_executor(), std::move(op),
                                     boost::asio::use_future);
    fut.get();
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
                               std::string_view key, const JsonValue &value)
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
  [[nodiscard]] auto get_previous_task_state(const DAGId &dag_id,
                                             const TaskId &task_id,
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
                  std::string_view stream, std::string_view content)
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
  boost::asio::thread_pool db_pool_;
  storage::MySQLDatabase db_;
};

} // namespace dagforge
