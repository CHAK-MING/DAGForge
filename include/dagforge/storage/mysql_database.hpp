#pragma once

#include "dagforge/config/system_config.hpp"
#include "dagforge/storage/database_service.hpp"

#include <boost/mysql/connection_pool.hpp>
#include <boost/mysql/pool_params.hpp>

namespace dagforge::storage {

class MySQLDatabase final : public DatabaseService {
public:
  explicit MySQLDatabase(boost::asio::any_io_executor executor,
                         const DatabaseConfig &config);
  ~MySQLDatabase() override;

  MySQLDatabase(const MySQLDatabase &) = delete;
  MySQLDatabase &operator=(const MySQLDatabase &) = delete;

  auto open() -> task<Result<void>> override;
  auto close() -> task<void> override;
  [[nodiscard]] auto is_open() const noexcept -> bool override;

  auto save_dag(const DAGInfo &dag) -> task<Result<int64_t>> override;
  auto set_dag_active(const DAGId &dag_id, bool active)
      -> task<Result<void>> override;
  auto set_dag_paused(const DAGId &dag_id, bool paused)
      -> task<Result<void>> override;
  auto get_dag_active(const DAGId &dag_id) -> task<Result<bool>> override;
  auto delete_dag(const DAGId &dag_id) -> task<Result<void>> override;
  auto get_dag(const DAGId &dag_id) -> task<Result<DAGInfo>> override;
  auto get_dag_by_rowid(int64_t dag_rowid) -> task<Result<DAGInfo>> override;
  auto list_dags() -> task<Result<std::vector<DAGInfo>>> override;

  auto save_task(const DAGId &dag_id, const TaskConfig &t)
      -> task<Result<int64_t>> override;
  auto delete_task(const DAGId &dag_id, const TaskId &task_id)
      -> task<Result<void>> override;
  auto get_tasks(const DAGId &dag_id)
      -> task<Result<std::vector<TaskConfig>>> override;

  auto save_task_dependencies(const DAGId &dag_id, const TaskId &task_id,
                              const std::vector<TaskId> &dep_task_ids,
                              std::string_view dependency_type = "success")
      -> task<Result<void>> override;
  auto get_task_dependencies(const DAGId &dag_id)
      -> task<Result<std::vector<std::pair<TaskId, TaskId>>>> override;
  auto clear_task_dependencies(const DAGId &dag_id)
      -> task<Result<void>> override;

  auto save_dag_run(const DAGRun &run) -> task<Result<int64_t>> override;
  auto update_dag_run_state(const DAGRunId &id, DAGRunState state)
      -> task<Result<void>> override;
  auto get_dag_run_state(const DAGRunId &id)
      -> task<Result<DAGRunState>> override;
  auto get_incomplete_dag_runs()
      -> task<Result<std::vector<DAGRunId>>> override;

  auto save_task_instance(const DAGRunId &run_id, const TaskInstanceInfo &info)
      -> task<Result<void>> override;
  auto update_task_instance(const DAGRunId &run_id,
                            const TaskInstanceInfo &info)
      -> task<Result<void>> override;
  auto get_task_instances(const DAGRunId &run_id)
      -> task<Result<std::vector<TaskInstanceInfo>>> override;
  auto save_task_instances_batch(const DAGRunId &run_id,
                                 const std::vector<TaskInstanceInfo> &instances)
      -> task<Result<void>> override;
  auto claim_task_instances(std::size_t limit, std::string_view worker_id)
      -> task<Result<std::vector<ClaimedTaskInstance>>> override;
  auto touch_task_heartbeat(const DAGRunId &run_id, int64_t task_rowid,
                            int attempt) -> task<Result<void>> override;
  auto reap_zombie_task_instances(std::int64_t heartbeat_timeout_ms)
      -> task<Result<std::size_t>> override;

  auto list_run_history(std::size_t limit = 50)
      -> task<Result<std::vector<RunHistoryEntry>>> override;
  auto list_dag_run_history(const DAGId &dag_id, std::size_t limit = 50)
      -> task<Result<std::vector<RunHistoryEntry>>> override;
  auto get_run_history(const DAGRunId &run_id)
      -> task<Result<RunHistoryEntry>> override;

  auto save_xcom(const DAGRunId &run_id, const TaskId &task_id,
                 std::string_view key, const JsonValue &value)
      -> task<Result<void>> override;
  auto get_xcom(const DAGRunId &run_id, const TaskId &task_id,
                std::string_view key) -> task<Result<XComEntry>> override;
  auto get_task_xcoms(const DAGRunId &run_id, const TaskId &task_id)
      -> task<Result<std::vector<XComEntry>>> override;
  auto get_run_xcoms(const DAGRunId &run_id)
      -> task<Result<std::vector<XComTaskEntry>>> override;
  auto delete_run_xcoms(const DAGRunId &run_id) -> task<Result<void>> override;

  auto get_last_execution_date(const DAGId &dag_id)
      -> task<Result<TimePoint>> override;
  auto run_exists(const DAGId &dag_id, TimePoint execution_time)
      -> task<Result<bool>> override;
  auto has_dag_run(const DAGId &dag_id, TimePoint execution_date)
      -> task<Result<bool>> override;

  auto save_watermark(const DAGId &dag_id, TimePoint ts)
      -> task<Result<void>> override;
  auto get_watermark(const DAGId &dag_id) -> task<Result<TimePoint>> override;
  auto update_watermark_success(const DAGId &dag_id, TimePoint ts)
      -> task<Result<void>> override;
  auto update_watermark_failure(const DAGId &dag_id, TimePoint ts)
      -> task<Result<void>> override;

  auto get_previous_task_state(const DAGId &dag_id, const TaskId &task_id,
                               TimePoint current_execution_date,
                               const DAGRunId &current_run_id)
      -> task<Result<TaskState>> override;

  auto append_task_log(const DAGRunId &run_id, const TaskId &task_id,
                       int attempt, std::string_view stream,
                       std::string_view content) -> task<Result<void>> override;
  auto get_task_logs(const DAGRunId &run_id, const TaskId &task_id, int attempt,
                     std::size_t limit = 5000)
      -> task<Result<std::vector<orm::TaskLogEntry>>> override;
  auto get_run_logs(const DAGRunId &run_id, std::size_t limit = 10000)
      -> task<Result<std::vector<orm::TaskLogEntry>>> override;

  auto clear_all_dag_data() -> task<Result<void>> override;
  auto mark_incomplete_runs_failed() -> task<Result<std::size_t>> override;

private:
  auto ensure_database_exists() -> task<Result<void>>;
  auto get_connection() -> task<Result<boost::mysql::pooled_connection>>;
  auto ensure_schema(boost::mysql::any_connection &conn) -> task<Result<void>>;
  auto get_dag_rowid(boost::mysql::any_connection &conn, const DAGId &dag_id)
      -> task<Result<int64_t>>;

  DatabaseConfig cfg_;
  boost::mysql::connection_pool pool_;
  bool open_{false};
};

} // namespace dagforge::storage
