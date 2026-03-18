#pragma once

#include "dagforge/app/services/persistence_service.hpp"
#include "dagforge/config/config.hpp"
#include "dagforge/config/dag_definition.hpp"
#include "dagforge/core/coroutine.hpp"
#include "dagforge/core/error.hpp"
#include "dagforge/core/runtime.hpp"
#include "dagforge/dag/dag.hpp"
#include "dagforge/dag/dag_manager.hpp"
#include "dagforge/dag/dag_run.hpp"
#include "dagforge/executor/executor.hpp"

#include <ankerl/unordered_dense.h>

#include <atomic>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace dagforge {

class ApiServer;
class ConfigWatcher;
class DAGRun;
class Engine;
class ExecutionService;
class PersistenceService;
class SchedulerService;

// Application facade - coordinates all services
class Application {
public:
  Application();
  explicit Application(Config config);
  ~Application();

  Application(const Application &) = delete;
  auto operator=(const Application &) -> Application & = delete;

  // Configuration
  [[nodiscard]] auto load_config(std::string_view path) -> Result<void>;
  [[nodiscard]] auto config() const noexcept -> const Config &;
  [[nodiscard]] auto config() noexcept -> Config &;

  // Lifecycle
  [[nodiscard]] auto init() -> Result<void>;
  [[nodiscard]] auto init_db_only() -> Result<void>;
  [[nodiscard]] auto start() -> Result<void>;
  auto stop() noexcept -> void;
  [[nodiscard]] auto is_running() const noexcept -> bool;

  // DAG loading
  [[nodiscard]] auto load_dags_from_directory(std::string_view dags_dir)
      -> Result<bool>;

  // DAG operations
  [[nodiscard]] auto trigger_run(
      DAGId dag_id, TriggerType trigger = TriggerType::Manual,
      std::optional<std::chrono::system_clock::time_point> execution_date =
          std::nullopt) -> task<Result<DAGRunId>>;

  [[nodiscard]] auto
  trigger_scheduled(DAGId dag_id,
                    std::chrono::system_clock::time_point execution_date)
      -> spawn_task;

  [[nodiscard]] auto trigger_run_blocking(
      const DAGId &dag_id, TriggerType trigger = TriggerType::Manual,
      std::optional<std::chrono::system_clock::time_point> execution_date =
          std::nullopt) -> Result<DAGRunId>;
  auto wait_for_completion_async(int timeout_ms = 60000) -> task<void>;
  auto wait_for_completion(int timeout_ms = 60000) -> void;
  [[nodiscard]] auto has_active_runs() const -> bool;
  [[nodiscard]] auto active_coroutines() const -> int;
  [[nodiscard]] auto mysql_batch_write_ops() const -> std::uint64_t;
  [[nodiscard]] auto dropped_persistence_events() const -> std::uint64_t;
  [[nodiscard]] auto event_bus_queue_length() const -> std::size_t;
  [[nodiscard]] auto trigger_batch_queue_depth() const -> std::size_t;
  [[nodiscard]] auto trigger_batch_last_size() const -> std::size_t;
  [[nodiscard]] auto trigger_batch_last_linger_us() const -> std::uint64_t;
  [[nodiscard]] auto trigger_batch_last_flush_ms() const -> std::uint64_t;
  [[nodiscard]] auto trigger_batch_requests_total() const -> std::uint64_t;
  [[nodiscard]] auto trigger_batch_commits_total() const -> std::uint64_t;
  [[nodiscard]] auto trigger_batch_fallback_total() const -> std::uint64_t;
  [[nodiscard]] auto trigger_batch_rejected_total() const -> std::uint64_t;
  [[nodiscard]] auto trigger_batch_wakeup_lag_us() const -> std::uint64_t;
  [[nodiscard]] auto shard_stall_age_ms(shard_id id) const -> std::uint64_t;
  [[nodiscard]] auto get_run_state(const DAGRunId &dag_run_id) const
      -> Result<DAGRunState>;
  [[nodiscard]] auto get_run_state_async(const DAGRunId &dag_run_id) const
      -> task<Result<DAGRunState>>;

  // DAG state management (pause/unpause — propagates to DB, memory, scheduler)
  [[nodiscard]] auto set_dag_paused(const DAGId &dag_id, bool paused)
      -> task<Result<void>>;

  // Cron schedule management
  [[nodiscard]] auto register_dag_cron(DAGId dag_id, std::string_view cron_expr)
      -> Result<void>;
  auto unregister_dag_cron(const DAGId &dag_id) -> void;
  [[nodiscard]] auto update_dag_cron(const DAGId &dag_id,
                                     std::string_view cron_expr, bool is_active)
      -> Result<void>;

  // Recovery
  [[nodiscard]] auto recover_from_crash() -> Result<void>;

  [[nodiscard]] auto persistence() -> PersistenceService * {
    return persistence_service();
  }
  // Service access
  [[nodiscard]] auto dag_manager() -> DAGManager &;
  [[nodiscard]] auto dag_manager() const -> const DAGManager &;
  [[nodiscard]] auto engine() -> Engine &;
  [[nodiscard]] auto persistence_service() -> PersistenceService *;
  [[nodiscard]] auto api_server() -> ApiServer *;
  [[nodiscard]] auto get_active_dag_run(DAGRunId dag_run_id) -> DAGRun *;
  [[nodiscard]] auto runtime() -> Runtime &;

  // Debug
  auto list_tasks() const -> void;
  auto show_status() const -> void;

private:
  struct RunLaunchPlan {
    DAGId dag_id;
    int64_t dag_rowid{0};
    int version{1};
    std::shared_ptr<const DAG> graph;
    std::shared_ptr<const std::vector<ExecutorConfig>> executor_configs;
    std::shared_ptr<const std::vector<TaskConfig>> indexed_task_configs;
  };

  struct DagOwnerState {
    int active_runs{0};
  };

  struct DagOwnerShardState {
    ankerl::unordered_dense::map<DAGId, DagOwnerState> dags;
  };

  auto setup_callbacks() -> void;
  [[nodiscard]] auto owner_shard(const DAGId &dag_id) const noexcept
      -> shard_id;
  [[nodiscard]] auto owner_shard(const DAGRunId &dag_run_id) const noexcept
      -> shard_id;
  auto trigger_scheduled_on_owner_shard(
      DAGId dag_id,
      std::chrono::system_clock::time_point execution_date) -> spawn_task;
  auto trigger_run_on_dag_owner_shard(
      DAGId dag_id, TriggerType trigger,
      std::optional<std::chrono::system_clock::time_point> execution_date,
      std::chrono::system_clock::time_point request_now)
      -> task<Result<DAGRunId>>;
  auto trigger_run_on_owner_shard(
      RunLaunchPlan plan, TriggerType trigger,
      std::optional<std::chrono::system_clock::time_point> execution_date,
      DAGRunId dag_run_id, std::chrono::system_clock::time_point request_now)
      -> task<Result<DAGRunId>>;
  [[nodiscard]] auto try_acquire_dag_run_slot(const DAGInfo &info)
      -> Result<void>;
  auto release_dag_run_slot(const DAGId &dag_id) -> void;
  auto on_run_finished(const DAGRunId &dag_run_id, DAGRunState status) -> void;
  [[nodiscard]] auto resolve_dag_id(const DAGRunId &dag_run_id) const
      -> std::optional<DAGId>;
  auto spawn_persistence_task(spawn_task task) -> void;
  template <typename OpFactory>
  auto run_persistence_factory(std::shared_ptr<OpFactory> factory,
                               bool count_mysql_write) -> spawn_task {
    auto res = co_await (*factory)();
    record_persistence_result(res, count_mysql_write);
    co_return;
  }
  template <typename OpFactory>
  auto enqueue_persistence(OpFactory &&factory,
                           bool count_mysql_write = true) -> void {
    auto shared_factory =
        std::make_shared<std::decay_t<OpFactory>>(std::forward<OpFactory>(factory));
    spawn_persistence_task(run_persistence_factory(std::move(shared_factory),
                                                  count_mysql_write));
  }
  template <typename T>
  auto record_persistence_result(const Result<T> &result,
                                 bool count_mysql_write = true) -> void {
    if (!result) {
      dropped_persistence_events_.fetch_add(1, std::memory_order_relaxed);
      return;
    }
    if (count_mysql_write) {
      mysql_batch_write_ops_.fetch_add(1, std::memory_order_relaxed);
    }
  }
  auto setup_config_watcher() -> void;
  auto handle_file_change(const std::string &filename) -> void;
  auto get_max_retries(const DAGRunId &dag_run_id, NodeIndex idx) const -> int;
  auto get_retry_interval(const DAGRunId &dag_run_id, NodeIndex idx) const
      -> std::chrono::seconds;

  [[nodiscard]] auto validate_dag_info(const DAGInfo &info) -> Result<void>;
  [[nodiscard]] auto create_dag_atomically(DAGId dag_id, const DAGInfo &info)
      -> Result<void>;
  [[nodiscard]] auto reload_single_dag(const DAGId &dag_id, const DAGInfo &info)
      -> Result<void>;
  std::atomic<bool> running_{false};
  Config config_;

  // Core runtime
  Runtime runtime_{0};
  std::unique_ptr<IExecutor> executor_;

  // Services
  std::atomic<std::uint64_t> dropped_persistence_events_{0};
  std::atomic<std::uint64_t> mysql_batch_write_ops_{0};
  std::unique_ptr<PersistenceService> persistence_;
  std::unique_ptr<SchedulerService> scheduler_;
  std::unique_ptr<ExecutionService> execution_;

  DAGManager dag_manager_;
  std::vector<DagOwnerShardState> dag_owner_states_;

  std::unique_ptr<ApiServer> api_;

  // Config file watching
  std::unique_ptr<ConfigWatcher> config_watcher_;

};

} // namespace dagforge
