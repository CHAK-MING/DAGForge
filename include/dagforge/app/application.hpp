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
  auto setup_callbacks() -> void;
  auto cache_run_dag_mapping(const DAGRunId &dag_run_id,
                             const DAGId &dag_id) const -> void;
  [[nodiscard]] auto resolve_dag_id_cached(const DAGRunId &dag_run_id) const
      -> std::optional<DAGId>;
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

  std::unique_ptr<ApiServer> api_;

  // Config file watching
  std::unique_ptr<ConfigWatcher> config_watcher_;

  using RunDagCache = ankerl::unordered_dense::map<DAGRunId, DAGId>;
  mutable std::atomic<std::shared_ptr<const RunDagCache>> run_dag_cache_state_{
      std::make_shared<RunDagCache>()};
  static constexpr std::size_t kMaxRunDagCacheEntries = 65536;
};

} // namespace dagforge
