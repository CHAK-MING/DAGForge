#pragma once

#include "dagforge/core/coroutine.hpp"
#include "dagforge/core/error.hpp"
#include "dagforge/dag/dag_manager.hpp"
#include "dagforge/executor/executor.hpp"
#include "dagforge/scheduler/engine.hpp"

#include <atomic>
#include <cstdint>
#include <unordered_map>

namespace dagforge {

class DAGManager;

using DAGTriggerCallback = std::move_only_function<void(
    const DAGId &, std::chrono::system_clock::time_point)>;
using RunExistsCallback = Engine::RunExistsCallback;
using GetWatermarkCallback = Engine::GetWatermarkCallback;
using SaveWatermarkCallback = Engine::SaveWatermarkCallback;
using ZombieReaperCallback =
    std::move_only_function<task<Result<std::size_t>>(std::int64_t)>;

class SchedulerService {
public:
  explicit SchedulerService(Runtime &runtime);
  ~SchedulerService() = default;

  SchedulerService(const SchedulerService &) = delete;
  auto operator=(const SchedulerService &) -> SchedulerService & = delete;

  auto set_on_dag_trigger(DAGTriggerCallback callback) -> void;
  auto set_run_exists_callback(RunExistsCallback callback) -> void;
  auto set_get_watermark_callback(GetWatermarkCallback callback) -> void;
  auto set_save_watermark_callback(SaveWatermarkCallback callback) -> void;
  auto set_zombie_reaper_callback(ZombieReaperCallback callback) -> void;
  auto set_zombie_reaper_config(int interval_sec, int heartbeat_timeout_sec)
      -> void;

  auto register_dag(DAGId dag_id, const DAGInfo &dag_info) -> void;

  auto unregister_dag(const DAGId &dag_id) -> void;

  auto start() -> void;
  auto stop() -> void;
  [[nodiscard]] auto is_running() const -> bool;

  [[nodiscard]] auto engine() -> Engine &;

private:
  Runtime &runtime_;
  Engine engine_;
  DAGTriggerCallback on_dag_trigger_;
  ZombieReaperCallback zombie_reaper_callback_;
  int zombie_reaper_interval_sec_{60};
  int zombie_heartbeat_timeout_sec_{75};
  std::atomic<bool> zombie_reaper_running_{false};
  std::atomic<int> zombie_reaper_inflight_{0};
  std::unordered_map<DAGId, TaskId> registered_root_tasks_;
};

} // namespace dagforge
