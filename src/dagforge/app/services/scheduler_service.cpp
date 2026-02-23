#include "dagforge/app/services/scheduler_service.hpp"

#include "dagforge/dag/dag_manager.hpp"
#include "dagforge/scheduler/cron.hpp"
#include "dagforge/util/log.hpp"

#include <algorithm>
#include <chrono>
#include <optional>
#include <thread>

namespace dagforge {

SchedulerService::SchedulerService(Runtime &runtime)
    : runtime_(runtime), engine_(runtime) {}

auto SchedulerService::set_on_dag_trigger(DAGTriggerCallback callback) -> void {
  on_dag_trigger_ = std::move(callback);
  engine_.set_on_dag_trigger(
      [this](const DAGId &dag_id,
             std::chrono::system_clock::time_point execution_date) {
        if (on_dag_trigger_) {
          on_dag_trigger_(dag_id, execution_date);
        }
      });
}

auto SchedulerService::set_run_exists_callback(RunExistsCallback callback)
    -> void {
  engine_.set_run_exists_callback(std::move(callback));
}

auto SchedulerService::set_get_watermark_callback(GetWatermarkCallback callback)
    -> void {
  engine_.set_get_watermark_callback(std::move(callback));
}

auto SchedulerService::set_save_watermark_callback(
    SaveWatermarkCallback callback) -> void {
  engine_.set_save_watermark_callback(std::move(callback));
}

auto SchedulerService::set_zombie_reaper_callback(ZombieReaperCallback callback)
    -> void {
  zombie_reaper_callback_ = std::move(callback);
}

auto SchedulerService::set_zombie_reaper_config(int interval_sec,
                                                int heartbeat_timeout_sec)
    -> void {
  zombie_reaper_interval_sec_ = interval_sec;
  zombie_heartbeat_timeout_sec_ = heartbeat_timeout_sec;
}

auto SchedulerService::register_dag(DAGId dag_id, const DAGInfo &dag_info)
    -> void {
  if (dag_info.cron.empty()) {
    return;
  }
  if (!engine_.is_running()) {
    return;
  }

  auto cron_expr_result = CronExpr::parse(dag_info.cron);
  if (!cron_expr_result) {
    log::warn("Invalid cron expression for DAG {}: {}", dag_id, dag_info.cron);
    return;
  }

  TaskId schedule_task_id{"__schedule__"};
  ExecutionInfo info{
      .dag_id = dag_id,
      .task_id = schedule_task_id,
      .name = dag_info.name,
      .cron_expr = std::make_optional(std::move(*cron_expr_result)),
      .start_date = dag_info.start_date,
      .end_date = dag_info.end_date,
      .catchup = dag_info.catchup,
  };

  engine_.add_task(std::move(info))
      .and_then([&]() -> Result<void> {
        log::info("Registered DAG schedule: {} with cron: {}", dag_id,
                  dag_info.cron);
        registered_root_tasks_[dag_id] = schedule_task_id;
        return ok();
      })
      .or_else([&](std::error_code ec) -> Result<void> {
        log::warn("Failed to register DAG schedule: {} - error: {}", dag_id,
                  ec.message());
        return ok();
      });
}

auto SchedulerService::unregister_dag(const DAGId &dag_id) -> void {
  auto it = registered_root_tasks_.find(dag_id);
  if (it == registered_root_tasks_.end()) {
    return;
  }

  engine_.remove_task(dag_id, it->second)
      .and_then([&]() -> Result<void> {
        log::info("Unregistered DAG schedule: {}", dag_id);
        return ok();
      })
      .or_else([&](std::error_code ec) -> Result<void> {
        log::warn("Failed to unregister DAG schedule: {} - error: {}", dag_id,
                  ec.message());
        return ok(); // Non-fatal for unregister
      });
  registered_root_tasks_.erase(it);
}

auto SchedulerService::start() -> void {
  engine_.start();

  if (zombie_reaper_interval_sec_ <= 0 || zombie_heartbeat_timeout_sec_ <= 0) {
    zombie_reaper_running_.store(false, std::memory_order_release);
    return;
  }
  zombie_reaper_running_.store(true, std::memory_order_release);

  auto reaper_loop = [](SchedulerService *self) -> task<void> {
    while (self->zombie_reaper_running_.load(std::memory_order_acquire)) {
      try {
        co_await async_sleep(
            std::chrono::seconds(self->zombie_reaper_interval_sec_));
      } catch (const std::exception &) {
        // Runtime shutdown cancels pending timers; exit quietly.
        break;
      }
      if (!self->zombie_reaper_running_.load(std::memory_order_acquire)) {
        break;
      }
      if (!self->zombie_reaper_callback_) {
        continue;
      }

      const auto timeout_ms = static_cast<std::int64_t>(
          self->zombie_heartbeat_timeout_sec_ * 1000LL);
      Result<std::size_t> reaped = fail(Error::Cancelled);
      self->zombie_reaper_inflight_.fetch_add(1, std::memory_order_acq_rel);
      try {
        reaped = co_await self->zombie_reaper_callback_(timeout_ms);
      } catch (const std::exception &) {
        self->zombie_reaper_inflight_.fetch_sub(1, std::memory_order_acq_rel);
        break;
      }
      self->zombie_reaper_inflight_.fetch_sub(1, std::memory_order_acq_rel);
      if (!reaped) {
        log::warn("Zombie reaper failed: {}", reaped.error().message());
        continue;
      }
      if (*reaped > 0) {
        log::warn("Zombie reaper marked {} task instance(s) failed", *reaped);
      }
    }
  };
  runtime_.spawn_external(reaper_loop(this));
}

auto SchedulerService::stop() -> void {
  zombie_reaper_running_.store(false, std::memory_order_release);

  // Best-effort drain: avoid tearing down persistence/runtime while a reaper
  // callback is still awaiting DB I/O (prevents shutdown races in parallel
  // integration tests).
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::milliseconds(200);
  while (zombie_reaper_inflight_.load(std::memory_order_acquire) > 0 &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(2));
  }

  engine_.stop();
}

auto SchedulerService::is_running() const -> bool {
  return engine_.is_running();
}

auto SchedulerService::engine() -> Engine & { return engine_; }

} // namespace dagforge
