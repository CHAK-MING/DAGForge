#include "dagforge/scheduler/engine.hpp"

#include "dagforge/core/runtime.hpp"
#include "dagforge/scheduler/cron.hpp"
#include "dagforge/scheduler/task.hpp"
#include "dagforge/util/id.hpp"
#include "dagforge/util/log.hpp"

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <algorithm>
#include <chrono>
#include <expected>
#include <optional>
#include <system_error>
#include <thread>
#include <utility>
#include <variant>

namespace dagforge {

Engine::Engine(Runtime &runtime)
    : Engine(runtime, runtime.shard(shard_id{0}).ctx()) {}

Engine::Engine(Runtime &runtime, boost::asio::io_context &io) : io_(io) {
  (void)runtime;
}

Engine::~Engine() { stop(); }

auto Engine::start() -> void {
  if (running_.exchange(true)) {
    return;
  }

  work_guard_.emplace(boost::asio::make_work_guard(io_));

  stopped_.store(false, std::memory_order_release);
  log::info("Engine started");
}

auto Engine::stop() -> void {
  if (!running_.exchange(false)) {
    return;
  }

  boost::asio::post(io_, [this] {
    handle_event(ShutdownEvent{});

    for (auto &[id, scheduled] : scheduled_tasks_) {
      (void)id;
      if (scheduled.timer) {
        scheduled.timer->cancel();
      }
    }
    scheduled_tasks_.clear();

    if (work_guard_.has_value()) {
      work_guard_->reset();
    }

    stopped_.store(true, std::memory_order_release);
    stopped_.notify_all();
  });

  constexpr auto kStopTimeout = std::chrono::seconds(5);
  const auto deadline = std::chrono::steady_clock::now() + kStopTimeout;
  while (!stopped_.load(std::memory_order_acquire)) {
    if (std::chrono::steady_clock::now() >= deadline) {
      log::warn("Engine stop timed out waiting for scheduler loop shutdown");
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

  log::info("Engine stopped");
}

auto Engine::run_loop() -> void {
  // Compatibility shim: Engine is now driven by Runtime's io_context threads.
  while (running_.load(std::memory_order_acquire)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
}

auto Engine::run_cron_task(DAGTaskId dag_task_id, TimePoint first_time)
    -> boost::asio::awaitable<void> {
  auto it = scheduled_tasks_.find(dag_task_id);
  if (it == scheduled_tasks_.end() || !it->second.timer) {
    co_return;
  }
  auto timer = it->second.timer;
  auto next_time = first_time;

  while (running_.load(std::memory_order_acquire)) {
    auto task_it = tasks_.find(dag_task_id);
    auto scheduled_it = scheduled_tasks_.find(dag_task_id);
    if (task_it == tasks_.end() || scheduled_it == scheduled_tasks_.end() ||
        scheduled_it->second.timer.get() != timer.get()) {
      co_return;
    }

    scheduled_it->second.next_run_time = next_time;

    const auto now = std::chrono::system_clock::now();
    auto delay = next_time - now;
    if (delay < std::chrono::milliseconds(0)) {
      delay = std::chrono::milliseconds(0);
    }
    timer->expires_after(
        std::chrono::duration_cast<boost::asio::steady_timer::duration>(delay));

    boost::system::error_code ec;
    co_await timer->async_wait(
        boost::asio::redirect_error(boost::asio::use_awaitable, ec));
    if (ec == boost::asio::error::operation_aborted) {
      co_return;
    }
    if (ec) {
      log::warn("Scheduler timer wait failed for {}: {}", dag_task_id,
                ec.message());
      co_return;
    }

    if (on_dag_trigger_) {
      log::info("Cron triggered DAG: {} for execution_date: {}",
                task_it->second.dag_id,
                std::chrono::duration_cast<std::chrono::seconds>(
                    next_time.time_since_epoch())
                    .count());
      on_dag_trigger_(task_it->second.dag_id, next_time);

      if (save_watermark_) {
        auto wm_res =
            co_await save_watermark_(task_it->second.dag_id, next_time);
        wm_res.or_else([&](std::error_code err) -> Result<void> {
          log::error("Failed to save watermark for DAG {}: {}",
                     task_it->second.dag_id, err.message());
          return fail(err);
        });
      }
    }

    if (!task_it->second.cron_expr.has_value()) {
      scheduled_tasks_.erase(dag_task_id);
      co_return;
    }

    auto next_run = task_it->second.cron_expr->next_after(next_time);
    if (task_it->second.end_date.has_value() &&
        next_run > *task_it->second.end_date) {
      log::info("DAG {} finished: next run time exceeds end_date",
                task_it->second.dag_id);
      scheduled_tasks_.erase(dag_task_id);
      co_return;
    }
    next_time = next_run;
  }
}

auto Engine::schedule_task(const DAGTaskId &dag_task_id, TimePoint next_time)
    -> void {
  unschedule_task(dag_task_id);

  auto timer = std::make_shared<boost::asio::steady_timer>(io_);
  scheduled_tasks_[dag_task_id] =
      ScheduledTask{.timer = std::move(timer), .next_run_time = next_time};
  boost::asio::co_spawn(io_, run_cron_task(dag_task_id, next_time),
                        boost::asio::detached);
}

auto Engine::unschedule_task(const DAGTaskId &dag_task_id) -> void {
  auto it = scheduled_tasks_.find(dag_task_id);
  if (it == scheduled_tasks_.end()) {
    return;
  }

  if (it->second.timer) {
    it->second.timer->cancel();
  }
  scheduled_tasks_.erase(it);
}

auto Engine::add_task(ExecutionInfo exec_info) -> Result<void> {
  if (!running_.load(std::memory_order_acquire)) {
    return fail(Error::SystemNotRunning);
  }

  auto event = AddTaskEvent{.exec_info = std::move(exec_info)};
  boost::asio::co_spawn(io_, handle_event(std::move(event)),
                        boost::asio::detached);

  return ok();
}

auto Engine::remove_task(DAGId dag_id, TaskId task_id) -> Result<void> {
  if (!running_.load(std::memory_order_acquire)) {
    return fail(Error::SystemNotRunning);
  }

  boost::asio::post(io_, [this, event = RemoveTaskEvent{
                                    .dag_id = std::move(dag_id),
                                    .task_id = std::move(task_id)}]() mutable {
    handle_event(event);
  });

  return ok();
}

auto Engine::set_on_dag_trigger(DAGTriggerCallback cb) -> void {
  on_dag_trigger_ = std::move(cb);
}

auto Engine::set_run_exists_callback(RunExistsCallback cb) -> void {
  run_exists_ = std::move(cb);
}

auto Engine::set_get_watermark_callback(GetWatermarkCallback cb) -> void {
  get_watermark_ = std::move(cb);
}

auto Engine::set_save_watermark_callback(SaveWatermarkCallback cb) -> void {
  save_watermark_ = std::move(cb);
}

auto Engine::handle_event(AddTaskEvent e) -> boost::asio::awaitable<void> {
  auto id = generate_dag_task_id(e.exec_info.dag_id, e.exec_info.task_id);

  if (tasks_.count(id)) {
    co_return;
  }

  tasks_.emplace(id, e.exec_info);

  if (e.exec_info.cron_expr.has_value()) {
    auto now = std::chrono::system_clock::now();
    const auto &cron = e.exec_info.cron_expr.value();

    TimePoint baseline_time = now;
    if (e.exec_info.start_date.has_value()) {
      baseline_time = *e.exec_info.start_date;
    }

    if (get_watermark_) {
      auto watermark_res = co_await get_watermark_(e.exec_info.dag_id);
      if (watermark_res && watermark_res->has_value()) {
        baseline_time = **watermark_res;
      }
    }

    TimePoint effective_baseline = baseline_time;

    if (e.exec_info.catchup) {
      auto next_run = cron.next_after(effective_baseline);
      while (next_run <= now) {
        if (e.exec_info.end_date.has_value() &&
            next_run > *e.exec_info.end_date) {
          break;
        }

        bool exists = false;
        if (run_exists_) {
          auto res = co_await run_exists_(e.exec_info.dag_id, next_run);
          if (!res) {
            log::error("Failed to check run existence for DAG {}: {}",
                       e.exec_info.dag_id, res.error().message());
            exists = true;
          } else {
            exists = *res;
          }
        }

        if (!exists && on_dag_trigger_) {
          log::info("Catchup triggering DAG: {} for execution_date: {}",
                    e.exec_info.dag_id,
                    std::chrono::duration_cast<std::chrono::seconds>(
                        next_run.time_since_epoch())
                        .count());
          on_dag_trigger_(e.exec_info.dag_id, next_run);

          if (save_watermark_) {
            auto wm_res =
                co_await save_watermark_(e.exec_info.dag_id, next_run);
            wm_res.or_else([&](std::error_code ec) -> Result<void> {
              log::error("Failed to save watermark for DAG {}: {}",
                         e.exec_info.dag_id, ec.message());
              return fail(ec);
            });
          }
        }

        effective_baseline = next_run;
        next_run = cron.next_after(effective_baseline);
      }
    }

    auto next_time = cron.next_after(std::max(effective_baseline, now));

    if (e.exec_info.end_date.has_value() && next_time > *e.exec_info.end_date) {
      if (scheduled_tasks_.find(id) == scheduled_tasks_.end()) {
        log::info("DAG {} not scheduled: next run time exceeds end_date",
                  e.exec_info.dag_id);
        co_return;
      }
    } else {
      schedule_task(id, next_time);
    }

    log::info("DAG : {}, Task added: {}, next scheduled at: {}",
              e.exec_info.dag_id, e.exec_info.task_id,
              std::chrono::duration_cast<std::chrono::seconds>(
                  next_time.time_since_epoch())
                  .count());
  }
}

auto Engine::handle_event(const RemoveTaskEvent &e) -> void {
  auto id = generate_dag_task_id(e.dag_id, e.task_id);
  unschedule_task(id);
  tasks_.erase(id);
  log::info("DAG: {}, Task removed: {}", e.dag_id, e.task_id);
}

auto Engine::handle_event(const ShutdownEvent &) -> void {
  running_.store(false, std::memory_order_release);
}

} // namespace dagforge
