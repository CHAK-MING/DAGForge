#pragma once

#include "dagforge/scheduler/event_queue.hpp"
#include "dagforge/scheduler/task.hpp"
#include "dagforge/util/id.hpp"

#include <boost/asio/awaitable.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <optional>
#include <unordered_map>

namespace dagforge {

class Runtime;

// Single-threaded event loop scheduler.
// All state mutations happen on the Engine shard in Runtime.
// External calls communicate via boost::asio::post to the scheduler context.
class Engine {
public:
  using TimePoint = std::chrono::system_clock::time_point;
  using DAGTriggerCallback =
      std::move_only_function<void(const DAGId &, TimePoint execution_date)>;
  using RunExistsCallback =
      std::move_only_function<boost::asio::awaitable<Result<bool>>(
          const DAGId &, TimePoint)>;
  using GetWatermarkCallback = std::move_only_function<
      boost::asio::awaitable<Result<std::optional<TimePoint>>>(const DAGId &)>;
  using SaveWatermarkCallback =
      std::move_only_function<boost::asio::awaitable<Result<void>>(
          const DAGId &, TimePoint)>;

  explicit Engine(Runtime &runtime);
  Engine(Runtime &runtime, boost::asio::io_context &io);
  ~Engine();

  Engine(const Engine &) = delete;
  auto operator=(const Engine &) -> Engine & = delete;

  auto start() -> void;
  auto stop() -> void;
  auto run_loop() -> void;
  [[nodiscard]] auto is_running() const noexcept -> bool {
    return running_.load();
  }

  [[nodiscard]] auto add_task(ExecutionInfo exec_info) -> Result<void>;
  [[nodiscard]] auto remove_task(DAGId dag_id, TaskId task_id) -> Result<void>;

  auto set_on_dag_trigger(DAGTriggerCallback cb) -> void;
  auto set_run_exists_callback(RunExistsCallback cb) -> void;
  auto set_get_watermark_callback(GetWatermarkCallback cb) -> void;
  auto set_save_watermark_callback(SaveWatermarkCallback cb) -> void;

private:
  auto run_cron_task(DAGTaskId dag_task_id, TimePoint first_time)
      -> boost::asio::awaitable<void>;
  auto schedule_task(const DAGTaskId &dag_task_id, TimePoint next_time) -> void;
  auto unschedule_task(const DAGTaskId &dag_task_id) -> void;

  auto handle_event(AddTaskEvent e) -> boost::asio::awaitable<void>;
  auto handle_event(const RemoveTaskEvent &e) -> void;
  auto handle_event(const ShutdownEvent &e) -> void;

  alignas(64) std::atomic<bool> running_{false};
  alignas(64) std::atomic<bool> stopped_{true};
  boost::asio::io_context &io_;
  std::optional<
      boost::asio::executor_work_guard<boost::asio::io_context::executor_type>>
      work_guard_;

  // Accessed only in event loop thread
  std::unordered_map<DAGTaskId, ExecutionInfo> tasks_;
  struct ScheduledTask {
    std::shared_ptr<boost::asio::steady_timer> timer;
    TimePoint next_run_time;
  };
  std::unordered_map<DAGTaskId, ScheduledTask> scheduled_tasks_;

  DAGTriggerCallback on_dag_trigger_;
  RunExistsCallback run_exists_;
  GetWatermarkCallback get_watermark_;
  SaveWatermarkCallback save_watermark_;
};

} // namespace dagforge
