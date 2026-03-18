#include "dagforge/app/application.hpp"

#include "dagforge/app/api/api_server.hpp"
#include "dagforge/app/services/execution_service.hpp"
#include "dagforge/app/services/persistence_service.hpp"
#include "dagforge/app/services/scheduler_service.hpp"
#include "dagforge/config/config_watcher.hpp"
#include "dagforge/config/dag_definition.hpp"
#include "dagforge/config/dag_file_loader.hpp"
#include "dagforge/core/error.hpp"
#include "dagforge/dag/dag_validator.hpp"
#include "dagforge/io/context.hpp"
#include "dagforge/util/hash.hpp"
#include "dagforge/util/json.hpp"
#include "dagforge/util/log.hpp"

#include <atomic>
#include <chrono>
#include <filesystem>
#include <print>
#include <ranges>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/use_future.hpp>
#include <csignal>

#include "dagforge/app/config_builder.hpp"
#include "dagforge/executor/composite_executor.hpp"
#include "dagforge/util/time.hpp"

namespace dagforge {
namespace {

template <typename T>
auto sync_wait_on_runtime(Runtime &runtime, task<Result<T>> op) -> Result<T> {
  static std::atomic<std::uint64_t> rr{0};
  const auto shard =
      static_cast<shard_id>(rr.fetch_add(1, std::memory_order_relaxed) %
                            std::max(1u, runtime.shard_count()));
  auto fut = boost::asio::co_spawn(runtime.shard(shard).ctx(), std::move(op),
                                   boost::asio::use_future);
  try {
    return fut.get();
  } catch (const std::future_error &e) {
    log::error("Runtime sync_wait failed: {}", e.what());
    return fail(Error::Unknown);
  } catch (const std::exception &e) {
    log::error("Runtime sync_wait failed: {}", e.what());
    return fail(Error::Unknown);
  }
}

auto sync_wait_on_runtime(Runtime &runtime, task<void> op) -> void {
  static std::atomic<std::uint64_t> rr{0};
  const auto shard =
      static_cast<shard_id>(rr.fetch_add(1, std::memory_order_relaxed) %
                            std::max(1u, runtime.shard_count()));
  auto fut = boost::asio::co_spawn(runtime.shard(shard).ctx(), std::move(op),
                                   boost::asio::use_future);
  try {
    fut.get();
  } catch (const std::future_error &e) {
    log::error("Runtime sync_wait(void) failed: {}", e.what());
  } catch (const std::exception &e) {
    log::error("Runtime sync_wait(void) failed: {}", e.what());
  }
}

template <typename T>
auto sync_wait_on_runtime(const Runtime &runtime, task<Result<T>> op)
    -> Result<T> {
  return sync_wait_on_runtime(const_cast<Runtime &>(runtime), std::move(op));
}

[[nodiscard]] auto build_indexed_task_configs(const DAGInfo &info, const DAG &graph)
    -> std::vector<TaskConfig> {
  std::vector<TaskConfig> task_cfgs(info.tasks.size());
  for (const auto &task : info.tasks) {
    NodeIndex idx = graph.get_index(task.task_id);
    if (idx != kInvalidNode && idx < task_cfgs.size()) {
      task_cfgs[idx] = task;
    }
  }
  return task_cfgs;
}

[[nodiscard]] auto has_unresolved_task_rowids(
    const std::vector<TaskConfig> &task_cfgs) -> bool {
  return std::ranges::any_of(task_cfgs, [](const TaskConfig &task) {
    return !task.task_id.empty() && task.task_rowid <= 0;
  });
}

} // namespace

Application::Application()
    : runtime_(config_.scheduler.shards, config_.scheduler.pin_shards_to_cores,
               static_cast<unsigned>(config_.scheduler.cpu_affinity_offset)),
      executor_(create_composite_executor(runtime_)),
      persistence_(
          std::make_unique<PersistenceService>(runtime_, config_.database)),
      scheduler_(std::make_unique<SchedulerService>(
          runtime_, static_cast<unsigned>(config_.scheduler.scheduler_shards))),
      execution_(std::make_unique<ExecutionService>(runtime_, *executor_)),
      dag_owner_states_(
          std::clamp(static_cast<unsigned>(config_.scheduler.scheduler_shards),
                     1U, std::max(1U, runtime_.shard_count()))) {
  std::signal(SIGPIPE, SIG_IGN);
  dag_manager_.set_persistence_service(persistence_.get());
  dag_manager_.set_runtime(&runtime_);
  execution_->set_max_concurrency(config_.scheduler.max_concurrency);
  setup_callbacks();
}

Application::Application(Config config)
    : config_(std::move(config)),
      runtime_(config_.scheduler.shards, config_.scheduler.pin_shards_to_cores,
               static_cast<unsigned>(config_.scheduler.cpu_affinity_offset)),
      executor_(create_composite_executor(runtime_)),
      persistence_(
          std::make_unique<PersistenceService>(runtime_, config_.database)),
      scheduler_(std::make_unique<SchedulerService>(
          runtime_, static_cast<unsigned>(config_.scheduler.scheduler_shards))),
      execution_(std::make_unique<ExecutionService>(runtime_, *executor_)),
      dag_owner_states_(
          std::clamp(static_cast<unsigned>(config_.scheduler.scheduler_shards),
                     1U, std::max(1U, runtime_.shard_count()))) {
  std::signal(SIGPIPE, SIG_IGN);
  dag_manager_.set_persistence_service(persistence_.get());
  dag_manager_.set_runtime(&runtime_);
  execution_->set_max_concurrency(config_.scheduler.max_concurrency);
  setup_callbacks();
}

Application::~Application() { stop(); }

auto Application::load_config(std::string_view path) -> Result<void> {
  return ConfigLoader::load_from_file(path).transform([this](Config &&cfg) {
    config_ = std::move(cfg);
    if (execution_) {
      execution_->set_max_concurrency(config_.scheduler.max_concurrency);
    }
    return;
  });
}

auto Application::config() const noexcept -> const Config & { return config_; }

auto Application::config() noexcept -> Config & { return config_; }

auto Application::init() -> Result<void> {
  return ok()
      .and_then([this]() -> Result<bool> {
        if (!config_.dag_source.directory.empty()) {
          return load_dags_from_directory(config_.dag_source.directory);
        }
        return ok(false);
      })
      .and_then([](bool /*directory_loaded*/) -> Result<void> { return ok(); })
      .and_then([this]() -> Result<void> {
        if (!api_) {
          api_ = std::make_unique<ApiServer>(*this);
        }
        return ok();
      });
}

auto Application::init_db_only() -> Result<void> {
  if (running_.exchange(true)) {
    return ok();
  }

  auto runtime_res = runtime_.start();
  if (!runtime_res) {
    running_ = false;
    return fail(runtime_res.error());
  }

  if (persistence_ && !persistence_->is_open()) {
    if (auto open_res = persistence_->sync_wait(persistence_->open());
        !open_res) {
      running_ = false;
      runtime_.stop();
      return fail(open_res.error());
    }
  }

  if (persistence_ && persistence_->is_open()) {
    persistence_->sync_wait(persistence_->close());
  }

  runtime_.stop();
  running_ = false;
  return ok();
}

auto Application::load_dags_from_directory(std::string_view dags_dir)
    -> Result<bool> {
  DAGFileLoader loader(dags_dir);
  return loader.load_all().and_then([&](std::vector<DAGFile> &&dags)
                                        -> Result<bool> {
    if (dags.empty()) {
      return ok(false);
    }

    dag_manager_.clear_all();

    for (const auto &dag_file : dags) {
      const auto &dag_id = dag_file.dag_id;
      const auto &info = dag_file.definition;

      auto result =
          validate_dag_info(info)
              .and_then([&]() { return create_dag_atomically(dag_id, info); })
              .transform([&]() {
                log::debug("Loaded DAG {} with {} tasks from {}", dag_id,
                           info.tasks.size(), dags_dir);
              })
              .or_else([&](std::error_code ec) -> Result<void> {
                log::error("Failed to create DAG {}: {}", dag_id, ec.message());
                return ok();
              });
    }

    return ok(true);
  });
}

auto Application::get_run_state(const DAGRunId &dag_run_id) const
    -> Result<DAGRunState> {
  return sync_wait_on_runtime(runtime_, get_run_state_async(dag_run_id));
}

auto Application::get_run_state_async(const DAGRunId &dag_run_id) const
    -> task<Result<DAGRunState>> {
  if (!execution_) {
    co_return fail(Error::NotFound);
  }

  auto *run = execution_->get_run(dag_run_id);
  if (run) {
    co_return ok(run->state());
  }

  if (!persistence_) {
    co_return fail(Error::NotFound);
  }

  co_return co_await persistence_->get_dag_run_state(dag_run_id);
}

auto Application::start() -> Result<void> {
  if (running_.exchange(true))
    return ok();

  auto runtime_res = runtime_.start();
  if (!runtime_res) {
    running_ = false;
    return fail(runtime_res.error());
  }

  log::start();
  log::info("Runtime started");

  if (persistence_ && !persistence_->is_open()) {
    if (auto open_res = persistence_->sync_wait(persistence_->open());
        !open_res) {
      running_ = false;
      return fail(open_res.error());
    }
  }

  // Re-load file-backed DAGs after persistence is open so DAG/task rowids are
  // materialized before the first trigger hits the hot path.
  if (!config_.dag_source.directory.empty()) {
    if (auto load_res = load_dags_from_directory(config_.dag_source.directory);
        !load_res) {
      running_ = false;
      return fail(load_res.error());
    }
  }

  if (auto load_res = dag_manager_.load_from_database(); !load_res) {
    running_ = false;
    return fail(load_res.error());
  }

  scheduler_->start();

  for (const auto &d :
       dag_manager_.list_dags() | std::views::filter([](const auto &dag) {
         return !dag.cron.empty();
       })) {
    scheduler_->register_dag(d.dag_id, d);
  }

  if (config_.api.enabled) {
    if (!api_)
      api_ = std::make_unique<ApiServer>(*this);
    if (auto api_res = api_->start(); !api_res) {
      if (api_) {
        api_->stop();
      }
      scheduler_->stop();
      if (persistence_ && persistence_->is_open()) {
        persistence_->sync_wait(persistence_->close());
      }
      runtime_.stop();
      running_ = false;
      return fail(api_res.error());
    }
    log::info("API server started on {}:{}", config_.api.host,
              config_.api.port);
  }

  setup_config_watcher();
  return ok();
}

auto Application::stop() noexcept -> void {
  if (!running_.exchange(false))
    return;

  log::info("Stopping DAGForge...");

  // Stop config watcher to prevent new DAG changes.
  if (config_watcher_) {
    config_watcher_->stop();
    config_watcher_.reset();
  }

  // Stop API server to reject new external requests.
  if (api_)
    api_->stop();

  // Stop scheduler to prevent new scheduled tasks.
  scheduler_->stop();

  // Wait for running tasks with a bounded shutdown budget.
  if (execution_ &&
      (execution_->coro_count() > 0 || execution_->has_active_runs())) {
    sync_wait_on_runtime(
        runtime_, wait_for_completion_async(3000)); // 3 s shutdown budget
    if (execution_->has_active_runs()) {
      log::warn("Shutdown timeout: {} run(s) still active",
                execution_->coro_count());
    }
  }
  // Close persistence while runtime is still alive.
  if (persistence_) {
    persistence_->sync_wait(persistence_->close());
  }

  // Stop runtime after background coroutines are asked to stop.
  runtime_.stop();

  // Cleanup.
  api_.reset();
  execution_.reset();
  executor_.reset();

  log::info("DAGForge stopped");
}

auto Application::is_running() const noexcept -> bool {
  return running_.load();
}

auto Application::spawn_persistence_task(spawn_task task) -> void {
  if (runtime_.is_current_shard()) {
    runtime_.spawn(std::move(task));
  } else {
    runtime_.spawn_external(std::move(task));
  }
}

auto Application::setup_callbacks() -> void {
  ExecutionCallbacks callbacks;
  callbacks.on_task_status = [this](const DAGRunId &dag_run_id,
                                    const TaskId &task_id, TaskState status) {
    if (!api_) {
      return;
    }
    const auto dag_id = resolve_dag_id(dag_run_id).value_or(DAGId{});
    auto now = util::to_unix_millis(std::chrono::system_clock::now());
    JsonValue j = {{"type", "task_status_changed"},
                   {"dag_id", dag_id.value()},
                   {"dag_run_id", dag_run_id.value()},
                   {"run_id", dag_run_id.value()},
                   {"task_id", task_id.value()},
                   {"status", enum_to_string(status)},
                   {"timestamp", now}};
    http::WebSocketHub::EventMessage ev;
    ev.timestamp = util::format_timestamp();
    ev.event = "task_status_changed";
    ev.dag_run_id = dag_run_id.str();
    ev.task_id = task_id.str();
    ev.data = dump_json(j);
    api_->websocket_hub().broadcast_event(ev);
  };

  callbacks.on_run_status = [this](const DAGRunId &dag_run_id,
                                   DAGRunState status) {
    on_run_finished(dag_run_id, status);
    if (!api_) {
      return;
    }
    const auto dag_id = resolve_dag_id(dag_run_id).value_or(DAGId{});
    auto now = util::to_unix_millis(std::chrono::system_clock::now());
    JsonValue j = {
        {"type", "dag_run_completed"},      {"dag_id", dag_id.value()},
        {"dag_run_id", dag_run_id.value()}, {"run_id", dag_run_id.value()},
        {"status", enum_to_string(status)}, {"timestamp", now}};
    http::WebSocketHub::EventMessage ev;
    ev.timestamp = util::format_timestamp();
    ev.event = "dag_run_completed";
    ev.dag_run_id = dag_run_id.str();
    ev.data = dump_json(j);
    api_->websocket_hub().broadcast_event(ev);
  };

  callbacks.on_log = [this](const DAGRunId &dag_run_id, const TaskId &task_id,
                            int attempt, std::string_view stream,
                            std::string_view msg) {
    if (api_) {
      api_->websocket_hub().broadcast_log(
          http::WebSocketHub::LogMessage{.timestamp = util::format_timestamp(),
                                         .dag_run_id = dag_run_id.str(),
                                         .task_id = task_id.str(),
                                         .stream = std::string(stream),
                                         .content = std::string(msg)});
    }
    if (!persistence_) {
      return;
    }
    auto persisted_run_id = dag_run_id.clone();
    auto persisted_task_id = task_id.clone();
    std::string persisted_stream{stream};
    std::string persisted_msg{msg};
    enqueue_persistence(
        [this, run_id = std::move(persisted_run_id),
         log_task_id = std::move(persisted_task_id), attempt,
         log_stream = std::move(persisted_stream),
         log_msg = std::move(persisted_msg)]() mutable -> task<Result<void>> {
          auto owned_run_id = std::move(run_id);
          auto owned_task_id = std::move(log_task_id);
          auto owned_stream = std::move(log_stream);
          auto owned_msg = std::move(log_msg);
          co_return co_await persistence_->append_task_log(
              owned_run_id, owned_task_id, attempt, std::move(owned_stream),
              std::move(owned_msg));
        },
        false);
  };

  // Persistence events go directly to PersistenceService async APIs.
  callbacks.on_persist_run = [this](std::shared_ptr<const DAGRun> run) {
    if (!persistence_) {
      return;
    }
    enqueue_persistence(
        [this, persisted_run = std::move(run)]() mutable -> task<Result<int64_t>> {
          auto owned_run = std::move(persisted_run);
          co_return co_await persistence_->save_dag_run(*owned_run);
    });
  };

  callbacks.on_persist_task = [this](const DAGRunId &dag_run_id,
                                     const TaskInstanceInfo &info) {
    if (!persistence_) {
      return;
    }
    auto persisted_run_id = dag_run_id.clone();
    TaskInstanceInfo persisted_info = info;
    enqueue_persistence(
        [this, run_id = std::move(persisted_run_id),
         task_info = std::move(persisted_info)]() mutable -> task<Result<void>> {
          auto owned_run_id = std::move(run_id);
          auto owned_task_info = std::move(task_info);
          co_return co_await persistence_->update_task_instance(owned_run_id,
                                                                owned_task_info);
        });
  };

  callbacks.on_persist_xcom = [this](const DAGRunId &dag_run_id,
                                     const TaskId &task_id,
                                     std::string_view key,
                                     const JsonValue &value) {
    if (!persistence_) {
      return;
    }
    auto persisted_run_id = dag_run_id.clone();
    auto persisted_task_id = task_id.clone();
    std::string persisted_key{key};
    JsonValue persisted_value = value;
    enqueue_persistence(
        [this, run_id = std::move(persisted_run_id),
         xcom_task_id = std::move(persisted_task_id),
         xcom_key = std::move(persisted_key),
         xcom_value = std::move(persisted_value)]() mutable -> task<Result<void>> {
          auto owned_run_id = std::move(run_id);
          auto owned_task_id = std::move(xcom_task_id);
          auto owned_key = std::move(xcom_key);
          auto owned_value = std::move(xcom_value);
          co_return co_await persistence_->save_xcom(
              owned_run_id, owned_task_id, std::move(owned_key), owned_value);
        });
  };

  callbacks.get_xcom = [this](const DAGRunId &dag_run_id, const TaskId &task_id,
                              std::string_view key) -> task<Result<XComEntry>> {
    if (!persistence_)
      co_return fail(Error::NotFound);
    co_return co_await persistence_->get_xcom(dag_run_id, task_id, key);
  };

  callbacks.get_task_xcoms =
      [this](const DAGRunId &dag_run_id,
             const TaskId &task_id) -> task<Result<std::vector<XComEntry>>> {
    if (!persistence_)
      co_return fail(Error::NotFound);
    co_return co_await persistence_->get_task_xcoms(dag_run_id, task_id);
  };

  callbacks.get_run_xcoms = [this](const DAGRunId &dag_run_id)
      -> task<Result<std::vector<XComTaskEntry>>> {
    if (!persistence_)
      co_return fail(Error::NotFound);
    co_return co_await persistence_->get_run_xcoms(dag_run_id);
  };

  callbacks.get_dag_id_by_run =
      [this](const DAGRunId &dag_run_id) -> task<Result<DAGId>> {
    if (!persistence_)
      co_return fail(Error::NotFound);
    auto entry = co_await persistence_->get_run_history(dag_run_id);
    if (!entry)
      co_return fail(entry.error());
    co_return ok(entry->dag_id.clone());
  };

  callbacks.get_max_retries = [this](const DAGRunId &dag_run_id,
                                     NodeIndex idx) {
    return get_max_retries(dag_run_id, idx);
  };

  callbacks.get_retry_interval = [this](const DAGRunId &dag_run_id,
                                        NodeIndex idx) {
    return get_retry_interval(dag_run_id, idx);
  };

  callbacks.check_previous_task_state =
      [this](const DAGRunId &dag_run_id, NodeIndex idx,
             std::chrono::system_clock::time_point execution_date,
             const DAGRunId &current_dag_run_id) -> task<Result<TaskState>> {
    if (!persistence_)
      co_return fail(Error::NotFound);
    auto entry = co_await persistence_->get_run_history(dag_run_id);
    if (!entry)
      co_return fail(entry.error());
    auto dag_id = entry->dag_id.clone();
    auto dag_info = dag_manager_.get_dag(dag_id);
    if (!dag_info || idx >= dag_info->tasks.size())
      co_return fail(Error::NotFound);
    const auto task_rowid = dag_info->tasks[idx].task_rowid;
    if (task_rowid <= 0) {
      co_return fail(Error::NotFound);
    }
    co_return co_await persistence_->get_previous_task_state(
        task_rowid, execution_date, current_dag_run_id);
  };

  execution_->set_callbacks(std::move(callbacks));

  scheduler_->set_run_exists_callback(
      [this](const DAGId &dag_id,
             std::chrono::system_clock::time_point execution_date)
          -> task<Result<bool>> {
        if (!persistence_)
          co_return fail(Error::NotFound);
        co_return co_await persistence_->has_dag_run(dag_id, execution_date);
      });

  scheduler_->set_list_run_execution_dates_callback(
      [this](const DAGId &dag_id,
             std::chrono::system_clock::time_point start,
             std::chrono::system_clock::time_point end)
          -> task<Result<std::vector<std::chrono::system_clock::time_point>>> {
        if (!persistence_) {
          co_return fail(Error::NotFound);
        }
        co_return co_await persistence_->list_dag_run_execution_dates(dag_id,
                                                                      start,
                                                                      end);
      });

  scheduler_->set_get_watermark_callback(
      [this](const DAGId &dag_id)
          -> task<
              Result<std::optional<std::chrono::system_clock::time_point>>> {
        if (!persistence_)
          co_return fail(Error::NotFound);
        auto watermark = co_await persistence_->get_watermark(dag_id);
        if (!watermark) {
          if (watermark.error() == make_error_code(Error::NotFound)) {
            co_return ok(
                std::optional<std::chrono::system_clock::time_point>{});
          }
          co_return fail(watermark.error());
        }
        co_return ok(
            std::optional<std::chrono::system_clock::time_point>{*watermark});
      });

  scheduler_->set_save_watermark_callback(
      [this](const DAGId &dag_id,
             std::chrono::system_clock::time_point watermark)
          -> task<Result<void>> {
        if (!persistence_)
          co_return fail(Error::NotFound);
        co_return co_await persistence_->save_watermark(dag_id, watermark);
      });

  scheduler_->set_zombie_reaper_config(
      config_.scheduler.zombie_reaper_interval_sec,
      config_.scheduler.zombie_heartbeat_timeout_sec);
  scheduler_->set_zombie_reaper_callback(
      [this](std::int64_t heartbeat_timeout_ms) -> task<Result<std::size_t>> {
        if (!persistence_) {
          co_return fail(Error::NotFound);
        }
        co_return co_await persistence_->reap_zombie_task_instances(
            heartbeat_timeout_ms);
      });

  // Scheduler triggers go directly to a new coroutine on the runtime.
  scheduler_->set_on_dag_trigger(
      [this](const DAGId &dag_id,
             std::chrono::system_clock::time_point execution_date) {
        auto target = owner_shard(dag_id);
        auto coro =
            trigger_scheduled_on_owner_shard(dag_id.clone(), execution_date);
        runtime_.spawn_on(target, std::move(coro));
      });
}

auto Application::owner_shard(const DAGId &dag_id) const noexcept -> shard_id {
  const auto shard_count =
      std::max(1U, static_cast<unsigned>(dag_owner_states_.size()));
  return static_cast<shard_id>(util::shard_of(
      std::hash<std::string_view>{}(dag_id.value()), shard_count));
}

auto Application::resolve_dag_id(const DAGRunId &dag_run_id) const
    -> std::optional<DAGId> {
  return dag_id_from_run_id(dag_run_id);
}

auto Application::trigger_scheduled(
    DAGId dag_id, std::chrono::system_clock::time_point execution_date)
    -> task<void> {
  auto owner = owner_shard(dag_id);
  if (!runtime_.is_current_shard() || runtime_.current_shard() != owner) {
    co_await boost::asio::co_spawn(
        runtime_.executor_for(owner),
        trigger_scheduled_on_owner_shard(dag_id.clone(), execution_date),
        boost::asio::use_awaitable);
    co_return;
  }

  auto r = co_await trigger_run_on_dag_owner_shard(
      dag_id, TriggerType::Schedule, execution_date,
      std::chrono::system_clock::now());
  if (!r) {
    log::error("Failed to trigger scheduled DAG: {} ({})", dag_id,
               r.error().message());
  }
}

auto Application::trigger_scheduled_on_owner_shard(
    DAGId dag_id, std::chrono::system_clock::time_point execution_date)
    -> spawn_task {
  auto r = co_await trigger_run_on_dag_owner_shard(
      dag_id.clone(), TriggerType::Schedule, execution_date,
      std::chrono::system_clock::now());
  if (!r) {
    log::error("Failed to trigger scheduled DAG: {} ({})", dag_id,
               r.error().message());
  }
}

auto Application::trigger_run(
    DAGId dag_id, TriggerType trigger,
    std::optional<std::chrono::system_clock::time_point> execution_date)
    -> task<Result<DAGRunId>> {
  const auto request_now = std::chrono::system_clock::now();
  auto owner = owner_shard(dag_id);
  if (!runtime_.is_current_shard() || runtime_.current_shard() != owner) {
    co_return co_await boost::asio::co_spawn(
        runtime_.executor_for(owner),
        trigger_run_on_dag_owner_shard(dag_id.clone(), trigger, execution_date,
                                       request_now),
        boost::asio::use_awaitable);
  }

  co_return co_await trigger_run_on_dag_owner_shard(std::move(dag_id), trigger,
                                                    execution_date, request_now);
}

auto Application::trigger_run_on_dag_owner_shard(
    DAGId dag_id, TriggerType trigger,
    std::optional<std::chrono::system_clock::time_point> execution_date,
    std::chrono::system_clock::time_point request_now)
    -> task<Result<DAGRunId>> {
  const auto started_at = std::chrono::steady_clock::now();
  auto dag_res = dag_manager_.get_dag(dag_id);
  if (!dag_res) {
    co_return fail(dag_res.error());
  }

  if (auto slot_res = try_acquire_dag_run_slot(*dag_res); !slot_res) {
    co_return fail(slot_res.error());
  }

  DAGInfo info = std::move(*dag_res);

  if (!persistence_) {
    release_dag_run_slot(dag_id);
    co_return fail(Error::DatabaseError);
  }

  auto ensure_rowids = [&]() -> task<Result<void>> {
    if (info.dag_rowid > 0 &&
        std::ranges::all_of(info.tasks,
                            [](const auto &task) { return task.task_rowid > 0; })) {
      co_return ok();
    }

    auto persisted = co_await persistence_->upsert_dag_definition(
        dag_id, info, info.dag_rowid > 0);
    if (!persisted) {
      co_return fail(persisted.error());
    }
    info = std::move(*persisted);
    dag_manager_.patch_dag_state_local(dag_id, state_from_snapshot_info(info));
    co_return ok();
  };

  if (auto ensure = co_await ensure_rowids(); !ensure) {
    release_dag_run_slot(dag_id);
    co_return fail(ensure.error());
  }
  const auto after_ensure_rowids = std::chrono::steady_clock::now();

  auto build_launch_plan = [&]() -> Result<RunLaunchPlan> {
    auto graph = info.compiled_graph;
    if (!graph) {
      auto graph_res = dag_manager_.build_dag_graph(dag_id);
      if (!graph_res) {
        return fail(graph_res.error());
      }
      graph = std::make_shared<DAG>(std::move(*graph_res));
    }

    auto executor_configs =
        info.compiled_executor_configs
            ? info.compiled_executor_configs
            : std::make_shared<const std::vector<ExecutorConfig>>(
                  ExecutorConfigBuilder::build(info, *graph));

    auto indexed_task_configs =
        info.compiled_indexed_task_configs
            ? info.compiled_indexed_task_configs
            : std::make_shared<const std::vector<TaskConfig>>(
                  build_indexed_task_configs(info, *graph));

    if (has_unresolved_task_rowids(*indexed_task_configs)) {
      auto rebuilt = build_indexed_task_configs(info, *graph);
      if (!has_unresolved_task_rowids(rebuilt)) {
        log::warn(
            "Rebuilt stale indexed_task_configs with unresolved task_rowid "
            "values for DAG {}",
            dag_id);
        indexed_task_configs = std::make_shared<const std::vector<TaskConfig>>(
            std::move(rebuilt));
      }
    }

    return ok(RunLaunchPlan{.dag_id = dag_id.clone(),
                            .dag_rowid = info.dag_rowid,
                            .version = info.version,
                            .graph = std::move(graph),
                            .executor_configs = std::move(executor_configs),
                            .indexed_task_configs =
                                std::move(indexed_task_configs)});
  };

  auto plan_res = build_launch_plan();
  if (!plan_res) {
    release_dag_run_slot(dag_id);
    co_return fail(plan_res.error());
  }
  auto plan = std::move(*plan_res);

  auto dag_run_id = generate_dag_run_id(dag_id);
  auto rollback_slot = [&]() { release_dag_run_slot(dag_id); };

  auto run_result = co_await trigger_run_on_owner_shard(
      std::move(plan), trigger, execution_date, dag_run_id.clone(),
      request_now);
  const auto after_run_create = std::chrono::steady_clock::now();

  if (!run_result) {
    rollback_slot();
    co_return fail(run_result.error());
  }

  log::info(
      "trigger_run_on_dag_owner_shard dag_id={} dag_run_id={} ensure_rowids_ms={} "
      "run_create_ms={} total_ms={}",
      dag_id, *run_result,
      std::chrono::duration_cast<std::chrono::milliseconds>(
          after_ensure_rowids - started_at)
          .count(),
      std::chrono::duration_cast<std::chrono::milliseconds>(
          after_run_create - after_ensure_rowids)
          .count(),
      std::chrono::duration_cast<std::chrono::milliseconds>(
          after_run_create - started_at)
          .count());

  co_return run_result;
}

auto Application::trigger_run_on_owner_shard(
    RunLaunchPlan plan, TriggerType trigger,
    std::optional<std::chrono::system_clock::time_point> execution_date,
    DAGRunId dag_run_id, std::chrono::system_clock::time_point request_now)
    -> task<Result<DAGRunId>> {
  const auto started_at = std::chrono::steady_clock::now();
  if (!persistence_) {
    co_return fail(Error::DatabaseError);
  }

  if (!plan.graph || !plan.executor_configs || !plan.indexed_task_configs ||
      plan.indexed_task_configs->empty()) {
    co_return fail(Error::InvalidArgument);
  }

  auto run_res = DAGRun::create(dag_run_id.clone(), plan.graph);
  if (!run_res) {
    co_return fail(run_res.error());
  }
  auto cfgs = plan.executor_configs;
  auto run = std::make_unique<DAGRun>(std::move(*run_res));
  run->set_scheduled_at(request_now);
  run->set_started_at(request_now);
  run->set_trigger_type(trigger);
  run->set_execution_date(execution_date.value_or(request_now));
  run->set_dag_rowid(plan.dag_rowid);
  run->set_dag_version(plan.version);

  // Stamp task rowids onto the run object. Instance IDs are generated lazily
  // when a task is actually dispatched to avoid startup string-allocation storms.
  auto stamp_run_task_metadata = [&]() -> Result<void> {
    for (NodeIndex idx = 0; idx < plan.indexed_task_configs->size(); ++idx) {
      const auto &task = (*plan.indexed_task_configs)[idx];
      if (task.task_id.empty()) {
        continue;
      }
      if (auto r = run->set_task_rowid(idx, task.task_rowid); !r) {
        return r;
      }
    }
    return ok();
  };

  if (auto r = stamp_run_task_metadata(); !r) {
    co_return fail(r.error());
  }

  auto indexed_task_cfgs = plan.indexed_task_configs;

  auto persist_snapshot = [this, &run]() -> task<Result<int64_t>> {
    DAGRun run_snapshot = *run;
    auto all_task_infos = run->all_task_info();
    std::vector<TaskInstanceInfo> task_infos;
    task_infos.reserve(all_task_infos.size());

    // Startup hot-path optimization:
    // only persist currently ready task rows at trigger time, and let later
    // state transitions upsert downstream tasks when they become active/terminal.
    for (const auto &info : all_task_infos) {
      if (run->is_task_ready(info.task_idx)) {
        task_infos.emplace_back(info);
      }
    }
    if (task_infos.empty()) {
      task_infos = std::move(all_task_infos);
    }

    const auto unresolved =
        std::count_if(task_infos.begin(), task_infos.end(), [](const auto &info) {
          return info.task_rowid <= 0;
        });
    if (unresolved != 0) {
      log::error(
          "Refusing to persist run {} with unresolved task_rowid values: "
          "batch_size={} unresolved={}",
          run_snapshot.id(), task_infos.size(), unresolved);
    }
    co_return co_await persistence_->create_run_with_task_instances(
        std::move(run_snapshot), std::move(task_infos));
  };

  auto persist_result = co_await persist_snapshot();
  const auto after_first_persist = std::chrono::steady_clock::now();

  // If persistence failed (e.g. stale FK rowids), refresh once and retry.
  if (!persist_result) {
    auto dag_res = dag_manager_.get_dag(plan.dag_id);
    if (dag_res) {
      auto info = std::move(*dag_res);
      auto refreshed = co_await persistence_->upsert_dag_definition(
          plan.dag_id, info, info.dag_rowid > 0);
      if (refreshed) {
        info = std::move(*refreshed);
        dag_manager_.patch_dag_state_local(plan.dag_id,
                                           state_from_snapshot_info(info));
        plan.dag_rowid = info.dag_rowid;
        plan.version = info.version;
        run->set_dag_rowid(info.dag_rowid);
        run->set_dag_version(info.version);

        auto refreshed_indexed_task_cfgs =
            info.compiled_indexed_task_configs
                ? *info.compiled_indexed_task_configs
                : build_indexed_task_configs(
                      info, *(info.compiled_graph ? info.compiled_graph : plan.graph));

        if (has_unresolved_task_rowids(refreshed_indexed_task_cfgs)) {
          refreshed_indexed_task_cfgs = build_indexed_task_configs(
              info, *(info.compiled_graph ? info.compiled_graph : plan.graph));
        }

        for (NodeIndex idx = 0; idx < refreshed_indexed_task_cfgs.size(); ++idx) {
          const auto &task = refreshed_indexed_task_cfgs[idx];
          if (task.task_id.empty()) {
            continue;
          }
          if (auto r = run->set_task_rowid(idx, task.task_rowid); !r) {
            co_return fail(r.error());
          }
        }
        indexed_task_cfgs = std::make_shared<const std::vector<TaskConfig>>(
            std::move(refreshed_indexed_task_cfgs));
        persist_result = co_await persist_snapshot();
      }
    }
  }

  co_return persist_result
      .and_then([this, run = std::move(run), cfgs = std::move(cfgs),
                 indexed_task_cfgs = std::move(indexed_task_cfgs), dag_run_id,
                 dag_id = plan.dag_id.clone(), trigger,
                 started_at, after_first_persist](int64_t rowid) mutable
                    -> Result<DAGRunId> {
        run->set_run_rowid(rowid);
        execution_->start_run(dag_run_id,
                              ExecutionService::RunContext{
                                  .run = std::move(run),
                                  .executor_configs = std::move(cfgs),
                                  .task_configs = std::move(indexed_task_cfgs),
                                  .dag_id = dag_id.clone()});
        const auto after_dispatch = std::chrono::steady_clock::now();
        log::info(
            "trigger_run_on_owner_shard dag_id={} dag_run_id={} first_persist_ms={} "
            "dispatch_ms={} total_ms={}",
            dag_id, dag_run_id,
            std::chrono::duration_cast<std::chrono::milliseconds>(
                after_first_persist - started_at)
                .count(),
            std::chrono::duration_cast<std::chrono::milliseconds>(
                after_dispatch - after_first_persist)
                .count(),
            std::chrono::duration_cast<std::chrono::milliseconds>(
                after_dispatch - started_at)
                .count());
        return ok(dag_run_id);
      })
      .transform([dag_id = plan.dag_id.clone(), trigger](DAGRunId id) {
        log::info("DAG run {} triggered for {} ({})", id, dag_id,
                  trigger == TriggerType::Schedule ? "schedule" : "manual");
        return id;
      })
      .or_else([dag_run_id](std::error_code ec) -> Result<DAGRunId> {
        log::error("Failed to persist dag run {}: {}", dag_run_id,
                   ec.message());
        return fail(ec);
      });
}

auto Application::owner_shard(const DAGRunId &dag_run_id) const noexcept
    -> shard_id {
  const auto shard_count = std::max(1U, runtime_.shard_count());
  return static_cast<shard_id>(util::shard_of(
      std::hash<std::string_view>{}(dag_run_id.value()), shard_count));
}

auto Application::try_acquire_dag_run_slot(const DAGInfo &info)
    -> Result<void> {
  if (info.is_paused) {
    return fail(Error::InvalidState);
  }

  const auto owner = owner_shard(info.dag_id);
  if (!runtime_.is_current_shard() || runtime_.current_shard() != owner) {
    return fail(Error::InvalidState);
  }

  auto &state = dag_owner_states_[owner].dags[info.dag_id];
  if (info.max_concurrent_runs > 0 &&
      state.active_runs >= info.max_concurrent_runs) {
    return fail(Error::HasActiveRuns);
  }
  ++state.active_runs;
  return ok();
}

auto Application::release_dag_run_slot(const DAGId &dag_id) -> void {
  const auto owner = owner_shard(dag_id);
  if (!runtime_.is_current_shard() || runtime_.current_shard() != owner) {
    runtime_.post_to(owner, [this, dag_id = dag_id.clone()]() mutable {
      release_dag_run_slot(dag_id);
    });
    return;
  }

  auto &dags = dag_owner_states_[owner].dags;
  if (auto it = dags.find(dag_id); it != dags.end()) {
    if (it->second.active_runs > 0) {
      --it->second.active_runs;
    }
  }
}

auto Application::on_run_finished(const DAGRunId &dag_run_id,
                                  DAGRunState status) -> void {
  if (status == DAGRunState::Queued || status == DAGRunState::Running) {
    return;
  }

  auto dag_id = resolve_dag_id(dag_run_id);
  if (!dag_id) {
    return;
  }
  release_dag_run_slot(*dag_id);
}

auto Application::trigger_run_blocking(
    const DAGId &dag_id, TriggerType trigger,
    std::optional<std::chrono::system_clock::time_point> execution_date)
    -> Result<DAGRunId> {
  return sync_wait_on_runtime(
      runtime_, trigger_run(dag_id.clone(), trigger, execution_date));
}

auto Application::wait_for_completion(int timeout_ms) -> void {
  sync_wait_on_runtime(runtime_, wait_for_completion_async(timeout_ms));
}

auto Application::wait_for_completion_async(int timeout_ms) -> task<void> {
  if (execution_) {
    co_await execution_->wait_for_completion_async(timeout_ms);
  }
}

auto Application::has_active_runs() const -> bool {
  return execution_ && execution_->has_active_runs();
}

auto Application::active_coroutines() const -> int {
  return execution_ ? execution_->coro_count() : 0;
}

auto Application::mysql_batch_write_ops() const -> std::uint64_t {
  return mysql_batch_write_ops_.load(std::memory_order_relaxed);
}

auto Application::dropped_persistence_events() const -> std::uint64_t {
  return dropped_persistence_events_.load(std::memory_order_relaxed);
}

auto Application::event_bus_queue_length() const -> std::size_t {
  return runtime_.pending_cross_shard_queue_length();
}

auto Application::trigger_batch_queue_depth() const -> std::size_t {
  return persistence_ ? persistence_->trigger_batch_queue_depth() : 0;
}

auto Application::trigger_batch_last_size() const -> std::size_t {
  return persistence_ ? persistence_->trigger_batch_last_size() : 0;
}

auto Application::trigger_batch_last_linger_us() const -> std::uint64_t {
  return persistence_ ? persistence_->trigger_batch_last_linger_us() : 0;
}

auto Application::trigger_batch_last_flush_ms() const -> std::uint64_t {
  return persistence_ ? persistence_->trigger_batch_last_flush_ms() : 0;
}

auto Application::trigger_batch_requests_total() const -> std::uint64_t {
  return persistence_ ? persistence_->trigger_batch_requests_total() : 0;
}

auto Application::trigger_batch_commits_total() const -> std::uint64_t {
  return persistence_ ? persistence_->trigger_batch_commits_total() : 0;
}

auto Application::trigger_batch_fallback_total() const -> std::uint64_t {
  return persistence_ ? persistence_->trigger_batch_fallback_total() : 0;
}

auto Application::trigger_batch_rejected_total() const -> std::uint64_t {
  return persistence_ ? persistence_->trigger_batch_rejected_total() : 0;
}

auto Application::trigger_batch_wakeup_lag_us() const -> std::uint64_t {
  return persistence_ ? persistence_->trigger_batch_wakeup_lag_us() : 0;
}

auto Application::shard_stall_age_ms(shard_id id) const -> std::uint64_t {
  return runtime_.stall_age_ms(id);
}

auto Application::get_max_retries(const DAGRunId &dag_run_id,
                                  NodeIndex idx) const -> int {
  std::optional<DAGId> dag_id;
  if (execution_) {
    dag_id = execution_->get_cached_dag_id(dag_run_id);
  }
  if (!dag_id) {
    dag_id = resolve_dag_id(dag_run_id);
  }
  // Do not block execution hot-path on DB fallback here.
  // If run->dag mapping is not in cache, use conservative default retries.
  if (!dag_id)
    return 3;

  if (auto dag_info = dag_manager_.get_dag(*dag_id);
      dag_info && idx < dag_info->tasks.size()) {
    return dag_info->tasks[idx].max_retries;
  }

  return 3;
}

auto Application::get_retry_interval(const DAGRunId &dag_run_id,
                                     NodeIndex idx) const
    -> std::chrono::seconds {
  std::optional<DAGId> dag_id;
  if (execution_) {
    dag_id = execution_->get_cached_dag_id(dag_run_id);
  }
  if (!dag_id) {
    dag_id = resolve_dag_id(dag_run_id);
  }
  // Do not block execution hot-path on DB fallback here.
  // If run->dag mapping is not in cache, use conservative default interval.
  if (!dag_id)
    return std::chrono::seconds(60);

  if (auto dag_info = dag_manager_.get_dag(*dag_id);
      dag_info && idx < dag_info->tasks.size()) {
    return dag_info->tasks[idx].retry_interval;
  }

  return std::chrono::seconds(60);
}

auto Application::set_dag_paused(const DAGId &dag_id, bool paused)
    -> task<Result<void>> {
  if (!persistence_) {
    co_return fail(Error::DatabaseError);
  }

  auto db_res = co_await persistence_->set_dag_paused(dag_id, paused);
  if (!db_res) {
    co_return db_res;
  }

  // Propagate pause state into the in-memory snapshot.
  auto dag_res = dag_manager_.get_dag(dag_id);
  if (dag_res) {
    DagStateRecord state = state_from_snapshot_info(*dag_res);
    state.is_paused = paused;
    state.updated_at = std::chrono::system_clock::now();
    dag_manager_.apply_dag_state(dag_id, state);
  }

  // Pause: unregister from scheduler so no new scheduled runs fire.
  // Unpause: re-register so scheduled runs resume.
  if (paused) {
    scheduler_->unregister_dag(dag_id);
  } else if (dag_res) {
    const auto &dag = *dag_res;
    if (!dag.cron.empty()) {
      scheduler_->register_dag(dag_id, dag);
    }
  }

  log::info("DAG {} {}", dag_id, paused ? "paused" : "unpaused");
  co_return ok();
}

auto Application::register_dag_cron(DAGId dag_id,
                                    std::string_view /*cron_expr*/)
    -> Result<void> {
  return dag_manager_.get_dag(dag_id)
      .and_then([&](const DAGInfo &dag) -> Result<void> {
        if (dag.tasks.empty()) {
          log::error("Cannot register cron for DAG {}: empty", dag_id);
          return fail(Error::InvalidArgument);
        }
        scheduler_->register_dag(dag_id, dag);
        return ok();
      })
      .or_else([&](std::error_code ec) -> Result<void> {
        log::error("Cannot register cron for DAG {}: not found", dag_id);
        return fail(ec);
      });
}

auto Application::unregister_dag_cron(const DAGId &dag_id) -> void {
  scheduler_->unregister_dag(dag_id);
}

auto Application::update_dag_cron(const DAGId &dag_id,
                                  std::string_view cron_expr, bool is_active)
    -> Result<void> {
  unregister_dag_cron(dag_id);

  if (!cron_expr.empty() && is_active) {
    return register_dag_cron(dag_id, cron_expr);
  }
  return ok();
}

auto Application::recover_from_crash() -> Result<void> {
  if (!persistence_ || !persistence_->is_open()) {
    return fail(Error::DatabaseError);
  }
  if (auto load_res = dag_manager_.load_from_database(); !load_res) {
    return fail(load_res.error());
  }
  // MySQL migration phase: we only ensure dangling in-flight runs are marked
  // failed at startup. Detailed DAGRun in-memory reconstruction can be added
  // back as a separate step.
  auto marked =
      persistence_->sync_wait(persistence_->mark_incomplete_runs_failed());
  if (!marked) {
    return fail(marked.error());
  }
  log::info("Recovery: marked {} incomplete run(s) failed", *marked);
  return ok();
}

auto Application::list_tasks() const -> void {
  for (const auto &dag : dag_manager_.list_dags()) {
    std::println("DAG: {} ({} tasks)", dag.dag_id, dag.tasks.size());
    std::println("  {:<20} {:<10} {}", "TASK", "TYPE", "DEPS");

    for (const auto &task : dag.tasks) {
      std::string deps = "-";
      if (!task.dependencies.empty()) {
        deps.clear();
        for (auto [i, dep] : task.dependencies | std::views::enumerate) {
          if (i > 0)
            deps += ",";
          deps += dep.task_id.str();
        }
      }

      std::println("  {:<20} {:<10} {}", task.task_id,
                   to_string_view(task.executor), deps);
    }
    std::println("");
  }
}

auto Application::show_status() const -> void {
  std::println("Status: {}", running_.load() ? "running" : "stopped");
  std::println("DAGs: {}", dag_manager_.list_dags().size());
  std::println("Active runs: {}", execution_ ? "checking..." : "N/A");
}

auto Application::dag_manager() -> DAGManager & { return dag_manager_; }

auto Application::dag_manager() const -> const DAGManager & {
  return dag_manager_;
}

auto Application::engine() -> Engine & { return scheduler_->engine(); }

auto Application::persistence_service() -> PersistenceService * {
  return persistence_.get();
}

auto Application::api_server() -> ApiServer * { return api_.get(); }

auto Application::runtime() -> Runtime & { return runtime_; }

auto Application::validate_dag_info(const DAGInfo &info) -> Result<void> {
  return DAGValidator::validate(info);
}

auto Application::create_dag_atomically(DAGId dag_id, const DAGInfo &info)
    -> Result<void> {
  return dag_manager_.create_dag(dag_id, info).transform([&]() {
    if (scheduler_ && scheduler_->is_running()) {
      scheduler_->register_dag(dag_id, info);
    }
    return;
  });
}

auto Application::setup_config_watcher() -> void {
  if (config_.dag_source.directory.empty()) {
    return;
  }

  const std::filesystem::path dag_dir = config_.dag_source.directory;
  if (!std::filesystem::exists(dag_dir)) {
    log::warn("Skip ConfigWatcher: DAG directory does not exist: {}",
              dag_dir.string());
    return;
  }

  if (config_watcher_) {
    config_watcher_->stop();
    config_watcher_.reset();
  }

  config_watcher_ =
      std::make_unique<ConfigWatcher>(runtime_, config_.dag_source.directory);
  config_watcher_->set_on_file_changed(
      [this](const std::filesystem::path &path) {
        handle_file_change(path.string());
      });
  config_watcher_->set_on_file_removed(
      [this](const std::filesystem::path &path) {
        handle_file_change(path.string());
      });
  if (auto r = config_watcher_->start(); !r) {
    log::error("Failed to start ConfigWatcher for {}: {}",
               config_.dag_source.directory, r.error().message());
    config_watcher_.reset();
  }
}

auto Application::handle_file_change(const std::string &filename) -> void {
  if (!std::filesystem::exists(filename)) {
    std::filesystem::path p(filename);
    DAGId dag_id{p.stem().string()};

    log::info("DAG file removed: {}", filename);
    if (auto r = dag_manager_.delete_dag(dag_id); !r.has_value()) {
      log::warn("Failed to delete DAG {}: {}", dag_id, r.error().message());
    }
    scheduler_->unregister_dag(dag_id);
    return;
  }

  log::info("DAG file changed: {}", filename);

  DAGFileLoader loader(config_.dag_source.directory);
  loader.load_file(filename)
      .and_then([&](const auto &file) {
        return reload_single_dag(file.dag_id, file.definition);
      })
      .transform(
          [&]() { log::info("Successfully reloaded DAG from {}", filename); })
      .or_else([&](std::error_code ec) -> Result<void> {
        log::error("Failed to reload DAG from {}: {}", filename, ec.message());
        return ok();
      });
}

auto Application::reload_single_dag(const DAGId &dag_id, const DAGInfo &info)
    -> Result<void> {
  return validate_dag_info(info)
      .and_then([&]() { return dag_manager_.upsert_dag(dag_id, info); })
      .and_then([&]() {
        return update_dag_cron(dag_id, info.cron, true)
            .or_else([&](std::error_code ec) -> Result<void> {
              log::warn("Failed to update DAG cron on reload for {}: {}",
                        dag_id, ec.message());
              return ok();
            });
      });
}

} // namespace dagforge
