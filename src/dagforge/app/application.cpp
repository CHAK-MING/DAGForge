#include "dagforge/app/application.hpp"
#include "dagforge/app/api/api_server.hpp"
#include "dagforge/app/config_builder.hpp"
#include "dagforge/app/services/dag_catalog_service.hpp"
#include "dagforge/app/services/execution_event_bridge.hpp"
#include "dagforge/app/services/execution_service.hpp"
#include "dagforge/app/services/persistence_service.hpp"
#include "dagforge/app/services/scheduler_service.hpp"
#include "dagforge/config/config_watcher.hpp"
#include "dagforge/executor/composite_executor.hpp"
#include "dagforge/util/log.hpp"

#include <atomic>
#include <chrono>
#include <filesystem>
#include <print>
#include <ranges>

#include <csignal>

namespace dagforge {
namespace {

[[nodiscard]] auto build_dag_state_index(std::vector<DagStateRecord> states)
    -> ankerl::unordered_dense::map<DAGId, DagStateRecord> {
  ankerl::unordered_dense::map<DAGId, DagStateRecord> out;
  out.reserve(states.size());
  for (auto &state : states) {
    out.insert_or_assign(state.dag_id, std::move(state));
  }
  return out;
}

[[nodiscard]] auto dag_run_terminal_state_index(DAGRunState state)
    -> std::optional<std::size_t> {
  switch (state) {
  case DAGRunState::Success:
    return 0;
  case DAGRunState::Failed:
    return 1;
  case DAGRunState::Skipped:
    return 2;
  case DAGRunState::Cancelled:
    return 3;
  case DAGRunState::Queued:
  case DAGRunState::Running:
    return std::nullopt;
  }
  return std::nullopt;
}

} // namespace

Application::Application() {
  std::signal(SIGPIPE, SIG_IGN);
  rebuild_services_from_config();
}

Application::Application(SystemConfig config) : config_(std::move(config)) {
  std::signal(SIGPIPE, SIG_IGN);
  rebuild_services_from_config();
}

Application::~Application() { stop(); }

auto Application::load_config(std::string_view path) -> Result<void> {
  if (is_running()) {
    return fail(Error::InvalidState);
  }
  return SystemConfigLoader::load_from_file(path).transform([this](SystemConfig &&cfg) {
    config_ = std::move(cfg);
    rebuild_services_from_config();
    return;
  });
}

auto Application::config() const noexcept -> const SystemConfig & {
  return config_;
}

auto Application::config() noexcept -> SystemConfig & { return config_; }

auto Application::rebuild_services_from_config() -> void {
  dag_catalog_.reset();
  execution_event_bridge_.reset();
  config_watcher_.reset();
  api_.reset();
  execution_.reset();
  scheduler_.reset();
  persistence_.reset();
  executor_.reset();
  runtime_.reset();

  runtime_.emplace(
      config_.scheduler.shards, config_.scheduler.pin_shards_to_cores,
      static_cast<unsigned>(config_.scheduler.cpu_affinity_offset));
  executor_ = create_composite_executor(*runtime_);
  persistence_ =
      std::make_unique<PersistenceService>(*runtime_, config_.database);
  scheduler_ = std::make_unique<SchedulerService>(
      *runtime_, static_cast<unsigned>(config_.scheduler.scheduler_shards));
  execution_ = std::make_unique<ExecutionService>(*runtime_, *executor_);
  dag_catalog_ = std::make_unique<DagCatalogService>(
      dag_manager_, persistence_.get(), scheduler_.get());
  dag_owner_states_ = std::vector<DagOwnerShardState>(
      std::clamp(static_cast<unsigned>(config_.scheduler.scheduler_shards), 1U,
                 std::max(1U, runtime_->shard_count())));

  dag_manager_.set_persistence_service(persistence_.get());
  dag_manager_.set_runtime(runtime_ ? &*runtime_ : nullptr);
  execution_->set_max_concurrency(config_.scheduler.max_concurrency);
  execution_->set_local_task_lease_timeout(
      std::chrono::seconds(config_.scheduler.zombie_heartbeat_timeout_sec));
  rebuild_execution_event_bridge();
}

auto Application::ensure_services_initialized() -> void {
  if (!runtime_ || !executor_ || !persistence_ || !scheduler_ || !execution_ ||
      !dag_catalog_) {
    rebuild_services_from_config();
  }
}

auto Application::rollback_partial_start(bool log_started) noexcept -> void {
  if (config_watcher_) {
    config_watcher_->stop();
    config_watcher_.reset();
  }

  if (api_) {
    api_->stop();
    api_.reset();
  }

  if (scheduler_) {
    scheduler_->stop();
  }

  if (persistence_ && persistence_->is_open() && runtime_ &&
      runtime_->is_running()) {
    persistence_->sync_wait(persistence_->close());
  }

  if (log_started) {
    log::stop();
  }

  if (runtime_ && runtime_->is_running()) {
    runtime_->stop();
  }

  running_.store(false, std::memory_order_release);
}

auto Application::init() -> Result<void> {
  ensure_services_initialized();
  if (!api_) {
    api_ = std::make_unique<ApiServer>(*this);
  }
  return ok();
}

auto Application::init_db_only() -> Result<void> {
  ensure_services_initialized();
  if (running_.exchange(true)) {
    return ok();
  }

  auto runtime_res = runtime_->start();
  if (!runtime_res) {
    running_ = false;
    return fail(runtime_res.error());
  }

  if (persistence_ && !persistence_->is_open()) {
    if (auto open_res = persistence_->sync_wait(persistence_->open());
        !open_res) {
      running_ = false;
      runtime_->stop();
      return fail(open_res.error());
    }
  }

  if (persistence_ && persistence_->is_open()) {
    persistence_->sync_wait(persistence_->close());
  }

  runtime_->stop();
  running_ = false;
  return ok();
}

auto Application::load_dags_from_directory(std::string_view dags_dir)
    -> Result<bool> {
  ensure_services_initialized();
  if (!dag_catalog_) {
    return fail(Error::InvalidState);
  }
  return dag_catalog_->load_directory(dags_dir);
}

auto Application::get_run_state(const DAGRunId &dag_run_id) const
    -> Result<DAGRunState> {
  return sync_wait_on_runtime(*runtime_, get_run_state_async(dag_run_id));
}

auto Application::get_run_state_async(const DAGRunId &dag_run_id) const
    -> task<Result<DAGRunState>> {
  if (!execution_) {
    co_return fail(Error::NotFound);
  }

  auto snapshot_res = co_await execution_->get_run_snapshot(dag_run_id);
  if (snapshot_res) {
    co_return ok((*snapshot_res)->state());
  }

  if (!persistence_) {
    co_return fail(Error::NotFound);
  }

  co_return co_await persistence_->get_dag_run_state(dag_run_id);
}

auto Application::start() -> Result<void> {
  ensure_services_initialized();
  if (running_.exchange(true))
    return ok();

  bool log_started = false;

  auto runtime_res = runtime_->start();
  if (!runtime_res) {
    running_ = false;
    return fail(runtime_res.error());
  }

  log::start();
  log_started = true;
  log::debug("Runtime started");

  if (persistence_ && !persistence_->is_open()) {
    if (auto open_res = persistence_->sync_wait(persistence_->open());
        !open_res) {
      rollback_partial_start(log_started);
      return fail(open_res.error());
    }
  }

  DagStateIndex dag_state_index;
  if (persistence_ && persistence_->is_open()) {
    auto states_res = persistence_->sync_wait(persistence_->list_dag_states());
    if (!states_res) {
      rollback_partial_start(log_started);
      return fail(states_res.error());
    }
    dag_state_index = build_dag_state_index(std::move(*states_res));
  }

  // Re-load file-backed DAGs after persistence is open so DAG/task rowids are
  // materialized before the first trigger hits the hot path.
  if (!config_.dag_source.directory.empty()) {
    if (auto load_res = dag_catalog_->load_directory(
            config_.dag_source.directory, &dag_state_index);
        !load_res) {
      rollback_partial_start(log_started);
      return fail(load_res.error());
    }
  }

  if (persistence_ && persistence_->is_open()) {
    bool has_db_only_dags = false;
    for (const auto &[dag_id, _state] : dag_state_index) {
      if (dag_manager_.has_dag(dag_id)) {
        continue;
      }
      has_db_only_dags = true;
      break;
    }
    if (has_db_only_dags) {
      if (auto load_res = dag_manager_.load_from_database(); !load_res) {
        rollback_partial_start(log_started);
        return fail(load_res.error());
      }
    } else {
      log::debug("Loaded {} DAG states from database", dag_state_index.size());
    }
  } else if (auto load_res = dag_manager_.load_from_database(); !load_res) {
    rollback_partial_start(log_started);
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
      rollback_partial_start(log_started);
      return fail(api_res.error());
    }
    log::debug("API server started on {}:{}", config_.api.host,
               config_.api.port);
  }

  setup_config_watcher();
  return ok();
}

auto Application::stop() noexcept -> void {
  if (!running_.exchange(false))
    return;

  log::debug("Stopping DAGForge...");

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
        *runtime_, wait_for_completion_async(3000)); // 3 s shutdown budget
    if (execution_->has_active_runs()) {
      log::warn("Shutdown timeout: {} run(s) still active",
                execution_->coro_count());
    }
  }
  // Close persistence while runtime is still alive.
  if (persistence_) {
    persistence_->sync_wait(persistence_->close());
  }

  // Destroy services that may still own executors or async work while the
  // runtime is still valid.
  api_.reset();
  execution_event_bridge_.reset();
  dag_catalog_.reset();
  scheduler_.reset();
  persistence_.reset();
  dag_manager_.set_persistence_service(nullptr);

  execution_.reset();
  executor_.reset();

  // Stop runtime after background services are fully torn down.
  if (runtime_) {
    runtime_->stop();
  }
  dag_manager_.set_runtime(nullptr);

  log::debug("DAGForge stopped");
}

auto Application::is_running() const noexcept -> bool {
  return running_.load();
}

auto Application::rebuild_execution_event_bridge() -> void {
  execution_event_bridge_ =
      std::make_unique<ExecutionEventBridge>(ExecutionEventBridge::Dependencies{
          .runtime = runtime_ ? &*runtime_ : nullptr,
          .execution = execution_.get(),
          .scheduler = scheduler_.get(),
          .persistence = persistence_.get(),
          .api_server = [this]() -> ApiServer * { return api_.get(); },
          .resolve_dag_id = [this](const DAGRunId &dag_run_id)
              -> std::optional<DAGId> { return resolve_dag_id(dag_run_id); },
          .on_run_status =
              [this](const DAGRunId &dag_run_id, DAGRunState status) {
                on_run_finished(dag_run_id, status);
              },
          .on_run_completed =
              [this](const DAGRunId &dag_run_id, const DAGRun &run) {
                record_dag_run_metrics(dag_run_id, run);
              },
          .get_max_retries =
              [this](const DAGRunId &dag_run_id, NodeIndex idx) {
                return get_max_retries(dag_run_id, idx);
              },
          .get_retry_interval =
              [this](const DAGRunId &dag_run_id, NodeIndex idx) {
                return get_retry_interval(dag_run_id, idx);
              },
          .on_scheduler_trigger =
              [this](const DAGId &dag_id,
                     std::chrono::system_clock::time_point execution_date) {
                auto target = owner_shard(dag_id);
                auto coro = trigger_scheduled_on_owner_shard(dag_id.clone(),
                                                             execution_date);
                runtime_->spawn_on(target, std::move(coro));
              },
          .dropped_persistence_events = &dropped_persistence_events_,
          .mysql_batch_write_ops = &mysql_batch_write_ops_,
      });
  execution_event_bridge_->wire();
  if (config_.scheduler.zombie_heartbeat_timeout_sec > 0) {
    scheduler_->set_zombie_reaper_config(0, 0);
  } else {
    scheduler_->set_zombie_reaper_config(
        config_.scheduler.zombie_reaper_interval_sec,
        config_.scheduler.zombie_heartbeat_timeout_sec);
  }
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
  if (!runtime_->is_current_shard() || runtime_->current_shard() != owner) {
    co_await boost::asio::co_spawn(
        runtime_->executor_for(owner),
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
  if (!running_.load(std::memory_order_acquire) || !runtime_ ||
      !runtime_->is_running()) {
    co_return fail(Error::InvalidState);
  }

  const auto request_now = std::chrono::system_clock::now();
  auto owner = owner_shard(dag_id);
  if (!runtime_->is_current_shard() || runtime_->current_shard() != owner) {
    co_return co_await boost::asio::co_spawn(
        runtime_->executor_for(owner),
        trigger_run_on_dag_owner_shard(dag_id.clone(), trigger, execution_date,
                                       request_now),
        boost::asio::use_awaitable);
  }

  co_return co_await trigger_run_on_dag_owner_shard(
      std::move(dag_id), trigger, execution_date, request_now);
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

  if (!dag_catalog_) {
    release_dag_run_slot(dag_id);
    co_return fail(Error::InvalidState);
  }

  if (auto materialized = co_await dag_catalog_->ensure_materialized(dag_id);
      !materialized) {
    release_dag_run_slot(dag_id);
    co_return fail(materialized.error());
  } else {
    info = std::move(*materialized);
  }
  const auto after_ensure_rowids = std::chrono::steady_clock::now();

  auto build_launch_plan = [&]() -> Result<RunLaunchPlan> {
    if (!info.compiled_graph || !info.compiled_executor_configs ||
        !info.compiled_indexed_task_configs) {
      log::error("Materialized DAG snapshot is incomplete for {}", dag_id);
      return fail(Error::InvalidState);
    }
    if (std::ranges::any_of(
            *info.compiled_indexed_task_configs,
            [](const TaskConfig::Compiled &task) {
              return !task.task_id.empty() && task.task_rowid <= 0;
            })) {
      log::error("Materialized DAG snapshot still has unresolved task_rowid "
                 "values for {}",
                 dag_id);
      return fail(Error::InvalidState);
    }

    return ok(RunLaunchPlan{.dag_id = dag_id.clone(),
                            .dag_rowid = info.dag_rowid,
                            .version = info.version,
                            .graph = info.compiled_graph,
                            .executor_configs = info.compiled_executor_configs,
                            .indexed_task_configs =
                                info.compiled_indexed_task_configs});
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

  log::debug("trigger_run_on_dag_owner_shard dag_id={} dag_run_id={} "
             "ensure_rowids_ms={} "
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
  // when a task is actually dispatched to avoid startup string-allocation
  // storms.
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
    // state transitions upsert downstream tasks when they become
    // active/terminal.
    for (const auto &info : all_task_infos) {
      if (run->is_task_ready(info.task_idx)) {
        task_infos.emplace_back(info);
      }
    }
    if (task_infos.empty()) {
      task_infos = std::move(all_task_infos);
    }

    const auto unresolved =
        std::count_if(task_infos.begin(), task_infos.end(),
                      [](const auto &info) { return info.task_rowid <= 0; });
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

  co_return persist_result
      .and_then(
          [this, run = std::move(run), cfgs = std::move(cfgs),
           indexed_task_cfgs = std::move(indexed_task_cfgs), dag_run_id,
           dag_id = plan.dag_id.clone(), trigger, started_at,
           after_first_persist](int64_t rowid) mutable -> Result<DAGRunId> {
            run->set_run_rowid(rowid);
            execution_->start_run(
                dag_run_id, ExecutionService::RunContext{
                                .run = std::move(run),
                                .executor_configs = std::move(cfgs),
                                .task_configs = std::move(indexed_task_cfgs),
                                .dag_id = dag_id.clone()});
            const auto after_dispatch = std::chrono::steady_clock::now();
            log::debug("trigger_run_on_owner_shard dag_id={} dag_run_id={} "
                       "first_persist_ms={} "
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
  const auto shard_count = std::max(1U, runtime_->shard_count());
  return static_cast<shard_id>(util::shard_of(
      std::hash<std::string_view>{}(dag_run_id.value()), shard_count));
}

auto Application::try_acquire_dag_run_slot(const DAGInfo &info)
    -> Result<void> {
  if (info.is_paused) {
    return fail(Error::InvalidState);
  }

  const auto owner = owner_shard(info.dag_id);
  if (!runtime_->is_current_shard() || runtime_->current_shard() != owner) {
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
  if (!runtime_->is_current_shard() || runtime_->current_shard() != owner) {
    runtime_->post_to(owner, [this, dag_id = dag_id.clone()]() mutable {
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
  if (!running_.load(std::memory_order_acquire) || !runtime_ ||
      !runtime_->is_running()) {
    return fail(Error::InvalidState);
  }

  return sync_wait_on_runtime(
      *runtime_, trigger_run(dag_id.clone(), trigger, execution_date));
}

auto Application::wait_for_completion(int timeout_ms) -> void {
  sync_wait_on_runtime(*runtime_, wait_for_completion_async(timeout_ms));
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
  return runtime_->pending_cross_shard_queue_length();
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

auto Application::task_update_batch_queue_depth() const -> std::size_t {
  return persistence_ ? persistence_->task_update_batch_queue_depth() : 0;
}

auto Application::task_update_batch_last_size() const -> std::size_t {
  return persistence_ ? persistence_->task_update_batch_last_size() : 0;
}

auto Application::task_update_batch_last_linger_us() const -> std::uint64_t {
  return persistence_ ? persistence_->task_update_batch_last_linger_us() : 0;
}

auto Application::task_update_batch_last_flush_ms() const -> std::uint64_t {
  return persistence_ ? persistence_->task_update_batch_last_flush_ms() : 0;
}

auto Application::task_update_batch_requests_total() const -> std::uint64_t {
  return persistence_ ? persistence_->task_update_batch_requests_total() : 0;
}

auto Application::task_update_batch_commits_total() const -> std::uint64_t {
  return persistence_ ? persistence_->task_update_batch_commits_total() : 0;
}

auto Application::task_update_batch_fallback_total() const -> std::uint64_t {
  return persistence_ ? persistence_->task_update_batch_fallback_total() : 0;
}

auto Application::task_update_batch_rejected_total() const -> std::uint64_t {
  return persistence_ ? persistence_->task_update_batch_rejected_total() : 0;
}

auto Application::task_update_batch_wakeup_lag_us() const -> std::uint64_t {
  return persistence_ ? persistence_->task_update_batch_wakeup_lag_us() : 0;
}

auto Application::dag_run_metrics() const
    -> std::vector<DagRunMetricsSnapshot> {
  return dag_run_metrics_.snapshot();
}

auto Application::shard_stall_age_ms(shard_id id) const -> std::uint64_t {
  return runtime_->stall_age_ms(id);
}

auto Application::record_dag_run_metrics(const DAGRunId &dag_run_id,
                                         const DAGRun &run) -> void {
  const auto dag_id = dag_id_from_run_id(dag_run_id);
  if (!dag_id) {
    return;
  }

  const auto state_index = dag_run_terminal_state_index(run.state());
  if (!state_index) {
    return;
  }

  std::uint64_t duration_ns = 0;
  const auto started_at = run.started_at();
  const auto finished_at = run.finished_at();
  if (finished_at > started_at) {
    duration_ns = static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(finished_at -
                                                             started_at)
            .count());
  }

  dag_run_metrics_.record(*dag_id, *state_index, duration_ns);
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

auto Application::execution_service() -> ExecutionService * {
  return execution_.get();
}

auto Application::execution_service() const -> const ExecutionService * {
  return execution_.get();
}

auto Application::scheduler_service() -> SchedulerService * {
  return scheduler_.get();
}

auto Application::scheduler_service() const -> const SchedulerService * {
  return scheduler_.get();
}

auto Application::engine() -> Engine & { return scheduler_->engine(); }

auto Application::persistence_service() -> PersistenceService * {
  return persistence_.get();
}

auto Application::persistence_service() const -> const PersistenceService * {
  return persistence_.get();
}

auto Application::api_server() -> ApiServer * { return api_.get(); }

auto Application::api_server() const -> const ApiServer * { return api_.get(); }

auto Application::runtime() -> Runtime & { return *runtime_; }

auto Application::runtime() const -> const Runtime & { return *runtime_; }

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
      std::make_unique<ConfigWatcher>(*runtime_, config_.dag_source.directory);
  config_watcher_->set_on_file_changed([this](
                                           const std::filesystem::path &path) {
    if (!dag_catalog_) {
      log::warn("Ignoring DAG file change for {}: DagCatalogService missing",
                path.string());
      return;
    }
    (void)dag_catalog_->handle_file_change(config_.dag_source.directory, path);
  });
  config_watcher_->set_on_file_removed([this](
                                           const std::filesystem::path &path) {
    if (!dag_catalog_) {
      log::warn("Ignoring DAG file removal for {}: DagCatalogService missing",
                path.string());
      return;
    }
    (void)dag_catalog_->handle_file_change(config_.dag_source.directory, path);
  });
  if (auto r = config_watcher_->start(); !r) {
    log::error("Failed to start ConfigWatcher for {}: {}",
               config_.dag_source.directory, r.error().message());
    config_watcher_.reset();
  }
}

} // namespace dagforge
