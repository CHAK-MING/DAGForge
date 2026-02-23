#include "dagforge/app/services/execution_service.hpp"
#include "dagforge/core/asio_awaitable.hpp"

#include "dagforge/core/runtime.hpp"
#include "dagforge/util/hash.hpp"
#include "dagforge/util/log.hpp"
#include "dagforge/xcom/template_resolver.hpp"
#include "dagforge/xcom/xcom_extractor.hpp"

#include <boost/asio/post.hpp>

#include <algorithm>
#include <boost/system/error_code.hpp>
#include <chrono>
#include <functional>
#include <ranges>
#include <unordered_map>
#include <unordered_set>

namespace dagforge {

ExecutionService::ExecutionService(Runtime &runtime, IExecutor &executor)
    : runtime_(runtime), executor_(executor), template_resolver_(),
      shard_states_(runtime_.shard_count()) {}

ExecutionService::~ExecutionService() = default;

auto ExecutionService::set_max_concurrency(int max_concurrency) -> void {
  max_concurrency_ = max_concurrency;
}

auto ExecutionService::owner_shard(const DAGRunId &dag_run_id) const noexcept
    -> shard_id {
  const auto shard_count = runtime_.shard_count();
  if (shard_count == 0) {
    return 0;
  }
  return static_cast<shard_id>(util::shard_of(
      std::hash<std::string_view>{}(dag_run_id.value()), shard_count));
}

auto ExecutionService::post_to_owner(const DAGRunId &dag_run_id,
                                     std::move_only_function<void()> fn)
    -> void {
  boost::asio::post(runtime_.shard(owner_shard(dag_run_id)).ctx(),
                    std::move(fn));
}

auto ExecutionService::dispatch_pending_on_shard(shard_id sid) -> void {
  auto &state = shard_states_[sid];
  for (const auto &[id, run_state] : state.runs) {
    if (running_tasks_ >= max_concurrency_)
      break;
    if (run_state.run && !run_state.run->is_complete()) {
      runtime_.spawn_on(sid, dispatch(id));
    }
  }
}

auto ExecutionService::dispatch_pending() -> void {
  // Called on owner shard — only dispatch local runs
  if (runtime_.is_current_shard()) {
    dispatch_pending_on_shard(runtime_.current_shard());
  }
}

auto ExecutionService::notify_capacity_available() -> void {
  for (unsigned i = 0; i < runtime_.shard_count(); ++i) {
    boost::asio::post(runtime_.shard(i).ctx(), [this, i]() {
      dispatch_pending_on_shard(static_cast<shard_id>(i));
    });
  }
}

auto ExecutionService::set_callbacks(ExecutionCallbacks callbacks) -> void {
  callbacks_ = std::move(callbacks);
  // The resolver's synchronous xcom_lookup is kept as a fallback for cached
  // misses in edge cases; the primary path is prefetch_xcom before execution.
  template_resolver_.set_xcom_lookup(nullptr);
}

auto ExecutionService::start_run(const DAGRunId &dag_run_id, RunContext ctx)
    -> void {
  ++active_run_count_;
  post_to_owner(dag_run_id, [this, id = dag_run_id.clone(),
                             ctx = std::move(ctx)]() mutable {
    auto &state = shard_state(id);
    state.runs.insert_or_assign(
        id.clone(),
        ActiveRunState{.run = std::move(ctx.run),
                       .executor_configs = std::move(ctx.executor_configs),
                       .task_configs = std::move(ctx.task_configs),
                       .dag_id = std::move(ctx.dag_id),
                       .xcom_cache = {}});
    runtime_.spawn_on(owner_shard(id), dispatch(id));
  });
}

auto ExecutionService::get_cached_dag_id(const DAGRunId &dag_run_id) const
    -> std::optional<DAGId> {
  const auto target = owner_shard(dag_run_id);
  if (!runtime_.is_current_shard() || runtime_.current_shard() != target) {
    return std::nullopt;
  }

  const auto &state = shard_states_[target];
  if (auto it = state.runs.find(dag_run_id); it != state.runs.end()) {
    if (it->second.dag_id) {
      return it->second.dag_id->clone();
    }
  }
  return std::nullopt;
}

auto ExecutionService::get_run(const DAGRunId &dag_run_id) -> DAGRun * {
  const auto target = owner_shard(dag_run_id);
  if (runtime_.is_current_shard() && runtime_.current_shard() == target) {
    auto &state = shard_states_[target];
    auto it = state.runs.find(dag_run_id);
    return it != state.runs.end() ? it->second.run.get() : nullptr;
  }
  return nullptr;
}

auto ExecutionService::get_run_snapshot(const DAGRunId &dag_run_id)
    -> task<Result<std::unique_ptr<DAGRun>>> {
  const auto target = owner_shard(dag_run_id);
  if (runtime_.is_current_shard() && runtime_.current_shard() == target) {
    auto &state = shard_states_[target];
    auto it = state.runs.find(dag_run_id);
    if (it == state.runs.end()) {
      co_return fail(Error::NotFound);
    }
    co_return ok(std::make_unique<DAGRun>(*it->second.run));
  }

  auto [ec, snapshot] = co_await boost::asio::async_initiate<
      const decltype(dagforge::use_nothrow),
      void(boost::system::error_code, std::unique_ptr<DAGRun>)>(
      [this, dag_run_id = dag_run_id.clone(), target](auto handler) mutable {
        boost::asio::post(runtime_.shard(target).ctx(),
                          [this, dag_run_id = std::move(dag_run_id), target,
                           handler = std::move(handler)]() mutable {
                            auto &state = shard_states_[target];
                            auto it = state.runs.find(dag_run_id);
                            if (it == state.runs.end()) {
                              handler(make_error_code(Error::NotFound),
                                      nullptr);
                              return;
                            }
                            handler(boost::system::error_code{},
                                    std::make_unique<DAGRun>(*it->second.run));
                          });
      },
      dagforge::use_nothrow);

  if (ec) {
    co_return fail(ec);
  }
  co_return ok(std::move(snapshot));
}

auto ExecutionService::has_active_runs() const -> bool {
  return active_run_count_.load(std::memory_order_acquire) > 0;
}

auto ExecutionService::wait_for_completion_async(int timeout_ms) -> task<void> {
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
  while (has_active_runs() && std::chrono::steady_clock::now() < deadline) {
    co_await async_sleep(std::chrono::milliseconds(50));
  }
}

auto ExecutionService::coro_count() const -> int { return coro_count_.load(); }

auto ExecutionService::maybe_persist_task(const DAGRunId &dag_run_id,
                                          const TaskInstanceInfo &info)
    -> void {
  if (callbacks_.on_persist_task) {
    callbacks_.on_persist_task(dag_run_id, info);
  }
}

auto ExecutionService::maybe_persist_tasks(
    const DAGRunId &dag_run_id, std::span<const TaskInstanceInfo> infos)
    -> void {
  if (!callbacks_.on_persist_task) {
    return;
  }
  for (const auto &info : infos) {
    callbacks_.on_persist_task(dag_run_id, info);
  }
}

auto ExecutionService::dispatch(DAGRunId dag_run_id) -> task<Result<void>> {
  const auto target = owner_shard(dag_run_id);

  // If not on owner shard, schedule a coroutine on the owner and return.
  if (!runtime_.is_current_shard() || runtime_.current_shard() != target) {
    auto coro = dispatch(dag_run_id.clone());
    runtime_.spawn_on(target, std::move(coro));
    co_return ok();
  }

  // --- We are on the owner shard; direct access, no locks ---
  auto &state = shard_states_[target];

  struct ReadyCandidate {
    NodeIndex idx{kInvalidNode};
    TaskId task_id;
    InstanceId inst_id;
    ExecutorConfig cfg;
    std::vector<XComPushConfig> xcom_push;
    std::vector<XComPullConfig> xcom_pull;
    int attempt{1};
    bool depends_on_past{false};
    std::chrono::system_clock::time_point execution_date;
  };

  std::vector<TaskJob> jobs;
  std::vector<NodeIndex> depends_on_past_blocked;
  std::vector<ReadyCandidate> candidates;
  std::vector<TaskInstanceInfo> blocked_to_persist;
  std::vector<std::pair<TaskId, TaskState>> blocked_status_updates;
  bool persist_after_dispatch{false};
  std::shared_ptr<DAGRun> run_snapshot;
  std::shared_ptr<DAGRun> completed_snapshot;
  bool completed{false};

  {
    auto it = state.runs.find(dag_run_id);
    if (it == state.runs.end()) [[unlikely]] {
      log::debug("dispatch: run {} not found", dag_run_id);
      co_return ok();
    }

    auto &run_state = it->second;
    if (!run_state.run) [[unlikely]] {
      log::debug("dispatch: run state for {} is empty", dag_run_id);
      co_return ok();
    }

    auto &run = *run_state.run;
    const auto &g = run.dag();
    const auto &task_cfgs = run_state.executor_configs;
    const auto &task_configs = run_state.task_configs;

    std::pmr::vector<NodeIndex> ready_tasks(current_memory_resource());
    run.copy_ready_tasks(ready_tasks);
    for (NodeIndex idx : ready_tasks) {
      if (static_cast<int>(candidates.size()) + running_tasks_.load() >=
          max_concurrency_) {
        log::debug("Max concurrency reached ({}/{})", running_tasks_.load(),
                   max_concurrency_);
        break;
      }

      if (idx >= task_cfgs.size()) [[unlikely]] {
        log::error("Config not found for task {}", idx);
        continue;
      }

      bool depends_on_past = false;
      if (idx < task_configs.size()) {
        depends_on_past = task_configs[idx].depends_on_past;
      }

      std::vector<XComPushConfig> xcom_push;
      std::vector<XComPullConfig> xcom_pull;
      if (idx < task_configs.size()) {
        xcom_push = task_configs[idx].xcom_push;
        xcom_pull = task_configs[idx].xcom_pull;
      }

      int attempt = 1;
      if (auto info = run.get_task_info(idx)) {
        attempt = info->attempt + 1;
      }

      TaskId task_id{std::string{g.get_key(idx)}};
      candidates.emplace_back(
          ReadyCandidate{.idx = idx,
                         .task_id = task_id,
                         .inst_id = generate_instance_id(dag_run_id, task_id),
                         .cfg = task_cfgs[idx],
                         .xcom_push = std::move(xcom_push),
                         .xcom_pull = std::move(xcom_pull),
                         .attempt = attempt,
                         .depends_on_past = depends_on_past,
                         .execution_date = run.execution_date()});
    }
  }

  for (const auto &candidate : candidates) {
    if (!candidate.depends_on_past) {
      continue;
    }
    if (!callbacks_.check_previous_task_state) {
      log::error("dispatch: task {} has depends_on_past but no callback "
                 "configured",
                 candidate.idx);
      depends_on_past_blocked.emplace_back(candidate.idx);
      continue;
    }

    auto state_res = co_await callbacks_.check_previous_task_state(
        dag_run_id, candidate.idx, candidate.execution_date, dag_run_id);
    bool blocked =
        state_res
            .and_then([&](auto state_val) -> Result<bool> {
              if (state_val != TaskState::Success &&
                  state_val != TaskState::Skipped) {
                log::info("dispatch: task {} blocked by depends_on_past "
                          "(previous state: {})",
                          candidate.idx, to_string_view(state_val));
                return ok(true);
              }
              return ok(false);
            })
            .or_else([&](std::error_code ec) -> Result<bool> {
              if (ec == make_error_code(Error::NotFound)) {
                return ok(false);
              }
              log::error("dispatch: task {} blocked by depends_on_past "
                         "(persistence error: {})",
                         candidate.idx, ec.message());
              return ok(true);
            })
            .value_or(true);
    if (blocked) {
      depends_on_past_blocked.emplace_back(candidate.idx);
    }
  }

  {
    auto it = state.runs.find(dag_run_id);
    if (it == state.runs.end()) [[unlikely]] {
      co_return ok();
    }
    auto &run = *it->second.run;
    std::unordered_set<NodeIndex> blocked_set;
    blocked_set.reserve(depends_on_past_blocked.size());
    for (const auto idx : depends_on_past_blocked) {
      blocked_set.insert(idx);
    }

    for (auto &candidate : candidates) {
      if (running_tasks_ >= max_concurrency_) {
        break;
      }

      if (!run.is_task_ready(candidate.idx)) {
        continue;
      }

      if (blocked_set.contains(candidate.idx)) {
        continue;
      }

      if (auto r = run.mark_task_started(candidate.idx, candidate.inst_id);
          !r) {
        log::error("dispatch: failed to mark task {} started: {}",
                   candidate.idx, r.error().message());
        co_return fail(r.error());
      }

      jobs.emplace_back(
          TaskJob{candidate.idx, std::move(candidate.task_id),
                  std::move(candidate.inst_id), std::move(candidate.cfg),
                  std::move(candidate.xcom_push),
                  std::move(candidate.xcom_pull), candidate.attempt});
      running_tasks_++;
      const auto &scheduled = jobs.back();
      log::debug("dispatch: scheduled task {} (idx={}, attempt={})",
                 scheduled.task_id, scheduled.idx, scheduled.attempt);
    }

    for (NodeIndex idx : depends_on_past_blocked) {
      if (!run.is_task_ready(idx)) {
        continue;
      }
      auto candidate_it = std::ranges::find_if(
          candidates, [idx](const ReadyCandidate &c) { return c.idx == idx; });
      if (candidate_it == candidates.end()) {
        log::error("dispatch: blocked task {} missing candidate metadata", idx);
        continue;
      }

      if (auto r = run.mark_task_started(idx, candidate_it->inst_id); !r) {
        log::error("dispatch: failed to mark blocked task {} started: {}", idx,
                   r.error().message());
        co_return fail(r.error());
      }
      if (auto r = run.mark_task_failed(idx, "depends_on_past blocked", 0,
                                        kExitCodeImmediateFail);
          !r) {
        log::error("dispatch: failed to fail blocked task {}: {}", idx,
                   r.error().message());
        co_return fail(r.error());
      }
    }

    persist_after_dispatch = !jobs.empty();

    if (!depends_on_past_blocked.empty()) {
      const auto all_infos = run.all_task_info();
      std::unordered_set<NodeIndex> seen;
      seen.reserve(all_infos.size());
      for (const auto &info : all_infos) {
        if (info.state != TaskState::Failed &&
            info.state != TaskState::UpstreamFailed &&
            info.state != TaskState::Skipped) {
          continue;
        }
        if (!seen.insert(info.task_idx).second) {
          continue;
        }
        blocked_to_persist.emplace_back(info);
        blocked_status_updates.emplace_back(
            TaskId{std::string(run.dag().get_key(info.task_idx))}, info.state);
      }
      persist_after_dispatch = true;

      if (run.is_complete()) {
        if (persist_after_dispatch && !run_snapshot) {
          run_snapshot = std::make_shared<DAGRun>(run);
        }
        completed_snapshot = std::make_shared<DAGRun>(run);
        state.runs.erase(it);
        completed = true;
      }
    }

    if (persist_after_dispatch && !completed) {
      run_snapshot = std::make_shared<DAGRun>(run);
    }
  }

  maybe_persist_tasks(dag_run_id, blocked_to_persist);
  if (!blocked_status_updates.empty() && callbacks_.on_task_status) {
    for (const auto &[task_id, task_state] : blocked_status_updates) {
      callbacks_.on_task_status(dag_run_id, task_id, task_state);
    }
  }
  if (persist_after_dispatch && callbacks_.on_persist_run) {
    if (run_snapshot) {
      callbacks_.on_persist_run(run_snapshot);
    }
  }
  if (completed_snapshot) {
    on_run_complete(*completed_snapshot, dag_run_id)
        .or_else([](std::error_code ec) {
          log::error("dispatch: on_run_complete failed: {}", ec.message());
          return ok();
        });
  }
  if (completed) {
    --active_run_count_;
  }

  for (auto &job : jobs) {
    log::info("Executing task {} (idx={}, inst_id={})", job.task_id, job.idx,
              job.inst_id);
    auto coro = run_task(dag_run_id, std::move(job));
    runtime_.spawn_on(target, std::move(coro));
  }

  if (!depends_on_past_blocked.empty()) {
    auto redispatch = dispatch_after_yield(dag_run_id.clone());
    runtime_.spawn_on(target, std::move(redispatch));
  }

  co_return ok();
}

namespace {

// dag_id is pre-fetched asynchronously by run_task before invoking the visitor,
// so all methods here are synchronous — no DB calls or async lookups needed.
struct ExecutionVisitor {
  ExecutionService &service;
  const DAGRunId &dag_run_id;
  ExecutionService::TaskJob &job;
  DAGId dag_id; // pre-fetched by run_task via co_await get_dag_id_by_run
  shard_id resume_shard;

  auto operator()(ShellExecutorConfig &cfg) const -> task<ExecutorResult> {
    apply_templates(cfg.command);
    apply_xcom_pull(cfg.env);
    co_return co_await execute_async(service.runtime(), service.executor(),
                                     job.inst_id, job.cfg, resume_shard);
  }

  auto operator()(DockerExecutorConfig &cfg) const -> task<ExecutorResult> {
    apply_templates(cfg.command);
    apply_xcom_pull(cfg.env);
    co_return co_await execute_async(service.runtime(), service.executor(),
                                     job.inst_id, job.cfg, resume_shard);
  }

  auto operator()(SensorExecutorConfig &cfg) const -> task<ExecutorResult> {
    apply_templates(cfg.target);
    co_return co_await execute_async(service.runtime(), service.executor(),
                                     job.inst_id, job.cfg, resume_shard);
  }

private:
  [[nodiscard]] auto make_template_ctx() const -> TemplateContext {
    auto *run = service.get_run(dag_run_id);
    return TemplateContext{
        .dag_run_id = dag_run_id,
        .dag_id = dag_id,
        .task_id = job.task_id,
        .execution_date = run ? run->execution_date()
                              : std::chrono::system_clock::time_point{},
        .data_interval_start = run ? run->data_interval_start()
                                   : std::chrono::system_clock::time_point{},
        .data_interval_end = run ? run->data_interval_end()
                                 : std::chrono::system_clock::time_point{},
    };
  }

  void apply_templates(std::string &target) const {
    auto ctx = make_template_ctx();
    auto result = service.template_resolver().resolve_template(target, ctx,
                                                               job.xcom_pull);
    if (result) {
      target = std::move(*result);
    } else {
      log::error("template resolution failed: {}", result.error().message());
    }
  }

  void apply_xcom_pull(std::flat_map<std::string, std::string> &env) const {
    if (job.xcom_pull.empty())
      return;

    auto ctx = make_template_ctx();
    auto resolved_env =
        service.template_resolver().resolve_env_vars(ctx, job.xcom_pull);
    if (!resolved_env) {
      log::error("xcom_pull resolution failed for task {}: {}",
                 job.task_id.value(), resolved_env.error().message());
      return;
    }

    for (auto &[name, value] : *resolved_env) {
      env[name] = std::move(value);
      log::debug("xcom_pull: {} injected", name);
    }
  }
};

} // namespace

auto ExecutionService::run_task(DAGRunId dag_run_id, TaskJob job)
    -> spawn_task {
  ++coro_count_;
  struct Guard {
    std::atomic<int> &c;
    ~Guard() { --c; }
  } guard{coro_count_};

  log::debug("run_task: starting {} (idx={}, attempt={})", job.task_id, job.idx,
             job.attempt);
  log::info("run_task: dag_run_id={} task={} inst_id={} attempt={}", dag_run_id,
            job.task_id, job.inst_id, job.attempt);

  std::string start_msg =
      job.attempt > 1 ? std::format("Task '{}' starting (attempt {})",
                                    job.task_id.value(), job.attempt)
                      : std::format("Task '{}' starting", job.task_id.value());

  if (callbacks_.on_log) {
    callbacks_.on_log(dag_run_id, job.task_id, job.attempt, "stdout",
                      start_msg);
  }
  if (callbacks_.on_task_status) {
    callbacks_.on_task_status(dag_run_id, job.task_id, TaskState::Running);
  }

  // --- Pre-fetch dag_id asynchronously (used by ExecutionVisitor) ---
  DAGId dag_id;
  if (auto cached = get_cached_dag_id(dag_run_id)) {
    dag_id = std::move(*cached);
  } else if (callbacks_.get_dag_id_by_run) {
    if (auto r = co_await callbacks_.get_dag_id_by_run(dag_run_id)) {
      dag_id = std::move(*r);
      if (!dag_id.value().empty()) {
        auto &state = shard_state(dag_run_id);
        if (auto it = state.runs.find(dag_run_id); it != state.runs.end()) {
          it->second.dag_id = dag_id.clone();
        }
      }
    } else {
      log::warn("run_task: could not resolve dag_id for run {}: {}", dag_run_id,
                r.error().message());
    }
  }

  // --- Pre-fetch XCom pulls into resolver cache (synchronous visitor reads
  // cache) ---
  if (!job.xcom_pull.empty()) {
    std::unordered_map<std::string, std::unordered_set<std::string>>
        missing_keys_by_task;
    missing_keys_by_task.reserve(job.xcom_pull.size());
    for (const auto &pull : job.xcom_pull) {
      missing_keys_by_task[pull.ref.task_id.str()].insert(pull.ref.key);
    }

    auto erase_task_if_empty = [&](const std::string &task_id_str) {
      auto it = missing_keys_by_task.find(task_id_str);
      if (it != missing_keys_by_task.end() && it->second.empty()) {
        missing_keys_by_task.erase(it);
      }
    };

    // Fast path: owner-shard local XCom cache (same-run produced values).
    auto &state = shard_state(dag_run_id);
    if (auto run_it = state.runs.find(dag_run_id); run_it != state.runs.end()) {
      std::vector<std::pair<std::string, std::string>> cache_hits;
      cache_hits.reserve(job.xcom_pull.size());
      for (const auto &[task_id_str, keys] : missing_keys_by_task) {
        for (const auto &key : keys) {
          auto cache_it = run_it->second.xcom_cache.find({task_id_str, key});
          if (cache_it != run_it->second.xcom_cache.end()) {
            template_resolver_.prefetch_xcom(dag_run_id, TaskId{task_id_str},
                                             key, cache_it->second);
            cache_hits.emplace_back(task_id_str, key);
          }
        }
      }
      for (const auto &[task_id_str, key] : cache_hits) {
        if (auto it = missing_keys_by_task.find(task_id_str);
            it != missing_keys_by_task.end()) {
          it->second.erase(key);
          erase_task_if_empty(task_id_str);
        }
      }
    }

    // Batch path: fetch all run XCom rows once.
    if (!missing_keys_by_task.empty() && callbacks_.get_run_xcoms) {
      auto run_xcoms = co_await callbacks_.get_run_xcoms(dag_run_id);
      if (run_xcoms) {
        for (const auto &entry : *run_xcoms) {
          const auto task_id_str = entry.task_id.str();
          auto it = missing_keys_by_task.find(task_id_str);
          if (it == missing_keys_by_task.end()) {
            continue;
          }
          if (!it->second.contains(entry.key)) {
            continue;
          }
          template_resolver_.prefetch_xcom(dag_run_id, entry.task_id, entry.key,
                                           entry.value);
          it->second.erase(entry.key);
          erase_task_if_empty(task_id_str);
        }
      } else if (run_xcoms.error() != make_error_code(Error::NotFound)) {
        log::warn("run_task: run-level xcom prefetch failed for {}: {}",
                  dag_run_id, run_xcoms.error().message());
      }
    }

    // Fallback path: fetch task-level sets.
    if (!missing_keys_by_task.empty() && callbacks_.get_task_xcoms) {
      std::vector<std::string> source_tasks;
      source_tasks.reserve(missing_keys_by_task.size());
      for (const auto &[task_id_str, _] : missing_keys_by_task) {
        source_tasks.emplace_back(task_id_str);
      }
      for (const auto &task_id_str : source_tasks) {
        auto it = missing_keys_by_task.find(task_id_str);
        if (it == missing_keys_by_task.end() || it->second.empty()) {
          continue;
        }
        TaskId source_task{task_id_str};
        auto task_xcoms =
            co_await callbacks_.get_task_xcoms(dag_run_id, source_task);
        if (!task_xcoms) {
          if (task_xcoms.error() != make_error_code(Error::NotFound)) {
            log::warn("run_task: task-level xcom prefetch failed for {}: {}",
                      source_task, task_xcoms.error().message());
          }
          continue;
        }
        for (const auto &entry : *task_xcoms) {
          if (it->second.contains(entry.key)) {
            template_resolver_.prefetch_xcom(dag_run_id, source_task, entry.key,
                                             entry.value);
            it->second.erase(entry.key);
          }
        }
        erase_task_if_empty(task_id_str);
      }
    }

    // Final fallback: remaining single-key reads.
    if (!missing_keys_by_task.empty() && callbacks_.get_xcom) {
      for (const auto &[task_id_str, keys] : missing_keys_by_task) {
        TaskId source_task{task_id_str};
        for (const auto &key : keys) {
          auto xcom =
              co_await callbacks_.get_xcom(dag_run_id, source_task, key);
          if (xcom) {
            template_resolver_.prefetch_xcom(dag_run_id, source_task, key,
                                             xcom->value);
          } else if (xcom.error() != make_error_code(Error::NotFound)) {
            log::warn("run_task: xcom_pull prefetch failed for {}/{}: {}",
                      source_task, key, xcom.error().message());
          }
        }
      }
    }
  }

  const auto target = owner_shard(dag_run_id);
  auto result =
      co_await std::visit(ExecutionVisitor{.service = *this,
                                           .dag_run_id = dag_run_id,
                                           .job = job,
                                           .dag_id = std::move(dag_id),
                                           .resume_shard = target},
                          job.cfg);
  // After co_await, we are guaranteed to be on owner shard (target)
  // because execute_async uses schedule_on(target) to resume.

  running_tasks_--;
  log::info("run_task: completed dag_run_id={} task={} inst_id={} exit_code={} "
            "err='{}'",
            dag_run_id, job.task_id, job.inst_id, result.exit_code,
            result.error);

  if (!result.stdout_output.empty()) {
    if (callbacks_.on_log) {
      callbacks_.on_log(dag_run_id, job.task_id, job.attempt, "stdout",
                        result.stdout_output);
    }
  }
  if (!result.stderr_output.empty()) {
    if (callbacks_.on_log) {
      callbacks_.on_log(dag_run_id, job.task_id, job.attempt, "stderr",
                        result.stderr_output);
    }
  }

  if (result.exit_code == 0 && result.error.empty()) {
    if (!job.xcom_push.empty()) {
      auto xcoms = xcom::extract(result, job.xcom_push);
      if (xcoms) {
        auto &state = shard_state(dag_run_id);
        for (const auto &xcom : *xcoms) {
          // Cache synchronously so branch reads in on_task_success don't race
          // against the async DB persistence write.
          if (auto it = state.runs.find(dag_run_id); it != state.runs.end()) {
            it->second.xcom_cache[{job.task_id.str(), xcom.key}] = xcom.value;
          }
          if (callbacks_.on_persist_xcom) {
            // Fire-and-forget: the Actor mailbox handles persistence
            // asynchronously.
            callbacks_.on_persist_xcom(dag_run_id, job.task_id, xcom.key,
                                       xcom.value);
          }
        }
      } else {
        log::warn("XCom extraction failed for {} {}: {}", dag_run_id,
                  job.task_id, xcoms.error().message());
      }
    }

    std::string success_msg =
        std::format("Task '{}' completed successfully", job.task_id.value());
    if (callbacks_.on_log) {
      callbacks_.on_log(dag_run_id, job.task_id, job.attempt, "stdout",
                        success_msg);
    }
    if (callbacks_.on_task_status) {
      callbacks_.on_task_status(dag_run_id, job.task_id, TaskState::Success);
    }
    log::debug("run_task: calling on_task_success for {} (idx={})", job.task_id,
               job.idx);
    // Already on owner shard — direct co_await, no cross-shard hop.
    if (auto r = co_await on_task_success(dag_run_id, job.idx); !r) {
      log::error("run_task: on_task_success failed: {}", r.error().message());
    }
  } else if (result.exit_code == kExitCodeSkip) {
    std::string skip_msg =
        std::format("Task '{}' skipped (exit code 100)", job.task_id.value());
    if (callbacks_.on_log) {
      callbacks_.on_log(dag_run_id, job.task_id, job.attempt, "stdout",
                        skip_msg);
    }
    if (callbacks_.on_task_status) {
      callbacks_.on_task_status(dag_run_id, job.task_id, TaskState::Skipped);
    }
    on_task_skipped(dag_run_id, job.idx).or_else([](std::error_code ec) {
      log::error("run_task: on_task_skipped failed: {}", ec.message());
      return ok();
    });
  } else if (result.exit_code == kExitCodeImmediateFail) {
    std::string fail_msg = std::format(
        "Task '{}' failed immediately (exit code 101)", job.task_id.value());
    if (callbacks_.on_log) {
      callbacks_.on_log(dag_run_id, job.task_id, job.attempt, "stderr",
                        fail_msg);
    }
    if (callbacks_.on_task_status) {
      callbacks_.on_task_status(dag_run_id, job.task_id, TaskState::Failed);
    }
    on_task_fail_immediately(dag_run_id, job.idx, result.error,
                             result.exit_code)
        .or_else([](std::error_code ec) {
          log::error("run_task: on_task_fail_immediately failed: {}",
                     ec.message());
          return ok();
        });
  } else {
    std::string error_msg =
        std::format("Task '{}' failed: {}", job.task_id.value(), result.error);
    if (callbacks_.on_log) {
      callbacks_.on_log(dag_run_id, job.task_id, job.attempt, "stderr",
                        error_msg);
    }
    auto retry_result =
        on_task_failure(dag_run_id, job.idx, result.error, result.exit_code)
            .or_else([](std::error_code ec) {
              log::error("run_task: on_task_failure failed: {}", ec.message());
              return ok(false);
            });
    const bool will_retry = retry_result.value_or(false);
    if (callbacks_.on_task_status) {
      callbacks_.on_task_status(dag_run_id, job.task_id,
                                will_retry ? TaskState::Retrying
                                           : TaskState::Failed);
    }
    if (will_retry) {
      auto retry_interval = std::chrono::seconds(60);
      if (callbacks_.get_retry_interval) {
        retry_interval = callbacks_.get_retry_interval(dag_run_id, job.idx);
      }
      auto retry =
          dispatch_after_delay(dag_run_id.clone(), job.idx, retry_interval);
      runtime_.spawn_on(target, std::move(retry));
    }
  }
}

auto ExecutionService::dispatch_after_delay(DAGRunId dag_run_id,
                                            NodeIndex retry_idx,
                                            std::chrono::seconds delay)
    -> spawn_task {
  co_await async_sleep(delay);
  auto target = owner_shard(dag_run_id);
  if (!runtime_.is_current_shard() || runtime_.current_shard() != target) {
    auto repost = dispatch_after_delay(dag_run_id.clone(), retry_idx,
                                       std::chrono::seconds{0});
    runtime_.spawn_on(target, std::move(repost));
    co_return;
  }

  auto &state = shard_states_[target];
  if (auto it = state.runs.find(dag_run_id); it != state.runs.end()) {
    if (auto r = it->second.run->mark_task_retry_ready(retry_idx); !r) {
      log::debug("dispatch_after_delay: skip retry arm for {} idx={} ({})",
                 dag_run_id, retry_idx, r.error().message());
      co_return;
    }
  } else {
    co_return;
  }

  auto coro = dispatch(dag_run_id.clone());
  runtime_.spawn_on(target, std::move(coro));
}

auto ExecutionService::dispatch_after_yield(DAGRunId dag_run_id) -> spawn_task {
  co_await async_yield();
  auto coro = dispatch(dag_run_id.clone());
  runtime_.spawn_on(owner_shard(dag_run_id), std::move(coro));
}

auto ExecutionService::on_task_success(const DAGRunId &dag_run_id,
                                       NodeIndex idx) -> task<Result<void>> {
  std::optional<TaskInstanceInfo> persisted_task_info;
  std::vector<TaskInstanceInfo> propagated_terminal_infos;
  std::vector<std::pair<TaskId, TaskState>> propagated_status_updates;
  std::shared_ptr<DAGRun> completed_snapshot;
  auto &state = shard_state(dag_run_id);
  log::info("on_task_success: dag_run_id={} idx={}", dag_run_id, idx);

  bool is_branch = false;
  std::string branch_key = "branch";
  TaskId task_id_for_branch;
  {
    auto it = state.runs.find(dag_run_id);
    if (it == state.runs.end())
      co_return ok();

    auto &run = *it->second.run;
    task_id_for_branch = TaskId{std::string(run.dag().get_key(idx))};

    {
      const auto &task_cfgs = it->second.task_configs;
      if (idx < task_cfgs.size()) {
        is_branch = task_cfgs[idx].is_branch;
        branch_key = task_cfgs[idx].branch_xcom_key;
      }
    }
  }

  // --- XCom read for branch tasks (before mutating run state) ---
  // Check in-memory cache first to avoid racing against async DB persistence.
  std::vector<TaskId> branch_selected;
  if (is_branch) {
    auto &state2 = shard_state(dag_run_id);
    auto run_it = state2.runs.find(dag_run_id);
    if (run_it != state2.runs.end()) {
      auto cache_it = run_it->second.xcom_cache.find(
          {task_id_for_branch.str(), branch_key});
      if (cache_it != run_it->second.xcom_cache.end()) {
        const auto &val = cache_it->second;
        if (val.is_array()) {
          for (const auto &item : val.get_array()) {
            if (item.is_string()) {
              branch_selected.emplace_back(item.as<std::string>());
            }
          }
        }
      }
    }
    if (branch_selected.empty() && callbacks_.get_xcom) {
      auto xcom = co_await callbacks_.get_xcom(dag_run_id, task_id_for_branch,
                                               branch_key);
      if (xcom && xcom->value.is_array()) {
        for (const auto &item : xcom->value.get_array()) {
          if (item.is_string()) {
            branch_selected.emplace_back(item.as<std::string>());
          }
        }
      }
    }
  }

  {
    auto it = state.runs.find(dag_run_id);
    if (it == state.runs.end())
      co_return ok();

    auto &run = *it->second.run;

    if (is_branch) {
      if (auto r = run.mark_task_completed_with_branch(idx, 0, branch_selected);
          !r) {
        log::error("on_task_success: failed to mark branch task {}: {}", idx,
                   r.error().message());
        co_return fail(r.error());
      }
    } else {
      if (auto r = run.mark_task_completed(idx, 0); !r) {
        log::error("on_task_success: failed to mark task {}: {}", idx,
                   r.error().message());
        co_return fail(r.error());
      }
    }

    if (auto info = run.get_task_info(idx); info) {
      persisted_task_info = *info;
    }

    for (const auto &info : run.all_task_info()) {
      if (info.state == TaskState::Skipped ||
          info.state == TaskState::UpstreamFailed) {
        propagated_terminal_infos.emplace_back(info);
        propagated_status_updates.emplace_back(
            TaskId{std::string(run.dag().get_key(info.task_idx))}, info.state);
      }
    }

    if (run.is_complete()) {
      completed_snapshot = std::make_shared<DAGRun>(run);
      state.runs.erase(it);
    }
  }

  finalize_task_transition(dag_run_id, std::move(persisted_task_info),
                           std::move(propagated_terminal_infos),
                           std::move(propagated_status_updates),
                           std::move(completed_snapshot));

  co_return ok();
}

auto ExecutionService::on_task_failure(const DAGRunId &dag_run_id,
                                       NodeIndex idx, std::string_view error,
                                       int exit_code) -> Result<bool> {

  log::info("on_task_failure: dag_run_id={} idx={} err='{}'", dag_run_id, idx,
            error);

  bool needs_retry = false;
  std::vector<TaskInstanceInfo> persisted_infos;
  std::vector<std::pair<TaskId, TaskState>> status_updates;
  std::shared_ptr<DAGRun> completed_snapshot;
  auto &state = shard_state(dag_run_id);

  {
    auto it = state.runs.find(dag_run_id);
    if (it == state.runs.end())
      return ok(false);

    auto &run = *it->second.run;

    int max_retries = 3;
    if (callbacks_.get_max_retries) {
      max_retries = callbacks_.get_max_retries(dag_run_id, idx);
    }
    if (idx < it->second.task_configs.size()) {
      const auto &task_cfg = it->second.task_configs[idx];
      // Sensor tasks already perform internal polling until timeout.
      // Applying default DAG-level retries multiplies wait time and looks
      // like a stall in UI. Keep explicit user overrides, but disable retries
      // for sensors when still using project default.
      if (task_cfg.executor == ExecutorType::Sensor &&
          max_retries == task_defaults::kMaxRetries) {
        max_retries = 0;
      }
    }

    run.mark_task_failed(idx, std::string(error), max_retries, exit_code)
        .or_else([&](std::error_code ec) {
          log::error("on_task_failure: failed to mark task {}: {}", idx,
                     ec.message());
          return ok();
        });

    if (auto info = run.get_task_info(idx)) {
      needs_retry = (info->state == TaskState::Retrying);
    }

    for (const auto &info : run.all_task_info()) {
      if (info.state == TaskState::Failed ||
          info.state == TaskState::UpstreamFailed ||
          info.state == TaskState::Skipped ||
          info.state == TaskState::Pending) {
        persisted_infos.emplace_back(info);
        if (info.state == TaskState::UpstreamFailed ||
            info.state == TaskState::Skipped) {
          status_updates.emplace_back(
              TaskId{std::string(run.dag().get_key(info.task_idx))},
              info.state);
        }
      }
    }

    if (run.is_complete()) {
      completed_snapshot = std::make_shared<DAGRun>(run);
      state.runs.erase(it);
      needs_retry = false;
    }
  }

  const bool run_complete = static_cast<bool>(completed_snapshot);
  finalize_task_transition(dag_run_id, std::nullopt, std::move(persisted_infos),
                           std::move(status_updates),
                           std::move(completed_snapshot));
  return ok(run_complete ? false : needs_retry);
}

auto ExecutionService::on_run_complete(DAGRun &run, const DAGRunId &dag_run_id)
    -> Result<void> {
  // Capture state before the potential move so callbacks still see it.
  const auto run_state = run.state();
  if (callbacks_.on_persist_run) {
    callbacks_.on_persist_run(std::make_shared<DAGRun>(std::move(run)));
  }
  if (callbacks_.on_run_status) {
    callbacks_.on_run_status(dag_run_id, run_state);
  }
  log::info("DAG run {} {}", dag_run_id, to_string_view(run_state));
  return ok();
}

auto ExecutionService::on_task_skipped(const DAGRunId &dag_run_id,
                                       NodeIndex idx) -> Result<void> {
  log::info("on_task_skipped: dag_run_id={} idx={}", dag_run_id, idx);

  std::optional<TaskInstanceInfo> persisted_task_info;
  std::vector<TaskInstanceInfo> propagated_terminal_infos;
  std::vector<std::pair<TaskId, TaskState>> status_updates;
  std::shared_ptr<DAGRun> completed_snapshot;
  auto &state = shard_state(dag_run_id);

  {
    auto it = state.runs.find(dag_run_id);
    if (it == state.runs.end())
      return ok();

    auto &run = *it->second.run;
    run.mark_task_skipped(idx).or_else([&](std::error_code ec) {
      log::error("on_task_skipped: failed to mark task {}: {}", idx,
                 ec.message());
      return ok();
    });

    if (auto info = run.get_task_info(idx); info) {
      persisted_task_info = *info;
    }

    for (const auto &info : run.all_task_info()) {
      if (info.state == TaskState::Skipped ||
          info.state == TaskState::UpstreamFailed) {
        propagated_terminal_infos.emplace_back(info);
        status_updates.emplace_back(
            TaskId{std::string(run.dag().get_key(info.task_idx))}, info.state);
      }
    }

    if (run.is_complete()) {
      completed_snapshot = std::make_shared<DAGRun>(run);
      state.runs.erase(it);
    }
  }

  finalize_task_transition(dag_run_id, std::move(persisted_task_info),
                           std::move(propagated_terminal_infos),
                           std::move(status_updates),
                           std::move(completed_snapshot));

  return ok();
}

auto ExecutionService::on_task_fail_immediately(const DAGRunId &dag_run_id,
                                                NodeIndex idx,
                                                std::string_view error,
                                                int exit_code) -> Result<void> {

  log::info("on_task_fail_immediately: dag_run_id={} idx={} err='{}'",
            dag_run_id, idx, error);

  std::optional<TaskInstanceInfo> persisted_task_info;
  std::vector<TaskInstanceInfo> propagated_terminal_infos;
  std::vector<std::pair<TaskId, TaskState>> status_updates;
  std::shared_ptr<DAGRun> completed_snapshot;
  auto &state = shard_state(dag_run_id);

  {
    auto it = state.runs.find(dag_run_id);
    if (it == state.runs.end())
      return ok();

    auto &run = *it->second.run;
    run.mark_task_failed(idx, std::string(error), 0, exit_code)
        .or_else([&](std::error_code ec) {
          log::error("on_task_fail_immediately: failed to mark task {}: {}",
                     idx, ec.message());
          return ok();
        });

    if (auto info = run.get_task_info(idx); info) {
      persisted_task_info = *info;
    }

    for (const auto &info : run.all_task_info()) {
      if (info.state == TaskState::UpstreamFailed ||
          info.state == TaskState::Skipped) {
        propagated_terminal_infos.emplace_back(info);
        status_updates.emplace_back(
            TaskId{std::string(run.dag().get_key(info.task_idx))}, info.state);
      }
    }

    if (run.is_complete()) {
      completed_snapshot = std::make_shared<DAGRun>(run);
      state.runs.erase(it);
    }
  }

  finalize_task_transition(dag_run_id, std::move(persisted_task_info),
                           std::move(propagated_terminal_infos),
                           std::move(status_updates),
                           std::move(completed_snapshot));

  return ok();
}

auto ExecutionService::finalize_task_transition(
    const DAGRunId &dag_run_id, std::optional<TaskInstanceInfo> persisted_info,
    std::vector<TaskInstanceInfo> persisted_infos,
    std::vector<std::pair<TaskId, TaskState>> status_updates,
    std::shared_ptr<DAGRun> completed_snapshot) -> void {
  if (persisted_info) {
    maybe_persist_task(dag_run_id, *persisted_info);
  }
  if (!persisted_infos.empty()) {
    maybe_persist_tasks(dag_run_id, persisted_infos);
  }
  if (!status_updates.empty() && callbacks_.on_task_status) {
    for (const auto &[task_id, task_state] : status_updates) {
      if (!task_id.value().empty()) {
        callbacks_.on_task_status(dag_run_id, task_id, task_state);
      }
    }
  }
  if (completed_snapshot) {
    on_run_complete(*completed_snapshot, dag_run_id)
        .or_else([](std::error_code ec) {
          log::error("finalize_task_transition: on_run_complete failed: {}",
                     ec.message());
          return ok();
        });
    --active_run_count_;
  }
  notify_capacity_available();
}

} // namespace dagforge
