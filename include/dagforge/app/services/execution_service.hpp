#pragma once

#include "dagforge/config/task_config.hpp"
#include "dagforge/core/coroutine.hpp"
#include "dagforge/core/error.hpp"
#include "dagforge/core/shard.hpp"
#include "dagforge/dag/dag.hpp"
#include "dagforge/dag/dag_run.hpp"
#include "dagforge/executor/executor.hpp"
#include "dagforge/util/id.hpp"
#include "dagforge/util/json.hpp"
#include "dagforge/xcom/template_resolver.hpp"

#include <atomic>
#include <cassert>
#include <deque>
#include <flat_map>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <vector>

namespace dagforge {

class Runtime;
class IExecutor;
struct TaskConfig;
struct XComPushConfig;

struct ExecutionCallbacks {
  std::move_only_function<void(const DAGRunId &dag_run_id, const TaskId &task,
                               TaskState status)>
      on_task_status;
  std::move_only_function<void(const DAGRunId &dag_run_id, DAGRunState status)>
      on_run_status;
  std::move_only_function<void(const DAGRunId &dag_run_id, const TaskId &task,
                               int attempt, std::string_view stream,
                               std::string_view msg)>
      on_log;
  // Fire-and-forget: called once when a run finishes; zero copies via
  // shared_ptr.
  std::move_only_function<void(std::shared_ptr<const DAGRun> run)>
      on_persist_run;
  // Fire-and-forget: per-task status persisted as it changes.
  std::move_only_function<void(const DAGRunId &dag_run_id,
                               const TaskInstanceInfo &info)>
      on_persist_task;
  // Fire-and-forget: XCom values are sent to the persistence actor.
  std::move_only_function<void(const DAGRunId &dag_run_id, const TaskId &task,
                               std::string_view key, const JsonValue &value)>
      on_persist_xcom;
  // Async reads — all go through the persistence Actor (ask pattern).
  std::move_only_function<task<Result<XComEntry>>(
      const DAGRunId &dag_run_id, const TaskId &task, std::string_view key)>
      get_xcom;
  std::move_only_function<task<Result<std::vector<XComEntry>>>(
      const DAGRunId &dag_run_id, const TaskId &task)>
      get_task_xcoms;
  std::move_only_function<task<Result<std::vector<XComTaskEntry>>>(
      const DAGRunId &dag_run_id)>
      get_run_xcoms;
  std::move_only_function<task<Result<DAGId>>(const DAGRunId &dag_run_id)>
      get_dag_id_by_run;
  std::move_only_function<task<Result<TaskState>>(
      const DAGRunId &dag_run_id, NodeIndex idx,
      std::chrono::system_clock::time_point execution_date,
      const DAGRunId &current_dag_run_id)>
      check_previous_task_state;
  // Synchronous reads from in-memory DAGManager (no DB round-trip needed).
  std::move_only_function<int(const DAGRunId &dag_run_id, NodeIndex idx)>
      get_max_retries;
  std::move_only_function<std::chrono::seconds(const DAGRunId &dag_run_id,
                                               NodeIndex idx)>
      get_retry_interval;
};

class ExecutionService {
public:
  ExecutionService(Runtime &runtime, IExecutor &executor);
  ~ExecutionService();

  ExecutionService(const ExecutionService &) = delete;
  auto operator=(const ExecutionService &) -> ExecutionService & = delete;

  auto set_callbacks(ExecutionCallbacks callbacks) -> void;

  struct RunContext {
    std::unique_ptr<DAGRun> run;
    std::shared_ptr<const std::vector<ExecutorConfig>> executor_configs;
    std::shared_ptr<const std::vector<TaskConfig>> task_configs;
    std::optional<DAGId> dag_id;
  };

  auto start_run(const DAGRunId &dag_run_id, RunContext ctx) -> void;

  [[nodiscard]] auto get_cached_dag_id(const DAGRunId &dag_run_id) const
      -> std::optional<DAGId>;

  // Non-blocking local lookup. Returns nullptr if called from a non-owner
  // shard.
  [[nodiscard]] auto get_run(const DAGRunId &dag_run_id) -> DAGRun *;
  // Cross-shard safe query that returns a deep snapshot.
  [[nodiscard]] auto get_run_snapshot(const DAGRunId &dag_run_id)
      -> task<Result<std::unique_ptr<DAGRun>>>;

  [[nodiscard]] auto has_active_runs() const -> bool;

  auto wait_for_completion_async(int timeout_ms) -> task<void>;

  [[nodiscard]] auto coro_count() const -> int;
  [[nodiscard]] auto dispatch_invocations() const -> std::uint64_t;
  [[nodiscard]] auto dispatch_scan_invocations() const -> std::uint64_t;

  auto set_max_concurrency(int max_concurrency) -> void;

  [[nodiscard]] auto runtime() noexcept -> Runtime & { return runtime_; }
  [[nodiscard]] auto executor() noexcept -> IExecutor & { return executor_; }
  [[nodiscard]] auto template_resolver() noexcept -> TemplateResolver & {
    return template_resolver_;
  }
  auto emit_log_chunk(const DAGRunId &dag_run_id, const TaskId &task_id,
                      int attempt, std::string_view stream,
                      std::string_view msg) -> void;

  struct TaskJob {
    NodeIndex idx{kInvalidNode};
    TaskId task_id;
    InstanceId inst_id;
    ExecutorConfig cfg;
    std::vector<XComPushConfig> xcom_push;
    std::vector<XComPullConfig> xcom_pull;
    int attempt{0};
  };

private:
  struct ActiveRunState {
    std::unique_ptr<DAGRun> run;
    std::shared_ptr<const std::vector<ExecutorConfig>> executor_configs;
    std::shared_ptr<const std::vector<TaskConfig>> task_configs;
    std::optional<DAGId> dag_id;
    std::map<std::pair<std::string, std::string>, JsonValue> xcom_cache;
    bool dispatch_scheduled{false};
    bool redispatch_requested{false};
    bool queued_for_dispatch{false};
  };

  struct TransitionEffects {
    std::optional<TaskInstanceInfo> persisted_task_info;
    std::vector<TaskInstanceInfo> persisted_infos;
    std::vector<std::pair<TaskId, TaskState>> status_updates;
    std::shared_ptr<DAGRun> completed_snapshot;
  };

  using RunMap = std::flat_map<DAGRunId, ActiveRunState>;

  // ---- Per-shard state (single-writer, no mutex needed on write path) ----
  struct ShardState {
    RunMap runs;
    std::deque<DAGRunId> ready_run_queue;
    bool dispatch_scan_scheduled{false};
    bool dispatch_scan_requested{false};
  };

  struct alignas(64) QueueSizeCounter {
    std::atomic<std::uint32_t> value{0};
  };

  auto dispatch(DAGRunId dag_run_id) -> task<Result<void>>;
  auto dispatch_pending() -> void;
  auto dispatch_after_yield(DAGRunId dag_run_id) -> spawn_task;
  auto dispatch_after_delay(DAGRunId dag_run_id, NodeIndex retry_idx,
                            std::chrono::seconds delay) -> spawn_task;
  auto run_task(DAGRunId dag_run_id, TaskJob job) -> spawn_task;
  auto on_task_success(const DAGRunId &dag_run_id, NodeIndex idx)
      -> task<Result<void>>;
  auto on_task_failure(const DAGRunId &dag_run_id, NodeIndex idx,
                       std::string_view error, int exit_code, bool timed_out)
      -> Result<bool>;
  auto on_task_skipped(const DAGRunId &dag_run_id, NodeIndex idx)
      -> Result<void>;
  auto on_task_fail_immediately(const DAGRunId &dag_run_id, NodeIndex idx,
                                std::string_view error, int exit_code)
      -> Result<void>;
  auto record_task_snapshot(const DAGRun &run, NodeIndex idx,
                            TransitionEffects &effects) const -> void;
  auto collect_propagated_terminal_updates(const DAGRun &run,
                                           TransitionEffects &effects) const
      -> void;
  auto collect_failure_updates(const DAGRun &run,
                               TransitionEffects &effects) const -> void;
  auto complete_transition_if_needed(ShardState &state, RunMap::iterator it,
                                     TransitionEffects &effects) -> void;
  auto finalize_task_transition(
      const DAGRunId &dag_run_id, TransitionEffects effects) -> void;
  auto on_run_complete(DAGRun &run, const DAGRunId &dag_run_id) -> Result<void>;
  auto maybe_persist_task(const DAGRunId &dag_run_id,
                          const TaskInstanceInfo &info) -> void;
  auto maybe_persist_tasks(const DAGRunId &dag_run_id,
                           std::span<const TaskInstanceInfo> infos) -> void;

  [[nodiscard]] auto owner_shard(const DAGRunId &dag_run_id) const noexcept
      -> shard_id;
  auto post_to_owner(const DAGRunId &dag_run_id,
                     std::move_only_function<void()> fn) -> void;
  [[nodiscard]] auto shard_state(const DAGRunId &dag_run_id) -> ShardState & {
    return shard_states_[owner_shard(dag_run_id)];
  }
  auto dispatch_pending_on_shard(shard_id sid) -> void;
  auto notify_capacity_available() -> void;
  auto schedule_dispatch_scan(shard_id sid) -> void;
  auto schedule_dispatch(const DAGRunId &dag_run_id) -> void;
  auto enqueue_ready_run(ShardState &state, const DAGRunId &dag_run_id)
      -> void;
  auto schedule_dispatch_on_owner(ShardState &state, const DAGRunId &dag_run_id)
      -> void;
  auto finish_dispatch_cycle(shard_id sid, const DAGRunId &dag_run_id) -> void;

  Runtime &runtime_;
  IExecutor &executor_;
  ExecutionCallbacks callbacks_;
  TemplateResolver template_resolver_;

  std::vector<ShardState> shard_states_;
  std::vector<QueueSizeCounter> ready_run_queue_sizes_;
  std::atomic<std::uint32_t> notify_rr_{0};
  std::atomic<int> active_run_count_{0};
  std::atomic<int> coro_count_{0};
  std::atomic<int> running_tasks_{0};
  std::atomic<std::uint64_t> dispatch_invocations_{0};
  std::atomic<std::uint64_t> dispatch_scan_invocations_{0};
  int max_concurrency_{100};

  friend struct ExecutionVisitor;
};

} // namespace dagforge
