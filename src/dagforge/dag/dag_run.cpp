#include "dagforge/dag/dag_run.hpp"

#include "dagforge/config/task_config.hpp"
#include "dagforge/core/error.hpp"
#include "dagforge/util/log.hpp"

#include <algorithm>
#include <atomic>
#include <boost/dynamic_bitset.hpp>
#include <cassert>
#include <deque>
#include <ranges>
#include <span>

namespace dagforge {

struct DAGRunDependencyState {
  explicit DAGRunDependencyState(std::size_t n)
      : in_degree(n, 0), terminal_dep_count(n, 0), success_dep_count(n, 0),
        failed_dep_count(n, 0), skipped_dep_count(n, 0),
        upstream_failed_dep_count(n, 0) {}

  std::vector<int> in_degree;
  std::vector<int> terminal_dep_count;
  std::vector<int> success_dep_count;
  std::vector<int> failed_dep_count;
  std::vector<int> skipped_dep_count;
  std::vector<int> upstream_failed_dep_count;
};

struct DAGRunRuntimeState {
  explicit DAGRunRuntimeState(std::size_t n)
      : ready_mask(n), running_mask(n), completed_mask(n), failed_mask(n),
        skipped_mask(n) {}

  DAGRunRuntimeState(const DAGRunRuntimeState &other)
      : ready_mask(other.ready_mask), running_mask(other.running_mask),
        completed_mask(other.completed_mask), failed_mask(other.failed_mask),
        skipped_mask(other.skipped_mask),
        pending_count(other.pending_count), ready_count(other.ready_count),
        running_count(other.running_count),
        completed_count(other.completed_count),
        failed_count(other.failed_count), skipped_count(other.skipped_count),
        failed_count_fast(other.failed_count_fast.load(
            std::memory_order_relaxed)),
        ready_queue(other.ready_queue) {}

  auto operator=(const DAGRunRuntimeState &other) -> DAGRunRuntimeState & {
    if (this != &other) {
      ready_mask = other.ready_mask;
      running_mask = other.running_mask;
      completed_mask = other.completed_mask;
      failed_mask = other.failed_mask;
      skipped_mask = other.skipped_mask;
      pending_count = other.pending_count;
      ready_count = other.ready_count;
      running_count = other.running_count;
      completed_count = other.completed_count;
      failed_count = other.failed_count;
      skipped_count = other.skipped_count;
      failed_count_fast.store(other.failed_count_fast.load(
                                  std::memory_order_relaxed),
                              std::memory_order_relaxed);
      ready_queue = other.ready_queue;
    }
    return *this;
  }

  auto terminal_count() const noexcept -> std::size_t {
    return completed_count + failed_count + skipped_count;
  }

  boost::dynamic_bitset<> ready_mask;
  boost::dynamic_bitset<> running_mask;
  boost::dynamic_bitset<> completed_mask;
  boost::dynamic_bitset<> failed_mask;
  boost::dynamic_bitset<> skipped_mask;

  std::size_t pending_count{0};
  std::size_t ready_count{0};
  std::size_t running_count{0};
  std::size_t completed_count{0};
  std::size_t failed_count{0};
  std::size_t skipped_count{0};
  std::atomic<std::size_t> failed_count_fast{0};

  std::deque<NodeIndex> ready_queue;
};

struct DAGRun::Impl {
  Impl(DAGRunId dag_run_id, std::shared_ptr<const DAG> dag)
      : dag_run_id_(std::move(dag_run_id)), dag_(std::move(dag)),
        deps_(dag_->size()), runtime_(dag_->size()) {
    std::size_t n = dag_->size();
    task_info_.resize(n);

    for (auto i : std::views::iota(0u, n)) {
      NodeIndex idx = static_cast<NodeIndex>(i);
      task_info_[i].task_idx = idx;
      task_info_[i].state = TaskState::Pending;
      deps_.in_degree[i] = static_cast<int>(dag_->get_deps_view(idx).size());
    }

    runtime_.pending_count = n;
    init_ready_set();
  }

  Impl(const Impl &other)
      : dag_run_id_(other.dag_run_id_), dag_(other.dag_), state_(other.state_),
        scheduled_at_(other.scheduled_at_), started_at_(other.started_at_),
        finished_at_(other.finished_at_),
        execution_date_(other.execution_date_),
        data_interval_start_(other.data_interval_start_),
        data_interval_end_(other.data_interval_end_),
        trigger_type_(other.trigger_type_), deps_(other.deps_),
        runtime_(other.runtime_),
        task_info_(other.task_info_),
        run_rowid_(other.run_rowid_), dag_rowid_(other.dag_rowid_),
        dag_version_(other.dag_version_) {}

  auto update_state() -> void {
    if (runtime_.ready_count > 0 || runtime_.running_count > 0) {
      return;
    }

    if (runtime_.failed_count > 0 && runtime_.pending_count == 0) {
      state_ = DAGRunState::Failed;
      finished_at_ = std::chrono::system_clock::now();
      return;
    }

    if (runtime_.terminal_count() == dag_->size()) {
      state_ = (runtime_.failed_count > 0) ? DAGRunState::Failed : DAGRunState::Success;
      finished_at_ = std::chrono::system_clock::now();
    }
#ifndef NDEBUG
    verify_invariants();
#endif
  }

  auto init_ready_set() -> void {
    runtime_.ready_mask.reset();
    runtime_.ready_count = 0;
    runtime_.ready_queue.clear();

    for (auto [i, deg] : std::views::enumerate(deps_.in_degree)) {
      if (deg == 0) {
        runtime_.ready_mask.set(static_cast<std::size_t>(i));
        runtime_.ready_queue.push_back(static_cast<NodeIndex>(i));
        ++runtime_.ready_count;
      } else if (dag_->get_trigger_rule(static_cast<NodeIndex>(i)) ==
                 TriggerRule::Always) {
        // Airflow-compatible: ALWAYS does not wait for upstream completion.
        runtime_.ready_mask.set(static_cast<std::size_t>(i));
        runtime_.ready_queue.push_back(static_cast<NodeIndex>(i));
        ++runtime_.ready_count;
      }
    }
    runtime_.pending_count -= runtime_.ready_count;
#ifndef NDEBUG
    verify_invariants();
#endif
  }

#ifndef NDEBUG
  auto verify_invariants() const noexcept -> void {
    const auto n = dag_->size();
    const auto terminal_count = runtime_.terminal_count();
    std::size_t ready_list_size = 0;
    for (auto idx : runtime_.ready_queue) {
      if (runtime_.ready_mask.test(idx)) {
        ++ready_list_size;
      }
    }
    assert(runtime_.ready_count == ready_list_size);
    assert(runtime_.ready_count == runtime_.ready_mask.count());
    assert(runtime_.running_count == runtime_.running_mask.count());
    assert(runtime_.completed_count == runtime_.completed_mask.count());
    assert(runtime_.failed_count == runtime_.failed_mask.count());
    assert(runtime_.skipped_count == runtime_.skipped_mask.count());
    assert(terminal_count <= n);
    assert(runtime_.pending_count + runtime_.ready_count + runtime_.running_count + terminal_count ==
           n);
  }
#endif

  auto update_dependent_counters(NodeIndex terminal_task, TaskState state,
                                 int delta) -> void {
    for (NodeIndex dep : dag_->get_dependents_view(terminal_task)) {
      deps_.terminal_dep_count[dep] += delta;
      switch (state) {
      case TaskState::Success:
        deps_.success_dep_count[dep] += delta;
        break;
      case TaskState::Failed:
        deps_.failed_dep_count[dep] += delta;
        break;
      case TaskState::UpstreamFailed:
        deps_.upstream_failed_dep_count[dep] += delta;
        [[fallthrough]];
      case TaskState::Skipped:
        deps_.skipped_dep_count[dep] += delta;
        break;
      default:
        break;
      }
    }
  }

  enum class TriggerStatus { Ready, Waiting, Skipped, UpstreamFailed };

  auto evaluate_trigger_rule(NodeIndex task) const -> TriggerStatus {
    const int total = deps_.in_degree[task];
    if (total == 0)
      return TriggerStatus::Ready;

    TriggerRule rule = dag_->get_trigger_rule(task);
    const int success = deps_.success_dep_count[task];
    const int failed = deps_.failed_dep_count[task];
    const int upstream_failed = deps_.upstream_failed_dep_count[task];
    const int any_failed = failed + upstream_failed;
    const int pure_skipped = deps_.skipped_dep_count[task] - upstream_failed;
    const int terminal = deps_.terminal_dep_count[task];
    const bool is_all_done = (terminal == total);

    switch (rule) {
    case TriggerRule::AllSuccess:
      if (success == total)
        return TriggerStatus::Ready;
      if (any_failed > 0)
        return TriggerStatus::UpstreamFailed;
      if (pure_skipped > 0)
        return TriggerStatus::Skipped;
      return TriggerStatus::Waiting;
    case TriggerRule::AllFailed:
      if (any_failed == total)
        return TriggerStatus::Ready;
      if (is_all_done) {
        if (any_failed > 0)
          return TriggerStatus::UpstreamFailed;
        return TriggerStatus::Skipped;
      }
      return TriggerStatus::Waiting;
    case TriggerRule::AllDone:
      if (is_all_done)
        return TriggerStatus::Ready;
      return TriggerStatus::Waiting;
    case TriggerRule::OneFailed:
      if (any_failed > 0)
        return TriggerStatus::Ready;
      if (is_all_done)
        return TriggerStatus::Skipped;
      return TriggerStatus::Waiting;
    case TriggerRule::OneSuccess:
      if (success > 0)
        return TriggerStatus::Ready;
      if (is_all_done) {
        if (pure_skipped == total)
          return TriggerStatus::Skipped;
        return TriggerStatus::UpstreamFailed;
      }
      return TriggerStatus::Waiting;
    case TriggerRule::NoneFailed:
      if (any_failed > 0)
        return TriggerStatus::UpstreamFailed;
      if (is_all_done)
        return TriggerStatus::Ready;
      return TriggerStatus::Waiting;
    case TriggerRule::Always:
      return TriggerStatus::Ready;
    case TriggerRule::NoneSkipped:
      if (pure_skipped > 0)
        return TriggerStatus::Skipped;
      if (is_all_done)
        return TriggerStatus::Ready;
      return TriggerStatus::Waiting;
    case TriggerRule::AllDoneMinOneSuccess:
      if (is_all_done) {
        if (success > 0)
          return TriggerStatus::Ready;
        if (any_failed > 0)
          return TriggerStatus::UpstreamFailed;
        if (pure_skipped > 0)
          return TriggerStatus::Skipped;
        return TriggerStatus::UpstreamFailed;
      }
      return TriggerStatus::Waiting;
    case TriggerRule::AllSkipped:
      if (pure_skipped == total)
        return TriggerStatus::Ready;
      if (any_failed > 0)
        return TriggerStatus::UpstreamFailed;
      if (success > 0)
        return TriggerStatus::Skipped;
      return TriggerStatus::Waiting;
    case TriggerRule::OneDone:
      if (success > 0 || any_failed > 0)
        return TriggerStatus::Ready;
      if (is_all_done)
        return TriggerStatus::Skipped;
      return TriggerStatus::Waiting;
    case TriggerRule::NoneFailedMinOneSuccess:
      if (any_failed > 0)
        return TriggerStatus::UpstreamFailed;
      if (is_all_done)
        return (success > 0) ? TriggerStatus::Ready : TriggerStatus::Skipped;
      return TriggerStatus::Waiting;
    }
    return TriggerStatus::Waiting;
  }

  auto propagate_terminal_to_downstream(NodeIndex terminal_task) -> void {
    for (NodeIndex dep : dag_->get_dependents_view(terminal_task)) {
      if (runtime_.ready_mask.test(dep) || runtime_.running_mask.test(dep) ||
          runtime_.completed_mask.test(dep) || runtime_.failed_mask.test(dep) ||
          runtime_.skipped_mask.test(dep)) {
        continue;
      }

      if (dag_->get_trigger_rule(dep) == TriggerRule::AllSuccess) {
        const int total = deps_.in_degree[dep];
        if (deps_.success_dep_count[dep] == total) {
          runtime_.ready_mask.set(dep);
          runtime_.ready_queue.push_back(dep);
          ++runtime_.ready_count;
          --runtime_.pending_count;
          continue;
        }

        const int any_failed =
            deps_.failed_dep_count[dep] + deps_.upstream_failed_dep_count[dep];
        if (any_failed > 0) {
          auto mark_res = mark_task_upstream_failed(dep);
          if (!mark_res) {
            continue;
          }
          continue;
        }

        const int pure_skipped =
            deps_.skipped_dep_count[dep] - deps_.upstream_failed_dep_count[dep];
        if (pure_skipped > 0) {
          auto mark_res = mark_task_branch_skipped(dep);
          if (!mark_res) {
            continue;
          }
        }
        continue;
      }

      auto status = evaluate_trigger_rule(dep);
      if (status == TriggerStatus::Ready) {
        runtime_.ready_mask.set(dep);
        runtime_.ready_queue.push_back(dep);
        ++runtime_.ready_count;
        --runtime_.pending_count;
      } else if (status == TriggerStatus::UpstreamFailed) {
        (void)mark_task_upstream_failed(dep);
      } else if (status == TriggerStatus::Skipped) {
        (void)mark_task_branch_skipped(dep);
      }
    }
  }

  auto mark_task_upstream_failed(NodeIndex idx) -> Result<void> {
    if (static_cast<std::size_t>(idx) >= task_info_.size())
      return fail(Error::NotFound);

    runtime_.skipped_mask.set(idx);
    ++runtime_.skipped_count;
    --runtime_.pending_count;
    task_info_[idx].state = TaskState::UpstreamFailed;
    update_dependent_counters(idx, TaskState::UpstreamFailed, +1);

    propagate_terminal_to_downstream(idx);
    update_state();
#ifndef NDEBUG
    verify_invariants();
#endif
    return ok();
  }

  auto mark_task_branch_skipped(NodeIndex idx) -> Result<void> {
    if (static_cast<std::size_t>(idx) >= task_info_.size())
      return fail(Error::NotFound);

    if (runtime_.completed_mask.test(idx) || runtime_.failed_mask.test(idx) ||
        runtime_.skipped_mask.test(idx)) {
      return ok();
    }

    if (runtime_.running_mask.test(idx)) {
      return ok();
    }

    if (runtime_.ready_mask.test(idx)) {
      runtime_.ready_mask.reset(idx);
      --runtime_.ready_count;
    } else {
      const auto state = task_info_[idx].state;
      if (state != TaskState::Pending && state != TaskState::Retrying) {
        return ok();
      }
      --runtime_.pending_count;
    }

    runtime_.skipped_mask.set(idx);
    ++runtime_.skipped_count;
    task_info_[idx].state = TaskState::Skipped;
    update_dependent_counters(idx, TaskState::Skipped, +1);

    propagate_terminal_to_downstream(idx);
    update_state();
#ifndef NDEBUG
    verify_invariants();
#endif
    return ok();
  }

  auto rebuild_runtime_state_from_task_info() -> void {
    runtime_.ready_mask.reset();
    runtime_.running_mask.reset();
    runtime_.completed_mask.reset();
    runtime_.failed_mask.reset();
    runtime_.skipped_mask.reset();
    runtime_.ready_queue.clear();

    std::ranges::fill(deps_.terminal_dep_count, 0);
    std::ranges::fill(deps_.success_dep_count, 0);
    std::ranges::fill(deps_.failed_dep_count, 0);
    std::ranges::fill(deps_.skipped_dep_count, 0);
    std::ranges::fill(deps_.upstream_failed_dep_count, 0);

    runtime_.pending_count = 0;
    runtime_.ready_count = 0;
    runtime_.running_count = 0;
    runtime_.completed_count = 0;
    runtime_.failed_count = 0;
    runtime_.skipped_count = 0;

    for (std::size_t i = 0; i < task_info_.size(); ++i) {
      const auto idx = static_cast<NodeIndex>(i);
      const auto state = task_info_[i].state;
      switch (state) {
      case TaskState::Pending:
      case TaskState::Retrying:
        ++runtime_.pending_count;
        break;
      case TaskState::Running:
        runtime_.running_mask.set(i);
        ++runtime_.running_count;
        break;
      case TaskState::Success:
        runtime_.completed_mask.set(i);
        ++runtime_.completed_count;
        update_dependent_counters(idx, TaskState::Success, +1);
        break;
      case TaskState::Failed:
        runtime_.failed_mask.set(i);
        ++runtime_.failed_count;
        update_dependent_counters(idx, TaskState::Failed, +1);
        break;
      case TaskState::Skipped:
        runtime_.skipped_mask.set(i);
        ++runtime_.skipped_count;
        update_dependent_counters(idx, TaskState::Skipped, +1);
        break;
      case TaskState::UpstreamFailed:
        runtime_.skipped_mask.set(i);
        ++runtime_.skipped_count;
        update_dependent_counters(idx, TaskState::UpstreamFailed, +1);
        break;
      }
    }
    runtime_.failed_count_fast.store(runtime_.failed_count, std::memory_order_relaxed);

    auto mark_ready = [&](NodeIndex idx) {
      if (runtime_.ready_mask.test(idx)) {
        return;
      }
      runtime_.ready_mask.set(idx);
      runtime_.ready_queue.push_back(idx);
      ++runtime_.ready_count;
      --runtime_.pending_count;
    };

    auto try_resolve_pending = [&](NodeIndex idx,
                                   std::vector<NodeIndex> &terminal_queue) {
      auto &info = task_info_[idx];
      if (info.state != TaskState::Pending &&
          info.state != TaskState::Retrying) {
        return;
      }

      const auto status = evaluate_trigger_rule(idx);
      if (status == TriggerStatus::Waiting) {
        return;
      }
      if (status == TriggerStatus::Ready) {
        mark_ready(idx);
        return;
      }

      info.state = (status == TriggerStatus::UpstreamFailed)
                       ? TaskState::UpstreamFailed
                       : TaskState::Skipped;
      if (info.finished_at == std::chrono::system_clock::time_point{}) {
        info.finished_at = std::chrono::system_clock::now();
      }
      runtime_.skipped_mask.set(idx);
      ++runtime_.skipped_count;
      --runtime_.pending_count;
      update_dependent_counters(idx, info.state, +1);
      terminal_queue.push_back(idx);
    };

    std::vector<NodeIndex> terminal_queue;
    terminal_queue.reserve(task_info_.size());
    for (std::size_t i = 0; i < task_info_.size(); ++i) {
      try_resolve_pending(static_cast<NodeIndex>(i), terminal_queue);
    }

    for (std::size_t head = 0; head < terminal_queue.size(); ++head) {
      for (NodeIndex dep : dag_->get_dependents_view(terminal_queue[head])) {
        try_resolve_pending(dep, terminal_queue);
      }
    }
  }

  auto restore_task_instance(const TaskInstanceInfo &info) -> Result<void> {
    NodeIndex idx = info.task_idx;
    if (static_cast<std::size_t>(idx) >= task_info_.size())
      return fail(Error::NotFound);

    task_info_[idx] = info;
    rebuild_runtime_state_from_task_info();
    update_state();
#ifndef NDEBUG
    verify_invariants();
#endif
    return ok();
  }

  DAGRunId dag_run_id_;
  std::shared_ptr<const DAG> dag_;
  DAGRunState state_{DAGRunState::Running};

  std::chrono::system_clock::time_point scheduled_at_;
  std::chrono::system_clock::time_point started_at_;
  std::chrono::system_clock::time_point finished_at_;
  std::chrono::system_clock::time_point execution_date_;
  std::chrono::system_clock::time_point data_interval_start_;
  std::chrono::system_clock::time_point data_interval_end_;
  TriggerType trigger_type_{TriggerType::Manual};

  DAGRunDependencyState deps_;
  DAGRunRuntimeState runtime_;
  std::vector<TaskInstanceInfo> task_info_;
  int64_t run_rowid_{-1};
  int64_t dag_rowid_{-1};
  int dag_version_{0};
};

DAGRun::DAGRun(DAGRunPrivateTag, DAGRunId dag_run_id,
               std::shared_ptr<const DAG> dag)
    : impl_(std::make_unique<Impl>(std::move(dag_run_id), std::move(dag))) {}

DAGRun::~DAGRun() = default;

DAGRun::DAGRun(DAGRun &&) noexcept = default;
DAGRun &DAGRun::operator=(DAGRun &&) noexcept = default;

DAGRun::DAGRun(const DAGRun &other)
    : impl_([&] { return std::make_unique<Impl>(*other.impl_); }()) {}

DAGRun &DAGRun::operator=(const DAGRun &other) {
  if (this != &other) {
    impl_ = std::make_unique<Impl>(*other.impl_);
  }
  return *this;
}

auto DAGRun::create(DAGRunId dag_run_id, std::shared_ptr<const DAG> dag)
    -> Result<DAGRun> {
  if (!dag)
    return fail(Error::InvalidArgument);
  return DAGRun(DAGRunPrivateTag{}, std::move(dag_run_id), std::move(dag));
}

auto DAGRun::id() const noexcept -> const DAGRunId & {
  return impl_->dag_run_id_;
}
auto DAGRun::state() const noexcept -> DAGRunState { return impl_->state_; }
auto DAGRun::dag() const noexcept -> const DAG & { return *impl_->dag_; }

auto DAGRun::get_ready_tasks(pmr::memory_resource *resource) const
    -> pmr::vector<NodeIndex> {
  pmr::vector<NodeIndex> result(resource);
  copy_ready_tasks(result);
  return result;
}

auto DAGRun::copy_ready_tasks(pmr::vector<NodeIndex> &out) const -> void {
  out.clear();
  if (impl_->runtime_.ready_count == 0) {
    return;
  }

  out.reserve(impl_->runtime_.ready_count);
  for (auto idx : impl_->runtime_.ready_queue) {
    if (impl_->runtime_.ready_mask.test(idx)) {
      out.emplace_back(idx);
    }
  }
}

auto DAGRun::is_task_ready(NodeIndex task_idx) const noexcept -> bool {
  return static_cast<std::size_t>(task_idx) < impl_->task_info_.size() &&
         impl_->runtime_.ready_mask.test(task_idx);
}

auto DAGRun::ready_task_stream() const -> std::generator<Result<NodeIndex>> {
  std::vector<NodeIndex> snapshot;
  {
    snapshot.reserve(impl_->runtime_.ready_count);
    for (auto idx : impl_->runtime_.ready_queue) {
      if (impl_->runtime_.ready_mask.test(idx)) {
        snapshot.emplace_back(idx);
      }
    }
  }
  for (auto idx : snapshot) {
    co_yield ok(idx);
  }
}

auto DAGRun::ready_count() const noexcept -> size_t {
  return impl_->runtime_.ready_count;
}

auto DAGRun::mark_task_started(NodeIndex task_idx,
                               const InstanceId &instance_id) -> Result<void> {
  if (static_cast<std::size_t>(task_idx) >= impl_->task_info_.size())
    return fail(Error::NotFound);
  if (!impl_->runtime_.ready_mask.test(task_idx))
    return fail(Error::InvalidState);

  impl_->runtime_.ready_mask.reset(task_idx);
  impl_->runtime_.running_mask.set(task_idx);

  --impl_->runtime_.ready_count;
  ++impl_->runtime_.running_count;

  auto &info = impl_->task_info_[task_idx];
  info.instance_id = instance_id;
  info.state = TaskState::Running;
  info.started_at = std::chrono::system_clock::now();
  info.attempt++;
  return ok();
}

auto DAGRun::mark_task_completed(NodeIndex task_idx, int exit_code)
    -> Result<void> {
  if (static_cast<std::size_t>(task_idx) >= impl_->task_info_.size())
    return fail(Error::NotFound);
  if (!impl_->runtime_.running_mask.test(task_idx))
    return fail(Error::InvalidState);

  impl_->runtime_.running_mask.reset(task_idx);
  impl_->runtime_.completed_mask.set(task_idx);

  --impl_->runtime_.running_count;
  ++impl_->runtime_.completed_count;

  auto &info = impl_->task_info_[task_idx];
  info.state = TaskState::Success;
  info.exit_code = exit_code;
  info.finished_at = std::chrono::system_clock::now();

  impl_->update_dependent_counters(task_idx, TaskState::Success, +1);
  impl_->propagate_terminal_to_downstream(task_idx);
  impl_->update_state();
  return ok();
}

auto DAGRun::mark_task_completed_with_branch(
    NodeIndex task_idx, int exit_code,
    std::span<const TaskId> selected_branches) -> Result<void> {
  if (static_cast<std::size_t>(task_idx) >= impl_->task_info_.size())
    return fail(Error::NotFound);
  if (!impl_->runtime_.running_mask.test(task_idx))
    return fail(Error::InvalidState);

  impl_->runtime_.running_mask.reset(task_idx);
  impl_->runtime_.completed_mask.set(task_idx);

  --impl_->runtime_.running_count;
  ++impl_->runtime_.completed_count;

  auto &info = impl_->task_info_[task_idx];
  info.state = TaskState::Success;
  info.exit_code = exit_code;
  info.finished_at = std::chrono::system_clock::now();

  if (impl_->dag_->is_branch_task(task_idx) && !selected_branches.empty()) {
    for (NodeIndex dep_idx : impl_->dag_->get_dependents_view(task_idx)) {
      TaskId dep_id = impl_->dag_->get_key(dep_idx);
      bool selected = false;
      for (const auto &branch_id : selected_branches) {
        if (branch_id == dep_id) {
          selected = true;
          break;
        }
      }

      if (!selected) {
        (void)impl_->mark_task_branch_skipped(dep_idx);
      }
    }
  }

  impl_->update_dependent_counters(task_idx, TaskState::Success, +1);
  impl_->propagate_terminal_to_downstream(task_idx);
  impl_->update_state();
  return ok();
}

auto DAGRun::mark_task_failed(NodeIndex task_idx, std::string_view error,
                              int max_retries, int exit_code) -> Result<void> {
  if (static_cast<std::size_t>(task_idx) >= impl_->task_info_.size())
    return fail(Error::NotFound);
  if (!impl_->runtime_.running_mask.test(task_idx))
    return fail(Error::InvalidState);

  auto &info = impl_->task_info_[task_idx];
  info.error_message = std::string(error);
  info.exit_code = exit_code;

  if (info.attempt < max_retries) {
    impl_->runtime_.running_mask.reset(task_idx);

    --impl_->runtime_.running_count;
    ++impl_->runtime_.pending_count;

    info.state = TaskState::Retrying;
    info.started_at = {};
    info.finished_at = {};
  } else {
    impl_->runtime_.running_mask.reset(task_idx);
    impl_->runtime_.failed_mask.set(task_idx);

    --impl_->runtime_.running_count;
    ++impl_->runtime_.failed_count;
    impl_->runtime_.failed_count_fast.store(impl_->runtime_.failed_count,
                                    std::memory_order_relaxed);

    info.state = TaskState::Failed;
    info.finished_at = std::chrono::system_clock::now();

    impl_->update_dependent_counters(task_idx, TaskState::Failed, +1);
    impl_->propagate_terminal_to_downstream(task_idx);
    impl_->update_state();
  }

  return ok();
}

auto DAGRun::mark_task_retry_ready(NodeIndex task_idx) -> Result<void> {
  if (static_cast<std::size_t>(task_idx) >= impl_->task_info_.size()) {
    return fail(Error::NotFound);
  }

  if (impl_->runtime_.ready_mask.test(task_idx) ||
      impl_->runtime_.running_mask.test(task_idx) ||
      impl_->runtime_.completed_mask.test(task_idx) ||
      impl_->runtime_.failed_mask.test(task_idx) ||
      impl_->runtime_.skipped_mask.test(task_idx)) {
    return fail(Error::InvalidState);
  }

  auto &info = impl_->task_info_[task_idx];
  if (info.state != TaskState::Retrying) {
    return fail(Error::InvalidState);
  }

  impl_->runtime_.ready_mask.set(task_idx);
  impl_->runtime_.ready_queue.push_back(task_idx);
  ++impl_->runtime_.ready_count;
  if (impl_->runtime_.pending_count > 0) {
    --impl_->runtime_.pending_count;
  }

  info.state = TaskState::Pending;
  return ok();
}

auto DAGRun::mark_task_skipped(NodeIndex task_idx) -> Result<void> {
  if (static_cast<std::size_t>(task_idx) >= impl_->task_info_.size())
    return fail(Error::NotFound);

  bool was_ready = impl_->runtime_.ready_mask.test(task_idx);
  bool was_running = impl_->runtime_.running_mask.test(task_idx);

  if (!was_ready && !was_running) {
    // Task is neither ready nor running, so it's an invalid state to skip from.
    return fail(Error::InvalidState);
  }

  if (was_ready) {
    impl_->runtime_.ready_mask.reset(task_idx);
    --impl_->runtime_.ready_count;
  } else { // was_running
    impl_->runtime_.running_mask.reset(task_idx);
    --impl_->runtime_.running_count;
  }

  impl_->runtime_.skipped_mask.set(task_idx);
  ++impl_->runtime_.skipped_count;

  impl_->task_info_[task_idx].state = TaskState::Skipped;

  impl_->update_dependent_counters(task_idx, TaskState::Skipped, +1);
  impl_->propagate_terminal_to_downstream(task_idx);
  impl_->update_state();
  return ok();
}

auto DAGRun::mark_task_upstream_failed(NodeIndex task_idx) -> Result<void> {
  return impl_->mark_task_upstream_failed(task_idx);
}

auto DAGRun::restore_task_instance(const TaskInstanceInfo &info)
    -> Result<void> {
  return impl_->restore_task_instance(info);
}

auto DAGRun::is_complete() const noexcept -> bool {
  return impl_->state_ == DAGRunState::Success ||
         impl_->state_ == DAGRunState::Failed;
}

auto DAGRun::has_failed() const noexcept -> bool {
  return impl_->runtime_.failed_count_fast.load(std::memory_order_relaxed) > 0;
}

auto DAGRun::all_task_info() const -> std::vector<TaskInstanceInfo> {
  return impl_->task_info_;
}

auto DAGRun::get_task_info(NodeIndex task_idx) const
    -> Result<TaskInstanceInfo> {
  if (static_cast<std::size_t>(task_idx) >= impl_->task_info_.size())
    return fail(Error::NotFound);
  return impl_->task_info_[task_idx];
}

auto DAGRun::set_instance_id(NodeIndex task_idx, const InstanceId &instance_id)
    -> Result<void> {
  if (static_cast<std::size_t>(task_idx) >= impl_->task_info_.size())
    return fail(Error::NotFound);
  impl_->task_info_[task_idx].instance_id = instance_id;
  return ok();
}

auto DAGRun::set_task_rowid(NodeIndex task_idx, int64_t task_rowid)
    -> Result<void> {
  if (static_cast<std::size_t>(task_idx) >= impl_->task_info_.size())
    return fail(Error::NotFound);
  impl_->task_info_[task_idx].task_rowid = task_rowid;
  return ok();
}

auto DAGRun::scheduled_at() const noexcept
    -> std::chrono::system_clock::time_point {
  return impl_->scheduled_at_;
}

auto DAGRun::set_scheduled_at(std::chrono::system_clock::time_point t) noexcept
    -> void {
  impl_->scheduled_at_ = t;
}

auto DAGRun::started_at() const noexcept
    -> std::chrono::system_clock::time_point {
  return impl_->started_at_;
}

auto DAGRun::set_started_at(std::chrono::system_clock::time_point t) noexcept
    -> void {
  impl_->started_at_ = t;
}

auto DAGRun::set_finished_at(std::chrono::system_clock::time_point t) noexcept
    -> void {
  impl_->finished_at_ = t;
}

auto DAGRun::finished_at() const noexcept
    -> std::chrono::system_clock::time_point {
  return impl_->finished_at_;
}

auto DAGRun::execution_date() const noexcept
    -> std::chrono::system_clock::time_point {
  return impl_->execution_date_;
}

auto DAGRun::set_execution_date(
    std::chrono::system_clock::time_point t) noexcept -> void {
  impl_->execution_date_ = t;
}

auto DAGRun::data_interval_start() const noexcept
    -> std::chrono::system_clock::time_point {
  return impl_->data_interval_start_;
}

auto DAGRun::set_data_interval_start(
    std::chrono::system_clock::time_point t) noexcept -> void {
  impl_->data_interval_start_ = t;
}

auto DAGRun::data_interval_end() const noexcept
    -> std::chrono::system_clock::time_point {
  return impl_->data_interval_end_;
}

auto DAGRun::set_data_interval_end(
    std::chrono::system_clock::time_point t) noexcept -> void {
  impl_->data_interval_end_ = t;
}

auto DAGRun::trigger_type() const noexcept -> TriggerType {
  return impl_->trigger_type_;
}

auto DAGRun::set_trigger_type(TriggerType t) noexcept -> void {
  impl_->trigger_type_ = t;
}

auto DAGRun::run_rowid() const noexcept -> int64_t { return impl_->run_rowid_; }

auto DAGRun::set_run_rowid(int64_t rowid) noexcept -> void {
  impl_->run_rowid_ = rowid;
  for (auto &info : impl_->task_info_) {
    info.run_rowid = rowid;
  }
}

auto DAGRun::dag_rowid() const noexcept -> int64_t { return impl_->dag_rowid_; }

void DAGRun::set_dag_rowid(int64_t rowid) noexcept {
  impl_->dag_rowid_ = rowid;
}

auto DAGRun::dag_version() const noexcept -> int { return impl_->dag_version_; }

void DAGRun::set_dag_version(int version) noexcept {
  impl_->dag_version_ = version;
}

} // namespace dagforge
