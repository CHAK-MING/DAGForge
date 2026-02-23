#include "dagforge/dag/dag_run.hpp"

#include "dagforge/config/task_config.hpp"
#include "dagforge/core/error.hpp"
#include "dagforge/util/log.hpp"

#include <algorithm>
#include <atomic>
#include <boost/dynamic_bitset.hpp>
#include <cassert>
#include <flat_set>
#include <ranges>
#include <span>

namespace dagforge {

struct DAGRun::Impl {
  Impl(DAGRunId dag_run_id, std::shared_ptr<const DAG> dag)
      : dag_run_id_(std::move(dag_run_id)), dag_(std::move(dag)) {
    std::size_t n = dag_->size();
    in_degree_.resize(n, 0);
    terminal_dep_count_.resize(n, 0);
    success_dep_count_.resize(n, 0);
    failed_dep_count_.resize(n, 0);
    skipped_dep_count_.resize(n, 0);
    upstream_failed_dep_count_.resize(n, 0);
    task_info_.resize(n);
    ready_mask_.resize(n);
    running_mask_.resize(n);
    completed_mask_.resize(n);
    failed_mask_.resize(n);
    skipped_mask_.resize(n);

    for (auto i : std::views::iota(0u, n)) {
      NodeIndex idx = static_cast<NodeIndex>(i);
      task_info_[i].task_idx = idx;
      task_info_[i].state = TaskState::Pending;
      in_degree_[i] = static_cast<int>(dag_->get_deps_view(idx).size());
    }

    pending_count_ = n;
    init_ready_set();
  }

  Impl(const Impl &other)
      : dag_run_id_(other.dag_run_id_), dag_(other.dag_), state_(other.state_),
        scheduled_at_(other.scheduled_at_), started_at_(other.started_at_),
        finished_at_(other.finished_at_),
        execution_date_(other.execution_date_),
        data_interval_start_(other.data_interval_start_),
        data_interval_end_(other.data_interval_end_),
        trigger_type_(other.trigger_type_), in_degree_(other.in_degree_),
        terminal_dep_count_(other.terminal_dep_count_),
        success_dep_count_(other.success_dep_count_),
        failed_dep_count_(other.failed_dep_count_),
        skipped_dep_count_(other.skipped_dep_count_),
        upstream_failed_dep_count_(other.upstream_failed_dep_count_),
        ready_mask_(other.ready_mask_), running_mask_(other.running_mask_),
        completed_mask_(other.completed_mask_),
        failed_mask_(other.failed_mask_), skipped_mask_(other.skipped_mask_),
        pending_count_(other.pending_count_), ready_count_(other.ready_count_),
        running_count_(other.running_count_),
        completed_count_(other.completed_count_),
        failed_count_(other.failed_count_),
        skipped_count_(other.skipped_count_),
        failed_count_fast_(
            other.failed_count_fast_.load(std::memory_order_relaxed)),
        ready_set_(other.ready_set_), task_info_(other.task_info_),
        run_rowid_(other.run_rowid_), dag_rowid_(other.dag_rowid_),
        dag_version_(other.dag_version_) {}

  auto update_state() -> void {
    if (ready_count_ > 0 || running_count_ > 0) {
      return;
    }

    if (failed_count_ > 0 && pending_count_ == 0) {
      state_ = DAGRunState::Failed;
      finished_at_ = std::chrono::system_clock::now();
      return;
    }

    if (completed_count_ + skipped_count_ + failed_count_ == dag_->size()) {
      state_ = (failed_count_ > 0) ? DAGRunState::Failed : DAGRunState::Success;
      finished_at_ = std::chrono::system_clock::now();
    }
#ifndef NDEBUG
    verify_invariants();
#endif
  }

  auto init_ready_set() -> void {
    ready_mask_.reset();
    ready_count_ = 0;
    ready_set_.clear();

    for (auto [i, deg] : std::views::enumerate(in_degree_)) {
      if (deg == 0) {
        ready_mask_.set(static_cast<std::size_t>(i));
        ready_set_.insert(static_cast<NodeIndex>(i));
        ++ready_count_;
      } else if (dag_->get_trigger_rule(static_cast<NodeIndex>(i)) ==
                 TriggerRule::Always) {
        // Airflow-compatible: ALWAYS does not wait for upstream completion.
        ready_mask_.set(static_cast<std::size_t>(i));
        ready_set_.insert(static_cast<NodeIndex>(i));
        ++ready_count_;
      }
    }
    pending_count_ -= ready_count_;
#ifndef NDEBUG
    verify_invariants();
#endif
  }

#ifndef NDEBUG
  auto verify_invariants() const noexcept -> void {
    const auto n = dag_->size();
    const auto terminal_count =
        completed_count_ + failed_count_ + skipped_count_;
    assert(ready_count_ == ready_set_.size());
    assert(ready_count_ == ready_mask_.count());
    assert(running_count_ == running_mask_.count());
    assert(completed_count_ == completed_mask_.count());
    assert(failed_count_ == failed_mask_.count());
    assert(skipped_count_ == skipped_mask_.count());
    assert(terminal_count <= n);
    assert(pending_count_ + ready_count_ + running_count_ + terminal_count ==
           n);
  }
#endif

  auto update_dependent_counters(NodeIndex terminal_task, TaskState state,
                                 int delta) -> void {
    for (NodeIndex dep : dag_->get_dependents_view(terminal_task)) {
      terminal_dep_count_[dep] += delta;
      switch (state) {
      case TaskState::Success:
        success_dep_count_[dep] += delta;
        break;
      case TaskState::Failed:
        failed_dep_count_[dep] += delta;
        break;
      case TaskState::UpstreamFailed:
        upstream_failed_dep_count_[dep] += delta;
        [[fallthrough]];
      case TaskState::Skipped:
        skipped_dep_count_[dep] += delta;
        break;
      default:
        break;
      }
    }
  }

  enum class TriggerStatus { Ready, Waiting, Skipped, UpstreamFailed };

  auto evaluate_trigger_rule(NodeIndex task) const -> TriggerStatus {
    const int total = in_degree_[task];
    if (total == 0)
      return TriggerStatus::Ready;

    TriggerRule rule = dag_->get_trigger_rule(task);
    const int success = success_dep_count_[task];
    const int failed = failed_dep_count_[task];
    const int upstream_failed = upstream_failed_dep_count_[task];
    const int any_failed = failed + upstream_failed;
    const int pure_skipped = skipped_dep_count_[task] - upstream_failed;
    const int terminal = terminal_dep_count_[task];
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
      if (ready_mask_.test(dep) || running_mask_.test(dep) ||
          completed_mask_.test(dep) || failed_mask_.test(dep) ||
          skipped_mask_.test(dep)) {
        continue;
      }

      if (dag_->get_trigger_rule(dep) == TriggerRule::AllSuccess) {
        const int total = in_degree_[dep];
        if (success_dep_count_[dep] == total) {
          ready_mask_.set(dep);
          ready_set_.insert(dep);
          ++ready_count_;
          --pending_count_;
          continue;
        }

        const int any_failed =
            failed_dep_count_[dep] + upstream_failed_dep_count_[dep];
        if (any_failed > 0) {
          auto mark_res = mark_task_upstream_failed(dep);
          if (!mark_res) {
            continue;
          }
          continue;
        }

        const int pure_skipped =
            skipped_dep_count_[dep] - upstream_failed_dep_count_[dep];
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
        ready_mask_.set(dep);
        ready_set_.insert(dep);
        ++ready_count_;
        --pending_count_;
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

    skipped_mask_.set(idx);
    ++skipped_count_;
    --pending_count_;
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

    if (completed_mask_.test(idx) || failed_mask_.test(idx) ||
        skipped_mask_.test(idx)) {
      return ok();
    }

    if (ready_mask_.test(idx)) {
      ready_mask_.reset(idx);
      ready_set_.erase(idx);
      --ready_count_;
    } else {
      --pending_count_;
    }

    skipped_mask_.set(idx);
    ++skipped_count_;
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
    ready_mask_.reset();
    running_mask_.reset();
    completed_mask_.reset();
    failed_mask_.reset();
    skipped_mask_.reset();
    ready_set_.clear();

    std::ranges::fill(terminal_dep_count_, 0);
    std::ranges::fill(success_dep_count_, 0);
    std::ranges::fill(failed_dep_count_, 0);
    std::ranges::fill(skipped_dep_count_, 0);
    std::ranges::fill(upstream_failed_dep_count_, 0);

    pending_count_ = 0;
    ready_count_ = 0;
    running_count_ = 0;
    completed_count_ = 0;
    failed_count_ = 0;
    skipped_count_ = 0;

    for (std::size_t i = 0; i < task_info_.size(); ++i) {
      const auto idx = static_cast<NodeIndex>(i);
      const auto state = task_info_[i].state;
      switch (state) {
      case TaskState::Pending:
      case TaskState::Retrying:
        ++pending_count_;
        break;
      case TaskState::Running:
        running_mask_.set(i);
        ++running_count_;
        break;
      case TaskState::Success:
        completed_mask_.set(i);
        ++completed_count_;
        update_dependent_counters(idx, TaskState::Success, +1);
        break;
      case TaskState::Failed:
        failed_mask_.set(i);
        ++failed_count_;
        update_dependent_counters(idx, TaskState::Failed, +1);
        break;
      case TaskState::Skipped:
        skipped_mask_.set(i);
        ++skipped_count_;
        update_dependent_counters(idx, TaskState::Skipped, +1);
        break;
      case TaskState::UpstreamFailed:
        skipped_mask_.set(i);
        ++skipped_count_;
        update_dependent_counters(idx, TaskState::UpstreamFailed, +1);
        break;
      }
    }
    failed_count_fast_.store(failed_count_, std::memory_order_relaxed);

    bool changed = true;
    while (changed) {
      changed = false;
      for (std::size_t i = 0; i < task_info_.size(); ++i) {
        const auto idx = static_cast<NodeIndex>(i);
        auto &info = task_info_[i];
        if (info.state != TaskState::Pending &&
            info.state != TaskState::Retrying) {
          continue;
        }

        auto status = evaluate_trigger_rule(idx);
        if (status == TriggerStatus::Waiting) {
          continue;
        }

        if (status == TriggerStatus::Ready) {
          if (!ready_mask_.test(i)) {
            ready_mask_.set(i);
            ready_set_.insert(idx);
            ++ready_count_;
            --pending_count_;
          }
          continue;
        }

        const auto new_state = (status == TriggerStatus::UpstreamFailed)
                                   ? TaskState::UpstreamFailed
                                   : TaskState::Skipped;

        info.state = new_state;
        if (info.finished_at == std::chrono::system_clock::time_point{}) {
          info.finished_at = std::chrono::system_clock::now();
        }
        skipped_mask_.set(i);
        ++skipped_count_;

        if (ready_mask_.test(i)) {
          ready_mask_.reset(i);
          ready_set_.erase(idx);
          --ready_count_;
        } else {
          --pending_count_;
        }

        update_dependent_counters(idx, new_state, +1);
        changed = true;
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

  std::vector<int> in_degree_;
  std::vector<int> terminal_dep_count_;
  std::vector<int> success_dep_count_;
  std::vector<int> failed_dep_count_;
  std::vector<int> skipped_dep_count_;
  std::vector<int> upstream_failed_dep_count_;
  boost::dynamic_bitset<> ready_mask_;
  boost::dynamic_bitset<> running_mask_;
  boost::dynamic_bitset<> completed_mask_;
  boost::dynamic_bitset<> failed_mask_;
  boost::dynamic_bitset<> skipped_mask_;

  std::size_t pending_count_{0};
  std::size_t ready_count_{0};
  std::size_t running_count_{0};
  std::size_t completed_count_{0};
  std::size_t failed_count_{0};
  std::size_t skipped_count_{0};
  std::atomic<std::size_t> failed_count_fast_{0};

  std::flat_set<NodeIndex> ready_set_;
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

auto DAGRun::get_ready_tasks(std::pmr::memory_resource *resource) const
    -> std::pmr::vector<NodeIndex> {
  std::pmr::vector<NodeIndex> result(resource);
  copy_ready_tasks(result);
  return result;
}

auto DAGRun::copy_ready_tasks(std::pmr::vector<NodeIndex> &out) const -> void {
  out.clear();
  if (impl_->ready_count_ == 0) {
    return;
  }

  out.reserve(impl_->ready_count_);
  for (const auto idx : impl_->ready_set_) {
    out.emplace_back(idx);
  }
}

auto DAGRun::is_task_ready(NodeIndex task_idx) const noexcept -> bool {
  return static_cast<std::size_t>(task_idx) < impl_->task_info_.size() &&
         impl_->ready_mask_.test(task_idx);
}

auto DAGRun::ready_task_stream() const -> std::generator<Result<NodeIndex>> {
  std::vector<NodeIndex> snapshot;
  {
    snapshot.reserve(impl_->ready_set_.size());
    for (auto idx : impl_->ready_set_) {
      snapshot.emplace_back(idx);
    }
  }
  for (auto idx : snapshot) {
    co_yield ok(idx);
  }
}

auto DAGRun::ready_count() const noexcept -> size_t {
  return impl_->ready_count_;
}

auto DAGRun::mark_task_started(NodeIndex task_idx,
                               const InstanceId &instance_id) -> Result<void> {
  if (static_cast<std::size_t>(task_idx) >= impl_->task_info_.size())
    return fail(Error::NotFound);
  if (!impl_->ready_mask_.test(task_idx))
    return fail(Error::InvalidState);

  impl_->ready_mask_.reset(task_idx);
  impl_->ready_set_.erase(task_idx);
  impl_->running_mask_.set(task_idx);

  --impl_->ready_count_;
  ++impl_->running_count_;

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
  if (!impl_->running_mask_.test(task_idx))
    return fail(Error::InvalidState);

  impl_->running_mask_.reset(task_idx);
  impl_->completed_mask_.set(task_idx);

  --impl_->running_count_;
  ++impl_->completed_count_;

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
  if (!impl_->running_mask_.test(task_idx))
    return fail(Error::InvalidState);

  impl_->running_mask_.reset(task_idx);
  impl_->completed_mask_.set(task_idx);

  --impl_->running_count_;
  ++impl_->completed_count_;

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
  if (!impl_->running_mask_.test(task_idx))
    return fail(Error::InvalidState);

  auto &info = impl_->task_info_[task_idx];
  info.error_message = std::string(error);
  info.exit_code = exit_code;

  if (info.attempt < max_retries) {
    impl_->running_mask_.reset(task_idx);

    --impl_->running_count_;
    ++impl_->pending_count_;

    info.state = TaskState::Retrying;
    info.started_at = {};
    info.finished_at = {};
  } else {
    impl_->running_mask_.reset(task_idx);
    impl_->failed_mask_.set(task_idx);

    --impl_->running_count_;
    ++impl_->failed_count_;
    impl_->failed_count_fast_.store(impl_->failed_count_,
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

  if (impl_->ready_mask_.test(task_idx) ||
      impl_->running_mask_.test(task_idx) ||
      impl_->completed_mask_.test(task_idx) ||
      impl_->failed_mask_.test(task_idx) ||
      impl_->skipped_mask_.test(task_idx)) {
    return fail(Error::InvalidState);
  }

  auto &info = impl_->task_info_[task_idx];
  if (info.state != TaskState::Retrying) {
    return fail(Error::InvalidState);
  }

  impl_->ready_mask_.set(task_idx);
  impl_->ready_set_.insert(task_idx);
  ++impl_->ready_count_;
  if (impl_->pending_count_ > 0) {
    --impl_->pending_count_;
  }

  info.state = TaskState::Pending;
  return ok();
}

auto DAGRun::mark_task_skipped(NodeIndex task_idx) -> Result<void> {
  if (static_cast<std::size_t>(task_idx) >= impl_->task_info_.size())
    return fail(Error::NotFound);

  bool was_ready = impl_->ready_mask_.test(task_idx);
  bool was_running = impl_->running_mask_.test(task_idx);

  if (!was_ready && !was_running) {
    // Task is neither ready nor running, so it's an invalid state to skip from.
    return fail(Error::InvalidState);
  }

  if (was_ready) {
    impl_->ready_mask_.reset(task_idx);
    impl_->ready_set_.erase(task_idx);
    --impl_->ready_count_;
  } else { // was_running
    impl_->running_mask_.reset(task_idx);
    --impl_->running_count_;
  }

  impl_->skipped_mask_.set(task_idx);
  ++impl_->skipped_count_;

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
  return impl_->failed_count_fast_.load(std::memory_order_relaxed) > 0;
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
