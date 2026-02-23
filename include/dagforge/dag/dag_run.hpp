#pragma once

#include "dagforge/dag/dag.hpp"
#include "dagforge/scheduler/task.hpp"
#include "dagforge/util/enum.hpp"
#include "dagforge/util/id.hpp"

#include <boost/describe/enum.hpp>
#include <chrono>
#include <cstdint>
#include <generator>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace dagforge {

struct DAGRunPrivateTag {};

enum class DAGRunState : std::uint8_t {
  Queued,
  Running,
  Success,
  Failed,
  Skipped,
  Cancelled,
};
BOOST_DESCRIBE_ENUM(DAGRunState, Queued, Running, Success, Failed, Skipped,
                    Cancelled)
DAGFORGE_DEFINE_ENUM_SERDE(DAGRunState, DAGRunState::Running)

enum class TriggerType : std::uint8_t {
  Manual,
  Schedule,
  Api,
  Backfill,
};
BOOST_DESCRIBE_ENUM(TriggerType, Manual, Schedule, Api, Backfill)
DAGFORGE_DEFINE_ENUM_SERDE(TriggerType, TriggerType::Manual)

struct TaskInstanceInfo {
  InstanceId instance_id;
  NodeIndex task_idx{kInvalidNode}; // In-memory graph index (runtime only)
  int64_t task_rowid{0}; // Stable DB reference (FK -> dag_tasks.task_rowid)
  TaskState state{TaskState::Pending};
  int attempt{0};
  std::chrono::system_clock::time_point started_at;
  std::chrono::system_clock::time_point finished_at;
  int exit_code{0};
  std::string error_message;
  std::string error_type;

  int64_t run_rowid{-1};
};

class DAGRun {
public:
  [[nodiscard]] static auto create(DAGRunId dag_run_id,
                                   std::shared_ptr<const DAG> dag)
      -> Result<DAGRun>;

  ~DAGRun();
  DAGRun(DAGRun &&) noexcept;
  DAGRun &operator=(DAGRun &&) noexcept;
  DAGRun(const DAGRun &);
  DAGRun &operator=(const DAGRun &);

  [[nodiscard]] auto id() const noexcept -> const DAGRunId &;
  [[nodiscard]] auto state() const noexcept -> DAGRunState;
  [[nodiscard]] auto dag() const noexcept -> const DAG &;

  [[nodiscard]] auto get_ready_tasks(std::pmr::memory_resource *resource =
                                         std::pmr::get_default_resource()) const
      -> std::pmr::vector<NodeIndex>;
  auto copy_ready_tasks(std::pmr::vector<NodeIndex> &out) const -> void;
  [[nodiscard]] auto is_task_ready(NodeIndex task_idx) const noexcept -> bool;
  [[nodiscard]] auto ready_task_stream() const
      -> std::generator<Result<NodeIndex>>;
  [[nodiscard]] auto ready_count() const noexcept -> size_t;

  [[nodiscard]] auto mark_task_started(NodeIndex task_idx,
                                       const InstanceId &instance_id)
      -> Result<void>;
  [[nodiscard]] auto mark_task_completed(NodeIndex task_idx, int exit_code)
      -> Result<void>;
  [[nodiscard]] auto
  mark_task_completed_with_branch(NodeIndex task_idx, int exit_code,
                                  std::span<const TaskId> selected_branches)
      -> Result<void>;
  [[nodiscard]] auto mark_task_failed(NodeIndex task_idx,
                                      std::string_view error, int max_retries,
                                      int exit_code = 1) -> Result<void>;
  [[nodiscard]] auto mark_task_retry_ready(NodeIndex task_idx) -> Result<void>;
  [[nodiscard]] auto mark_task_skipped(NodeIndex task_idx) -> Result<void>;
  [[nodiscard]] auto mark_task_upstream_failed(NodeIndex task_idx)
      -> Result<void>;
  [[nodiscard]] auto restore_task_instance(const TaskInstanceInfo &info)
      -> Result<void>;

  [[nodiscard]] auto set_instance_id(NodeIndex task_idx,
                                     const InstanceId &instance_id)
      -> Result<void>;
  [[nodiscard]] auto set_task_rowid(NodeIndex task_idx, int64_t task_rowid)
      -> Result<void>;

  [[nodiscard]] auto is_complete() const noexcept -> bool;
  [[nodiscard]] auto has_failed() const noexcept -> bool;

  [[nodiscard]] auto get_task_info(NodeIndex task_idx) const
      -> Result<TaskInstanceInfo>;
  [[nodiscard]] auto all_task_info() const -> std::vector<TaskInstanceInfo>;

  [[nodiscard]] auto scheduled_at() const noexcept
      -> std::chrono::system_clock::time_point;
  [[nodiscard]] auto started_at() const noexcept
      -> std::chrono::system_clock::time_point;
  [[nodiscard]] auto finished_at() const noexcept
      -> std::chrono::system_clock::time_point;

  auto set_scheduled_at(std::chrono::system_clock::time_point t) noexcept
      -> void;
  auto set_started_at(std::chrono::system_clock::time_point t) noexcept -> void;
  auto set_finished_at(std::chrono::system_clock::time_point t) noexcept
      -> void;

  [[nodiscard]] auto trigger_type() const noexcept -> TriggerType;
  auto set_trigger_type(TriggerType t) noexcept -> void;

  [[nodiscard]] auto execution_date() const noexcept
      -> std::chrono::system_clock::time_point;
  auto set_execution_date(std::chrono::system_clock::time_point t) noexcept
      -> void;

  [[nodiscard]] auto data_interval_start() const noexcept
      -> std::chrono::system_clock::time_point;
  auto set_data_interval_start(std::chrono::system_clock::time_point t) noexcept
      -> void;

  [[nodiscard]] auto data_interval_end() const noexcept
      -> std::chrono::system_clock::time_point;
  auto set_data_interval_end(std::chrono::system_clock::time_point t) noexcept
      -> void;

  void set_run_rowid(int64_t rowid) noexcept;
  [[nodiscard]] auto run_rowid() const noexcept -> int64_t;

  void set_dag_rowid(int64_t rowid) noexcept;
  [[nodiscard]] auto dag_rowid() const noexcept -> int64_t;

  void set_dag_version(int version) noexcept;
  [[nodiscard]] auto dag_version() const noexcept -> int;

private:
  DAGRun(DAGRunPrivateTag, DAGRunId dag_run_id, std::shared_ptr<const DAG> dag);

  struct Impl;
  std::unique_ptr<Impl> impl_;
};

} // namespace dagforge
