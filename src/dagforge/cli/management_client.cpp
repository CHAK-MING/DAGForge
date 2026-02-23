#include "dagforge/cli/management_client.hpp"

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/use_future.hpp>

#include <chrono>
#include <future>

#include "dagforge/util/log.hpp"

namespace dagforge::cli {
namespace {

template <typename T>
auto run_task(boost::asio::io_context &io, task<Result<T>> op) -> Result<T> {
  auto fut = boost::asio::co_spawn(io, std::move(op), boost::asio::use_future);
  while (fut.wait_for(std::chrono::seconds(0)) != std::future_status::ready) {
    io.run_one();
  }
  io.restart();
  try {
    return fut.get();
  } catch (const std::future_error &e) {
    log::error("ManagementClient async operation failed: {}", e.what());
    return fail(Error::Unknown);
  } catch (const std::exception &e) {
    log::error("ManagementClient async operation failed: {}", e.what());
    return fail(Error::Unknown);
  }
}

auto run_task_void(boost::asio::io_context &io, task<void> op) -> void {
  auto fut = boost::asio::co_spawn(io, std::move(op), boost::asio::use_future);
  while (fut.wait_for(std::chrono::seconds(0)) != std::future_status::ready) {
    io.run_one();
  }
  io.restart();
  try {
    fut.get();
  } catch (const std::future_error &e) {
    log::error("ManagementClient async void operation failed: {}", e.what());
  } catch (const std::exception &e) {
    log::error("ManagementClient async void operation failed: {}", e.what());
  }
}

} // namespace

ManagementClient::ManagementClient(const DatabaseConfig &db_config)
    : db_config_(db_config), db_(io_.get_executor(), db_config_) {}

ManagementClient::~ManagementClient() {
  if (db_.is_open()) {
    run_task_void(io_, db_.close());
  }
}

auto ManagementClient::open() -> Result<void> {
  return run_task(io_, db_.open());
}

auto ManagementClient::list_dags() const -> Result<std::vector<DAGInfo>> {
  return run_task(io_, db_.list_dags());
}

auto ManagementClient::get_dag(const DAGId &dag_id) const -> Result<DAGInfo> {
  return run_task(io_, db_.get_dag(dag_id));
}

auto ManagementClient::list_runs(std::size_t limit) const
    -> Result<std::vector<RunHistoryEntry>> {
  return run_task(io_, db_.list_run_history(limit));
}

auto ManagementClient::list_dag_runs(const DAGId &dag_id,
                                     std::size_t limit) const
    -> Result<std::vector<RunHistoryEntry>> {
  return run_task(io_, db_.list_dag_run_history(dag_id, limit));
}

auto ManagementClient::get_run(const DAGRunId &run_id) const
    -> Result<RunHistoryEntry> {
  return run_task(io_, db_.get_run_history(run_id));
}

auto ManagementClient::get_task_instances(const DAGRunId &run_id) const
    -> Result<std::vector<TaskInstanceInfo>> {
  return run_task(io_, db_.get_task_instances(run_id));
}

auto ManagementClient::get_task_xcoms(const DAGRunId &run_id,
                                      const TaskId &task_id) const
    -> Result<std::vector<XComEntry>> {
  return run_task(io_, db_.get_task_xcoms(run_id, task_id));
}

auto ManagementClient::clear_failed_tasks(const DAGRunId &run_id)
    -> Result<void> {
  auto tasks = run_task(io_, db_.get_task_instances(run_id));
  if (!tasks) {
    return std::unexpected(tasks.error());
  }

  for (auto &task : *tasks) {
    if (task.state == TaskState::Failed) {
      task.state = TaskState::Pending;
      task.exit_code = 0;
      task.error_message.clear();
      if (auto r = run_task(io_, db_.update_task_instance(run_id, task)); !r) {
        return r;
      }
    }
  }
  return {};
}

auto ManagementClient::clear_all_tasks(const DAGRunId &run_id) -> Result<void> {
  auto tasks = run_task(io_, db_.get_task_instances(run_id));
  if (!tasks) {
    return std::unexpected(tasks.error());
  }

  for (auto &task : *tasks) {
    task.state = TaskState::Pending;
    task.exit_code = 0;
    task.error_message.clear();
    if (auto r = run_task(io_, db_.update_task_instance(run_id, task)); !r) {
      return r;
    }
  }
  return {};
}

auto ManagementClient::clear_task(const DAGRunId &run_id, NodeIndex task_idx)
    -> Result<void> {
  auto tasks = run_task(io_, db_.get_task_instances(run_id));
  if (!tasks) {
    return std::unexpected(tasks.error());
  }

  for (auto &task : *tasks) {
    if (task.task_idx == task_idx) {
      task.state = TaskState::Pending;
      task.exit_code = 0;
      task.error_message.clear();
      return run_task(io_, db_.update_task_instance(run_id, task));
    }
  }
  return fail(Error::NotFound);
}

auto ManagementClient::set_dag_active(const DAGId &dag_id, bool active)
    -> Result<void> {
  return run_task(io_, db_.set_dag_active(dag_id, active));
}

auto ManagementClient::set_dag_paused(const DAGId &dag_id, bool paused)
    -> Result<void> {
  return run_task(io_, db_.set_dag_paused(dag_id, paused));
}

auto ManagementClient::delete_dag(const DAGId &dag_id) -> Result<void> {
  return run_task(io_, db_.delete_dag(dag_id));
}

auto ManagementClient::get_run_logs(const DAGRunId &run_id,
                                    std::size_t limit) const
    -> Result<std::vector<orm::TaskLogEntry>> {
  return run_task(io_, db_.get_run_logs(run_id, limit));
}

auto ManagementClient::get_task_logs(const DAGRunId &run_id,
                                     const TaskId &task_id, int attempt,
                                     std::size_t limit) const
    -> Result<std::vector<orm::TaskLogEntry>> {
  return run_task(io_, db_.get_task_logs(run_id, task_id, attempt, limit));
}

auto ManagementClient::get_latest_run(const DAGId &dag_id) const
    -> Result<RunHistoryEntry> {
  auto runs = run_task(io_, db_.list_dag_run_history(dag_id, 1));
  if (!runs)
    return std::unexpected(runs.error());
  if (runs->empty())
    return fail(Error::NotFound);
  return ok(std::move(runs->front()));
}

} // namespace dagforge::cli
