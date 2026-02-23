#include "dagforge/cli/commands.hpp"
#include "dagforge/cli/formatting.hpp"
#include "dagforge/cli/management_client.hpp"
#include "dagforge/config/config.hpp"
#include "dagforge/util/log.hpp"

#include <chrono>
#include <print>
#include <thread>
#include <vector>

namespace dagforge::cli {

namespace {

auto resolve_run_id_prefix(ManagementClient &client, const DAGId &dag_id,
                           std::string_view input) -> Result<DAGRunId> {
  auto runs = client.list_dag_runs(dag_id, 500);
  if (!runs) {
    return fail(runs.error());
  }
  std::vector<DAGRunId> matches;
  for (const auto &r : *runs) {
    const auto id = r.dag_run_id.str();
    if (id.starts_with(input)) {
      matches.push_back(r.dag_run_id.clone());
    }
  }
  if (matches.empty()) {
    return fail(Error::NotFound);
  }
  if (matches.size() > 1) {
    return fail(Error::InvalidArgument);
  }
  return ok(std::move(matches.front()));
}

auto resolve_run_id(ManagementClient &client, const LogsOptions &opts)
    -> Result<DAGRunId> {
  if (opts.latest || opts.run_id.empty() || opts.run_id == "latest") {
    if (opts.dag_id.empty()) {
      return fail(Error::InvalidArgument);
    }
    auto run = client.get_latest_run(DAGId{opts.dag_id});
    if (!run) {
      std::println(stderr, "Error: No runs found for DAG '{}'", opts.dag_id);
      return fail(run.error());
    }
    return ok(run->dag_run_id.clone());
  }
  auto exact = client.get_run(DAGRunId{opts.run_id});
  if (exact) {
    return ok(DAGRunId{opts.run_id});
  }
  auto resolved =
      resolve_run_id_prefix(client, DAGId{opts.dag_id}, opts.run_id);
  if (!resolved) {
    if (resolved.error() == make_error_code(Error::InvalidArgument)) {
      std::println(stderr,
                   "Error: run_id prefix '{}' is ambiguous for DAG '{}'.",
                   opts.run_id, opts.dag_id);
    }
    return fail(resolved.error());
  }
  return resolved;
}

auto print_log_entry_json(const orm::TaskLogEntry &e, bool short_time) -> void {
  auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                e.logged_at.time_since_epoch())
                .count();
  auto ts =
      short_time ? fmt::format_timestamp_short(ms) : fmt::format_timestamp(ms);
  // Emit a simple JSON object per line (newline-delimited JSON)
  std::println("{{\"task_id\":\"{}\",\"attempt\":{},\"stream\":\"{}\","
               "\"logged_at\":\"{}\",\"content\":\"{}\"}}",
               e.task_id.str(), e.attempt, e.stream, ts, e.content);
}

auto print_log_entry(const orm::TaskLogEntry &e, bool short_time) -> void {
  auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                e.logged_at.time_since_epoch())
                .count();
  auto ts =
      short_time ? fmt::format_timestamp_short(ms) : fmt::format_timestamp(ms);
  auto prefix = std::format("[{}][{}][attempt={}][{}]", ts, e.task_id.str(),
                            e.attempt, e.stream);
  if (e.stream == "stderr") {
    std::println("{} {}", fmt::ansi::red(prefix), e.content);
  } else {
    std::println("{} {}", fmt::ansi::dim(prefix), e.content);
  }
}

auto print_log_entries(const std::vector<orm::TaskLogEntry> &entries,
                       bool json_mode, bool short_time) -> void {
  for (const auto &e : entries) {
    if (json_mode) {
      print_log_entry_json(e, short_time);
    } else {
      print_log_entry(e, short_time);
    }
  }
}

} // namespace

auto cmd_logs(const LogsOptions &opts) -> int {
  log::set_output_stderr();
  auto config_res = ConfigLoader::load_from_file(opts.config_file);
  if (!config_res) {
    std::println(stderr, "Error: {}", config_res.error().message());
    return 1;
  }

  ManagementClient client(config_res->database);
  if (auto r = client.open(); !r) {
    std::println(stderr, "Error: {}", r.error().message());
    return 1;
  }

  auto run_id_res = resolve_run_id(client, opts);
  if (!run_id_res) {
    return 1;
  }
  const auto run_id = std::move(*run_id_res);

  if (!opts.json && (opts.latest || opts.run_id.empty())) {
    std::println("Showing logs for run: {}", run_id.str());
  }

  if (!opts.follow) {
    Result<std::vector<orm::TaskLogEntry>> logs_res;
    if (!opts.task_id.empty()) {
      logs_res =
          client.get_task_logs(run_id, TaskId{opts.task_id}, opts.attempt);
    } else {
      logs_res = client.get_run_logs(run_id);
    }

    if (!logs_res) {
      std::println(stderr, "Error: {}", logs_res.error().message());
      return 1;
    }

    if (logs_res->empty()) {
      if (!opts.json) {
        std::println("No logs found for run '{}'.", run_id.str());
      } else {
        std::println("[]");
      }
      return 0;
    }

    print_log_entries(*logs_res, opts.json, opts.short_time);
    return 0;
  }

  // --follow mode: poll for new log lines
  if (!opts.json) {
    std::println("Following logs for run '{}' (Ctrl+C to stop)...",
                 run_id.str());
  }

  std::int64_t last_log_rowid = 0;
  const auto poll_interval = std::chrono::milliseconds(500);
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::hours(2);

  while (std::chrono::steady_clock::now() < deadline) {
    Result<std::vector<orm::TaskLogEntry>> logs_res;
    if (!opts.task_id.empty()) {
      logs_res =
          client.get_task_logs(run_id, TaskId{opts.task_id}, opts.attempt);
    } else {
      logs_res = client.get_run_logs(run_id);
    }

    if (logs_res) {
      for (const auto &e : *logs_res) {
        if (e.log_rowid > last_log_rowid) {
          last_log_rowid = e.log_rowid;
          print_log_entries({e}, opts.json, opts.short_time);
        }
      }

      // Check if the run has finished
      auto run_res = client.get_run(run_id);
      if (run_res) {
        const auto state = run_res->state;
        if (state == DAGRunState::Success || state == DAGRunState::Failed ||
            state == DAGRunState::Cancelled || state == DAGRunState::Skipped) {
          if (!opts.json) {
            std::println("\nRun finished: {}",
                         fmt::colorize_dag_run_state(enum_to_string(state)));
          }
          break;
        }
      }
    }

    std::this_thread::sleep_for(poll_interval);
  }

  return 0;
}

} // namespace dagforge::cli
