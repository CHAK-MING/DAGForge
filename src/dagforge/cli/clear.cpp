#include "dagforge/cli/commands.hpp"
#include "dagforge/cli/formatting.hpp"
#include "dagforge/cli/management_client.hpp"
#include "dagforge/config/config.hpp"
#include "dagforge/util/json.hpp"
#include "dagforge/util/log.hpp"

#include <deque>
#include <print>
#include <ranges>
#include <unordered_set>

namespace dagforge::cli {

auto cmd_clear(const ClearOptions &opts) -> int {
  log::set_output_stderr();
  if (opts.run_id.empty()) {
    std::println(stderr, "Error: --run is required for clear command");
    return 1;
  }

  auto config_res = ConfigLoader::load_from_file(opts.config_file)
                        .or_else([&](std::error_code ec) -> Result<Config> {
                          std::println(stderr, "Error: {}", ec.message());
                          return fail(ec);
                        });
  if (!config_res) {
    return 1;
  }

  ManagementClient client(config_res->database);

  if (auto r = client.open(); !r) {
    std::println(stderr, "Error: {}", r.error().message());
    return 1;
  }

  auto run_result =
      client.get_run(DAGRunId{opts.run_id})
          .or_else([&](std::error_code)
                       -> Result<ManagementClient::RunHistoryEntry> {
            std::println(stderr, "Error: Run '{}' not found", opts.run_id);
            return fail(Error::NotFound);
          });
  if (!run_result) {
    return 1;
  }

  if (opts.task.has_value()) {
    auto dag_result = client.get_dag(DAGId{opts.dag_id});
    if (!dag_result) {
      std::println(stderr, "Error: DAG '{}' not found", opts.dag_id);
      return 1;
    }
    const auto &dag = *dag_result;
    auto it = dag.task_index.find(TaskId{*opts.task});
    if (it == dag.task_index.end()) {
      std::println(stderr, "Error: Task '{}' not found in DAG '{}'", *opts.task,
                   opts.dag_id);
      return 1;
    }
    auto task_idx = static_cast<NodeIndex>(it->second);

    std::vector<NodeIndex> to_clear;
    to_clear.emplace_back(task_idx);

    if (opts.downstream) {
      std::deque<TaskId> q;
      std::unordered_set<std::string> visited;
      q.emplace_back(TaskId{*opts.task});
      visited.emplace(*opts.task);

      while (!q.empty()) {
        auto cur = std::move(q.front());
        q.pop_front();

        for (const auto &task_cfg : dag.tasks) {
          const bool depends_on_cur = std::ranges::any_of(
              task_cfg.dependencies,
              [&](const TaskDependency &dep) { return dep.task_id == cur; });
          if (!depends_on_cur) {
            continue;
          }

          auto [_, inserted] = visited.emplace(task_cfg.task_id.str());
          if (!inserted) {
            continue;
          }

          q.emplace_back(task_cfg.task_id.clone());
          auto it_idx = dag.task_index.find(task_cfg.task_id);
          if (it_idx != dag.task_index.end()) {
            to_clear.emplace_back(static_cast<NodeIndex>(it_idx->second));
          }
        }
      }
    }

    if (!opts.dry_run) {
      for (auto idx : to_clear) {
        auto r = client.clear_task(DAGRunId{opts.run_id}, idx);
        if (!r) {
          std::println(stderr, "Error: {}", r.error().message());
          return 1;
        }
      }
    }

    if (opts.json) {
      std::println(
          "{}",
          dump_json(JsonValue{
              {"dag_id", opts.dag_id},
              {"run_id", opts.run_id},
              {"task", *opts.task},
              {"dry_run", opts.dry_run},
              {"action", opts.downstream ? "cleared_downstream" : "cleared"},
              {"cleared_count", static_cast<std::int64_t>(to_clear.size())},
          }));
    } else {
      if (opts.downstream) {
        std::println("{} Task '{}' and {} downstream task(s) in run '{}' "
                     "{} to pending",
                     opts.dry_run ? fmt::ansi::yellow("Dry-run:")
                                  : fmt::ansi::green("\u2713"),
                     *opts.task, to_clear.size() > 0 ? to_clear.size() - 1 : 0,
                     opts.run_id,
                     opts.dry_run ? "would be cleared" : "cleared");
      } else {
        std::println("{} Task '{}' in run '{}' {} to pending",
                     opts.dry_run ? fmt::ansi::yellow("Dry-run:")
                                  : fmt::ansi::green("\u2713"),
                     *opts.task, opts.run_id,
                     opts.dry_run ? "would be cleared" : "cleared");
      }
    }
  } else if (opts.failed_only) {
    if (!opts.dry_run) {
      auto r = client.clear_failed_tasks(DAGRunId{opts.run_id});
      if (!r) {
        std::println(stderr, "Error: {}", r.error().message());
        return 1;
      }
    }

    if (opts.json) {
      std::println("{}", dump_json(JsonValue{
                             {"dag_id", opts.dag_id},
                             {"run_id", opts.run_id},
                             {"dry_run", opts.dry_run},
                             {"action", "cleared_failed"},
                         }));
    } else {
      std::println("{} Failed tasks in run '{}' {} to pending",
                   opts.dry_run ? fmt::ansi::yellow("Dry-run:")
                                : fmt::ansi::green("\u2713"),
                   opts.run_id, opts.dry_run ? "would be cleared" : "cleared");
    }
  } else if (opts.all_tasks) {
    if (!opts.dry_run) {
      auto r = client.clear_all_tasks(DAGRunId{opts.run_id});
      if (!r) {
        std::println(stderr, "Error: {}", r.error().message());
        return 1;
      }
    }

    if (opts.json) {
      std::println("{}", dump_json(JsonValue{
                             {"dag_id", opts.dag_id},
                             {"run_id", opts.run_id},
                             {"dry_run", opts.dry_run},
                             {"action", "cleared_all"},
                         }));
    } else {
      std::println("{} All tasks in run '{}' {} to pending",
                   opts.dry_run ? fmt::ansi::yellow("Dry-run:")
                                : fmt::ansi::green("\u2713"),
                   opts.run_id, opts.dry_run ? "would be cleared" : "cleared");
    }
  } else {
    std::println(
        stderr,
        "Error: Specify --task <id>, --failed, or --all to clear tasks");
    return 1;
  }
  return 0;
}

} // namespace dagforge::cli
