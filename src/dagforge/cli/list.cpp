#include "dagforge/cli/commands.hpp"
#include "dagforge/cli/formatting.hpp"
#include "dagforge/cli/management_client.hpp"
#include "dagforge/config/config.hpp"
#include "dagforge/config/dag_definition.hpp"
#include "dagforge/config/dag_file_loader.hpp"
#include "dagforge/dag/dag_domain.hpp"
#include "dagforge/util/json.hpp"
#include "dagforge/util/log.hpp"

#include <ankerl/unordered_dense.h>

#include <algorithm>
#include <filesystem>
#include <print>
#include <unordered_map>
#include <unordered_set>

namespace dagforge::cli {

auto cmd_list_dags(const ListDagsOptions &opts) -> int {
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

  const bool has_file_source = !config_res->dag_source.directory.empty();
  std::vector<DAGInfo> dags;
  std::unordered_set<std::string> dag_ids;
  std::unordered_map<std::string, std::string> dag_sources;

  if (!config_res->dag_source.directory.empty()) {
    DAGFileLoader loader(config_res->dag_source.directory);
    auto file_dags = loader.load_all();
    if (!file_dags) {
      std::println(stderr, "Error: {}", file_dags.error().message());
      return 1;
    }
    dags.reserve(file_dags->size());
    for (auto &dag_file : *file_dags) {
      DAGInfo dag = std::move(dag_file.definition);
      dag.dag_id = dag_file.dag_id;
      dag.rebuild_task_index();
      dag_ids.insert(dag.dag_id.str());
      dag_sources.insert_or_assign(dag.dag_id.str(), "file");
      dags.emplace_back(std::move(dag));
    }
  }

  auto result = client.list_dags();
  if (!result) {
    std::println(stderr, "Error: {}", result.error().message());
    return 1;
  }

  // Build a map of DB records for fast lookup during merge.
  ankerl::unordered_dense::map<DAGId, DAGInfo> db_by_id;
  db_by_id.reserve(result->size());
  for (auto &db_dag : *result) {
    db_dag.rebuild_task_index();
    db_by_id.insert_or_assign(db_dag.dag_id, db_dag);
  }

  // Merge DB runtime state (rowids, is_paused, version) into file-loaded DAGs.
  for (auto &dag : dags) {
    if (auto it = db_by_id.find(dag.dag_id); it != db_by_id.end()) {
      merge_db_state_into_dag_info(dag, it->second);
      dag_sources.insert_or_assign(dag.dag_id.str(), "file+db");
      db_by_id.erase(it);
    }
  }

  // Append DB-only DAGs (not present as files).
  for (auto &[id, db_dag] : db_by_id) {
    if (dag_ids.contains(id.str())) {
      continue;
    }
    const auto dag_file =
        std::filesystem::path(config_res->dag_source.directory) /
        std::format("{}.toml", id.str());
    if (std::filesystem::exists(dag_file)) {
      if (auto dag_from_file =
              DAGDefinitionLoader::load_from_file(dag_file.string());
          dag_from_file) {
        merge_db_state_into_dag_info(*dag_from_file, db_dag);
        dag_ids.insert(dag_from_file->dag_id.str());
        dag_sources.insert_or_assign(dag_from_file->dag_id.str(), "file+db");
        dags.emplace_back(std::move(*dag_from_file));
        continue;
      }
    }
    dag_ids.insert(id.str());
    dag_sources.insert_or_assign(id.str(), "db");
    if (!has_file_source || opts.include_stale) {
      dags.emplace_back(std::move(db_dag));
    }
  }

  std::ranges::sort(dags, [](const DAGInfo &lhs, const DAGInfo &rhs) {
    return lhs.dag_id.str() < rhs.dag_id.str();
  });
  if (dags.size() > opts.limit) {
    dags.resize(opts.limit);
  }

  const bool json_mode = opts.json || opts.output == "json";
  if (json_mode) {
    JsonValue arr = std::vector<JsonValue>{};
    for (const auto &dag : dags) {
      JsonValue tasks_arr = std::vector<JsonValue>{};
      for (const auto &t : dag.tasks) {
        tasks_arr.get_array().emplace_back(t.task_id.str());
      }
      JsonValue obj{
          {"dag_id", dag.dag_id.str()},
          {"name", dag.name},
          {"cron", dag.cron},
          {"is_paused", dag.is_paused},
          {"version", static_cast<std::int64_t>(dag.version)},
          {"max_concurrent_runs",
           static_cast<std::int64_t>(dag.max_concurrent_runs)},
          {"source", dag_sources[dag.dag_id.str()]},
          {"task_count", static_cast<std::int64_t>(dag.tasks.size())},
          {"tasks", std::move(tasks_arr)},
      };
      arr.get_array().emplace_back(std::move(obj));
    }
    std::println("{}", dump_json(arr));
    return 0;
  }

  if (dags.empty()) {
    std::println("No DAGs found.");
    return 0;
  }

  fmt::Table table(
      {{.header = "DAG_ID", .width = 22},
       {.header = "NAME", .width = 20},
       {.header = "TASKS", .width = 7, .right_align = true},
       {.header = "SCHEDULE", .width = 16},
       {.header = "SOURCE", .width = 10},
       {.header = "STATUS", .width = 8},
       {.header = "CONCURRENCY", .width = 13, .right_align = true}});
  table.print_header();

  for (const auto &dag : dags) {
    table.print_row(
        {dag.dag_id.str(), dag.name.empty() ? dag.dag_id.str() : dag.name,
         std::format("{}", dag.tasks.size()), dag.cron.empty() ? "-" : dag.cron,
         dag_sources[dag.dag_id.str()], dag.is_paused ? "paused" : "active",
         std::format("{}", dag.max_concurrent_runs)});
  }

  std::println("\nTotal: {} DAGs", dags.size());
  return 0;
}

auto cmd_list_runs(const ListRunsOptions &opts) -> int {
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

  auto result = opts.dag_id.empty()
                    ? client.list_runs(opts.limit)
                    : client.list_dag_runs(DAGId{opts.dag_id}, opts.limit);
  if (!result) {
    std::println(stderr, "Error: {}", result.error().message());
    return 1;
  }

  auto runs = std::move(*result);

  // Apply state filter if specified
  if (!opts.state.empty()) {
    auto target = parse<DAGRunState>(opts.state);
    std::erase_if(runs, [&](const auto &r) { return r.state != target; });
  }

  const bool json_mode = opts.json || opts.output == "json";
  if (json_mode) {
    JsonValue arr = std::vector<JsonValue>{};
    for (const auto &run : runs) {
      arr.get_array().emplace_back(JsonValue{
          {"dag_run_id", run.dag_run_id.str()},
          {"dag_id", run.dag_id.str()},
          {"state", enum_to_string(run.state)},
          {"trigger_type", enum_to_string(run.trigger_type)},
          {"started_at", fmt::format_timestamp(run.started_at)},
          {"finished_at", fmt::format_timestamp(run.finished_at)},
      });
    }
    std::println("{}", dump_json(arr));
    return 0;
  }

  if (runs.empty()) {
    std::println("No runs found.");
    return 0;
  }

  fmt::Table table({{.header = "RUN_ID", .width = 36},
                    {.header = "DAG", .width = 18},
                    {.header = "STATUS", .width = 10},
                    {.header = "TRIGGER", .width = 10},
                    {.header = "STARTED", .width = 20},
                    {.header = "DURATION", .width = 10}});
  table.print_header();

  for (const auto &run : runs) {
    auto state_sv = to_string_view(run.state);
    table.print_row(
        {run.dag_run_id.str(), run.dag_id.str(),
         fmt::colorize_dag_run_state(state_sv),
         enum_to_string(run.trigger_type),
         fmt::format_timestamp_short(run.started_at),
         fmt::format_duration_seconds(run.started_at, run.finished_at)});
  }

  std::println("\nShowing {} of {} runs", std::min(runs.size(), opts.limit),
               runs.size());
  return 0;
}

namespace {

auto print_tasks_table(const DAGInfo &dag, bool show_dag_col) -> void {
  if (show_dag_col) {
    fmt::Table table({{.header = "DAG", .width = 20},
                      {.header = "TASK_ID", .width = 28},
                      {.header = "TRIGGER_RULE", .width = 18},
                      {.header = "DEPENDENCIES", .width = 36}});
    table.print_header();
    for (const auto &task : dag.tasks) {
      std::string deps;
      for (std::size_t i = 0; i < task.dependencies.size(); ++i) {
        if (i > 0)
          deps += ", ";
        deps += task.dependencies[i].task_id.str();
      }
      table.print_row({dag.dag_id.str(), task.task_id.str(),
                       enum_to_string(task.trigger_rule),
                       deps.empty() ? "-" : deps});
    }
  } else {
    fmt::Table table({{.header = "TASK_ID", .width = 28},
                      {.header = "TRIGGER_RULE", .width = 18},
                      {.header = "DEPENDENCIES", .width = 40}});
    table.print_header();
    for (const auto &task : dag.tasks) {
      std::string deps;
      for (std::size_t i = 0; i < task.dependencies.size(); ++i) {
        if (i > 0)
          deps += ", ";
        deps += task.dependencies[i].task_id.str();
      }
      table.print_row({task.task_id.str(), enum_to_string(task.trigger_rule),
                       deps.empty() ? "-" : deps});
    }
  }
}

auto dag_to_json(const DAGInfo &dag, bool include_dag_id) -> JsonValue {
  JsonValue arr = std::vector<JsonValue>{};
  for (const auto &task : dag.tasks) {
    JsonValue deps = std::vector<JsonValue>{};
    for (const auto &dep : task.dependencies) {
      deps.get_array().emplace_back(dep.task_id.str());
    }
    JsonValue entry{
        {"task_id", task.task_id.str()},
        {"trigger_rule", enum_to_string(task.trigger_rule)},
        {"dependencies", std::move(deps)},
    };
    if (include_dag_id) {
      entry.get_object().emplace("dag_id", dag.dag_id.str());
    }
    arr.get_array().emplace_back(std::move(entry));
  }
  return arr;
}

} // namespace

auto cmd_list_tasks(const ListTasksOptions &opts) -> int {
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

  const bool json_mode = opts.json || opts.output == "json";
  // Single DAG mode
  if (!opts.dag_id.empty()) {
    const auto dag_file =
        std::filesystem::path(config_res->dag_source.directory) /
        std::format("{}.toml", opts.dag_id);
    std::optional<DAGInfo> dag;
    if (std::filesystem::exists(dag_file)) {
      if (auto r = DAGDefinitionLoader::load_from_file(dag_file.string()); r) {
        dag = std::move(*r);
      }
    }
    if (!dag.has_value()) {
      auto result = client.get_dag(DAGId{opts.dag_id});
      if (!result) {
        std::println(stderr, "Error: DAG '{}' not found", opts.dag_id);
        return 1;
      }
      dag = std::move(*result);
    }

    if (json_mode) {
      std::println("{}", dump_json(dag_to_json(*dag, false)));
      return 0;
    }
    if (dag->tasks.empty()) {
      std::println("No tasks defined in DAG '{}'.", opts.dag_id);
      return 0;
    }
    print_tasks_table(*dag, false);
    std::println("\nTotal: {} tasks", dag->tasks.size());
    return 0;
  }

  // All DAGs mode
  auto db_dags = client.list_dags();
  if (!db_dags) {
    std::println(stderr, "Error: {}", db_dags.error().message());
    return 1;
  }

  if (json_mode) {
    JsonValue arr = std::vector<JsonValue>{};
    for (const auto &dag : *db_dags) {
      for (auto &entry : dag_to_json(dag, true).get_array()) {
        arr.get_array().emplace_back(std::move(entry));
      }
    }
    std::println("{}", dump_json(arr));
    return 0;
  }

  if (db_dags->empty()) {
    std::println("No DAGs found.");
    return 0;
  }

  std::size_t total = 0;
  for (const auto &dag : *db_dags) {
    total += dag.tasks.size();
  }
  print_tasks_table((*db_dags)[0], true);
  for (std::size_t i = 1; i < db_dags->size(); ++i) {
    print_tasks_table((*db_dags)[i], true);
  }
  std::println("\nTotal: {} tasks across {} DAGs", total, db_dags->size());
  return 0;
}

} // namespace dagforge::cli
