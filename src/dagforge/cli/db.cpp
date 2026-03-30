#include "dagforge/app/application.hpp"
#include "dagforge/cli/commands.hpp"
#include "dagforge/cli/context.hpp"
#include "dagforge/config/dag_file_loader.hpp"
#include "dagforge/util/log.hpp"

#include <algorithm>
#include <filesystem>
#include <print>
#include <unordered_set>
#include <vector>


namespace dagforge::cli {
namespace {

auto run_db_ensure(const DbOptions &opts, std::string_view mode) -> int {
  log::set_output_stderr();
  auto config_res = load_config_or_print(opts.config_file);
  if (!config_res) {
    return 1;
  }

  Application app(std::move(*config_res));
  app.config().api.enabled = false;

  if (auto r = app.init_db_only(); !r) {
    std::println(stderr, "Error: {}", r.error().message());
    return 1;
  }

  if (mode == "init") {
    std::println("Database schema initialized.");
  } else {
    std::println(
        "Database schema migration check complete (up to date).\n"
        "Note: only idempotent schema ensure is implemented currently.");
  }

  return 0;
}

} // namespace

auto cmd_db_init(const DbOptions &opts) -> int {
  return run_db_ensure(opts, "init");
}

auto cmd_db_migrate(const DbOptions &opts) -> int {
  return run_db_ensure(opts, "migrate");
}

auto cmd_db_prune_stale(const DbOptions &opts) -> int {
  log::set_output_stderr();
  auto ctx = load_context_or_print(opts.config_file);
  if (!ctx) {
    return 1;
  }
  auto &config = ctx->config;
  auto &client = ctx->db();

  std::unordered_set<std::string> file_dag_ids;
  if (!config.dag_source.directory.empty() &&
      std::filesystem::exists(config.dag_source.directory)) {
    DAGFileLoader loader(config.dag_source.directory);
    auto file_dags = loader.load_all();
    if (!file_dags) {
      std::println(stderr, "Error: {}", file_dags.error().message());
      return 1;
    }
    file_dag_ids.reserve(file_dags->size());
    for (const auto &dag_file : *file_dags) {
      file_dag_ids.insert(dag_file.dag_id.str());
    }
  }

  auto db_dags = client.list_dags();
  if (!db_dags) {
    std::println(stderr, "Error: {}", db_dags.error().message());
    return 1;
  }

  std::vector<DAGId> stale_ids =
      *db_dags | std::views::filter([&](const DAGInfo &dag) {
        return !file_dag_ids.contains(dag.dag_id.str());
      }) |
      std::views::transform(&DAGInfo::dag_id) | std::ranges::to<std::vector>();
  std::ranges::sort(stale_ids, [](const DAGId &lhs, const DAGId &rhs) {
    return lhs.str() < rhs.str();
  });

  if (stale_ids.empty()) {
    std::println("No stale DAGs found in database.");
    return 0;
  }

  std::println("Found {} stale DAG(s):", stale_ids.size());
  for (const auto &id : stale_ids) {
    std::println("  - {}", id.str());
  }
  if (opts.dry_run) {
    std::println("Dry run mode enabled; no rows were deleted.");
    return 0;
  }

  std::size_t deleted = 0;
  for (const auto &id : stale_ids) {
    if (auto r = client.delete_dag(id); !r) {
      std::println(stderr, "Error deleting DAG '{}': {}", id.str(),
                   r.error().message());
      return 1;
    }
    ++deleted;
  }

  std::println("Deleted {} stale DAG(s) from database.", deleted);
  return 0;
}

} // namespace dagforge::cli
