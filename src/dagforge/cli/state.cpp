#include "dagforge/cli/commands.hpp"
#include "dagforge/cli/context.hpp"
#include "dagforge/cli/formatting.hpp"
#include "dagforge/config/dag_info_loader.hpp"
#include "dagforge/util/json.hpp"
#include "dagforge/util/log.hpp"

#include <filesystem>
#include <optional>
#include <print>


namespace dagforge::cli {

namespace {

auto dag_exists_in_source(const SystemConfig &config, std::string_view dag_id)
    -> std::optional<bool> {
  const auto &dir = config.dag_source.directory;
  if (dir.empty() || !std::filesystem::exists(dir) ||
      !std::filesystem::is_directory(dir)) {
    return std::nullopt;
  }

  for (const auto &entry : std::filesystem::directory_iterator(dir)) {
    if (!entry.is_regular_file() || entry.path().extension() != ".toml") {
      continue;
    }

    auto dag = DAGInfoLoader::load_from_file(entry.path().string());
    if (!dag) {
      continue;
    }
    if (dag->dag_id.value() == dag_id) {
      return true;
    }
  }

  return false;
}

auto set_dag_paused(const std::string &config_file, const std::string &dag_id,
                    bool paused) -> int {
  log::set_output_stderr();
  auto ctx = load_context_or_print(config_file);
  if (!ctx) {
    return 1;
  }
  auto &config = ctx->config;
  auto &client = ctx->db();

  if (auto r = client.set_dag_paused(DAGId{dag_id}, paused); !r) {
    if (r.error() == Error::NotFound) {
      auto in_source = dag_exists_in_source(config, dag_id);
      if (in_source.has_value() && *in_source) {
        std::println(stderr,
                     "Error: DAG '{}' exists in DAG files but is not "
                     "registered in database.",
                     dag_id);
        std::println(stderr,
                     "Hint: start service to load DAGs: dagforge serve -c {}",
                     config_file);
        std::println(stderr, "Hint: then verify with: dagforge list dags -c {}",
                     config_file);
      } else if (in_source.has_value()) {
        std::println(
            stderr,
            "Error: DAG '{}' not found in database or DAG source directory.",
            dag_id);
        std::println(
            stderr,
            "Hint: check id and validate DAG files: dagforge validate -c {}",
            config_file);
      } else {
        std::println(stderr, "Error: DAG '{}' not found", dag_id);
        std::println(stderr, "Hint: DAG source directory is unavailable; check "
                             "[dag_source].directory in config");
      }
    } else {
      std::println(stderr, "Error: {}", r.error().message());
    }
    return 1;
  }
  return 0;
}

} // namespace

auto cmd_pause(const PauseOptions &opts) -> int {
  if (int rc = set_dag_paused(opts.config_file, opts.dag_id, true); rc != 0) {
    return rc;
  }

  if (opts.json) {
    std::println("{}", dump_json(JsonValue{
                           {"dag_id", opts.dag_id},
                           {"is_paused", true},
                       }));
  } else {
    std::println("{} DAG '{}' paused", fmt::ansi::yellow("\u23F8"),
                 opts.dag_id);
  }
  return 0;
}

auto cmd_unpause(const UnpauseOptions &opts) -> int {
  if (int rc = set_dag_paused(opts.config_file, opts.dag_id, false); rc != 0) {
    return rc;
  }

  if (opts.json) {
    std::println("{}", dump_json(JsonValue{
                           {"dag_id", opts.dag_id},
                           {"is_paused", false},
                       }));
  } else {
    std::println("{} DAG '{}' unpaused", fmt::ansi::green("\u25B6"),
                 opts.dag_id);
  }
  return 0;
}

} // namespace dagforge::cli
