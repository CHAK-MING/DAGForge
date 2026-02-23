#include "dagforge/cli/commands.hpp"
#include "dagforge/cli/formatting.hpp"
#include "dagforge/config/config.hpp"
#include "dagforge/config/dag_file_loader.hpp"
#include "dagforge/dag/dag.hpp"
#include "dagforge/util/json.hpp"
#include "dagforge/util/log.hpp"

#include <filesystem>
#include <print>
#include <vector>

namespace dagforge::cli {

namespace {

struct ValidationResult {
  std::string dag_id;
  std::string file_path;
  bool valid{false};
  std::string error;
};

auto validate_single_file(const std::filesystem::path &path)
    -> ValidationResult {
  auto dag_id = path.stem().string();
  ValidationResult vr{.dag_id = dag_id,
                      .file_path = path.string(),
                      .valid = false,
                      .error = {}};

  std::string diagnostic;
  auto res =
      DAGDefinitionLoader::load_from_file(path.string(), &diagnostic)
          .and_then([&](const auto &def) -> Result<void> {
            if (def.tasks.empty()) {
              return fail(Error::InvalidArgument);
            }

            DAG dag;
            for (const auto &task : def.tasks) {
              [[maybe_unused]] auto idx =
                  dag.add_node(task.task_id, task.trigger_rule);
            }

            for (const auto &task : def.tasks) {
              for (const auto &dep : task.dependencies) {
                if (auto r = dag.add_edge(dep.task_id, task.task_id); !r) {
                  return fail(r.error());
                }
              }
            }

            return dag.is_valid();
          });

  vr.valid = res.has_value();
  if (!vr.valid) {
    vr.error = diagnostic.empty() ? res.error().message() : diagnostic;
  }
  return vr;
}

} // namespace

auto cmd_validate(const ValidateOptions &opts) -> int {
  log::set_output_stderr();
  std::vector<ValidationResult> results;

  if (opts.file.has_value()) {
    if (!std::filesystem::exists(*opts.file)) {
      std::println(stderr, "Error: File does not exist: {}", *opts.file);
      return 1;
    }
    results.emplace_back(validate_single_file(*opts.file));
  } else {
    auto config_res = ConfigLoader::load_from_file(opts.config_file);
    if (!config_res) {
      std::println(stderr, "Error: {}", config_res.error().message());
      return 1;
    }

    const auto &config = *config_res;
    if (config.dag_source.directory.empty()) {
      std::println(stderr, "Error: DAG directory not configured");
      return 1;
    }

    const auto &dir = config.dag_source.directory;
    if (!std::filesystem::exists(dir)) {
      std::println(stderr, "Error: Directory does not exist: {}", dir);
      return 1;
    }

    for (const auto &entry : std::filesystem::directory_iterator(dir)) {
      if (!entry.is_regular_file())
        continue;
      if (entry.path().extension() != ".toml")
        continue;
      results.emplace_back(validate_single_file(entry.path()));
    }
  }

  int valid_count = 0;
  int invalid_count = 0;

  if (opts.json) {
    JsonValue arr = std::vector<JsonValue>{};
    for (const auto &vr : results) {
      JsonValue obj{
          {"dag_id", vr.dag_id},
          {"file", vr.file_path},
          {"valid", vr.valid},
      };
      if (!vr.valid) {
        obj.get_object().emplace("error", vr.error);
      }
      arr.get_array().emplace_back(std::move(obj));
      if (vr.valid)
        valid_count++;
      else
        invalid_count++;
    }
    JsonValue output{
        {"results", std::move(arr)},
        {"summary",
         JsonValue{
             {"valid", static_cast<std::int64_t>(valid_count)},
             {"invalid", static_cast<std::int64_t>(invalid_count)},
             {"total", static_cast<std::int64_t>(results.size())},
         }},
    };
    std::println("{}", dump_json(output));
  } else {
    if (!opts.file.has_value()) {
      std::println("Validating DAG files...\n");
    }

    for (const auto &vr : results) {
      if (vr.valid) {
        std::println("{} {} - {}", fmt::ansi::green("\u2713"), vr.dag_id,
                     fmt::ansi::green("Valid"));
        valid_count++;
      } else {
        std::println("{} {} - {}", fmt::ansi::red("\u2717"), vr.dag_id,
                     fmt::ansi::red(vr.error));
        invalid_count++;
      }
    }

    std::println("\nSummary: {} valid, {} invalid out of {} DAG files",
                 fmt::ansi::green(std::format("{}", valid_count)),
                 invalid_count > 0
                     ? fmt::ansi::red(std::format("{}", invalid_count))
                     : std::format("{}", invalid_count),
                 results.size());
  }

  return invalid_count > 0 ? 1 : 0;
}

} // namespace dagforge::cli
