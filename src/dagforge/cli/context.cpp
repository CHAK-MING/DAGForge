#include "dagforge/cli/context.hpp"

#include "dagforge/cli/management_client.hpp"

#include <cstdlib>
#include <print>
#include <utility>

namespace dagforge::cli {

CliContext::CliContext() = default;
CliContext::~CliContext() = default;
CliContext::CliContext(CliContext &&) noexcept = default;
auto CliContext::operator=(CliContext &&) noexcept -> CliContext & = default;

auto load_config_or_print(std::string_view path) -> Result<SystemConfig> {
  return SystemConfigLoader::load_from_file(path).or_else(
      [&](std::error_code ec) -> Result<SystemConfig> {
        std::println(stderr, "Error: {}", ec.message());
        return fail(ec);
      });
}

auto open_client_or_print(const DatabaseConfig &db_config)
    -> Result<std::unique_ptr<ManagementClient>> {
  auto client = std::make_unique<ManagementClient>(db_config);
  if (auto r = client->open(); !r) {
    std::println(stderr, "Error: {}", r.error().message());
    return fail(r.error());
  }
  return ok(std::move(client));
}

auto load_context_or_print(std::string_view path) -> Result<CliContext> {
  auto config = load_config_or_print(path);
  if (!config) {
    return fail(config.error());
  }

  auto client = open_client_or_print(config->database);
  if (!client) {
    return fail(client.error());
  }

  CliContext ctx;
  ctx.config = std::move(*config);
  ctx.client = std::move(*client);
  return ok(std::move(ctx));
}

auto resolve_pid_file(const SystemConfig &config,
                      const std::optional<std::string> &override_pid_file)
    -> std::string {
  if (override_pid_file.has_value() && !override_pid_file->empty()) {
    return *override_pid_file;
  }
  if (!config.scheduler.pid_file.empty()) {
    return config.scheduler.pid_file;
  }
  if (const char *env = std::getenv("DAGFORGE_PID_FILE"); env && *env) {
    return env;
  }
  return "/tmp/dagforge.pid";
}

} // namespace dagforge::cli
