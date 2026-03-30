#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/cli/management_client.hpp"
#include "dagforge/config/system_config_loader.hpp"
#endif

#include <memory>
#include <optional>
#include <string>
#include <string_view>

namespace dagforge::cli {

struct CliContext {
  CliContext();
  ~CliContext();
  CliContext(CliContext &&) noexcept;
  auto operator=(CliContext &&) noexcept -> CliContext &;
  CliContext(const CliContext &) = delete;
  auto operator=(const CliContext &) -> CliContext & = delete;

  SystemConfig config;
  std::unique_ptr<ManagementClient> client;

  [[nodiscard]] auto db() noexcept -> ManagementClient & { return *client; }
  [[nodiscard]] auto db() const noexcept -> const ManagementClient & {
    return *client;
  }
};

[[nodiscard]] auto load_config_or_print(std::string_view path)
    -> Result<SystemConfig>;

[[nodiscard]] auto
open_client_or_print(const DatabaseConfig &db_config)
    -> Result<std::unique_ptr<ManagementClient>>;

[[nodiscard]] auto load_context_or_print(std::string_view path)
    -> Result<CliContext>;

[[nodiscard]] auto
resolve_pid_file(const SystemConfig &config,
                 const std::optional<std::string> &override_pid_file)
    -> std::string;

} // namespace dagforge::cli
