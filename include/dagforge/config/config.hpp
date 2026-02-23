#pragma once

#include "dagforge/config/system_config.hpp"
#include "dagforge/core/error.hpp"

namespace dagforge {

using Config = SystemConfig;

class ConfigLoader {
public:
  [[nodiscard]] static auto load_from_file(std::string_view path)
      -> Result<SystemConfig>;
  [[nodiscard]] static auto load_from_string(std::string_view toml_str)
      -> Result<SystemConfig>;
};

} // namespace dagforge
