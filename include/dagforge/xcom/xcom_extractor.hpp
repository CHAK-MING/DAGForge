#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/config/task_config.hpp"
#include "dagforge/executor/executor.hpp"
#endif

#include <string>
#include <string_view>
#include <vector>


namespace dagforge::xcom {

struct ExtractedXCom {
  std::string key;
  // Serialized JSON payload.
  std::string value;
};

[[nodiscard]] auto extract(const ExecutorResult &result,
                           const std::vector<XComPushConfig> &configs)
    -> Result<std::vector<ExtractedXCom>>;

} // namespace dagforge::xcom
