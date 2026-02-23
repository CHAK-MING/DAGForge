#pragma once

#include "dagforge/config/task_config.hpp"
#include "dagforge/core/error.hpp"
#include "dagforge/executor/executor.hpp"
#include "dagforge/util/json.hpp"

#include <string>
#include <string_view>
#include <vector>

namespace dagforge::xcom {

struct ExtractedXCom {
  std::string key;
  JsonValue value;
};

[[nodiscard]] auto extract(const ExecutorResult &result,
                           const std::vector<XComPushConfig> &configs)
    -> Result<std::vector<ExtractedXCom>>;

} // namespace dagforge::xcom
