#pragma once

#include "dagforge/util/json.hpp"
#include <string>

namespace dagforge::xcom {

[[nodiscard]] auto stringify(const JsonValue &value) -> std::string;

} // namespace dagforge::xcom
