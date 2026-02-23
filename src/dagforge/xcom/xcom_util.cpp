#include "dagforge/xcom/xcom_util.hpp"

namespace dagforge::xcom {

auto stringify(const JsonValue &value) -> std::string {
  if (value.is_string()) {
    return value.as<std::string>();
  }
  return dump_json(value);
}

} // namespace dagforge::xcom
