#include "dagforge/xcom/xcom_util.hpp"
#include "dagforge/util/json.hpp"


namespace dagforge::xcom {

auto render_serialized_json(std::string_view json_text)
    -> Result<std::string> {
  auto parsed = parse_json(json_text);
  if (!parsed) {
    return fail(parsed.error());
  }
  if (parsed->is_string()) {
    return ok(parsed->as<std::string>());
  }
  return ok(dump_json(*parsed));
}

} // namespace dagforge::xcom
