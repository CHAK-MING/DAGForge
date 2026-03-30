#pragma once

#if !defined(DAGFORGE_BUILDING_MODULE_INTERFACE) &&                              \
    (!defined(DAGFORGE_CONSUME_NAMED_MODULES) ||                                \
     !DAGFORGE_CONSUME_NAMED_MODULES)
#include "dagforge/core/error.hpp"
#endif

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include <glaze/json.hpp>

#include <string>
#include <string_view>
#endif


namespace dagforge {

using JsonValue = glz::generic_json<glz::num_mode::i64>;

[[nodiscard]] inline auto dump_json(const JsonValue &value) -> std::string {
  auto out = glz::write_json(value);
  return out ? *out : "null";
}

[[nodiscard]] inline auto parse_json(std::string_view input)
    -> Result<JsonValue> {
  JsonValue value{};
  constexpr auto kOpts = glz::opts{.null_terminated = false};
  if (auto ec = glz::read<kOpts>(value, input); ec) {
    return fail(Error::ParseError);
  }
  return ok(std::move(value));
}

[[nodiscard]] inline auto is_valid_json(std::string_view input) -> bool {
  JsonValue value{};
  constexpr auto kOpts = glz::opts{.null_terminated = false};
  return !static_cast<bool>(glz::read<kOpts>(value, input));
}

} // namespace dagforge
