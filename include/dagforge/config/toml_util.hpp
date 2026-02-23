#pragma once

#include "dagforge/core/error.hpp"
#include "dagforge/util/log.hpp"

#include <glaze/toml.hpp>

#include <fstream>
#include <iterator>
#include <string>
#include <string_view>

namespace dagforge::toml_util {

/// Read entire file into a string.
[[nodiscard]] inline auto read_file(std::string_view path)
    -> Result<std::string> {
  std::ifstream in(std::string(path), std::ios::binary);
  if (!in) {
    return fail(Error::FileNotFound);
  }
  return ok(std::string((std::istreambuf_iterator<char>(in)),
                        std::istreambuf_iterator<char>()));
}

/// Parse TOML text into a glaze-compatible struct T.
template <typename T>
[[nodiscard]] auto parse_toml(std::string_view text,
                              std::string *diagnostic = nullptr) -> Result<T> {
  T raw{};
  constexpr auto kOpts =
      glz::opts{.format = glz::TOML, .error_on_unknown_keys = false};
  if (auto ec = glz::read<kOpts>(raw, text); ec) {
    auto detail = glz::format_error(ec, text);
    log::error("TOML parse error: {}", detail);
    if (diagnostic) {
      *diagnostic = std::move(detail);
    }
    return fail(Error::ParseError);
  }
  return ok(std::move(raw));
}

} // namespace dagforge::toml_util
