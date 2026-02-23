#pragma once

#include <functional>
#include <string>
#include <string_view>

namespace dagforge {

// Transparent string hash for heterogeneous lookup
struct StringHash {
  using is_transparent = void;

  [[nodiscard]] auto operator()(std::string_view sv) const noexcept
      -> std::size_t {
    return std::hash<std::string_view>{}(sv);
  }

  [[nodiscard]] auto operator()(const std::string &s) const noexcept
      -> std::size_t {
    return std::hash<std::string_view>{}(s);
  }

  [[nodiscard]] auto operator()(const char *s) const noexcept -> std::size_t {
    return std::hash<std::string_view>{}(s);
  }
};

using StringEqual = std::equal_to<>;

} // namespace dagforge
