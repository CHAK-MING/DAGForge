#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include <boost/describe/enum.hpp>
#include <boost/describe/enumerators.hpp>
#include <boost/mp11/algorithm.hpp>

#include <array>
#include <cctype>
#include <concepts>
#include <ranges>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#endif

namespace dagforge {

template <typename T>
[[nodiscard]] auto parse(std::string_view s) noexcept -> T;

template <typename E>
[[nodiscard]] inline auto enum_to_string(E value) -> std::string {
  return std::string{to_string_view(value)};
}

namespace util {

[[nodiscard]] inline auto normalize_enum_token(std::string_view token)
    -> std::string {
  std::string out;
  out.reserve(token.size());
  for (char c : token) {
    const auto uch = static_cast<unsigned char>(c);
    if (std::isalnum(uch) != 0) {
      out.push_back(static_cast<char>(std::tolower(uch)));
    }
  }
  return out;
}

[[nodiscard]] inline auto enum_name_to_snake_case(std::string_view name)
    -> std::string {
  std::string out;
  out.reserve(name.size() * 2);

  for (auto [i, ch] : name | std::views::enumerate) {
    const auto uch = static_cast<unsigned char>(ch);
    if (std::isupper(uch) != 0 && i > 0) {
      const bool prev_lower =
          std::islower(static_cast<unsigned char>(name[i - 1])) != 0;
      const bool next_lower =
          (static_cast<std::size_t>(i) + 1 < name.size()) &&
          std::islower(static_cast<unsigned char>(name[i + 1])) != 0;
      if (prev_lower || next_lower) {
        out.push_back('_');
      }
    }
    out.push_back(static_cast<char>(std::tolower(uch)));
  }

  return out;
}

template <typename E>
[[nodiscard]] inline auto
enum_to_string_view(E value, std::string_view fallback = "unknown") noexcept
    -> std::string_view {
  std::string_view out = fallback;
  boost::mp11::mp_for_each<boost::describe::describe_enumerators<E>>(
      [&](auto descriptor) {
        if (value == descriptor.value) {
          out = descriptor.name;
        }
      });
  return out;
}

template <typename E>
[[nodiscard]] inline auto enum_to_snake_case(E value) -> std::string {
  const auto enum_name = enum_to_string_view(value, "unknown");
  return enum_name_to_snake_case(enum_name);
}

namespace detail {

template <typename E, typename Transform>
[[nodiscard]] inline auto make_enum_table(Transform transform)
    -> std::array<std::pair<E, std::string>,
                  boost::mp11::mp_size<
                      boost::describe::describe_enumerators<E>>::value> {
  using descriptors = boost::describe::describe_enumerators<E>;
  constexpr std::size_t kCount = boost::mp11::mp_size<descriptors>::value;
  std::array<std::pair<E, std::string>, kCount> out{};
  std::size_t i = 0;
  boost::mp11::mp_for_each<descriptors>([&](auto descriptor) {
    out[i++] = {descriptor.value, transform(descriptor.name)};
  });
  return out;
}

} // namespace detail

template <typename E>
[[nodiscard]] inline auto
enum_to_snake_case_view(E value, std::string_view fallback = "unknown") noexcept
    -> std::string_view {
  static const auto table = detail::make_enum_table<E>([](std::string_view name) {
    return enum_name_to_snake_case(name);
  });

  for (const auto &[enum_value, text] : table) {
    if (enum_value == value) {
      return text;
    }
  }

  return fallback;
}

template <typename E>
[[nodiscard]] inline auto parse_enum(std::string_view input,
                                     E default_value) noexcept -> E {
  const auto normalized_input = normalize_enum_token(input);
  E out = default_value;
  static const auto table = detail::make_enum_table<E>([](std::string_view name) {
    return normalize_enum_token(name);
  });
  for (const auto &[enum_value, text] : table) {
    if (normalized_input == text) {
      out = enum_value;
    }
  }
  return out;
}

template <typename E>
  requires std::is_enum_v<E>
[[nodiscard]] constexpr auto enum_to_code(E value) noexcept
    -> std::underlying_type_t<E> {
  return static_cast<std::underlying_type_t<E>>(value);
}

template <typename E, typename I>
  requires(std::is_enum_v<E> && std::is_integral_v<I>)
[[nodiscard]] inline auto parse_enum_code(I code, E default_value) noexcept
    -> E {
  using U = std::underlying_type_t<E>;
  const auto raw = static_cast<U>(code);
  E out = default_value;
  boost::mp11::mp_for_each<boost::describe::describe_enumerators<E>>(
      [&](auto descriptor) {
        if (static_cast<U>(descriptor.value) == raw) {
          out = descriptor.value;
        }
      });
  return out;
}

} // namespace util

#define DAGFORGE_DEFINE_ENUM_SERDE(EnumType, DefaultValue)                     \
  [[nodiscard]] constexpr auto to_string_view(EnumType value) noexcept         \
      -> std::string_view {                                                    \
    return ::dagforge::util::enum_to_snake_case_view(value);                   \
  }                                                                            \
  template <>                                                                  \
  [[nodiscard]] inline auto parse<EnumType>(std::string_view s) noexcept       \
      -> EnumType {                                                            \
    return ::dagforge::util::parse_enum(s, DefaultValue);                      \
  }

} // namespace dagforge
