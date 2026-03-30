#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include <bit>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <string_view>
#include <type_traits>
#endif

namespace dagforge::util {

[[nodiscard]] inline constexpr auto murmur3_mix32(std::uint32_t h) noexcept
    -> std::uint32_t {
  h ^= h >> 16;
  h *= 0x85ebca6b;
  h ^= h >> 13;
  h *= 0xc2b2ae35;
  h ^= h >> 16;
  return h;
}

[[nodiscard]] inline constexpr auto murmur3_mix64(std::uint64_t h) noexcept
    -> std::uint64_t {
  h ^= h >> 33;
  h *= 0xff51afd7ed558ccdULL;
  h ^= h >> 33;
  h *= 0xc4ceb9fe1a85ec53ULL;
  h ^= h >> 33;
  return h;
}

template <typename T>
[[nodiscard]] inline auto hash_value(T value) noexcept -> std::size_t {
  std::uintptr_t raw = 0;
  if constexpr (std::is_pointer_v<T>) {
    raw = reinterpret_cast<std::uintptr_t>(value);
  } else {
    raw = static_cast<std::uintptr_t>(value);
  }

  if constexpr (sizeof(std::uintptr_t) <= 4) {
    return murmur3_mix32(static_cast<std::uint32_t>(raw));
  }
  return static_cast<std::size_t>(
      murmur3_mix64(static_cast<std::uint64_t>(raw)));
}

template <typename T>
[[nodiscard]] inline auto shard_of(T value, unsigned shard_count) noexcept
    -> unsigned {
  return hash_value(value) % shard_count;
}

template <typename T>
inline auto mix_into(std::size_t &seed, const T &value) noexcept -> void {
  constexpr std::size_t kMagic = 0x9e3779b97f4a7c15ULL;
  seed ^= std::hash<T>{}(value) + kMagic + (seed << 6) + (seed >> 2);
}

template <typename... Ts>
[[nodiscard]] inline auto combine(const Ts &...values) noexcept -> std::size_t {
  std::size_t seed = 0;
  (mix_into(seed, values), ...);
  return seed;
}

struct TransparentStringHash {
  using is_transparent = void;

  [[nodiscard]] auto operator()(std::string_view value) const noexcept
      -> std::size_t {
    return std::hash<std::string_view>{}(value);
  }
};

struct TransparentStringEqual {
  using is_transparent = void;

  [[nodiscard]] auto operator()(std::string_view lhs,
                                std::string_view rhs) const noexcept -> bool {
    return lhs == rhs;
  }
};

} // namespace dagforge::util
