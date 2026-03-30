#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/core/memory.hpp"

#include <array>
#include <cstddef>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>
#endif

namespace dagforge {

template <std::size_t N = 1024> class Arena {
public:
  Arena() : resource_(buffer_.data(), buffer_.size()) {}

  Arena(const Arena &) = delete;
  Arena &operator=(const Arena &) = delete;

  template <typename T> [[nodiscard]] auto vector() -> pmr::vector<T> {
    return pmr::vector<T>(&resource_);
  }

  template <typename T>
  [[nodiscard]] auto vector(std::size_t reserve_size) -> pmr::vector<T> {
    pmr::vector<T> v(&resource_);
    v.reserve(reserve_size);
    return v;
  }

  [[nodiscard]] auto string() -> pmr::string {
    return pmr::string(&resource_);
  }

  [[nodiscard]] auto string(std::string_view s) -> pmr::string {
    return pmr::string(s, &resource_);
  }

  template <typename K, typename V, typename Hash = std::hash<K>,
            typename Eq = std::equal_to<K>>
  [[nodiscard]] auto unordered_map()
      -> pmr::unordered_map<K, V, Hash, Eq> {
    return pmr::unordered_map<K, V, Hash, Eq>(&resource_);
  }

  [[nodiscard]] auto resource() -> pmr::memory_resource * {
    return &resource_;
  }

  void reset() { resource_.release(); }

private:
  std::array<std::byte, N> buffer_;
  pmr::monotonic_buffer_resource resource_;
};

template <typename T>
[[nodiscard]] auto to_std(pmr::vector<T> &&pmr_vec) -> std::vector<T> {
  return std::vector<T>(std::make_move_iterator(pmr_vec.begin()),
                        std::make_move_iterator(pmr_vec.end()));
}

[[nodiscard]] inline auto to_std(pmr::string &&pmr_str) -> std::string {
  return std::string(pmr_str);
}

} // namespace dagforge
