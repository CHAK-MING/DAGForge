#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include <cstddef>
#include <memory_resource>
#endif

namespace dagforge {

namespace pmr = std::pmr;

using Allocator = pmr::polymorphic_allocator<std::byte>;

namespace detail {
inline thread_local pmr::memory_resource *override_memory_resource = nullptr;
}

class ScopedMemoryResourceOverride {
public:
  explicit ScopedMemoryResourceOverride(pmr::memory_resource *resource)
      : previous_(detail::override_memory_resource) {
    detail::override_memory_resource = resource;
  }

  ScopedMemoryResourceOverride(const ScopedMemoryResourceOverride &) = delete;
  auto operator=(const ScopedMemoryResourceOverride &)
      -> ScopedMemoryResourceOverride & = delete;

  ~ScopedMemoryResourceOverride() {
    detail::override_memory_resource = previous_;
  }

private:
  pmr::memory_resource *previous_;
};

} // namespace dagforge
