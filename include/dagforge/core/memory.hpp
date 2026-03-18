#pragma once

#include <cstddef>
#include <memory_resource>

namespace dagforge {

namespace pmr = std::pmr;

using Allocator = pmr::polymorphic_allocator<std::byte>;

} // namespace dagforge
