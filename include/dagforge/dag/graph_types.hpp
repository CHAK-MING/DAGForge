#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include <cstdint>
#include <limits>
#endif

namespace dagforge {

using NodeIndex = std::uint32_t;
inline constexpr NodeIndex kInvalidNode =
    std::numeric_limits<NodeIndex>::max();

} // namespace dagforge
