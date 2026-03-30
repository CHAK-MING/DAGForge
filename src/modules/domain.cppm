module;

#include <algorithm>
#include <cctype>
#include <chrono>
#include <compare>
#include <concepts>
#include <cstddef>
#include <format>
#include <functional>
#include <optional>
#include <ostream>
#include <random>
#include <string>
#include <string_view>
#include <utility>

export module dagforge.domain;

#define DAGFORGE_BUILDING_MODULE_INTERFACE 1
export {
#include "dagforge/util/id.hpp"
}
#undef DAGFORGE_BUILDING_MODULE_INTERFACE
