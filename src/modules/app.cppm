module;

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_map>
#include <vector>

export module dagforge.app;

export import dagforge.client;
export import dagforge.core;
export import dagforge.dag;

#define DAGFORGE_BUILDING_MODULE_INTERFACE 1
export {
#include "dagforge/app/metrics_registry.hpp"
}
#undef DAGFORGE_BUILDING_MODULE_INTERFACE
