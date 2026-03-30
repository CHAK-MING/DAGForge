module;

#include <ankerl/unordered_dense.h>

#include <chrono>
#include <functional>
#include <optional>
#include <regex>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

export module dagforge.xcom;

export import dagforge.base;
export import dagforge.domain;
export import dagforge.task_types;
export import dagforge.util;

#define DAGFORGE_BUILDING_MODULE_INTERFACE 1
export {
#include "dagforge/xcom/xcom_types.hpp"
#include "dagforge/xcom/xcom_util.hpp"
#include "dagforge/xcom/xcom_codec.hpp"
#include "dagforge/xcom/template_resolver.hpp"
}
#undef DAGFORGE_BUILDING_MODULE_INTERFACE
