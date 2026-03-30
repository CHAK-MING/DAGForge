#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/scheduler/cron.hpp"
#endif

#if !defined(DAGFORGE_BUILDING_MODULE_INTERFACE) &&                              \
    (!defined(DAGFORGE_CONSUME_NAMED_MODULES) ||                                \
     !DAGFORGE_CONSUME_NAMED_MODULES)
#include "dagforge/util/id.hpp"
#endif

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include <chrono>
#include <optional>
#include <string>
#endif

namespace dagforge {

struct ExecutionInfo {
  DAGId dag_id;
  TaskId task_id;
  std::string name;
  std::optional<CronExpr> cron_expr;
  std::optional<std::chrono::system_clock::time_point> start_date;
  std::optional<std::chrono::system_clock::time_point> end_date;
  bool catchup{false};
};

} // namespace dagforge
