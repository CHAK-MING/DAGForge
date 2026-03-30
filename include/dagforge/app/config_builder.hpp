#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/executor/executor.hpp"
#endif

#include <vector>


namespace dagforge {

struct DAGInfo;
class DAG;

class ExecutorConfigBuilder {
public:
  [[nodiscard]] static auto build(const DAGInfo &dag_info, const DAG &graph)
      -> std::vector<ExecutorConfig>;
};

} // namespace dagforge
