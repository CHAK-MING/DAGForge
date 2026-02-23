#pragma once

#include "dagforge/executor/executor.hpp"

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
