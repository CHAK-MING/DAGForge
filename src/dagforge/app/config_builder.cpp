#include "dagforge/app/config_builder.hpp"

#include "dagforge/dag/dag.hpp"
#include "dagforge/dag/dag_manager.hpp"

#include <string>
#include <variant>

namespace dagforge {

auto ExecutorConfigBuilder::build(const DAGInfo &dag_info, const DAG &graph)
    -> std::vector<ExecutorConfig> {
  std::vector<ExecutorConfig> cfgs(dag_info.tasks.size());
  auto &registry = ExecutorRegistry::instance();

  for (const auto &task : dag_info.tasks) {
    NodeIndex idx = graph.get_index(task.task_id);
    if (idx != kInvalidNode && idx < cfgs.size()) {
      if (auto config = registry.build_config(task); config) {
        cfgs[idx] = std::move(*config);
      }
    }
  }

  return cfgs;
}

} // namespace dagforge
