#pragma once

#include "dagforge/config/task_config.hpp"
#include "dagforge/core/error.hpp"
#include "dagforge/util/id.hpp"

#include <ankerl/unordered_dense.h>

#include <cstdint>
#include <functional>
#include <span>
#include <vector>

namespace dagforge {

using NodeIndex = std::uint32_t;
constexpr NodeIndex kInvalidNode = UINT32_MAX;

class DAG {
public:
  [[nodiscard]] auto add_node(TaskId task_id,
                              TriggerRule rule = TriggerRule::AllSuccess,
                              bool is_branch = false) -> Result<NodeIndex>;
  [[nodiscard]] auto add_edge(const TaskId &from, const TaskId &to)
      -> Result<void>;
  [[nodiscard]] auto add_edge(NodeIndex from, NodeIndex to) -> Result<void>;

  [[nodiscard]] auto has_node(const TaskId &task_id) const -> bool;
  [[nodiscard]] auto is_valid() const -> Result<void>;

  [[nodiscard]] auto get_topological_order() const -> std::vector<TaskId>;
  [[nodiscard]] auto get_deps(NodeIndex idx) const -> std::vector<NodeIndex>;
  [[nodiscard]] auto get_dependents(NodeIndex idx) const
      -> std::vector<NodeIndex>;

  [[nodiscard]] auto get_deps_view(NodeIndex idx) const noexcept
      -> std::span<const NodeIndex>;
  [[nodiscard]] auto get_dependents_view(NodeIndex idx) const noexcept
      -> std::span<const NodeIndex>;

  [[nodiscard]] auto get_index(const TaskId &task_id) const -> NodeIndex;
  [[nodiscard]] auto get_key(NodeIndex idx) const -> TaskId;
  [[nodiscard]] auto get_trigger_rule(NodeIndex idx) const noexcept
      -> TriggerRule;
  [[nodiscard]] auto is_branch_task(NodeIndex idx) const noexcept -> bool;

  [[nodiscard]] auto size() const noexcept -> std::size_t {
    return nodes_.size();
  }
  [[nodiscard]] auto empty() const noexcept -> bool { return nodes_.empty(); }
  auto clear() -> void;

  [[nodiscard]] auto all_nodes() const -> std::vector<TaskId>;

private:
  [[nodiscard]] auto has_edge(NodeIndex from, NodeIndex to) const noexcept
      -> bool;

  struct Node {
    std::vector<NodeIndex> deps;
    std::vector<NodeIndex> dependents;
    TriggerRule trigger_rule{TriggerRule::AllSuccess};
    bool is_branch{false};
  };

  std::vector<Node> nodes_;
  std::vector<TaskId> keys_;
  ankerl::unordered_dense::map<TaskId, NodeIndex> key_to_idx_;
};

using TaskDAG = DAG;

} // namespace dagforge
