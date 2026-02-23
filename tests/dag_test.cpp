#include "dagforge/dag/dag.hpp"
#include "dagforge/util/id.hpp"

#include "gtest/gtest.h"

using namespace dagforge;

class DAGTest : public ::testing::Test {
protected:
  DAG dag_;
};

TEST_F(DAGTest, EmptyDAG) {
  EXPECT_EQ(dag_.size(), 0);
  EXPECT_TRUE(dag_.empty());
  EXPECT_TRUE(dag_.is_valid().has_value());
}

TEST_F(DAGTest, AddSingleNode) {
  auto idx_result = dag_.add_node(TaskId("task1"));
  ASSERT_TRUE(idx_result.has_value());
  auto idx = *idx_result;
  EXPECT_NE(idx, kInvalidNode);
  EXPECT_EQ(dag_.size(), 1);
  EXPECT_TRUE(dag_.has_node(TaskId("task1")));
  EXPECT_EQ(dag_.get_index(TaskId("task1")), idx);
  EXPECT_EQ(dag_.get_key(idx), TaskId("task1"));
}

TEST_F(DAGTest, AddDuplicateNodeReturnsSameIndex) {
  auto idx1_result = dag_.add_node(TaskId("task1"));
  auto idx2_result = dag_.add_node(TaskId("task1"));
  ASSERT_TRUE(idx1_result.has_value());
  ASSERT_TRUE(idx2_result.has_value());
  EXPECT_EQ(*idx1_result, *idx2_result);
  EXPECT_EQ(dag_.size(), 1);
}

TEST_F(DAGTest, AddMultipleNodes) {
  ASSERT_TRUE(dag_.add_node(TaskId("task1")).has_value());
  ASSERT_TRUE(dag_.add_node(TaskId("task2")).has_value());
  ASSERT_TRUE(dag_.add_node(TaskId("task3")).has_value());

  EXPECT_EQ(dag_.size(), 3);
  EXPECT_TRUE(dag_.has_node(TaskId("task1")));
  EXPECT_TRUE(dag_.has_node(TaskId("task2")));
  EXPECT_TRUE(dag_.has_node(TaskId("task3")));

  auto all_nodes = dag_.all_nodes();
  EXPECT_EQ(all_nodes.size(), 3);
}

TEST_F(DAGTest, AddEdgeCreatesDependency) {
  auto from_result = dag_.add_node(TaskId("task1"));
  auto to_result = dag_.add_node(TaskId("task2"));
  ASSERT_TRUE(from_result.has_value());
  ASSERT_TRUE(to_result.has_value());
  auto from = *from_result;
  auto to = *to_result;

  auto result = dag_.add_edge(TaskId("task1"), TaskId("task2"));
  EXPECT_TRUE(result.has_value());

  auto deps = dag_.get_deps(to);
  EXPECT_EQ(deps.size(), 1);
  EXPECT_EQ(deps[0], from);

  auto dependents = dag_.get_dependents(from);
  EXPECT_EQ(dependents.size(), 1);
  EXPECT_EQ(dependents[0], to);
}

TEST_F(DAGTest, AddEdgeByIndex) {
  auto from_result = dag_.add_node(TaskId("task1"));
  auto to_result = dag_.add_node(TaskId("task2"));
  ASSERT_TRUE(from_result.has_value());
  ASSERT_TRUE(to_result.has_value());
  auto from = *from_result;
  auto to = *to_result;

  auto result = dag_.add_edge(from, to);
  EXPECT_TRUE(result.has_value());

  auto deps = dag_.get_deps(to);
  EXPECT_EQ(deps.size(), 1);
  EXPECT_EQ(deps[0], from);
}

TEST_F(DAGTest, GetDepsView) {
  ASSERT_TRUE(dag_.add_node(TaskId("task1")).has_value());
  ASSERT_TRUE(dag_.add_node(TaskId("task2")).has_value());
  ASSERT_TRUE(dag_.add_node(TaskId("task3")).has_value());

  EXPECT_TRUE(dag_.add_edge(TaskId("task1"), TaskId("task3")).has_value());
  EXPECT_TRUE(dag_.add_edge(TaskId("task2"), TaskId("task3")).has_value());

  auto idx3 = dag_.get_index(TaskId("task3"));
  auto deps_view = dag_.get_deps_view(idx3);
  EXPECT_EQ(deps_view.size(), 2);
}

TEST_F(DAGTest, GetDependentsView) {
  ASSERT_TRUE(dag_.add_node(TaskId("task1")).has_value());
  ASSERT_TRUE(dag_.add_node(TaskId("task2")).has_value());
  ASSERT_TRUE(dag_.add_node(TaskId("task3")).has_value());

  EXPECT_TRUE(dag_.add_edge(TaskId("task1"), TaskId("task2")).has_value());
  EXPECT_TRUE(dag_.add_edge(TaskId("task1"), TaskId("task3")).has_value());

  auto idx1 = dag_.get_index(TaskId("task1"));
  auto dependents_view = dag_.get_dependents_view(idx1);
  EXPECT_EQ(dependents_view.size(), 2);
}

TEST_F(DAGTest, TopologicalOrder) {
  ASSERT_TRUE(dag_.add_node(TaskId("task1")).has_value());
  ASSERT_TRUE(dag_.add_node(TaskId("task2")).has_value());
  ASSERT_TRUE(dag_.add_node(TaskId("task3")).has_value());

  EXPECT_TRUE(dag_.add_edge(TaskId("task1"), TaskId("task2")).has_value());
  EXPECT_TRUE(dag_.add_edge(TaskId("task2"), TaskId("task3")).has_value());

  auto order = dag_.get_topological_order();
  EXPECT_EQ(order.size(), 3);
  EXPECT_EQ(order[0], TaskId("task1"));
  EXPECT_EQ(order[1], TaskId("task2"));
  EXPECT_EQ(order[2], TaskId("task3"));
}

TEST_F(DAGTest, CycleDetection) {
  ASSERT_TRUE(dag_.add_node(TaskId("task1")).has_value());
  ASSERT_TRUE(dag_.add_node(TaskId("task2")).has_value());
  ASSERT_TRUE(dag_.add_node(TaskId("task3")).has_value());

  EXPECT_TRUE(dag_.add_edge(TaskId("task1"), TaskId("task2")).has_value());
  EXPECT_TRUE(dag_.add_edge(TaskId("task2"), TaskId("task3")).has_value());

  // add_edge no longer performs immediate cycle detection for performance
  // reasons
  auto cycle_result = dag_.add_edge(TaskId("task3"), TaskId("task1"));
  EXPECT_TRUE(cycle_result.has_value());

  // Cycle is detected during validation
  EXPECT_FALSE(dag_.is_valid().has_value());
}

TEST_F(DAGTest, IsValidWithCycle) {
  ASSERT_TRUE(dag_.add_node(TaskId("task1")).has_value());
  ASSERT_TRUE(dag_.add_node(TaskId("task2")).has_value());

  EXPECT_TRUE(dag_.add_edge(TaskId("task1"), TaskId("task2")).has_value());
  EXPECT_TRUE(dag_.is_valid().has_value());

  EXPECT_TRUE(dag_.add_edge(TaskId("task2"), TaskId("task1")).has_value());
  EXPECT_FALSE(dag_.is_valid().has_value());
}

TEST_F(DAGTest, Clear) {
  ASSERT_TRUE(dag_.add_node(TaskId("task1")).has_value());
  ASSERT_TRUE(dag_.add_node(TaskId("task2")).has_value());
  EXPECT_TRUE(dag_.add_edge(TaskId("task1"), TaskId("task2")).has_value());

  EXPECT_EQ(dag_.size(), 2);

  dag_.clear();

  EXPECT_EQ(dag_.size(), 0);
  EXPECT_TRUE(dag_.empty());
}

TEST_F(DAGTest, GetNonExistentNode) {
  auto idx = dag_.get_index(TaskId("nonexistent"));
  EXPECT_EQ(idx, kInvalidNode);
}

TEST_F(DAGTest, HasNodeNonExistent) {
  EXPECT_FALSE(dag_.has_node(TaskId("nonexistent")));
}

TEST_F(DAGTest, AllNodes) {
  ASSERT_TRUE(dag_.add_node(TaskId("a")).has_value());
  ASSERT_TRUE(dag_.add_node(TaskId("b")).has_value());
  ASSERT_TRUE(dag_.add_node(TaskId("c")).has_value());

  auto nodes = dag_.all_nodes();
  EXPECT_EQ(nodes.size(), 3);
}

TEST_F(DAGTest, ComplexDAG) {
  ASSERT_TRUE(dag_.add_node(TaskId("start")).has_value());
  ASSERT_TRUE(dag_.add_node(TaskId("middle1")).has_value());
  ASSERT_TRUE(dag_.add_node(TaskId("middle2")).has_value());
  ASSERT_TRUE(dag_.add_node(TaskId("end")).has_value());

  EXPECT_TRUE(dag_.add_edge(TaskId("start"), TaskId("middle1")).has_value());
  EXPECT_TRUE(dag_.add_edge(TaskId("start"), TaskId("middle2")).has_value());
  EXPECT_TRUE(dag_.add_edge(TaskId("middle1"), TaskId("end")).has_value());
  EXPECT_TRUE(dag_.add_edge(TaskId("middle2"), TaskId("end")).has_value());

  EXPECT_TRUE(dag_.is_valid().has_value());

  auto order = dag_.get_topological_order();
  EXPECT_EQ(order.size(), 4);
  EXPECT_EQ(order[0], TaskId("start"));
  EXPECT_EQ(order[3], TaskId("end"));
}

TEST_F(DAGTest, AddEdge_SelfLoop_Fails) {
  ASSERT_TRUE(dag_.add_node(TaskId("task1")).has_value());

  auto result = dag_.add_edge(TaskId("task1"), TaskId("task1"));

  EXPECT_FALSE(result.has_value());
}

TEST_F(DAGTest, AddEdge_TwoNodeCycle_AllowsEdgeButIsValidFails) {
  ASSERT_TRUE(dag_.add_node(TaskId("task1")).has_value());
  ASSERT_TRUE(dag_.add_node(TaskId("task2")).has_value());
  EXPECT_TRUE(dag_.add_edge(TaskId("task1"), TaskId("task2")).has_value());
  EXPECT_TRUE(dag_.add_edge(TaskId("task2"), TaskId("task1")).has_value());
  EXPECT_FALSE(dag_.is_valid().has_value());
}

TEST_F(DAGTest, AddEdge_NonExistentSource_Fails) {
  ASSERT_TRUE(dag_.add_node(TaskId("task2")).has_value());

  auto result = dag_.add_edge(TaskId("nonexistent"), TaskId("task2"));

  EXPECT_FALSE(result.has_value());
}
