#include "dagforge/app/application.hpp"
#include "dagforge/dag/dag_manager.hpp"

#include "gtest/gtest.h"

using namespace dagforge;

TEST(IntegrationTest, ApplicationConstructs) {
  Application app;
  EXPECT_FALSE(app.is_running());
}

TEST(IntegrationTest, DagManagerCreateDeleteInMemory) {
  Application app;

  DAGId dag_id{"it_dag"};
  DAGInfo info;
  info.dag_id = dag_id;
  info.name = "Integration DAG";

  auto create = app.dag_manager().create_dag(dag_id, info);
  ASSERT_TRUE(create.has_value());

  auto got = app.dag_manager().get_dag(dag_id);
  ASSERT_TRUE(got.has_value());
  EXPECT_EQ(got->name, "Integration DAG");

  auto del = app.dag_manager().delete_dag(dag_id);
  EXPECT_TRUE(del.has_value());
}
