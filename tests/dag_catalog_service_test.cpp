#include "dagforge/app/application.hpp"
#include "dagforge/app/services/dag_catalog_service.hpp"
#include "test_utils.hpp"

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>

using namespace dagforge;

namespace {

auto make_catalog_service(Application &app) -> DagCatalogService {
  return DagCatalogService(app.dag_manager(), app.persistence_service(),
                           app.scheduler_service());
}

} // namespace

TEST(DagCatalogServiceTest, LoadDirectoryIsTransactionalWhenAFileIsInvalid) {
  Application app;
  auto service = make_catalog_service(app);

  DAGId existing_dag_id{"existing_dag"};
  DAGInfo existing_info;
  existing_info.dag_id = existing_dag_id;
  existing_info.name = "Existing DAG";
  ASSERT_TRUE(
      app.dag_manager().create_dag(existing_dag_id, existing_info).has_value());

  const auto temp_dir = test::make_temp_dir("dagforge_catalog_dags_");
  ASSERT_FALSE(temp_dir.empty());

  {
    std::ofstream out(std::filesystem::path(temp_dir) / "valid.toml",
                      std::ios::trunc);
    ASSERT_TRUE(out.is_open());
    out << R"(
id = "valid_dag"
name = "valid_dag"

[[tasks]]
id = "task1"
command = "echo 1"
)";
  }

  {
    std::ofstream out(std::filesystem::path(temp_dir) / "invalid.toml",
                      std::ios::trunc);
    ASSERT_TRUE(out.is_open());
    out << R"(
id = "invalid_dag"
name = ""
tasks = []
)";
  }

  const auto load_res = service.load_directory(temp_dir);
  EXPECT_FALSE(load_res.has_value());

  auto existing = app.dag_manager().get_dag(existing_dag_id);
  ASSERT_TRUE(existing.has_value());
  EXPECT_EQ(existing->name, "Existing DAG");
  EXPECT_FALSE(app.dag_manager().has_dag(DAGId{"valid_dag"}));

  std::filesystem::remove_all(temp_dir);
}

TEST(DagCatalogServiceTest, HandleFileChangeDeletesRemovedDag) {
  Application app;
  auto service = make_catalog_service(app);

  const auto temp_dir = test::make_temp_dir("dagforge_catalog_reload_");
  ASSERT_FALSE(temp_dir.empty());

  const auto dag_path = std::filesystem::path(temp_dir) / "dag1.toml";
  {
    std::ofstream out(dag_path, std::ios::trunc);
    ASSERT_TRUE(out.is_open());
    out << R"(
id = "dag1"
name = "dag1"

[[tasks]]
id = "task1"
command = "echo 1"
)";
  }

  auto load_res = service.load_directory(temp_dir);
  ASSERT_TRUE(load_res.has_value());
  ASSERT_TRUE(*load_res);
  ASSERT_TRUE(app.dag_manager().has_dag(DAGId{"dag1"}));

  std::filesystem::remove(dag_path);

  auto change_res = service.handle_file_change(temp_dir, dag_path);
  ASSERT_TRUE(change_res.has_value()) << change_res.error().message();
  EXPECT_FALSE(app.dag_manager().has_dag(DAGId{"dag1"}));

  std::filesystem::remove_all(temp_dir);
}

TEST(DagCatalogServiceTest, HandleFileChangeReloadsUpdatedDag) {
  Application app;
  auto service = make_catalog_service(app);

  const auto temp_dir = test::make_temp_dir("dagforge_catalog_reload_");
  ASSERT_FALSE(temp_dir.empty());

  const auto dag_path = std::filesystem::path(temp_dir) / "dag1.toml";
  {
    std::ofstream out(dag_path, std::ios::trunc);
    ASSERT_TRUE(out.is_open());
    out << R"(
id = "dag1"
name = "dag1"
description = "original"

[[tasks]]
id = "task1"
command = "echo 1"
)";
  }

  auto load_res = service.load_directory(temp_dir);
  ASSERT_TRUE(load_res.has_value());
  ASSERT_TRUE(*load_res);

  {
    std::ofstream out(dag_path, std::ios::trunc);
    ASSERT_TRUE(out.is_open());
    out << R"(
id = "dag1"
name = "dag1"
description = "updated"

[[tasks]]
id = "task1"
command = "echo 2"
)";
  }

  auto change_res = service.handle_file_change(temp_dir, dag_path);
  ASSERT_TRUE(change_res.has_value()) << change_res.error().message();

  auto dag_res = app.dag_manager().get_dag(DAGId{"dag1"});
  ASSERT_TRUE(dag_res.has_value());
  EXPECT_EQ(dag_res->description, "updated");
  ASSERT_EQ(dag_res->tasks.size(), 1);
  EXPECT_EQ(dag_res->tasks.front().command, "echo 2");

  std::filesystem::remove_all(temp_dir);
}

TEST(DagCatalogServiceTest,
     EnsureMaterializedReturnsExistingDagWhenRowidsAlreadyPresent) {
  Application app;
  auto service = make_catalog_service(app);

  DAGInfo info;
  info.dag_id = DAGId{"materialized_dag"};
  info.name = "materialized_dag";
  info.dag_rowid = 17;
  info.tasks.push_back(test::create_task_config(TaskId{"task1"}, "echo 1"));
  info.tasks.front().task_rowid = 42;

  ASSERT_TRUE(app.dag_manager().create_dag(info.dag_id, info).has_value());

  const auto result =
      test::run_coro(service.ensure_materialized(DAGId{"materialized_dag"}));
  ASSERT_TRUE(result.has_value()) << result.error().message();
  EXPECT_EQ(result->dag_rowid, 17);
  ASSERT_EQ(result->tasks.size(), 1);
  EXPECT_EQ(result->tasks.front().task_rowid, 42);
  ASSERT_TRUE(result->compiled_graph);
  ASSERT_TRUE(result->compiled_executor_configs);
  ASSERT_TRUE(result->compiled_indexed_task_configs);
  ASSERT_EQ(result->compiled_indexed_task_configs->size(), 1U);
  EXPECT_EQ((*result->compiled_indexed_task_configs)[0].task_rowid, 42);
}
