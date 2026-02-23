#include "dagforge/dag/dag_domain.hpp"
#include "dagforge/dag/dag_manager.hpp"
#include "dagforge/util/id.hpp"

#include "test_utils.hpp"

#include "gtest/gtest.h"

using namespace dagforge;

TEST(DagStateRecordTest, HasValidRowid) {
  DagStateRecord state;
  EXPECT_FALSE(state.has_valid_rowid());
  state.dag_rowid = 42;
  EXPECT_TRUE(state.has_valid_rowid());
}

TEST(DagStateRecordTest, AllTaskRowidsValid) {
  DagStateRecord state;
  state.dag_rowid = 1;
  auto t1 = test::create_task_config(TaskId{"t1"});
  EXPECT_FALSE(state.all_task_rowids_valid({t1}));
  state.task_rowids.emplace(TaskId{"t1"}, 10);
  EXPECT_TRUE(state.all_task_rowids_valid({t1}));
}

TEST(DagStateRecordTest, AllTaskRowidsValid_NoDagRowid) {
  DagStateRecord state;
  state.task_rowids.emplace(TaskId{"t1"}, 10);
  auto t1 = test::create_task_config(TaskId{"t1"});
  EXPECT_FALSE(state.all_task_rowids_valid({t1}));
}

TEST(MergeDbStateTest, StampsRowidsAndPauseFlag) {
  DAGInfo file_dag;
  file_dag.dag_id = DAGId{"dag1"};
  file_dag.name = "From File";
  file_dag.tasks.push_back(test::create_task_config(TaskId{"t1"}));
  file_dag.tasks.push_back(test::create_task_config(TaskId{"t2"}));
  file_dag.rebuild_task_index();

  DAGInfo db_dag;
  db_dag.dag_id = DAGId{"dag1"};
  db_dag.name = "From DB";
  db_dag.dag_rowid = 7;
  db_dag.version = 3;
  db_dag.is_paused = true;
  auto dt1 = test::create_task_config(TaskId{"t1"});
  dt1.task_rowid = 11;
  auto dt2 = test::create_task_config(TaskId{"t2"});
  dt2.task_rowid = 22;
  db_dag.tasks = {dt1, dt2};
  db_dag.rebuild_task_index();

  merge_db_state_into_dag_info(file_dag, db_dag);

  EXPECT_EQ(file_dag.name, "From File");
  EXPECT_EQ(file_dag.dag_rowid, 7);
  EXPECT_EQ(file_dag.version, 3);
  EXPECT_TRUE(file_dag.is_paused);
  EXPECT_EQ(file_dag.find_task(TaskId{"t1"})->task_rowid, 11);
  EXPECT_EQ(file_dag.find_task(TaskId{"t2"})->task_rowid, 22);
}

TEST(MergeDbStateTest, MissingTaskRowidRemainsZero) {
  DAGInfo file_dag;
  file_dag.dag_id = DAGId{"dag1"};
  file_dag.tasks.push_back(test::create_task_config(TaskId{"t1"}));
  file_dag.tasks.push_back(test::create_task_config(TaskId{"t2"}));
  file_dag.rebuild_task_index();

  DAGInfo db_dag;
  db_dag.dag_id = DAGId{"dag1"};
  db_dag.dag_rowid = 1;
  auto dt1 = test::create_task_config(TaskId{"t1"});
  dt1.task_rowid = 10;
  db_dag.tasks = {dt1};
  db_dag.rebuild_task_index();

  merge_db_state_into_dag_info(file_dag, db_dag);

  EXPECT_EQ(file_dag.find_task(TaskId{"t1"})->task_rowid, 10);
  EXPECT_EQ(file_dag.find_task(TaskId{"t2"})->task_rowid, 0);
}

TEST(StateFromSnapshotInfoTest, ExtractsFromDAGInfo) {
  DAGInfo info;
  info.dag_id = DAGId{"dag1"};
  info.dag_rowid = 55;
  info.version = 4;
  info.is_paused = true;
  auto t1 = test::create_task_config(TaskId{"t1"});
  t1.task_rowid = 88;
  info.tasks.push_back(t1);
  info.rebuild_task_index();

  auto state = state_from_snapshot_info(info);

  EXPECT_EQ(state.dag_rowid, 55);
  EXPECT_EQ(state.version, 4);
  EXPECT_TRUE(state.is_paused);
  ASSERT_TRUE(state.task_rowids.contains(TaskId{"t1"}));
  EXPECT_EQ(state.task_rowids.at(TaskId{"t1"}), 88);
}

TEST(StateFromSnapshotInfoTest, ZeroRowidTasksExcluded) {
  DAGInfo info;
  info.dag_id = DAGId{"dag1"};
  info.dag_rowid = 1;
  info.tasks.push_back(test::create_task_config(TaskId{"t1"}));
  info.rebuild_task_index();

  auto state = state_from_snapshot_info(info);
  EXPECT_FALSE(state.task_rowids.contains(TaskId{"t1"}));
}

TEST(DAGManagerPatchStateTest, PatchUpdatesRowids) {
  DAGManager mgr;
  DAGId id{"dag1"};
  ASSERT_TRUE(
      mgr.create_dag(id, test::create_dag_info_with_task("DAG1", TaskId{"t1"}))
          .has_value());

  DagStateRecord state;
  state.dag_rowid = 100;
  state.version = 2;
  state.updated_at = std::chrono::system_clock::now();
  state.task_rowids.emplace(TaskId{"t1"}, 200);
  mgr.patch_dag_state(id, state);

  auto result = mgr.get_dag(id);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->dag_rowid, 100);
  EXPECT_EQ(result->version, 2);
  EXPECT_EQ(result->tasks[0].task_rowid, 200);
}

TEST(DAGManagerPatchStateTest, PatchNonExistentDagIsNoOp) {
  DAGManager mgr;
  DagStateRecord state;
  state.dag_rowid = 1;
  mgr.patch_dag_state(DAGId{"ghost"}, state);
  EXPECT_FALSE(mgr.has_dag(DAGId{"ghost"}));
}

TEST(DAGManagerApplyStateTest, ApplyUpdatesPauseAndVersion) {
  DAGManager mgr;
  DAGId id{"dag1"};
  ASSERT_TRUE(
      mgr.create_dag(id, test::create_dag_info_with_task("DAG1", TaskId{"t1"}))
          .has_value());

  DagStateRecord state;
  state.is_paused = true;
  state.version = 5;
  state.updated_at = std::chrono::system_clock::now();
  mgr.apply_dag_state(id, state);

  auto result = mgr.get_dag(id);
  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result->is_paused);
  EXPECT_EQ(result->version, 5);
}

TEST(DAGManagerApplyStateTest, ApplyDoesNotTouchTaskRowids) {
  DAGManager mgr;
  DAGId id{"dag1"};
  ASSERT_TRUE(
      mgr.create_dag(id, test::create_dag_info_with_task("DAG1", TaskId{"t1"}))
          .has_value());

  DagStateRecord patch;
  patch.dag_rowid = 10;
  patch.task_rowids.emplace(TaskId{"t1"}, 20);
  mgr.patch_dag_state(id, patch);

  DagStateRecord apply;
  apply.is_paused = true;
  apply.version = 2;
  apply.updated_at = std::chrono::system_clock::now();
  mgr.apply_dag_state(id, apply);

  auto result = mgr.get_dag(id);
  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result->is_paused);
  EXPECT_EQ(result->tasks[0].task_rowid, 20);
}

TEST(DAGManagerMergeTest, FileDagNamePreservedAfterPatch) {
  DAGManager mgr;
  DAGId id{"dag1"};
  DAGInfo file_dag;
  file_dag.dag_id = id;
  file_dag.name = "From File";
  file_dag.tasks.push_back(test::create_task_config(TaskId{"t1"}));
  file_dag.tasks.push_back(test::create_task_config(TaskId{"t2"}));
  file_dag.rebuild_task_index();
  ASSERT_TRUE(mgr.create_dag(id, file_dag).has_value());

  DagStateRecord state;
  state.dag_rowid = 7;
  state.version = 2;
  state.is_paused = true;
  state.task_rowids.emplace(TaskId{"t1"}, 11);
  state.task_rowids.emplace(TaskId{"t2"}, 22);
  mgr.patch_dag_state(id, state);

  auto result = mgr.get_dag(id);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->name, "From File");
  EXPECT_EQ(result->dag_rowid, 7);
  EXPECT_TRUE(result->is_paused);
  EXPECT_EQ(result->find_task(TaskId{"t1"})->task_rowid, 11);
  EXPECT_EQ(result->find_task(TaskId{"t2"})->task_rowid, 22);
}

TEST(DAGManagerMergeTest, DbOnlyDagInserted) {
  DAGManager mgr;
  DAGId id{"db_only"};
  DAGInfo db_dag;
  db_dag.dag_id = id;
  db_dag.name = "DB Only";
  db_dag.dag_rowid = 3;
  db_dag.tasks.push_back(test::create_task_config(TaskId{"t1"}));
  db_dag.tasks[0].task_rowid = 9;
  db_dag.rebuild_task_index();
  ASSERT_TRUE(mgr.upsert_dag(id, db_dag).has_value());

  auto result = mgr.get_dag(id);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->name, "DB Only");
  EXPECT_EQ(result->dag_rowid, 3);
  EXPECT_EQ(result->tasks[0].task_rowid, 9);
}
