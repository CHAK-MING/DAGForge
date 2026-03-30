#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/core/coroutine.hpp"
#include "dagforge/dag/dag_manager.hpp"
#endif

#include <ankerl/unordered_dense.h>

#include <filesystem>
#include <string_view>
#include <vector>

namespace dagforge {

class PersistenceService;
class SchedulerService;

class DagCatalogService {
public:
  using DagStateIndex = ankerl::unordered_dense::map<DAGId, DagStateRecord>;

  DagCatalogService(DAGManager &dag_manager, PersistenceService *persistence,
                    SchedulerService *scheduler);

  auto set_persistence_service(PersistenceService *persistence) -> void;
  auto set_scheduler_service(SchedulerService *scheduler) -> void;

  [[nodiscard]] auto load_directory(std::string_view dags_dir) -> Result<bool>;
  [[nodiscard]] auto load_directory(std::string_view dags_dir,
                                    const DagStateIndex *state_index)
      -> Result<bool>;
  [[nodiscard]] auto handle_file_change(std::string_view dags_dir,
                                        const std::filesystem::path &filename)
      -> Result<void>;
  [[nodiscard]] auto ensure_materialized(const DAGId &dag_id)
      -> task<Result<DAGInfo>>;

private:
  [[nodiscard]] auto validate_dag_info(const DAGInfo &info) const
      -> Result<void>;
  [[nodiscard]] auto
  stage_dags_from_directory(std::string_view dags_dir,
                            const DagStateIndex *state_index) const
      -> Result<std::vector<DAGInfo>>;
  [[nodiscard]] auto reload_single_dag(const DAGId &dag_id, const DAGInfo &info)
      -> Result<void>;
  [[nodiscard]] auto materialize_snapshot(const DAGId &dag_id, DAGInfo info,
                                          bool force_refresh)
      -> task<Result<DAGInfo>>;
  auto
  refresh_scheduler_registrations(const std::vector<DAGInfo> &previous_dags)
      -> void;

  DAGManager &dag_manager_;
  PersistenceService *persistence_;
  SchedulerService *scheduler_;
};

} // namespace dagforge
