#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/config/dag_info_loader.hpp"
#include "dagforge/core/error.hpp"
#endif

#include <filesystem>
#include <string_view>
#include <vector>


namespace dagforge {

struct DAGFile {
  DAGId dag_id;
  DAGInfo info;
};

class DAGFileLoader {
public:
  explicit DAGFileLoader(std::string_view directory);

  [[nodiscard]] auto load_all() -> Result<std::vector<DAGFile>>;
  [[nodiscard]] auto load_all_strict() -> Result<std::vector<DAGFile>>;
  [[nodiscard]] auto load_file(const std::filesystem::path &path)
      -> Result<DAGFile>;

  [[nodiscard]] auto directory() const -> const std::filesystem::path &;

private:
  [[nodiscard]] auto load_all_impl(bool ignore_invalid_files)
      -> Result<std::vector<DAGFile>>;

  std::filesystem::path directory_;
};

} // namespace dagforge
