#pragma once

#include "dagforge/config/dag_definition.hpp"
#include "dagforge/core/error.hpp"

#include <filesystem>
#include <string_view>
#include <vector>

namespace dagforge {

struct DAGFile {
  DAGId dag_id;
  DAGDefinition definition;
};

class DAGFileLoader {
public:
  explicit DAGFileLoader(std::string_view directory);

  [[nodiscard]] auto load_all() -> Result<std::vector<DAGFile>>;
  [[nodiscard]] auto load_file(const std::filesystem::path &path)
      -> Result<DAGFile>;

  [[nodiscard]] auto directory() const -> const std::filesystem::path &;

private:
  std::filesystem::path directory_;
};

} // namespace dagforge
